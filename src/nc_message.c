/*
 * twemproxy - A fast and lightweight proxy for memcached protocol.
 * Copyright (C) 2011 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <stdio.h>
#include <stdlib.h>

#include <sys/uio.h>

#include <nc_core.h>
#include <nc_server.h>
#include <proto/nc_proto.h>

#if (IOV_MAX > 128)
#define NC_IOV_MAX 128
#else
#define NC_IOV_MAX IOV_MAX
#endif

/*
 *            nc_message.[ch]
 *         message (struct msg)
 *            +        +            .
 *            |        |            .
 *            /        \            .
 *         Request    Response      .../ nc_mbuf.[ch]  (mesage buffers)
 *      nc_request.c  nc_response.c .../ nc_memcache.c; nc_redis.c (message parser)
 *
 * Messages in nutcracker are manipulated by a chain of processing handlers,
 * where each handler is responsible for taking the input and producing an
 * output for the next handler in the chain. This mechanism of processing
 * loosely conforms to the standard chain-of-responsibility design pattern
 *
 * At the high level, each handler takes in a message: request or response
 * and produces the message for the next handler in the chain. The input
 * for a handler is either a request or response, but never both and
 * similarly the output of an handler is either a request or response or
 * nothing.
 *
 * Each handler itself is composed of two processing units:
 *
 * 1). filter: manipulates output produced by the handler, usually based
 *     on a policy. If needed, multiple filters can be hooked into each
 *     location.
 * 2). forwarder: chooses one of the backend servers to send the request
 *     to, usually based on the configured distribution and key hasher.
 *
 * Handlers are registered either with Client or Server or Proxy
 * connections. A Proxy connection only has a read handler as it is only
 * responsible for accepting new connections from client. Read handler
 * (conn_recv_t) registered with client is responsible for reading requests,
 * while that registered with server is responsible for reading responses.
 * Write handler (conn_send_t) registered with client is responsible for
 * writing response, while that registered with server is responsible for
 * writing requests.
 *
 * Note that in the above discussion, the terminology send is used
 * synonymously with write or OUT event. Similarly recv is used synonymously
 * with read or IN event
 *
 *             Client+             Proxy           Server+
 *                              (nutcracker)
 *                                   .
 *       msg_recv {read event}       .       msg_recv {read event}
 *         +                         .                         +
 *         |                         .                         |
 *         \                         .                         /
 *         req_recv_next             .             rsp_recv_next
 *           +                       .                       +
 *           |                       .                       |       Rsp
 *           req_recv_done           .           rsp_recv_done      <===
 *             +                     .                     +
 *             |                     .                     |
 *    Req      \                     .                     /
 *    ===>     req_filter*           .           *rsp_filter
 *               +                   .                   +
 *               |                   .                   |
 *               \                   .                   /
 *               req_forward-//  (a) . (c)  \\-rsp_forward
 *                                   .
 *                                   .
 *       msg_send {write event}      .      msg_send {write event}
 *         +                         .                         +
 *         |                         .                         |
 *    Rsp' \                         .                         /     Req'
 *   <===  rsp_send_next             .             req_send_next     ===>
 *           +                       .                       +
 *           |                       .                       |
 *           \                       .                       /
 *           rsp_send_done-//    (d) . (b)    //-req_send_done
 *
 *
 * (a) -> (b) -> (c) -> (d) is the normal flow of transaction consisting
 * of a single request response, where (a) and (b) handle request from
 * client, while (c) and (d) handle the corresponding response from the
 * server.
 */

static uint64_t msg_id;          /* message id counter */
static uint64_t frag_id;         /* fragment id counter */
static uint32_t nfree_msgq;      /* # free msg q */
static struct msg_tqh free_msgq; /* free msg q */
static struct rbtree tmo_rbt;    /* timeout rbtree */
static struct rbnode tmo_rbs;    /* timeout rbtree sentinel */

#define CONNECTION_CODEC(ACTION)               \
    ACTION( CONN_NONE,       none        ) \
    ACTION( CONN_RIAK,       riak        ) \
    ACTION( CONN_REDIS,      redis       ) \
    ACTION( CONN_MEMCACHE,   memcache    ) \
/* TODO: investigate why we are needing to define DEFINE_ACTION here again */
#define DEFINE_ACTION(_conn, _name) string(#_name),
struct string connection_strings[] = {
    CONNECTION_CODEC( DEFINE_ACTION )
    null_string
};
#undef DEFINE_ACTION

#define DEFINE_ACTION(_name) string(#_name),
static struct string msg_type_strings[] = {
    MSG_TYPE_CODEC( DEFINE_ACTION )
    null_string
};
#undef DEFINE_ACTION

struct mbuf* get_next_mbuf(struct msg* msg);

static struct msg *
msg_from_rbe(struct rbnode *node)
{
    struct msg *msg;
    int offset;

    offset = offsetof(struct msg, tmo_rbe);
    msg = (struct msg *)((char *)node - offset);

    return msg;
}

struct msg *
msg_tmo_min(void)
{
    struct rbnode *node;

    node = rbtree_min(&tmo_rbt);
    if (node == NULL) {
        return NULL;
    }

    return msg_from_rbe(node);
}

void
msg_tmo_insert(struct msg *msg, struct conn *conn)
{
    struct rbnode *node;
    int timeout;

    ASSERT(msg->request);
    ASSERT(!msg->quit && !msg->noreply);

    timeout = server_timeout(conn);
    if (timeout <= 0) {
        return;
    }

    node = &msg->tmo_rbe;
    node->key = nc_msec_now() + timeout;
    node->data = conn;

    rbtree_insert(&tmo_rbt, node);

    log_debug(LOG_VERB, "insert msg %"PRIu64" into tmo rbt with expiry of "
              "%d msec", msg->id, timeout);
}

void
msg_tmo_delete(struct msg *msg)
{
    struct rbnode *node;

    node = &msg->tmo_rbe;

    /* already deleted */

    if (node->data == NULL) {
        return;
    }

    rbtree_delete(&tmo_rbt, node);

    log_debug(LOG_VERB, "delete msg %"PRIu64" from tmo rbt", msg->id);
}

static struct msg *
_msg_get(void)
{
    struct msg *msg;

    if (!TAILQ_EMPTY(&free_msgq)) {
        ASSERT(nfree_msgq > 0);

        msg = TAILQ_FIRST(&free_msgq);
        nfree_msgq--;
        TAILQ_REMOVE(&free_msgq, msg, m_tqe);
        goto done;
    }

    msg = nc_alloc(sizeof(*msg));
    if (msg == NULL) {
        return NULL;
    }

    msg->backend_resend_servers = NULL;

done:
    /* c_tqe, s_tqe, and m_tqe are left uninitialized */
    msg->id = ++msg_id;
    msg->peer = NULL;
    msg->owner = NULL;

    rbtree_node_init(&msg->tmo_rbe);

    STAILQ_INIT(&msg->mhdr);
    msg->mlen = 0;
    msg->start_ts = 0;

    msg->state = 0;
    msg->pos = NULL;
    msg->token = NULL;

    msg->parser = NULL;
    msg->add_auth = NULL;
    msg->result = MSG_PARSE_OK;

    msg->fragment = NULL;
    msg->reply = NULL;
    msg->pre_coalesce = NULL;
    msg->post_coalesce = NULL;

    msg->backend_process = NULL;
    msg->backend_parser = NULL;

    msg->type = MSG_UNKNOWN;

    msg->keys = array_create(1, sizeof(struct keypos));
    if (msg->keys == NULL) {
        nc_free(msg);
        return NULL;
    }

    if (msg->backend_resend_servers == NULL) {
        msg->backend_resend_servers = array_create(1, sizeof(struct server**));
        if (msg->backend_resend_servers == NULL) {
            nc_free(msg);
            return NULL;
        }
    }

    msg->vlen = 0;
    msg->end = NULL;

    msg->frag_owner = NULL;
    msg->frag_seq = NULL;
    msg->nfrag = 0;
    msg->nfrag_done = 0;
    msg->frag_id = 0;

    msg->narg_start = NULL;
    msg->narg_end = NULL;
    msg->narg = 0;
    msg->rnarg = 0;
    msg->rlen = 0;
    msg->integer = 0;

    msg->err = 0;
    msg->error = 0;
    msg->ferror = 0;
    msg->request = 0;
    msg->quit = 0;
    msg->noreply = 0;
    msg->noforward = 0;
    msg->done = 0;
    msg->fdone = 0;
    msg->swallow = 0;
    msg->redis = 0;

    return msg;
}

/**.......................................................................
 * Get a new message from the pool of free messages.  Initializes
 * message handlers depending on the type of connection
 */
struct msg *
msg_get(struct conn *conn, bool request)
{
    struct msg *msg = NULL;

    msg = _msg_get();
    if (msg == NULL) {
        return NULL;
    }

    msg->owner = conn;
    msg->request = request ? 1 : 0;

    switch (conn->type) {
    case CONN_REDIS:
        if (request) {
            msg->parser = redis_parse_req;
            msg->repack = redis_repack;
        } else {
            msg->parser = redis_parse_rsp;
            msg->repack = redis_repack;
        }

        msg->add_auth = redis_add_auth_packet;
        msg->fragment = redis_fragment;
        msg->reply = redis_reply;
        msg->pre_coalesce = redis_pre_coalesce;
        msg->post_coalesce = redis_post_coalesce;
        break;

    case CONN_RIAK:
        if (request) {
            msg->parser = riak_parse_req;
            msg->repack = riak_repack;
        } else {
            msg->parser = riak_parse_rsp;
            msg->repack = riak_repack;
        }

        msg->add_auth = riak_add_auth_packet;
        msg->fragment = riak_fragment;
        msg->pre_coalesce = riak_pre_coalesce;
        msg->post_coalesce = riak_post_coalesce;
        break;

    default:
        if (request) {
            msg->parser = memcache_parse_req;
            msg->repack = memcache_repack;
        } else {
            msg->parser = memcache_parse_rsp;
            msg->repack = memcache_repack;
        }

        msg->add_auth = memcache_add_auth_packet;
        msg->fragment = memcache_fragment;
        msg->pre_coalesce = memcache_pre_coalesce;
        msg->post_coalesce = memcache_post_coalesce;
        break;
    }

    msg->backend_process = backend_process_rsp;

    if (log_loggable(LOG_NOTICE) != 0) {
        msg->start_ts = nc_usec_now();
    }

    log_debug(LOG_VVERB, "get msg %p id %"PRIu64" request %d owner sd %d",
              msg, msg->id, msg->request, conn->sd);

    return msg;
}

struct msg *
msg_get_error(struct conn* conn, err_t err)
{
    struct msg *msg;
    struct mbuf *mbuf;
    int n;
    char *errstr = err ? strerror(err) : "unknown";
    char *protstr = (conn->type == CONN_REDIS) ? "-ERR" : "SERVER_ERROR";

    msg = _msg_get();
    if (msg == NULL) {
        return NULL;
    }

    msg->state = 0;
    msg->type = MSG_RSP_MC_SERVER_ERROR;
    msg->error = 1;

    mbuf = mbuf_get();
    if (mbuf == NULL) {
        msg_put(msg);
        return NULL;
    }
    mbuf_insert(&msg->mhdr, mbuf);

    n = nc_scnprintf(mbuf->last, mbuf_size(mbuf), "%s %s"CRLF, protstr, errstr);
    mbuf->last += n;
    msg->mlen = (uint32_t)n;

    log_debug(LOG_VVERB, "get msg %p id %"PRIu64" len %"PRIu32" error '%s'",
              msg, msg->id, msg->mlen, errstr);

    return msg;
}

static void
msg_free(struct msg *msg)
{
    ASSERT(STAILQ_EMPTY(&msg->mhdr));

    if (msg->backend_resend_servers != NULL) {
        msg->backend_resend_servers->nelem = 0;
        array_destroy(msg->backend_resend_servers);
        msg->backend_resend_servers = NULL;
    }

    log_debug(LOG_VVERB, "free msg %p id %"PRIu64"", msg, msg->id);
    nc_free(msg);
}

void
msg_put(struct msg *msg)
{
    log_debug(LOG_VVERB, "put msg %p id %"PRIu64"", msg, msg->id);

    while (!STAILQ_EMPTY(&msg->mhdr)) {
        struct mbuf *mbuf = STAILQ_FIRST(&msg->mhdr);
        mbuf_remove(&msg->mhdr, mbuf);
        mbuf_put(mbuf);
    }

    if (msg->frag_seq) {
        nc_free(msg->frag_seq);
        msg->frag_seq = NULL;
    }

    if (msg->keys) {
        msg->keys->nelem = 0; /* a hack here */
        array_destroy(msg->keys);
        msg->keys = NULL;
    }

    if (msg->backend_resend_servers) {
        msg->backend_resend_servers->nelem = 0;
    }

    nfree_msgq++;
    TAILQ_INSERT_HEAD(&free_msgq, msg, m_tqe);
}

void
msg_dump(struct msg *msg, int level)
{
    struct mbuf *mbuf;
    struct string *msg_type;

    if (log_loggable(level) == 0) {
        return;
    }

    msg_type = msg_type_string(msg->type);

    loga("msg dump id %"PRIu64" request %d len %"PRIu32" type %.*s done %d "
         "error %d (err %d)", msg->id, msg->request, msg->mlen, msg_type->len,
         msg_type->data, msg->done, msg->error, msg->err);

    STAILQ_FOREACH(mbuf, &msg->mhdr, next) {
        uint8_t *p, *q;
        long int len;

        p = mbuf->start;
        q = mbuf->last;
        len = q - p;

        loga_hexdump(p, len, "mbuf [%p] with %ld bytes of data", p, len);
    }
}

void
msg_init(void)
{
    log_debug(LOG_DEBUG, "msg size %d", sizeof(struct msg));
    msg_id = 0;
    frag_id = 0;
    nfree_msgq = 0;
    TAILQ_INIT(&free_msgq);
    rbtree_init(&tmo_rbt, &tmo_rbs);
}

void
msg_deinit(void)
{
    struct msg *msg, *nmsg;

    for (msg = TAILQ_FIRST(&free_msgq); msg != NULL;
         msg = nmsg, nfree_msgq--) {
        ASSERT(nfree_msgq > 0);
        nmsg = TAILQ_NEXT(msg, m_tqe);
        msg_free(msg);
    }
    ASSERT(nfree_msgq == 0);
}

struct string *
msg_type_string(msg_type_t type)
{
    return &msg_type_strings[type];
}

bool
msg_empty(struct msg *msg)
{
    return msg->mlen == 0 ? true : false;
}

bool 
msg_nil(struct msg *msg)
{
    struct mbuf *mbuf;
    bool matches_nil;

    /* applies only to redis replies */
    matches_nil = false;
    if (msg->type == MSG_RSP_REDIS_BULK) {
        STAILQ_FOREACH(mbuf, &msg->mhdr, next) {
            uint8_t nil[5] = {0x24, 0x2d, 0x31, 0x0d, 0x0a};
            uint8_t *p, *q, cur;
            long int len;

            p = mbuf->start;
            q = mbuf->last;
            len = q - p;

            /* dead-simple memory comparison of 5-byte responses against
             * the value redis returns for nils */
            if (len == 5) {
                matches_nil = true;
                for (cur=0; cur<5; ++cur) {
                    if (*(p+cur) != *(nil+cur)) {
                        matches_nil = false;
                        break;
                    }
                }
            }
        }
    }

    return matches_nil;
}

uint32_t
msg_backend_idx(struct msg *msg, uint8_t *key, uint32_t keylen)
{
    struct conn *conn = msg->owner;
    ASSERT(conn != NULL);
    struct server_pool *pool = conn->owner;
    ASSERT(pool != NULL);

    return servers_idx(&pool->frontends, key, keylen);
}

struct mbuf *
msg_ensure_mbuf(struct msg *msg, size_t len)
{
    struct mbuf *mbuf;

    if (STAILQ_EMPTY(&msg->mhdr) ||
        mbuf_size(STAILQ_LAST(&msg->mhdr, mbuf, next)) < len) {
        mbuf = mbuf_get();
        if (mbuf == NULL) {
            return NULL;
        }
        mbuf_insert(&msg->mhdr, mbuf);
    } else {
        mbuf = STAILQ_LAST(&msg->mhdr, mbuf, next);
    }
    return mbuf;
}

/*
 * append small(small than a mbuf) content into msg
 */
rstatus_t
msg_append(struct msg *msg, uint8_t *pos, size_t n)
{
    struct mbuf *mbuf;

    ASSERT(n <= mbuf_data_size());

    mbuf = msg_ensure_mbuf(msg, n);
    if (mbuf == NULL) {
        return NC_ENOMEM;
    }

    ASSERT(n <= mbuf_size(mbuf));

    mbuf_copy(mbuf, pos, n);
    msg->mlen += (uint32_t)n;
    return NC_OK;
}

/*
 * prepend small(small than a mbuf) content into msg
 */
rstatus_t
msg_prepend(struct msg *msg, uint8_t *pos, size_t n)
{
    struct mbuf *mbuf;

    mbuf = mbuf_get();
    if (mbuf == NULL) {
        return NC_ENOMEM;
    }

    ASSERT(n <= mbuf_size(mbuf));

    mbuf_copy(mbuf, pos, n);
    msg->mlen += (uint32_t)n;

    STAILQ_INSERT_HEAD(&msg->mhdr, mbuf, next);
    return NC_OK;
}

/*
 * Prepend a formatted string into msg. Returns an error if the formatted
 * string does not fit in a single mbuf.
 */
rstatus_t
msg_prepend_format(struct msg *msg, const char *fmt, ...)
{
    struct mbuf *mbuf;
    int n;
    uint32_t size;
    va_list args;

    mbuf = mbuf_get();
    if (mbuf == NULL) {
        return NC_ENOMEM;
    }

    size = mbuf_size(mbuf);

    va_start(args, fmt);
    n = nc_vsnprintf(mbuf->last, size, fmt, args);
    va_end(args);
    if (n <= 0 || n >= (int)size) {
        return NC_ERROR;
    }

    mbuf->last += n;
    msg->mlen += (uint32_t)n;
    STAILQ_INSERT_HEAD(&msg->mhdr, mbuf, next);

    return NC_OK;
}

inline uint64_t
msg_gen_frag_id(void)
{
    return ++frag_id;
}

static rstatus_t
msg_parsed(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct msg *nmsg;
    struct mbuf *mbuf, *nbuf;

    mbuf = STAILQ_LAST(&msg->mhdr, mbuf, next);
    if (msg->pos == mbuf->last) {
        /* no more data to parse -- repack the message if necessary, and return */
        msg->repack(msg);
        conn->recv_done(ctx, conn, msg, NULL, false, true);

        if (msg->error) {
            return NC_ERROR;
        } else {
            return NC_OK;
        }
    }

    /*
     * Input mbuf has un-parsed data. Split mbuf of the current message msg
     * into (mbuf, nbuf), where mbuf is the portion of the message that has
     * been parsed and nbuf is the portion of the message that is un-parsed.
     * Parse nbuf as a new message nmsg in the next iteration.
     */
    nbuf = mbuf_split(&msg->mhdr, msg->pos, NULL, NULL);
    if (nbuf == NULL) {
        return NC_ENOMEM;
    }

    nmsg = msg_get(msg->owner, msg->request);
    if (nmsg == NULL) {
        mbuf_put(nbuf);
        return NC_ENOMEM;
    }
    mbuf_insert(&nmsg->mhdr, nbuf);
    nmsg->pos = nbuf->pos;

    /* update length of current (msg) and new message (nmsg) */
    nmsg->mlen = mbuf_length(nbuf);
    msg->mlen -= nmsg->mlen;

    msg->repack(msg);
    conn->recv_done(ctx, conn, msg, nmsg, false, true);

    return NC_OK;
}

static rstatus_t
msg_repair(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct mbuf *nbuf;

    nbuf = mbuf_split(&msg->mhdr, msg->pos, NULL, NULL);
    if (nbuf == NULL) {
        return NC_ENOMEM;
    }
    mbuf_insert(&msg->mhdr, nbuf);
    msg->pos = nbuf->pos;

    return NC_OK;
}

static rstatus_t
msg_parse(struct context *ctx, struct conn *conn, struct msg *msg)
{
    rstatus_t status;

    if (msg_empty(msg)) {
        /* no data to parse */
        conn->recv_done(ctx, conn, msg, NULL, false, true);
        return NC_OK;
    }

    msg->parser(msg);

    switch (msg->result) {
    case MSG_PARSE_OK:
        status = msg_parsed(ctx, conn, msg);
        break;

    case MSG_PARSE_REPAIR:
        status = msg_repair(ctx, conn, msg);
        break;

    case MSG_PARSE_AGAIN:
        status = NC_OK;
        break;

    default:
        status = NC_ERROR;
        conn->err = errno;
        break;
    }

    return conn->err != 0 ? NC_ERROR : status;
}

static rstatus_t
msg_recv_chain(struct context *ctx, struct conn *conn, struct msg *msg)
{
    rstatus_t status;
    struct msg *nmsg;
    struct mbuf *mbuf;
    size_t msize;
    ssize_t n;

    mbuf = STAILQ_LAST(&msg->mhdr, mbuf, next);
    if (mbuf == NULL || mbuf_full(mbuf)) {
        mbuf = mbuf_get();
        if (mbuf == NULL) {
            return NC_ENOMEM;
        }
        mbuf_insert(&msg->mhdr, mbuf);
        msg->pos = mbuf->pos;
    }
    ASSERT(mbuf->end - mbuf->last > 0);

    msize = mbuf_size(mbuf);

    n = conn_recv(conn, mbuf->last, msize);
    if (n < 0) {
        if (n == NC_EAGAIN) {
            return NC_OK;
        }
        return NC_ERROR;
    }

    ASSERT((mbuf->last + n) <= mbuf->end);
    mbuf->last += n;
    msg->mlen += (uint32_t)n;

    for (;;) {
        status = msg_parse(ctx, conn, msg);
        if (status != NC_OK) {
            return status;
        }

        /* get next message to parse */
        nmsg = conn->recv_next(ctx, conn, false);
        if (nmsg == NULL || nmsg == msg) {
            /* no more data to parse */
            break;
        }

        msg = nmsg;
    }

    return NC_OK;
}

rstatus_t
msg_recv(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *msg;

    ASSERT(conn->recv_active);

    conn->recv_ready = 1;
    do {
        msg = conn->recv_next(ctx, conn, true);
        if (msg == NULL) {
            return NC_OK;
        }

        status = msg_recv_chain(ctx, conn, msg);
        if (status != NC_OK) {
            return status;
        }
    } while (conn->recv_ready);

    return NC_OK;
}

static rstatus_t
msg_send_chain(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct msg_tqh send_msgq;            /* send msg q */
    struct msg *nmsg;                    /* next msg */
    struct mbuf *mbuf, *nbuf;            /* current and next mbuf */
    size_t mlen;                         /* current mbuf data length */
    struct iovec *ciov, iov[NC_IOV_MAX]; /* current iovec */
    struct array sendv;                  /* send iovec */
    size_t nsend, nsent;                 /* bytes to send; bytes sent */
    size_t limit;                        /* bytes to send limit */
    ssize_t n;                           /* bytes sent by sendv */

    TAILQ_INIT(&send_msgq);

    array_set(&sendv, iov, sizeof(iov[0]), NC_IOV_MAX);

    /* preprocess - build iovec */

    nsend = 0;
    /*
     * readv() and writev() returns EINVAL if the sum of the iov_len values
     * overflows an ssize_t value Or, the vector count iovcnt is less than
     * zero or greater than the permitted maximum.
     */
    limit = SSIZE_MAX;

    for (;;) {
        ASSERT(conn->smsg == msg);

        TAILQ_INSERT_TAIL(&send_msgq, msg, m_tqe);

        for (mbuf = STAILQ_FIRST(&msg->mhdr);
             mbuf != NULL && array_n(&sendv) < NC_IOV_MAX && nsend < limit;
             mbuf = nbuf) {
            nbuf = STAILQ_NEXT(mbuf, next);

            if (mbuf_empty(mbuf)) {
                continue;
            }

            mlen = mbuf_length(mbuf);
            if ((nsend + mlen) > limit) {
                mlen = limit - nsend;
            }

            ciov = array_push(&sendv);
            ciov->iov_base = mbuf->pos;
            ciov->iov_len = mlen;

            nsend += mlen;
        }

        if (array_n(&sendv) >= NC_IOV_MAX || nsend >= limit) {
            break;
        }

        msg = conn->send_next(ctx, conn);
        if (msg == NULL) {
            break;
        }
    }

    /*
     * (nsend == 0) is possible in redis multi-del
     * see PR: https://github.com/twitter/twemproxy/pull/225
     */
    conn->smsg = NULL;
    if (!TAILQ_EMPTY(&send_msgq) && nsend != 0) {
        n = conn_sendv(conn, &sendv, nsend);
    } else {
        n = 0;
    }

    nsent = n > 0 ? (size_t)n : 0;

    /* postprocess - process sent messages in send_msgq */

    for (msg = TAILQ_FIRST(&send_msgq); msg != NULL; msg = nmsg) {
        nmsg = TAILQ_NEXT(msg, m_tqe);

        TAILQ_REMOVE(&send_msgq, msg, m_tqe);

        if (nsent == 0) {
            if (msg->mlen == 0) {
                conn->send_done(ctx, conn, msg);
            }
            continue;
        }

        /* adjust mbufs of the sent message */
        for (mbuf = STAILQ_FIRST(&msg->mhdr); mbuf != NULL; mbuf = nbuf) {
            nbuf = STAILQ_NEXT(mbuf, next);

            if (mbuf_empty(mbuf)) {
                continue;
            }

            mlen = mbuf_length(mbuf);
            if (nsent < mlen) {
                /* mbuf was sent partially; process remaining bytes later */
                mbuf->pos += nsent;
                ASSERT(mbuf->pos < mbuf->last);
                nsent = 0;
                break;
            }

            /* mbuf was sent completely; mark it empty */
            mbuf->pos = mbuf->last;
            nsent -= mlen;
        }

        /* message has been sent completely, finalize it */
        if (mbuf == NULL) {
            conn->send_done(ctx, conn, msg);
        }
    }

    ASSERT(TAILQ_EMPTY(&send_msgq));

    if (n >= 0) {
        return NC_OK;
    }

    return (n == NC_EAGAIN) ? NC_OK : NC_ERROR;
}

rstatus_t
msg_send(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *msg;

    ASSERT(conn->send_active);

    conn->send_ready = 1;
    do {
        msg = conn->send_next(ctx, conn);
        if (msg == NULL) {
            /* nothing to send */
            return NC_OK;
        }

        status = msg_send_chain(ctx, conn, msg);
        if (status != NC_OK) {
            return status;
        }

    } while (conn->send_ready);

    return NC_OK;
}

struct msg *
msg_content_clone(struct msg *src)
{
    rstatus_t status = NC_OK;
    struct mbuf *mbuf = NULL;
    uint32_t mlen = src->mlen;
    char *content = nc_alloc(mlen + 1);
    if (content == NULL) {
        return NULL;
    }
    struct msg *psrc = src->peer;
    struct msg *pdest = NULL;
    struct msg *dest = msg_get(src->owner, src->request);
    if (dest == NULL) {
        nc_free(content);
        return NULL;
    }
    if (psrc != NULL) {
        pdest = msg_get(psrc->owner, psrc->request);
        if (pdest == NULL) {
            nc_free(content);
            msg_put(dest);
            return NULL;
        }
    }
    dest->peer = pdest;
    dest->noreply = 1;
    dest->parser = src->parser;

    if ((status = msg_extract_char(src, content, mlen)) != NC_OK) {
        nc_free(content);
        msg_put(dest);
        if (pdest != NULL) {
            msg_put(pdest);
        }
        return NULL;
    }
    content[mlen] = '\0';

    if ((status = msg_append(dest, (uint8_t *)content, mlen)) != NC_OK) {
        nc_free(content);
        msg_put(dest);
        if (pdest != NULL) {
            msg_put(pdest);
        }
        return NULL;
    }
    if (content != NULL) {
        nc_free(content);
    }
    mbuf = STAILQ_FIRST(&dest->mhdr);
    dest->pos = mbuf->pos;
    return dest;
}

/**.......................................................................
 * Append an arbitrary amount of content into msg, adding mbufs as
 * necessary
 */
rstatus_t
msg_copy_char(struct msg *msg, char* pos, size_t n)
{
    return msg_copy(msg, (uint8_t*)pos, n);
}

rstatus_t
msg_copy(struct msg *msg, uint8_t *pos, size_t n)
{
    size_t nremaining = n;
    size_t ncopied = 0;
    size_t ncopy = 0;

    while (nremaining > 0) {
        struct mbuf* mbuf = get_next_mbuf(msg);

        if (mbuf == NULL)
            return NC_ENOMEM;

        size_t navail = mbuf_size(mbuf);
        ncopy = (navail < nremaining) ? navail : nremaining;
        mbuf_copy(mbuf, pos + ncopied, ncopy);

        nremaining -= ncopy;
        ncopied += ncopy;
        msg->mlen += (uint32_t)ncopy;
        msg->pos = mbuf->last;
    }

    return NC_OK;
}

/**.......................................................................
 * Extract up to n bytes of the content of this message into the
 * passed buffer
 */
rstatus_t
msg_extract_char(struct msg *msg, char* pos, size_t n)
{
    return msg_extract(msg, (uint8_t*)pos, n);
}

rstatus_t
msg_extract(struct msg *msg, uint8_t *pos, size_t n)
{
    size_t nremaining = (n > msg->mlen) ? msg->mlen : n;
    size_t ncopied = 0;
    size_t ncopy = 0;

    struct mbuf* next_mbuf = 0;
    struct mbuf* mbuf = 0;
    for (mbuf = STAILQ_FIRST(&msg->mhdr); mbuf != NULL; mbuf = next_mbuf) {
        next_mbuf = STAILQ_NEXT(mbuf, next);

        size_t ndata = (size_t)(mbuf->last - mbuf->start);

        ncopy = (ndata < nremaining) ? ndata : nremaining;

        unsigned i;
        for (i = 0; i < ncopy; i++) {
            *(pos + ncopied++) = *(mbuf->start + i);
        }

        nremaining -= ncopy;

        if (nremaining == 0)
            break;
    }

    return NC_OK;
}

/**.......................................................................
 * Rewind every mbuf in this message
 */
void
msg_rewind(struct msg* msg)
{
    struct mbuf* next_mbuf = 0;
    struct mbuf* mbuf = 0;
    for (mbuf = STAILQ_FIRST(&msg->mhdr); mbuf != NULL; mbuf = next_mbuf) {
        mbuf_rewind(mbuf);
        next_mbuf = STAILQ_NEXT(mbuf, next);
    }
}

/**.......................................................................
 * Reset every mbuf pos in this message to its head
 */
void
msg_reset_pos(struct msg* msg)
{
    struct mbuf* next_mbuf = 0;
    struct mbuf* mbuf = 0;
    for (mbuf = STAILQ_FIRST(&msg->mhdr); mbuf != NULL; mbuf = next_mbuf) {
        mbuf->pos = mbuf->start;
        next_mbuf = STAILQ_NEXT(mbuf, next);
    }
}

/**.......................................................................
 * Return the next available mbuf, inserting a new one if none is
 * available
 */
struct mbuf *
get_next_mbuf(struct msg* msg)
{
    struct mbuf* mbuf = STAILQ_LAST(&msg->mhdr, mbuf, next);
    if (mbuf == NULL || mbuf_full(mbuf)) {
        mbuf = mbuf_get();
        if (mbuf == NULL) {
            return NULL;
        }
        mbuf_insert(&msg->mhdr, mbuf);
        msg->pos = mbuf->pos;
    }

    ASSERT(mbuf->end - mbuf->last > 0);
    return mbuf;
}

/**.......................................................................
 * Initialize a msg pos
 */
struct msg_pos
msg_pos_init(void)
{
    struct msg_pos pos;

    pos.msg = 0;
    pos.mbuf = 0;
    pos.ptr = 0;
    pos.result = MSG_NOTFOUND;

    return pos;
}

/**.......................................................................
 * Initialize a msg pos to the first valid position in the message
 */
void
msg_pos_init_start(struct msg* msg, struct msg_pos* pos)
{
    pos->msg = msg;
    pos->mbuf = STAILQ_FIRST(&msg->mhdr);
    pos->ptr = pos->mbuf->start;
    pos->result = MSG_FOUND;
}

/**.......................................................................
 * Initialize a msg pos to the last valid position in the message
 */
void
msg_pos_init_last(struct msg* msg, struct msg_pos* pos)
{
    pos->msg = msg;
    pos->mbuf = STAILQ_LAST(&msg->mhdr, mbuf, next);
    pos->ptr = pos->mbuf->last;
    pos->result = MSG_FOUND;
}

/**.......................................................................
 * Return the position of the first sequence matching search_seq from
 * the start_pos or from head of message if start_pos == NULL
 */
void
msg_find_char(struct msg* src, char* search_seq, uint32_t search_seq_len,
              struct msg_pos* start_pos, struct msg_pos* res_pos)
{
    return msg_find(src, (uint8_t*)search_seq, search_seq_len, start_pos,
                    res_pos);
}

void
msg_find(struct msg* src, uint8_t* search_seq, uint32_t search_seq_len,
         struct msg_pos* start_pos, struct msg_pos* res_pos)
{
    struct mbuf *next_mbuf = 0;
    struct mbuf *match_start_mbuf = 0;
    uint8_t *match_start_ptr = 0;

    uint8_t *curr_search_seq_ptr = search_seq;
    uint8_t *curr_ptr = 0;
    size_t mbuf_len = 0;

    uint32_t nmatch = 0;

    ASSERT(res_pos != NULL);

    if (start_pos != NULL)
        ASSERT(src == start_pos->msg);

    struct mbuf *initial_mbuf =
            start_pos ? start_pos->mbuf : STAILQ_FIRST(&src->mhdr);
    uint8_t *initial_ptr = start_pos ? start_pos->ptr : initial_mbuf->start;

    res_pos->result = MSG_NOTFOUND;

    struct mbuf* curr_mbuf;
    for (curr_mbuf = initial_mbuf; curr_mbuf != NULL; curr_mbuf = next_mbuf) {

        next_mbuf = STAILQ_NEXT(curr_mbuf, next);

        curr_ptr = (curr_mbuf == initial_mbuf) ? initial_ptr : curr_mbuf->start;
        mbuf_len = (size_t)(curr_mbuf->last - curr_ptr);

        unsigned i;
        for (i = 0; i < mbuf_len; i++, curr_ptr++) {
            if (*curr_ptr == *curr_search_seq_ptr) {
                if (nmatch == 0) {
                    match_start_mbuf = curr_mbuf;
                    match_start_ptr = curr_ptr;
                }
                ++nmatch;

                if (nmatch == search_seq_len) {
                    res_pos->msg = src;
                    res_pos->mbuf = match_start_mbuf;
                    res_pos->ptr = match_start_ptr;
                    res_pos->result = MSG_FOUND;
                    return;
                }

                curr_search_seq_ptr++;
            } else if (nmatch > 0) {
                nmatch = 0;
                curr_search_seq_ptr = search_seq;
            }
        }
    }

    return;
}

/**.......................................................................
 * Return the position offset bytes from the start position
 */
void
msg_offset_from(struct msg_pos* start_pos, uint32_t offset,
                struct msg_pos* res_pos)
{
    if (!msg_pos_is_valid(start_pos))
        return;

    ASSERT(res_pos != NULL);

    size_t mbuf_size = 0;
    size_t remaining_offset = offset;
    uint8_t *start_ptr = 0;

    struct mbuf *next_mbuf = 0;
    struct mbuf *mbuf;
    for (mbuf = start_pos->mbuf; mbuf != NULL; mbuf = next_mbuf) {
        next_mbuf = STAILQ_NEXT(mbuf, next);
        if (mbuf == start_pos->mbuf) {
            start_ptr = start_pos->ptr;
        } else {
            start_ptr = mbuf->start;
        }

        mbuf_size = (size_t)(mbuf->last - start_ptr);

        if (mbuf_size >= remaining_offset) {
            res_pos->msg = start_pos->msg;
            res_pos->mbuf = mbuf;
            res_pos->ptr = start_ptr + remaining_offset;
            res_pos->result = MSG_FOUND;
            return;
        } else {
            remaining_offset -= mbuf_size;
        }
    }

    return;
}

/**.......................................................................
 * Return the offset in bytes between two positions
 */
rstatus_t
msg_offset_between(struct msg_pos* start_pos, struct msg_pos* end_pos,
                   uint32_t* offset)
{
    if (!msg_pos_is_valid(start_pos) || !msg_pos_is_valid(end_pos))
        return NC_ERROR;

    *offset = 0;

    struct mbuf* next_mbuf = 0;
    struct mbuf* mbuf = 0;
    for (mbuf = start_pos->mbuf; mbuf != NULL; mbuf = next_mbuf) {
        next_mbuf = STAILQ_NEXT(mbuf, next);

        if (mbuf == start_pos->mbuf) {
            if (mbuf == end_pos->mbuf) {
                *offset += (uint32_t)(end_pos->ptr - start_pos->ptr);
                return NC_OK;
            } else {
                *offset += (uint32_t)(mbuf->last - start_pos->ptr);
            }
        } else if (mbuf == end_pos->mbuf) {
            *offset += (uint32_t)(end_pos->ptr - mbuf->start);
            return NC_OK;
        } else {
            *offset += (uint32_t)(mbuf->last - mbuf->start);
        }
    }

    return NC_ERROR;
}

/**.......................................................................
 * Return true if this position is valid
 */
bool
msg_pos_is_valid(struct msg_pos* pos)
{
    if (pos == NULL) {
        return false;
    }

    if (pos->result == MSG_NOTFOUND) {
        return false;
    }

    if (pos->mbuf == NULL) {
        return false;
    }

    if (pos->ptr == NULL) {
        return false;
    }

    if (!(pos->ptr >= pos->mbuf->start && pos->ptr <= pos->mbuf->last)) {
        return false;
    }

    return true;
}

/**.......................................................................
 * Copy from start_pos to end_pos into the destination message
 */
rstatus_t
msg_copy_between_pos(struct msg* dest, struct msg_pos* start_pos,
                     struct msg_pos* end_pos)
{
    if (!msg_pos_is_valid(start_pos) || !msg_pos_is_valid(end_pos)) {
        return NC_ERROR;
    }

    if (start_pos->msg != end_pos->msg) {
        return NC_ERROR;
    }

    ASSERT(dest != NULL);

    rstatus_t status = NC_OK;
    size_t ncopy = 0;

    struct mbuf* next_mbuf = 0;
    struct mbuf* mbuf = 0;
    for (mbuf = start_pos->mbuf; mbuf != NULL; mbuf = next_mbuf) {
        next_mbuf = STAILQ_NEXT(mbuf, next);
        if (mbuf == start_pos->mbuf) {
            if (mbuf == end_pos->mbuf) {
                ncopy = (size_t)(end_pos->ptr - start_pos->ptr);

                if ((status = msg_copy(dest, start_pos->ptr, ncopy)) != NC_OK) {
                    return status;
                }
                return NC_OK;
            } else {
                ncopy = (size_t)(mbuf->last - start_pos->ptr);

                if ((status = msg_copy(dest, start_pos->ptr, ncopy)) != NC_OK) {
                    return status;
                }
            }
        } else if (mbuf == end_pos->mbuf) {
            ncopy = (size_t)(end_pos->ptr - mbuf->start);

            if ((status = msg_copy(dest, end_pos->ptr, ncopy)) != NC_OK)
                return status;

            return NC_OK;
        } else {
            ncopy = (size_t)(mbuf->last - mbuf->start);

            if ((status = msg_copy(dest, mbuf->start, ncopy)) != NC_OK) {
                return status;
            }
        }
    }

    return NC_ERROR;
}

/**.......................................................................
 * Copy from start_pos to end_pos into the destination buffer
 */
rstatus_t
msg_extract_between_pos_char(char* dest, struct msg_pos* start_pos,
                             struct msg_pos* end_pos)
{
    return msg_extract_between_pos((uint8_t*)dest, start_pos, end_pos);
}

rstatus_t
msg_extract_between_pos(uint8_t* dest, struct msg_pos* start_pos,
                        struct msg_pos* end_pos)
{
    if (!msg_pos_is_valid(start_pos) || !msg_pos_is_valid(end_pos)) {
        return NC_ERROR;
    }

    if (start_pos->msg != end_pos->msg) {
        return NC_ERROR;
    }

    ASSERT(dest != NULL);

    uint8_t* destptr = dest;

    size_t ncopy = 0;

    struct mbuf* next_mbuf = 0;
    struct mbuf* mbuf = 0;
    for (mbuf = start_pos->mbuf; mbuf != NULL; mbuf = next_mbuf) {
        next_mbuf = STAILQ_NEXT(mbuf, next);

        if (mbuf == start_pos->mbuf) {
            if (mbuf == end_pos->mbuf) {
                ncopy = (size_t)(end_pos->ptr - start_pos->ptr);

                unsigned i;
                for (i = 0; i < ncopy; i++) {
                    *(destptr++) = *(start_pos->ptr + i);
                }

                return NC_OK;
            } else {
                ncopy = (size_t)(mbuf->last - start_pos->ptr);

                unsigned i;
                for (i = 0; i < ncopy; i++) {
                    *(destptr++) = *(start_pos->ptr + i);
                }
            }
        } else if (mbuf == end_pos->mbuf) {
            ncopy = (size_t)(end_pos->ptr - mbuf->start);

            unsigned i;
            for (i = 0; i < ncopy; i++) {
                *(destptr++) = *(mbuf->start + i);
            }

            return NC_OK;
        } else {
            ncopy = (size_t)(mbuf->last - mbuf->start);

            unsigned i;
            for (i = 0; i < ncopy; i++) {
                *(destptr++) = *(mbuf->start + i);
            }
        }
    }

    return NC_ERROR;
}

/**.......................................................................
 * Copy n bytes from start_pos into the destination message
 */
rstatus_t
msg_copy_from_pos(struct msg* dest, struct msg_pos* start_pos, size_t n)
{
    if (!msg_pos_is_valid(start_pos)) {
        return NC_ERROR;
    }

    ASSERT(dest != NULL);

    rstatus_t status = NC_OK;

    struct mbuf* next_mbuf = 0;
    size_t nremaining = n;
    size_t msize = 0;
    size_t ncopy = 0;

    uint8_t* start_ptr = 0;

    struct mbuf* mbuf = 0;
    for (mbuf = start_pos->mbuf; mbuf != NULL && nremaining > 0; mbuf =
            next_mbuf) {
        next_mbuf = STAILQ_NEXT(mbuf, next);

        if (mbuf == start_pos->mbuf) {
            start_ptr = start_pos->ptr;
        } else {
            start_ptr = mbuf->start;
        }

        msize = (size_t)(mbuf->last - start_ptr);
        ncopy = (msize < nremaining) ? msize : nremaining;

        if ((status = msg_copy(dest, start_ptr, ncopy)) != NC_OK) {
            return status;
        }

        nremaining -= ncopy;
    }

    return NC_OK;
}

/**.......................................................................
 * Copy n bytes from start_pos into the destination buffer
 */
rstatus_t
msg_extract_from_pos_char(char* dest, struct msg_pos* start_pos, size_t n)
{
    return msg_extract_from_pos((uint8_t*)dest, start_pos, n);
}

rstatus_t
msg_extract_from_pos(uint8_t* dest, struct msg_pos* start_pos, size_t n)
{
    if (!msg_pos_is_valid(start_pos)) {
        return NC_ERROR;
    }

    ASSERT(dest != NULL);

    struct mbuf *next_mbuf = 0;
    size_t nremaining = n;
    size_t msize = 0;
    size_t ncopy = 0;

    uint8_t *start_ptr = 0;
    uint8_t *destptr = dest;

    struct mbuf* mbuf = 0;
    for (mbuf = start_pos->mbuf; mbuf != NULL && nremaining > 0; mbuf =
            next_mbuf) {
        next_mbuf = STAILQ_NEXT(mbuf, next);

        if (mbuf == start_pos->mbuf) {
            start_ptr = start_pos->ptr;
        } else {
            start_ptr = mbuf->start;
        }

        msize = (size_t)(mbuf->last - start_ptr);
        ncopy = (msize < nremaining) ? msize : nremaining;

        unsigned i;
        for (i = 0; i < ncopy; i++) {
            *(destptr++) = *(start_ptr + i);
        }

        nremaining -= ncopy;
    }

    return NC_OK;
}

void
msg_print(struct msg* msg)
{
    char str[msg->mlen + 1];
    msg_extract_char(msg, str, msg->mlen);

    unsigned i;
    for (i = 0; i < msg->mlen; i++) {
        if (str[i] == '\r') {
            str[i] = 'R';
        }
        if (str[i] == '\n') {
            str[i] = 'N';
        }
    }

    str[msg->mlen] = '\0';

    fprintf(stdout, "Msg = %s\n", str);
}

/**.......................................................................
 * Pack a printable version of msg into the supplied buffer str, which
 * had better be big enough to contain it
 */
void
msg_get_printable(struct msg* msg, char* str)
{
    ASSERT(strlen(str) > msg->mlen);

    msg_extract_char(msg, str, msg->mlen);

    unsigned i = 0;
    for (i = 0; i < msg->mlen; i++) {
        if (str[i] == '\r') {
            str[i] = 'R';
        } else if (str[i] == '\n') {
            str[i] = 'N';
        } else if (!isprint(str[i])) {
            str[i] = '^';
        }
    }

    str[msg->mlen] = '\0';
}

/**.......................................................................
 * Utility function for retrieving the server pool that own's a
 * message's connection
 */
struct server_pool *
msg_get_server_pool(struct msg* r)
{
    struct conn *conn = r->owner;
    struct server *conn_server = (struct server*)conn->owner;
    return conn_server->owner;
}

const uint8_t *
msg_key0(struct msg* req, size_t* bucket_len, size_t* key_len)
{
    const static uint8_t empty[] = "";
    *bucket_len = 0;
    *key_len = 0;

    if (!req->request) {
        return empty;
    }
    if (req->keys == NULL) {
        return empty;
    }
    if (array_n(req->keys) < 1) {
        return empty;
    }

    struct keypos *kpos = array_get(req->keys, 0);
    if (kpos == NULL) {
        return empty;
    }

    *key_len = (size_t)(kpos->end - kpos->start);

    if (req->type == MSG_REQ_RIAK_GET) {
        *bucket_len = kpos->bucket_len;
    }

    if (*bucket_len > 0) {
        *key_len -= *bucket_len;
    }

    return kpos->start;
}

void
msg_set_keypos(struct msg* req, uint32_t keyn, int start_offset, int len,
               size_t bucket_len)
{
    struct mbuf *mbuf;

    if (!req->request) {
        return;
    }
    if (req->keys == NULL) {
        return;
    }
    if (array_n(req->keys) < keyn) {
        return;
    }
    struct keypos *kpos = array_get(req->keys, keyn);
    if (kpos == NULL) {
        return;
    }

    if (len <= 0) {
        len = (int)(kpos->end - kpos->start) - len;
    }

    /* set key start by offset of first mbuf */
    STAILQ_FOREACH(mbuf, &req->mhdr, next) {
        uint8_t *p;

        p = mbuf->start;
        kpos->start = p + start_offset;
        break;
    }

    kpos->end = kpos->start + len;
    kpos->bucket_len = bucket_len;
}
