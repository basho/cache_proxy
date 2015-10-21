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

#include <nc_core.h>
#include <nc_server.h>

void req_forward(struct context *ctx, struct conn *c_conn, struct msg *msg, bool backend, bool enqueue);

/*
* Backend processing utilities
*/

/* Return true if this request should be forwarded to the backend servers*/
bool should_forward_req_to_backend(struct conn *c_conn, struct msg* msg);

/*
* Get the next request to send -- returns NULL instead of the next
* message if we are still waiting for a response to the last request
*/
struct msg* get_next_req_to_send(struct msg* nmsg);

struct msg *
req_get(struct conn *conn)
{
    struct msg *msg;

    ASSERT(conn->client && !conn->proxy);

    msg = msg_get(conn, true);
    if (msg == NULL) {
        conn->err = errno;
    }
    return msg;
}

static void
req_log(struct msg *req)
{
    struct msg *rsp;           /* peer message (response) */
    int64_t req_time;          /* time cost for this request */
    char *peer_str;            /* peer client ip:port */
    uint32_t req_len, rsp_len; /* request and response length */
    struct string *req_type;   /* request type string */
    struct keypos *kpos;

    if (log_loggable(LOG_NOTICE) == 0) {
        return;
    }

    /* a fragment? */
    if (req->frag_id != 0 && req->frag_owner != req) {
        return;
    }

    /* conn close normally? */
    if (req->mlen == 0) {
        return;
    }
    /*
     * there is a race scenario where a requests comes in, the log level is not LOG_NOTICE,
     * and before the response arrives you modify the log level to LOG_NOTICE
     * using SIGTTIN OR SIGTTOU, then req_log() wouldn't have msg->start_ts set
     */
    if (req->start_ts == 0) {
        return;
    }

    req_time = nc_usec_now() - req->start_ts;

    rsp = req->peer;
    req_len = req->mlen;
    rsp_len = (rsp != NULL) ? rsp->mlen : 0;

    if (array_n(req->keys) < 1) {
        return;
    }

    kpos = array_get(req->keys, 0);
    if (kpos->end != NULL) {
        *(kpos->end) = '\0';
    }

    /*
     * FIXME: add backend addr here
     * Maybe we can store addrstr just like server_pool in conn struct
     * when connections are resolved
     */
    peer_str = nc_unresolve_peer_desc(req->owner->sd);

    req_type = msg_type_string(req->type);

  /*
   * FIXME: add backend addr here
   * Maybe we can store addrstr just like server_pool in conn struct
   * when connections are resolved
   */
  peer_str = nc_unresolve_peer_desc(req->owner->sd);

  req_type = msg_type_string(req->type);

  size_t bucket_len = 0;
  size_t key_len = 0;
  const uint8_t *key0 = msg_key0(req, &bucket_len, &key_len);

  if (bucket_len <= 0) {
    log_debug(LOG_NOTICE, "req %"PRIu64" done on c %d req_time %"PRIi64".%03"PRIi64
        " msec type %.*s narg %"PRIu32" req_len %"PRIu32" rsp_len %"PRIu32
        " key0 '%.*s'"
        " peer '%s' done %d error %d",
        req->id, req->owner->sd, req_time / 1000, req_time % 1000,
        req_type->len, req_type->data, req->narg, req_len, rsp_len,
        key_len, key0, /*<< key portion*/
        peer_str, req->done, req->error);
  } else {
    log_debug(LOG_NOTICE, "req %"PRIu64" done on c %d req_time %"PRIi64".%03"PRIi64
        " msec type %.*s narg %"PRIu32" req_len %"PRIu32" rsp_len %"PRIu32
        " key0 '%.*s:%.*s'"
        " peer '%s' done %d error %d",
        req->id, req->owner->sd, req_time / 1000, req_time % 1000,
        req_type->len, req_type->data, req->narg, req_len, rsp_len,
        bucket_len, key0, /*<< bucket portion*/
        key_len, key0 + bucket_len + 1, /*<< key portion*/
        peer_str, req->done, req->error);
  }
}

void
req_put(struct msg *msg)
{
    struct msg *pmsg; /* peer message (response) */

    ASSERT(msg->request);

    req_log(msg);

    pmsg = msg->peer;
    if (pmsg != NULL) {
        ASSERT(!pmsg->request && pmsg->peer == msg);
        msg->peer = NULL;
        pmsg->peer = NULL;
        rsp_put(pmsg);
    }

    msg_tmo_delete(msg);

    msg_put(msg);
}

/*
 * Return true if request is done, false otherwise
 *
 * A request is done, if we received response for the given request.
 * A request vector is done if we received responses for all its
 * fragments.
 */
bool
req_done(struct conn *conn, struct msg *msg)
{
    struct msg *cmsg;        /* current message */
    uint64_t id;             /* fragment id */
    uint32_t nfragment;      /* # fragment */

    ASSERT(conn->client && !conn->proxy);
    ASSERT(msg->request || msg->error);

    if (!msg->done) {
        return false;
    }

    id = msg->frag_id;
    if (id == 0) {
        return true;
    }

    if (msg->fdone) {
        /* request has already been marked as done */
        return true;
    }

    if (msg->nfrag_done < msg->nfrag) {
        return false;
    }

    /* check all fragments of the given request vector are done */

    for (cmsg = TAILQ_PREV(msg, msg_tqh, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         cmsg = TAILQ_PREV(cmsg, msg_tqh, c_tqe)) {

        if (!cmsg->done) {
            return false;
        }
    }

    for (cmsg = TAILQ_NEXT(msg, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         cmsg = TAILQ_NEXT(cmsg, c_tqe)) {

        if (!cmsg->done) {
            return false;
        }
    }

    /*
     * At this point, all the fragments including the last fragment have
     * been received.
     *
     * Mark all fragments of the given request vector to be done to speed up
     * future req_done calls for any of fragments of this request
     */

    msg->fdone = 1;
    nfragment = 0;

    for (cmsg = TAILQ_PREV(msg, msg_tqh, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         cmsg = TAILQ_PREV(cmsg, msg_tqh, c_tqe)) {
        cmsg->fdone = 1;
        nfragment++;
    }

    for (cmsg = TAILQ_NEXT(msg, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         cmsg = TAILQ_NEXT(cmsg, c_tqe)) {
        cmsg->fdone = 1;
        nfragment++;
    }

    msg->post_coalesce(msg->frag_owner);

    log_debug(LOG_DEBUG, "req from c %d with fid %"PRIu64" and %"PRIu32" "
              "fragments is done", conn->sd, id, nfragment);

    return true;
}


/*
 * Return true if request is in error, false otherwise
 *
 * A request is in error, if there was an error in receiving response for the
 * given request. A multiget request is in error if there was an error in
 * receiving response for any its fragments.
 */
bool
req_error(struct conn *conn, struct msg *msg)
{
    struct msg *cmsg; /* current message */
    uint64_t id;
    uint32_t nfragment;

    ASSERT(msg->request && req_done(conn, msg));

    if (msg->error) {
        return true;
    }

    id = msg->frag_id;
    if (id == 0) {
        return false;
    }

    if (msg->ferror) {
        /* request has already been marked to be in error */
        return true;
    }

    /* check if any of the fragments of the given request are in error */

    for (cmsg = TAILQ_PREV(msg, msg_tqh, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         cmsg = TAILQ_PREV(cmsg, msg_tqh, c_tqe)) {

        if (cmsg->error) {
            goto ferror;
        }
    }

    for (cmsg = TAILQ_NEXT(msg, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         cmsg = TAILQ_NEXT(cmsg, c_tqe)) {

        if (cmsg->error) {
            goto ferror;
        }
    }

    return false;

ferror:

    /*
     * Mark all fragments of the given request to be in error to speed up
     * future req_error calls for any of fragments of this request
     */

    msg->ferror = 1;
    nfragment = 1;

    for (cmsg = TAILQ_PREV(msg, msg_tqh, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         cmsg = TAILQ_PREV(cmsg, msg_tqh, c_tqe)) {
        cmsg->ferror = 1;
        nfragment++;
    }

    for (cmsg = TAILQ_NEXT(msg, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         cmsg = TAILQ_NEXT(cmsg, c_tqe)) {
        cmsg->ferror = 1;
        nfragment++;
    }

    log_debug(LOG_DEBUG, "req from c %d with fid %"PRIu64" and %"PRIu32" "
              "fragments is in error", conn->sd, id, nfragment);

    return true;
}

void
req_server_enqueue_imsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(!conn->client && !conn->proxy);

    /*
     * timeout clock starts ticking the instant the message is enqueued into
     * the server in_q; the clock continues to tick until it either expires
     * or the message is dequeued from the server out_q
     *
     * noreply request are free from timeouts because client is not interested
     * in the response anyway!
     */
    if (!msg->noreply) {
        msg_tmo_insert(msg, conn);
    }

    TAILQ_INSERT_TAIL(&conn->imsg_q, msg, s_tqe);

    stats_server_incr(ctx, conn->owner, in_queue);
    stats_server_incr_by(ctx, conn->owner, in_queue_bytes, msg->mlen);
}

void
req_server_enqueue_imsgq_head(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(!conn->client && !conn->proxy);

    /*
     * timeout clock starts ticking the instant the message is enqueued into
     * the server in_q; the clock continues to tick until it either expires
     * or the message is dequeued from the server out_q
     *
     * noreply request are free from timeouts because client is not intrested
     * in the reponse anyway!
     */
    if (!msg->noreply) {
        msg_tmo_insert(msg, conn);
    }

    TAILQ_INSERT_HEAD(&conn->imsg_q, msg, s_tqe);

    stats_server_incr(ctx, conn->owner, in_queue);
    stats_server_incr_by(ctx, conn->owner, in_queue_bytes, msg->mlen);
}

void
req_server_dequeue_imsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(!conn->client && !conn->proxy);

    TAILQ_REMOVE(&conn->imsg_q, msg, s_tqe);

    stats_server_decr(ctx, conn->owner, in_queue);
    stats_server_decr_by(ctx, conn->owner, in_queue_bytes, msg->mlen);
}

void
req_client_enqueue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(conn->client && !conn->proxy);

    TAILQ_INSERT_TAIL(&conn->omsg_q, msg, c_tqe);
}

/**.......................................................................
 * Enqueue a request in the output message queue of the server.
 * Called when a request is written to a server and we are waiting for
 * a response.
 */
void
req_server_enqueue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(!conn->client && !conn->proxy);

    TAILQ_INSERT_TAIL(&conn->omsg_q, msg, s_tqe);

    stats_server_incr(ctx, conn->owner, out_queue);
    stats_server_incr_by(ctx, conn->owner, out_queue_bytes, msg->mlen);
}

/**.......................................................................
 * Dequeue a request from the output message queue of the client.
 * Called when a response has been read back from a server and is
 * forwarded back to the client.
 */
void
req_client_dequeue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(conn->client && !conn->proxy);

    TAILQ_REMOVE(&conn->omsg_q, msg, c_tqe);
}

/**.......................................................................
 * Dequeue a request from a server's output message queue.  Called
 * when a response has been read back from the server
 */
void
req_server_dequeue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(!conn->client && !conn->proxy);

    msg_tmo_delete(msg);
    TAILQ_REMOVE(&conn->omsg_q, msg, s_tqe);

    stats_server_decr(ctx, conn->owner, out_queue);
    stats_server_decr_by(ctx, conn->owner, out_queue_bytes, msg->mlen);
}

struct msg *
req_recv_next(struct context *ctx, struct conn *conn, bool alloc)
{
    struct msg *msg;

    ASSERT(conn->client && !conn->proxy);

    if (conn->eof) {
        msg = conn->rmsg;

        /* client sent eof before sending the entire request */
        if (msg != NULL) {
            conn->rmsg = NULL;

            ASSERT(msg->peer == NULL);
            ASSERT(msg->request && !msg->done);

            log_error("eof c %d discarding incomplete req %"PRIu64" len "
                      "%"PRIu32"", conn->sd, msg->id, msg->mlen);

            req_put(msg);
        }

        /*
         * TCP half-close enables the client to terminate its half of the
         * connection (i.e. the client no longer sends data), but it still
         * is able to receive data from the proxy. The proxy closes its
         * half (by sending the second FIN) when the client has no
         * outstanding requests
         */
        if (!conn->active(conn)) {
            conn->done = 1;
            log_debug(LOG_INFO, "c %d is done", conn->sd);
        }
        return NULL;
    }

    msg = conn->rmsg;
    if (msg != NULL) {
        ASSERT(msg->request);
        return msg;
    }

    if (!alloc) {
        return NULL;
    }

    msg = req_get(conn);
    if (msg != NULL) {
        conn->rmsg = msg;
    }

    return msg;
}

static rstatus_t
req_make_reply(struct context *ctx, struct conn *conn, struct msg *req)
{
    struct msg *msg;

    msg = msg_get(conn, false);
    if (msg == NULL) {
        conn->err = errno;
        return NC_ENOMEM;
    }

    req->peer = msg;
    msg->peer = req;
    msg->request = 0;

    req->done = 1;
    conn->enqueue_outq(ctx, conn, req);
    return NC_OK;
}

static bool
req_filter(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(conn->client && !conn->proxy);

    if (msg_empty(msg)) {
        ASSERT(conn->rmsg == NULL);
        log_debug(LOG_VERB, "filter empty req %"PRIu64" from c %d", msg->id,
                  conn->sd);
        req_put(msg);
        return true;
    }

    /*
     * Handle "quit\r\n" (memcache) or "*1\r\n$4\r\nquit\r\n" (redis), which
     * is the protocol way of doing a passive close. The connection is closed
     * as soon as all pending replies have been written to the client.
     */
    if (msg->quit) {
        log_debug(LOG_INFO, "filter quit req %"PRIu64" from c %d", msg->id,
                  conn->sd);
        if (conn->rmsg != NULL) {
            log_debug(LOG_INFO, "discard invalid req %"PRIu64" len %"PRIu32" "
                      "from c %d sent after quit req", conn->rmsg->id,
                      conn->rmsg->mlen, conn->sd);
        }
        conn->eof = 1;
        conn->recv_ready = 0;
        req_put(msg);
        return true;
    }

    /*
     * If this conn is not authenticated, we will mark it as noforward,
     * and handle it in the redis_reply handler.
     */
    if (conn->need_auth) {
        msg->noforward = 1;
    }

    return false;
}

static void
req_forward_error(struct context *ctx, struct conn *conn, struct msg *msg)
{
    rstatus_t status;

    ASSERT(conn->client && !conn->proxy);

    log_debug(LOG_INFO, "forward req %"PRIu64" len %"PRIu32" type %d from "
              "c %d failed: %s", msg->id, msg->mlen, msg->type, conn->sd,
              strerror(errno));

    msg->done = 1;
    msg->error = 1;
    msg->err = errno;

    /* noreply request don't expect any response */
    if (msg->noreply) {
        req_put(msg);
        return;
    }

    if (req_done(conn, TAILQ_FIRST(&conn->omsg_q))) {
        status = event_add_out(ctx->evb, conn);
        if (status != NC_OK) {
            conn->err = errno;
        }
    }
}

static void
req_forward_stats(struct context *ctx, struct server *server, struct msg *msg)
{
    ASSERT(msg->request);

    stats_server_incr(ctx, server, requests);
    stats_server_incr_by(ctx, server, request_bytes, msg->mlen);
}

void
req_forward(struct context *ctx, struct conn *c_conn, struct msg *msg, bool backend, bool enqueue)
{
    rstatus_t status;
    struct conn *s_conn;
    uint8_t *key;
    uint32_t keylen;
    struct keypos *kpos;

    ASSERT(c_conn->client && !c_conn->proxy);

    if (!msg->noreply && enqueue) {
        c_conn->enqueue_outq(ctx, c_conn, msg);
    }

    ASSERT(array_n(msg->keys) > 0);
    /* TODO: investigate key here, seeing logged messages w/ incorrect key
    * this is not terribly important for Riak as a backend under happy path,
    * but in the face of a network partition, the consistent routing of
    * requests based on key to a specific coordinator is important.
    */
    kpos = array_get(msg->keys, 0);
    key = kpos->start;
    keylen = (uint32_t)(kpos->end - kpos->start);

    struct server *server = NULL;
    struct server_pool *pool = (struct server_pool *)c_conn->owner;

    if (backend) {
        do {
            server = get_next_backend_server(msg, c_conn, key, keylen);
            s_conn = server_pool_conn_backend(ctx, pool, key, keylen, server);
        } while (!backend_resend_q_empty(msg) && s_conn==NULL);
    } else {
        s_conn = server_pool_conn_frontend(ctx, pool, key, keylen, server);
    }

    if (s_conn == NULL) {
        msg->error = 1;
        return;
    }
    ASSERT(!s_conn->client && !s_conn->proxy);

    if (s_conn->req_remap(s_conn, msg)==NC_ERROR) {
      msg->error = 1;
      return;
    }

    if (TAILQ_EMPTY(&s_conn->imsg_q)) {
        status = event_add_out(ctx->evb, s_conn);
        if (status != NC_OK) {
            req_forward_error(ctx, c_conn, msg);
            s_conn->err = errno;
            return;
        }
    }

    if (s_conn->need_auth) {
        status = msg->add_auth(ctx, c_conn, s_conn);
        if (status != NC_OK) {
            req_forward_error(ctx, c_conn, msg);
            s_conn->err = errno;
            return;
        }
    }

    s_conn->enqueue_inq(ctx, s_conn, msg);

    req_forward_stats(ctx, s_conn->owner, msg);

    log_debug(LOG_VERB, "forward from c %d to s %d req %"PRIu64" len %"PRIu32
              " type %d with key '%.*s'", c_conn->sd, s_conn->sd, msg->id,
              msg->mlen, msg->type, keylen, key);

    /* NOTE: we shall NOT enqueue post messages here, the point of post messages
    * is to use value gained by having performed the request+response cycle, ie
    * getting the vclock of a Riak object.
    */
}

/**.......................................................................
 * Called on receipt of a request by a client connection.
 *
 * This function forwards the request to the server pool, optionally
 * fragmenting the message across multiple servers.
 */
void
req_recv_done(struct context *ctx, struct conn *conn, struct msg *msg,
              struct msg *nmsg, bool backend, bool enqueue)
{
    rstatus_t status;
    struct server_pool *pool;
    struct msg_tqh frag_msgq;
    struct msg *sub_msg;
    struct msg *tmsg; /* tmp next message */

    ASSERT(conn->client && !conn->proxy);
    ASSERT(msg->request);
    ASSERT(msg->owner == conn);
    ASSERT(conn->rmsg == msg);
    ASSERT(nmsg == NULL || nmsg->request);

    /* enqueue next message (request), if any */
    conn->rmsg = nmsg;

    if (req_filter(ctx, conn, msg)) {
        return;
    }

    if (backend == false) {
        if (should_forward_req_to_backend(conn, msg)) {
            backend = true;
        }
    }

    if (msg->noforward) {
        status = req_make_reply(ctx, conn, msg);
        if (status != NC_OK) {
            conn->err = errno;
            return;
        }

        status = msg->reply(msg);
        if (status != NC_OK) {
            conn->err = errno;
            return;
        }

        status = event_add_out(ctx->evb, conn);
        if (status != NC_OK) {
            conn->err = errno;
        }

        return;
    }

    pool = conn->owner;
    TAILQ_INIT(&frag_msgq);
    status = msg->fragment(msg, pool->frontends.ncontinuum, &frag_msgq);
    if (status != NC_OK) {
        if (!msg->noreply) {
            if (enqueue) {
                conn->enqueue_outq(ctx, conn, msg);
            }
        }
        req_forward_error(ctx, conn, msg);
    }

    if (TAILQ_EMPTY(&frag_msgq)) {
        req_forward(ctx, conn, msg, backend, enqueue);
        return;
    }

    status = req_make_reply(ctx, conn, msg);
    if (status != NC_OK) {
        if (!msg->noreply) {
            if (enqueue) {
                conn->enqueue_outq(ctx, conn, msg);
            }
        }
        req_forward_error(ctx, conn, msg);
    }

    for (sub_msg = TAILQ_FIRST(&frag_msgq); sub_msg != NULL; sub_msg = tmsg) {
        tmsg = TAILQ_NEXT(sub_msg, m_tqe);

        TAILQ_REMOVE(&frag_msgq, sub_msg, m_tqe);
        req_forward(ctx, conn, sub_msg, backend, enqueue);
    }

    ASSERT(TAILQ_EMPTY(&frag_msgq));
    return;
}

struct msg *
req_send_next(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *msg, *nmsg; /* current and next message */

    ASSERT(!conn->client && !conn->proxy);

    if (conn->connecting) {
        server_connected(ctx, conn);
    }

    nmsg = TAILQ_FIRST(&conn->imsg_q);
    if (nmsg == NULL) {
        /* nothing to send as the server inq is empty */
        status = event_del_out(ctx->evb, conn);
        if (status != NC_OK) {
            conn->err = errno;
        }

        return NULL;
    }

    msg = conn->smsg;
    if (msg != NULL) {
        ASSERT(msg->request && !msg->done);
        nmsg = TAILQ_NEXT(msg, s_tqe);
    }

    nmsg = get_next_req_to_send(nmsg);

    conn->smsg = nmsg;

    if (nmsg == NULL) {
        return NULL;
    }

    ASSERT(nmsg->request && !nmsg->done);

    log_debug(LOG_VVERB, "send next req %"PRIu64" len %"PRIu32" type %d on "
              "s %d", nmsg->id, nmsg->mlen, nmsg->type, conn->sd);

    return nmsg;
}

/**.......................................................................
 * Return the next request to send to the server
 */
struct msg *
get_next_req_to_send(struct msg* nmsg)
{
    if (nmsg != NULL) {
        /*
        * See if this message's owner pool has backend servers -- if it
        * does, and this is a message that might be forwarded to the
        * backend, we enforce that requests are only sent if they are at
        * the head of the client's output queue.  This ensures that the
        * previous request has been responded to before the next request
        * is sent.  If we don't, then we can get in a situation where the
        * backend responds to requests out of order.
        */

        struct conn* client = nmsg->owner;
        struct server_pool* sp = (struct server_pool*)client->owner;
        if (sp != NULL) {
            unsigned nbackend = sp->backends.server_arr.nelem;

            /*
            * If this is a fragmented message, it won't be the head of the
            * client omsg_q -- its fragments will be -- so go ahead and send
            * it anyway (for the time being anyway, we won't be forwarding
            * fragmented messages to the backend).  Also, if this message
            * expects no reply, send it, since the reply -- and thus the
            * order of the reply -- is irrelevant.
            */

            if (nbackend > 0 && nmsg->frag_id == 0 && !nmsg->swallow) {
                struct msg* clientHead = TAILQ_FIRST(&client->omsg_q);

                if (nmsg != clientHead) {
                    return NULL;
                }
            }
        }
    }

    return nmsg;
}

void
req_send_done(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(!conn->client && !conn->proxy);
    ASSERT(msg != NULL && conn->smsg == NULL);
    ASSERT(msg->request && !msg->done);
    ASSERT(msg->owner != conn);

    log_debug(LOG_VVERB, "send done req %"PRIu64" len %"PRIu32" type %d on "
              "s %d", msg->id, msg->mlen, msg->type, conn->sd);

    /* dequeue the message (request) from server inq */
    conn->dequeue_inq(ctx, conn, msg);

    /*
     * noreply request instructs the server not to send any response. So,
     * enqueue message (request) in server outq, if response is expected.
     * Otherwise, free the noreply request
     */
    if (!msg->noreply) {
        conn->enqueue_outq(ctx, conn, msg);
    } else {
        req_put(msg);
    }
}

rstatus_t
req_remap(struct conn* conn, struct msg *msg)
{
    return NC_OK;
}

/**.......................................................................
 * Return true if this request should be forward to the backend
 */
bool
should_forward_req_to_backend(struct conn *c_conn, struct msg* msg)
{
    if (conn_nbackend(c_conn) == 0) {
        return false;
    }

    switch(msg->type) {
    case MSG_REQ_REDIS_SET:
        return true;

    case MSG_REQ_REDIS_DEL:
        return true;

    default:
        break;
    }

    return false;
}
