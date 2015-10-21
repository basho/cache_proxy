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

#include <stdlib.h>
#include <unistd.h>
#include <nc_core.h>
#include <nc_conf.h>
#include <nc_server.h>
#include <nc_proxy.h>

static uint32_t ctx_id; /* context generation */

rstatus_t core_readd_out(struct context* ctx, struct conn* conn);
rstatus_t core_readd_in(struct context* ctx, struct conn* conn);

static rstatus_t
core_calc_connections(struct context *ctx)
{
    int status;
    struct rlimit limit;

    status = getrlimit(RLIMIT_NOFILE, &limit);
    if (status < 0) {
        log_error("getrlimit failed: %s", strerror(errno));
        return NC_ERROR;
    }

    ctx->max_nfd = (uint32_t)limit.rlim_cur;
    ctx->max_ncconn = ctx->max_nfd - ctx->max_nsconn - RESERVED_FDS;
    log_debug(LOG_NOTICE, "max fds %"PRIu32" max client conns %"PRIu32" "
              "max server conns %"PRIu32"", ctx->max_nfd, ctx->max_ncconn,
              ctx->max_nsconn);

    return NC_OK;
}

static struct context *
core_ctx_create(struct instance *nci)
{
    rstatus_t status;
    struct context *ctx;

    ctx = nc_alloc(sizeof(*ctx));
    if (ctx == NULL) {
        return NULL;
    }
    ctx->id = ++ctx_id;
    ctx->cf = NULL;
    ctx->stats = NULL;
    ctx->evb = NULL;
    array_null(&ctx->pool);
    ctx->max_timeout = nci->stats_interval;
    ctx->timeout = ctx->max_timeout;
    ctx->max_nfd = 0;
    ctx->max_ncconn = 0;
    ctx->max_nsconn = 0;

    /* parse and create configuration */
    ctx->cf = conf_create(nci->conf_filename);
    if (ctx->cf == NULL) {
        nc_free(ctx);
        return NULL;
    }

    /* initialize server pool from configuration */
    status = server_pool_init(&ctx->pool, &ctx->cf->pool, ctx);
    if (status != NC_OK) {
        conf_destroy(ctx->cf);
        nc_free(ctx);
        return NULL;
    }

    /*
     * Get rlimit and calculate max client connections after we have
     * calculated max server connections
     */
    status = core_calc_connections(ctx);
    if (status != NC_OK) {
        server_pool_deinit(&ctx->pool);
        conf_destroy(ctx->cf);
        nc_free(ctx);
        return NULL;
    }

    /* create stats per server pool */
    ctx->stats = stats_create(nci->stats_port, nci->stats_addr, nci->stats_interval,
                              nci->hostname, &ctx->pool);
    if (ctx->stats == NULL) {
        server_pool_deinit(&ctx->pool);
        conf_destroy(ctx->cf);
        nc_free(ctx);
        return NULL;
    }

    /* initialize event handling for client, proxy and server */
    ctx->evb = event_base_create(EVENT_SIZE, &core_core);
    if (ctx->evb == NULL) {
        stats_destroy(ctx->stats);
        server_pool_deinit(&ctx->pool);
        conf_destroy(ctx->cf);
        nc_free(ctx);
        return NULL;
    }

    /* preconnect? servers in server pool */
    status = server_pool_preconnect(ctx);
    if (status != NC_OK) {
        server_pool_disconnect(ctx);
        event_base_destroy(ctx->evb);
        stats_destroy(ctx->stats);
        server_pool_deinit(&ctx->pool);
        conf_destroy(ctx->cf);
        nc_free(ctx);
        return NULL;
    }

    /* initialize proxy per server pool */
    status = proxy_init(ctx);
    if (status != NC_OK) {
        server_pool_disconnect(ctx);
        event_base_destroy(ctx->evb);
        stats_destroy(ctx->stats);
        server_pool_deinit(&ctx->pool);
        conf_destroy(ctx->cf);
        nc_free(ctx);
        return NULL;
    }

    log_debug(LOG_VVERB, "created ctx %p id %"PRIu32"", ctx, ctx->id);

    return ctx;
}

static void
core_ctx_destroy(struct context *ctx)
{
    log_debug(LOG_VVERB, "destroy ctx %p id %"PRIu32"", ctx, ctx->id);
    proxy_deinit(ctx);
    server_pool_disconnect(ctx);
    event_base_destroy(ctx->evb);
    stats_destroy(ctx->stats);
    server_pool_deinit(&ctx->pool);
    conf_destroy(ctx->cf);
    nc_free(ctx);
}

struct context *
core_start(struct instance *nci)
{
    struct context *ctx;

    mbuf_init(nci);
    msg_init();
    conn_init();

    ctx = core_ctx_create(nci);
    if (ctx != NULL) {
        nci->ctx = ctx;
        return ctx;
    }

    conn_deinit();
    msg_deinit();
    mbuf_deinit();

    return NULL;
}

void
core_stop(struct context *ctx)
{
    conn_deinit();
    msg_deinit();
    mbuf_deinit();
    core_ctx_destroy(ctx);
}

static rstatus_t
core_recv(struct context *ctx, struct conn *conn)
{
    rstatus_t status;

    status = conn->recv(ctx, conn);
    if (status != NC_OK) {
        log_debug(LOG_INFO, "recv on %c %d failed: %s",
                  conn->client ? 'c' : (conn->proxy ? 'p' : 's'), conn->sd,
                  strerror(errno));
        return status;
    }

    status = core_readd_in(ctx, conn);
    status = core_readd_out(ctx, conn);

    return status;
}

static rstatus_t
core_send(struct context *ctx, struct conn *conn)
{
    rstatus_t status;

    status = conn->send(ctx, conn);
    if (status != NC_OK) {
        log_debug(LOG_INFO, "send on %c %d failed: status: %d errno: %d %s",
                  conn->client ? 'c' : (conn->proxy ? 'p' : 's'), conn->sd,
                  status, errno, strerror(errno));
        return status;
    }

    status = core_readd_out(ctx, conn);
    status = core_readd_in(ctx, conn);

    return status;
}

rstatus_t
core_readd_out(struct context* ctx, struct conn* conn)
{
    if (!conn->client)
    {
        if (!TAILQ_EMPTY(&conn->imsg_q))
        {
            conn->send_active = false;
            return event_add_out(ctx->evb, conn);
        }
    }

  return NC_OK;
}

rstatus_t
core_readd_in(struct context* ctx, struct conn* conn)
{
    if (!conn->client)
    {
        if (!TAILQ_EMPTY(&conn->omsg_q))
        {
            conn->recv_active = false;
            return event_add_in(ctx->evb, conn);
        }
    }

  return NC_OK;
}

static void
core_close(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    char type, *addrstr;

    ASSERT(conn->sd > 0);

    if (conn->client) {
        type = 'c';
        addrstr = nc_unresolve_peer_desc(conn->sd);
    } else {
        type = conn->proxy ? 'p' : 's';
        addrstr = nc_unresolve_addr(conn->addr, conn->addrlen);
    }
    log_debug(LOG_NOTICE, "close %c %d '%s' on event %04"PRIX32" eof %d done "
              "%d rb %zu sb %zu%c %s", type, conn->sd, addrstr, conn->events,
              conn->eof, conn->done, conn->recv_bytes, conn->send_bytes,
              conn->err ? ':' : ' ', conn->err ? strerror(conn->err) : "");

    status = event_del_conn(ctx->evb, conn);
    if (status < 0) {
        log_warn("event del conn %c %d failed, ignored: %s",
                 type, conn->sd, strerror(errno));
    }

    conn->close(ctx, conn);
}

static void
core_error(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    char type = conn->client ? 'c' : (conn->proxy ? 'p' : 's');

    status = nc_get_soerror(conn->sd);
    if (status < 0) {
        log_warn("get soerr on %c %d failed, ignored: %s", type, conn->sd,
                  strerror(errno));
    }
    conn->err = errno;

    core_close(ctx, conn);
}

static void
core_timeout(struct context *ctx)
{
    for (;;) {
        struct msg *msg;
        struct conn *conn;
        int64_t now, then;

        msg = msg_tmo_min();
        if (msg == NULL) {
            ctx->timeout = ctx->max_timeout;
            return;
        }

        /* skip over req that are in-error or done */

        if (msg->error || msg->done) {
            msg_tmo_delete(msg);
            continue;
        }

        /*
         * timeout expired req and all the outstanding req on the timing
         * out server
         */

        conn = msg->tmo_rbe.data;
        then = msg->tmo_rbe.key;

        now = nc_msec_now();
        if (now < then) {
            int delta = (int)(then - now);
            ctx->timeout = MIN(delta, ctx->max_timeout);
            return;
        }

        log_debug(LOG_INFO, "req %"PRIu64" on s %d timedout", msg->id, conn->sd);

        msg_tmo_delete(msg);
        conn->err = ETIMEDOUT;

        core_close(ctx, conn);
    }
}

rstatus_t
core_core(void *arg, uint32_t events)
{
    rstatus_t status;
    struct conn *conn = arg;
    struct context *ctx = conn_to_ctx(conn);

    log_debug(LOG_VVERB, "event %04"PRIX32" on %c %d", events,
              conn->client ? 'c' : (conn->proxy ? 'p' : 's'), conn->sd);

    conn->events = events;

    if ((events & EVENT_ERR) ||
        (events & EVENT_READ) ||
        (events & EVENT_WRITE)) {
        core_log_msg_queues(ctx);
    }

    /* error takes precedence over read | write */
    if (events & EVENT_ERR) {
        core_error(ctx, conn);
        return NC_ERROR;
    }

    /* read takes precedence over write */
    if (events & EVENT_READ) {
        status = core_recv(ctx, conn);
        if (status != NC_OK || conn->done || conn->err) {
            core_close(ctx, conn);
            return NC_ERROR;
        }
    }

    if (events & EVENT_WRITE) {
        status = core_send(ctx, conn);
        if (status != NC_OK || conn->done || conn->err) {
            core_close(ctx, conn);
            return NC_ERROR;
        }
    }

    return NC_OK;
}

void
_core_log_msg_queues(struct context *ctx)
{
    uint32_t i;
    log_debug(LOG_DEBUG, "-----------------loop data----------------");
    struct server_pool *pool;
    for (i = 0; i < ctx->pool.nelem; i++) {
        pool = array_get(&ctx->pool, i);
        _core_log_msg_queues_pool(pool);
    }
}

void
_core_log_msg_queues_pool(struct server_pool *pool)
{
    log_debug(LOG_DEBUG, "**** Listen '%.*s' queue:", pool->addrstr.len,
              pool->addrstr.data);
    _core_log_msg_queues_client(pool);
    _core_log_msg_queues_server_arr("Frontend", &pool->frontends.server_arr);
    _core_log_msg_queues_server_arr("Backend ", &pool->backends.server_arr);
}

void
_core_log_msg_queues_server_arr(char* pool_type, struct array *server_arr)
{
    uint32_t i, n;
    struct server *server;
    n = array_n(server_arr);
    for (i = 0; i < n; i++) {
        server = array_get(server_arr, i);
        log_debug(LOG_DEBUG, "**** %s server '%.*s' queue:", pool_type,
                  server->name.len, server->name.data);
        _core_log_msg_queues_server(server);
    }
}

void
_core_log_msg_queues_client(struct server_pool *pool)
{
    struct conn *conn;
    TAILQ_FOREACH(conn, &pool->c_conn_q, conn_tqe)
    {
        _core_log_msg_queues_connection(conn);
    }
}

void
_core_log_msg_queues_server(struct server *server)
{
    struct conn *conn;
    TAILQ_FOREACH(conn, &server->s_conn_q, conn_tqe)
    {
        _core_log_msg_queues_connection(conn);
    }
}

void
_core_log_msg_queues_connection(struct conn *conn)
{
    struct msg *msg, *nmsg;
    struct msg_tqh msg_q;

    msg = conn->rmsg;
    if (msg != NULL) {
        log_debug(LOG_DEBUG, "***** Message recv on sd %d", conn->sd);
        _core_log_msg_queues_msg(msg);
    }

    msg = conn->smsg;
    if (msg != NULL) {
        log_debug(LOG_DEBUG, "***** Message send on sd %d", conn->sd);
        _core_log_msg_queues_msg(msg);
    }

    msg_q = conn->imsg_q;
    log_debug(LOG_DEBUG, "***** Message queue in on sd %d", conn->sd);
    for (msg = TAILQ_FIRST(&msg_q); msg != NULL; msg = nmsg) {
        nmsg = TAILQ_NEXT(msg, s_tqe);
        _core_log_msg_queues_msg(msg);
    }

    msg_q = conn->omsg_q;
    log_debug(LOG_DEBUG, "***** Message queue out on sd %d", conn->sd);
    for (msg = TAILQ_FIRST(&msg_q); msg != NULL; msg = nmsg) {
        nmsg = TAILQ_NEXT(msg, s_tqe);
        _core_log_msg_queues_msg(msg);
    }
}

void
_core_log_msg_queues_msg(struct msg* msg) {
    struct msg* pmsg = msg->peer;
    struct string *msg_type;
    msg_type = msg_type_string(msg->type);
    log_debug(LOG_DEBUG, "****** Message id: %d, type: %.*s, request: %d",
            msg->id, msg_type->len, msg_type->data, msg->request);
    if (pmsg != NULL) {
        msg_type = msg_type_string(pmsg->type);
        log_debug(LOG_DEBUG, "******* Peer Message id: %d, type: %.*s,"
                " request: %d",
                pmsg->id, msg_type->len, msg_type->data, pmsg->request);
    }
}

rstatus_t
core_loop(struct context *ctx)
{
    int nsd;

    nsd = event_wait(ctx->evb, ctx->timeout);
    if (nsd < 0) {
        return nsd;
    }

    core_timeout(ctx);

    stats_swap(ctx->stats);

    return NC_OK;
}
