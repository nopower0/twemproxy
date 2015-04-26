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
#include <nc_server.h>
#include <nc_conf.h>

#define SERVER_MAX_CANDIDATES 64 /* should be enough when route read requests */

void
server_ref(struct conn *conn, void *owner)
{
    struct server *server = owner;

    ASSERT(conn != NULL && owner != NULL);
    ASSERT(!conn->client && !conn->proxy);
    ASSERT(conn->owner == NULL);

    conn->family = server->family;
    conn->addrlen = server->addrlen;
    conn->addr = server->addr;

    if (conn->is_read) {
        server->ns_conn_q_rd++;
        TAILQ_INSERT_TAIL(&server->s_conn_q_rd, conn, conn_tqe);
        ASSERT(!TAILQ_EMPTY(&server->s_conn_q_rd));
    } else {
        server->ns_conn_q_wr++;
        TAILQ_INSERT_TAIL(&server->s_conn_q_wr, conn, conn_tqe);
        ASSERT(!TAILQ_EMPTY(&server->s_conn_q_wr));
    }

    conn->owner = owner;

    log_debug(LOG_VVERB, "ref conn %p owner %p into '%.*s", conn, server,
              server->pname.len, server->pname.data);
}

void
server_unref(struct conn *conn)
{
    struct server *server;

    ASSERT(!conn->client && !conn->proxy);
    ASSERT(conn->owner != NULL);

    server = conn->owner;
    conn->owner = NULL;

    if (conn->is_read) {
        ASSERT(server->ns_conn_q_rd != 0);
        server->ns_conn_q_rd--;
        TAILQ_REMOVE(&server->s_conn_q_rd, conn, conn_tqe);
    } else {
        ASSERT(server->ns_conn_q_wr != 0);
        server->ns_conn_q_wr--;
        TAILQ_REMOVE(&server->s_conn_q_wr, conn, conn_tqe);
    }

    log_debug(LOG_VVERB, "unref conn %p owner %p from '%.*s'", conn, server,
              server->pname.len, server->pname.data);
}

int
server_timeout(struct conn *conn)
{
    struct server *server;
    struct server_pool *pool;

    ASSERT(!conn->client && !conn->proxy);

    server = conn->owner;
    pool = server->owner;

    return pool->timeout;
}

bool
server_active(struct conn *conn)
{
    ASSERT(!conn->client && !conn->proxy);

    if (!TAILQ_EMPTY(&conn->imsg_q)) {
        log_debug(LOG_VVERB, "s %d is active", conn->sd);
        return true;
    }

    if (!TAILQ_EMPTY(&conn->omsg_q)) {
        log_debug(LOG_VVERB, "s %d is active", conn->sd);
        return true;
    }

    if (conn->rmsg != NULL) {
        log_debug(LOG_VVERB, "s %d is active", conn->sd);
        return true;
    }

    if (conn->smsg != NULL) {
        log_debug(LOG_VVERB, "s %d is active", conn->sd);
        return true;
    }

    log_debug(LOG_VVERB, "s %d is inactive", conn->sd);

    return false;
}

rstatus_t
server_init(struct array *server, struct array *conf_server,
            struct server_pool *sp)
{
    /*
     * Originaly, every server will occupy a position in the pool distribution.
     * In order to support slave hosts, we combine the master and all its
     * slaves into a group, and this group will occupy one position in the pool
     * distribution. If so, distribution calcuation will stay the same.
     *
     * The master should be the stub server of its group, and all its slaves
     * will be appended into the slave_pool.
     * If the master is not configed, the first slave will act as the stub
     * server. But don't worry, the write request will not routed to it,
     * because we would check the slave flag. Moreover, this slave will NOT
     * pushed into the slave_pool.
     */

    ASSERT(array_n(conf_server) != 0);
    ASSERT(array_n(server) == 0);

    rstatus_t status;
    uint32_t i, nstub, nserver, slave_pool_size;
    struct conf_server *cs;
    struct server *s, *last_stub;

    /* We precalloc the server array to make sure not realloc, which may waste
     * some memory. The same for slave_pool_size. */
    slave_pool_size = array_n(conf_server);
    status = array_init(server, array_n(conf_server), sizeof(struct server));
    if (status != NC_OK) {
        return status;
    }

    nstub = 0;
    nserver = 0;
    last_stub = NULL;
    /* conf_server is already sorted by name+slave in conf_validate_server */
    for (i = 0; i < array_n(conf_server); i++) {
        cs = array_get(conf_server, i);

        if (last_stub == NULL
            || string_compare(&cs->name, &last_stub->name) != 0) {
            /* this server belongs a new name */
            s = array_push(server);
            ASSERT(s != NULL);
            nstub++;
            last_stub = s;

            s->idx = array_idx(server, s);
            s->sidx = nserver++;
            s->owner = sp;
            status = conf_server_transform(cs, s, slave_pool_size);
            if (status != NC_OK) {
                server_deinit(server);
                return status;
            }
            ASSERT(array_n(&s->slave_pool) == 0);

            if (s->local) {
                last_stub->local_server = s; /* last local win */
            }

            log_debug(LOG_VERB, "server init server %"PRIu32" stub '%.*s' %s",
                      s->idx, s->pname.len, s->pname.data,
                      (s->slave == 0 ? "master" : "slave"));
        } else {
            /* this server belongs the same name */
            s = array_push(&last_stub->slave_pool);
            ASSERT(s != NULL);

            s->idx = array_idx(&last_stub->slave_pool, s);
            s->sidx = nserver++;
            s->owner = sp;
            status = conf_server_transform(cs, s, 0);
            if (status != NC_OK) {
                server_deinit(server);
                return status;
            }

            if (s->local) {
                last_stub->local_server = s; /* last local win */
            }

            log_debug(LOG_VERB, "server init server %"PRIu32" slave_pool "
                      "%"PRIu32" '%.*s' %s",
                      last_stub->idx, s->idx, s->pname.len, s->pname.data,
                      (s->slave == 0 ? "master" : "slave"));
        }
    }
    ASSERT(array_n(server) == nstub);
    ASSERT(array_n(conf_server) == nserver);

    log_debug(LOG_DEBUG, "init %"PRIu32" servers in pool %"PRIu32" '%.*s'",
              nserver, sp->idx, sp->name.len, sp->name.data);

    return NC_OK;
}

void
server_deinit(struct array *server)
{
    uint32_t i, nserver;

    for (i = 0, nserver = array_n(server); i < nserver; i++) {
        struct server *s;

        s = array_pop(server);
        ASSERT(TAILQ_EMPTY(&s->s_conn_q) && s->ns_conn_q == 0);

        server_deinit(&s->slave_pool);
        array_deinit(&s->slave_pool);
    }

    array_deinit(server);
}

struct conn *
server_conn2(struct server *server, bool is_read)
{
    struct server_pool *pool;
    struct conn *conn;
    unsigned int max_connections;
    uint32_t ns_conn_q;
    struct conn_tqh *s_conn_q;

    pool = server->owner;
    if (is_read) {
        max_connections = pool->server_connections - 1;
        ns_conn_q = server->ns_conn_q_rd;
        s_conn_q = &server->s_conn_q_rd;
    } else {
        max_connections = 1;
        ns_conn_q = server->ns_conn_q_wr;
        s_conn_q = &server->s_conn_q_wr;
    }
    if (max_connections < 1)
        max_connections = 1;

    /*
     * FIXME: handle multiple server connections per server and do load
     * balancing on it. Support multiple algorithms for
     * 'server_connections:' > 0 key
     */

    if (ns_conn_q < max_connections) {
        return conn_get4(server, false, pool->redis, is_read);
    }
    ASSERT(ns_conn_q == max_connections);

    /*
     * Pick a server connection from the head of the queue and insert
     * it back into the tail of queue to maintain the lru order
     */
    conn = TAILQ_FIRST(s_conn_q);
    ASSERT(!conn->client && !conn->proxy);

    TAILQ_REMOVE(s_conn_q, conn, conn_tqe);
    TAILQ_INSERT_TAIL(s_conn_q, conn, conn_tqe);

    return conn;
}

struct conn *
server_conn(struct server *server)
{
    return server_conn2(server, true);
}

static rstatus_t
server_each_preconnect(void *elem, void *data)
{
    rstatus_t status;
    struct server *server;
    struct server_pool *pool;
    struct conn *conn;

    server = elem;
    pool = server->owner;

    conn = server_conn(server);
    if (conn == NULL) {
        return NC_ENOMEM;
    }

    status = server_connect(pool->ctx, server, conn);
    if (status != NC_OK) {
        log_warn("connect to server '%.*s' failed, ignored: %s",
                 server->pname.len, server->pname.data, strerror(errno));
        server_close(pool->ctx, conn);
    }

    return NC_OK;
}

static rstatus_t
server_each_disconnect(void *elem, void *data)
{
    struct server *server;
    struct server_pool *pool;

    server = elem;
    pool = server->owner;

    while (!TAILQ_EMPTY(&server->s_conn_q_rd)) {
        struct conn *conn;

        ASSERT(server->ns_conn_q_rd > 0);

        conn = TAILQ_FIRST(&server->s_conn_q_rd);
        conn->close(pool->ctx, conn);
    }

    while (!TAILQ_EMPTY(&server->s_conn_q_wr)) {
        struct conn *conn;

        ASSERT(server->ns_conn_q_wr > 0);

        conn = TAILQ_FIRST(&server->s_conn_q_wr);
        conn->close(pool->ctx, conn);
    }

    return NC_OK;
}

static void
_server_failure(struct context *ctx, struct server *server, bool update_pool)
{
    struct server_pool *pool = server->owner;
    int64_t now, next;
    rstatus_t status;

    server->failure_count++;

    log_debug(LOG_VERB, "server '%.*s' failure count %"PRIu32" limit %"PRIu32,
              server->pname.len, server->pname.data, server->failure_count,
              pool->server_failure_limit);

    if (server->failure_count < pool->server_failure_limit) {
        return;
    }

    now = nc_usec_now();
    if (now < 0) {
        return;
    }

    stats_server_set_ts(ctx, server, server_ejected_at, now);
    stats_pool_incr(ctx, pool, server_ejects);

    next = now + pool->server_retry_timeout;
    server->failure_count = 0;
    server->next_retry = next;

    if (!update_pool) {
        return;
    }

    log_warn("update pool %"PRIu32" '%.*s' to delete server '%.*s' "
             "for next %"PRIu32" secs", pool->idx, pool->name.len,
             pool->name.data, server->pname.len, server->pname.data,
             pool->server_retry_timeout / 1000 / 1000);

    status = server_pool_run(pool);
    if (status != NC_OK) {
        log_error("updating pool %"PRIu32" '%.*s' failed: %s", pool->idx,
                  pool->name.len, pool->name.data, strerror(errno));
    }
}

static void
server_failure(struct context *ctx, struct server *server)
{
    struct server_pool *pool = server->owner;

    if (server->slave || array_n(&server->slave_pool) > 0) {
        /*
         * If server is slave, auto_eject_hosts should be false,
         * which is already checked in conf.
         * Here, we only mark failure without update the pool.
         */
        _server_failure(ctx, server, false);
    } else if (pool->auto_eject_hosts) {
        _server_failure(ctx, server, true);
    }
}

static void
server_close_stats(struct context *ctx, struct server *server, err_t err,
                   unsigned eof, unsigned connected)
{
    if (connected) {
        stats_server_decr(ctx, server, server_connections);
    }

    if (eof) {
        stats_server_incr(ctx, server, server_eof);
        return;
    }

    switch (err) {
    case ETIMEDOUT:
        stats_server_incr(ctx, server, server_timedout);
        break;
    case EPIPE:
    case ECONNRESET:
    case ECONNABORTED:
    case ECONNREFUSED:
    case ENOTCONN:
    case ENETDOWN:
    case ENETUNREACH:
    case EHOSTDOWN:
    case EHOSTUNREACH:
    default:
        stats_server_incr(ctx, server, server_err);
        break;
    }
}

void
server_close(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *msg, *nmsg; /* current and next message */
    struct conn *c_conn;    /* peer client connection */

    ASSERT(!conn->client && !conn->proxy);

    server_close_stats(ctx, conn->owner, conn->err, conn->eof,
                       conn->connected);

    if (conn->sd < 0) {
        server_failure(ctx, conn->owner);
        conn->unref(conn);
        conn_put(conn);
        return;
    }

    for (msg = TAILQ_FIRST(&conn->imsg_q); msg != NULL; msg = nmsg) {
        nmsg = TAILQ_NEXT(msg, s_tqe);

        /* dequeue the message (request) from server inq */
        conn->dequeue_inq(ctx, conn, msg);

        /*
         * Don't send any error response, if
         * 1. request is tagged as noreply or,
         * 2. client has already closed its connection
         */
        if (msg->swallow || msg->noreply) {
            log_info("close s %d swallow req %"PRIu64" len %"PRIu32
                     " type %d", conn->sd, msg->id, msg->mlen, msg->type);
            req_put(msg);
        } else {
            c_conn = msg->owner;
            ASSERT(c_conn->client && !c_conn->proxy);

            msg->done = 1;
            msg->error = 1;
            if (msg->frag_owner) {
                msg->frag_owner->nfrag_done++;
                msg->frag_owner->nfrag_error++;
            }
            msg->err = conn->err;

            if (req_done(c_conn, TAILQ_FIRST(&c_conn->omsg_q))) {
                event_add_out(ctx->evb, msg->owner);
            }

            log_info("close s %d schedule error for req %"PRIu64" "
                     "len %"PRIu32" type %d from c %d%c %s", conn->sd, msg->id,
                     msg->mlen, msg->type, c_conn->sd, conn->err ? ':' : ' ',
                     conn->err ? strerror(conn->err): " ");
        }
    }
    ASSERT(TAILQ_EMPTY(&conn->imsg_q));

    for (msg = TAILQ_FIRST(&conn->omsg_q); msg != NULL; msg = nmsg) {
        nmsg = TAILQ_NEXT(msg, s_tqe);

        /* dequeue the message (request) from server outq */
        conn->dequeue_outq(ctx, conn, msg);

        if (msg->swallow) {
            log_info("close s %d swallow req %"PRIu64" len %"PRIu32
                     " type %d", conn->sd, msg->id, msg->mlen, msg->type);
            req_put(msg);
        } else {
            c_conn = msg->owner;
            ASSERT(c_conn->client && !c_conn->proxy);

            msg->done = 1;
            msg->error = 1;
            if (msg->frag_owner) {
                msg->frag_owner->nfrag_done++;
                msg->frag_owner->nfrag_error++;
            }
            msg->err = conn->err;

            if (req_done(c_conn, TAILQ_FIRST(&c_conn->omsg_q))) {
                event_add_out(ctx->evb, msg->owner);
            }

            log_info("close s %d schedule error for req %"PRIu64" "
                     "len %"PRIu32" type %d from c %d%c %s", conn->sd, msg->id,
                     msg->mlen, msg->type, c_conn->sd, conn->err ? ':' : ' ',
                     conn->err ? strerror(conn->err): " ");
        }
    }
    ASSERT(TAILQ_EMPTY(&conn->omsg_q));

    msg = conn->rmsg;
    if (msg != NULL) {
        conn->rmsg = NULL;

        ASSERT(!msg->request);
        ASSERT(msg->peer == NULL);

        rsp_put(msg);

        log_info("close s %d discarding rsp %"PRIu64" len %"PRIu32" "
                 "in error", conn->sd, msg->id, msg->mlen);
    }

    ASSERT(conn->smsg == NULL);

    server_failure(ctx, conn->owner);

    conn->unref(conn);

    status = close(conn->sd);
    if (status < 0) {
        log_error("close s %d failed, ignored: %s", conn->sd, strerror(errno));
    }
    conn->sd = -1;

    conn_put(conn);
}

rstatus_t
server_connect(struct context *ctx, struct server *server, struct conn *conn)
{
    rstatus_t status;

    ASSERT(!conn->client && !conn->proxy);

    if (conn->sd > 0) {
        /* already connected on server connection */
        return NC_OK;
    }

    log_debug(LOG_VVERB, "connect to server '%.*s'", server->pname.len,
              server->pname.data);

    conn->sd = socket(conn->family, SOCK_STREAM, 0);
    if (conn->sd < 0) {
        log_error("socket for server '%.*s' failed: %s", server->pname.len,
                  server->pname.data, strerror(errno));
        status = NC_ERROR;
        goto error;
    }

    status = nc_set_nonblocking(conn->sd);
    if (status != NC_OK) {
        log_error("set nonblock on s %d for server '%.*s' failed: %s",
                  conn->sd,  server->pname.len, server->pname.data,
                  strerror(errno));
        goto error;
    }

    if (server->pname.data[0] != '/') {
        status = nc_set_tcpnodelay(conn->sd);
        if (status != NC_OK) {
            log_warn("set tcpnodelay on s %d for server '%.*s' failed, ignored: %s",
                     conn->sd, server->pname.len, server->pname.data,
                     strerror(errno));
        }
    }

    status = event_add_conn(ctx->evb, conn);
    if (status != NC_OK) {
        log_error("event add conn s %d for server '%.*s' failed: %s",
                  conn->sd, server->pname.len, server->pname.data,
                  strerror(errno));
        goto error;
    }

    ASSERT(!conn->connecting && !conn->connected);

    status = connect(conn->sd, conn->addr, conn->addrlen);
    if (status != NC_OK) {
        if (errno == EINPROGRESS) {
            conn->connecting = 1;
            log_debug(LOG_DEBUG, "connecting on s %d to server '%.*s'",
                      conn->sd, server->pname.len, server->pname.data);
            return NC_OK;
        }

        log_error("connect on s %d to server '%.*s' failed: %s", conn->sd,
                  server->pname.len, server->pname.data, strerror(errno));

        goto error;
    }

    ASSERT(!conn->connecting);
    conn->connected = 1;
    log_info("connected on s %d to server '%.*s'", conn->sd,
             server->pname.len, server->pname.data);

    return NC_OK;

error:
    conn->err = errno;
    return status;
}

void
server_connected(struct context *ctx, struct conn *conn)
{
    struct server *server = conn->owner;

    ASSERT(!conn->client && !conn->proxy);
    ASSERT(conn->connecting && !conn->connected);

    stats_server_incr(ctx, server, server_connections);

    conn->connecting = 0;
    conn->connected = 1;

    log_info("connected on s %d to server '%.*s'", conn->sd,
             server->pname.len, server->pname.data);
}

void
server_ok(struct context *ctx, struct conn *conn)
{
    struct server *server = conn->owner;

    ASSERT(!conn->client && !conn->proxy);
    ASSERT(conn->connected);

    if (server->failure_count != 0) {
        log_debug(LOG_VERB, "reset server '%.*s' failure count from %"PRIu32
                  " to 0", server->pname.len, server->pname.data,
                  server->failure_count);
        server->failure_count = 0;
        server->next_retry = 0LL;
    }
}

static rstatus_t
server_pool_update(struct server_pool *pool)
{
    rstatus_t status;
    int64_t now;
    uint32_t pnlive_server; /* prev # live server */

    if (!pool->auto_eject_hosts) {
        return NC_OK;
    }

    if (pool->next_rebuild == 0LL) {
        return NC_OK;
    }

    now = nc_usec_now();
    if (now < 0) {
        return NC_ERROR;
    }

    if (now <= pool->next_rebuild) {
        if (pool->nlive_server == 0) {
            errno = ECONNREFUSED;
            return NC_ERROR;
        }
        return NC_OK;
    }

    pnlive_server = pool->nlive_server;

    status = server_pool_run(pool);
    if (status != NC_OK) {
        log_error("updating pool %"PRIu32" with dist %d failed: %s", pool->idx,
                  pool->dist_type, strerror(errno));
        return status;
    }

    log_info("update pool %"PRIu32" '%.*s' to add %"PRIu32" servers",
             pool->idx, pool->name.len, pool->name.data,
             pool->nlive_server - pnlive_server);


    return NC_OK;
}

static uint32_t
server_pool_hash(struct server_pool *pool, uint8_t *key, uint32_t keylen)
{
    ASSERT(array_n(&pool->server) != 0);

    if (array_n(&pool->server) == 1) {
        return 0;
    }

    ASSERT(key != NULL && keylen != 0);

    return pool->key_hash((char *)key, keylen);
}

static struct server *
server_pool_server(struct server_pool *pool, uint8_t *key, uint32_t keylen)
{
    struct server *server;
    uint32_t hash, idx;

    ASSERT(array_n(&pool->server) != 0);
    ASSERT(key != NULL && keylen != 0);

    switch (pool->dist_type) {
    case DIST_KETAMA:
        hash = server_pool_hash(pool, key, keylen);
        idx = ketama_dispatch(pool->continuum, pool->ncontinuum, hash);
        break;

    case DIST_MODULA:
        hash = server_pool_hash(pool, key, keylen);
        idx = modula_dispatch(pool->continuum, pool->ncontinuum, hash);
        break;

    case DIST_RANDOM:
        idx = random_dispatch(pool->continuum, pool->ncontinuum, 0);
        break;

    default:
        NOT_REACHED();
        return NULL;
    }
    ASSERT(idx < array_n(&pool->server));

    server = array_get(&pool->server, idx);

    log_debug(LOG_VERB, "key '%.*s' on dist %d maps to server '%.*s'", keylen,
              key, pool->dist_type, server->pname.len, server->pname.data);

    return server;
}

static struct server *
server_pool_server_balance(struct server_pool *pool, struct server *stub,
                           bool is_read, uint32_t hint)
{
    int64_t now;
    if (!is_read) { /* write request */
        if (stub->slave) {
            log_warn("no server for write request in pool '%.*s'",
                     pool->name.len, pool->name.data);
            return NULL;
        } else {
            log_debug(LOG_DEBUG, "write request balance to server '%.*s'",
                      stub->pname.len, stub->pname.data);
            return stub;
        }
    }

    if (array_n(&stub->slave_pool) == 0) {
        log_debug(LOG_DEBUG, "read request balance to the only server '%.*s'",
                  stub->pname.len, stub->pname.data);
        return stub;
    }

    now = nc_usec_now();
    if (now < 0) {
        return NULL;
    }

    /* Prefer the master when it's available */
    if (pool->read_prefer == READ_PREFER_MASTER
        && !stub->slave && stub->next_retry <= now) {
        log_debug(LOG_DEBUG, "read request balance to master %.*s",
                  stub->pname.len, stub->pname.data);
        return stub;
    }

    /*
     * Prefer the server at local when it's available, and
     * 1. we prefer slave and it's a slave
     * 2. or we prefer none
     */
    if (pool->read_local_first
        && stub->local_server && stub->local_server->next_retry <= now
        && ((pool->read_prefer == READ_PREFER_SLAVE && stub->local_server->slave)
            || pool->read_prefer == READ_PREFER_NONE)) {
        log_debug(LOG_DEBUG, "read request balance to local server '%.*s'",
                  stub->local_server->pname.len, stub->local_server->pname.data);
        return stub->local_server;
    }

    /* now, we try to filter servers available and select it based on client */

    struct server *candidates[SERVER_MAX_CANDIDATES] = {NULL};
    uint32_t i, num;
    /*
     * Add stub into candidates, if
     * 1. stub is slave
     * 2. or stub is master and we don't prefer the slave
     */
    num = 0;
    if (stub->next_retry <= now && (stub->slave
        || (!stub->slave && pool->read_prefer != READ_PREFER_SLAVE))) {
        candidates[num++] = stub;
        log_debug(LOG_VERB, "read request balance add candidate '%.*s'",
                  stub->pname.len, stub->pname.data);
    }
    for (i = 0; i < array_n(&stub->slave_pool); i++) {
        if (num >= SERVER_MAX_CANDIDATES) {
            break;
        }
        struct server *s = array_get(&stub->slave_pool, i);
        ASSERT(s != NULL);
        if (s->next_retry <= now) {
            candidates[num++] = s;
            log_debug(LOG_VERB, "read request balance add candidate '%.*s'",
                      s->pname.len, s->pname.data);
        }
    }

    if (num > 0) {
        struct server *s = candidates[hint % num];
        log_debug(LOG_DEBUG, "read request balance to server '%.*s' "
                  "hint %"PRIu32"", s->pname.len, s->pname.data, hint);
        return s;
    }

    /* Failover to stub if no server is available. Maybe stub is in failure,
     * but we have no better idea */
    log_debug(LOG_DEBUG, "no slave for read request, failover to '%.*s'",
              stub->pname.len, stub->pname.data);
    return stub;
}

struct conn *
server_pool_conn(struct context *ctx, struct server_pool *pool, uint8_t *key,
                 uint32_t keylen, bool is_read, uint32_t hint)
{
    rstatus_t status;
    struct server *server;
    struct conn *conn;

    status = server_pool_update(pool);
    if (status != NC_OK) {
        return NULL;
    }

    /* from a given {key, keylen} pick a server from pool */
    server = server_pool_server(pool, key, keylen);
    if (server == NULL) {
        return NULL;
    }

    /* route read request to slaves if possible */
    server = server_pool_server_balance(pool, server, is_read, hint);
    if (server == NULL) {
        return NULL;
    }

    /* pick a connection to a given server */
    conn = server_conn2(server, is_read);
    if (conn == NULL) {
        return NULL;
    }

    status = server_connect(ctx, server, conn);
    if (status != NC_OK) {
        server_close(ctx, conn);
        return NULL;
    }

    return conn;
}

static rstatus_t
server_pool_each_preconnect(void *elem, void *data)
{
    rstatus_t status;
    struct server_pool *sp = elem;

    if (!sp->preconnect) {
        return NC_OK;
    }

    status = array_each(&sp->server, server_each_preconnect, NULL);
    if (status != NC_OK) {
        return status;
    }

    return NC_OK;
}

rstatus_t
server_pool_preconnect(struct context *ctx)
{
    rstatus_t status;

    status = array_each(&ctx->pool, server_pool_each_preconnect, NULL);
    if (status != NC_OK) {
        return status;
    }

    return NC_OK;
}

static rstatus_t
server_pool_each_disconnect(void *elem, void *data)
{
    rstatus_t status;
    struct server_pool *sp = elem;

    status = array_each(&sp->server, server_each_disconnect, NULL);
    if (status != NC_OK) {
        return status;
    }

    return NC_OK;
}

void
server_pool_disconnect(struct context *ctx)
{
    array_each(&ctx->pool, server_pool_each_disconnect, NULL);
}

static rstatus_t
server_pool_each_set_owner(void *elem, void *data)
{
    struct server_pool *sp = elem;
    struct context *ctx = data;

    sp->ctx = ctx;

    return NC_OK;
}

static rstatus_t
server_pool_each_calc_connections(void *elem, void *data)
{
    struct server_pool *sp = elem;
    struct context *ctx = data;

    ctx->max_server_connections += sp->server_connections * array_n(&sp->server);
    ctx->max_server_connections += 1; /* pool listening socket */

    return NC_OK;
}

rstatus_t
server_pool_run(struct server_pool *pool)
{
    ASSERT(array_n(&pool->server) != 0);

    switch (pool->dist_type) {
    case DIST_KETAMA:
        return ketama_update(pool);

    case DIST_MODULA:
        return modula_update(pool);

    case DIST_RANDOM:
        return random_update(pool);

    default:
        NOT_REACHED();
        return NC_ERROR;
    }

    return NC_OK;
}

static rstatus_t
server_pool_each_run(void *elem, void *data)
{
    return server_pool_run(elem);
}

rstatus_t
server_pool_init(struct array *server_pool, struct array *conf_pool,
                 struct context *ctx)
{
    rstatus_t status;
    uint32_t npool;

    npool = array_n(conf_pool);
    ASSERT(npool != 0);
    ASSERT(array_n(server_pool) == 0);

    status = array_init(server_pool, npool, sizeof(struct server_pool));
    if (status != NC_OK) {
        return status;
    }

    /* transform conf pool to server pool */
    status = array_each(conf_pool, conf_pool_each_transform, server_pool);
    if (status != NC_OK) {
        server_pool_deinit(server_pool);
        return status;
    }
    ASSERT(array_n(server_pool) == npool);

    /* set ctx as the server pool owner */
    status = array_each(server_pool, server_pool_each_set_owner, ctx);
    if (status != NC_OK) {
        server_pool_deinit(server_pool);
        return status;
    }

    /* calculate max server connections */
    ctx->max_server_connections = 0;
    status = array_each(server_pool, server_pool_each_calc_connections, ctx);
    if (status != NC_OK) {
        server_pool_deinit(server_pool);
        return status;
    }

    /* update server pool continuum */
    status = array_each(server_pool, server_pool_each_run, NULL);
    if (status != NC_OK) {
        server_pool_deinit(server_pool);
        return status;
    }

    log_debug(LOG_DEBUG, "init %"PRIu32" pools", npool);

    return NC_OK;
}

void
server_pool_deinit(struct array *server_pool)
{
    uint32_t i, npool;

    for (i = 0, npool = array_n(server_pool); i < npool; i++) {
        struct server_pool *sp;

        sp = array_pop(server_pool);
        ASSERT(sp->p_conn == NULL);
        ASSERT(TAILQ_EMPTY(&sp->c_conn_q) && sp->nc_conn_q == 0);

        if (sp->continuum != NULL) {
            nc_free(sp->continuum);
            sp->ncontinuum = 0;
            sp->nserver_continuum = 0;
            sp->nlive_server = 0;
        }

        server_deinit(&sp->server);

        log_debug(LOG_DEBUG, "deinit pool %"PRIu32" '%.*s'", sp->idx,
                  sp->name.len, sp->name.data);
    }

    array_deinit(server_pool);

    log_debug(LOG_DEBUG, "deinit %"PRIu32" pools", npool);
}
