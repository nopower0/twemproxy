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
static uint64_t ntotal_msg;      /* total # message counter from start */
static struct rbtree tmo_rbt;    /* timeout rbtree */
static struct rbnode tmo_rbs;    /* timeout rbtree sentinel */
static size_t msg_free_limit;    /* msg free limit */

static void msg_free(struct msg *msg);

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
    struct msg *msg, *tmp_msg;

    if (!TAILQ_EMPTY(&free_msgq)) {
        ASSERT(nfree_msgq > 0);

        msg = TAILQ_FIRST(&free_msgq);
        nfree_msgq--;
        TAILQ_REMOVE(&free_msgq, msg, m_tqe);

        if (msg_free_limit != 0 && nfree_msgq > msg_free_limit) {
            tmp_msg = TAILQ_FIRST(&free_msgq);
            nfree_msgq--;
            TAILQ_REMOVE(&free_msgq, tmp_msg, m_tqe);
            msg_free(tmp_msg);
        }

        goto done;
    }

    msg = nc_alloc(sizeof(*msg));
    if (msg == NULL) {
        return NULL;
    }
    ntotal_msg++;

done:
    /* c_tqe, s_tqe, and m_tqe are left uninitialized */
    msg->id = ++msg_id;
    msg->peer = NULL;
    msg->owner = NULL;

    rbtree_node_init(&msg->tmo_rbe);

    STAILQ_INIT(&msg->mhdr);
    msg->mlen = 0;

    msg->state = 0;
    msg->pos = NULL;
    msg->token = NULL;

    msg->parser = NULL;
    msg->result = MSG_PARSE_OK;

    msg->pre_splitcopy = NULL;
    msg->post_splitcopy = NULL;
    msg->pre_coalesce = NULL;
    msg->post_coalesce = NULL;

    msg->type = MSG_UNKNOWN;

    msg->key_start = NULL;
    msg->key_end = NULL;

    msg->vlen = 0;
    msg->end = NULL;

    msg->frag_owner = NULL;
    msg->nfrag = 0;
    msg->nfrag_done = 0;
    msg->nfrag_error = 0;
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
    msg->done = 0;
    msg->fdone = 0;
    msg->first_fragment = 0;
    msg->last_fragment = 0;
    msg->swallow = 0;
    msg->redis = 0;

    return msg;
}

struct msg *
msg_get(struct conn *conn, bool request, bool redis)
{
    struct msg *msg;

    msg = _msg_get();
    if (msg == NULL) {
        return NULL;
    }

    msg->owner = conn;
    msg->request = request ? 1 : 0;
    msg->redis = redis ? 1 : 0;

    if (redis) {
        if (request) {
            msg->parser = redis_parse_req;
        } else {
            msg->parser = redis_parse_rsp;
        }
        msg->pre_splitcopy = redis_pre_splitcopy;
        msg->post_splitcopy = redis_post_splitcopy;
        msg->pre_coalesce = redis_pre_coalesce;
        msg->post_coalesce = redis_post_coalesce;
    } else {
        if (request) {
            msg->parser = memcache_parse_req;
        } else {
            msg->parser = memcache_parse_rsp;
        }
        msg->pre_splitcopy = memcache_pre_splitcopy;
        msg->post_splitcopy = memcache_post_splitcopy;
        msg->pre_coalesce = memcache_pre_coalesce;
        msg->post_coalesce = memcache_post_coalesce;
    }

    log_debug(LOG_VVERB, "get msg %p id %"PRIu64" request %d owner sd %d",
              msg, msg->id, msg->request, conn->sd);

    return msg;
}

struct msg *
msg_get_error(bool redis, err_t err)
{
    struct msg *msg;
    struct mbuf *mbuf;
    int n;
    char *errstr = err ? strerror(err) : "unknown";
    char *protstr = redis ? "-ERR" : "SERVER_ERROR";

    msg = _msg_get();
    if (msg == NULL) {
        return NULL;
    }

    msg->state = 0;
    msg->type = MSG_RSP_MC_SERVER_ERROR;

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

    nfree_msgq++;
    TAILQ_INSERT_HEAD(&free_msgq, msg, m_tqe);
}

void
msg_dump(struct msg *msg)
{
    struct mbuf *mbuf;

    loga("msg dump id %"PRIu64" request %d len %"PRIu32" type %d done %d "
         "error %d (err %d)", msg->id, msg->request, msg->mlen, msg->type,
         msg->done, msg->error, msg->err);

    STAILQ_FOREACH(mbuf, &msg->mhdr, next) {
        uint8_t *p, *q;
        long int len;

        p = mbuf->start;
        q = mbuf->last;
        len = q - p;

        loga_hexdump(p, len, "mbuf with %ld bytes of data", len);
    }
}

void
msg_init(struct instance *nci)
{
    log_debug(LOG_DEBUG, "msg size %d", sizeof(struct msg));
    msg_id = 0;
    frag_id = 0;
    nfree_msgq = 0;
    TAILQ_INIT(&free_msgq);
    rbtree_init(&tmo_rbt, &tmo_rbs);

    msg_free_limit = nci->mbuf_free_limit; /* use mbuf_free_limit */
    log_debug(LOG_DEBUG, "msg free limit %zu", msg_free_limit);
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

uint32_t msg_nfree(void)
{
    return nfree_msgq;
}

uint64_t msg_ntotal(void)
{
    return ntotal_msg;
}

bool
msg_empty(struct msg *msg)
{
    return msg->mlen == 0 ? true : false;
}

static rstatus_t
msg_parsed(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct msg *nmsg;
    struct mbuf *mbuf, *nbuf;

    /*
     * Warning if the msg has too large fragments. Put this in other places?
     */
    if (conn->client && msg->frag_owner && msg->frag_owner->nfrag >= 1024) {
        log_warn("c %d '%s' msg %"PRIu64" has %u fragments",
                 conn->sd, nc_unresolve_peer_desc(conn->sd),
                 msg->id, msg->frag_owner->nfrag);
    }

    mbuf = STAILQ_LAST(&msg->mhdr, mbuf, next);
    if (msg->pos == mbuf->last) {
        /* no more data to parse */
        conn->recv_done(ctx, conn, msg, NULL);
        return NC_OK;
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

    nmsg = msg_get(msg->owner, msg->request, conn->redis);
    if (nmsg == NULL) {
        mbuf_put(nbuf);
        return NC_ENOMEM;
    }
    mbuf_insert(&nmsg->mhdr, nbuf);
    nmsg->pos = nbuf->pos;

    /* update length of current (msg) and new message (nmsg) */
    nmsg->mlen = mbuf_length(nbuf);
    msg->mlen -= nmsg->mlen;

    conn->recv_done(ctx, conn, msg, nmsg);

    return NC_OK;
}

static rstatus_t
msg_fragment(struct context *ctx, struct conn *conn, struct msg *msg)
{
    rstatus_t status;  /* return status */
    struct msg *nmsg;  /* new message */
    struct mbuf *nbuf; /* new mbuf */

    ASSERT(conn->client && !conn->proxy);
    ASSERT(msg->request);

    nbuf = mbuf_split(&msg->mhdr, msg->pos, msg->pre_splitcopy, msg);
    if (nbuf == NULL) {
        return NC_ENOMEM;
    }

    status = msg->post_splitcopy(msg);
    if (status != NC_OK) {
        mbuf_put(nbuf);
        return status;
    }

    nmsg = msg_get(msg->owner, msg->request, msg->redis);
    if (nmsg == NULL) {
        mbuf_put(nbuf);
        return NC_ENOMEM;
    }
    mbuf_insert(&nmsg->mhdr, nbuf);
    nmsg->pos = nbuf->pos;

    /* update length of current (msg) and new message (nmsg) */
    nmsg->mlen = mbuf_length(nbuf);
    msg->mlen -= nmsg->mlen;

    /*
     * Attach unique fragment id to all fragments of the message vector. All
     * fragments of the message, including the first fragment point to the
     * first fragment through the frag_owner pointer. The first_fragment and
     * last_fragment identify first and last fragment respectively.
     *
     * For example, a message vector given below is split into 3 fragments:
     *  'get key1 key2 key3\r\n'
     *  Or,
     *  '*4\r\n$4\r\nmget\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n'
     *
     *   +--------------+
     *   |  msg vector  |
     *   |(original msg)|
     *   +--------------+
     *
     *       frag_owner         frag_owner
     *     /-----------+      /------------+
     *     |           |      |            |
     *     |           v      v            |
     *   +--------------------+     +---------------------+
     *   |   frag_id = 10     |     |   frag_id = 10      |
     *   | first_fragment = 1 |     |  first_fragment = 0 |
     *   | last_fragment = 0  |     |  last_fragment = 0  |
     *   |     nfrag = 3      |     |      nfrag = 0      |
     *   +--------------------+     +---------------------+
     *               ^
     *               |  frag_owner
     *               \-------------+
     *                             |
     *                             |
     *                  +---------------------+
     *                  |   frag_id = 10      |
     *                  |  first_fragment = 0 |
     *                  |  last_fragment = 1  |
     *                  |      nfrag = 0      |
     *                  +---------------------+
     *
     *
     */
    if (msg->frag_id == 0) {
        msg->frag_id = ++frag_id;
        msg->first_fragment = 1;
        msg->nfrag = 1;
        msg->frag_owner = msg;
    }
    nmsg->frag_id = msg->frag_id;
    msg->last_fragment = 0;
    nmsg->last_fragment = 1;
    nmsg->frag_owner = msg->frag_owner;
    msg->frag_owner->nfrag++;

    stats_pool_incr(ctx, conn->owner, fragments);

    log_debug(LOG_VERB, "fragment msg into %"PRIu64" and %"PRIu64" frag id "
              "%"PRIu64"", msg->id, nmsg->id, msg->frag_id);

    conn->recv_done(ctx, conn, msg, nmsg);

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
        conn->recv_done(ctx, conn, msg, NULL);
        return NC_OK;
    }

    msg->parser(msg);

    switch (msg->result) {
    case MSG_PARSE_OK:
        status = msg_parsed(ctx, conn, msg);
        break;

    case MSG_PARSE_FRAGMENT:
        status = msg_fragment(ctx, conn, msg);
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

    ASSERT(!TAILQ_EMPTY(&send_msgq) && nsend != 0);

    conn->smsg = NULL;

    n = conn_sendv(conn, &sendv, nsend);

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

    if (n > 0) {
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

typedef enum msg_rw_type {
    MSG_RW_UNKNOWN,
    MSG_RW_READ,
    MSG_RW_WRITE
} msg_rw_type_t;

typedef struct msg_type_info {
    msg_type_t type;
    const char *str;
    msg_rw_type_t rw_type;
} msg_type_info_t;

static msg_type_info_t type_info[] = {
    { MSG_UNKNOWN,                    "unknown",          MSG_RW_UNKNOWN },

    /* memcache retrieval requests */
    { MSG_REQ_MC_GET,                 "get",              MSG_RW_READ },
    { MSG_REQ_MC_GETS,                "gets",             MSG_RW_WRITE },

    /* memcache delete request */
    { MSG_REQ_MC_DELETE,              "delete",           MSG_RW_WRITE },

    /* memcache cas request and storage request */
    { MSG_REQ_MC_CAS,                 "cas",              MSG_RW_WRITE },

    /* memcache storage request */
    { MSG_REQ_MC_SET,                 "set",              MSG_RW_WRITE },
    { MSG_REQ_MC_ADD,                 "add",              MSG_RW_WRITE },
    { MSG_REQ_MC_REPLACE,             "replace",          MSG_RW_WRITE },
    { MSG_REQ_MC_APPEND,              "append",           MSG_RW_WRITE },
    { MSG_REQ_MC_PREPEND,             "prepend",          MSG_RW_WRITE },

    /* memcache arithmetic request */
    { MSG_REQ_MC_INCR,                "incr",             MSG_RW_WRITE },
    { MSG_REQ_MC_DECR,                "decr",             MSG_RW_WRITE },

    /* memcache quit request */
    { MSG_REQ_MC_QUIT,                "quit",             MSG_RW_UNKNOWN },

    /* memcache arithmetic response */
    { MSG_RSP_MC_NUM,                 "num",              MSG_RW_UNKNOWN },

    /* memcache cas and storage response */
    { MSG_RSP_MC_STORED,              "stored",           MSG_RW_UNKNOWN },
    { MSG_RSP_MC_NOT_STORED,          "not_stored",       MSG_RW_UNKNOWN },
    { MSG_RSP_MC_EXISTS,              "exists",           MSG_RW_UNKNOWN },
    { MSG_RSP_MC_NOT_FOUND,           "not_found",        MSG_RW_UNKNOWN },
    { MSG_RSP_MC_END,                 "end",              MSG_RW_UNKNOWN },
    { MSG_RSP_MC_VALUE,               "value",            MSG_RW_UNKNOWN },

    /* memcache delete response */
    { MSG_RSP_MC_DELETED,             "deleted",          MSG_RW_UNKNOWN },

    /* memcache error responses */
    { MSG_RSP_MC_ERROR,               "error",            MSG_RW_UNKNOWN },
    { MSG_RSP_MC_CLIENT_ERROR,        "client_error",     MSG_RW_UNKNOWN },
    { MSG_RSP_MC_SERVER_ERROR,        "server_error",     MSG_RW_UNKNOWN },

    /* redis commands - keys */
    { MSG_REQ_REDIS_DEL,              "del",              MSG_RW_WRITE },
    { MSG_REQ_REDIS_EXISTS,           "exists",           MSG_RW_READ },
    { MSG_REQ_REDIS_EXPIRE,           "expire",           MSG_RW_WRITE },
    { MSG_REQ_REDIS_EXPIREAT,         "expireat",         MSG_RW_WRITE },
    { MSG_REQ_REDIS_PEXPIRE,          "pexpire",          MSG_RW_WRITE },
    { MSG_REQ_REDIS_PEXPIREAT,        "pexpireat",        MSG_RW_WRITE },
    { MSG_REQ_REDIS_PERSIST,          "persist",          MSG_RW_WRITE },
    { MSG_REQ_REDIS_PTTL,             "pttl",             MSG_RW_READ },
    { MSG_REQ_REDIS_TTL,              "ttl",              MSG_RW_READ },
    { MSG_REQ_REDIS_TYPE,             "type",             MSG_RW_READ },

    /* redis requests - string */
    { MSG_REQ_REDIS_APPEND,           "append",           MSG_RW_WRITE },
    { MSG_REQ_REDIS_BITCOUNT,         "bitcount",         MSG_RW_READ },
    { MSG_REQ_REDIS_DECR,             "decr",             MSG_RW_WRITE },
    { MSG_REQ_REDIS_DECRBY,           "decrby",           MSG_RW_WRITE },
    { MSG_REQ_REDIS_DUMP,             "dump",             MSG_RW_READ },
    { MSG_REQ_REDIS_GET,              "get",              MSG_RW_READ },
    { MSG_REQ_REDIS_GETBIT,           "getbit",           MSG_RW_READ },
    { MSG_REQ_REDIS_GETRANGE,         "getrange",         MSG_RW_READ },
    { MSG_REQ_REDIS_GETSET,           "getset",           MSG_RW_WRITE },
    { MSG_REQ_REDIS_INCR,             "incr",             MSG_RW_WRITE },
    { MSG_REQ_REDIS_INCRBY,           "incrby",           MSG_RW_WRITE },
    { MSG_REQ_REDIS_INCRBYFLOAT,      "incrbyfloat",      MSG_RW_WRITE },
    { MSG_REQ_REDIS_MGET,             "mget",             MSG_RW_READ },
    { MSG_REQ_REDIS_PSETEX,           "psetex",           MSG_RW_WRITE },
    { MSG_REQ_REDIS_RESTORE,          "restore",          MSG_RW_WRITE },
    { MSG_REQ_REDIS_SET,              "set",              MSG_RW_WRITE },
    { MSG_REQ_REDIS_SETBIT,           "setbit",           MSG_RW_WRITE },
    { MSG_REQ_REDIS_SETEX,            "setex",            MSG_RW_WRITE },
    { MSG_REQ_REDIS_SETNX,            "setnx",            MSG_RW_WRITE },
    { MSG_REQ_REDIS_SETRANGE,         "setrange",         MSG_RW_WRITE },
    { MSG_REQ_REDIS_STRLEN,           "strlen",           MSG_RW_READ },

    /* redis requests - hashes */
    { MSG_REQ_REDIS_HDEL,             "hdel",             MSG_RW_WRITE },
    { MSG_REQ_REDIS_HEXISTS,          "hexists",          MSG_RW_READ },
    { MSG_REQ_REDIS_HGET,             "hget",             MSG_RW_READ },
    { MSG_REQ_REDIS_HGETALL,          "hgetall",          MSG_RW_READ },
    { MSG_REQ_REDIS_HINCRBY,          "hincrby",          MSG_RW_WRITE },
    { MSG_REQ_REDIS_HINCRBYFLOAT,     "hincrbyfloat",     MSG_RW_WRITE },
    { MSG_REQ_REDIS_HKEYS,            "hkeys",            MSG_RW_READ },
    { MSG_REQ_REDIS_HLEN,             "hlen",             MSG_RW_READ },
    { MSG_REQ_REDIS_HMGET,            "hmget",            MSG_RW_READ },
    { MSG_REQ_REDIS_HMSET,            "hmset",            MSG_RW_WRITE },
    { MSG_REQ_REDIS_HSET,             "hset",             MSG_RW_WRITE },
    { MSG_REQ_REDIS_HSETNX,           "hsetnx",           MSG_RW_WRITE },
    { MSG_REQ_REDIS_HVALS,            "hvals",            MSG_RW_READ },

    /* redis requests - lists */
    { MSG_REQ_REDIS_LINDEX,           "lindex",           MSG_RW_READ },
    { MSG_REQ_REDIS_LINSERT,          "linsert",          MSG_RW_WRITE },
    { MSG_REQ_REDIS_LLEN,             "llen",             MSG_RW_READ },
    { MSG_REQ_REDIS_LPOP,             "lpop",             MSG_RW_WRITE },
    { MSG_REQ_REDIS_LPUSH,            "lpush",            MSG_RW_WRITE },
    { MSG_REQ_REDIS_LPUSHX,           "lpushx",           MSG_RW_WRITE },
    { MSG_REQ_REDIS_LRANGE,           "lrange",           MSG_RW_READ },
    { MSG_REQ_REDIS_LREM,             "lrem",             MSG_RW_WRITE },
    { MSG_REQ_REDIS_LSET,             "lset",             MSG_RW_WRITE },
    { MSG_REQ_REDIS_LTRIM,            "ltrim",            MSG_RW_WRITE },
    { MSG_REQ_REDIS_RPOP,             "rpop",             MSG_RW_WRITE },
    { MSG_REQ_REDIS_RPOPLPUSH,        "rpoplpush",        MSG_RW_WRITE },
    { MSG_REQ_REDIS_RPUSH,            "rpush",            MSG_RW_WRITE },
    { MSG_REQ_REDIS_RPUSHX,           "rpushx",           MSG_RW_WRITE },

    /* redis requests - sets */
    { MSG_REQ_REDIS_SADD,             "sadd",             MSG_RW_WRITE },
    { MSG_REQ_REDIS_SCARD,            "scard",            MSG_RW_READ },
    { MSG_REQ_REDIS_SDIFF,            "sdiff",            MSG_RW_READ },
    { MSG_REQ_REDIS_SDIFFSTORE,       "sdiffstore",       MSG_RW_WRITE },
    { MSG_REQ_REDIS_SINTER,           "sinter",           MSG_RW_READ },
    { MSG_REQ_REDIS_SINTERSTORE,      "sinterstore",      MSG_RW_WRITE },
    { MSG_REQ_REDIS_SISMEMBER,        "sismember",        MSG_RW_READ },
    { MSG_REQ_REDIS_SMEMBERS,         "smembers",         MSG_RW_READ },
    { MSG_REQ_REDIS_SMOVE,            "smove",            MSG_RW_WRITE },
    { MSG_REQ_REDIS_SPOP,             "spop",             MSG_RW_WRITE },
    { MSG_REQ_REDIS_SRANDMEMBER,      "srandmember",      MSG_RW_READ },
    { MSG_REQ_REDIS_SREM,             "srem",             MSG_RW_WRITE },
    { MSG_REQ_REDIS_SUNION,           "sunion",           MSG_RW_READ },
    { MSG_REQ_REDIS_SUNIONSTORE,      "sunionstore",      MSG_RW_WRITE },

    /* redis requests - sorted sets */
    { MSG_REQ_REDIS_ZADD,             "zadd",             MSG_RW_WRITE },
    { MSG_REQ_REDIS_ZCARD,            "zcard",            MSG_RW_READ },
    { MSG_REQ_REDIS_ZCOUNT,           "zcount",           MSG_RW_READ },
    { MSG_REQ_REDIS_ZINCRBY,          "zincrby",          MSG_RW_WRITE },
    { MSG_REQ_REDIS_ZINTERSTORE,      "zinterstore",      MSG_RW_WRITE },
    { MSG_REQ_REDIS_ZRANGE,           "zrange",           MSG_RW_READ },
    { MSG_REQ_REDIS_ZRANGEBYSCORE,    "zrangebyscore",    MSG_RW_READ },
    { MSG_REQ_REDIS_ZRANK,            "zrank",            MSG_RW_READ },
    { MSG_REQ_REDIS_ZREM,             "zrem",             MSG_RW_WRITE },
    { MSG_REQ_REDIS_ZREMRANGEBYRANK,  "zremrangebyrank",  MSG_RW_WRITE },
    { MSG_REQ_REDIS_ZREMRANGEBYSCORE, "zremrangebyscore", MSG_RW_WRITE },
    { MSG_REQ_REDIS_ZREVRANGE,        "zrevrange",        MSG_RW_READ },
    { MSG_REQ_REDIS_ZREVRANGEBYSCORE, "zrevrangebyscore", MSG_RW_READ },
    { MSG_REQ_REDIS_ZREVRANK,         "zrevrank",         MSG_RW_READ },
    { MSG_REQ_REDIS_ZSCORE,           "zscore",           MSG_RW_READ },
    { MSG_REQ_REDIS_ZUNIONSTORE,      "zunionstore",      MSG_RW_WRITE },

    /* redis requests - eval */
    { MSG_REQ_REDIS_EVAL,             "eval",             MSG_RW_WRITE },
    { MSG_REQ_REDIS_EVALSHA,          "evalsha",          MSG_RW_WRITE },

    /* redis response */
    { MSG_RSP_REDIS_STATUS,           "status",           MSG_RW_UNKNOWN },
    { MSG_RSP_REDIS_ERROR,            "error",            MSG_RW_UNKNOWN },
    { MSG_RSP_REDIS_INTEGER,          "integer",          MSG_RW_UNKNOWN },
    { MSG_RSP_REDIS_BULK,             "bulk",             MSG_RW_UNKNOWN },
    { MSG_RSP_REDIS_MULTIBULK,        "multibulk",        MSG_RW_UNKNOWN },
    { MSG_SENTINEL,                   "MAX",              MSG_RW_UNKNOWN }
};

bool
msg_type_check()
{
    unsigned i;
    for (i = MSG_UNKNOWN; i < MSG_SENTINEL; i++) {
        if (type_info[i].type != i) {
            log_crit("msg type info %d mismatch %d %s",
                     i, type_info[i].type, type_info[i].str);
            return false;
        }
    }
    return true;
}

const char *
msg_type_string(msg_type_t type)
{
    ASSERT(type > MSG_UNKNOWN && type < MSG_SENTINEL);
    return type_info[type].str;
}

bool
msg_type_is_read(msg_type_t type)
{
    ASSERT(type > MSG_UNKNOWN && type < MSG_SENTINEL);
    return type_info[type].rw_type == MSG_RW_READ;
}
