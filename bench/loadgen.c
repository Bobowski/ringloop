// loadgen: minimal keep-alive HTTP/1.1 load generator (epoll, one thread per CPU).
// Usage: loadgen -c CONNS -d SECONDS -t THREADS [-C cpu,cpu] [-p PIPELINE] [-P post-body] [-H "Header: v"] http://host:port/path
// Prints one JSON line: req/s, latency percentiles (µs), errors, bytes.
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <sched.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define RBUF 65536
#define NBUCKET 4096  // log-ish histogram: 1µs resolution to 1024µs, then coarser
#define MAXPIPE 64

typedef struct {
    int fd;
    int inflight;            // requests sent, not yet answered
    size_t woff;             // bytes of the current batch already written
    int wpending;            // batch not fully written
    uint64_t sent_at[MAXPIPE];
    int sa_head, sa_tail;
    // response parser
    int state;               // 0 headers, 1 body(content-length), 2 chunked
    long body_left;
    char hbuf[8192];
    int hlen;
    // chunked
    int cstate; long chunk_left;
} conn_t;

typedef struct {
    pthread_t th;
    int cpu, nconn, ep;
    conn_t *conns;
    uint64_t done, errors, bytes, non2xx;
    uint64_t hist[NBUCKET];
    uint64_t max_lat;
} worker_t;

static struct sockaddr_in addr;
static char *req; static size_t req_len;
static char *batch; static size_t batch_len;
static int pipeline = 1;
static int busy = 0;  // -b: spin instead of sleeping in epoll_wait
static volatile int stop_flag = 0, measuring = 0;

static inline uint64_t now_us(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000ull + ts.tv_nsec / 1000;
}
static inline int bucket(uint64_t us) {
    if (us < 1024) return (int)us;
    int b = 1024; uint64_t lim = 1024; // 16 sub-buckets per octave above 1024µs
    while (us >= lim * 2 && b < NBUCKET - 16) { lim *= 2; b += 16; }
    int sub = (int)((us - lim) * 16 / lim);
    b += sub; return b < NBUCKET ? b : NBUCKET - 1;
}
static inline uint64_t bucket_val(int b) {
    if (b < 1024) return b;
    int oct = (b - 1024) / 16, sub = (b - 1024) % 16;
    uint64_t lim = 1024ull << oct; return lim + lim * sub / 16;
}

static int conn_open(worker_t *w, conn_t *c) {
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (fd < 0) return -1;
    int one = 1; setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    if (connect(fd, (struct sockaddr *)&addr, sizeof addr) < 0 && errno != EINPROGRESS) { close(fd); return -1; }
    memset(c, 0, sizeof *c); c->fd = fd; c->wpending = 1;
    struct epoll_event ev = {.events = EPOLLIN | EPOLLOUT, .data.ptr = c};
    epoll_ctl(w->ep, EPOLL_CTL_ADD, fd, &ev);
    return 0;
}
static void conn_reset(worker_t *w, conn_t *c) {
    close(c->fd); w->errors++;
    conn_open(w, c);
}
static void conn_send(worker_t *w, conn_t *c) {
    // (continue) writing one batch of `pipeline` requests
    while (c->woff < batch_len) {
        ssize_t n = send(c->fd, batch + c->woff, batch_len - c->woff, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EAGAIN) {
                struct epoll_event ev = {.events = EPOLLIN | EPOLLOUT, .data.ptr = c};
                epoll_ctl(w->ep, EPOLL_CTL_MOD, c->fd, &ev); c->wpending = 1; return;
            }
            conn_reset(w, c); return;
        }
        c->woff += n;
    }
    if (c->wpending) {
        struct epoll_event ev = {.events = EPOLLIN, .data.ptr = c};
        epoll_ctl(w->ep, EPOLL_CTL_MOD, c->fd, &ev); c->wpending = 0;
    }
    c->woff = 0;
    uint64_t t = now_us();
    for (int i = 0; i < pipeline; i++) { c->sent_at[c->sa_tail] = t; c->sa_tail = (c->sa_tail + 1) % MAXPIPE; }
    c->inflight += pipeline;
}
static void response_done(worker_t *w, conn_t *c) {
    uint64_t lat = now_us() - c->sent_at[c->sa_head]; c->sa_head = (c->sa_head + 1) % MAXPIPE;
    c->inflight--;
    if (measuring) { w->done++; w->hist[bucket(lat)]++; if (lat > w->max_lat) w->max_lat = lat; }
    c->state = 0; c->hlen = 0;
}
// returns bytes consumed, parsing as many responses as present
static int conn_feed(worker_t *w, conn_t *c, const char *p, size_t n) {
    size_t i = 0;
    while (i < n) {
        if (c->state == 0) {
            // accumulate until \r\n\r\n
            while (i < n) {
                if (c->hlen >= (int)sizeof c->hbuf - 1) return -1;
                c->hbuf[c->hlen++] = p[i++];
                if (c->hlen >= 4 && memcmp(c->hbuf + c->hlen - 4, "\r\n\r\n", 4) == 0) break;
            }
            if (c->hlen < 4 || memcmp(c->hbuf + c->hlen - 4, "\r\n\r\n", 4) != 0) return 0;
            c->hbuf[c->hlen] = 0;
            if (c->hlen < 12 || c->hbuf[9] != '2') { if (measuring) w->non2xx++; }
            char *cl = strcasestr(c->hbuf, "\r\ncontent-length:");
            if (cl) { c->body_left = strtol(cl + 17, NULL, 10); c->state = 1; }
            else if (strcasestr(c->hbuf, "chunked")) { c->state = 2; c->cstate = 0; c->chunk_left = 0; }
            else { c->body_left = 0; c->state = 1; }
            if (c->state == 1 && c->body_left == 0) response_done(w, c);
        } else if (c->state == 1) {
            size_t take = n - i; if ((long)take > c->body_left) take = c->body_left;
            i += take; c->body_left -= take;
            if (c->body_left == 0) response_done(w, c);
        } else {
            // chunked: cstate 0 size line, 1 data, 2 CRLF after data, 3 trailer line(s)
            char ch = p[i];
            if (c->cstate == 0) {
                i++;
                if (ch == '\n') { if (c->chunk_left == 0) c->cstate = 3, c->hlen = 0; else c->cstate = 1; }
                else if (ch != '\r') { int v = (ch >= '0' && ch <= '9') ? ch - '0' : (ch | 32) - 'a' + 10; c->chunk_left = c->chunk_left * 16 + v; }
            } else if (c->cstate == 1) {
                size_t take = n - i; if ((long)take > c->chunk_left) take = c->chunk_left;
                i += take; c->chunk_left -= take; if (c->chunk_left == 0) c->cstate = 2;
            } else if (c->cstate == 2) { i++; if (ch == '\n') c->cstate = 0; }
            else { i++; if (ch == '\n') { if (c->hlen == 0) response_done(w, c); c->hlen = 0; } else if (ch != '\r') c->hlen++; }
        }
    }
    return 0;
}

static void *worker(void *arg) {
    worker_t *w = arg;
    if (w->cpu >= 0) { cpu_set_t s; CPU_ZERO(&s); CPU_SET(w->cpu, &s); pthread_setaffinity_np(pthread_self(), sizeof s, &s); }
    w->ep = epoll_create1(0);
    w->conns = calloc(w->nconn, sizeof(conn_t));
    for (int i = 0; i < w->nconn; i++) conn_open(w, &w->conns[i]);
    struct epoll_event evs[512]; static __thread char buf[RBUF];
    while (!stop_flag) {
        int n = epoll_wait(w->ep, evs, 512, busy ? 0 : 100);
        for (int k = 0; k < n; k++) {
            conn_t *c = evs[k].data.ptr;
            if (evs[k].events & (EPOLLERR | EPOLLHUP)) { conn_reset(w, c); continue; }
            if (evs[k].events & EPOLLOUT) { if (c->wpending && c->inflight == 0) conn_send(w, c); else if (c->wpending) conn_send(w, c); }
            if (evs[k].events & EPOLLIN) {
                int dead = 0;
                for (;;) {
                    ssize_t r = recv(c->fd, buf, RBUF, 0);
                    if (r > 0) { if (measuring) w->bytes += r; if (conn_feed(w, c, buf, r) < 0) { dead = 1; break; } if (r < RBUF) break; }
                    else if (r == 0) { dead = 1; break; }
                    else { if (errno != EAGAIN) dead = 1; break; }
                }
                if (dead) { conn_reset(w, c); continue; }
                if (c->inflight == 0 && !c->wpending) conn_send(w, c);
            }
        }
    }
    return NULL;
}

int main(int argc, char **argv) {
    int conns = 128, dur = 10, threads = 2, warm = 2; const char *cpus = NULL, *body = NULL; char extra[4096] = ""; int opt;
    while ((opt = getopt(argc, argv, "c:d:t:C:p:P:H:w:b")) != -1) switch (opt) {
        case 'c': conns = atoi(optarg); break; case 'd': dur = atoi(optarg); break;
        case 't': threads = atoi(optarg); break; case 'C': cpus = optarg; break;
        case 'p': pipeline = atoi(optarg); break; case 'P': body = optarg; break;
        case 'w': warm = atoi(optarg); break;
        case 'b': busy = 1; break;
        case 'H': strcat(extra, optarg); strcat(extra, "\r\n"); break;
    }
    if (optind >= argc || pipeline > MAXPIPE) { fprintf(stderr, "usage\n"); return 2; }
    char host[256], path[2048] = "/"; int port = 80;
    if (sscanf(argv[optind], "http://%255[^:/]:%d%2047s", host, &port, path) < 2) { fprintf(stderr, "bad url\n"); return 2; }
    addr.sin_family = AF_INET; addr.sin_port = htons(port); inet_pton(AF_INET, host, &addr.sin_addr);
    req = malloc(8192 + (body ? strlen(body) : 0));
    if (body) req_len = sprintf(req, "POST %s HTTP/1.1\r\nHost: %s:%d\r\nContent-Type: application/json\r\nContent-Length: %zu\r\n%s\r\n%s", path, host, port, strlen(body), extra, body);
    else req_len = sprintf(req, "GET %s HTTP/1.1\r\nHost: %s:%d\r\n%s\r\n", path, host, port, extra);
    batch = malloc(req_len * pipeline); for (int i = 0; i < pipeline; i++) memcpy(batch + i * req_len, req, req_len); batch_len = req_len * pipeline;
    int cpu_list[64], ncpu = 0;
    if (cpus) { char *s = strdup(cpus), *tok = strtok(s, ","); while (tok) { cpu_list[ncpu++] = atoi(tok); tok = strtok(NULL, ","); } }
    worker_t *ws = calloc(threads, sizeof(worker_t));
    for (int i = 0; i < threads; i++) {
        ws[i].cpu = ncpu ? cpu_list[i % ncpu] : -1;
        ws[i].nconn = conns / threads + (i < conns % threads);
        pthread_create(&ws[i].th, NULL, worker, &ws[i]);
    }
    sleep(warm); measuring = 1; uint64_t t0 = now_us();
    sleep(dur); measuring = 0; uint64_t t1 = now_us(); stop_flag = 1;
    uint64_t done = 0, errors = 0, bytes = 0, non2xx = 0, maxl = 0; static uint64_t hist[NBUCKET];
    for (int i = 0; i < threads; i++) {
        pthread_join(ws[i].th, NULL);
        done += ws[i].done; errors += ws[i].errors; bytes += ws[i].bytes; non2xx += ws[i].non2xx;
        if (ws[i].max_lat > maxl) maxl = ws[i].max_lat;
        for (int b = 0; b < NBUCKET; b++) hist[b] += ws[i].hist[b];
    }
    double pct[] = {0.5, 0.9, 0.99, 0.999}; uint64_t pv[4] = {0}; uint64_t acc = 0; int pi = 0;
    for (int b = 0; b < NBUCKET && pi < 4; b++) { acc += hist[b]; while (pi < 4 && acc >= (uint64_t)(pct[pi] * done) && done) pv[pi++] = bucket_val(b); }
    double secs = (t1 - t0) / 1e6;
    printf("{\"rps\": %.0f, \"p50_us\": %lu, \"p90_us\": %lu, \"p99_us\": %lu, \"p999_us\": %lu, \"max_us\": %lu, \"requests\": %lu, \"errors\": %lu, \"non2xx\": %lu, \"bytes_per_req\": %.0f}\n",
           done / secs, pv[0], pv[1], pv[2], pv[3], maxl, done, errors, non2xx, done ? (double)bytes / done : 0.0);
    return 0;
}
