// Floor with io_uring: multishot accept, multishot recv into a provided-buffer
// ring, send as SQEs; one io_uring_enter per loop turn (submit + wait).
// SEND_SYNC=1 env: answer with a plain send() syscall instead of a send SQE.
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <linux/io_uring.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <unistd.h>

#define RESP1 "HTTP/1.1 200 OK\r\ncontent-type: text/plain; charset=utf-8\r\ndate: Tue, 10 Nov 2026 12:00:00 GMT\r\ncontent-length: 13\r\n\r\nHello, World!"
#define RL (sizeof RESP1 - 1)
#define NBUF 1024
#define BUFSZ 16384
#define BGID 1
enum { OP_ACCEPT = 1, OP_RECV = 2, OP_SEND = 3 };

static int rfd;
static unsigned *sq_head, *sq_tail, *sq_mask, *sq_array, *cq_head, *cq_tail, *cq_mask;
static struct io_uring_sqe *sqes; static struct io_uring_cqe *cqes;
static unsigned sq_local_tail, sq_entries;
static struct io_uring_buf_ring *br; static char *bufmem; static unsigned br_tail;

static int enter(unsigned submit, unsigned wait, unsigned flags) {
    return syscall(__NR_io_uring_enter, rfd, submit, wait, flags, NULL, 0);
}
static unsigned pending(void) { return sq_local_tail - atomic_load_explicit((_Atomic unsigned *)sq_head, memory_order_acquire); }
static struct io_uring_sqe *get_sqe(void) {
    if (pending() >= sq_entries) { atomic_store_explicit((_Atomic unsigned *)sq_tail, sq_local_tail, memory_order_release); enter(pending(), 0, IORING_ENTER_GETEVENTS); }
    struct io_uring_sqe *s = &sqes[sq_local_tail++ & *sq_mask];
    memset(s, 0, sizeof *s); return s;
}
static void buf_return(unsigned bid) {
    struct io_uring_buf *b = &br->bufs[br_tail & (NBUF - 1)];
    b->addr = (uint64_t)(bufmem + (size_t)bid * BUFSZ); b->len = BUFSZ; b->bid = bid;
    br_tail++; atomic_store_explicit((_Atomic unsigned short *)&br->tail, (unsigned short)br_tail, memory_order_release);
}
static void arm_recv(int fd) {
    struct io_uring_sqe *s = get_sqe();
    s->opcode = IORING_OP_RECV; s->fd = fd; s->ioprio = IORING_RECV_MULTISHOT;
    s->flags = IOSQE_BUFFER_SELECT; s->buf_group = BGID; s->user_data = ((uint64_t)OP_RECV << 32) | (unsigned)fd;
}

int main(int argc, char **argv) {
    int port = argc > 1 ? atoi(argv[1]) : 8201, one = 1, send_sync = getenv("SEND_SYNC") != NULL;
    static char out[RL * 64]; for (int i = 0; i < 64; i++) memcpy(out + i * RL, RESP1, RL);
    int ls = socket(AF_INET, SOCK_STREAM, 0);
    setsockopt(ls, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    struct sockaddr_in a = {.sin_family = AF_INET, .sin_port = htons(port), .sin_addr.s_addr = htonl(INADDR_LOOPBACK)};
    if (bind(ls, (void *)&a, sizeof a) || listen(ls, 2048)) { perror("bind"); return 1; }

    struct io_uring_params p = {0};
    p.flags = IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN | IORING_SETUP_CQSIZE; p.cq_entries = 8192;
    rfd = syscall(__NR_io_uring_setup, 2048, &p); if (rfd < 0) { perror("io_uring_setup"); return 1; }
    size_t sqsz = p.sq_off.array + p.sq_entries * sizeof(unsigned), cqsz = p.cq_off.cqes + p.cq_entries * sizeof(struct io_uring_cqe);
    size_t rsz = sqsz > cqsz ? sqsz : cqsz;
    char *ring = mmap(0, rsz, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, rfd, IORING_OFF_SQ_RING);
    sqes = mmap(0, p.sq_entries * sizeof(struct io_uring_sqe), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, rfd, IORING_OFF_SQES);
    if (ring == MAP_FAILED || (void *)sqes == MAP_FAILED) { perror("mmap"); return 1; }
    sq_head = (void *)(ring + p.sq_off.head); sq_tail = (void *)(ring + p.sq_off.tail); sq_mask = (void *)(ring + p.sq_off.ring_mask);
    sq_array = (void *)(ring + p.sq_off.array); cq_head = (void *)(ring + p.cq_off.head); cq_tail = (void *)(ring + p.cq_off.tail);
    cq_mask = (void *)(ring + p.cq_off.ring_mask); cqes = (void *)(ring + p.cq_off.cqes); sq_entries = p.sq_entries;
    for (unsigned i = 0; i < p.sq_entries; i++) sq_array[i] = i;

    br = mmap(0, NBUF * sizeof(struct io_uring_buf), PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    bufmem = mmap(0, (size_t)NBUF * BUFSZ, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    struct io_uring_buf_reg reg = {.ring_addr = (uint64_t)br, .ring_entries = NBUF, .bgid = BGID};
    if (syscall(__NR_io_uring_register, rfd, IORING_REGISTER_PBUF_RING, &reg, 1) < 0) { perror("pbuf_ring"); return 1; }
    for (unsigned i = 0; i < NBUF; i++) buf_return(i);

    struct io_uring_sqe *s = get_sqe();
    s->opcode = IORING_OP_ACCEPT; s->fd = ls; s->ioprio = IORING_ACCEPT_MULTISHOT; s->user_data = (uint64_t)OP_ACCEPT << 32;

    for (;;) {
        unsigned n = pending();
        atomic_store_explicit((_Atomic unsigned *)sq_tail, sq_local_tail, memory_order_release);
        int r = enter(n, 1, IORING_ENTER_GETEVENTS);
        if (r < 0 && errno != EINTR && errno != EBUSY && errno != EAGAIN) { perror("enter"); return 1; }
        unsigned head = *cq_head, tail = atomic_load_explicit((_Atomic unsigned *)cq_tail, memory_order_acquire);
        for (; head != tail; head++) {
            struct io_uring_cqe *c = &cqes[head & *cq_mask];
            int op = c->user_data >> 32, fd = (int)(c->user_data & 0xffffffff);
            if (op == OP_ACCEPT) {
                if (c->res >= 0) { setsockopt(c->res, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one); arm_recv(c->res); }
                if (!(c->flags & IORING_CQE_F_MORE)) { s = get_sqe(); s->opcode = IORING_OP_ACCEPT; s->fd = ls; s->ioprio = IORING_ACCEPT_MULTISHOT; s->user_data = (uint64_t)OP_ACCEPT << 32; }
            } else if (op == OP_RECV) {
                if (c->res <= 0) {
                    if (c->res == -ENOBUFS) { arm_recv(fd); continue; }
                    close(fd); continue;
                }
                unsigned bid = c->flags >> IORING_CQE_BUFFER_SHIFT;
                char *b = bufmem + (size_t)bid * BUFSZ, *q = b, *end = b + c->res; int k = 0;
                while ((q = memmem(q, end - q, "\r\n\r\n", 4))) { k++; q += 4; }
                buf_return(bid);
                if (k > 64) k = 64;
                if (k) {
                    if (send_sync) send(fd, out, k * RL, MSG_NOSIGNAL);
                    else { s = get_sqe(); s->opcode = IORING_OP_SEND; s->fd = fd; s->addr = (uint64_t)out; s->len = k * RL; s->msg_flags = MSG_NOSIGNAL; s->user_data = ((uint64_t)OP_SEND << 32) | (unsigned)fd; }
                }
                if (!(c->flags & IORING_CQE_F_MORE)) arm_recv(fd);
            }
        }
        atomic_store_explicit((_Atomic unsigned *)cq_head, head, memory_order_release);
    }
}
