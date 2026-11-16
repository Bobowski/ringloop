/* Minimal io_uring layer for ringloop: raw syscalls, no liburing, and no
 * kernel headers (the ABI it needs is written out in ll_abi.h).
 *
 * One ring per loop thread (SINGLE_ISSUER + DEFER_TASKRUN, Linux 6.1), one
 * provided-buffer ring for multishot recv.
 */
#ifndef LL_URING_H
#define LL_URING_H

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <errno.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

#include "ll_abi.h"

#define LL_BGID 1

typedef struct {
    int fd;
    unsigned flags;            /* setup flags the kernel accepted */
    /* submission queue */
    unsigned *sq_khead, *sq_ktail;
    unsigned sq_mask, sq_entries, sq_tail; /* sq_tail: local, published on submit */
    struct ll_sqe *sqes;
    /* completion queue */
    unsigned *cq_khead, *cq_ktail;
    unsigned cq_mask;
    struct ll_cqe *cqes;
    /* mappings */
    void *ring_ptr; size_t ring_sz;
    void *cq_ptr; size_t cq_sz;
    size_t sqes_sz;
    /* provided buffers */
    struct ll_buf *br             /* entry 0 also holds the tail */;
    char *buf_mem;
    unsigned buf_count, buf_size, br_tail;
    /* SQEs the submission queue had no room for (see ll_get_sqe) */
    struct ll_sqe *spill;
    unsigned spill_n, spill_cap;
    unsigned long spilled;     /* how many SQEs ever went through the spill */
    int broken;                /* an SQE was lost (out of memory): the ring cannot be trusted */
    struct ll_sqe sink;
    unsigned test_fail_submits; /* tests: pretend the kernel refuses the next N mid-turn submits */
} ll_ring;

static inline int ll__enter(ll_ring *r, unsigned submit, unsigned wait, unsigned flags, void *arg, size_t argsz) {
    return (int)syscall(__NR_io_uring_enter, r->fd, submit, wait, flags, arg, argsz);
}

static inline unsigned ll_sq_pending(ll_ring *r) {
    return r->sq_tail - atomic_load_explicit((_Atomic unsigned *)r->sq_khead, memory_order_acquire);
}

static inline void ll_buf_return(ll_ring *r, unsigned bid) {
    struct ll_buf *b = &r->br[r->br_tail & (r->buf_count - 1)];
    b->addr = (uint64_t)(uintptr_t)(r->buf_mem + (size_t)bid * r->buf_size);
    b->len = r->buf_size;
    b->bid = (uint16_t)bid;
    r->br_tail++;
    atomic_store_explicit((_Atomic unsigned short *)&r->br[0].resv, (unsigned short)r->br_tail, memory_order_release);
}

static inline char *ll_buf_ptr(ll_ring *r, unsigned bid) {
    return r->buf_mem + (size_t)bid * r->buf_size;
}

/* The ring is released when its last reference goes: the fd and the three
 * ring mappings each hold one, and the kernel cancels outstanding requests
 * from there, asynchronously. Nothing here can make that synchronous; the
 * order below only keeps our own bookkeeping simple. */
static void ll_ring_close(ll_ring *r) {
    if (r->fd >= 0) close(r->fd);
    free(r->spill);
    if (r->sqes && (void *)r->sqes != MAP_FAILED) munmap(r->sqes, r->sqes_sz);
    if (r->cq_ptr && r->cq_ptr != r->ring_ptr && r->cq_ptr != MAP_FAILED) munmap(r->cq_ptr, r->cq_sz);
    if (r->ring_ptr && r->ring_ptr != MAP_FAILED) munmap(r->ring_ptr, r->ring_sz);
    if (r->br && (void *)r->br != MAP_FAILED) munmap(r->br, r->buf_count * sizeof(struct ll_buf));
    if (r->buf_mem && r->buf_mem != MAP_FAILED) munmap(r->buf_mem, (size_t)r->buf_count * r->buf_size);
    memset(r, 0, sizeof *r);
    r->fd = -1;
}

/* Returns 0, or -errno. buf_count must be a power of two (see RECV_BUFS).
 *
 * DEFER_TASKRUN (Linux 6.1) is required: completions then happen only
 * inside this thread's own io_uring_enter, which the loop relies on when it
 * closes or replaces a ring. An older kernel refuses the flags with -EINVAL
 * and the loop uses epoll. The ring belongs to the thread that creates it. */
/* Which step of the last failed ll_ring_init of this thread failed (for
 * `python -m ringloop`, which says why a system gets no io_uring). */
static __thread const char *ll_init_step = "";

static int ll_ring_init(ll_ring *r, unsigned entries, unsigned buf_count, unsigned buf_size) {
    struct ll_params p;
    memset(r, 0, sizeof *r);
    r->fd = -1;
    ll_init_step = "io_uring_setup";
    memset(&p, 0, sizeof p);
    p.flags = LL_SETUP_SINGLE_ISSUER | LL_SETUP_DEFER_TASKRUN | LL_SETUP_SUBMIT_ALL
            | LL_SETUP_CQSIZE | LL_SETUP_CLAMP;
    p.cq_entries = entries * 4;
    r->fd = (int)syscall(__NR_io_uring_setup, entries, &p);
    if (r->fd < 0) return -errno;
    r->flags = p.flags;
    ll_init_step = "mmap of the rings";

    r->ring_sz = p.sq_off.array + p.sq_entries * sizeof(unsigned);
    r->cq_sz = p.cq_off.cqes + p.cq_entries * sizeof(struct ll_cqe);
    if (p.features & LL_FEAT_SINGLE_MMAP) {
        if (r->cq_sz > r->ring_sz) r->ring_sz = r->cq_sz;
        r->cq_sz = r->ring_sz;
    }
    r->ring_ptr = mmap(0, r->ring_sz, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, r->fd, LL_OFF_SQ_RING);
    if (r->ring_ptr == MAP_FAILED) goto fail;
    if (p.features & LL_FEAT_SINGLE_MMAP) {
        r->cq_ptr = r->ring_ptr;
    } else {
        r->cq_ptr = mmap(0, r->cq_sz, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, r->fd, LL_OFF_CQ_RING);
        if (r->cq_ptr == MAP_FAILED) goto fail;
    }
    r->sqes_sz = p.sq_entries * sizeof(struct ll_sqe);
    r->sqes = mmap(0, r->sqes_sz, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, r->fd, LL_OFF_SQES);
    if ((void *)r->sqes == MAP_FAILED) goto fail;

    char *sq = r->ring_ptr, *cq = r->cq_ptr;
    r->sq_khead = (unsigned *)(sq + p.sq_off.head);
    r->sq_ktail = (unsigned *)(sq + p.sq_off.tail);
    r->sq_mask = *(unsigned *)(sq + p.sq_off.ring_mask);
    r->sq_entries = p.sq_entries;
    unsigned *array = (unsigned *)(sq + p.sq_off.array);
    for (unsigned i = 0; i < p.sq_entries; i++) array[i] = i;
    r->sq_tail = *r->sq_ktail;
    r->cq_khead = (unsigned *)(cq + p.cq_off.head);
    r->cq_ktail = (unsigned *)(cq + p.cq_off.tail);
    r->cq_mask = *(unsigned *)(cq + p.cq_off.ring_mask);
    r->cqes = (struct ll_cqe *)(cq + p.cq_off.cqes);

    r->buf_count = buf_count;
    r->buf_size = buf_size;
    ll_init_step = "mmap of the receive buffers";
    r->br = mmap(0, buf_count * sizeof(struct ll_buf), PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    if ((void *)r->br == MAP_FAILED) goto fail;
    r->buf_mem = mmap(0, (size_t)buf_count * buf_size, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    if (r->buf_mem == MAP_FAILED) goto fail;
    struct ll_buf_reg reg;
    memset(&reg, 0, sizeof reg);
    reg.ring_addr = (uint64_t)(uintptr_t)r->br;
    reg.ring_entries = buf_count;
    reg.bgid = LL_BGID;
    ll_init_step = "io_uring_register(PBUF_RING)";
    if (syscall(__NR_io_uring_register, r->fd, LL_REGISTER_PBUF_RING, &reg, 1) < 0) {
        /* Ubuntu's 6.8 kernels (seen: 6.8.0-1070-azure) carry a backport
         * with the test of the reserved bytes inverted: the registration
         * is refused when they are zero, as they must be everywhere else,
         * and taken when they are not. Nothing in that kernel reads them.
         * A kernel without the defect refuses this second form too, and
         * the loop then uses epoll. */
        if (errno != EINVAL) goto fail;
        reg.resv[2] = 1;
        if (syscall(__NR_io_uring_register, r->fd, LL_REGISTER_PBUF_RING, &reg, 1) < 0) {
            errno = EINVAL;
            goto fail;
        }
    }
    for (unsigned i = 0; i < buf_count; i++) ll_buf_return(r, i);
    ll_init_step = "";
    return 0;
fail: {
        int e = errno ? errno : EINVAL;
        ll_ring_close(r);
        return -e;
    }
}

/* Publish queued SQEs and hand them to the kernel without waiting.
 * Returns how many it took, or -errno. */
static inline int ll_submit(ll_ring *r) {
    unsigned n = ll_sq_pending(r);
    if (!n) return 0;
    if (r->test_fail_submits) { r->test_fail_submits--; return -EBUSY; }
    atomic_store_explicit((_Atomic unsigned *)r->sq_ktail, r->sq_tail, memory_order_release);
    int rc = ll__enter(r, n, 0, LL_ENTER_GETEVENTS, NULL, 0);
    return rc < 0 ? -errno : rc;
}

/* Move spilled SQEs into the submission queue, oldest first. */
static void ll_unspill(ll_ring *r) {
    unsigned moved = 0;
    while (moved < r->spill_n && ll_sq_pending(r) < r->sq_entries)
        r->sqes[r->sq_tail++ & r->sq_mask] = r->spill[moved++];
    if (moved) {
        r->spill_n -= moved;
        memmove(r->spill, r->spill + moved, (size_t)r->spill_n * sizeof *r->spill);
    }
}

/* The submission queue is full (or already spilling). Hand it to the kernel;
 * if the kernel does not take it (it can refuse while completions are
 * waiting to be reaped, or when short of memory), the new SQE goes to a side
 * list that ll_submit_and_wait feeds in, in order. Once one SQE has spilled,
 * the following ones do too, so nothing overtakes it. */
__attribute__((noinline, cold))
static struct ll_sqe *ll_get_sqe_slow(ll_ring *r) {
    struct ll_sqe *s;
    if (r->spill_n == 0) {
        ll_submit(r);
        if (ll_sq_pending(r) < r->sq_entries) {
            s = &r->sqes[r->sq_tail++ & r->sq_mask];
            memset(s, 0, sizeof *s);
            return s;
        }
    }
    if (r->spill_n == r->spill_cap) {
        unsigned cap = r->spill_cap ? r->spill_cap * 2 : 64;
        void *p = realloc(r->spill, (size_t)cap * sizeof *r->spill);
        if (!p) {
            /* Nowhere to keep it. Fail loudly at the next submit instead of
             * running on with a request missing. */
            r->broken = 1;
            memset(&r->sink, 0, sizeof r->sink);
            return &r->sink;
        }
        r->spill = p;
        r->spill_cap = cap;
    }
    s = &r->spill[r->spill_n++];
    r->spilled++;
    memset(s, 0, sizeof *s);
    return s;
}

/* Never fails and never overwrites a queued SQE. The pointer is valid until
 * the next call into this file. */
static inline struct ll_sqe *ll_get_sqe(ll_ring *r) {
    if (__builtin_expect(r->spill_n != 0 || ll_sq_pending(r) >= r->sq_entries, 0))
        return ll_get_sqe_slow(r);
    struct ll_sqe *s = &r->sqes[r->sq_tail++ & r->sq_mask];
    memset(s, 0, sizeof *s);
    return s;
}

/* SQEs the kernel has not taken yet: queued, or waiting in the spill list. */
static inline unsigned ll_unsubmitted(ll_ring *r) {
    return ll_sq_pending(r) + r->spill_n;
}

/* Hand everything queued to the kernel now, the spill list included,
 * without waiting. Returns how many SQEs it did not take (0: all of them).
 * The kernel refuses (and nothing is taken) when called from a thread other
 * than the ring's owner (SINGLE_ISSUER), or while completions overflow. */
static unsigned ll_submit_all(ll_ring *r) {
    while (!r->broken) {
        unsigned before = ll_unsubmitted(r);
        if (before == 0) return 0;
        ll_submit(r);
        ll_unspill(r);
        if (ll_unsubmitted(r) >= before) break;  /* no progress */
    }
    return ll_unsubmitted(r);
}

/* Submit everything queued and wait for at least `wait` completions.
 * timeout_ns < 0: no timeout. Returns 0, 1 if a signal interrupted the wait,
 * or -errno. */
static inline int ll_submit_and_wait(ll_ring *r, unsigned wait, int64_t timeout_ns) {
    if (__builtin_expect(r->broken, 0)) return -ENOMEM;
    if (__builtin_expect(r->spill_n != 0, 0)) {
        /* Feed the side list in and do not sleep: what is still spilled may
         * be the very request whose completion the caller would wait for. */
        int rc = ll_submit(r);
        if (rc < 0 && rc != -EBUSY && rc != -EAGAIN && rc != -EINTR) return rc;
        ll_unspill(r);
        wait = 0;
    }
    unsigned n = ll_sq_pending(r);
    atomic_store_explicit((_Atomic unsigned *)r->sq_ktail, r->sq_tail, memory_order_release);
    int rc;
    if (wait && timeout_ns >= 0) {
        struct ll_timespec ts = {.tv_sec = timeout_ns / 1000000000LL, .tv_nsec = timeout_ns % 1000000000LL};
        struct ll_getevents_arg arg;
        memset(&arg, 0, sizeof arg);
        arg.ts = (uint64_t)(uintptr_t)&ts;
        rc = ll__enter(r, n, wait, LL_ENTER_GETEVENTS | LL_ENTER_EXT_ARG, &arg, sizeof arg);
    } else {
        rc = ll__enter(r, n, wait, LL_ENTER_GETEVENTS, NULL, 0);
    }
    if (rc < 0) {
        if (errno == EINTR) return 1;
        if (errno != ETIME && errno != EBUSY && errno != EAGAIN) return -errno;
    }
    return 0;
}

static inline const char *ll_last_init_step(void) { return ll_init_step; }

static inline struct ll_cqe *ll_cqe_at(ll_ring *r, unsigned head) {
    return &r->cqes[head & r->cq_mask];
}
static inline unsigned ll_cq_head(ll_ring *r) { return *r->cq_khead; }
static inline unsigned ll_cq_tail(ll_ring *r) {
    return atomic_load_explicit((_Atomic unsigned *)r->cq_ktail, memory_order_acquire);
}
static inline void ll_cq_advance(ll_ring *r, unsigned head) {
    atomic_store_explicit((_Atomic unsigned *)r->cq_khead, head, memory_order_release);
}

/* SQE builders. user_data is set by the caller. */
static inline struct ll_sqe *ll_prep_recv_multishot(ll_ring *r, int fd, uint64_t ud) {
    struct ll_sqe *s = ll_get_sqe(r);
    s->opcode = LL_OP_RECV; s->fd = fd; s->ioprio = LL_RECV_MULTISHOT;
    s->flags = LL_IOSQE_BUFFER_SELECT; s->buf_group = LL_BGID; s->user_data = ud;
    return s;
}
static inline struct ll_sqe *ll_prep_send(ll_ring *r, int fd, const char *p, size_t n, uint64_t ud) {
    struct ll_sqe *s = ll_get_sqe(r);
    /* len is 32 bits: a huge buffer goes out in pieces (the completion handler sends the rest). */
    s->opcode = LL_OP_SEND; s->fd = fd; s->addr = (uint64_t)(uintptr_t)p;
    s->len = n > 0x40000000u ? 0x40000000u : (unsigned)n;
    s->op_flags = MSG_NOSIGNAL | MSG_WAITALL; s->user_data = ud;
    return s;
}
/* One connection per request, not multishot. A multishot accept keeps its
 * place at the head of the listening socket's queue of waiters, so of several
 * processes (or threads) that accept on one socket the first would get every
 * connection. A request that is queued again goes to the back of that queue. */
static inline struct ll_sqe *ll_prep_accept(ll_ring *r, int fd, int flags, uint64_t ud) {
    struct ll_sqe *s = ll_get_sqe(r);
    s->opcode = LL_OP_ACCEPT; s->fd = fd; s->op_flags = (unsigned)flags; s->user_data = ud;
    return s;
}
static inline struct ll_sqe *ll_prep_cancel(ll_ring *r, uint64_t target_ud, uint64_t ud) {
    struct ll_sqe *s = ll_get_sqe(r);
    s->opcode = LL_OP_ASYNC_CANCEL; s->fd = -1; s->addr = target_ud; s->user_data = ud;
    return s;
}
/* Cancel every request on one descriptor. */
static inline struct ll_sqe *ll_prep_cancel_fd(ll_ring *r, int fd, uint64_t ud) {
    struct ll_sqe *s = ll_get_sqe(r);
    s->opcode = LL_OP_ASYNC_CANCEL; s->fd = fd; s->user_data = ud;
    s->op_flags = LL_ASYNC_CANCEL_FD | LL_ASYNC_CANCEL_ALL;
    return s;
}
/* Cancel every request of the ring. */
static inline struct ll_sqe *ll_prep_cancel_everything(ll_ring *r, uint64_t ud) {
    struct ll_sqe *s = ll_get_sqe(r);
    s->opcode = LL_OP_ASYNC_CANCEL; s->fd = -1; s->user_data = ud;
    s->op_flags = LL_ASYNC_CANCEL_ANY | LL_ASYNC_CANCEL_ALL;
    return s;
}
static inline struct ll_sqe *ll_prep_poll(ll_ring *r, int fd, unsigned mask, uint64_t ud) {
    struct ll_sqe *s = ll_get_sqe(r);
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
    /* The kernel swaps the half-words of poll32_events on big-endian
     * machines (swahw32), as liburing's io_uring_prep_poll_add does. */
    mask = (mask << 16) | (mask >> 16);
#endif
    s->opcode = LL_OP_POLL_ADD; s->fd = fd; s->op_flags = mask; s->user_data = ud;
    return s;
}
static inline struct ll_sqe *ll_prep_nop(ll_ring *r, uint64_t ud) {
    struct ll_sqe *s = ll_get_sqe(r);
    s->opcode = LL_OP_NOP; s->fd = -1; s->user_data = ud;
    return s;
}

/* epoll_wait with a nanosecond timeout (epoll_pwait2, Linux 5.11) where the
 * kernel and the seccomp policy allow it, else rounded up to milliseconds.
 * ns < 0 waits without limit. *pwait2 starts at 1 and is cleared for good
 * the first time the call is refused. */
static inline int ll_epoll_wait(int epfd, struct epoll_event *evs, int max, int64_t ns, int *pwait2) {
    if (ns <= 0) return epoll_wait(epfd, evs, max, ns < 0 ? -1 : 0);
    if (*pwait2) {
        struct ll_timespec ts = {.tv_sec = ns / 1000000000LL, .tv_nsec = ns % 1000000000LL};
        int n = (int)syscall(__NR_epoll_pwait2, epfd, evs, max, &ts, NULL, (size_t)0);
        if (n >= 0 || errno == EINTR) return n;
        *pwait2 = 0;
    }
    int64_t ms = (ns + 999999) / 1000000;
    return epoll_wait(epfd, evs, max, ms > 0x7fffffff ? 0x7fffffff : (int)ms);
}

#endif
