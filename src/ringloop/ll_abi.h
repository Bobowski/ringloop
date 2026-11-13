/* The part of the Linux io_uring ABI that ringloop uses, written out here.
 *
 * The extension must build wherever Python does, also against kernel headers
 * older than the kernel it will run on: the manylinux images that wheels are
 * built in have headers from 4.18, which know nothing of io_uring, and the
 * backend needs names from 6.1. The kernel's user-space ABI never changes
 * once released, so these definitions are safe to carry. Whether the running
 * kernel has the features is found out at run time: io_uring_setup refuses
 * the flags, and the loop uses epoll.
 *
 * Names carry an ll_/LL_ prefix so that they cannot clash with a system
 * header. tests/abi_check.c compares every one of them with
 * <linux/io_uring.h> on a machine that has a recent one.
 */
#ifndef LL_ABI_H
#define LL_ABI_H

#include <stdint.h>
#include <sys/syscall.h>

/* The same numbers on every architecture Linux has added since 5.1 and on
 * all the older ones except alpha. */
#ifndef __NR_io_uring_setup
#define __NR_io_uring_setup 425
#endif
#ifndef __NR_io_uring_enter
#define __NR_io_uring_enter 426
#endif
#ifndef __NR_io_uring_register
#define __NR_io_uring_register 427
#endif
#ifndef __NR_epoll_pwait2
#define __NR_epoll_pwait2 441
#endif

struct ll_sqe {                 /* struct io_uring_sqe, 64 bytes */
    uint8_t opcode;
    uint8_t flags;              /* LL_IOSQE_* */
    uint16_t ioprio;            /* recv and accept: the multishot flag */
    int32_t fd;
    uint64_t off;
    uint64_t addr;              /* buffer; cancel: the user_data to cancel */
    uint32_t len;
    uint32_t op_flags;          /* msg_flags, accept_flags, poll32_events, cancel_flags */
    uint64_t user_data;
    uint16_t buf_group;
    uint16_t personality;
    int32_t file_index;
    uint64_t addr3;
    uint64_t pad;
};

struct ll_cqe {                 /* struct io_uring_cqe, 16 bytes */
    uint64_t user_data;
    int32_t res;
    uint32_t flags;             /* LL_CQE_F_* */
};

struct ll_sq_offsets {          /* struct io_sqring_offsets */
    uint32_t head, tail, ring_mask, ring_entries, flags, dropped, array, resv1;
    uint64_t user_addr;
};

struct ll_cq_offsets {          /* struct io_cqring_offsets */
    uint32_t head, tail, ring_mask, ring_entries, overflow, cqes, flags, resv1;
    uint64_t user_addr;
};

struct ll_params {              /* struct io_uring_params, 120 bytes */
    uint32_t sq_entries, cq_entries, flags, sq_thread_cpu, sq_thread_idle, features, wq_fd;
    uint32_t resv[3];
    struct ll_sq_offsets sq_off;
    struct ll_cq_offsets cq_off;
};

struct ll_buf {                 /* struct io_uring_buf, 16 bytes */
    uint64_t addr;
    uint32_t len;
    uint16_t bid;
    uint16_t resv;              /* in entry 0: the ring's tail (struct io_uring_buf_ring) */
};

struct ll_buf_reg {             /* struct io_uring_buf_reg, 40 bytes */
    uint64_t ring_addr;
    uint32_t ring_entries;
    uint16_t bgid;
    uint16_t flags;
    uint64_t resv[3];
};

struct ll_getevents_arg {       /* struct io_uring_getevents_arg, 24 bytes */
    uint64_t sigmask;
    uint32_t sigmask_sz;
    uint32_t pad;
    uint64_t ts;
};

struct ll_timespec {            /* struct __kernel_timespec */
    int64_t tv_sec;
    long long tv_nsec;
};

/* mmap offsets */
#define LL_OFF_SQ_RING 0ULL
#define LL_OFF_CQ_RING 0x8000000ULL
#define LL_OFF_SQES 0x10000000ULL

/* io_uring_setup flags, with the kernel that added each */
#define LL_SETUP_CQSIZE (1U << 3)           /* 5.5 */
#define LL_SETUP_CLAMP (1U << 4)            /* 5.6 */
#define LL_SETUP_SUBMIT_ALL (1U << 7)       /* 5.18 */
#define LL_SETUP_SINGLE_ISSUER (1U << 12)   /* 6.0 */
#define LL_SETUP_DEFER_TASKRUN (1U << 13)   /* 6.1 */
#define LL_FEAT_SINGLE_MMAP (1U << 0)

/* io_uring_enter flags */
#define LL_ENTER_GETEVENTS (1U << 0)
#define LL_ENTER_EXT_ARG (1U << 3)          /* 5.11 */

/* io_uring_register opcode */
#define LL_REGISTER_PBUF_RING 22            /* 5.19 */

/* SQE opcodes and their flags */
#define LL_OP_NOP 0
#define LL_OP_POLL_ADD 6
#define LL_OP_ACCEPT 13
#define LL_OP_ASYNC_CANCEL 14
#define LL_OP_SEND 26
#define LL_OP_RECV 27
#define LL_IOSQE_BUFFER_SELECT (1U << 5)
#define LL_RECV_MULTISHOT (1U << 1)         /* in ioprio; 6.0 */
#define LL_ASYNC_CANCEL_ALL (1U << 0)       /* 5.19 */
#define LL_ASYNC_CANCEL_FD (1U << 1)        /* 5.19 */
#define LL_ASYNC_CANCEL_ANY (1U << 2)       /* 5.19 */

/* CQE flags */
#define LL_CQE_F_BUFFER (1U << 0)
#define LL_CQE_F_MORE (1U << 1)
#define LL_CQE_BUFFER_SHIFT 16

_Static_assert(sizeof(struct ll_sqe) == 64, "io_uring_sqe");
_Static_assert(sizeof(struct ll_cqe) == 16, "io_uring_cqe");
_Static_assert(sizeof(struct ll_params) == 120, "io_uring_params");
_Static_assert(sizeof(struct ll_buf) == 16, "io_uring_buf");
_Static_assert(sizeof(struct ll_buf_reg) == 40, "io_uring_buf_reg");
_Static_assert(sizeof(struct ll_getevents_arg) == 24, "io_uring_getevents_arg");
_Static_assert(sizeof(struct ll_timespec) == 16, "__kernel_timespec");

#endif
