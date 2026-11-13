/* Compile-time check of src/ringloop/ll_abi.h against the kernel's own
 * header. Needs <linux/io_uring.h> from Linux 6.1 or newer:
 *
 *     cc -fsyntax-only -Isrc/ringloop tests/abi_check.c
 */
#include <stddef.h>
#include <linux/io_uring.h>
#include <linux/time_types.h>
#include "ll_abi.h"

#define SAME_SIZE(a, b) _Static_assert(sizeof(struct a) == sizeof(struct b), #a)
#define SAME_FIELD(a, fa, b, fb) \
    _Static_assert(offsetof(struct a, fa) == offsetof(struct b, fb) && \
                   sizeof(((struct a *)0)->fa) == sizeof(((struct b *)0)->fb), #a "." #fa)
#define SAME(a, b) _Static_assert((unsigned long long)(a) == (unsigned long long)(b), #a)

SAME_SIZE(ll_sqe, io_uring_sqe);
SAME_FIELD(ll_sqe, opcode, io_uring_sqe, opcode);
SAME_FIELD(ll_sqe, flags, io_uring_sqe, flags);
SAME_FIELD(ll_sqe, ioprio, io_uring_sqe, ioprio);
SAME_FIELD(ll_sqe, fd, io_uring_sqe, fd);
SAME_FIELD(ll_sqe, addr, io_uring_sqe, addr);
SAME_FIELD(ll_sqe, len, io_uring_sqe, len);
SAME_FIELD(ll_sqe, op_flags, io_uring_sqe, msg_flags);
SAME_FIELD(ll_sqe, op_flags, io_uring_sqe, accept_flags);
SAME_FIELD(ll_sqe, op_flags, io_uring_sqe, cancel_flags);
SAME_FIELD(ll_sqe, op_flags, io_uring_sqe, poll32_events);
SAME_FIELD(ll_sqe, user_data, io_uring_sqe, user_data);
SAME_FIELD(ll_sqe, buf_group, io_uring_sqe, buf_group);

SAME_SIZE(ll_cqe, io_uring_cqe);
SAME_FIELD(ll_cqe, user_data, io_uring_cqe, user_data);
SAME_FIELD(ll_cqe, res, io_uring_cqe, res);
SAME_FIELD(ll_cqe, flags, io_uring_cqe, flags);

SAME_SIZE(ll_params, io_uring_params);
SAME_FIELD(ll_params, cq_entries, io_uring_params, cq_entries);
SAME_FIELD(ll_params, flags, io_uring_params, flags);
SAME_FIELD(ll_params, features, io_uring_params, features);
SAME_FIELD(ll_params, sq_off.head, io_uring_params, sq_off.head);
SAME_FIELD(ll_params, sq_off.tail, io_uring_params, sq_off.tail);
SAME_FIELD(ll_params, sq_off.ring_mask, io_uring_params, sq_off.ring_mask);
SAME_FIELD(ll_params, sq_off.array, io_uring_params, sq_off.array);
SAME_FIELD(ll_params, cq_off.head, io_uring_params, cq_off.head);
SAME_FIELD(ll_params, cq_off.tail, io_uring_params, cq_off.tail);
SAME_FIELD(ll_params, cq_off.ring_mask, io_uring_params, cq_off.ring_mask);
SAME_FIELD(ll_params, cq_off.cqes, io_uring_params, cq_off.cqes);

SAME_SIZE(ll_buf, io_uring_buf);
SAME_FIELD(ll_buf, addr, io_uring_buf, addr);
SAME_FIELD(ll_buf, len, io_uring_buf, len);
SAME_FIELD(ll_buf, bid, io_uring_buf, bid);
SAME_FIELD(ll_buf, resv, io_uring_buf_ring, tail);   /* the tail lives in entry 0 */

SAME_SIZE(ll_buf_reg, io_uring_buf_reg);
SAME_FIELD(ll_buf_reg, ring_addr, io_uring_buf_reg, ring_addr);
SAME_FIELD(ll_buf_reg, ring_entries, io_uring_buf_reg, ring_entries);
SAME_FIELD(ll_buf_reg, bgid, io_uring_buf_reg, bgid);

SAME_SIZE(ll_getevents_arg, io_uring_getevents_arg);
SAME_FIELD(ll_getevents_arg, ts, io_uring_getevents_arg, ts);

SAME_SIZE(ll_timespec, __kernel_timespec);
SAME_FIELD(ll_timespec, tv_sec, __kernel_timespec, tv_sec);
SAME_FIELD(ll_timespec, tv_nsec, __kernel_timespec, tv_nsec);

SAME(LL_OFF_SQ_RING, IORING_OFF_SQ_RING);
SAME(LL_OFF_CQ_RING, IORING_OFF_CQ_RING);
SAME(LL_OFF_SQES, IORING_OFF_SQES);
SAME(LL_SETUP_CQSIZE, IORING_SETUP_CQSIZE);
SAME(LL_SETUP_CLAMP, IORING_SETUP_CLAMP);
SAME(LL_SETUP_SUBMIT_ALL, IORING_SETUP_SUBMIT_ALL);
SAME(LL_SETUP_SINGLE_ISSUER, IORING_SETUP_SINGLE_ISSUER);
SAME(LL_SETUP_DEFER_TASKRUN, IORING_SETUP_DEFER_TASKRUN);
SAME(LL_FEAT_SINGLE_MMAP, IORING_FEAT_SINGLE_MMAP);
SAME(LL_ENTER_GETEVENTS, IORING_ENTER_GETEVENTS);
SAME(LL_ENTER_EXT_ARG, IORING_ENTER_EXT_ARG);
SAME(LL_REGISTER_PBUF_RING, IORING_REGISTER_PBUF_RING);
SAME(LL_OP_NOP, IORING_OP_NOP);
SAME(LL_OP_POLL_ADD, IORING_OP_POLL_ADD);
SAME(LL_OP_ACCEPT, IORING_OP_ACCEPT);
SAME(LL_OP_ASYNC_CANCEL, IORING_OP_ASYNC_CANCEL);
SAME(LL_OP_SEND, IORING_OP_SEND);
SAME(LL_OP_RECV, IORING_OP_RECV);
SAME(LL_IOSQE_BUFFER_SELECT, IOSQE_BUFFER_SELECT);
SAME(LL_RECV_MULTISHOT, IORING_RECV_MULTISHOT);
SAME(LL_ASYNC_CANCEL_ALL, IORING_ASYNC_CANCEL_ALL);
SAME(LL_ASYNC_CANCEL_FD, IORING_ASYNC_CANCEL_FD);
SAME(LL_ASYNC_CANCEL_ANY, IORING_ASYNC_CANCEL_ANY);
SAME(LL_CQE_F_BUFFER, IORING_CQE_F_BUFFER);
SAME(LL_CQE_F_MORE, IORING_CQE_F_MORE);
SAME(LL_CQE_BUFFER_SHIFT, IORING_CQE_BUFFER_SHIFT);
SAME(__NR_io_uring_setup, 425);
SAME(__NR_io_uring_enter, 426);
SAME(__NR_io_uring_register, 427);
SAME(__NR_epoll_pwait2, 441);

int main(void) { return 0; }
