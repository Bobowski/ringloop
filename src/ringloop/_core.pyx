# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, initializedcheck=False, freethreading_compatible=True, auto_pickle=False
"""ringloop core: scheduling, the I/O turn, stream transports, accept.

`LoopCore` is the event loop's engine: the ready queue, timers, and one
`Core`. `Core.poll()` is one I/O turn:

1. flush every transport that buffered output since the last turn,
2. one `io_uring_enter` (submit + wait), or one `epoll_wait`,
3. run the completions: accepted sockets, received bytes, finished sends.

`add_reader` / `add_writer` registrations and the wake-up eventfd live in an
epoll set the core owns. Under io_uring that epoll fd is watched with a
one-shot poll, re-armed each turn, which keeps those registrations
level-triggered.

Threads. A loop belongs to one thread at a time: the thread running it, or,
while no thread runs it, the thread that is calling into it. That thread
"owns" the loop (`Core.owner`). Transports, listeners, the timer heap, the
ready queue and the ring are touched only by the owner; a call from any other
thread raises RuntimeError. Scheduling (`call_soon`, `call_soon_threadsafe`,
`call_later`, `call_at`, `stop`) works from every thread: a thread that is not
running the loop posts the handle to a small locked queue that the running
thread empties at the start of each turn. `Handle.cancel()` works from every
thread too (a per-handle critical section). This holds on free-threaded
builds, where the extension runs without the GIL.

Nothing here touches asyncio internals: the loop built on this
(`ringloop.loop.Loop`) implements `asyncio.AbstractEventLoop` itself.
"""

cimport cython
from cpython.buffer cimport (
    PyBUF_SIMPLE, PyBUF_WRITABLE, PyBuffer_Release, PyObject_GetBuffer,
)
from cpython.list cimport PyList_Append, PyList_GET_SIZE, PyList_SET_ITEM
from cpython.ref cimport Py_DECREF, Py_INCREF, PyObject
from cpython.bytes cimport (
    PyBytes_AS_STRING, PyBytes_CheckExact, PyBytes_FromStringAndSize,
    PyBytes_GET_SIZE,
)
from libc.errno cimport (
    EAGAIN, ECONNABORTED, EEXIST, EINTR, EMFILE, ENFILE, ENOBUFS, ENOENT,
    ENOMEM, errno,
)
from libc.stdint cimport int64_t, uint32_t, uint64_t, uintptr_t
from libc.stdlib cimport free, malloc, realloc
from libc.string cimport memcpy, strerror
from posix.types cimport off_t
from posix.unistd cimport getpid

import asyncio
import contextvars
import inspect
import logging
import os
import reprlib
import sys
import traceback
import socket as _socket
from asyncio.trsock import TransportSocket

cdef extern from "<errno.h>" nogil:
    enum: ECANCELED
    enum: EBADR

cdef extern from "Python.h":
    ctypedef struct PyListObject:
        PyObject **ob_item
    int PyContext_Enter(object ctx) except -1
    int PyContext_Exit(object ctx) except -1
    # The same two, on a borrowed pointer (no temporary reference).
    int context_enter "PyContext_Enter"(PyObject *ctx) except -1
    int context_exit "PyContext_Exit"(PyObject *ctx) except -1
    object PyContext_CopyCurrent()
    object PyContext_Copy(object ctx)
    int PyErr_CheckSignals() except -1
    object PyObject_CallNoArgs(object func)
    object PyObject_CallOneArg(object func, object arg)
    object PyObject_Call(object func, object args, void *kwargs)
    Py_ssize_t PyTuple_GET_SIZE(object t)
    PyObject *PyTuple_GET_ITEM(object t, Py_ssize_t i)

cdef extern from "<time.h>" nogil:
    struct timespec:
        long tv_sec
        long tv_nsec
    int clock_gettime(int clk, timespec *ts)
    enum: CLOCK_MONOTONIC

cdef extern from "<sys/socket.h>" nogil:
    ssize_t c_send "send"(int fd, const void *buf, size_t n, int flags)
    ssize_t c_recv "recv"(int fd, void *buf, size_t n, int flags)
    int c_accept4 "accept4"(int fd, void *addr, void *addrlen, int flags)
    int c_shutdown "shutdown"(int fd, int how)
    int c_setsockopt "setsockopt"(int fd, int level, int name, const void *val, unsigned int len)
    int c_getsockopt "getsockopt"(int fd, int level, int name, void *val, unsigned int *len)
    enum:
        MSG_NOSIGNAL
        MSG_DONTWAIT
        SHUT_WR
        SHUT_RDWR
        SOCK_NONBLOCK
        SOCK_CLOEXEC
        SOL_SOCKET
        SO_ERROR

cdef extern from "<netinet/in.h>" nogil:
    enum: IPPROTO_TCP

cdef extern from "<netinet/tcp.h>" nogil:
    enum: TCP_NODELAY

cdef extern from "<unistd.h>" nogil:
    int usleep(unsigned int usec)
    int c_close "close"(int fd)
    ssize_t c_read "read"(int fd, void *buf, size_t n)
    ssize_t c_write "write"(int fd, const void *buf, size_t n)

cdef extern from "<sys/eventfd.h>" nogil:
    int eventfd(unsigned int initval, int flags)
    enum:
        EFD_NONBLOCK
        EFD_CLOEXEC

cdef extern from "pythread.h":
    unsigned long PyThread_get_thread_ident()

cdef extern from "<poll.h>" nogil:
    enum:
        POLLIN
        POLLOUT
        POLLRDHUP
        POLLERR
        POLLHUP

cdef extern from "<sys/sendfile.h>" nogil:
    ssize_t c_sendfile "sendfile"(int out_fd, int in_fd, off_t *offset, size_t count)

cdef extern from "<sys/epoll.h>" nogil:
    ctypedef union epoll_data_t:
        int fd
        uint64_t u64
    struct epoll_event:
        uint32_t events
        epoll_data_t data
    int epoll_create1(int flags)
    int epoll_ctl(int epfd, int op, int fd, epoll_event *ev)
    enum:
        EPOLL_CTL_ADD
        EPOLL_CTL_MOD
        EPOLL_CTL_DEL
        EPOLL_CLOEXEC
        EPOLLIN
        EPOLLOUT
        EPOLLERR
        EPOLLHUP

cdef extern from "ll_sync.h" nogil:
    unsigned long ll_thread_token()
    void ll_add_ulong(unsigned long *p, unsigned long v)
    unsigned long ll_swap_ulong(unsigned long *p, unsigned long v)
    unsigned long ll_load_ulong(unsigned long *p)
    void ll_store_ulong(unsigned long *p, unsigned long v)
    bint ll_cas_ulong(unsigned long *p, unsigned long expect, unsigned long v)
    int ll_load_int(int *p)
    void ll_store_int(int *p, int v)

cdef extern from "ll_uring.h" nogil:
    struct ll_cqe:
        uint64_t user_data
        int res
        unsigned flags
    ctypedef struct ll_ring:
        int fd
        unsigned flags
        unsigned buf_size
        unsigned spill_n
        unsigned long spilled
        unsigned test_fail_submits
    int ll_ring_init(ll_ring *r, unsigned entries, unsigned buf_count, unsigned buf_size)
    void ll_ring_close(ll_ring *r)
    const char *ll_last_init_step()
    int ll_submit(ll_ring *r)
    unsigned ll_submit_all(ll_ring *r)
    unsigned ll_unsubmitted(ll_ring *r)
    int ll_submit_and_wait(ll_ring *r, unsigned wait, int64_t timeout_ns)
    int ll_epoll_wait(int epfd, epoll_event *evs, int maxevents, int64_t ns, int *pwait2)
    void *ll_prep_nop(ll_ring *r, uint64_t ud)
    unsigned ll_cq_head(ll_ring *r)
    unsigned ll_cq_tail(ll_ring *r)
    ll_cqe *ll_cqe_at(ll_ring *r, unsigned head)
    void ll_cq_advance(ll_ring *r, unsigned head)
    void ll_buf_return(ll_ring *r, unsigned bid)
    char *ll_buf_ptr(ll_ring *r, unsigned bid)
    void *ll_prep_recv_multishot(ll_ring *r, int fd, uint64_t ud)
    void *ll_prep_send(ll_ring *r, int fd, const char *p, size_t n, uint64_t ud)
    void *ll_prep_accept(ll_ring *r, int fd, int flags, uint64_t ud)
    void *ll_prep_cancel(ll_ring *r, uint64_t target, uint64_t ud)
    void *ll_prep_cancel_fd(ll_ring *r, int fd, uint64_t ud)
    void *ll_prep_cancel_everything(ll_ring *r, uint64_t ud)
    void *ll_prep_poll(ll_ring *r, int fd, unsigned mask, uint64_t ud)
    enum:
        LL_CQE_F_BUFFER
        LL_CQE_F_MORE
        LL_CQE_BUFFER_SHIFT


# user_data = object pointer | op (objects are at least 8-byte aligned;
# checked once per object, see check_aligned).
DEF OP_IGNORE = 0
DEF OP_RECV = 1
DEF OP_SEND = 2
DEF OP_ACCEPT = 3
DEF OP_EPOLL = 4
DEF OP_WRITABLE = 5
DEF OP_HUP = 6
DEF OP_MASK = 7

# On some kernels (6.17, not 6.1-6.13) ring memory is charged to
# RLIMIT_MEMLOCK (8 MiB by default) unless the process has CAP_IPC_LOCK, and
# a process may run one loop per thread: keep a ring small. 1024 requests and
# 4096 completions take about 140 KiB. Neither size limits a turn: a full
# submission queue is handed to the kernel part-way through (see
# ll_get_sqe), and the kernel keeps completions that do not fit.
DEF RING_ENTRIES = 1024
# Provided buffers (a power of two). 256 x 16 KiB is 4 MiB of address space
# per loop, resident only as far as it is used; the kernel would take more
# (it looks a multi-page buffer ring up page by page from 6.1 on), the
# count is a memory choice. Their ring is 16 bytes per buffer.
DEF RECV_BUFS = 256
DEF RECV_BUF_SIZE = 16384
DEF SCRATCH_SIZE = 262144    # epoll backend read buffer (one per loop)
DEF MAX_EVENTS = 256
DEF OUT_MIN = 1024
DEF OUT_KEEP = 262144        # output buffers above this are freed, not pooled
DEF POOL_MAX = 64            # spare output buffers kept per loop
DEF HIGH_WATER = 65536
DEF MAX_WAIT = 1e6           # seconds; a longer wait is cut into pieces

cdef object BufferedProtocol = asyncio.BufferedProtocol
cdef object copy_context = contextvars.copy_context

# asyncio's wording (its debug mode raises the same error).
cdef str WRONG_THREAD = "Non-thread-safe operation invoked on an event loop other than the current one"


cdef struct OutBuf:
    char *p
    Py_ssize_t len   # bytes filled
    Py_ssize_t off   # bytes already sent
    Py_ssize_t cap


cdef inline void out_free(OutBuf *b) noexcept:
    if b.p != NULL:
        free(b.p)
    b.p = NULL
    b.len = b.off = b.cap = 0


cdef inline int out_append(OutBuf *b, const char *p, Py_ssize_t n) except -1:
    cdef Py_ssize_t need = b.len + n
    cdef Py_ssize_t cap
    cdef char *q
    if need > b.cap:
        cap = b.cap * 2
        if cap < need:
            cap = need
        if cap < OUT_MIN:
            cap = OUT_MIN
        q = <char *>realloc(b.p, cap)
        if q == NULL:
            raise MemoryError()
        b.p = q
        b.cap = cap
    memcpy(b.p + b.len, p, n)
    b.len = need
    return 0


cdef inline uint64_t ud_of(object obj, int op) noexcept:
    return (<uint64_t><uintptr_t><void *>obj) | <uint64_t>op


cdef inline int check_aligned(object obj) except -1:
    """The low bits of a request's user_data carry the operation."""
    if (<uintptr_t><void *>obj) & OP_MASK:
        raise SystemError("ringloop: object is not 8-byte aligned")
    return 0


cdef inline object os_error(int err):
    return OSError(err, strerror(err).decode("utf-8", "replace"))


cdef class Core
cdef class Handle
cdef class LoopCore


cdef inline bint owned(Core core) noexcept:
    """This thread owns the loop: it is running it, or holds it for a call."""
    return ll_load_ulong(&core.owner) == PyThread_get_thread_ident()


class _TransportClass(asyncio.Transport):
    """What `transport.__class__` answers for a stream transport.

    An extension type cannot inherit from `asyncio.Transport` (a Python class
    with its own instance layout), and code written for asyncio checks
    `isinstance(transport, asyncio.Transport)`. `isinstance` consults
    `__class__`, so the transport names this class there. `type(transport)`
    is still the extension type."""

    __slots__ = ()

    def __init__(self, *args, **kwargs):
        raise TypeError("ringloop transports are created by the event loop")


_TransportClass.__name__ = _TransportClass.__qualname__ = "Transport"
_TransportClass.__module__ = "ringloop"


cdef class Transport:
    """Stream transport over one connected socket."""

    cdef object __weakref__
    cdef Core core
    cdef object loop
    cdef object protocol
    cdef object server
    cdef object sock            # socket.socket, or None while we hold a bare fd
    cdef dict extra
    cdef object ctx             # contextvars.Context for protocol callbacks
    cdef object p_get_buffer
    cdef object p_buffer_updated
    cdef object p_data_received
    cdef bint buffered
    cdef int fd                 # -1 once the socket is closed
    cdef int family             # of an accepted socket (from its listener); -1: ask the kernel
    cdef int socktype
    cdef int proto
    cdef bint closing
    cdef bint closed            # connection_lost() has run; the socket closes once no request refers to it
    cdef bint lost_scheduled
    cdef bint reading
    cdef bint started
    cdef bint pause_early       # pause_reading() came before the first read was armed
    cdef bint send_unsent       # uring: the send SQE was queued on a ring that is being replaced
    cdef unsigned long wgen     # bumped by every write()/writelines()/abort: see writelines
    cdef bint recv_armed        # uring: multishot recv outstanding
    cdef bint hup_armed         # uring: the poll for "peer closed or reset" is outstanding
    cdef bint cancel_sent
    cdef bint send_inflight     # uring: send SQE outstanding
    cdef bint pollout           # epoll: waiting for EPOLLOUT
    cdef bint in_epoll          # epoll: fd is registered
    cdef bint dirty
    cdef bint eof_in
    cdef bint eof_out
    cdef bint eof_out_pending
    cdef bint proto_paused
    cdef bint stash_eof
    cdef int stash_err          # uring: a read error that arrived while paused (errno), reported after the stash
    cdef bint sendfile_active   # loop.sendfile() has the write side: write() is refused
    cdef bint wpoll_armed       # uring: a poll for "writable" is outstanding
    cdef bint wwaiter_writable  # wwaiter waits for "writable" (else for "output buffer empty")
    cdef object wwaiter         # Future of loop.sendfile(), see _sendfile_wait
    cdef int ops                # uring: SQEs naming this socket whose final CQE has not arrived
    cdef OutBuf cur             # output being collected
    cdef OutBuf fly             # uring: output owned by the in-flight send
    cdef Py_ssize_t high
    cdef Py_ssize_t low
    cdef list stash             # bytes received while paused

    def __cinit__(self):
        self.fd = -1
        self.family = -1
        self.high = HIGH_WATER
        self.low = HIGH_WATER // 4

    def __dealloc__(self):
        out_free(&self.cur)
        out_free(&self.fly)

    cdef _init(self, Core core, int fd, object sock, object protocol,
               dict extra, object server, object ctx):
        check_aligned(self)
        self.core = core
        self.loop = core.loop
        self.fd = fd
        self.sock = sock
        self.extra = extra
        self.server = server
        self.ctx = ctx
        self._set_protocol(protocol)
        core.live.add(self)
        core.fds[fd] = self
        if server is not None:
            server._attach(self)

    cdef _set_protocol(self, object protocol):
        cdef object old
        with cython.critical_section(self):  # get_protocol() may run on another thread
            old = self.protocol
            self.protocol = protocol
        if isinstance(protocol, BufferedProtocol):
            self.buffered = True
            self.p_get_buffer = protocol.get_buffer
            self.p_buffer_updated = protocol.buffer_updated
            self.p_data_received = None
        else:
            self.buffered = False
            self.p_get_buffer = None
            self.p_buffer_updated = None
            self.p_data_received = protocol.data_received

    # -- start ---------------------------------------------------------------

    cdef _connection_made_now(self):
        """Accept path: call connection_made inline, then start reading."""
        self.protocol.connection_made(self)
        self._start_reading()

    def _start_reading(self):
        if self.started or self.closing:
            return
        self.started = True
        self._arm_hup()
        if self.pause_early:
            return  # paused from connection_made(): resume_reading() arms
        self.reading = True
        self._arm_recv()

    # A multishot recv alone can miss the end of a connection. The kernel
    # keeps it parked after a receive that emptied the socket, and wakes it
    # for the next event; but when data and the peer's close (or reset) both
    # arrive before the loop takes the completions, the two wake-ups count as
    # one. The recv then reports the data, finds the socket empty, and waits
    # for a wake-up that has already happened. TCP hides this for an orderly
    # close (a FIN counts as pending input); a reset, and any close of a Unix
    # socket, would leave the connection open for good. So each transport
    # also has one poll for "peer closed or reset". When it fires, the recv is
    # cancelled and armed again: a new recv reads the socket first and so
    # sees what is there, in order.

    cdef _arm_hup(self):
        cdef Core core = self.core
        if core.uring and not self.hup_armed and not self.closed:
            ll_prep_poll(&core.ring, self.fd, POLLRDHUP, ud_of(self, OP_HUP))
            self.hup_armed = True
            self.ops += 1

    cdef _on_hup(self, int res):
        self.ops -= 1
        self.hup_armed = False
        if self.closed:
            self._maybe_release()
            return
        if res < 0:
            return  # cancelled
        if self.eof_in:
            # The protocol kept the connection after the peer's end of input
            # (eof_received() returned true), so nothing reads it any more.
            self._hup_after_eof(res)
        elif self.recv_armed and not self.cancel_sent:
            # Decided when this run of completions is through (_after_hups):
            # after an orderly TCP close the recv usually reports the end
            # itself, a few completions further on.
            self.core.hups.append(self)

    cdef _hup_after_eof(self, int mask):
        """A half-closed connection that is finished or broken is closed, as
        under epoll (which reports errors and hang-ups whatever is asked
        for): both directions shut down, or a reset. Without this nothing
        would ever notice, and the connection would stay until the protocol
        closed it."""
        cdef int err = 0
        cdef unsigned int errlen = sizeof(err)
        if self.closing or self.lost_scheduled:
            return
        if mask & POLLERR:
            c_getsockopt(self.fd, SOL_SOCKET, SO_ERROR, &err, &errlen)
            self._force_close(os_error(err) if err else ConnectionResetError())
        elif mask & POLLHUP:
            self._force_close(None)
        else:
            # Only the peer's end of input so far: wait for the rest.
            ll_prep_poll(&self.core.ring, self.fd, 0, ud_of(self, OP_HUP))
            self.hup_armed = True
            self.ops += 1

    cdef _after_hup(self):
        if not self.closed:
            self._cancel_recv()

    cdef _arm_recv(self):
        cdef Core core = self.core
        if self.closed or self.eof_in or self.stash_eof or self.stash_err:
            return  # the end of input (or an error) is known, just not delivered yet
        if core.uring:
            if not self.recv_armed:
                ll_prep_recv_multishot(&core.ring, self.fd, ud_of(self, OP_RECV))
                self.recv_armed = True
                self.cancel_sent = False
                self.ops += 1
        else:
            self._epoll_update()

    cdef _epoll_update(self):
        cdef epoll_event ev
        cdef int err
        if self.closed:
            return
        ev.events = 0
        ev.data.u64 = 0
        ev.data.fd = self.fd
        if self.reading and not self.eof_in and not self.closing:
            ev.events |= EPOLLIN
        if self.pollout:
            ev.events |= EPOLLOUT
        if self.in_epoll:
            if epoll_ctl(self.core.epfd, EPOLL_CTL_MOD, self.fd, &ev) == 0 or errno != ENOENT:
                return
        if epoll_ctl(self.core.epfd, EPOLL_CTL_ADD, self.fd, &ev) < 0:
            if errno == EEXIST:
                epoll_ctl(self.core.epfd, EPOLL_CTL_MOD, self.fd, &ev)
            else:
                # ENOSPC (fs.epoll.max_user_watches) or ENOMEM: without a
                # registration the connection would hang in silence.
                err = errno
                if not self.lost_scheduled:
                    self._force_close(os_error(err))
                return
        self.in_epoll = True

    cdef inline void _cancel_recv(self) noexcept:
        """uring: end the multishot recv. Its final completion arms a new one
        if the transport still reads then (see _on_recv)."""
        if self.recv_armed and not self.cancel_sent:
            ll_prep_cancel(&self.core.ring, ud_of(self, OP_RECV), OP_IGNORE)
            self.cancel_sent = True

    cdef _stop_reading(self):
        if self.core.uring:
            self._cancel_recv()
        else:
            self._epoll_update()

    # -- read side -----------------------------------------------------------

    cdef _on_recv(self, int res, unsigned flags):
        """One CQE of the multishot recv."""
        cdef Core core = self.core
        cdef bint more = (flags & LL_CQE_F_MORE) != 0
        cdef unsigned bid
        if not more:
            self.recv_armed = False
            self.ops -= 1
        try:
            if flags & LL_CQE_F_BUFFER:
                bid = flags >> LL_CQE_BUFFER_SHIFT
                try:
                    if res > 0 and not self.closing:
                        # Behind stashed data even if reading has been resumed:
                        # the stash is delivered by a scheduled callback, and
                        # this must not overtake it.
                        if self.reading and self.stash is None:
                            self._deliver(ll_buf_ptr(&core.ring, bid), res)
                        else:
                            self._stash(ll_buf_ptr(&core.ring, bid), res)
                finally:
                    ll_buf_return(&core.ring, bid)
            if res == 0:
                if not self.closing:
                    if self.reading and self.stash is None:
                        self._on_eof()
                    else:
                        self.stash_eof = True
            elif res < 0 and res != -ENOBUFS and res != -ECANCELED and res != -EINTR and res != -EAGAIN:
                if self.lost_scheduled:
                    pass
                elif not self.closing and (not self.reading or self.stash is not None):
                    # Paused (the recv stays armed through a pause): the
                    # error waits behind the input that came before it, as
                    # the end of input does. A reset must not cost the
                    # protocol what the peer sent first.
                    self.stash_err = -res
                else:
                    self._fatal_error(os_error(-res), "Fatal read error on socket transport")
        finally:
            # Also when a callback raised (Ctrl-C): a recv that has ended must
            # be armed again, or released.
            if not more:
                if self.closed:
                    self._maybe_release()
                elif self.reading and not self.closing:
                    self._arm_recv()

    cdef _stash(self, const char *p, Py_ssize_t n):
        if self.stash is None:
            self.stash = []
        self.stash.append(PyBytes_FromStringAndSize(p, n))
        # Data arrived while paused: now stop the kernel side too, so TCP
        # backpressure reaches the peer.
        if not self.reading:
            self._stop_reading()

    cdef _deliver(self, const char *p, Py_ssize_t n):
        cdef Py_buffer view
        cdef Py_ssize_t k
        cdef object buf
        try:
            if self.buffered:
                while n > 0:
                    if not self.buffered:
                        # buffer_updated() handed the connection to another
                        # protocol: the rest of the chunk is its input.
                        self.p_data_received(PyBytes_FromStringAndSize(p, n))
                        break
                    buf = self.p_get_buffer(-1)
                    PyObject_GetBuffer(buf, &view, PyBUF_WRITABLE)
                    k = view.len if view.len < n else n
                    if k > 0:
                        memcpy(view.buf, p, k)
                    PyBuffer_Release(&view)
                    if k == 0:
                        raise RuntimeError("get_buffer() returned an empty buffer")
                    self.p_buffer_updated(k)
                    p += k
                    n -= k
                    if n > 0 and (self.closing or not self.reading):
                        if not self.closing:
                            self._stash(p, n)
                        break
            else:
                self.p_data_received(PyBytes_FromStringAndSize(p, n))
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            self._fatal_error(exc, "Fatal error: protocol.data_received() call failed.")

    cdef _on_eof(self):
        cdef object keep_open
        if self.eof_in:
            return
        self.eof_in = True
        try:
            keep_open = self.protocol.eof_received()
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            self._fatal_error(exc, "Fatal error: protocol.eof_received() call failed.")
            return
        if keep_open:
            # Half-closed: keep the write side, stop watching for input.
            if not self.core.uring:
                self._epoll_update()
            elif not self.hup_armed and not self.closing:
                self._hup_after_eof(0)  # watch for the end of the other half
        else:
            self.close()

    def _resume_stash(self):
        cdef list chunks
        cdef bytes chunk
        cdef Py_ssize_t i
        if self.closing or not self.reading:
            return
        chunks = self.stash
        self.stash = None
        if chunks is not None:
            for i in range(len(chunks)):
                chunk = chunks[i]
                try:
                    self._deliver(PyBytes_AS_STRING(chunk), PyBytes_GET_SIZE(chunk))
                except BaseException:
                    # Ctrl-C in the protocol: the rest is still to deliver if
                    # the program runs the loop again.
                    rest = chunks[i + 1:]
                    if self.stash is not None:
                        rest = self.stash + rest
                    self.stash = rest if rest else None
                    if not self.closing:
                        self.loop.call_soon(self._resume_stash, context=self.ctx)
                    raise
                if self.closing:
                    return
                if not self.reading:
                    # Paused again mid-way: keep the rest, in order.
                    rest = chunks[i + 1:]
                    if self.stash is not None:
                        rest = self.stash + rest
                    self.stash = rest if rest else None
                    return
        if self.stash_eof:
            self._on_eof()
            self.stash_eof = False
            return
        if self.stash_err:
            err = self.stash_err
            self.stash_err = 0
            self._fatal_error(os_error(err), "Fatal read error on socket transport")
            return
        self._arm_recv()

    def pause_reading(self):
        if not owned(self.core):
            return self.core._claimed_call(Transport.pause_reading, (self,))
        if not self.started:
            self.pause_early = True
            return
        if not self.reading:
            return
        self.reading = False
        # io_uring: leave the multishot recv armed. A pause that ends within
        # the same turn then costs nothing; data that does arrive while paused
        # is stashed and stops the recv (see _stash).
        if not self.core.uring:
            self._epoll_update()

    def resume_reading(self):
        if not owned(self.core):
            return self.core._claimed_call(Transport.resume_reading, (self,))
        if not self.started:
            self.pause_early = False
            return
        if self.reading or self.closing:
            return
        self.reading = True
        if self.stash is not None or self.stash_eof or self.stash_err:
            self.loop.call_soon(self._resume_stash, context=self.ctx)
        else:
            self._arm_recv()

    def is_reading(self):
        if not self.started:
            return not self.pause_early and not self.closing
        return self.reading and not self.closing

    @property
    def _paused(self):
        """True between pause_reading() and resume_reading() (the name
        asyncio's transports use)."""
        return (not self.reading) if self.started else self.pause_early

    @property
    def _protocol_paused(self):
        """True between pause_writing() and resume_writing() (as asyncio's
        transports have it; `loop.sendfile` reads it)."""
        return self.proto_paused

    # -- write side ----------------------------------------------------------

    cdef inline Py_ssize_t _buffered(self) noexcept:
        return (self.cur.len - self.cur.off) + (self.fly.len - self.fly.off)

    cdef inline int _append(self, object data) except -1:
        """Copy `data` into the output buffer. The caller has made sure the
        transport holds a buffer if the pool had one (`_take`)."""
        cdef Py_buffer view
        if PyBytes_CheckExact(data):
            out_append(&self.cur, PyBytes_AS_STRING(data), PyBytes_GET_SIZE(data))
            return 0
        PyObject_GetBuffer(data, &view, PyBUF_SIMPLE)
        try:
            out_append(&self.cur, <const char *>view.buf, view.len)
        finally:
            PyBuffer_Release(&view)
        return 0

    cdef bint _write_refused(self) except -1:
        if self.sendfile_active:
            raise RuntimeError("unable to write; sendfile is in progress")
        if self.eof_out or self.eof_out_pending:
            raise RuntimeError("Cannot call write() after write_eof()")
        if self.closing or self.lost_scheduled:
            return True
        return False

    def write(self, data):
        if not owned(self.core):
            return self.core._claimed_call(Transport.write, (self, data))
        if self._write_refused():
            return
        self.wgen += 1
        if self.cur.p == NULL:
            self.core._take(&self.cur)
        try:
            self._append(data)
        finally:
            # Also after a TypeError: an idle transport gives the buffer back.
            self._after_write()

    def writelines(self, list_of_data):
        cdef object data
        cdef Py_ssize_t mark
        cdef unsigned long gen
        cdef bint complete = False
        if not owned(self.core):
            return self.core._claimed_call(Transport.writelines, (self, list_of_data))
        if self._write_refused():
            return
        self.wgen += 1
        gen = self.wgen
        if self.cur.p == NULL:
            self.core._take(&self.cur)
        mark = self.cur.len
        # try/finally with a flag, not try/except: Cython's except saves and
        # restores the exception state on every call, which showed up as
        # ~2% of a request on this path.
        try:
            for data in list_of_data:
                self._append(data)
            complete = True
        finally:
            if not complete and self.wgen == gen and self.cur.off <= mark <= self.cur.len:
                # All or nothing: drop the pieces already copied. Only if
                # nothing else touched the output meanwhile: an iterator
                # that itself writes to (or aborts) this transport may have
                # sent or replaced the buffer, and then `mark` means nothing.
                self.cur.len = mark
            self._after_write()

    cdef _after_write(self):
        cdef ssize_t n
        if self.cur.len == self.cur.off:
            # Nothing was written (empty data): an idle transport holds no
            # buffer, so the one just taken goes back.
            if self.cur.len == 0 and not self.dirty:
                self.core._give(&self.cur)
            return
        if not self.dirty:
            self.dirty = True
            self.core.dirty.append(self)
        if self._buffered() > self.high:
            # More than the high-water mark is waiting for the end of the
            # turn: try the socket now, as a plain send, before pausing the
            # protocol for no reason.
            if not self.send_inflight and not self.pollout:
                n = c_send(self.fd, self.cur.p + self.cur.off,
                           self.cur.len - self.cur.off, MSG_NOSIGNAL | MSG_DONTWAIT)
                if n > 0:
                    self.cur.off += n
                    if self.cur.off == self.cur.len:
                        self.core._give(&self.cur)
                    if self.proto_paused:
                        # An earlier write paused the protocol; this send may
                        # have taken the buffer below the low-water mark, and
                        # the flush at the end of the turn finds nothing to
                        # send and would say nothing.
                        self._drained()
                elif n < 0 and errno != EAGAIN and errno != EINTR:
                    self._fatal_error(os_error(errno), "Fatal write error on socket transport")
                    return
            self._maybe_pause_protocol()

    cdef _maybe_pause_protocol(self):
        if self._buffered() <= self.high or self.proto_paused:
            return
        self.proto_paused = True
        try:
            self.protocol.pause_writing()
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            self.loop.call_exception_handler({
                "message": "protocol.pause_writing() failed",
                "exception": exc,
                "transport": self,
                "protocol": self.protocol,
            })

    cdef _flush(self):
        """Hand buffered output to the kernel (called once per turn)."""
        cdef Core core = self.core
        cdef OutBuf tmp
        cdef ssize_t n
        self.dirty = False
        if self.closed or self.cur.len == self.cur.off:
            return
        if core.uring:
            if self.send_inflight:
                return  # the send's completion flushes the rest
            tmp = self.fly
            self.fly = self.cur
            self.cur = tmp
            ll_prep_send(&core.ring, self.fd, self.fly.p + self.fly.off,
                          self.fly.len - self.fly.off, ud_of(self, OP_SEND))
            self.send_inflight = True
            self.send_unsent = core.moving
            self.ops += 1
        else:
            if self.pollout:
                return
            n = c_send(self.fd, self.cur.p + self.cur.off,
                       self.cur.len - self.cur.off, MSG_NOSIGNAL | MSG_DONTWAIT)
            if n < 0:
                if errno != EAGAIN and errno != EINTR:
                    self._fatal_error(os_error(errno), "Fatal write error on socket transport")
                    return
                n = 0
            self.cur.off += n
            if self.cur.off < self.cur.len:
                self.pollout = True
                self._epoll_update()
            else:
                self.core._give(&self.cur)
                self._drained()

    cdef _on_send(self, int res):
        cdef Core core = self.core
        self.ops -= 1
        self.send_inflight = False
        if self.closed:
            self.core._give(&self.fly)
            self._maybe_release()
            return
        if res < 0:
            if res == -EAGAIN or res == -EINTR:
                res = 0
            else:
                self.core._give(&self.fly)
                if not self.lost_scheduled:
                    self._fatal_error(os_error(-res), "Fatal write error on socket transport")
                return
        self.fly.off += res
        if self.fly.off < self.fly.len:
            ll_prep_send(&core.ring, self.fd, self.fly.p + self.fly.off,
                          self.fly.len - self.fly.off, ud_of(self, OP_SEND))
            self.send_inflight = True
            self.send_unsent = core.moving
            self.ops += 1
            return
        self.core._give(&self.fly)
        if self.cur.len > self.cur.off:
            self._flush()
        self._drained()

    cdef _on_epoll(self, uint32_t events):
        cdef Core core = self.core
        cdef ssize_t n
        cdef int err
        cdef unsigned int errlen
        if self.closed:
            return
        if events & EPOLLOUT and self.pollout:
            self.pollout = False
            self._flush()
            if not self.pollout:
                self._epoll_update()
                if self.wwaiter is not None and self.wwaiter_writable:
                    self._wake_wwaiter()
            if self.closed:
                return
        if events & (EPOLLIN | EPOLLERR | EPOLLHUP):
            if not self.reading or self.closing or self.eof_in or self.stash is not None:
                # Nothing to read for now (paused, or resumed with stashed
                # input that a scheduled callback delivers first), but epoll
                # reports ERR/HUP whatever the interest mask is.
                if self.lost_scheduled:
                    return
                if self.closing or self.eof_in:
                    # Nothing more will be read: an error or the end of the
                    # other half ends the connection.
                    if events & EPOLLERR:
                        err = 0
                        errlen = sizeof(err)
                        c_getsockopt(self.fd, SOL_SOCKET, SO_ERROR, &err, &errlen)
                        if self.closing:
                            self._force_close(None)
                        else:
                            self._force_close(os_error(err) if err else ConnectionResetError())
                    elif events & EPOLLHUP:
                        self._force_close(None)
                elif events & (EPOLLERR | EPOLLHUP):
                    # Paused, perhaps with unread input in the socket and a
                    # reset behind it: park the fd until resume_reading()
                    # registers it again. The read then meets what is there,
                    # in order (input first, then the error), as under
                    # asyncio.
                    epoll_ctl(core.epfd, EPOLL_CTL_DEL, self.fd, NULL)
                    self.in_epoll = False
                    if self.wwaiter is not None and self.wwaiter_writable:
                        # sendfile waits for "writable": let it meet the
                        # error itself.
                        self.pollout = False
                        self._wake_wwaiter()
                return
            # MSG_DONTWAIT: never block the loop, even if someone made the
            # socket blocking behind the transport's back.
            n = c_recv(self.fd, core.scratch, SCRATCH_SIZE, MSG_DONTWAIT)
            if n > 0:
                self._deliver(core.scratch, n)
            elif n == 0:
                self._on_eof()
            elif errno != EAGAIN and errno != EINTR:
                self._fatal_error(os_error(errno), "Fatal read error on socket transport")

    cdef _drained(self):
        """Some output left the buffer: finish a close, resume the protocol."""
        cdef bint resume = self.proto_paused and self._buffered() <= self.low
        # What a close or write_eof() waits for comes first: it only
        # schedules, so resume_writing() below is still the first call the
        # protocol sees, and an exception from it (Ctrl-C) loses nothing.
        if self._buffered() == 0:
            if self.wwaiter is not None and not self.wwaiter_writable:
                self._wake_wwaiter()
            if self.closing:
                self._schedule_lost(None)
            elif self.eof_out_pending:
                self.eof_out_pending = False
                self.eof_out = True
                c_shutdown(self.fd, SHUT_WR)
        if resume:
            self.proto_paused = False
            try:
                self.protocol.resume_writing()
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException as exc:
                self.loop.call_exception_handler({
                    "message": "protocol.resume_writing() failed",
                    "exception": exc,
                    "transport": self,
                    "protocol": self.protocol,
                })

    # -- sendfile -------------------------------------------------------------
    #
    # `loop.sendfile()` (extras.py) drives these: it takes the write side
    # (write() raises meanwhile), waits until buffered output has gone, then
    # calls sendfile(2) on the socket until the file is through, waiting for
    # the socket to be writable in between.

    def _sendfile_begin(self):
        if not owned(self.core):
            return self.core._claimed_call(Transport._sendfile_begin, (self,))
        if self.sendfile_active:
            raise RuntimeError("sendfile is in progress")
        if self.closing or self.eof_out or self.eof_out_pending:
            raise RuntimeError("Transport is closing")
        self.sendfile_active = True

    def _sendfile_end(self):
        cdef object waiter
        if not owned(self.core):
            return self.core._claimed_call(Transport._sendfile_end, (self,))
        self.sendfile_active = False
        waiter, self.wwaiter = self.wwaiter, None
        if waiter is not None and not waiter.done():
            waiter.cancel()
        # A poll still armed under io_uring completes (or is cancelled at
        # close) and finds no waiter. Under epoll, stop asking for EPOLLOUT.
        if not self.core.uring and self.pollout and self.cur.len == self.cur.off and not self.closed:
            self.pollout = False
            self._epoll_update()

    def _sendfile_wait(self, waiter, bint writable):
        """Resolve `waiter` when the output buffer is empty (`writable`
        false) or when the socket can take more (true). If the connection is
        lost first, the waiter gets the error."""
        cdef Core core = self.core
        if not owned(core):
            return core._claimed_call(Transport._sendfile_wait, (self, waiter, writable))
        if self.closing or self.lost_scheduled:
            waiter.set_exception(ConnectionError("Connection closed by peer"))
            return
        if not writable and self._buffered() == 0:
            waiter.set_result(None)
            return
        self.wwaiter = waiter
        self.wwaiter_writable = writable
        if not writable:
            return  # _drained() wakes it
        if core.uring:
            if not self.wpoll_armed:
                ll_prep_poll(&core.ring, self.fd, POLLOUT, ud_of(self, OP_WRITABLE))
                self.wpoll_armed = True
                self.ops += 1
        else:
            self.pollout = True
            self._epoll_update()

    def _sendfile_send(self, int in_fd, offset, count):
        """One sendfile(2) call on the socket. Returns the bytes sent (0 at
        the end of the file), or -1 if the socket takes no more for now."""
        cdef off_t off = offset
        cdef size_t n = 0x7ffff000   # what one sendfile(2) call moves at most
        cdef ssize_t sent
        cdef int fd = self.fd
        cdef int err
        if not owned(self.core):
            return self.core._claimed_call(Transport._sendfile_send, (self, in_fd, offset, count))
        if self.closing or fd < 0:
            raise ConnectionError("Connection closed by peer")
        if self._buffered():
            raise RuntimeError("sendfile: the output buffer is not empty")
        if count < n:
            n = count
        with nogil:  # reading the file can block on the disk
            sent = c_sendfile(fd, in_fd, &off, n)
            err = errno
        if sent < 0:
            if err == EAGAIN or err == EINTR:
                return -1
            raise os_error(err)
        return sent

    cdef _wake_wwaiter(self):
        cdef object waiter
        waiter, self.wwaiter = self.wwaiter, None
        if waiter is not None and not waiter.done():
            waiter.set_result(None)

    cdef _on_writable(self):
        """uring: the poll for "writable" completed (also on error or hang-up:
        the next sendfile call reports it)."""
        self.ops -= 1
        self.wpoll_armed = False
        if self.closed:
            self._maybe_release()
        elif self.wwaiter is not None and self.wwaiter_writable:
            self._wake_wwaiter()

    def write_eof(self):
        if not owned(self.core):
            return self.core._claimed_call(Transport.write_eof, (self,))
        if self.closing or self.eof_out or self.eof_out_pending:
            return
        if self._buffered() == 0:
            self.eof_out = True
            c_shutdown(self.fd, SHUT_WR)
        else:
            self.eof_out_pending = True

    def can_write_eof(self):
        return True

    def get_write_buffer_size(self):
        return self._buffered()

    def get_write_buffer_limits(self):
        return (self.low, self.high)

    def set_write_buffer_limits(self, high=None, low=None):
        if not owned(self.core):
            return self.core._claimed_call(Transport.set_write_buffer_limits, (self, high, low))
        if high is None:
            high = HIGH_WATER if low is None else 4 * low
        if low is None:
            low = high // 4
        if not high >= low >= 0:
            raise ValueError(f"high ({high!r}) must be >= low ({low!r}) must be >= 0")
        self.high = high
        self.low = low
        # As asyncio: the new mark may already be exceeded.
        if not self.closing:
            self._maybe_pause_protocol()

    # -- close ---------------------------------------------------------------

    def is_closing(self):
        return self.closing

    def close(self):
        if not owned(self.core):
            # Not an error, unlike the other calls from a wrong thread: the
            # garbage collector runs finalizers (StreamWriter.__del__ and its
            # like) on whichever thread it happens to be, and a refused
            # close() would leave the connection open for good.
            return self.core.run_owned_or_schedule(self.close)
        if self.closing:
            return
        self.closing = True
        if self._buffered() == 0:
            self._schedule_lost(None)  # which cancels what is outstanding
        else:
            self._stop_reading()       # no more input while the output drains

    def abort(self):
        if not owned(self.core):
            return self.core.run_owned_or_schedule(self.abort)
        self._force_close(None)

    def _force_close(self, exc):
        if not owned(self.core):
            return self.core._claimed_call(Transport._force_close, (self, exc))
        if self.lost_scheduled:
            return
        self.cur.len = self.cur.off = 0
        self.wgen += 1
        self.closing = True
        self._schedule_lost(exc)

    cdef _fatal_error(self, exc, message):
        if not isinstance(exc, OSError):
            self.loop.call_exception_handler({
                "message": message,
                "exception": exc,
                "transport": self,
                "protocol": self.protocol,
            })
        self._force_close(exc)

    cdef _schedule_lost(self, exc):
        if self.lost_scheduled:
            return
        self.lost_scheduled = True
        self.loop.call_soon(self._call_connection_lost, exc, context=self.ctx)

    def _call_connection_lost(self, exc):
        cdef Core core = self.core
        try:
            if self.protocol is not None:
                self.protocol.connection_lost(exc)
        finally:
            if core.uring and not core.closed:
                # Cancel what is outstanding on the socket (the recv, the
                # polls, a send), with one request. The socket itself is closed
                # once the last of those requests has completed (see
                # _maybe_release): until then a queued request may still name
                # its descriptor, and a number closed too early can be handed
                # to another connection or file.
                if self.ops > 0:
                    ll_prep_cancel_fd(&core.ring, self.fd, OP_IGNORE)
                    self.cancel_sent = True
            self.closed = True
            self.sendfile_active = False
            waiter, self.wwaiter = self.wwaiter, None
            if waiter is not None and not waiter.done():
                waiter.set_exception(exc if exc is not None else ConnectionError("Connection closed by peer"))
            waiter = None
            with cython.critical_section(self):
                protocol = self.protocol
                self.protocol = None
            protocol = None
            self.p_get_buffer = self.p_buffer_updated = self.p_data_received = None
            self.stash = None
            server = self.server
            if server is not None:
                self.server = None
                server._detach(self)
            self._maybe_release()

    cdef _maybe_release(self):
        if self.closed and (self.ops == 0 or self.core.closed):
            self._close_fd()
            self.core.live.discard(self)

    cdef _close_fd(self):
        cdef Core core = self.core
        cdef int fd = self.fd
        cdef object sock
        if fd < 0:
            return
        with cython.critical_section(self):  # get_extra_info() may run on another thread
            sock = self.sock
            self.sock = None
            self.fd = -1
        if core.fds.get(fd) is self:
            del core.fds[fd]
        if self.in_epoll:
            # Explicitly: a duplicate of the descriptor would otherwise keep
            # the registration (and its events) alive under this number.
            self.in_epoll = False
            if core.epfd >= 0:
                epoll_ctl(core.epfd, EPOLL_CTL_DEL, fd, NULL)
        # This runs inside completion handlers too: nothing may escape into
        # the loop. Closing a socket can only fail if someone closed the
        # descriptor behind the transport's back (EBADF).
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass
        else:
            c_close(fd)

    # -- info ----------------------------------------------------------------

    # get_extra_info() and get_protocol() may be called from any thread. The
    # fields they read (protocol, sock, fd, extra) are written under the
    # transport's critical section. A critical section is given up whenever
    # the thread blocks, so nothing that can block (a system call, such as
    # creating a socket object) runs inside one.

    cdef object _socket_obj(self):
        """The socket object over our descriptor, created on first use."""
        cdef int fd
        cdef object sock, existing
        with cython.critical_section(self):
            if self.sock is not None or self.fd < 0:
                return self.sock
            fd = self.fd
        try:
            # System calls: outside the section. With the family known (an
            # accepted connection) the socket module does not have to ask
            # the kernel for it, three calls less.
            if self.family >= 0:
                sock = _socket.socket(self.family, self.socktype, self.proto, fileno=fd)
            else:
                sock = _socket.socket(fileno=fd)
        except OSError:
            return None  # the transport closed the descriptor meanwhile
        with cython.critical_section(self):
            existing = self.sock
            if existing is None and self.fd == fd:
                # From here on the socket object owns the descriptor.
                self.sock = sock
                return sock
        # Another thread was first, or the transport closed meanwhile. Ours
        # must not close the descriptor when it is collected.
        sock.detach()
        return existing

    def get_extra_info(self, name, default=None):
        cdef object sock, value
        with cython.critical_section(self):
            if self.extra is not None and name in self.extra:
                return self.extra[name]
        if name == "socket":
            sock = self._socket_obj()
            if sock is None:
                return default
            # As asyncio: a view that cannot close or read from the socket
            # behind the transport's back.
            value = TransportSocket(sock)
        elif name == "peername" or name == "sockname":
            sock = self._socket_obj()
            if sock is None:
                return default
            try:
                value = sock.getpeername() if name == "peername" else sock.getsockname()
            except OSError:
                return default  # closed meanwhile
        else:
            return default
        with cython.critical_section(self):
            if self.extra is None:
                self.extra = {}
            return self.extra.setdefault(name, value)

    @property
    def __class__(self):
        return _TransportClass  # isinstance(transport, asyncio.Transport): see there

    def get_protocol(self):
        with cython.critical_section(self):
            return self.protocol

    def set_protocol(self, protocol):
        if not owned(self.core):
            return self.core._claimed_call(Transport.set_protocol, (self, protocol))
        self._set_protocol(protocol)

    @property
    def _start_tls_compatible(self):
        return True

    @property
    def _sendfile_compatible(self):
        return True

    def __repr__(self):
        state = "closed" if self.closed else "closing" if self.closing else "open"
        return f"<ringloop.Transport fd={self.fd} {state}>"


cdef class Listener:
    """One listening socket."""

    cdef Core core
    cdef object factory
    cdef object sock
    cdef object sslcontext
    cdef object server
    cdef object ctx
    cdef object handshake_timeout
    cdef object shutdown_timeout
    cdef int fd
    cdef int backlog
    cdef int family             # what accepted sockets are, for their socket objects
    cdef int socktype
    cdef int proto
    cdef bint armed             # an accept is outstanding (uring) / registered (epoll)
    cdef bint paused            # backing off after running out of descriptors
    cdef bint closed
    cdef bint fd_held           # closed, but the descriptor is ours to close later

    cdef _arm(self):
        cdef Core core = self.core
        cdef epoll_event ev
        if self.closed or self.armed or self.paused:
            return
        self.armed = True
        if core.uring:
            ll_prep_accept(&core.ring, self.fd, SOCK_NONBLOCK | SOCK_CLOEXEC,
                           ud_of(self, OP_ACCEPT))
        else:
            ev.events = EPOLLIN
            ev.data.u64 = 0
            ev.data.fd = self.fd
            if epoll_ctl(core.epfd, EPOLL_CTL_ADD, self.fd, &ev) < 0:
                self.armed = False
                raise os_error(errno)

    def _rearm(self):
        self.paused = False
        try:
            self._arm()
        except OSError as exc:
            # No epoll registration to be had either: try again later, or
            # the server would never accept again.
            self.paused = True
            self.core.loop.call_exception_handler({
                "message": "could not start accepting again", "exception": exc, "socket": self.sock})
            self.core.loop.call_later(1.0, self._rearm)

    cdef _accept_error(self, int err):
        cdef Core core = self.core
        if err in (EAGAIN, EINTR, ECONNABORTED, ECANCELED):
            return
        core.loop.call_exception_handler({
            "message": "socket.accept() out of system resource" if err in (EMFILE, ENFILE, ENOBUFS, ENOMEM) else "socket.accept() failed",
            "exception": os_error(err),
            "socket": self.sock,
        })
        if err in (EMFILE, ENFILE, ENOBUFS, ENOMEM) and not self.paused:
            # Same policy as asyncio: stop accepting for a second.
            self.paused = True
            if not core.uring and self.armed:
                epoll_ctl(core.epfd, EPOLL_CTL_DEL, self.fd, NULL)
                self.armed = False
            core.loop.call_later(1.0, self._rearm)

    cdef _on_accept_cqe(self, int res, unsigned flags):
        cdef bint more = (flags & LL_CQE_F_MORE) != 0
        if not more:
            self.armed = False
        try:
            if res >= 0:
                if self.closed:
                    c_close(res)
                else:
                    self.core._accepted(self, res)
            else:
                self._accept_error(-res)
        finally:
            # Also when a callback raised (Ctrl-C): an accept that has ended
            # must be armed again, or the listener released.
            if self.closed:
                if not self.armed:
                    self._release()
            elif not more:
                self._arm()  # unless backing off (paused)

    cdef _on_epoll(self):
        cdef int i, fd
        for i in range(self.backlog if self.backlog > 0 else 1):
            fd = c_accept4(self.fd, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC)
            if fd < 0:
                self._accept_error(errno)
                return
            self.core._accepted(self, fd)
            if self.closed:
                return

    cdef _close(self):
        """Stop accepting and close the listening socket."""
        cdef Core core = self.core
        cdef object sock = self.sock
        if self.closed:
            return
        self.closed = True
        self.factory = None
        self.server = None
        if core.fds.get(self.fd) is self:
            del core.fds[self.fd]
        if core.uring and self.armed and not core.closed:
            ll_prep_cancel(&core.ring, ud_of(self, OP_ACCEPT), OP_IGNORE)
            # The armed accept holds the listening socket open: hand the
            # cancel (and everything queued before it) to the kernel now, so
            # the address is free as soon as the socket is closed.
            if ll_submit_all(&core.ring) == 0:
                sock.close()
            else:
                # The kernel has not taken every queued request (this is not
                # the ring's thread, or completions overflowed), and one may
                # name this descriptor: keep it open until the accept's final
                # completion, or until the ring goes.
                sock.detach()
                self.fd_held = True
            return  # released at the accept's final completion
        if not core.uring and self.armed and core.epfd >= 0:
            epoll_ctl(core.epfd, EPOLL_CTL_DEL, self.fd, NULL)
        self.armed = False
        sock.close()
        self._release()

    cdef _release(self):
        """No request names the descriptor any more: forget the listener."""
        if self.fd_held:
            self.fd_held = False
            c_close(self.fd)
        self.sock = None
        self.core.live.discard(self)


cdef inline double monotonic() noexcept nogil:
    cdef timespec ts
    clock_gettime(CLOCK_MONOTONIC, &ts)
    return <double>ts.tv_sec + <double>ts.tv_nsec * 1e-9


cdef int uring_available = -1


cdef bint _uring_available():
    """Whether this process can create a ring at all (asked once)."""
    global uring_available
    cdef ll_ring probe
    if uring_available < 0:
        if ll_ring_init(&probe, 8, 8, 4096) == 0:
            ll_ring_close(&probe)
            uring_available = 1
        else:
            uring_available = 0
    return uring_available == 1


def uring_probe():
    """Try to set up a ring like a loop's, and drop it again. Returns None
    if that works, else `(errno, step)`: why loops on this system (as this
    user, with these limits) use epoll."""
    cdef ll_ring probe
    cdef int rc = ll_ring_init(&probe, RING_ENTRIES, RECV_BUFS, RECV_BUF_SIZE)
    if rc == 0:
        ll_ring_close(&probe)
        return None
    return -rc, ll_last_init_step().decode()


cdef bint warned_no_ring_memory = False


cdef _warn_no_ring_memory():
    global warned_no_ring_memory
    if warned_no_ring_memory:
        return
    warned_no_ring_memory = True
    logging.getLogger("ringloop").warning(
        "io_uring_setup failed with ENOMEM (on many kernels ring memory counts against "
        "RLIMIT_MEMLOCK unless the process has CAP_IPC_LOCK); this loop uses epoll instead")


def _made_then_read(Transport t, waiter):
    if not waiter.done():
        waiter.set_result(None)  # schedules the awaiting task's step
    t.loop.call_soon(t._start_reading, context=t.ctx)  # behind that step


cdef class Core:
    cdef ll_ring ring
    cdef readonly bint uring
    cdef int ready             # backend chosen and set up (read atomically by other threads)
    cdef bint closed
    cdef unsigned long owner   # thread that owns the loop now (0: none); see owned()
    cdef int epfd              # add_reader/add_writer fds, wake fd; epoll backend: everything
    cdef int wakefd
    cdef readonly object loop
    cdef dict watch            # fd -> [reader Handle | None, writer Handle | None]
    cdef list dirty
    cdef list hups             # transports whose "peer closed" poll fired in this run of completions
    cdef set live
    cdef dict fds              # fd -> Transport | Listener holding it
    cdef bint epoll_armed
    cdef bint epoll_ready
    cdef int pwait2            # epoll_pwait2 is usable
    cdef int pid               # the process that created the loop
    cdef unsigned long ring_thread  # ll_thread_token() of the thread that owns the ring
    cdef bint moving           # _bind_thread is draining the old ring: nothing queued now is submitted
    cdef char *scratch
    cdef str want
    cdef epoll_event events[MAX_EVENTS]
    cdef OutBuf pool[POOL_MAX] # spare output buffers: idle connections hold none
    cdef int pool_n
    cdef readonly unsigned long turns
    cdef readonly unsigned long completions

    cdef inline void _give(self, OutBuf *b) noexcept:
        """An output buffer has been sent: keep it for the next writer."""
        b.len = b.off = 0
        if b.p == NULL:
            return
        if b.cap > OUT_KEEP or self.pool_n >= POOL_MAX:
            free(b.p)
        else:
            self.pool[self.pool_n] = b[0]
            self.pool_n += 1
        b.p = NULL
        b.cap = 0

    cdef inline void _take(self, OutBuf *b) noexcept:
        """Give an empty transport an output buffer (b.p is NULL)."""
        if self.pool_n > 0:
            self.pool_n -= 1
            b[0] = self.pool[self.pool_n]
        else:
            # A real buffer even for an empty write, so out_append never
            # copies to NULL. If this fails, out_append reports it.
            b.p = <char *>malloc(OUT_MIN)
            if b.p != NULL:
                b.cap = OUT_MIN

    def __cinit__(self, backend=None):
        cdef epoll_event ev
        self.epfd = -1
        self.wakefd = -1
        self.ring.fd = -1
        self.pwait2 = 1
        self.pid = getpid()
        self.watch = {}
        self.dirty = []
        self.hups = []
        self.live = set()
        self.fds = {}
        self.want = backend or os.environ.get("RINGLOOP_BACKEND") or "auto"
        if self.want not in ("auto", "uring", "epoll"):
            raise ValueError(f"unknown ringloop backend {self.want!r}")
        self.epfd = epoll_create1(EPOLL_CLOEXEC)
        if self.epfd < 0:
            raise os_error(errno)
        self.wakefd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)
        if self.wakefd < 0:
            raise os_error(errno)
        ev.events = EPOLLIN
        ev.data.u64 = 0
        ev.data.fd = self.wakefd
        if epoll_ctl(self.epfd, EPOLL_CTL_ADD, self.wakefd, &ev) < 0:
            raise os_error(errno)

    def __dealloc__(self):
        if self.ring.fd >= 0:
            ll_ring_close(&self.ring)
        if self.scratch != NULL:
            free(self.scratch)
        while self.pool_n > 0:
            self.pool_n -= 1
            free(self.pool[self.pool_n].p)
        if self.epfd >= 0:
            c_close(self.epfd)
        if self.wakefd >= 0:
            c_close(self.wakefd)

    def on_owner_thread(self):
        """This thread owns the loop: it runs it, or holds it for a call."""
        return owned(self)

    def run_owned(self, func, *args):
        """Run `func(*args)` as the loop's owner: directly on the owning
        thread, else holding the loop for the call. For Python code that
        changes several things the loop owns, so that a call from the wrong
        thread fails before it has changed anything."""
        if owned(self):
            return func(*args)
        return self._claimed_call(func, args)

    def run_owned_or_schedule(self, func):
        """For close() and abort() from a thread that does not own the loop:
        run `func()` holding the loop if nobody runs it, else let the loop's
        thread run it soon."""
        cdef unsigned long me = PyThread_get_thread_ident()
        if owned(self):
            return func()
        if ll_cas_ulong(&self.owner, 0, me):
            try:
                return func()
            finally:
                ll_store_ulong(&self.owner, 0)
        try:
            self.loop.call_soon_threadsafe(func)
        except RuntimeError:
            pass  # the loop was closed meanwhile, and everything with it

    cdef object _claimed_call(self, object func, tuple args):
        """Run `func(*args)` holding the loop, for a thread that does not own
        it: allowed only while no other thread does (the loop is not running,
        and no other call is in progress)."""
        cdef unsigned long me = PyThread_get_thread_ident()
        if not ll_cas_ulong(&self.owner, 0, me):
            raise RuntimeError(WRONG_THREAD)
        try:
            return func(*args)
        finally:
            ll_store_ulong(&self.owner, 0)

    cdef int _ensure(self) except -1:
        """Pick the backend when the first socket (or a question about the
        backend) needs it, on the thread that owns the loop then."""
        cdef int rc
        if self.ready:
            return 0
        if self.want != "epoll":
            rc = ll_ring_init(&self.ring, RING_ENTRIES, RECV_BUFS, RECV_BUF_SIZE)
            if rc == 0:
                self.uring = True
                self.ring_thread = ll_thread_token()
            elif self.want == "uring":
                raise os_error(-rc)
            elif rc == -ENOMEM:
                # Not "the kernel has no io_uring" but "this process may not
                # have another ring": worth saying, once.
                _warn_no_ring_memory()
        if not self.uring:
            self.scratch = <char *>malloc(SCRATCH_SIZE)
            if self.scratch == NULL:
                raise MemoryError()
        ll_store_int(&self.ready, 1)
        return 0

    def _choose_backend(self):
        self._ensure()

    @property
    def backend(self):
        if not ll_load_int(&self.ready):
            if owned(self):
                self._ensure()
            else:
                try:
                    self._claimed_call(Core._choose_backend, (self,))
                except RuntimeError:
                    # Another thread runs the loop and it has opened no
                    # socket yet (the ring belongs to that thread, so it
                    # cannot be made here): say what it will be.
                    if self.want == "epoll":
                        return "epoll"
                    return "uring" if self.want == "uring" or _uring_available() else "epoll"
        return "uring" if self.uring else "epoll"

    def live_count(self):
        """Transports and listeners the core still holds (0 when idle)."""
        return len(self.live)

    cdef inline void _submit_now(self) noexcept:
        """Hand queued requests to the kernel without waiting for the turn
        (the kernel refuses if this is not the ring's thread)."""
        if self.uring and self.ring.fd >= 0:
            ll_submit_all(&self.ring)

    cdef _settle(self):
        """The loop is stopping. Send what the last turn wrote and finish
        what it closed, as if the loop took one more look at its sockets:
        under asyncio a write reaches the socket and a closed transport's
        socket is closed without another iteration, and the loop may not run
        again for a while (or ever)."""
        if self.closed or not self.ready:
            return
        self._flush_dirty()
        if self.uring:
            # The cancels of closed transports go to the kernel, their
            # completions come back, and the sockets close (_maybe_release).
            ll_submit_all(&self.ring)
            self._run_completions()

    cdef int _bind_thread(self) except -1:
        """Called when the loop starts running. An io_uring ring belongs to
        the thread that created it (SINGLE_ISSUER); if the loop now runs on
        another thread, replace the ring and arm everything again on it."""
        cdef Transport t
        cdef Listener l
        cdef int rc
        cdef list moved
        cdef ll_ring fresh
        if not self.uring or self.closed:
            return 0
        if ll_thread_token() == self.ring_thread:
            return 0
        # Completions the old thread left unread (a submit in the middle of
        # its last turn can post some): their input is already off the
        # sockets, so run them before the ring goes. Reading the completion
        # queue is not tied to a thread; what the handlers queue on the old
        # ring is dropped and armed again below.
        self.moving = True
        try:
            self._run_completions()
        except BaseException:
            # The move is off (Ctrl-C in a callback): what the drain queued
            # on the old ring may yet be submitted by its own thread.
            self._forget_unsent()
            raise
        finally:
            self.moving = False
        moved = list(self.live)
        for obj in moved:
            t = <Transport>obj if type(obj) is Transport else None
            if t is not None and t.send_inflight and not t.send_unsent and not t.closed:
                # How much of that send the kernel has done is only known to
                # the old ring.
                self._forget_unsent()
                raise RuntimeError(
                    "this loop was last run on another thread and still has output in "
                    "flight there; it cannot move to this thread yet")
        # The new ring first: if it cannot be had, the loop keeps the old one
        # (and its thread) instead of being left without a ring.
        rc = ll_ring_init(&fresh, RING_ENTRIES, RECV_BUFS, RECV_BUF_SIZE)
        if rc < 0:
            self._forget_unsent()
            raise os_error(-rc)
        # Nothing queued on the old ring will be submitted: from here on no
        # request names any descriptor, so held ones can be closed.
        ll_ring_close(&self.ring)
        self.ring = fresh
        self.ring_thread = ll_thread_token()
        self.epoll_armed = False
        self.epoll_ready = False
        for obj in moved:
            if type(obj) is Transport:
                t = <Transport>obj
                t.ops = 0
                t.recv_armed = False
                t.cancel_sent = False
                t.wpoll_armed = False
                t.hup_armed = False
                if t.send_inflight and (t.closed or not t.send_unsent):
                    # The old ring is torn down in the background and its
                    # send may still read this buffer: freed, never reused.
                    t.send_inflight = False
                    out_free(&t.fly)
                if t.closed:
                    t._maybe_release()
                    continue
                if t.send_inflight:
                    # Queued while the old ring was being drained and never
                    # handed to the kernel: queue it again here.
                    t.send_unsent = False
                    ll_prep_send(&self.ring, t.fd, t.fly.p + t.fly.off,
                                 t.fly.len - t.fly.off, ud_of(t, OP_SEND))
                    t.ops += 1
                if t.started and not t.closing:
                    if t.eof_in:
                        t._hup_after_eof(0)  # half-closed: watch the other half again
                    else:
                        t._arm_hup()
                if t.started and t.reading and not t.closing:
                    t._arm_recv()
                if t.wwaiter is not None and t.wwaiter_writable:
                    ll_prep_poll(&self.ring, t.fd, POLLOUT, ud_of(t, OP_WRITABLE))
                    t.wpoll_armed = True
                    t.ops += 1
            else:
                l = <Listener>obj
                l.armed = False
                if l.closed:
                    l._release()
                else:
                    l._arm()
        return 0

    cdef _forget_unsent(self):
        for obj in self.live:
            if type(obj) is Transport:
                (<Transport>obj).send_unsent = False

    def _test_spill(self, unsigned sqes, unsigned fail_submits):
        """Tests only: queue `sqes` no-op requests while the kernel "refuses"
        the next `fail_submits` mid-turn submits. Returns how many requests
        have ever taken the spill path."""
        cdef unsigned i
        if not owned(self):
            return self._claimed_call(Core._test_spill, (self, sqes, fail_submits))
        self._ensure()
        if not self.uring:
            return 0
        self.ring.test_fail_submits = fail_submits
        for i in range(sqes):
            ll_prep_nop(&self.ring, OP_IGNORE)
        return self.ring.spilled

    cdef inline void _wake(self) noexcept:
        """Interrupt a blocked poll(). The caller holds the loop's queue lock,
        which keeps the eventfd open (see LoopCore._post)."""
        cdef uint64_t one = 1
        if self.wakefd >= 0:
            if c_write(self.wakefd, &one, sizeof(one)) < 0:
                pass  # EAGAIN: the counter is at its maximum, the loop will wake

    cdef _close(self):
        """Close everything (the loop owns the core; see LoopCore._close_core)."""
        cdef Transport t
        if self.closed:
            return
        self.closed = True
        if self.uring and self.ring.fd >= 0 and self.ring_thread == ll_thread_token():
            # Cancel everything and let the kernel finish it now. A request
            # holds its socket open: without this a listening address stays
            # taken, and peers see no end, until the kernel has torn the ring
            # down in the background (tens of milliseconds).
            ll_prep_cancel_everything(&self.ring, OP_IGNORE)
            ll_submit_all(&self.ring)
            self._close_accepted()
        for obj in list(self.live):
            if type(obj) is Transport:
                t = <Transport>obj
                t.closed = True
                t.closing = True
                t._close_fd()
            else:
                (<Listener>obj)._release()
        if self.ring.fd >= 0:
            ll_ring_close(&self.ring)
        self._forget_everything()

    cdef void _close_accepted(self) noexcept:
        """Closing: connections the kernel accepted in the last moments have
        no transport and never get one."""
        cdef ll_ring *ring = &self.ring
        cdef unsigned head = ll_cq_head(ring)
        cdef unsigned tail = ll_cq_tail(ring)
        cdef ll_cqe *cqe
        while head != tail:
            cqe = ll_cqe_at(ring, head)
            if <int>(cqe.user_data & OP_MASK) == OP_ACCEPT and cqe.res >= 0:
                c_close(cqe.res)
            head += 1
        ll_cq_advance(ring, head)

    cdef void _forget_everything(self) noexcept:
        self.live.clear()
        self.fds.clear()
        self.dirty.clear()
        self.hups.clear()
        self.watch.clear()
        if self.epfd >= 0:
            c_close(self.epfd)
            self.epfd = -1
        if self.wakefd >= 0:
            c_close(self.wakefd)
            self.wakefd = -1

    cdef _abandon(self):
        """In a forked child: the ring and the epoll set are the parent's.
        Drop this process's references to them without a word to the kernel
        (a request queued here would land in the parent's submission queue,
        an epoll_ctl would change the parent's set), close this process's
        copies of the connections, and leave the loop closed. Listening
        sockets stay with their owners: a child may serve on them with a
        loop of its own."""
        cdef Transport t
        cdef Listener l
        if self.closed:
            return
        self.closed = True
        if self.ring.fd >= 0:
            ll_ring_close(&self.ring)  # unmaps and closes: nothing is submitted
        if self.epfd >= 0:
            c_close(self.epfd)
            self.epfd = -1
        if self.wakefd >= 0:
            c_close(self.wakefd)
            self.wakefd = -1
        for obj in list(self.live):
            if type(obj) is Transport:
                t = <Transport>obj
                t.closed = t.closing = t.lost_scheduled = True
                t.in_epoll = False
                t._close_fd()
            else:
                l = <Listener>obj
                l.closed = True
                l.armed = False
                l._release()
        self._forget_everything()

    # -- add_reader / add_writer ----------------------------------------------

    cdef int _watch_sync(self, int fd, bint reading, bint writing, bint known) except -1:
        """Make the epoll registration of `fd` match. `known`: it is
        registered already (MOD), else it is new (ADD)."""
        cdef epoll_event ev
        ev.events = 0
        ev.data.u64 = 0
        ev.data.fd = fd
        if reading:
            ev.events |= EPOLLIN
        if writing:
            ev.events |= EPOLLOUT
        if ev.events == 0:
            epoll_ctl(self.epfd, EPOLL_CTL_DEL, fd, NULL)  # EBADF: closed by its owner already
            return 0
        if known:
            # ENOENT: the descriptor was closed (dropping its registration)
            # and the number reused.
            if epoll_ctl(self.epfd, EPOLL_CTL_MOD, fd, &ev) == 0:
                return 0
            if errno == ENOENT and epoll_ctl(self.epfd, EPOLL_CTL_ADD, fd, &ev) == 0:
                return 0
        else:
            if epoll_ctl(self.epfd, EPOLL_CTL_ADD, fd, &ev) == 0:
                return 0
            if errno == EEXIST and epoll_ctl(self.epfd, EPOLL_CTL_MOD, fd, &ev) == 0:
                return 0
        raise os_error(errno)

    cdef _watch_add(self, int fd, int slot, Handle handle):
        cdef list ent = self.watch.get(fd)
        cdef Handle old
        if self.closed:
            raise RuntimeError("Event loop is closed")
        holder = self.fds.get(fd)
        if holder is not None:
            # Its own I/O would compete with the callback (and, under epoll,
            # its registration would be overwritten).
            raise RuntimeError(f"File descriptor {fd} is used by a transport or a server ({holder!r})")
        if ent is None:
            self._watch_sync(fd, slot == 0, slot == 1, False)
            ent = [None, None]
            ent[slot] = handle
            self.watch[fd] = ent
            return
        old = ent[slot]
        # Also when a handle is replaced: the descriptor may have been closed
        # under it (which drops its registration) and the number given to a
        # new file since, which is then not registered at all.
        self._watch_sync(fd, slot == 0 or ent[0] is not None, slot == 1 or ent[1] is not None, True)
        ent[slot] = handle
        if old is not None:
            old.cancel()

    cdef bint _watch_remove(self, int fd, int slot) except -1:
        cdef list ent = self.watch.get(fd)
        cdef Handle old
        if ent is None or ent[slot] is None:
            return False
        old = ent[slot]
        ent[slot] = None
        old.cancel()
        if ent[0] is None and ent[1] is None:
            del self.watch[fd]
        # A watch on a number a transport or listener holds can only be stale
        # (_watch_add refuses those numbers): the descriptor it was for was
        # closed under it and the number reused. The registration is the
        # transport's; leave it alone.
        if not self.closed and fd not in self.fds:
            self._watch_sync(fd, ent[0] is not None, ent[1] is not None, True)
        return True

    # -- factories -----------------------------------------------------------

    def make_transport(self, sock, protocol, waiter, extra, server):
        """A transport over a connected socket. `connection_made`, then the
        first read, then the waiter: the order asyncio uses."""
        cdef Transport t
        cdef int fd
        cdef int one = 1
        if not owned(self):
            return self._claimed_call(Core.make_transport, (self, sock, protocol, waiter, extra, server))
        if self.closed:
            raise RuntimeError("Event loop is closed")
        fd = sock.fileno()
        if fd < 0:
            raise ValueError("the socket is closed")
        self._ensure()
        if sock.family in (_socket.AF_INET, _socket.AF_INET6) and sock.type == _socket.SOCK_STREAM:
            c_setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one))  # as asyncio
        t = Transport.__new__(Transport)
        ctx = copy_context()
        t._init(self, fd, sock, protocol, dict(extra) if extra else None, server, ctx)
        loop = self.loop
        loop.call_soon(protocol.connection_made, t, context=ctx)
        if waiter is None:
            loop.call_soon(t._start_reading, context=ctx)
        else:
            # Whoever awaits the waiter (`create_connection`) resumes before
            # the first data_received(), as under asyncio. Input is delivered
            # from inside the I/O turn, ahead of the callbacks of that turn,
            # so reading starts one turn later: after the waiter's task has
            # had its step.
            loop.call_soon(_made_then_read, t, waiter, context=ctx)
        return t

    def listen(self, factory, sock, sslcontext, server, backlog,
               handshake_timeout, shutdown_timeout):
        cdef Listener l
        cdef int one = 1
        if not owned(self):
            return self._claimed_call(Core.listen, (self, factory, sock, sslcontext, server,
                                                    backlog, handshake_timeout, shutdown_timeout))
        if self.closed:
            raise RuntimeError("Event loop is closed")
        self._ensure()
        l = Listener.__new__(Listener)
        check_aligned(l)
        l.core = self
        l.factory = factory
        l.sock = sock
        l.sslcontext = sslcontext
        l.server = server
        l.backlog = backlog
        l.handshake_timeout = handshake_timeout
        l.shutdown_timeout = shutdown_timeout
        l.fd = sock.fileno()
        l.family = sock.family
        l.socktype = sock.type
        l.proto = sock.proto
        if sock.family in (_socket.AF_INET, _socket.AF_INET6) and sock.type == _socket.SOCK_STREAM:
            # As asyncio sets it on every connection; accepted sockets
            # inherit it from the listening socket, one call for all.
            c_setsockopt(l.fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one))
        l.ctx = copy_context()
        l._arm()
        self.fds[l.fd] = l
        self.live.add(l)

    def unlisten(self, sock):
        """Stop accepting on `sock` and close it (now, or as soon as no
        queued request names its descriptor)."""
        if not owned(self):
            return self._claimed_call(Core.unlisten, (self, sock))
        holder = self.fds.get(sock.fileno())
        if type(holder) is Listener:
            (<Listener>holder)._close()
        else:
            sock.close()

    cdef _accepted(self, Listener l, int fd):
        cdef Transport t = None
        cdef bint owned_fd = False
        cdef object ctx
        try:
            # Each connection gets its own copy of the server's context, as
            # under asyncio: a variable set by one connection's callbacks is
            # not seen by the next connection.
            ctx = PyContext_Copy(l.ctx)
            PyContext_Enter(ctx)
            try:
                if l.sslcontext is None:
                    protocol = l.factory()
                    t = Transport.__new__(Transport)
                    t.family = l.family
                    t.socktype = l.socktype
                    t.proto = l.proto
                    t._init(self, fd, None, protocol, None, l.server, ctx)
                    owned_fd = True
                    t._connection_made_now()
                else:
                    conn = _socket.socket(l.family, l.socktype, l.proto, fileno=fd)
                    owned_fd = True
                    self.loop._accepted_tls(
                        conn, l.factory, l.sslcontext, l.server,
                        l.handshake_timeout, l.shutdown_timeout)
            finally:
                PyContext_Exit(ctx)
        except BaseException as exc:
            if not owned_fd:
                if t is not None:
                    # The transport was not set up (a factory that returned
                    # no protocol): it must not close the descriptor as well.
                    t.fd = -1
                    self.live.discard(t)
                    if self.fds.get(fd) is t:
                        del self.fds[fd]
                c_close(fd)
            elif t is not None:
                # Also on Ctrl-C in connection_made(): no half-made transport
                # stays behind, open and never read.
                t._force_close(None if isinstance(exc, (SystemExit, KeyboardInterrupt)) else exc)
            if isinstance(exc, (SystemExit, KeyboardInterrupt)):
                raise
            self.loop.call_exception_handler({
                "message": "Error on transport creation for incoming connection",
                "exception": exc,
            })

    # -- the turn ------------------------------------------------------------

    cdef _flush_dirty(self):
        cdef list dirty = self.dirty
        cdef Transport t
        cdef Py_ssize_t i, n = len(dirty)
        if n == 0:
            return
        self.dirty = []
        i = 0
        try:
            while i < n:
                t = <Transport>dirty[i]
                i += 1
                t._flush()
        finally:
            if i < n:
                # A callback raised (Ctrl-C in resume_writing): the rest
                # still have output waiting and stay queued.
                self.dirty = dirty[i:] + self.dirty

    cdef _poll(self, double timeout):
        """One I/O turn. timeout < 0 waits without limit, 0 does not wait."""
        cdef int64_t ns = -1
        cdef unsigned wait = 1
        cdef int rc
        if self.closed:
            return
        if timeout == 0:
            wait = 0
            ns = 0
        elif timeout > 0:
            if timeout > MAX_WAIT:
                timeout = MAX_WAIT
            ns = <int64_t>(timeout * 1e9) + 1
        self.turns += 1
        if not self.uring:
            # The epoll backend, or no backend yet: a loop that has no socket
            # of its own so far (only callbacks, timers, add_reader/add_writer
            # and the wake-up fd, which live in the epoll set either way)
            # waits here and creates no ring.
            self._epoll_turn(ns)
            return
        if not self.epoll_armed:
            ll_prep_poll(&self.ring, self.epfd, POLLIN, OP_EPOLL)
            self.epoll_armed = True
        if wait:
            with nogil:
                rc = ll_submit_and_wait(&self.ring, 1, ns)
        else:
            rc = ll_submit_and_wait(&self.ring, 0, -1)
        if rc < 0:
            if rc == -EBADR:
                # The kernel had no memory to keep completions the queue
                # had no room for, and dropped them: requests whose end the
                # loop will never see (a send never finishes, a socket is
                # never released). Nothing can recover them.
                raise RuntimeError(
                    "io_uring dropped completions (the kernel ran out of memory); "
                    "this loop cannot continue: close it and create a new one")
            raise os_error(-rc)
        if rc > 0:
            # A signal ended the wait: let Python run its handler (Ctrl-C
            # raises KeyboardInterrupt from here).
            PyErr_CheckSignals()
        self._run_completions()
        if self.epoll_ready:
            self.epoll_ready = False
            self._epoll_turn(0)

    cdef _run_completions(self):
        cdef ll_ring *ring = &self.ring
        cdef unsigned head = ll_cq_head(ring)
        cdef unsigned tail = ll_cq_tail(ring)
        cdef ll_cqe *cqe
        cdef uint64_t ud
        cdef int res, op
        cdef unsigned flags
        cdef Transport t
        cdef Listener l
        cdef PyObject *ctx
        cdef PyObject *entered = NULL   # context we are inside; we own a reference
        # Consecutive completions that share a context (those of one
        # connection) run inside a single Enter/Exit.
        try:
            while head != tail:
                cqe = ll_cqe_at(ring, head)
                ud = cqe.user_data
                res = cqe.res
                flags = cqe.flags
                head += 1
                ll_cq_advance(ring, head)
                self.completions += 1
                op = <int>(ud & OP_MASK)
                if op == OP_RECV or op == OP_SEND or op == OP_WRITABLE or op == OP_HUP:
                    t = <Transport><void *><uintptr_t>(ud & ~<uint64_t>OP_MASK)
                    ctx = <PyObject *>t.ctx
                    if ctx != entered:
                        if entered != NULL:
                            context_exit(entered)
                            Py_DECREF(<object>entered)
                            entered = NULL
                        context_enter(ctx)
                        Py_INCREF(<object>ctx)
                        entered = ctx
                    if op == OP_RECV:
                        t._on_recv(res, flags)
                    elif op == OP_SEND:
                        t._on_send(res)
                    elif op == OP_HUP:
                        t._on_hup(res)
                    else:
                        t._on_writable()
                elif op == OP_ACCEPT:
                    l = <Listener><void *><uintptr_t>(ud & ~<uint64_t>OP_MASK)
                    ctx = <PyObject *>l.ctx
                    if ctx != entered:
                        if entered != NULL:
                            context_exit(entered)
                            Py_DECREF(<object>entered)
                            entered = NULL
                        context_enter(ctx)
                        Py_INCREF(<object>ctx)
                        entered = ctx
                    l._on_accept_cqe(res, flags)
                elif op == OP_EPOLL:
                    self.epoll_armed = False
                    self.epoll_ready = True
                if head == tail:
                    tail = ll_cq_tail(ring)
        finally:
            if entered != NULL:
                ctx, entered = entered, NULL
                try:
                    context_exit(ctx)
                finally:
                    Py_DECREF(<object>ctx)
            if PyList_GET_SIZE(self.hups):
                self._after_hups()

    cdef _after_hups(self):
        cdef list hups = self.hups
        self.hups = []
        for t in hups:
            (<Transport>t)._after_hup()

    cdef _epoll_turn(self, int64_t ns):
        """epoll_wait on the core's set: native sockets (epoll backend) run
        here; reader/writer callbacks go onto the loop's ready queue."""
        cdef int n, i, fd
        cdef uint32_t ev
        cdef uint64_t drained
        cdef object obj
        cdef PyObject *ctx
        cdef PyObject *entered = NULL   # as in _run_completions
        cdef list ent
        cdef Handle h
        cdef dict fds = self.fds
        cdef dict watch = self.watch
        cdef bint native = not self.uring
        cdef Transport t
        cdef Listener l
        if ns != 0:
            with nogil:
                n = ll_epoll_wait(self.epfd, self.events, MAX_EVENTS, ns, &self.pwait2)
            if n < 0:
                if errno != EINTR:
                    raise os_error(errno)
                # A signal ended the wait: let Python run its handler.
                PyErr_CheckSignals()
                return
        else:
            n = ll_epoll_wait(self.epfd, self.events, MAX_EVENTS, 0, &self.pwait2)
        # As in _run_completions: one Enter/Exit for a run of events that
        # share a context.
        try:
            for i in range(n):
                fd = self.events[i].data.fd
                ev = self.events[i].events
                if fd == self.wakefd:
                    if c_read(fd, &drained, sizeof(drained)) < 0:
                        pass  # already drained
                    continue
                obj = fds.get(fd) if native else None
                if obj is not None:
                    if type(obj) is Transport:
                        t = <Transport>obj
                        ctx = <PyObject *>t.ctx
                    else:
                        t = None
                        l = <Listener>obj
                        ctx = <PyObject *>l.ctx
                    if ctx != entered:
                        if entered != NULL:
                            context_exit(entered)
                            Py_DECREF(<object>entered)
                            entered = NULL
                        context_enter(ctx)
                        Py_INCREF(<object>ctx)
                        entered = ctx
                    if t is not None:
                        t._on_epoll(ev)
                    else:
                        l._on_epoll()
                    continue
                ent = watch.get(fd)
                if ent is None:
                    continue
                if ev & ~<uint32_t>EPOLLOUT:
                    h = ent[0]
                    if h is not None:
                        if h.cancelled_c():
                            self._watch_remove(fd, 0)
                        else:
                            PyList_Append((<LoopCore>self.loop).ready, h)
                if ev & ~<uint32_t>EPOLLIN:
                    h = ent[1]
                    if h is not None:
                        if h.cancelled_c():
                            self._watch_remove(fd, 1)
                        else:
                            PyList_Append((<LoopCore>self.loop).ready, h)
        finally:
            if entered != NULL:
                ctx, entered = entered, NULL
                try:
                    context_exit(ctx)
                finally:
                    Py_DECREF(<object>ctx)


# -- scheduling ----------------------------------------------------------------

def format_callback(callback, args):
    """`name(arg, ...)` for a handle's repr and error messages. Arguments go
    through reprlib, as in asyncio: bounded in size, and a repr that raises
    cannot break the error report."""
    name = getattr(callback, "__qualname__", None) or getattr(callback, "__name__", None) or reprlib.repr(callback)
    return "{}({})".format(name, ", ".join(reprlib.repr(a) for a in args or ()))


def _extract_stack():
    """The Python stack of the caller, innermost frame last, without source
    lines (they are looked up only if the traceback is printed)."""
    try:
        frame = sys._getframe(0)
    except ValueError:
        # No Python frame at all: called from a thread that is not running
        # Python code (call_soon_threadsafe passed to start_new_thread, a C
        # library's thread). This file is Cython and pushes none of its own.
        return traceback.StackSummary()
    stack = traceback.StackSummary.extract(
        traceback.walk_stack(frame), limit=10, lookup_lines=False)
    stack.reverse()
    return stack


def _format_handle(Handle h):
    cb = h._callback
    task = getattr(cb, "__self__", None)
    if isinstance(task, asyncio.Task):
        return repr(task)  # a task step: the task says more than the handle
    return repr(h)


@cython.freelist(512)
cdef class Handle:
    """What `call_soon`, `add_reader` and `add_writer` return.

    The underscore attributes mirror `asyncio.Handle` for tools that look at
    them; they are read-only because the loop runs handles without checking
    them again. `cancel()` may be called from any thread."""

    cdef object __weakref__
    cdef object callback        # cleared by cancel(): read under the handle's critical section
    cdef tuple args             # likewise
    cdef object context
    cdef object loop
    cdef object source_traceback  # debug mode: where the handle was created
    cdef int cancelled_flag     # atomic

    cdef inline bint cancelled_c(self) noexcept:
        return ll_load_int(&self.cancelled_flag) != 0

    cdef bint _cancel_c(self):
        """Mark cancelled and drop the callback. False if it already was."""
        cdef object cb
        cdef tuple args
        with cython.critical_section(self):
            if self.cancelled_flag:
                return False
            ll_store_int(&self.cancelled_flag, 1)
            cb = self.callback
            args = self.args
            self.callback = None
            self.args = None
        return True  # cb and args are released here, outside the section

    @property
    def _callback(self):
        with cython.critical_section(self):
            return self.callback

    @property
    def _args(self):
        with cython.critical_section(self):
            return self.args

    @property
    def _context(self):
        return self.context

    @property
    def _loop(self):
        return self.loop

    @property
    def _cancelled(self):
        return self.cancelled_c()

    @property
    def _source_traceback(self):
        return self.source_traceback

    def get_context(self):
        return self.context

    def cancel(self):
        self._cancel_c()

    def cancelled(self):
        return self.cancelled_c()

    def _repr_info(self):
        cdef object cb
        cdef tuple args
        with cython.critical_section(self):
            cb = self.callback
            args = self.args
        info = [type(self).__name__]
        if self.cancelled_c():
            info.append("cancelled")
        if cb is not None:
            info.append(format_callback(cb, args))
        if self.source_traceback:
            frame = self.source_traceback[len(self.source_traceback) - 1]
            info.append(f"created at {frame[0]}:{frame[1]}")
        return info

    def __repr__(self):
        return "<{}>".format(" ".join(self._repr_info()))

    def _run(self):
        self._run_c()

    cdef _run_c(self):
        cdef object cb
        cdef tuple args
        cdef object ctx = self.context
        cdef Py_ssize_t n
        with cython.critical_section(self):
            if self.cancelled_flag:
                return
            cb = self.callback
            args = self.args
        try:
            PyContext_Enter(ctx)
            try:
                n = PyTuple_GET_SIZE(args)
                if n == 1:
                    PyObject_CallOneArg(cb, <object>PyTuple_GET_ITEM(args, 0))
                elif n == 0:
                    PyObject_CallNoArgs(cb)
                else:
                    PyObject_Call(cb, args, NULL)
            finally:
                PyContext_Exit(ctx)
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            context = {
                "message": f"Exception in callback {format_callback(cb, args)}",
                "exception": exc,
                "handle": self,
            }
            if self.source_traceback:
                context["source_traceback"] = self.source_traceback
            self.loop.call_exception_handler(context)


cdef class TimerHandle(Handle):
    """What `call_later` and `call_at` return."""

    cdef double _when
    cdef uint64_t seq           # order among timers due at the same time
    cdef bint scheduled         # in the heap (the loop's thread only)
    cdef bint counted           # counted in LoopCore.timers_cancelled

    def when(self):
        return self._when

    def cancel(self):
        cdef LoopCore loop = <LoopCore>self.loop
        if not self._cancel_c():
            return
        # Count it for the heap clean-up. On the loop's own thread exactly
        # (`scheduled` belongs to that thread); from any other thread in a
        # separate atomic counter, which may count a timer that is not in
        # the heap (any more): at worst that brings a clean-up forward.
        if ll_load_ulong(&loop.thread_id) == PyThread_get_thread_ident():
            if self.scheduled:
                self.counted = True
                loop.timers_cancelled += 1
        else:
            ll_add_ulong(&loop.cancelled_elsewhere, 1)

    def _repr_info(self):
        info = Handle._repr_info(self)
        info.insert(2 if self.cancelled_c() else 1, f"when={self._when}")
        return info

    def __hash__(self):
        return hash(self._when)

    def __lt__(self, other):
        if isinstance(other, TimerHandle):
            return self._when < (<TimerHandle>other)._when
        return NotImplemented

    def __le__(self, other):
        if isinstance(other, TimerHandle):
            return self._when <= (<TimerHandle>other)._when
        return NotImplemented

    def __gt__(self, other):
        if isinstance(other, TimerHandle):
            return self._when > (<TimerHandle>other)._when
        return NotImplemented

    def __ge__(self, other):
        if isinstance(other, TimerHandle):
            return self._when >= (<TimerHandle>other)._when
        return NotImplemented

    def __eq__(self, other):
        return self is other


# The timer heap is a list of TimerHandle ordered by (when, seq): the same
# binary heap as `heapq`, comparing the two C fields directly instead of
# going through tuples and rich comparison. Slots are permuted in place, so
# no reference counts change while sifting. Only the loop's thread touches it.

cdef inline bint timer_before(PyObject *a, PyObject *b) noexcept:
    cdef double wa = (<TimerHandle>a)._when
    cdef double wb = (<TimerHandle>b)._when
    return wa < wb or (wa == wb and (<TimerHandle>a).seq < (<TimerHandle>b).seq)


cdef inline void timers_sift_down(PyObject **items, Py_ssize_t pos, Py_ssize_t n) noexcept:
    cdef PyObject *item = items[pos]
    cdef Py_ssize_t child
    while True:
        child = 2 * pos + 1
        if child >= n:
            break
        if child + 1 < n and timer_before(items[child + 1], items[child]):
            child += 1
        if not timer_before(items[child], item):
            break
        items[pos] = items[child]
        pos = child
    items[pos] = item


cdef inline int timers_push(list heap, TimerHandle h) except -1:
    cdef PyObject **items
    cdef PyObject *item = <PyObject *>h
    cdef Py_ssize_t pos, parent
    PyList_Append(heap, h)
    items = (<PyListObject *>heap).ob_item
    pos = PyList_GET_SIZE(heap) - 1
    while pos > 0:
        parent = (pos - 1) >> 1
        if not timer_before(item, items[parent]):
            break
        items[pos] = items[parent]
        pos = parent
    items[pos] = item
    return 0


cdef inline TimerHandle timers_top(list heap):
    return <TimerHandle>(<PyListObject *>heap).ob_item[0]


cdef inline TimerHandle timers_pop(list heap):
    """Remove and return the earliest timer. The heap must not be empty."""
    cdef PyObject **items = (<PyListObject *>heap).ob_item
    cdef Py_ssize_t n = PyList_GET_SIZE(heap) - 1
    cdef PyObject *top = items[0]
    cdef TimerHandle h
    items[0] = items[n]
    items[n] = top
    h = <TimerHandle>heap.pop()
    if n > 1:
        timers_sift_down((<PyListObject *>heap).ob_item, 0, n)
    return h


cdef inline void timers_heapify(list heap) noexcept:
    cdef Py_ssize_t n = PyList_GET_SIZE(heap)
    cdef Py_ssize_t i = n // 2
    while i > 0:
        i -= 1
        timers_sift_down((<PyListObject *>heap).ob_item, i, n)


cdef class LoopCore:
    """The engine under `ringloop.loop.Loop`: ready queue, timers, I/O turn.

    `ready`, `batch` and `timers` belong to the thread running the loop.
    Other threads post handles to `inbox` under `lock`; the running thread
    moves them over at the start of each turn (`_drain_inbox`)."""

    cdef object __weakref__
    cdef readonly Core _core
    cdef list ready                  # handles to run on the next turn
    cdef list batch                  # handles of the turn in progress (empty otherwise)
    cdef list timers                 # heap of TimerHandle, see timers_push
    cdef uint64_t timer_seq
    cdef Py_ssize_t timers_cancelled  # cancelled timers in the heap, counted on the loop's thread
    cdef unsigned long cancelled_elsewhere  # atomic: timers cancelled on other threads
    cdef int _closed                 # atomic, written under `lock`
    cdef int _stopping               # atomic
    cdef unsigned long thread_id     # thread running the loop, else 0; atomic, written under `lock`
    cdef cython.pymutex lock         # guards inbox and the writes of thread_id and _closed
    cdef list inbox                  # handles posted by other threads
    cdef list inbox_spare            # the other list of the pair (empty between drains)
    cdef int posted                  # atomic: inbox is not empty
    cdef bint draining               # _drain_inbox is running (a finalizer may schedule meanwhile)
    cdef public bint _debug
    cdef public double slow_callback_duration  # debug mode: log callbacks that take longer (seconds)
    cdef readonly Handle _current_handle       # debug mode: the handle being run

    def __cinit__(self, *args, **kwargs):
        self.ready = []
        self.batch = []
        self.timers = []
        self.inbox = []
        self.inbox_spare = []
        self.slow_callback_duration = 0.1

    def _init_core(self, backend=None):
        self._core = Core(backend)
        self._core.loop = self

    @property
    def backend(self):
        """`"uring"` or `"epoll"` (decided on first use)."""
        return self._core.backend

    # -- callbacks -----------------------------------------------------------

    cdef inline Handle _handle(self, object callback, tuple args, object context):
        cdef Handle h = Handle.__new__(Handle)
        h.callback = callback
        h.args = args
        h.context = PyContext_CopyCurrent() if context is None else context
        h.loop = self
        return h

    cdef _debug_check(self, Handle h, object callback, str method):
        """Debug mode: refuse what cannot be a callback, as asyncio does, and
        remember where the handle was created."""
        if asyncio.iscoroutine(callback) or inspect.iscoroutinefunction(callback):
            raise TypeError(f"coroutines cannot be used with {method}()")
        if not callable(callback):
            raise TypeError(f"a callable object was expected by {method}(), got {callback!r}")
        h.source_traceback = _extract_stack()

    cdef _run_debug(self, Handle h):
        """Debug mode: run one handle and log it if it was slow."""
        cdef double t0 = monotonic()
        cdef double dt
        self._current_handle = h
        try:
            h._run_c()
        finally:
            self._current_handle = None
        dt = monotonic() - t0
        if dt >= self.slow_callback_duration:
            logging.getLogger("asyncio").warning(
                "Executing %s took %.3f seconds", _format_handle(h), dt)

    cdef inline bint _on_loop_thread(self) noexcept:
        return ll_load_ulong(&self.thread_id) == PyThread_get_thread_ident()

    cdef _post(self, Handle h):
        """Queue a handle from a thread that is not running the loop (or
        while nobody runs it). Wakes the running thread, if there is one."""
        cdef bint closed
        # Nothing in here runs Python code or drops a last reference (the
        # append may only resize the list): a finalizer run from inside could
        # post again and deadlock.
        with self.lock:
            closed = self._closed
            if not closed:
                PyList_Append(self.inbox, h)
                if not self.posted:
                    # The first handle since the loop last emptied the inbox
                    # wakes it; until it has emptied it again (which it does
                    # before it sleeps) that wake-up stands for the others.
                    ll_store_int(&self.posted, 1)
                    if ll_load_ulong(&self.thread_id) != 0:
                        self._core._wake()
        if closed:
            raise RuntimeError("Event loop is closed")

    cdef _drain_inbox(self):
        """Move posted handles to the ready queue and the timer heap (the
        running thread only)."""
        cdef list items
        cdef object obj
        cdef TimerHandle th
        if self.draining:
            # Scheduling from a finalizer run by this very drain: swapping
            # the lists again now would hand other threads the list being
            # emptied. What was posted meanwhile waits for the next drain.
            return
        self.draining = True
        with self.lock:
            items = self.inbox
            self.inbox = self.inbox_spare
            self.inbox_spare = items
            ll_store_int(&self.posted, 0)
        try:
            for obj in items:
                if type(obj) is TimerHandle:
                    th = <TimerHandle>obj
                    if not th.cancelled_c():
                        self._push_timer(th)
                else:
                    PyList_Append(self.ready, obj)
        finally:
            items.clear()
            self.draining = False

    cdef _push_timer(self, TimerHandle h):
        self.timer_seq += 1
        h.seq = self.timer_seq
        h.scheduled = True
        timers_push(self.timers, h)

    cdef inline _schedule(self, Handle h):
        if self._on_loop_thread():
            if ll_load_int(&self.posted):
                # Handles posted earlier by other threads go first, as with
                # a single queue.
                self._drain_inbox()
            PyList_Append(self.ready, h)
        else:
            self._post(h)

    # A closed loop is never running, so the "Event loop is closed" check
    # happens in _post, under the lock.

    def call_soon(self, callback, *args, context=None):
        cdef Handle h = self._handle(callback, args, context)
        if self._debug:
            self._debug_check(h, callback, "call_soon")
        self._schedule(h)
        return h

    def call_soon_threadsafe(self, callback, *args, context=None):
        # The same as call_soon: every scheduling call is safe from any thread.
        cdef Handle h = self._handle(callback, args, context)
        if self._debug:
            self._debug_check(h, callback, "call_soon_threadsafe")
        self._schedule(h)
        return h

    def time(self):
        return monotonic()

    def call_later(self, delay, callback, *args, context=None):
        if delay is None:
            raise TypeError("delay must not be None")
        return self._call_at(monotonic() + <double>delay, callback, args, context, "call_later")

    def call_at(self, when, callback, *args, context=None):
        if when is None:
            raise TypeError("when cannot be None")
        return self._call_at(<double>when, callback, args, context, "call_at")

    cdef _call_at(self, double when, object callback, tuple args, object context, str method):
        cdef TimerHandle h
        if when != when:
            raise ValueError("a timer cannot be due at NaN")
        h = TimerHandle.__new__(TimerHandle)
        h.callback = callback
        h.args = args
        h.context = PyContext_CopyCurrent() if context is None else context
        h.loop = self
        h._when = when
        if self._debug:
            self._debug_check(h, callback, method)
        if self._on_loop_thread():
            if ll_load_int(&self.posted):
                self._drain_inbox()  # as in _schedule: earlier posts first
            self._push_timer(h)
        else:
            self._post(h)
        return h

    def _timer_handle_cancelled(self, handle):
        pass

    # -- fd callbacks --------------------------------------------------------

    cdef inline int _fileno(self, object fileobj) except -1:
        cdef int fd
        if isinstance(fileobj, int):
            fd = fileobj
        else:
            fd = fileobj.fileno()
        if fd < 0:
            raise ValueError(f"Invalid file descriptor: {fd}")
        return fd

    def add_reader(self, fd, callback, *args):
        cdef Handle h
        if not owned(self._core):
            return self._core._claimed_call(LoopCore.add_reader, (self, fd, callback) + args)
        h = self._handle(callback, args, None)
        self._core._watch_add(self._fileno(fd), 0, h)
        return h

    def remove_reader(self, fd):
        if not owned(self._core):
            return self._core._claimed_call(LoopCore.remove_reader, (self, fd))
        return self._core._watch_remove(self._fileno(fd), 0)

    def add_writer(self, fd, callback, *args):
        cdef Handle h
        if not owned(self._core):
            return self._core._claimed_call(LoopCore.add_writer, (self, fd, callback) + args)
        h = self._handle(callback, args, None)
        self._core._watch_add(self._fileno(fd), 1, h)
        return h

    def remove_writer(self, fd):
        if not owned(self._core):
            return self._core._claimed_call(LoopCore.remove_writer, (self, fd))
        return self._core._watch_remove(self._fileno(fd), 1)

    # -- running -------------------------------------------------------------

    def is_running(self):
        return ll_load_ulong(&self.thread_id) != 0

    def is_closed(self):
        return ll_load_int(&self._closed) != 0

    def stop(self):
        cdef unsigned long running
        # Under the lock that guards thread_id: a loop that starts on another
        # thread at this moment either sees the flag or is seen running.
        with self.lock:
            running = ll_load_ulong(&self.thread_id)
            if running == 0 or running == PyThread_get_thread_ident():
                ll_store_int(&self._stopping, 1)
                running = 0
        if running:
            # Running on another thread: that thread stops itself, woken up.
            try:
                self._post(self._handle(self.stop, (), None))
            except RuntimeError:
                pass  # closed meanwhile: there is nothing left to stop

    def get_debug(self):
        return self._debug

    def set_debug(self, enabled):
        self._debug = enabled

    def _run_turns(self):
        """Run until stop(). The caller has set the running loop."""
        cdef Core core = self._core
        cdef unsigned long me = PyThread_get_thread_ident()
        if getpid() != core.pid:
            # A forked child shares the parent's ring and epoll set: their
            # completions and events would go to whichever process asks first.
            raise RuntimeError("this event loop was created in another process (before fork()); "
                               "create a new loop in this process")
        cdef int tries = 0
        while not ll_cas_ulong(&core.owner, 0, me):
            if ll_load_ulong(&self.thread_id) != 0:
                raise RuntimeError("This event loop is already running")
            # Another thread holds the idle loop for one call (a finalizer
            # that closes a transport, say): that takes microseconds.
            tries += 1
            if tries > 2000:
                raise RuntimeError("This event loop is in use by another thread")
            with nogil:
                usleep(100)
        try:
            if self._closed:
                raise RuntimeError("Event loop is closed")
            with self.lock:
                ll_store_ulong(&self.thread_id, me)
            try:
                core._bind_thread()
                while True:
                    self._turn()
                    if ll_load_int(&self._stopping):
                        break
                core._settle()  # not when an exception ends the run
            finally:
                ll_store_int(&self._stopping, 0)
                with self.lock:
                    ll_store_ulong(&self.thread_id, 0)
                # Leave nothing queued but unsubmitted on this thread's ring:
                # the loop may next run on another thread (see _bind_thread).
                core._submit_now()
        finally:
            ll_store_ulong(&core.owner, 0)

    def _abandon_after_fork(self):
        """Called in a forked child (see Core._abandon): the loop is closed
        there, and what it had scheduled does not run."""
        ll_store_int(&self._closed, 1)
        ll_store_int(&self._stopping, 1)  # if the fork came from a callback, the run ends
        ll_store_ulong(&self.thread_id, 0)  # and scheduling says "closed" from here on
        for obj in self.batch:
            if obj is not None:
                (<Handle>obj)._cancel_c()
        self.ready.clear()
        self.timers.clear()
        self.inbox.clear()
        self.inbox_spare.clear()
        self._core._abandon()

    def _close_core(self):
        cdef Core core = self._core
        cdef unsigned long me = PyThread_get_thread_ident()
        cdef bint claimed = False
        if ll_load_ulong(&core.owner) != me:
            if not ll_cas_ulong(&core.owner, 0, me):
                raise RuntimeError("Cannot close a running event loop")
            claimed = True
        try:
            with self.lock:
                ll_store_int(&self._closed, 1)
            # From here no thread posts (it sees _closed under the lock) or
            # writes to the wake-up eventfd, so the core may close it.
            self.ready.clear()
            self.batch.clear()
            self.timers.clear()
            self.inbox.clear()
            self.inbox_spare.clear()
            core._close()
        finally:
            if claimed:
                ll_store_ulong(&core.owner, 0)

    cdef _turn(self):
        cdef list timers
        cdef list batch
        cdef double timeout = -1
        cdef double now
        cdef Py_ssize_t n, i
        cdef Handle h
        cdef TimerHandle th
        cdef Py_ssize_t cancelled
        if ll_load_int(&self.posted):
            self._drain_inbox()
        # Output written in the last turn goes to the kernel now (io_uring:
        # is queued for the wait below). This comes before the timeout is
        # worked out: under epoll a send that empties a buffer calls
        # resume_writing() or schedules connection_lost(), and what those
        # make ready or set a timer for must not be slept over.
        self._core._flush_dirty()
        timers = self.timers
        if PyList_GET_SIZE(timers):
            cancelled = self.timers_cancelled + <Py_ssize_t>ll_load_ulong(&self.cancelled_elsewhere)
            if cancelled > 64 and cancelled * 2 > PyList_GET_SIZE(timers):
                # Mostly cancelled: rebuild the heap without them. Cancels
                # from other threads that land during the scan count next time.
                ll_swap_ulong(&self.cancelled_elsewhere, 0)
                kept = []
                for th in timers:
                    if th.cancelled_c():
                        th.scheduled = False
                    else:
                        kept.append(th)
                timers_heapify(kept)
                self.timers = timers = kept
                self.timers_cancelled = 0
            else:
                while PyList_GET_SIZE(timers):
                    th = timers_top(timers)
                    if not th.cancelled_c():
                        break
                    timers_pop(timers)
                    th.scheduled = False
                    if th.counted:
                        self.timers_cancelled -= 1
        if not PyList_GET_SIZE(self.ready) and not ll_load_int(&self._stopping):
            # About to sleep: run pending Python signal handlers first. They
            # only run when asked from here (this is not bytecode), and one of
            # them may raise (Ctrl-C) or schedule a callback.
            PyErr_CheckSignals()
        if (PyList_GET_SIZE(self.ready) or ll_load_int(&self._stopping)
                or PyList_GET_SIZE(self._core.dirty)):
            # dirty: under epoll the flush above can empty a buffer and call
            # resume_writing(), and what that writes waits for the next turn's
            # flush; there is nothing to wake the loop for it. (Under io_uring
            # the flush only queues sends, so dirty is empty here.)
            timeout = 0
        elif PyList_GET_SIZE(timers):
            timeout = timers_top(timers)._when - monotonic()
            if timeout < 0:
                timeout = 0

        self._core._poll(timeout)

        if PyList_GET_SIZE(timers):
            now = monotonic()
            while PyList_GET_SIZE(timers):
                th = timers_top(timers)
                if th._when > now:
                    break
                timers_pop(timers)
                th.scheduled = False
                if th.cancelled_c():
                    if th.counted:
                        self.timers_cancelled -= 1
                else:
                    PyList_Append(self.ready, th)
            th = None

        # Run what is ready now; what these callbacks schedule waits for the
        # next turn (it goes to the other list).
        batch = self.ready
        n = PyList_GET_SIZE(batch)
        if n == 0:
            return
        self.ready = self.batch
        self.batch = batch
        i = 0
        try:
            while i < n:
                h = <Handle>(<PyListObject *>batch).ob_item[i]
                # Give up the queue's reference now, so the handle (and what
                # it holds) is released as soon as it has run.
                Py_INCREF(None)
                PyList_SET_ITEM(batch, i, None)
                Py_DECREF(h)
                i += 1
                if self._debug:
                    self._run_debug(h)
                else:
                    h._run_c()  # a no-op if cancelled
        except BaseException:
            # SystemExit / KeyboardInterrupt: keep what has not run, ahead of
            # anything scheduled meanwhile.
            self.ready[0:0] = batch[i:]
            raise
        finally:
            h = None
            batch.clear()
