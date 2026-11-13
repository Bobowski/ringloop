"""`Loop`: an `asyncio.AbstractEventLoop` on the ringloop core.

Scheduling, fd callbacks, stream transports and accept are native
(`_core.LoopCore`). This module adds the rest of the public loop interface
on top of those, using public APIs only: running and closing, tasks and
futures, the exception handler, executors and DNS, signals, servers,
client connections, and the `sock_*` coroutines.
"""

import asyncio
import collections.abc
import concurrent.futures
import errno
import functools
import inspect
import itertools
import logging
import os
import signal
import socket
import ssl as ssl_module
import stat
import sys
import threading
import traceback
import warnings
import weakref
from asyncio.staggered import staggered_race
from asyncio.trsock import TransportSocket

from ._core import LoopCore
from .extras import ExtrasMixin
from .tls import SSLTransport

logger = logging.getLogger("asyncio")

_DEBUG_STACK_DEPTH = 10
_orphaned_wakeup_sockets = []  # see Loop._close_signal_socket

# Every loop of the process, for the fork hook below.
_loops = weakref.WeakSet()
_loops_lock = threading.Lock()


def _after_fork_in_child():
    global _loops_lock
    _loops_lock = threading.Lock()  # another thread may have held it at the fork
    for loop in list(_loops):
        loop._after_fork()


os.register_at_fork(after_in_child=_after_fork_in_child)


def _stop_when_done(fut):
    if not fut.cancelled():
        exc = fut.exception()
        if isinstance(exc, (SystemExit, KeyboardInterrupt)):
            return
    fut.get_loop().stop()


def _noop_signal_handler(signum, frame):
    """The loop reads the signal from the wake-up socket instead."""


def _numeric_addrinfo(host, port, family, type, proto):
    """What getaddrinfo would answer for a numeric address, or None.

    Saves the trip through the thread pool when there is nothing to look up
    (asyncio does the same)."""
    if host is None or proto not in (0, socket.IPPROTO_TCP, socket.IPPROTO_UDP):
        return None
    if type == socket.SOCK_STREAM:
        proto = socket.IPPROTO_TCP
    elif type == socket.SOCK_DGRAM:
        proto = socket.IPPROTO_UDP
    else:
        return None
    if port is None or port == "" or port == b"":
        port = 0
    else:
        try:
            port = int(port)
        except (TypeError, ValueError):
            return None  # a service name
    if isinstance(host, bytes):
        try:
            host = host.decode("idna")
        except UnicodeError:
            return None
    if not isinstance(host, str) or "%" in host:
        return None  # a scoped IPv6 address needs getaddrinfo
    if family == socket.AF_UNSPEC:
        families = (socket.AF_INET, socket.AF_INET6)
    else:
        families = (family,)
    for af in families:
        try:
            socket.inet_pton(af, host)
        except (OSError, ValueError):
            continue
        if af == socket.AF_INET6:
            return af, type, proto, "", (host, port, 0, 0)
        return af, type, proto, "", (host, port)
    return None


def _interleave_addrinfos(infos, first_family_count=1):
    """Order addresses for Happy Eyeballs (RFC 8305, section 4): the address
    families take turns, after `first_family_count` addresses of the family
    the resolver put first."""
    by_family = {}
    for info in infos:
        by_family.setdefault(info[0], []).append(info)
    groups = list(by_family.values())
    ordered = []
    if first_family_count > 1:
        ordered.extend(groups[0][:first_family_count - 1])
        del groups[0][:first_family_count - 1]
    for row in itertools.zip_longest(*groups):
        ordered.extend(info for info in row if info is not None)
    return ordered


def _check_ssl_args(ssl, handshake_timeout, shutdown_timeout):
    if ssl is not None and not isinstance(ssl, ssl_module.SSLContext):
        raise TypeError("ssl argument must be an SSLContext or None")
    if handshake_timeout is not None and not ssl:
        raise ValueError("ssl_handshake_timeout is only meaningful with ssl")
    if shutdown_timeout is not None and not ssl:
        raise ValueError("ssl_shutdown_timeout is only meaningful with ssl")


class Server(asyncio.AbstractServer):
    """What `create_server` returns."""

    def __init__(self, loop, sockets, protocol_factory, ssl_context, backlog,
                 ssl_handshake_timeout, ssl_shutdown_timeout):
        self._loop = loop
        self._sockets = sockets
        self._clients = set()
        self._waiters = []
        self._protocol_factory = protocol_factory
        self._backlog = backlog
        self._ssl_context = ssl_context
        self._ssl_handshake_timeout = ssl_handshake_timeout
        self._ssl_shutdown_timeout = ssl_shutdown_timeout
        self._serving = False
        self._serving_forever_fut = None
        self._unix_paths = {}   # socket -> (path, inode) to unlink on close

    def __repr__(self):
        return f"<{type(self).__name__} sockets={self.sockets!r}>"

    # Transports call these two.
    def _attach(self, transport):
        self._clients.add(transport)

    def _detach(self, transport):
        self._clients.discard(transport)
        if not self._clients and self._sockets is None:
            self._wakeup()

    def _wakeup(self):
        waiters, self._waiters = self._waiters, None
        if waiters is None:
            return
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(None)

    def get_loop(self):
        return self._loop

    def is_serving(self):
        return self._serving

    @property
    def sockets(self):
        if self._sockets is None:
            return ()
        # As asyncio: views that cannot close or read the listening sockets.
        return tuple(TransportSocket(sock) for sock in self._sockets)

    def _start_serving(self):
        # Several steps on loop-owned state: all on the loop's thread, or none.
        self._loop._core.run_owned(self._start_serving_owned)

    def _start_serving_owned(self):
        if self._serving:
            return
        if self._sockets is None:
            raise RuntimeError(f"server {self!r} is closed")
        self._serving = True
        for sock in self._sockets:
            sock.listen(self._backlog)
            self._loop._core.listen(
                self._protocol_factory, sock, self._ssl_context, self,
                self._backlog, self._ssl_handshake_timeout, self._ssl_shutdown_timeout)

    async def start_serving(self):
        self._start_serving()
        await asyncio.sleep(0)

    async def serve_forever(self):
        if self._serving_forever_fut is not None:
            raise RuntimeError(f"server {self!r} is already being awaited on serve_forever()")
        if self._sockets is None:
            raise RuntimeError(f"server {self!r} is closed")
        self._start_serving()
        self._serving_forever_fut = self._loop.create_future()
        try:
            await self._serving_forever_fut
        except asyncio.CancelledError:
            try:
                self.close()
                self.close_clients()  # as current asyncio: do not wait for idle clients
                await self.wait_closed()
            finally:
                raise
        finally:
            self._serving_forever_fut = None

    def close(self):
        self._loop._core.run_owned(self._close_owned)

    def _close_owned(self):
        sockets = self._sockets
        if sockets is None:
            return
        self._sockets = None
        for sock in sockets:
            self._unlink_unix_path(sock)
            # The core closes the socket: at once, or (io_uring) as soon as no
            # queued request names its descriptor any more.
            self._loop._core.unlisten(sock)
        self._serving = False
        if self._serving_forever_fut is not None and not self._serving_forever_fut.done():
            self._serving_forever_fut.cancel()
            self._serving_forever_fut = None
        if not self._clients:
            self._wakeup()

    def _remember_unix_path(self, sock):
        """`cleanup_socket`: note the socket file so close() can remove it."""
        path = sock.getsockname()
        if not path or path[0] in (0, "\x00"):
            return  # unnamed or abstract: nothing on disk
        try:
            self._unix_paths[sock] = (path, os.stat(path).st_ino)
        except OSError:
            pass

    def _unlink_unix_path(self, sock):
        entry = self._unix_paths.pop(sock, None)
        if entry is None:
            return
        path, inode = entry
        try:
            # Only if it is still our file: another server may have replaced it.
            if os.stat(path).st_ino == inode:
                os.unlink(path)
        except FileNotFoundError:
            pass
        except OSError as err:
            logger.error("Unable to clean up listening UNIX socket %r: %r", path, err)

    def close_clients(self):
        for transport in list(self._clients):
            transport.close()

    def abort_clients(self):
        for transport in list(self._clients):
            transport.abort()

    async def wait_closed(self):
        if self._waiters is None:
            return
        waiter = self._loop.create_future()
        self._waiters.append(waiter)
        await waiter


class Loop(LoopCore, ExtrasMixin, asyncio.AbstractEventLoop):
    def __init__(self, *, backend=None):
        self._init_core(backend)
        # As asyncio: `python -X dev` and PYTHONASYNCIODEBUG switch debug mode on.
        self._debug = sys.flags.dev_mode or (
            not sys.flags.ignore_environment and bool(os.environ.get("PYTHONASYNCIODEBUG")))
        with _loops_lock:
            _loops.add(self)
        self._exception_handler = None
        self._task_factory = None
        self._default_executor = None
        self._executor_shutdown_called = False
        self._asyncgens = weakref.WeakSet()
        self._asyncgens_shutdown_called = False
        self._signal_handlers = {}
        self._signal_rsock = None
        self._signal_wsock = None
        self._signal_wakeup_set = False

    def _after_fork(self):
        """In a forked child (see Core._abandon): this loop is closed here.
        Its ring, epoll set and signal wake-up socket are the parent's."""
        self._abandon_after_fork()
        self._signal_handlers.clear()
        rsock, wsock = self._signal_rsock, self._signal_wsock
        self._signal_rsock = self._signal_wsock = None
        if self._signal_wakeup_set:
            self._signal_wakeup_set = False
            try:
                signal.set_wakeup_fd(-1)  # or this process's signals would reach the parent's loop
            except (ValueError, OSError):
                pass
        for sock in (rsock, wsock):
            if sock is not None:
                sock.close()
        self._default_executor = None  # its threads did not come along

    def __repr__(self):
        return (f"<{type(self).__name__} running={self.is_running()} "
                f"closed={self.is_closed()} debug={self.get_debug()}>")

    def _check_closed(self):
        if self.is_closed():
            raise RuntimeError("Event loop is closed")

    # -- running -------------------------------------------------------------

    def run_forever(self):
        self._check_closed()
        if self.is_running():
            raise RuntimeError("This event loop is already running")
        if asyncio._get_running_loop() is not None:
            raise RuntimeError("Cannot run the event loop while another loop is running")
        old_hooks = sys.get_asyncgen_hooks()
        sys.set_asyncgen_hooks(firstiter=self._asyncgen_firstiter,
                               finalizer=self._asyncgen_finalizer)
        # Debug mode: "coroutine ... was never awaited" says where it was created.
        old_depth = sys.get_coroutine_origin_tracking_depth()
        if self._debug:
            sys.set_coroutine_origin_tracking_depth(_DEBUG_STACK_DEPTH)
        asyncio._set_running_loop(self)
        try:
            self._run_turns()
        finally:
            asyncio._set_running_loop(None)
            sys.set_coroutine_origin_tracking_depth(old_depth)
            sys.set_asyncgen_hooks(*old_hooks)

    def run_until_complete(self, future):
        self._check_closed()
        if self.is_running():
            raise RuntimeError("This event loop is already running")
        if asyncio._get_running_loop() is not None:
            # Before the coroutine becomes a task that nothing would run.
            raise RuntimeError("Cannot run the event loop while another loop is running")
        new_task = not asyncio.isfuture(future)
        future = asyncio.ensure_future(future, loop=self)
        future.add_done_callback(_stop_when_done)
        try:
            self.run_forever()
        except BaseException:
            if new_task and future.done() and not future.cancelled():
                # The caller cannot retrieve it: mark the exception retrieved.
                future.exception()
            raise
        finally:
            future.remove_done_callback(_stop_when_done)
        if not future.done():
            raise RuntimeError("Event loop stopped before Future completed.")
        return future.result()

    def close(self):
        if self.is_running():
            raise RuntimeError("Cannot close a running event loop")
        if self.is_closed():
            return
        # Every step as the loop's owner, or none: a close refused because
        # another thread holds the loop must not have shut anything down.
        self._core.run_owned(self._close_owned)

    def _close_owned(self):
        if self.is_closed():
            return
        for sig in list(self._signal_handlers):
            self.remove_signal_handler(sig)
        self._close_signal_socket()
        self._executor_shutdown_called = True
        executor, self._default_executor = self._default_executor, None
        if executor is not None:
            executor.shutdown(wait=False)
        self._close_core()

    def __del__(self, _warn=warnings.warn):
        if not self.is_closed():
            _warn(f"unclosed event loop {self!r}", ResourceWarning, source=self)
            try:
                if not self.is_running():
                    self.close()
            except Exception:
                pass

    # -- futures and tasks ---------------------------------------------------

    def create_future(self):
        return asyncio.Future(loop=self)

    def create_task(self, coro, **kwargs):
        self._check_closed()
        factory = self._task_factory
        if factory is None:
            return asyncio.Task(coro, loop=self, **kwargs)
        if sys.version_info >= (3, 14):
            return factory(self, coro, **kwargs)
        # Before 3.14 asyncio hands a factory the context only, and names
        # the task itself: factories are written for that.
        name = kwargs.pop("name", None)
        if kwargs.get("context") is None:
            kwargs.pop("context", None)
        task = factory(self, coro, **kwargs)
        try:
            if name is not None:
                task.set_name(name)
            return task
        finally:
            # An eager task may have failed inside the factory call: its
            # traceback holds this frame, which must not hold the task.
            del task

    def set_task_factory(self, factory):
        if factory is not None and not callable(factory):
            raise TypeError("task factory must be a callable or None")
        self._task_factory = factory

    def get_task_factory(self):
        return self._task_factory

    # -- exception handler ---------------------------------------------------

    def get_exception_handler(self):
        return self._exception_handler

    def set_exception_handler(self, handler):
        if handler is not None and not callable(handler):
            raise TypeError(f"A callable object or None is expected, got {handler!r}")
        self._exception_handler = handler

    def default_exception_handler(self, context):
        message = context.get("message") or "Unhandled exception in event loop"
        exception = context.get("exception")
        exc_info = (type(exception), exception, exception.__traceback__) if exception is not None else False
        if "source_traceback" not in context:
            current = self._current_handle  # debug mode only
            if current is not None and current._source_traceback:
                context["handle_traceback"] = current._source_traceback
        lines = [message]
        for key in sorted(context):
            if key in ("message", "exception"):
                continue
            value = context[key]
            if key == "source_traceback":
                value = "Object created at (most recent call last):\n" + \
                    "".join(traceback.format_list(value)).rstrip()
            elif key == "handle_traceback":
                value = "Handle created at (most recent call last):\n" + \
                    "".join(traceback.format_list(value)).rstrip()
            else:
                value = repr(value)
            lines.append(f"{key}: {value}")
        logger.error("\n".join(lines), exc_info=exc_info)

    def call_exception_handler(self, context):
        handler = self._exception_handler
        if handler is None:
            try:
                self.default_exception_handler(context)
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException:
                logger.error("Exception in default exception handler", exc_info=True)
            return
        try:
            # In the context of the task, future or handle that failed, as
            # asyncio does: the handler sees the context variables they set.
            thing = context.get("task")
            if thing is None:
                thing = context.get("future")
            if thing is None:
                thing = context.get("handle")
            ctx = thing.get_context() if thing is not None and hasattr(thing, "get_context") else None
            if ctx is not None and hasattr(ctx, "run"):
                ctx.run(handler, self, context)
            else:
                handler(self, context)
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            try:
                self.default_exception_handler({
                    "message": "Unhandled error in exception handler",
                    "exception": exc,
                    "context": context,
                })
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException:
                logger.error("Exception in default exception handler while handling "
                             "an unexpected error in custom exception handler", exc_info=True)

    # -- async generators ----------------------------------------------------

    def _asyncgen_firstiter(self, agen):
        if self._asyncgens_shutdown_called:
            warnings.warn(f"asynchronous generator {agen!r} was scheduled after "
                          f"loop.shutdown_asyncgens() call", ResourceWarning, source=self)
        self._asyncgens.add(agen)

    def _asyncgen_finalizer(self, agen):
        self._asyncgens.discard(agen)
        if not self.is_closed():
            self.call_soon_threadsafe(self.create_task, agen.aclose())

    async def shutdown_asyncgens(self):
        self._asyncgens_shutdown_called = True
        if not len(self._asyncgens):
            return
        closing = list(self._asyncgens)
        self._asyncgens.clear()
        results = await asyncio.gather(*[ag.aclose() for ag in closing], return_exceptions=True)
        for result, agen in zip(results, closing):
            if isinstance(result, Exception):
                self.call_exception_handler({
                    "message": f"an error occurred during closing of asynchronous generator {agen!r}",
                    "exception": result,
                    "asyncgen": agen,
                })

    # -- executors and DNS ---------------------------------------------------

    def run_in_executor(self, executor, func, *args):
        self._check_closed()
        if executor is None:
            executor = self._default_executor
            if self._executor_shutdown_called:
                raise RuntimeError("Executor shutdown has been called")
            if executor is None:
                executor = concurrent.futures.ThreadPoolExecutor(thread_name_prefix="asyncio")
                self._default_executor = executor
        return asyncio.wrap_future(executor.submit(func, *args), loop=self)

    def set_default_executor(self, executor):
        if not isinstance(executor, concurrent.futures.ThreadPoolExecutor):
            raise TypeError("executor must be ThreadPoolExecutor instance")
        self._default_executor = executor

    async def shutdown_default_executor(self, timeout=None):
        self._executor_shutdown_called = True
        executor = self._default_executor
        if executor is None:
            return
        future = self.create_future()

        def work():
            try:
                executor.shutdown(wait=True)
                if not self.is_closed():
                    self.call_soon_threadsafe(_set_result, future, None)
            except Exception as exc:
                if not self.is_closed():
                    self.call_soon_threadsafe(_set_exception, future, exc)

        thread = threading.Thread(target=work)
        thread.start()
        try:
            async with asyncio.timeout(timeout):
                await future
        except TimeoutError:
            warnings.warn(f"The executor did not finishing joining its threads within "
                          f"{timeout} seconds.", RuntimeWarning, stacklevel=2)
            executor.shutdown(wait=False)
        else:
            thread.join()

    async def getaddrinfo(self, host, port, *, family=0, type=0, proto=0, flags=0):
        return await self.run_in_executor(
            None, socket.getaddrinfo, host, port, family, type, proto, flags)

    async def getnameinfo(self, sockaddr, flags=0):
        return await self.run_in_executor(None, socket.getnameinfo, sockaddr, flags)

    async def _resolve(self, host, port, *, family=0, type=socket.SOCK_STREAM, proto=0, flags=0):
        info = _numeric_addrinfo(host, port, family, type, proto)
        if info is not None:
            return [info]
        infos = await self.getaddrinfo(host, port, family=family, type=type, proto=proto, flags=flags)
        if not infos:
            raise OSError(f"getaddrinfo({host!r}) returned empty list")
        return infos

    # -- signals -------------------------------------------------------------

    def add_signal_handler(self, sig, callback, *args):
        if asyncio.iscoroutine(callback) or inspect.iscoroutinefunction(callback):
            raise TypeError("coroutines cannot be used with add_signal_handler()")
        self._check_signal(sig)
        self._check_closed()
        if self._signal_rsock is None:
            rsock, wsock = socket.socketpair()
            rsock.setblocking(False)
            wsock.setblocking(False)
            try:
                self.add_reader(rsock.fileno(), self._read_signals)
            except BaseException:
                rsock.close()
                wsock.close()
                raise
            self._signal_rsock, self._signal_wsock = rsock, wsock
        try:
            # On every call, as asyncio: another loop may have reset it.
            signal.set_wakeup_fd(self._signal_wsock.fileno(), warn_on_full_buffer=False)
            self._signal_wakeup_set = True
        except (ValueError, OSError) as exc:
            if not self._signal_handlers:
                self._close_signal_socket()
            raise RuntimeError(str(exc)) from None
        self._signal_handlers[sig] = (callback, args)
        try:
            signal.signal(sig, _noop_signal_handler)
            signal.siginterrupt(sig, False)
        except OSError as exc:
            del self._signal_handlers[sig]
            if not self._signal_handlers:
                self._close_signal_socket()
            if exc.errno == errno.EINVAL:
                raise RuntimeError(f"sig {sig} cannot be caught") from None
            raise

    def remove_signal_handler(self, sig):
        self._check_signal(sig)
        if self._signal_handlers.pop(sig, None) is None:
            return False
        handler = signal.default_int_handler if sig == signal.SIGINT else signal.SIG_DFL
        try:
            signal.signal(sig, handler)
        except OSError as exc:
            if exc.errno == errno.EINVAL:
                raise RuntimeError(f"sig {sig} cannot be caught") from None
            raise
        except ValueError:
            pass  # not the main thread (interpreter shutdown)
        if not self._signal_handlers:
            self._close_signal_socket()
        return True

    def _check_signal(self, sig):
        if not isinstance(sig, int):
            raise TypeError(f"sig must be an int, not {sig!r}")
        if sig not in signal.valid_signals():
            raise ValueError(f"invalid signal number {sig}")

    def _read_signals(self):
        try:
            data = self._signal_rsock.recv(4096)
        except (BlockingIOError, InterruptedError):
            return
        for signum in data:
            entry = self._signal_handlers.get(signum)
            if entry is not None:
                callback, args = entry
                self.call_soon(callback, *args)

    def _close_signal_socket(self):
        rsock, wsock = self._signal_rsock, self._signal_wsock
        if rsock is None:
            return
        self._signal_rsock = self._signal_wsock = None
        if not self.is_closed():
            self.remove_reader(rsock.fileno())
        if self._signal_wakeup_set:
            self._signal_wakeup_set = False
            try:
                signal.set_wakeup_fd(-1)
            except (ValueError, OSError):
                # Not the main thread: the process keeps writing a byte per
                # signal to this descriptor number. Leave the pair open (two
                # descriptors) instead of letting the number go to a file
                # that would then receive those bytes.
                _orphaned_wakeup_sockets.append((rsock, wsock))
                return
        rsock.close()
        wsock.close()

    # -- sock_* --------------------------------------------------------------

    async def _sock_io(self, sock, writing, attempt):
        """Run `attempt()` now, or each time the socket becomes ready."""
        if self._debug and sock.gettimeout() != 0:  # as asyncio: checked in debug mode
            raise ValueError("the socket must be non-blocking")
        try:
            return attempt()
        except (BlockingIOError, InterruptedError):
            pass
        future = self.create_future()
        fd = sock.fileno()

        def ready():
            if future.done():
                return
            try:
                result = attempt()
            except (BlockingIOError, InterruptedError):
                return
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException as exc:
                future.set_exception(exc)
            else:
                future.set_result(result)

        handle = self.add_writer(fd, ready) if writing else self.add_reader(fd, ready)
        try:
            return await future
        finally:
            self._unwatch(fd, writing, handle)

    def _unwatch(self, fd, writing, handle):
        """Drop the fd callback `handle`, unless a later registration for the
        same fd has replaced (and so cancelled) it: a cancelled `sock_recv`
        finishes after the next `sock_recv` on that socket has started."""
        if handle.cancelled() or self.is_closed():
            return
        if writing:
            self.remove_writer(fd)
        else:
            self.remove_reader(fd)

    async def sock_recv(self, sock, n):
        return await self._sock_io(sock, False, lambda: sock.recv(n))

    async def sock_recv_into(self, sock, buf):
        return await self._sock_io(sock, False, lambda: sock.recv_into(buf))

    async def sock_recvfrom(self, sock, bufsize):
        return await self._sock_io(sock, False, lambda: sock.recvfrom(bufsize))

    async def sock_recvfrom_into(self, sock, buf, nbytes=0):
        if not nbytes:
            nbytes = len(buf)
        return await self._sock_io(sock, False, lambda: sock.recvfrom_into(buf, nbytes))

    async def sock_sendall(self, sock, data):
        view = memoryview(data).cast("B")
        while len(view):
            sent = await self._sock_io(sock, True, lambda: sock.send(view))
            view = view[sent:]

    async def sock_sendto(self, sock, data, address):
        return await self._sock_io(sock, True, lambda: sock.sendto(data, address))

    async def sock_accept(self, sock):
        def attempt():
            conn, address = sock.accept()
            conn.setblocking(False)
            return conn, address

        return await self._sock_io(sock, False, attempt)

    async def sock_connect(self, sock, address):
        if self._debug and sock.gettimeout() != 0:
            raise ValueError("the socket must be non-blocking")
        if sock.family in (socket.AF_INET, socket.AF_INET6):
            infos = await self._resolve(*address[:2], family=sock.family, type=sock.type, proto=sock.proto)
            # Keep the caller's IPv6 flow info and scope id (a link-local
            # address is useless without its scope).
            resolved = infos[0][4]
            address = resolved[:2] + tuple(address[2:]) if len(address) > 2 else resolved
        try:
            sock.connect(address)
            return
        except (BlockingIOError, InterruptedError):
            pass
        future = self.create_future()
        fd = sock.fileno()

        def writable():
            if future.done():
                return
            err = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if err:
                future.set_exception(OSError(err, f"Connect call failed {address}"))
            else:
                future.set_result(None)

        handle = self.add_writer(fd, writable)
        try:
            await future
        finally:
            self._unwatch(fd, True, handle)

    # -- transports ----------------------------------------------------------

    def _make_tls_transport(self, sock, protocol, sslcontext, waiter=None, *,
                            server_side=False, server_hostname=None, extra=None,
                            server=None, handshake_timeout=None, shutdown_timeout=None):
        tls = SSLTransport(
            self, protocol, sslcontext, waiter, server_side, server_hostname,
            handshake_timeout, shutdown_timeout)
        self._core.make_transport(sock, tls, None, extra, server)
        return tls

    def _accepted_tls(self, conn, protocol_factory, sslcontext, server,
                      handshake_timeout, shutdown_timeout):
        """Called by the core for each connection accepted on a TLS listener."""
        try:
            conn.setblocking(False)
            try:
                peername = conn.getpeername()
            except OSError:
                peername = None
            tls = SSLTransport(
                self, protocol_factory(), sslcontext, None, True, None,
                handshake_timeout, shutdown_timeout)
        except BaseException:
            conn.close()  # no transport owns it yet
            raise
        self._core.make_transport(conn, tls, None, {"peername": peername}, server)

    async def _transport_for(self, sock, protocol_factory, ssl, server_hostname,
                             handshake_timeout, shutdown_timeout, *, server_side=False):
        try:
            sock.setblocking(False)
            protocol = protocol_factory()
            waiter = self.create_future()
            if ssl:
                if ssl is True:
                    sslcontext = ssl_module.create_default_context()
                    if not server_hostname:
                        sslcontext.check_hostname = False  # as asyncio: "" means no name check
                else:
                    sslcontext = ssl
                transport = self._make_tls_transport(
                    sock, protocol, sslcontext, waiter, server_side=server_side,
                    server_hostname=server_hostname,
                    handshake_timeout=handshake_timeout, shutdown_timeout=shutdown_timeout)
            else:
                transport = self._core.make_transport(sock, protocol, waiter, None, None)
        except BaseException:
            sock.close()  # no transport owns it
            raise
        try:
            await waiter
        except BaseException:
            transport.close()
            raise
        finally:
            waiter = None  # the exception's traceback holds this frame: no cycle through the future
        return transport, protocol

    async def start_tls(self, transport, protocol, sslcontext, *, server_side=False,
                        server_hostname=None, ssl_handshake_timeout=None,
                        ssl_shutdown_timeout=None):
        if not isinstance(sslcontext, ssl_module.SSLContext):
            raise TypeError(f"sslcontext is expected to be an instance of ssl.SSLContext, got {sslcontext!r}")
        if not getattr(transport, "_start_tls_compatible", False):
            raise TypeError(f"transport {transport!r} is not supported by start_tls()")
        waiter = self.create_future()
        tls = SSLTransport(
            self, protocol, sslcontext, waiter, server_side, server_hostname,
            ssl_handshake_timeout, ssl_shutdown_timeout, call_connection_made=False)
        transport.pause_reading()
        if server_side:
            # A stream server that calls start_tls() after the client has
            # already sent its ClientHello has those bytes in its
            # StreamReader. asyncio moves them into the TLS layer here
            # (gh-142352); there is no public way to take them out.
            reader = getattr(protocol, "_stream_reader", None)
            buffered = getattr(reader, "_buffer", None)
            if isinstance(protocol, asyncio.StreamReaderProtocol) and buffered:
                tls._incoming.write(buffered)
                buffered.clear()
        transport.set_protocol(tls)
        made = self.call_soon(tls.connection_made, transport)
        resume = self.call_soon(transport.resume_reading)
        try:
            await waiter
        except BaseException:
            transport.close()
            made.cancel()
            resume.cancel()
            raise
        finally:
            waiter = None  # the exception's traceback holds this frame: no cycle through the future
        return tls

    # -- clients -------------------------------------------------------------

    async def create_connection(
            self, protocol_factory, host=None, port=None, *, ssl=None, family=0,
            proto=0, flags=0, sock=None, local_addr=None, server_hostname=None,
            ssl_handshake_timeout=None, ssl_shutdown_timeout=None,
            happy_eyeballs_delay=None, interleave=None, all_errors=False):
        if server_hostname is not None and not ssl:
            raise ValueError("server_hostname is only meaningful with ssl")
        if server_hostname is None and ssl:
            if not host:
                raise ValueError("You must set server_hostname when using ssl without a host")
            server_hostname = host
        if ssl is not None and ssl is not True and not isinstance(ssl, ssl_module.SSLContext):
            raise TypeError("ssl argument must be an SSLContext, True or None")
        if ssl_handshake_timeout is not None and not ssl:
            raise ValueError("ssl_handshake_timeout is only meaningful with ssl")
        if ssl_shutdown_timeout is not None and not ssl:
            raise ValueError("ssl_shutdown_timeout is only meaningful with ssl")

        if host is not None or port is not None:
            if sock is not None:
                raise ValueError("host/port and sock can not be specified at the same time")
            infos = await self._resolve(host, port, family=family, proto=proto, flags=flags)
            local_infos = None
            if local_addr is not None:
                local_infos = await self._resolve(*local_addr, family=family, proto=proto, flags=flags)
            if happy_eyeballs_delay is not None and interleave is None:
                interleave = 1  # RFC 8305: alternate the address families
            if interleave:
                infos = _interleave_addrinfos(infos, interleave)
            errors = []
            try:
                if happy_eyeballs_delay is None:
                    # One address at a time, in order.
                    for info in infos:
                        try:
                            sock = await self._connect_one(info, local_infos)
                            break
                        except OSError as exc:
                            errors.append(exc)
                else:
                    # Happy Eyeballs: start the next address when the current
                    # one fails or has taken `happy_eyeballs_delay` seconds;
                    # the first to connect wins and the others are cancelled
                    # (each closes its own socket).
                    # The race also reports the CancelledError of each
                    # attempt it cancelled: the errors that count are kept
                    # here.
                    async def attempt(info):
                        try:
                            return await self._connect_one(info, local_infos)
                        except OSError as exc:
                            errors.append(exc)
                            raise

                    sock, _, _ = await staggered_race(
                        [functools.partial(attempt, info) for info in infos],
                        happy_eyeballs_delay, loop=self)
                if sock is None:
                    if all_errors:
                        raise ExceptionGroup("create_connection failed", errors)
                    if len(errors) == 1:
                        raise errors[0]
                    first = str(errors[0])
                    if all(str(exc) == first for exc in errors):
                        raise errors[0]
                    raise OSError("Multiple exceptions: {}".format(", ".join(str(e) for e in errors)))
            finally:
                errors = None  # the exceptions hold this frame: break the cycle
        else:
            if sock is None:
                raise ValueError("host and port was not specified and no sock specified")
            if sock.type != socket.SOCK_STREAM:
                raise ValueError(f"A Stream Socket was expected, got {sock!r}")
        return await self._transport_for(
            sock, protocol_factory, ssl, server_hostname,
            ssl_handshake_timeout, ssl_shutdown_timeout)

    async def _connect_one(self, info, local_infos):
        family, type_, proto, _, address = info
        sock = socket.socket(family=family, type=type_, proto=proto)
        try:
            sock.setblocking(False)
            if local_infos is not None:
                bound = False
                last = None
                for lfamily, _, _, _, laddr in local_infos:
                    if lfamily != family:
                        continue
                    try:
                        sock.bind(laddr)
                        bound = True
                        break
                    except OSError as exc:
                        last = OSError(exc.errno, f"error while attempting to bind on address {laddr!r}: {str(exc).lower()}")
                if not bound:
                    raise last or OSError(f"no matching local address with {family=} found")
            if family in (socket.AF_INET, socket.AF_INET6) and type_ == socket.SOCK_STREAM:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            try:
                sock.connect(address)
            except (BlockingIOError, InterruptedError):
                future = self.create_future()
                fd = sock.fileno()

                def writable():
                    if not future.done():
                        future.set_result(None)

                handle = self.add_writer(fd, writable)
                try:
                    await future
                finally:
                    self._unwatch(fd, True, handle)
                err = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
                if err:
                    raise OSError(err, f"Connect call failed {address}")
            return sock
        except BaseException:
            sock.close()
            raise

    async def create_unix_connection(
            self, protocol_factory, path=None, *, ssl=None, sock=None,
            server_hostname=None, ssl_handshake_timeout=None, ssl_shutdown_timeout=None):
        if ssl:
            if server_hostname is None:
                raise ValueError("you have to pass server_hostname when using ssl")
        else:
            if server_hostname is not None:
                raise ValueError("server_hostname is only meaningful with ssl")
            if ssl_handshake_timeout is not None:
                raise ValueError("ssl_handshake_timeout is only meaningful with ssl")
            if ssl_shutdown_timeout is not None:
                raise ValueError("ssl_shutdown_timeout is only meaningful with ssl")
        if path is not None:
            if sock is not None:
                raise ValueError("path and sock can not be specified at the same time")
            path = os.fspath(path)
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM, 0)
            try:
                sock.setblocking(False)
                await self.sock_connect(sock, path)
            except BaseException:
                sock.close()
                raise
        else:
            if sock is None:
                raise ValueError("no path and sock were specified")
            if sock.family != socket.AF_UNIX or sock.type != socket.SOCK_STREAM:
                raise ValueError(f"A UNIX Domain Stream Socket was expected, got {sock!r}")
        return await self._transport_for(
            sock, protocol_factory, ssl, server_hostname,
            ssl_handshake_timeout, ssl_shutdown_timeout)

    async def connect_accepted_socket(
            self, protocol_factory, sock, *, ssl=None,
            ssl_handshake_timeout=None, ssl_shutdown_timeout=None):
        if sock.type != socket.SOCK_STREAM:
            raise ValueError(f"A Stream Socket was expected, got {sock!r}")
        _check_ssl_args(ssl, ssl_handshake_timeout, ssl_shutdown_timeout)
        return await self._transport_for(
            sock, protocol_factory, ssl, None,
            ssl_handshake_timeout, ssl_shutdown_timeout, server_side=True)

    # -- servers -------------------------------------------------------------

    async def create_server(
            self, protocol_factory, host=None, port=None, *,
            family=socket.AF_UNSPEC, flags=socket.AI_PASSIVE, sock=None, backlog=100,
            ssl=None, reuse_address=None, reuse_port=None, keep_alive=None,
            ssl_handshake_timeout=None, ssl_shutdown_timeout=None, start_serving=True):
        if isinstance(ssl, bool):
            raise TypeError("ssl argument must be an SSLContext or None")
        _check_ssl_args(ssl, ssl_handshake_timeout, ssl_shutdown_timeout)
        if host is not None or port is not None:
            if sock is not None:
                raise ValueError("host/port and sock can not be specified at the same time")
            if reuse_address is None:
                reuse_address = True
            if host == "":
                hosts = [None]
            elif isinstance(host, str) or not isinstance(host, collections.abc.Iterable):
                hosts = [host]
            else:
                hosts = host
            results = await asyncio.gather(*[
                self._resolve(h, port, family=family, flags=flags) for h in hosts])
            infos = set(itertools.chain.from_iterable(results))
            sockets = []
            completed = False
            try:
                for af, socktype, proto, _, sa in infos:
                    try:
                        sock = socket.socket(af, socktype, proto)
                    except OSError:
                        # e.g. the host has IPv6 addresses but no IPv6 stack
                        continue
                    sockets.append(sock)
                    if reuse_address:
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
                    if reuse_port:
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
                    if keep_alive:
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, True)
                    if af == socket.AF_INET6:
                        sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, True)
                    try:
                        sock.bind(sa)
                    except OSError as err:
                        msg = f"error while attempting to bind on address {sa!r}: {str(err).lower()}"
                        if err.errno == errno.EADDRNOTAVAIL:
                            sockets.pop()
                            sock.close()
                            continue
                        raise OSError(err.errno, msg) from None
                if not sockets:
                    raise OSError(f"could not bind on any address out of {[info[4] for info in infos]!r}")
                completed = True
            finally:
                if not completed:
                    for sock in sockets:
                        sock.close()
        else:
            if sock is None:
                raise ValueError("Neither host/port nor sock were specified")
            if sock.type != socket.SOCK_STREAM:
                raise ValueError(f"A Stream Socket was expected, got {sock!r}")
            sockets = [sock]
        for sock in sockets:
            sock.setblocking(False)
        server = Server(self, sockets, protocol_factory, ssl, backlog,
                        ssl_handshake_timeout, ssl_shutdown_timeout)
        if start_serving:
            server._start_serving()
            # One iteration, so the listeners are armed before returning.
            await asyncio.sleep(0)
        return server

    async def create_unix_server(
            self, protocol_factory, path=None, *, sock=None, backlog=100, ssl=None,
            ssl_handshake_timeout=None, ssl_shutdown_timeout=None,
            start_serving=True, cleanup_socket=sys.version_info >= (3, 13)):
        if isinstance(ssl, bool):
            raise TypeError("ssl argument must be an SSLContext or None")
        _check_ssl_args(ssl, ssl_handshake_timeout, ssl_shutdown_timeout)
        if path is not None:
            if sock is not None:
                raise ValueError("path and sock can not be specified at the same time")
            path = os.fspath(path)
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            # Remove a stale socket file left by an earlier run.
            if path[0] not in (0, "\x00"):
                try:
                    if stat.S_ISSOCK(os.stat(path).st_mode):
                        os.remove(path)
                except FileNotFoundError:
                    pass
                except OSError as err:
                    logger.error("Unable to check or remove stale UNIX socket %r: %r", path, err)
            try:
                sock.bind(path)
            except OSError as exc:
                sock.close()
                if exc.errno == errno.EADDRINUSE:
                    raise OSError(errno.EADDRINUSE, f"Address {path!r} is already in use") from None
                raise
            except BaseException:
                sock.close()
                raise
        else:
            if sock is None:
                raise ValueError("path was not specified, and no sock specified")
            if sock.family != socket.AF_UNIX or sock.type != socket.SOCK_STREAM:
                raise ValueError(f"A UNIX Domain Stream Socket was expected, got {sock!r}")
        sock.setblocking(False)
        server = Server(self, [sock], protocol_factory, ssl, backlog,
                        ssl_handshake_timeout, ssl_shutdown_timeout)
        if cleanup_socket:
            server._remember_unix_path(sock)
        if start_serving:
            server._start_serving()
            await asyncio.sleep(0)
        return server


def _set_result(future, result):
    if not future.done():
        future.set_result(result)


def _set_exception(future, exc):
    if not future.done():
        future.set_exception(exc)
