"""The parts of the loop interface a web server rarely touches: pipes,
subprocesses, datagram endpoints, sendfile.

Written in Python on `add_reader` / `add_writer`. Correct first; none of it
is on a hot path for an HTTP server.
"""

import asyncio
import collections
import errno
import logging
import os
import socket
import stat
import subprocess
import threading
import warnings
from asyncio.trsock import TransportSocket

from ._core import Transport as StreamTransport
from ._owner import loop_owned, loop_owned_close

HIGH_WATER = 64 * 1024


def _describe(transport, fd, closed):
    state = "closed" if closed else "closing" if transport.is_closing() else "open"
    return f"<{type(transport).__module__}.{type(transport).__name__} fd={fd} {state}>"


class _FlowControl:
    """pause_writing / resume_writing bookkeeping shared by write transports."""

    _high = HIGH_WATER
    _low = HIGH_WATER // 4
    _protocol_paused = False

    def get_write_buffer_limits(self):
        return (self._low, self._high)

    @loop_owned
    def set_write_buffer_limits(self, high=None, low=None):
        if high is None:
            high = HIGH_WATER if low is None else 4 * low
        if low is None:
            low = high // 4
        if not high >= low >= 0:
            raise ValueError(f"high ({high!r}) must be >= low ({low!r}) must be >= 0")
        self._high, self._low = high, low
        self._maybe_pause()

    def _maybe_pause(self):
        if self.get_write_buffer_size() > self._high and not self._protocol_paused:
            self._protocol_paused = True
            self._call_protocol("pause_writing")

    def _maybe_resume(self):
        if self._protocol_paused and self.get_write_buffer_size() <= self._low:
            self._protocol_paused = False
            self._call_protocol("resume_writing")

    def _call_protocol(self, name):
        try:
            getattr(self._protocol, name)()
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            self._loop.call_exception_handler({
                "message": f"protocol.{name}() failed",
                "exception": exc,
                "transport": self,
                "protocol": self._protocol,
            })


class ReadPipeTransport(asyncio.ReadTransport):
    max_size = 256 * 1024

    def __init__(self, loop, pipe, protocol, waiter=None, extra=None):
        super().__init__(extra)
        self._extra["pipe"] = pipe
        self._loop = loop
        self._pipe = pipe
        self._fileno = pipe.fileno()
        self._protocol = protocol
        self._closing = False
        self._paused = False
        mode = os.fstat(self._fileno).st_mode
        if not (stat.S_ISFIFO(mode) or stat.S_ISSOCK(mode) or stat.S_ISCHR(mode)):
            self._pipe = None
            raise ValueError("Pipe transport is for pipes/sockets only.")
        os.set_blocking(self._fileno, False)
        loop.call_soon(protocol.connection_made, self)
        loop.call_soon(self._add_reader)
        if waiter is not None:
            loop.call_soon(_set_waiter, waiter)

    def _add_reader(self):
        if not self._closing and not self._paused:
            self._loop.add_reader(self._fileno, self._read_ready)

    def _read_ready(self):
        try:
            data = os.read(self._fileno, self.max_size)
        except (BlockingIOError, InterruptedError):
            return
        except OSError as exc:
            self._fatal_error(exc, "Fatal read error on pipe transport")
            return
        if data:
            self._protocol.data_received(data)
        else:
            self._closing = True
            self._loop.remove_reader(self._fileno)
            self._loop.call_soon(self._protocol.eof_received)
            self._loop.call_soon(self._call_connection_lost, None)

    @loop_owned
    def pause_reading(self):
        if not self.is_reading():
            return
        self._paused = True
        self._loop.remove_reader(self._fileno)

    @loop_owned
    def resume_reading(self):
        if self._closing or not self._paused:
            return
        self._paused = False
        self._loop.add_reader(self._fileno, self._read_ready)

    def is_reading(self):
        return not self._paused and not self._closing

    @loop_owned
    def set_protocol(self, protocol):
        self._protocol = protocol

    def get_protocol(self):
        return self._protocol

    def is_closing(self):
        return self._closing

    @loop_owned_close
    def close(self):
        if not self._closing:
            self._close(None)

    def _fatal_error(self, exc, message):
        if not (isinstance(exc, OSError) and exc.errno == errno.EIO):
            self._loop.call_exception_handler({
                "message": message, "exception": exc,
                "transport": self, "protocol": self._protocol,
            })
        self._close(exc)

    def _close(self, exc):
        self._closing = True
        self._loop.remove_reader(self._fileno)
        self._loop.call_soon(self._call_connection_lost, exc)

    def _call_connection_lost(self, exc):
        try:
            self._protocol.connection_lost(exc)
        finally:
            if self._pipe is not None:
                self._pipe.close()
            self._pipe = None
            self._protocol = None

    def __repr__(self):
        return _describe(self, self._fileno, self._pipe is None)

    def __del__(self, _warn=warnings.warn):
        if getattr(self, "_pipe", None) is not None:
            _warn(f"unclosed transport {self!r}", ResourceWarning, source=self)
            self._pipe.close()


class WritePipeTransport(_FlowControl, asyncio.WriteTransport):
    def __init__(self, loop, pipe, protocol, waiter=None, extra=None):
        super().__init__(extra)
        self._extra["pipe"] = pipe
        self._loop = loop
        self._pipe = pipe
        self._fileno = pipe.fileno()
        self._protocol = protocol
        self._buffer = bytearray()
        self._conn_lost = 0
        self._closing = False
        mode = os.fstat(self._fileno).st_mode
        is_char = stat.S_ISCHR(mode)
        is_fifo = stat.S_ISFIFO(mode)
        is_socket = stat.S_ISSOCK(mode)
        if not (is_char or is_fifo or is_socket):
            self._pipe = None
            raise ValueError("Pipe transport is only for pipes, sockets and character devices")
        os.set_blocking(self._fileno, False)
        loop.call_soon(protocol.connection_made, self)
        # A reader on the write end reports the other side closing.
        self._watch_close = is_socket or is_fifo
        if self._watch_close:
            loop.call_soon(self._add_close_watch)
        if waiter is not None:
            loop.call_soon(_set_waiter, waiter)

    def _add_close_watch(self):
        if not self._closing:
            self._loop.add_reader(self._fileno, self._read_ready)

    def _read_ready(self):
        # The pipe was closed by the peer.
        if self._buffer:
            self._close(BrokenPipeError())
        else:
            self._close(None)

    def get_write_buffer_size(self):
        return len(self._buffer)

    @loop_owned
    def write(self, data):
        if isinstance(data, bytearray):
            data = memoryview(data)
        if not data:
            return
        if self._conn_lost or self._closing:
            self._conn_lost += 1
            return
        if not self._buffer:
            try:
                n = os.write(self._fileno, data)
            except (BlockingIOError, InterruptedError):
                n = 0
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException as exc:
                self._conn_lost += 1
                self._fatal_error(exc, "Fatal write error on pipe transport")
                return
            if n == len(data):
                return
            if n > 0:
                data = memoryview(data)[n:]
            self._loop.add_writer(self._fileno, self._write_ready)
        self._buffer += data
        self._maybe_pause()

    def _write_ready(self):
        try:
            n = os.write(self._fileno, self._buffer)
        except (BlockingIOError, InterruptedError):
            return
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            self._buffer.clear()
            self._conn_lost += 1
            self._loop.remove_writer(self._fileno)
            self._fatal_error(exc, "Fatal write error on pipe transport")
            return
        if n == len(self._buffer):
            self._buffer.clear()
            self._loop.remove_writer(self._fileno)
            self._maybe_resume()
            if self._closing:
                if self._watch_close:
                    self._loop.remove_reader(self._fileno)
                self._loop.call_soon(self._call_connection_lost, None)
            return
        if n > 0:
            del self._buffer[:n]

    def can_write_eof(self):
        return True

    @loop_owned
    def write_eof(self):
        if self._closing:
            return
        self._closing = True
        if not self._buffer:
            if self._watch_close:
                self._loop.remove_reader(self._fileno)
            self._loop.call_soon(self._call_connection_lost, None)

    @loop_owned
    def set_protocol(self, protocol):
        self._protocol = protocol

    def get_protocol(self):
        return self._protocol

    def is_closing(self):
        return self._closing

    @loop_owned_close
    def close(self):
        if self._pipe is not None and not self._closing:
            self.write_eof()

    @loop_owned_close
    def abort(self):
        self._close(None)

    def _fatal_error(self, exc, message):
        if not isinstance(exc, OSError):
            self._loop.call_exception_handler({
                "message": message, "exception": exc,
                "transport": self, "protocol": self._protocol,
            })
        self._close(exc)

    def _close(self, exc=None):
        if self._conn_lost and self._closing and self._pipe is None:
            return
        self._closing = True
        if self._buffer:
            self._loop.remove_writer(self._fileno)
        self._buffer.clear()
        if self._watch_close:
            self._loop.remove_reader(self._fileno)
        self._conn_lost += 1
        self._loop.call_soon(self._call_connection_lost, exc)

    def _call_connection_lost(self, exc):
        if self._pipe is None:
            return
        try:
            self._protocol.connection_lost(exc)
        finally:
            self._pipe.close()
            self._pipe = None
            self._protocol = None

    def __repr__(self):
        return _describe(self, self._fileno, self._pipe is None)

    def __del__(self, _warn=warnings.warn):
        if getattr(self, "_pipe", None) is not None:
            _warn(f"unclosed transport {self!r}", ResourceWarning, source=self)
            self._pipe.close()


class _PipeToProcess(asyncio.Protocol):
    """Protocol on one of a child's pipes; forwards to the process transport."""

    def __init__(self, proc, fd):
        self.proc = proc
        self.fd = fd
        self.pipe = None
        self.disconnected = False

    def connection_made(self, transport):
        self.pipe = transport

    def data_received(self, data):
        self.proc._pipe_data_received(self.fd, data)

    def eof_received(self):
        pass

    def connection_lost(self, exc):
        self.disconnected = True
        self.proc._pipe_connection_lost(self.fd, exc)
        self.proc = None

    def pause_writing(self):
        self.proc._protocol.pause_writing()

    def resume_writing(self):
        self.proc._protocol.resume_writing()


class ProcessTransport(asyncio.SubprocessTransport):
    def __init__(self, loop, protocol, args, shell, stdin, stdout, stderr, bufsize,
                 waiter=None, extra=None, **kwargs):
        super().__init__(extra)
        self._loop = loop
        self._protocol = protocol
        self._closed = False
        self._returncode = None
        self._exit_waiters = []
        self._pending_calls = collections.deque()
        self._pipes = {}
        self._finished = False
        self._pidfd = -1
        self._proc = None
        if stdin == subprocess.PIPE:
            self._pipes[0] = None
        if stdout == subprocess.PIPE:
            self._pipes[1] = None
        if stderr == subprocess.PIPE:
            self._pipes[2] = None
        self._proc = subprocess.Popen(
            args, shell=shell, stdin=stdin, stdout=stdout, stderr=stderr,
            universal_newlines=False, bufsize=bufsize, **kwargs)
        self._pid = self._proc.pid
        self._extra["subprocess"] = self._proc
        try:
            self._pidfd = os.pidfd_open(self._pid)
        except (AttributeError, OSError):
            # No pidfd (Linux before 5.3, or out of descriptors): wait for
            # the child in a thread instead.
            threading.Thread(target=self._wait_in_thread, args=(self._proc,),
                             name=f"ringloop-waitpid-{self._pid}", daemon=True).start()
        else:
            loop.add_reader(self._pidfd, self._on_exit)
        loop.create_task(self._connect_pipes(waiter))

    def _wait_in_thread(self, proc):
        proc.wait()
        try:
            self._loop.call_soon_threadsafe(self._on_exit)
        except RuntimeError:
            pass  # the loop is closed

    async def _connect_pipes(self, waiter):
        try:
            proc, loop = self._proc, self._loop
            if proc.stdin is not None:
                _, pipe = await loop.connect_write_pipe(lambda: _PipeToProcess(self, 0), proc.stdin)
                self._pipes[0] = pipe
            if proc.stdout is not None:
                _, pipe = await loop.connect_read_pipe(lambda: _PipeToProcess(self, 1), proc.stdout)
                self._pipes[1] = pipe
            if proc.stderr is not None:
                _, pipe = await loop.connect_read_pipe(lambda: _PipeToProcess(self, 2), proc.stderr)
                self._pipes[2] = pipe
            loop.call_soon(self._protocol.connection_made, self)
            for callback, data in self._pending_calls:
                loop.call_soon(callback, *data)
            self._pending_calls = None
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            if waiter is not None and not waiter.cancelled():
                waiter.set_exception(exc)
        else:
            if waiter is not None and not waiter.cancelled():
                waiter.set_result(None)

    def _call(self, callback, *data):
        if self._pending_calls is not None:
            self._pending_calls.append((callback, data))
        else:
            self._loop.call_soon(callback, *data)

    def _pipe_data_received(self, fd, data):
        self._call(self._protocol.pipe_data_received, fd, data)

    def _pipe_connection_lost(self, fd, exc):
        self._call(self._protocol.pipe_connection_lost, fd, exc)
        self._try_finish()

    def _on_exit(self):
        if self._pidfd >= 0:
            self._loop.remove_reader(self._pidfd)
            os.close(self._pidfd)
            self._pidfd = -1
        if self._proc is None:
            return
        self._returncode = self._proc.wait()
        self._call(self._protocol.process_exited)
        for waiter in self._exit_waiters:
            if not waiter.cancelled():
                waiter.set_result(self._returncode)
        self._exit_waiters = None
        self._try_finish()

    def _try_finish(self):
        if self._returncode is None or self._finished:
            return
        if all(p is not None and p.disconnected for p in self._pipes.values()):
            self._finished = True
            self._call(self._call_connection_lost, None)

    def _call_connection_lost(self, exc):
        try:
            self._protocol.connection_lost(exc)
        finally:
            self._proc = None
            self._protocol = None

    async def _wait(self):
        """Used by `asyncio.subprocess.Process.wait()`."""
        if self._returncode is not None:
            return self._returncode
        waiter = self._loop.create_future()
        self._exit_waiters.append(waiter)
        return await waiter

    def get_pid(self):
        return self._pid

    def get_returncode(self):
        return self._returncode

    def get_pipe_transport(self, fd):
        proto = self._pipes.get(fd)
        return proto.pipe if proto is not None else None

    def _check_proc(self):
        if self._proc is None or self._returncode is not None:
            raise ProcessLookupError()

    def send_signal(self, signal):
        self._check_proc()
        self._proc.send_signal(signal)

    def terminate(self):
        self._check_proc()
        self._proc.terminate()

    def kill(self):
        self._check_proc()
        self._proc.kill()

    @loop_owned
    def set_protocol(self, protocol):
        self._protocol = protocol

    def get_protocol(self):
        return self._protocol

    def is_closing(self):
        return self._closed

    @loop_owned_close
    def close(self):
        if self._closed:
            return
        self._closed = True
        for proto in self._pipes.values():
            if proto is not None and proto.pipe is not None:
                proto.pipe.close()
        if self._proc is not None and self._returncode is None and self._proc.poll() is None:
            try:
                self._proc.kill()
            except (ProcessLookupError, PermissionError):
                pass

    def __del__(self, _warn=warnings.warn, _close=os.close):
        if self._pidfd >= 0:
            # The loop went away before the child did.
            try:
                _close(self._pidfd)
            except OSError:
                pass
            self._pidfd = -1
        if not self._closed and self._returncode is None and self._proc is not None:
            _warn(f"unclosed transport {self!r}", ResourceWarning, source=self)


class DatagramTransport(_FlowControl, asyncio.DatagramTransport, asyncio.Transport):
    max_size = 256 * 1024

    def __init__(self, loop, sock, protocol, address=None, waiter=None, extra=None):
        super().__init__(extra)
        self._extra["socket"] = TransportSocket(sock)  # as asyncio: a view that cannot close it
        try:
            self._extra["sockname"] = sock.getsockname()
        except OSError:
            self._extra["sockname"] = None
        try:
            # Set only for a connected socket: it decides send() or sendto().
            self._extra["peername"] = sock.getpeername()
        except OSError:
            self._extra["peername"] = None
        self._loop = loop
        self._sock = sock
        self._fileno = sock.fileno()
        self._protocol = protocol
        self._address = address
        self._buffer = collections.deque()
        self._buffer_size = 0
        self._closing = False
        self._conn_lost = 0
        loop.call_soon(protocol.connection_made, self)
        loop.call_soon(self._add_reader)
        if waiter is not None:
            loop.call_soon(_set_waiter, waiter)

    def _add_reader(self):
        if not self._closing:
            self._loop.add_reader(self._fileno, self._read_ready)

    def get_write_buffer_size(self):
        return self._buffer_size

    def _read_ready(self):
        if self._conn_lost:
            return
        try:
            data, addr = self._sock.recvfrom(self.max_size)
        except (BlockingIOError, InterruptedError):
            return
        except OSError as exc:
            self._protocol.error_received(exc)
            return
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            self._fatal_error(exc, "Fatal read error on datagram transport")
            return
        self._protocol.datagram_received(data, addr)

    @loop_owned
    def sendto(self, data, addr=None):
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError(f"data argument must be a bytes-like object, not {type(data).__name__!r}")
        if self._address:
            if addr not in (None, self._address):
                raise ValueError(f"Invalid address: must be None or {self._address}")
            addr = self._address
        if self._conn_lost and self._address:
            self._conn_lost += 1
            return
        if not self._buffer:
            try:
                if self._extra["peername"]:
                    self._sock.send(data)
                else:
                    self._sock.sendto(data, addr)
                return
            except (BlockingIOError, InterruptedError):
                self._loop.add_writer(self._fileno, self._write_ready)
            except OSError as exc:
                self._protocol.error_received(exc)
                return
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException as exc:
                self._fatal_error(exc, "Fatal write error on datagram transport")
                return
        self._buffer.append((bytes(data), addr))
        self._buffer_size += len(data) + 8
        self._maybe_pause()

    def _write_ready(self):
        while self._buffer:
            data, addr = self._buffer.popleft()
            self._buffer_size -= len(data) + 8
            try:
                if self._extra["peername"]:
                    self._sock.send(data)
                else:
                    self._sock.sendto(data, addr)
            except (BlockingIOError, InterruptedError):
                self._buffer.appendleft((data, addr))
                self._buffer_size += len(data) + 8
                break
            except OSError as exc:
                self._protocol.error_received(exc)
                return
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException as exc:
                self._fatal_error(exc, "Fatal write error on datagram transport")
                return
        self._maybe_resume()
        if not self._buffer:
            self._loop.remove_writer(self._fileno)
            if self._closing:
                self._call_connection_lost(None)

    @loop_owned
    def set_protocol(self, protocol):
        self._protocol = protocol

    def get_protocol(self):
        return self._protocol

    def is_closing(self):
        return self._closing

    @loop_owned_close
    def close(self):
        if self._closing:
            return
        self._closing = True
        self._loop.remove_reader(self._fileno)
        if not self._buffer:
            self._conn_lost += 1
            self._loop.call_soon(self._call_connection_lost, None)

    @loop_owned_close
    def abort(self):
        self._force_close(None)

    def _fatal_error(self, exc, message):
        if not isinstance(exc, OSError):
            self._loop.call_exception_handler({
                "message": message, "exception": exc,
                "transport": self, "protocol": self._protocol,
            })
        self._force_close(exc)

    def _force_close(self, exc):
        if self._conn_lost:
            return
        if self._buffer:
            self._buffer.clear()
            self._buffer_size = 0
            self._loop.remove_writer(self._fileno)
        if not self._closing:
            self._closing = True
            self._loop.remove_reader(self._fileno)
        self._conn_lost += 1
        self._loop.call_soon(self._call_connection_lost, exc)

    def _call_connection_lost(self, exc):
        if self._sock is None:
            return
        try:
            self._protocol.connection_lost(exc)
        finally:
            self._sock.close()
            self._sock = None
            self._protocol = None

    def __repr__(self):
        return _describe(self, self._fileno, self._sock is None)

    def __del__(self, _warn=warnings.warn):
        if getattr(self, "_sock", None) is not None:
            _warn(f"unclosed transport {self!r}", ResourceWarning, source=self)
            self._sock.close()


class _SendfileDrain(asyncio.Protocol):
    """Stands in for a transport's protocol while a file is copied through it."""

    def __init__(self, transport):
        self.transport = transport
        self.proto = transport.get_protocol()
        self.loop = asyncio.get_running_loop()
        # Input waits in the transport until the real protocol is back.
        self.was_reading = transport.is_reading()
        transport.pause_reading()
        # The transport does not repeat pause_writing() for us if it has
        # already told the protocol. Its own flag is the truth (it stays
        # paused until the buffer is down to the low-water mark); the buffer
        # size is only a guess for transports that do not show it.
        paused = getattr(transport, "_protocol_paused", None)
        if paused is None:
            paused = transport.get_write_buffer_size() > transport.get_write_buffer_limits()[1]
        self.proto_paused = bool(paused)
        self.paused = self.loop.create_future() if self.proto_paused else None
        transport.set_protocol(self)

    async def drain(self):
        if self.transport.is_closing():
            raise ConnectionError("Connection closed by peer")
        if self.paused is not None:
            await self.paused

    def pause_writing(self):
        if self.paused is None:
            self.paused = self.loop.create_future()

    def resume_writing(self):
        paused, self.paused = self.paused, None
        if paused is not None and not paused.done():
            paused.set_result(None)

    def connection_made(self, transport):
        raise RuntimeError("Invalid state: connection should have been established already.")

    def connection_lost(self, exc):
        if self.paused is not None and not self.paused.done():
            self.paused.set_exception(exc or ConnectionError("Connection is closed by peer"))
        self.proto.connection_lost(exc)

    def data_received(self, data):
        raise RuntimeError("Invalid state: reading should be paused")

    def eof_received(self):
        raise RuntimeError("Invalid state: reading should be paused")

    def restore(self):
        transport, proto = self.transport, self.proto
        transport.set_protocol(proto)
        paused = self.paused is not None
        if paused:
            self.paused.cancel()
            self.paused = None
        # Leave the protocol believing what the transport will act on: the
        # next thing the transport says is the opposite of its current state.
        if paused != self.proto_paused and not transport.is_closing():
            if paused:
                proto.pause_writing()
            else:
                proto.resume_writing()
        if self.was_reading:
            transport.resume_reading()


class ExtrasMixin:
    # -- pipes ---------------------------------------------------------------

    async def connect_read_pipe(self, protocol_factory, pipe):
        protocol = protocol_factory()
        waiter = self.create_future()
        transport = ReadPipeTransport(self, pipe, protocol, waiter)
        try:
            await waiter
        except BaseException:
            transport.close()
            raise
        finally:
            waiter = None  # the exception's traceback holds this frame: no cycle through the future
        return transport, protocol

    async def connect_write_pipe(self, protocol_factory, pipe):
        protocol = protocol_factory()
        waiter = self.create_future()
        transport = WritePipeTransport(self, pipe, protocol, waiter)
        try:
            await waiter
        except BaseException:
            transport.close()
            raise
        finally:
            waiter = None  # the exception's traceback holds this frame: no cycle through the future
        return transport, protocol

    # -- subprocesses --------------------------------------------------------

    async def _spawn(self, protocol_factory, args, shell, stdin, stdout, stderr, bufsize, kwargs):
        protocol = protocol_factory()
        waiter = self.create_future()
        transport = ProcessTransport(
            self, protocol, args, shell, stdin, stdout, stderr, bufsize, waiter, **kwargs)
        try:
            await waiter
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException:
            transport.close()
            await transport._wait()
            raise
        finally:
            waiter = None  # the exception's traceback holds this frame: no cycle through the future
        return transport, protocol

    async def subprocess_shell(self, protocol_factory, cmd, *, stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               universal_newlines=False, shell=True, bufsize=0,
                               encoding=None, errors=None, text=None, **kwargs):
        if not isinstance(cmd, (bytes, str)):
            raise ValueError("cmd must be a string")
        if universal_newlines:
            raise ValueError("universal_newlines must be False")
        if not shell:
            raise ValueError("shell must be True")
        if bufsize != 0:
            raise ValueError("bufsize must be 0")
        if text:
            raise ValueError("text must be False")
        if encoding is not None:
            raise ValueError("encoding must be None")
        if errors is not None:
            raise ValueError("errors must be None")
        return await self._spawn(protocol_factory, cmd, True, stdin, stdout, stderr, bufsize, kwargs)

    async def subprocess_exec(self, protocol_factory, program, *args, stdin=subprocess.PIPE,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              universal_newlines=False, shell=False, bufsize=0,
                              encoding=None, errors=None, text=None, **kwargs):
        if universal_newlines:
            raise ValueError("universal_newlines must be False")
        if shell:
            raise ValueError("shell must be False")
        if bufsize != 0:
            raise ValueError("bufsize must be 0")
        if text:
            raise ValueError("text must be False")
        if encoding is not None:
            raise ValueError("encoding must be None")
        if errors is not None:
            raise ValueError("errors must be None")
        return await self._spawn(protocol_factory, (program, *args), False,
                                 stdin, stdout, stderr, bufsize, kwargs)

    # -- datagrams -----------------------------------------------------------

    async def create_datagram_endpoint(
            self, protocol_factory, local_addr=None, remote_addr=None, *, family=0,
            proto=0, flags=0, reuse_port=None, allow_broadcast=None, sock=None):
        if sock is not None:
            if sock.type == socket.SOCK_STREAM:
                raise ValueError(f"A datagram socket was expected, got {sock!r}")
            if local_addr or remote_addr or family or proto or flags or reuse_port or allow_broadcast:
                raise ValueError("socket modifier keyword arguments can not be used when sock is specified.")
            sock.setblocking(False)
            address = None
        else:
            if not (local_addr or remote_addr):
                if family == 0:
                    raise ValueError("unexpected address family")
                pairs = [((family, proto), (None, None))]
            elif family == socket.AF_UNIX:
                for addr in (local_addr, remote_addr):
                    if addr is not None and not isinstance(addr, str):
                        raise TypeError("string is expected")
                if local_addr and local_addr[0] not in (0, "\x00"):
                    # Remove a stale socket file left by an earlier run.
                    try:
                        if stat.S_ISSOCK(os.stat(local_addr).st_mode):
                            os.remove(local_addr)
                    except FileNotFoundError:
                        pass
                    except OSError as err:
                        logging.getLogger("asyncio").error(
                            "Unable to check or remove stale UNIX socket %r: %r", local_addr, err)
                pairs = [((family, proto), (local_addr, remote_addr))]
            else:
                found = {}
                for index, addr in ((0, local_addr), (1, remote_addr)):
                    if addr is None:
                        continue
                    if not (isinstance(addr, tuple) and len(addr) == 2):
                        raise TypeError("2-tuple is expected")
                    infos = await self._resolve(*addr, family=family, type=socket.SOCK_DGRAM,
                                                proto=proto, flags=flags)
                    for fam, _, pro, _, address in infos:
                        found.setdefault((fam, pro), [None, None])[index] = address
                pairs = [
                    (key, pair) for key, pair in found.items()
                    if not ((local_addr and pair[0] is None) or (remote_addr and pair[1] is None))
                ]
                if not pairs:
                    raise ValueError("can not get address information")
            errors = []
            sock = None
            address = None
            for (fam, pro), (local, remote) in pairs:
                try:
                    sock = socket.socket(family=fam, type=socket.SOCK_DGRAM, proto=pro)
                    if reuse_port:
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
                    if allow_broadcast:
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                    sock.setblocking(False)
                    if local:
                        sock.bind(local)
                    if remote:
                        if not allow_broadcast:
                            await self.sock_connect(sock, remote)
                        address = remote
                except OSError as exc:
                    if sock is not None:
                        sock.close()
                    sock = None
                    errors.append(exc)
                except BaseException:
                    if sock is not None:
                        sock.close()
                    raise
                else:
                    break
            else:
                raise errors[0]
        protocol = protocol_factory()
        waiter = self.create_future()
        transport = DatagramTransport(self, sock, protocol, address, waiter)
        try:
            await waiter
        except BaseException:
            transport.close()
            raise
        finally:
            waiter = None  # the exception's traceback holds this frame: no cycle through the future
        return transport, protocol

    # -- sendfile ------------------------------------------------------------

    async def sock_sendfile(self, sock, file, offset=0, count=None, *, fallback=True):
        if self._debug and sock.gettimeout() != 0:
            raise ValueError("the socket must be non-blocking")
        if sock.type != socket.SOCK_STREAM:
            raise ValueError("only SOCK_STREAM type sockets are supported")
        _check_sendfile_args(file, offset, count)
        try:
            fileno = file.fileno()
            st = os.fstat(fileno)
            size = st.st_size
            if not stat.S_ISREG(st.st_mode):
                fileno = None  # a pipe or device has no size to go by
        except (AttributeError, OSError):
            fileno = None
        if fileno is None:
            if not fallback:
                raise asyncio.SendfileNotAvailableError("not a regular file")
            return await self._sock_sendfile_fallback(sock, file, offset, count)
        remaining = (size - offset) if count is None else min(count, size - offset)
        total = 0
        fd = sock.fileno()
        while remaining > 0:
            try:
                sent = os.sendfile(fd, fileno, offset, remaining)
            except (BlockingIOError, InterruptedError):
                await self._wait_writable(fd)
                continue
            except OSError as exc:
                if total == 0 and exc.errno in (errno.EINVAL, errno.ENOSYS, errno.ENOTSOCK):
                    if not fallback:
                        raise asyncio.SendfileNotAvailableError("os.sendfile call failed") from None
                    return await self._sock_sendfile_fallback(sock, file, offset, count)
                raise
            if sent == 0:
                break
            offset += sent
            total += sent
            remaining -= sent
        if total > 0:
            _seek(file, offset)
        return total

    async def _wait_writable(self, fd):
        future = self.create_future()
        handle = self.add_writer(fd, _set_waiter, future)
        try:
            await future
        finally:
            self._unwatch(fd, True, handle)

    async def _sock_sendfile_fallback(self, sock, file, offset, count):
        _seek_start(file, offset)
        blocksize = min(count, 256 * 1024) if count else 256 * 1024
        buf = bytearray(blocksize)
        total = 0
        try:
            while True:
                if count:
                    blocksize = min(count - total, blocksize)
                    if blocksize <= 0:
                        break
                view = memoryview(buf)[:blocksize]
                read = await self.run_in_executor(None, file.readinto, view)
                if not read:
                    break
                await self.sock_sendall(sock, view[:read])
                total += read
            return total
        finally:
            if total > 0:
                _seek(file, offset + total)

    async def sendfile(self, transport, file, offset=0, count=None, *, fallback=True):
        if transport.is_closing():
            raise RuntimeError("Transport is closing")
        if not getattr(transport, "_sendfile_compatible", False):
            raise RuntimeError(f"sendfile is not supported for transport {transport!r}")
        _check_sendfile_args(file, offset, count)
        if type(transport) is StreamTransport:
            # A plain stream transport: sendfile(2) straight into its socket.
            try:
                return await self._sendfile_native(transport, file, offset, count)
            except asyncio.SendfileNotAvailableError:
                if not fallback:
                    raise
        elif not fallback:
            raise asyncio.SendfileNotAvailableError(
                f"fallback is disabled and native sendfile is not supported for transport {transport!r}")
        return await self._sendfile_fallback(transport, file, offset, count)

    async def _sendfile_native(self, transport, file, offset, count):
        try:
            fileno = file.fileno()
            st = os.fstat(fileno)
        except (AttributeError, OSError):
            raise asyncio.SendfileNotAvailableError("not a regular file") from None
        if not stat.S_ISREG(st.st_mode):
            raise asyncio.SendfileNotAvailableError("not a regular file")
        remaining = st.st_size - offset
        if count is not None:
            remaining = min(count, remaining)
        if remaining <= 0:
            _seek(file, offset)
            return 0
        transport._sendfile_begin()  # from here write() raises
        was_reading = transport.is_reading()
        transport.pause_reading()    # input waits, as under asyncio
        total = 0
        try:
            if transport.get_write_buffer_size():
                waiter = self.create_future()
                transport._sendfile_wait(waiter, False)
                await waiter
            while remaining > 0:
                try:
                    sent = transport._sendfile_send(fileno, offset, remaining)
                except OSError as exc:
                    if total == 0 and exc.errno in (errno.EINVAL, errno.ENOSYS):
                        raise asyncio.SendfileNotAvailableError("os.sendfile call failed") from None
                    raise
                if sent < 0:
                    waiter = self.create_future()
                    transport._sendfile_wait(waiter, True)
                    await waiter
                elif sent == 0:
                    break  # the file is shorter than its size said
                else:
                    offset += sent
                    total += sent
                    remaining -= sent
            return total
        finally:
            waiter = None
            transport._sendfile_end()
            _seek(file, offset)  # where the copy stopped, also after an error
            if was_reading:
                transport.resume_reading()

    async def _sendfile_fallback(self, transport, file, offset, count):
        _seek_start(file, offset)
        blocksize = min(count, 64 * 1024) if count else 64 * 1024
        buf = bytearray(blocksize)
        total = 0
        drain = _SendfileDrain(transport)
        try:
            while True:
                if count:
                    blocksize = min(count - total, blocksize)
                    if blocksize <= 0:
                        return total
                view = memoryview(buf)[:blocksize]
                read = await self.run_in_executor(None, file.readinto, view)
                if not read:
                    return total
                transport.write(view[:read])
                await drain.drain()
                total += read
        finally:
            if total > 0:
                _seek(file, offset + total)
            drain.restore()


def _check_sendfile_args(file, offset, count):
    if "b" not in getattr(file, "mode", "b"):
        raise ValueError("file should be opened in binary mode")
    if count is not None:
        if not isinstance(count, int):
            raise TypeError(f"count must be a positive integer (got {count!r})")
        if count <= 0:
            raise ValueError(f"count must be a positive integer (got {count!r})")
    if not isinstance(offset, int):
        raise TypeError(f"offset must be a non-negative integer (got {offset!r})")
    if offset < 0:
        raise ValueError(f"offset must be a non-negative integer (got {offset!r})")


def _seek_start(file, offset):
    """Start reading at `offset`, wherever the file is now. A file that
    cannot seek is read from where it is, if `offset` is 0."""
    if offset:
        file.seek(offset)
    else:
        try:
            file.seek(0)
        except (AttributeError, OSError):
            pass


def _seek(file, position):
    """Leave the file where the copy stopped, if it can seek at all."""
    try:
        file.seek(position)
    except (AttributeError, OSError):
        pass


def _set_waiter(waiter):
    if not waiter.done():
        waiter.set_result(None)
