"""TLS over a ringloop transport.

`SSLTransport` is both sides of the sandwich in one object: the protocol
the raw transport feeds ciphertext to, and the transport the application
protocol writes plaintext to. OpenSSL runs on two memory BIOs
(`SSLContext.wrap_bio`), so no socket is involved here.
"""

import asyncio
import ssl

from ._owner import loop_owned, loop_owned_close

HANDSHAKE_TIMEOUT = 60.0
SHUTDOWN_TIMEOUT = 30.0
READ_SIZE = 16384   # one TLS record at most: what a single read() can return

# States
_HANDSHAKE = 1
_OPEN = 2
_SHUTDOWN = 3   # our close_notify is out (or on its way); waiting for the peer
_CLOSED = 4

_WANT = (ssl.SSLWantReadError, ssl.SSLWantWriteError)


class SSLTransport(asyncio.Transport):
    def __init__(self, loop, app_protocol, sslcontext, waiter=None, server_side=False,
                 server_hostname=None, handshake_timeout=None, shutdown_timeout=None,
                 call_connection_made=True):
        super().__init__()
        if handshake_timeout is None:
            handshake_timeout = HANDSHAKE_TIMEOUT
        elif handshake_timeout <= 0:
            raise ValueError(f"ssl_handshake_timeout should be a positive number, got {handshake_timeout}")
        if shutdown_timeout is None:
            shutdown_timeout = SHUTDOWN_TIMEOUT
        elif shutdown_timeout <= 0:
            raise ValueError(f"ssl_shutdown_timeout should be a positive number, got {shutdown_timeout}")
        self._loop = loop
        self._waiter = waiter
        self._server_side = server_side
        self._handshake_timeout = handshake_timeout
        self._shutdown_timeout = shutdown_timeout
        self._call_connection_made = call_connection_made
        self._incoming = ssl.MemoryBIO()
        self._outgoing = ssl.MemoryBIO()
        self._sslobj = sslcontext.wrap_bio(
            self._incoming, self._outgoing, server_side=server_side,
            # An empty name means "do not check the host name", as in asyncio.
            server_hostname=server_hostname if server_hostname and not server_side else None)
        self._extra["sslcontext"] = sslcontext
        self._raw = None
        self._state = _HANDSHAKE
        self._timer = None
        self._closing = False
        self._app_made = False
        self._app_paused = False
        self._app_lost = False
        self._eof_seen = False
        self._backlog = None      # writes OpenSSL could not take yet
        self._set_app_protocol(app_protocol)

    def _set_app_protocol(self, protocol):
        self._app = protocol
        if isinstance(protocol, asyncio.BufferedProtocol):
            self._app_get_buffer = protocol.get_buffer
            self._app_buffer_updated = protocol.buffer_updated
            self._app_data_received = None
        else:
            self._app_get_buffer = None
            self._app_buffer_updated = None
            self._app_data_received = protocol.data_received

    # -- protocol side: called by the raw transport ---------------------------

    def connection_made(self, transport):
        self._raw = transport
        self._timer = self._loop.call_later(self._handshake_timeout, self._handshake_timed_out)
        self._do_handshake()

    def data_received(self, data):
        self._incoming.write(data)
        state = self._state
        if state == _OPEN:
            self._read_app()
        elif state == _HANDSHAKE:
            self._do_handshake()
        elif state == _SHUTDOWN:
            self._do_shutdown()

    def eof_received(self):
        # TCP closed without close_notify.
        state = self._state
        if state == _HANDSHAKE:
            self._fail_handshake(ConnectionResetError("SSL handshake was interrupted"))
        elif state == _OPEN:
            # What is still in OpenSSL's buffers first (after
            # resume_reading(), whose own read is only scheduled).
            self._read_app()
            if self._state == _OPEN:
                self._deliver_eof()
        return False  # the raw transport closes itself

    def connection_lost(self, exc):
        self._state = _CLOSED
        self._closing = True
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        waiter, self._waiter = self._waiter, None
        if waiter is not None and not waiter.done():
            waiter.set_exception(exc if exc is not None else ConnectionResetError())
        try:
            if self._app_made and not self._app_lost:
                self._app_lost = True
                self._app.connection_lost(exc)
        finally:
            # Break the reference cycles (a protocol usually keeps its
            # transport): nothing here may need the garbage collector to go.
            self._backlog = None
            self._raw = None
            self._app = None
            self._app_get_buffer = self._app_buffer_updated = self._app_data_received = None

    @property
    def _protocol_paused(self):
        """Whether the application protocol is between pause_writing() and
        resume_writing(): the raw transport's calls are passed straight on."""
        raw = self._raw
        return bool(raw is not None and self._app_made and getattr(raw, "_protocol_paused", False))

    def pause_writing(self):
        if self._app_made and not self._app_lost:
            self._app.pause_writing()

    def resume_writing(self):
        if self._app_made and not self._app_lost:
            self._app.resume_writing()

    # -- handshake -----------------------------------------------------------

    def _do_handshake(self):
        try:
            self._sslobj.do_handshake()
        except _WANT:
            self._flush()
            return
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            self._flush()  # the alert, if OpenSSL produced one
            self._fail_handshake(exc)
            return
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        self._flush()
        sslobj = self._sslobj
        self._extra.update(
            peercert=sslobj.getpeercert(), cipher=sslobj.cipher(),
            compression=sslobj.compression(), ssl_object=sslobj)
        self._state = _OPEN
        self._app_made = True
        if self._call_connection_made:
            try:
                self._app.connection_made(self)
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException as exc:
                self._fatal_error(exc, "protocol.connection_made() call failed.")
                return
        waiter, self._waiter = self._waiter, None
        if waiter is not None and not waiter.done():
            waiter.set_result(None)
        if self._incoming.pending and self._state == _OPEN:
            self._read_app()

    def _handshake_timed_out(self):
        self._timer = None
        if self._state == _HANDSHAKE:
            self._fail_handshake(ConnectionAbortedError(
                f"SSL handshake is taking longer than {self._handshake_timeout} seconds: "
                f"aborting the connection"))

    def _fail_handshake(self, exc):
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        waiter, self._waiter = self._waiter, None
        if waiter is not None and not waiter.done():
            waiter.set_exception(exc)
        elif not isinstance(exc, OSError):
            self._loop.call_exception_handler({
                "message": "SSL handshake failed", "exception": exc,
                "transport": self._raw, "protocol": self,
            })
        self._state = _CLOSED
        self._closing = True
        raw = self._raw
        if raw is not None:
            if self._outgoing.pending == 0 and raw.get_write_buffer_size() == 0:
                raw.abort()
            else:
                raw.close()  # let the alert out first

    # -- reading -------------------------------------------------------------

    def _read_app(self):
        """Decrypt what is buffered and hand it to the application."""
        if self._app_paused or self._state != _OPEN:
            return
        sslobj = self._sslobj
        eof = False
        try:
            incoming = self._incoming
            if self._app_get_buffer is not None:
                get_buffer = self._app_get_buffer
                buffer_updated = self._app_buffer_updated
                while True:
                    buf = get_buffer(-1)
                    size = len(buf)
                    if not size:
                        raise RuntimeError("get_buffer() returned an empty buffer")
                    n = sslobj.read(size, buf)
                    if n == 0:
                        eof = True
                        break
                    buffer_updated(n)
                    if self._app_paused or self._state != _OPEN:
                        break
                    if not incoming.pending and not sslobj.pending():
                        break  # nothing left to decrypt
                    if self._app_get_buffer is not get_buffer:
                        # buffer_updated() handed the connection to another
                        # protocol: the rest is its input.
                        return self._read_app()
            else:
                first = None
                rest = None
                try:
                    while True:
                        chunk = sslobj.read(READ_SIZE)
                        if not chunk:
                            eof = True
                            break
                        if first is None:
                            first = chunk
                        elif rest is None:
                            rest = [first, chunk]
                        else:
                            rest.append(chunk)
                        if not incoming.pending and not sslobj.pending():
                            # Nothing left to decrypt: asking again would only
                            # raise SSLWantReadError.
                            break
                except _WANT:
                    pass
                except ssl.SSLZeroReturnError:
                    eof = True
                if rest is not None:
                    self._app_data_received(b"".join(rest))
                elif first is not None:
                    self._app_data_received(first)
        except _WANT:
            pass
        except ssl.SSLZeroReturnError:
            eof = True
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            self._fatal_error(exc, "Fatal error on SSL protocol")
            return
        if self._outgoing.pending:
            self._flush()
        if self._backlog is not None:
            self._retry_backlog()
        if eof and self._state == _OPEN:
            self._deliver_eof()

    def _deliver_eof(self):
        """The peer is done sending (close_notify or TCP EOF)."""
        if self._eof_seen:
            return
        self._eof_seen = True
        try:
            keep_open = self._app.eof_received()
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            self._fatal_error(exc, "protocol.eof_received() call failed.")
            return
        if keep_open:
            self._loop.call_exception_handler({
                "message": "returning true from eof_received() has no effect when using ssl",
                "protocol": self._app,
            })
        self.close()

    @loop_owned
    def pause_reading(self):
        if self._app_paused:
            return
        self._app_paused = True
        if self._raw is not None:
            self._raw.pause_reading()

    @loop_owned
    def resume_reading(self):
        if not self._app_paused:
            return
        self._app_paused = False
        if self._raw is not None:
            self._raw.resume_reading()
            if self._state == _OPEN:
                self._loop.call_soon(self._read_app)

    def is_reading(self):
        return not self._app_paused and not self._closing

    # -- writing -------------------------------------------------------------

    def _flush(self):
        outgoing = self._outgoing
        if outgoing.pending and self._raw is not None:
            self._raw.write(outgoing.read())

    @loop_owned
    def write(self, data):
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError(f"data: expecting a bytes-like instance, got {type(data).__name__}")
        if data and self._state == _OPEN and not self._closing:
            self._write(data)

    @loop_owned
    def writelines(self, list_of_data):
        if self._state != _OPEN or self._closing:
            return
        # Joined once (the argument may be a one-shot iterator): one TLS
        # record for the lot.
        data = b"".join(list_of_data)
        if data:
            self._write(data)

    def _write(self, data):
        if self._backlog is not None:
            self._backlog.append(bytes(data))
            return
        try:
            self._sslobj.write(data)
        except _WANT:
            # Only while the peer renegotiates (TLS 1.2): OpenSSL takes no
            # application data until it has read more. Retried after input.
            self._backlog = [bytes(data)]
            self._flush()
            return
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            self._fatal_error(exc, "Fatal error on SSL protocol")
            return
        outgoing = self._outgoing
        if outgoing.pending:
            self._raw.write(outgoing.read())

    def _retry_backlog(self):
        backlog, self._backlog = self._backlog, None
        for i, data in enumerate(backlog):
            try:
                self._sslobj.write(data)
            except _WANT:
                self._backlog = backlog[i:]
                break
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException as exc:
                self._fatal_error(exc, "Fatal error on SSL protocol")
                return
        self._flush()

    def can_write_eof(self):
        return False

    def write_eof(self):
        raise NotImplementedError("TLS does not support half-closed connections")

    def get_write_buffer_size(self):
        raw = self._raw
        size = self._outgoing.pending + (raw.get_write_buffer_size() if raw is not None else 0)
        backlog = self._backlog
        if backlog is not None:
            size += sum(map(len, backlog))
        return size

    def get_write_buffer_limits(self):
        raw = self._raw
        if raw is None:
            return (16384, 65536)  # the connection is gone: asyncio's defaults
        return raw.get_write_buffer_limits()

    @loop_owned
    def set_write_buffer_limits(self, high=None, low=None):
        raw = self._raw
        if raw is not None:
            raw.set_write_buffer_limits(high, low)

    # -- closing -------------------------------------------------------------

    def is_closing(self):
        return self._closing

    @loop_owned_close
    def close(self):
        if self._closing:
            return
        self._closing = True
        raw = self._raw
        if raw is None:
            return
        if self._state != _OPEN:
            raw.abort()
            return
        self._state = _SHUTDOWN
        self._timer = self._loop.call_later(self._shutdown_timeout, self._shutdown_timed_out)
        if self._app_paused:
            # The peer's close_notify still has to be read.
            raw.resume_reading()
        self._do_shutdown()

    def _do_shutdown(self):
        try:
            self._sslobj.unwrap()
        except _WANT:
            # Our close_notify is queued; the peer's has not arrived yet.
            self._flush()
            if self._eof_seen or self._raw is None:
                self._finish_shutdown()
            return
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException:
            pass  # a broken shutdown still ends in closing the socket
        self._flush()
        self._finish_shutdown()

    def _finish_shutdown(self):
        # The TLS part is over; the raw transport closes once its buffer has
        # gone out. The timer keeps running until then (connection_lost
        # cancels it): a peer that does not read must not hold the
        # connection open for good.
        if self._raw is not None:
            self._raw.close()

    def _shutdown_timed_out(self):
        self._timer = None
        raw = self._raw
        if raw is not None:
            # connection_lost(TimeoutError), as asyncio reports it
            raw._force_close(TimeoutError("SSL shutdown timed out"))

    @loop_owned_close
    def abort(self):
        self._force_close(None)

    def _force_close(self, exc):
        """Drop the connection now; `connection_lost(exc)` follows. (Also
        what a TLS layer on top of this one calls.)"""
        self._closing = True
        if self._raw is not None:
            self._raw._force_close(exc)

    def _fatal_error(self, exc, message):
        if not isinstance(exc, OSError):
            self._loop.call_exception_handler({
                "message": message, "exception": exc,
                "transport": self, "protocol": self._app,
            })
        self._closing = True
        raw = self._raw
        if raw is not None:
            raw._force_close(exc)

    # -- info ----------------------------------------------------------------

    def get_extra_info(self, name, default=None):
        # May run on any thread: read _raw once (connection_lost clears it).
        if name in self._extra:
            return self._extra[name]
        raw = self._raw
        if raw is not None:
            return raw.get_extra_info(name, default)
        return default

    def get_protocol(self):
        return self._app

    @loop_owned
    def set_protocol(self, protocol):
        self._set_app_protocol(protocol)

    @property
    def _start_tls_compatible(self):
        return True

    _sendfile_compatible = True  # `loop.sendfile` copies through write()
