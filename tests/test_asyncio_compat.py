"""Behaviour asyncio programs rely on. Most of these were found by running
CPython's own asyncio test suite on ringloop (conformance/run_cpython.py)."""

import asyncio
import contextvars
import gc
import logging
import os
import socket
import ssl
import struct
import warnings
import weakref

import pytest

import ringloop

from .test_loop import HOST, Recorder, backend, run, serve, tls  # noqa: F401  (fixtures)


# -- the end of a connection is never missed -------------------------------------

@pytest.mark.parametrize("kind,how", [
    ("unix", "close"), ("unix", "shutdown"),
    ("tcp", "close"), ("tcp", "shutdown"), ("tcp", "reset"),
])
def test_data_followed_at_once_by_the_end(backend, kind, how):
    # The peer writes and closes (or resets) before the loop looks: both
    # events reach an idle multishot recv as one wake-up. The data must be
    # delivered and then the end, not the data alone.
    async def main():
        loop = asyncio.get_running_loop()
        if kind == "unix":
            ours, peer = socket.socketpair()
        else:
            with socket.socket() as listener:
                listener.bind((HOST, 0))
                listener.listen()
                peer = socket.socket()
                peer.connect(listener.getsockname())
                ours, _ = listener.accept()
        transport, proto = await loop.connect_accepted_socket(Recorder, ours)
        await asyncio.sleep(0.05)  # the recv is armed and idle
        peer.send(b"x" * 100)
        if how == "close":
            peer.close()
        elif how == "shutdown":
            peer.shutdown(socket.SHUT_WR)
        else:
            peer.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            peer.close()
        exc = await asyncio.wait_for(proto.lost, 5)
        peer.close()
        assert bytes(proto.data) == b"x" * 100
        if how == "reset":
            assert isinstance(exc, ConnectionResetError)
        else:
            assert exc is None and "eof" in proto.events

    run(main(), backend)


def test_reset_while_reading_is_paused_is_seen_on_resume(backend):
    async def main():
        loop = asyncio.get_running_loop()
        with socket.socket() as listener:
            listener.bind((HOST, 0))
            listener.listen()
            peer = socket.socket()
            peer.connect(listener.getsockname())
            ours, _ = listener.accept()
        transport, proto = await loop.connect_accepted_socket(Recorder, ours)
        await asyncio.sleep(0.05)
        transport.pause_reading()
        peer.send(b"abc")
        peer.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        peer.close()
        await asyncio.sleep(0.1)
        transport.resume_reading()
        await asyncio.wait_for(proto.lost, 5)

    run(main(), backend)


# -- ordering ------------------------------------------------------------------

def test_create_connection_returns_before_the_first_data(backend):
    # A protocol may use what the caller does with the transport right after
    # `await create_connection()`: no data_received() before that.
    async def main():
        loop = asyncio.get_running_loop()
        order = []

        class Greeter(asyncio.Protocol):
            def connection_made(self, transport):
                transport.write(b"hello")  # in the client's socket before it looks

        class Client(asyncio.Protocol):
            def __init__(self):
                self.got = loop.create_future()

            def data_received(self, data):
                order.append("data")
                self.got.set_result(data)

        server, port = await serve(Greeter)
        # Connect by hand and let the greeting arrive first.
        sock = socket.socket()
        sock.setblocking(False)
        await loop.sock_connect(sock, (HOST, port))
        await asyncio.sleep(0.05)
        transport, proto = await loop.create_connection(Client, sock=sock)
        order.append("returned")
        assert await asyncio.wait_for(proto.got, 5) == b"hello"
        assert order == ["returned", "data"]
        transport.close()
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_close_after_write_finishes_without_another_event(backend):
    # Nothing else happens on the loop: the flush at the start of the I/O
    # turn is what makes connection_lost() ready, and the loop must not go
    # to sleep on top of it.
    async def main():
        loop = asyncio.get_running_loop()
        ours, peer = socket.socketpair()
        with peer:
            transport, proto = await loop.create_connection(Recorder, sock=ours)
            transport.write(b"data")
            transport.close()
            started = loop.time()
            await asyncio.wait_for(proto.lost, 5)
            assert loop.time() - started < 1
            assert peer.recv(10) == b"data"

    run(main(), backend)


def test_cancelled_sock_recv_does_not_unregister_the_next_one(backend):
    async def main():
        loop = asyncio.get_running_loop()
        a, b = socket.socketpair()
        a.setblocking(False)
        with a, b:
            first = asyncio.create_task(loop.sock_recv(a, 10))
            await asyncio.sleep(0)
            first.cancel()  # its clean-up runs after the next call has registered
            second = asyncio.create_task(loop.sock_recv(a, 10))
            await asyncio.sleep(0.01)
            b.send(b"ping")
            assert await asyncio.wait_for(second, 5) == b"ping"

    run(main(), backend)


# -- exception handler -----------------------------------------------------------

def test_exception_handler_runs_in_the_context_of_the_failing_callback(backend):
    var = contextvars.ContextVar("var", default="outside")

    async def main():
        loop = asyncio.get_running_loop()
        seen = []
        loop.set_exception_handler(lambda loop, context: seen.append(var.get()))

        def callback():
            var.set("callback")
            1 / 0

        async def task():
            var.set("task")
            1 / 0

        loop.call_soon(callback)
        t = asyncio.create_task(task())
        await asyncio.sleep(0.01)
        del t
        gc.collect()  # "Task exception was never retrieved"
        await asyncio.sleep(0.01)
        assert sorted(seen) == ["callback", "task"]

    run(main(), backend)


# -- debug mode ----------------------------------------------------------------

def test_debug_mode_logs_slow_callbacks_and_checks_callbacks(backend, caplog):
    async def main():
        loop = asyncio.get_running_loop()
        assert loop.get_debug()
        loop.slow_callback_duration = 0.01

        def slow():
            import time
            time.sleep(0.05)

        handle = loop.call_soon(slow)
        assert "created at" in repr(handle) and handle._source_traceback
        await asyncio.sleep(0.1)

        async def coro():
            pass

        with pytest.raises(TypeError, match="coroutines cannot be used"):
            loop.call_soon(coro)
        with pytest.raises(TypeError, match="callable object was expected"):
            loop.call_later(1, None)

    with caplog.at_level(logging.WARNING, logger="asyncio"):
        ringloop.run(main(), backend=backend, debug=True)
    assert any("Executing" in r.getMessage() and "slow" in r.getMessage() for r in caplog.records)


def test_no_debug_overhead_objects_without_debug(backend):
    async def main():
        loop = asyncio.get_running_loop()
        handle = loop.call_soon(lambda: None)
        assert handle._source_traceback is None
        assert "created at" not in repr(handle)

    ringloop.run(main(), backend=backend, debug=False)


def test_unclosed_loop_warns():
    loop = ringloop.new_event_loop()
    with pytest.warns(ResourceWarning, match="unclosed event loop"):
        del loop
        gc.collect()


# -- what code written for asyncio checks ------------------------------------------

def test_transports_are_asyncio_transports(backend, tls):
    server_ctx, client_ctx = tls

    async def main():
        loop = asyncio.get_running_loop()
        server, port = await serve(Recorder)
        transport, _ = await loop.create_connection(Recorder, HOST, port)
        assert isinstance(transport, asyncio.Transport)
        assert isinstance(transport, asyncio.BaseTransport)
        assert type(transport).__name__ == "Transport"
        transport.close()
        # Listening sockets are shown as asyncio shows them.
        assert all(isinstance(s, asyncio.trsock.TransportSocket) for s in server.sockets)
        server.close()
        await server.wait_closed()

        tls_server = await loop.create_server(Recorder, HOST, 0, ssl=server_ctx)
        port = tls_server.sockets[0].getsockname()[1]
        transport, _ = await loop.create_connection(
            Recorder, HOST, port, ssl=client_ctx, server_hostname="localhost")
        assert isinstance(transport, asyncio.Transport)
        transport.close()
        tls_server.close()
        await tls_server.wait_closed()

        transport, _ = await loop.create_datagram_endpoint(asyncio.DatagramProtocol, local_addr=(HOST, 0))
        assert isinstance(transport, asyncio.DatagramTransport)
        assert "open" in repr(transport)
        transport.close()

    run(main(), backend)


def test_serve_forever_cancelled_does_not_wait_for_idle_clients(backend):
    async def main():
        loop = asyncio.get_running_loop()
        connected = asyncio.Event()

        class Proto(asyncio.Protocol):
            def connection_made(self, transport):
                connected.set()

        server = await loop.create_server(Proto, HOST, 0)
        port = server.sockets[0].getsockname()[1]
        serving = asyncio.create_task(server.serve_forever())
        with socket.socket() as client:  # connects and then says nothing
            client.connect((HOST, port))
            await connected.wait()
            serving.cancel()
            await asyncio.wait_for(server.wait_closed(), 2)
        assert not server.is_serving()

    run(main(), backend)


# -- TLS -------------------------------------------------------------------------

def test_tls_empty_server_hostname_disables_the_name_check(backend, tls):
    server_ctx, _ = tls
    client_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    client_ctx.check_hostname = False
    client_ctx.verify_mode = ssl.CERT_NONE

    async def main():
        async def handle(reader, writer):
            writer.write(b"ok")
            writer.close()

        server = await asyncio.start_server(handle, HOST, 0, ssl=server_ctx)
        port = server.sockets[0].getsockname()[1]
        reader, writer = await asyncio.open_connection(HOST, port, ssl=client_ctx, server_hostname="")
        assert await reader.read() == b"ok"
        writer.close()
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_failed_transport_creation_closes_the_socket(backend):
    async def main():
        loop = asyncio.get_running_loop()
        server, port = await serve(Recorder)
        ctx = ssl.create_default_context()
        with warnings.catch_warnings():
            warnings.simplefilter("error", ResourceWarning)
            with pytest.raises(ValueError):
                # A host name OpenSSL refuses: fails after the connect.
                await loop.create_connection(Recorder, HOST, port, ssl=ctx, server_hostname=".bad")
            gc.collect()
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_tls_leaves_no_garbage(backend, tls):
    server_ctx, _ = tls

    def client_context():
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx

    class Keeper(asyncio.Protocol):
        def __init__(self):
            self.lost = asyncio.get_running_loop().create_future()

        def connection_made(self, transport):
            self.transport = transport  # protocol <-> transport, as most protocols have it

        def connection_lost(self, exc):
            self.lost.set_result(None)

    async def main():
        loop = asyncio.get_running_loop()
        server = await loop.create_server(Recorder, HOST, 0, ssl=server_ctx)
        port = server.sockets[0].getsockname()[1]

        ctx = client_context()
        transport, proto = await loop.create_connection(Keeper, HOST, port, ssl=ctx)
        transport.close()
        await proto.lost
        ctx_ref, transport_ref = weakref.ref(ctx), weakref.ref(transport)
        del ctx, transport, proto
        await asyncio.sleep(0.01)
        assert ctx_ref() is None and transport_ref() is None

        # A handshake that fails: the raw transport goes without the collector.
        verifying = ssl.create_default_context()  # does not trust the test certificate
        raw, proto = await loop.create_connection(asyncio.Protocol, HOST, port)
        raw_ref = weakref.ref(raw)
        try:
            await loop.start_tls(raw, proto, verifying, server_hostname="localhost")
        except ssl.SSLCertVerificationError:
            pass
        else:
            raise AssertionError("the handshake should have failed")
        del raw, proto
        await asyncio.sleep(0.05)
        assert raw_ref() is None
        server.close()
        await server.wait_closed()

    gc.collect()
    gc.disable()
    try:
        run(main(), backend)
    finally:
        gc.enable()


# -- Happy Eyeballs ----------------------------------------------------------------

def test_happy_eyeballs_tries_the_next_address(backend):
    async def main():
        loop = asyncio.get_running_loop()
        server, port = await serve(Recorder)
        dead = socket.socket()
        dead.bind((HOST, 0))  # bound but not listening: refuses
        dead_port = dead.getsockname()[1]
        attempts = []

        async def getaddrinfo(host, port_, **kw):
            return [
                (socket.AF_INET, socket.SOCK_STREAM, 6, "", (HOST, dead_port)),
                (socket.AF_INET, socket.SOCK_STREAM, 6, "", (HOST, port)),
            ]

        loop.getaddrinfo = getaddrinfo
        connect_one = loop._connect_one

        async def spy(info, local_infos):
            attempts.append(info[4][1])
            return await connect_one(info, local_infos)

        loop._connect_one = spy
        for delay in (None, 0.25):
            attempts.clear()
            transport, _ = await loop.create_connection(
                Recorder, "example.invalid", 80, happy_eyeballs_delay=delay)
            assert transport.get_extra_info("peername")[1] == port
            assert attempts == [dead_port, port]
            transport.close()
        with pytest.raises(ExceptionGroup):
            async def only_dead(host, port_, **kw):
                return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (HOST, dead_port))] * 2
            loop.getaddrinfo = only_dead
            await loop.create_connection(Recorder, "example.invalid", 80,
                                         happy_eyeballs_delay=0.1, all_errors=True)
        dead.close()
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_interleave_address_families():
    from ringloop.loop import _interleave_addrinfos
    v4 = [(socket.AF_INET, 1, 6, "", (f"10.0.0.{i}", 80)) for i in range(3)]
    v6 = [(socket.AF_INET6, 1, 6, "", (f"::{i}", 80, 0, 0)) for i in range(2)]
    assert _interleave_addrinfos(v6 + v4) == [v6[0], v4[0], v6[1], v4[1], v4[2]]
    assert _interleave_addrinfos(v6 + v4, 2) == [v6[0], v6[1], v4[0], v4[1], v4[2]]


# -- sendfile ----------------------------------------------------------------------

@pytest.fixture
def big_file(tmp_path):
    path = tmp_path / "data.bin"
    data = os.urandom(3 * 1024 * 1024 + 123)
    path.write_bytes(data)
    return path, data


class Sink(asyncio.Protocol):
    def __init__(self):
        self.data = bytearray()
        self.lost = asyncio.get_running_loop().create_future()

    def data_received(self, data):
        self.data += data

    def connection_lost(self, exc):
        self.lost.set_result(None)


@pytest.mark.parametrize("fallback", [True, False])
@pytest.mark.parametrize("offset,count", [(0, None), (1000, None), (5, 70000), (0, 1)])
def test_sendfile_native(backend, big_file, fallback, offset, count):
    path, data = big_file

    async def main():
        loop = asyncio.get_running_loop()
        sinks = []
        server, port = await serve(lambda: sinks.append(Sink()) or sinks[-1])
        transport, _ = await loop.create_connection(asyncio.Protocol, HOST, port)
        transport.write(b"head:")
        with open(path, "rb") as f:
            f.seek(77)  # where the file happens to be does not matter
            sent = await loop.sendfile(transport, f, offset, count, fallback=fallback)
            expected = data[offset:] if count is None else data[offset:offset + count]
            assert sent == len(expected)
            assert f.tell() == offset + sent
        transport.write(b":tail")
        transport.close()
        await asyncio.wait_for(sinks[0].lost, 10)
        assert bytes(sinks[0].data) == b"head:" + expected + b":tail"
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_sendfile_refuses_writes_and_other_transports(backend, big_file):
    path, data = big_file

    async def main():
        loop = asyncio.get_running_loop()
        sinks = []
        server, port = await serve(lambda: sinks.append(Sink()) or sinks[-1])
        transport, _ = await loop.create_connection(asyncio.Protocol, HOST, port)
        with open(path, "rb") as f:
            task = asyncio.create_task(loop.sendfile(transport, f))
            await asyncio.sleep(0)
            with pytest.raises(RuntimeError, match="sendfile is in progress"):
                transport.write(b"in the middle")
            assert await task == len(data)
        transport.write(b"after")  # allowed again
        transport.close()
        await asyncio.wait_for(sinks[0].lost, 10)
        assert bytes(sinks[0].data) == data + b"after"

        udp, _ = await loop.create_datagram_endpoint(asyncio.DatagramProtocol, local_addr=(HOST, 0))
        with open(path, "rb") as f:
            with pytest.raises(RuntimeError, match="not supported"):
                await loop.sendfile(udp, f)
        udp.close()
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_sendfile_cancelled_gives_the_write_side_back(backend, big_file):
    path, _ = big_file

    async def main():
        loop = asyncio.get_running_loop()
        server, port = await serve(Recorder)
        sock = socket.create_connection((HOST, port))  # reads nothing for now
        while not server._clients:
            await asyncio.sleep(0.01)
        (transport,) = server._clients
        with open(path, "rb") as f:
            task = loop.create_task(loop.sendfile(transport, f))
            await asyncio.sleep(0.2)  # the socket is full: waiting for "writable"
            assert not task.done()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert 0 < f.tell() < os.path.getsize(path)  # left where the copy stopped
            sent = f.tell()
        assert transport.is_reading()
        transport.write(b"END")
        transport.close()
        sock.settimeout(10)
        received = bytearray()
        while chunk := sock.recv(1 << 20):
            received += chunk
            await asyncio.sleep(0)
        sock.close()
        assert len(received) == sent + 3 and received.endswith(b"END")
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_sendfile_to_a_peer_that_goes_away(backend, big_file):
    path, _ = big_file

    async def main():
        loop = asyncio.get_running_loop()

        class Deaf(asyncio.Protocol):
            def connection_made(self, transport):
                transport.pause_reading()
                loop.call_later(0.1, transport.abort)

        server, port = await serve(Deaf)
        transport, _ = await loop.create_connection(asyncio.Protocol, HOST, port)
        with open(path, "rb") as f:
            with pytest.raises(OSError):
                await asyncio.wait_for(loop.sendfile(transport, f), 10)
        transport.abort()
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_sendfile_over_tls_uses_the_fallback(backend, tls, big_file):
    server_ctx, client_ctx = tls
    path, data = big_file

    async def main():
        loop = asyncio.get_running_loop()
        sinks = []
        server = await loop.create_server(lambda: sinks.append(Sink()) or sinks[-1], HOST, 0, ssl=server_ctx)
        port = server.sockets[0].getsockname()[1]
        transport, _ = await loop.create_connection(
            asyncio.Protocol, HOST, port, ssl=client_ctx, server_hostname="localhost")
        with open(path, "rb") as f:
            f.seek(50)
            with pytest.raises(asyncio.SendfileNotAvailableError):
                await loop.sendfile(transport, f, fallback=False)
            assert await loop.sendfile(transport, f) == len(data)
        transport.close()
        await asyncio.wait_for(sinks[0].lost, 20)
        assert bytes(sinks[0].data) == data
        server.close()
        await server.wait_closed()

    run(main(), backend)


# -- rings -------------------------------------------------------------------------

def _ring_fds():
    count = 0
    for fd in os.listdir("/proc/self/fd"):
        try:
            if "io_uring" in os.readlink(f"/proc/self/fd/{fd}"):
                count += 1
        except OSError:
            pass
    return count


def test_a_loop_without_sockets_creates_no_ring():
    before = _ring_fds()

    async def main():
        loop = asyncio.get_running_loop()
        await asyncio.sleep(0.01)
        await loop.run_in_executor(None, sum, [1, 2])
        proc = await asyncio.create_subprocess_exec("true")
        await proc.wait()
        return _ring_fds()

    assert ringloop.run(main()) == before


def test_many_loops_fit_in_the_default_memlock_limit(backend):
    # Ring memory counts against RLIMIT_MEMLOCK (8 MiB by default): a process
    # with one loop per thread must still get a ring for each.
    if backend != "uring":
        pytest.skip("io_uring only")
    resource = pytest.importorskip("resource")
    soft, _ = resource.getrlimit(resource.RLIMIT_MEMLOCK)
    if soft != resource.RLIM_INFINITY and soft < 8 * 1024 * 1024:
        pytest.skip("RLIMIT_MEMLOCK is below the usual 8 MiB")
    loops = [ringloop.new_event_loop() for _ in range(40)]
    try:
        assert [loop.backend for loop in loops] == ["uring"] * 40
    finally:
        for loop in loops:
            loop.close()


# -- the package ---------------------------------------------------------------------

def test_loop_factory_and_version():
    assert ringloop.NATIVE and ringloop.__version__

    async def main():
        return type(asyncio.get_running_loop())

    assert asyncio.run(main(), loop_factory=ringloop.new_event_loop) is ringloop.Loop


def test_other_systems_get_asyncios_loop():
    # The engine is Linux-only; elsewhere the same program runs on asyncio's loop.
    import subprocess
    import sys
    import textwrap
    script = textwrap.dedent("""
        import sys
        sys.platform = "darwin"
        import asyncio, ringloop
        assert not ringloop.NATIVE
        ringloop.install()  # nothing to install here
        assert "ringloop._core" not in sys.modules
        async def main():
            await asyncio.sleep(0)
            return type(asyncio.get_running_loop()).__module__
        assert ringloop.run(main()).startswith("asyncio.")
        loop = ringloop.new_event_loop()
        assert not isinstance(loop, ringloop.Loop)
        loop.close()
        try:
            ringloop.new_event_loop(backend="uring")
        except ValueError:
            pass
        else:
            raise SystemExit("backend= must be refused")
    """)
    proc = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=30)
    assert proc.returncode == 0, proc.stderr


def test_private_names_asyncio_tools_read(backend):
    # Not API, but what tests and tools written against asyncio look at.
    async def main():
        loop = asyncio.get_running_loop()
        server, port = await serve(Recorder)
        transport, _ = await loop.create_connection(Recorder, HOST, port)
        assert transport._paused is False and transport._protocol_paused is False
        transport.pause_reading()
        assert transport._paused is True
        transport.resume_reading()
        assert transport._paused is False
        transport.close()
        server.close()
        await server.wait_closed()

    run(main(), backend)


# -- found by review ---------------------------------------------------------------

def test_resume_writing_after_a_write_that_empties_the_buffer(backend):
    # Paused by one write; a later write in the same turn finds room in the
    # socket and sends everything. The protocol must hear resume_writing().
    async def main():
        loop = asyncio.get_running_loop()
        with socket.socket() as listener:
            listener.bind((HOST, 0))
            listener.listen()
            peer = socket.socket()
            peer.connect(listener.getsockname())
            ours, _ = listener.accept()
        peer.setblocking(False)
        transport, proto = await loop.connect_accepted_socket(Recorder, ours)
        await asyncio.sleep(0.01)
        transport.set_write_buffer_limits(high=0)
        transport.write(bytes(8 * 1024 * 1024))  # more than the socket takes
        assert proto.events.count("pause") == 1 and transport.get_write_buffer_size() > 0
        for _ in range(1000):  # no await: all of this is one turn of the loop
            try:
                while peer.recv(1 << 20):
                    pass
            except BlockingIOError:
                pass
            if transport.get_write_buffer_size() == 0:
                break
            transport.write(b"x")
        assert transport.get_write_buffer_size() == 0
        await asyncio.sleep(0.05)
        assert proto.events.count("resume") == 1
        transport.abort()
        peer.close()

    run(main(), backend)


def test_factory_without_a_protocol_does_not_close_a_descriptor_twice(backend):
    async def main():
        loop = asyncio.get_running_loop()
        errors = []
        loop.set_exception_handler(lambda loop, context: errors.append(context))
        server, port = await serve(lambda: None)
        client = socket.create_connection((HOST, port))
        while not errors:
            await asyncio.sleep(0.01)
        # Whatever now gets the number of the refused connection is not ours
        # to close.
        files = [open(os.devnull, "rb") for _ in range(8)]
        await asyncio.sleep(0.05)
        for f in files:
            os.fstat(f.fileno())
            f.close()
        client.close()
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_happy_eyeballs_when_the_first_address_hangs(backend):
    async def main():
        loop = asyncio.get_running_loop()
        server, port = await serve(Recorder)

        async def getaddrinfo(host, port_, **kw):
            return [
                (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("192.0.2.1", 9)),  # stands for one that hangs
                (socket.AF_INET, socket.SOCK_STREAM, 6, "", (HOST, port)),
            ]

        loop.getaddrinfo = getaddrinfo
        connect_one = loop._connect_one

        async def slow_first(info, local_infos):
            if info[4][0] == "192.0.2.1":
                await asyncio.sleep(30)
            return await connect_one(info, local_infos)

        loop._connect_one = slow_first
        transport, _ = await asyncio.wait_for(
            loop.create_connection(Recorder, "example.invalid", 80, happy_eyeballs_delay=0.05), 5)
        assert transport.get_extra_info("peername")[1] == port
        transport.close()
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_datagram_transport_over_a_connected_socket(backend):
    async def main():
        loop = asyncio.get_running_loop()
        got = loop.create_future()

        class Receiver(asyncio.DatagramProtocol):
            def datagram_received(self, data, addr):
                got.set_result(data)

        receiver, _ = await loop.create_datagram_endpoint(Receiver, local_addr=(HOST, 0))
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(receiver.get_extra_info("sockname"))
        sender, _ = await loop.create_datagram_endpoint(asyncio.DatagramProtocol, sock=sock)
        assert sender.get_extra_info("peername") == receiver.get_extra_info("sockname")
        sender.sendto(b"hello")  # no address: the socket is connected
        assert await asyncio.wait_for(got, 5) == b"hello"
        sender.close()
        receiver.close()

    run(main(), backend)


def test_tls_write_of_a_wrong_type_raises_and_keeps_the_connection(backend, tls):
    server_ctx, client_ctx = tls

    async def main():
        loop = asyncio.get_running_loop()
        server = await loop.create_server(Recorder, HOST, 0, ssl=server_ctx)
        port = server.sockets[0].getsockname()[1]
        transport, proto = await loop.create_connection(
            Recorder, HOST, port, ssl=client_ctx, server_hostname="localhost")
        with pytest.raises(TypeError):
            transport.write("text")
        assert not transport.is_closing()
        # ssl=True with an empty name: no name check, the certificate still counts.
        with pytest.raises(ssl.SSLCertVerificationError):
            await loop.create_connection(Recorder, HOST, port, ssl=True, server_hostname="")
        transport.close()
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_task_factory_and_task_names(backend):
    async def main():
        loop = asyncio.get_running_loop()
        made = []

        def factory(loop, coro, **kwargs):
            made.append(kwargs)
            return asyncio.Task(coro, loop=loop, **kwargs)

        loop.set_task_factory(factory)
        task = loop.create_task(asyncio.sleep(0), name="named")
        assert task.get_name() == "named"
        await task

    run(main(), backend)


def test_run_until_complete_inside_another_loop_schedules_nothing(backend):
    other = ringloop.new_event_loop(backend=backend)
    ran = []

    async def never():
        ran.append(1)

    async def main():
        coro = never()
        with pytest.raises(RuntimeError, match="another loop is running"):
            other.run_until_complete(coro)
        coro.close()

    run(main(), backend)
    other.run_until_complete(asyncio.sleep(0.01))
    other.close()
    assert ran == []


def test_a_server_cannot_start_on_a_closed_loop(backend):
    async def make():
        return await asyncio.get_running_loop().create_server(Recorder, HOST, 0, start_serving=False)

    loop = ringloop.new_event_loop(backend=backend)
    server = loop.run_until_complete(make())
    assert loop.backend == backend
    socks = list(server._sockets)
    loop.close()
    with pytest.raises(RuntimeError, match="closed"):
        server._start_serving()
    for sock in socks:
        sock.close()
