"""Transport, accept, and loop-integration behaviour, on both backends."""

import asyncio
import gc
import os
import shutil
import signal
import socket
import ssl
import subprocess
import threading
import time

import pytest

import ringloop

HOST = "127.0.0.1"


@pytest.fixture(params=["uring", "epoll"])
def backend(request):
    if request.param == "uring":
        loop = ringloop.new_event_loop(backend="auto")
        try:
            if loop.backend != "uring":
                pytest.skip("io_uring not available here")
        finally:
            loop.close()
    return request.param


def run(coro, backend):
    return ringloop.run(asyncio.wait_for(coro, 30), backend=backend)


def core_of(loop):
    return loop._core


async def settle():
    for _ in range(5):
        await asyncio.sleep(0.01)


class Recorder(asyncio.Protocol):
    def __init__(self):
        self.events = []
        self.data = bytearray()
        self.transport = None
        self.lost = asyncio.get_running_loop().create_future()
        self.eof = asyncio.get_running_loop().create_future()

    def connection_made(self, transport):
        self.transport = transport
        self.events.append("made")

    def data_received(self, data):
        self.data += data

    def eof_received(self):
        self.events.append("eof")
        if not self.eof.done():
            self.eof.set_result(None)
        return getattr(self, "keep_open", False)

    def pause_writing(self):
        self.events.append("pause")

    def resume_writing(self):
        self.events.append("resume")

    def connection_lost(self, exc):
        self.events.append(("lost", exc))
        if not self.lost.done():
            self.lost.set_result(exc)


async def serve(factory, **kw):
    loop = asyncio.get_running_loop()
    server = await loop.create_server(factory, HOST, 0, **kw)
    return server, server.sockets[0].getsockname()[1]


def test_backend_reported(backend):
    async def main():
        return asyncio.get_running_loop().backend

    assert run(main(), backend) == backend


def test_streams_echo(backend):
    async def main():
        async def handle(r, w):
            while data := await r.readline():
                w.write(b"echo:" + data)
                await w.drain()
            w.close()

        server = await asyncio.start_server(handle, HOST, 0)
        port = server.sockets[0].getsockname()[1]
        r, w = await asyncio.open_connection(HOST, port)
        for i in range(50):
            w.write(b"line %d\n" % i)
            assert await r.readline() == b"echo:line %d\n" % i
        w.close()
        await w.wait_closed()
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_large_transfer_both_ways_with_backpressure(backend):
    size = 24 * 1024 * 1024

    async def main():
        async def handle(r, w):
            data = await r.readexactly(size)
            w.write(data[::-1])
            await w.drain()
            w.close()

        server = await asyncio.start_server(handle, HOST, 0)
        port = server.sockets[0].getsockname()[1]
        r, w = await asyncio.open_connection(HOST, port)
        payload = os.urandom(size)
        w.write(payload)
        await w.drain()
        back = await r.readexactly(size)
        assert back == payload[::-1]
        assert await r.read() == b""
        w.close()
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_pause_writing_and_resume_writing(backend):
    async def main():
        loop = asyncio.get_running_loop()
        made = loop.create_future()

        def factory():
            p = Recorder()
            made.set_result(p)
            return p

        server, port = await serve(factory)
        # A raw client that does not read yet.
        sock = socket.create_connection((HOST, port))
        sock.setblocking(False)
        proto = await made
        total = 16 * 1024 * 1024
        proto.transport.write(b"z" * total)
        assert "pause" in proto.events
        assert proto.transport.get_write_buffer_size() > 0
        got = 0
        while got < total:
            got += len(await loop.sock_recv(sock, 1 << 20))
        await settle()
        assert proto.events.count("resume") == 1
        assert proto.transport.get_write_buffer_size() == 0
        sock.close()
        await proto.lost
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_buffered_protocol(backend):
    class Buffered(asyncio.BufferedProtocol):
        def __init__(self):
            self.buf = bytearray(7)  # small on purpose: one read spans many
            self.view = memoryview(self.buf)
            self.got = bytearray()

        def connection_made(self, transport):
            self.transport = transport

        def get_buffer(self, sizehint):
            return self.view

        def buffer_updated(self, nbytes):
            self.got += self.buf[:nbytes]
            if self.got.endswith(b"\n"):
                self.transport.write(bytes(self.got).upper())
                self.got.clear()

        def eof_received(self):
            return False

        def connection_lost(self, exc):
            pass

    async def main():
        server, port = await serve(Buffered)
        r, w = await asyncio.open_connection(HOST, port)
        msg = b"the quick brown fox jumps over the lazy dog " * 40 + b"\n"
        w.write(msg)
        assert await r.readexactly(len(msg)) == msg.upper()
        w.close()
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_pause_reading_holds_data_until_resume(backend):
    async def main():
        loop = asyncio.get_running_loop()
        made = loop.create_future()

        def factory():
            p = Recorder()
            made.set_result(p)
            return p

        server, port = await serve(factory)
        r, w = await asyncio.open_connection(HOST, port)
        proto = await made
        w.write(b"one")
        await settle()
        assert bytes(proto.data) == b"one"
        proto.transport.pause_reading()
        assert not proto.transport.is_reading()
        chunks = [b"two" * 1000, b"three" * 5000, b"four" * 20000]
        for c in chunks:
            w.write(c)
            await settle()
        assert bytes(proto.data) == b"one", "data delivered while paused"
        proto.transport.resume_reading()
        expect = b"one" + b"".join(chunks)
        for _ in range(200):
            if len(proto.data) == len(expect):
                break
            await asyncio.sleep(0.01)
        assert bytes(proto.data) == expect
        # pause + resume inside one turn loses nothing
        proto.transport.pause_reading()
        proto.transport.resume_reading()
        w.write(b"five")
        await settle()
        assert bytes(proto.data) == expect + b"five"
        # EOF while paused is delivered after resume
        proto.transport.pause_reading()
        w.write(b"six")
        w.close()
        await settle()
        assert "eof" not in proto.events
        proto.transport.resume_reading()
        await proto.lost
        assert bytes(proto.data) == expect + b"fivesix"
        assert "eof" in proto.events
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_backpressure_reaches_the_peer_while_paused(backend):
    async def main():
        loop = asyncio.get_running_loop()
        made = loop.create_future()

        def factory():
            p = Recorder()
            made.set_result(p)
            return p

        server, port = await serve(factory)
        sock = socket.create_connection((HOST, port))
        sock.setblocking(False)
        proto = await made
        proto.transport.pause_reading()
        sent = 0
        chunk = b"q" * 65536
        stalled = 0
        while stalled < 20 and sent < 256 * 1024 * 1024:
            try:
                sent += sock.send(chunk)
                stalled = 0
            except BlockingIOError:
                stalled += 1
                await asyncio.sleep(0.01)
        # The sender stalls: the paused side must not swallow unbounded data.
        assert sent < 64 * 1024 * 1024
        proto.transport.resume_reading()
        for _ in range(500):
            if len(proto.data) == sent:
                break
            await asyncio.sleep(0.01)
        assert len(proto.data) == sent
        sock.close()
        await proto.lost
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_half_close(backend):
    async def main():
        loop = asyncio.get_running_loop()
        made = loop.create_future()

        def factory():
            p = Recorder()
            p.keep_open = True
            made.set_result(p)
            return p

        server, port = await serve(factory)
        r, w = await asyncio.open_connection(HOST, port)
        proto = await made
        w.write(b"request")
        w.write_eof()
        await proto.eof
        assert bytes(proto.data) == b"request"
        assert not proto.transport.is_closing()
        proto.transport.write(b"reply after eof")
        proto.transport.write_eof()
        with pytest.raises(RuntimeError):
            proto.transport.write(b"more")
        assert await r.read() == b"reply after eof"
        proto.transport.close()
        assert await proto.lost is None
        w.close()
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_close_flushes_pending_output(backend):
    size = 8 * 1024 * 1024

    async def main():
        class Dump(asyncio.Protocol):
            def connection_made(self, transport):
                transport.write(b"a" * size)
                transport.writelines([b"b", bytearray(b"c"), memoryview(b"d")])
                transport.close()
                transport.write(b"ignored after close")
                assert transport.is_closing()

        server, port = await serve(Dump)
        r, w = await asyncio.open_connection(HOST, port)
        data = await r.read()
        assert len(data) == size + 3 and data.endswith(b"bcd")
        w.close()
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_abort_drops_output_and_reports_none(backend):
    async def main():
        loop = asyncio.get_running_loop()
        made = loop.create_future()

        def factory():
            p = Recorder()
            made.set_result(p)
            return p

        server, port = await serve(factory)
        sock = socket.create_connection((HOST, port))
        proto = await made
        proto.transport.write(b"x" * (32 * 1024 * 1024))  # peer never reads
        await settle()
        proto.transport.abort()
        assert await proto.lost is None
        await settle()
        sock.settimeout(5)
        seen = 0
        try:
            while chunk := sock.recv(1 << 20):
                seen += len(chunk)
        except ConnectionResetError:
            pass
        assert seen < 32 * 1024 * 1024
        sock.close()
        server.close()
        await server.wait_closed()
        await settle()
        assert not core_of(loop).live_count()

    run(main(), backend)


def test_peer_reset_reports_error_or_eof(backend):
    async def main():
        loop = asyncio.get_running_loop()
        made = loop.create_future()

        def factory():
            p = Recorder()
            made.set_result(p)
            return p

        server, port = await serve(factory)
        sock = socket.create_connection((HOST, port))
        proto = await made
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, b"\x01\x00\x00\x00\x00\x00\x00\x00")
        sock.close()  # RST
        exc = await proto.lost
        assert exc is None or isinstance(exc, ConnectionError)
        proto.transport.write(b"after loss")  # must not raise
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_protocol_exception_closes_connection(backend):
    async def main():
        loop = asyncio.get_running_loop()
        errors = []
        loop.set_exception_handler(lambda loop, ctx: errors.append(ctx))
        made = loop.create_future()

        class Boom(Recorder):
            def data_received(self, data):
                raise ValueError("boom")

        def factory():
            p = Boom()
            made.set_result(p)
            return p

        server, port = await serve(factory)
        r, w = await asyncio.open_connection(HOST, port)
        proto = await made
        w.write(b"x")
        exc = await proto.lost
        assert isinstance(exc, ValueError)
        assert errors and isinstance(errors[0]["exception"], ValueError)
        assert await r.read() == b""
        w.close()
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_extra_info(backend):
    async def main():
        loop = asyncio.get_running_loop()
        made = loop.create_future()

        def factory():
            p = Recorder()
            made.set_result(p)
            return p

        server, port = await serve(factory)
        r, w = await asyncio.open_connection(HOST, port)
        proto = await made
        t = proto.transport
        assert t.get_extra_info("sockname") == (HOST, port)
        assert t.get_extra_info("peername") == w.get_extra_info("sockname")
        assert t.get_extra_info("socket").getsockname() == (HOST, port)
        assert t.get_extra_info("ssl_object") is None
        assert t.get_extra_info("nope", 7) == 7
        assert t.get_protocol() is proto
        # still works after the socket object was created
        w.write(b"ping")
        await settle()
        assert bytes(proto.data) == b"ping"
        t.write(b"pong")
        assert await r.readexactly(4) == b"pong"
        w.close()
        await proto.lost
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_many_connections_and_no_leak(backend):
    n = 600

    async def main():
        loop = asyncio.get_running_loop()

        async def handle(r, w):
            data = await r.readexactly(16)
            w.write(data * 64)
            await w.drain()
            w.close()

        server = await asyncio.start_server(handle, HOST, 0, backlog=1024)
        port = server.sockets[0].getsockname()[1]

        async def client(i):
            r, w = await asyncio.open_connection(HOST, port)
            msg = b"%016d" % i
            w.write(msg)
            assert await r.read() == msg * 64
            w.close()
            await w.wait_closed()

        for _round in range(3):
            await asyncio.gather(*(client(i) for i in range(n)))
        server.close()
        await server.wait_closed()
        await settle()
        gc.collect()
        assert core_of(loop).live_count() == 0

    run(main(), backend)


def test_server_close_with_open_connections(backend):
    async def main():
        loop = asyncio.get_running_loop()
        protos = []

        def factory():
            p = Recorder()
            protos.append(p)
            return p

        server, port = await serve(factory)
        conns = [await asyncio.open_connection(HOST, port) for _ in range(5)]
        await settle()
        assert len(protos) == 5
        server.close()
        with pytest.raises(OSError):
            await asyncio.open_connection(HOST, port)
        # existing connections keep working after the listener is gone
        conns[0][1].write(b"still here")
        await settle()
        assert bytes(protos[0].data) == b"still here"
        server.close_clients()
        await server.wait_closed()
        assert all(p.lost.done() for p in protos)
        for _r, w in conns:
            w.close()

    run(main(), backend)


def test_unix_socket_server(backend, tmp_path):
    path = str(tmp_path / "s.sock")

    async def main():
        async def handle(r, w):
            w.write((await r.readline()).upper())
            await w.drain()
            w.close()

        server = await asyncio.start_unix_server(handle, path)
        r, w = await asyncio.open_unix_connection(path)
        w.write(b"unix\n")
        assert await r.readline() == b"UNIX\n"
        w.close()
        server.close()
        await server.wait_closed()

    run(main(), backend)


@pytest.fixture(scope="session")
def tls(tmp_path_factory):
    openssl = shutil.which("openssl")
    if openssl is None:
        pytest.skip("openssl not installed")
    d = tmp_path_factory.mktemp("tls")
    cert, key = str(d / "cert.pem"), str(d / "key.pem")
    subprocess.run(
        [openssl, "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
         "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
         "-keyout", key, "-out", cert],
        check=True, capture_output=True,
    )
    server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_ctx.load_cert_chain(cert, key)
    client_ctx = ssl.create_default_context(cafile=cert)
    return server_ctx, client_ctx


def test_tls_echo_large(backend, tls):
    server_ctx, client_ctx = tls
    size = 4 * 1024 * 1024

    async def main():
        async def handle(r, w):
            assert w.get_extra_info("ssl_object") is not None
            data = await r.readexactly(size)
            w.write(data)
            await w.drain()
            w.close()

        server = await asyncio.start_server(handle, HOST, 0, ssl=server_ctx)
        port = server.sockets[0].getsockname()[1]
        r, w = await asyncio.open_connection(HOST, port, ssl=client_ctx, server_hostname="localhost")
        payload = os.urandom(size)
        w.write(payload)
        await w.drain()
        assert await r.readexactly(size) == payload
        w.close()
        await w.wait_closed()
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_add_reader_writer_and_sock_api(backend):
    async def main():
        loop = asyncio.get_running_loop()
        a, b = socket.socketpair()
        a.setblocking(False)
        b.setblocking(False)
        got = loop.create_future()
        loop.add_reader(a, lambda: got.set_result(a.recv(100)))
        await loop.sock_sendall(b, b"hello")
        assert await got == b"hello"
        assert loop.remove_reader(a)
        writable = loop.create_future()
        loop.add_writer(a, lambda: writable.done() or writable.set_result(True))
        assert await writable
        assert loop.remove_writer(a)
        # level-triggered: unread data keeps firing
        b.send(b"abcdef")
        hits = []
        done = loop.create_future()

        def on_read():
            hits.append(a.recv(2))
            if len(hits) == 3:
                loop.remove_reader(a)
                done.set_result(None)

        loop.add_reader(a, on_read)
        await done
        assert hits == [b"ab", b"cd", b"ef"]
        a.close()
        b.close()

    run(main(), backend)


def test_threads_signals_subprocess_timers(backend):
    async def main():
        loop = asyncio.get_running_loop()
        # call_soon_threadsafe wakes a sleeping loop
        fut = loop.create_future()
        t0 = time.monotonic()
        threading.Timer(0.05, lambda: loop.call_soon_threadsafe(fut.set_result, 1)).start()
        assert await fut == 1
        assert time.monotonic() - t0 < 1.0
        assert await asyncio.to_thread(sum, [1, 2, 3]) == 6
        # signals
        sig = loop.create_future()
        loop.add_signal_handler(signal.SIGUSR1, sig.set_result, "usr1")
        os.kill(os.getpid(), signal.SIGUSR1)
        assert await sig == "usr1"
        loop.remove_signal_handler(signal.SIGUSR1)
        # subprocess
        proc = await asyncio.create_subprocess_exec(
            "/bin/sh", "-c", "cat; echo done", stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        out, _ = await proc.communicate(b"piped ")
        assert out == b"piped done\n" and proc.returncode == 0
        # timers
        t0 = loop.time()
        await asyncio.sleep(0.1)
        assert 0.09 <= loop.time() - t0 < 0.3
        order = []
        loop.call_later(0.03, order.append, 3)
        loop.call_later(0.01, order.append, 1)
        loop.call_later(0.02, order.append, 2)
        await asyncio.sleep(0.08)
        assert order == [1, 2, 3]

    run(main(), backend)


def test_connections_made_by_client_api_use_native_transport(backend):
    async def main():
        loop = asyncio.get_running_loop()
        server, port = await serve(Recorder)
        transport, proto = await loop.create_connection(Recorder, HOST, port)
        assert type(transport).__module__.startswith("ringloop")
        transport.close()
        await proto.lost
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_loop_close_with_open_connections(backend):
    loop = ringloop.new_event_loop(backend=backend)

    async def main():
        server, port = await serve(Recorder)
        await asyncio.open_connection(HOST, port)
        await settle()

    loop.run_until_complete(main())
    loop.close()
    assert loop.is_closed()


def test_subprocess_exec_shell_and_kill(backend):
    async def main():
        proc = await asyncio.create_subprocess_shell(
            "echo out; echo err 1>&2; exit 3", stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = await proc.communicate()
        assert (out, err, proc.returncode) == (b"out\n", b"err\n", 3)
        # large data through stdin/stdout
        proc = await asyncio.create_subprocess_exec(
            "cat", stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        blob = os.urandom(3 * 1024 * 1024)
        out, _ = await proc.communicate(blob)
        assert out == blob and proc.returncode == 0
        # no pipes at all
        proc = await asyncio.create_subprocess_exec("true")
        assert await proc.wait() == 0
        # kill a sleeper
        proc = await asyncio.create_subprocess_exec("sleep", "30")
        proc.kill()
        assert await proc.wait() == -signal.SIGKILL

    run(main(), backend)


def test_pipes(backend):
    async def main():
        loop = asyncio.get_running_loop()
        r, w = os.pipe()
        reader = asyncio.StreamReader()
        rt, _ = await loop.connect_read_pipe(
            lambda: asyncio.StreamReaderProtocol(reader), os.fdopen(r, "rb", 0))
        wt, proto = await loop.connect_write_pipe(Recorder, os.fdopen(w, "wb", 0))
        blob = os.urandom(2 * 1024 * 1024)
        wt.write(blob)
        got = await reader.readexactly(len(blob))
        assert got == blob
        wt.close()
        assert await reader.read() == b""
        await proto.lost
        rt.close()

    run(main(), backend)


def test_datagram_endpoint(backend):
    class Echo(asyncio.DatagramProtocol):
        def connection_made(self, transport):
            self.transport = transport

        def datagram_received(self, data, addr):
            self.transport.sendto(data.upper(), addr)

    class Client(asyncio.DatagramProtocol):
        def __init__(self):
            self.got = asyncio.get_running_loop().create_future()

        def datagram_received(self, data, addr):
            self.got.set_result(data)

    async def main():
        loop = asyncio.get_running_loop()
        st, _ = await loop.create_datagram_endpoint(Echo, local_addr=(HOST, 0))
        port = st.get_extra_info("sockname")[1]
        ct, client = await loop.create_datagram_endpoint(Client, remote_addr=(HOST, port))
        ct.sendto(b"datagram")
        assert await client.got == b"DATAGRAM"
        ct.close()
        st.close()
        await settle()

    run(main(), backend)


def test_sendfile(backend, tmp_path):
    path = tmp_path / "blob"
    blob = os.urandom(5 * 1024 * 1024)
    path.write_bytes(blob)

    async def main():
        loop = asyncio.get_running_loop()

        async def handle(r, w):
            with open(path, "rb") as f:
                sent = await loop.sendfile(w.transport, f)
            assert sent == len(blob)
            w.close()

        server = await asyncio.start_server(handle, HOST, 0)
        port = server.sockets[0].getsockname()[1]
        r, w = await asyncio.open_connection(HOST, port)
        assert await r.read() == blob
        w.close()
        # sock_sendfile
        a, b = socket.socketpair()
        a.setblocking(False)
        b.setblocking(False)
        with open(path, "rb") as f:
            task = asyncio.ensure_future(loop.sock_sendfile(a, f, 100, 1_000_000))
            got = bytearray()
            while len(got) < 1_000_000:
                got += await loop.sock_recv(b, 1 << 20)
            assert await task == 1_000_000
        assert bytes(got) == blob[100:1_000_100]
        a.close()
        b.close()
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_run_forever_stop_close_semantics(backend):
    loop = ringloop.new_event_loop(backend=backend)
    order = []
    loop.call_soon(order.append, 1)
    loop.call_soon(loop.stop)
    loop.call_soon(order.append, 2)
    loop.run_forever()
    assert order == [1, 2]
    loop.stop()  # stop before run: one iteration
    loop.call_soon(order.append, 3)
    loop.run_forever()
    assert order == [1, 2, 3]

    async def boom():
        raise ValueError("x")

    with pytest.raises(ValueError):
        loop.run_until_complete(boom())
    assert loop.run_until_complete(asyncio.sleep(0, "v")) == "v"
    h = loop.call_later(10, order.append, 4)
    assert h.when() > loop.time() and not h.cancelled()
    h.cancel()
    assert h.cancelled()
    loop.close()
    loop.close()
    with pytest.raises(RuntimeError):
        loop.call_soon(order.append, 5)
    with pytest.raises(RuntimeError):
        loop.run_forever()


def test_many_timers_and_cancellation(backend):
    async def main():
        loop = asyncio.get_running_loop()
        fired = []
        handles = [loop.call_later(0.02 + (i % 50) * 0.001, fired.append, i) for i in range(2000)]
        for h in handles[::2]:
            h.cancel()
        await asyncio.sleep(0.2)
        assert sorted(fired) == list(range(1, 2000, 2))
        whens = [handles[i].when() for i in fired]
        assert whens == sorted(whens)
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.02):
                await asyncio.sleep(1)
        assert await asyncio.wait_for(asyncio.sleep(0.01, "ok"), 1) == "ok"

    run(main(), backend)


def test_exception_handler_task_factory_debug(backend):
    async def main():
        loop = asyncio.get_running_loop()
        seen = []
        loop.set_exception_handler(lambda l, ctx: seen.append(ctx))
        loop.call_soon(lambda: 1 / 0)
        await asyncio.sleep(0.01)
        assert isinstance(seen[0]["exception"], ZeroDivisionError)
        loop.set_exception_handler(None)
        made = []

        def factory(loop, coro, **kw):
            made.append(coro)
            return asyncio.Task(coro, loop=loop, **kw)

        loop.set_task_factory(factory)
        assert loop.get_task_factory() is factory
        assert await asyncio.create_task(asyncio.sleep(0, 5)) == 5
        assert made
        loop.set_task_factory(None)
        t = loop.create_task(asyncio.sleep(0, 6), name="named")
        assert t.get_name() == "named" and await t == 6

        async def agen():
            try:
                yield 1
                yield 2
            finally:
                seen.append("agen closed")

        g = agen()
        assert await g.__anext__() == 1
        await loop.shutdown_asyncgens()
        assert "agen closed" in seen

    run(main(), backend)
    ringloop.run(asyncio.sleep(0), debug=True, backend=backend)


def test_tls_details(backend, tls):
    server_ctx, client_ctx = tls

    async def main():
        loop = asyncio.get_running_loop()
        seen = {}

        async def handle(r, w):
            seen["cipher"] = w.get_extra_info("cipher")
            seen["peername"] = w.get_extra_info("peername")
            line = await r.readline()
            w.write(line.upper())
            await w.drain()
            seen["rest"] = await r.read()  # client close_notify -> EOF
            w.close()
            await w.wait_closed()

        server = await asyncio.start_server(handle, HOST, 0, ssl=server_ctx)
        port = server.sockets[0].getsockname()[1]
        r, w = await asyncio.open_connection(HOST, port, ssl=client_ctx, server_hostname="localhost")
        assert w.get_extra_info("ssl_object").version().startswith("TLS")
        assert w.get_extra_info("peercert")
        w.write(b"hello\n")
        assert await r.readline() == b"HELLO\n"
        w.close()
        await w.wait_closed()
        await settle()
        assert seen["rest"] == b"" and seen["cipher"] and seen["peername"][0] == HOST

        # a client that does not trust the certificate fails cleanly
        with pytest.raises(ssl.SSLError):
            await asyncio.open_connection(HOST, port, ssl=ssl.create_default_context(),
                                          server_hostname="localhost")
        # a plaintext client on a TLS port is dropped, the server lives on
        r, w = await asyncio.open_connection(HOST, port)
        w.write(b"GET / HTTP/1.1\r\n\r\n")
        assert await r.read() is not None
        w.close()
        r, w = await asyncio.open_connection(HOST, port, ssl=client_ctx, server_hostname="localhost")
        w.write(b"again\n")
        assert await r.readline() == b"AGAIN\n"
        w.close()
        await w.wait_closed()
        server.close()
        await server.wait_closed()
        await settle()
        assert core_of(loop).live_count() == 0

    run(main(), backend)


def test_tls_handshake_timeout(backend, tls):
    server_ctx, _ = tls

    async def main():
        server = await asyncio.start_server(lambda r, w: None, HOST, 0, ssl=server_ctx,
                                            ssl_handshake_timeout=0.2)
        port = server.sockets[0].getsockname()[1]
        r, w = await asyncio.open_connection(HOST, port)  # says nothing
        t0 = time.monotonic()
        assert await r.read() == b""
        assert 0.1 < time.monotonic() - t0 < 2
        w.close()
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_start_tls(backend, tls):
    server_ctx, client_ctx = tls

    async def main():
        loop = asyncio.get_running_loop()

        async def handle(r, w):
            assert await r.readline() == b"STARTTLS\n"
            w.write(b"OK\n")
            await w.drain()
            await w.start_tls(server_ctx)
            w.write((await r.readline()).upper())
            await w.drain()
            w.close()

        server = await asyncio.start_server(handle, HOST, 0)
        port = server.sockets[0].getsockname()[1]
        r, w = await asyncio.open_connection(HOST, port)
        w.write(b"STARTTLS\n")
        assert await r.readline() == b"OK\n"
        await w.start_tls(client_ctx, server_hostname="localhost")
        w.write(b"secret\n")
        assert await r.readline() == b"SECRET\n"
        w.close()
        server.close()
        await server.wait_closed()

    run(main(), backend)
