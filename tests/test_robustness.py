"""Edge cases: signals, threads, queue overflow, API misuse, cleanup."""

import asyncio
import gc
import os
import socket
import ssl
import subprocess
import sys
import textwrap
import threading
import time

import pytest

import ringloop

from .test_loop import HOST, Recorder, backend, run, serve, tls  # noqa: F401  (fixtures)


@pytest.mark.parametrize("mode", ["runner", "run_forever"])
def test_ctrl_c_interrupts_an_idle_loop(backend, mode):
    # Nothing is scheduled, so the loop sleeps in the kernel: SIGINT has to
    # get it out and raise KeyboardInterrupt, as it does under asyncio.
    script = textwrap.dedent(f"""
        import asyncio, os, signal, threading, time, ringloop
        threading.Timer(0.3, os.kill, (os.getpid(), signal.SIGINT)).start()
        threading.Timer(10, os._exit, (3,)).start()
        try:
            if {mode!r} == "runner":
                ringloop.run(asyncio.sleep(60), backend={backend!r})
            else:
                ringloop.new_event_loop(backend={backend!r}).run_forever()
        except KeyboardInterrupt:
            os._exit(0)
        os._exit(2)
    """)
    proc = subprocess.run([sys.executable, "-c", script], timeout=30)
    assert proc.returncode == 0


def test_loop_set_up_on_one_thread_runs_on_another(backend):
    loop = ringloop.new_event_loop(backend=backend)
    errors = []
    try:
        server = loop.run_until_complete(loop.create_server(Recorder, HOST, 0))
        port = server.sockets[0].getsockname()[1]
        # A connection that is open and idle when the loop changes thread.
        client, proto = loop.run_until_complete(loop.create_connection(Recorder, HOST, port))
        loop.run_until_complete(asyncio.sleep(0.05))
        (accepted,) = server._clients

        def runner():
            try:
                loop.run_forever()
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        thread = threading.Thread(target=runner)
        thread.start()
        try:
            # New connections are accepted on the new thread...
            with socket.create_connection((HOST, port), 5) as sock:
                sock.settimeout(5)
                loop.call_soon_threadsafe(accepted.write, b"over the old connection")
                # ...and the old one still carries data both ways.
                deadline = time.monotonic() + 5
                while bytes(proto.data) != b"over the old connection" and time.monotonic() < deadline:
                    time.sleep(0.01)
                assert bytes(proto.data) == b"over the old connection"
                loop.call_soon_threadsafe(client.write, b"and back")
                while not accepted.get_protocol().data and time.monotonic() < deadline:
                    time.sleep(0.01)
                assert bytes(accepted.get_protocol().data) == b"and back"
        finally:
            loop.call_soon_threadsafe(loop.stop)
            thread.join(10)
        assert not thread.is_alive() and errors == []
    finally:
        loop.close()


def test_backend_probe_on_another_thread_then_run(backend):
    loop = ringloop.new_event_loop(backend=backend)
    seen = []
    thread = threading.Thread(target=lambda: seen.append(loop.backend))
    thread.start()
    thread.join()
    try:
        assert seen == [backend]
        assert loop.run_until_complete(asyncio.sleep(0.01, "ran")) == "ran"
    finally:
        loop.close()


def test_more_requests_than_the_submission_queue_holds(backend):
    if backend != "uring":
        pytest.skip("io_uring only")

    async def main():
        loop = asyncio.get_running_loop()

        class Echo(Recorder):
            def data_received(self, data):
                self.transport.write(data)

        server, port = await serve(Echo)
        transport, proto = await loop.create_connection(Recorder, HOST, port)
        before = loop._core._test_spill(0, 0)
        # Fill the queue past its size while the kernel "refuses" to take it:
        # nothing may be overwritten, and what is queued afterwards (the
        # write below) must still go out, in order.
        spilled = loop._core._test_spill(6000, 3) - before
        assert spilled > 0
        for i in range(20):
            transport.write(b"%04d" % i)
            await asyncio.sleep(0)
        while len(proto.data) < 80:
            await asyncio.sleep(0.01)
        assert bytes(proto.data) == b"".join(b"%04d" % i for i in range(20))
        transport.close()
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_many_writers_in_one_turn(backend):
    # More dirty transports in one turn than the submission queue has
    # entries: the queue is handed over part-way through the turn.
    resource = pytest.importorskip("resource")
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    count = 2600
    if soft < 2 * count + 200:
        if hard != resource.RLIM_INFINITY and hard < 2 * count + 200:
            pytest.skip("not enough file descriptors")
        resource.setrlimit(resource.RLIMIT_NOFILE, (2 * count + 200, hard))

    async def main():
        loop = asyncio.get_running_loop()
        server, port = await serve(Recorder, backlog=4096)
        pairs = [await loop.create_connection(Recorder, HOST, port) for _ in range(count)]
        while len(server._clients) < count:
            await asyncio.sleep(0.01)
        # Both ends of every connection write in the same turn.
        for i, (transport, _) in enumerate(pairs):
            transport.write(b"c%05d" % i)
        for transport in server._clients:
            transport.write(b"hello")
        deadline = loop.time() + 20
        while loop.time() < deadline:
            if (all(len(p.data) == 5 for _, p in pairs)
                    and all(len(t.get_protocol().data) == 6 for t in server._clients)):
                break
            await asyncio.sleep(0.02)
        assert all(bytes(p.data) == b"hello" for _, p in pairs)
        assert sorted(bytes(t.get_protocol().data) for t in server._clients) == [b"c%05d" % i for i in range(count)]
        for transport, _ in pairs:
            transport.close()
        server.close()
        await server.wait_closed()

    try:
        run(main(), backend)
    finally:
        resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))


def test_address_is_free_right_after_server_close(backend):
    async def main():
        server, port = await serve(Recorder)
        server.close()
        # No turn of the loop in between.
        with socket.socket() as sock:
            sock.bind((HOST, port))
            sock.listen()

    run(main(), backend)


def test_closed_server_cannot_start_serving(backend):
    async def main():
        server, _ = await serve(Recorder)
        server.close()
        await server.wait_closed()
        with pytest.raises(RuntimeError, match="is closed"):
            await server.start_serving()

    run(main(), backend)


def test_unix_server_removes_its_socket_file(backend, tmp_path):
    async def main():
        loop = asyncio.get_running_loop()
        path = str(tmp_path / "a.sock")
        server = await loop.create_unix_server(Recorder, path, cleanup_socket=True)
        assert os.path.exists(path)
        server.close()
        await server.wait_closed()
        assert not os.path.exists(path)
        # The default is asyncio's of the running version: 3.12 never removes it.
        server = await loop.create_unix_server(Recorder, path)
        server.close()
        await server.wait_closed()
        assert os.path.exists(path) == (sys.version_info < (3, 13))
        # cleanup_socket=False leaves it; so does a file that was replaced.
        server = await loop.create_unix_server(Recorder, path, cleanup_socket=False)
        server.close()
        assert os.path.exists(path)
        server = await loop.create_unix_server(Recorder, path, cleanup_socket=True)
        os.unlink(path)
        with open(path, "w") as f:
            f.write("someone else's")
        server.close()
        assert os.path.exists(path)

    run(main(), backend)


def test_numeric_addresses_skip_the_thread_pool(backend):
    async def main():
        loop = asyncio.get_running_loop()
        server, port = await serve(Recorder)
        transport, proto = await loop.create_connection(Recorder, HOST, port)
        assert loop._default_executor is None
        transport.close()
        sock = socket.socket()
        sock.setblocking(False)
        await loop.sock_connect(sock, (HOST, port))
        sock.close()
        assert loop._default_executor is None
        # A name still resolves (through the pool).
        transport, proto = await loop.create_connection(Recorder, "localhost", port, family=socket.AF_INET)
        transport.close()
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_handles_are_safe_to_poke(backend):
    async def main():
        loop = asyncio.get_running_loop()
        hits = []
        handle = loop.call_soon(hits.append, 1)
        assert handle._callback == hits.append and handle._args == (1,) and not handle._cancelled
        with pytest.raises(AttributeError):
            handle._args = None
        with pytest.raises(AttributeError):
            handle._loop = object()
        handle.cancel()
        handle._run()  # cancelled: a no-op, not a crash
        timer = loop.call_later(60, hits.append, 2)
        timer.cancel()
        timer._run()
        live = loop.call_soon(hits.append, 3)
        live._run()  # what nest_asyncio-style code does
        live.cancel()
        await asyncio.sleep(0)
        assert hits == [3]

    run(main(), backend)


def test_very_long_timers(backend):
    async def main():
        loop = asyncio.get_running_loop()
        handles = [loop.call_later(1e12, print), loop.call_later(float("inf"), print),
                   loop.call_at(1e300, print)]
        t0 = loop.time()
        await asyncio.sleep(0.05)
        assert loop.time() - t0 < 1
        for handle in handles:
            handle.cancel()

    run(main(), backend)


def test_short_timers_are_not_rounded_up_to_a_millisecond(backend):
    t0 = time.perf_counter()
    for _ in range(20):
        time.sleep(0.0001)
    if (time.perf_counter() - t0) / 20 > 0.0005:
        pytest.skip("this machine cannot sleep for 0.1 ms at all (no high-resolution timers)")

    async def main():
        best = None
        for _ in range(5):
            t0 = time.perf_counter()
            for _ in range(100):
                await asyncio.sleep(0.0001)
            took = (time.perf_counter() - t0) / 100
            best = took if best is None else min(best, took)
        return best

    # 0.1 ms asked for; a millisecond-granular wait would take over 1 ms.
    assert run(main(), backend) < 0.0008


def test_writelines_is_all_or_nothing(backend):
    async def main():
        loop = asyncio.get_running_loop()
        server, port = await serve(Recorder)
        transport, proto = await loop.create_connection(Recorder, HOST, port)
        with pytest.raises(TypeError):
            transport.writelines([b"lost-", "not bytes"])
        transport.writelines(iter([b"a", bytearray(b"b"), memoryview(b"c"), b""]))
        transport.write(b"")
        transport.write_eof()
        while not server._clients:
            await asyncio.sleep(0.01)
        (accepted,) = server._clients
        await accepted.get_protocol().eof
        assert bytes(accepted.get_protocol().data) == b"abc"
        transport.close()
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_add_reader_on_a_transport_fd_is_refused(backend):
    # As asyncio. Under epoll the transport's own registration would be
    # overwritten; under io_uring the callback would compete for its input.
    async def main():
        loop = asyncio.get_running_loop()
        server, port = await serve(Recorder)
        transport, proto = await loop.create_connection(Recorder, HOST, port)
        sock = transport.get_extra_info("socket")
        with pytest.raises(RuntimeError, match="used by a transport"):
            loop.add_reader(sock.fileno(), print)
        with pytest.raises(RuntimeError, match="used by a transport"):
            loop.add_writer(server.sockets[0].fileno(), print)
        # The transport is unharmed.
        transport.write(b"still works")
        while not server._clients or not next(iter(server._clients)).get_protocol().data:
            await asyncio.sleep(0.01)
        transport.close()
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_sendfile_with_input_arriving_meanwhile(backend, tmp_path):
    path = tmp_path / "blob"
    blob = os.urandom(3 * 1024 * 1024)
    path.write_bytes(blob)

    async def main():
        loop = asyncio.get_running_loop()
        result = loop.create_future()

        class Sender(Recorder):
            def connection_made(self, transport):
                super().connection_made(transport)
                self.task = loop.create_task(self.send())

            async def send(self):
                try:
                    with open(path, "rb") as f:
                        sent = await loop.sendfile(self.transport, f)
                    # What the peer sent during the copy arrives afterwards,
                    # at the real protocol.
                    while bytes(self.data) != b"next request":
                        await asyncio.sleep(0.01)
                    result.set_result((sent, self.transport.is_reading(), list(self.events)))
                except BaseException as exc:  # noqa: BLE001
                    result.set_exception(exc)
                self.transport.close()

        server, port = await serve(Sender)
        r, w = await asyncio.open_connection(HOST, port, limit=1 << 16)
        w.write(b"next request")
        await asyncio.sleep(0.1)  # the server is blocked on our full buffers by now
        assert await r.readexactly(len(blob)) == blob
        sent, reading, events = await result
        assert sent == len(blob) and reading
        # Flow control reached the copy, not the protocol, and is balanced.
        assert events.count("pause") == events.count("resume")
        w.close()
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_sendfile_cancelled_while_paused_leaves_protocol_in_sync(backend, tmp_path):
    path = tmp_path / "blob"
    path.write_bytes(bytes(8 * 1024 * 1024))

    async def main():
        loop = asyncio.get_running_loop()
        server, port = await serve(Recorder)
        sock = socket.create_connection((HOST, port))  # never reads
        while not server._clients:
            await asyncio.sleep(0.01)
        (transport,) = server._clients
        proto = transport.get_protocol()
        with open(path, "rb") as f:
            # The copy through write() (what TLS transports get): a plain
            # transport would use sendfile(2) and buffer nothing.
            task = loop.create_task(loop._sendfile_fallback(transport, f, 0, None))
            await asyncio.sleep(0.3)  # buffers full: the copy waits for the transport
            assert not task.done()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        # The transport is still over its high-water mark, and now the
        # protocol knows; the matching resume comes when the peer reads.
        assert transport.get_protocol() is proto
        assert proto.events.count("pause") == 1 and "resume" not in proto.events
        sock.settimeout(10)
        while sock.recv(1 << 20):
            if "resume" in proto.events:
                break
            await asyncio.sleep(0)
        assert proto.events.count("resume") == 1
        sock.close()
        transport.abort()
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_sock_sendfile_from_a_pipe_falls_back(backend):
    async def main():
        loop = asyncio.get_running_loop()
        a, b = socket.socketpair()
        a.setblocking(False)
        b.setblocking(False)
        rfd, wfd = os.pipe()
        os.write(wfd, b"through a pipe")
        os.close(wfd)
        with open(rfd, "rb", buffering=0) as f:
            assert await loop.sock_sendfile(a, f) == 14
            with pytest.raises(asyncio.SendfileNotAvailableError):
                await loop.sock_sendfile(a, f, fallback=False)
        assert await loop.sock_recv(b, 100) == b"through a pipe"
        a.close()
        b.close()

    run(main(), backend)


def test_subprocess_without_pidfd(backend, monkeypatch):
    def no_pidfd(pid, flags=0):
        raise OSError(38, "Function not implemented")

    monkeypatch.setattr(os, "pidfd_open", no_pidfd)

    async def main():
        proc = await asyncio.create_subprocess_exec(
            "/bin/sh", "-c", "cat; exit 7", stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        out, _ = await proc.communicate(b"waited for in a thread")
        assert out == b"waited for in a thread" and proc.returncode == 7

    run(main(), backend)


def test_failed_spawn_leaves_nothing_behind(backend, monkeypatch):
    unraisable = []
    monkeypatch.setattr(sys, "unraisablehook", unraisable.append)

    async def main():
        with pytest.raises(FileNotFoundError):
            await asyncio.create_subprocess_exec("/no/such/program")

    run(main(), backend)
    gc.collect()
    assert unraisable == []


def test_tls_writelines_takes_a_one_shot_iterator(backend, tls):
    server_ctx, client_ctx = tls

    async def main():
        loop = asyncio.get_running_loop()

        class Lines(Recorder):
            def connection_made(self, transport):
                super().connection_made(transport)
                transport.writelines(part for part in (b"one ", b"two ", b"three"))
                transport.writelines([])
                transport.close()

        server, port = await serve(Lines, ssl=server_ctx)
        r, w = await asyncio.open_connection(HOST, port, ssl=client_ctx, server_hostname="localhost")
        assert await r.read() == b"one two three"
        w.close()
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_tls_close_while_reading_is_paused(backend, tls):
    server_ctx, client_ctx = tls

    async def main():
        loop = asyncio.get_running_loop()
        accepted = loop.create_future()

        class Paused(Recorder):
            def connection_made(self, transport):
                super().connection_made(transport)
                transport.pause_reading()
                accepted.set_result(self)

        server, port = await serve(Paused, ssl=server_ctx)
        r, w = await asyncio.open_connection(HOST, port, ssl=client_ctx, server_hostname="localhost")
        proto = await accepted
        t0 = loop.time()
        proto.transport.write(b"bye")
        proto.transport.close()
        assert await r.read() == b"bye"
        w.close()
        # The peer's close_notify is read although the application had
        # paused reading: no waiting for the shutdown timeout.
        assert await asyncio.wait_for(proto.lost, 5) is None
        assert loop.time() - t0 < 5
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_timers_fire_in_time_then_insertion_order(backend):
    import random

    async def main():
        loop = asyncio.get_running_loop()
        rng = random.Random(1234)
        fired = []
        expected = []
        cancelled = []
        base = loop.time() - 10  # all already due: one turn runs them all
        for seq in range(5000):
            when = base + rng.randrange(400) / 100  # many ties
            handle = loop.call_at(when, fired.append, seq)
            assert handle.when() == when
            if rng.random() < 0.4:
                cancelled.append(handle)
            else:
                expected.append((when, seq))
        rng.shuffle(cancelled)
        for handle in cancelled:  # enough to trigger the heap rebuild
            handle.cancel()
        far = [loop.call_later(3600 + i, fired.append, -1) for i in range(300)]
        await asyncio.sleep(0.01)
        assert fired == [seq for _, seq in sorted(expected)]
        for handle in far:
            handle.cancel()
        # Timers scheduled from timers, and interleaved real delays.
        order = []
        for i, delay in enumerate([0.03, 0.01, 0.02, 0.01, 0.0, 0.02]):
            loop.call_later(delay, order.append, i)
        await asyncio.sleep(0.1)
        assert order == [4, 1, 3, 2, 5, 0]

    run(main(), backend)


def test_interrupted_turn_keeps_the_callbacks_that_did_not_run(backend):
    loop = ringloop.new_event_loop(backend=backend)
    ran = []

    def interrupt():
        loop.call_soon(ran.append, "scheduled meanwhile")
        raise KeyboardInterrupt

    try:
        loop.call_soon(ran.append, 1)
        loop.call_soon(interrupt)
        loop.call_soon(ran.append, 2)
        loop.call_soon(ran.append, 3)
        with pytest.raises(KeyboardInterrupt):
            loop.run_forever()
        assert ran == [1]
        loop.call_soon(loop.stop)
        loop.run_forever()
        assert ran == [1, 2, 3, "scheduled meanwhile"]
    finally:
        loop.close()


def test_handles_are_released_as_soon_as_they_have_run(backend):
    import weakref

    class Payload:
        pass

    async def main():
        loop = asyncio.get_running_loop()
        seen = []
        payload = Payload()
        ref = weakref.ref(payload)
        loop.call_soon(lambda p: None, payload)
        del payload
        # Runs later in the same turn: the first handle is gone by then.
        loop.call_soon(lambda: seen.append(ref() is None))
        await asyncio.sleep(0)
        assert seen == [True]

    run(main(), backend)


@pytest.mark.parametrize("buffered", [False, True])
def test_tls_many_small_records_in_one_read(backend, tls, buffered):
    server_ctx, client_ctx = tls

    async def main():
        loop = asyncio.get_running_loop()
        done = loop.create_future()

        class Plain(Recorder):
            def connection_lost(self, exc):
                super().connection_lost(exc)
                done.set_result(self)

        class Buffered(asyncio.BufferedProtocol):
            def __init__(self):
                self.data = bytearray()
                self.events = []
                self.buf = bytearray(7)  # smaller than a record: several reads per record

            def get_buffer(self, sizehint):
                return self.buf

            def buffer_updated(self, nbytes):
                self.data += self.buf[:nbytes]

            def eof_received(self):
                self.events.append("eof")

            def connection_lost(self, exc):
                done.set_result(self)

        server, port = await serve(Buffered if buffered else Plain, ssl=server_ctx)
        # A blocking client in a thread: each send() is its own TLS record,
        # and they reach the server coalesced into few TCP segments.
        def client():
            with socket.create_connection((HOST, port)) as raw:
                raw.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 0)
                with client_ctx.wrap_socket(raw, server_hostname="localhost") as conn:
                    for i in range(400):
                        conn.send(b"%05d" % i)
                    conn.unwrap()

        await asyncio.to_thread(client)
        proto = await asyncio.wait_for(done, 10)
        assert bytes(proto.data) == b"".join(b"%05d" % i for i in range(400))
        assert "eof" in proto.events
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_callbacks_run_in_the_context_their_server_or_client_was_created_in(backend):
    # Each connection runs in its own copy of its server's (or client's)
    # context; one server's or client's context must not show up in
    # another's callbacks. Isolation between connections of one server:
    # tests/test_threads.py::test_connections_of_one_server_do_not_share_context_changes.
    import contextvars

    var = contextvars.ContextVar("var", default="unset")

    async def main():
        loop = asyncio.get_running_loop()
        seen = []

        class Tagged(Recorder):
            def connection_made(self, transport):
                super().connection_made(transport)
                seen.append(("made", var.get()))

            def data_received(self, data):
                seen.append((bytes(data), var.get()))

        class Echo(Tagged):
            def data_received(self, data):
                super().data_received(data)
                self.transport.write(data)

        # Two servers and two clients, each created under a different value;
        # their events interleave within the same turns of the loop.
        var.set("server-a")
        server_a, port_a = await serve(Echo)
        var.set("server-b")
        server_b, port_b = await serve(Echo)
        clients = []
        for name, port in (("client-1", port_a), ("client-2", port_b), ("client-3", port_a)):
            var.set(name)
            clients.append(await loop.create_connection(Tagged, HOST, port))
        var.set("main")
        for _ in range(3):
            for i, (transport, _) in enumerate(clients):
                transport.write(b"c%d" % (i + 1))
            await asyncio.sleep(0.02)
        for transport, _ in clients:
            transport.close()
        server_a.close()
        server_b.close()
        await asyncio.sleep(0.02)
        assert var.get() == "main"
        # Requests are seen by the server they went to, echoes by the client
        # that sent them, each under the value it was created with.
        owner = {b"c1": ("server-a", "client-1"), b"c2": ("server-b", "client-2"), b"c3": ("server-a", "client-3")}
        data_events = [(d, v) for d, v in seen if d != "made"]
        assert len(data_events) == 18
        for data, value in data_events:
            assert value in owner[data], (data, value)
        for data, (server_value, client_value) in owner.items():
            assert [v for d, v in data_events if d == data].count(server_value) == 3
            assert [v for d, v in data_events if d == data].count(client_value) == 3
        assert sorted(v for d, v in seen if d == "made") == [
            "client-1", "client-2", "client-3", "server-a", "server-a", "server-b"]

    run(main(), backend)


# -- found by an independent review of the changes above ---------------------


def test_loop_runs_on_a_thread_that_reuses_a_dead_threads_id(backend):
    # pthread ids are handed to the next thread once one exits; the ring's
    # owner must be told apart anyway.
    loop = ringloop.new_event_loop(backend=backend)
    results = []

    def go():
        try:
            results.append(loop.run_until_complete(asyncio.sleep(0.01, "ok")))
        except BaseException as exc:  # noqa: BLE001
            results.append(exc)

    try:
        for _ in range(4):
            thread = threading.Thread(target=go)
            thread.start()
            thread.join()
        assert results == ["ok"] * 4
    finally:
        loop.close()


def test_input_completed_before_a_thread_change_is_not_lost(backend):
    loop = ringloop.new_event_loop(backend=backend)
    try:
        async def setup():
            return (await loop.create_server(Recorder, HOST, 0),
                    await loop.create_server(Recorder, HOST, 0))

        server, other = loop.run_until_complete(setup())
        client = socket.create_connection(server.sockets[0].getsockname())
        loop.run_until_complete(asyncio.sleep(0.05))
        (accepted,) = server._clients

        def last_turn():
            client.sendall(b"hello")
            time.sleep(0.05)
            other.close()  # hands the queue to the kernel mid-turn: the recv completes now
            loop.stop()

        loop.call_soon(last_turn)
        loop.run_forever()
        thread = threading.Thread(target=lambda: loop.run_until_complete(asyncio.sleep(0.2)))
        thread.start()
        thread.join()
        assert bytes(accepted.get_protocol().data) == b"hello"
        client.close()
        server.close()
    finally:
        loop.close()


def test_pause_reading_in_connection_made(backend):
    async def main():
        loop = asyncio.get_running_loop()

        class PausedFromTheStart(Recorder):
            def connection_made(self, transport):
                super().connection_made(transport)
                assert transport.is_reading()
                transport.pause_reading()
                assert not transport.is_reading()

        server, port = await serve(PausedFromTheStart)
        transport, proto = await loop.create_connection(PausedFromTheStart, HOST, port)
        while not server._clients:
            await asyncio.sleep(0.01)
        (accepted,) = server._clients
        transport.write(b"to the server")
        accepted.write(b"to the client")
        await asyncio.sleep(0.1)
        assert not transport.is_reading() and not accepted.is_reading()
        assert not proto.data and not accepted.get_protocol().data
        transport.resume_reading()
        accepted.resume_reading()
        await asyncio.sleep(0.1)
        assert bytes(proto.data) == b"to the client"
        assert bytes(accepted.get_protocol().data) == b"to the server"
        transport.close()
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_sendfile_starting_while_the_protocol_is_paused(backend, tmp_path):
    # The buffer is between the low and high marks and the protocol has been
    # told to pause: the copy must wait for the transport, and the protocol
    # must get its resume_writing() in the end.
    path = tmp_path / "blob"
    path.write_bytes(b"c" * (8 << 20))

    async def main():
        loop = asyncio.get_running_loop()
        state = {"got": 0, "stop_at": None, "transport": None}

        class Sink(asyncio.Protocol):
            def connection_made(self, transport):
                state["transport"] = transport
                transport.pause_reading()

            def data_received(self, data):
                state["got"] += len(data)
                if state["stop_at"] is not None and state["got"] >= state["stop_at"]:
                    state["transport"].pause_reading()

        lsock = socket.socket()
        lsock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 2048)
        lsock.bind((HOST, 0))
        server = await loop.create_server(Sink, sock=lsock)
        csock = socket.socket()
        csock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        csock.connect(lsock.getsockname())
        csock.setblocking(False)
        transport, proto = await loop.create_connection(Recorder, sock=csock)
        big = 2 << 20
        transport.write(b"a" * big)
        await asyncio.sleep(0.1)
        assert proto.events.count("pause") == 1
        transport.write(b"b" * 30000)
        state["stop_at"] = big  # the server reads the first write only
        state["transport"].resume_reading()
        low, high = transport.get_write_buffer_limits()
        for _ in range(500):
            await asyncio.sleep(0.01)
            if state["got"] >= big and transport.get_write_buffer_size() <= high:
                break
        if not (low < transport.get_write_buffer_size() <= high and "resume" not in proto.events):
            pytest.skip("could not park the buffer between the water marks here")
        state["stop_at"] = None
        state["transport"].resume_reading()
        with open(path, "rb") as f:
            task = loop.create_task(loop.sendfile(transport, f))
            peak = 0
            while not task.done():
                peak = max(peak, transport.get_write_buffer_size())
                await asyncio.sleep(0.001)
            assert task.result() == 8 << 20
        assert peak < 1 << 20  # backpressure held: nowhere near the whole file
        for _ in range(500):
            if transport.get_write_buffer_size() == 0:
                break
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.05)
        assert [e for e in proto.events if e in ("pause", "resume")] == ["pause", "resume"]
        transport.close()
        server.close()

    run(main(), backend)


def test_writelines_iterator_that_writes_then_fails(backend):
    async def main():
        loop = asyncio.get_running_loop()
        server, port = await serve(Recorder)
        transport, proto = await loop.create_connection(Recorder, HOST, port)
        transport.write(b"X" * 10)

        def parts():
            yield b"A"
            transport.write(b"B" * 100_000)  # over the high-water mark: goes out at once
            yield "not bytes"

        with pytest.raises(TypeError):
            transport.writelines(parts())
        transport.write(b"Z")
        await asyncio.sleep(0.2)
        (accepted,) = server._clients
        data = bytes(accepted.get_protocol().data)
        # No error, nothing foreign: only bytes that were really written.
        assert not proto.lost.done()
        assert data.startswith(b"X" * 10) and data.endswith(b"Z") and set(data) <= set(b"XABZ")
        assert data.count(b"B") == 100_000
        transport.close()
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_nan_timer_is_refused(backend):
    async def main():
        loop = asyncio.get_running_loop()
        with pytest.raises(ValueError):
            loop.call_later(float("nan"), print)
        with pytest.raises(ValueError):
            loop.call_at(float("nan"), print)

    run(main(), backend)


def test_eof_behind_stashed_data_is_reported_once(backend):
    async def main():
        loop = asyncio.get_running_loop()
        state = {"calls": 0, "eof": 0, "data": bytearray(), "transport": None}

        class HalfClosing(asyncio.Protocol):
            def connection_made(self, transport):
                state["transport"] = transport

            def data_received(self, data):
                state["calls"] += 1
                state["data"] += data
                if state["calls"] in (1, 2):
                    state["transport"].pause_reading()

            def eof_received(self):
                state["eof"] += 1
                return True  # keep the write side open

        server, port = await serve(HalfClosing)
        client = socket.create_connection((HOST, port))
        client.sendall(b"a")
        await asyncio.sleep(0.05)
        transport = state["transport"]

        def resume():
            transport.resume_reading()
            loop.call_soon(transport.resume_reading)  # after the stash paused it again
            client.shutdown(socket.SHUT_WR)
            time.sleep(0.05)

        def burst():
            client.sendall(b"b" * 40000)
            time.sleep(0.05)
            loop.call_soon(resume)

        loop.call_soon(burst)
        await asyncio.sleep(0.5)
        assert len(state["data"]) == 40001 and state["eof"] == 1
        client.close()
        transport.close()
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_writelines_rollback_after_a_partial_send_from_the_same_buffer(backend):
    async def main():
        loop = asyncio.get_running_loop()
        # A large buffer in the pool, so the re-entrant write below fits
        # without moving it.
        server, port = await serve(Recorder)
        warm, _ = await loop.create_connection(Recorder, HOST, port)
        warm.write(b"w" * 200_000)
        await asyncio.sleep(0.2)
        stalled = []

        class Stall(asyncio.Protocol):
            def connection_made(self, transport):
                transport.pause_reading()
                stalled.append(transport)

        lsock = socket.socket()
        lsock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        lsock.bind((HOST, 0))
        server2 = await loop.create_server(Stall, sock=lsock)
        csock = socket.socket()
        csock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        csock.connect(lsock.getsockname())
        csock.setblocking(False)
        transport, proto = await loop.create_connection(Recorder, sock=csock)
        await asyncio.sleep(0.05)
        transport.write(b"X" * 10)

        def parts():
            yield b"A"
            transport.write(b"B" * 100_000)  # partly sent at once; the peer is not reading
            yield "not bytes"

        with pytest.raises(TypeError):
            transport.writelines(parts())
        assert 0 <= transport.get_write_buffer_size() <= 100_011
        received = Recorder()
        stalled[0].set_protocol(received)
        stalled[0].resume_reading()
        for _ in range(300):
            await asyncio.sleep(0.01)
            if len(received.data) >= 100_011:
                break
        await asyncio.sleep(0.1)
        # Exactly what was written and not taken back, nothing from beyond it.
        assert bytes(received.data) == b"X" * 10 + b"A" + b"B" * 100_000
        assert not proto.lost.done()
        for t in (warm, transport):
            t.close()
        server.close()
        server2.close()

    run(main(), backend)
