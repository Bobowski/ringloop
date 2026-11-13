"""Threads: what may cross between threads, what is refused, and running one
loop per thread. Everything here also runs on the free-threaded build, where
the extension runs without the GIL."""

import asyncio
import asyncio.trsock
import contextvars
import os
import socket
import subprocess
import sys
import sysconfig
import threading
import time

import pytest

import ringloop

from .test_loop import HOST, Recorder, backend, run, serve, tls  # noqa: F401  (fixtures)

FREE_THREADED = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))


def start_loop_thread(backend):
    """A loop running forever on its own thread."""
    loop = ringloop.new_event_loop(backend=backend)
    started = threading.Event()
    loop.call_soon(started.set)
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    assert started.wait(5)
    return loop, thread


def stop_loop_thread(loop, thread):
    loop.call_soon_threadsafe(loop.stop)
    thread.join(5)
    assert not thread.is_alive()
    loop.close()


def wait_for(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("timed out")
        time.sleep(0.002)


@pytest.mark.skipif(not FREE_THREADED, reason="free-threaded build only")
def test_import_keeps_the_gil_disabled():
    out = subprocess.run(
        [sys.executable, "-W", "error::RuntimeWarning", "-c",
         "import sys, ringloop; ringloop.run(__import__('asyncio').sleep(0)); "
         "print(sys._is_gil_enabled())"],
        capture_output=True, text=True, env={**os.environ, "PYTHON_GIL": ""}, timeout=60)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "False"


def test_call_soon_threadsafe_from_many_threads_loses_nothing(backend):
    loop, thread = start_loop_thread(backend)
    threads, per_thread = 8, 3000
    got = [[] for _ in range(threads)]
    done = threading.Event()
    remaining = [threads * per_thread]

    def record(t, i):
        got[t].append(i)
        remaining[0] -= 1
        if not remaining[0]:
            done.set()

    def producer(t):
        for i in range(per_thread):
            loop.call_soon_threadsafe(record, t, i)

    try:
        workers = [threading.Thread(target=producer, args=(t,)) for t in range(threads)]
        for w in workers:
            w.start()
        for w in workers:
            w.join()
        assert done.wait(10)
        # Every callback ran, once, and each thread's in the order it posted them.
        assert got == [list(range(per_thread))] * threads
    finally:
        stop_loop_thread(loop, thread)


def test_scheduling_from_another_thread_while_the_loop_runs(backend):
    async def main():
        loop = asyncio.get_running_loop()
        events = []
        done = loop.create_future()

        def worker():
            loop.call_soon(events.append, "soon")
            loop.call_later(0.01, events.append, "later")
            loop.call_at(loop.time() + 0.02, events.append, "at")
            cancelled = loop.call_later(0.01, events.append, "cancelled")
            cancelled.cancel()
            assert cancelled.cancelled()
            loop.call_later(0.05, done.set_result, None)

        threading.Thread(target=worker).start()
        await done
        assert events == ["soon", "later", "at"]

    run(main(), backend)


def test_scheduling_while_no_thread_runs_the_loop_keeps_its_order(backend):
    loop = ringloop.new_event_loop(backend=backend)
    try:
        events = []
        loop.call_soon(events.append, 1)
        t = threading.Thread(target=lambda: [loop.call_soon(events.append, n) for n in (2, 3)])
        t.start()
        t.join()
        loop.call_later(0, events.append, 5)
        loop.call_soon(events.append, 4)
        loop.run_until_complete(asyncio.sleep(0.01))
        assert events == [1, 2, 3, 4, 5]
    finally:
        loop.close()


def test_stop_from_another_thread_wakes_an_idle_loop(backend):
    loop = ringloop.new_event_loop(backend=backend)
    try:
        threading.Timer(0.05, loop.stop).start()
        t0 = time.monotonic()
        loop.run_forever()  # nothing scheduled: only stop() can end it
        assert time.monotonic() - t0 < 5
        assert not loop.is_running()
    finally:
        loop.close()


def test_transport_calls_from_another_thread_are_refused_while_it_runs(backend):
    async def main():
        loop = asyncio.get_running_loop()
        server, port = await serve(Recorder)
        transport, proto = await loop.create_connection(Recorder, HOST, port)
        errors = []
        reads = []

        def worker():
            for call in (lambda: transport.write(b"x"), transport.write_eof, lambda: transport.writelines([b"x"]),
                         transport.pause_reading, lambda: loop.add_reader(0, print),
                         lambda: loop.remove_writer(0), server.close):
                try:
                    call()
                except RuntimeError as exc:
                    errors.append(str(exc))
            # Questions are fine from any thread.
            reads.append(transport.get_protocol() is proto)
            reads.append(transport.get_extra_info("peername")[1] == port)
            reads.append(transport.is_closing() is False)
            reads.append(loop.is_running())
            reads.append(loop.backend == backend)

        t = threading.Thread(target=worker)
        t.start()
        while t.is_alive():
            await asyncio.sleep(0.005)
        assert len(errors) == 7
        assert all("Non-thread-safe operation" in e for e in errors)
        assert reads == [True] * 5
        # Nothing was harmed: the connection still works.
        transport.write(b"still here")
        while not server._clients or not next(iter(server._clients)).get_protocol().data:
            await asyncio.sleep(0.01)
        transport.close()
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_a_loop_that_no_thread_runs_may_be_used_from_any_thread(backend):
    loop = ringloop.new_event_loop(backend=backend)
    try:
        a, b = socket.socketpair()
        hits = []

        def worker():
            loop.add_reader(a.fileno(), lambda: hits.append(a.recv(10)))

        t = threading.Thread(target=worker)
        t.start()
        t.join()
        b.send(b"ping")
        loop.run_until_complete(asyncio.sleep(0.05))
        assert hits == [b"ping"]
        assert loop.remove_reader(a.fileno())
        a.close()
        b.close()
    finally:
        loop.close()


def test_running_one_loop_on_two_threads_is_refused(backend):
    loop, thread = start_loop_thread(backend)
    try:
        with pytest.raises(RuntimeError, match="already running"):
            loop.run_forever()
        with pytest.raises(RuntimeError, match="running"):
            loop.close()
        with pytest.raises(RuntimeError):
            loop._run_turns()  # below run_forever's own check: the atomic claim
    finally:
        stop_loop_thread(loop, thread)


def test_handles_cancelled_from_another_thread(backend):
    loop, thread = start_loop_thread(backend)
    try:
        ran = []
        lock = threading.Lock()

        def hit(i):
            with lock:
                ran.append(i)

        handles = []
        created = threading.Event()

        def create():
            for i in range(2000):
                handles.append(loop.call_later(0.2, hit, i))
            created.set()

        loop.call_soon_threadsafe(create)
        assert created.wait(5)
        # Cancel every other one from here (and look at them) while the
        # loop's thread is running.
        for h in handles[::2]:
            h.cancel()
            repr(h)
        wait_for(lambda: len(ran) == 1000)
        time.sleep(0.05)
        assert sorted(ran) == list(range(1, 2000, 2))
        assert all(h.cancelled() and h._callback is None for h in handles[::2])
    finally:
        stop_loop_thread(loop, thread)


def test_one_loop_per_thread_all_serving_at_once(backend):
    loops, conns, rounds = 4, 8, 50
    results = [None] * loops

    async def echo_session(index):
        async def handle(reader, writer):
            while data := await reader.read(65536):
                writer.write(data)
                await writer.drain()
            writer.close()

        server = await asyncio.start_server(handle, HOST, 0)
        port = server.sockets[0].getsockname()[1]

        async def client(c):
            reader, writer = await asyncio.open_connection(HOST, port)
            for r in range(rounds):
                msg = b"%d:%d:%d|" % (index, c, r) * 64
                writer.write(msg)
                assert await reader.readexactly(len(msg)) == msg
            writer.close()
            await writer.wait_closed()
            return c

        done = await asyncio.gather(*(client(c) for c in range(conns)))
        server.close()
        await server.wait_closed()
        return sorted(done)

    def worker(index):
        results[index] = ringloop.run(asyncio.wait_for(echo_session(index), 30), backend=backend)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(loops)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(60)
    assert results == [list(range(conns))] * loops
    if FREE_THREADED:
        assert not sys._is_gil_enabled()


def test_connections_of_one_server_do_not_share_context_changes(backend):
    var = contextvars.ContextVar("var", default="unset")

    async def main():
        loop = asyncio.get_running_loop()
        seen = []

        class Tagging(Recorder):
            def data_received(self, data):
                seen.append((bytes(data), var.get()))
                var.set(bytes(data))  # visible to this connection's later callbacks only
                self.transport.write(b"ok")

        server, port = await serve(Tagging)
        clients = [await loop.create_connection(Recorder, HOST, port) for _ in range(2)]
        for round_ in (b"1", b"2"):
            for i, (transport, _) in enumerate(clients):
                transport.write(b"c%d-%s" % (i, round_))
                await asyncio.sleep(0.02)
        for transport, _ in clients:
            transport.close()
        server.close()
        await server.wait_closed()
        assert seen == [(b"c0-1", "unset"), (b"c1-1", "unset"),
                        (b"c0-2", b"c0-1"), (b"c1-2", b"c1-1")]

    run(main(), backend)


def test_extra_info_socket_cannot_be_closed_behind_the_transport(backend):
    async def main():
        loop = asyncio.get_running_loop()
        server, port = await serve(Recorder)
        transport, proto = await loop.create_connection(Recorder, HOST, port)
        sock = transport.get_extra_info("socket")
        assert isinstance(sock, asyncio.trsock.TransportSocket)
        assert not hasattr(sock, "close") and not hasattr(sock, "recv")
        assert sock.getpeername()[1] == port
        assert sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY)
        transport.close()
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_socket_outlives_connection_lost_until_its_requests_are_done(backend):
    # io_uring: a request names the socket by number, so the number must not
    # be freed (and handed to another socket) while one is still queued or
    # in flight. epoll has no such requests and closes at once.
    async def main():
        loop = asyncio.get_running_loop()
        with socket.socket() as peer:  # accepts nothing, reads nothing
            peer.bind((HOST, 0))
            peer.listen()
            port = peer.getsockname()[1]
            transport, proto = await loop.create_connection(Recorder, HOST, port)
            fd = transport.get_extra_info("socket").fileno()
            transport.write(b"x" * (32 << 20))  # more than the socket buffers hold
            await asyncio.sleep(0.05)  # io_uring: a send is now waiting in the kernel
            state = []
            transport.abort()  # schedules connection_lost...

            def right_after():  # ...and this runs just after it, in the same turn
                try:
                    os.fstat(fd)
                    state.append("open")
                except OSError:
                    state.append("closed")

            loop.call_soon(right_after)
            await proto.lost
            await asyncio.sleep(0.05)
            assert state == (["open"] if loop.backend == "uring" else ["closed"])
            with pytest.raises(OSError):
                os.fstat(fd)  # closed once the cancelled send completed
            assert loop._core.live_count() == 0

    run(main(), backend)


def test_server_closed_from_another_thread_while_no_thread_runs_the_loop(backend):
    loop = ringloop.new_event_loop(backend=backend)
    try:
        server = loop.run_until_complete(loop.create_server(asyncio.Protocol, HOST, 0))
        port = server.sockets[0].getsockname()[1]
        # Closed from a thread that is not the io_uring ring's: the kernel
        # cannot be handed the cancel from there, so the core keeps the
        # descriptor until the loop runs again.
        t = threading.Thread(target=server.close)
        t.start()
        t.join()
        loop.run_until_complete(asyncio.sleep(0.01))
        with socket.socket() as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            probe.bind((HOST, port))  # the address is free again
            probe.listen()
        assert loop._core.live_count() == 0
    finally:
        loop.close()


def test_set_write_buffer_limits_pauses_when_already_above(backend):
    async def main():
        loop = asyncio.get_running_loop()
        server, port = await serve(Recorder)
        paused = []

        class Watch(Recorder):
            def pause_writing(self):
                paused.append(True)

        transport, proto = await loop.create_connection(Watch, HOST, port)
        transport.write(b"x" * 1000)  # buffered until the end of the turn
        transport.set_write_buffer_limits(high=100)
        assert paused == [True]
        transport.close()
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_callback_error_report_survives_bad_and_huge_reprs(backend):
    class BadRepr:
        def __repr__(self):
            raise ValueError("no repr for you")

    def boom(*args):
        raise RuntimeError("boom")

    async def main():
        loop = asyncio.get_running_loop()
        reports = []
        loop.set_exception_handler(lambda loop, ctx: reports.append(ctx))
        loop.call_soon(boom, BadRepr(), b"x" * 1_000_000)
        await asyncio.sleep(0.01)
        assert len(reports) == 1
        message = reports[0]["message"]
        assert message.startswith("Exception in callback") and "boom(" in message
        assert len(message) < 500
        assert isinstance(reports[0]["exception"], RuntimeError)

    run(main(), backend)


# -- found by an independent review of the changes above ---------------------


def test_a_post_from_another_thread_runs_before_what_the_loop_schedules_after(backend):
    # As with one queue (the stdlib loop, uvloop): thread A posts h1 and then
    # signals; the loop thread, having seen the signal, schedules h2.
    async def main():
        loop = asyncio.get_running_loop()
        order = []
        signalled = threading.Event()

        def poster():
            loop.call_soon_threadsafe(order.append, "posted-first")
            loop.call_later(0, order.append, "timer-posted-first")
            signalled.set()

        threading.Thread(target=poster).start()
        while not signalled.is_set():
            time.sleep(0.001)  # block the loop thread: nothing is drained meanwhile
        loop.call_soon(order.append, "loop-second")
        loop.call_later(0, order.append, "timer-loop-second")
        await asyncio.sleep(0.01)
        assert order.index("posted-first") < order.index("loop-second")
        assert order.index("timer-posted-first") < order.index("timer-loop-second")

    run(main(), backend)


def test_tls_write_from_another_thread_is_refused_before_it_encrypts(backend, tls):
    server_ctx, client_ctx = tls

    async def main():
        loop = asyncio.get_running_loop()
        got = bytearray()
        done = loop.create_future()

        class Sink(asyncio.Protocol):
            def data_received(self, data):
                got.extend(data)
                if got.endswith(b"after|"):
                    done.set_result(None)

        server = await loop.create_server(Sink, HOST, 0, ssl=server_ctx)
        port = server.sockets[0].getsockname()[1]
        transport, _ = await loop.create_connection(
            asyncio.Protocol, HOST, port, ssl=client_ctx, server_hostname="localhost")
        transport.write(b"before|")
        errors = []

        def worker():
            try:
                transport.write(b"from-a-thread|")
            except RuntimeError as exc:
                errors.append(exc)

        t = threading.Thread(target=worker)
        t.start()
        while t.is_alive():
            await asyncio.sleep(0.005)
        assert len(errors) == 1
        transport.write(b"after|")  # the TLS stream is intact
        await asyncio.wait_for(done, 5)
        assert bytes(got) == b"before|after|"
        transport.close()
        server.close()
        await server.wait_closed()

    run(main(), backend)


def test_timers_cancelled_from_other_threads_are_cleaned_up(backend):
    import gc

    def live_timer_handles():
        return sum(1 for o in gc.get_objects() if type(o).__name__ == "TimerHandle")

    async def main():
        loop = asyncio.get_running_loop()
        keep = loop.call_later(3600, print)  # one live timer due before all of them
        handles = [loop.call_later(7200, print) for _ in range(20000)]
        t = threading.Thread(target=lambda: [h.cancel() for h in handles])
        t.start()
        while t.is_alive():
            await asyncio.sleep(0.005)
        del handles
        for _ in range(3):
            await asyncio.sleep(0)
        gc.collect()
        assert live_timer_handles() < 1000
        keep.cancel()

    run(main(), backend)


def test_a_loop_that_cannot_get_a_new_ring_keeps_its_old_one(backend, tmp_path):
    # Moving to another thread needs a new io_uring ring. Without one (here:
    # no descriptor left), the move is refused and the loop stays usable on
    # its own thread; nothing may touch a missing ring.
    if backend != "uring":
        pytest.skip("io_uring only")
    script = tmp_path / "ring.py"
    script.write_text(r'''
import asyncio, os, resource, socket, threading
import ringloop

loop = ringloop.new_event_loop(backend="uring")
server = loop.run_until_complete(loop.create_server(asyncio.Protocol, "127.0.0.1", 0))
port = server.sockets[0].getsockname()[1]
transport, _ = loop.run_until_complete(loop.create_connection(asyncio.Protocol, "127.0.0.1", port))
soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
lowest = os.open(os.devnull, os.O_RDONLY)  # the lowest free descriptor number
os.close(lowest)
errors = []

def run_elsewhere():
    try:
        loop.run_until_complete(asyncio.sleep(0))
    except OSError as exc:
        errors.append(exc)

resource.setrlimit(resource.RLIMIT_NOFILE, (lowest, hard))  # no new descriptor
try:
    t = threading.Thread(target=run_elsewhere)
    t.start()
    t.join()
finally:
    resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))
assert len(errors) == 1, errors
transport.close()
server.close()
loop.run_until_complete(asyncio.sleep(0.01))  # still runs here, on its own ring
assert loop._core.live_count() == 0
loop.close()
print("ok")
''')
    out = subprocess.run([sys.executable, str(script)], capture_output=True, text=True, timeout=60,
                         env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)})
    assert out.returncode == 0, out.stdout + out.stderr
    assert out.stdout.strip() == "ok"


def test_a_loop_cannot_run_in_a_forked_child(backend, tmp_path):
    script = tmp_path / "fork.py"
    script.write_text(r"""
import asyncio, os, sys
import ringloop

loop = ringloop.new_event_loop(backend=sys.argv[1])
loop.run_until_complete(asyncio.sleep(0))
pid = os.fork()
if pid == 0:
    try:
        loop.run_until_complete(asyncio.sleep(0))
    except RuntimeError as exc:
        ok = "closed" in str(exc) and loop.is_closed()
    else:
        ok = False
    loop.close()
    os._exit(0 if ok else 1)
_, status = os.waitpid(pid, 0)
assert os.waitstatus_to_exitcode(status) == 0
loop.run_until_complete(asyncio.sleep(0))  # the parent's loop is unharmed
loop.close()
print("ok")
""")
    out = subprocess.run([sys.executable, str(script), backend], capture_output=True, text=True, timeout=60,
                         env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)})
    assert out.returncode == 0, out.stdout + out.stderr
    assert out.stdout.strip() == "ok"
