"""What a release audit found: workers that share a listening socket, fork,
calls from other threads, protocol hand-over, half-closed connections."""

import asyncio
import gc
import os
import socket
import struct
import subprocess
import sys
import textwrap
import threading

import pytest

import ringloop

from .test_loop import HOST, Recorder, backend, run, serve, tls  # noqa: F401  (fixtures)


def script(code, *args, env=None, timeout=60):
    proc = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code), *args], capture_output=True, text=True, timeout=timeout,
        env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path), **(env or {})})
    assert proc.returncode == 0, proc.stdout + proc.stderr
    return proc.stdout


# -- several workers, one listening socket -----------------------------------------

def test_workers_that_share_a_listening_socket_share_the_connections(backend):
    # `uvicorn --workers N` and its like: every worker accepts on the socket
    # the parent bound. No worker may take all the connections.
    out = script("""
        import asyncio, os, socket, sys, threading, collections
        import ringloop
        sock = socket.socket(); sock.bind(("127.0.0.1", 0)); sock.listen(128)
        port = sock.getsockname()[1]
        class Pid(asyncio.Protocol):
            def connection_made(self, transport):
                asyncio.get_running_loop().call_later(0.01, self.answer, transport)  # a request that takes a while
            def answer(self, transport):
                transport.write(str(os.getpid()).encode()); transport.close()
        async def serve():
            await asyncio.get_running_loop().create_server(Pid, sock=sock)
            await asyncio.sleep(3600)
        workers = []
        for _ in range(3):
            pid = os.fork()
            if pid == 0:
                ringloop.run(serve(), backend=sys.argv[1]); os._exit(0)
            workers.append(pid)
        import time; time.sleep(0.5)
        answers = []
        def client():
            for _ in range(10):
                with socket.create_connection(("127.0.0.1", port), 10) as c:
                    answers.append(c.recv(100))
        threads = [threading.Thread(target=client) for _ in range(9)]
        for t in threads: t.start()
        for t in threads: t.join()
        for pid in workers: os.kill(pid, 9); os.waitpid(pid, 0)
        print(sorted(collections.Counter(answers).values()))
    """, backend)
    shares = eval(out)
    # With the defect one worker served all 90. How even the split is depends
    # on the machine; that every worker gets a real share does not.
    assert len(shares) == 3 and sum(shares) == 90 and min(shares) >= 3, shares


# -- fork ----------------------------------------------------------------------------

def test_a_forked_child_cannot_disturb_the_parents_loop(backend):
    # multiprocessing with the fork start method, os.fork() in a handler: the
    # child has copies of everything. What it does with them stays its own.
    script("""
        import asyncio, os, socket, sys
        import ringloop
        async def main():
            loop = asyncio.get_running_loop()
            class Echo(asyncio.Protocol):
                def connection_made(self, t): self.t = t
                def data_received(self, d): self.t.write(d)
            server = await loop.create_server(Echo, "127.0.0.1", 0)
            port = server.sockets[0].getsockname()[1]
            r1, w1 = await asyncio.open_connection("127.0.0.1", port)
            r2, w2 = await asyncio.open_connection("127.0.0.1", port)
            w1.write(b"a"); assert await r1.readexactly(1) == b"a"
            pid = os.fork()
            if pid == 0:
                ok = loop.is_closed()
                try:
                    w2.transport.abort(); w1.close(); server.close()     # all copies: no effect on the parent
                    for t in list(server._clients): t.abort()
                except Exception as exc:
                    print("child:", repr(exc)); ok = False
                try:
                    loop.call_soon_threadsafe(print)
                    ok = False
                except RuntimeError:
                    pass
                os._exit(0 if ok else 1)
            _, status = os.waitpid(pid, 0)
            assert os.waitstatus_to_exitcode(status) == 0, "the child failed"
            for i in range(20):                                           # both connections still work here
                w1.write(b"x"); w2.write(b"y")
                assert await asyncio.wait_for(r1.readexactly(1), 5) == b"x"
                assert await asyncio.wait_for(r2.readexactly(1), 5) == b"y"
            r3, w3 = await asyncio.open_connection("127.0.0.1", port)     # and the server still accepts
            w3.write(b"z"); assert await asyncio.wait_for(r3.readexactly(1), 5) == b"z"
            for w in (w1, w2, w3):
                w.close(); await w.wait_closed()
            server.close(); await server.wait_closed()
        ringloop.run(main(), backend=sys.argv[1])
        print("ok")
    """, backend)


# -- other threads -----------------------------------------------------------------

def test_close_from_another_thread_closes(backend):
    # The garbage collector runs finalizers on whichever thread triggers it;
    # StreamWriter.__del__ closes its transport.
    async def main():
        loop = asyncio.get_running_loop()
        server, port = await serve(Recorder)
        reader, writer = await asyncio.open_connection(HOST, port)
        while not server._clients:
            await asyncio.sleep(0.01)
        (peer,) = [t.get_protocol() for t in server._clients]
        cycle = {"writer": writer, "reader": reader}
        cycle["self"] = cycle
        del reader, writer, cycle
        await loop.run_in_executor(None, gc.collect)
        assert await asyncio.wait_for(peer.lost, 5) is None

        # And said outright, for both kinds of transport call.
        transport, proto = await loop.create_connection(Recorder, HOST, port)
        thread = threading.Thread(target=transport.abort)
        thread.start()
        await asyncio.wait_for(proto.lost, 5)
        thread.join()
        server.close()
        await server.wait_closed()

    gc.collect()
    gc.disable()
    try:
        run(main(), backend)
    finally:
        gc.enable()


def test_backend_is_answered_from_another_thread_before_the_first_socket(backend):
    async def main():
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: loop.backend)

    assert run(main(), backend) == backend


# -- protocol hand-over --------------------------------------------------------------

class Small(asyncio.BufferedProtocol):
    """Takes 100 bytes, then hands the connection to a Recorder."""

    def __init__(self):
        self.buf = bytearray(100)
        self.got = bytearray()
        self.next = Recorder()

    def connection_made(self, transport):
        self.transport = transport

    def get_buffer(self, sizehint):
        return self.buf

    def buffer_updated(self, nbytes):
        self.got += self.buf[:nbytes]
        self.next.transport = self.transport
        self.transport.set_protocol(self.next)


@pytest.mark.parametrize("secure", [False, True])
def test_set_protocol_inside_buffer_updated_gives_the_rest_to_the_new_protocol(backend, tls, secure):
    server_ctx, client_ctx = tls

    async def main():
        loop = asyncio.get_running_loop()
        protos = []
        server = await loop.create_server(lambda: protos.append(Small()) or protos[-1], HOST, 0,
                                          ssl=server_ctx if secure else None)
        port = server.sockets[0].getsockname()[1]
        reader, writer = await asyncio.open_connection(
            HOST, port, ssl=client_ctx if secure else None, server_hostname="localhost" if secure else None)
        payload = os.urandom(1000)
        writer.write(payload)
        await writer.drain()
        while not protos or len(protos[0].got) + len(protos[0].next.data) < 1000:
            await asyncio.sleep(0.01)
        assert bytes(protos[0].got) == payload[:100]
        assert bytes(protos[0].next.data) == payload[100:]
        writer.close()
        server.close()
        await server.wait_closed()

    run(main(), backend)


# -- the ends of connections ---------------------------------------------------------

async def _pair(loop, factory):
    with socket.socket() as listener:
        listener.bind((HOST, 0))
        listener.listen()
        peer = socket.socket()
        peer.connect(listener.getsockname())
        ours, _ = listener.accept()
    transport, proto = await loop.connect_accepted_socket(factory, ours)
    await asyncio.sleep(0.02)
    return transport, proto, peer


@pytest.mark.parametrize("ending", ["close", "reset"])
def test_a_half_closed_connection_is_released_when_the_other_half_ends(backend, ending):
    # eof_received() returned true and the protocol never closes: once both
    # directions are finished (or the peer resets) nothing could ever use
    # the connection again, and nothing else would notice.
    async def main():
        loop = asyncio.get_running_loop()

        def keeper():
            proto = Recorder()
            proto.keep_open = True
            return proto

        transport, proto, peer = await _pair(loop, keeper)
        peer.shutdown(socket.SHUT_WR)
        await asyncio.wait_for(proto.eof, 5)
        transport.write(b"the answer")
        await asyncio.sleep(0.05)
        assert peer.recv(100) == b"the answer"
        assert not proto.lost.done()  # still usable for writing
        if ending == "close":
            transport.write_eof()
        else:
            peer.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            peer.close()
        await asyncio.wait_for(proto.lost, 5)
        peer.close()
        await asyncio.sleep(0.05)
        assert loop._core.live_count() == 0

    run(main(), backend)


def test_input_before_a_reset_is_delivered_after_a_pause(backend):
    async def main():
        loop = asyncio.get_running_loop()
        transport, proto, peer = await _pair(loop, Recorder)
        transport.pause_reading()
        peer.send(b"x" * 1000)
        peer.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        peer.close()
        await asyncio.sleep(0.1)
        assert not proto.lost.done()
        transport.resume_reading()
        exc = await asyncio.wait_for(proto.lost, 5)
        assert bytes(proto.data) == b"x" * 1000 and isinstance(exc, ConnectionResetError)

    run(main(), backend)


def test_the_address_is_free_as_soon_as_the_loop_is_closed(backend):
    # asyncio.run() that returns with a server still open, then the next run
    # binds the same port: test suites and restart loops do this.
    with socket.socket() as probe:
        probe.bind((HOST, 0))
        port = probe.getsockname()[1]

    async def main():
        loop = asyncio.get_running_loop()
        await loop.create_server(Recorder, HOST, port)
        _, writer = await asyncio.open_connection(HOST, port)  # and a connection in flight
        writer.write(b"x")

    for _ in range(10):
        run(main(), backend)


def test_accepted_connections_have_nodelay_and_a_socket_object(backend):
    async def main():
        loop = asyncio.get_running_loop()
        server, port = await serve(Recorder)
        reader, writer = await asyncio.open_connection(HOST, port)
        while not server._clients:
            await asyncio.sleep(0.01)
        (transport,) = server._clients
        sock = transport.get_extra_info("socket")
        assert sock.family == socket.AF_INET and sock.type == socket.SOCK_STREAM
        assert sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY)
        assert transport.get_extra_info("peername") == writer.get_extra_info("sockname")
        writer.close()
        server.close()
        await server.wait_closed()

        udp, _ = await loop.create_datagram_endpoint(asyncio.DatagramProtocol, local_addr=(HOST, 0))
        assert isinstance(udp.get_extra_info("socket"), asyncio.trsock.TransportSocket)
        udp.close()

    run(main(), backend)


def test_out_of_descriptors_backs_off_and_recovers(backend):
    script("""
        import asyncio, resource, socket, sys, os
        import ringloop
        async def main():
            loop = asyncio.get_running_loop()
            errors = []
            loop.set_exception_handler(lambda loop, context: errors.append(context["message"]))
            class Hello(asyncio.Protocol):
                def connection_made(self, t): t.write(b"hi")
            server = await loop.create_server(Hello, "127.0.0.1", 0)
            port = server.sockets[0].getsockname()[1]
            def connect():
                s = socket.socket(); s.setblocking(False)
                try: s.connect(("127.0.0.1", port))
                except BlockingIOError: pass
                return s
            warm = connect(); await asyncio.sleep(0.2)                 # everything the loop needs is open
            clients = [connect() for _ in range(40)]                   # ours, before the limit drops
            await asyncio.sleep(0.3)
            used = len(os.listdir("/proc/self/fd"))
            more = [connect() for _ in range(30)]
            resource.setrlimit(resource.RLIMIT_NOFILE, (used + 30 + 5, resource.getrlimit(resource.RLIMIT_NOFILE)[1]))
            late = []
            try:
                late = [connect() for _ in range(4)]                   # uses up what is left; the server cannot accept all
            except OSError:
                pass
            for _ in range(50):
                await asyncio.sleep(0.05)
                if errors: break
            assert any("out of system resource" in e for e in errors), errors
            for s in clients + more + late: s.close()                  # room again
            await asyncio.sleep(1.5)                                   # the one-second back-off
            reader, writer = await asyncio.wait_for(asyncio.open_connection("127.0.0.1", port), 5)
            assert await asyncio.wait_for(reader.readexactly(2), 5) == b"hi"
            writer.close(); warm.close(); server.close()
        ringloop.run(main(), backend=sys.argv[1])
        print("ok")
    """, backend)


# -- the package -------------------------------------------------------------------

def test_install_and_debug_environment():
    out = script("""
        import asyncio, ringloop
        ringloop.install()
        async def main():
            return type(asyncio.get_running_loop())
        assert asyncio.run(main()) is ringloop.Loop
        loop = asyncio.new_event_loop()
        assert type(loop) is ringloop.Loop
        print(loop.get_debug())
        loop.close()
    """, env={"PYTHONASYNCIODEBUG": "1", "PYTHONWARNINGS": "error::DeprecationWarning"})
    assert out.strip() == "True"
    loop = ringloop.new_event_loop()
    assert loop.get_debug() is bool(sys.flags.dev_mode or os.environ.get("PYTHONASYNCIODEBUG"))
    loop.close()


def test_python_m_ringloop_says_what_this_system_gets():
    proc = subprocess.run([sys.executable, "-m", "ringloop"], capture_output=True, text=True, timeout=60,
                          env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)})
    assert proc.returncode == 0, proc.stderr
    assert ringloop.__version__ in proc.stdout and "a new loop uses: " in proc.stdout
    assert ("io_uring: available" in proc.stdout) or ("failed with" in proc.stdout)
