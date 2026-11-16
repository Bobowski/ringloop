"""What comparing the loop with uvloop, rloop and the io_uring runtimes
found: output slept over under epoll, add_reader bookkeeping after a
descriptor number is reused, TLS shutdown while the peer still sends, debug
mode on a thread without a Python frame."""

import _thread
import asyncio
import socket
import threading
import time

import pytest

from .test_loop import HOST, backend, run, tls  # noqa: F401  (fixtures)


# -- epoll: output written while the turn's flush drains ---------------------------

def _stream_capacity(sndbuf):
    a, b = socket.socketpair()
    try:
        a.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, sndbuf)
        a.setblocking(False)
        return a.send(b"x" * (4 << 20))
    finally:
        a.close()
        b.close()


def test_write_from_resume_writing_in_flush_is_sent(backend):
    """Under epoll the end-of-turn flush can drain a paused transport and call
    resume_writing(); what that writes must not wait for the next event."""
    sndbuf = 32768
    total = _stream_capacity(sndbuf) + 8000

    class P(asyncio.Protocol):
        paused = False

        def connection_made(self, tr):
            self.tr = tr
            tr.set_write_buffer_limits(high=1000, low=0)

        def pause_writing(self):
            self.paused = True

        def resume_writing(self):
            self.paused = False
            self.tr.write(b"Z")

    async def main():
        loop = asyncio.get_running_loop()
        a, b = socket.socketpair()
        a.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, sndbuf)
        a.setblocking(False)
        tr, pr = await loop.connect_accepted_socket(P, a)
        got = bytearray()
        done = loop.create_future()

        def peer():
            b.settimeout(1.0)
            try:
                while len(got) < total + 1:
                    d = b.recv(1 << 20)
                    if not d:
                        break
                    got.extend(d)
            except socket.timeout:
                pass
            loop.call_soon_threadsafe(done.set_result, len(got))

        tr.write(b"y" * total)
        assert pr.paused
        threading.Thread(target=peer, daemon=True).start()
        time.sleep(0.3)  # the peer empties the socket; the loop is blocked here
        n = await done
        tr.close()
        b.close()
        assert n == total + 1

    run(main(), backend)


# -- add_reader bookkeeping after a descriptor number is reused --------------------

def test_cancelled_sock_recv_on_closed_socket_spares_new_transport(backend):
    """A sock_recv() whose socket was closed under it, cancelled after the
    number went to an accepted connection, must not unregister that
    connection (epoll backend)."""
    async def main():
        loop = asyncio.get_running_loop()
        got = loop.create_future()
        made = loop.create_future()

        class Srv(asyncio.Protocol):
            def connection_made(self, tr):
                made.set_result(tr)

            def data_received(self, d):
                if not got.done():
                    got.set_result(d)

        srv = await loop.create_server(Srv, HOST, 0)
        addr = srv.sockets[0].getsockname()
        s1, s2 = socket.socketpair()
        s1.setblocking(False)
        pending = asyncio.ensure_future(loop.sock_recv(s1, 100))
        for _ in range(3):
            await asyncio.sleep(0)
        num = s1.fileno()
        c = socket.socket()
        s1.close()
        c.connect(addr)
        tr = await asyncio.wait_for(made, 5)
        if tr.get_extra_info("socket").fileno() != num:
            pytest.skip("descriptor number not reused")
        pending.cancel()
        with pytest.raises(BaseException):
            await pending
        c.sendall(b"hello")
        assert await asyncio.wait_for(got, 2) == b"hello"
        c.close()
        s2.close()
        tr.close()
        srv.close()

    run(main(), backend)


def test_sock_recv_on_reused_number_is_registered(backend):
    """A socket closed under a pending sock_recv(); a new socket gets the
    same number; a sock_recv() on it must wake when data arrives."""
    async def main():
        loop = asyncio.get_running_loop()
        s1, s2 = socket.socketpair()
        s1.setblocking(False)
        stale = asyncio.ensure_future(loop.sock_recv(s1, 100))
        for _ in range(3):
            await asyncio.sleep(0)
        num = s1.fileno()
        s1.close()
        s2.close()
        s3, s4 = socket.socketpair()
        r, w = (s3, s4) if s3.fileno() == num else (s4, s3)
        if r.fileno() != num:
            pytest.skip("descriptor number not reused")
        r.setblocking(False)
        t = asyncio.ensure_future(loop.sock_recv(r, 100))
        for _ in range(3):
            await asyncio.sleep(0)
        w.send(b"ping")
        assert await asyncio.wait_for(t, 2) == b"ping"
        stale.cancel()
        try:
            await stale
        except BaseException:
            pass
        s3.close()
        s4.close()

    run(main(), backend)


def test_remove_writer_after_a_transport_took_the_socket_lets_the_loop_sleep(backend):
    """add_writer(), then a transport over the same socket, then remove_writer()
    (cleanup in a finally): the registration must go, or under io_uring (where
    the transport has none in the epoll set) "writable" fires every turn and
    the idle loop spins."""
    async def main():
        loop = asyncio.get_running_loop()
        a, b = socket.socketpair()
        a.setblocking(False)
        loop.add_writer(a, lambda: None)
        tr, _ = await loop.connect_accepted_socket(asyncio.Protocol, a)
        loop.remove_writer(a)
        core = loop._core
        await asyncio.sleep(0.05)
        before = core.turns
        await asyncio.sleep(0.3)
        turns = core.turns - before
        tr.close()
        b.close()
        assert turns < 50, f"{turns} turns in 0.3 s of idling"

    run(main(), backend)


# -- TLS shutdown while the peer still sends ---------------------------------------

def test_tls_close_while_peer_still_sends(backend, tls):
    """The server closes (close_notify out) while the client still sends
    application data, then the client answers with close_notify: a clean
    TLS shutdown on both sides, not a dropped TCP connection."""
    server_ctx, client_ctx = tls

    async def main():
        loop = asyncio.get_running_loop()
        lost = loop.create_future()

        class Srv(asyncio.Protocol):
            n = 0

            def connection_made(self, tr):
                self.tr = tr

            def data_received(self, data):
                self.n += 1
                if self.n == 1:
                    self.tr.close()

            def connection_lost(self, exc):
                lost.set_result(exc)

        srv = await loop.create_server(Srv, HOST, 0, ssl=server_ctx)
        port = srv.sockets[0].getsockname()[1]
        result = {}

        def client():
            raw = socket.create_connection((HOST, port), timeout=5)
            try:
                ss = client_ctx.wrap_socket(raw, server_hostname="localhost")
                ss.sendall(b"hello")
                time.sleep(0.2)
                for _ in range(8):
                    ss.sendall(b"t" * 4000)
                time.sleep(0.1)
                ss.settimeout(5)
                ss.unwrap()
                result["client"] = "clean"
            except Exception as exc:
                result["client"] = repr(exc)
            finally:
                raw.close()

        t = threading.Thread(target=client, daemon=True)
        t.start()
        exc = await asyncio.wait_for(lost, 10)
        await loop.run_in_executor(None, t.join)
        srv.close()
        assert exc is None
        assert result["client"] == "clean"

    run(main(), backend)


# -- debug mode --------------------------------------------------------------------

def test_debug_call_soon_threadsafe_without_python_frame(backend):
    """Debug mode: call_soon_threadsafe() from a thread with no Python frame
    (a native thread) must schedule the callback."""
    async def main():
        loop = asyncio.get_running_loop()
        loop.set_debug(True)
        ran = loop.create_future()
        _thread.start_new_thread(loop.call_soon_threadsafe, (lambda: ran.set_result(1),))
        assert await asyncio.wait_for(ran, 2) == 1

    run(main(), backend)
