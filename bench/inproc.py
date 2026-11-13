"""In-process loop micro-benchmarks: no network peer, no framework.

    python bench/inproc.py [--loop ringloop|asyncio|uvloop] [--backend uring|epoll] [--json]

Each case reports CPU nanoseconds per operation (user + system time of this
process, best of --repeat runs), so a busy machine distorts it less than
wall-clock time would. `--wall` reports elapsed time instead.
"""
import argparse
import asyncio
import json
import os
import socket
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

ap = argparse.ArgumentParser()
ap.add_argument("--loop", default="ringloop")
ap.add_argument("--backend", default=None)
ap.add_argument("--repeat", type=int, default=5)
ap.add_argument("--json", action="store_true")
ap.add_argument("--wall", action="store_true")
ap.add_argument("--cases", default=None, help="comma-separated case names (default: all)")
ap.add_argument("--instructions", action="store_true",
                help="count user-space instructions per operation instead of time (perf_event_open); "
                     "nearly immune to whatever else the machine is doing")
args = ap.parse_args()


def noop(*a):
    pass


async def call_soon_batch(loop, n=200_000):
    # Schedule n callbacks, then let them all run.
    done = loop.create_future()
    call_soon = loop.call_soon
    for _ in range(n - 1):
        call_soon(noop)
    call_soon(done.set_result, None)
    await done
    return n


async def call_soon_chain(loop, n=200_000):
    # One callback per loop turn: each schedules the next.
    done = loop.create_future()
    left = [n]

    def step():
        left[0] -= 1
        if left[0]:
            loop.call_soon(step)
        else:
            done.set_result(None)

    loop.call_soon(step)
    await done
    return n


async def sleep0(loop, n=100_000):
    for _ in range(n):
        await asyncio.sleep(0)
    return n


async def tasks(loop, n=50_000):
    async def child():
        pass

    for _ in range(n // 100):
        await asyncio.gather(*[child() for _ in range(100)])
    return n


async def timer_churn(loop, n=200_000):
    # The timeout pattern: arm a timer, cancel it before it fires.
    call_later = loop.call_later
    for i in range(n):
        call_later(10 + (i % 97), noop).cancel()
        if not i % 1000:
            await asyncio.sleep(0)
    return n


async def timer_fire(loop, n=100_000):
    done = loop.create_future()
    call_later = loop.call_later
    for i in range(n - 1):
        call_later((i % 13) * 1e-6, noop)
    call_later(20e-6, done.set_result, None)
    await done
    await asyncio.sleep(0.001)
    return n


async def threadsafe(loop, n=100_000):
    done = loop.create_future()
    cst = loop.call_soon_threadsafe
    for _ in range(n - 1):
        cst(noop)
    cst(done.set_result, None)
    await done
    return n


async def sock_pingpong(loop, n=30_000):
    # add_reader-driven sock_* API over a socketpair.
    a, b = socket.socketpair()
    a.setblocking(False)
    b.setblocking(False)

    async def echo():
        while data := await loop.sock_recv(b, 64):
            await loop.sock_sendall(b, data)

    t = loop.create_task(echo())
    for _ in range(n):
        await loop.sock_sendall(a, b"x")
        await loop.sock_recv(a, 64)
    a.close()
    await t
    b.close()
    return n


class _Echo(asyncio.Protocol):
    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        self.transport.write(data)


async def stream_echo(loop, n=30_000):
    # One client and one server transport in the same loop, 1 KiB each way.
    server = await loop.create_server(_Echo, "127.0.0.1", 0)
    reader, writer = await asyncio.open_connection("127.0.0.1", server.sockets[0].getsockname()[1])
    payload = b"x" * 1024
    for _ in range(n):
        writer.write(payload)
        await reader.readexactly(1024)
    writer.close()
    server.close()
    await server.wait_closed()
    return n


class _HttpLike(asyncio.BufferedProtocol):
    """The shape of Stario's protocol: buffered reads, writelines replies."""

    PARTS = (b"HTTP/1.1 200 OK\r\n", b"date: Tue, 10 Nov 2026 12:00:00 GMT\r\n", b"content-type: ",
             b"text/plain; charset=utf-8", b"\r\ncontent-length: ", b"13", b"\r\n\r\n", b"Hello, World!")

    def __init__(self):
        self.buf = memoryview(bytearray(65536))

    def connection_made(self, transport):
        self.writelines = transport.writelines

    def get_buffer(self, sizehint):
        return self.buf

    def buffer_updated(self, nbytes):
        self.writelines(self.PARTS)


async def transport_pingpong(loop, n=50_000, clients=16):
    # Protocol callbacks only, no tasks: request -> writelines reply -> next
    # request, on `clients` connections at once. The loop's own cost per
    # request/response pair on the transport path.
    done = loop.create_future()
    left = [n]
    request = b"GET /plaintext HTTP/1.1\r\nhost: bench\r\n\r\n"

    class Client(asyncio.Protocol):
        def connection_made(self, transport):
            self.transport = transport
            transport.write(request)

        def data_received(self, data):
            left[0] -= 1
            if left[0] > 0:
                self.transport.write(request)
            elif not done.done():
                done.set_result(None)

    server = await loop.create_server(_HttpLike, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    transports = [(await loop.create_connection(Client, "127.0.0.1", port))[0] for _ in range(clients)]
    await done
    for transport in transports:
        transport.abort()
    server.close()
    server.abort_clients()
    await server.wait_closed()
    return n


async def connect_close(loop, n=3_000):
    server = await loop.create_server(asyncio.Protocol, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    for _ in range(n):
        transport, _ = await loop.create_connection(asyncio.Protocol, "127.0.0.1", port)
        transport.close()
    server.close()
    return n


_TLS = None


def _tls_contexts():
    """A throwaway self-signed certificate (needs the openssl command)."""
    global _TLS
    if _TLS is None:
        import os
        import ssl
        import subprocess
        import tempfile

        d = tempfile.mkdtemp()
        cert, key = os.path.join(d, "cert.pem"), os.path.join(d, "key.pem")
        subprocess.run(
            ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2", "-subj", "/CN=localhost",
             "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1", "-keyout", key, "-out", cert],
            check=True, capture_output=True)
        server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server.load_cert_chain(cert, key)
        _TLS = server, ssl.create_default_context(cafile=cert)
    return _TLS


async def tls_echo(loop, n=20_000):
    # As stream_echo, with TLS on both ends.
    server_ctx, client_ctx = _tls_contexts()
    server = await loop.create_server(_Echo, "127.0.0.1", 0, ssl=server_ctx)
    reader, writer = await asyncio.open_connection(
        "127.0.0.1", server.sockets[0].getsockname()[1], ssl=client_ctx, server_hostname="localhost")
    payload = b"x" * 1024
    for _ in range(n):
        writer.write(payload)
        await reader.readexactly(1024)
    writer.close()
    server.close()
    await server.wait_closed()
    return n


CASES = [call_soon_batch, call_soon_chain, sleep0, tasks, timer_churn, timer_fire,
         threadsafe, sock_pingpong, transport_pingpong, stream_echo, tls_echo, connect_close]


async def main():
    clock = time.perf_counter if args.wall else time.process_time
    if args.instructions:
        from perfcount import instruction_counter

        count = instruction_counter()
        clock = lambda: count() / 1e9  # noqa: E731  (same scaling as the clocks)
    loop = asyncio.get_running_loop()
    out = {}
    for case in CASES:
        if args.cases and case.__name__ not in args.cases.split(","):
            continue
        best = None
        for _ in range(args.repeat):
            t0 = clock()
            n = await case(loop)
            ns = (clock() - t0) / n * 1e9
            best = ns if best is None else min(best, ns)
        out[case.__name__] = round(best, 1)
    return out


if args.loop == "uvloop":
    import uvloop

    result = uvloop.run(main())
elif args.loop == "ringloop":
    import ringloop

    result = ringloop.run(main(), backend=args.backend)
else:
    result = asyncio.run(main())
if args.json:
    print(json.dumps(result))
else:
    for name, ns in result.items():
        print(f"{name:<20}{ns:>10.1f} {'instructions' if args.instructions else 'ns'}/op")
