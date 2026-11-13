"""A minimal HTTP-shaped protocol, no framework: isolates the loop's own cost.

    python bench/micro_server.py --loop ringloop --port 8000 [--read plain|buffered]
                                 [--write one|lines|lines8] [--threads N] [--work N] [--defer]

Answers every request head with the same 130-byte response Stario sends for
/plaintext. `--read buffered` uses asyncio.BufferedProtocol (as Stario
does); `--write lines` sends the response as four pieces with writelines()
(as Stario does), `one` as a single write().

`--threads N` runs one loop per thread, each with its own listening socket
on the port (SO_REUSEPORT: the kernel spreads the connections). That is the
way to use several cores on a free-threaded build. `--work N` adds N steps
of pure-Python arithmetic per request, to stand in for an application.
`--defer` answers from one call_soon callback after the turn's reads instead
of inside the read callback, which is what ringloop's end-of-turn flush does
on its own (see results/stario-2026-11-12/README.md).

`--loop`: ringloop, uvloop, rloop or asyncio. RINGLOOP_BACKEND picks the
ringloop backend.
"""
import argparse
import asyncio
import sys
import threading

ap = argparse.ArgumentParser()
ap.add_argument("--loop", default="asyncio")
ap.add_argument("--port", type=int, default=8000)
ap.add_argument("--read", default="plain")
ap.add_argument("--write", default="one")
ap.add_argument("--threads", type=int, default=1)
ap.add_argument("--work", type=int, default=0)
ap.add_argument("--defer", action="store_true")
args = ap.parse_args()

PARTS = (
    b"HTTP/1.1 200 OK\r\n",
    b"content-type: text/plain; charset=utf-8\r\ndate: Tue, 10 Nov 2026 12:00:00 GMT\r\n",
    b"content-length: 13\r\n\r\n",
    b"Hello, World!",
)
if args.write == "lines8":
    # The eight pieces Stario hands to writelines() for a plaintext response.
    PARTS = (b"HTTP/1.1 200 OK\r\n", b"date: Tue, 10 Nov 2026 12:00:00 GMT\r\n", b"content-type: ",
             b"text/plain; charset=utf-8", b"\r\ncontent-length: ", b"13", b"\r\n\r\n", b"Hello, World!")
LINES = args.write.startswith("lines")
WORK = args.work
DEFER = args.defer


def make_protocols():
    """The protocol classes of one thread. Each thread gets its own classes
    and its own response objects: on a free-threaded build, objects that
    every thread touches on every request (their reference counts) would
    measure that sharing, not the loop."""
    parts = tuple(bytes(bytearray(p)) for p in PARTS)
    resp = b"".join(parts)
    pending = []

    def flush():
        for write, n in pending:
            reply(write, n)
        pending.clear()

    def respond(write, n):
        if DEFER:
            pending.append((write, n))
            if len(pending) == 1:
                asyncio.get_running_loop().call_soon(flush)
        else:
            reply(write, n)

    def reply(write, n):
        if WORK:
            for _ in range(n):
                total = 0
                for i in range(WORK):
                    total += i * i % 7
        if LINES:
            for _ in range(n):
                write(parts)
        else:
            write(resp * n if n != 1 else resp)

    class Plain(asyncio.Protocol):
        def connection_made(self, transport):
            self.write = transport.writelines if LINES else transport.write

        def data_received(self, data):
            respond(self.write, data.count(b"\r\n\r\n"))

    class Buffered(asyncio.BufferedProtocol):
        def __init__(self):
            self.buf = bytearray(65536)
            self.view = memoryview(self.buf)

        def connection_made(self, transport):
            self.write = transport.writelines if LINES else transport.write

        def get_buffer(self, sizehint):
            return self.view

        def buffer_updated(self, nbytes):
            respond(self.write, self.buf.count(b"\r\n\r\n", 0, nbytes))

    return Buffered if args.read == "buffered" else Plain


async def main():
    loop = asyncio.get_running_loop()
    server = await loop.create_server(make_protocols(), "127.0.0.1", args.port, backlog=2048,
                                      reuse_port=True)
    # Not serve_forever(): rloop 0.5 stops listening when it is called on a
    # server that create_server() has already started.
    await asyncio.Event().wait()
    server.close()


def loop_factory():
    if args.loop == "uvloop":
        import uvloop
        return uvloop.new_event_loop()
    if args.loop == "rloop":
        import rloop
        return rloop.new_event_loop()
    if args.loop == "ringloop":
        import ringloop
        return ringloop.new_event_loop()
    return asyncio.new_event_loop()


def run():
    asyncio.run(main(), loop_factory=loop_factory)


if args.threads == 1:
    run()
else:
    if getattr(sys, "_is_gil_enabled", lambda: True)():
        print("warning: the GIL is on; threads will not run in parallel", file=sys.stderr)
    threads = [threading.Thread(target=run, daemon=True) for _ in range(args.threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
