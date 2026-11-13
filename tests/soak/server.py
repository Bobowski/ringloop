"""Soak-test server: every connection picks a behaviour with its first byte.

    python tests/soak/server.py --port 9000 [--tls-port 9001 --cert c.pem --key k.pem]

  E  echo what arrives
  B  answer with 4 MiB, then close
  P  pause reading for 20 ms on every chunk, then echo it
  A  abort the connection after the first chunk
  W  echo the first chunk, then write_eof() (half-close)
  S  send a 1 MiB file with loop.sendfile(), then close
  ?  (control) answer one line of JSON: transports the loop still holds,
     open descriptors, resident memory; then close

tests/soak/client.py drives it and checks that nothing is left behind.
"""
import argparse
import asyncio
import json
import os
import ssl
import tempfile

import ringloop

ap = argparse.ArgumentParser()
ap.add_argument("--port", type=int, required=True)
ap.add_argument("--tls-port", type=int)
ap.add_argument("--cert")
ap.add_argument("--key")
args = ap.parse_args()

BIG = os.urandom(1024) * 4096
FILE = tempfile.NamedTemporaryFile()
FILE.write(os.urandom(1024 * 1024))
FILE.flush()


def stats(loop):
    with open("/proc/self/statm") as f:
        rss = int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE")
    return {"live": loop._core.live_count(), "fds": len(os.listdir("/proc/self/fd")), "rss": rss,
            "backend": loop.backend, "tasks": len(asyncio.all_tasks(loop))}


class Chaos(asyncio.Protocol):
    def connection_made(self, transport):
        self.transport = transport
        self.mode = None
        self.loop = asyncio.get_running_loop()

    def data_received(self, data):
        transport = self.transport
        if self.mode is None:
            self.mode, data = data[:1], data[1:]
            if self.mode == b"?":
                transport.write(json.dumps(stats(self.loop)).encode() + b"\n")
                transport.close()
                return
            if self.mode == b"B":
                transport.write(BIG)
                transport.close()
                return
            if self.mode == b"A":
                transport.abort()
                return
            if self.mode == b"S":
                self.loop.create_task(self.send_file())
                return
        if self.mode == b"E":
            transport.write(data)
        elif self.mode == b"P":
            transport.pause_reading()
            self.loop.call_later(0.02, self.resume, data)
        elif self.mode == b"W":
            transport.write(data)
            if transport.can_write_eof():
                transport.write_eof()
            else:
                transport.close()

    def resume(self, data):
        if not self.transport.is_closing():
            self.transport.write(data)
            self.transport.resume_reading()

    async def send_file(self):
        try:
            with open(FILE.name, "rb") as f:  # its own file position
                await self.loop.sendfile(self.transport, f, 0)
        except (OSError, RuntimeError):
            pass
        self.transport.close()

    def eof_received(self):
        return self.mode == b"W"  # keep the write side for a half-closed peer


async def main():
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(lambda loop, context: None)  # resets are the point
    servers = [await loop.create_server(Chaos, "127.0.0.1", args.port, backlog=1024)]
    if args.tls_port:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(args.cert, args.key)
        servers.append(await loop.create_server(Chaos, "127.0.0.1", args.tls_port, ssl=ctx, backlog=1024,
                                                ssl_handshake_timeout=10, ssl_shutdown_timeout=10))
    print("ready", flush=True)
    await asyncio.Event().wait()


ringloop.run(main())
