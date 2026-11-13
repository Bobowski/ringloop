"""TLS keep-alive load: asyncio + ssl clients, one process per client CPU.

    python bench/tls_client.py PORT CONNS SECONDS CA_FILE

Python clients cannot saturate a fast server, so read the server's CPU per
request (printed by bench/tls.py), not the req/s.
"""
import asyncio
import json
import multiprocessing as mp
import os
import ssl
import sys
import time

REQ = b"GET /plaintext HTTP/1.1\r\nHost: localhost\r\n\r\n"


def worker(port, conns, seconds, ca):
    import uvloop

    ctx = ssl.create_default_context(cafile=ca)
    ctx.set_alpn_protocols(["http/1.1"])
    count = 0

    async def one(deadline):
        nonlocal count
        r, w = await asyncio.open_connection("127.0.0.1", port, ssl=ctx, server_hostname="localhost")
        while time.monotonic() < deadline:
            w.write(REQ)
            head = await r.readuntil(b"\r\n\r\n")
            i = head.lower().index(b"content-length:") + 15
            await r.readexactly(int(head[i:head.index(b"\r\n", i)]))
            count += 1
        w.close()

    async def main():
        deadline = time.monotonic() + seconds
        await asyncio.gather(*(one(deadline) for _ in range(conns)))

    uvloop.run(main())
    return count


if __name__ == "__main__":
    port, conns, seconds, ca = int(sys.argv[1]), int(sys.argv[2]), float(sys.argv[3]), sys.argv[4]
    procs = len(os.sched_getaffinity(0))
    with mp.get_context("fork").Pool(procs) as pool:
        counts = pool.starmap(worker, [(port, conns // procs, seconds, ca)] * procs)
    print(json.dumps({"requests": sum(counts), "rps": sum(counts) / seconds}))
