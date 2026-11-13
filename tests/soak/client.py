"""Soak-test client: hammer tests/soak/server.py with well-behaved and badly
behaved connections, then check that the server holds nothing.

    python tests/soak/client.py --port 9000 [--tls-port 9001] --seconds 60 [--concurrency 200]

Runs on asyncio's own loop (the server is what is under test). Exits 1 if
the server still holds transports or descriptors afterwards, if its memory
kept growing, or if a well-behaved exchange got wrong data.
"""
import argparse
import asyncio
import json
import os
import random
import socket
import ssl
import struct
import sys
import time

ap = argparse.ArgumentParser()
ap.add_argument("--port", type=int, required=True)
ap.add_argument("--tls-port", type=int)
ap.add_argument("--seconds", type=float, default=30)
ap.add_argument("--concurrency", type=int, default=200)
ap.add_argument("--seed", type=int, default=1)
ap.add_argument("--modes", default="EEEBPAWS", help="server behaviours to pick from (see server.py)")
ap.add_argument("--endings", default="close,close,reset,vanish,half")
ap.add_argument("--tls-share", type=float, default=0.3)
args = ap.parse_args()

LINGER0 = struct.pack("ii", 1, 0)
counts = {}
wrong = []


def reset(writer):
    sock = writer.get_extra_info("socket")
    if sock is not None:
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, LINGER0)
        except OSError:
            pass
    writer.transport.abort()


async def one(rng, tls_ctx, what):
    use_tls = tls_ctx is not None and rng.random() < args.tls_share
    mode = rng.choice(args.modes.encode())
    ending = rng.choice(args.endings.split(","))
    key = f"{'tls' if use_tls else 'tcp'}:{chr(mode)}:{ending}"
    counts[key] = counts.get(key, 0) + 1
    what.append(key)
    port = args.tls_port if use_tls else args.port
    reader, writer = await asyncio.open_connection("127.0.0.1", port, ssl=tls_ctx if use_tls else None)
    try:
        payload = os.urandom(rng.choice([1, 100, 5000, 70000, 300000]))
        writer.write(bytes([mode]) + payload)
        if ending == "vanish" and rng.random() < 0.5:
            reset(writer)  # at once, with the request maybe still in our buffer
            return
        if mode in b"EP":
            if ending in ("reset", "vanish"):
                await reader.read(rng.choice([1, 1000, 100000]))
                reset(writer)
                return
            got = await reader.readexactly(len(payload))
            if got != payload:
                wrong.append(key)
        elif mode in b"BS":
            want = 4096 * 1024 if mode == ord("B") else 1024 * 1024
            if ending in ("reset", "vanish"):
                await reader.read(rng.choice([1, 50000, 2000000]))  # part of it, then go
                reset(writer)
                return
            got = await reader.read(-1)
            if len(got) != want:
                wrong.append(f"{key} got {len(got)} of {want}")
        elif mode == ord("W"):
            if ending == "half" and not use_tls:
                writer.write_eof()
            await reader.read(-1)
        elif mode == ord("A"):
            await reader.read(-1)
        if ending == "reset":
            reset(writer)
        else:
            writer.close()
            await writer.wait_closed()
    except (ConnectionError, asyncio.IncompleteReadError, ssl.SSLError, OSError):
        writer.transport.abort()


async def worker(rng, tls_ctx, deadline):
    while time.monotonic() < deadline:
        what = []
        try:
            await asyncio.wait_for(one(rng, tls_ctx, what), 30)
        except TimeoutError:
            wrong.append(f"{what[0] if what else 'connect'}: no end after 30 s")
        except (ConnectionError, OSError, ssl.SSLError):
            pass


async def stats():
    reader, writer = await asyncio.open_connection("127.0.0.1", args.port)
    writer.write(b"?")
    line = await reader.readline()
    writer.close()
    return json.loads(line)


async def main():
    tls_ctx = None
    if args.tls_port:
        tls_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        tls_ctx.check_hostname = False
        tls_ctx.verify_mode = ssl.CERT_NONE
    before = await stats()
    samples = []
    rounds = 4
    for i in range(rounds):  # memory may reach a plateau, but must not climb round after round
        deadline = time.monotonic() + args.seconds / rounds
        await asyncio.gather(*[worker(random.Random(args.seed * 100000 + i * 1000 + n), tls_ctx, deadline)
                               for n in range(args.concurrency)])
        await asyncio.sleep(11)  # server-side TLS handshake and shutdown timeouts (10 s) run out
        samples.append(await stats())
    after = samples[-1]
    total = sum(counts.values())
    print(f"{after['backend']}: {total} connections in {args.seconds:.0f} s; "
          f"live {before['live']} -> {after['live']}, fds {before['fds']} -> {after['fds']}, "
          f"rss MiB {before['rss'] >> 20} -> {' -> '.join(str(s['rss'] >> 20) for s in samples)}, tasks {after['tasks']}")
    problems = list(dict.fromkeys(wrong))
    if after["live"] != before["live"]:
        problems.append(f"the loop still holds {after['live'] - before['live']} transports")
    if after["fds"] != before["fds"]:
        problems.append(f"{after['fds'] - before['fds']} descriptors were not released")
    if samples[-1]["rss"] > max(s["rss"] for s in samples[:-1]) * 1.15 + (16 << 20):
        problems.append("memory kept growing")
    for p in problems[:20]:
        print("PROBLEM:", p)
    sys.exit(1 if problems else 0)


asyncio.run(main())
