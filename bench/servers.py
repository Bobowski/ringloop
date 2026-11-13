"""One ASGI application under several servers and loops.

    python bench/servers.py [--rounds 7] [--duration 3]

uvicorn (httptools) on ringloop (both backends), uvloop and asyncio, and
Granian (one worker) on rloop, uvloop, asyncio and ringloop. Every server is
pinned to CPUs 0-1 and loaded by bench/loadgen from CPUs 2-3; rounds are
interleaved. Granian does its HTTP and its I/O in Rust threads, so it uses
more than one core: read "cores" next to "req/s".

Needs uvicorn, httptools, uvloop, rloop and granian (a server that is not
installed is left out).
"""
import argparse
import json
import os
import signal
import statistics
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
from run import wait_port  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--rounds", type=int, default=7)
ap.add_argument("--duration", type=int, default=3)
ap.add_argument("--connections", default="128")
args = ap.parse_args()
PY = sys.executable
ENV = dict(os.environ, PYTHONPATH=os.pathsep.join([os.path.join(ROOT, "src"), HERE]))


def uvicorn(loop, **env):
    return lambda port: (["env", *[f"{k}={v}" for k, v in env.items()], PY, "-m", "uvicorn", "asgi_app:app",
                          "--port", str(port), "--loop", loop, "--http", "httptools",
                          "--no-access-log", "--log-level", "warning"])


def granian(loop):
    return lambda port: [PY, "-m", "granian", "--interface", "asgi", "--loop", loop, "--workers", "1",
                         "--port", str(port), "--no-access-log", "--log-level", "warning", "asgi_app:app"]


SERVERS = {
    "uvicorn + ringloop": uvicorn("ringloop:new_event_loop"),
    "uvicorn + ringloop/epoll": uvicorn("ringloop:new_event_loop", RINGLOOP_BACKEND="epoll"),
    "uvicorn + uvloop": uvicorn("uvloop"),
    "uvicorn + asyncio": uvicorn("asyncio"),
    "granian + rloop": granian("rloop"),
    "granian + uvloop": granian("uvloop"),
    "granian + asyncio": granian("asyncio"),
    "granian + ringloop": lambda port: [PY, os.path.join(HERE, "granian_on_ringloop.py"), str(port)],
}


def tree_cpu(pid):
    """CPU seconds of a process and its children (Granian's workers)."""
    tick = os.sysconf("SC_CLK_TCK")
    pids = [pid] + [int(p) for p in subprocess.run(["pgrep", "-P", str(pid)], capture_output=True,
                                                   text=True).stdout.split()]
    total = 0.0
    for p in pids:
        try:
            with open(f"/proc/{p}/stat") as f:
                parts = f.read().rsplit(")", 1)[1].split()
            total += (int(parts[11]) + int(parts[12])) / tick
        except OSError:
            pass
    return total


procs = {}
for i, (name, command) in enumerate(SERVERS.items()):
    port = 8700 + i
    p = subprocess.Popen(["taskset", "-c", "0,1", *command(port)], env=ENV, cwd=HERE,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    try:
        wait_port(port, p)
        procs[name] = (p, port)
    except SystemExit as exc:
        print(f"{name}: left out ({exc})")
try:
    res = {name: [] for name in procs}
    for _ in range(args.rounds):
        for name, (p, port) in procs.items():
            before = tree_cpu(p.pid)
            out = json.loads(subprocess.run(
                [os.path.join(HERE, "loadgen"), "-c", args.connections, "-d", str(args.duration), "-w", "1",
                 "-t", "2", "-C", "2,3", f"http://127.0.0.1:{port}/"], capture_output=True, text=True, check=True).stdout)
            cpu = tree_cpu(p.pid) - before
            total = out["rps"] * (args.duration + 1) or 1
            res[name].append((out["rps"], cpu / total * 1e6, cpu / (args.duration + 1),
                              out["p99_us"] / 1000, out["errors"] + out["non2xx"]))
    print(f"hello-world ASGI app, {args.connections} connections, median of {args.rounds}x{args.duration}s")
    print(f"{'server':<28}{'req/s':>9}{'CPU µs/req':>12}{'cores':>7}{'p99 ms':>8}  errors")
    for name, rows in res.items():
        def med(k):
            return statistics.median(r[k] for r in rows)
        print(f"{name:<28}{med(0):>9.0f}{med(1):>12.1f}{med(2):>7.2f}{med(3):>8.2f}  {sum(r[4] for r in rows)}")
finally:
    for p, _ in procs.values():
        try:
            os.killpg(p.pid, signal.SIGKILL)
        except OSError:
            pass
