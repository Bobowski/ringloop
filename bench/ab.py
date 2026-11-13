"""A/B two ringloop checkouts against each other, interleaved, on one machine.

    python bench/ab.py --base /path/to/other/checkout [--micro] [--inproc] [--rounds 5]

`--base` is another checkout of this repository with its extension built in
place (`python setup.py build_ext --inplace`); "new" is this checkout. The
same benchmark files (from this checkout) run against both, alternating, so
machine drift hits both sides alike. Prints medians and the relative change.

  --inproc  bench/inproc.py cases, CPU ns/op (lower is better);
            with --instructions, user-space instructions/op
  --micro   bench/micro_server.py under bench/loadgen: req/s, server CPU
            per request, and the server's user-space instructions per request
  --churn   bench/micro_server.py under bench/churn_client.py (one request
            per connection): server CPU per connection

Time-based numbers need a quiet machine. The instruction counts do not: use
them when other work is running, or for changes of a few percent.
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
from perfcount import instruction_counter  # noqa: E402
from run import cpu_times, wait_port  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--base", required=True)
ap.add_argument("--inproc", action="store_true")
ap.add_argument("--micro", action="store_true")
ap.add_argument("--churn", action="store_true")
ap.add_argument("--instructions", action="store_true",
                help="with --inproc: user-space instructions per operation instead of CPU time")
ap.add_argument("--rounds", type=int, default=5)
ap.add_argument("--cases", default=None, help="with --inproc: comma-separated case names")
ap.add_argument("--duration", type=int, default=3)
ap.add_argument("--backends", default="uring,epoll")
ap.add_argument("--styles", default="buffered:one,buffered:lines8,plain:one")
ap.add_argument("--pipeline", default="1,16")
ap.add_argument("--connections", default="128")
args = ap.parse_args()
SIDES = [("base", os.path.join(os.path.abspath(args.base), "src")), ("new", os.path.join(ROOT, "src"))]
BACKENDS = args.backends.split(",")


def env_for(src, backend):
    return dict(os.environ, PYTHONPATH=src, RINGLOOP_BACKEND=backend)


def pct(new, base):
    return f"{(new / base - 1) * 100:+6.1f}%" if base else "   n/a"


if args.inproc:
    unit = "user-space instructions/op" if args.instructions else "CPU ns/op"
    print(f"in-process, {unit}, median of {args.rounds} processes (lower is better)")
    print(f"{'case':<20}{'backend':<8}{'base':>10}{'new':>10}{'change':>9}")
    res = {}
    for _ in range(args.rounds):
        for backend in BACKENDS:
            for side, src in SIDES:
                out = subprocess.run(
                    ["taskset", "-c", "3", sys.executable, os.path.join(HERE, "inproc.py"), "--json", "--repeat", "3",
                     *(["--instructions"] if args.instructions else []),
                     *(["--cases", args.cases] if args.cases else [])],
                    env=env_for(src, backend), capture_output=True, text=True, check=True).stdout
                for case, ns in json.loads(out).items():
                    res.setdefault((case, backend, side), []).append(ns)
    for case in dict.fromkeys(k[0] for k in res):
        for backend in BACKENDS:
            base = statistics.median(res[(case, backend, "base")])
            new = statistics.median(res[(case, backend, "new")])
            print(f"{case:<20}{backend:<8}{base:>10.1f}{new:>10.1f}{pct(new, base):>9}")
    print(flush=True)

if args.micro:
    print(f"micro_server under loadgen, median of {args.rounds}x{args.duration}s (req/s higher, CPU µs/req lower)")
    print(f"{'read:write':<16}{'backend':<8}{'c':>5}{'pipe':>5}{'base req/s':>12}{'new req/s':>11}{'change':>9}"
          f"{'base µs':>9}{'new µs':>8}{'change':>9}{'base instr':>12}{'new instr':>11}{'change':>9}")
    for style in args.styles.split(","):
        read, write = style.split(":")
        procs = []
        for backend in BACKENDS:
            for side, src in SIDES:
                port = 8900 + len(procs)
                p = subprocess.Popen(
                    ["taskset", "-c", "3", sys.executable, os.path.join(HERE, "micro_server.py"), "--loop", "ringloop",
                     "--port", str(port), "--read", read, "--write", write], env=env_for(src, backend))
                wait_port(port, p)
                try:
                    counter = instruction_counter(p.pid)
                except OSError:
                    counter = None
                procs.append((backend, side, p, port, counter))
        try:
            for conns in args.connections.split(","):
                for pipeline in args.pipeline.split(","):
                    res = {}
                    for _ in range(args.rounds):
                        for backend, side, p, port, counter in procs:
                            u0, s0 = cpu_times(p.pid)
                            i0 = counter() if counter else 0
                            out = json.loads(subprocess.run(
                                [os.path.join(HERE, "loadgen"), "-c", conns, "-d", str(args.duration), "-w", "1",
                                 "-t", "2", "-C", "1,2", "-p", pipeline, f"http://127.0.0.1:{port}/plaintext"],
                                capture_output=True, text=True, check=True).stdout)
                            u1, s1 = cpu_times(p.pid)
                            i1 = counter() if counter else 0
                            # CPU and instructions cover warm-up + measured window.
                            total = out["rps"] * (args.duration + 1) or 1
                            res.setdefault((backend, side), []).append(
                                (out["rps"], (u1 - u0 + s1 - s0) / total * 1e6, out["errors"] + out["non2xx"],
                                 (i1 - i0) / total))
                    for backend in BACKENDS:
                        b, n = res[(backend, "base")], res[(backend, "new")]
                        brps, nrps = (statistics.median(x[0] for x in r) for r in (b, n))
                        bcpu, ncpu = (statistics.median(x[1] for x in r) for r in (b, n))
                        bins, nins = (statistics.median(x[3] for x in r) for r in (b, n))
                        errs = sum(x[2] for x in b + n)
                        print(f"{style:<16}{backend:<8}{conns:>5}{pipeline:>5}{brps:>12.0f}{nrps:>11.0f}{pct(nrps, brps):>9}"
                              f"{bcpu:>9.2f}{ncpu:>8.2f}{pct(ncpu, bcpu):>9}"
                              f"{bins:>12.0f}{nins:>11.0f}{pct(nins, bins):>9}" + (f"  errors={errs}" if errs else ""),
                              flush=True)
        finally:
            for _, _, p, _, _ in procs:
                p.send_signal(signal.SIGKILL)
                p.wait()

if args.churn:
    print(f"micro_server, one request per connection, median of {args.rounds}x{args.duration}s "
          f"(server CPU µs per connection, lower is better)")
    print(f"{'backend':<8}{'base conn/s':>12}{'new conn/s':>11}{'base µs':>9}{'new µs':>8}{'change':>9}")
    procs = []
    for backend in BACKENDS:
        for side, src in SIDES:
            port = 8950 + len(procs)
            p = subprocess.Popen(
                ["taskset", "-c", "3", sys.executable, os.path.join(HERE, "micro_server.py"), "--loop", "ringloop",
                 "--port", str(port), "--read", "buffered", "--write", "one"], env=env_for(src, backend))
            wait_port(port, p)
            procs.append((backend, side, p, port))
    try:
        res = {}
        for _ in range(args.rounds):
            for backend, side, p, port in procs:
                u0, s0 = cpu_times(p.pid)
                out = json.loads(subprocess.run(
                    ["taskset", "-c", "1,2", sys.executable, os.path.join(HERE, "churn_client.py"),
                     "--port", str(port), "--seconds", str(args.duration)],
                    capture_output=True, text=True, check=True).stdout)
                u1, s1 = cpu_times(p.pid)
                n = out["connections"] or 1
                res.setdefault((backend, side), []).append((n / args.duration, (u1 - u0 + s1 - s0) / n * 1e6))
        for backend in BACKENDS:
            b, n = res[(backend, "base")], res[(backend, "new")]
            brate, nrate = (statistics.median(x[0] for x in r) for r in (b, n))
            bcpu, ncpu = (statistics.median(x[1] for x in r) for r in (b, n))
            print(f"{backend:<8}{brate:>12.0f}{nrate:>11.0f}{bcpu:>9.2f}{ncpu:>8.2f}{pct(ncpu, bcpu):>9}", flush=True)
    finally:
        for _, _, p, _ in procs:
            p.send_signal(signal.SIGKILL)
            p.wait()
