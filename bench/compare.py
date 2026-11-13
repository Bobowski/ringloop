"""Event loops side by side, on one thread and on several.

    python bench/compare.py [--loops ringloop,ringloop@epoll,uvloop,rloop,asyncio]
                            [--threads 1,2,3] [--work 0,300] [--pipeline 1,16]
                            [--python /path/to/python3.14t] [--rounds 5] [--duration 3]

For every combination, bench/micro_server.py runs with `--threads N` (one
loop per thread, SO_REUSEPORT) pinned to the first N CPUs, and bench/loadgen
loads it from the remaining CPUs. Rounds are interleaved between the loops,
so machine drift hits all of them alike; the table shows medians.

  req/s      what the clients got. When the clients have fewer CPUs than they
             need (many server threads, cheap requests), this is the clients'
             limit, not the server's: read the next column.
  µs/req     server CPU time (user + system, all threads) per request.
  cores      server CPUs busy during the run (N means the server was the limit).
  capacity   threads / (µs/req): the requests per second N busy cores give.
  scale      capacity relative to the same loop on one thread.

More than one thread needs a free-threaded Python (`--python`).

`--processes` runs N one-thread server processes on the port in place of N
threads in one process. With the same load, the same µs/req in both modes
means the loops of one process do not get in each other's way.
"""
import argparse
import json
import os
import signal
import statistics
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
from run import cpu_times, wait_port  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--loops", default="ringloop,ringloop@epoll,uvloop,rloop,asyncio")
ap.add_argument("--threads", default="1")
ap.add_argument("--work", default="0")
ap.add_argument("--pipeline", default="1")
ap.add_argument("--read", default="buffered")
ap.add_argument("--python", default=sys.executable)
ap.add_argument("--rounds", type=int, default=5)
ap.add_argument("--duration", type=int, default=3)
ap.add_argument("--connections", type=int, default=128, help="per server thread")
ap.add_argument("--cpus", type=int, default=os.cpu_count())
ap.add_argument("--loadgen", default=os.path.join(HERE, "loadgen"))
ap.add_argument("--processes", action="store_true", help="N processes with one thread each, not N threads")
args = ap.parse_args()
LOOPS = args.loops.split(",")

print(f"{args.python} | micro_server --read {args.read} | median of {args.rounds}x{args.duration}s | {args.cpus} CPUs"
      + (" | one process per 'thread'" if args.processes else ""))
print(f"{'work':>5}{'pipe':>5}{'thr':>4}  {'loop':<18}{'req/s':>10}{'µs/req':>8}{'cores':>7}{'capacity':>10}{'scale':>7}  errors")
base_capacity = {}
for work in args.work.split(","):
    for pipeline in args.pipeline.split(","):
        for threads in (int(t) for t in args.threads.split(",")):
            server_cpus = ",".join(str(c) for c in range(threads))
            client_cpus = [str(c) for c in range(threads, args.cpus)]
            if not client_cpus:
                raise SystemExit(f"{threads} server threads leave no CPU for the clients")
            procs = []
            try:
                for i, name in enumerate(LOOPS):
                    loop, _, backend = name.partition("@")
                    env = dict(os.environ, PYTHONPATH=os.path.join(ROOT, "src"))
                    if backend:
                        env["RINGLOOP_BACKEND"] = backend
                    port = 8600 + i
                    group = []
                    for _ in range(threads if args.processes else 1):
                        group.append(subprocess.Popen(
                            ["taskset", "-c", server_cpus, args.python, os.path.join(HERE, "micro_server.py"),
                             "--loop", loop, "--port", str(port), "--read", args.read,
                             "--threads", "1" if args.processes else str(threads), "--work", work], env=env))
                    procs.append((name, group, port))
                for name, group, port in procs:
                    wait_port(port, group[0])
                time.sleep(0.5 if args.processes else 0)  # every process of a group is listening
                res = {name: [] for name in LOOPS}
                for _ in range(args.rounds):
                    for name, group, port in procs:
                        before = sum(sum(cpu_times(p.pid)) for p in group)
                        out = json.loads(subprocess.run(
                            [args.loadgen, "-c", str(args.connections * threads), "-d", str(args.duration), "-w", "1",
                             "-t", str(len(client_cpus)), "-C", ",".join(client_cpus), "-p", pipeline,
                             f"http://127.0.0.1:{port}/plaintext"],
                            capture_output=True, text=True, check=True).stdout)
                        cpu = sum(sum(cpu_times(p.pid)) for p in group) - before  # warm-up + measured window
                        total = out["rps"] * (args.duration + 1) or 1
                        res[name].append((out["rps"], cpu / total * 1e6, cpu / (args.duration + 1),
                                          out["errors"] + out["non2xx"]))
                for name in LOOPS:
                    rps = statistics.median(r[0] for r in res[name])
                    us = statistics.median(r[1] for r in res[name])
                    cores = statistics.median(r[2] for r in res[name])
                    capacity = threads / us * 1e6
                    key = (work, pipeline, name)
                    base_capacity.setdefault(key, capacity)
                    print(f"{work:>5}{pipeline:>5}{threads:>4}  {name:<18}{rps:>10.0f}{us:>8.2f}{cores:>7.2f}"
                          f"{capacity:>10.0f}{capacity / base_capacity[key]:>6.2f}x  {sum(r[3] for r in res[name])}",
                          flush=True)
            finally:
                for _, group, _ in procs:
                    for p in group:
                        p.send_signal(signal.SIGKILL)
                        p.wait()
            print(flush=True)
