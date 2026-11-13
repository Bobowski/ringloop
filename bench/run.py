"""Benchmark Stario's app across event loops with bench/loadgen.

    python bench/run.py --loops asyncio,uvloop,ringloop --cases plaintext,json,page

One server at a time, pinned to --server-cpu; loadgen pinned to --client-cpus.
Reports the median of --repeat runs: req/s, p50/p99, the server's user and
kernel CPU microseconds per request (from /proc/<pid>/stat), and its
user-space instructions per request (perf_event_open; the one column that
stays meaningful when the machine is busy with other work).
"""
import argparse
import json
import os
import signal
import socket
import statistics
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
CASES = {
    "plaintext": ["/plaintext"],
    "json": ["/json"],
    "params": ["/user/42"],
    "validate": ["-P", '{"name": "Ada", "age": 36}', "/validate"],
    "page": ["/page"],
    "page-baked": ["/page-baked"],
    "yield": ["/yield"],
    "sleep": ["/sleep"],
}
TICK = os.sysconf("SC_CLK_TCK")


def cpu_times(pid):
    with open(f"/proc/{pid}/stat") as f:
        parts = f.read().rsplit(")", 1)[1].split()
    return int(parts[11]) / TICK, int(parts[12]) / TICK  # utime, stime


def wait_port(port, proc):
    for _ in range(200):
        if proc.poll() is not None:
            raise SystemExit(f"server exited: {proc.returncode}")
        try:
            socket.create_connection(("127.0.0.1", port), 0.2).close()
            return
        except OSError:
            time.sleep(0.05)
    raise SystemExit("server did not start")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--loops", default="asyncio,uvloop")
    ap.add_argument("--cases", default="plaintext,json,params,validate,page")
    ap.add_argument("--connections", "-c", type=int, default=128)
    ap.add_argument("--duration", "-d", type=int, default=5)
    ap.add_argument("--repeat", "-r", type=int, default=3)
    ap.add_argument("--pipeline", "-p", type=int, default=1)
    ap.add_argument("--server-cpu", default="3")
    ap.add_argument("--client-cpus", default="1,2")
    ap.add_argument("--port", type=int, default=8100)
    ap.add_argument("--busy-client", action="store_true",
                    help="load generator spins instead of sleeping, so the server never pays to wake it")
    ap.add_argument("--json", default=None)
    ap.add_argument("--server", default=os.path.join(HERE, "serve.py"))
    args = ap.parse_args()
    loops = args.loops.split(",")
    cases = args.cases.split(",")
    nthreads = len(args.client_cpus.split(","))
    results = {}
    # Interleave: for each repeat round, every loop once, so drift hits all alike.
    servers = {}
    try:
        for i, loop in enumerate(loops):
            port = args.port + i
            env = dict(os.environ)
            name, _, envs = loop.partition("@")  # e.g. ringloop@RINGLOOP_URING=1
            for kv in filter(None, envs.split(";")):
                k, _, v = kv.partition("=")
                env[k] = v
            proc = subprocess.Popen(
                ["taskset", "-c", args.server_cpu, sys.executable, args.server,
                 "--loop", name, "--port", str(port)],
                env=env, stdout=subprocess.DEVNULL, stderr=sys.stderr,
            )
            wait_port(port, proc)
            try:
                from perfcount import instruction_counter

                counter = instruction_counter(proc.pid)
            except (ImportError, OSError):
                counter = None
            servers[loop] = (proc, port, counter)
        for case in cases:
            for _round in range(args.repeat):
                for loop in loops:
                    proc, port, counter = servers[loop]
                    u0, s0 = cpu_times(proc.pid)
                    i0 = counter() if counter else 0
                    out = subprocess.run(
                        [os.path.join(HERE, "loadgen"), "-c", str(args.connections),
                         "-d", str(args.duration), "-t", str(nthreads), "-w", "1",
                         "-C", args.client_cpus, "-p", str(args.pipeline), *(["-b"] if args.busy_client else []),
                         *CASES[case][:-1], f"http://127.0.0.1:{port}{CASES[case][-1]}"],
                        capture_output=True, text=True, check=True,
                    ).stdout
                    u1, s1 = cpu_times(proc.pid)
                    i1 = counter() if counter else 0
                    r = json.loads(out)
                    # CPU covers warm-up + measured window; scale by total requests ≈ rps*(d+1)
                    total = r["rps"] * (args.duration + 1) or 1
                    r["user_us"] = (u1 - u0) / total * 1e6
                    r["sys_us"] = (s1 - s0) / total * 1e6
                    r["instructions"] = (i1 - i0) / total
                    results.setdefault((case, loop), []).append(r)
    finally:
        for proc, _, _ in servers.values():
            proc.send_signal(signal.SIGTERM)
        for proc, _, _ in servers.values():
            try:
                proc.wait(5)
            except subprocess.TimeoutExpired:
                proc.kill()

    def med(rs, k):
        return statistics.median(r[k] for r in rs)

    print(f"\nc={args.connections} pipeline={args.pipeline} {args.repeat}x{args.duration}s, "
          f"server cpu {args.server_cpu}, client cpus {args.client_cpus}")
    print(f"{'case':<11}{'loop':<34}{'req/s':>9}{'vs first':>9}{'p50 ms':>8}{'p99 ms':>8}"
          f"{'user µs':>9}{'sys µs':>8}{'instr':>9}{'err':>5}")
    dump = []
    for case in cases:
        base = med(results[(case, loops[0])], "rps")
        for loop in loops:
            rs = results[(case, loop)]
            rps = med(rs, "rps")
            errs = sum(r["errors"] + r["non2xx"] for r in rs)
            print(f"{case:<11}{loop:<34}{rps:>9.0f}{rps / (base or 1):>8.2f}x{med(rs, 'p50_us') / 1000:>8.2f}"
                  f"{med(rs, 'p99_us') / 1000:>8.2f}{med(rs, 'user_us'):>9.2f}{med(rs, 'sys_us'):>8.2f}"
                  f"{med(rs, 'instructions'):>9.0f}{errs:>5}")
            dump.append({"case": case, "loop": loop, "runs": rs})
    if args.json:
        with open(args.json, "w") as f:
            json.dump(dump, f, indent=1)


if __name__ == "__main__":
    main()
