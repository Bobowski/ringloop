"""SSE fan-out and connected-users runs, using Stario's own benchmark clients.

    python bench/clients.py sse   --loops uvloop,ringloop --clients 2000 --interval 0.1
    python bench/clients.py conns --loops uvloop,ringloop --clients 10000

sse:   N clients hold GET /sse; the server broadcasts every --interval seconds.
conns: N keep-alive clients each GET /page every 2 s.
Reports the client's latency numbers plus the server's CPU share, CPU
microseconds per delivered event (sse), and peak RSS.
"""
import argparse
import json
import os
import signal
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from run import TICK, cpu_times, wait_port  # noqa: E402

STARIO = os.environ.get("STARIO_CHECKOUT") or os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".deps", "stario")
CLIENTS = os.path.join(STARIO, "benchmarks", "server")


def rss_peak_mb(pid):
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if line.startswith("VmHWM:"):
                return int(line.split()[1]) / 1024
    return 0.0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["sse", "conns"])
    ap.add_argument("--loops", default="uvloop,ringloop")
    ap.add_argument("--clients", type=int, default=2000)
    ap.add_argument("--seconds", type=int, default=10)
    ap.add_argument("--interval", type=float, default=0.5)
    ap.add_argument("--repeat", type=int, default=1)
    ap.add_argument("--server-cpu", default="3")
    ap.add_argument("--client-cpus", default="1,2")
    ap.add_argument("--port", type=int, default=8500)
    args = ap.parse_args()
    rows = []
    for _ in range(args.repeat):
        for i, loop in enumerate(args.loops.split(",")):
            port = args.port + i
            env = dict(os.environ, BENCH_SSE_INTERVAL=str(args.interval))
            name, _, envs = loop.partition("@")
            for kv in filter(None, envs.split(";")):
                k, _, v = kv.partition("=")
                env[k] = v
            proc = subprocess.Popen(
                ["taskset", "-c", args.server_cpu, sys.executable, os.path.join(HERE, "serve.py"),
                 "--loop", name, "--port", str(port)],
                env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                preexec_fn=lambda: __import__("resource").setrlimit(
                    __import__("resource").RLIMIT_NOFILE,
                    (__import__("resource").getrlimit(__import__("resource").RLIMIT_NOFILE)[1],) * 2),
            )
            try:
                wait_port(port, proc)
                if args.mode == "sse":
                    cmd = ["sse_client.py", f"http://127.0.0.1:{port}/sse", str(args.clients), str(args.seconds)]
                else:
                    cmd = ["conn_client.py", "127.0.0.1", str(port), str(args.clients), str(args.seconds)]
                u0, s0 = cpu_times(proc.pid)
                t0 = time.monotonic()
                out = subprocess.run(
                    ["taskset", "-c", args.client_cpus, sys.executable, *cmd],
                    cwd=CLIENTS, capture_output=True, text=True, timeout=args.seconds + 180,
                )
                wall = time.monotonic() - t0
                u1, s1 = cpu_times(proc.pid)
                line = [x for x in out.stdout.splitlines() if x.startswith("{")]
                if not line:
                    print(loop, "client failed:", out.stderr[-400:])
                    continue
                r = json.loads(line[-1])
                r["loop"] = loop
                r["cpu"] = (u1 - u0 + s1 - s0) / wall
                r["user_s"], r["sys_s"] = u1 - u0, s1 - s0
                r["rss_peak_mb"] = rss_peak_mb(proc.pid)
                rows.append(r)
                print(json.dumps(r), flush=True)
            finally:
                proc.send_signal(signal.SIGTERM)
                try:
                    proc.wait(10)
                except subprocess.TimeoutExpired:
                    proc.kill()
            time.sleep(1)


if __name__ == "__main__":
    main()
