"""Load Stario servers on another machine, so the client's wake-ups and TCP
receive work stay off the server's CPU (unlike loopback).

On the server machine (one server per loop, all pinned to the same core):

    for spec in "8100 uvloop" "8101 ringloop uring" "8102 ringloop epoll"; do
      set -- $spec
      RINGLOOP_BACKEND=${3:-uring} taskset -c 3 .venv/bin/python bench/serve.py \\
        --loop $2 --port $1 --host 0.0.0.0 &
    done

On the client machine (build bench/loadgen there; IPv4 address only):

    python bench/remote_client.py --host 10.0.0.5 \\
      --targets uvloop:8100,ringloop-uring:8101,ringloop-epoll:8102

Rounds are interleaved (every server once per round) and medians reported.
Pass --threads/--cpus to match the client machine; the client must not be the
bottleneck: check that the server core sits at ~100% during a run.
"""
import argparse
import json
import os
import statistics
import subprocess

HERE = os.path.dirname(os.path.abspath(__file__))
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
ap = argparse.ArgumentParser()
ap.add_argument("--host", required=True)
ap.add_argument("--targets", required=True, help="name:port,name:port,... (first is the baseline)")
ap.add_argument("--cases", default="plaintext,json,validate,yield,sleep,page")
ap.add_argument("--connections", "-c", default="128", help="comma-separated")
ap.add_argument("--pipeline", "-p", type=int, default=1)
ap.add_argument("--repeat", "-r", type=int, default=5)
ap.add_argument("--duration", "-d", type=int, default=5)
ap.add_argument("--threads", "-t", type=int, default=os.cpu_count())
ap.add_argument("--cpus", default=None, help="pin loadgen threads, e.g. 0,1,2,3")
args = ap.parse_args()
targets = [t.rsplit(":", 1) for t in args.targets.split(",")]

for conns in args.connections.split(","):
    res = {}
    for case in args.cases.split(","):
        for _ in range(args.repeat):
            for name, port in targets:
                cmd = [os.path.join(HERE, "loadgen"), "-c", conns, "-d", str(args.duration), "-w", "1",
                       "-t", str(args.threads), "-p", str(args.pipeline), *(["-C", args.cpus] if args.cpus else []),
                       *CASES[case][:-1], f"http://{args.host}:{port}{CASES[case][-1]}"]
                res.setdefault((case, name), []).append(
                    json.loads(subprocess.run(cmd, capture_output=True, text=True, check=True).stdout))
    print(f"\nremote {args.host}  c={conns} pipeline={args.pipeline} {args.repeat}x{args.duration}s, "
          f"loadgen threads {args.threads}")
    print(f"{'case':<11}{'loop':<20}{'req/s':>9}{'min':>9}{'max':>9}{'vs first':>9}{'p50 ms':>8}{'p99 ms':>8}{'err':>5}")
    for case in args.cases.split(","):
        base = statistics.median(r["rps"] for r in res[(case, targets[0][0])]) or 1
        for name, _ in targets:
            rs = res[(case, name)]
            rps = [r["rps"] for r in rs]
            med = statistics.median(rps)
            errs = sum(r["errors"] + r["non2xx"] for r in rs)
            print(f"{case:<11}{name:<20}{med:>9.0f}{min(rps):>9.0f}{max(rps):>9.0f}{med / base:>8.2f}x"
                  f"{statistics.median(r['p50_us'] for r in rs) / 1000:>8.2f}"
                  f"{statistics.median(r['p99_us'] for r in rs) / 1000:>8.2f}{errs:>5}", flush=True)
