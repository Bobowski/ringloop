"""Direct-TLS run: server CPU per HTTPS request, per loop.

    python bench/tls.py --loops uvloop,ringloop
"""
import argparse
import json
import os
import signal
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from run import cpu_times  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--loops", default="uvloop,ringloop")
ap.add_argument("--connections", type=int, default=64)
ap.add_argument("--seconds", type=int, default=8)
ap.add_argument("--repeat", type=int, default=3)
ap.add_argument("--server-cpu", default="3")
ap.add_argument("--client-cpus", default="1,2,5,6")
ap.add_argument("--port", type=int, default=8600)
args = ap.parse_args()

tmp = tempfile.mkdtemp()
cert, key = os.path.join(tmp, "cert.pem"), os.path.join(tmp, "key.pem")
subprocess.run(
    ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2", "-subj", "/CN=localhost",
     "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1", "-keyout", key, "-out", cert],
    check=True, capture_output=True)

import statistics  # noqa: E402
import time  # noqa: E402

rows = {}
for _ in range(args.repeat):
    for i, loop in enumerate(args.loops.split(",")):
        name, _, envs = loop.partition("@")
        env = dict(os.environ, STARIO_SSL_CERTFILE=cert, STARIO_SSL_KEYFILE=key)
        for kv in filter(None, envs.split(";")):
            k, _, v = kv.partition("=")
            env[k] = v
        port = args.port + i
        proc = subprocess.Popen(["taskset", "-c", args.server_cpu, sys.executable, os.path.join(HERE, "serve.py"),
                                 "--loop", name, "--port", str(port)], env=env,
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            time.sleep(1.5)
            u0, s0 = cpu_times(proc.pid)
            out = subprocess.run(["taskset", "-c", args.client_cpus, sys.executable, os.path.join(HERE, "tls_client.py"),
                                  str(port), str(args.connections), str(args.seconds), cert],
                                 capture_output=True, text=True)
            u1, s1 = cpu_times(proc.pid)
            if not out.stdout.strip():
                print(loop, "client failed", out.stderr[-300:])
                continue
            r = json.loads(out.stdout)
            r["cpu_us"] = (u1 - u0 + s1 - s0) / r["requests"] * 1e6
            rows.setdefault(loop, []).append(r)
        finally:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(10)
            except subprocess.TimeoutExpired:
                proc.kill()
print(f"{'loop':<34}{'req/s':>9}{'server CPU µs/req':>20}")
for loop, rs in rows.items():
    print(f"{loop:<34}{statistics.median(r['rps'] for r in rs):>9.0f}{statistics.median(r['cpu_us'] for r in rs):>20.2f}")
