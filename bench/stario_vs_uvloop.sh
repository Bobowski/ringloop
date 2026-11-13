#!/bin/bash
# Stario on uvloop vs Stario on ringloop (io_uring and epoll), same everything else.
# Same interpreter, same Stario wheel, same app (bench/serve.py), same env, same CPU pinning,
# same load generator; runs interleaved round by round so machine drift hits every loop alike.
set -u
cd "$(dirname "$0")/.."
PY=${PY:-.venv/bin/python}
OUT=${OUT:-results/stario-$(date -u +%F)}
mkdir -p "$OUT"
LOOPS="uvloop,ringloop@RINGLOOP_BACKEND=uring,ringloop@RINGLOOP_BACKEND=epoll"
{
  echo "date: $(date -u +%FT%TZ)"; uname -r; nproc; grep -m1 "model name" /proc/cpuinfo
  $PY -c "import sys, stario, uvloop, ringloop; print(sys.version, 'gil', sys._is_gil_enabled()); print('stario', stario.__version__ if hasattr(stario,'__version__') else '?', 'uvloop', uvloop.__version__)"
  git rev-parse HEAD; git -C .deps/stario rev-parse HEAD
} > "$OUT/env.txt" 2>&1
$PY bench/run.py --loops "$LOOPS" --cases plaintext,json,params,validate,yield,sleep,page,page-baked \
  -r 5 -d 4 --json "$OUT/throughput-c128.json" > "$OUT/throughput-c128.txt" 2>&1
for c in 1 16 1024; do
  $PY bench/run.py --loops "$LOOPS" --cases plaintext,page -c $c -r 3 -d 4 \
    --json "$OUT/sweep-c$c.json" > "$OUT/sweep-c$c.txt" 2>&1
done
$PY bench/run.py --loops "$LOOPS" --cases plaintext -p 16 -r 3 -d 4 > "$OUT/pipeline16.txt" 2>&1
$PY bench/clients.py sse --loops "$LOOPS" --clients 2000 --interval 0.1 --seconds 10 --repeat 2 > "$OUT/sse-2000.txt" 2>&1
$PY bench/clients.py conns --loops "$LOOPS" --clients 10000 --seconds 15 > "$OUT/conns-10000.txt" 2>&1
$PY bench/tls.py --loops "$LOOPS" --client-cpus 0,1,2 > "$OUT/tls.txt" 2>&1
