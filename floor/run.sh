#!/bin/bash
# floor/run.sh <name> <port> <cmd...>: start the server on CPU 3, load it, print req/s and CPU µs/request.
# Needs bench/loadgen (see bench/README.md).
here=$(cd "$(dirname "$0")" && pwd)
name=$1; port=$2; shift 2
taskset -c 3 "$@" $port & P=$!
sleep 0.4
for pl in 1 16; do
  read -r _ u0 s0 < <(awk '{print 0,$14,$15}' /proc/$P/stat)
  out=$("$here/../bench/loadgen" -c 128 -d 4 -w 1 -t 2 -C 1,2 -p $pl http://127.0.0.1:$port/plaintext)
  read -r _ u1 s1 < <(awk '{print 0,$14,$15}' /proc/$P/stat)
  python3 -c "
import json,sys; r=json.loads('$out'); tot=r['rps']*5
print(f'$name pipeline=$pl: {r[\"rps\"]:>9.0f} req/s  p99 {r[\"p99_us\"]/1000:.2f} ms  user {($u1-$u0)/100/tot*1e6:.2f} µs  sys {($s1-$s0)/100/tot*1e6:.2f} µs  err {r[\"errors\"]+r[\"non2xx\"]}')"
done
kill $P; wait $P 2>/dev/null
