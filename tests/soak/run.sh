#!/bin/sh
# tests/soak/run.sh [seconds] [python]: the soak test on both backends, with TLS.
# Exit status 0 when the server released everything on both.
seconds=${1:-60}; py=${2:-python}
here=$(cd "$(dirname "$0")" && pwd); tmp=$(mktemp -d); rc=0
openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj /CN=localhost -keyout $tmp/k.pem -out $tmp/c.pem 2>/dev/null
port=${SOAK_PORT:-9400}
for backend in uring epoll; do
  RINGLOOP_BACKEND=$backend $py $here/server.py --port $port --tls-port $((port + 1)) --cert $tmp/c.pem --key $tmp/k.pem > $tmp/server.log 2>&1 &
  pid=$!
  for i in 1 2 3 4 5 6 7 8 9 10; do grep -q ready $tmp/server.log && break; sleep 0.3; done
  $py $here/client.py --port $port --tls-port $((port + 1)) --seconds $seconds || rc=1
  kill -0 $pid 2>/dev/null || { echo "PROBLEM: the server died"; tail -20 $tmp/server.log; rc=1; }
  kill $pid 2>/dev/null; wait $pid 2>/dev/null
  grep -v "^ready" $tmp/server.log | tail -15
  port=$((port + 2))
done
rm -rf $tmp
exit $rc
