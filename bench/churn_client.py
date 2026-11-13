"""One request per connection: connect, send, read the response, close.

    python bench/churn_client.py --port 8000 --seconds 3 --threads 2

Prints the number of connections served as JSON. The client is plain
blocking Python, so it (not the server) limits the rate: read the server's
CPU time per connection, not connections per second.
"""
import argparse
import json
import socket
import threading
import time

ap = argparse.ArgumentParser()
ap.add_argument("--port", type=int, required=True)
ap.add_argument("--seconds", type=float, default=3)
ap.add_argument("--threads", type=int, default=2)
ap.add_argument("--reset", action="store_true", help="close with a reset instead of an orderly close")
args = ap.parse_args()

REQ = b"GET /plaintext HTTP/1.1\r\nhost: x\r\n\r\n"
counts = [0] * args.threads
deadline = time.monotonic() + args.seconds


def work(i):
    import struct
    linger = struct.pack("ii", 1, 0)
    n = 0
    while time.monotonic() < deadline:
        s = socket.create_connection(("127.0.0.1", args.port))
        s.sendall(REQ)
        got = 0
        while got < 130:
            chunk = s.recv(4096)
            if not chunk:
                break
            got += len(chunk)
        if args.reset:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, linger)
        s.close()
        n += 1
    counts[i] = n


threads = [threading.Thread(target=work, args=(i,)) for i in range(args.threads)]
for t in threads:
    t.start()
for t in threads:
    t.join()
print(json.dumps({"connections": sum(counts)}))
