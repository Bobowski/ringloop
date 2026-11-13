# Benchmarks

Linux only. Every tool pins the server and the load generator to different
CPUs (`taskset`), runs the candidates in interleaved rounds, and prints the
server's CPU time per request next to the requests per second. Read the CPU
column: it does not depend on how fast the clients are.

Build the load generator first:

```sh
cc -O2 -pthread -o bench/loadgen bench/loadgen.c
```

| Tool | Measures | Needs |
| --- | --- | --- |
| `compare.py` | Event loops side by side under a bare protocol (`micro_server.py`): one thread, one loop per thread (`--threads`), one process per loop (`--processes`) | the loops to compare (uvloop, rloop) |
| `ab.py` | Two checkouts of this repository against each other: `--micro` (keep-alive), `--churn` (one request per connection), `--inproc` (scheduling, timers, TLS without a network peer) | another checkout, built in place |
| `servers.py` | One ASGI application (`asgi_app.py`) under uvicorn and Granian on each loop | uvicorn, httptools, granian |
| `run.py` | Stario's benchmark application on each loop (`serve.py`) | a checkout of [Stario](https://github.com/stariodev/stario) in `STARIO_CHECKOUT` |
| `clients.py` | The same application with 2,000 SSE clients, or 10,000 idle keep-alive clients | the same |
| `tls.py` | The same application over TLS (`tls_client.py`) | the same, and `openssl` |
| `stario_vs_uvloop.sh` | `run.py`, `clients.py` and `tls.py` in one go: Stario on uvloop vs ringloop (io_uring and epoll); results in `../results/stario-<date>/` | the same, and uvloop |
| `remote_client.py` | Stario's application loaded from another machine (`serve.py --host 0.0.0.0` on the server) | `loadgen` built on the client machine |

`inproc.py --instructions` and the `instructions` columns count user-space
instructions with `perf_event_open` (`perfcount.py`). The count stays the
same on a busy machine, so use it for a change of a few percent. A virtual
machine usually has no such counter.

`../floor/` holds two C servers that do the least a server can do per
request, one on epoll and one on io_uring. They show how much of a
measurement is the kernel and the load generator:

```sh
cc -O2 -o floor/floor_epoll floor/floor_epoll.c
cc -O2 -o floor/floor_uring floor/floor_uring.c
floor/run.sh epoll 9001 floor/floor_epoll
floor/run.sh uring 9002 floor/floor_uring
```

`../results/` holds the output of these tools for a release, with the
machine and the command lines in its `README.txt`.
