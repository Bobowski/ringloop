# Stario on uvloop vs Stario on ringloop — 2026-11-12

> These runs measured a build from before 1.0.0 (`e62f8ee`), by wall-clock
> time on a noisy virtual machine without performance counters. Read the
> ratios as what that setup showed, not as a general speed claim. The
> section "Why the gap" below explains how the setup inflates
> them. For counted (not timed) figures on 1.0.0, see
> `../2026-11-12/counts.txt`.

Reproduce: `OUT=results/stario-2026-11-12 bench/stario_vs_uvloop.sh` (needs `.venv` with Python 3.14, the
`stario==5.8.3` wheel, `uvloop`, `orjson`, ringloop built in place, a Stario
checkout in `.deps/stario` for the benchmark app, and `bench/loadgen` built).

## What is held equal

| | |
| --- | --- |
| Interpreter | one venv, CPython 3.14.6 (GIL build), for every server and client |
| Framework | the same installed `stario==5.8.3` wheel (Stario checkout `b1f4e9b` supplies only the benchmark app, `benchmarks/server/apps/stario_app.py`) |
| App | the same `bench/serve.py`, `STARIO_TRACER=noop`, `STARIO_THREADS=1`, orjson codec, default GC |
| Runner | `uvloop.run(...)` vs `ringloop.run(...)`: both are `asyncio.Runner(loop_factory=...)`, debug off, no task-factory changes |
| Stario code paths | Stario checks the loop type only to label it in telemetry (`loop_implementation`); there is no loop-specific branch in its Python or Cython code. It creates tasks with `asyncio.Task(..., eager_start=...)` on every loop |
| Output | byte-identical responses (headers + body, minus `Date`) on every endpoint from all three servers (checked with curl) |
| Machine | server pinned to CPU 3, load generator to CPUs 1–2, CPU 0 left free; all servers started up front and run **interleaved round by round** so drift hits each loop alike; median of N rounds |
| Load | the same `bench/loadgen` (C, epoll, keep-alive), the same clients for SSE, many-connection and TLS runs |

Versions: uvloop 0.23.0 (PyPI wheel), ringloop `e62f8ee` (built `-O2`), the main branch
before the 1.0.0 work from the `maturity` branch was merged.
Machine: 4 vCPU Intel Xeon @ 2.30GHz, Firecracker VM, Linux 6.18.44. The VM has
no hardware perf counters, so the `instr` column in the raw files is 0; and it
is noisy (see the min–max ranges below).

ringloop is shown on both backends. **epoll vs epoll is the strictest
apples-to-apples comparison** (libuv also uses epoll); io_uring is ringloop's
default where the kernel allows it.

## Results

### Throughput, c=128, 5 × 4 s (req/s, median)

| case | uvloop | ringloop io_uring | ringloop epoll | io_uring / uvloop | epoll / uvloop |
| --- | ---: | ---: | ---: | ---: | ---: |
| plaintext | 80,210 | 113,099 | 149,643 | 1.41x | 1.87x |
| json | 75,360 | 118,161 | 147,568 | 1.57x | 1.96x |
| params | 59,625 | 113,173 | 149,910 | 1.90x | 2.51x |
| validate (POST) | 51,488 | 113,631 | 133,316 | 2.21x | 2.59x |
| yield (`sleep(0)`) | 94,488 | 101,919 | 114,188 | 1.08x | 1.21x |
| sleep (1 ms timer) | 52,080 | 92,681 | 84,488 | 1.78x | 1.62x |
| page (HTML render) | 8,781 | 10,263 | 10,376 | 1.17x | 1.18x |
| page-baked | 13,723 | 18,453 | 17,745 | 1.34x | 1.29x |

The server was CPU-bound in every run (CPU µs/req × req/s ≈ 1 core), so the
load generator was not the limit. Range across rounds (min–max req/s):

| case | uvloop | io_uring | epoll |
| --- | --- | --- | --- |
| plaintext | 76k–89k | 99k–134k | 121k–160k |
| validate | 46k–57k | 110k–119k | 110k–138k |
| yield | 81k–109k | 92k–116k | 94k–126k |
| page | 8.5k–8.8k | 9.9k–10.4k | 9.6k–10.8k |

Every case has no overlap between rounds except `yield`, where the ranges
overlap and the gap is within noise.

Server CPU per request, plaintext: uvloop 3.97 µs user + 8.05 µs sys;
ringloop io_uring 1.36 + 7.29; epoll 1.53 + 5.16. On this VM the kernel time
dominates, which caps what any loop can gain on small responses.

### Connection sweep (req/s, median of 3 × 4 s)

| case | c | uvloop | io_uring | epoll | p99 ms uvloop / uring / epoll |
| --- | ---: | ---: | ---: | ---: | --- |
| plaintext | 1 | 26,206 | 26,535 (1.01x) | 27,032 (1.03x) | 0.05 / 0.05 / 0.05 |
| plaintext | 16 | 69,656 | 108,328 (1.56x) | 112,505 (1.62x) | 0.47 / 0.25 / 0.32 |
| plaintext | 1024 | 68,669 | 139,744 (2.04x) | 168,114 (2.45x) | 23.6 / 12.3 / 9.2 |
| page | 1 | 7,898 | 7,375 (**0.93x**) | 6,855 (**0.87x**) | 0.20 / 0.20 / 0.25 |
| page | 16 | 8,819 | 10,459 (1.19x) | 10,280 (1.17x) | 3.3 / 3.1 / 3.5 |
| page | 1024 | 8,609 | 10,220 (1.19x) | 9,984 (1.16x) | 127 / 147 / 139 |

`page` at c=1 is the one place uvloop wins, and it wins every round
(uvloop 7.8k–8.2k vs io_uring 7.3k–7.4k, epoll 6.8k–7.3k).

### Pipelining, c=128, 16 requests in flight per connection

| | uvloop | io_uring | epoll |
| --- | ---: | ---: | ---: |
| req/s | 89,958 | 819,931 (9.1x) | 803,658 (8.9x) |
| server sys µs/req | 7.03 | 0.34 | 0.28 |

This is a real difference in how the loops write, not a harness artifact.
strace on the servers: uvloop makes one `writev` per response (26,028 writev
for 1,738 reads), while ringloop gathers a turn's writes into one `sendto` per
batch of requests (15,538 sendto for 15,666 recvfrom). Stario calls
`writelines` once per response on both loops.

### SSE fan-out: 2,000 clients, broadcast every 100 ms, 10 s, 2 runs

| | uvloop | io_uring | epoll |
| --- | --- | --- | --- |
| broadcasts done in 10 s (target ≈100) | 76, 77 | 97, 97 | 90, 89 |
| server CPU µs per delivered event | 29.9, 27.3 | 17.1, 16.0 | 18.6, 18.0 |
| p99 delivery ms | 44.5, 49.0 | 33.0, 30.5 | 32.8, 32.3 |
| peak RSS MB | 67, 65 | 68, 68 | 61, 72 |

The ticker sleeps 100 ms after each broadcast, so a slower broadcast means
fewer of them. uvloop did fewer broadcasts and still spent more CPU on each event.

### 10,000 keep-alive connections, each GET /page every 2 s (1 run, 15 s)

| | uvloop | io_uring | epoll |
| --- | ---: | ---: | ---: |
| req/s | 4,141 | 4,346 | 4,355 |
| p99 ms | 29.1 | 11.0 | 23.9 |
| server CPU share | 0.59 | 0.52 | 0.52 |
| peak RSS MB | 434 | 428 | 429 |

There was a single run, so treat the p99 numbers as indicative only.

### TLS, 64 keep-alive connections, 3 × 8 s (client CPUs 0–2)

| | uvloop | io_uring | epoll |
| --- | ---: | ---: | ---: |
| req/s | 39,785 | 116,777 | 115,848 |
| server CPU µs/req | 15.11 | 8.43 | 8.52 |

The CPU per request is the cleaner number here (1.8x). uvloop's server was
not CPU-saturated in this run (15.11 µs × 39.8k ≈ 0.60 core), while
ringloop's was (≈ 0.98 core). So uvloop's 2.9x lower req/s comes partly from
latency in its TLS path, not CPU alone. The client was the same for all three.

## Why the gap: when the loop writes, not tasks

Tasks are not the difference. Both loops use the stock C `asyncio.Task`.
Stario creates its tasks itself (`asyncio.Task(coro, loop=..., eager_start=True)`),
so on both loops a `/plaintext` handler runs to completion inside the read
callback.

The difference is **when the response reaches the socket**:

- **uvloop** `writelines()` → `uv_try_write` → `writev` *right away*, still
  inside the read callback. The send happens in the middle of Python work.
- **ringloop** `writelines()` copies the pieces into a pooled buffer and
  marks the transport dirty. After the turn's reads, every dirty transport
  is flushed back to back (epoll: one `send` each; io_uring: all sends plus
  the next wait in one `io_uring_enter`).

On epoll, the syscalls per request are the same: one read, one write, and one
wait per ~100 requests (strace). What differs is the cost of each send. On
loopback, a send runs the receiver's TCP path and wakes the client thread in
the sender's context. Spread-out sends wake a sleeping client thread nearly
every time, which costs an IPI, a VM exit on this VM. Back-to-back sends wake
it once.

The evidence, from the same Stario app with a load generator that spins
instead of sleeping (`--busy-client`, 3 × 3 s):

| plaintext req/s | uvloop | ringloop epoll | ringloop io_uring |
| --- | ---: | ---: | ---: |
| sleeping client (normal) | 81,698 | 145,747 (1.78x) | 113,829 (1.39x) |
| spinning client | 144,649 | 150,060 (1.04x) | 156,675 (1.08x) |

- Once the client never needs waking, uvloop nearly doubles and the gap
  closes to 4–8%.
- uvloop's own `/yield` endpoint (91.7k) beats its `/plaintext` (81.7k)
  despite an extra task step. `sleep(0)` happens to move the write into the
  ready queue, after all the reads, which batches the sends.
- In the loop-only micro server (no Stario, Stario's 8-piece `writelines`),
  deferring uvloop's writes to one `call_soon` after the reads
  (`bench/micro_server.py --defer`) raises it from 116.6k to 134.2k req/s.
  ringloop epoll does 156.6k with or without `--defer`.

The rest is ordinary per-call overhead. In the micro server uvloop spends
1.87 µs user per request against ringloop's 0.85. Per read, uvloop calls
`get_buffer` / `buffer_updated` through Python attribute lookups, and per
`writelines` it exports 8 buffers, allocates a `_StreamWriteContext` and does
an 8-iovec `writev`. ringloop copies the 8 pieces into one buffer and sends
it contiguously.

Pipelining (9x) is the same mechanism at its extreme: 16 requests in one read
become 16 `writev` calls on uvloop and one `send` on ringloop.

What this means for the numbers above: the c≥16 small-response gap is real,
but this setup amplifies it, with a local client that sleeps and wakes on
every response, on a VM where wake-ups are expensive. Against clients that
don't need waking, or across a real network (not measured here), the gap is likely closer to
the busy-client row plus ringloop's lower per-call overhead (single-digit to
tens of percent) than 1.8x.

## Takeaways

- With everything else identical, ringloop serves Stario 1.4–2.6x faster than
  uvloop on small and medium responses at 16+ connections, and ~1.2–1.3x on
  HTML pages. Tail latency is equal or lower, except `page` at c=1024, where
  p99 is 127 ms on uvloop vs 139–147 ms on ringloop.
- At c=1 the loops are even on plaintext, and uvloop is 7–13% faster on the
  CPU-heavy `page`. That is worth looking into.
- On this VM, ringloop's **epoll** backend beats its io_uring backend on most
  small-response cases (plaintext 150k vs 113k). io_uring wins only on `sleep`.
  This is the reverse of the earlier results in `results/` from another machine,
  so the backend default is machine-dependent.
- Pipelining (9x) and TLS (1.8x CPU, 2.9x req/s) show the largest gaps. Both
  come from how the loop writes to the socket, not from Stario.

## Where ringloop's time goes, and what is left to win

`perf record -e cpu-clock -g` on the server, Stario `/plaintext`, c=128
(the VM has no hardware PMU, so this samples on a timer):

| share of server CPU | uvloop | ringloop epoll | ringloop io_uring |
| --- | ---: | ---: | ---: |
| kernel | 62% | 71% | 78% |
| CPython interpreter | 14% | 10.5% | 8% |
| Stario (`protocol` + `exchange` .so) | 12% | 11% | 9% |
| the loop's own code | 7% (`uvloop/loop.so`) | 4.7% (`_core.so`) | 3.3% |
| libc | 4.5% | 2.9% | 0.9% |

- **The loop is close to its floor for this workload.** Even if ringloop's
  own code cost nothing, the server would gain under 5%.
- **The kernel time is mostly the TCP send path.** Waking the load generator
  (`sock_def_readable` → wake-up, inside the send) alone is 15% of server CPU
  on epoll and 26% on io_uring.

Syscalls and client wake-ups per response (`perf stat`, 4 s at full load):

| | uvloop | ringloop epoll | ringloop io_uring |
| --- | ---: | ---: | ---: |
| server syscalls / request | 2.14 | 2.05 | 0.03 |
| client context switches / request | 0.71 | 0.29 | 0.54 |

io_uring makes ~70x fewer syscalls, yet loses to epoll here because the
client gets woken almost twice as often. With a spinning client (no
wake-ups) io_uring is the fastest backend (see above). Wake-ups are a
property of a local, sleeping load generator; with remote clients that cost
lands on another machine.

Cheap knobs tried (5 × 3 s, ringloop epoll):

| | plaintext | validate | page |
| --- | ---: | ---: | ---: |
| `PYTHON_JIT=1` | +6% | +7% | -4% |
| `gc.disable()` | +2% | +7% | -6% |

All within this VM's run-to-run noise (±7%). No clear win.

Tasks: Stario already starts handler tasks with `eager_start=True` and skips
the done-callbacks when the handler finishes during that first step. Skipping
the Task entirely (driving the coroutine with `send(None)` and making a Task
only if it suspends) would save ~170 ns per request in-process
(`Task(eager_start=True)` 320 ns vs `coro.send(None)` 154 ns). That is about
2.5% of a 6.7 µs request, and it costs `current_task()`, per-task context
and cancellation for handlers that never suspend.
