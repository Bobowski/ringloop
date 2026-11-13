ringloop on 2026-11-12, during the work on 1.0.0.

Machine: a 4-vCPU Linux 6.17 VM (podman, aarch64) on an Apple M1. The VM has
no performance counters and its timing noise is a few percent, sometimes
more. Read the numbers as ratios between rows of one table, not as absolute
figures.

Loops: ringloop (io_uring and epoll backends), uvloop 0.23.0, rloop 0.5.0,
asyncio of CPython 3.14.10. Server: bench/micro_server.py --read buffered.
Columns are explained at the top of bench/compare.py; "µs/req" (server CPU
per request) is the one that does not depend on how fast the clients are.

single.txt          one thread, CPython 3.14 (GIL build):
                    compare.py --pipeline 1,16 --rounds 7
threads-work.txt    one loop per thread, CPython 3.14t, 300 steps of Python
                    work per request (the server is the limit):
                    compare.py --python python3.14t --threads 1,2,3 --work 300 --rounds 7
threads-micro.txt   one loop per thread, no work per request (the clients
                    are the limit from 2 threads on):
                    compare.py --python python3.14t --threads 1,2 --rounds 7
two-threads.txt     2 threads in one process ...
two-processes.txt   ... against 2 processes with one thread each, same load:
                    compare.py --python python3.14t --threads 2 [--processes]
ab-micro.txt        against e62f8ee (the commit before this branch):
                    ab.py --base ... --micro --styles buffered:one,plain:one
ab-churn.txt        the same, one request per connection: ab.py --base ... --churn

What they show:

- One thread: ringloop spends 1.67 µs of CPU per request, uvloop 2.73,
  rloop 2.49, asyncio 3.43 (1.6, 1.5 and 2.1 times more). On the epoll
  backend it is 2.03. With 16 pipelined requests per round trip the loops
  are closer: 0.17 against 0.21, 0.20 and 0.25.
- With application work (about 10 µs of Python per request) and one loop per
  thread on a free-threaded build, two threads give about twice the
  capacity of one, for every loop (1.8 to 2.1); ringloop serves 36 % more
  requests per second than uvloop on one thread and 16 % more on two. Three server threads leave the load
  generator one CPU of four, and every loop gets slower per request by the
  same amount: that row measures the machine.
- Without application work the CPU per request rises with a second thread
  (1.88 to 2.51 µs). That is not the loops disturbing each other: two
  processes with one thread each cost the same as two threads in one process
  (2.46 against 2.35 µs/req). Each loop simply gets less load than the single
  loop had, so it batches less per iteration.
- Against the commit before this branch: no change for keepalive traffic
  beyond the noise; one request per connection costs 2 to 3 % more server
  CPU on both backends (inside the noise; io_uring now arms one more request
  per connection).

Also measured (not kept as files): the smaller ring (1024/4096 entries, was
4096/32768) at 2000 and 8000 connections against the old one: 2.73 against
2.66 µs/req at 2000, 3.22 against 3.24 at 8000.

rloop 0.5.0 stops listening when Server.serve_forever() is called on a
server that create_server() already started; micro_server waits on an Event
instead so that it can be measured.

After the release audit (1.0.0b1, the same day, on a busier host): accept is
one request per connection now, so that workers share a listening socket
fairly. `ab.py --churn --rounds 31 --duration 2` against e62f8ee: +1.6 %
server CPU per connection on io_uring, +3.6 % on epoll, both inside the
noise. The keepalive ratios between the loops did not change (2.16 µs per
request against 3.43 for uvloop and 2.93 for rloop in that session). The
files above were not replaced: they are the quieter measurement.

Granian 2.8 (`loop="asyncio"`, launcher with ringloop.install()) serves on
ringloop; one worker handled 76k to 85k requests per second of a trivial
ASGI app against 61k on asyncio's loop. Granian does its I/O in Rust, so the
loop only carries the callbacks there.

counts.txt has what does not depend on the machine: system calls and
user-space instructions per request for a Stario application on each loop.
It also says what the time-based figures here do not prove.
