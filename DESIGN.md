# Design

## What the loop is

`ringloop.Loop` implements `asyncio.AbstractEventLoop` on its own engine. It
does not subclass or patch asyncio's loop classes, so it is a drop-in loop
for any asyncio program:

```python
import ringloop
ringloop.run(main())                                  # like asyncio.run
asyncio.run(main(), loop_factory=ringloop.new_event_loop)
```

| File | Holds |
| --- | --- |
| `_core.pyx` (Cython) | `LoopCore`: ready queue, timer heap, `call_soon` / `call_later` / `add_reader`. `Core`: the I/O turn on io_uring or epoll. `Transport`: stream sockets. `Listener`: accept |
| `ll_uring.h` (C) | io_uring by raw syscalls: ring setup, provided-buffer ring, SQE builders, the spill list for a full submission queue. No liburing, no libuv |
| `ll_abi.h` (C) | The part of the io_uring ABI the engine uses, written out, so that the extension builds against any kernel headers (`tests/abi_check.c` compares it with the kernel's header) |
| `ll_sync.h` (C) | Atomic loads, stores and compare-and-swap for the few fields other threads touch |
| `loop.py` | `Loop`: run / stop / close, tasks, exception handler, executors, DNS, signals, `sock_*`, `create_server`, `create_connection`, Unix sockets, `start_tls`; `Server` |
| `tls.py` | `SSLTransport`: TLS on `ssl.MemoryBIO`, one object that is both the raw transport's protocol and the application's transport |
| `extras.py` | Pipes, subprocesses, datagram endpoints (Python on `add_reader` / `add_writer`); `sendfile` |
| `__init__.py` | `run`, `new_event_loop`; on systems other than Linux these hand out asyncio's loop |

### What it still touches that is not plainly public

- `asyncio._set_running_loop` / `asyncio._get_running_loop`: underscore
  names, but exported by asyncio for exactly this (every third-party loop
  uses them).
- `asyncio.subprocess.Process.wait()` calls `transport._wait()`, so
  `ProcessTransport` provides it.
- `get_extra_info("socket")` returns `asyncio.trsock.TransportSocket`, the
  same restricted view asyncio returns.
- `ringloop.pytest_plugin` (test tooling only) swaps the default policy's
  loop factory to run other projects' test suites on ringloop.
- `asyncio.staggered.staggered_race` runs the connection attempts of Happy
  Eyeballs (the module has an `__all__`, but is not in the documentation).
- `start_tls` on the server side moves bytes a `StreamReader` has already
  buffered (the client's ClientHello) into the TLS layer, through the
  reader's private `_buffer`, exactly as asyncio does (gh-142352). There is
  no public way to take them out.

A stream transport is an extension type, and an extension type cannot
inherit from `asyncio.Transport` (a Python class with its own instance
layout). Code written for asyncio checks `isinstance(transport,
asyncio.Transport)`, so the transport answers `__class__` with a subclass of
`asyncio.Transport`: `isinstance` consults `__class__`. `type(transport)` is
still the extension type. The transports written in Python inherit from the
asyncio classes directly.

## Two backends

- **io_uring** (default when the kernel allows it). One ring per loop with
  `SINGLE_ISSUER` and `DEFER_TASKRUN`, 1024 submission and 4096 completion
  entries. Linux 6.1 is the minimum because `DEFER_TASKRUN` is: an older
  kernel fails ring setup and gets epoll. Receives are one multishot `RECV` per
  connection drawing from a shared ring of 256 x 16 KiB buffers (4 MiB of
  address space, resident only as far as it is used; the count is a memory
  choice, not a kernel limit). Sends are
  `SEND` with `MSG_WAITALL` from a buffer the transport owns until the
  completion arrives. Accept is one request per connection (see "Accept is
  inline").
- **epoll** (`RINGLOOP_BACKEND=epoll`, or automatically when
  `io_uring_setup` fails). Same transports, readiness-driven: `recv` into a
  loop-wide scratch buffer, `send` at the end of the turn. Container
  runtimes often block io_uring with seccomp, so this is the backend many
  deployments get. `loop.backend` says which one is in use. Timeouts are
  exact (`epoll_pwait2`, Linux 5.11) unless the kernel or the seccomp policy
  refuses that call; then they round up to a millisecond.

A `SINGLE_ISSUER` ring belongs to the thread that created it. The ring is
created on first use; if the loop is later run on a different thread (set up
with `run_until_complete` on the main thread, then `run_forever` on a
worker), the core replaces the ring when the loop starts running and arms
its listeners and open connections again on the new one. The owner is
tracked by a per-thread number that no other thread of the process ever
gets (`pthread_self()` values are reused as soon as a thread exits, kernel
thread ids once the pid space wraps). Before the old ring is dropped,
completions still sitting in its queue are run, and with `DEFER_TASKRUN` no
others can exist, because completions only happen inside the loop's own
`io_uring_enter`. The move is refused, with an error, in two cases, and the
loop keeps its ring and its thread: a send is still in flight on the old
thread (how much of it the kernel has done is known only to the old ring),
or the new ring cannot be created (out of memory or descriptors; it is
created before the old one is dropped).

The backend is chosen, and the ring created, when the loop opens its first
socket (or is asked for `loop.backend`). Until then the loop waits in
`epoll_wait`: callbacks, timers, `add_reader` / `add_writer` and the wake-up
eventfd need nothing else. A program that only runs tasks, timers or
subprocesses never creates a ring, and a test suite that makes a loop per
test does not create and destroy a ring (and its 4 MiB of receive buffers)
per test.

Where ring memory is charged depends on the kernel version. Linux 6.1 to
6.13 charge it to the memory cgroup; some later kernels (seen on 6.17) count
it against `RLIMIT_MEMLOCK` (8 MiB by default) unless the process has
`CAP_IPC_LOCK`, and give it back some time after the ring is closed. That is
why the ring is small: about 140 KiB, so some 50 loops fit in the default
limit (one loop per thread on a large machine). The
sizes do not limit a turn (see "The submission queue cannot overflow"; the
kernel keeps completions that do not fit and hands them over on the next
call). When `io_uring_setup` fails with `ENOMEM` the loop logs one warning
and uses epoll.

`add_reader` / `add_writer` registrations and the wake-up eventfd live in an
epoll set the core owns, on both backends. Under io_uring that epoll fd is
watched with a one-shot `POLL_ADD`, re-armed each turn, which keeps those
registrations level-triggered as asyncio code expects.

## Threads

A loop belongs to one thread at a time: the thread running it, or, while no
thread runs it, the thread calling into it. That thread is the loop's
*owner* (`Core.owner`, claimed with an atomic compare-and-swap).

- **Scheduling works from every thread.** `call_soon`,
  `call_soon_threadsafe`, `call_later`, `call_at` and `stop` are all safe
  from any thread (the first two are the same function). The running thread
  appends to its own queue directly. Any other thread posts the handle to a
  small queue under a mutex and wakes the loop; the running thread moves
  posted handles to its queue and timer heap at the start of each turn.
  Nothing is lost, the handles one thread posts run in the order it posted
  them, and a handle posted before the loop thread schedules its own runs
  first (the loop thread takes in what was posted before it adds to its
  queue), as with a single queue. Under the same mutex, `close()` marks the loop closed before
  it closes the wake-up eventfd, so a late `call_soon_threadsafe` raises
  instead of writing to a descriptor number that may belong to someone else
  by then.
- **Everything else belongs to the owner.** Transports, listeners,
  `add_reader` / `add_writer`, `create_server`'s listening and closing: a call
  from a thread that does not own the loop raises `RuntimeError` (asyncio's
  debug-mode message). While no thread runs the loop, such a call claims it
  for its duration, so setting up on one thread and running on another
  works; two threads using an idle loop at the same moment get the error
  instead of a race. `Server.close()`, `start_serving()`, `loop.close()` and
  the transports written in Python (TLS, pipes, subprocesses, UDP) check
  before they change anything (a TLS record encrypted but never sent would
  break the connection).
- **`close()` and `abort()` are the exception.** The garbage collector runs
  finalizers on whichever thread triggers it, and finalizers close
  transports (`StreamWriter.__del__`, aiohttp's connections). A refused
  close would leave the connection open for good, with nothing left that
  refers to it. From a thread that does not own the loop, `close()` and
  `abort()` of every transport are handed to the loop's thread
  (`call_soon_threadsafe`).
- **Questions are answered on every thread.** `get_protocol()`,
  `get_extra_info()`, `is_closing()`, `is_running()`, `is_closed()`,
  `loop.backend`. (`get_write_buffer_size()`, `is_closing()` and
  `is_reading()` do not crash either, but they read plain flags and
  counters: from another thread the answer may be a moment old.) The object
  fields the others read (protocol, socket, extra info) are written under
  the transport's critical section; the socket object behind
  `get_extra_info` is created outside it (creating one makes system calls,
  and a critical section is given up whenever its thread blocks) and
  published with a compare-and-set.
- **`Handle.cancel()` works from every thread.** The handle's fields are read
  and cleared under its critical section; the cancelled flag is atomic.
  Cancelled timers are counted for the heap clean-up exactly on the loop's
  thread and in a separate atomic counter on other threads.
- **Free-threaded builds.** The extension declares that it does not need the
  GIL (`freethreading_compatible`), so 3.14t keeps the GIL off. One loop per
  thread is the intended way to use several cores: each loop has its own
  ring, buffers and queues and shares nothing with the others. Give each
  loop's server `reuse_port=True` to let the kernel spread connections. On
  the regular build the critical sections compile to nothing; the owner
  check is a thread-id comparison.

## Choices that matter

- **The submission queue cannot overflow.** A turn may queue more requests
  than the 1024-entry queue holds (one send per connection that wrote). A
  full queue is handed to the kernel part-way through the turn; if the
  kernel does not take it, further requests go to a side list that is fed
  in, in order, before the loop sleeps again. A queued request is never
  overwritten or dropped: the send buffers are pooled, so a lost or
  repeated send could put one connection's bytes on another.
- **The end of a connection has its own poll.** A multishot recv can miss
  it. The kernel parks the recv after a receive that emptied the socket and
  wakes it for the next event. But when data and the peer's close (or reset)
  both arrive before the loop takes its completions, the two wake-ups count
  as one: the recv reports the data, finds the socket empty, and waits for a
  wake-up that has already happened. TCP hides this for an orderly close (a
  FIN counts as pending input), not for a reset, and not on Unix sockets
  (seen on Linux 6.17). The connection would stay open for good. So every
  transport also holds one one-shot poll for "peer closed or reset"
  (`POLLRDHUP`; errors and hang-ups are always reported). When it fires, the
  recv is cancelled and armed again, and a new recv reads the socket before
  it parks: what is there arrives, in order, then the end. The cancel waits
  for the end of that run of completions, because after an orderly TCP close
  the recv usually reports the end itself a few completions later and
  nothing needs cancelling. The cost is one request per connection, none
  per message.
- **Stopping settles.** When `stop()` ends the run, the loop sends what the
  last turn wrote and takes the completions of what it closed, so those
  sockets are closed when `run_until_complete()` returns. Under asyncio a
  write reaches the socket at once and a closed transport's socket is closed
  in `connection_lost`; a program may not run the loop again for a while.
- **Signals are checked before sleeping.** The loop is not bytecode, so
  Python-level signal handlers run only when it asks: before each wait and
  whenever a wait returns `EINTR`. That is what makes Ctrl-C (and
  `asyncio.Runner`'s SIGINT handling) work on an idle loop. A signal that
  lands between that check and the start of the wait causes no `EINTR`; its
  handler then runs at the next wake-up (as under asyncio, whose window is
  wider). `add_signal_handler` handlers are not affected: they go through a
  wake-up socket.
- **Scheduling is two lists and a native heap.** `call_soon` appends to a
  list; a turn swaps it for an empty one and runs the batch, so callbacks
  scheduled by callbacks wait for the next turn. Timers are a binary heap of
  the `TimerHandle` objects themselves, ordered by (time, insertion) on C
  fields; both containers are ordinary lists, so the garbage collector still
  sees everything a pending callback keeps alive.
- **Writes wait for the end of the turn.** `transport.write()` copies into
  an output buffer and marks the transport dirty; the next I/O turn flushes
  every dirty transport before entering the kernel. With io_uring, N
  responses plus the wait for new input are one syscall. If more than the
  high-water mark is buffered, the transport first tries a plain
  non-blocking `send`, so a large response does not pause the protocol for
  nothing.
- **Output is copied, into pooled buffers.** A transport takes a buffer from
  a per-loop pool on its first write and gives it back when the send
  completes, so idle connections hold none. The copy is ~1 µs for 16 KiB.
  A multi-megabyte `write()` is copied once too; sending large buffers by
  reference is not done.
- **Accept is inline.** `protocol_factory()` and `connection_made()` run
  from the accept completion: no coroutine, no Task, no socket object per
  connection (one is created only if `get_extra_info("socket")`,
  `"peername"` or `"sockname"` is asked for, and then with the listener's
  family, so the socket module asks the kernel nothing). `TCP_NODELAY` is
  set once, on the listening socket; accepted sockets inherit it.
- **Accept is not multishot.** A multishot accept keeps its place at the
  head of the listening socket's queue of waiters. Of several processes
  that accept on one socket (`uvicorn --workers`, gunicorn, any pre-fork
  server) the one that armed first would then get every connection, also
  while it is busy and the others idle. A one-shot accept that is queued
  again after each connection goes to the back of that queue, and the
  kernel deals the connections round. It costs one request per connection.
- **`pause_reading()` is lazy under io_uring.** The multishot recv stays
  armed, so a pause that ends within the same turn costs nothing. If data
  does arrive while paused it is kept, and only then is the recv cancelled
  so TCP backpressure reaches the peer.
- **Closing cancels, then closes.** An io_uring request holds its file open,
  so `close(fd)` alone would leave the socket open while a recv is armed.
  The transport cancels its outstanding requests when it closes (one
  cancel for everything on the descriptor) and closes the socket when their
  final completions have arrived, usually within the next turn. Not earlier: a request still queued names the socket by its
  descriptor number, and a number freed too early can be given to a new
  connection (or any file another thread opens), which would then receive
  this connection's bytes. A listener closes its socket at once when the
  kernel takes the queued requests at once, else when its accept's final
  completion arrives. Under epoll the socket is removed from the epoll set
  explicitly and closed at once. `loop.close()` cancels every request of
  the ring and lets the kernel finish them before the ring goes, so a
  listening address is free, and peers see the end, when `close()` returns.
- **Each connection has its own context.** As under asyncio, every accepted
  connection runs its protocol callbacks (the factory included) in its own
  copy of the context the server started listening in, so a context
  variable one connection sets is not seen by the next. A run of
  completions for the same connection costs one Enter/Exit.
- **Input is delivered from inside the I/O turn.** `data_received` and the
  other protocol callbacks of a socket run while the loop goes through its
  completions, before the callbacks queued with `call_soon` in the previous
  turn. asyncio queues both in arrival order. The one place where programs
  rely on the order is kept: `await loop.create_connection()` (and
  `connect_accepted_socket`, `create_unix_connection`) returns before the
  first `data_received`; for these, reading starts one turn later, behind
  the waiting task's step.
- **`sendfile` is `sendfile(2)`.** On a plain connection `loop.sendfile`
  takes the transport's write side (a `write()` meanwhile raises, as under
  asyncio), waits for buffered output to leave, and calls `sendfile(2)` on
  the socket, waiting for "writable" in between (a one-shot poll under
  io_uring, `EPOLLOUT` under epoll). Over TLS, and for files that are not
  regular files, it copies through `write()`.
- **TLS is one object and one hop.** Ciphertext goes from the raw transport
  into OpenSSL's memory BIO; plaintext is read straight into the
  application's buffer (`BufferedProtocol`) or handed over as one `bytes`.
  `writelines` becomes a single TLS record. The read loop stops when neither
  the BIO nor OpenSSL has bytes left instead of reading until
  `SSLWantReadError`; this relies on OpenSSL taking one record at a time
  from the BIO (no read-ahead, which `ssl` never enables).

## Known gaps

- In a forked child a loop of the parent is closed: create a new one. A
  fork hook (`os.register_at_fork`) does it. The ring's queues and the epoll
  set are shared with the parent, so the child must not queue a request or
  change a registration: the hook drops the child's references without a
  word to the kernel, closes the child's copies of the connections, and
  leaves listening sockets to whoever holds them (a pre-fork server passes
  them to the child's own loop).
- A loop cannot move to another thread while a send is in flight (see
  above).
- On kernels before 6.13 a turn in which more than 256 connections have
  input runs out of receive buffers. The recvs that got none end with
  `ENOBUFS` and are armed again: correct, with some extra work. (From 6.13
  the kernel runs at most 20 items of deferred work per call, which makes
  it rare there.) More buffers, or incremental buffer consumption (6.12),
  would avoid it at a memory cost.
- From about Linux 6.5, a thread waiting in `io_uring_enter` while it has
  requests outstanding counts as I/O wait, and a loop always has some (its
  recvs and accepts). An idle loop thread therefore shows up to 100% iowait
  in `top` and in monitoring; the CPU is idle. Newer kernels have
  `IORING_ENTER_NO_IOWAIT` to turn this off; the loop does not use it yet.
- Ubuntu's 6.8 kernels (seen: 6.8.0-1070-azure, GitHub's `ubuntu-22.04`
  runners) carry a backport of the buffer ring registration in which the
  test of the reserved bytes is inverted (`!memchr_inv(reg.resv, ...)`): the
  correct call gets `EINVAL`. `ll_ring_init` then repeats the call with one
  reserved byte set, which only such a kernel takes. The same defect is in
  the unregister call, which the engine does not use (closing the ring
  frees the buffer ring).
- Pipes, subprocesses and UDP are correct but not accelerated.
- Not done: zero-copy send, registered file descriptors, large writes by
  reference, a Cython build of `tls.py`.
- The engine is Linux only. Elsewhere `ringloop.run()` and
  `new_event_loop()` use asyncio's loop (`ringloop.NATIVE` is false).
- `Handle.cancel()` from another thread does not wait for a callback that is
  already running (asyncio's handle for `call_soon_threadsafe` holds a lock
  while it runs).

## Tests

- `tests/`: this repository's suite, every test on both backends. It also
  smoke-tests uvicorn/Starlette, FastAPI, aiohttp and httpx on the loop.
- `conformance/run_cpython.py`: CPython's own asyncio test suite
  (`Lib/test/test_asyncio`) on ringloop. Every way that suite makes a loop
  is pointed at `ringloop.Loop`. The tests that examine private parts of
  asyncio's own loop classes are listed in `conformance/cpython_skips.txt`,
  each with its reason; an entry that matches no test is reported. CI runs
  it on both backends.
- Kernels: CI runs the suite with io_uring on the runners' own kernels
  (7.0, and Ubuntu's 6.8) and, in a virtual machine (virtme-ng), on the
  latest 6.1, 6.6 and 6.12 kernels.
- `tests/soak`: a server under clients that misbehave, then a check that
  nothing is left behind.
- The fallback on systems other than Linux: CI installs the package on macOS
  and Windows and runs a program on it.
- [Stario](https://stario.dev)'s suite, through `ringloop.pytest_plugin`
  (run by hand before a release).

## Checking a change

```sh
python setup.py build_ext --inplace && python -m pytest       # both backends
python conformance/run_cpython.py --lib ../cpython/Lib         # CPython's asyncio suite
```

`--lib` is the `Lib` directory of a CPython source tree of the version that
runs the tests (`git clone --depth 1 --branch v3.14.0
https://github.com/python/cpython`).

On a free-threaded build (3.14t), run the suite with
`-W error::RuntimeWarning`: importing an extension that would turn the GIL
back on then fails instead of warning.

Memory errors in the C and Cython code (AddressSanitizer + UBSan, GCC):

```sh
CFLAGS="-fsanitize=address,undefined -fno-omit-frame-pointer -O1 -g" \
LDFLAGS="-fsanitize=address,undefined" python setup.py build_ext --inplace --force
LD_PRELOAD=$(gcc -print-file-name=libasan.so) ASAN_OPTIONS=detect_leaks=0 \
PYTHONMALLOC=malloc python -m pytest tests/test_loop.py tests/test_robustness.py
```

Performance against another checkout (built in place), interleaved:

```sh
python bench/ab.py --base ../ringloop-main --inproc --instructions   # scheduling, timers, TLS: instructions per operation
python bench/ab.py --base ../ringloop-main --micro                   # req/s and CPU per request under bench/loadgen
python bench/ab.py --base ../ringloop-main --churn                   # server CPU per connection, one request each
```

Against other loops (uvloop, rloop, asyncio), on one thread and with one
loop per thread on a free-threaded build:

```sh
python bench/compare.py --pipeline 1,16                                       # one thread
python bench/compare.py --python python3.14t --threads 1,2,3 --work 300       # one loop per thread
```

`--instructions` counts user-space instructions with `perf_event_open`, so
it gives the same answer on a busy machine; use it for anything smaller than
a few percent. `bench/run.py` and `--micro` print the server's instructions
per request next to the time-based columns for the same reason.
`bench/README.md` lists every tool.
