# ringloop

An asyncio event loop for Linux on io_uring, with an epoll backend for the
places where io_uring is not allowed. It is a drop-in loop: it implements
`asyncio.AbstractEventLoop` on its own engine, so asyncio programs,
frameworks and libraries run on it unchanged.

```python
import ringloop

ringloop.run(main())                      # like asyncio.run(main())
```

## Install

```sh
pip install ringloop        # or: uv add ringloop
```

- Python 3.12 or newer, including the free-threaded builds (3.14t and newer).
- Linux, x86-64 or aarch64, for the engine. io_uring needs Linux 6.1; an older
  kernel, or a container whose seccomp policy refuses io_uring (the defaults
  of Docker and Podman do), gets the epoll backend automatically. `python -m
  ringloop` shows what a system gets, and why. A seccomp policy that kills
  the process instead of refusing the call (systemd's `SystemCallFilter=`
  without an error number) cannot be detected: set `RINGLOOP_BACKEND=epoll`
  there.
- On macOS and Windows the package installs too, and `ringloop.run()` /
  `ringloop.new_event_loop()` hand out asyncio's own loop. One program then
  runs on a developer's laptop and on the Linux server. `ringloop.NATIVE`
  says which it is.
- No runtime dependencies. No libuv, no liburing.

## Use

Anything that takes a loop factory takes `ringloop.new_event_loop`:

```python
asyncio.run(main(), loop_factory=ringloop.new_event_loop)

with asyncio.Runner(loop_factory=ringloop.new_event_loop) as runner:
    runner.run(main())
```

```sh
uvicorn app:app --loop ringloop:new_event_loop
uvicorn app:app --loop ringloop:new_event_loop --workers 4
```

For a program that creates its loop itself and takes no factory
(`asyncio.new_event_loop()` somewhere inside a framework), make ringloop the
default first. This uses asyncio's event loop policy, which Python 3.16
removes:

```python
import ringloop
ringloop.install()
```

Granian is such a program. It creates the loop before it imports the
application, so `install()` belongs in a launcher (Granian's workers import
the launcher again):

```python
# serve.py
import ringloop
ringloop.install()

from granian import Granian

if __name__ == "__main__":
    Granian("app:app", interface="asgi", loop="asyncio").serve()
```

To run another project's test suite on ringloop (pytest-asyncio, anyio,
plain `asyncio.run`):

```sh
pytest -p ringloop.pytest_plugin
```

Which backend a loop uses:

```python
loop.backend                               # "uring" or "epoll"
ringloop.new_event_loop(backend="epoll")  # or RINGLOOP_BACKEND=epoll
```

```sh
python -m ringloop          # which backend this system gets, and why
```

### More than one core

A loop belongs to one thread. On a free-threaded build, run one loop per
thread and let the kernel spread the connections:

```python
def worker():
    async def serve():
        server = await asyncio.start_server(handle, "0.0.0.0", 8000, reuse_port=True)
        await server.serve_forever()
    ringloop.run(serve())

for _ in range(os.cpu_count()):
    threading.Thread(target=worker).start()
```

Each loop has its own ring, buffers and queues. The extension does not need
the GIL and does not turn it back on. `call_soon_threadsafe`,
`run_coroutine_threadsafe`, `Handle.cancel()`, `loop.stop()` and
`transport.close()` / `abort()` work from any thread.

Worker processes that accept on one listening socket (`--workers`, gunicorn,
a pre-fork server) share the connections evenly on both backends. After
`fork()` the child's copy of a loop is closed: the child creates its own.

## What you get

- **The whole loop interface.** TCP and Unix servers and clients, TLS
  (`ssl=`, `start_tls`), `sock_*`, UDP, pipes, subprocesses, signals,
  executors, `sendfile` (through `sendfile(2)` on plain connections), Happy
  Eyeballs, debug mode.
- **It behaves like asyncio.** CPython's own asyncio test suite runs against
  it in CI (`conformance/`): more than 2,100 tests, on both backends. The tests
  that do not apply (they examine private parts of asyncio's own loop) are
  listed one by one, with the reason, in `conformance/cpython_skips.txt`.
- **Speed.** One `io_uring_enter` per loop iteration submits every write of
  that iteration and waits for input; receives are multishot into a shared
  buffer ring; an accepted connection costs no task and no socket object.
  Measured with a bare protocol under a C load generator
  (`results/2026-11-12/`, a small VM: read the ratios, not the figures):

  | server CPU per request | 1 request per round trip | 16 pipelined |
  | --- | --- | --- |
  | ringloop, io_uring | 1.67 µs | 0.17 µs |
  | ringloop, epoll | 2.03 µs | 0.17 µs |
  | rloop 0.5 | 2.49 µs | 0.20 µs |
  | uvloop 0.23 | 2.73 µs | 0.21 µs |
  | asyncio 3.14 | 3.43 µs | 0.25 µs |

  With a [Stario](https://stario.dev) application on top, counted per JSON
  request (`results/2026-11-12/counts.txt`): 0.03 system calls on io_uring
  (one `io_uring_enter` carries about 38 requests) against 2 on uvloop and
  on the epoll backend; 19,800 user-space instructions against 23,900 on
  uvloop, 22,300 on rloop and 69,000 on asyncio's loop. In CPU time the
  same VM measured about half of uvloop's for small responses
  (`stario.txt`). That ratio is larger than the counts explain, and it has
  not been confirmed on a machine with performance counters. For a rendered
  page the loop makes no difference. `bench/README.md` lists the tools.
- **Threads.** On a free-threaded build with one loop per thread, two
  threads have about twice the capacity of one, and two loops in one process
  cost the same CPU per request as two processes with one loop each: the
  loops share nothing. The same VM could not measure more than two server
  threads; `bench/compare.py --threads` does it on a larger machine.
  `DESIGN.md` explains the choices.

## Differences from asyncio

- A stream transport is an extension type. `isinstance(transport,
  asyncio.Transport)` is true (through `__class__`), `type(transport)` is
  `ringloop._core.Transport`.
- Protocol callbacks for a socket run from inside the I/O step of the loop,
  not as separate `call_soon` callbacks. A callback scheduled with
  `call_soon` can therefore run after input that arrived later. `await
  loop.create_connection()` still returns before the first
  `data_received()`.
- Transport methods other than `close()` and `abort()` raise `RuntimeError`
  when they are called from a thread that is not the loop's while the loop
  runs. asyncio does not check (and corrupts its state in silence). Use
  `loop.call_soon_threadsafe(transport.write, data)`.
- `Handle.cancel()` from another thread does not wait for a callback that is
  already running.
- In a child process after `fork()` the loop is closed (asyncio leaves a
  loop that shares its selector with the parent). Create a new loop in the
  child.
- A half-closed connection that the protocol keeps open (`eof_received()`
  returned true) is closed, with `connection_lost`, when its other half ends
  or the peer resets it. asyncio notices at the next write.
- `resume_writing()` is called when the write buffer is empty, not at the
  low-water mark (as uvloop does it).
- `add_reader` / `add_writer` refuse a descriptor that a transport or server
  of the same loop owns.

## Limits

- Ring memory counts against `RLIMIT_MEMLOCK` unless the process has
  `CAP_IPC_LOCK`. A ring takes about 140 KiB, so the usual 8 MiB limit holds
  about 50 loops. A loop that cannot get a ring logs one warning and uses
  epoll.
- A loop creates its ring when it opens its first socket. A program that
  only runs timers, tasks or subprocesses never creates one.

## Development

```sh
pip install -e . --group test
python -m pytest                           # every test on both backends
python conformance/run_cpython.py --lib /path/to/cpython/Lib
```

`DESIGN.md` describes the engine, the threading model, and how to check a
change (sanitizer build, A/B benchmarks).

## License

MIT
