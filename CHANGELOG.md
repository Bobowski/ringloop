# Changelog

The versions follow semantic versioning. The public interface is what
`README.md` describes: `ringloop.run`, `new_event_loop`, `install`, `Loop`,
`Server`, `NATIVE`, `loop.backend`, the `RINGLOOP_BACKEND` variable,
`python -m ringloop` and `ringloop.pytest_plugin`. Everything else,
`ringloop._core` included, can change in any release.

## Unreleased

Fixes found by comparing the loop with uvloop, rloop and the io_uring
runtimes (`tests/test_other_loops.py` holds a test for each):

- epoll backend: output that `resume_writing()` wrote while the turn's flush
  emptied a buffer waited for an unrelated event before it was sent.
- `add_reader` / `add_writer` (and so `sock_recv`, `sock_sendall`,
  `sock_connect`) after a descriptor was closed under a pending call and its
  number reused: a new registration on that number was never made (the call
  hung, both backends), and cancelling the old call could unregister a
  connection that now held the number (epoll backend).
- TLS: closing while the peer still sends reads that data away before the
  TLS shutdown, as uvloop does, so the peer gets a clean `close_notify`
  instead of a broken pipe or a reset.
- Debug mode: `call_soon_threadsafe` from a thread with no Python frame
  failed.
- io_uring: a ring whose kernel dropped completions (out of memory) now
  stops with a clear error instead of a bare `OSError(EBADR)`.
- io_uring polls carry the right events on big-endian machines (sdist builds
  only; the wheels are little-endian).
- DESIGN.md and README.md: where ring memory is charged (by kernel version),
  the receive buffers' size (4 MiB), when the kernel caps deferred work
  (6.13), the signal check window, and iowait on idle loop threads.

## 1.0.0

The first public release.

- An asyncio event loop for Linux on io_uring (Linux 6.1 and newer), with an
  epoll backend for older kernels and for containers that do not allow
  io_uring. The loop chooses the backend itself.
- The whole `asyncio.AbstractEventLoop` interface: TCP and Unix servers and
  clients, TLS, `start_tls`, `sock_*`, UDP, pipes, subprocesses, signals,
  executors, `sendfile`, Happy Eyeballs, debug mode.
- CPython 3.12 to 3.15, and the free-threaded builds 3.14t and 3.15t with
  one loop per thread. Wheels for manylinux and musllinux, x86-64 and
  aarch64.
- On systems other than Linux the package installs and hands out asyncio's
  loop.
- `ringloop.install()` for programs that take no loop factory, and
  `ringloop.pytest_plugin` to run a test suite on the loop.

How it was checked before the release:

- CPython's own asyncio test suite runs against the loop on both backends
  (`conformance/`), on Python 3.12 to 3.15.
- This repository's suite runs on both backends, on every supported Python,
  with io_uring on kernels 6.1, 6.6, 6.8 (Ubuntu), 6.12, 6.17 and 7.0.
- AddressSanitizer and UBSan builds, a soak test with misbehaving clients
  (`tests/soak`), and smoke tests of uvicorn/Starlette, FastAPI, aiohttp
  and httpx on the loop. The test suite of Stario (1,804 tests) passes on
  both backends.
- Two independent code reviews of the engine. What they found is fixed, and
  `tests/test_hardening.py` holds a test for each defect.

`README.md` lists the differences from asyncio and the limits.
