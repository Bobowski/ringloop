"""`python -m ringloop`: what loops on this system get, and why."""

import errno
import os
import platform
import sys

import ringloop


def main():
    print(f"ringloop {ringloop.__version__}, Python {platform.python_version()}"
          f"{' (free-threaded)' if not getattr(sys, '_is_gil_enabled', lambda: True)() else ''}, "
          f"{platform.system()} {platform.release()} {platform.machine()}")
    if not ringloop.NATIVE:
        print("engine: none for this system; new_event_loop() returns asyncio's loop")
        return
    from ringloop._core import uring_probe

    forced = os.environ.get("RINGLOOP_BACKEND")
    if forced:
        print(f"RINGLOOP_BACKEND={forced}")
    failure = uring_probe()
    if failure is None:
        print("io_uring: available")
    else:
        code, step = failure
        hint = {
            errno.ENOSYS: "the kernel has no io_uring",
            errno.EPERM: "not permitted (a seccomp or container policy, or kernel.io_uring_disabled)",
            errno.EACCES: "not permitted (a security policy)",
            errno.EINVAL: "the kernel is older than 6.1, or refuses this setup",
            errno.ENOMEM: "no memory for a ring: RLIMIT_MEMLOCK is too low (see `ulimit -l`)",
        }.get(code, "")
        print(f"io_uring: not available: {step} failed with {errno.errorcode.get(code, code)}"
              f" ({os.strerror(code)}){': ' + hint if hint else ''}")
    try:
        with open("/proc/sys/kernel/io_uring_disabled") as f:
            print(f"kernel.io_uring_disabled = {f.read().strip()}")
    except OSError:
        pass
    try:
        import resource
        soft, _ = resource.getrlimit(resource.RLIMIT_MEMLOCK)
        print(f"RLIMIT_MEMLOCK = {'unlimited' if soft == resource.RLIM_INFINITY else f'{soft // 1024} KiB'}")
    except (ImportError, OSError):
        pass
    loop = ringloop.new_event_loop()
    try:
        print(f"a new loop uses: {loop.backend}")
    finally:
        loop.close()


main()
