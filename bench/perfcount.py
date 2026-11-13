"""User-space instruction counts through perf_event_open (Linux).

Unlike time, the count barely moves when the machine is busy with other
work, so it can show a change of a percent or two. Needs
/proc/sys/kernel/perf_event_paranoid <= 2 (the usual default).
"""
import ctypes
import os
import platform
import struct

# The syscall number differs between architectures.
_PERF_EVENT_OPEN = {"x86_64": 298, "aarch64": 241}.get(platform.machine(), 298)


def instruction_counter(pid=0):
    """Return a function giving the instructions executed so far, in user
    space, by thread `pid` (0: the calling thread)."""
    # struct perf_event_attr: type = PERF_TYPE_HARDWARE (0), size,
    # config = PERF_COUNT_HW_INSTRUCTIONS (1); flags: exclude_kernel, exclude_hv.
    attr = bytearray(128)
    struct.pack_into("IIQ", attr, 0, 0, 128, 1)
    struct.pack_into("Q", attr, 40, (1 << 5) | (1 << 6))
    libc = ctypes.CDLL(None, use_errno=True)
    fd = libc.syscall(_PERF_EVENT_OPEN, (ctypes.c_char * 128).from_buffer(attr), pid, -1, -1, 0)
    if fd < 0:
        raise OSError(ctypes.get_errno(), "perf_event_open failed (see /proc/sys/kernel/perf_event_paranoid)")
    return lambda: struct.unpack("Q", os.read(fd, 8))[0]
