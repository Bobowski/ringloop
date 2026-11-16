"""ringloop: an asyncio event loop on io_uring (epoll fallback).

    import ringloop
    ringloop.run(main())

Works with any asyncio code: `Loop` implements `asyncio.AbstractEventLoop`
on its own engine and does not depend on asyncio's internal loop classes.

The engine is Linux-only. On other systems the package still installs and
`run()` / `new_event_loop()` use asyncio's own loop, so one program runs on a
developer's Mac and on the Linux server; `ringloop.NATIVE` says which it is.
"""

import asyncio
import sys
from collections.abc import Coroutine
from typing import Any, TypeVar

_T = TypeVar("_T")

__version__ = "1.0.1"

__all__ = ["Loop", "NATIVE", "Server", "__version__", "install", "new_event_loop", "run"]

#: True where the ringloop engine is used (Linux), False where
#: `new_event_loop()` returns asyncio's loop.
NATIVE: bool = sys.platform == "linux"

if NATIVE:
    from .loop import Loop, Server
else:
    class Loop:
        """The ringloop event loop. Its engine needs Linux: on this system
        there are no instances (`isinstance(loop, ringloop.Loop)` is false)."""

        def __init__(self, *args, **kwargs):
            raise RuntimeError("ringloop.Loop needs Linux; ringloop.new_event_loop() "
                               "returns asyncio's loop on this system")

    class Server(Loop):
        """What `Loop.create_server` returns (Linux only)."""


def new_event_loop(*, backend: str | None = None) -> asyncio.AbstractEventLoop:
    """A new loop. `backend`: "uring", "epoll", or None to choose
    (io_uring when the kernel allows it; RINGLOOP_BACKEND overrides).

    This is a loop factory: `asyncio.run(main(), loop_factory=ringloop.new_event_loop)`,
    `uvicorn --loop ringloop:new_event_loop`."""
    if not NATIVE:
        if backend is not None:
            raise ValueError(f"the {backend!r} backend needs Linux")
        return asyncio.EventLoop() if hasattr(asyncio, "EventLoop") else asyncio.new_event_loop()
    return Loop(backend=backend)


def run(main: Coroutine[Any, Any, _T], *, debug: bool | None = None, backend: str | None = None) -> _T:
    """`asyncio.run` on a ringloop loop."""
    with asyncio.Runner(debug=debug, loop_factory=lambda: new_event_loop(backend=backend)) as runner:
        return runner.run(main)


def install() -> None:
    """Make ringloop the loop that asyncio creates by default.

    For programs that cannot be given a loop factory: after this,
    `asyncio.run()`, `asyncio.new_event_loop()` and everything built on them
    (a server started by a framework, for one) get a ringloop loop. It
    works through asyncio's event loop policy, which Python 3.16 removes;
    there, and wherever a loop factory can be passed, pass
    `ringloop.new_event_loop`. On systems other than Linux it does nothing.
    """
    if not NATIVE:
        return
    try:
        from asyncio.unix_events import _UnixDefaultEventLoopPolicy as base
    except ImportError:
        raise RuntimeError(
            "this Python has no event loop policies: pass "
            "loop_factory=ringloop.new_event_loop to asyncio.run() or asyncio.Runner()") from None
    from asyncio import events

    class EventLoopPolicy(base):
        _loop_factory = Loop

    # 3.14 and 3.15 deprecate the public setter; the private one is the same.
    setter = getattr(events, "_set_event_loop_policy", None) or events.set_event_loop_policy
    setter(EventLoopPolicy())
