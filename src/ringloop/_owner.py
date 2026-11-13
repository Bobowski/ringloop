"""Loop ownership for the transports written in Python (TLS, pipes,
subprocesses, UDP).

A method that changes what the loop owns runs on the thread that owns the
loop or, while no thread runs the loop, holds the loop for the call (see
DESIGN.md, "Threads"). The check comes first, so a call from the wrong thread
fails before it has changed anything: a TLS record encrypted but never sent,
for one, would break the connection.

`close()` and `abort()` are the exception. The garbage collector runs
finalizers on whichever thread it happens to be on, and finalizers close
transports; a refused close would leave the connection open for good. From
another thread they are handed to the loop's thread instead.
"""

import functools


def loop_owned(method):
    @functools.wraps(method)
    def call(self, *args, **kwargs):
        core = self._loop._core
        if core.on_owner_thread():
            return method(self, *args, **kwargs)
        return core.run_owned(functools.partial(method, self, *args, **kwargs))

    return call


def loop_owned_close(method):
    @functools.wraps(method)
    def call(self):
        core = self._loop._core
        if core.on_owner_thread():
            return method(self)
        return core.run_owned_or_schedule(functools.partial(method, self))

    return call
