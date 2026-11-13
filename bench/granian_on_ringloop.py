"""Granian with ringloop as its event loop: `python granian_on_ringloop.py PORT`.

Granian makes the loop before it imports the application, so ringloop is
installed here, in the launcher (the workers import this module again)."""

import sys

import ringloop

ringloop.install()

from granian import Granian  # noqa: E402

if __name__ == "__main__":
    Granian("asgi_app:app", interface="asgi", loop="asyncio", port=int(sys.argv[1]), workers=1,
            log_access=False).serve()
