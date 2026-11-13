"""pytest plugin: run another project's asyncio tests on ringloop.

    pytest -p ringloop.pytest_plugin ...

Every loop asyncio creates by default (pytest-asyncio, `asyncio.run`,
`asyncio.new_event_loop`) becomes a `ringloop.Loop`: the plugin calls
`ringloop.install()`. RINGLOOP_BACKEND picks the backend as usual. On
systems other than Linux the plugin does nothing.
"""
import asyncio

import ringloop

ringloop.install()


def pytest_report_header(config):
    if not ringloop.NATIVE:
        return "event loop: asyncio (ringloop has no engine for this system)"
    loop = asyncio.new_event_loop()
    try:
        return f"event loop: {type(loop).__module__}.{type(loop).__name__} ({loop.backend})"
    finally:
        loop.close()
