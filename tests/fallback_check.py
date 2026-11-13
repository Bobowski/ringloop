"""On a system other than Linux the package installs and hands out asyncio's
loop. CI runs this on macOS and Windows: `python tests/fallback_check.py`."""
import asyncio

import ringloop


async def main():
    async def echo(reader, writer):
        writer.write(await reader.readline())
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(echo, "127.0.0.1", 0)
    reader, writer = await asyncio.open_connection(*server.sockets[0].getsockname()[:2])
    writer.write(b"hello\n")
    assert await reader.readline() == b"hello\n"
    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()
    return asyncio.get_running_loop()


assert not ringloop.NATIVE, "this check is for systems without the engine"
stdlib = asyncio.new_event_loop()
stdlib.close()
assert type(ringloop.run(main())) is type(stdlib)
assert type(asyncio.run(main(), loop_factory=ringloop.new_event_loop)) is type(stdlib)
ringloop.install()  # does nothing here
for refused, error in ((lambda: ringloop.new_event_loop(backend="epoll"), ValueError), (ringloop.Loop, RuntimeError)):
    try:
        refused()
    except error:
        pass
    else:
        raise AssertionError("expected " + error.__name__)
print("ok:", ringloop.__version__, type(stdlib).__name__)
