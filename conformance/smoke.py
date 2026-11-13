"""A built wheel imports and carries a request: the check cibuildwheel runs."""

import asyncio

import ringloop


async def main():
    async def handle(reader, writer):
        writer.write((await reader.readline()).upper())
        writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    reader, writer = await asyncio.open_connection(*server.sockets[0].getsockname())
    writer.write(b"hello\n")
    assert await reader.read() == b"HELLO\n"
    writer.close()
    server.close()
    await server.wait_closed()
    return asyncio.get_running_loop().backend


assert ringloop.NATIVE
print("ringloop", ringloop.__version__, "on", ringloop.run(main()))
