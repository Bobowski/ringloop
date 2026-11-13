"""Plug-and-play: other asyncio frameworks run unchanged on ringloop."""

import asyncio

import pytest

import ringloop

from .test_loop import backend  # noqa: F401  (fixture: skips io_uring where the kernel refuses it)

HOST = "127.0.0.1"


def run(coro, backend):
    return ringloop.run(asyncio.wait_for(coro, 30), backend=backend)


def test_uvicorn_starlette_with_httpx_client(backend):
    uvicorn = pytest.importorskip("uvicorn")
    httpx = pytest.importorskip("httpx")
    from starlette.applications import Starlette
    from starlette.responses import JSONResponse, StreamingResponse
    from starlette.routing import Route, WebSocketRoute

    async def hello(request):
        return JSONResponse({"hello": request.query_params.get("name", "world")})

    async def echo(request):
        return JSONResponse({"n": len(await request.body())})

    async def stream(request):
        async def gen():
            for i in range(50):
                yield b"chunk %d\n" % i
                await asyncio.sleep(0)

        return StreamingResponse(gen())

    async def ws(websocket):
        await websocket.accept()
        text = await websocket.receive_text()
        await websocket.send_text(text[::-1])
        await websocket.close()

    app = Starlette(routes=[Route("/", hello), Route("/echo", echo, methods=["POST"]),
                            Route("/stream", stream), WebSocketRoute("/ws", ws)])

    async def main():
        assert type(asyncio.get_running_loop()).__module__.startswith("ringloop")
        config = uvicorn.Config(app, host=HOST, port=0, log_level="warning", lifespan="off")
        server = uvicorn.Server(config)
        task = asyncio.create_task(server.serve())
        while not server.started:
            await asyncio.sleep(0.01)
        port = server.servers[0].sockets[0].getsockname()[1]
        async with httpx.AsyncClient(base_url=f"http://{HOST}:{port}") as client:
            r = await client.get("/", params={"name": "suv"})
            assert r.json() == {"hello": "suv"}
            r = await client.post("/echo", content=b"x" * 2_000_000)
            assert r.json() == {"n": 2_000_000}
            r = await client.get("/stream")
            assert r.text.count("chunk") == 50
            rs = await asyncio.gather(*(client.get("/") for _ in range(200)))
            assert all(x.status_code == 200 for x in rs)
        server.should_exit = True
        await task

    run(main(), backend)


def test_aiohttp_server_and_client_with_websocket(backend):
    aiohttp = pytest.importorskip("aiohttp")
    from aiohttp import web

    async def hello(request):
        return web.json_response({"path": request.path})

    async def upload(request):
        return web.Response(text=str(len(await request.read())))

    async def ws(request):
        sock = web.WebSocketResponse()
        await sock.prepare(request)
        async for msg in sock:
            await sock.send_str(msg.data.upper())
        return sock

    async def main():
        app = web.Application(client_max_size=10 * 1024 * 1024)
        app.add_routes([web.get("/hello", hello), web.post("/upload", upload), web.get("/ws", ws)])
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, HOST, 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        base = f"http://{HOST}:{port}"
        async with aiohttp.ClientSession() as session:
            async with session.get(base + "/hello") as r:
                assert await r.json() == {"path": "/hello"}
            async with session.post(base + "/upload", data=b"y" * 3_000_000) as r:
                assert await r.text() == "3000000"
            async with session.ws_connect(base + "/ws") as sock:
                await sock.send_str("ping")
                assert (await sock.receive()).data == "PING"
            results = await asyncio.gather(*(session.get(base + "/hello") for _ in range(100)))
            assert all(r.status == 200 for r in results)
        await runner.cleanup()

    run(main(), backend)


def test_fastapi_app(backend):
    fastapi = pytest.importorskip("fastapi")
    uvicorn = pytest.importorskip("uvicorn")
    httpx = pytest.importorskip("httpx")
    app = fastapi.FastAPI()

    @app.get("/items/{item_id}")
    async def item(item_id: int, q: str = ""):
        await asyncio.sleep(0.001)
        return {"id": item_id, "q": q}

    @app.get("/sync")
    def sync_endpoint():  # runs in the thread pool
        return {"sync": True}

    async def main():
        server = uvicorn.Server(uvicorn.Config(app, host=HOST, port=0, log_level="warning"))
        task = asyncio.create_task(server.serve())
        while not server.started:
            await asyncio.sleep(0.01)
        port = server.servers[0].sockets[0].getsockname()[1]
        async with httpx.AsyncClient(base_url=f"http://{HOST}:{port}") as client:
            assert (await client.get("/items/7", params={"q": "z"})).json() == {"id": 7, "q": "z"}
            assert (await client.get("/sync")).json() == {"sync": True}
            assert (await client.get("/items/x")).status_code == 422
        server.should_exit = True
        await task

    run(main(), backend)
