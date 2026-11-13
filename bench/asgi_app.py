"""The smallest ASGI application, for bench/servers.py."""



async def app(scope, receive, send):
    if scope["type"] != "http":
        return
    await send({"type": "http.response.start", "status": 200,
                "headers": [(b"content-type", b"text/plain; charset=utf-8"), (b"content-length", b"13")]})
    await send({"type": "http.response.body", "body": b"Hello, World!"})
