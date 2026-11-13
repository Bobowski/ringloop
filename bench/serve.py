"""Run Stario's benchmark app on a chosen event loop.

    python bench/serve.py --loop asyncio|uvloop|ringloop [--port 8000] [--host 0.0.0.0]
"""
import argparse
import asyncio
import os
import sys

STARIO = os.environ.get("STARIO_CHECKOUT") or os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".deps", "stario")
if not os.path.isdir(os.path.join(STARIO, "benchmarks", "server")):
    sys.exit("set STARIO_CHECKOUT to a checkout of https://github.com/stariodev/stario (or clone it to .deps/stario)")
sys.path.insert(0, os.path.join(STARIO, "benchmarks", "server"))
os.environ.setdefault("STARIO_TRACER", "noop")

ap = argparse.ArgumentParser()
ap.add_argument("--loop", default="asyncio")
ap.add_argument("--port", type=int, default=8000)
ap.add_argument("--host", default="127.0.0.1", help="0.0.0.0 to take load from another machine")
args = ap.parse_args()
os.environ["STARIO_HOST"] = args.host
os.environ["STARIO_PORT"] = str(args.port)

if os.environ.get("BENCH_GC") == "0":
    import gc

    gc.disable()

from apps.stario_app import bootstrap as stario_bootstrap  # noqa: E402
from stario import Route, serve  # noqa: E402


async def yield_once(c, w):
    # One trip through the ready queue before answering.
    await asyncio.sleep(0)
    w.text("Hello, World!")


async def sleep_1ms(c, w):
    # One timer per request.
    await asyncio.sleep(0.001)
    w.text("Hello, World!")


async def bootstrap(app, span):
    app.add(Route("GET /yield"), yield_once)
    app.add(Route("GET /sleep"), sleep_1ms)
    inner = stario_bootstrap(app, span)
    await inner.__anext__()
    yield
    try:
        await inner.__anext__()
    except StopAsyncIteration:
        pass


if args.loop == "asyncio":
    asyncio.run(serve(bootstrap))
elif args.loop == "uvloop":
    import uvloop

    uvloop.run(serve(bootstrap))
elif args.loop == "rloop":
    import rloop

    asyncio.run(serve(bootstrap), loop_factory=rloop.new_event_loop)
else:
    import ringloop

    ringloop.run(serve(bootstrap))
