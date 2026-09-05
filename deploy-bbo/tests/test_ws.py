import asyncio
import json

from aiohttp import web, WSMsgType

from bbo_trader.venues.ws import Backoff, chunk, WSAdapter, WSRunner


def test_chunk_and_backoff():
    assert chunk([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4], [5]]
    assert chunk([1, 2], 0) == [[1, 2]]
    b = Backoff(healthy_s=20.0, cap_s=30.0, base_s=1.0)
    assert [b.next(0.0) for _ in range(6)] == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0]
    assert b.next(25.0) == 1.0            # a healthy socket resets the ladder
    assert b.next(0.0) == 1.0 and b.next(0.0) == 2.0


async def test_runner_shards_subscribes_parses_and_ignores_non_json():
    subs = []

    async def handler(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        first = await ws.receive()
        subs.append(json.loads(first.data))
        await ws.send_str("pong")                                  # non-JSON frame must be ignored
        await ws.send_json({"channel": "x", "v": len(subs), "symbol": "A"})
        async for m in ws:
            if m.type == WSMsgType.TEXT and m.data == "ping":
                await ws.send_str("pong")
        return ws

    app = web.Application()
    app.router.add_get("/ws", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    items = []
    adapter = WSAdapter(name="t", url=f"ws://127.0.0.1:{port}/ws",
                        subscribe=lambda insts: [{"op": "sub", "args": insts}],
                        parse=lambda raw, state: [raw["v"]] if raw.get("channel") == "x" else [],
                        max_topics=2, ping=(0.05, "ping"))
    r = WSRunner(adapter, on_items=items.extend)
    r.set_instruments(["C", "A", "B"])
    task = asyncio.create_task(r.run())
    for _ in range(100):
        if len(items) >= 2:
            break
        await asyncio.sleep(0.02)
    assert sorted(items) == [1, 2] and r.connections == 2 and r.connected
    assert {tuple(s["args"]) for s in subs} == {("A", "B"), ("C",)}
    await r.stop()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await runner.cleanup()
