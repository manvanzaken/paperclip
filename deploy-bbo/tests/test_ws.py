import asyncio
import json
import logging

from aiohttp import web, WSMsgType

from bbo_trader.venues.ws import Backoff, chunk, WSAdapter, WSRunner


def test_chunk_and_backoff():
    assert chunk([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4], [5]]
    assert chunk([1, 2], 0) == [[1, 2]]
    b = Backoff(healthy_s=20.0, cap_s=30.0, base_s=1.0)
    assert [b.next(0.0) for _ in range(6)] == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0]
    assert b.next(25.0) == 1.0            # a healthy socket resets the ladder
    assert b.next(0.0) == 1.0 and b.next(0.0) == 2.0


async def _serve(handler):
    app = web.Application()
    app.router.add_get("/ws", handler)
    runner = web.AppRunner(app, shutdown_timeout=0.2)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    return site._server.sockets[0].getsockname()[1], runner


def _fast_sleep(delays):
    async def sleep(d):               # records the runner's requested delays, waits (almost) nothing
        delays.append(d)
        await asyncio.sleep(0.005)
    return sleep


async def _until(pred, n=200):
    for _ in range(n):
        if pred():
            return True
        await asyncio.sleep(0.01)
    return pred()


async def _shutdown(r, task, runner):
    await r.stop()
    await asyncio.wait_for(task, 2.0)     # stop() ends run() and tears down every connection
    assert r.connections == 0
    await runner.cleanup()


async def test_runner_shards_subscribes_parses_and_ignores_non_json():
    subs, pings = [], []

    async def handler(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        first = await ws.receive()
        subs.append(json.loads(first.data))
        await ws.send_str("pong")                                  # non-JSON frame must be ignored
        await ws.send_json({"channel": "x", "v": len(subs), "symbol": "A"})
        async for m in ws:
            if m.type == WSMsgType.TEXT and m.data == "ping":
                pings.append(1)
                await ws.send_str("pong")
        return ws

    port, runner = await _serve(handler)
    items = []
    adapter = WSAdapter(name="t", url=f"ws://127.0.0.1:{port}/ws",
                        subscribe=lambda insts: [{"op": "sub", "args": insts}],
                        parse=lambda raw, state: [raw["v"]] if raw.get("channel") == "x" else [],
                        max_topics=2, ping=(0.02, "ping"))
    r = WSRunner(adapter, on_items=items.extend)
    r.set_instruments(["C", "A", "B"])
    task = asyncio.create_task(r.run())
    assert await _until(lambda: len(items) >= 2 and len(pings) >= 2)
    assert sorted(items) == [1, 2] and r.connections == 2 and r.connected
    assert {tuple(s["args"]) for s in subs} == {("A", "B"), ("C",)}
    r.set_instruments(["A", "B", "C", "D"])                        # reshard while running
    assert await _until(lambda: {tuple(s["args"]) for s in subs} >= {("A", "B"), ("C", "D")})
    await _shutdown(r, task, runner)


async def test_consumer_exception_costs_one_frame_not_the_socket(caplog):
    conns = []

    async def handler(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        conns.append(1)
        await ws.receive()
        for v in range(1, 30):
            await ws.send_json({"v": v})
            await asyncio.sleep(0.005)
        async for _ in ws:
            pass
        return ws

    port, runner = await _serve(handler)
    got = []

    def parse(raw, state):
        if raw["v"] == 2:
            raise KeyError("boom")                                # a parser / consumer bug on one frame
        return [raw["v"]]
    r = WSRunner(WSAdapter("t", f"ws://127.0.0.1:{port}/ws", lambda i: ["sub"], parse), on_items=got.extend)
    r.set_instruments(["A"])
    task = asyncio.create_task(r.run())
    with caplog.at_level(logging.ERROR, logger="bbo.ws"):
        assert await _until(lambda: 10 in got)
    assert 2 not in got and 1 in got and len(conns) == 1 and r.connected   # same socket, one quote lost
    assert "dropped frame #1" in caplog.text
    await _shutdown(r, task, runner)


async def test_silent_socket_and_dead_keepalive_are_dropped_and_reconnected(caplog):
    conns = []

    async def handler(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        conns.append(1)
        await ws.receive()
        await ws.send_json({"v": len(conns)})
        await asyncio.sleep(30)                                   # then silence: no frames, no CLOSE
        return ws

    port, runner = await _serve(handler)
    delays, got = [], []
    a = WSAdapter("t", f"ws://127.0.0.1:{port}/ws", lambda i: ["sub"], lambda raw, st: [raw["v"]],
                  receive_timeout=0.1, heartbeat=None)
    r = WSRunner(a, on_items=got.extend, sleep=_fast_sleep(delays))
    r.set_instruments(["A"])
    task = asyncio.create_task(r.run())
    assert await _until(lambda: len(conns) >= 3)                  # silent sockets are dropped and reopened
    assert [d for d in delays if d != 0.5][:2] == [1.0, 2.0]      # the backoff ladder, not a hot loop
    await _shutdown(r, task, runner)
    conns.clear()

    async def listening(request):                                  # a venue that answers the close handshake
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        conns.append(1)
        async for _ in ws:
            pass
        return ws
    port, runner = await _serve(listening)
    a2 = WSAdapter("t", f"ws://127.0.0.1:{port}/ws", lambda i: ["sub"], lambda raw, st: [raw["v"]],
                   ping=(0.01, object()), receive_timeout=None, heartbeat=None)   # unserializable keepalive
    r2 = WSRunner(a2, on_items=got.extend, sleep=_fast_sleep([]))
    r2.set_instruments(["A"])
    task2 = asyncio.create_task(r2.run())
    with caplog.at_level(logging.WARNING, logger="bbo.ws"):
        assert await _until(lambda: len(conns) >= 2)              # a dead keepalive drops the socket visibly
    assert "keepalive failed" in caplog.text
    await _shutdown(r2, task2, runner)


async def test_server_close_reconnects_with_backoff_ladder():
    conns, delays = [], []

    async def handler(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        conns.append(1)
        await ws.receive()
        await ws.close()                                          # venue drops us right after the subscribe
        return ws

    port, runner = await _serve(handler)
    r = WSRunner(WSAdapter("t", f"ws://127.0.0.1:{port}/ws", lambda i: ["sub"], lambda raw, st: []),
                 on_items=lambda items: None, sleep=_fast_sleep(delays))
    r.set_instruments(["A"])
    task = asyncio.create_task(r.run())
    assert await _until(lambda: len(conns) >= 4)
    assert [d for d in delays if d != 0.5][:3] == [1.0, 2.0, 4.0]
    await _shutdown(r, task, runner)
