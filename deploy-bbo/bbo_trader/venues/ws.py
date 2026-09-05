"""Generic sharded WebSocket runner (ported from SpreadWatch's WSFeed, incl. its hard-won lessons):
one connection task per shard of `max_topics` instruments, uptime-keyed reconnect backoff, optional
app-level keepalive, non-JSON frames ignored, server closes logged with the socket's lifetime."""
from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Callable, Iterable

import aiohttp

log = logging.getLogger("bbo.ws")


class Backoff:
    """Escalates only for sockets that die FAST; a socket that lived healthy_s resets to base."""

    def __init__(self, healthy_s: float = 20.0, cap_s: float = 30.0, base_s: float = 1.0):
        self.healthy_s, self.cap_s, self.base_s = healthy_s, cap_s, base_s
        self._d = base_s

    def next(self, uptime_s: float) -> float:
        if uptime_s >= self.healthy_s:
            self._d = self.base_s
            return self.base_s
        d = self._d
        self._d = min(self._d * 2.0, self.cap_s)
        return d


def chunk(items: list, max_topics: int) -> list[list]:
    if not max_topics:
        return [list(items)]
    return [list(items[i:i + max_topics]) for i in range(0, len(items), max_topics)]


@dataclass
class WSAdapter:
    name: str
    url: str
    subscribe: Callable[[list[str]], list]        # -> messages (dict → JSON, str → text frame)
    parse: Callable[[dict, dict], list]           # (raw json, per-connection scratch) -> parsed items
    max_topics: int = 0
    ping: tuple[float, object] | None = None      # (interval_s, message) app-level keepalive
    text_ping_reply: tuple[str, str] | None = None  # (server text frame, our reply)
    heartbeat: float | None = 20.0                # aiohttp protocol ping


async def _send(ws, msg) -> None:
    if isinstance(msg, str):
        await ws.send_str(msg)
    else:
        await ws.send_json(msg)


class WSRunner:
    def __init__(self, adapter: WSAdapter, on_items: Callable[[list], None],
                 session_factory=aiohttp.ClientSession, sleep=asyncio.sleep):
        self.adapter = adapter
        self.on_items = on_items
        self._session_factory = session_factory
        self._sleep = sleep
        self._insts: list[str] = []
        self._reconnect = False
        self._stop = False
        self._connected: set[int] = set()

    def set_instruments(self, insts: Iterable[str]) -> None:
        new = sorted(set(insts))
        if new != self._insts:
            self._insts = new
            self._reconnect = True

    @property
    def connected(self) -> bool:
        return bool(self._connected)

    @property
    def connections(self) -> int:
        return len(self._connected)

    async def stop(self) -> None:
        self._stop = True

    async def run(self) -> None:
        while not self._stop:
            self._reconnect = False
            shards = chunk(self._insts, self.adapter.max_topics) if self._insts else []
            tasks = [asyncio.create_task(self._run_conn(i, s)) for i, s in enumerate(shards)]
            try:
                while not self._reconnect and not self._stop:
                    await self._sleep(0.5)
                if self._reconnect:
                    log.info("%s instruments changed, resharding (%d)", self.adapter.name, len(self._insts))
            finally:
                for t in tasks:
                    t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                self._connected.clear()

    async def _run_conn(self, conn_id: int, insts: list[str]) -> None:
        backoff = Backoff()
        a = self.adapter
        while not self._stop:
            opened = None
            loop = asyncio.get_running_loop()
            try:
                async with self._session_factory() as session:
                    async with session.ws_connect(a.url, heartbeat=a.heartbeat) as ws:
                        for m in a.subscribe(insts):
                            await _send(ws, m)
                        pinger = None
                        if a.ping:
                            interval, msg = a.ping

                            async def _pinger():
                                while True:
                                    await asyncio.sleep(interval)
                                    await _send(ws, msg)
                            pinger = asyncio.create_task(_pinger())
                        self._connected.add(conn_id)
                        opened = loop.time()
                        state: dict = {}
                        try:
                            async for msg in ws:
                                if msg.type != aiohttp.WSMsgType.TEXT:
                                    if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                        break
                                    continue
                                data = msg.data
                                if a.text_ping_reply and data == a.text_ping_reply[0]:
                                    await ws.send_str(a.text_ping_reply[1])
                                    continue
                                try:
                                    raw = json.loads(data)
                                except ValueError:
                                    continue
                                if not isinstance(raw, dict):
                                    continue
                                items = a.parse(raw, state)
                                if items:
                                    self.on_items(items)
                        finally:
                            if pinger:
                                pinger.cancel()
                        log.info("%s conn %d closed by server (%d insts, lived %.0fs)",
                                 a.name, conn_id, len(insts), loop.time() - opened)
            except asyncio.CancelledError:
                self._connected.discard(conn_id)
                raise
            except Exception as e:  # noqa: BLE001 — any transport error → reconnect with backoff
                log.warning("%s conn %d error: %r", a.name, conn_id, e)
            self._connected.discard(conn_id)
            uptime = (loop.time() - opened) if opened is not None else 0.0
            await self._sleep(backoff.next(uptime))
