from __future__ import annotations
import asyncio
import json as _json
import logging
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Awaitable, Callable, Dict, Iterable, List, Optional

import aiohttp
import websockets

from .base import Source
from ..enumerator import Market
from ..models import Quote, Book, BookLevel

log = logging.getLogger(__name__)


@dataclass
class KucoinSource(Source):
    _session: Optional[aiohttp.ClientSession] = field(default=None, init=False, repr=False)

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def _get_json(self, path: str, params: Optional[dict] = None):
        sess = await self._ensure_session()
        url = self.rest_url.rstrip("/") + path
        async with sess.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as r:
            r.raise_for_status()
            return await r.json()

    @staticmethod
    def _to_native(symbol: str) -> str:
        return symbol.replace("/", "-")

    async def fetch_markets(self) -> List[Market]:
        sym = await self._get_json("/api/v2/symbols")
        active = [s for s in sym["data"] if s.get("enableTrading")]
        stats = await self._get_json("/api/v1/market/allTickers")
        vol_by = {t["symbol"]: float(t.get("volValue", "0")) for t in stats["data"]["ticker"]}
        out = []
        for s in active:
            std = f"{s['baseCurrency']}/{s['quoteCurrency']}"
            out.append(Market(symbol=std, base=s["baseCurrency"], quote=s["quoteCurrency"],
                              volume_24h_usd=vol_by.get(s["symbol"], 0.0)))
        return out

    async def fetch_tickers(self, symbols: Iterable[str]) -> Dict[str, Quote]:
        stats = await self._get_json("/api/v1/market/allTickers")
        wanted = {self._to_native(s): s for s in symbols}
        ts = int(time.time() * 1000)
        out: Dict[str, Quote] = {}
        for r in stats["data"]["ticker"]:
            if r["symbol"] in wanted:
                std = wanted[r["symbol"]]
                bid = r.get("buy"); ask = r.get("sell")
                if bid is None or ask is None:
                    continue
                try:
                    out[std] = Quote(self.name, std, Decimal(bid), Decimal(ask), ts)
                except Exception:
                    continue
        return out

    async def _get_ws_endpoint(self):
        sess = await self._ensure_session()
        async with sess.post(self.rest_url.rstrip("/") + "/api/v1/bullet-public") as r:
            r.raise_for_status()
            body = await r.json()
        server = body["data"]["instanceServers"][0]
        token = body["data"]["token"]
        return f"{server['endpoint']}?token={token}"

    async def subscribe_book(self, symbol: str, on_update):
        native = self._to_native(symbol)
        cancelled = asyncio.Event()

        async def runner():
            while not cancelled.is_set():
                try:
                    ws_url = await self._get_ws_endpoint()
                    async with websockets.connect(ws_url, ping_interval=20, ping_timeout=10) as ws:
                        welcome = _json.loads(await ws.recv())
                        if welcome.get("type") != "welcome":
                            log.warning("kucoin: no welcome got %s", welcome)
                        sub_id = str(int(time.time() * 1000))
                        await ws.send(_json.dumps({
                            "id": sub_id, "type": "subscribe",
                            "topic": f"/market/level2:{native}", "response": True,
                        }))

                        buffered: list[dict] = []
                        snap_task = asyncio.create_task(self._get_json(
                            "/api/v3/market/orderbook/level2", params={"symbol": native},
                        ))
                        while not snap_task.done():
                            try:
                                msg = await asyncio.wait_for(ws.recv(), timeout=0.05)
                                data = _json.loads(msg)
                                if data.get("subject") == "trade.l2update":
                                    buffered.append(data["data"])
                            except asyncio.TimeoutError:
                                continue
                        snap = await snap_task
                        book = _book_from_kucoin_snapshot(self.name, symbol, snap["data"])

                        bootstrapped = False
                        for evt in buffered:
                            if int(evt["sequenceEnd"]) <= book.seq:
                                continue
                            if not bootstrapped and not (int(evt["sequenceStart"]) <= book.seq + 1 <= int(evt["sequenceEnd"])):
                                raise RuntimeError("kucoin bootstrap gap")
                            _apply_kucoin_diff(book, evt)
                            bootstrapped = True
                        await on_update(book)

                        while not cancelled.is_set():
                            msg = _json.loads(await ws.recv())
                            if msg.get("subject") != "trade.l2update":
                                continue
                            evt = msg["data"]
                            if int(evt["sequenceStart"]) > book.seq + 1:
                                log.warning("kucoin %s seq gap (%s vs %s); resnapshotting",
                                            symbol, evt["sequenceStart"], book.seq)
                                break
                            _apply_kucoin_diff(book, evt)
                            await on_update(book)
                except Exception as e:
                    if cancelled.is_set():
                        return
                    log.warning("kucoin ws %s error: %s", symbol, e)
                    await asyncio.sleep(1.0)

        task = asyncio.create_task(runner())

        async def cancel():
            cancelled.set()
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        return cancel

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()


def _book_from_kucoin_snapshot(exchange: str, symbol: str, data: dict) -> Book:
    bids = [BookLevel(Decimal(p), Decimal(q)) for p, q in data.get("bids", [])]
    asks = [BookLevel(Decimal(p), Decimal(q)) for p, q in data.get("asks", [])]
    bids.sort(key=lambda l: l.price, reverse=True)
    asks.sort(key=lambda l: l.price)
    return Book(exchange=exchange, symbol=symbol, bids=bids, asks=asks,
                ts_ms=int(time.time() * 1000), seq=int(data["sequence"]))


def _apply_kucoin_diff(book: Book, data: dict) -> None:
    changes = data.get("changes", {})
    for side_key, levels in (("bids", book.bids), ("asks", book.asks)):
        for change in changes.get(side_key, []):
            price = Decimal(change[0])
            size = Decimal(change[1])
            idx = next((i for i, lv in enumerate(levels) if lv.price == price), -1)
            if size == 0:
                if idx >= 0:
                    levels.pop(idx)
            else:
                if idx >= 0:
                    levels[idx] = BookLevel(price, size)
                else:
                    levels.append(BookLevel(price, size))
        if side_key == "bids":
            levels.sort(key=lambda l: l.price, reverse=True)
        else:
            levels.sort(key=lambda l: l.price)
    book.seq = int(data["sequenceEnd"])
    book.ts_ms = int(time.time() * 1000)
