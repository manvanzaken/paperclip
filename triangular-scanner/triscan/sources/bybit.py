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
class BybitSource(Source):
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
        return symbol.replace("/", "")

    async def fetch_markets(self) -> List[Market]:
        info = await self._get_json("/v5/market/instruments-info", params={"category": "spot"})
        active = [s for s in info["result"]["list"] if s.get("status") == "Trading"]
        tickers = await self._get_json("/v5/market/tickers", params={"category": "spot"})
        vol_by = {t["symbol"]: float(t.get("turnover24h", "0")) for t in tickers["result"]["list"]}
        out = []
        for s in active:
            std = f"{s['baseCoin']}/{s['quoteCoin']}"
            out.append(Market(symbol=std, base=s["baseCoin"], quote=s["quoteCoin"],
                              volume_24h_usd=vol_by.get(s["symbol"], 0.0)))
        return out

    async def fetch_tickers(self, symbols: Iterable[str]) -> Dict[str, Quote]:
        rows = await self._get_json("/v5/market/tickers", params={"category": "spot"})
        wanted = {self._to_native(s): s for s in symbols}
        ts = int(time.time() * 1000)
        out: Dict[str, Quote] = {}
        for r in rows["result"]["list"]:
            if r["symbol"] in wanted:
                std = wanted[r["symbol"]]
                try:
                    out[std] = Quote(self.name, std, Decimal(r["bid1Price"]), Decimal(r["ask1Price"]), ts)
                except Exception:
                    continue
        return out

    async def subscribe_book(self, symbol: str, on_update):
        native = self._to_native(symbol)
        topic = f"orderbook.50.{native}"
        cancelled = asyncio.Event()

        async def runner():
            while not cancelled.is_set():
                try:
                    async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=10) as ws:
                        await ws.send(_json.dumps({"op": "subscribe", "args": [topic]}))
                        book = None
                        while not cancelled.is_set():
                            msg = _json.loads(await ws.recv())
                            if msg.get("topic") != topic:
                                continue
                            if msg.get("type") == "snapshot":
                                book = _book_from_bybit_snapshot(self.name, symbol, msg)
                                await on_update(book)
                            elif msg.get("type") == "delta" and book is not None:
                                new_u = int(msg["data"]["u"])
                                if new_u != book.seq + 1:
                                    log.warning("bybit %s seq gap (%s vs %s) — re-subscribe", symbol, new_u, book.seq)
                                    break
                                _apply_bybit_delta(book, msg)
                                await on_update(book)
                except Exception as e:
                    if cancelled.is_set():
                        return
                    log.warning("bybit ws %s error: %s", symbol, e)
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


def _book_from_bybit_snapshot(exchange: str, symbol: str, msg: dict) -> Book:
    d = msg["data"]
    bids = [BookLevel(Decimal(p), Decimal(q)) for p, q in d.get("b", [])]
    asks = [BookLevel(Decimal(p), Decimal(q)) for p, q in d.get("a", [])]
    bids.sort(key=lambda l: l.price, reverse=True)
    asks.sort(key=lambda l: l.price)
    return Book(exchange=exchange, symbol=symbol, bids=bids, asks=asks,
                ts_ms=int(msg.get("ts", 0)), seq=int(d.get("u", 0)))


def _apply_bybit_delta(book: Book, msg: dict) -> None:
    d = msg["data"]
    for raw, levels in ((d.get("b", []), book.bids), (d.get("a", []), book.asks)):
        for p_str, q_str in raw:
            price = Decimal(p_str)
            size = Decimal(q_str)
            idx = next((i for i, lv in enumerate(levels) if lv.price == price), -1)
            if size == 0:
                if idx >= 0:
                    levels.pop(idx)
            else:
                if idx >= 0:
                    levels[idx] = BookLevel(price, size)
                else:
                    levels.append(BookLevel(price, size))
    book.bids.sort(key=lambda l: l.price, reverse=True)
    book.asks.sort(key=lambda l: l.price)
    book.seq = int(d["u"])
    book.ts_ms = int(msg.get("ts", book.ts_ms))
