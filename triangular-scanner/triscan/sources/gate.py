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
class GateSource(Source):
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
        return symbol.replace("/", "_")

    async def fetch_markets(self) -> List[Market]:
        pairs = await self._get_json("/api/v4/spot/currency_pairs")
        active = [p for p in pairs if p.get("trade_status") == "tradable"]
        tickers = await self._get_json("/api/v4/spot/tickers")
        vol_by = {t["currency_pair"]: float(t.get("quote_volume", "0")) for t in tickers}
        out = []
        for p in active:
            std = f"{p['base']}/{p['quote']}"
            out.append(Market(symbol=std, base=p["base"], quote=p["quote"],
                              volume_24h_usd=vol_by.get(p["id"], 0.0)))
        return out

    async def fetch_tickers(self, symbols: Iterable[str]) -> Dict[str, Quote]:
        rows = await self._get_json("/api/v4/spot/tickers")
        wanted = {self._to_native(s): s for s in symbols}
        ts = int(time.time() * 1000)
        out: Dict[str, Quote] = {}
        for r in rows:
            cp = r.get("currency_pair")
            if cp in wanted:
                std = wanted[cp]
                try:
                    out[std] = Quote(self.name, std, Decimal(r["highest_bid"]), Decimal(r["lowest_ask"]), ts)
                except Exception:
                    continue
        return out

    async def subscribe_book(self, symbol: str, on_update, on_failure=None):
        native = self._to_native(symbol)
        cancelled = asyncio.Event()

        async def runner():
            while not cancelled.is_set():
                try:
                    async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=10) as ws:
                        sub = {"time": int(time.time()), "channel": "spot.order_book",
                               "event": "subscribe", "payload": [native, "20", "100ms"]}
                        await ws.send(_json.dumps(sub))
                        while not cancelled.is_set():
                            msg = _json.loads(await ws.recv())
                            if msg.get("channel") != "spot.order_book" or msg.get("event") != "update":
                                continue
                            book = _book_from_gate_snapshot(self.name, symbol, msg["result"])
                            await on_update(book)
                except Exception as e:
                    if cancelled.is_set():
                        return
                    log.warning("gate ws %s error: %s", symbol, e)
                    if on_failure is not None:
                        try:
                            await on_failure()
                        except Exception:
                            pass
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


def _book_from_gate_snapshot(exchange: str, symbol: str, result: dict) -> Book:
    bids = [BookLevel(Decimal(p), Decimal(q)) for p, q in result.get("bids", [])]
    asks = [BookLevel(Decimal(p), Decimal(q)) for p, q in result.get("asks", [])]
    bids.sort(key=lambda l: l.price, reverse=True)
    asks.sort(key=lambda l: l.price)
    return Book(exchange=exchange, symbol=symbol, bids=bids, asks=asks,
                ts_ms=int(result.get("t", 0)), seq=int(result.get("lastUpdateId", 0)))
