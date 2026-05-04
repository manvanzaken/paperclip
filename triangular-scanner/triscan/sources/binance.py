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


def _book_from_snapshot(exchange: str, symbol: str, snap: dict) -> Book:
    bids = [BookLevel(Decimal(p), Decimal(q)) for p, q in snap["bids"]]
    asks = [BookLevel(Decimal(p), Decimal(q)) for p, q in snap["asks"]]
    bids.sort(key=lambda l: l.price, reverse=True)
    asks.sort(key=lambda l: l.price)
    return Book(exchange=exchange, symbol=symbol, bids=bids, asks=asks,
                ts_ms=int(snap.get("E", 0)) or 0, seq=int(snap["lastUpdateId"]))


def _apply_diff(book: Book, diff: dict) -> None:
    """Apply a single Binance depthUpdate event in place."""
    for raw, levels, side in (
        (diff.get("b", []), book.bids, "bid"),
        (diff.get("a", []), book.asks, "ask"),
    ):
        for p_str, q_str in raw:
            price = Decimal(p_str); size = Decimal(q_str)
            idx = next((i for i, lv in enumerate(levels) if lv.price == price), -1)
            if size == 0:
                if idx >= 0:
                    levels.pop(idx)
            else:
                if idx >= 0:
                    levels[idx] = BookLevel(price, size)
                else:
                    levels.append(BookLevel(price, size))
        if side == "bid":
            levels.sort(key=lambda l: l.price, reverse=True)
        else:
            levels.sort(key=lambda l: l.price)
    book.seq = int(diff["u"])
    if "E" in diff:
        book.ts_ms = int(diff["E"])


@dataclass
class BinanceSource(Source):
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

    async def fetch_markets(self) -> List[Market]:
        info = await self._get_json("/api/v3/exchangeInfo")
        active = [
            s for s in info.get("symbols", [])
            if s.get("status") == "TRADING" and s.get("isSpotTradingAllowed")
        ]
        vol = await self._get_json("/api/v3/ticker/24hr")
        vol_by = {t["symbol"]: float(t.get("quoteVolume", "0")) for t in vol}
        out = []
        for s in active:
            std = f"{s['baseAsset']}/{s['quoteAsset']}"
            out.append(Market(symbol=std, base=s["baseAsset"], quote=s["quoteAsset"],
                              volume_24h_usd=vol_by.get(s["symbol"], 0.0)))
        return out

    @staticmethod
    def _to_native(symbol: str) -> str:
        return symbol.replace("/", "")

    async def fetch_tickers(self, symbols: Iterable[str]) -> Dict[str, Quote]:
        rows = await self._get_json("/api/v3/ticker/bookTicker")
        wanted = {self._to_native(s): s for s in symbols}
        ts = int(time.time() * 1000)
        out: Dict[str, Quote] = {}
        for r in rows:
            if r["symbol"] in wanted:
                std = wanted[r["symbol"]]
                try:
                    out[std] = Quote(self.name, std, Decimal(r["bidPrice"]), Decimal(r["askPrice"]), ts)
                except Exception:
                    continue
        return out

    async def subscribe_book(self, symbol: str, on_update, on_failure=None):
        """Maintain a live L2 book for `symbol`. Calls `on_update(book)` after each accepted diff."""
        native = self._to_native(symbol).lower()
        stream_url = f"{self.ws_url.rstrip('/')}/{native}@depth@100ms"

        cancelled = asyncio.Event()

        async def runner():
            while not cancelled.is_set():
                try:
                    async with websockets.connect(stream_url, ping_interval=20, ping_timeout=10) as ws:
                        buffered: list[dict] = []
                        snap_task = asyncio.create_task(self._get_json(
                            "/api/v3/depth", params={"symbol": self._to_native(symbol), "limit": 1000},
                        ))
                        while not snap_task.done():
                            try:
                                msg = await asyncio.wait_for(ws.recv(), timeout=0.05)
                                buffered.append(_json.loads(msg))
                            except asyncio.TimeoutError:
                                continue
                        snap = await snap_task
                        book = _book_from_snapshot(self.name, symbol, snap)
                        last_u = book.seq

                        def _process(evt: dict) -> bool:
                            nonlocal last_u
                            if evt.get("u", -1) <= last_u:
                                return True  # stale, skip
                            if last_u == book.seq and not (evt["U"] <= last_u + 1 <= evt["u"]):
                                return False
                            if "pu" in evt and evt["pu"] != last_u:
                                return False
                            _apply_diff(book, evt)
                            last_u = book.seq
                            return True

                        for evt in buffered:
                            if not _process(evt):
                                raise RuntimeError("buffered event sequence gap; resnapshotting")
                        await on_update(book)

                        while not cancelled.is_set():
                            msg = await ws.recv()
                            evt = _json.loads(msg)
                            if not _process(evt):
                                log.warning("binance %s sequence gap, resnapshotting", symbol)
                                break
                            await on_update(book)
                except Exception as e:
                    if cancelled.is_set():
                        return
                    log.warning("binance ws %s error: %s — reconnecting in 1s", symbol, e)
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
