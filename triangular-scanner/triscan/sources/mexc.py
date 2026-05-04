from __future__ import annotations
import asyncio
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Awaitable, Callable, Dict, Iterable, List, Optional

import aiohttp

from .base import Source
from ..enumerator import Market
from ..models import Quote, Book


@dataclass
class MexcSource(Source):
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
        active = [s for s in info.get("symbols", []) if str(s.get("status")) == "1"]
        tickers = await self._get_json("/api/v3/ticker/24hr")
        vol_by_sym = {t["symbol"]: float(t.get("quoteVolume", "0")) for t in tickers}
        out: List[Market] = []
        for s in active:
            mexc_sym = s["symbol"]
            base = s["baseAsset"]
            quote = s["quoteAsset"]
            std = f"{base}/{quote}"
            out.append(Market(symbol=std, base=base, quote=quote, volume_24h_usd=vol_by_sym.get(mexc_sym, 0.0)))
        return out

    @staticmethod
    def _to_native(symbol: str) -> str:
        return symbol.replace("/", "")

    async def fetch_tickers(self, symbols: Iterable[str]) -> Dict[str, Quote]:
        rows = await self._get_json("/api/v3/ticker/bookTicker")
        wanted = {self._to_native(s): s for s in symbols}
        ts = int(time.time() * 1000)
        out: Dict[str, Quote] = {}
        for row in rows:
            native = row["symbol"]
            if native in wanted:
                std = wanted[native]
                try:
                    out[std] = Quote(
                        exchange=self.name, symbol=std,
                        bid=Decimal(row["bidPrice"]), ask=Decimal(row["askPrice"]),
                        ts_ms=ts,
                    )
                except Exception:
                    continue
        return out

    async def subscribe_book(self, symbol, on_update):
        raise NotImplementedError("WS not implemented in Phase 2")

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
