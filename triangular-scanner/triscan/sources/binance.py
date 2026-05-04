from __future__ import annotations
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Awaitable, Callable, Dict, Iterable, List, Optional

import aiohttp

from .base import Source
from ..enumerator import Market
from ..models import Quote, Book


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

    async def subscribe_book(self, symbol, on_update):
        raise NotImplementedError("WS implemented in Phase 4")

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
