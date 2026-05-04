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

    async def subscribe_book(self, symbol, on_update):
        raise NotImplementedError("WS implemented in Phase 5")

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
