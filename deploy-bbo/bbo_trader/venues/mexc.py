"""MEXC USDT-M contract venue — public side: BBO from `sub.depth.full limit=5` (top level only),
REST specs / volumes / funding / depth fallback. Plan 2 adds MexcPrivate and MexcTrading here."""
from __future__ import annotations

import time
from typing import Callable, Iterable

import aiohttp

from ..config import VenueConfig
from ..models import BBO, VenueSpec
from .ws import WSAdapter, WSRunner

NAME = "mexc"
WS_URL = "wss://contract.mexc.com/edge"
REST = "https://contract.mexc.com"
HEADERS = {"User-Agent": "Mozilla/5.0 (bbo-trader/1.0)"}   # Cloudflare rejects requests without a UA


def to_instrument(symbol: str) -> str:
    return f"{symbol[:-4]}_USDT"


def to_symbol(instrument: str) -> str:
    return instrument.replace("_", "")


def subscribe(insts: list[str]) -> list[dict]:
    return [{"method": "sub.depth.full", "param": {"symbol": i, "limit": 5}} for i in insts]


def parse_depth(raw: dict, contract_size: dict[str, float], now: float) -> list[BBO]:
    """`push.depth.full` → one BBO from the top levels. Levels are [price, contracts, order_count]."""
    if raw.get("channel") != "push.depth.full":
        return []
    d = raw.get("data") or {}
    bids, asks = d.get("bids") or [], d.get("asks") or []
    if not bids or not asks:
        return []
    inst = str(raw.get("symbol", ""))
    return [BBO(NAME, to_symbol(inst), float(bids[0][0]), float(bids[0][1]), float(asks[0][0]),
                float(asks[0][1]), float(raw.get("ts") or 0) / 1000.0, now, contract_size.get(inst, 1.0))]


def parse_depth_rest(raw: dict, inst: str, contract_size: float, now: float) -> BBO | None:
    d = raw.get("data") or {}
    bids, asks = d.get("bids") or [], d.get("asks") or []
    if not bids or not asks:
        return None
    ts = float(d.get("timestamp") or d.get("ts") or 0) / 1000.0
    return BBO(NAME, to_symbol(inst), float(bids[0][0]), float(bids[0][1]), float(asks[0][0]),
               float(asks[0][1]), ts, now, contract_size)


def parse_specs(raw: dict) -> dict[str, VenueSpec]:
    """`/api/v1/contract/detail` → specs for enabled (state 0) USDT contracts."""
    out: dict[str, VenueSpec] = {}
    for c in raw.get("data") or []:
        if c.get("quoteCoin") != "USDT" or int(c.get("state", 0) or 0) != 0:
            continue
        inst = c.get("symbol") or ""
        sym = to_symbol(inst)
        if not inst or not sym.endswith("USDT"):
            continue
        out[sym] = VenueSpec(NAME, sym, inst, float(c.get("contractSize") or 1), float(c.get("volUnit") or 1),
                             float(c.get("minVol") or 1), float(c.get("priceUnit") or 0.0001))
    return out


def parse_tickers(raw: dict) -> dict[str, float]:
    """`/api/v1/contract/ticker` → 24 h turnover in USDT per symbol (`amount24`)."""
    return {to_symbol(t["symbol"]): float(t.get("amount24") or 0)
            for t in raw.get("data") or [] if str(t.get("symbol", "")).endswith("_USDT")}


def parse_funding(raw: dict) -> dict[str, tuple[float, float]]:
    """`/api/v1/contract/funding_rate` → symbol -> (rate, next settlement ts seconds)."""
    out = {}
    for f in raw.get("data") or []:
        inst = str(f.get("symbol", ""))
        if not inst.endswith("_USDT"):
            continue
        out[to_symbol(inst)] = (float(f.get("fundingRate") or 0), float(f.get("nextSettleTime") or 0) / 1000.0)
    return out


class MexcPublic:
    def __init__(self, cfg: VenueConfig, on_bbo: Callable[[BBO], None], clock=time.time,
                 session_factory=aiohttp.ClientSession):
        self.cfg = cfg
        self.on_bbo = on_bbo
        self.clock = clock
        self.contract_size: dict[str, float] = {}
        self._runner = WSRunner(
            WSAdapter(name=NAME, url=WS_URL, subscribe=subscribe, parse=self._parse,
                      max_topics=cfg.max_topics, ping=(15.0, {"method": "ping"})),
            on_items=self._emit, session_factory=session_factory)

    def _parse(self, raw: dict, state: dict) -> list[BBO]:
        return parse_depth(raw, self.contract_size, self.clock())

    def _emit(self, items: list[BBO]) -> None:
        for b in items:
            self.on_bbo(b)

    def set_specs(self, specs: dict[str, VenueSpec]) -> None:
        self.contract_size = {s.instrument: s.contract_size for s in specs.values()}

    def set_symbols(self, symbols: Iterable[str]) -> None:
        self._runner.set_instruments(to_instrument(s) for s in symbols)

    @property
    def connected(self) -> bool:
        return self._runner.connected

    async def run(self) -> None:
        await self._runner.run()


class MexcMarket:
    def __init__(self, session: aiohttp.ClientSession, clock=time.time, specs: dict[str, VenueSpec] | None = None):
        self.session = session
        self.clock = clock
        self.specs = specs or {}

    async def _get(self, path: str, timeout: float = 10.0) -> dict:
        async with self.session.get(REST + path, headers=HEADERS,
                                    timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            return await r.json(content_type=None)

    async def fetch_specs(self) -> dict[str, VenueSpec]:
        self.specs = parse_specs(await self._get("/api/v1/contract/detail"))
        return self.specs

    async def fetch_volumes(self) -> dict[str, float]:
        return parse_tickers(await self._get("/api/v1/contract/ticker"))

    async def fetch_funding(self) -> dict[str, tuple[float, float]]:
        return parse_funding(await self._get("/api/v1/contract/funding_rate"))

    async def fetch_bbo(self, symbol: str) -> BBO | None:
        inst = to_instrument(symbol)
        spec = self.specs.get(symbol)
        raw = await self._get(f"/api/v1/contract/depth/{inst}?limit=5", timeout=5.0)
        return parse_depth_rest(raw, inst, spec.contract_size if spec else 1.0, self.clock())