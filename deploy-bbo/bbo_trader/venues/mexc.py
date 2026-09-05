"""MEXC USDT-M contract venue — public side: BBO from `sub.depth.full limit=5` (top level only),
REST specs / volumes / funding / depth fallback. Plan 2 adds MexcPrivate and MexcTrading here.

Verified against the live API on 2026-09-05: errors come back as HTTP 200 + {"success": false, "code",
"message"} (so `_get` checks the envelope, not just the status); `state` is 0 for every contract while
`apiAllowed` is false for ~30 live ones whose orders the API rejects; contract sizes span 1e-5 … 1e7, so an
instrument without a known size is dropped rather than given a default; `collectCycle` is 4 h for about
half the book; a bad subscription answers `{"channel": "rs.error", ...}` on an otherwise healthy socket."""
from __future__ import annotations

import json
import logging
import math
import time
from typing import Callable, Iterable

import aiohttp

from ..config import VenueConfig
from ..models import BBO, VenueSpec
from .base import VenueError
from .ws import WSAdapter, WSRunner

log = logging.getLogger("bbo.mexc")

NAME = "mexc"
WS_URL = "wss://contract.mexc.com/edge"
REST = "https://contract.mexc.com"
HEADERS = {"User-Agent": "Mozilla/5.0 (bbo-trader/1.0)"}   # Cloudflare rejects requests without a UA


def to_instrument(symbol: str) -> str:
    if len(symbol) <= 4 or not symbol.endswith("USDT"):
        raise ValueError(f"mexc: not a USDT symbol: {symbol!r}")
    return f"{symbol[:-4]}_USDT"


def to_symbol(instrument: str) -> str:
    return instrument.replace("_", "")


def subscribe(insts: list[str]) -> list[dict]:
    return [{"method": "sub.depth.full", "param": {"symbol": i, "limit": 5}} for i in insts]


def _num(x: object) -> float:
    """Strict numeric field: missing/empty/non-finite raise so the caller drops the row instead of guessing."""
    if x is None or x == "":
        raise ValueError("missing numeric field")
    f = float(x)
    if not math.isfinite(f):
        raise ValueError("non-finite numeric field")
    return f


def _top(d: dict, inst: str, contract_size: float, ts_ms: object, now: float) -> BBO | None:
    bids, asks = d.get("bids") or [], d.get("asks") or []
    if not bids or not asks:
        return None
    return BBO(NAME, to_symbol(inst), float(bids[0][0]), float(bids[0][1]), float(asks[0][0]), float(asks[0][1]),
               float(ts_ms or 0) / 1000.0, now, contract_size)


def parse_depth(raw: dict, contract_size: dict[str, float], now: float) -> list[BBO]:
    """`push.depth.full` → one BBO from the top levels. Levels are [price, contracts, order_count]. An
    instrument without a known contract size yields nothing: touch_notional would be wrong by up to 1e4×."""
    if raw.get("channel") != "push.depth.full":
        return []
    d = raw.get("data")
    if not isinstance(d, dict):
        return []
    inst = str(raw.get("symbol", ""))
    cs = contract_size.get(inst)
    if cs is None:
        return []
    b = _top(d, inst, cs, d.get("cts") or raw.get("ts"), now)     # book time when present, push time otherwise
    return [b] if b is not None else []


def parse_depth_rest(raw: dict, inst: str, contract_size: float, now: float) -> BBO | None:
    d = raw.get("data")
    if not isinstance(d, dict):
        return None
    return _top(d, inst, contract_size, d.get("timestamp") or d.get("ts"), now)


def parse_specs(raw: dict) -> dict[str, VenueSpec]:
    """`/api/v1/contract/detail` → specs for enabled, API-tradable USDT contracts. One malformed row costs
    that row, never the refresh."""
    out: dict[str, VenueSpec] = {}
    rows = raw.get("data") or []
    dropped = 0
    for c in rows:
        try:
            if c.get("quoteCoin") != "USDT" or int(c.get("state", 0) or 0) != 0 or not c.get("apiAllowed", True):
                continue
            inst = str(c.get("symbol") or "")
            if not inst.endswith("_USDT"):
                continue
            sym = to_symbol(inst)
            out[sym] = VenueSpec(NAME, sym, inst, _num(c.get("contractSize")), _num(c.get("volUnit")),
                                 _num(c.get("minVol")), _num(c.get("priceUnit")))
        except (TypeError, ValueError, AttributeError):
            dropped += 1
    if dropped:
        log.warning("SPEC_ROWS_DROPPED mexc %d of %d", dropped, len(rows))
    return out


def parse_tickers(raw: dict) -> dict[str, float]:
    """`/api/v1/contract/ticker` → 24 h turnover in USDT per symbol (`amount24`; `volume24` is contracts)."""
    out: dict[str, float] = {}
    for t in raw.get("data") or []:
        try:
            inst = str(t.get("symbol", ""))
            if not inst.endswith("_USDT"):
                continue
            out[to_symbol(inst)] = _num(t.get("amount24"))
        except (TypeError, ValueError, AttributeError):
            continue
    return out


def parse_funding(raw: dict) -> dict[str, tuple[float, float]]:
    """`/api/v1/contract/funding_rate` → symbol -> (rate FRACTION, next settlement unix SECONDS)."""
    out: dict[str, tuple[float, float]] = {}
    for f in raw.get("data") or []:
        try:
            inst = str(f.get("symbol", ""))
            if not inst.endswith("_USDT"):
                continue
            out[to_symbol(inst)] = (_num(f.get("fundingRate")), _num(f.get("nextSettleTime")) / 1000.0)
        except (TypeError, ValueError, AttributeError):
            continue
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
        if raw.get("channel") == "rs.error":        # a rejected topic on an otherwise healthy socket
            n = state["errors"] = state.get("errors", 0) + 1
            if n in (1, 10, 100) or n % 1000 == 0:
                log.warning("mexc rs.error #%d: %.200s", n, raw.get("data"))
            return []
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
        """GET + envelope check: MEXC reports errors as HTTP 200 + {"success": false}; a swallowed error would
        look like an empty market (no contracts, no volumes) to the caller."""
        async with self.session.get(REST + path, headers=HEADERS,
                                    timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            body = await r.text()
            if r.status != 200:
                raise VenueError(f"mexc {path} HTTP {r.status}: {body[:200]}")
        try:
            d = json.loads(body)
        except ValueError as e:
            raise VenueError(f"mexc {path} non-JSON body: {body[:200]}") from e
        if not isinstance(d, dict) or d.get("success") is not True:
            raise VenueError(f"mexc {path} error envelope: {str(d)[:200]}")
        return d

    async def fetch_specs(self) -> dict[str, VenueSpec]:
        specs = parse_specs(await self._get("/api/v1/contract/detail"))
        if not specs:
            raise VenueError("mexc contract/detail returned no usable contracts")   # never hand out an empty universe
        self.specs = specs
        return self.specs

    async def fetch_volumes(self) -> dict[str, float]:
        return parse_tickers(await self._get("/api/v1/contract/ticker"))

    async def fetch_funding(self) -> dict[str, tuple[float, float]]:
        return parse_funding(await self._get("/api/v1/contract/funding_rate"))

    async def fetch_bbo(self, symbol: str) -> BBO | None:
        spec = self.specs.get(symbol)
        if spec is None:
            return None                              # no spec → no contract size → no honest quote
        raw = await self._get(f"/api/v1/contract/depth/{spec.instrument}?limit=5", timeout=5.0)
        return parse_depth_rest(raw, spec.instrument, spec.contract_size, self.clock())
