"""BloFin USDT swaps venue — public side: BBO from `books5` snapshots (top level only), REST
instruments / tickers / funding / books fallback. Plan 2 adds BlofinPrivate and BlofinTrading here.

Shapes as captured live (SpreadWatch 2026-08-23, re-verified 2026-09-05): books5 `data` is a DICT on the
WS and a one-element LIST on REST, prices/sizes are strings, keepalive is a bare text `ping` answered with
`pong`. Errors come back as HTTP 200 + {"code": "152002", "msg": ...} with no `data` (so `_get` checks
`code == "0"`). A subscribe MESSAGE is validated atomically: one unknown instId answers {"event": "error",
"code": "60012", ...} and subscribes NONE of the other args in that message while the socket stays open —
hence one message per instrument. `fundingTime` is the UPCOMING settlement and `fundingInterval` is 4 or
8 hours (three 1 h contracts); every instrument is `state: live` today (delistings simply vanish from the
list), `expireTime` is a year-2124 sentinel, contract values span 1e-4 … 1e7, and the USDT set includes
equity/index/commodity perps (`assetClass`), which are excluded: their underlying markets close."""
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

log = logging.getLogger("bbo.blofin")

NAME = "blofin"
WS_URL = "wss://openapi.blofin.com/ws/public"
REST = "https://openapi.blofin.com"
HEADERS = {"User-Agent": "Mozilla/5.0 (bbo-trader/1.0)"}


def to_instrument(symbol: str) -> str:
    if len(symbol) <= 4 or not symbol.endswith("USDT"):
        raise ValueError(f"blofin: not a USDT symbol: {symbol!r}")
    return f"{symbol[:-4]}-USDT"


def to_symbol(instrument: str) -> str:
    return instrument.replace("-", "")


def subscribe(insts: list[str]) -> list[dict]:
    """One message per instrument (verified live 2026-09-05): BloFin validates a subscribe message atomically,
    so a single delisted instId in a batched message would black out the whole shard — silently, because
    our own text pings keep the empty socket alive."""
    return [{"op": "subscribe", "args": [{"channel": "books5", "instId": i}]} for i in insts]


def _num(x: object) -> float:
    """Strict numeric field: missing/empty/non-finite raise so the caller drops the row instead of guessing."""
    if x is None or x == "":
        raise ValueError("missing numeric field")
    f = float(x)
    if not math.isfinite(f):
        raise ValueError("non-finite numeric field")
    return f


def _rows(data) -> list[dict]:
    if isinstance(data, dict):
        return [data]
    return [d for d in (data or []) if isinstance(d, dict)]


def _top(d: dict, inst: str, contract_size: float, now: float) -> BBO | None:
    bids, asks = d.get("bids") or [], d.get("asks") or []
    if not bids or not asks:
        return None
    return BBO(NAME, to_symbol(inst), _num(bids[0][0]), _num(bids[0][1]), _num(asks[0][0]), _num(asks[0][1]),
               float(d.get("ts") or 0) / 1000.0, now, contract_size)


def parse_books5(raw: dict, contract_size: dict[str, float], now: float) -> list[BBO]:
    """books5 push → one BBO per row. An instrument without a known contract size yields nothing. books5 is
    snapshot-only; the sibling `books` channel sends `action: "update"` deltas with partial sides, so anything
    but a snapshot is ignored rather than read as a top of book."""
    if (raw.get("arg") or {}).get("channel") != "books5" or "data" not in raw:
        return []
    if raw.get("action") not in (None, "snapshot"):
        return []
    inst = str(raw["arg"].get("instId", ""))
    cs = contract_size.get(inst)
    if cs is None:
        return []
    out = []
    for d in _rows(raw["data"]):
        b = _top(d, inst, cs, now)
        if b is not None:
            out.append(b)
    return out


def parse_books_rest(raw: dict, inst: str, contract_size: float, now: float) -> BBO | None:
    rows = _rows(raw.get("data"))
    return _top(rows[0], inst, contract_size, now) if rows else None


def parse_instruments(raw: dict) -> dict[str, VenueSpec]:
    """`/api/v1/market/instruments` → specs for live USDT swaps. One malformed row costs that row only."""
    out: dict[str, VenueSpec] = {}
    rows = raw.get("data") or []
    dropped = 0
    for c in rows:
        try:
            inst = str(c.get("instId", ""))
            if not inst.endswith("-USDT") or str(c.get("state", "live")) != "live":
                continue
            if c.get("contractType", "linear") != "linear" or c.get("assetClass", "Crypto") != "Crypto":
                continue        # inverse contracts size the other way; equity/commodity perps gap when their market is closed
            sym = to_symbol(inst)
            out[sym] = VenueSpec(NAME, sym, inst, _num(c.get("contractValue")), _num(c.get("lotSize")),
                                 _num(c.get("minSize")), _num(c.get("tickSize")))
        except (TypeError, ValueError, AttributeError):
            dropped += 1
    if dropped:
        log.warning("SPEC_ROWS_DROPPED blofin %d of %d", dropped, len(rows))
    return out


def parse_tickers(raw: dict) -> dict[str, float]:
    """`/api/v1/market/tickers` → 24 h USD volume ≈ volCurrency24h (base units) × last."""
    out: dict[str, float] = {}
    for t in raw.get("data") or []:
        try:
            inst = str(t.get("instId", ""))
            if not inst.endswith("-USDT"):
                continue
            out[to_symbol(inst)] = _num(t.get("volCurrency24h")) * _num(t.get("last"))
        except (TypeError, ValueError, AttributeError):
            continue
    return out


def parse_funding(raw: dict) -> dict[str, tuple[float, float]]:
    """`/api/v1/market/funding-rate` → symbol -> (rate FRACTION, upcoming settlement unix SECONDS)."""
    out: dict[str, tuple[float, float]] = {}
    for f in raw.get("data") or []:
        try:
            inst = str(f.get("instId", ""))
            if not inst.endswith("-USDT"):
                continue
            out[to_symbol(inst)] = (_num(f.get("fundingRate")), _num(f.get("fundingTime")) / 1000.0)
        except (TypeError, ValueError, AttributeError):
            continue
    return out


class BlofinPublic:
    def __init__(self, cfg: VenueConfig, on_bbo: Callable[[BBO], None], clock=time.time,
                 session_factory=aiohttp.ClientSession):
        self.cfg = cfg
        self.on_bbo = on_bbo
        self.clock = clock
        self.contract_size: dict[str, float] = {}
        self._runner = WSRunner(
            WSAdapter(name=NAME, url=WS_URL, subscribe=subscribe, parse=self._parse,
                      max_topics=cfg.max_topics, ping=(25.0, "ping")),
            on_items=self._emit, session_factory=session_factory)

    def _parse(self, raw: dict, state: dict) -> list[BBO]:
        if raw.get("event") == "error":              # the whole subscribe message was refused; the socket stays open
            n = state["errors"] = state.get("errors", 0) + 1
            if n in (1, 10, 100) or n % 1000 == 0:
                log.warning("blofin error event #%d: code=%s %.200s", n, raw.get("code"), raw.get("msg"))
            return []
        return parse_books5(raw, self.contract_size, self.clock())

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


class BlofinMarket:
    def __init__(self, session: aiohttp.ClientSession, clock=time.time, specs: dict[str, VenueSpec] | None = None):
        self.session = session
        self.clock = clock
        self.specs = specs or {}

    async def _get(self, path: str, timeout: float = 10.0) -> dict:
        """GET + envelope check: BloFin reports errors as HTTP 200 + {"code": "<non-zero>", "msg"} without `data`."""
        async with self.session.get(REST + path, headers=HEADERS,
                                    timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            body = await r.text()
            if r.status != 200:
                raise VenueError(f"blofin {path} HTTP {r.status}: {body[:200]}")
        try:
            d = json.loads(body)
        except ValueError as e:
            raise VenueError(f"blofin {path} non-JSON body: {body[:200]}") from e
        if not isinstance(d, dict) or str(d.get("code")) != "0":
            raise VenueError(f"blofin {path} error envelope: {str(d)[:200]}")
        return d

    async def fetch_specs(self) -> dict[str, VenueSpec]:
        specs = parse_instruments(await self._get("/api/v1/market/instruments"))
        if not specs:
            raise VenueError("blofin market/instruments returned no usable instruments")
        self.specs = specs
        return self.specs

    async def fetch_volumes(self) -> dict[str, float]:
        return parse_tickers(await self._get("/api/v1/market/tickers"))

    async def fetch_funding(self) -> dict[str, tuple[float, float]]:
        return parse_funding(await self._get("/api/v1/market/funding-rate"))

    async def fetch_bbo(self, symbol: str) -> BBO | None:
        spec = self.specs.get(symbol)
        if spec is None:
            return None
        raw = await self._get(f"/api/v1/market/books?instId={spec.instrument}&size=5", timeout=5.0)
        return parse_books_rest(raw, spec.instrument, spec.contract_size, self.clock())
