# Canonical Types — Task-Level Implementation Details

> Companion to `2026-04-27-canonical-types.md`. Contains code skeletons, edge cases, and acceptance criteria for the highest-risk tasks. Tasks not detailed here follow patterns established below — see plan file for guidance.

**Detailed below:** Tasks 5, 6, 7, 10, 11, 12, 16.
**Pattern-only (not detailed):** Tasks 1–4 (additive schema, mechanical), 8 (`round_size`/`round_price` follow Task 7), 9 (startup wiring, ~30 lines), 13–15 (mirror Task 10 for other exchanges in warn→enforce mode), 17–19 (small reconciler additions + cleanup).

---

## Task 5 — Create `symbols.py` skeleton

**Why high-risk:** Every later task depends on `CanonicalSymbol` and `Instrument` having the right shape. Decimal vs float here is irreversible without a migration.

### Files

- **Create:** `deploy-live/symbols.py` (~250 lines target)
- **Create:** `deploy-live/tests/test_symbols.py` (~150 lines)

### Implementation skeleton

```python
# deploy-live/symbols.py
from __future__ import annotations
from dataclasses import dataclass
from decimal import Decimal
from typing import Literal, Callable
import time

ExchangeName = Literal["OKX", "BYBIT", "MEXC", "BLOFIN"]
InstrumentType = Literal["spot", "perp"]

@dataclass(frozen=True)
class CanonicalSymbol:
    value: str

    def __post_init__(self) -> None:
        v = self.value
        if not v.endswith("USDT"):
            raise ValueError(f"Canonical must end with USDT: {v!r}")
        if not v.isascii() or not v.isupper():
            raise ValueError(f"Canonical must be uppercase ASCII: {v!r}")
        if "/" in v or "-" in v or "_" in v or " " in v:
            raise ValueError(f"Canonical must contain no separators: {v!r}")
        if len(v) <= 4:
            raise ValueError(f"Canonical missing base: {v!r}")

    @property
    def base(self) -> str:
        return self.value[:-4]

    def __str__(self) -> str:
        return self.value

@dataclass(frozen=True)
class Instrument:
    canonical: CanonicalSymbol
    exchange: ExchangeName
    venue_symbol: str
    instrument_type: InstrumentType
    contract_size: Decimal
    lot_size: Decimal
    tick_size: Decimal
    min_notional_usd: Decimal
    is_tradeable: bool
    refreshed_at_ms: int

def _normalize_exchange(name: str) -> ExchangeName:
    """Case-insensitive: 'Bybit', 'BYBIT', 'bybit' all map to 'BYBIT'.
    Kills the dual-key bug in _NORMALIZERS."""
    upper = name.upper()
    if upper not in ("OKX", "BYBIT", "MEXC", "BLOFIN"):
        raise ValueError(f"Unknown exchange: {name!r}")
    return upper  # type: ignore[return-value]

def _to_canonical_okx(raw: str) -> CanonicalSymbol:
    s = raw.upper().replace("-USDT-SWAP", "USDT").replace("-SWAP", "")
    s = s.replace("-", "")
    return CanonicalSymbol(s)

def _to_canonical_bybit(raw: str) -> CanonicalSymbol:
    return CanonicalSymbol(raw.upper())

def _to_canonical_mexc(raw: str) -> CanonicalSymbol:
    return CanonicalSymbol(raw.upper().replace("_", ""))

def _to_canonical_blofin(raw: str) -> CanonicalSymbol:
    return CanonicalSymbol(raw.upper().replace("-", ""))

_TO_CANONICAL: dict[ExchangeName, Callable[[str], CanonicalSymbol]] = {
    "OKX": _to_canonical_okx,
    "BYBIT": _to_canonical_bybit,
    "MEXC": _to_canonical_mexc,
    "BLOFIN": _to_canonical_blofin,
}

def _to_venue_okx(c: CanonicalSymbol, instr_type: InstrumentType) -> str:
    return f"{c.base}-USDT-SWAP" if instr_type == "perp" else f"{c.base}-USDT"

def _to_venue_bybit(c, instr_type): return c.value
def _to_venue_mexc(c, instr_type): return c.value
def _to_venue_blofin(c, instr_type): return f"{c.base}-USDT"

_TO_VENUE: dict[ExchangeName, Callable[[CanonicalSymbol, InstrumentType], str]] = {
    "OKX": _to_venue_okx, "BYBIT": _to_venue_bybit,
    "MEXC": _to_venue_mexc, "BLOFIN": _to_venue_blofin,
}

class InstrumentRegistry:
    def __init__(self) -> None:
        self._instruments: dict[tuple[ExchangeName, str], Instrument] = {}
        self._last_refresh: dict[ExchangeName, int] = {}

    def to_canonical(self, exchange: str, raw_symbol: str) -> CanonicalSymbol:
        ex = _normalize_exchange(exchange)
        return _TO_CANONICAL[ex](raw_symbol)

    def to_venue(self, exchange: str, canonical: CanonicalSymbol,
                 instrument_type: InstrumentType = "perp") -> str:
        ex = _normalize_exchange(exchange)
        instr = self._instruments.get((ex, canonical.value))
        if instr is not None:
            return instr.venue_symbol
        return _TO_VENUE[ex](canonical, instrument_type)

    def get(self, exchange: str, canonical: CanonicalSymbol) -> Instrument | None:
        return self._instruments.get((_normalize_exchange(exchange), canonical.value))

    def supports(self, exchange: str, canonical: CanonicalSymbol) -> bool:
        instr = self.get(exchange, canonical)
        return instr is not None and instr.is_tradeable

    def round_size(self, exchange, canonical, raw_size):
        raise NotImplementedError("Implemented in Task 8")

    def round_price(self, exchange, canonical, raw_price, side):
        raise NotImplementedError("Implemented in Task 8")

    async def refresh(self, exchange):
        raise NotImplementedError("Implemented in Task 7")

    async def refresh_all(self, http_client):
        raise NotImplementedError("Implemented in Task 7")
```

### Tests

```python
# deploy-live/tests/test_symbols.py
import pytest
from symbols import CanonicalSymbol, InstrumentRegistry

@pytest.mark.parametrize("bad", [
    "BTC", "BTCUSD", "btcusdt", "BTC/USDT", "BTC-USDT",
    "USDT", "BTC USDT", "BTC_USDT", "",
])
def test_canonical_rejects_invalid(bad):
    with pytest.raises(ValueError):
        CanonicalSymbol(bad)

@pytest.mark.parametrize("good,base", [
    ("BTCUSDT", "BTC"), ("ETHUSDT", "ETH"),
    ("1000PEPEUSDT", "1000PEPE"), ("BONKUSDT", "BONK"),
])
def test_canonical_accepts_valid(good, base):
    c = CanonicalSymbol(good)
    assert c.value == good
    assert c.base == base

@pytest.fixture
def reg(): return InstrumentRegistry()

@pytest.mark.parametrize("exchange,raw", [
    ("OKX", "BTC-USDT-SWAP"), ("OKX", "BTC-USDT"),
    ("BYBIT", "BTCUSDT"), ("MEXC", "BTC_USDT"),
    ("MEXC", "BTCUSDT"), ("BLOFIN", "BTC-USDT"),
])
def test_to_canonical_btc_consistent(reg, exchange, raw):
    assert reg.to_canonical(exchange, raw) == CanonicalSymbol("BTCUSDT")

@pytest.mark.parametrize("exchange,raw,base", [
    ("OKX", "1000PEPE-USDT-SWAP", "1000PEPE"),
    ("BYBIT", "1000PEPEUSDT", "1000PEPE"),
    ("MEXC", "1000PEPE_USDT", "1000PEPE"),
    ("BLOFIN", "1000PEPE-USDT", "1000PEPE"),
])
def test_multiplier_preserved(reg, exchange, raw, base):
    c = reg.to_canonical(exchange, raw)
    assert c.base == base
    assert c.value == f"{base}USDT"

@pytest.mark.parametrize("name", ["Bybit", "BYBIT", "bybit", "ByBiT"])
def test_exchange_name_case_insensitive(reg, name):
    assert reg.to_canonical(name, "BTCUSDT") == CanonicalSymbol("BTCUSDT")

def test_unknown_exchange_raises(reg):
    with pytest.raises(ValueError, match="Unknown exchange"):
        reg.to_canonical("KRAKEN", "BTCUSDT")
```

### Gotchas

1. **Bybit SPOT vs perp:** `to_venue` defaults to `"perp"`. Bybit is SPOT-only; the parameter is moot for Bybit (returns `BTCUSDT` either way).
2. **MEXC current canonical is `BTCUSDT`** — bot does not use the `_` form for venue calls. `_to_venue_mexc` returns `c.value`. Confirm with current `MEXCExecutor` if behavior changes.
3. **OKX `_to_canonical_okx`** strips `-USDT-SWAP` BEFORE `-SWAP` — order matters.
4. **`Decimal` everywhere on `Instrument`.** Do not relax to float later.
5. **`refreshed_at_ms` is epoch ms,** matching `schemas.py` convention. Use `int(time.time() * 1000)`.

### Acceptance criteria

- [ ] All tests in `test_symbols.py` pass.
- [ ] `mypy --strict deploy-live/symbols.py` clean.
- [ ] No imports from `normalizers.py`, `live_exchange_fetcher.py`, or `real_trader.py` (registry is a leaf module).
- [ ] No `float` outside validation messages.

---

## Task 6 — Replace duplicate symbol-stripping with registry calls

**Why high-risk:** First task that touches production code. Behavior must be byte-identical or every downstream symbol-keyed dict misaligns.

### Files

- **Modify:** `deploy-live/live_exchange_fetcher.py`
- **Modify:** `deploy-live/normalizers.py`
- **Create:** `deploy-live/tests/test_symbol_consolidation.py` — snapshot test
- **Create:** `deploy-live/tests/fixtures/symbol_snapshot.json`

### Sub-step 6a: Wire registry into `live_exchange_fetcher.py`

Remove `_strip_swap` (around line 153) and any `_strip_dash` callers. Each `_norm_<exchange>` becomes:

```python
class LiveExchangeFetcher:
    def __init__(self, executors, registry: InstrumentRegistry):
        self._executors = executors
        self._registry = registry

    def _norm_okx(self, p: dict) -> dict:
        venue_sym = str(p.get("instId", ""))
        canonical = self._registry.to_canonical("OKX", venue_sym).value
        return {"symbol": canonical, ...}  # rest unchanged

    # Same pattern for _norm_mexc, _norm_bybit, _norm_blofin.
```

**Kill the dual-key bug** (lines 235–242):

```python
# OLD:
_NORMALIZERS = {"OKX": _norm_okx, "Bybit": _norm_bybit,
                "BYBIT": _norm_bybit, "MEXC": _norm_mexc,
                "BloFin": _norm_blofin, "BLOFIN": _norm_blofin}

# NEW:
_NORMALIZERS = {
    "OKX": _norm_okx, "BYBIT": _norm_bybit,
    "MEXC": _norm_mexc, "BLOFIN": _norm_blofin,
}

def get_normalizer(name: str):
    return _NORMALIZERS[_normalize_exchange(name)]
```

Find all callsites of `_NORMALIZERS[...]` and replace with `get_normalizer(...)`:

```bash
rg -n '_NORMALIZERS\[' deploy-live/
```

### Sub-step 6b: Wire registry into `normalizers.py`

Remove `_strip_dash` (line 64). Each `normalize_<exchange>_order` takes registry as arg:

```python
def normalize_okx_order(raw: dict, registry: InstrumentRegistry, ...) -> ExchangeOrderResponse:
    venue_sym = str(raw.get("instId", ""))
    canonical = registry.to_canonical("OKX", venue_sym).value
    # ... use `canonical` instead of `_strip_dash(...)`
```

Update `real_trader.py` callers to thread registry through (Task 9 finishes startup wiring; Task 6 just changes signatures).

### Sub-step 6c: Snapshot regression test

```python
# deploy-live/tests/test_symbol_consolidation.py
import json
from pathlib import Path
from symbols import InstrumentRegistry

FIXTURE = Path(__file__).parent / "fixtures" / "symbol_snapshot.json"

def test_snapshot_unchanged():
    """fixtures/symbol_snapshot.json: list of {exchange, raw, canonical}."""
    cases = json.loads(FIXTURE.read_text())
    reg = InstrumentRegistry()
    failures = []
    for c in cases:
        got = reg.to_canonical(c["exchange"], c["raw"]).value
        if got != c["canonical"]:
            failures.append(f"{c['exchange']} {c['raw']!r}: "
                            f"got {got!r}, expected {c['canonical']!r}")
    assert not failures, "\n".join(failures)
```

**Synthetic fixture** (per option 3b — synthetic now, real validation in paper-trader gate):

```json
[
  {"exchange": "OKX", "raw": "BTC-USDT-SWAP", "canonical": "BTCUSDT"},
  {"exchange": "OKX", "raw": "ETH-USDT-SWAP", "canonical": "ETHUSDT"},
  {"exchange": "OKX", "raw": "1000PEPE-USDT-SWAP", "canonical": "1000PEPEUSDT"},
  {"exchange": "BYBIT", "raw": "BTCUSDT", "canonical": "BTCUSDT"},
  {"exchange": "BYBIT", "raw": "ETHUSDT", "canonical": "ETHUSDT"},
  {"exchange": "MEXC", "raw": "BTC_USDT", "canonical": "BTCUSDT"},
  {"exchange": "MEXC", "raw": "BTCUSDT", "canonical": "BTCUSDT"},
  {"exchange": "BLOFIN", "raw": "BTC-USDT", "canonical": "BTCUSDT"},
  {"exchange": "BLOFIN", "raw": "ETH-USDT", "canonical": "ETHUSDT"}
]
```

Expand with real production data when paper-trader gate runs (≥7 days of real symbols).

### Acceptance criteria

- [ ] `rg -n '_strip_swap|_strip_dash' deploy-live/` returns 0 hits in non-test code.
- [ ] `_NORMALIZERS` has exactly 4 keys, all uppercase.
- [ ] `test_symbol_consolidation.py` passes.
- [ ] Paper trader runs 1 day post-merge: zero diff in canonical symbols emitted vs production-log baseline.

### Gotchas

1. **Don't regenerate the snapshot fixture as a "fix"** if the test fails. Diff = consolidation changed behavior. Investigate.
2. **Order of replacement matters:** consolidate `live_exchange_fetcher.py` and `normalizers.py` in the SAME commit.
3. **Hyperliquid (`real_trader.py:3038`) is NOT touched** — D4 keeps it out of scope.

---

## Task 7 — Implement `InstrumentRegistry.refresh()` per exchange

**Why high-risk:** Three of four endpoints are net-new code. Wrong field mapping silently propagates lot/tick errors to every rounding decision.

### Files

- **Modify:** `deploy-live/symbols.py` — implement `_refresh_*` per exchange + `refresh_all`.
- **Create:** `deploy-live/tests/fixtures/instruments/{okx,bybit,mexc,blofin}_snapshot.json` — synthetic v1.
- **Create:** `deploy-live/tests/test_instrument_registry.py`.
- **Create:** `deploy-live/tools/snapshot_instruments.py` — utility for generating real fixtures later.

### Sub-step 7.1 — OKX

Port logic from `OKXExecutor._load_contract_specs` (`real_trader.py:527–599`):

```python
async def _refresh_okx(self, http_client) -> None:
    resp = await http_client.get(
        "https://www.okx.com/api/v5/public/instruments",
        params={"instType": "SWAP"},
    )
    resp.raise_for_status()
    data = resp.json()["data"]
    new = {}
    for row in data:
        inst_id = row["instId"]
        if not inst_id.endswith("-USDT-SWAP"):
            continue
        canonical = _to_canonical_okx(inst_id)
        instr = Instrument(
            canonical=canonical, exchange="OKX",
            venue_symbol=inst_id, instrument_type="perp",
            contract_size=Decimal(row["ctVal"]),
            lot_size=Decimal(row["lotSz"]),
            tick_size=Decimal(row["tickSz"]),
            min_notional_usd=Decimal(row.get("minSz", "0")) * Decimal(row["ctVal"]),
            is_tradeable=row.get("state") == "live",
            refreshed_at_ms=int(time.time() * 1000),
        )
        new[("OKX", canonical.value)] = instr
    self._atomic_swap("OKX", new)
```

### Sub-step 7.2 — Bybit (`/v5/market/instruments-info?category=spot`)

```python
async def _refresh_bybit(self, http_client) -> None:
    resp = await http_client.get(
        "https://api.bybit.com/v5/market/instruments-info",
        params={"category": "spot"},
    )
    resp.raise_for_status()
    data = resp.json()["result"]["list"]
    new = {}
    for row in data:
        sym = row["symbol"]
        if not sym.endswith("USDT"):
            continue
        canonical = _to_canonical_bybit(sym)
        lot_filter = row["lotSizeFilter"]
        price_filter = row["priceFilter"]
        instr = Instrument(
            canonical=canonical, exchange="BYBIT",
            venue_symbol=sym, instrument_type="spot",
            contract_size=Decimal("1"),
            lot_size=Decimal(lot_filter["basePrecision"]),  # NOT qtyStep
            tick_size=Decimal(price_filter["tickSize"]),
            min_notional_usd=Decimal(lot_filter.get("minNotionalValue", "5")),
            is_tradeable=row["status"] == "Trading",
            refreshed_at_ms=int(time.time() * 1000),
        )
        new[("BYBIT", canonical.value)] = instr
    self._atomic_swap("BYBIT", new)
```

| Field | Source |
|---|---|
| `lot_size` | `lotSizeFilter.basePrecision` (NOT `qtyStep` — that's for futures) |
| `tick_size` | `priceFilter.tickSize` |
| `min_notional_usd` | `lotSizeFilter.minNotionalValue` (default `5` if missing) |
| `is_tradeable` | `status == "Trading"` |

### Sub-step 7.3 — MEXC

⚠️ **Operational Question #3 must be resolved first.** Inspect current `MEXCExecutor` to confirm endpoint (spot `/api/v3/exchangeInfo` vs perp `/api/v1/contract/detail`).

**Path A — SPOT:**
```python
async def _refresh_mexc(self, http_client) -> None:
    resp = await http_client.get("https://api.mexc.com/api/v3/exchangeInfo")
    resp.raise_for_status()
    data = resp.json()["symbols"]
    new = {}
    for row in data:
        if row["quoteAsset"] != "USDT" or row["status"] != "ENABLED":
            continue
        canonical = _to_canonical_mexc(row["symbol"])
        base_prec = int(row["baseAssetPrecision"])
        quote_prec = int(row["quotePrecision"])
        instr = Instrument(
            canonical=canonical, exchange="MEXC",
            venue_symbol=row["symbol"], instrument_type="spot",
            contract_size=Decimal("1"),
            lot_size=Decimal(10) ** -base_prec,
            tick_size=Decimal(10) ** -quote_prec,
            min_notional_usd=Decimal(row.get("quoteAmountPrecision", "5")),
            is_tradeable=True,
            refreshed_at_ms=int(time.time() * 1000),
        )
        new[("MEXC", canonical.value)] = instr
    self._atomic_swap("MEXC", new)
```

**Path B — PERP** (host is `contract.mexc.com`):
```python
async def _refresh_mexc(self, http_client) -> None:
    resp = await http_client.get("https://contract.mexc.com/api/v1/contract/detail")
    resp.raise_for_status()
    data = resp.json()["data"]
    new = {}
    for row in data:
        if row["quoteCoin"] != "USDT" or row["state"] != 0:
            continue
        canonical = _to_canonical_mexc(row["symbol"])
        instr = Instrument(
            canonical=canonical, exchange="MEXC",
            venue_symbol=row["symbol"], instrument_type="perp",
            contract_size=Decimal(str(row["contractSize"])),
            lot_size=Decimal(str(row["volUnit"])),
            tick_size=Decimal(str(row["priceUnit"])),
            min_notional_usd=Decimal(str(row.get("minVol", 1))) * Decimal(str(row["contractSize"])),
            is_tradeable=True,
            refreshed_at_ms=int(time.time() * 1000),
        )
        new[("MEXC", canonical.value)] = instr
    self._atomic_swap("MEXC", new)
```

Pick based on what `MEXCExecutor.place_market_order` calls today. Document choice in code comments.

### Sub-step 7.4 — BloFin

```python
async def _refresh_blofin(self, http_client) -> None:
    resp = await http_client.get("https://openapi.blofin.com/api/v1/market/instruments")
    resp.raise_for_status()
    data = resp.json()["data"]
    new = {}
    for row in data:
        inst_id = row["instId"]
        if not inst_id.endswith("-USDT"):
            continue
        canonical = _to_canonical_blofin(inst_id)
        instr = Instrument(
            canonical=canonical, exchange="BLOFIN",
            venue_symbol=inst_id, instrument_type="perp",
            contract_size=Decimal(str(row["contractValue"])),
            lot_size=Decimal(str(row["lotSize"])),
            tick_size=Decimal(str(row["tickSize"])),
            min_notional_usd=Decimal(str(row.get("minSize", 1))) * Decimal(str(row["contractValue"])),
            is_tradeable=row["state"] == "live",  # lowercase "live", NOT "Trading"
            refreshed_at_ms=int(time.time() * 1000),
        )
        new[("BLOFIN", canonical.value)] = instr
    self._atomic_swap("BLOFIN", new)
```

### Sub-step 7.5 — `refresh_all` orchestration

```python
class InstrumentRefreshError(RuntimeError):
    """Raised when registry.refresh_all fails — bot must hard-fail boot (D5)."""

async def refresh_all(self, http_client) -> None:
    # Per Operational Question #2: parallel via asyncio.gather; total budget < 30s.
    results = await asyncio.gather(
        asyncio.wait_for(self._refresh_okx(http_client), timeout=10.0),
        asyncio.wait_for(self._refresh_bybit(http_client), timeout=10.0),
        asyncio.wait_for(self._refresh_mexc(http_client), timeout=10.0),
        asyncio.wait_for(self._refresh_blofin(http_client), timeout=10.0),
        return_exceptions=True,
    )
    failures = [
        (ex, r) for ex, r in zip(("OKX","BYBIT","MEXC","BLOFIN"), results)
        if isinstance(r, BaseException)
    ]
    if failures:
        msg = "; ".join(f"{ex}: {type(err).__name__}: {err}" for ex, err in failures)
        raise InstrumentRefreshError(f"Initial refresh failed: {msg}")
```

### Sub-step 7.6 — Atomic swap

```python
def _atomic_swap(self, exchange: ExchangeName, new_instruments: dict) -> None:
    """Replace all instruments for one exchange in a single dict assignment.
    Prevents torn reads under asyncio."""
    other = {k: v for k, v in self._instruments.items() if k[0] != exchange}
    self._instruments = {**other, **new_instruments}
    self._last_refresh[exchange] = int(time.time() * 1000)
```

### Tests

```python
# deploy-live/tests/test_instrument_registry.py
import json, pytest, asyncio
from decimal import Decimal
from pathlib import Path
from symbols import InstrumentRegistry, CanonicalSymbol, InstrumentRefreshError

FIX_DIR = Path(__file__).parent / "fixtures" / "instruments"

class FakeHTTP:
    def __init__(self, mapping): self._mapping = mapping
    async def get(self, url, params=None):
        for prefix, payload in self._mapping.items():
            if url.startswith(prefix):
                return _Resp(payload)
        raise KeyError(f"FakeHTTP unmocked: {url}")

class _Resp:
    def __init__(self, p): self._p = p
    def raise_for_status(self): pass
    def json(self): return self._p

@pytest.fixture
def fixtures():
    return {
        "okx": json.loads((FIX_DIR / "okx_snapshot.json").read_text()),
        "bybit": json.loads((FIX_DIR / "bybit_snapshot.json").read_text()),
        "mexc": json.loads((FIX_DIR / "mexc_snapshot.json").read_text()),
        "blofin": json.loads((FIX_DIR / "blofin_snapshot.json").read_text()),
    }

@pytest.mark.asyncio
async def test_refresh_all_populates_btc_on_every_exchange(fixtures):
    reg = InstrumentRegistry()
    http = FakeHTTP({
        "https://www.okx.com": fixtures["okx"],
        "https://api.bybit.com": fixtures["bybit"],
        "https://api.mexc.com": fixtures["mexc"],
        "https://openapi.blofin.com": fixtures["blofin"],
    })
    await reg.refresh_all(http)
    btc = CanonicalSymbol("BTCUSDT")
    for ex in ("OKX", "BYBIT", "MEXC", "BLOFIN"):
        instr = reg.get(ex, btc)
        assert instr is not None, f"{ex} missing BTCUSDT"
        assert instr.is_tradeable
        assert instr.lot_size > Decimal(0)
        assert instr.tick_size > Decimal(0)

@pytest.mark.asyncio
async def test_refresh_all_hard_fails_on_any_outage(fixtures):
    """D5: any exchange refresh failure → raise."""
    reg = InstrumentRegistry()
    http = FakeHTTP({"https://www.okx.com": fixtures["okx"]})
    with pytest.raises(InstrumentRefreshError) as e:
        await reg.refresh_all(http)
    for missing in ("BYBIT", "MEXC", "BLOFIN"):
        assert missing in str(e.value)

@pytest.mark.asyncio
async def test_atomic_swap_no_torn_read(fixtures):
    reg = InstrumentRegistry()
    http = FakeHTTP({"https://www.okx.com": fixtures["okx"]})

    async def reader():
        for _ in range(1000):
            instr = reg.get("OKX", CanonicalSymbol("BTCUSDT"))
            if instr is not None:
                assert instr.lot_size > Decimal(0)

    async def refresher():
        for _ in range(10):
            await reg._refresh_okx(http)

    await asyncio.gather(reader(), refresher())
```

### Synthetic fixtures (per option 3b)

Generate v1 fixtures with shape-correct synthetic data. Real fixtures generated later via `tools/snapshot_instruments.py`:

```python
# deploy-live/tools/snapshot_instruments.py
"""Run once to capture real /instruments responses.
Output: deploy-live/tests/fixtures/instruments/{venue}_snapshot.json"""
import asyncio, httpx, json
from pathlib import Path

OUT = Path(__file__).parent.parent / "tests" / "fixtures" / "instruments"

async def main():
    OUT.mkdir(parents=True, exist_ok=True)
    async with httpx.AsyncClient(timeout=30) as c:
        for name, url, params in [
            ("okx", "https://www.okx.com/api/v5/public/instruments", {"instType": "SWAP"}),
            ("bybit", "https://api.bybit.com/v5/market/instruments-info", {"category": "spot"}),
            ("mexc", "https://api.mexc.com/api/v3/exchangeInfo", None),
            ("blofin", "https://openapi.blofin.com/api/v1/market/instruments", None),
        ]:
            r = await c.get(url, params=params or {})
            (OUT / f"{name}_snapshot.json").write_text(json.dumps(r.json(), indent=2))
            print(f"saved {name}: {len(r.text)} bytes")

if __name__ == "__main__":
    asyncio.run(main())
```

### Acceptance criteria

- [ ] All 4 `_refresh_*` populate `Instrument` for ≥10 USDT pairs each from synthetic fixtures.
- [ ] `refresh_all` returns within 30s when all exchanges respond.
- [ ] `InstrumentRefreshError` raised when any exchange times out / errors.
- [ ] Atomic-swap test passes 1000 concurrent reads without partial state.
- [ ] BTC lot_size on each venue matches venue documentation (manual once).

### Gotchas

1. **Bybit `qtyStep` vs `basePrecision`** — `qtyStep` is for derivatives. Bot is SPOT-only on Bybit. Wrong one yields 1.0 lot size (silent breakage).
2. **MEXC perp endpoint domain is `contract.mexc.com`** — different host. Confirm before implementing.
3. **BloFin `state` is `"live"` (lowercase)**, not `"Trading"`. Easy copy-paste mistake.
4. **OKX `minSz`** is in contracts, not USD. `min_notional_usd = minSz × ctVal × mark_price` — but mark drifts. Compute lazily in executor (see Task 11), not at refresh time.
5. **Decimal precision** — ALL numeric fields must come from `Decimal(str(value))`, not `Decimal(value)` when source is float.

---

## Task 10 — Migrate `OKXExecutor._compute_sz` to `registry.round_size`

**Why highest-correctness-risk:** Wrong rounding = wrong order size = real money. OKX is the only working venue today; preserve byte-identical behavior, then extend to others with confidence.

### Files

- **Modify:** `deploy-live/symbols.py` — implement `round_size`, `round_price`.
- **Modify:** `deploy-live/real_trader.py` — `OKXExecutor._compute_sz` becomes a thin call.
- **Create:** `deploy-live/tests/test_okx_size_parity.py` — diff-test harness.
- **Create:** `deploy-live/tests/fixtures/okx_size_parity/production_orders.jsonl` — synthetic v1.

### Implementation

```python
# deploy-live/symbols.py
from decimal import Decimal, ROUND_DOWN, ROUND_FLOOR

class InstrumentRegistry:
    def round_size(self, exchange, canonical, raw_size: Decimal) -> Decimal:
        instr = self.get(exchange, canonical)
        if instr is None:
            raise KeyError(f"No instrument: {exchange} {canonical}")
        steps = (raw_size / instr.lot_size).quantize(Decimal("1"), rounding=ROUND_FLOOR)
        return steps * instr.lot_size

    def round_price(self, exchange, canonical, raw_price: Decimal,
                    side: Literal["buy","sell"]) -> Decimal:
        instr = self.get(exchange, canonical)
        if instr is None:
            raise KeyError(f"No instrument: {exchange} {canonical}")
        rounding = ROUND_FLOOR if side == "buy" else ROUND_DOWN
        steps = (raw_price / instr.tick_size).quantize(Decimal("1"), rounding=rounding)
        return steps * instr.tick_size
```

### Migration of `OKXExecutor._compute_sz`

```python
# deploy-live/real_trader.py
class OKXExecutor(ExchangeExecutor):
    def __init__(self, ..., registry: InstrumentRegistry):
        self.registry = registry
        # ... rest

    def _compute_sz(self, canonical_symbol: str, requested_usd: Decimal,
                    mark_price: Decimal) -> Decimal:
        c = CanonicalSymbol(canonical_symbol)
        instr = self.registry.get("OKX", c)
        if instr is None:
            raise ValueError(f"OKX instrument not in registry: {c}")
        contracts_raw = requested_usd / (mark_price * instr.contract_size)
        return self.registry.round_size("OKX", c, contracts_raw)
```

### Diff-test harness

```python
# deploy-live/tests/test_okx_size_parity.py
"""Lock-in test for OKX size rounding migration.
Captured (canonical, requested_usd, mark_price, expected_contracts) tuples
from synthetic fixtures (real production capture replaces these in paper-gate)."""

import json, pytest
from decimal import Decimal
from pathlib import Path

FIXTURE = Path(__file__).parent / "fixtures" / "okx_size_parity" / "production_orders.jsonl"

@pytest.fixture(scope="module")
def populated_registry():
    """Load registry from a recorded snapshot, NOT live."""
    from symbols import InstrumentRegistry
    reg = InstrumentRegistry()
    snapshot = Path(__file__).parent / "fixtures" / "instruments" / "okx_snapshot.json"
    # Test-only helper; load straight into _instruments:
    reg._load_from_snapshot(json.loads(snapshot.read_text()))
    return reg

def test_okx_size_parity(populated_registry):
    from real_trader import OKXExecutor
    failures = []
    with FIXTURE.open() as f:
        for line in f:
            row = json.loads(line)
            executor = OKXExecutor.__new__(OKXExecutor)  # bypass init
            executor.registry = populated_registry
            got = executor._compute_sz(
                row["canonical"], Decimal(row["requested_usd"]),
                Decimal(row["mark_price"]),
            )
            if got != Decimal(row["expected_contracts"]):
                failures.append(
                    f"{row['canonical']} usd={row['requested_usd']} "
                    f"mark={row['mark_price']}: got {got}, "
                    f"expected {row['expected_contracts']}"
                )
    assert not failures, "\n".join(failures[:50])
```

### Synthetic v1 fixture

```jsonl
{"canonical":"BTCUSDT","requested_usd":"100","mark_price":"65000","expected_contracts":"0.001"}
{"canonical":"BTCUSDT","requested_usd":"500","mark_price":"65000","expected_contracts":"0.007"}
{"canonical":"ETHUSDT","requested_usd":"100","mark_price":"3500","expected_contracts":"0.02"}
```

Real `production_orders.jsonl` generated later via order log scrape (≥30 days OKX history, ≥1000 rows). Acceptance criterion below requires this for the live gate.

### Acceptance criteria

- [ ] `tests/test_okx_size_parity.py` passes on synthetic fixtures (Phase A unit test).
- [ ] **Paper-trader gate:** zero diff vs production OKX size history for ≥30 days, ≥1000 orders. Required before flipping enforce.
- [ ] `_compute_sz` body in `real_trader.py` ≤6 lines (delegates to registry).
- [ ] `OKXExecutor` constructor takes `registry`; instantiation without it fails fast.
- [ ] `mypy --strict` passes.
- [ ] No `float` arithmetic on contract sizes anywhere in `OKXExecutor`.

### Gotchas

1. **`Decimal` vs `float` precision** — old code did `round(raw / lot) * lot` with floats. New uses `Decimal` + `ROUND_FLOOR`. Some edges round differently. **This is an improvement, not a regression** — but the parity test will flag. Strategy: tolerance = 0.5 lot (one ulp); larger diff = real bug.

2. **`min_notional_usd` for OKX** — OKX returns `minSz` (contracts), not USD. Compute USD lazily at order time (Task 11 handles this in executor flow), not at refresh time (mark drifts).

3. **Atomic snapshot swap** — single-threaded under asyncio, but `dict.update` would race with concurrent reads. Build new dict, swap with one assignment.

4. **Registry must initialize before `OKXExecutor`** — Task 9 enforces at startup. Task 10 must raise on missing instrument, never default silently.

---

## Task 11 — Migrate executor signature

**Why high-risk:** Touches all 4 executors *and* every callsite. `requested_size_usd` becomes `Field(gt=0)` non-optional after Task 12 — wrong USD passed = downstream `model_validator` failure.

### Files

- **Modify:** `deploy-live/real_trader.py` — `ExchangeExecutor` ABC + 4 concrete classes + ~15 callsites.
- **Modify:** `deploy-live/tests/test_real_trader.py` (or wherever executor tests live).

### Sub-step 11.1 — Real ABC

```python
# deploy-live/real_trader.py (around line 433)
from abc import ABC, abstractmethod
from decimal import Decimal
from symbols import CanonicalSymbol, InstrumentRegistry
from schemas import ExchangeOrderResponse, BalanceSnapshot, PositionRecord, OrderStatus, RejectionCode

class ExchangeExecutor(ABC):
    exchange_name: str

    def __init__(self, registry: InstrumentRegistry, ...):
        self.registry = registry
        ...

    @abstractmethod
    async def place_market_order(
        self, canonical: CanonicalSymbol, side: Literal["buy","sell"],
        requested_usd: Decimal, mark_price: Decimal,
        client_order_id: str | None = None,
    ) -> ExchangeOrderResponse: ...

    @abstractmethod
    async def get_order_status(self, order_id: str) -> ExchangeOrderResponse: ...

    @abstractmethod
    async def cancel_order(self, order_id: str) -> bool: ...

    @abstractmethod
    async def get_balance(self) -> BalanceSnapshot: ...

    @abstractmethod
    async def get_open_positions(self) -> list[PositionRecord]: ...
```

Add `@abstractmethod` decorators where audit found informal `raise NotImplementedError`. Subclasses without override now fail at instantiation.

### Sub-step 11.2 — Each concrete executor

Pattern is identical. OKX example:

```python
class OKXExecutor(ExchangeExecutor):
    exchange_name = "OKX"

    async def place_market_order(self, canonical, side, requested_usd, mark_price,
                                 client_order_id=None) -> ExchangeOrderResponse:
        venue_sym = self.registry.to_venue("OKX", canonical, "perp")
        instr = self.registry.get("OKX", canonical)
        if instr is None or not instr.is_tradeable:
            return ExchangeOrderResponse(
                exchange="OKX", order_id="", side=side,
                requested_size_usd=float(requested_usd), filled_size_usd=0.0,
                status=OrderStatus.REJECTED, rejection_code=RejectionCode.UNKNOWN,
                rejection_message=f"Instrument not tradeable: {canonical}",
                raw={}, received_at_ms=int(time.time() * 1000),
            )
        size_contracts = self._compute_sz(canonical.value, requested_usd, mark_price)
        notional = size_contracts * mark_price * instr.contract_size
        if notional < instr.min_notional_usd:
            return ExchangeOrderResponse(
                exchange="OKX", order_id="", side=side,
                requested_size_usd=float(requested_usd), filled_size_usd=0.0,
                status=OrderStatus.REJECTED, rejection_code=RejectionCode.MIN_NOTIONAL,
                rejection_message=f"notional {notional} < min {instr.min_notional_usd}",
                raw={}, received_at_ms=int(time.time() * 1000),
            )
        started_at_ms = int(time.time() * 1000)
        raw = await self._http_place_order(venue_sym, side, size_contracts, client_order_id)
        return normalize_okx_order(
            raw, registry=self.registry,
            requested_usd=float(requested_usd), received_at_ms=started_at_ms,
        )
```

Repeat for Bybit (instr_type=`"spot"`), MEXC, BloFin. Same shape, different `_http_place_order` body.

### Sub-step 11.3 — Update callsites

```bash
rg -n 'place_market_order\(' deploy-live/real_trader.py
```

Each callsite migrated:

```python
# OLD:
result = await executor.place_market_order("BTCUSDT", "buy", size_usd=100)

# NEW:
canonical = CanonicalSymbol("BTCUSDT")
mark = Decimal(str(self._latest_mark_price[canonical.value]))
result = await executor.place_market_order(
    canonical=canonical, side="buy",
    requested_usd=Decimal("100"), mark_price=mark,
    client_order_id=ulid.new().str,
)
```

**Mark price source must be deterministic.** Use one canonical cache (likely `LivePosition._latest_mark[canonical.value]`), route all callsites through it.

### Sub-step 11.4 — Strict `model_validator` flip

After Task 11 lands and all callsites are migrated:

```python
# schemas.py
@model_validator(mode="after")
def _consistency(self) -> "ExchangeOrderResponse":
    if self.status is None:
        raise ValueError("status is required after Task 11")
    if self.status == OrderStatus.FILLED:
        assert (self.requested_size_usd - self.filled_size_usd) / self.requested_size_usd < 0.01
    if self.status == OrderStatus.PARTIALLY_FILLED:
        assert 0 < self.filled_size_usd < self.requested_size_usd
    if self.status in (OrderStatus.REJECTED, OrderStatus.CANCELLED):
        assert self.filled_size_usd == 0
        assert self.rejection_code is not None
    return self
```

### Tests

Three categories:

```python
def test_executor_abstract_methods_enforced():
    with pytest.raises(TypeError, match="abstract"):
        class Broken(ExchangeExecutor): pass
        Broken(registry=InstrumentRegistry())

@pytest.mark.parametrize("cls", [OKXExecutor, BybitExecutor, MEXCExecutor, BloFinExecutor])
async def test_signature(cls, mock_registry, mock_http):
    ex = cls(registry=mock_registry, ...)
    resp = await ex.place_market_order(
        canonical=CanonicalSymbol("BTCUSDT"), side="buy",
        requested_usd=Decimal("100"), mark_price=Decimal("65000"),
    )
    assert isinstance(resp, ExchangeOrderResponse)
    assert resp.requested_size_usd == 100.0

async def test_min_notional_local_reject(mock_registry):
    # Mock instrument with min_notional_usd=5. Submit $1 order.
    resp = await ex.place_market_order(
        canonical=CanonicalSymbol("BTCUSDT"), side="buy",
        requested_usd=Decimal("1"), mark_price=Decimal("65000"),
    )
    assert resp.status == OrderStatus.REJECTED
    assert resp.rejection_code == RejectionCode.MIN_NOTIONAL
    assert mock_http.call_count == 0  # never reached the network
```

### Acceptance criteria

- [ ] `ExchangeExecutor` is `ABC` with `@abstractmethod` on all 5 methods.
- [ ] All 4 concrete executors satisfy the new signature.
- [ ] Every callsite passes `canonical`, `requested_usd`, `mark_price` (verified by grep + AST).
- [ ] `mypy --strict deploy-live/real_trader.py` clean.
- [ ] Strict `model_validator` enabled; tests pass.
- [ ] Paper trader runs 1 day with no `model_validator` violations.

### Gotchas

1. **`Decimal` everywhere at call boundaries.** Mixing `float` USD with `Decimal` produces subtly wrong sizes. Add runtime assertion or `mypy` rule.
2. **`client_order_id` is new** — bot doesn't currently send. v1 default `None` (executor generates internally). Persisting in `ExchangeOrderResponse.client_order_id` is a follow-up.
3. **Bybit `instr_type="spot"`** — must pass `"spot"` to `to_venue("BYBIT", canonical, instr_type)`. Hardcode in `BybitExecutor.place_market_order`.
4. **Mark-price source must be deterministic.** Two callsites pulling from different caches can disagree on min-notional pass/fail.
5. **Don't mix Phase B and Phase C.** Lot rounding for MEXC/Bybit/BloFin is Tasks 13–14 (warn first). In Task 11 those three executors keep existing logic — only OKX uses registry-rounded sizes.

---

## Task 12 — Populate `OrderStatus` end-to-end via normalizers

**Why high-risk:** Mistakes silently corrupt audit log. `PARTIALLY_FILLED` mistakenly classified as `FILLED` causes under-tracked exposure forever.

### Files

- **Modify:** `deploy-live/normalizers.py` — replace each `_norm_<exchange>_order` body.
- **Create:** `deploy-live/tests/fixtures/orders/{okx,bybit,mexc,blofin}/{filled,partial,rejected,cancelled,silent_zero}.json` — synthetic v1.
- **Modify:** `deploy-live/tests/test_normalizers.py`.

### Sub-step 12.1 — Shared classifier

```python
# normalizers.py
from schemas import OrderStatus, RejectionCode

FILLED_TOLERANCE = Decimal("0.99")  # D8

def _classify(requested_usd: Decimal, filled_usd: Decimal,
              raw_status_indicates_failure: bool,
              raw_status_indicates_cancel: bool) -> OrderStatus:
    if filled_usd <= 0:
        if raw_status_indicates_cancel: return OrderStatus.CANCELLED
        if raw_status_indicates_failure: return OrderStatus.REJECTED
        return OrderStatus.UNKNOWN
    if filled_usd >= requested_usd * FILLED_TOLERANCE:
        return OrderStatus.FILLED
    return OrderStatus.PARTIALLY_FILLED
```

### Sub-step 12.2 — `map_rejection`

```python
def map_rejection(exchange: str, raw: dict) -> tuple[RejectionCode, str]:
    code_str = str(raw.get("code") or raw.get("retCode") or "")
    msg_str = str(raw.get("msg") or raw.get("retMsg") or "")[:500]

    if exchange == "OKX":
        if code_str == "51155": return RejectionCode.COMPLIANCE_BLOCKED, msg_str
        if code_str == "51008": return RejectionCode.INSUFFICIENT_BALANCE, msg_str
        if code_str == "51000" and "size" in msg_str.lower():
            return RejectionCode.LOT_SIZE, msg_str
    elif exchange == "BYBIT":
        if code_str == "110007": return RejectionCode.INSUFFICIENT_BALANCE, msg_str
    elif exchange == "MEXC":
        if code_str == "30005": return RejectionCode.INSUFFICIENT_BALANCE, msg_str
        if code_str == "30001": return RejectionCode.LOT_SIZE, msg_str

    if exchange == "BLOFIN" and raw.get("state") == "filled" and float(raw.get("filledSize", 0)) == 0:
        return RejectionCode.SILENT_ZERO_FILL, "BloFin reported state=filled with zero filledSize"
    if exchange == "MEXC" and raw.get("status") == "FILLED" and float(raw.get("cummulativeQuoteQty", 0)) == 0:
        return RejectionCode.SILENT_ZERO_FILL, "MEXC reported FILLED with zero cummulativeQuoteQty"
    if raw.get("__http_status") == 429:
        return RejectionCode.RATE_LIMITED, msg_str

    return RejectionCode.UNKNOWN, msg_str or code_str or "unparseable"
```

### Sub-step 12.3 — Per-exchange normalizers

OKX example (others follow same skeleton):

```python
def normalize_okx_order(raw, registry, requested_usd, received_at_ms) -> ExchangeOrderResponse:
    venue_sym = str(raw.get("instId", ""))
    canonical = registry.to_canonical("OKX", venue_sym).value
    state = str(raw.get("state", "")).lower()

    fill_price = float(raw.get("avgPx") or 0) or None
    filled_contracts = float(raw.get("accFillSz") or 0)
    instr = registry.get("OKX", CanonicalSymbol(canonical))
    contract_size = float(instr.contract_size) if instr else 1.0
    filled_usd = filled_contracts * (fill_price or 0) * contract_size

    fail = state in ("rejected", "failed") or raw.get("code") not in ("0", "", None)
    cancel = state in ("canceled", "mmp_canceled")
    status = _classify(Decimal(str(requested_usd)), Decimal(str(filled_usd)), fail, cancel)
    if state == "live": status = OrderStatus.SUBMITTED

    rej_code, rej_msg = (None, None)
    if status == OrderStatus.REJECTED:
        rej_code, rej_msg = map_rejection("OKX", raw)

    return ExchangeOrderResponse(
        exchange="OKX",
        order_id=str(raw.get("ordId", "")),
        side="buy" if str(raw.get("side", "")).lower() == "buy" else "sell",
        requested_size_usd=requested_usd,
        filled_size_usd=filled_usd,
        fill_price=fill_price,
        fees_usd=abs(float(raw.get("fee", 0))),  # OKX fees are negative
        status=status,
        rejection_code=rej_code,
        rejection_message=rej_msg,
        raw=raw,
        received_at_ms=received_at_ms,
        exchange_ts_ms=int(raw.get("uTime", 0)) or None,
    )
```

Apply same skeleton to `normalize_bybit_order`, `normalize_mexc_order`, `normalize_blofin_order`. Each differs in:
- Field names for status/price/size.
- USD computation (Bybit spot: `price × size`; MEXC perp: `cummulativeQuoteQty`; BloFin: `filledSize`).
- Failure/cancel flag derivation.

### Tests

```python
@pytest.mark.parametrize("exchange,fn", [
    ("okx", normalize_okx_order),
    ("bybit", normalize_bybit_order),
    ("mexc", normalize_mexc_order),
    ("blofin", normalize_blofin_order),
])
@pytest.mark.parametrize("case,expected_status,expected_rej", [
    ("filled", OrderStatus.FILLED, None),
    ("partial", OrderStatus.PARTIALLY_FILLED, None),
    ("rejected", OrderStatus.REJECTED, RejectionCode.INSUFFICIENT_BALANCE),
    ("cancelled", OrderStatus.CANCELLED, None),
    ("silent_zero", OrderStatus.REJECTED, RejectionCode.SILENT_ZERO_FILL),
])
def test_status_mapping(reg, exchange, fn, case, expected_status, expected_rej):
    fixture = json.loads((ORD_DIR / exchange / f"{case}.json").read_text())
    resp = fn(fixture["raw"], registry=reg,
              requested_usd=fixture["requested_usd"], received_at_ms=1700000000000)
    assert resp.status == expected_status
    if expected_rej is not None:
        assert resp.rejection_code == expected_rej
```

### Acceptance criteria

- [ ] 5 fixtures × 4 exchanges = 20 captured order-response files committed (synthetic v1; replaced with real captures in paper-gate).
- [ ] All 20 mapping tests pass.
- [ ] `success` shim still works: `resp.success == (resp.status in {FILLED, PARTIALLY_FILLED})`.
- [ ] Paper trader 1 day post-merge: every `ExchangeOrderResponse.status` is non-`None`. Grep audit log for `"status": null` returns zero.

### Gotchas

1. **OKX returns negative fees** (`-0.5`). `abs()` before storing.
2. **Bybit `orderStatus` is case-mixed** — `"PartiallyFilled"`, `"PartiallyFilledCanceled"`. Lowercase before matching.
3. **MEXC `status=FILLED` is a lie** when `cummulativeQuoteQty == 0`. Always check both.
4. **BloFin same trick:** `state=filled, filledSize=0` is silent failure, NOT successful 0-USD fill.
5. **OKX `code="0"` means success.** Don't invert.
6. **Don't fall through to `OrderStatus.UNKNOWN` silently** — emit warning log; investigate every occurrence.
7. **Status precedence:** Some exchanges return `status="filled"` and `code != 0` simultaneously (rare; partial + late error). Treat size as truth: `filled_usd > 0` → fill regardless of code.

---

## Task 16 — Partial-fill handling in `LivePosition` open path (D2)

**Why high-risk:** First task where new schema changes trader behavior. Wrong sizing here = real exposure error.

### Files

- **Modify:** `deploy-live/real_trader.py` — `LivePosition.open` path.
- **Modify:** `deploy-live/reconciler.py` — emit `partial_fill_accepted` event.
- **Create:** `deploy-live/tests/test_partial_fill.py`.

### Sub-step 16.1 — Locate the open path

```bash
rg -n 'place_market_order|LivePosition\(' deploy-live/real_trader.py
```

Find:
```python
result_a = await executor_a.place_market_order(...)
result_b = await executor_b.place_market_order(...)
if result_a.success and result_b.success:
    position = LivePosition(size_usd=requested_usd, ...)  # BUG: assumes full fill
```

### Sub-step 16.2 — D2 implementation

```python
async def _open_two_leg(self, canonical, requested_usd, mark_a, mark_b, ...):
    result_a, result_b = await asyncio.gather(
        executor_a.place_market_order(canonical, "buy", requested_usd, mark_a),
        executor_b.place_market_order(canonical, "sell", requested_usd, mark_b),
    )

    if result_a.status not in (OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED):
        await self._unwind_leg(result_b, reason="leg_a_failed")
        return None
    if result_b.status not in (OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED):
        await self._unwind_leg(result_a, reason="leg_b_failed")
        return None

    # D2: accept reduced size = MIN of the two filled sides.
    filled_a = Decimal(str(result_a.filled_size_usd))
    filled_b = Decimal(str(result_b.filled_size_usd))
    accepted = min(filled_a, filled_b)

    if filled_a != filled_b:
        # Unwind excess on the larger leg to restore symmetry:
        excess_leg = "a" if filled_a > filled_b else "b"
        excess_usd = abs(filled_a - filled_b)
        await self._unwind_partial(excess_leg, excess_usd, reason="leg_imbalance")

    if accepted < Decimal(str(requested_usd)):
        self.reconciler.emit_event(
            severity="info", category="partial_fill_accepted",
            symbol=canonical.value,
            payload={
                "requested_usd": float(requested_usd),
                "accepted_usd": float(accepted),
                "leg_a_filled_usd": float(filled_a),
                "leg_b_filled_usd": float(filled_b),
                "exchange_a": result_a.exchange,
                "exchange_b": result_b.exchange,
            },
        )

    position = LivePosition(
        canonical=canonical,
        size_usd=float(accepted),                    # KEY: not requested_usd
        requested_size_usd=float(requested_usd),     # NEW for Task 17
        leg_a_order_id=result_a.order_id,
        leg_b_order_id=result_b.order_id,
        ...
    )
    return position
```

### Sub-step 16.3 — Add `requested_size_usd` to `LivePosition`

```python
@dataclass
class LivePosition:
    ...
    size_usd: float                 # actual filled (D2)
    requested_size_usd: float       # NEW for Task 17 reconciler check
```

Persist in `state_store` (additive migration; default to `size_usd` for legacy rows).

### Tests

```python
@pytest.mark.asyncio
async def test_50pct_partial_scales_position(trader_with_mocks):
    trader, mocks = trader_with_mocks
    mocks.exec_a.place_market_order = AsyncMock(
        return_value=_resp("OKX", OrderStatus.PARTIALLY_FILLED, 100, 50)
    )
    mocks.exec_b.place_market_order = AsyncMock(
        return_value=_resp("BYBIT", OrderStatus.FILLED, 100, 50)
    )
    position = await trader._open_two_leg(canonical, requested_usd=Decimal("100"), ...)
    assert position is not None
    assert position.size_usd == 50.0
    assert position.requested_size_usd == 100.0

@pytest.mark.asyncio
async def test_leg_imbalance_unwinds_excess(trader_with_mocks):
    trader, mocks = trader_with_mocks
    mocks.exec_a.place_market_order = AsyncMock(
        return_value=_resp("OKX", OrderStatus.FILLED, 100, 100))
    mocks.exec_b.place_market_order = AsyncMock(
        return_value=_resp("BYBIT", OrderStatus.PARTIALLY_FILLED, 100, 80))
    position = await trader._open_two_leg(canonical, Decimal("100"), ...)
    assert position.size_usd == 80.0
    mocks.unwind_partial.assert_awaited_once_with("a", Decimal("20"), reason="leg_imbalance")

@pytest.mark.asyncio
async def test_995pct_treated_as_filled(trader_with_mocks):
    """D8: 99.5% fill is FILLED, no event."""
    trader, mocks = trader_with_mocks
    mocks.exec_a.place_market_order = AsyncMock(
        return_value=_resp("OKX", OrderStatus.FILLED, 100, 99.5))
    mocks.exec_b.place_market_order = AsyncMock(
        return_value=_resp("BYBIT", OrderStatus.FILLED, 100, 99.5))
    await trader._open_two_leg(canonical, Decimal("100"), ...)
    mocks.reconciler.emit_event.assert_not_called()

@pytest.mark.asyncio
async def test_silent_zero_fill_does_not_open_position(trader_with_mocks):
    trader, mocks = trader_with_mocks
    mocks.exec_a.place_market_order = AsyncMock(
        return_value=_resp("BLOFIN", OrderStatus.REJECTED, 100, 0))
    mocks.exec_b.place_market_order = AsyncMock(
        return_value=_resp("BYBIT", OrderStatus.FILLED, 100, 100))
    position = await trader._open_two_leg(canonical, Decimal("100"), ...)
    assert position is None
    mocks.unwind_leg.assert_awaited_once()
```

### Acceptance criteria

- [ ] All 5 partial-fill tests pass.
- [ ] `LivePosition.size_usd == min(leg_a_filled, leg_b_filled)` post-open.
- [ ] `LivePosition.requested_size_usd` populated; persisted in SQLite.
- [ ] Leg imbalance triggers single `_unwind_partial` with correct excess.
- [ ] `partial_fill_accepted` emitted exactly when `accepted < requested * 0.99`.
- [ ] 24h paper run with `OrderStatus.PARTIALLY_FILLED` injected → no naked exposure visible.

### Gotchas

1. **`min(filled_a, filled_b)`, not `(a+b)/2`** — average books exposure that doesn't exist.
2. **Unwind path is fresh market order on larger-filled venue.** If THAT partial-fills, recursive problem. v1 punt: assume unwind succeeds; if not, `unwind_failed` critical event fires and human deals with it.
3. **Don't emit `partial_fill_accepted` when both legs filled identically below requested** — that's symmetric and well-tracked. Only emit when `accepted < requested * 0.99`.
4. **`requested_size_usd` field on `LivePosition`** — Task 17's reconciler uses this. Don't skip thinking `size_usd` is enough.
5. **Unwind BEFORE record.** Crash between record and unwind = bot believes it has leg-balanced position when it doesn't. Either: place → measure → unwind → record, OR wrap recording + unwind in SQLite transaction. Recommend transaction.
6. **D2 says no chase** — do NOT add code that retries the under-filled leg's remainder. Auto-unwind of *over-filled* leg is different and necessary.

---

## Patterns for tasks not detailed

- **Tasks 1–4** (additive schema, SQLite migration): mechanical. Follow Pydantic patterns from existing `schemas.py`. SQLite migration is `ALTER TABLE ... ADD COLUMN ... DEFAULT NULL`.
- **Task 8** (`round_size`/`round_price`): small additions to `InstrumentRegistry`. Already shown in Task 10. Unit tests against populated fixtures.
- **Task 9** (startup wiring): ~30 lines. Instantiate `InstrumentRegistry` early, call `await registry.refresh_all(http)`. On `InstrumentRefreshError` → log + `sys.exit(1)`. Add hourly refresh `asyncio.Task`. Test: refresh-failure path exits cleanly.
- **Tasks 13–14** (lot enforcement on Bybit/MEXC/BloFin in warn mode): mirror Task 10's pattern per exchange. Each gets its own size-parity test with synthetic v1 fixtures, real validation in paper-gate. `SIZE_ENFORCEMENT=warn` gates the new rounding (logs divergence; submits original).
- **Task 15** (flip enforce): one env var default change. Acceptance test: rejected orders never hit network.
- **Task 17** (reconciler `partial_fill_unhandled`): ~20 lines in `reconciler.py`. After T+30s, any open position with `summed_fills < requested * 0.99` raises warning. One test fixture.
- **Task 18** (stale + open position): `registry.supports()` short-circuit at strategy entry. One test simulating stale + open.
- **Task 19** (remove `success` shim): `git rm` the property; `rg success` post-check returns 0 in non-test code.
