# SpreadWatch Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Standalone local tool showing live + recorded cross-exchange spreads (fee-adjusted, depth-aware, grabbability-scored) for USDT perps and pre-IPO perps across 7 exchanges.

**Architecture:** Single Python asyncio process. WS L2 streams (Binance/Bybit/OKX/Bitget/Gate) + budgeted REST polling (MEXC/BloFin) feed an in-memory BookStore; a 1s scan engine computes executable/net spreads and grabbability per exchange-pair, records full tick history to daily SQLite files, and pushes live state to a FastAPI-served vanilla-JS dashboard.

**Tech Stack:** Python 3.12+, asyncio, aiohttp (WS + REST), FastAPI + uvicorn, SQLite (stdlib sqlite3), ccxt (symbol discovery only), pytest + pytest-asyncio. Frontend: vanilla JS + uPlot (charts), no build step.

**Spec:** `docs/superpowers/specs/2026-08-03-spreadwatch-design.md` (in the Claude Paperclip repo)

**IMPORTANT — project location:** Everything is built in a NEW folder `/Users/vandenboogaard/Claude projects/spreadwatch/` with its own git repo. All file paths below are relative to that root. This plan and the spec live in the Paperclip repo; copy the spec into `spreadwatch/docs/` in Task 1 so the new repo is self-contained.

---

## File Structure

```
spreadwatch/
├── run.py                          # entry point: wires collector + engine + API
├── config.yaml                     # user config (sizes, fees, retention, pre-IPO list)
├── requirements.txt
├── docs/design-spec.md             # copy of the design spec
├── spreadwatch/
│   ├── __init__.py
│   ├── config.py                   # load config.yaml + defaults
│   ├── core/
│   │   ├── __init__.py
│   │   ├── models.py               # Book, SpreadTick, Opportunity dataclasses
│   │   ├── spread.py               # vwap_fill, executable_spread_pct, depth_usd
│   │   ├── fees.py                 # taker fee table + total_fees_pct
│   │   ├── grabbability.py         # score_grabbability (pure)
│   │   ├── lifecycle.py            # OpportunityTracker (open/peak/close)
│   │   └── engine.py               # ScanEngine: per-second pair comparison
│   ├── collector/
│   │   ├── __init__.py
│   │   ├── bookstore.py            # in-memory books w/ per-book ts + staleness
│   │   ├── adapters.py             # per-exchange WS subscribe/parse functions
│   │   ├── ws_manager.py           # generic WS client w/ reconnect+backoff
│   │   ├── rest_poller.py          # MEXC/BloFin budgeted depth polling
│   │   └── discovery.py            # ccxt symbol discovery + pre-IPO tagging
│   ├── storage/
│   │   ├── __init__.py
│   │   ├── db.py                   # daily SQLite files, schema, batched writes
│   │   └── queries.py              # history/rollup/ranking queries
│   ├── api/
│   │   ├── __init__.py
│   │   └── app.py                  # FastAPI: REST + WS push + static files
│   └── web/
│       ├── index.html              # matrix view + detail drawer
│       ├── history.html            # history/research page
│       ├── app.js
│       ├── history.js
│       └── style.css
└── tests/
    ├── test_spread.py
    ├── test_fees.py
    ├── test_grabbability.py
    ├── test_lifecycle.py
    ├── test_engine.py
    ├── test_bookstore.py
    ├── test_adapters.py
    ├── test_rest_poller.py
    ├── test_db.py
    └── test_queries.py
```

---

### Task 1: Project scaffold

**Files:**
- Create: `run.py` (stub), `config.yaml`, `requirements.txt`, `.gitignore`, `spreadwatch/__init__.py`, `spreadwatch/config.py`, all package `__init__.py` files, `tests/test_config.py`
- Copy: spec → `docs/design-spec.md`

- [ ] **Step 1: Create folder, git repo, venv, install deps**

```bash
mkdir -p "/Users/vandenboogaard/Claude projects/spreadwatch"
cd "/Users/vandenboogaard/Claude projects/spreadwatch"
git init
mkdir -p spreadwatch/core spreadwatch/collector spreadwatch/storage spreadwatch/api spreadwatch/web tests docs data/ticks
touch spreadwatch/__init__.py spreadwatch/core/__init__.py spreadwatch/collector/__init__.py spreadwatch/storage/__init__.py spreadwatch/api/__init__.py
cp "/Users/vandenboogaard/Claude projects/Claude Paperclip/docs/superpowers/specs/2026-08-03-spreadwatch-design.md" docs/design-spec.md
python3 -m venv .venv
.venv/bin/pip install aiohttp fastapi "uvicorn[standard]" ccxt pyyaml pytest pytest-asyncio
```

- [ ] **Step 2: Write `requirements.txt` and `.gitignore`**

`requirements.txt`:
```
aiohttp>=3.9
fastapi>=0.110
uvicorn[standard]>=0.29
ccxt>=4.3
pyyaml>=6.0
pytest>=8.0
pytest-asyncio>=0.23
```

`.gitignore`:
```
.venv/
__pycache__/
data/
*.pyc
.pytest_cache/
```

- [ ] **Step 3: Write `config.yaml`**

```yaml
target_size_usd: 25.0
min_profit_margin_pct: 0.05
max_sane_spread_pct: 10.0
fill_window_s: 0.85
sync_tolerance_ms: 250
velocity_k: 1.0
staleness_cutoff_s: 3.0
scan_interval_s: 1.0
retention_days: 30
port: 8377
rest_budget_per_cycle: 20        # MEXC+BloFin depth calls per scan cycle
grab_weights:
  duration: 1.0
  depth: 1.0
  margin: 1.0
  velocity: 1.0
  sync: 1.0
taker_fees_pct:                  # per leg, percent
  binance: 0.05
  bybit: 0.055
  okx: 0.05
  bitget: 0.06
  gate: 0.05
  mexc: 0.02
  blofin: 0.06
exchanges: [binance, bybit, okx, bitget, gate, mexc, blofin]
pre_ipo_symbols: []              # e.g. [SPACEX, OPENAI] — verify listings first
```

- [ ] **Step 4: Write failing test for config loading** — `tests/test_config.py`

```python
from spreadwatch.config import load_config

def test_load_config_defaults(tmp_path):
    cfg = load_config(str(tmp_path / "missing.yaml"))
    assert cfg["target_size_usd"] == 25.0
    assert cfg["port"] == 8377
    assert cfg["taker_fees_pct"]["mexc"] == 0.02

def test_load_config_override(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text("target_size_usd: 100\n")
    cfg = load_config(str(p))
    assert cfg["target_size_usd"] == 100
    assert cfg["port"] == 8377  # non-overridden keys keep defaults
```

- [ ] **Step 5: Run test, verify FAIL** — `.venv/bin/pytest tests/test_config.py -v` → ImportError

- [ ] **Step 6: Implement `spreadwatch/config.py`**

```python
import copy, os
import yaml

DEFAULTS = {
    "target_size_usd": 25.0,
    "min_profit_margin_pct": 0.05,
    "max_sane_spread_pct": 10.0,
    "fill_window_s": 0.85,
    "sync_tolerance_ms": 250,
    "velocity_k": 1.0,
    "staleness_cutoff_s": 3.0,
    "scan_interval_s": 1.0,
    "retention_days": 30,
    "port": 8377,
    "rest_budget_per_cycle": 20,
    "grab_weights": {"duration": 1.0, "depth": 1.0, "margin": 1.0,
                     "velocity": 1.0, "sync": 1.0},
    "taker_fees_pct": {"binance": 0.05, "bybit": 0.055, "okx": 0.05,
                       "bitget": 0.06, "gate": 0.05, "mexc": 0.02,
                       "blofin": 0.06},
    "exchanges": ["binance", "bybit", "okx", "bitget", "gate", "mexc", "blofin"],
    "pre_ipo_symbols": [],
}

def load_config(path: str = "config.yaml") -> dict:
    cfg = copy.deepcopy(DEFAULTS)
    if os.path.exists(path):
        with open(path) as f:
            user = yaml.safe_load(f) or {}
        for k, v in user.items():
            if isinstance(v, dict) and isinstance(cfg.get(k), dict):
                cfg[k].update(v)
            else:
                cfg[k] = v
    return cfg
```

- [ ] **Step 7: Run test, verify PASS** — `.venv/bin/pytest tests/test_config.py -v`

- [ ] **Step 8: Stub `run.py`** (filled in Task 16)

```python
if __name__ == "__main__":
    print("spreadwatch: wiring arrives in Task 16")
```

- [ ] **Step 9: Commit**

```bash
git add -A && git commit -m "chore: scaffold spreadwatch project with config loader"
```

---

### Task 2: Core models + spread math

**Files:**
- Create: `spreadwatch/core/models.py`, `spreadwatch/core/spread.py`
- Test: `tests/test_spread.py`

- [ ] **Step 1: Write `spreadwatch/core/models.py`** (plain dataclasses, no logic — no dedicated test needed; exercised by every other test)

```python
from dataclasses import dataclass, field

@dataclass
class Book:
    bids: list  # [(price, qty)] sorted price desc
    asks: list  # [(price, qty)] sorted price asc
    ts: float   # epoch seconds the book was current

@dataclass
class SpreadTick:
    ts: float
    symbol: str
    category: str            # "perp" | "pre_ipo"
    ex_short: str            # richer exchange (sell here)
    ex_long: str             # cheaper exchange (buy here)
    raw_spread_pct: float
    exec_spread_pct: float | None
    net_spread_pct: float | None
    depth_short_usd: float
    depth_long_usd: float
    grabbability: float | None
    factors: dict | None
    ob_skew_ms: float

@dataclass
class Opportunity:
    symbol: str
    category: str
    ex_short: str
    ex_long: str
    open_ts: float
    close_ts: float | None = None
    peak_net_spread_pct: float = 0.0
    peak_min_depth_usd: float = 0.0
    peak_grabbability: float = 0.0
    peak_factors: dict = field(default_factory=dict)
    tick_count: int = 0
```

- [ ] **Step 2: Write failing tests** — `tests/test_spread.py`

```python
import pytest
from spreadwatch.core.models import Book
from spreadwatch.core.spread import vwap_fill, executable_spread_pct, depth_usd, raw_spread_pct

BIDS = [(100.0, 1.0), (99.0, 1.0)]      # $100 + $99 available
ASKS = [(101.0, 1.0), (102.0, 1.0)]

def test_vwap_fill_single_level():
    vwap, filled = vwap_fill(BIDS, 50.0)
    assert vwap == pytest.approx(100.0)
    assert filled == pytest.approx(50.0)

def test_vwap_fill_walks_levels():
    vwap, filled = vwap_fill(BIDS, 150.0)   # $100 @100 + $50 @99
    assert filled == pytest.approx(150.0)
    assert 99.0 < vwap < 100.0

def test_vwap_fill_insufficient_depth():
    vwap, filled = vwap_fill(BIDS, 1000.0)
    assert vwap is None and filled < 1000.0

def test_vwap_fill_empty():
    assert vwap_fill([], 25.0) == (None, 0.0)

def test_executable_spread_positive():
    short = Book(bids=[(102.0, 10.0)], asks=[(103.0, 10.0)], ts=0)
    long = Book(bids=[(99.0, 10.0)], asks=[(100.0, 10.0)], ts=0)
    s = executable_spread_pct(short, long, 25.0)
    assert s == pytest.approx((102.0 - 100.0) / 100.0 * 100)

def test_executable_spread_none_when_thin():
    short = Book(bids=[(102.0, 0.01)], asks=[], ts=0)   # ~$1 depth
    long = Book(bids=[], asks=[(100.0, 10.0)], ts=0)
    assert executable_spread_pct(short, long, 25.0) is None

def test_depth_usd():
    assert depth_usd(BIDS) == pytest.approx(199.0)
    assert depth_usd(BIDS, max_levels=1) == pytest.approx(100.0)

def test_raw_spread_pct():
    a = Book(bids=[(101.0, 1)], asks=[(103.0, 1)], ts=0)   # mid 102
    b = Book(bids=[(99.0, 1)], asks=[(101.0, 1)], ts=0)    # mid 100
    assert raw_spread_pct(a, b) == pytest.approx(2.0)
```

- [ ] **Step 3: Run, verify FAIL** — `.venv/bin/pytest tests/test_spread.py -v` → ImportError

- [ ] **Step 4: Implement `spreadwatch/core/spread.py`**

```python
from .models import Book

def vwap_fill(levels, size_usd):
    """Walk [(price, qty)] spending size_usd. Return (vwap, filled_usd).
    vwap is None if the full size cannot be filled."""
    remaining = size_usd
    qty_acc = 0.0
    usd_acc = 0.0
    for price, qty in levels:
        take = min(price * qty, remaining)
        usd_acc += take
        qty_acc += take / price
        remaining -= take
        if remaining <= 1e-9:
            return (usd_acc / qty_acc, usd_acc)
    return (None, usd_acc)

def executable_spread_pct(book_short: Book, book_long: Book, size_usd: float):
    """Spread realized by selling size_usd into short-side bids and buying
    from long-side asks. None if either side lacks depth."""
    sell_vwap, _ = vwap_fill(book_short.bids, size_usd)
    buy_vwap, _ = vwap_fill(book_long.asks, size_usd)
    if sell_vwap is None or buy_vwap is None:
        return None
    return (sell_vwap - buy_vwap) / buy_vwap * 100.0

def depth_usd(levels, max_levels: int = 20) -> float:
    return sum(p * q for p, q in levels[:max_levels])

def mid(book: Book):
    if not book.bids or not book.asks:
        return None
    return (book.bids[0][0] + book.asks[0][0]) / 2.0

def raw_spread_pct(book_a: Book, book_b: Book):
    """Mid-price spread of a over b, percent. None if either mid missing."""
    ma, mb = mid(book_a), mid(book_b)
    if ma is None or mb is None or mb == 0:
        return None
    return (ma - mb) / mb * 100.0
```

- [ ] **Step 5: Run, verify PASS** — `.venv/bin/pytest tests/test_spread.py -v`

- [ ] **Step 6: Commit** — `git add -A && git commit -m "feat: core models and VWAP spread math"`

---

### Task 3: Fee table

**Files:**
- Create: `spreadwatch/core/fees.py`
- Test: `tests/test_fees.py`

- [ ] **Step 1: Write failing tests** — `tests/test_fees.py`

```python
import pytest
from spreadwatch.core.fees import total_fees_pct

FEES = {"mexc": 0.02, "blofin": 0.06}

def test_total_fees_sums_both_legs():
    assert total_fees_pct("mexc", "blofin", FEES) == pytest.approx(0.08)

def test_unknown_exchange_raises():
    with pytest.raises(KeyError):
        total_fees_pct("mexc", "nope", FEES)
```

- [ ] **Step 2: Run, verify FAIL** — `.venv/bin/pytest tests/test_fees.py -v`

- [ ] **Step 3: Implement `spreadwatch/core/fees.py`**

```python
def total_fees_pct(ex_a: str, ex_b: str, taker_fees_pct: dict) -> float:
    """Round-trip taker cost in percent: one taker fill on each exchange."""
    return taker_fees_pct[ex_a] + taker_fees_pct[ex_b]
```

- [ ] **Step 4: Run, verify PASS**, then **commit** — `git add -A && git commit -m "feat: taker fee table helper"`

---

### Task 4: Grabbability scorer

**Files:**
- Create: `spreadwatch/core/grabbability.py`
- Test: `tests/test_grabbability.py`

- [ ] **Step 1: Write failing tests** — `tests/test_grabbability.py`

```python
import pytest
from spreadwatch.core.grabbability import score_grabbability

KW = dict(fill_window_s=0.85, target_size_usd=25.0,
          sync_tolerance_ms=250.0, velocity_k=1.0,
          weights={"duration": 1, "depth": 1, "margin": 1, "velocity": 1, "sync": 1})

def base(**over):
    args = dict(duration_s=2.0, min_depth_usd=50.0, exec_spread_pct=0.4,
                total_fees_pct=0.1, velocity=0.0, ob_skew_ms=0.0, **KW)
    args.update(over)
    return score_grabbability(**args)

def test_bounds_and_factor_keys():
    comp, factors = base()
    assert 0 <= comp <= 100
    assert set(factors) == {"duration", "depth", "margin", "velocity", "sync"}
    assert all(0 <= v <= 100 for v in factors.values())

def test_duration_monotone():
    lo, _ = base(duration_s=0.1)
    hi, _ = base(duration_s=5.0)
    assert hi > lo

def test_depth_monotone_and_saturates():
    _, f_lo = base(min_depth_usd=5.0)
    _, f_hi = base(min_depth_usd=25.0)
    _, f_over = base(min_depth_usd=500.0)
    assert f_lo["depth"] < f_hi["depth"] == f_over["depth"] == 100

def test_margin_zero_at_breakeven():
    _, f = base(exec_spread_pct=0.1, total_fees_pct=0.1)
    assert f["margin"] == 0

def test_velocity_directions():
    _, widening = base(velocity=0.5)
    _, flat = base(velocity=0.0)
    _, collapsing = base(velocity=-0.5)
    assert widening["velocity"] > flat["velocity"] == 50 > collapsing["velocity"]

def test_sync_penalty():
    _, perfect = base(ob_skew_ms=0.0)
    _, bad = base(ob_skew_ms=250.0)
    assert perfect["sync"] == 100 and bad["sync"] == 0

def test_weights_shift_composite():
    heavy_depth = dict(KW, weights={"duration": 0, "depth": 1, "margin": 0,
                                    "velocity": 0, "sync": 0})
    comp, _ = base(min_depth_usd=25.0, **{})
    comp_d, _ = score_grabbability(duration_s=0.0, min_depth_usd=25.0,
                                   exec_spread_pct=0.1, total_fees_pct=0.1,
                                   velocity=-1.0, ob_skew_ms=999.0, **heavy_depth)
    assert comp_d == 100  # only depth counts and it saturates
```

- [ ] **Step 2: Run, verify FAIL**

- [ ] **Step 3: Implement `spreadwatch/core/grabbability.py`**

```python
def _clamp(x, lo=0.0, hi=1.0):
    return max(lo, min(hi, x))

def score_grabbability(*, duration_s, min_depth_usd, exec_spread_pct,
                       total_fees_pct, velocity, ob_skew_ms,
                       fill_window_s, target_size_usd, sync_tolerance_ms,
                       velocity_k, weights):
    """Return (composite 0-100, per-factor dict 0-100).
    velocity = EWMA of per-tick spread deltas (pct-points/tick)."""
    factors = {
        "duration": _clamp(duration_s / fill_window_s) * 100,
        "depth": _clamp(min_depth_usd / target_size_usd) * 100,
        "margin": _clamp((exec_spread_pct - total_fees_pct) /
                         total_fees_pct if total_fees_pct > 0 else 0.0) * 100,
        "velocity": _clamp(0.5 + velocity * velocity_k) * 100,
        "sync": _clamp(1.0 - ob_skew_ms / sync_tolerance_ms) * 100,
    }
    wsum = sum(weights.get(k, 0.0) for k in factors)
    if wsum == 0:
        return 0.0, factors
    composite = sum(factors[k] * weights.get(k, 0.0) for k in factors) / wsum
    return composite, factors
```

- [ ] **Step 4: Run, verify PASS**, then **commit** — `git add -A && git commit -m "feat: grabbability scorer with factor breakdown"`

---

### Task 5: Opportunity lifecycle tracker

**Files:**
- Create: `spreadwatch/core/lifecycle.py`
- Test: `tests/test_lifecycle.py`

- [ ] **Step 1: Write failing tests** — `tests/test_lifecycle.py`

```python
import pytest
from spreadwatch.core.lifecycle import OpportunityTracker
from spreadwatch.core.models import SpreadTick

def tick(net, ts, grab=50.0, depth=100.0):
    return SpreadTick(ts=ts, symbol="ALCH", category="perp",
                      ex_short="mexc", ex_long="gate",
                      raw_spread_pct=net, exec_spread_pct=net,
                      net_spread_pct=net, depth_short_usd=depth,
                      depth_long_usd=depth, grabbability=grab,
                      factors={"depth": 100.0}, ob_skew_ms=0.0)

def test_opens_above_margin_and_closes_below_zero():
    t = OpportunityTracker(min_profit_margin_pct=0.05)
    assert t.on_tick(tick(0.02, ts=1.0)) is None          # below margin: no open
    assert t.on_tick(tick(0.30, ts=2.0)) is None          # opens, nothing closed
    assert t.active_count() == 1
    closed = t.on_tick(tick(-0.01, ts=5.0))               # closes
    assert closed is not None
    assert closed.open_ts == 2.0 and closed.close_ts == 5.0
    assert closed.tick_count == 1                          # ticks while open (close tick excluded)
    assert t.active_count() == 0

def test_peaks_tracked():
    t = OpportunityTracker(min_profit_margin_pct=0.05)
    t.on_tick(tick(0.30, ts=1.0, grab=40, depth=80))
    t.on_tick(tick(0.55, ts=2.0, grab=70, depth=120))
    t.on_tick(tick(0.10, ts=3.0, grab=90, depth=60))       # still open (net > 0)
    closed = t.on_tick(tick(-0.10, ts=4.0))
    assert closed.peak_net_spread_pct == pytest.approx(0.55)
    assert closed.peak_min_depth_usd == pytest.approx(120)
    assert closed.peak_grabbability == pytest.approx(90)

def test_none_net_spread_treated_as_closed():
    t = OpportunityTracker(min_profit_margin_pct=0.05)
    t.on_tick(tick(0.30, ts=1.0))
    tk = tick(0.30, ts=2.0)
    tk.net_spread_pct = None                               # depth vanished
    closed = t.on_tick(tk)
    assert closed is not None and closed.close_ts == 2.0

def test_open_age():
    t = OpportunityTracker(min_profit_margin_pct=0.05)
    t.on_tick(tick(0.30, ts=1.0))
    assert t.open_age("ALCH", "mexc", "gate", now=3.5) == pytest.approx(2.5)
    assert t.open_age("XXX", "mexc", "gate", now=3.5) is None
```

- [ ] **Step 2: Run, verify FAIL**

- [ ] **Step 3: Implement `spreadwatch/core/lifecycle.py`**

```python
from .models import SpreadTick, Opportunity

class OpportunityTracker:
    """Tracks open opportunities per (symbol, ex_short, ex_long).
    Opens when net spread > min_profit_margin_pct; closes when net <= 0
    (or becomes None). on_tick returns the closed Opportunity, else None."""

    def __init__(self, min_profit_margin_pct: float):
        self.min_margin = min_profit_margin_pct
        self._active: dict[tuple, Opportunity] = {}

    def _key(self, t: SpreadTick):
        return (t.symbol, t.ex_short, t.ex_long)

    def on_tick(self, t: SpreadTick):
        key = self._key(t)
        opp = self._active.get(key)
        net = t.net_spread_pct
        if opp is None:
            if net is not None and net > self.min_margin:
                self._active[key] = Opportunity(
                    symbol=t.symbol, category=t.category,
                    ex_short=t.ex_short, ex_long=t.ex_long, open_ts=t.ts,
                    peak_net_spread_pct=net,
                    peak_min_depth_usd=min(t.depth_short_usd, t.depth_long_usd),
                    peak_grabbability=t.grabbability or 0.0,
                    peak_factors=dict(t.factors or {}), tick_count=1)
            return None
        if net is None or net <= 0:
            opp.close_ts = t.ts
            del self._active[key]
            return opp
        opp.tick_count += 1
        if net > opp.peak_net_spread_pct:
            opp.peak_net_spread_pct = net
        md = min(t.depth_short_usd, t.depth_long_usd)
        if md > opp.peak_min_depth_usd:
            opp.peak_min_depth_usd = md
        if (t.grabbability or 0.0) > opp.peak_grabbability:
            opp.peak_grabbability = t.grabbability
            opp.peak_factors = dict(t.factors or {})
        return None

    def active_count(self) -> int:
        return len(self._active)

    def open_age(self, symbol, ex_short, ex_long, now: float):
        opp = self._active.get((symbol, ex_short, ex_long))
        return None if opp is None else now - opp.open_ts
```

Note: the first opening tick sets `tick_count=1`; `test_opens_above_margin_and_closes_below_zero` closes on the very next tick, so `tick_count == 1`.

- [ ] **Step 4: Run, verify PASS**, then **commit** — `git add -A && git commit -m "feat: opportunity lifecycle tracker"`

---

### Task 6: Storage — daily SQLite, batched writes, retention

**Files:**
- Create: `spreadwatch/storage/db.py`
- Test: `tests/test_db.py`

- [ ] **Step 1: Write failing tests** — `tests/test_db.py`

```python
import json, os, sqlite3, time
import pytest
from spreadwatch.storage.db import Storage
from spreadwatch.core.models import SpreadTick, Opportunity

def tick(ts, net=0.3):
    return SpreadTick(ts=ts, symbol="ALCH", category="perp", ex_short="mexc",
                      ex_long="gate", raw_spread_pct=0.4, exec_spread_pct=0.35,
                      net_spread_pct=net, depth_short_usd=100, depth_long_usd=200,
                      grabbability=70.0, factors={"depth": 100}, ob_skew_ms=5.0)

def test_ticks_batched_and_flushed(tmp_path):
    s = Storage(str(tmp_path), retention_days=30)
    s.add_tick(tick(1000.0)); s.add_tick(tick(1001.0))
    s.flush()
    day = time.strftime("%Y-%m-%d", time.gmtime(1001.0))
    db = sqlite3.connect(os.path.join(tmp_path, f"{day}.db"))
    assert db.execute("SELECT COUNT(*) FROM ticks").fetchone()[0] == 2

def test_opportunity_written(tmp_path):
    s = Storage(str(tmp_path), retention_days=30)
    opp = Opportunity(symbol="ALCH", category="perp", ex_short="mexc",
                      ex_long="gate", open_ts=1000.0, close_ts=1009.0,
                      peak_net_spread_pct=0.55, peak_min_depth_usd=120.0,
                      peak_grabbability=90.0, peak_factors={"sync": 80},
                      tick_count=9)
    s.add_opportunity(opp); s.flush()
    day = time.strftime("%Y-%m-%d", time.gmtime(1000.0))
    db = sqlite3.connect(os.path.join(tmp_path, f"{day}.db"))
    row = db.execute("SELECT duration_s, peak_factors FROM opportunities").fetchone()
    assert row[0] == pytest.approx(9.0)
    assert json.loads(row[1])["sync"] == 80

def test_rollups_upsert_best_and_avg(tmp_path):
    s = Storage(str(tmp_path), retention_days=30)
    s.add_tick(tick(1000.0, net=0.2)); s.add_tick(tick(1030.0, net=0.6))
    s.flush()
    day = time.strftime("%Y-%m-%d", time.gmtime(1000.0))
    db = sqlite3.connect(os.path.join(tmp_path, f"{day}.db"))
    row = db.execute("SELECT best_net, avg_net, n FROM rollups_1m").fetchone()
    assert row[0] == pytest.approx(0.6)
    assert row[1] == pytest.approx(0.4)
    assert row[2] == 2

def test_retention_deletes_old_files(tmp_path):
    old = tmp_path / "2020-01-01.db"; old.write_bytes(b"")
    new = tmp_path / time.strftime("%Y-%m-%d.db")
    new.write_bytes(b"")
    s = Storage(str(tmp_path), retention_days=30)
    s.prune()
    assert not old.exists() and new.exists()
```

- [ ] **Step 2: Run, verify FAIL**

- [ ] **Step 3: Implement `spreadwatch/storage/db.py`**

```python
import glob, json, os, sqlite3, time

SCHEMA = """
CREATE TABLE IF NOT EXISTS ticks (
  ts REAL, symbol TEXT, category TEXT, ex_short TEXT, ex_long TEXT,
  raw_pct REAL, exec_pct REAL, net_pct REAL,
  depth_short REAL, depth_long REAL, grab REAL, factors TEXT, ob_skew_ms REAL);
CREATE INDEX IF NOT EXISTS ix_ticks ON ticks(symbol, ex_short, ex_long, ts);
CREATE TABLE IF NOT EXISTS opportunities (
  symbol TEXT, category TEXT, ex_short TEXT, ex_long TEXT,
  open_ts REAL, close_ts REAL, duration_s REAL,
  peak_net REAL, peak_min_depth REAL, peak_grab REAL,
  peak_factors TEXT, tick_count INTEGER);
CREATE TABLE IF NOT EXISTS rollups_1m (
  minute INTEGER, symbol TEXT, ex_short TEXT, ex_long TEXT,
  best_net REAL, sum_net REAL, n INTEGER, avg_net REAL,
  PRIMARY KEY (minute, symbol, ex_short, ex_long));
"""

class Storage:
    """Daily SQLite files (<dir>/YYYY-MM-DD.db, UTC). Ticks buffer in memory
    until flush() — the engine calls flush once per scan cycle."""

    def __init__(self, data_dir: str, retention_days: int):
        self.dir = data_dir
        self.retention_days = retention_days
        os.makedirs(data_dir, exist_ok=True)
        self._buf_ticks = []
        self._buf_opps = []
        self._conns: dict[str, sqlite3.Connection] = {}

    def _conn(self, ts: float) -> sqlite3.Connection:
        day = time.strftime("%Y-%m-%d", time.gmtime(ts))
        if day not in self._conns:
            c = sqlite3.connect(os.path.join(self.dir, f"{day}.db"))
            c.execute("PRAGMA journal_mode=WAL")
            c.executescript(SCHEMA)
            self._conns[day] = c
        return self._conns[day]

    def add_tick(self, t):
        self._buf_ticks.append(t)

    def add_opportunity(self, o):
        self._buf_opps.append(o)

    def flush(self):
        for t in self._buf_ticks:
            c = self._conn(t.ts)
            c.execute("INSERT INTO ticks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                      (t.ts, t.symbol, t.category, t.ex_short, t.ex_long,
                       t.raw_spread_pct, t.exec_spread_pct, t.net_spread_pct,
                       t.depth_short_usd, t.depth_long_usd, t.grabbability,
                       json.dumps(t.factors), t.ob_skew_ms))
            if t.net_spread_pct is not None:
                minute = int(t.ts // 60)
                c.execute("""INSERT INTO rollups_1m
                    (minute, symbol, ex_short, ex_long, best_net, sum_net, n, avg_net)
                    VALUES (?,?,?,?,?,?,1,?)
                    ON CONFLICT(minute, symbol, ex_short, ex_long) DO UPDATE SET
                      best_net = MAX(best_net, excluded.best_net),
                      sum_net = sum_net + excluded.sum_net,
                      n = n + 1,
                      avg_net = (sum_net + excluded.sum_net) / (n + 1)""",
                    (minute, t.symbol, t.ex_short, t.ex_long,
                     t.net_spread_pct, t.net_spread_pct, t.net_spread_pct))
        for o in self._buf_opps:
            c = self._conn(o.open_ts)
            c.execute("INSERT INTO opportunities VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                      (o.symbol, o.category, o.ex_short, o.ex_long, o.open_ts,
                       o.close_ts, (o.close_ts or o.open_ts) - o.open_ts,
                       o.peak_net_spread_pct, o.peak_min_depth_usd,
                       o.peak_grabbability, json.dumps(o.peak_factors),
                       o.tick_count))
        for c in self._conns.values():
            c.commit()
        self._buf_ticks.clear()
        self._buf_opps.clear()

    def prune(self):
        cutoff = time.time() - self.retention_days * 86400
        for path in glob.glob(os.path.join(self.dir, "*.db")):
            day = os.path.basename(path)[:-3]
            try:
                ts = time.mktime(time.strptime(day, "%Y-%m-%d"))
            except ValueError:
                continue
            if ts < cutoff:
                os.remove(path)

    def db_path_for_day(self, day: str) -> str:
        return os.path.join(self.dir, f"{day}.db")
```

- [ ] **Step 4: Run, verify PASS** — `.venv/bin/pytest tests/test_db.py -v`

- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat: daily SQLite storage with rollups and retention"`

---

### Task 7: History queries (rollups, opportunity log, rankings)

**Files:**
- Create: `spreadwatch/storage/queries.py`
- Test: `tests/test_queries.py`

- [ ] **Step 1: Write failing tests** — `tests/test_queries.py`

```python
import pytest, time
from spreadwatch.storage.db import Storage
from spreadwatch.storage.queries import spread_series, opportunity_log, rankings
from spreadwatch.core.models import SpreadTick, Opportunity

def tick(ts, net, sym="ALCH"):
    return SpreadTick(ts=ts, symbol=sym, category="perp", ex_short="mexc",
                      ex_long="gate", raw_spread_pct=net, exec_spread_pct=net,
                      net_spread_pct=net, depth_short_usd=100, depth_long_usd=100,
                      grabbability=50.0, factors={}, ob_skew_ms=0.0)

def opp(open_ts, peak_net, peak_depth, sym="ALCH"):
    return Opportunity(symbol=sym, category="perp", ex_short="mexc",
                       ex_long="gate", open_ts=open_ts, close_ts=open_ts + 10,
                       peak_net_spread_pct=peak_net, peak_min_depth_usd=peak_depth,
                       peak_grabbability=60.0, peak_factors={}, tick_count=10)

@pytest.fixture
def store(tmp_path):
    now = time.time()
    s = Storage(str(tmp_path), retention_days=30)
    s.add_tick(tick(now - 120, 0.2)); s.add_tick(tick(now - 60, 0.6))
    s.add_opportunity(opp(now - 100, peak_net=0.5, peak_depth=100))   # value 0.125 (capped at $25)
    s.add_opportunity(opp(now - 50, peak_net=1.0, peak_depth=10, sym="PNUT"))  # value 0.10
    s.flush()
    return s, now

def test_spread_series_from_rollups(store):
    s, now = store
    rows = spread_series(s, "ALCH", "mexc", "gate", start=now - 300, end=now)
    assert len(rows) == 2
    assert rows[-1]["best_net"] == pytest.approx(0.6)

def test_opportunity_log(store):
    s, now = store
    rows = opportunity_log(s, start=now - 300, end=now)
    assert {r["symbol"] for r in rows} == {"ALCH", "PNUT"}

def test_rankings_by_capped_value(store):
    s, now = store
    r = rankings(s, start=now - 300, end=now, target_size_usd=25.0)
    pairs = r["pairs"]
    assert pairs[0]["symbol"] == "ALCH"          # 0.5% × $25 = 0.125 beats 1.0% × $10 = 0.10
    assert pairs[0]["value_usd"] == pytest.approx(0.125)
    assert isinstance(r["hours"], list) and len(r["hours"]) <= 24
```

- [ ] **Step 2: Run, verify FAIL**

- [ ] **Step 3: Implement `spreadwatch/storage/queries.py`**

```python
import os, sqlite3, time

def _days_between(start: float, end: float):
    day = int(start // 86400)
    while day * 86400 <= end:
        yield time.strftime("%Y-%m-%d", time.gmtime(day * 86400))
        day += 1

def _query_days(storage, sql, params, start, end):
    out = []
    for day in _days_between(start, end):
        path = storage.db_path_for_day(day)
        if not os.path.exists(path):
            continue
        c = sqlite3.connect(path)
        c.row_factory = sqlite3.Row
        out.extend(dict(r) for r in c.execute(sql, params))
        c.close()
    return out

def spread_series(storage, symbol, ex_short, ex_long, start, end):
    return _query_days(storage,
        """SELECT minute*60 AS ts, best_net, avg_net, n FROM rollups_1m
           WHERE symbol=? AND ex_short=? AND ex_long=? AND minute BETWEEN ? AND ?
           ORDER BY minute""",
        (symbol, ex_short, ex_long, int(start // 60), int(end // 60)), start, end)

def raw_ticks(storage, symbol, ex_short, ex_long, start, end, limit=5000):
    return _query_days(storage,
        """SELECT * FROM ticks WHERE symbol=? AND ex_short=? AND ex_long=?
           AND ts BETWEEN ? AND ? ORDER BY ts LIMIT ?""",
        (symbol, ex_short, ex_long, start, end, limit), start, end)

def opportunity_log(storage, start, end, limit=500):
    rows = _query_days(storage,
        """SELECT * FROM opportunities WHERE open_ts BETWEEN ? AND ?
           ORDER BY open_ts DESC LIMIT ?""", (start, end, limit), start, end)
    return rows[:limit]

def rankings(storage, start, end, target_size_usd):
    """Opportunity value = peak_net/100 × min(peak_min_depth, target_size)."""
    opps = _query_days(storage,
        "SELECT * FROM opportunities WHERE open_ts BETWEEN ? AND ?",
        (start, end), start, end)
    pairs, hours = {}, {}
    for o in opps:
        value = o["peak_net"] / 100.0 * min(o["peak_min_depth"], target_size_usd)
        pk = (o["symbol"], o["ex_short"], o["ex_long"])
        p = pairs.setdefault(pk, {"symbol": o["symbol"], "ex_short": o["ex_short"],
                                  "ex_long": o["ex_long"], "value_usd": 0.0, "count": 0})
        p["value_usd"] += value; p["count"] += 1
        h = int(time.gmtime(o["open_ts"]).tm_hour)
        hh = hours.setdefault(h, {"hour": h, "value_usd": 0.0, "count": 0})
        hh["value_usd"] += value; hh["count"] += 1
    return {"pairs": sorted(pairs.values(), key=lambda p: -p["value_usd"]),
            "hours": sorted(hours.values(), key=lambda h: h["hour"])}
```

- [ ] **Step 4: Run, verify PASS**, then **commit** — `git add -A && git commit -m "feat: history queries — series, opportunity log, rankings"`

---

### Task 8: BookStore (shared cache + staleness)

**Files:**
- Create: `spreadwatch/collector/bookstore.py`
- Test: `tests/test_bookstore.py`

- [ ] **Step 1: Write failing tests** — `tests/test_bookstore.py`

```python
from spreadwatch.collector.bookstore import BookStore

def test_set_get_fresh():
    bs = BookStore(staleness_cutoff_s=3.0)
    bs.set_book("binance", "ALCH", bids=[(100.0, 1.0)], asks=[(101.0, 1.0)], ts=1000.0)
    b = bs.get_fresh("binance", "ALCH", now=1002.0)
    assert b is not None and b.ts == 1000.0

def test_stale_returns_none_but_raw_get_works():
    bs = BookStore(staleness_cutoff_s=3.0)
    bs.set_book("binance", "ALCH", bids=[(100.0, 1.0)], asks=[(101.0, 1.0)], ts=1000.0)
    assert bs.get_fresh("binance", "ALCH", now=1010.0) is None
    assert bs.get("binance", "ALCH") is not None

def test_exchanges_with_fresh_book():
    bs = BookStore(staleness_cutoff_s=3.0)
    bs.set_book("binance", "ALCH", [(100.0, 1)], [(101.0, 1)], ts=1000.0)
    bs.set_book("gate", "ALCH", [(99.0, 1)], [(100.0, 1)], ts=1000.0)
    bs.set_book("okx", "ALCH", [(99.0, 1)], [(100.0, 1)], ts=900.0)  # stale
    assert bs.fresh_exchanges("ALCH", now=1001.0) == ["binance", "gate"]

def test_symbols():
    bs = BookStore(staleness_cutoff_s=3.0)
    bs.set_book("binance", "ALCH", [(1, 1)], [(2, 1)], ts=0)
    bs.set_book("gate", "PNUT", [(1, 1)], [(2, 1)], ts=0)
    assert bs.symbols() == {"ALCH", "PNUT"}
```

- [ ] **Step 2: Run, verify FAIL**

- [ ] **Step 3: Implement `spreadwatch/collector/bookstore.py`**

```python
from spreadwatch.core.models import Book

class BookStore:
    """In-memory latest L2 book per (exchange, symbol), each with its own
    as-of ts. get_fresh enforces the staleness cutoff — stale books are
    never compared (spec: Error handling)."""

    def __init__(self, staleness_cutoff_s: float):
        self.cutoff = staleness_cutoff_s
        self._books: dict[tuple[str, str], Book] = {}

    def set_book(self, exchange: str, symbol: str, bids, asks, ts: float):
        self._books[(exchange, symbol)] = Book(bids=bids, asks=asks, ts=ts)

    def get(self, exchange: str, symbol: str):
        return self._books.get((exchange, symbol))

    def get_fresh(self, exchange: str, symbol: str, now: float):
        b = self._books.get((exchange, symbol))
        if b is None or now - b.ts > self.cutoff:
            return None
        return b

    def fresh_exchanges(self, symbol: str, now: float) -> list[str]:
        return sorted(ex for (ex, sym), b in self._books.items()
                      if sym == symbol and now - b.ts <= self.cutoff)

    def symbols(self) -> set[str]:
        return {sym for (_, sym) in self._books}
```

- [ ] **Step 4: Run, verify PASS**, then **commit** — `git add -A && git commit -m "feat: BookStore cache with staleness enforcement"`

---

### Task 9: WS adapters — subscribe builders + message parsers

**Files:**
- Create: `spreadwatch/collector/adapters.py`
- Test: `tests/test_adapters.py`

Each adapter converts exchange-specific WS payloads into a uniform `(inst_id, bids, asks, ts_seconds)` update. Quantities stay in the exchange's native unit (contracts or coins); the ws_manager scales by `contract_size` from discovery (Task 12). Bybit is the only delta-based feed; it merges into per-instrument state. **Note for the engineer:** the fixture payloads below encode each exchange's documented format as of this writing — during Task 16's live smoke test, verify against real traffic and adjust parsers (not tests' intent) if a field moved.

- [ ] **Step 1: Write failing tests** — `tests/test_adapters.py`

```python
import json
import pytest
from spreadwatch.collector import adapters as A

def test_binance_url_and_parse():
    url = A.binance_url(["ALCHUSDT", "PNUTUSDT"])
    assert url.startswith("wss://fstream.binance.com/stream?streams=")
    assert "alchusdt@depth20@100ms" in url
    msg = {"stream": "alchusdt@depth20@100ms",
           "data": {"e": "depthUpdate", "E": 1700000000123, "s": "ALCHUSDT",
                    "b": [["100.0", "2.0"]], "a": [["101.0", "3.0"]]}}
    upd = A.binance_parse(msg, state={})
    assert upd == [("ALCHUSDT", [(100.0, 2.0)], [(101.0, 3.0)], 1700000000.123)]

def test_bybit_subscribe_and_snapshot_delta_merge():
    subs = A.bybit_subscribe(["ALCHUSDT"])
    assert subs == [{"op": "subscribe", "args": ["orderbook.50.ALCHUSDT"]}]
    state = {}
    snap = {"topic": "orderbook.50.ALCHUSDT", "type": "snapshot",
            "ts": 1700000000000,
            "data": {"s": "ALCHUSDT", "b": [["100", "2"], ["99", "1"]],
                     "a": [["101", "1"]]}}
    upd = A.bybit_parse(snap, state)
    assert upd[0][1] == [(100.0, 2.0), (99.0, 1.0)]
    delta = {"topic": "orderbook.50.ALCHUSDT", "type": "delta",
             "ts": 1700000001000,
             "data": {"s": "ALCHUSDT", "b": [["100", "0"], ["98", "5"]],
                      "a": []}}
    upd = A.bybit_parse(delta, state)
    inst, bids, asks, ts = upd[0]
    assert bids == [(99.0, 1.0), (98.0, 5.0)]      # 100 removed, 98 added
    assert asks == [(101.0, 1.0)]                   # unchanged
    assert ts == pytest.approx(1700000001.0)

def test_okx_parse():
    subs = A.okx_subscribe(["ALCH-USDT-SWAP"])
    assert subs == [{"op": "subscribe",
                     "args": [{"channel": "books5", "instId": "ALCH-USDT-SWAP"}]}]
    msg = {"arg": {"channel": "books5", "instId": "ALCH-USDT-SWAP"},
           "data": [{"bids": [["100", "10", "0", "1"]],
                     "asks": [["101", "20", "0", "1"]], "ts": "1700000000500"}]}
    upd = A.okx_parse(msg, state={})
    assert upd == [("ALCH-USDT-SWAP", [(100.0, 10.0)], [(101.0, 20.0)], 1700000000.5)]

def test_bitget_parse():
    subs = A.bitget_subscribe(["ALCHUSDT"])
    assert subs[0]["args"][0]["channel"] == "books15"
    msg = {"action": "snapshot",
           "arg": {"instType": "USDT-FUTURES", "channel": "books15",
                   "instId": "ALCHUSDT"},
           "data": [{"bids": [["100", "1"]], "asks": [["101", "2"]],
                     "ts": "1700000000250"}]}
    upd = A.bitget_parse(msg, state={})
    assert upd == [("ALCHUSDT", [(100.0, 1.0)], [(101.0, 2.0)], 1700000000.25)]

def test_gate_subscribe_and_parse():
    subs = A.gate_subscribe(["ALCH_USDT"])
    assert subs[0]["channel"] == "futures.order_book"
    assert subs[0]["payload"] == ["ALCH_USDT", "20", "0"]
    msg = {"channel": "futures.order_book", "event": "all",
           "result": {"contract": "ALCH_USDT", "t": 1700000000750,
                      "bids": [{"p": "100", "s": 3}],
                      "asks": [{"p": "101", "s": 4}]}}
    upd = A.gate_parse(msg, state={})
    assert upd == [("ALCH_USDT", [(100.0, 3.0)], [(101.0, 4.0)], 1700000000.75)]

def test_parsers_ignore_noise():
    for parse in (A.binance_parse, A.bybit_parse, A.okx_parse,
                  A.bitget_parse, A.gate_parse):
        assert parse({"event": "pong"}, state={}) == []

def test_registry_complete():
    assert set(A.WS_ADAPTERS) == {"binance", "bybit", "okx", "bitget", "gate"}
    for a in A.WS_ADAPTERS.values():
        assert callable(a["parse"]) and callable(a["inst_id"])
```

- [ ] **Step 2: Run, verify FAIL**

- [ ] **Step 3: Implement `spreadwatch/collector/adapters.py`**

```python
"""Uniform WS adapters. Each parse(msg, state) -> list of
(inst_id, bids, asks, ts_seconds); bids desc / asks asc, native-unit sizes."""

def _levels(raw, key_p=0, key_q=1):
    return [(float(l[key_p]), float(l[key_q])) for l in raw]

# ---------- Binance USDT-M futures: partial depth snapshots via stream URL ----
def binance_url(insts):
    streams = "/".join(f"{i.lower()}@depth20@100ms" for i in insts)
    return f"wss://fstream.binance.com/stream?streams={streams}"

def binance_parse(msg, state):
    d = msg.get("data") or {}
    if d.get("e") != "depthUpdate":
        return []
    return [(d["s"], _levels(d.get("b", [])), _levels(d.get("a", [])),
             d["E"] / 1000.0)]

# ---------- Bybit linear perp: orderbook.50 snapshot + delta merge -----------
def bybit_subscribe(insts):
    return [{"op": "subscribe", "args": [f"orderbook.50.{i}" for i in insts]}]

def _bybit_merge(side_map, changes):
    for price_s, qty_s in changes:
        price, qty = float(price_s), float(qty_s)
        if qty == 0:
            side_map.pop(price, None)
        else:
            side_map[price] = qty

def bybit_parse(msg, state):
    if not str(msg.get("topic", "")).startswith("orderbook."):
        return []
    d = msg["data"]
    inst = d["s"]
    st = state.setdefault(inst, {"bids": {}, "asks": {}})
    if msg.get("type") == "snapshot":
        st["bids"] = {float(p): float(q) for p, q in d.get("b", []) if float(q) > 0}
        st["asks"] = {float(p): float(q) for p, q in d.get("a", []) if float(q) > 0}
    else:
        _bybit_merge(st["bids"], d.get("b", []))
        _bybit_merge(st["asks"], d.get("a", []))
    bids = sorted(st["bids"].items(), key=lambda x: -x[0])[:50]
    asks = sorted(st["asks"].items(), key=lambda x: x[0])[:50]
    return [(inst, bids, asks, msg["ts"] / 1000.0)]

# ---------- OKX swap: books5 full snapshots ----------------------------------
def okx_subscribe(insts):
    return [{"op": "subscribe",
             "args": [{"channel": "books5", "instId": i} for i in insts]}]

def okx_parse(msg, state):
    if msg.get("arg", {}).get("channel") != "books5" or "data" not in msg:
        return []
    inst = msg["arg"]["instId"]
    out = []
    for d in msg["data"]:
        out.append((inst, _levels(d.get("bids", [])), _levels(d.get("asks", [])),
                    float(d["ts"]) / 1000.0))
    return out

# ---------- Bitget USDT-FUTURES: books15 snapshots ---------------------------
def bitget_subscribe(insts):
    return [{"op": "subscribe",
             "args": [{"instType": "USDT-FUTURES", "channel": "books15",
                       "instId": i} for i in insts]}]

def bitget_parse(msg, state):
    if msg.get("arg", {}).get("channel") != "books15" or "data" not in msg:
        return []
    inst = msg["arg"]["instId"]
    out = []
    for d in msg["data"]:
        out.append((inst, _levels(d.get("bids", [])), _levels(d.get("asks", [])),
                    float(d["ts"]) / 1000.0))
    return out

# ---------- Gate USDT futures: order_book snapshots (depth 20) ---------------
def gate_subscribe(insts):
    import time as _t
    return [{"time": int(_t.time()), "channel": "futures.order_book",
             "event": "subscribe", "payload": [i, "20", "0"]} for i in insts]

def gate_parse(msg, state):
    if msg.get("channel") != "futures.order_book" or msg.get("event") != "all":
        return []
    r = msg["result"]
    bids = [(float(l["p"]), float(l["s"])) for l in r.get("bids", [])]
    asks = [(float(l["p"]), float(l["s"])) for l in r.get("asks", [])]
    return [(r["contract"], bids, asks, r["t"] / 1000.0)]

# ---------- Instrument-id builders (internal symbol -> exchange inst) --------
WS_ADAPTERS = {
    "binance": {"url": binance_url, "subscribe": None, "parse": binance_parse,
                "inst_id": lambda sym: f"{sym}USDT",
                "static_url": None},
    "bybit": {"url": None, "subscribe": bybit_subscribe, "parse": bybit_parse,
              "inst_id": lambda sym: f"{sym}USDT",
              "static_url": "wss://stream.bybit.com/v5/public/linear"},
    "okx": {"url": None, "subscribe": okx_subscribe, "parse": okx_parse,
            "inst_id": lambda sym: f"{sym}-USDT-SWAP",
            "static_url": "wss://ws.okx.com:8443/ws/v5/public"},
    "bitget": {"url": None, "subscribe": bitget_subscribe, "parse": bitget_parse,
               "inst_id": lambda sym: f"{sym}USDT",
               "static_url": "wss://ws.bitget.com/v2/ws/public"},
    "gate": {"url": None, "subscribe": gate_subscribe, "parse": gate_parse,
             "inst_id": lambda sym: f"{sym}_USDT",
             "static_url": "wss://fx-ws.gateio.ws/v4/ws/usdt"},
}
```

- [ ] **Step 4: Run, verify PASS** — `.venv/bin/pytest tests/test_adapters.py -v`

- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat: WS adapters for binance/bybit/okx/bitget/gate"`

---

### Task 10: WS manager (reconnect, scaling, ingestion)

**Files:**
- Create: `spreadwatch/collector/ws_manager.py`
- Test: `tests/test_ws_manager.py`

The connection loop itself is thin aiohttp glue verified in Task 16's live smoke test; the testable logic (update ingestion with contract-size scaling and inst→symbol mapping, backoff schedule) is factored into pure methods and unit-tested.

- [ ] **Step 1: Write failing tests** — `tests/test_ws_manager.py`

```python
from spreadwatch.collector.ws_manager import WSFeed, backoff_delays
from spreadwatch.collector.bookstore import BookStore

def make_feed():
    bs = BookStore(staleness_cutoff_s=3.0)
    feed = WSFeed(exchange="okx", bookstore=bs,
                  inst_to_symbol={"ALCH-USDT-SWAP": "ALCH"},
                  contract_size={"ALCH-USDT-SWAP": 10.0})
    return feed, bs

def test_ingest_scales_and_maps():
    feed, bs = make_feed()
    feed.ingest([("ALCH-USDT-SWAP", [(100.0, 2.0)], [(101.0, 3.0)], 1000.0)])
    b = bs.get("okx", "ALCH")
    assert b.bids == [(100.0, 20.0)]     # qty × contract_size
    assert b.asks == [(101.0, 30.0)]
    assert b.ts == 1000.0

def test_ingest_ignores_unknown_inst():
    feed, bs = make_feed()
    feed.ingest([("UNKNOWN-USDT-SWAP", [(1.0, 1.0)], [], 1000.0)])
    assert bs.symbols() == set()

def test_backoff_schedule():
    ds = backoff_delays()
    assert next(ds) == 1
    assert next(ds) == 2
    assert next(ds) == 4
    for _ in range(10):
        d = next(ds)
    assert d == 60                        # capped
```

- [ ] **Step 2: Run, verify FAIL**

- [ ] **Step 3: Implement `spreadwatch/collector/ws_manager.py`**

```python
import asyncio, json, logging
import aiohttp
from .adapters import WS_ADAPTERS

log = logging.getLogger("spreadwatch.ws")

def backoff_delays():
    d = 1
    while True:
        yield d
        d = min(d * 2, 60)

class WSFeed:
    """One WS connection per exchange. Auto-reconnects with exponential
    backoff; a dead feed only makes its own books go stale (spec)."""

    def __init__(self, exchange, bookstore, inst_to_symbol, contract_size):
        self.exchange = exchange
        self.adapter = WS_ADAPTERS[exchange]
        self.bookstore = bookstore
        self.inst_to_symbol = inst_to_symbol
        self.contract_size = contract_size
        self.state = {}          # adapter scratch (bybit merge)
        self.connected = False

    def ingest(self, updates):
        for inst, bids, asks, ts in updates:
            sym = self.inst_to_symbol.get(inst)
            if sym is None:
                continue
            m = self.contract_size.get(inst, 1.0)
            self.bookstore.set_book(
                self.exchange, sym,
                [(p, q * m) for p, q in bids],
                [(p, q * m) for p, q in asks], ts)

    async def run(self):
        insts = list(self.inst_to_symbol)
        delays = backoff_delays()
        while True:
            try:
                url = (self.adapter["url"](insts) if self.adapter["url"]
                       else self.adapter["static_url"])
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url, heartbeat=20) as ws:
                        if self.adapter["subscribe"]:
                            for m in self.adapter["subscribe"](insts):
                                await ws.send_json(m)
                        self.connected = True
                        delays = backoff_delays()   # reset after success
                        async for msg in ws:
                            if msg.type != aiohttp.WSMsgType.TEXT:
                                continue
                            try:
                                raw = json.loads(msg.data)
                            except ValueError:
                                continue
                            self.ingest(self.adapter["parse"](raw, self.state))
            except Exception as e:
                log.warning("%s WS error: %r", self.exchange, e)
            self.connected = False
            await asyncio.sleep(next(delays))
```

- [ ] **Step 4: Run, verify PASS**, then **commit** — `git add -A && git commit -m "feat: WS manager with reconnect and contract-size scaling"`

---

### Task 11: REST poller for MEXC + BloFin (budgeted, spread-prioritized)

**Files:**
- Create: `spreadwatch/collector/rest_poller.py`
- Test: `tests/test_rest_poller.py`

Public endpoints (no auth):
- MEXC depth: `GET https://contract.mexc.com/api/v1/contract/depth/{inst}` (inst like `ALCH_USDT`) → `{"data":{"bids":[[price,vol,ord],...],"asks":[...], "timestamp": ms}}` — vol in contracts (`contractSize` scaling applies).
- MEXC tickers (all, 1 call): `GET https://contract.mexc.com/api/v1/contract/ticker` → `{"data":[{"symbol":"ALCH_USDT","bid1":...,"ask1":...},...]}`
- BloFin depth: `GET https://openapi.blofin.com/api/v1/market/books?instId=ALCH-USDT&size=20` → `{"data":[{"bids":[["price","size"]],"asks":[...],"ts":"ms"}]}` — size in contracts.
- BloFin tickers: `GET https://openapi.blofin.com/api/v1/market/tickers` → `{"data":[{"instId":"ALCH-USDT","bidPrice":...,"askPrice":...}]}`

Strategy per cycle (the Paperclip "tiered attention" pattern): 2 cheap ticker calls give top-of-book for ALL symbols → store as 1-level books (fine for raw spread); the depth budget (`rest_budget_per_cycle`, default 20) is spent on the symbols whose raw ticker spread vs any fresh other-exchange book is widest. A 429 halves the budget for 60s.

- [ ] **Step 1: Write failing tests** — `tests/test_rest_poller.py` (parsing + prioritization + budget are pure; HTTP fetch is thin glue)

```python
import pytest
from spreadwatch.collector.rest_poller import (parse_mexc_depth, parse_blofin_depth,
    parse_mexc_tickers, parse_blofin_tickers, pick_depth_targets, RestBudget)
from spreadwatch.collector.bookstore import BookStore

def test_parse_mexc_depth():
    raw = {"data": {"bids": [[100.0, 5, 1]], "asks": [[101.0, 6, 1]],
                    "timestamp": 1700000000123}}
    bids, asks, ts = parse_mexc_depth(raw)
    assert bids == [(100.0, 5.0)] and asks == [(101.0, 6.0)]
    assert ts == pytest.approx(1700000000.123)

def test_parse_blofin_depth():
    raw = {"data": [{"bids": [["100", "5"]], "asks": [["101", "6"]],
                     "ts": "1700000000500"}]}
    bids, asks, ts = parse_blofin_depth(raw)
    assert bids == [(100.0, 5.0)] and ts == pytest.approx(1700000000.5)

def test_parse_tickers():
    m = parse_mexc_tickers({"data": [{"symbol": "ALCH_USDT", "bid1": 100.0,
                                      "ask1": 101.0}]})
    assert m == {"ALCH_USDT": (100.0, 101.0)}
    b = parse_blofin_tickers({"data": [{"instId": "ALCH-USDT",
                                        "bidPrice": "100", "askPrice": "101"}]})
    assert b == {"ALCH-USDT": (100.0, 101.0)}

def test_pick_depth_targets_prefers_wide_spreads():
    bs = BookStore(staleness_cutoff_s=3.0)
    bs.set_book("binance", "ALCH", [(95.0, 10)], [(95.1, 10)], ts=1000.0)  # ~5% vs ticker
    bs.set_book("binance", "PNUT", [(100.0, 10)], [(100.1, 10)], ts=1000.0)  # ~0%
    tickers = {"ALCH": (100.0, 100.2), "PNUT": (100.0, 100.2)}
    targets = pick_depth_targets(tickers, bs, exchange="mexc", now=1001.0, budget=1)
    assert targets == ["ALCH"]

def test_budget_halves_on_429_and_recovers():
    b = RestBudget(base=20, halve_for_s=60.0)
    assert b.current(now=0.0) == 20
    b.on_429(now=10.0)
    assert b.current(now=11.0) == 10
    assert b.current(now=80.0) == 20
```

- [ ] **Step 2: Run, verify FAIL**

- [ ] **Step 3: Implement `spreadwatch/collector/rest_poller.py`**

```python
import asyncio, logging, time
import aiohttp
from spreadwatch.core.spread import mid

log = logging.getLogger("spreadwatch.rest")

# ---------- pure parsers -----------------------------------------------------
def parse_mexc_depth(raw):
    d = raw["data"]
    bids = [(float(l[0]), float(l[1])) for l in d.get("bids", [])]
    asks = [(float(l[0]), float(l[1])) for l in d.get("asks", [])]
    return bids, asks, d["timestamp"] / 1000.0

def parse_blofin_depth(raw):
    d = raw["data"][0]
    bids = [(float(p), float(q)) for p, q in d.get("bids", [])]
    asks = [(float(p), float(q)) for p, q in d.get("asks", [])]
    return bids, asks, float(d["ts"]) / 1000.0

def parse_mexc_tickers(raw):
    return {t["symbol"]: (float(t["bid1"]), float(t["ask1"]))
            for t in raw.get("data", [])
            if t.get("bid1") is not None and t.get("ask1") is not None}

def parse_blofin_tickers(raw):
    return {t["instId"]: (float(t["bidPrice"]), float(t["askPrice"]))
            for t in raw.get("data", [])
            if t.get("bidPrice") and t.get("askPrice")}

# ---------- prioritization ---------------------------------------------------
def pick_depth_targets(tickers_by_symbol, bookstore, exchange, now, budget):
    """Rank symbols by widest |raw spread| between this exchange's ticker mid
    and any OTHER exchange's fresh book mid; return top `budget` symbols."""
    scored = []
    for sym, (bid, ask) in tickers_by_symbol.items():
        my_mid = (bid + ask) / 2.0
        if my_mid <= 0:
            continue
        best = 0.0
        for other_ex in bookstore.fresh_exchanges(sym, now):
            if other_ex == exchange:
                continue
            b = bookstore.get_fresh(other_ex, sym, now)
            om = mid(b)
            if om:
                best = max(best, abs(my_mid - om) / om * 100.0)
        scored.append((best, sym))
    scored.sort(reverse=True)
    return [sym for _, sym in scored[:budget]]

class RestBudget:
    def __init__(self, base: int, halve_for_s: float = 60.0):
        self.base = base
        self.halve_for_s = halve_for_s
        self._halved_until = 0.0

    def on_429(self, now: float):
        self._halved_until = now + self.halve_for_s

    def current(self, now: float) -> int:
        return self.base // 2 if now < self._halved_until else self.base

# ---------- poller -----------------------------------------------------------
MEXC = {"tickers": "https://contract.mexc.com/api/v1/contract/ticker",
        "depth": "https://contract.mexc.com/api/v1/contract/depth/{inst}",
        "inst_id": lambda s: f"{s}_USDT", "parse_depth": parse_mexc_depth,
        "parse_tickers": parse_mexc_tickers}
BLOFIN = {"tickers": "https://openapi.blofin.com/api/v1/market/tickers",
          "depth": "https://openapi.blofin.com/api/v1/market/books?instId={inst}&size=20",
          "inst_id": lambda s: f"{s}-USDT", "parse_depth": parse_blofin_depth,
          "parse_tickers": parse_blofin_tickers}
REST_EXCHANGES = {"mexc": MEXC, "blofin": BLOFIN}

class RestPoller:
    def __init__(self, exchange, bookstore, symbols, contract_size,
                 budget: RestBudget, interval_s: float = 1.0):
        self.exchange = exchange
        self.cfg = REST_EXCHANGES[exchange]
        self.bookstore = bookstore
        self.symbols = symbols            # internal symbols this exchange lists
        self.contract_size = contract_size  # inst -> multiplier
        self.budget = budget
        self.interval_s = interval_s
        self.connected = False

    async def _get_json(self, session, url):
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
            if r.status == 429:
                self.budget.on_429(time.time())
                return None
            r.raise_for_status()
            return await r.json()

    async def run(self):
        inst_of = self.cfg["inst_id"]
        my_insts = {inst_of(s): s for s in self.symbols}
        async with aiohttp.ClientSession() as session:
            while True:
                started = time.time()
                try:
                    raw = await self._get_json(session, self.cfg["tickers"])
                    tickers = {}
                    if raw:
                        for inst, (bid, ask) in self.cfg["parse_tickers"](raw).items():
                            sym = my_insts.get(inst)
                            if sym:
                                tickers[sym] = (bid, ask)
                                # 1-level book from ticker: raw spread for all
                                self.bookstore.set_book(self.exchange, sym,
                                    [(bid, 0.0)], [(ask, 0.0)], time.time())
                    now = time.time()
                    targets = pick_depth_targets(tickers, self.bookstore,
                                                 self.exchange, now,
                                                 self.budget.current(now))
                    for sym in targets:
                        inst = inst_of(sym)
                        url = self.cfg["depth"].format(inst=inst)
                        raw = await self._get_json(session, url)
                        if raw is None:
                            break            # 429 — stop this cycle
                        bids, asks, ts = self.cfg["parse_depth"](raw)
                        m = self.contract_size.get(inst, 1.0)
                        self.bookstore.set_book(self.exchange, sym,
                            [(p, q * m) for p, q in bids],
                            [(p, q * m) for p, q in asks], ts)
                    self.connected = True
                except Exception as e:
                    self.connected = False
                    log.warning("%s REST error: %r", self.exchange, e)
                await asyncio.sleep(max(0.0, self.interval_s - (time.time() - started)))
```

Note: ticker-derived books have qty 0.0 → `executable_spread_pct` returns None for them, so only raw spread shows until a real depth pull lands. That is intended (matrix dims exec/net columns for those rows).

- [ ] **Step 4: Run, verify PASS**, then **commit** — `git add -A && git commit -m "feat: budgeted spread-prioritized REST poller for MEXC/BloFin"`

---

### Task 12: Symbol discovery (ccxt) + pre-IPO tagging

**Files:**
- Create: `spreadwatch/collector/discovery.py`
- Test: `tests/test_discovery.py`

ccxt is used ONLY here (startup + hourly refresh), never in the hot path. The pure selection logic is tested; the ccxt fetch is thin glue.

- [ ] **Step 1: Write failing tests** — `tests/test_discovery.py`

```python
from spreadwatch.collector.discovery import build_universe

# markets_by_exchange: {exchange: {base_symbol: contract_size}}
MARKETS = {
    "binance": {"ALCH": 1.0, "PNUT": 1.0, "ONLYB": 1.0},
    "gate": {"ALCH": 10.0, "SPACEX": 1.0},
    "mexc": {"ALCH": 0.1, "PNUT": 1.0, "SPACEX": 1.0},
}

def test_universe_needs_two_exchanges():
    u = build_universe(MARKETS, pre_ipo_symbols=[])
    assert "ALCH" in u and "PNUT" in u
    assert "ONLYB" not in u          # only 1 exchange

def test_category_tagging():
    u = build_universe(MARKETS, pre_ipo_symbols=["SPACEX"])
    assert u["SPACEX"]["category"] == "pre_ipo"
    assert u["ALCH"]["category"] == "perp"

def test_exchange_and_contract_size_maps():
    u = build_universe(MARKETS, pre_ipo_symbols=[])
    assert u["ALCH"]["exchanges"] == ["binance", "gate", "mexc"]
    assert u["ALCH"]["contract_size"]["gate"] == 10.0
```

- [ ] **Step 2: Run, verify FAIL**

- [ ] **Step 3: Implement `spreadwatch/collector/discovery.py`**

```python
import asyncio, logging
import ccxt.async_support as ccxt

log = logging.getLogger("spreadwatch.discovery")

def build_universe(markets_by_exchange: dict, pre_ipo_symbols: list) -> dict:
    """{symbol: {category, exchanges: [..], contract_size: {ex: mult}}}
    for symbols listed on >= 2 exchanges."""
    by_symbol = {}
    for ex, markets in markets_by_exchange.items():
        for sym, csize in markets.items():
            e = by_symbol.setdefault(sym, {"exchanges": [], "contract_size": {}})
            e["exchanges"].append(ex)
            e["contract_size"][ex] = csize
    pre = set(pre_ipo_symbols)
    out = {}
    for sym, e in by_symbol.items():
        if len(e["exchanges"]) < 2:
            continue
        out[sym] = {"category": "pre_ipo" if sym in pre else "perp",
                    "exchanges": sorted(e["exchanges"]),
                    "contract_size": e["contract_size"]}
    return out

CCXT_IDS = {"binance": "binanceusdm", "bybit": "bybit", "okx": "okx",
            "bitget": "bitget", "gate": "gateio", "mexc": "mexc",
            "blofin": "blofin"}

async def fetch_markets(exchanges: list) -> dict:
    """{exchange: {base_symbol: contract_size}} for linear USDT perps."""
    async def one(name):
        client = getattr(ccxt, CCXT_IDS[name])()
        try:
            markets = await client.load_markets()
            out = {}
            for m in markets.values():
                if (m.get("swap") and m.get("linear")
                        and m.get("quote") == "USDT" and m.get("active", True)):
                    out[m["base"]] = float(m.get("contractSize") or 1.0)
            return name, out
        except Exception as e:
            log.warning("discovery %s failed: %r", name, e)
            return name, {}
        finally:
            await client.close()
    results = await asyncio.gather(*(one(n) for n in exchanges))
    return {name: mk for name, mk in results if mk}
```

- [ ] **Step 4: Run, verify PASS**, then **commit** — `git add -A && git commit -m "feat: ccxt symbol discovery with pre-IPO tagging"`

---

### Task 13: Scan engine

**Files:**
- Create: `spreadwatch/core/engine.py`
- Test: `tests/test_engine.py`

Each cycle: for every symbol, for every pair of exchanges with fresh books, orient short=richer mid, compute raw/exec/net spread + grabbability, feed lifecycle + storage, and refresh the live matrix snapshot the API serves.

- [ ] **Step 1: Write failing tests** — `tests/test_engine.py`

```python
import pytest
from spreadwatch.core.engine import ScanEngine
from spreadwatch.collector.bookstore import BookStore
from spreadwatch.config import DEFAULTS
import copy

CFG = copy.deepcopy(DEFAULTS)

def make_engine():
    bs = BookStore(staleness_cutoff_s=3.0)
    universe = {"ALCH": {"category": "perp",
                         "exchanges": ["gate", "mexc"], "contract_size": {}}}
    eng = ScanEngine(cfg=CFG, bookstore=bs, universe=universe, storage=None)
    return eng, bs

def set_books(bs, high_mid_ex="mexc", low_mid_ex="gate"):
    bs.set_book(high_mid_ex, "ALCH", [(102.0, 10.0)], [(102.2, 10.0)], ts=1000.0)
    bs.set_book(low_mid_ex, "ALCH", [(100.0, 10.0)], [(100.2, 10.0)], ts=1000.05)

def test_scan_produces_oriented_tick():
    eng, bs = make_engine()
    set_books(bs)
    ticks = eng.scan(now=1001.0)
    assert len(ticks) == 1
    t = ticks[0]
    assert t.ex_short == "mexc" and t.ex_long == "gate"   # short the rich side
    assert t.raw_spread_pct > 0
    # exec: sell mexc bids @102 vs buy gate asks @100.2
    assert t.exec_spread_pct == pytest.approx((102.0 - 100.2) / 100.2 * 100)
    fees = CFG["taker_fees_pct"]["mexc"] + CFG["taker_fees_pct"]["gate"]
    assert t.net_spread_pct == pytest.approx(t.exec_spread_pct - fees)
    assert t.ob_skew_ms == pytest.approx(50.0)
    assert t.grabbability is not None and set(t.factors) == \
        {"duration", "depth", "margin", "velocity", "sync"}

def test_stale_book_skipped():
    eng, bs = make_engine()
    set_books(bs)
    assert eng.scan(now=1010.0) == []      # both books stale

def test_insane_spread_no_opportunity():
    eng, bs = make_engine()
    bs.set_book("mexc", "ALCH", [(200.0, 10.0)], [(200.2, 10.0)], ts=1000.0)
    bs.set_book("gate", "ALCH", [(100.0, 10.0)], [(100.2, 10.0)], ts=1000.0)
    ticks = eng.scan(now=1001.0)            # ~100% spread > max_sane 10%
    assert len(ticks) == 1                  # still recorded as a tick
    assert eng.tracker.active_count() == 0  # but no opportunity opened

def test_opportunity_opens_via_engine():
    eng, bs = make_engine()
    set_books(bs)
    eng.scan(now=1001.0)
    assert eng.tracker.active_count() == 1

def test_velocity_ewma_updates():
    eng, bs = make_engine()
    set_books(bs)
    eng.scan(now=1001.0)
    v1 = eng.velocity_of("ALCH", "mexc", "gate")
    bs.set_book("mexc", "ALCH", [(103.0, 10.0)], [(103.2, 10.0)], ts=1001.5)
    bs.set_book("gate", "ALCH", [(100.0, 10.0)], [(100.2, 10.0)], ts=1001.5)
    eng.scan(now=1002.0)
    assert eng.velocity_of("ALCH", "mexc", "gate") > v1   # spread widened
```

- [ ] **Step 2: Run, verify FAIL**

- [ ] **Step 3: Implement `spreadwatch/core/engine.py`**

```python
import itertools
from dataclasses import replace
from .models import SpreadTick
from .spread import executable_spread_pct, depth_usd, mid, raw_spread_pct
from .fees import total_fees_pct
from .grabbability import score_grabbability
from .lifecycle import OpportunityTracker

EWMA_ALPHA = 0.3

class ScanEngine:
    def __init__(self, cfg, bookstore, universe, storage):
        self.cfg = cfg
        self.bookstore = bookstore
        self.universe = universe          # {symbol: {category, exchanges, ...}}
        self.storage = storage            # may be None in tests
        self.tracker = OpportunityTracker(cfg["min_profit_margin_pct"])
        self._ewma: dict[tuple, float] = {}      # velocity per pair key
        self._last_exec: dict[tuple, float] = {}
        self.latest_ticks: list[SpreadTick] = [] # live matrix snapshot

    def velocity_of(self, symbol, ex_short, ex_long):
        return self._ewma.get((symbol, ex_short, ex_long), 0.0)

    def scan(self, now: float) -> list[SpreadTick]:
        ticks = []
        for symbol, meta in self.universe.items():
            fresh = [ex for ex in meta["exchanges"]
                     if self.bookstore.get_fresh(ex, symbol, now)]
            for ex_a, ex_b in itertools.combinations(fresh, 2):
                ba = self.bookstore.get_fresh(ex_a, symbol, now)
                bb = self.bookstore.get_fresh(ex_b, symbol, now)
                if mid(ba) is None or mid(bb) is None:
                    continue
                # orient: short the richer mid
                if mid(ba) >= mid(bb):
                    ex_s, ex_l, bs_, bl_ = ex_a, ex_b, ba, bb
                else:
                    ex_s, ex_l, bs_, bl_ = ex_b, ex_a, bb, ba
                raw = raw_spread_pct(bs_, bl_)
                size = self.cfg["target_size_usd"]
                exec_pct = executable_spread_pct(bs_, bl_, size)
                fees = total_fees_pct(ex_s, ex_l, self.cfg["taker_fees_pct"])
                net = None if exec_pct is None else exec_pct - fees
                key = (symbol, ex_s, ex_l)
                # velocity: EWMA of exec-spread deltas per scan
                if exec_pct is not None:
                    prev = self._last_exec.get(key)
                    if prev is not None:
                        delta = exec_pct - prev
                        self._ewma[key] = (EWMA_ALPHA * delta +
                                           (1 - EWMA_ALPHA) *
                                           self._ewma.get(key, 0.0))
                    self._last_exec[key] = exec_pct
                skew_ms = abs(bs_.ts - bl_.ts) * 1000.0
                d_s = depth_usd(bs_.bids)
                d_l = depth_usd(bl_.asks)
                grab, factors = (None, None)
                if exec_pct is not None:
                    age = self.tracker.open_age(symbol, ex_s, ex_l, now) or 0.0
                    grab, factors = score_grabbability(
                        duration_s=age, min_depth_usd=min(d_s, d_l),
                        exec_spread_pct=exec_pct, total_fees_pct=fees,
                        velocity=self._ewma.get(key, 0.0), ob_skew_ms=skew_ms,
                        fill_window_s=self.cfg["fill_window_s"],
                        target_size_usd=size,
                        sync_tolerance_ms=self.cfg["sync_tolerance_ms"],
                        velocity_k=self.cfg["velocity_k"],
                        weights=self.cfg["grab_weights"])
                tick = SpreadTick(ts=now, symbol=symbol,
                                  category=meta["category"],
                                  ex_short=ex_s, ex_long=ex_l,
                                  raw_spread_pct=raw, exec_spread_pct=exec_pct,
                                  net_spread_pct=net, depth_short_usd=d_s,
                                  depth_long_usd=d_l, grabbability=grab,
                                  factors=factors, ob_skew_ms=skew_ms)
                ticks.append(tick)
                # sanity cap: insane spreads recorded but never open opportunities
                lifecycle_tick = tick
                if raw is not None and abs(raw) > self.cfg["max_sane_spread_pct"]:
                    lifecycle_tick = replace(tick, net_spread_pct=None)
                closed = self.tracker.on_tick(lifecycle_tick)
                if self.storage:
                    self.storage.add_tick(tick)
                    if closed:
                        self.storage.add_opportunity(closed)
        self.latest_ticks = ticks
        if self.storage:
            self.storage.flush()
        return ticks
```

- [ ] **Step 4: Run, verify PASS**, then **commit** — `git add -A && git commit -m "feat: scan engine — oriented pair comparison, velocity EWMA, sanity cap"`

---

### Task 14: FastAPI app — REST + WS push + static

**Files:**
- Create: `spreadwatch/api/app.py`
- Test: `tests/test_api.py`

- [ ] **Step 1: Write failing tests** — `tests/test_api.py` (uses FastAPI TestClient; `pip` already installed httpx via fastapi extras — if missing: `.venv/bin/pip install httpx`)

```python
import copy, time
import pytest
from fastapi.testclient import TestClient
from spreadwatch.api.app import create_app
from spreadwatch.core.engine import ScanEngine
from spreadwatch.collector.bookstore import BookStore
from spreadwatch.storage.db import Storage
from spreadwatch.config import DEFAULTS

@pytest.fixture
def client(tmp_path):
    cfg = copy.deepcopy(DEFAULTS)
    bs = BookStore(staleness_cutoff_s=3.0)
    universe = {"ALCH": {"category": "perp", "exchanges": ["gate", "mexc"],
                         "contract_size": {}}}
    storage = Storage(str(tmp_path), retention_days=30)
    eng = ScanEngine(cfg=cfg, bookstore=bs, universe=universe, storage=storage)
    now = time.time()
    bs.set_book("mexc", "ALCH", [(102.0, 10.0)], [(102.2, 10.0)], ts=now)
    bs.set_book("gate", "ALCH", [(100.0, 10.0)], [(100.2, 10.0)], ts=now)
    eng.scan(now=now)
    app = create_app(cfg=cfg, engine=eng, bookstore=bs, storage=storage,
                     feeds={})
    return TestClient(app)

def test_matrix(client):
    rows = client.get("/api/matrix").json()["rows"]
    assert len(rows) == 1
    r = rows[0]
    assert r["symbol"] == "ALCH" and r["ex_short"] == "mexc"
    assert r["net_pct"] is not None and r["grab"] is not None

def test_pair_detail(client):
    d = client.get("/api/pair/ALCH/mexc/gate").json()
    assert d["depth"]["short_bids"][0][0] == 102.0
    assert "series" in d and "factors" in d

def test_status(client):
    s = client.get("/api/status").json()
    assert "exchanges" in s and "symbols" in s

def test_history_endpoints(client):
    assert client.get("/api/history/opportunities").status_code == 200
    assert client.get("/api/history/rankings").status_code == 200
    r = client.get("/api/history/spreads",
                   params={"symbol": "ALCH", "ex_short": "mexc",
                           "ex_long": "gate"})
    assert r.status_code == 200
```

- [ ] **Step 2: Run, verify FAIL**

- [ ] **Step 3: Implement `spreadwatch/api/app.py`**

```python
import asyncio, dataclasses, os, time
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from spreadwatch.storage import queries

WEB_DIR = os.path.join(os.path.dirname(__file__), "..", "web")

def _tick_row(t, engine, now):
    return {"symbol": t.symbol, "category": t.category,
            "ex_short": t.ex_short, "ex_long": t.ex_long,
            "raw_pct": t.raw_spread_pct, "exec_pct": t.exec_spread_pct,
            "net_pct": t.net_spread_pct, "depth_short": t.depth_short_usd,
            "depth_long": t.depth_long_usd, "grab": t.grabbability,
            "ob_skew_ms": t.ob_skew_ms,
            "opp_age_s": engine.tracker.open_age(t.symbol, t.ex_short,
                                                 t.ex_long, now)}

def create_app(cfg, engine, bookstore, storage, feeds):
    app = FastAPI(title="spreadwatch")

    @app.get("/api/matrix")
    def matrix():
        now = time.time()
        return {"ts": now,
                "rows": [_tick_row(t, engine, now) for t in engine.latest_ticks]}

    @app.get("/api/pair/{symbol}/{ex_short}/{ex_long}")
    def pair(symbol: str, ex_short: str, ex_long: str):
        now = time.time()
        bs_ = bookstore.get(ex_short, symbol)
        bl_ = bookstore.get(ex_long, symbol)
        tick = next((t for t in engine.latest_ticks
                     if (t.symbol, t.ex_short, t.ex_long) ==
                        (symbol, ex_short, ex_long)), None)
        series = queries.spread_series(storage, symbol, ex_short, ex_long,
                                       start=now - 3600, end=now)
        return {"symbol": symbol,
                "depth": {"short_bids": (bs_.bids[:10] if bs_ else []),
                          "long_asks": (bl_.asks[:10] if bl_ else [])},
                "series": series,
                "factors": (tick.factors if tick else None),
                "tick": (_tick_row(tick, engine, now) if tick else None)}

    @app.get("/api/history/spreads")
    def history_spreads(symbol: str, ex_short: str, ex_long: str,
                        hours: float = 24.0):
        now = time.time()
        return {"series": queries.spread_series(storage, symbol, ex_short,
                                                ex_long, now - hours * 3600, now)}

    @app.get("/api/history/opportunities")
    def history_opps(hours: float = 24.0):
        now = time.time()
        return {"opportunities": queries.opportunity_log(storage,
                                                         now - hours * 3600, now)}

    @app.get("/api/history/rankings")
    def history_rankings(hours: float = 24.0):
        now = time.time()
        return queries.rankings(storage, now - hours * 3600, now,
                                cfg["target_size_usd"])

    @app.get("/api/status")
    def status():
        now = time.time()
        return {"exchanges": {name: {"connected": f.connected}
                              for name, f in feeds.items()},
                "symbols": len(engine.universe),
                "live_rows": len(engine.latest_ticks),
                "open_opportunities": engine.tracker.active_count(),
                "ts": now}

    @app.websocket("/api/live")
    async def live(ws: WebSocket):
        await ws.accept()
        try:
            while True:
                now = time.time()
                await ws.send_json({"ts": now,
                                    "rows": [_tick_row(t, engine, now)
                                             for t in engine.latest_ticks]})
                await asyncio.sleep(cfg["scan_interval_s"])
        except WebSocketDisconnect:
            pass

    app.mount("/", StaticFiles(directory=WEB_DIR, html=True), name="web")
    return app
```

- [ ] **Step 4: Create empty `spreadwatch/web/index.html`** (placeholder so StaticFiles mount works; real UI in Task 15)

```html
<!DOCTYPE html><html><body>spreadwatch UI arrives in Task 15</body></html>
```

- [ ] **Step 5: Run, verify PASS** — `.venv/bin/pytest tests/test_api.py -v`

- [ ] **Step 6: Commit** — `git add -A && git commit -m "feat: FastAPI endpoints, WS live push, static mount"`

---

### Task 15: Web dashboard (matrix + drawer + history)

**Files:**
- Create: `spreadwatch/web/index.html`, `spreadwatch/web/app.js`, `spreadwatch/web/history.html`, `spreadwatch/web/history.js`, `spreadwatch/web/style.css`

No JS framework, no build step. Charts: inline SVG polylines (drop uPlot — YAGNI). Verified by browser smoke test in Task 16, not unit tests.

**Security note:** symbol/exchange strings originate from exchange APIs (untrusted). Every string interpolated into HTML MUST go through the `esc()` helper below; numeric values are only rendered via `toFixed`/`Math.round`.

- [ ] **Step 1: Write `spreadwatch/web/style.css`**

```css
:root { --bg:#0f1115; --panel:#171a21; --text:#d7dae0; --dim:#6b7280;
        --green:#34d399; --red:#f87171; --accent:#8b5cf6; --line:#262a33; }
* { box-sizing: border-box; }
body { margin:0; background:var(--bg); color:var(--text);
       font:13px/1.45 "SF Mono", Menlo, monospace; }
header { display:flex; gap:16px; align-items:center; padding:10px 16px;
         border-bottom:1px solid var(--line); }
header h1 { font-size:15px; margin:0; }
header a { color:var(--dim); text-decoration:none; }
header a.active { color:var(--text); }
#status { margin-left:auto; color:var(--dim); font-size:11px; }
#status .down { color:var(--red); }
.controls { display:flex; gap:8px; padding:10px 16px; }
.controls input, .controls select, .controls button {
  background:var(--panel); color:var(--text); border:1px solid var(--line);
  border-radius:6px; padding:5px 8px; font:inherit; }
.controls label { display:flex; align-items:center; gap:4px; color:var(--dim); }
table { width:100%; border-collapse:collapse; }
th, td { padding:5px 10px; text-align:right; white-space:nowrap; }
th:first-child, td:first-child { text-align:left; }
th { color:var(--dim); font-weight:normal; cursor:pointer; position:sticky;
     top:0; background:var(--bg); border-bottom:1px solid var(--line); }
tbody tr { border-bottom:1px solid var(--line); cursor:pointer; }
tbody tr:hover { background:var(--panel); }
tr.stale { opacity:.45; }
.pos { color:var(--green); } .neg { color:var(--red); }
.badge { background:var(--accent); color:#fff; border-radius:3px;
         font-size:10px; padding:0 4px; margin-left:4px; }
#drawer { position:fixed; right:0; top:0; bottom:0; width:420px;
  background:var(--panel); border-left:1px solid var(--line); padding:16px;
  overflow-y:auto; display:none; }
#drawer.open { display:block; }
#drawer h2 { margin:0 0 8px; font-size:14px; }
#drawer .close { float:right; cursor:pointer; color:var(--dim); }
.bar { height:8px; background:var(--line); border-radius:4px; margin:2px 0 8px; }
.bar > div { height:100%; background:var(--accent); border-radius:4px; }
.ladder { display:grid; grid-template-columns:1fr 1fr; gap:12px; }
.ladder div span { display:flex; justify-content:space-between; }
.section-title { color:var(--dim); text-transform:uppercase; font-size:10px;
                 margin:14px 0 4px; }
svg.spark { width:100%; height:60px; }
svg.spark polyline { fill:none; stroke:var(--accent); stroke-width:1.5; }
td svg { width:80px; height:16px; }
td svg polyline { fill:none; stroke:var(--dim); stroke-width:1; }
```

- [ ] **Step 2: Write `spreadwatch/web/index.html`**

```html
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>spreadwatch</title>
<link rel="stylesheet" href="style.css"></head>
<body>
<header>
  <h1>spreadwatch</h1>
  <a href="/" class="active">Matrix</a>
  <a href="history.html">History</a>
  <span id="status">connecting…</span>
</header>
<div class="controls">
  <input id="q" placeholder="Filter symbol…">
  <label><input type="checkbox" id="netpos"> net &gt; 0 only</label>
  <label><input type="checkbox" id="preipo"> pre-IPO only</label>
</div>
<table id="matrix">
  <thead><tr>
    <th data-k="symbol">SYM</th><th data-k="ex_short">SHORT@</th>
    <th data-k="ex_long">LONG@</th><th data-k="raw_pct">RAW%</th>
    <th data-k="exec_pct">EXEC%</th><th data-k="net_pct">NET%</th>
    <th data-k="depth">DEPTH$</th><th data-k="grab">GRAB</th>
    <th data-k="opp_age_s">AGE</th><th>1h</th>
  </tr></thead>
  <tbody></tbody>
</table>
<div id="drawer"></div>
<script src="app.js"></script>
</body>
</html>
```

- [ ] **Step 3: Write `spreadwatch/web/app.js`**

```javascript
let rows = [], sortKey = "net_pct", sortDir = -1, selected = null;
const sparkCache = {};   // "SYM|a|b" -> [net,...] (last 60 pushes ≈ 1 min live)

const $ = (s) => document.querySelector(s);
const esc = (s) => String(s).replace(/[&<>"']/g, (c) => ({
  "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const fmt = (v, d = 2) => v == null ? "—" : v.toFixed(d);
const cls = (v) => v == null ? "" : v > 0 ? "pos" : "neg";

function spark(vals, w = 80, h = 16) {
  if (!vals || vals.length < 2) return "";
  const min = Math.min(...vals), max = Math.max(...vals), r = max - min || 1;
  const pts = vals.map((v, i) =>
    `${(i / (vals.length - 1)) * w},${h - ((v - min) / r) * (h - 2) - 1}`);
  return `<svg viewBox="0 0 ${w} ${h}"><polyline points="${pts.join(" ")}"/></svg>`;
}

function render() {
  const q = $("#q").value.toUpperCase();
  const netpos = $("#netpos").checked, preipo = $("#preipo").checked;
  let view = rows.filter(r =>
    (!q || r.symbol.includes(q)) &&
    (!netpos || (r.net_pct != null && r.net_pct > 0)) &&
    (!preipo || r.category === "pre_ipo"));
  view.sort((a, b) => {
    let av = a[sortKey], bv = b[sortKey];
    if (sortKey === "depth") { av = Math.min(a.depth_short, a.depth_long);
                               bv = Math.min(b.depth_short, b.depth_long); }
    if (av == null) return 1; if (bv == null) return -1;
    return (av < bv ? -1 : av > bv ? 1 : 0) * sortDir;
  });
  $("#matrix tbody").innerHTML = view.map(r => {
    const key = `${r.symbol}|${r.ex_short}|${r.ex_long}`;
    const badge = r.category === "pre_ipo" ? '<span class="badge">PRE-IPO</span>' : "";
    return `<tr data-key="${esc(key)}" class="${r.exec_pct == null ? "stale" : ""}">
      <td>${esc(r.symbol)}${badge}</td><td>${esc(r.ex_short)}</td>
      <td>${esc(r.ex_long)}</td>
      <td>${fmt(r.raw_pct)}</td><td>${fmt(r.exec_pct)}</td>
      <td class="${cls(r.net_pct)}">${fmt(r.net_pct)}</td>
      <td>${fmt(Math.min(r.depth_short, r.depth_long), 0)}</td>
      <td>${r.grab == null ? "—" : Math.round(r.grab)}</td>
      <td>${r.opp_age_s == null ? "—" : Math.round(r.opp_age_s) + "s"}</td>
      <td>${spark(sparkCache[key])}</td></tr>`;
  }).join("");
}

async function openDrawer(key) {
  selected = key;
  const [sym, a, b] = key.split("|");
  const d = await (await fetch(`/api/pair/${encodeURIComponent(sym)}/` +
    `${encodeURIComponent(a)}/${encodeURIComponent(b)}`)).json();
  const f = d.factors || {};
  const bars = ["duration", "depth", "margin", "velocity", "sync"].map(k =>
    `<div>${k} ${Math.round(f[k] ?? 0)}</div>
     <div class="bar"><div style="width:${Math.max(0, Math.min(100, f[k] ?? 0))}%">
     </div></div>`).join("");
  const ladder = (side) => side.map(([p, q]) =>
    `<span><b>${Number(p)}</b><i>$${Math.round(p * q)}</i></span>`).join("");
  const series = (d.series || []).map(x => x.best_net);
  $("#drawer").innerHTML = `
    <span class="close" id="drawer-close">✕ close</span>
    <h2>${esc(sym)} — short ${esc(a)} / long ${esc(b)}</h2>
    <div class="section-title">net spread, last hour (1m best)</div>
    ${series.length > 1 ? `<svg class="spark" viewBox="0 0 400 60">
       <polyline points="${series.map((v, i) => {
         const mn = Math.min(...series), r = (Math.max(...series) - mn) || 1;
         return `${(i / (series.length - 1)) * 400},${58 - ((v - mn) / r) * 56}`;
       }).join(" ")}"/></svg>` : "<div>no history yet</div>"}
    <div class="section-title">grabbability factors</div>${bars}
    <div class="section-title">depth (top 10)</div>
    <div class="ladder">
      <div><b>${esc(a)} bids</b>${ladder(d.depth.short_bids)}</div>
      <div><b>${esc(b)} asks</b>${ladder(d.depth.long_asks)}</div>
    </div>`;
  $("#drawer-close").onclick = closeDrawer;
  $("#drawer").classList.add("open");
}
function closeDrawer() { selected = null; $("#drawer").classList.remove("open"); }

function connect() {
  const ws = new WebSocket(`ws://${location.host}/api/live`);
  ws.onmessage = (e) => {
    rows = JSON.parse(e.data).rows;
    for (const r of rows) {
      const key = `${r.symbol}|${r.ex_short}|${r.ex_long}`;
      if (r.net_pct != null) {
        (sparkCache[key] = sparkCache[key] || []).push(r.net_pct);
        if (sparkCache[key].length > 60) sparkCache[key].shift();
      }
    }
    render();
    if (selected) openDrawer(selected);
  };
  ws.onclose = () => { $("#status").textContent = "disconnected — retrying";
    setTimeout(connect, 2000); };
  ws.onopen = async () => {
    const s = await (await fetch("/api/status")).json();
    const parts = Object.entries(s.exchanges).map(([n, x]) =>
      x.connected ? esc(n) : `<span class="down">${esc(n)}</span>`);
    $("#status").innerHTML =
      `${Number(s.symbols)} syms · ${Number(s.open_opportunities)} opps open · ` +
      parts.join(" ");
  };
}

document.querySelectorAll("th[data-k]").forEach(th => th.onclick = () => {
  const k = th.dataset.k;
  if (sortKey === k) sortDir *= -1; else { sortKey = k; sortDir = -1; }
  render();
});
$("#matrix tbody").onclick = (e) => {
  const tr = e.target.closest("tr"); if (tr) openDrawer(tr.dataset.key);
};
["q", "netpos", "preipo"].forEach(id => $("#" + id).oninput = render);
connect();
```

Note: `/api/matrix` rows need `category` — extend Task 14's `_tick_row` with `"category": t.category` if not already present (it is in the code above).

- [ ] **Step 4: Write `spreadwatch/web/history.html`**

```html
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>spreadwatch — history</title>
<link rel="stylesheet" href="style.css"></head>
<body>
<header>
  <h1>spreadwatch</h1>
  <a href="/">Matrix</a>
  <a href="history.html" class="active">History</a>
  <span id="status"></span>
</header>
<div class="controls">
  <select id="hours">
    <option value="1">1h</option><option value="24" selected>24h</option>
    <option value="168">7d</option>
  </select>
  <input id="pair" placeholder="Pair chart: SYM ex_short ex_long (e.g. ALCH mexc gate)">
  <button id="load">Load</button>
</div>
<div style="padding:0 16px" id="chart"></div>
<div style="display:flex; gap:24px; padding:16px;">
  <div style="flex:1"><div class="section-title">Best pairs (opportunity value $)</div>
    <table id="pairs"><thead><tr><th>SYM</th><th>SHORT@</th><th>LONG@</th>
      <th>VALUE$</th><th>COUNT</th></tr></thead><tbody></tbody></table></div>
  <div style="flex:1"><div class="section-title">Best hours (UTC)</div>
    <table id="hoursT"><thead><tr><th>HOUR</th><th>VALUE$</th><th>COUNT</th>
      </tr></thead><tbody></tbody></table></div>
</div>
<div style="padding:0 16px"><div class="section-title">Opportunity log</div>
  <table id="opps"><thead><tr><th>OPENED</th><th>SYM</th><th>SHORT@</th>
    <th>LONG@</th><th>DUR s</th><th>PEAK NET%</th><th>PEAK DEPTH$</th>
    <th>PEAK GRAB</th></tr></thead><tbody></tbody></table></div>
<script src="history.js"></script>
</body>
</html>
```

- [ ] **Step 5: Write `spreadwatch/web/history.js`**

```javascript
const $ = (s) => document.querySelector(s);
const esc = (s) => String(s).replace(/[&<>"']/g, (c) => ({
  "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const hours = () => $("#hours").value;

async function loadTables() {
  const r = await (await fetch(`/api/history/rankings?hours=${hours()}`)).json();
  $("#pairs tbody").innerHTML = r.pairs.slice(0, 30).map(p =>
    `<tr><td>${esc(p.symbol)}</td><td>${esc(p.ex_short)}</td>
     <td>${esc(p.ex_long)}</td><td>${p.value_usd.toFixed(3)}</td>
     <td>${Number(p.count)}</td></tr>`).join("");
  $("#hoursT tbody").innerHTML = r.hours.map(h =>
    `<tr><td>${String(h.hour).padStart(2, "0")}:00</td>
     <td>${h.value_usd.toFixed(3)}</td><td>${Number(h.count)}</td></tr>`).join("");
  const o = await (await fetch(`/api/history/opportunities?hours=${hours()}`)).json();
  $("#opps tbody").innerHTML = o.opportunities.slice(0, 100).map(x =>
    `<tr><td>${new Date(x.open_ts * 1000).toISOString().slice(5, 19)}</td>
     <td>${esc(x.symbol)}</td><td>${esc(x.ex_short)}</td><td>${esc(x.ex_long)}</td>
     <td>${x.duration_s.toFixed(1)}</td><td>${x.peak_net.toFixed(3)}</td>
     <td>${Math.round(x.peak_min_depth)}</td>
     <td>${Math.round(x.peak_grab)}</td></tr>`).join("");
}

async function loadChart() {
  const parts = $("#pair").value.trim().split(/\s+/);
  if (parts.length !== 3) return;
  const [sym, a, b] = parts.map(encodeURIComponent);
  const r = await (await fetch(`/api/history/spreads?symbol=${sym}&ex_short=${a}` +
    `&ex_long=${b}&hours=${hours()}`)).json();
  const vals = r.series.map(x => x.best_net);
  if (vals.length < 2) { $("#chart").textContent = "no data"; return; }
  const mn = Math.min(...vals), rg = (Math.max(...vals) - mn) || 1;
  const pts = vals.map((v, i) =>
    `${(i / (vals.length - 1)) * 800},${118 - ((v - mn) / rg) * 116}`);
  $("#chart").innerHTML = `<svg class="spark" style="height:120px"
    viewBox="0 0 800 120"><polyline points="${pts.join(" ")}"/></svg>
    <div class="section-title">${esc($("#pair").value)} — best net % per minute,
    min ${mn.toFixed(3)} max ${(mn + rg).toFixed(3)}</div>`;
}

$("#load").onclick = () => { loadTables(); loadChart(); };
$("#hours").onchange = () => { loadTables(); loadChart(); };
loadTables();
```

- [ ] **Step 6: Commit** — `git add -A && git commit -m "feat: matrix dashboard, detail drawer, history page"`

---

### Task 16: Wiring (`run.py`), smoke test, README

**Files:**
- Modify: `run.py`
- Create: `README.md`

- [ ] **Step 1: Implement `run.py`**

```python
import asyncio, logging, time
import uvicorn
from spreadwatch.config import load_config
from spreadwatch.collector.bookstore import BookStore
from spreadwatch.collector.ws_manager import WSFeed
from spreadwatch.collector.rest_poller import RestPoller, RestBudget, REST_EXCHANGES
from spreadwatch.collector.adapters import WS_ADAPTERS
from spreadwatch.collector.discovery import fetch_markets, build_universe
from spreadwatch.core.engine import ScanEngine
from spreadwatch.storage.db import Storage
from spreadwatch.api.app import create_app

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(name)s %(levelname)s %(message)s")
log = logging.getLogger("spreadwatch")

async def scan_loop(engine, interval_s):
    while True:
        started = time.time()
        try:
            ticks = engine.scan(now=started)
            if int(started) % 30 == 0:
                log.info("scan: %d rows, %d open opps", len(ticks),
                         engine.tracker.active_count())
        except Exception:
            log.exception("scan cycle failed")
        await asyncio.sleep(max(0.0, interval_s - (time.time() - started)))

async def rediscovery_loop(cfg, engine):
    while True:
        await asyncio.sleep(3600)
        try:
            markets = await fetch_markets(cfg["exchanges"])
            engine.universe = build_universe(markets, cfg["pre_ipo_symbols"])
            log.info("rediscovery: %d symbols", len(engine.universe))
        except Exception:
            log.exception("rediscovery failed")

async def main():
    cfg = load_config()
    storage = Storage("data/ticks", cfg["retention_days"])
    storage.prune()
    bookstore = BookStore(cfg["staleness_cutoff_s"])

    log.info("discovering symbols on %s ...", cfg["exchanges"])
    markets = await fetch_markets(cfg["exchanges"])
    universe = build_universe(markets, cfg["pre_ipo_symbols"])
    log.info("universe: %d symbols on >=2 exchanges", len(universe))

    engine = ScanEngine(cfg=cfg, bookstore=bookstore, universe=universe,
                        storage=storage)
    feeds = {}
    tasks = []
    for ex in cfg["exchanges"]:
        syms = [s for s, m in universe.items() if ex in m["exchanges"]]
        if not syms:
            continue
        if ex in WS_ADAPTERS:
            inst_id = WS_ADAPTERS[ex]["inst_id"]
            feed = WSFeed(exchange=ex, bookstore=bookstore,
                          inst_to_symbol={inst_id(s): s for s in syms},
                          contract_size={inst_id(s):
                                         universe[s]["contract_size"].get(ex, 1.0)
                                         for s in syms})
        elif ex in REST_EXCHANGES:
            inst_id = REST_EXCHANGES[ex]["inst_id"]
            feed = RestPoller(exchange=ex, bookstore=bookstore, symbols=syms,
                              contract_size={inst_id(s):
                                             universe[s]["contract_size"].get(ex, 1.0)
                                             for s in syms},
                              budget=RestBudget(cfg["rest_budget_per_cycle"]),
                              interval_s=cfg["scan_interval_s"])
        else:
            continue
        feeds[ex] = feed
        tasks.append(asyncio.create_task(feed.run()))

    tasks.append(asyncio.create_task(scan_loop(engine, cfg["scan_interval_s"])))
    tasks.append(asyncio.create_task(rediscovery_loop(cfg, engine)))

    app = create_app(cfg=cfg, engine=engine, bookstore=bookstore,
                     storage=storage, feeds=feeds)
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1",
                                           port=cfg["port"], log_level="warning"))
    log.info("dashboard: http://localhost:%d", cfg["port"])
    await server.serve()
    for t in tasks:
        t.cancel()

if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 2: Full test suite** — `.venv/bin/pytest -v` → all green

- [ ] **Step 3: Live smoke test** (~3 minutes)

```bash
.venv/bin/python run.py &
sleep 90
curl -s localhost:8377/api/status | python3 -m json.tool
curl -s localhost:8377/api/matrix | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d['rows']), 'rows'); print(d['rows'][:3])"
```

Verify: (1) status shows ≥5 exchanges connected; (2) matrix has hundreds of rows; (3) rows for WS-fed exchange pairs have non-null `exec_pct`/`net_pct`/`grab`; (4) `ob_skew_ms` mostly < 500 for WS pairs; (5) open `http://localhost:8377` — table renders and updates ~1/s, sorting/filtering works, row click opens drawer; (6) after a few minutes `data/ticks/<today>.db` exists and grows; history page rankings populate. Fix any adapter payload mismatches found here (adjust parsers, keep tests' intent). Kill the process afterward.

- [ ] **Step 4: Write `README.md`** — brief: what it is, setup (`python3 -m venv .venv && .venv/bin/pip install -r requirements.txt`), run (`.venv/bin/python run.py`), dashboard URL, one-line-per-key config table for `config.yaml`, data layout (`data/ticks/YYYY-MM-DD.db`), link to `docs/design-spec.md`.

- [ ] **Step 5: Final commit** — `git add -A && git commit -m "feat: wire collector, engine, API into run.py; README"`

---

## Self-Review Notes (completed during plan writing)

- **Spec coverage:** config/scaffold (T1), models + VWAP/net math (T2-3), grabbability (T4), lifecycle (T5), full-tick storage + rollups + retention (T6), history queries + capped-value rankings (T7), staleness (T8), WS feeds for 5 exchanges (T9-10), MEXC/BloFin budgeted REST + 429 halving + tiered prioritization (T11), discovery + pre-IPO tagging (T12), scan engine + sanity cap + ob_skew + velocity (T13), API + WS push + status (T14), matrix/drawer/history UI with pre-IPO filter + stale dimming + XSS-escaped rendering (T15), wiring + hourly rediscovery + live smoke test (T16). No gaps.
- **Known simplifications (intentional, YAGNI):** SVG sparklines instead of a chart lib; ticker-derived 1-level books produce raw-spread-only rows until a depth pull lands; OKX depth limited to 5 levels, Gate/Binance 20, Bitget 15 (ample for the $25 target size).
- **Risk flag for the engineer:** WS/REST payload field names in Task 9/11 fixtures reflect documented formats; the Task 16 smoke test is ground truth — adjust parsers there if an exchange moved a field.

## Execution

Execute tasks in order; each task ends green + committed. Total: 16 tasks.
