# Opportunity Mapper Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Map every *tradeable* (fee+profit-clearing) cross-exchange opportunity with synchronized two-sided OB depth, per-leg pull timestamps, and a 0–100 grabbability rating + per-factor breakdown.

**Architecture:** Extend the existing `OBSnapshotTracker` with a pure grabbability scorer and richer snapshot fields. Capture the wider opportunity set early in the scan loop (before the trade-timing gates), then run a synchronized OB-pull pass (`PaperTrader.map_opportunities`) that draws REST budget only after live entries are served. Everything behind an `OPP_MAPPER` flag.

**Tech Stack:** Python 3, `asyncio`, `aiohttp`; tests with `pytest`.

**Spec:** `docs/superpowers/specs/2026-06-19-opportunity-mapper-design.md`

**Working directory (all paths relative to it):** `/Users/vandenboogaard/.paperclip/instances/default/workspaces/857e37f3-bfdc-423f-941e-95d33d9ebe17/deploy/`
This is the paper-trader git repo. Run tests from here: `python -m pytest tests/test_opportunity_mapper.py -v`. Start work on a fresh branch off the deploy repo's `master` (the mapper is independent of the un-merged `claude/sequential-fill` work; new tests live in a new file so there is no conflict).

---

### Task 1: Constants + Def-2 predicate

**Files:**
- Modify: `paper_trader.py` (add a constants block + module function after the `MAX_SANE_SPREAD_PCT = 15.0` line, currently `paper_trader.py:361`)
- Test: `tests/test_opportunity_mapper.py` (new file)

- [ ] **Step 1: Write the failing tests**

Create `tests/test_opportunity_mapper.py`:

```python
import time
import asyncio
import paper_trader as pt


def test_opp_constants_present():
    assert hasattr(pt, "OPP_MAPPER")
    assert pt.OPP_DEPTH_TARGET_USD == 25.0
    assert pt.OPP_MIN_PROFIT_MARGIN == 0.05
    assert pt.OPP_FILL_WINDOW_S == 0.85
    assert pt.OPP_SYNC_TOLERANCE_MS == 250.0
    assert pt.OPP_VELOCITY_K == 1.0
    assert set(pt.OPP_GRAB_WEIGHTS) == {"duration", "depth", "margin", "velocity", "sync"}


def test_is_mappable_boundary():
    # total fees 0.10 + margin 0.05 => threshold 0.15
    assert pt.is_mappable_opportunity(0.15, 0.10) is True
    assert pt.is_mappable_opportunity(0.149, 0.10) is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_opportunity_mapper.py::test_opp_constants_present -v`
Expected: FAIL with `AttributeError: module 'paper_trader' has no attribute 'OPP_MAPPER'`

- [ ] **Step 3: Add the constants and predicate**

In `paper_trader.py`, immediately after the line `MAX_SANE_SPREAD_PCT = 15.0     # Hard cap — ignore anything above 15%` (line 361), insert:

```python
# ═══════════════════════════════════════════════════════════════════════════
# OPPORTUNITY MAPPER — characterize every fee-clearing opportunity
# ═══════════════════════════════════════════════════════════════════════════
OPP_MAPPER = int(os.getenv("OPP_MAPPER", "1"))   # 0 = feature off
OPP_DEPTH_TARGET_USD = 25.0      # live order size we care about grabbing (NOT paper MAX_POSITION_USD=500)
OPP_MIN_PROFIT_MARGIN = 0.05     # Def-2 margin over fees (matches trading's min_spread_needed)
OPP_FILL_WINDOW_S = 0.85         # react+fill window: POLL_INTERVAL_FAST (0.5) + LEG_LATENCY_MAX_MS/1000 (0.35)
OPP_SYNC_TOLERANCE_MS = 250.0    # per-leg fill window; skew above this => sync factor 0
OPP_VELOCITY_K = 1.0             # velocity factor slope
OPP_GRAB_WEIGHTS = {"duration": 1.0, "depth": 1.0, "margin": 1.0, "velocity": 1.0, "sync": 1.0}
OPP_MAP_LOG_EVERY = 50           # sampled OPP_MAP log cadence


def is_mappable_opportunity(spread_pct: float, total_fees: float) -> bool:
    """Def-2: opportunity is mappable when executable spread clears fees + min profit."""
    return spread_pct >= total_fees + OPP_MIN_PROFIT_MARGIN
```

(`os` is already imported at the top of `paper_trader.py`.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_opportunity_mapper.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add paper_trader.py tests/test_opportunity_mapper.py
git commit -m "feat(opp-mapper): add constants and Def-2 mappability predicate"
```

---

### Task 2: Pure grabbability scorer

**Files:**
- Modify: `paper_trader.py` — add a `@staticmethod _score_grabbability` to `OBSnapshotTracker` (class starts at `paper_trader.py:509`; insert after `get_stats`, currently ending at line 636, before `_ob_snapshots = OBSnapshotTracker()` at line 638)
- Test: `tests/test_opportunity_mapper.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_opportunity_mapper.py`:

```python
def test_score_grabbability_bounds():
    score, factors = pt.OBSnapshotTracker._score_grabbability(
        duration_s=1.0, short_depth_usd=100, long_depth_usd=100,
        spread_pct=0.5, total_fees=0.1, velocity=0.0, ob_skew_ms=0.0)
    assert 0.0 <= score <= 100.0
    assert set(factors) == {"duration", "depth", "margin", "velocity", "sync"}
    for v in factors.values():
        assert 0.0 <= v <= 100.0


def test_depth_factor_increases_with_depth():
    low = pt.OBSnapshotTracker._score_grabbability(1, 5, 5, 0.5, 0.1, 0, 0)[1]["depth"]
    high = pt.OBSnapshotTracker._score_grabbability(1, 50, 50, 0.5, 0.1, 0, 0)[1]["depth"]
    assert high > low
    assert high == 100.0  # 50 >= target 25


def test_sync_factor_decreases_with_skew():
    tight = pt.OBSnapshotTracker._score_grabbability(1, 50, 50, 0.5, 0.1, 0, 0)[1]["sync"]
    loose = pt.OBSnapshotTracker._score_grabbability(1, 50, 50, 0.5, 0.1, 0, 500)[1]["sync"]
    assert tight == 100.0
    assert loose == 0.0  # skew 500 >= tolerance 250


def test_velocity_factor_orders_collapse_flat_widen():
    flat = pt.OBSnapshotTracker._score_grabbability(1, 50, 50, 0.5, 0.1, 0.0, 0)[1]["velocity"]
    collapsing = pt.OBSnapshotTracker._score_grabbability(1, 50, 50, 0.5, 0.1, -0.6, 0)[1]["velocity"]
    widening = pt.OBSnapshotTracker._score_grabbability(1, 50, 50, 0.5, 0.1, 0.6, 0)[1]["velocity"]
    assert collapsing < flat < widening
    assert flat == 50.0


def test_duration_factor_subwindow_low():
    flash = pt.OBSnapshotTracker._score_grabbability(0.1, 50, 50, 0.5, 0.1, 0, 0)[1]["duration"]
    long_lived = pt.OBSnapshotTracker._score_grabbability(2.0, 50, 50, 0.5, 0.1, 0, 0)[1]["duration"]
    assert flash < long_lived
    assert long_lived == 100.0  # 2.0s > 0.85 window
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_opportunity_mapper.py::test_score_grabbability_bounds -v`
Expected: FAIL with `AttributeError: type object 'OBSnapshotTracker' has no attribute '_score_grabbability'`

- [ ] **Step 3: Add the scorer**

In `paper_trader.py`, inside `class OBSnapshotTracker`, after the `get_stats` method (after line 636, before the closing `_ob_snapshots = OBSnapshotTracker()`), add:

```python
    @staticmethod
    def _score_grabbability(duration_s, short_depth_usd, long_depth_usd,
                            spread_pct, total_fees, velocity, ob_skew_ms,
                            fill_window_s=None, depth_target_usd=None,
                            sync_tolerance_ms=None, velocity_k=None, weights=None):
        """Pure 0–100 grabbability scorer. Returns (composite, factors_dict).
        All knobs injectable for testing; default to module constants."""
        fill_window_s = OPP_FILL_WINDOW_S if fill_window_s is None else fill_window_s
        depth_target_usd = OPP_DEPTH_TARGET_USD if depth_target_usd is None else depth_target_usd
        sync_tolerance_ms = OPP_SYNC_TOLERANCE_MS if sync_tolerance_ms is None else sync_tolerance_ms
        velocity_k = OPP_VELOCITY_K if velocity_k is None else velocity_k
        weights = OPP_GRAB_WEIGHTS if weights is None else weights

        def clamp01(x):
            return 0.0 if x < 0 else (1.0 if x > 1 else x)

        f_duration = clamp01(duration_s / fill_window_s) * 100.0 if fill_window_s > 0 else 0.0
        depth = min(short_depth_usd, long_depth_usd)
        f_depth = clamp01(depth / depth_target_usd) * 100.0 if depth_target_usd > 0 else 0.0
        f_margin = clamp01((spread_pct - total_fees) / total_fees) * 100.0 if total_fees > 0 else 100.0
        f_velocity = clamp01(0.5 + velocity * velocity_k) * 100.0
        f_sync = clamp01(1.0 - ob_skew_ms / sync_tolerance_ms) * 100.0 if sync_tolerance_ms > 0 else 100.0

        factors = {
            "duration": round(f_duration, 1), "depth": round(f_depth, 1),
            "margin": round(f_margin, 1), "velocity": round(f_velocity, 1),
            "sync": round(f_sync, 1),
        }
        wsum = sum(weights.values()) or 1.0
        composite = sum(factors[k] * weights[k] for k in factors) / wsum
        return round(composite, 1), factors
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_opportunity_mapper.py -v`
Expected: PASS (7 passed)

- [ ] **Step 5: Commit**

```bash
git add paper_trader.py tests/test_opportunity_mapper.py
git commit -m "feat(opp-mapper): add pure grabbability scorer"
```

---

### Task 3: WS cache timestamp accessor

**Files:**
- Modify: `paper_trader.py` — add `get_orderbook_with_ts` to `OrderbookWSManager` (after `get_orderbook`, currently ending at line 1254)
- Test: `tests/test_opportunity_mapper.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_opportunity_mapper.py`:

```python
def test_get_orderbook_with_ts_returns_ts():
    m = pt.OrderbookWSManager()
    m._store("Binance_PERP", "BTC", [[100.0, 1.0]], [[101.0, 1.0]])
    bids, asks, ts = m.get_orderbook_with_ts("Binance", "BTC", "PERP")
    assert bids and asks
    assert isinstance(ts, float)


def test_get_orderbook_with_ts_missing_returns_none():
    m = pt.OrderbookWSManager()
    assert m.get_orderbook_with_ts("Binance", "NOPE", "PERP") == (None, None, None)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_opportunity_mapper.py::test_get_orderbook_with_ts_returns_ts -v`
Expected: FAIL with `AttributeError: 'OrderbookWSManager' object has no attribute 'get_orderbook_with_ts'`

- [ ] **Step 3: Add the accessor**

In `paper_trader.py`, inside `class OrderbookWSManager`, immediately after the `get_orderbook` method (after line 1254), add:

```python
    def get_orderbook_with_ts(self, exchange: str, symbol: str, instrument: str):
        """Like get_orderbook but also returns the cache as-of timestamp.
        Does NOT touch hit/miss counters (mapping must not pollute trading metrics).
        Returns (bids, asks, ts) or (None, None, None) if missing/stale."""
        key = f"{exchange}_{instrument}"
        data = self.cache.get(key, {}).get(symbol)
        if not data:
            return None, None, None
        if time.time() - data["ts"] > OB_WS_STALE_SECONDS:
            return None, None, None
        return data["bids"], data["asks"], data["ts"]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_opportunity_mapper.py -v`
Expected: PASS (9 passed)

- [ ] **Step 5: Commit**

```bash
git add paper_trader.py tests/test_opportunity_mapper.py
git commit -m "feat(opp-mapper): add WS orderbook timestamp accessor"
```

---

### Task 4: Extend record_opportunity + finalize on close

**Files:**
- Modify: `paper_trader.py` — `OBSnapshotTracker.record_opportunity` (line 542) and `close_opportunity` (line 596)
- Test: `tests/test_opportunity_mapper.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_opportunity_mapper.py`:

```python
def _bare_tracker():
    t = pt.OBSnapshotTracker.__new__(pt.OBSnapshotTracker)
    t.snapshots = []
    t._active_opps = {}
    t._last_save = 1e18      # huge => save() always early-returns, no disk write
    t.persist_path = "/tmp/opp_mapper_test.json"
    return t


def test_record_opportunity_stores_grabbability_and_skew():
    t = _bare_tracker()
    t.record_opportunity(
        pair_key="BTC|A|B", symbol="BTC",
        exchange_short="A", instrument_short="PERP",
        exchange_long="B", instrument_long="PERP",
        spread_pct=0.5,
        short_bids=[(100.0, 30.0)], long_asks=[(101.0, 30.0)],
        total_fees=0.1, ts_short=1000.000, ts_long=1000.050, velocity=0.2)
    snap = t.snapshots[-1]
    assert 0.0 <= snap["grabbability"] <= 100.0
    assert set(snap["factors"]) == {"duration", "depth", "margin", "velocity", "sync"}
    assert snap["ob_skew_ms"] == 50.0
    assert snap["total_fees"] == 0.1


def test_close_opportunity_writes_final_grabbability():
    t = _bare_tracker()
    t.record_opportunity(
        pair_key="BTC|A|B", symbol="BTC",
        exchange_short="A", instrument_short="PERP",
        exchange_long="B", instrument_long="PERP",
        spread_pct=0.5,
        short_bids=[(100.0, 30.0)], long_asks=[(101.0, 30.0)],
        total_fees=0.1, ts_short=1000.0, ts_long=1000.0, velocity=0.0)
    # back-date so the closed duration exceeds the fill window
    t._active_opps["BTC|A|B"]["first_seen"] = time.time() - 2.0
    t.close_opportunity("BTC|A|B")
    snap = t.snapshots[-1]
    assert "final_grabbability" in snap
    assert snap["final_factors"]["duration"] == 100.0  # 2s > 0.85 window
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_opportunity_mapper.py::test_record_opportunity_stores_grabbability_and_skew -v`
Expected: FAIL — `record_opportunity` raises `TypeError: ... unexpected keyword argument 'total_fees'`

- [ ] **Step 3a: Extend `record_opportunity`**

In `paper_trader.py`, change the `record_opportunity` signature (lines 542-548) from:

```python
    def record_opportunity(self, pair_key: str, symbol: str,
                           exchange_short: str, instrument_short: str,
                           exchange_long: str, instrument_long: str,
                           spread_pct: float,
                           short_bids: list, long_asks: list):
```

to (add the five new keyword params with defaults — keeps any existing caller working):

```python
    def record_opportunity(self, pair_key: str, symbol: str,
                           exchange_short: str, instrument_short: str,
                           exchange_long: str, instrument_long: str,
                           spread_pct: float,
                           short_bids: list, long_asks: list,
                           total_fees: float = 0.0, target_size_usd: float = None,
                           ts_short: float = None, ts_long: float = None,
                           velocity: float = 0.0):
```

Then, in the same method, locate where `snapshot` is built (lines 571-586). Replace that dict-construction block:

```python
        snapshot = {
            "ts": now_iso,
            "symbol": symbol,
            "exchange_short": exchange_short,
            "instrument_short": instrument_short,
            "exchange_long": exchange_long,
            "instrument_long": instrument_long,
            "spread_pct": round(spread_pct, 4),
            "peak_spread_pct": round(opp["peak_spread"], 4),
            "duration_s": round(duration_s, 1),
            "tick_count": opp["tick_count"],
            "short_bids_total_usd": round(sum(u for _, u in short_bids), 2),
            "long_asks_total_usd": round(sum(u for _, u in long_asks), 2),
            "short_bids_levels": short_levels,
            "long_asks_levels": long_levels,
        }
```

with:

```python
        short_total = sum(u for _, u in short_bids)
        long_total = sum(u for _, u in long_asks)
        if target_size_usd is None:
            target_size_usd = OPP_DEPTH_TARGET_USD
        ob_skew_ms = (abs(ts_short - ts_long) * 1000.0
                      if (ts_short is not None and ts_long is not None) else 0.0)
        grabbability, factors = self._score_grabbability(
            duration_s=duration_s, short_depth_usd=short_total, long_depth_usd=long_total,
            spread_pct=spread_pct, total_fees=total_fees, velocity=velocity,
            ob_skew_ms=ob_skew_ms)

        snapshot = {
            "ts": now_iso,
            "symbol": symbol,
            "exchange_short": exchange_short,
            "instrument_short": instrument_short,
            "exchange_long": exchange_long,
            "instrument_long": instrument_long,
            "spread_pct": round(spread_pct, 4),
            "peak_spread_pct": round(opp["peak_spread"], 4),
            "duration_s": round(duration_s, 1),
            "tick_count": opp["tick_count"],
            "short_bids_total_usd": round(short_total, 2),
            "long_asks_total_usd": round(long_total, 2),
            "short_bids_levels": short_levels,
            "long_asks_levels": long_levels,
            "total_fees": round(total_fees, 4),
            "target_size_usd": target_size_usd,
            "ts_short": ts_short,
            "ts_long": ts_long,
            "ob_skew_ms": round(ob_skew_ms, 1),
            "velocity": round(velocity, 4),
            "grabbability": grabbability,
            "factors": factors,
        }
```

- [ ] **Step 3b: Finalize grabbability in `close_opportunity`**

In `paper_trader.py`, replace the body of `close_opportunity` (lines 596-607):

```python
    def close_opportunity(self, pair_key: str):
        """Mark an opportunity as ended and record final duration."""
        if pair_key in self._active_opps:
            opp = self._active_opps.pop(pair_key)
            duration = time.time() - opp["first_seen"]
            # Update the last snapshot for this pair with final duration
            for snap in reversed(self.snapshots):
                snap_key = f"{snap['symbol']}|{snap['exchange_short']}|{snap['exchange_long']}"
                if snap_key in pair_key:
                    snap["final_duration_s"] = round(duration, 1)
                    snap["final_tick_count"] = opp["tick_count"]
                    break
```

with:

```python
    def close_opportunity(self, pair_key: str):
        """Mark an opportunity as ended; record final duration and grabbability."""
        if pair_key in self._active_opps:
            opp = self._active_opps.pop(pair_key)
            duration = time.time() - opp["first_seen"]
            # Update the last snapshot for this pair with final duration + grabbability
            for snap in reversed(self.snapshots):
                snap_key = f"{snap['symbol']}|{snap['exchange_short']}|{snap['exchange_long']}"
                if snap_key in pair_key:
                    snap["final_duration_s"] = round(duration, 1)
                    snap["final_tick_count"] = opp["tick_count"]
                    if "grabbability" in snap:
                        fg, ff = self._score_grabbability(
                            duration_s=duration,
                            short_depth_usd=snap.get("short_bids_total_usd", 0.0),
                            long_depth_usd=snap.get("long_asks_total_usd", 0.0),
                            spread_pct=snap.get("spread_pct", 0.0),
                            total_fees=snap.get("total_fees", 0.0),
                            velocity=snap.get("velocity", 0.0),
                            ob_skew_ms=snap.get("ob_skew_ms", 0.0))
                        snap["final_grabbability"] = fg
                        snap["final_factors"] = ff
                    break
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_opportunity_mapper.py -v`
Expected: PASS (11 passed)

- [ ] **Step 5: Commit**

```bash
git add paper_trader.py tests/test_opportunity_mapper.py
git commit -m "feat(opp-mapper): record grabbability/skew and finalize on close"
```

---

### Task 5: Grabbability aggregates in get_stats

**Files:**
- Modify: `paper_trader.py` — `OBSnapshotTracker.get_stats` (lines 619-636)
- Test: `tests/test_opportunity_mapper.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_opportunity_mapper.py`:

```python
def test_get_stats_includes_grabbability_aggregates():
    t = _bare_tracker()
    t.snapshots = [
        {"grabbability": 80.0, "ob_skew_ms": 10.0, "spread_pct": 0.5,
         "short_bids_total_usd": 100.0, "long_asks_total_usd": 100.0},
        {"grabbability": 60.0, "ob_skew_ms": 30.0, "spread_pct": 0.4,
         "short_bids_total_usd": 50.0, "long_asks_total_usd": 50.0},
    ]
    st = t.get_stats()
    assert st["avg_grabbability"] == 70.0
    assert st["median_grabbability"] in (60.0, 80.0)
    assert st["avg_ob_skew_ms"] == 20.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_opportunity_mapper.py::test_get_stats_includes_grabbability_aggregates -v`
Expected: FAIL with `KeyError: 'avg_grabbability'`

- [ ] **Step 3: Extend get_stats**

In `paper_trader.py`, replace the `get_stats` method body (lines 619-636):

```python
    def get_stats(self):
        """Summary statistics for dashboard."""
        if not self.snapshots:
            return {}
        durations = [s["duration_s"] for s in self.snapshots[-500:] if s.get("final_duration_s")]
        final_durations = [s["final_duration_s"] for s in self.snapshots[-500:] if s.get("final_duration_s")]
        short_liqs = [s["short_bids_total_usd"] for s in self.snapshots[-500:]]
        long_liqs = [s["long_asks_total_usd"] for s in self.snapshots[-500:]]
        spreads = [s["spread_pct"] for s in self.snapshots[-500:]]
        return {
            "total_snapshots": len(self.snapshots),
            "avg_duration_s": round(sum(final_durations) / len(final_durations), 1) if final_durations else 0,
            "median_duration_s": round(sorted(final_durations)[len(final_durations) // 2], 1) if final_durations else 0,
            "avg_short_liq_usd": round(sum(short_liqs) / len(short_liqs), 0) if short_liqs else 0,
            "avg_long_liq_usd": round(sum(long_liqs) / len(long_liqs), 0) if long_liqs else 0,
            "avg_spread_pct": round(sum(spreads) / len(spreads), 3) if spreads else 0,
            "active_opportunities": len(self._active_opps),
        }
```

with (adds three grabbability/skew aggregates):

```python
    def get_stats(self):
        """Summary statistics for dashboard."""
        if not self.snapshots:
            return {}
        recent = self.snapshots[-500:]
        final_durations = [s["final_duration_s"] for s in recent if s.get("final_duration_s")]
        short_liqs = [s["short_bids_total_usd"] for s in recent]
        long_liqs = [s["long_asks_total_usd"] for s in recent]
        spreads = [s["spread_pct"] for s in recent]
        grabs = [s["grabbability"] for s in recent if "grabbability" in s]
        skews = [s["ob_skew_ms"] for s in recent if "ob_skew_ms" in s]
        return {
            "total_snapshots": len(self.snapshots),
            "avg_duration_s": round(sum(final_durations) / len(final_durations), 1) if final_durations else 0,
            "median_duration_s": round(sorted(final_durations)[len(final_durations) // 2], 1) if final_durations else 0,
            "avg_short_liq_usd": round(sum(short_liqs) / len(short_liqs), 0) if short_liqs else 0,
            "avg_long_liq_usd": round(sum(long_liqs) / len(long_liqs), 0) if long_liqs else 0,
            "avg_spread_pct": round(sum(spreads) / len(spreads), 3) if spreads else 0,
            "avg_grabbability": round(sum(grabs) / len(grabs), 1) if grabs else 0,
            "median_grabbability": round(sorted(grabs)[len(grabs) // 2], 1) if grabs else 0,
            "avg_ob_skew_ms": round(sum(skews) / len(skews), 1) if skews else 0,
            "active_opportunities": len(self._active_opps),
        }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_opportunity_mapper.py -v`
Expected: PASS (12 passed)

- [ ] **Step 5: Commit**

```bash
git add paper_trader.py tests/test_opportunity_mapper.py
git commit -m "feat(opp-mapper): add grabbability/skew aggregates to get_stats"
```

---

### Task 6: Synchronized OB-pull pass (`map_opportunities`)

**Files:**
- Modify: `paper_trader.py` — add an `async def map_opportunities` method to `class PaperTrader` (place it next to `fetch_orderbook_levels`, near `paper_trader.py:2566`)
- Test: `tests/test_opportunity_mapper.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_opportunity_mapper.py`:

```python
class _FakeWS:
    def __init__(self, cache):
        self.cache = cache  # {(exchange, symbol, instrument): (bids, asks, ts)}

    def get_orderbook_with_ts(self, ex, sym, inst):
        return self.cache.get((ex, sym, inst), (None, None, None))


def _q(ex, inst="PERP"):
    return pt.PriceQuote(exchange=ex, symbol="BTC", bid=100.0, ask=100.0, mid=100.0,
                         volume_24h_usd=1e6, funding_rate=0.0, instrument=inst)


def _opp():
    return {"symbol": "BTC", "q_high": _q("A"), "q_low": _q("B"),
            "pair_key": "BTC|A|B", "spread_pct": 0.5, "raw_spread_pct": 0.5,
            "velocity": 0.1, "total_fees": 0.1}


def test_map_opportunities_uses_ws_cache_no_rest():
    t = pt.PaperTrader.__new__(pt.PaperTrader)
    tracker = _bare_tracker()
    ws = _FakeWS({
        ("A", "BTC", "PERP"): ([(100.0, 30.0)], [(100.0, 30.0)], 1000.00),
        ("B", "BTC", "PERP"): ([(101.0, 30.0)], [(101.0, 30.0)], 1000.02),
    })

    async def no_fetch(ex, sym, inst):
        raise AssertionError("must not REST when WS-cached")

    used = asyncio.run(t.map_opportunities([_opp()], ws, no_fetch, ob_budget=10, tracker=tracker))
    assert used == 0
    assert len(tracker.snapshots) == 1
    assert tracker.snapshots[-1]["ob_skew_ms"] == 20.0


def test_map_opportunities_rest_within_budget():
    t = pt.PaperTrader.__new__(pt.PaperTrader)
    tracker = _bare_tracker()
    ws = _FakeWS({})  # nothing cached

    async def fetch(ex, sym, inst):
        return ([(100.0, 30.0)], [(100.0, 30.0)])

    used = asyncio.run(t.map_opportunities([_opp()], ws, fetch, ob_budget=10, tracker=tracker))
    assert used >= 1
    assert len(tracker.snapshots) == 1


def test_map_opportunities_respects_zero_budget():
    t = pt.PaperTrader.__new__(pt.PaperTrader)
    tracker = _bare_tracker()
    ws = _FakeWS({})

    async def fetch(ex, sym, inst):
        return ([(100.0, 30.0)], [(100.0, 30.0)])

    used = asyncio.run(t.map_opportunities([_opp()], ws, fetch, ob_budget=0, tracker=tracker))
    assert used == 0
    assert len(tracker.snapshots) == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_opportunity_mapper.py::test_map_opportunities_uses_ws_cache_no_rest -v`
Expected: FAIL with `AttributeError: 'PaperTrader' object has no attribute 'map_opportunities'`

- [ ] **Step 3: Add `map_opportunities`**

In `paper_trader.py`, inside `class PaperTrader`, add this method (next to `fetch_orderbook_levels`, ~line 2566):

```python
    async def map_opportunities(self, mappable_opps, ws_mgr, fetch_levels, ob_budget, tracker=None):
        """Pull both books per opportunity (WS-cache-first, REST within ob_budget) and
        record a grabbability snapshot for each. Short side = bids on q_high; long side =
        asks on q_low. REST legs draw from ob_budget so live trading is never starved.
        fetch_levels(exchange, symbol, instrument) -> awaitable (bids, asks) or None.
        Returns the number of REST legs consumed."""
        if tracker is None:
            tracker = _ob_snapshots
        used = 0
        for opp in mappable_opps:
            q_high, q_low = opp["q_high"], opp["q_low"]
            sb, _sa, ts_s = ws_mgr.get_orderbook_with_ts(q_high.exchange, opp["symbol"], q_high.instrument)
            _lb, la, ts_l = ws_mgr.get_orderbook_with_ts(q_low.exchange, opp["symbol"], q_low.instrument)
            short_bids = sb
            long_asks = la
            need_short = short_bids is None
            need_long = long_asks is None
            if need_short or need_long:
                rest_legs = (1 if need_short else 0) + (1 if need_long else 0)
                if used + rest_legs > ob_budget:
                    continue  # no budget this cycle; duration keeps ticking via cleanup_stale
                now = time.time()
                res_high, res_low = await asyncio.gather(
                    fetch_levels(q_high.exchange, opp["symbol"], q_high.instrument),
                    fetch_levels(q_low.exchange, opp["symbol"], q_low.instrument))
                used += rest_legs
                if need_short:
                    short_bids = res_high[0] if res_high else None
                    ts_s = now
                if need_long:
                    long_asks = res_low[1] if res_low else None
                    ts_l = now
            if not short_bids or not long_asks:
                continue
            tracker.record_opportunity(
                pair_key=opp["pair_key"], symbol=opp["symbol"],
                exchange_short=q_high.exchange, instrument_short=q_high.instrument,
                exchange_long=q_low.exchange, instrument_long=q_low.instrument,
                spread_pct=opp["spread_pct"],
                short_bids=short_bids, long_asks=long_asks,
                total_fees=opp["total_fees"], ts_short=ts_s, ts_long=ts_l,
                velocity=opp["velocity"])
        return used
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_opportunity_mapper.py -v`
Expected: PASS (15 passed)

- [ ] **Step 5: Commit**

```bash
git add paper_trader.py tests/test_opportunity_mapper.py
git commit -m "feat(opp-mapper): add synchronized OB-pull pass map_opportunities"
```

---

### Task 7: Wire into the scan/execute cycle

**Files:**
- Modify: `paper_trader.py` — the main async scan loop: init at `paper_trader.py:6286`; capture point ~`6589-6600`; remove the old post-gate `record_opportunity` call at ~`6890`; add the map call after the execute loop ~`6940`; update `cleanup_stale` active keys at ~`6949`
- Test: `tests/test_opportunity_mapper.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_opportunity_mapper.py`:

```python
def test_capture_predicate_then_map_end_to_end():
    # Emulate the scan-loop capture guard, then run the mapping pass end-to-end.
    qh, ql = _q("A"), _q("B")
    total_fees, spread_pct = 0.1, 0.5
    mappable = []
    if pt.OPP_MAPPER and spread_pct <= pt.MAX_SANE_SPREAD_PCT \
            and pt.is_mappable_opportunity(spread_pct, total_fees):
        mappable.append({"symbol": "BTC", "q_high": qh, "q_low": ql,
                         "pair_key": "BTC|A|B", "spread_pct": spread_pct,
                         "raw_spread_pct": spread_pct, "velocity": 0.1,
                         "total_fees": total_fees})
    assert mappable, "fee-clearing opp must be captured"

    t = pt.PaperTrader.__new__(pt.PaperTrader)
    tracker = _bare_tracker()
    ws = _FakeWS({
        ("A", "BTC", "PERP"): ([(100.0, 30.0)], [(100.0, 30.0)], 1000.0),
        ("B", "BTC", "PERP"): ([(101.0, 30.0)], [(101.0, 30.0)], 1000.0),
    })

    async def nf(*a):
        raise AssertionError("WS-cached; no REST")

    asyncio.run(t.map_opportunities(mappable, ws, nf, ob_budget=5, tracker=tracker))
    snap = tracker.snapshots[-1]
    assert 0.0 <= snap["grabbability"] <= 100.0
    assert "factors" in snap
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_opportunity_mapper.py::test_capture_predicate_then_map_end_to_end -v`
Expected: PASS for the predicate logic but this guards the wiring — if it fails, it's because `OPP_MAPPER`/`is_mappable_opportunity`/`map_opportunities` are not all present. (After Tasks 1–6 it passes; this test locks the contract the wiring depends on. Proceed to wire the loop in Step 3.)

- [ ] **Step 3a: Initialize `mappable_opps`**

In `paper_trader.py`, find (line 6286-6287):

```python
            entry_candidates = []  # Collect all valid entries, then prioritize
            ob_calls_this_cycle = 0  # [2] Rate limiting counter
```

Add a line:

```python
            entry_candidates = []  # Collect all valid entries, then prioritize
            ob_calls_this_cycle = 0  # [2] Rate limiting counter
            mappable_opps = []  # [opp-mapper] every Def-2 opportunity this cycle
```

- [ ] **Step 3b: Add the early capture guard**

In `paper_trader.py`, find the block where `is_relative_entry` is initialized, just before the spread-threshold branch (line 6598-6600):

```python
                        rel_baseline, rel_n = trader.update_entry_baseline(pair_key, spread_pct)
                        is_relative_entry = False

                        if spread_pct > MAX_SANE_SPREAD_PCT:
```

Insert the capture guard between `is_relative_entry = False` and the `if spread_pct > MAX_SANE_SPREAD_PCT:` line:

```python
                        rel_baseline, rel_n = trader.update_entry_baseline(pair_key, spread_pct)
                        is_relative_entry = False

                        # [opp-mapper] Capture the wider Def-2 set BEFORE the trade-timing
                        # gates (threshold/reversion/confirm) which continue out below.
                        if OPP_MAPPER and spread_pct <= MAX_SANE_SPREAD_PCT:
                            map_fees = trader.compute_fees(q_high.exchange, q_low.exchange,
                                                           q_high.instrument, q_low.instrument) * 2
                            if is_mappable_opportunity(spread_pct, map_fees):
                                mappable_opps.append({
                                    "symbol": symbol, "q_high": q_high, "q_low": q_low,
                                    "pair_key": pair_key, "spread_pct": spread_pct,
                                    "raw_spread_pct": spread_pct, "velocity": velocity,
                                    "total_fees": map_fees,
                                })

                        if spread_pct > MAX_SANE_SPREAD_PCT:
```

- [ ] **Step 3c: Remove the old post-gate snapshot call**

In `paper_trader.py`, delete the old narrow recording block (lines 6889-6897), which is now superseded by `map_opportunities`:

```python
                    # ── OB Snapshot: record orderbook state at this opportunity ──
                    _ob_snapshots.record_opportunity(
                        pair_key=cand.get("pair_key", f"{cand['symbol']}|{q_high.exchange}|{q_low.exchange}"),
                        symbol=cand["symbol"],
                        exchange_short=q_high.exchange, instrument_short=q_high.instrument,
                        exchange_long=q_low.exchange, instrument_long=q_low.instrument,
                        spread_pct=cand["spread_pct"],
                        short_bids=short_bids_levels, long_asks=long_asks_levels,
                    )
```

(Delete those lines entirely. The surrounding `try:`/`except` and the `OB_OK` log line above them remain unchanged.)

- [ ] **Step 3d: Run the mapping pass after the execute loop**

In `paper_trader.py`, find where the execute loop ends and the shadow strategies begin (line 6940-6946):

```python
            # Log execute loop results
            if exec_log and entry_candidates:
                log.info(f"EXEC: {' | '.join(exec_log[:10])}")

            # ── SHADOW STRATEGIES — evaluate same prices with different configs ──
            for shadow in shadows:
                shadow.process_cycle(all_prices, now, trader)
```

Insert the mapping pass between the EXEC log and the shadow loop:

```python
            # Log execute loop results
            if exec_log and entry_candidates:
                log.info(f"EXEC: {' | '.join(exec_log[:10])}")

            # ── OPPORTUNITY MAPPER — characterize the wider Def-2 set (after entries) ──
            if OPP_MAPPER and mappable_opps:
                remaining_budget = max(0, MAX_OB_CALLS_PER_CYCLE - ob_calls_this_cycle)

                async def _map_fetch(ex, sym, inst):
                    return await trader.fetch_orderbook_levels(session, ex, sym, inst)

                mapped_used = await trader.map_opportunities(
                    mappable_opps, _ob_ws, _map_fetch, remaining_budget)
                ob_calls_this_cycle += mapped_used
                if mappable_opps and len(_ob_snapshots.snapshots) % OPP_MAP_LOG_EVERY == 1:
                    last = _ob_snapshots.snapshots[-1]
                    log.info(f"OPP_MAP {last['symbol']} grab={last['grabbability']:.0f} "
                             f"spread={last['spread_pct']:.2f}% dur={last['duration_s']:.0f}s "
                             f"depth=${min(last['short_bids_total_usd'], last['long_asks_total_usd']):,.0f} "
                             f"skew={last['ob_skew_ms']:.0f}ms vel={last['velocity']:.3f} "
                             f"(mapped={len(mappable_opps)} rest={mapped_used})")

            # ── SHADOW STRATEGIES — evaluate same prices with different configs ──
            for shadow in shadows:
                shadow.process_cycle(all_prices, now, trader)
```

- [ ] **Step 3e: Close durations for the wider mapped set**

In `paper_trader.py`, find the cleanup (line 6948-6950):

```python
            # ── Cleanup stale OB snapshot opportunities ──
            active_keys = {c.get("pair_key", "") for c in entry_candidates} if entry_candidates else set()
            _ob_snapshots.cleanup_stale(active_keys)
```

Replace with (use the wider mapped set so durations close correctly):

```python
            # ── Cleanup stale OB snapshot opportunities ──
            active_keys = {o["pair_key"] for o in mappable_opps}
            _ob_snapshots.cleanup_stale(active_keys)
```

- [ ] **Step 4: Verify the wiring compiles and the suite is green**

Run: `python -c "import paper_trader"`
Expected: no output, no traceback (module imports cleanly).

Run: `python -m pytest tests/test_opportunity_mapper.py -v`
Expected: PASS (16 passed)

Run: `python -m pytest tests/ -v`
Expected: all tests pass (the new file plus the pre-existing `test_fill_model.py` suite; no regressions).

- [ ] **Step 5: Commit**

```bash
git add paper_trader.py tests/test_opportunity_mapper.py
git commit -m "feat(opp-mapper): wire capture + mapping pass into scan cycle"
```

---

## Self-Review

**1. Spec coverage**
- Def-2 capture of the wider set → Task 1 (predicate) + Task 7 (early capture guard). ✓
- Synchronized OB pull, WS-first, REST-after-entries, per-leg timestamps + skew → Task 3 (ts accessor) + Task 6 (`map_opportunities`) + Task 7 (budget-after-entries call). ✓
- 0–100 composite + per-factor breakdown → Task 2 (scorer) + Task 4 (storage). ✓
- Live vs. final grabbability → Task 4 (`close_opportunity` finalization). ✓
- Depth target $25 (not paper's 500), sync tolerance 250 ms, velocity K, fill window 0.85 → Task 1 constants. ✓
- `get_stats` aggregates → Task 5. ✓
- `OPP_MAPPER` flag, zero trading change when off → Task 1 (flag) + Task 7 (guards on capture and map). ✓
- Observability `OPP_MAP` log marker → Task 7. ✓
- Dashboard view → intentionally OUT of scope per spec; not planned. ✓

**2. Placeholder scan:** No TBD/TODO; every code step shows complete code; every run step shows the exact command + expected result. ✓

**3. Type consistency:** `_score_grabbability(duration_s, short_depth_usd, long_depth_usd, spread_pct, total_fees, velocity, ob_skew_ms, ...)` is called identically in Task 4's `record_opportunity` and `close_opportunity`. `record_opportunity` new kwargs (`total_fees, target_size_usd, ts_short, ts_long, velocity`) match the call in `map_opportunities` (Task 6). `get_orderbook_with_ts` returns `(bids, asks, ts)` consumed positionally in `map_opportunities`. `map_opportunities(mappable_opps, ws_mgr, fetch_levels, ob_budget, tracker=None)` matches all call sites (tests use `tracker=`; Task 7 wiring omits it → defaults to `_ob_snapshots`). `is_mappable_opportunity(spread_pct, total_fees)` consistent across Task 1, Task 7 guard, and the Task 7 test. ✓
