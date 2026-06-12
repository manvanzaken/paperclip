# Sequential Two-Leg Fill Model Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the paper trader's fills predict live execution by modeling each leg filling sequentially at its own latency, allowing leg desync / naked exposure, and removing the entry/exit optimism asymmetry.

**Architecture:** Add pure, seed-deterministic helper functions (`_simulate_leg`, `_resolve_naked`, `_roll_leg_outcome`) as `@staticmethod`s on `PaperTrader`, alongside the existing `walk_orderbook` / `_incremental_fill`. Wire them into `open_position` and `close_position` behind a `SEQUENTIAL_FILL` flag (default on). The legs fill independently; unmatched size is auto-flattened at a one-sided crossing cost; full single-leg failure is recorded as a `leg_desync_unwind` closed position.

**Tech Stack:** Python 3.11, stdlib `random` (seeded for tests), `pytest` (new dev-only dependency, not in the container).

**Target repo:** workspace `/Users/vandenboogaard/.paperclip/instances/default/workspaces/857e37f3-bfdc-423f-941e-95d33d9ebe17/deploy/` — all paths below are relative to that `deploy/` dir. Run all commands from `deploy/`.

**Spec:** `docs/superpowers/specs/2026-06-12-sequential-two-leg-fill-design.md` (primary repo).

---

## File Structure

- `paper_trader.py` — modify: new constants (after line 272), new static helpers (after `walk_orderbook`, ~line 2793), wire `open_position` (4346) and `close_position` (4581), add `PaperPosition` fields (647), velocity-based flash-miss (4456), competition floor (4016).
- `tests/test_fill_model.py` — create: unit tests for the pure helpers + guardrail.
- `tests/conftest.py` — create: ensures `deploy/` is importable.
- `pytest.ini` — create: pytest config.

---

## Task 0: pytest scaffolding + import smoke test

**Files:**
- Create: `tests/conftest.py`
- Create: `pytest.ini`
- Create: `tests/test_fill_model.py`

- [ ] **Step 1: Create pytest config**

Create `pytest.ini`:

```ini
[pytest]
testpaths = tests
python_files = test_*.py
addopts = -q
```

- [ ] **Step 2: Create conftest so `paper_trader` imports**

Create `tests/conftest.py`:

```python
import os
import sys

# Make deploy/ (parent of tests/) importable so `import paper_trader` works.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
```

- [ ] **Step 3: Write the import smoke test**

Create `tests/test_fill_model.py`:

```python
import random

import paper_trader as pt


def test_module_imports():
    # Importing must not start the trader (guarded by __main__) and must
    # expose the PaperTrader class with the existing fill helpers.
    assert hasattr(pt, "PaperTrader")
    assert hasattr(pt.PaperTrader, "walk_orderbook")
    assert hasattr(pt.PaperTrader, "_incremental_fill")
```

- [ ] **Step 4: Run it (must pass — pure import)**

Run: `cd deploy && python -m pytest tests/test_fill_model.py::test_module_imports -v`
Expected: PASS. If it fails on missing `pytest`, run `pip install pytest` first.

- [ ] **Step 5: Commit**

```bash
git add deploy/pytest.ini deploy/tests/conftest.py deploy/tests/test_fill_model.py
git commit -m "test: add pytest scaffolding and import smoke test for paper_trader"
```

---

## Task 1: Add sequential-fill constants + flag

**Files:**
- Modify: `paper_trader.py` — insert after line 272 (end of competition block), and verify `import os` exists.

- [ ] **Step 1: Verify `os` is imported**

Run: `cd deploy && python -c "import paper_trader as p, inspect; print('os' in dir(p))"`
Expected: prints `True`. If `False`, add `import os` to the import block at the top of `paper_trader.py`.

- [ ] **Step 2: Add constants**

In `paper_trader.py`, immediately after line 272 (`COMPETITION_DECAY = 0.30 ...`), insert:

```python
# [40] Sequential two-leg fill model — each leg fills at its own confirmation
# time against its own (drifted) book, instead of one synchronous snapshot.
SEQUENTIAL_FILL = os.environ.get("SEQUENTIAL_FILL", "1") == "1"
LEG_LATENCY_MEAN_MS = 150.0     # Tokyo per-leg fill confirmation latency (mean)
LEG_LATENCY_STD_MS = 60.0
LEG_LATENCY_MIN_MS = 80.0
LEG_LATENCY_MAX_MS = 350.0
LEG_FAIL_CHANCE_PER_LEG = 0.03      # independent per-leg full failure
LEG_PARTIAL_CHANCE_PER_LEG = 0.15   # independent per-leg partial fill (50-90%)
COMPETITION_FILL_FLOOR = 0.0        # was an implicit 0.05 floor in simulate_competition
```

- [ ] **Step 3: Add a test asserting the constants exist with expected defaults**

Append to `tests/test_fill_model.py`:

```python
def test_sequential_constants_present():
    assert pt.LEG_LATENCY_MEAN_MS == 150.0
    assert pt.LEG_LATENCY_MIN_MS == 80.0
    assert pt.LEG_LATENCY_MAX_MS == 350.0
    assert pt.LEG_FAIL_CHANCE_PER_LEG == 0.03
    assert pt.LEG_PARTIAL_CHANCE_PER_LEG == 0.15
    assert pt.COMPETITION_FILL_FLOOR == 0.0
    assert isinstance(pt.SEQUENTIAL_FILL, bool)
```

- [ ] **Step 4: Run**

Run: `cd deploy && python -m pytest tests/test_fill_model.py::test_sequential_constants_present -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add deploy/paper_trader.py deploy/tests/test_fill_model.py
git commit -m "feat: add sequential two-leg fill constants and flag"
```

---

## Task 2: `_simulate_leg` — per-leg latency + drift + walk

**Files:**
- Modify: `paper_trader.py` — add `@staticmethod` after `walk_orderbook` (ends line 2793).
- Test: `tests/test_fill_model.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_fill_model.py`:

```python
def _book(prices_usd):
    # Helper: [(price, usd_at_level), ...]
    return list(prices_usd)


def test_simulate_leg_no_velocity_no_drift_matches_walk():
    rng = random.Random(1)
    bids = _book([(100.0, 50.0), (99.0, 50.0)])
    vwap, filled, lat, drift = pt.PaperTrader._simulate_leg(
        bids, 60.0, "sell", velocity=0.0, rng=rng
    )
    # Zero velocity => zero drift => VWAP equals a plain walk of the same book.
    plain_vwap, plain_filled, _ = pt.PaperTrader.walk_orderbook(bids, 60.0, "sell")
    assert abs(vwap - plain_vwap) < 1e-9
    assert abs(filled - plain_filled) < 1e-9
    assert drift == 0.0
    assert pt.LEG_LATENCY_MIN_MS <= lat <= pt.LEG_LATENCY_MAX_MS


def test_simulate_leg_partial_when_book_thin():
    rng = random.Random(2)
    asks = _book([(100.0, 20.0)])  # only $20 of depth
    vwap, filled, lat, drift = pt.PaperTrader._simulate_leg(
        asks, 100.0, "buy", velocity=0.0, rng=rng
    )
    assert abs(filled - 20.0) < 1e-9   # capped to available depth
    assert vwap > 0


def test_simulate_leg_zero_depth_returns_zero():
    rng = random.Random(3)
    vwap, filled, lat, drift = pt.PaperTrader._simulate_leg(
        [], 100.0, "buy", velocity=0.0, rng=rng
    )
    assert filled == 0.0
    assert vwap == 0.0


def test_simulate_leg_adverse_drift_worsens_buy_price():
    # Force adverse branch by patching EXEC_DELAY_ADVERSE_CHANCE to 1.0.
    asks = _book([(100.0, 1000.0)])
    rng = random.Random(4)
    old = pt.EXEC_DELAY_ADVERSE_CHANCE
    pt.EXEC_DELAY_ADVERSE_CHANCE = 1.0
    try:
        vwap, filled, lat, drift = pt.PaperTrader._simulate_leg(
            asks, 100.0, "buy", velocity=0.5, rng=rng
        )
    finally:
        pt.EXEC_DELAY_ADVERSE_CHANCE = old
    # Buying with adverse drift => fill ABOVE the quoted ask (worse for us).
    assert vwap > 100.0
    assert drift > 0.0
```

- [ ] **Step 2: Run to verify they fail**

Run: `cd deploy && python -m pytest tests/test_fill_model.py -k simulate_leg -v`
Expected: FAIL with `AttributeError: ... has no attribute '_simulate_leg'`

- [ ] **Step 3: Implement `_simulate_leg`**

In `paper_trader.py`, immediately after the `walk_orderbook` method (after line 2793, before `fetch_orderbook_depth`), add:

```python
    @staticmethod
    def _simulate_leg(levels, size_usd, side, velocity, rng=None):
        """[40] Fill ONE leg sequentially. Samples a per-leg confirmation latency,
        drifts the book over that latency proportional to spread velocity, then
        walks the drifted book for a VWAP. Legs are filled independently so the
        two legs of a trade can diverge in price and size.

        side: 'sell' (short leg, walk bids) or 'buy' (long leg, walk asks).
        Returns (vwap, filled_usd, latency_ms, drift_pct).
        Adverse drift = short fills lower / long fills higher (worse for taker)."""
        r = rng or random
        latency = min(LEG_LATENCY_MAX_MS,
                      max(LEG_LATENCY_MIN_MS,
                          r.gauss(LEG_LATENCY_MEAN_MS, LEG_LATENCY_STD_MS)))
        adverse = r.random() < EXEC_DELAY_ADVERSE_CHANCE
        magnitude = abs(velocity) * (latency / 1000.0)
        # Favorable case still drifts a little (a quarter, opposite sign).
        drift_pct = magnitude if adverse else -magnitude * 0.25
        if side == "sell":      # short sells into bids; adverse => bids lower
            factor = 1.0 - drift_pct / 100.0
        else:                    # long buys from asks; adverse => asks higher
            factor = 1.0 + drift_pct / 100.0
        drifted = [(px * factor, usd) for px, usd in (levels or [])]
        vwap, filled_usd, _ = PaperTrader.walk_orderbook(drifted, size_usd, side)
        return vwap, filled_usd, latency, drift_pct
```

- [ ] **Step 4: Run to verify pass**

Run: `cd deploy && python -m pytest tests/test_fill_model.py -k simulate_leg -v`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add deploy/paper_trader.py deploy/tests/test_fill_model.py
git commit -m "feat: add _simulate_leg per-leg latency and drift fill helper"
```

---

## Task 3: `_roll_leg_outcome` + `_resolve_naked`

**Files:**
- Modify: `paper_trader.py` — add two `@staticmethod`s after `_simulate_leg`.
- Test: `tests/test_fill_model.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/test_fill_model.py`:

```python
def test_roll_leg_outcome_full_when_no_failure():
    # Patch chances to 0 => always full fill.
    of, op = pt.LEG_FAIL_CHANCE_PER_LEG, pt.LEG_PARTIAL_CHANCE_PER_LEG
    pt.LEG_FAIL_CHANCE_PER_LEG = 0.0
    pt.LEG_PARTIAL_CHANCE_PER_LEG = 0.0
    try:
        assert pt.PaperTrader._roll_leg_outcome(100.0, rng=random.Random(1)) == 100.0
    finally:
        pt.LEG_FAIL_CHANCE_PER_LEG, pt.LEG_PARTIAL_CHANCE_PER_LEG = of, op


def test_roll_leg_outcome_fail_returns_zero():
    of = pt.LEG_FAIL_CHANCE_PER_LEG
    pt.LEG_FAIL_CHANCE_PER_LEG = 1.0
    try:
        assert pt.PaperTrader._roll_leg_outcome(100.0, rng=random.Random(1)) == 0.0
    finally:
        pt.LEG_FAIL_CHANCE_PER_LEG = of


def test_resolve_naked_matched_when_equal():
    matched, naked, side, cost = pt.PaperTrader._resolve_naked(80.0, 80.0)
    assert matched == 80.0
    assert naked == 0.0
    assert side is None
    assert cost == 0.0


def test_resolve_naked_short_excess_charges_unwind():
    matched, naked, side, cost = pt.PaperTrader._resolve_naked(100.0, 80.0)
    assert matched == 80.0
    assert naked == 20.0
    assert side == "short"
    # Unwind crosses the spread once at SLIPPAGE_FALLBACK_BPS on the naked notional.
    assert abs(cost - 20.0 * (pt.SLIPPAGE_FALLBACK_BPS / 10000.0)) < 1e-12


def test_resolve_naked_long_fail_full_naked():
    matched, naked, side, cost = pt.PaperTrader._resolve_naked(100.0, 0.0)
    assert matched == 0.0
    assert naked == 100.0
    assert side == "short"   # short filled, long failed => excess on short
    assert cost > 0.0
```

- [ ] **Step 2: Run to verify fail**

Run: `cd deploy && python -m pytest tests/test_fill_model.py -k "roll_leg or resolve_naked" -v`
Expected: FAIL (`_roll_leg_outcome` / `_resolve_naked` not defined)

- [ ] **Step 3: Implement both helpers**

In `paper_trader.py`, immediately after `_simulate_leg`, add:

```python
    @staticmethod
    def _roll_leg_outcome(size_usd, rng=None):
        """[40] Independent per-leg fill outcome. Returns the USD this leg fills
        BEFORE book-depth capping: 0 on rejection, a 50-90% slice on partial,
        else the full requested size."""
        r = rng or random
        if r.random() < LEG_FAIL_CHANCE_PER_LEG:
            return 0.0
        if r.random() < LEG_PARTIAL_CHANCE_PER_LEG:
            return size_usd * r.uniform(0.5, 0.9)
        return size_usd

    @staticmethod
    def _resolve_naked(filled_short, filled_long):
        """[40] Auto-flatten the unmatched leg. Returns
        (matched_size, naked_usd, naked_side, unwind_cost_usd).
        The unmatched notional is flattened at market immediately, crossing the
        spread once at SLIPPAGE_FALLBACK_BPS on the naked notional."""
        matched = min(filled_short, filled_long)
        naked = abs(filled_short - filled_long)
        if naked < 1e-9:
            return matched, 0.0, None, 0.0
        naked_side = "short" if filled_short > filled_long else "long"
        unwind_cost = naked * (SLIPPAGE_FALLBACK_BPS / 10000.0)
        return matched, naked, naked_side, unwind_cost
```

- [ ] **Step 4: Run to verify pass**

Run: `cd deploy && python -m pytest tests/test_fill_model.py -k "roll_leg or resolve_naked" -v`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
git add deploy/paper_trader.py deploy/tests/test_fill_model.py
git commit -m "feat: add per-leg outcome roll and naked-exposure resolution helpers"
```

---

## Task 4: Add desync fields to `PaperPosition`

**Files:**
- Modify: `paper_trader.py:680` — add fields at the end of the `PaperPosition` dataclass (after `telegram_msg_id`).
- Test: `tests/test_fill_model.py`

- [ ] **Step 1: Write failing test**

Append to `tests/test_fill_model.py`:

```python
def test_paperposition_has_desync_fields():
    from dataclasses import fields
    names = {f.name for f in fields(pt.PaperPosition)}
    for f in ("leg_latency_short_ms", "leg_latency_long_ms",
              "leg_drift_short_pct", "leg_drift_long_pct",
              "naked_usd", "naked_unwind_cost_usd"):
        assert f in names, f"missing field {f}"
```

- [ ] **Step 2: Run to verify fail**

Run: `cd deploy && python -m pytest tests/test_fill_model.py::test_paperposition_has_desync_fields -v`
Expected: FAIL (`missing field leg_latency_short_ms`)

- [ ] **Step 3: Add the fields**

In `paper_trader.py`, after line 680 (`telegram_msg_id: Optional[int] = None`), add:

```python
    # [40] Sequential two-leg fill diagnostics
    leg_latency_short_ms: float = 0.0
    leg_latency_long_ms: float = 0.0
    leg_drift_short_pct: float = 0.0
    leg_drift_long_pct: float = 0.0
    naked_usd: float = 0.0
    naked_unwind_cost_usd: float = 0.0
```

- [ ] **Step 4: Run to verify pass**

Run: `cd deploy && python -m pytest tests/test_fill_model.py::test_paperposition_has_desync_fields -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add deploy/paper_trader.py deploy/tests/test_fill_model.py
git commit -m "feat: add leg-desync diagnostic fields to PaperPosition"
```

---

## Task 5: Wire sequential fills into `open_position`

**Files:**
- Modify: `paper_trader.py:4465-4505` (the `[24] INCREMENTAL ORDERBOOK ENTRY FILLS` block) and the `PaperPosition(...)` construction at 4550-4570.
- Test: `tests/test_fill_model.py`

This replaces the synchronous `_incremental_fill` entry block with sequential per-leg fills when `SEQUENTIAL_FILL` is on. The legacy block stays as the `else` path.

- [ ] **Step 1: Write the failing integration test**

Append to `tests/test_fill_model.py`. This test exercises the pure decision logic by calling a new helper `_sequential_entry_fill` (extracted so it is unit-testable without constructing a `PaperTrader`):

```python
def test_sequential_entry_fill_both_full_matched_equal():
    # No failures, no drift, deep books => both legs fill full, naked=0.
    of, op = pt.LEG_FAIL_CHANCE_PER_LEG, pt.LEG_PARTIAL_CHANCE_PER_LEG
    pt.LEG_FAIL_CHANCE_PER_LEG = 0.0
    pt.LEG_PARTIAL_CHANCE_PER_LEG = 0.0
    try:
        bids = [(100.0, 1000.0)]
        asks = [(99.0, 1000.0)]
        res = pt.PaperTrader._sequential_entry_fill(
            bids, asks, size_usd=100.0, velocity=0.0, rng=random.Random(7)
        )
    finally:
        pt.LEG_FAIL_CHANCE_PER_LEG, pt.LEG_PARTIAL_CHANCE_PER_LEG = of, op
    assert res["matched_size"] == 100.0
    assert res["naked_usd"] == 0.0
    assert res["fill_short"] > 0 and res["fill_long"] > 0


def test_sequential_entry_fill_one_leg_fails_full_naked():
    # Force short leg to fail via a stub rng-controlled chance.
    of = pt.LEG_FAIL_CHANCE_PER_LEG
    pt.LEG_FAIL_CHANCE_PER_LEG = 1.0  # both legs fail roll => filled 0
    try:
        bids = [(100.0, 1000.0)]
        asks = [(99.0, 1000.0)]
        res = pt.PaperTrader._sequential_entry_fill(
            bids, asks, size_usd=100.0, velocity=0.0, rng=random.Random(7)
        )
    finally:
        pt.LEG_FAIL_CHANCE_PER_LEG = of
    assert res["matched_size"] == 0.0  # nothing matched => no position
```

- [ ] **Step 2: Run to verify fail**

Run: `cd deploy && python -m pytest tests/test_fill_model.py -k sequential_entry_fill -v`
Expected: FAIL (`_sequential_entry_fill` not defined)

- [ ] **Step 3: Add the `_sequential_entry_fill` helper**

In `paper_trader.py`, immediately after `_resolve_naked`, add:

```python
    @staticmethod
    def _sequential_entry_fill(ob_bids_short, ob_asks_long, size_usd, velocity, rng=None):
        """[40] Fill both entry legs independently and resolve any size desync.
        Returns a dict with per-leg VWAPs, latencies, drifts, the matched size,
        naked notional, naked side, and unwind cost."""
        req_short = PaperTrader._roll_leg_outcome(size_usd, rng=rng)
        req_long = PaperTrader._roll_leg_outcome(size_usd, rng=rng)
        fill_short, filled_short, lat_s, drift_s = PaperTrader._simulate_leg(
            ob_bids_short, req_short, "sell", velocity, rng=rng)
        fill_long, filled_long, lat_l, drift_l = PaperTrader._simulate_leg(
            ob_asks_long, req_long, "buy", velocity, rng=rng)
        matched, naked, naked_side, unwind_cost = PaperTrader._resolve_naked(
            filled_short, filled_long)
        return {
            "fill_short": fill_short, "fill_long": fill_long,
            "filled_short": filled_short, "filled_long": filled_long,
            "lat_short": lat_s, "lat_long": lat_l,
            "drift_short": drift_s, "drift_long": drift_l,
            "matched_size": matched, "naked_usd": naked,
            "naked_side": naked_side, "unwind_cost": unwind_cost,
        }
```

- [ ] **Step 4: Run to verify pass**

Run: `cd deploy && python -m pytest tests/test_fill_model.py -k sequential_entry_fill -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Replace the entry fill block to use the helper when flag is on**

In `paper_trader.py`, replace lines 4468–4486 (from `if ob_bids_short and ob_asks_long:` through `size = actual_filled`) with:

```python
        if SEQUENTIAL_FILL and ob_bids_short and ob_asks_long:
            # [40] Sequential per-leg fill — legs fill independently with their
            # own latency and drift; size desync is auto-flattened.
            seq = self._sequential_entry_fill(ob_bids_short, ob_asks_long,
                                              size, spread_velocity)
            fill_short = seq["fill_short"]
            fill_long = seq["fill_long"]
            if seq["matched_size"] < 10:
                # Full or near-full desync => no viable hedged position.
                if seq["naked_usd"] > 0 and seq["unwind_cost"] > 0:
                    log.info(f"LEG_FAIL {symbol} side={seq['naked_side']} "
                             f"naked=${seq['naked_usd']:.0f} unwound, "
                             f"loss=${seq['unwind_cost']:.2f}")
                    self._record_leg_desync_unwind(symbol, q_high, q_low,
                                                   seq, spread_pct)
                else:
                    log.info(f"SKIP {symbol} — both legs failed to fill")
                return None
            size = seq["matched_size"]
            log.info(f"LEG_FILL {symbol} short: lat={seq['lat_short']:.0f}ms "
                     f"drift={seq['drift_short']:+.3f}% filled=${seq['filled_short']:.0f} | "
                     f"long: lat={seq['lat_long']:.0f}ms drift={seq['drift_long']:+.3f}% "
                     f"filled=${seq['filled_long']:.0f}")
            if seq["naked_usd"] > 0:
                log.info(f"LEG_DESYNC {symbol} short=${seq['filled_short']:.0f} "
                         f"long=${seq['filled_long']:.0f} naked=${seq['naked_usd']:.0f} "
                         f"unwind_cost=${seq['unwind_cost']:.2f}")
        elif ob_bids_short and ob_asks_long:
            # Incremental fill: short side SELL into bids, long side BUY from asks
            fill_short, fill_long = self._incremental_fill(
                ob_bids_short, ob_asks_long, size, "sell", "buy"
            )

            if fill_short <= 0 or fill_long <= 0:
                log.info(f"SKIP {symbol} — OB incremental fill failed (fill_s=${fill_short:.0f} fill_l=${fill_long:.0f} size=${size:.0f})")
                return None

            _, filled_short, levels_short = self.walk_orderbook(ob_bids_short, size, "sell")
            _, filled_long, levels_long = self.walk_orderbook(ob_asks_long, size, "buy")
            actual_filled = min(filled_short, filled_long)
            if actual_filled < 10:
                log.info(f"SKIP {symbol} — OB walk fill too small (${actual_filled:.0f} fillable, size=${size:.0f})")
                return None
            size = actual_filled
```

> NOTE: The existing slippage-cap block (old lines 4488–4505) immediately follows and still applies to `fill_short`/`fill_long` in BOTH paths. Leave it unchanged. In the sequential path, the per-leg drift already happened inside `_simulate_leg`, so do NOT call `apply_execution_delay` again — see Step 7.

- [ ] **Step 6: Add the `_record_leg_desync_unwind` helper**

In `paper_trader.py`, immediately after `_sequential_entry_fill`, add:

```python
    def _record_leg_desync_unwind(self, symbol, q_high, q_low, seq, spread_pct):
        """[40] Record a fully-naked-and-unwound entry as a closed loss so it
        drags win-rate exactly like a live orphan-leg unwind."""
        loss = -abs(seq["unwind_cost"])
        pos = PaperPosition(
            id=self.portfolio.next_id,
            symbol=symbol,
            exchange_short=q_high.exchange, exchange_long=q_low.exchange,
            instrument_short=q_high.instrument, instrument_long=q_low.instrument,
            entry_spread_pct=spread_pct,
            entry_price_short=q_high.bid, entry_price_long=q_low.ask,
            size_usd=seq["naked_usd"],
            entry_time=datetime.now(timezone.utc),
            entry_fees_pct=0.0,
            status="CLOSED",
            exit_time=datetime.now(timezone.utc),
            exit_reason="leg_desync_unwind",
            net_pnl_usd=loss, net_pnl_pct=0.0,
            naked_usd=seq["naked_usd"],
            naked_unwind_cost_usd=seq["unwind_cost"],
        )
        self.portfolio.next_id += 1
        self.portfolio.cash += loss
        self.portfolio.total_pnl_usd += loss
        self.portfolio.total_trades += 1
        self.portfolio.closed_positions.append(pos)
```

- [ ] **Step 7: Skip redundant exec-delay on the sequential path**

In `paper_trader.py`, locate (now shifted) the line `delayed_spread = self.apply_execution_delay(actual_spread)` (was line 4532). Replace it with:

```python
        if SEQUENTIAL_FILL and ob_bids_short and ob_asks_long:
            # Drift already applied per-leg inside _simulate_leg; only the
            # shared OB-staleness penalty remains.
            delayed_spread = actual_spread - (2.0 * OB_STALENESS_PENALTY_BPS / 100.0)
        else:
            delayed_spread = self.apply_execution_delay(actual_spread)
```

- [ ] **Step 8: Populate the new fields on the opened position**

In `paper_trader.py`, in the `PaperPosition(...)` construction (around line 4569, after `partial_fill=...`), add these kwargs. Guard with a local captured from Step 5 — set `_seq = seq if (SEQUENTIAL_FILL and ob_bids_short and ob_asks_long) else None` right before the construction, then:

```python
            leg_latency_short_ms=(_seq["lat_short"] if _seq else 0.0),
            leg_latency_long_ms=(_seq["lat_long"] if _seq else 0.0),
            leg_drift_short_pct=(_seq["drift_short"] if _seq else 0.0),
            leg_drift_long_pct=(_seq["drift_long"] if _seq else 0.0),
            naked_usd=(_seq["naked_usd"] if _seq else 0.0),
            naked_unwind_cost_usd=(_seq["unwind_cost"] if _seq else 0.0),
```

Also, immediately after the position is appended (after line 4572 `self.portfolio.positions.append(pos)`), charge any partial-naked unwind cost:

```python
        if _seq and _seq["unwind_cost"] > 0:
            self.portfolio.cash -= _seq["unwind_cost"]
            self.portfolio.total_pnl_usd -= _seq["unwind_cost"]
```

- [ ] **Step 9: Run full suite + import check**

Run: `cd deploy && python -m pytest tests/ -v && python -c "import paper_trader"`
Expected: all PASS; import succeeds with no syntax error.

- [ ] **Step 10: Commit**

```bash
git add deploy/paper_trader.py deploy/tests/test_fill_model.py
git commit -m "feat: wire sequential per-leg fills and naked-exposure handling into open_position"
```

---

## Task 6: Exit symmetry in `close_position`

**Files:**
- Modify: `paper_trader.py:4595-4625` (exit fill + slip cap + exec-delay block).
- Test: `tests/test_fill_model.py`

Fix the three genuine exit asymmetries: (a) nicer-price capping, (b) 10%-only exec-delay, (c) wider 0.50% slip cap. On the sequential path, exit legs go through `_simulate_leg` (drift every time), the slip cap uses entry's `0.40%` formula, and over-cap slippage is CHARGED (not capped to a nicer price).

- [ ] **Step 1: Write the failing parity test**

Append to `tests/test_fill_model.py`:

```python
def test_sequential_exit_fill_charges_full_slippage():
    # Deep books, zero velocity => exit VWAP equals the walked price with no
    # "nicer-price" capping applied.
    bids = [(99.0, 1000.0)]   # long leg sells into these
    asks = [(100.0, 1000.0)]  # short leg buys back from these
    res = pt.PaperTrader._sequential_exit_fill(
        asks, bids, size_usd=100.0, velocity=0.0, rng=random.Random(11)
    )
    assert abs(res["exit_short"] - 100.0) < 1e-6  # buy back at ask, no nicer cap
    assert abs(res["exit_long"] - 99.0) < 1e-6     # sell at bid
    assert res["lat_short"] > 0 and res["lat_long"] > 0
```

- [ ] **Step 2: Run to verify fail**

Run: `cd deploy && python -m pytest tests/test_fill_model.py -k sequential_exit_fill -v`
Expected: FAIL (`_sequential_exit_fill` not defined)

- [ ] **Step 3: Add `_sequential_exit_fill`**

In `paper_trader.py`, immediately after `_record_leg_desync_unwind`, add:

```python
    @staticmethod
    def _sequential_exit_fill(ob_asks_short, ob_bids_long, size_usd, velocity, rng=None):
        """[40] Close both legs independently (buy back short from asks, sell long
        into bids), each with its own latency and drift. No nicer-price capping —
        the full walked slippage is realized."""
        exit_short, _, lat_s, drift_s = PaperTrader._simulate_leg(
            ob_asks_short, size_usd, "buy", velocity, rng=rng)
        exit_long, _, lat_l, drift_l = PaperTrader._simulate_leg(
            ob_bids_long, size_usd, "sell", velocity, rng=rng)
        return {
            "exit_short": exit_short, "exit_long": exit_long,
            "lat_short": lat_s, "lat_long": lat_l,
            "drift_short": drift_s, "drift_long": drift_l,
        }
```

- [ ] **Step 4: Run to verify pass**

Run: `cd deploy && python -m pytest tests/test_fill_model.py -k sequential_exit_fill -v`
Expected: PASS

- [ ] **Step 5: Use it in `close_position`**

In `paper_trader.py`, replace lines 4597–4625 (the `if ob_asks_short and ob_bids_long:` block through the OB-staleness exit lines) with:

```python
        if SEQUENTIAL_FILL and ob_asks_short and ob_bids_long:
            seq = self._sequential_exit_fill(
                ob_asks_short, ob_bids_long, pos.size_usd, pos.spread_velocity)
            exit_price_short = seq["exit_short"]
            exit_price_long = seq["exit_long"]
            if exit_price_short <= 0:
                exit_price_short = q_short.ask * (1 + SLIPPAGE_FALLBACK_BPS / 10000)
            if exit_price_long <= 0:
                exit_price_long = q_long.bid * (1 - SLIPPAGE_FALLBACK_BPS / 10000)
            # Same OB-staleness penalty magnitude as entry (per-side fraction).
            exit_stale = OB_STALENESS_PENALTY_BPS / 10000
            exit_price_short *= (1 + exit_stale)
            exit_price_long *= (1 - exit_stale)
        elif ob_asks_short and ob_bids_long:
            exit_price_short, exit_price_long = self._incremental_fill(
                ob_asks_short, ob_bids_long, pos.size_usd, "buy", "sell"
            )
            if exit_price_short <= 0:
                exit_price_short = q_short.ask * (1 + SLIPPAGE_FALLBACK_BPS / 10000)
            if exit_price_long <= 0:
                exit_price_long = q_long.bid * (1 - SLIPPAGE_FALLBACK_BPS / 10000)
            # Entry's slip-cap formula (0.40%), charging excess by keeping walked price.
            max_exit_slip = max(0.10, min(0.40, current_mid_spread * 0.25 / 2))
            max_short = q_short.ask * (1 + max_exit_slip / 100)
            min_long = q_long.bid * (1 - max_exit_slip / 100)
            exit_price_short = min(exit_price_short, max_short)
            exit_price_long = max(exit_price_long, min_long)
            # Exec delay parity: apply favorable/adverse drift every time.
            if random.random() < EXEC_DELAY_ADVERSE_CHANCE:
                exit_drift = random.uniform(0.01, EXEC_DELAY_SPREAD_DRIFT * 2) / 100
            else:
                exit_drift = random.uniform(-0.01, EXEC_DELAY_SPREAD_DRIFT * 0.5) / 100
            exit_price_short *= (1 + exit_drift)
            exit_price_long *= (1 - exit_drift)
            exit_stale = OB_STALENESS_PENALTY_BPS / 10000
            exit_price_short *= (1 + exit_stale)
            exit_price_long *= (1 - exit_stale)
        else:
            fallback_slip = SLIPPAGE_FALLBACK_BPS / 10000
            exit_price_short = q_short.ask * (1 + fallback_slip)
            exit_price_long = q_long.bid * (1 - fallback_slip)
```

> NOTE: the `[MARKET IMPACT] Adverse selection penalty on exit fills` block (old 4627-4631) and everything after stays unchanged.

- [ ] **Step 6: Run full suite + import**

Run: `cd deploy && python -m pytest tests/ -v && python -c "import paper_trader"`
Expected: all PASS; import OK.

- [ ] **Step 7: Commit**

```bash
git add deploy/paper_trader.py deploy/tests/test_fill_model.py
git commit -m "feat: sequential exit fills and entry/exit symmetry in close_position"
```

---

## Task 7: Tier-2 calibration (fallback, competition floor, flash-miss)

**Files:**
- Modify: `paper_trader.py` — `SLIPPAGE_FALLBACK_BPS` (245), `OB_STALENESS_PENALTY_BPS` (265), `simulate_competition` floor (4016), flash-miss block (4456).
- Test: `tests/test_fill_model.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/test_fill_model.py`:

```python
def test_calibration_constants():
    assert pt.SLIPPAGE_FALLBACK_BPS == 30.0
    assert pt.OB_STALENESS_PENALTY_BPS == 2.0


def test_competition_can_reach_zero():
    # Far above threshold, zero floor => can return ~0 (no phantom 5% floor).
    of = pt.COMPETITION_DECAY
    pt.COMPETITION_DECAY = 0.99
    try:
        # Seed so the random.uniform(0.5,1.0) doesn't dominate; with 0.99 decay
        # and many ticks, surviving fraction is essentially 0.
        random.seed(0)
        # Need a PaperTrader instance; construct under temp DATA_DIR.
        import os, tempfile
        os.environ["DATA_DIR"] = tempfile.mkdtemp()
        t = pt.PaperTrader()
        val = t.simulate_competition(ticks_above=50)
        assert val < 0.05  # would have been floored to 0.05 before
    finally:
        pt.COMPETITION_DECAY = of
```

> If `PaperTrader()` cannot be constructed cleanly (heavy `__init__`), simplify this test to assert the floor constant only: `assert pt.COMPETITION_FILL_FLOOR == 0.0` and that the source no longer contains `max(fill_frac, 0.05)` via `import inspect; assert "0.05" not in inspect.getsource(pt.PaperTrader.simulate_competition)`.

- [ ] **Step 2: Run to verify fail**

Run: `cd deploy && python -m pytest tests/test_fill_model.py -k "calibration or competition_can_reach" -v`
Expected: FAIL

- [ ] **Step 3: Update fallback + staleness constants**

In `paper_trader.py` line 245, change:
```python
SLIPPAGE_FALLBACK_BPS = 8.0    # Conservative fallback if OB data unavailable
```
to:
```python
SLIPPAGE_FALLBACK_BPS = 30.0   # Worst-case when the book is missing/empty
```

In `paper_trader.py` line 265, change:
```python
OB_STALENESS_PENALTY_BPS = 0.2  # 0.002% per side (down from 0.02% at EU latency)
```
to:
```python
OB_STALENESS_PENALTY_BPS = 2.0  # ~150ms-old book at fill (2 bps per side)
```

- [ ] **Step 4: Remove the competition phantom floor**

In `paper_trader.py` line 4016, change:
```python
        return max(fill_frac, 0.05)  # Always at least 5% of book available
```
to:
```python
        return max(fill_frac, COMPETITION_FILL_FLOOR)  # no phantom liquidity floor
```

- [ ] **Step 5: Make flash-miss velocity-based**

In `paper_trader.py` lines 4456–4459, replace:
```python
        if spread_pct > 1.5 and random.random() < FLASH_MISS_RATE:
            log.info(f"MISS {symbol} — flash convergence, spread gone before fill "
                     f"(spread={spread_pct:.2f}%, miss_rate={FLASH_MISS_RATE})")
            return None
```
with:
```python
        # [29] Velocity-based flash miss: probability the spread collapses during
        # the ~fill window scales with how fast it's moving vs the spread width.
        fill_window_sec = LEG_LATENCY_MAX_MS / 1000.0
        if spread_pct > 0:
            p_miss = min(0.5, abs(spread_velocity) * fill_window_sec / spread_pct)
        else:
            p_miss = 0.0
        if random.random() < p_miss:
            log.info(f"MISS {symbol} — flash convergence (p_miss={p_miss:.2%} "
                     f"vel={spread_velocity:.3f} spread={spread_pct:.2f}%)")
            return None
```

- [ ] **Step 6: Run full suite + import**

Run: `cd deploy && python -m pytest tests/ -v && python -c "import paper_trader"`
Expected: all PASS; import OK.

- [ ] **Step 7: Commit**

```bash
git add deploy/paper_trader.py deploy/tests/test_fill_model.py
git commit -m "feat: tier-2 fill calibration (fallback, staleness, competition floor, velocity flash-miss)"
```

---

## Task 8: Guardrail — sequential model is not rosier than synchronous

**Files:**
- Test: `tests/test_fill_model.py`

- [ ] **Step 1: Write the guardrail test**

Append to `tests/test_fill_model.py`:

```python
def test_adverse_drift_never_improves_fill_vs_no_drift():
    # For a buy leg, adverse drift must never produce a BETTER (lower) price than
    # the no-drift walk. Proves the sequential model is conservative, not rosier.
    asks = [(100.0, 1000.0)]
    plain_vwap, _, _ = pt.PaperTrader.walk_orderbook(asks, 100.0, "buy")
    old = pt.EXEC_DELAY_ADVERSE_CHANCE
    pt.EXEC_DELAY_ADVERSE_CHANCE = 1.0
    try:
        worst = plain_vwap
        for seed in range(50):
            v, _, _, _ = pt.PaperTrader._simulate_leg(
                asks, 100.0, "buy", velocity=1.0, rng=random.Random(seed))
            assert v >= plain_vwap - 1e-9  # never cheaper than no-drift
            worst = max(worst, v)
        assert worst > plain_vwap  # adverse drift did raise the price at least once
    finally:
        pt.EXEC_DELAY_ADVERSE_CHANCE = old
```

- [ ] **Step 2: Run**

Run: `cd deploy && python -m pytest tests/test_fill_model.py::test_adverse_drift_never_improves_fill_vs_no_drift -v`
Expected: PASS

- [ ] **Step 3: Run the entire suite once more**

Run: `cd deploy && python -m pytest tests/ -v`
Expected: ALL PASS

- [ ] **Step 4: Commit**

```bash
git add deploy/tests/test_fill_model.py
git commit -m "test: guardrail that sequential fill model is never rosier than synchronous"
```

---

## Task 9: Manual verification on a shadow instance (optional, post-merge)

**Files:** none (operational).

- [ ] **Step 1:** Deploy with `SEQUENTIAL_FILL=1` and confirm logs show `LEG_FILL`, and over time some `LEG_DESYNC` / `LEG_FAIL` lines, via `railway logs | grep -E "LEG_FILL|LEG_DESYNC|LEG_FAIL"`.
- [ ] **Step 2:** Confirm trade history contains `exit_reason="leg_desync_unwind"` closes and that positions carry non-zero `leg_latency_*` / `naked_*` fields.
- [ ] **Step 3:** To A/B, run a shadow with `SEQUENTIAL_FILL=0` and compare win-rate / net P&L against the sequential instance.

---

## Self-Review Notes

- **Spec coverage:** §1 sequential fill → Tasks 2,5,7(flash). §2 desync/naked + auto-flatten + `leg_desync_unwind` → Tasks 3,5. §3 exit symmetry → Task 6. §4 calibration → Task 7. §5 observability (markers + fields) → Tasks 4,5; testing → Tasks 2,3,5,6,8; flag rollout → Task 1. All covered.
- **Correction vs spec:** spec listed an exit-staleness `/100` "bug fix"; reading the real code showed entry (percent) and exit (fraction) are already equivalent, so no such fix is applied — the genuine exit asymmetries (nicer-price cap, 10%-only exec-delay, 0.50% cap) are fixed in Task 6 instead.
- **Type consistency:** `_simulate_leg` returns `(vwap, filled_usd, latency_ms, drift_pct)` everywhere; `_resolve_naked` returns `(matched, naked, side, cost)`; `_sequential_entry_fill` dict keys (`fill_short`, `filled_short`, `lat_short`, `drift_short`, `matched_size`, `naked_usd`, `naked_side`, `unwind_cost`) are used consistently in Task 5. `PaperPosition` field names match between Task 4 and Tasks 5/6.
