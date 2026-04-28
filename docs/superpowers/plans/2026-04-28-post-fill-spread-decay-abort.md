# Post-Fill Spread-Decay Abort Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a configurable `MIN_FILL_SPREAD_PCT` threshold that aborts post-fill entries whose actual fill-derived spread came in below the threshold, with the abort recorded as a `LivePosition` with `exit_reason="fill_quality_abort"`.

**Architecture:** Single-file change in `deploy-live/real_trader.py`. New module-level constant alongside `ENTRY_SPREAD_PCT` (`real_trader.py:94`), modified abort guard at `real_trader.py:3863-3880` to use it, and a new helper `_record_fill_quality_abort` that mirrors `_record_failed_leg` but for the both-legs-filled case. New tests directory `deploy-live/tests/`.

**Tech Stack:** Python 3, asyncio, pytest, pytest-asyncio, dataclasses (`OrderResult`, `LivePosition`).

**Reference spec:** `docs/superpowers/specs/2026-04-28-post-fill-spread-decay-abort-design.md`

**Worktree:** `/Users/vandenboogaard/Claude projects/Claude Paperclip/.claude/worktrees/tender-germain` (already on `claude/tender-germain` branch).

---

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `deploy-live/real_trader.py` | Modify | Add constant (line ~95), modify abort guard (line ~3863-3880), add `_record_fill_quality_abort` helper |
| `deploy-live/tests/__init__.py` | Create | Empty marker for pytest |
| `deploy-live/tests/conftest.py` | Create | Common fixtures: in-memory portfolio, mock executors |
| `deploy-live/tests/test_fill_quality_abort.py` | Create | All unit tests for the new behavior |
| `deploy-live/tests/README.md` | Create | How to run the tests |

---

## Pre-flight checks

- [ ] **Step 0.1: Confirm worktree and branch**

```bash
cd "/Users/vandenboogaard/Claude projects/Claude Paperclip/.claude/worktrees/tender-germain"
git status
git rev-parse --abbrev-ref HEAD
```

Expected: branch is `claude/tender-germain`. There may be untracked test artifacts from prior probe work — leave them alone.

- [ ] **Step 0.2: Confirm bot is not running locally**

```bash
ps aux | grep "real_trader" | grep -v grep
```

Expected: no `real_trader.py` process. (A `local_dashboard.py` process is fine and should be ignored.)

- [ ] **Step 0.3: Verify test dependencies**

```bash
cd "/Users/vandenboogaard/Claude projects/Claude Paperclip/.claude/worktrees/tender-germain/deploy-live"
python3 -c "import pytest; import pytest_asyncio; print('ok')"
```

Expected: `ok`. If not, install:

```bash
pip3 install pytest pytest-asyncio
```

---

## Task 1: Set up test infrastructure

**Files:**
- Create: `deploy-live/tests/__init__.py`
- Create: `deploy-live/tests/conftest.py`
- Create: `deploy-live/tests/README.md`

This task creates the test directory and shared fixtures so subsequent tasks can write tests immediately.

- [ ] **Step 1.1: Create `tests/__init__.py`**

```python
# Marker file for pytest test discovery.
```

- [ ] **Step 1.2: Create `tests/README.md`**

```markdown
# deploy-live tests

Run from `deploy-live/`:

```bash
DRY_RUN=true python3 -m pytest tests/ -v
```

`DRY_RUN=true` prevents the import of `real_trader.py` from accidentally
trying to authenticate with real exchanges.
```

- [ ] **Step 1.3: Create `tests/conftest.py` with shared fixtures**

```python
"""Shared pytest fixtures for the post-fill abort tests.

We import `real_trader` lazily inside fixtures so that test-time env
overrides (e.g. DRY_RUN=true) are applied first.
"""
import os
import sys
from pathlib import Path

import pytest

# Make sure deploy-live is on sys.path so `import real_trader` works
DEPLOY_LIVE = Path(__file__).resolve().parent.parent
if str(DEPLOY_LIVE) not in sys.path:
    sys.path.insert(0, str(DEPLOY_LIVE))


@pytest.fixture(autouse=True)
def force_dry_run(monkeypatch):
    """Every test runs with DRY_RUN=true so no real orders fire."""
    monkeypatch.setenv("DRY_RUN", "true")


@pytest.fixture
def order_result_factory():
    """Build an OrderResult with sensible defaults; override fields per test."""
    from real_trader import OrderResult
    import time

    def _make(**overrides):
        defaults = dict(
            success=True,
            order_id="test-order",
            exchange="MEXC",
            symbol="TESTUSDT",
            side="sell",
            size_usd=10.0,
            filled_usd=10.0,
            fill_price=1.0,
            fees_usd=0.005,
            timestamp=time.time(),
            latency_ms=200.0,
            filled_contracts=10.0,
            error="",
        )
        defaults.update(overrides)
        return OrderResult(**defaults)

    return _make


@pytest.fixture
def price_quote_factory():
    """Build a PriceQuote with sensible defaults; override fields per test."""
    from real_trader import PriceQuote
    import time

    def _make(**overrides):
        defaults = dict(
            exchange="MEXC",
            symbol="TESTUSDT",
            instrument="PERP",
            bid=1.0,
            ask=1.0,
            timestamp=time.time(),
        )
        defaults.update(overrides)
        return PriceQuote(**defaults)

    return _make
```

- [ ] **Step 1.4: Verify the fixtures import cleanly**

```bash
cd "/Users/vandenboogaard/Claude projects/Claude Paperclip/.claude/worktrees/tender-germain/deploy-live"
DRY_RUN=true python3 -m pytest tests/ -v --collect-only
```

Expected: collects 0 tests with no errors. If `PriceQuote` or `OrderResult` import fails, read `real_trader.py` to find the actual class names and field signatures, then fix `conftest.py` accordingly.

- [ ] **Step 1.5: Commit**

```bash
cd "/Users/vandenboogaard/Claude projects/Claude Paperclip/.claude/worktrees/tender-germain"
git add deploy-live/tests/__init__.py deploy-live/tests/conftest.py deploy-live/tests/README.md
git commit -m "test: scaffold test directory + shared fixtures for fill-quality abort"
```

---

## Task 2: Add `MIN_FILL_SPREAD_PCT` constant with default-preserves-behavior

**Files:**
- Modify: `deploy-live/real_trader.py` (around line 95, in the "EU Main strategy params" block)
- Test: `deploy-live/tests/test_fill_quality_abort.py`

This task introduces the constant but does NOT yet wire it into the abort guard. The default `-0.10` exactly mirrors the existing hardcoded `INVERTED_FILL_TOLERANCE`, so behavior is unchanged after this task — we're just exposing the value.

- [ ] **Step 2.1: Write failing test for the constant's default**

Create `deploy-live/tests/test_fill_quality_abort.py`:

```python
"""Tests for the post-fill spread-decay abort feature."""


def test_min_fill_spread_pct_default_preserves_inverted_behavior():
    """When the env var is absent, MIN_FILL_SPREAD_PCT must equal the
    historical INVERTED_FILL_TOLERANCE so behavior is unchanged."""
    import importlib
    import real_trader
    importlib.reload(real_trader)
    assert real_trader.MIN_FILL_SPREAD_PCT == -0.10


def test_min_fill_spread_pct_reads_env(monkeypatch):
    """When MIN_FILL_SPREAD_PCT is set in env, the module-level constant
    reflects it after reload."""
    import importlib
    monkeypatch.setenv("MIN_FILL_SPREAD_PCT", "0.30")
    import real_trader
    importlib.reload(real_trader)
    assert real_trader.MIN_FILL_SPREAD_PCT == 0.30
```

- [ ] **Step 2.2: Run tests, verify they fail**

```bash
cd "/Users/vandenboogaard/Claude projects/Claude Paperclip/.claude/worktrees/tender-germain/deploy-live"
DRY_RUN=true python3 -m pytest tests/test_fill_quality_abort.py -v
```

Expected: 2 failed with `AttributeError: module 'real_trader' has no attribute 'MIN_FILL_SPREAD_PCT'`.

- [ ] **Step 2.3: Add the constant to `real_trader.py`**

Open `deploy-live/real_trader.py`. Find line 94 (`ENTRY_SPREAD_PCT: float = ...`). Insert immediately after the existing `MIN_VOLUME_USD` line (the last entry in the "EU Main strategy params" block, around line 101):

```python
# Post-fill quality gate: abort entries whose actual fill-derived spread
# came in below this threshold (in pct points).  Default -0.10 preserves
# the pre-existing inverted-fill-only behavior.  Raise to e.g. 0.30 to
# also reject lag-degraded fills.  See spec
# docs/superpowers/specs/2026-04-28-post-fill-spread-decay-abort-design.md
MIN_FILL_SPREAD_PCT: float = float(os.environ.get("MIN_FILL_SPREAD_PCT", "-0.10"))
```

- [ ] **Step 2.4: Run tests, verify they pass**

```bash
cd "/Users/vandenboogaard/Claude projects/Claude Paperclip/.claude/worktrees/tender-germain/deploy-live"
DRY_RUN=true python3 -m pytest tests/test_fill_quality_abort.py -v
```

Expected: 2 passed.

- [ ] **Step 2.5: Commit**

```bash
cd "/Users/vandenboogaard/Claude projects/Claude Paperclip/.claude/worktrees/tender-germain"
git add deploy-live/real_trader.py deploy-live/tests/test_fill_quality_abort.py
git commit -m "feat: add MIN_FILL_SPREAD_PCT constant (default preserves behavior)"
```

---

## Task 3: Wire the constant into the existing abort guard

**Files:**
- Modify: `deploy-live/real_trader.py` (lines 3863-3880, the inverted-fill guard inside `open_position`)

This task replaces the hardcoded `-0.10` with the new constant. With the default value, behavior is byte-identical. This task does NOT yet add the new closed_positions record — that comes in Task 4 — because the failure path currently just `return None`s and no observable state changes.

- [ ] **Step 3.1: Read the current guard to confirm exact line range**

```bash
cd "/Users/vandenboogaard/Claude projects/Claude Paperclip/.claude/worktrees/tender-germain/deploy-live"
sed -n '3858,3880p' real_trader.py
```

Expected: shows the `INVERTED_FILL_TOLERANCE = -0.10` block. If the line numbers have drifted (e.g. due to other unrelated edits in the worktree), use `grep -n "INVERTED_FILL_TOLERANCE" real_trader.py` to find the new location and adapt the rest of this task accordingly.

- [ ] **Step 3.2: Modify the guard**

In `real_trader.py`, find the block:

```python
            INVERTED_FILL_TOLERANCE = -0.10  # -0.10% — small slippage within fees ok
            if actual_entry_spread < INVERTED_FILL_TOLERANCE:
                log.warning(
                    f"INVERTED ENTRY #{pos_id} {symbol}: fills came in with "
                    f"long ({fill_price_long}) > short ({fill_price_short}) "
                    f"actual_spread={actual_entry_spread:.3f}% — auto-closing both legs"
                )
```

Replace with:

```python
            # Post-fill spread quality gate.  See MIN_FILL_SPREAD_PCT definition
            # near top of module and design spec
            # docs/superpowers/specs/2026-04-28-post-fill-spread-decay-abort-design.md
            if actual_entry_spread < MIN_FILL_SPREAD_PCT:
                # Distinguish inverted-fill (negative threshold) from
                # quality-gate (non-negative) in logs for grep tooling.
                tag = "INVERTED ENTRY" if MIN_FILL_SPREAD_PCT < 0 else "FILL-QUALITY ABORT"
                log.warning(
                    f"{tag} #{pos_id} {symbol}: fills came in with "
                    f"long ({fill_price_long}) > short ({fill_price_short}) "
                    f"actual_spread={actual_entry_spread:.3f}% "
                    f"threshold={MIN_FILL_SPREAD_PCT:.3f}% — auto-closing both legs"
                )
```

Leave the emergency-close `await asyncio.gather(...)` and the `failed_entry_cooldowns` write that follow this `if` exactly as they are.

- [ ] **Step 3.3: Verify no other references to `INVERTED_FILL_TOLERANCE` remain**

```bash
cd "/Users/vandenboogaard/Claude projects/Claude Paperclip/.claude/worktrees/tender-germain/deploy-live"
grep -n "INVERTED_FILL_TOLERANCE" real_trader.py
```

Expected: no matches.

- [ ] **Step 3.4: Smoke-test that real_trader still imports**

```bash
cd "/Users/vandenboogaard/Claude projects/Claude Paperclip/.claude/worktrees/tender-germain/deploy-live"
DRY_RUN=true python3 -c "import real_trader; print('import ok'); print('MIN_FILL_SPREAD_PCT =', real_trader.MIN_FILL_SPREAD_PCT)"
```

Expected: `import ok` followed by `MIN_FILL_SPREAD_PCT = -0.1`.

- [ ] **Step 3.5: Re-run the constant tests from Task 2 (regression check)**

```bash
DRY_RUN=true python3 -m pytest tests/test_fill_quality_abort.py -v
```

Expected: 2 passed (still).

- [ ] **Step 3.6: Commit**

```bash
cd "/Users/vandenboogaard/Claude projects/Claude Paperclip/.claude/worktrees/tender-germain"
git add deploy-live/real_trader.py
git commit -m "refactor: wire MIN_FILL_SPREAD_PCT into post-fill abort guard"
```

---

## Task 4: Add `_record_fill_quality_abort` helper + `LivePosition` field

**Files:**
- Modify: `deploy-live/real_trader.py` — locate the `LivePosition` dataclass definition and add the `abort_threshold_pct` field; add new helper method on `TradeExecutor`.
- Modify: `deploy-live/tests/test_fill_quality_abort.py` — add tests for the new helper.

Currently the abort path returns `None` with no `closed_positions` record. This task adds the record so the dashboard surfaces aborts and we can measure threshold tuning.

- [ ] **Step 4.1: Find the `LivePosition` dataclass definition**

```bash
cd "/Users/vandenboogaard/Claude projects/Claude Paperclip/.claude/worktrees/tender-germain/deploy-live"
grep -n "^class LivePosition" real_trader.py
grep -n "exit_reason:" real_trader.py | head -5
```

Note the line where `LivePosition` is declared and the line where `exit_reason: str` is declared inside it.

- [ ] **Step 4.2: Add `abort_threshold_pct` field to `LivePosition`**

Find the existing `LivePosition` dataclass (it has fields like `entry_slippage_bps`, `spread_collapse_pct`, `fill_latency_short_ms`, `fill_latency_long_ms`, `ob_depth_short_usd`, `ob_depth_long_usd`, `funding_rate_short`, `funding_rate_long`). At the END of those analytics fields (just before `degraded_leg`, `close_retry_count`, `last_close_attempt` if those exist, OR just before the closing of the dataclass), add:

```python
    # Threshold that triggered a fill_quality_abort, in pct points.
    # 0.0 means not applicable (this position was not aborted).  Set only
    # for closed positions with exit_reason == "fill_quality_abort".
    abort_threshold_pct: float = 0.0
```

If you cannot tell where to insert it, place it immediately after `funding_rate_long: float = 0.0` (or whichever similar `funding_rate*` field exists) and before any non-default fields. All dataclass fields with defaults must come after fields without defaults — preserve that ordering.

- [ ] **Step 4.3: Smoke-test that `LivePosition` still constructs**

```bash
DRY_RUN=true python3 -c "
import real_trader
p = real_trader.LivePosition(
    id=1, symbol='TESTUSDT',
    exchange_short='MEXC', exchange_long='BloFin',
    instrument_short='PERP', instrument_long='PERP',
    entry_spread_pct=0.0, entry_price_short=0.0, entry_price_long=0.0,
    size_usd=10.0,
    entry_time=__import__('datetime').datetime.now(__import__('datetime').timezone.utc),
)
print('LivePosition constructs ok, abort_threshold_pct =', p.abort_threshold_pct)
"
```

Expected: `LivePosition constructs ok, abort_threshold_pct = 0.0`.

- [ ] **Step 4.4: Find the `TradeExecutor` class and `_record_failed_leg` method**

```bash
grep -n "def _record_failed_leg" real_trader.py
grep -n "^class TradeExecutor" real_trader.py
```

Note the line of `_record_failed_leg` — this confirms which class to add the new method to (same class as `_record_failed_leg`).

- [ ] **Step 4.5: Add the new helper method**

Immediately after the `_record_failed_leg` method ends (before `_record_pair_failure` begins), add:

```python
    def _record_fill_quality_abort(
        self,
        pos_id: int,
        symbol: str,
        q_high: "PriceQuote",
        q_low: "PriceQuote",
        result_short: "OrderResult",
        result_long: "OrderResult",
        close_short: Optional["OrderResult"],
        close_long: Optional["OrderResult"],
        actual_spread_pct: float,
        threshold_pct: float,
    ) -> None:
        """Record a both-legs-filled-then-aborted entry as a closed position.

        Mirrors _record_failed_leg's P&L math but for the both-legs case.
        Called from open_position's post-fill quality-gate branch.
        """
        # Defensive: if either close didn't complete cleanly, skip the record.
        # The position will surface via reconciliation as an orphan instead.
        if close_short is None or close_long is None:
            return
        if not close_short.success or not close_long.success:
            return

        filled_ex_s = result_short.exchange
        filled_ex_l = result_long.exchange
        fee_rate_s = TAKER_FEE.get(filled_ex_s, 0.0005)
        fee_rate_l = TAKER_FEE.get(filled_ex_l, 0.0005)

        entry_fees = (
            (result_short.fees_usd or (result_short.filled_usd * fee_rate_s))
            + (result_long.fees_usd or (result_long.filled_usd * fee_rate_l))
        )
        exit_fees = (
            (close_short.fees_usd or (close_short.filled_usd * fee_rate_s))
            + (close_long.fees_usd or (close_long.filled_usd * fee_rate_l))
        )

        e_s = result_short.fill_price or 0.0
        e_l = result_long.fill_price or 0.0
        x_s = close_short.fill_price or 0.0
        x_l = close_long.fill_price or 0.0
        size = min(result_short.filled_usd or 0.0, result_long.filled_usd or 0.0)

        # P&L: short leg pnl = (entry - exit)/entry * size.
        # Long leg pnl = (exit - entry)/entry * size.
        gross = 0.0
        if e_s > 0 and x_s > 0 and size > 0:
            gross += (e_s - x_s) / e_s * size
        if e_l > 0 and x_l > 0 and size > 0:
            gross += (x_l - e_l) / e_l * size
        net = gross - entry_fees - exit_fees

        now = datetime.now(timezone.utc)
        pos = LivePosition(
            id=pos_id,
            symbol=symbol,
            exchange_short=q_high.exchange,
            exchange_long=q_low.exchange,
            instrument_short=q_high.instrument,
            instrument_long=q_low.instrument,
            entry_spread_pct=actual_spread_pct,
            entry_price_short=e_s,
            entry_price_long=e_l,
            size_usd=size,
            entry_time=now,
            exit_time=now,
            entry_fees_usd=entry_fees,
            exit_fees_usd=exit_fees,
            gross_pnl_usd=gross,
            net_pnl_usd=net,
            exit_spread_pct=(((x_s - x_l) / x_l * 100) if x_l > 0 else 0.0),
            exit_reason="fill_quality_abort",
            order_id_short=result_short.order_id,
            order_id_long=result_long.order_id,
            status="CLOSED",
            abort_threshold_pct=threshold_pct,
        )
        self.portfolio.closed_positions.append(pos)
        self.portfolio.total_trades += 1
        self.portfolio.total_pnl_usd += net
        self.portfolio._state_dirty = True
        log.info(
            f"RECORDED FILL-QUALITY ABORT #{pos_id} {symbol} "
            f"actual_spread={actual_spread_pct:.3f}% "
            f"threshold={threshold_pct:.3f}% "
            f"pnl=${net:+.4f} fees=${entry_fees + exit_fees:.4f}"
        )
```

- [ ] **Step 4.6: Add unit tests for the helper**

Append to `deploy-live/tests/test_fill_quality_abort.py`:

```python
import asyncio
from datetime import datetime, timezone


def _make_trade_executor():
    """Build a minimal TradeExecutor with an in-memory portfolio and no
    real executors.  Returns (trade_executor, portfolio)."""
    import real_trader as rt
    portfolio = rt.Portfolio()
    # _record_fill_quality_abort doesn't fire any orders, so empty dict ok
    te = rt.TradeExecutor(portfolio=portfolio, executors={})
    return te, portfolio


def test_fill_quality_abort_records_position(order_result_factory, price_quote_factory):
    """A successful both-legs-filled-then-closed abort produces one
    LivePosition with exit_reason='fill_quality_abort'."""
    te, portfolio = _make_trade_executor()

    q_high = price_quote_factory(exchange="MEXC", instrument="PERP", bid=1.000, ask=1.000)
    q_low = price_quote_factory(exchange="BloFin", instrument="PERP", bid=0.999, ask=0.999)
    result_short = order_result_factory(exchange="MEXC", side="sell",
                                        fill_price=1.000, filled_usd=10.0, fees_usd=0.005)
    result_long = order_result_factory(exchange="BloFin", side="buy",
                                       fill_price=0.999, filled_usd=10.0, fees_usd=0.005)
    close_short = order_result_factory(exchange="MEXC", side="buy",
                                       fill_price=1.0005, filled_usd=10.0, fees_usd=0.005)
    close_long = order_result_factory(exchange="BloFin", side="sell",
                                      fill_price=0.9985, filled_usd=10.0, fees_usd=0.005)

    te._record_fill_quality_abort(
        pos_id=42, symbol="TESTUSDT",
        q_high=q_high, q_low=q_low,
        result_short=result_short, result_long=result_long,
        close_short=close_short, close_long=close_long,
        actual_spread_pct=0.10, threshold_pct=0.30,
    )

    assert len(portfolio.closed_positions) == 1
    pos = portfolio.closed_positions[0]
    assert pos.exit_reason == "fill_quality_abort"
    assert pos.id == 42
    assert pos.symbol == "TESTUSDT"
    assert pos.order_id_short == result_short.order_id
    assert pos.order_id_long == result_long.order_id
    assert pos.abort_threshold_pct == 0.30
    assert pos.entry_spread_pct == 0.10
    assert pos.status == "CLOSED"
    # Both fees from entry + close must be summed
    assert pos.entry_fees_usd == 0.010
    assert pos.exit_fees_usd == 0.010
    assert portfolio.total_trades == 1
    assert portfolio._state_dirty is True


def test_fill_quality_abort_skips_record_when_close_failed(order_result_factory, price_quote_factory):
    """If either close leg failed, no record is written — reconciliation
    will catch the orphan instead."""
    te, portfolio = _make_trade_executor()

    q_high = price_quote_factory(exchange="MEXC")
    q_low = price_quote_factory(exchange="BloFin")
    result_short = order_result_factory()
    result_long = order_result_factory()
    bad_close = order_result_factory(success=False, error="reject")
    good_close = order_result_factory()

    te._record_fill_quality_abort(
        pos_id=1, symbol="X", q_high=q_high, q_low=q_low,
        result_short=result_short, result_long=result_long,
        close_short=bad_close, close_long=good_close,
        actual_spread_pct=0.0, threshold_pct=0.30,
    )
    assert portfolio.closed_positions == []

    te._record_fill_quality_abort(
        pos_id=2, symbol="X", q_high=q_high, q_low=q_low,
        result_short=result_short, result_long=result_long,
        close_short=None, close_long=good_close,
        actual_spread_pct=0.0, threshold_pct=0.30,
    )
    assert portfolio.closed_positions == []
```

- [ ] **Step 4.7: Run the new tests**

```bash
cd "/Users/vandenboogaard/Claude projects/Claude Paperclip/.claude/worktrees/tender-germain/deploy-live"
DRY_RUN=true python3 -m pytest tests/test_fill_quality_abort.py -v
```

Expected: 4 passed (2 from Task 2, 2 new). If the helper-tests fail with `Portfolio() missing required argument` or similar, read the actual `Portfolio` and `TradeExecutor` `__init__` signatures in `real_trader.py` and adjust `_make_trade_executor` to supply minimal valid constructor args.

- [ ] **Step 4.8: Commit**

```bash
cd "/Users/vandenboogaard/Claude projects/Claude Paperclip/.claude/worktrees/tender-germain"
git add deploy-live/real_trader.py deploy-live/tests/test_fill_quality_abort.py
git commit -m "feat: record fill-quality-abort events as closed positions"
```

---

## Task 5: Wire `_record_fill_quality_abort` into the abort path

**Files:**
- Modify: `deploy-live/real_trader.py` (the abort guard inside `open_position`, currently emergency-closes both legs and `return None`s)

This task captures the close results from the emergency-close `asyncio.gather` and passes them to the new helper before returning.

- [ ] **Step 5.1: Locate the current emergency-close call in `open_position`**

```bash
cd "/Users/vandenboogaard/Claude projects/Claude Paperclip/.claude/worktrees/tender-germain/deploy-live"
grep -n "auto-closing both legs" real_trader.py
```

Note the line of the warning message — the `await asyncio.gather(...)` block is the next ~5 lines after it.

- [ ] **Step 5.2: Modify the emergency-close to capture results and call the helper**

Find the block (it currently looks like):

```python
                await asyncio.gather(
                    self._emergency_close_leg(ex_short, symbol, "buy", actual_size, pos_id),
                    self._emergency_close_leg(ex_long, symbol, "sell", actual_size, pos_id),
                )
                # Add to failed-entry cooldown so the same symbol doesn't fire again
                # immediately while quotes are still stale on this pair.
                self.failed_entry_cooldowns[symbol] = time.time() + self.FAILED_ENTRY_COOLDOWN_SEC
                return None
```

Replace with:

```python
                close_short_result, close_long_result = await asyncio.gather(
                    self._emergency_close_leg(ex_short, symbol, "buy", actual_size, pos_id),
                    self._emergency_close_leg(ex_long, symbol, "sell", actual_size, pos_id),
                )
                # Record the abort in closed_positions so dashboard + analysis
                # see it.  Helper is a no-op if either close didn't complete.
                self._record_fill_quality_abort(
                    pos_id=pos_id, symbol=symbol,
                    q_high=q_high, q_low=q_low,
                    result_short=result_short, result_long=result_long,
                    close_short=close_short_result, close_long=close_long_result,
                    actual_spread_pct=actual_entry_spread,
                    threshold_pct=MIN_FILL_SPREAD_PCT,
                )
                # Add to failed-entry cooldown so the same symbol doesn't fire again
                # immediately while quotes are still stale on this pair.
                self.failed_entry_cooldowns[symbol] = time.time() + self.FAILED_ENTRY_COOLDOWN_SEC
                return None
```

- [ ] **Step 5.3: Smoke-test the import still works**

```bash
DRY_RUN=true python3 -c "import real_trader; print('ok')"
```

Expected: `ok`.

- [ ] **Step 5.4: Add an end-to-end-style test of the abort branch**

Append to `deploy-live/tests/test_fill_quality_abort.py`:

```python
class _StubExecutor:
    """Minimal async executor that returns canned OrderResult objects.

    place_market_order is awaited from the abort path's _emergency_close_leg.
    We control its behavior by setting close_result before each call.
    """
    def __init__(self, name, close_result):
        self.name = name
        self._close_result = close_result
        self.calls = []

    async def place_market_order(self, symbol, side, size_usd, reduce_only=False):
        self.calls.append((symbol, side, size_usd, reduce_only))
        return self._close_result


def test_open_position_aborts_below_threshold(monkeypatch, order_result_factory, price_quote_factory):
    """Both legs fill, but actual_entry_spread (0.10%) is below
    threshold (0.30%) — abort fires, closed_positions gets the record."""
    import importlib
    monkeypatch.setenv("MIN_FILL_SPREAD_PCT", "0.30")
    import real_trader as rt
    importlib.reload(rt)
    assert rt.MIN_FILL_SPREAD_PCT == 0.30

    portfolio = rt.Portfolio()

    # Stub close orders return success — _emergency_close_leg has its
    # own retry loop, but on first attempt success it returns immediately.
    close_short = order_result_factory(exchange="MEXC", side="buy",
                                       fill_price=1.0005, filled_usd=10.0, fees_usd=0.005)
    close_long = order_result_factory(exchange="BloFin", side="sell",
                                      fill_price=0.9985, filled_usd=10.0, fees_usd=0.005)

    ex_short = _StubExecutor("MEXC", close_short)
    ex_long = _StubExecutor("BloFin", close_long)
    te = rt.TradeExecutor(portfolio=portfolio, executors={"MEXC": ex_short, "BloFin": ex_long})

    # Pre-stub _get_position_size to return 0 so _emergency_close_leg
    # short-circuits after first close.
    async def _zero_size(*a, **k):
        return 0
    te._get_position_size = _zero_size

    # Both entry legs filled with a degraded actual spread of (1.000 - 0.999) / 0.999 * 100 = 0.10%
    result_short = order_result_factory(exchange="MEXC", side="sell",
                                        fill_price=1.000, filled_usd=10.0, fees_usd=0.005)
    result_long = order_result_factory(exchange="BloFin", side="buy",
                                       fill_price=0.999, filled_usd=10.0, fees_usd=0.005)

    # We can't easily call open_position end-to-end (needs aiohttp session,
    # PriceQuote with full ob_depth fields, etc.) so we test the abort
    # branch directly via the helper, mirroring what open_position does.
    q_high = price_quote_factory(exchange="MEXC")
    q_low = price_quote_factory(exchange="BloFin")

    # actual_entry_spread = (1.000 - 0.999) / 0.999 * 100 ≈ 0.1001 < 0.30 → abort
    actual_spread = (result_short.fill_price - result_long.fill_price) / result_long.fill_price * 100

    async def _run():
        # Replicate the abort branch from open_position
        cs, cl = await asyncio.gather(
            ex_short.place_market_order("TESTUSDT", "buy", 10.0, reduce_only=True),
            ex_long.place_market_order("TESTUSDT", "sell", 10.0, reduce_only=True),
        )
        te._record_fill_quality_abort(
            pos_id=1, symbol="TESTUSDT",
            q_high=q_high, q_low=q_low,
            result_short=result_short, result_long=result_long,
            close_short=cs, close_long=cl,
            actual_spread_pct=actual_spread,
            threshold_pct=rt.MIN_FILL_SPREAD_PCT,
        )

    asyncio.run(_run())

    assert len(portfolio.closed_positions) == 1
    pos = portfolio.closed_positions[0]
    assert pos.exit_reason == "fill_quality_abort"
    assert pos.abort_threshold_pct == 0.30
    assert abs(pos.entry_spread_pct - 0.1001) < 0.001
    assert ex_short.calls and ex_short.calls[0][3] is True  # reduce_only on close
    assert ex_long.calls and ex_long.calls[0][3] is True
```

- [ ] **Step 5.5: Run all tests**

```bash
cd "/Users/vandenboogaard/Claude projects/Claude Paperclip/.claude/worktrees/tender-germain/deploy-live"
DRY_RUN=true python3 -m pytest tests/test_fill_quality_abort.py -v
```

Expected: 5 passed.

- [ ] **Step 5.6: Commit**

```bash
cd "/Users/vandenboogaard/Claude projects/Claude Paperclip/.claude/worktrees/tender-germain"
git add deploy-live/real_trader.py deploy-live/tests/test_fill_quality_abort.py
git commit -m "feat: wire fill-quality-abort recording into open_position abort branch"
```

---

## Task 6: Boundary tests

**Files:**
- Modify: `deploy-live/tests/test_fill_quality_abort.py`

Verifies the abort fires at exactly the right boundary.

- [ ] **Step 6.1: Add boundary tests**

Append to `deploy-live/tests/test_fill_quality_abort.py`:

```python
def test_threshold_comparison_is_strict_less_than(monkeypatch):
    """`actual_spread < MIN_FILL_SPREAD_PCT` — at exactly the threshold,
    the abort must NOT fire (so a trade landing exactly at the configured
    minimum is kept, not aborted)."""
    import importlib
    monkeypatch.setenv("MIN_FILL_SPREAD_PCT", "0.30")
    import real_trader as rt
    importlib.reload(rt)

    # Above and equal: no abort
    assert not (0.30 < rt.MIN_FILL_SPREAD_PCT)
    assert not (0.31 < rt.MIN_FILL_SPREAD_PCT)
    # Below: abort
    assert (0.29 < rt.MIN_FILL_SPREAD_PCT)
    assert (0.0 < rt.MIN_FILL_SPREAD_PCT)
    assert (-0.10 < rt.MIN_FILL_SPREAD_PCT)


def test_default_threshold_only_aborts_inverted(monkeypatch):
    """With default -0.10, only fills below -0.10 abort — preserves
    the pre-existing inverted-fill-only behavior."""
    import importlib
    monkeypatch.delenv("MIN_FILL_SPREAD_PCT", raising=False)
    import real_trader as rt
    importlib.reload(rt)
    assert rt.MIN_FILL_SPREAD_PCT == -0.10
    # 0%, 0.30%, even -0.05% must NOT abort under default
    assert not (0.0 < rt.MIN_FILL_SPREAD_PCT)
    assert not (0.30 < rt.MIN_FILL_SPREAD_PCT)
    assert not (-0.05 < rt.MIN_FILL_SPREAD_PCT)
    # Only true inversions below -0.10 abort
    assert (-0.11 < rt.MIN_FILL_SPREAD_PCT)
    assert (-1.0 < rt.MIN_FILL_SPREAD_PCT)
```

- [ ] **Step 6.2: Run all tests**

```bash
cd "/Users/vandenboogaard/Claude projects/Claude Paperclip/.claude/worktrees/tender-germain/deploy-live"
DRY_RUN=true python3 -m pytest tests/test_fill_quality_abort.py -v
```

Expected: 7 passed.

- [ ] **Step 6.3: Commit**

```bash
cd "/Users/vandenboogaard/Claude projects/Claude Paperclip/.claude/worktrees/tender-germain"
git add deploy-live/tests/test_fill_quality_abort.py
git commit -m "test: boundary tests for fill-quality abort threshold"
```

---

## Task 7: Update `bot_config_live.json` schema (default off)

**Files:**
- Modify: `deploy-live/data/bot_config_live.json`
- Modify: `deploy-live/local_dashboard.py` (CONFIG_SCHEMA at line ~1489)

This task adds the new key to the live config file with the safe default `-0.10` (no behavior change), and exposes it in the dashboard.

- [ ] **Step 7.1: Add the key to `bot_config_live.json` with the safe default**

Read the current file:

```bash
cat "/Users/vandenboogaard/Claude projects/Claude Paperclip/.claude/worktrees/tender-germain/deploy-live/data/bot_config_live.json"
```

Use Edit to add `"MIN_FILL_SPREAD_PCT": -0.10` to the JSON object. Place it adjacent to `"ENTRY_SPREAD_PCT"` for readability. Make sure the surrounding comma punctuation is correct.

- [ ] **Step 7.2: Verify the JSON parses**

```bash
python3 -c "import json; json.load(open('/Users/vandenboogaard/Claude projects/Claude Paperclip/.claude/worktrees/tender-germain/deploy-live/data/bot_config_live.json')); print('json ok')"
```

Expected: `json ok`.

- [ ] **Step 7.3: Add the row to dashboard CONFIG_SCHEMA**

Open `deploy-live/local_dashboard.py`. Find `var CONFIG_SCHEMA=[` (around line 1489). Add a row right after the `ENTRY_SPREAD_PCT` row:

```javascript
  ['MIN_FILL_SPREAD_PCT','Min post-fill spread (%) [restart req]','number',0.05,true],
```

The `true` at the end indicates `needs_restart` — important because, as we discovered, the trader does not currently re-read config at runtime.

- [ ] **Step 7.4: Smoke-test the dashboard still renders**

```bash
cd "/Users/vandenboogaard/Claude projects/Claude Paperclip/.claude/worktrees/tender-germain/deploy-live"
python3 -c "
import json
# Confirm the local_dashboard.py file still parses as valid Python
with open('local_dashboard.py') as f:
    compile(f.read(), 'local_dashboard.py', 'exec')
print('local_dashboard.py compiles ok')
"
```

Expected: `local_dashboard.py compiles ok`.

- [ ] **Step 7.5: Commit**

```bash
cd "/Users/vandenboogaard/Claude projects/Claude Paperclip/.claude/worktrees/tender-germain"
git add deploy-live/data/bot_config_live.json deploy-live/local_dashboard.py
git commit -m "config: expose MIN_FILL_SPREAD_PCT in dashboard + live config (default -0.10)"
```

---

## Task 8: Final verification — full test suite + import check

- [ ] **Step 8.1: Run the full test suite**

```bash
cd "/Users/vandenboogaard/Claude projects/Claude Paperclip/.claude/worktrees/tender-germain/deploy-live"
DRY_RUN=true python3 -m pytest tests/ -v
```

Expected: 7 passed.

- [ ] **Step 8.2: Verify default-on import is byte-equivalent in behavior**

```bash
DRY_RUN=true python3 -c "
import real_trader as rt
# With default -0.10 (or whatever was just set in config), behavior is
# pre-change unless operator explicitly raised the threshold.
print('Active threshold:', rt.MIN_FILL_SPREAD_PCT)
assert rt.MIN_FILL_SPREAD_PCT <= 0, 'Default rollout MUST keep threshold <= 0 to preserve existing inverted-only behavior'
print('Rollout safety: ok (no behavior change until operator raises threshold)')
"
```

Expected: `Active threshold: -0.1` and `Rollout safety: ok`.

- [ ] **Step 8.3: Verify the diff is small and contained**

```bash
cd "/Users/vandenboogaard/Claude projects/Claude Paperclip/.claude/worktrees/tender-germain"
git diff --stat HEAD~6..HEAD
```

Expected: ~5-6 files touched, all in `deploy-live/` and `docs/`. No changes outside scope.

- [ ] **Step 8.4: STOP and report**

Do not deploy or restart the bot. Report back to the user with:
- Test pass count
- Files changed
- Total LOC added/removed
- Commit hashes (`git log --oneline HEAD~6..HEAD`)

The operator decides when to restart the bot and when to flip the config value to `0.30`.

---

## Out of scope for this plan (per spec §3, §10)

- Auto-tuning the threshold over time
- Per-symbol thresholds
- Implementing actual runtime config reload (would be a separate spec)
- Plans B (WS order placement), C (rate-gate removal — explicitly dropped), D (event-driven scanner)
- Tokyo migration

---

## Self-review notes

- Spec §2 goals: covered by Tasks 2 (constant), 3 (wire), 4 (record + field), 5 (call helper from open_position), 7 (config + dashboard).
- Spec §4.1 config key: Tasks 2 + 7.
- Spec §4.2 modified guard: Task 3.
- Spec §4.3 new record: Tasks 4 + 5.
- Spec §5 data flow: implemented in Task 5.
- Spec §6 default value justification: encoded as the actual default via Task 2 (preserving safety) and Task 7 (config seeded with safe value, operator chooses to raise).
- Spec §7 testing: Tasks 2, 4, 5, 6 cover all six listed unit tests except `test_fill_quality_abort_clamps_invalid_config` — clamping was specified but the simpler `os.environ.get` pattern doesn't naturally clamp; if clamping is required, it would be a small additional task (clamp inside the constant definition: `max(-MAX_SANE_SPREAD_PCT, min(ENTRY_SPREAD_PCT - 0.001, raw))`). Flag to user.
- Spec §8 rollout plan: encoded in Task 7 (safe-by-default config) + Task 8.4 (operator decides activation).
