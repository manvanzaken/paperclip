# Post-Fill Spread-Decay Abort — Design Spec

> Reject entries whose actual fill-derived spread came in below a configurable quality threshold, even when both legs filled successfully. Closes the "leak band" between the inverted-fill guard (-0.10%) and the original detection threshold where 9%-win-rate trades currently land.

**Author:** Claude (drafted from analysis session 2026-04-28)
**Status:** Approved for implementation
**Scope:** `deploy-live/real_trader.py` only — no scanner, executor, or close-path changes
**Related:** Plan B (WebSocket order placement) and Plan D (event-driven scanner) deferred to separate specs

---

## 1. Background

A failure-mode analysis on the live trader's 200-record state file (session 2026-04-28) found that **66% of "regular" trades (43 of 65 convergence + dynamic_exit closures)** filled at an actual spread *below* the configured `ENTRY_SPREAD_PCT = 0.6%`, despite the bot only triggering entry when the *detected* spread met that threshold. The only mechanism that explains this gap is **decay of the spread between detection and order fill** — quote-cache lag plus REST round-trip latency consuming 0.5–2 seconds of an opportunity whose median lifetime is ~5 s.

These lag-degraded entries had a **9% win rate and -$0.82 total net P&L** versus the 22 clean entries (≥ 0.6% actual fill) which had a **73% win rate and +$2.01 total**. The strategy as a whole is positive (~+$0.018/trade) only because the few clean wins are 3.7× larger than the many degraded losses.

The bot already has an inverted-fill guard at `real_trader.py:3863-3880` that emergency-closes both legs if the actual fill spread came in below `-0.10%`. This spec widens that guard to a configurable threshold so it also rejects the **positive but degraded** band where the win rate cliff sits.

## 2. Goals

- Add one configurable, hot-reloadable threshold `MIN_FILL_SPREAD_PCT` that controls the post-fill abort behavior.
- Default value `0.30%`, justified by the empirical bucket data (see §6).
- Aborts emergency-close both legs (already happens), set the existing 60 s `failed_entry_cooldowns` entry (already happens), AND record a `LivePosition` in `closed_positions` with new `exit_reason="fill_quality_abort"` so the dashboard surfaces them and the data is available for future tuning.
- Zero-risk rollout: when the new config key is **absent**, behavior is byte-identical to the current code (default = -0.10).

## 3. Non-goals

- Any change to the detection threshold (`ENTRY_SPREAD_PCT`).
- Any change to the scanner, executors, close path, or reconciliation.
- Auto-tuning the threshold over time. (Possible future work once we have post-deploy data.)
- Per-symbol thresholds. v1 is one global value.
- Latency reduction itself (covered by deferred Plans B and D).
- Anything to do with BloFin's `_rate_gate` (Plan C explicitly dropped — see §9).

## 4. Architecture changes

### 4.1 New config key

`bot_config_live.json` gains one field:

```json
{
  "MIN_FILL_SPREAD_PCT": 0.30
}
```

- **Type:** float (percentage points, same units as `ENTRY_SPREAD_PCT`).
- **Range:** must satisfy `-MAX_SANE_SPREAD_PCT ≤ MIN_FILL_SPREAD_PCT < ENTRY_SPREAD_PCT`. On out-of-range values, log a warning and clamp to `-0.10` (current default).
- **Activation:** read by `_apply_bot_config_overrides()` at startup, same lifecycle as `ENTRY_SPREAD_PCT`. **Bot restart required to change** — the trader does not currently re-read config at runtime despite some dashboard `needs_restart=false` flags suggesting otherwise. Implementing true runtime reload is out of scope.
- **Default if key missing:** `-0.10` (preserves current behavior exactly — guarantees the deploy is a no-op until the operator sets it).

### 4.2 Modified abort guard

In `real_trader.py:3863-3880` (`open_position`, "both legs filled" branch):

- Replace the local constant `INVERTED_FILL_TOLERANCE = -0.10` with a reference to the new module-level `MIN_FILL_SPREAD_PCT`, defined alongside `ENTRY_SPREAD_PCT` (around line 95) using the same `float(os.environ.get(...))` pattern. Default: `-0.10` (preserves current behavior).
- Change the comparison to `actual_entry_spread < MIN_FILL_SPREAD_PCT`.
- When the active threshold is `≥ 0` (i.e., operator has activated the new behavior), the log line changes from `INVERTED ENTRY` to `FILL-QUALITY ABORT` so it's distinguishable in logs and the existing log-grepping tooling.

The emergency-close of both legs and the `failed_entry_cooldowns` write are unchanged.

### 4.3 New record in `closed_positions`

Currently the abort path returns `None` after closing both legs and writes nothing to `closed_positions`. This spec adds a record so the dashboard and future analysis can see them.

A new helper `_record_fill_quality_abort(pos_id, symbol, q_high, q_low, result_short, result_long, close_results, actual_spread, threshold)` writes a `LivePosition` to `portfolio.closed_positions` with:

- `exit_reason="fill_quality_abort"` (new value)
- `entry_price_short`, `entry_price_long`, `entry_spread_pct=actual_spread`
- `exit_price_short`, `exit_price_long`, `exit_spread_pct` from the close results
- `entry_fees_usd`, `exit_fees_usd` summed from all four legs
- `gross_pnl_usd`, `net_pnl_usd` computed from short+long P&L (mirrors `_record_failed_leg` math)
- `order_id_short`, `order_id_long` (both populated, unlike `failed_entry`)
- `status="CLOSED"`, `entry_time` and `exit_time` set
- New analytics field `abort_threshold_pct = threshold` so we can later analyze whether the threshold was right

`portfolio.total_trades += 1`, `total_pnl_usd += net_pnl`, `_state_dirty = True` — same as a normal close.

## 5. Data flow

```
detect spread ≥ ENTRY_SPREAD_PCT
   ↓
open_position fires both legs in parallel
   ↓
both legs return success
   ↓
recompute actual_entry_spread from real fills (existing, line 3851)
   ↓
read MIN_FILL_SPREAD_PCT from config (new)
   ↓
actual_entry_spread < threshold?
   ├── YES → emergency-close both legs
   │         set failed_entry_cooldowns[symbol]
   │         _record_fill_quality_abort(...)        ← NEW
   │         return None
   └── NO  → proceed to LivePosition creation (existing)
```

## 6. Default value justification

From the empirical analysis on 65 regular closures:

| Actual fill spread | N | Wins | Win rate | Avg net P&L |
|---|---|---|---|---|
| `< 0.30%` | 38 | 3 | 7% | -$0.020 |
| `≥ 0.30%` | 27 | 17 | 63% | +$0.092 |

A `0.30%` threshold would have aborted the 38 lag-degraded trades that produced `-$0.76` total. Cost of aborting = 4 taker fees instead of 2 ≈ ~$0.06 per abort × 38 = ~$2.30 in extra abort fees. **Net first-order cost: roughly break-even on these specific trades, but with three additional benefits**:

1. Frees concentration slots immediately (currently a degraded position blocks one of `MAX_CONCURRENT = 3` slots until exit).
2. Eliminates loss-tail outliers like GENIUSUSDT (-$0.49 single trade, entry capped at `MAX_SANE_SPREAD_PCT`).
3. Avoids the holding period during which more degradation can occur.

`0.30%` also sits cleanly below the current `ENTRY_SPREAD_PCT = 0.6%` so it allows up to half-decay before aborting.

## 7. Testing

### Unit tests

- `test_fill_quality_abort_triggers_below_threshold`: mock `OrderResult` legs producing `actual_entry_spread = 0.20%` with threshold `0.30%`; verify abort fires, both close orders are sent, `closed_positions` gets the new record with `exit_reason="fill_quality_abort"`.
- `test_fill_quality_abort_does_not_trigger_at_threshold`: same setup but `actual_entry_spread = 0.30%`; verify abort does NOT fire, `LivePosition` is created normally.
- `test_fill_quality_abort_boundary`: `actual_entry_spread = 0.2999%` (just below) and `0.3001%` (just above) — boundary correctness.
- `test_fill_quality_abort_disabled_default`: when `MIN_FILL_SPREAD_PCT` key is absent from config, threshold defaults to `-0.10` and only inverted fills abort (existing behavior preserved).
- `test_fill_quality_abort_record_has_correct_fields`: verify recorded `LivePosition` has both order_ids populated, summed fees from all four legs, computed P&L matching the price deltas, and `abort_threshold_pct` populated.
- `test_fill_quality_abort_clamps_invalid_config`: `MIN_FILL_SPREAD_PCT = 10.0` (greater than `MAX_SANE_SPREAD_PCT`) clamps to `-0.10` and logs a warning.

### Integration check

Before deploying, run the full bot with `MIN_FILL_SPREAD_PCT = -0.10` (or absent) for one cycle and verify no behavior change vs. `git checkout HEAD~1`. This is the "no-op rollout" gate.

## 8. Rollout plan

1. Merge code with `MIN_FILL_SPREAD_PCT` defaulting to `-0.10`. Deploy.
2. Verify zero behavioral change in production for at least 30 minutes.
3. Edit `bot_config_live.json` to set `MIN_FILL_SPREAD_PCT = 0.30` AND restart the bot (the trader does not currently re-read config at runtime).
4. Monitor `fill_quality_abort` records appearing in the dashboard and logs.
5. After 24 h, compare:
   - Number of `fill_quality_abort` events vs. our predicted ~38/200 (~19%) frequency.
   - Net P&L of remaining live trades — expected to improve since the lag-degraded losers are removed.
   - If aborts fire too often → raise threshold (e.g. `0.20`); too rare → lower (e.g. `0.40`).

Easy revert: set the config key back to `-0.10` and restart the bot.

## 9. Explicitly dropped: rate-gate removal

A removal of `BloFin._rate_gate` (Plan C in the original brainstorm) was considered and **dropped** during design. The 1.5-second min-gap exists specifically because rapid-fire BloFin orders trigger silent risk-control rejections (`real_trader.py:444-451, 1873`) — the same failure mode that produces ~22% of currently-recorded failed entries. Removing it would re-introduce that failure mode for a small per-trade latency win. If we want to dial down `MIN_ORDER_GAP_SEC` below 1.5 s, that needs its own measurement-driven spec, not a config flip.

## 10. Out-of-scope follow-ups

- **Plan D — event-driven scanner**: eliminate the 0–1000 ms `POLL_INTERVAL` wait. Separate spec.
- **Plan B — WebSocket order placement**: -100 to -300 ms per leg. Separate spec.
- Tokyo migration: removes 500–1500 ms per leg of EU→Asia round-trip. Operational, not a code change.
- Auto-tuning `MIN_FILL_SPREAD_PCT` per-symbol or over time. After we have post-deploy data.
