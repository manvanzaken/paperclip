# Sequential Two-Leg Fill Model — Design Spec

**Date:** 2026-06-12
**Target file:** `deploy/paper_trader.py` (paper trader, workspace `857e37f3.../deploy/`)
**Goal:** Make paper-trader fills predict live execution. Today the model is net-optimistic by ~0.2–0.5% per round-trip; this closes the gaps that change the *sign* of the paper→live error.

## Background

Audit of the current execution path (5 parallel read-only agents) found the paper trader treats a two-leg arbitrage entry/exit as a single synchronous event priced off one orderbook snapshot. The four highest-impact realism gaps:

1. **No leg desync / naked exposure.** `open_position` forces `actual_filled = min(filled_short, filled_long)` and `simulate_leg_risk` is all-or-nothing/symmetric. Live: one leg can fail or partial-fill independently, leaving a naked directional leg — invisible in current paper P&L.
2. **Synchronized fills, no inter-leg drift.** Both legs priced off the same snapshot; `apply_execution_delay` is a flat haircut + one random sample. Live fills leg 1 at T0 and leg 2 at T0+100–300ms against a *different* book.
3. **Stale quote at fill, never re-validated.** Detection quote up to ~1s old (30s tolerance) used directly; spread never re-checked against fresh top-of-book; WS OB tolerates 5s staleness.
4. **Entry/exit asymmetry inflates P&L.** Exit never rejects (always falls back to top-of-book +8bps), wider slip cap (0.50% vs 0.40%), exec-delay only 10% of the time vs guaranteed on entry, OB-staleness ~200× weaker on exit (a `/100` bug).

## Scope

In scope (Approach 2 — Sequential two-leg execution model):
- Sequential per-leg fill with independent latency + velocity-driven drift (entry and exit).
- Independent per-leg fill/fail outcomes → leg desync → **auto-flatten** of naked exposure with realized cost.
- Entry/exit symmetry (shared staleness/delay/slip math; fix the `/100` exit staleness bug).
- Tier-2 calibration (fallback slippage, staleness penalty, velocity-based flash-miss, remove competition floor).
- Observability (log markers + position fields) and TDD coverage.
- Config flag for A/B + instant revert.

Explicitly OUT of scope (separate future tasks):
- Market-impact model upgrade (linear/own-volume-only → square-root + competitor flow).
- Per-exchange-side impact pool split.
- Full event-driven tick-replay simulator (Approach 3).

## Design

### 1. Sequential two-leg fill model

Replace the synchronous `_incremental_fill(both legs, one snapshot)` in `open_position` with a per-leg sequential model. Both legs are still *submitted* at T0 (matches live parallel submission), but each fills against its own book at its own confirmation time.

New pure helper `_simulate_leg(ob_levels, size_usd, side, velocity) -> (vwap, filled_usd, latency_ms, drift_pct)`:
1. **Sample per-leg latency** independently: `latency = clamp(random.gauss(LEG_LATENCY_MEAN_MS, LEG_LATENCY_STD_MS), LEG_LATENCY_MIN_MS, LEG_LATENCY_MAX_MS)`. Tokyo baseline mean ~150ms.
2. **Drift the leg's book** over its latency: `drift_pct = velocity * (latency/1000) * direction`, where `direction` is adverse with probability `LEG_ADVERSE_PROB`, using the per-candidate `spread_velocity` already tracked (passed into `open_position`). Adverse = short fills lower / long fills higher.
3. **Walk the drifted book** via the existing `walk_orderbook` → `(vwap, filled_usd, levels)`, independently per leg.

`actual_spread` is computed from the two independently-drifted VWAPs. Existing post-walk gates (slip cap, min-viable-spread, absolute floor) still apply. The legs are no longer forced to equal size — `filled_short` and `filled_long` are independent.

### 2. Leg desync & naked-exposure handling

Replace all-or-nothing `simulate_leg_risk` with an **independent per-leg roll**: each leg independently full-fills, partial-fills (sampled %), or fails. Four outcomes:
- both fill → hedged position at matched size = `min(filled_short, filled_long)`
- both fail / both below min-notional → trade skipped (as today)
- one fills, other fails → naked single leg
- both fill, different sizes → naked excess = `abs(filled_short − filled_long)`

**Naked-exposure resolution = auto-flatten (confirmed).** Any unmatched notional is immediately closed at market on its own exchange, incurring one-sided taker slip (that leg's book / fallback slip). The realized cost is booked against the position.

**Recording:**
- Full single-leg failure (one leg entirely naked then unwound) → recorded as `closed_positions` with `exit_reason="leg_desync_unwind"` and the realized unwind loss, so it drags win-rate exactly like live.
- Partial naked excess → position opens normally at matched size, with the small unwind cost folded into entry P&L.

**Starting failure probabilities** (independent per leg, tunable later): `LEG_FAIL_CHANCE_PER_LEG ≈ 0.03`, `LEG_PARTIAL_CHANCE_PER_LEG ≈ 0.15`, partial fill fraction sampled `uniform(0.5, 0.9)`. Chosen so trade-level failure stays near today's combined rate.

### 3. Exit symmetry

`close_position` must close (cannot skip), so symmetry = same penalty *magnitudes* as entry:
1. Reuse `_simulate_leg` for both closing legs (independent latency + drift).
2. **Fix the `/100` exit staleness bug** (`close_position:~4623`): unify entry and exit on one shared staleness helper.
3. Exec-delay parity: exit uses the same `LEG_ADVERSE_PROB` as entry (not a hardcoded 10%).
4. Slip-cap parity: exit uses entry's `0.40%` formula. Exit cannot reject; when the walked exit price exceeds the cap, the **excess slippage is charged to the fill** (not silently capped to a nicer price), so the cost is realized.
5. Keep the `net_pnl ≤ entry_spread` cap (correctly conservative).

### 4. Tier-2 calibration

| Item | Location | Now | Proposed |
|---|---|---|---|
| Fallback slippage | `SLIPPAGE_FALLBACK_BPS` | 8 bps | **30 bps** |
| OB staleness penalty | `OB_STALENESS_PENALTY_BPS` | 0.2 bps | **2 bps** |
| Flash-miss trigger | `open_position` flash block | spread>1.5% flat 5% | **velocity-based:** `P(miss)=clamp(velocity * fill_window_sec / spread, 0, cap)` |
| Competition floor | `simulate_competition` | 5% min fill | **0%** (allow zero-fill) |

### 5. Observability & testing

**Log markers** (grep-able, `OB_OK` style):
- `LEG_FILL {sym} short: lat=Xms drift=Y% filled=$Z | long: lat=Xms drift=Y% filled=$Z`
- `LEG_DESYNC {sym} short=$A long=$B naked=$C unwind_cost=$D`
- `LEG_FAIL {sym} side=short → naked long unwound, loss=$X`

**New `PaperPosition` fields:** `leg_latency_short_ms`, `leg_latency_long_ms`, `leg_drift_short_pct`, `leg_drift_long_pct`, `naked_usd`, `naked_unwind_cost_usd`.

**Tests (TDD):**
- `_simulate_leg` (seeded RNG): drift direction, latency clamping, partial = capped to book depth, zero-depth → zero fill.
- Naked-resolution: both-fill → naked=0; one-leg-fail → full naked + unwind cost > 0; partial mismatch → naked = abs(diff), opens at matched size.
- Exit-symmetry: identical staleness/delay magnitude entry vs exit (regression guard for the `/100` bug).
- Guardrail: paper round-trip P&L for a fixed seeded scenario is **≤** the old model's (proves more conservative, not rosier).

**Rollout:** gate behind config flag `SEQUENTIAL_FILL=1` (default on) for A/B on a shadow instance and instant revert without redeploy.

## Success criteria

- Paper trade history shows non-zero `leg_desync_unwind` closes and naked-unwind costs.
- Entry and exit apply identical staleness/delay magnitudes (test-enforced).
- Guardrail test confirms new model is ≤ old model P&L on the fixed scenario.
- New log markers visible in Railway logs; new fields populated on positions.
- Flag toggles between old/new model cleanly.

## New constants (initial values)

```
LEG_LATENCY_MEAN_MS = 150
LEG_LATENCY_STD_MS  = 60
LEG_LATENCY_MIN_MS  = 80
LEG_LATENCY_MAX_MS  = 350
LEG_ADVERSE_PROB    = (reuse entry's EXEC_DELAY_ADVERSE_CHANCE, 0.10)
LEG_FAIL_CHANCE_PER_LEG    = 0.03
LEG_PARTIAL_CHANCE_PER_LEG = 0.15
SEQUENTIAL_FILL = 1   # env/config override
# calibration
SLIPPAGE_FALLBACK_BPS   = 30   # was 8
OB_STALENESS_PENALTY_BPS = 2   # was 0.2
COMPETITION_FILL_FLOOR  = 0.0  # was 0.05
```
