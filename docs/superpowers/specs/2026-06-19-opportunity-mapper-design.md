# Opportunity Mapper — Design Spec

**Date:** 2026-06-19
**Target file:** `deploy/paper_trader.py` (paper trader, workspace `857e37f3.../deploy/`)
**Goal:** Map every *tradeable* cross-exchange opportunity (spread that clears fees + minimum profit), recording its duration, max spread, synchronized two-sided orderbook depth, and a 0–100 "grabbability" rating that estimates how realistically an algo could capture it. This makes the paper trader's view of reality auditable before risking live capital.

## Background

The paper trader already has `OBSnapshotTracker` (`paper_trader.py:509`) that records, per opportunity: duration (`first_seen`→`last_seen`, `tick_count`), peak/max spread (`peak_spread_pct`), and two-sided OB depth (total USD + top-10 levels per side), persisting to `ob_snapshots.json` with a `/api/ob-snapshots` endpoint.

Two limitations motivate this work:
1. **It only records opportunities that survived the entire trade-gate stack** (confirmation ticks, reversion filter, blacklists) AND fit inside the 120-REST-calls/cycle OB budget. It does not map the wider set of opportunities that were *real and profitable* but rejected for timing/quality reasons.
2. **No grabbability rating**, and **no recorded per-leg pull timestamps** — the two books are already fetched concurrently via one `asyncio.gather` (`paper_trader.py:6844`), but the snapshot stores a single `ts`, so the time-sync of the two pulls is done but not *measured/proven*.

## Decisions (locked during brainstorming)

- **Scope = B (tradeable-only, full depth):** map only opportunities worth trading, but characterize each fully (synchronized L2 depth + per-leg timestamps + rating). Not every raw sub-threshold spread.
- **Opportunity definition = Def 2:** an opportunity exists when the realistic executable spread clears **both legs' taker fees plus a minimum profit margin** — i.e. the existing `realistic_entry_spread >= min_spread_needed` check at `paper_trader.py:~6700`, where `min_spread_needed = total_fees + 0.05`.
- **Rating format = composite + breakdown:** a single 0–100 `grabbability` score **and** a per-factor breakdown stored on each opportunity.
- **Placement = inline, early tap:** capture inside the live scan/execute cycle, reusing the trader's exact quote feed and WS OB cache, so mapped books are the same objects a trade would hit. No separate process.

### Resolved defaults
- **Depth target** for the depth factor = an explicit `OPP_DEPTH_TARGET_USD = 25` constant, representing the **live** order size we care about grabbing. NOT bound to the paper trader's `MAX_POSITION_USD` (which is 500); the goal is realism vs. the live strategy's $25 cap.
- **Sync tolerance** for the sync factor = **250 ms** (the per-leg fill window).
- **MEXC/BloFin reality (accepted):** these two (the live pair) do not stream WS L2, so every map there costs a REST call. Mapping draws from the REST budget **only after** real `entry_candidates` are served, so trading is never starved; mapping coverage on MEXC/BloFin may be partial on busy cycles. No reserved slice.

## Scope

In scope:
- A `mappable_opps` capture point that records every Def-2 opportunity per cycle (a superset of `entry_candidates`).
- A synchronized OB-fetch pass (WS-cache-first, REST-budgeted-after-entries) that records per-leg as-of timestamps and `ob_skew_ms`.
- Extension of `OBSnapshotTracker.record_opportunity` to store fees, target size, per-leg timestamps/skew, the composite `grabbability`, and the `factors` breakdown.
- A pure `_score_grabbability` scorer (live per-tick) + `final_grabbability` finalized at `close_opportunity` once total duration is known.
- `get_stats` additions (avg/median grabbability + skew).
- TDD coverage of the scorer, skew computation, Def-2 boundary, and finalization.
- Config flag for instant enable/disable.

Explicitly OUT of scope (follow-ups):
- A dashboard UI view for the mapped opportunities (data is persisted + API-exposed; visualization is a separate task).
- Reserved REST slice for MEXC/BloFin mapping.
- Migrating the shadow-strategy simulator (`process_cycle`) to use mapped data.

## Design

### 1. Capture the wider opportunity set (`mappable_opps`)

The capture must happen **early** in the scan loop — right after the raw executable spread, velocity, and `pair_key` are computed (`paper_trader.py:~6589`), and *before* the spread-threshold / reversion / confirmation-tick gates. Those gates `continue` out of the loop before the later fee check (`~6700`), so tapping at the fee check would only see the narrow, already-confirmed set and defeat the purpose. Def-2 mappability is fee-based (physics), independent of the strategy's entry threshold (a knob), so it is evaluated here on the raw `spread_pct`:

```
# after spread_pct, velocity, pair_key computed (~6589); before threshold gate
if OPP_MAPPER and spread_pct <= MAX_SANE_SPREAD_PCT:
    map_fees = trader.compute_fees(q_high.exchange, q_low.exchange,
                                   q_high.instrument, q_low.instrument) * 2
    if is_mappable_opportunity(spread_pct, map_fees):
        mappable_opps.append({
            "symbol": symbol, "q_high": q_high, "q_low": q_low,
            "pair_key": pair_key,
            "spread_pct": spread_pct,
            "raw_spread_pct": spread_pct,
            "velocity": velocity,
            "total_fees": map_fees,
        })
```

This fires for **every** Def-2 opportunity, including ones later rejected by the threshold, reversion (`check_reversion_entry`), or confirmation-tick (`count < ENTRY_MIN_TICKS`) gates — those gates govern *when to trade*, not *whether the opportunity was real*. `entry_candidates` remains a strict subset of `mappable_opps`. Insanely-wide spreads (`> MAX_SANE_SPREAD_PCT`) are still excluded.

### 2. Synchronized OB pull pass

After the scan, before/alongside the existing execute loop, run a mapping pass over `mappable_opps`:

1. **WS-cache-first:** call the WS cache for both legs. The cache must expose the per-symbol `ts` (today `get_orderbook` returns `(bids, asks)` and drops `ts`) — add a `get_orderbook_with_ts(...) -> (bids, asks, ts)` accessor (or return the existing `data["ts"]`). WS hits are free and don't count against the budget.
2. **REST fallback, budgeted-after-entries:** for legs not in WS cache, fetch via the same `asyncio.gather(fetch_high, fetch_low)` concurrent pattern as the execute loop, capturing `time.time()` immediately before/after each leg's resolution as that leg's as-of timestamp. These REST calls draw from the **remaining** `MAX_OB_CALLS_PER_CYCLE` budget *after* `entry_candidates` have been served, so live trading is never starved.
3. **Compute skew:** `ob_skew_ms = abs(ts_short - ts_long) * 1000`. This is the recorded proof the two books were pulled near-identically in time.
4. Opportunities whose books can't be obtained within budget this cycle are still tracked for duration/spread (they keep ticking in `_active_opps`); their depth/grabbability simply isn't refreshed this cycle.

Reuse the union: when an opportunity is also an `entry_candidate`, the OB already fetched in the execute loop is shared rather than re-fetched.

### 3. `OBSnapshotTracker` extension

Extend `record_opportunity` to also accept `total_fees`, `target_size_usd`, `ts_short`, `ts_long`, and compute/store:
- `ob_skew_ms`
- `grabbability` (composite 0–100)
- `factors` (dict of the five factor scores, each 0–100)

New snapshot dict fields: `total_fees`, `target_size_usd`, `ts_short`, `ts_long`, `ob_skew_ms`, `grabbability`, `factors`.

Add a pure static/helper method:

```
@staticmethod
def _score_grabbability(duration_s, fill_window_s, short_depth_usd, long_depth_usd,
                        target_size_usd, spread_pct, total_fees, velocity,
                        ob_skew_ms, sync_tolerance_ms, weights) -> (float, dict):
    ...
    return composite_0_100, {"duration": ..., "depth": ..., "margin": ...,
                             "velocity": ..., "sync": ...}
```

### 4. Grabbability factors

Each factor returns 0–100; composite = weighted blend (weights default-equal, configurable).

| Factor | Definition |
|---|---|
| `duration` | `clamp(duration_s / OPP_FILL_WINDOW_S, 0, 1) * 100` (`OPP_FILL_WINDOW_S = 0.85`s react+fill). Sub-window flashes → low. |
| `depth` | `clamp(min(short_depth_usd, long_depth_usd) / OPP_DEPTH_TARGET_USD, 0, 1) * 100` (`OPP_DEPTH_TARGET_USD = 25`). |
| `margin` | `clamp((spread_pct - total_fees) / total_fees, 0, 1) * 100` (cushion over breakeven; ≥100% excess saturates at full marks). |
| `velocity` | `clamp(0.5 + velocity * OPP_VELOCITY_K, 0, 1) * 100` (`OPP_VELOCITY_K = 1.0`): widening → toward 100, collapsing → toward 0, flat → 50. |
| `sync` | `clamp(1 - ob_skew_ms / sync_tolerance_ms, 0, 1) * 100`. 0 ms → 100; ≥ tolerance → 0. |

**Live vs. final:** `duration` grows over an opportunity's life, so the per-tick `grabbability` is a *live* estimate. `close_opportunity` recomputes a `final_grabbability` using the full `final_duration_s` (mirrors the existing `final_duration_s` / `final_tick_count` write-back) and stamps it on the last snapshot for that pair.

### 5. Config flag

Gate behind `OPP_MAPPER = 1` (env / `bot_config` override). When 0, the capture, OB pass, and scoring are skipped entirely — zero behavior change to trading.

### 6. Observability

Log marker (grep-able, `OB_OK` style), emitted on a sampled cadence (e.g. every Nth map):
- `OPP_MAP {sym} grab=NN spread=X% dur=Ys depth=$Z skew=Wms vel=V`

`get_stats` gains: `avg_grabbability`, `median_grabbability`, `avg_ob_skew_ms`.

## Testing (TDD)

- **`_score_grabbability` monotonicity & bounds:** each factor in [0,100]; composite in [0,100]; raising each input moves its factor in the expected direction (e.g. larger depth ⇒ higher depth score; larger skew ⇒ lower sync score; negative velocity ⇒ lower velocity score; sub-window duration ⇒ low duration score).
- **Skew computation:** `ob_skew_ms = abs(ts_short - ts_long) * 1000` for representative timestamps.
- **Def-2 boundary:** an opportunity with `realistic_entry_spread` just **below** `total_fees + 0.05` is NOT captured into `mappable_opps`; just **above** IS captured.
- **Finalization:** after `close_opportunity`, the pair's last snapshot carries `final_grabbability` computed from `final_duration_s`, and it differs from the early-tick live score when duration grew.
- **Flag off:** with `OPP_MAPPER = 0`, no snapshots recorded for a Def-2 opportunity and no OB pass runs.

## Success criteria

- `ob_snapshots.json` shows mapped opportunities that are NOT in trade history (the wider Def-2 set), each with `grabbability`, `factors`, and `ob_skew_ms` populated.
- Recorded `ob_skew_ms` values confirm the two books are pulled near-identically in time (well under the 250 ms tolerance for WS-cached symbols).
- `final_grabbability` is written on opportunity close and reflects total duration.
- `OPP_MAP` log markers visible; `get_stats` reports grabbability + skew aggregates.
- `OPP_MAPPER` flag toggles the whole feature cleanly with no trading-path change when off.

## New constants (initial values)

```
OPP_MAPPER = 1                 # env/config override; 0 = feature off
OPP_DEPTH_TARGET_USD = 25.0    # live order size we care about grabbing (NOT paper MAX_POSITION_USD=500)
OPP_MIN_PROFIT_MARGIN = 0.05   # Def-2 margin over fees (matches trading's min_spread_needed)
OPP_FILL_WINDOW_S = 0.85       # react+fill window = POLL_INTERVAL_FAST (0.5) + LEG_LATENCY_MAX_MS/1000 (0.35)
OPP_SYNC_TOLERANCE_MS = 250
OPP_VELOCITY_K = 1.0           # velocity factor: clamp(0.5 + velocity*K, 0, 1)*100
OPP_GRAB_WEIGHTS = {"duration": 1, "depth": 1, "margin": 1, "velocity": 1, "sync": 1}  # equal default
OPP_MAP_LOG_EVERY = 50         # sampled log cadence
```
