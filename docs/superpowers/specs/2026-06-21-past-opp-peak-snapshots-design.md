# Past-Opportunity Peak Snapshots — Design Spec

**Date:** 2026-06-21
**Component:** `OBSnapshotTracker` (paper_trader.py) + Opportunities dashboard (dashboard.py)
**Branch:** `claude/opportunity-mapper`

## Problem

Today a closed ("Past") opportunity shows only **one** orderbook (OB) depth capture: the
levels recorded at the *last scan tick before it closed* (paper_trader.py `close_opportunity`
stamps `final_*` onto the most recent snapshot for that `pair_key`). That last tick is often
the moment the opportunity was already decaying, so the depth ladder under-represents how
grabbable the opportunity actually was at its best.

We want a Past opportunity to also carry:
1. The OB depth at its **peak spread %**.
2. The OB depth at its **peak grabbability**.
3. **Roughly how long** the opportunity stayed near peak ("how long this depth was available").
4. The existing **last-scan** capture (unchanged).

## Non-Goals

- Storing a full per-tick ladder time-series (only the two peak captures + last scan are kept).
- Changing the Live view (it continues to show the single latest non-finalized snapshot).
- Exact integral of time-in-band — a span approximation ("roughly") is acceptable.

## Definitions

- **Peak spread cycle** — the scan cycle where `spread_pct` reaches its maximum over the
  opportunity's life.
- **Peak grabbability cycle** — the scan cycle where the composite `grabbability` score reaches
  its maximum. May or may not be the same cycle as peak spread.
- **Near-peak window** — the time span the spread stayed within 10% of its final peak:
  `last_tick_ts − first_tick_ts` over all ticks where `spread_pct ≥ 0.9 × peak_spread`.
  Single-tick opportunities → `0.0`.

## Design

### Recording — `OBSnapshotTracker` (paper_trader.py)

Per-opportunity state lives in `_active_opps[pair_key]`. Extend it (initialized in
`record_opportunity` when the pair is first seen) with:

- `peak_spread` *(already exists)* — running max spread.
- `peak_spread_snap` — a captured **bundle** copied at each new spread max:
  `{spread_pct, grabbability, short_bids_levels, long_asks_levels,
    short_bids_total_usd, long_asks_total_usd, ob_skew_ms, ts, duration_s}`.
- `peak_grab` — running max grabbability.
- `peak_grab_snap` — same bundle shape, copied at each new grabbability max.
- `spread_ticks` — list of `(ts_epoch, spread_pct)` appended every cycle.

In `record_opportunity`, **after** the grabbability/levels are computed (so the bundle reflects
the current cycle):
- Append `(now, spread_pct)` to `spread_ticks`.
- If `spread_pct > peak_spread` (or first tick): update `peak_spread` and set
  `peak_spread_snap` to the current bundle.
- If `grabbability > peak_grab` (or first tick): update `peak_grab` and set `peak_grab_snap`.

Live snapshots appended to `self.snapshots` are **unchanged** — no peak fields are written
during the opportunity's life, keeping live snapshots small.

In `close_opportunity`, when finalizing the most recent snapshot for the `pair_key` (where
`final_duration_s` etc. are already written), additionally stamp:
- `near_peak_window_s` — computed from `spread_ticks` against `peak_spread` (band = ≥ 90% of
  peak). Span of qualifying ticks; `0.0` if one tick.
- `peak_spread_ob` — the `peak_spread_snap` bundle.
- `peak_grab_ob` — the `peak_grab_snap` bundle.

These attach to the same final snapshot that already holds the last-scan levels, so a Past row
is self-contained.

### Dashboard — Past expanded row (dashboard.py, `renderOppRows`)

When `_oppView === 'past'` **and** the row is expanded, replace the current single
two-ladder block with up to **three** stacked, labeled depth blocks (each block is the existing
short/long `buildLadder` pair, retaining the per-leg exchange links):

1. **PEAK SPREAD** — from `peak_spread_ob`. Block caption shows
   `spread% · grab NN · available ~Ns` where `Ns` is `near_peak_window_s`.
2. **PEAK GRAB** — from `peak_grab_ob`. Rendered **only if** its `ts` differs from
   `peak_spread_ob.ts` (when the same cycle won both, skip the duplicate).
3. **LAST SCAN** — the existing `short_bids_levels` / `long_asks_levels` on the snapshot.

If `peak_spread_ob` / `peak_grab_ob` are absent (older snapshots recorded before this change),
fall back to the current single last-scan block. The **Live** expanded view is unchanged.

## Data Shape (added to a finalized snapshot)

```jsonc
{
  // ... existing fields incl. final_duration_s, short_bids_levels, long_asks_levels ...
  "near_peak_window_s": 7.0,
  "peak_spread_ob": {
    "spread_pct": 0.42, "grabbability": 71, "ts": "2026-06-21T10:03:11+00:00",
    "duration_s": 3.0, "ob_skew_ms": 120.0,
    "short_bids_total_usd": 1840.0, "long_asks_total_usd": 2100.0,
    "short_bids_levels": [{"price": 1.234, "usd": 500.0}, ...],
    "long_asks_levels":  [{"price": 1.236, "usd": 620.0}, ...]
  },
  "peak_grab_ob": { /* same shape; omit-from-UI if ts == peak_spread_ob.ts */ }
}
```

## Testing

- **Unit (paper_trader):** feed `record_opportunity` a sequence of cycles with a rising-then-
  falling spread (and a grabbability peak at a *different* cycle than the spread peak), then
  `close_opportunity`. Assert the final snapshot's `peak_spread_ob.spread_pct` == max spread,
  `peak_grab_ob.grabbability` == max grab, the two bundles have different `ts`, and
  `near_peak_window_s` equals the span of ticks ≥ 90% of peak.
- **Edge:** single-tick opportunity → `near_peak_window_s == 0.0`, both peak bundles == that tick.
- **Dashboard:** preview verify a Past row expands to PEAK SPREAD + LAST SCAN (and PEAK GRAB
  when distinct); older snapshots without `peak_spread_ob` still render the last-scan fallback.

## Backward Compatibility

Snapshots persisted before this change lack `peak_spread_ob`/`peak_grab_ob`/`near_peak_window_s`.
The dashboard falls back to the single last-scan block for those, so no migration is needed.
