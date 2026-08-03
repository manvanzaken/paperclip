# SpreadWatch — Cross-Exchange Spread Viewer — Design Spec

**Date:** 2026-08-03
**Location:** NEW standalone project at `/Users/vandenboogaard/Claude projects/spreadwatch/` — its own git repo, fully independent of the Paperclip repo.
**Goal:** A live dashboard + full-history recorder for cross-exchange spreads on the same asset, built on the measurement lessons from the Paperclip trading system: mid-price spreads lie — fees, orderbook depth at a target size, opportunity lifetime, and book time-sync decide whether a spread is real.

## Decisions (locked during brainstorming)

- **Purpose:** both a live view and a recorder (one collector feeds both).
- **Exchanges:** MEXC, BloFin, Binance, Bybit, OKX, Bitget, Gate (the Paperclip set).
- **Measurement:** full realism — L2 executable spread at a target order size, minus taker fees, plus a 0-100 grabbability score (opportunity-mapper model).
- **Universe:** USDT perps listed on ≥2 covered exchanges, PLUS equity pre-IPO perps (SPACEX/OPENAI-style USDT contracts on MEXC/Gate/Bitget) as a tagged `pre_ipo` category via a configurable symbol list. Actual per-exchange availability of pre-IPO symbols to be verified during implementation.
- **Deployment:** local Mac only.
- **History:** full tick history (every computed spread tick), with retention pruning.
- **Architecture:** single Python asyncio process + FastAPI + SQLite (Approach A).
- **Main UI:** Spread Matrix (sortable live table), with row-click detail drawer and a history/research page. No alerts in v1.

## Project layout

```
spreadwatch/
├── spreadwatch/
│   ├── collector/        # exchange feeds: WS L2 managers + REST pollers
│   ├── core/             # models, fee tables, spread math, grabbability
│   ├── storage/          # SQLite writer, retention, query helpers
│   ├── api/              # FastAPI app: REST + WebSocket push, serves UI
│   └── web/              # static dashboard (vanilla JS, no build step)
├── tests/
├── config.yaml           # exchanges, target size, thresholds, retention
└── run.py                # single entry point: collector + API in one process
```

`python run.py` starts everything; dashboard at `http://localhost:8377`.

## Collector

- **WS L2 streams** for Binance, Bybit, OKX, Bitget, Gate — in-memory orderbook cache per symbol; each cached book retains its own `ts` (per-leg as-of time is always known).
- **REST polling** for MEXC + BloFin (neither streams WS L2) — budgeted calls per cycle (cf. Paperclip `MAX_OB_CALLS_PER_CYCLE`), prioritized by which symbols currently show the widest raw (ticker-level) spreads: a cheap ticker scan decides where to spend expensive book pulls.
- **Symbol discovery** at startup + hourly refresh: all USDT perps on ≥2 covered exchanges; pre-IPO symbols come from a config list and are tagged `category: pre_ipo`.
- **Scan cycle ~1s:** for each symbol, compare every exchange pair where both books are fresh (staleness cutoff, default 3s); compute spread metrics; emit ticks to storage and to the live in-memory state consumed by the API.

## Measurement model

Per symbol × exchange-pair, per tick:

- **Raw spread %** — mid-price difference (cheap; always computed and shown).
- **Executable spread %** — VWAP walk of both L2 books at `TARGET_SIZE_USD` (default 25, configurable): sell into bids on the rich exchange, buy from asks on the cheap exchange.
- **Net spread %** — executable spread minus both legs' taker fees (per-exchange fee table in config, overridable).
- **Grabbability 0-100** — composite of five 0-100 factors (equal weights, config-overridable), stored as composite + per-factor breakdown:
  - `duration`: `clamp(opp_age_s / FILL_WINDOW_S, 0, 1) * 100`, `FILL_WINDOW_S = 0.85`
  - `depth`: `clamp(min(depth_a, depth_b) / TARGET_SIZE_USD, 0, 1) * 100`
  - `margin`: `clamp((exec_spread - total_fees) / total_fees, 0, 1) * 100`
  - `velocity`: `clamp(0.5 + ewma_spread_delta * VELOCITY_K, 0, 1) * 100`, `VELOCITY_K = 1.0`
  - `sync`: `clamp(1 - ob_skew_ms / SYNC_TOLERANCE_MS, 0, 1) * 100`, `SYNC_TOLERANCE_MS = 250`, `ob_skew_ms = |ts_a - ts_b| * 1000`
- **Opportunity lifecycle** — opens when net spread crosses above `MIN_PROFIT_MARGIN_PCT` (default 0.05); tracks peak net spread, peak depth, tick count; closes when net spread drops below 0. Final grabbability recomputed at close using full duration.
- **Sanity cap** — spreads above `MAX_SANE_SPREAD_PCT` (default 10.0) are excluded from opportunities but logged.

## Storage

SQLite, WAL mode, one DB file per day (`data/ticks/YYYY-MM-DD.db`):

- **`ticks`** — every computed spread tick: ts, symbol, category, exchange_a, exchange_b, raw/executable/net spread %, depth_a/depth_b USD, grabbability, ob_skew_ms. Batched inserts (one batch write per second, not per row).
- **`opportunities`** — one row per lifecycle: open_ts, close_ts, duration_s, peak net spread, peak grabbability + factor breakdown (JSON), tick_count, category.
- **`rollups_1m`** — per-minute best/avg net spread per pair, written live so the history page never scans raw ticks for charts.
- **Retention:** `retention_days` (default 30); startup task deletes expired daily files. Estimated growth ~1-2 GB/week at ~500 pairs × 1s cadence.

## API

FastAPI on `localhost:8377`:

- `GET /api/matrix` — current live matrix; same payload pushed over `WS /api/live` every second.
- `GET /api/pair/{symbol}/{ex_a}/{ex_b}` — detail: recent spread series, current depth ladders, grabbability factor breakdown.
- `GET /api/history/spreads`, `/api/history/opportunities`, `/api/history/rankings` — spread-over-time (rollups, drill-down to raw ticks), opportunity log, best-pairs / best-hours rankings. Ranking metric: `opportunity value = peak_net_spread_pct/100 × min(peak_min_depth_usd, TARGET_SIZE_USD)` (estimated capturable USD per opportunity), summed per pair / per hour-of-day.
- `GET /api/status` — per-exchange feed health: WS connected, book staleness, REST budget usage.

## Dashboard (static, vanilla JS + lightweight chart lib)

- **Matrix view (main):** sortable/filterable table — symbol (with `PRE-IPO` badge), short/long exchange, raw %, net %, depth $, grabbability, opportunity age, 1h sparkline. Filters: net > 0 only, pre-IPO category, symbol search. Pairs with stale feeds are dimmed.
- **Detail drawer** (row click): live spread chart, two-sided depth ladder at target size, grabbability factor bars.
- **History page:** pair picker → spread chart over time; opportunity log table; best-pairs and best-hours rankings.

## Error handling

- Per-exchange WS auto-reconnect with exponential backoff; a dead exchange never blocks the scan cycle — its pairs go stale and dim.
- REST 429 responses temporarily halve that exchange's call budget (Paperclip pattern).
- Books older than the staleness cutoff are never compared.
- Out-of-sanity spreads logged, excluded from opportunities.

## Testing (TDD)

- Unit coverage of all pure functions: VWAP walk, net-spread math, each grabbability factor (bounds [0,100] + monotonicity), composite blending, opportunity lifecycle transitions (open/close/peak tracking/final score), rollup aggregation, retention file selection.
- Collector integration tested against recorded fixture orderbooks — never live APIs in tests.

## Config defaults

```
TARGET_SIZE_USD = 25.0
MIN_PROFIT_MARGIN_PCT = 0.05
MAX_SANE_SPREAD_PCT = 10.0
FILL_WINDOW_S = 0.85
SYNC_TOLERANCE_MS = 250
VELOCITY_K = 1.0
GRAB_WEIGHTS = equal
STALENESS_CUTOFF_S = 3.0
SCAN_INTERVAL_S = 1.0
RETENTION_DAYS = 30
PORT = 8377
```

## Success criteria

- Matrix shows live fee-adjusted executable spreads across all 7 exchanges with grabbability, updating ~1/s.
- Pre-IPO perps appear as a filterable tagged category.
- Full tick history accumulates in daily SQLite files; history page answers "which pairs/hours have the best opportunities" from rollups.
- Killing one exchange feed degrades only that exchange's pairs.
- All pure-math tests pass; no live-API calls in the test suite.

## Out of scope (v1)

- Alerts/notifications, trade execution, server deployment, spot markets, React UI.
