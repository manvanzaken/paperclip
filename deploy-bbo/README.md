# BBO Trader (deploy-bbo)

Event-driven cross-venue convergence trader that needs only real-time best bid/ask from each venue.
Every quote update re-evaluates the symbol; opportunities are taken taker/taker when the spread pays for
it immediately, otherwise taker/maker (rest one leg post-only, hedge the other at market on fill).
Spec: `docs/superpowers/specs/2026-09-05-bbo-taker-maker-trader-design.md`.

## Run (paper mode, no keys)

    python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
    ./start.sh                     # MODE=paper DATA_DIR=./data

State: `data/real_state.json` (same schema as the legacy dashboard reads — run `dashboard.py` with
`DATA_DIR=<this data dir>`). Log: `data/bbo_trader_paper.log`. Heartbeat: `data/bbo_heartbeat_paper` (never
`heartbeat_live`, which belongs to the legacy bot; DATA_DIR must not be the legacy bot's data dir — refused with exit 5). Flags: `data/stop.flag` halts entries and
cancels resting orders, `data/start.flag` resumes. Telegram: `/stop /start /close_all /status`.

## Tests

    .venv/bin/python -m pytest -q

## Layout

`bbo_trader/edge.py` (pure edge math) · `strategy.py` (gates → intents) · `execution.py` (TT + pegged
maker flows) · `positions.py` (state machine + state file) · `venues/` (protocols, WS runner, MEXC,
BloFin, SimVenue, registry) · `app.py` (wiring + sweep) · `main.py` (entry point).

## Config

Environment variables override `config.py` defaults; `DATA_DIR/bot_config.json` overrides both
(restart to apply). Venues, fees and rate limits: `config/venues.json`. Blocked symbols:
`config/blocked_symbols.json`.

Per-venue `staleness_override_s` widens the quote-staleness budget (default `STALE_QUOTE_S`, 2 s) for feeds
that push only on change: MEXC's depth channel measured a p99 inter-update gap of 5.2 s (2026-09-05), so it
runs at 5 s — this also lets an entry be priced off a quote up to 5 s old on that venue. BloFin needs none.
Open positions' legs are REST-refreshed at half the budget; the state file's `bbo.metrics.funnel` counters
`fallback_ok` / `fallback_timeout` / `fallback_failed` show whether those refreshes answer in time.
