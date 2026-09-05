# BBO Trader — Event-Driven Taker/Maker Convergence Trader — Design Spec

**Date:** 2026-09-05
**Location:** NEW self-contained package `deploy-bbo/` on branch `claude/trader-realtime-bid-ask-f22eff` (this repo). Fully separate from `deploy-live/real_trader.py` (branch `claude/tender-germain`): no shared runtime code, state files, data directory, or systemd unit.
**Goal:** A second live trader that (1) earns on venues that expose only real-time best bid/ask — no dependence on L2 depth, (2) reacts in well under one second from quote to order and from maker fill to hedge, and (3) executes every opportunity as **taker/taker when the spread pays for it right now**, and otherwise as **taker/maker**: rest one leg post-only, hedge the other leg at market the moment the resting leg fills.

## Why a new trader instead of changing the current one

- `real_trader.py` polls REST (3 s idle, 0.3 s with a position open) and its hot path is serial REST: a position pre-check before every entry (2 GETs), a ticker GET inside every order for sizing, fill verification by polling every 300 ms for up to 10 s, and a 5 s sleep after every close. Tokyo per-leg fill latency is 100–300 ms; the loop around it adds seconds.
- Every order is a market order. A MEXC↔BloFin round trip costs 0.16% in taker fees (0.02 + 0.06 in, 0.02 + 0.06 out). SpreadWatch's 53 clean-feed trades: sustained convergence available median 0.35pp against a 0.46% round-trip cost — taker-only loses even with perfect exits (32% profitable); maker on the entry legs lifts the same population to 62% profitable (+0.14%/trade). The fee floor, not the signal, is the wall.
- 5,757 lines in one file with four exchange executors, two of which (OKX, Bybit) are unusable for EU live trading.

## Decisions (locked for v1) — please confirm or change

1. **Venues:** MEXC + BloFin USDT perps only (the only live-tradeable pair for this account). Universe = symbols listed on both (~376), minus a blocked list and the persisted BloFin risk-control blacklist.
2. **Data contract is BBO-only.** A venue adapter delivers `bid, bid_qty, ask, ask_qty, ts` per symbol. Where that comes from is the adapter's business (MEXC: top level of `sub.depth.full limit=5`; BloFin: top level of `books5`). The strategy never reads L2. New BBO venues later = one adapter file.
3. **No fixed scan cycle.** Every quote update re-evaluates its symbol immediately (event-driven). A 500 ms sweep handles timers (TTL, timeouts, staleness). Effective decision latency is bounded by feed cadence (BloFin ≤ 100 ms on change; MEXC ~220 ms–1 s per symbol on `sub.depth.full`, to be probed against incremental `sub.depth`) — not by a poll interval.
4. **Fill confirmation via private WebSocket order events**, not REST polling. REST order queries are a fallback only when no event arrives within 1.5 s.
5. **Two execution modes, chosen per opportunity:** TT (taker/taker, both market orders in parallel) when the TT edge clears the threshold; otherwise TM (taker/maker) when the TM edge clears it. TT has priority because it carries no fill risk. TM entries **and** TM exits use one component (`PeggedMaker`).
6. **Position sizing and risk defaults carry over from real_trader:** `MAX_POSITION_USD=25`, `MAX_CONCURRENT=3`, kill switch at 10% drawdown, `MAX_HOLD_MIN=30`, one position per symbol.
7. **Modes:** `paper` (default — real feeds, simulated venue, never touches the accounts) and `live`. Live refuses to start while the legacy bot's heartbeat file is fresh (< 120 s), because both bots would race for the same balances.
8. **Dashboard:** the trader writes `real_state.json` in the schema `dashboard.py` already reads (plus a `bbo` section). Run the existing dashboard unmodified with `DATA_DIR` pointed at the new trader's data directory. No dashboard code changes in v1.
9. **Deployment target:** Tokyo Lightsail, its own systemd unit `bbotrader.service`, its own `DATA_DIR` (`/app/data_bbo`). Paper mode may run anywhere (also alongside the old bot).
10. **Calibration:** the 200 historical live trades are never used (user rule). Thresholds start from SpreadWatch's clean-feed evidence and from the fee arithmetic below; the trader's own paper/live trades feed later tuning.

## Approaches considered

- **A. Event-driven rewrite as a new package (recommended, this spec).** WS BBO feeds → in-memory quote board → per-update evaluation → parallel REST placement → private-WS fill events → pegged-maker executor. Meets the sub-second target, has clean testable boundaries, is portable to other BBO venues. Cost: most new code; exchange quirks must be re-learned — mitigated by porting the proven MEXC/BloFin signing, sizing, and side-code logic from `real_trader.py` and by probe-first fixtures (the SpreadWatch lesson: docs were wrong for 4 of 5 venues).
- **B. Retrofit `real_trader.py`.** Add post-only orders and private WS to the existing file, keep the 0.3 s poll loop. Reuses the most code, but the hot path stays serial REST, the loop cannot react faster than its poll, the file is already unmaintainable, and the user asked for a separate version.
- **C. SpreadWatch as brain + thin live executor.** Reuses the lab's gates, but SpreadWatch scans at 1.5 s (saturated at 14 venues), runs on the Mac over a home network (the NordVPN incident), and couples a research tool to live money. Its *parameters* are reused here as config defaults instead.

## Architecture

```
            MEXC public WS (depth.full l5, 30 topics/conn ×13)     BloFin public WS (books5, 50/conn ×8)
                        │                                                   │
                        ▼                                                   ▼
                  ┌───────────────────── QuoteBoard (in-memory BBO, staleness) ─────────────────────┐
                  │  on_quote(venue, symbol) ──► PairEvaluator.evaluate() ──► Intent                │
                  └─────────────────────────────────────┬────────────────────────────────────────────┘
                                                        ▼
   ┌──────── RiskManager (kill switch, blacklists, balances, funding, concurrency) ────────┐
   └──────────────────────────────────────┬────────────────────────────────────────────────┘
                                          ▼
                  ┌────────────────── Executor ──────────────────┐
                  │  TakerTaker flow        PeggedMaker flow     │──► REST order/cancel/amend (aiohttp keep-alive)
                  │  (both legs parallel)   (post, peg, hedge)   │      ▲ RateBudget: token buckets per venue
                  └──────────────────────┬──────────────────────┘
                                         ▼
      MEXC private WS (order, order.deal)  ─┐            ┌─ BloFin private WS (orders, positions)
                                            ▼            ▼
                              OrderEventRouter (match by clientOrderId) ──► PositionManager (state machine)
                                                                                    │
                                                                    StateStore (atomic JSON) · Metrics · Telegram
```

Single Python 3.12 asyncio process, `aiohttp` for WS + REST (one keep-alive session per venue), no other runtime dependencies. Periodic tasks (500 ms sweep, 30 s balances, 60 s reconcile + metrics flush, hourly discovery) run beside the event path and never block it.

### Package layout

```
deploy-bbo/
├── README.md, requirements.txt (aiohttp), start.sh, bbotrader.service
├── bbo_trader/
│   ├── config.py        # env + DATA_DIR/bot_config.json → frozen Config
│   ├── models.py        # BBO, PairQuote, Intent, OrderEvent, Fill, Position
│   ├── quotes.py        # QuoteBoard: set/get BBO, staleness, touch notional
│   ├── edge.py          # PURE: tt/tm edges, mode + maker-venue choice, peg price
│   ├── strategy.py      # PairEvaluator: gates + intent (pure given inputs)
│   ├── sizing.py        # PURE: USD → contracts per venue, mismatch check
│   ├── budget.py        # PURE: token buckets + reserve for risk-reducing calls
│   ├── positions.py     # Position state machine + StateStore (real_state.json)
│   ├── execution.py     # Executor: TT flow, PeggedMaker flow, hedge, flatten
│   ├── risk.py          # kill switch, blacklists/strikes, balances, funding gate
│   ├── reconcile.py     # startup + periodic reconciliation vs exchange truth
│   ├── metrics.py       # latency histograms, feed coverage watchdog
│   ├── notify.py        # Telegram
│   ├── venues/base.py   # Venue interface: bbo_feed(), order_feed(), trading calls
│   ├── venues/mexc.py, venues/blofin.py, venues/sim.py
│   └── main.py          # wiring, sweep, shutdown
└── tests/               # pytest, no live API calls
```

## Data model

- `BBO(venue, symbol, bid, bid_qty, ask, ask_qty, ts_exchange, ts_local)`; `touch_notional_bid = bid × bid_qty × contract_size` (and ask). Stale when `now − ts_local > STALE_QUOTE_S`.
- `PairQuote(symbol, A, B)` — A is the venue with the higher bid (we sell/short there), B the other (we buy/long there). Both directions are evaluated on every update.
- `Intent`: `TT_ENTER(size)`, `TM_ENTER(maker_venue, rest_price, size)`, `TT_EXIT(reason)`, `TM_EXIT(maker_venue, rest_price)`, `REQUOTE(price)`, `CANCEL(reason)`, or `NONE(reason)` — every rejection reason is counted in a funnel (SpreadWatch pattern).
- `OrderEvent(venue, client_id, order_id, state, filled_qty, avg_price, fee, liquidity∈{maker,taker}, position_id, ts)` — normalized from MEXC `push.personal.order(.deal)` and BloFin `orders`. States: `ack, partial, filled, canceled, rejected`.
- `Position` — the `LivePosition` fields (ids, venues, entry/exit prices and spreads, fees, P&L, filled contracts, analytics) plus: `entry_mode ∈ {TT, TM}`, `exit_mode`, `maker_venue`, `maker_client_id`, `maker_rest_price`, `maker_filled_qty`, `hedged_qty`, `position_id_mexc`, `latency` (detect→submit, submit→ack, ack→fill, fill→hedge_submitted, fill→hedged, all ms), `fee_liquidity` per leg (what the venue actually charged: maker or taker).

## Strategy: edge math and mode selection

All quantities in percent points. Taker fees `t_A, t_B`, maker fees `m_A, m_B` from config (verified against realized fees, see Observability).

**Taker/taker entry**
- `spread_tt = (bid_A − ask_B) / ask_B × 100`
- `edge_tt = spread_tt − (t_A + t_B) − FEES_OUT_EST − EXIT_SPREAD_PCT − SLIP_PCT`
- `FEES_OUT_EST = t_A + t_B` (conservative: timeouts and stops always exit taker/taker).

**Taker/maker entry** — two candidates, the venue with the larger edge makes:
- Make on A (rest SELL at A's ask, hedge BUY on B at `ask_B`): `spread_tm_A = (ask_A − ask_B) / ask_B × 100`, `edge_tm_A = spread_tm_A − (m_A + t_B) − FEES_OUT_EST − EXIT_SPREAD_PCT − SLIP_PCT`
- Make on B (rest BUY at B's bid, hedge SELL on A at `bid_A`): `spread_tm_B = (bid_A − bid_B) / bid_B × 100`, `edge_tm_B = spread_tm_B − (t_A + m_B) − FEES_OUT_EST − EXIT_SPREAD_PCT − SLIP_PCT`
- The maker does not cross its own venue's bid-ask, so `spread_tm ≥ spread_tt` by that venue's touch width; plus the fee saving `t − m`. That is why TM makes many more pairs viable.

**Mode rule**
1. `edge_tt ≥ MIN_EDGE_PCT` → **TT** now (no fill risk; grab it before it vanishes).
2. else `max(edge_tm_A, edge_tm_B) ≥ MIN_EDGE_PCT + TM_EXTRA_EDGE_PCT` → **TM** on the winning venue (`MAKER_VENUE_POLICY=best_edge`; `mexc`/`blofin` force it for experiments).
3. else `NONE`.

Worked example (MEXC↔BloFin, defaults): TT needs `spread_tt ≥ 0.08 + 0.08 + 0.15 + 0.05 + 0.05 = 0.41%`. TM on BloFin needs `spread_tm ≥ 0.04 + 0.08 + 0.15 + 0.05 + 0.10 = 0.42%`, but `spread_tm` already contains BloFin's touch width (typically 0.05–0.30% on alts). A 0.30% raw spread on a symbol whose BloFin book is 0.20% wide is a TM candidate; it is nothing for TT.

**Pegged maker price** (make on A, sell). The resting price must both join the queue and still clear the edge against B's live quote:
- `p_floor = ask_B × (1 + (MIN_EDGE_PCT + TM_EXTRA_EDGE_PCT + m_A + t_B + FEES_OUT_EST + EXIT_SPREAD_PCT + SLIP_PCT) / 100)`
- `p_rest = round_up_to_tick(max(ask_A − IMPROVE_TICKS × tick, p_floor))`; requires `p_rest > bid_A` (post-only would otherwise be rejected) — else cancel/skip.
- Make on B (buy) mirrors: `p_cap = bid_A × (1 − (...) / 100)`, `p_rest = round_down_to_tick(min(bid_B + IMPROVE_TICKS × tick, p_cap))`, requires `p_rest < ask_B`.
- **Requote** when the recomputed `p_rest` differs from the working price by `≥ REQUOTE_TICKS`, at most once per `MIN_REQUOTE_MS` per order, subject to the venue's rate budget. **Cancel** when the edge condition has failed continuously for `EDGE_GONE_MS`, when either quote is stale, or at `MAKER_TTL_S`.
- **Upgrade to TT**: if `edge_tt ≥ MIN_EDGE_PCT` appears while a maker rests, cancel it and enter TT (a fill that races the cancel is simply hedged).

**Exit rules** for an open position (entered short A / long B at entry spread `s_e`):
- `spread_exit_tt = (ask_A − bid_B) / bid_B × 100` — what closing at market costs now (both crossings included).
- **TT exit** when `spread_exit_tt ≤ EXIT_SPREAD_PCT`, or on `MAX_HOLD_MIN` timeout, or divergence stop `spread_tt ≥ s_e + STOP_PCT`, or kill switch. Timeout/stop/kill are always TT.
- **TM exit** (resting take-profit), while `spread_exit_tt > EXIT_SPREAD_PCT` and `TM_EXIT_ENABLED`: on the exit maker venue (policy `EXIT_MAKER_VENUE_POLICY=best_fee` → the venue with the larger `t − m`), rest a reduce-only post-only order at the price that realizes the target against the other venue's live touch — e.g. make on A: `p_rest = round_down(min(bid_A + IMPROVE_TICKS × tick, bid_B × (1 + EXIT_SPREAD_PCT / 100)))`, `p_rest < ask_A`; peg it as `bid_B` moves. On fill → immediate reduce-only market hedge on B. Whenever the TT exit condition becomes true, cancel the resting exit order and exit TT.
- Realized P&L uses actual fills and fees from order events, as today (`_enrich` pattern becomes unnecessary: the events carry price and fee).

## Execution

### Hot-path rules (this is where the sub-second comes from)
- No REST GET on the path from quote to order: contract specs are preloaded at startup and refreshed hourly; sizing uses the cached touch; no position pre-check (fills are verified by events); MEXC `positionId` for hedge-mode closes is captured from the entry's `push.personal.order` event, not queried.
- BloFin leverage is set per symbol **when a pair arms** (edge crosses 50% of the threshold) — `ARM → prewarm` — never inside the entry. Symbols already set are persisted in state.
- Both TT legs are submitted concurrently; the TM hedge is submitted within the same event-loop turn as the fill event.
- Order events are matched to intents by `clientOrderId` (MEXC `externalOid`, BloFin `clientOrderId`, ≤32 chars, prefix `b<mode><pos>-<leg>-<seq>`), so a fill arriving before the REST ack is handled correctly. Order ids are also idempotency keys for retries.
- `RateBudget`: token buckets from the documented limits — MEXC 20 orders/2 s and 20 cancels/2 s; BloFin 30 trading requests/10 s (all trading endpoints share it). `RESERVE_TOKENS` (MEXC 4, BloFin 6) are only spendable by hedges, closes, flattens, and cancels, so new entries and requotes can never starve a risk-reducing call. `MAX_RESTING_MAKERS=2` keeps requote demand inside BloFin's budget.
- Fixed `MIN_ORDER_GAP_SEC=1.5` from the old bot is dropped; the budget replaces it.

### TakerTaker flow
Submit sell on A and buy on B in parallel → wait for both terminal events (fallback: REST query after 1.5 s, poll 300 ms, up to 5 s; a market order with no terminal state after that is treated as filled at submitted size, as the old bot does for MEXC) → both filled: match sizes, flatten any excess `> MAX_LEG_MISMATCH_PCT`, open position with actual fills; fill-quality guard as today (`MIN_FILL_SPREAD_PCT`) → one leg failed: flatten the filled leg (retry ladder 1/2/5/10/10/10 s, then `DEGRADED`), record `failed_entry`, pair strike → both failed: symbol cooldown 60 s, pair strike.

### PeggedMaker flow (entry and exit)
1. `post`: post-only order at `p_rest`, TTL timer, `MAKER_RESTING`. Rejected (would cross / 102127 / other) → counted, pair strike where applicable, back to idle (102127 → blacklist).
2. `peg`: on every quote update of either venue recompute `p_rest`; requote via BloFin `amend-order` (1 call) or MEXC cancel + new (2 calls, new clientOrderId); cancel on edge-gone/stale/TTL.
3. `fill`: on any fill event (partial or full): cancel the remainder (one call, may race — fine), compute `unhedged = filled_qty − hedged_qty`; if the hedge venue can express it (`≥ min contracts`) → market hedge that quantity immediately; else if the order is terminal → flatten the unhedgeable remainder on the maker venue (reduce-only market). Later fill events hedge their delta the same way. Position opens (or closes) at matched size when the maker order is terminal and `hedged_qty == filled_qty`.
4. Hedge failure → retry 3× (200 ms apart, budget reserve) → flatten the maker fill → record `leg_desync_unwind`, pair strike. Naked exposure is bounded by `MAX_NAKED_MS` (alert if exceeded) and is always resolved by either a hedge or a flatten — never left open.

### Latency budget (Tokyo, measured and persisted as p50/p95)

| step | budget |
|---|---|
| quote update → intent | < 5 ms |
| intent → REST submitted (both legs) | < 5 ms |
| submit → ack | 50–150 ms |
| ack → fill event | 50–200 ms |
| **TT: signal → both fills confirmed** | **≈ 150–400 ms** |
| maker fill event → hedge submitted | < 10 ms |
| hedge submit → hedge fill event | 100–300 ms |
| **TM: fill → hedged** | **≈ 150–350 ms** |
| requote (amend or cancel+new) | 50–150 ms, budget-throttled |

Targets: p50 signal→hedged < 500 ms, p95 < 1,000 ms. Event-loop lag is sampled every second; sustained lag > 50 ms is logged and shown in state.

## Position state machine

`IDLE → TT_ENTERING → OPEN`, `IDLE → MAKER_RESTING → HEDGING → OPEN`, `OPEN → TT_EXITING → CLOSED`, `OPEN → EXIT_MAKER_RESTING → EXIT_HEDGING → CLOSED`, any leg failure to close → `DEGRADED` (retry ladder every sweep, as today) → `CLOSED`. Every transition persists state atomically (`tmp` + `os.replace`); a dirty-flag save also runs at most once per second. Events driving transitions: `quote`, `order_event`, `timer` (sweep), `command` (stop/kill). Cancel-after-fill and fill-after-cancel are first-class transitions, not errors.

Crash recovery: reload state → for every non-terminal position query open orders on both venues → cancel orders with our clientOrderId prefix, reconcile their fills into `filled_qty`/`hedged_qty` → resume the flow (hedge, flatten, or monitor). Unknown positions on either exchange are orphans → closed and recorded (`orphan_cleanup`), never adopted silently.

## Risk and gates (v1, deliberately lean)

Quote freshness (`STALE_QUOTE_S`), sanity cap (`MAX_SANE_SPREAD_PCT`, logged and skipped), `MIN_EDGE_PCT` (+ `TM_EXTRA_EDGE_PCT` for TM), touch-depth guard (skip when a touch we would cross has notional `< size × TOUCH_DEPTH_MULT` — TT: both touches; TM: the hedge venue's touch), 24 h volume on both venues, funding gate (block entry when a settlement with unfavorable net funding falls within `FUNDING_BLOCK_S`), blocked symbols + persisted BloFin 102127 blacklist, 2-strike pair blacklist (24 h), symbol blacklist 6 h after a loss > 0.10% of size, failed-entry cooldown 60 s, pair win-rate gate (≥ 5 of the *new trader's own* closes and < 30% wins → skip), `MAX_CONCURRENT`, one position per symbol, per-exchange balance check from the 30 s cache with 5% buffer, kill switch (drawdown % / equity floor) → cancel all resting orders, close everything TT, halt with cooldown. Not included in v1: pullback-rate, opportunity-age, crossing cap, trade-confirm (need L2 or trade streams); they can be added later as pure gates.

## Paper mode

`SimVenue` implements the same venue interface over the real public BBO feeds: taker orders fill at the current touch plus `SIM_TAKER_SLIP_BPS` after `SIM_LATENCY_MS`; post-only orders rest and fill only when the opposite touch **crosses** the resting price on a quote newer than the post (conservative, the SpreadWatch `post_hedge` rule), for at most `MAKER_TOP_LEVEL_FRAC × touch notional`; would-cross posts are rejected like the real venue; events are emitted with the same shapes and latencies as live. Everything above the venue interface is identical in paper and live, so a paper soak exercises the real state machine, budget, and persistence.

## Venue facts (researched 2026-09-05; verify with a live probe before coding parsers)

| | MEXC contract | BloFin |
|---|---|---|
| Fees (base tier) | 0.00% maker / 0.02% taker | 0.02% maker / 0.06% taker |
| BBO source | WS `sub.depth.full` `limit:5` (~1–4.5 pushes/s/symbol; probe incremental `sub.depth` and use whichever is faster); `sub.ticker` pushes only 1/s | WS `books5` (every 100 ms on change, `data` is a dict); `tickers` 1/s |
| WS caps (measured by SpreadWatch) | 30 topics/conn; app ping `{"method":"ping"}` ≤ 60 s | 50 topics/conn; bare text `ping` / `pong` |
| Private WS | `wss://contract.mexc.com/edge`, login `HMAC(secret, apiKey + reqTime)`; `push.personal.order` (state 1 uninformed / 2 uncompleted / 3 completed / 4 cancelled / 5 invalid; `positionId`, `dealVol`, `dealAvgPrice`, `takerFee`, `makerFee`, `externalOid`), `push.personal.order.deal` (`price`, `vol`, `fee`, `isTaker`) | `wss://openapi.blofin.com/ws/private`, sign = base64(hex(HMAC(secret, "/users/self/verify" + "GET" + ts + nonce))); `orders` (state `live / partially_filled / filled / canceled / failed`; `fillPrice`, `fillSize`, `fillFee`, `execType` maker/taker, `clientOrderId`), `positions` |
| Order types | `type` 1 limit, **2 post-only**, 3 IOC, 4 FOK, 5 market; `externalOid` ≤ 32 | `orderType` `market`, `limit`, **`post_only`**, `fok`, `ioc`; `clientOrderId` ≤ 32 |
| Amend | none → cancel + new | `POST /api/v1/trade/amend-order` (`newPrice`, `newSize`) |
| Rate limits | orders 20 / 2 s; cancels 20 / 2 s | 30 trading requests / 10 s per user |
| Position mode | hedge: side 1 open long, 2 close short, 3 open short, 4 close long | one-way (`net`), `reduceOnly` on closes |
| Sizing | `vol` in contracts; `contractSize`, `volUnit`, `minVol`, `priceUnit` from `/api/v1/contract/detail` | `size` in contracts; `contractValue`, `lotSize`, `minSize`, `tickSize` from `/api/v1/market/instruments`; leverage per symbol via `set-leverage` |
| Known quirks (from real_trader) | order-status endpoint lags seconds after market fills; `dealVol` misreports → trust submitted vol for market orders; Cloudflare requires a User-Agent; GET signing is `apiKey + ts (+ query)` | 102127 risk-control rejects (blacklist persistently); accepted-but-never-filled orders; `/trade/order?orderId=` returns 152409 → use `orders-history`; empty/text bodies possible |

Sources: MEXC contract API docs (`mexcdevelop.github.io/apidocs/contract_v1_en`), BloFin API docs (`docs.blofin.com`), MEXC/BloFin fee pages, SpreadWatch collector notes.

## Error handling

- Public WS drop → uptime-keyed reconnect backoff (SpreadWatch's `Backoff`: reset only after ≥ 20 s uptime, cap 30 s); affected quotes go stale → pairs untradeable, resting makers cancelled; symbols with **open positions** fall back to a 1 s REST ticker poll so exits never depend on WS health. Coverage watchdog logs fresh-quote counts per venue each minute and warns under a floor.
- Private WS drop → order tracking falls back to REST status polling (1.5 s grace, 300 ms cadence) until it reconnects; resting makers are re-verified on reconnect.
- REST 429 / "too frequent" → that venue's budget halves for 60 s; entries on it pause; hedges/closes keep the reserve.
- Post-only rejects, amend failures, and cancel-on-filled are normal transitions with counters, not exceptions.
- Realized fee check: if a fill's fee differs from the configured rate for its liquidity type by > 20%, log `FEE_MISMATCH` and send one Telegram warning per venue per hour.
- Any unhandled exception in the event path is caught per event, logged with the intent, and never kills the loop; `systemd Restart=always` covers process death; state is crash-safe by construction.

## Observability

Grep-able markers: `INTENT`, `TT_ENTER`, `TM_POST`, `TM_REQUOTE`, `TM_CANCEL`, `TM_FILL`, `TM_HEDGE`, `UPGRADE_TT`, `LEG_DESYNC`, `FLATTEN`, `CLOSE`, `ORPHAN`, `FEE_MISMATCH`, `LOOP_LAG`, `FEED_COVERAGE`. Per-position latency fields (see Data model). A funnel dict of rejection reasons, latency p50/p95 per stage, budget utilization, feed coverage, and pending makers live in the `bbo` section of `real_state.json` and are logged every 60 s. Telegram on open/close/desync/kill, plus the existing heartbeat file (`heartbeat_<mode>`).

## Build order (for the implementation plan)

1. **Paper-complete core:** config, models, QuoteBoard, edge/sizing/budget (pure), state machine + StateStore, PairEvaluator, Executor with both flows, RiskManager, metrics, SimVenue, public BBO adapters for MEXC and BloFin, `main.py` — the whole pipeline runs in paper mode.
2. **Live adapters:** MEXC and BloFin private WS order feeds, REST trading calls (post-only, market, cancel, amend), reconciliation, legacy-heartbeat guard, Tokyo unit files.
3. **Rollout** as below.

## Testing (TDD, no live API calls in tests)

- **Pure units:** edge math for both directions and both maker venues (including the worked example above); mode choice; peg price rounding, post-only guard, requote/cancel decisions; sizing and contract rounding, mismatch handling, unhedgeable partials; token buckets and reserve; staleness; kill-switch math; funding window.
- **State machine:** every transition including races — fill after cancel, cancel after fill, partial fills across cancel, hedge failure → flatten, crash recovery from each non-terminal state with scripted open-order/fill snapshots.
- **Adapters:** parser tests on fixture messages **captured live** (MEXC `push.depth.full`, `push.personal.order`, `push.personal.order.deal`; BloFin `books5`, `orders`); signing functions against known vectors; contract-spec parsing.
- **Integration with fake venues:** scripted BBO sequences + scripted order-event replies drive full TT and TM entry/exit flows end-to-end; assert intents, orders sent, state persisted, latency fields populated, budget respected.
- **Paper soak (acceptance before live):** ≥ 48 h on live feeds: zero unhandled exceptions, feed coverage stable, p95 signal→submit within budget, maker fills recorded with `maker_detail`, realized paper P&L and funnel reviewed.

## Configuration defaults (env, overridable by `DATA_DIR/bot_config.json`, restart to apply)

```
MODE=paper | live                    DATA_DIR=/app/data_bbo
MAX_POSITION_USD=25                  POSITION_SIZE_PCT=0.125     MIN_POSITION_USD=10
MAX_CONCURRENT=3                     MAX_RESTING_MAKERS=2
MIN_EDGE_PCT=0.05                    TM_EXTRA_EDGE_PCT=0.05      SLIP_PCT=0.05
EXIT_SPREAD_PCT=0.15                 STOP_PCT=1.5                MAX_HOLD_MIN=30
MIN_FILL_SPREAD_PCT=-0.10            MAX_SANE_SPREAD_PCT=10.0    STALE_QUOTE_S=2.0
TT_ENABLED=true  TM_ENTRY_ENABLED=true  TM_EXIT_ENABLED=true
MAKER_VENUE_POLICY=best_edge         EXIT_MAKER_VENUE_POLICY=best_fee
MAKER_TTL_S=30  IMPROVE_TICKS=0  REQUOTE_TICKS=1  MIN_REQUOTE_MS mexc 500 / blofin 1000  EDGE_GONE_MS=300
MAX_NAKED_MS=1500                    MAX_LEG_MISMATCH_PCT=5
TOUCH_DEPTH_MULT=1.0                 MIN_VOLUME_USD=50000        FUNDING_BLOCK_S=600
KILL_SWITCH_DRAWDOWN_PCT=10          HARD_EQUITY_FLOOR_USD=0     COOLDOWN_AFTER_KILL_SEC=3600
FEES: mexc taker 0.02 maker 0.00 · blofin taker 0.06 maker 0.02   (pct per leg)
BUDGET: mexc 20/2s orders, 20/2s cancels, reserve 4 · blofin 30/10s, reserve 6
SIM_LATENCY_MS=150  SIM_TAKER_SLIP_BPS=2  MAKER_TOP_LEVEL_FRAC=0.5
LEGACY_HEARTBEAT_PATH=/app/data/heartbeat_live   (live start refused if < 120 s old)
```

## Rollout

1. Paper mode on the Mac and/or Tokyo (no account access) — 48 h soak, review funnel, latencies, maker fills.
2. Live on Tokyo with the legacy bot stopped (`systemctl stop realtrader`), `TM_ENTRY_ENABLED=false` first (TT only, proves feeds/events/persistence with real money), then enable TM entries, then TM exits.
3. Dashboard: `DATA_DIR=/app/data_bbo` for `dashboard.py`.

## Success criteria

- Measured on Tokyo live: p50 signal→hedged < 500 ms, p95 < 1,000 ms; no polling on the entry path.
- TM trades show `fee_liquidity=maker` on the resting leg and realized fees at the maker rate; a TM/TM round trip on MEXC↔BloFin costs ≤ 0.08% versus 0.16% TT/TT.
- Opportunities with `edge_tt ≥ MIN_EDGE_PCT` are still taken TT within the latency budget.
- No naked leg persists beyond `MAX_NAKED_MS` without a hedge or flatten in flight; reconciliation finds zero unexplained positions.
- The whole pipeline runs in paper mode with zero code differences above the venue interface.

## Out of scope (v1)

L2 depth or trade streams, additional venues, spot, WebSocket order placement (neither venue supports it), dashboard code changes, maker/maker (both legs resting), funding-rate arbitrage, the paper lab's history-dependent gates, migrating or reading the legacy bot's state.
