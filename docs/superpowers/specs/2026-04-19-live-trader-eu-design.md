# Live Trader EU — Design Spec

## Overview

A live convergence arbitrage trading bot (`real_trader.py`) deployed on AWS Lightsail Singapore ($5/mo), executing the EU Main strategy with real orders on OKX, MEXC, Bybit, and BloFin via raw API integration.

Starting equity: $200 ($50 per exchange). Scaling path: $200 → $400 → $1,000 → $10,000.

## Architecture

Single `real_trader.py` file, same monolithic async style as `paper_trader.py`. Separate service, separate deployment. No shared runtime with the paper trader.

```
┌──────────────────────────────────────────────────────┐
│                   real_trader.py                      │
│                                                      │
│  ┌────────────┐  ┌────────────┐  ┌───────────────┐  │
│  │ Price Feeds │  │  Strategy  │  │   Exchange    │  │
│  │ (WS + REST) │→│  Engine    │→│   Executor    │  │
│  │ (read-only) │  │ (EU Main)  │  │ (authenticated│  │
│  └────────────┘  └────────────┘  │  order API)   │  │
│                                   └───────┬───────┘  │
│  ┌────────────┐  ┌────────────┐          │          │
│  │ Risk Mgr   │  │ Position   │←─────────┘          │
│  │ (kill switch│  │ Tracker    │                      │
│  │  drawdown)  │  │ (reconcile)│                      │
│  └────────────┘  └────────────┘                      │
│                                                      │
│  ┌────────────┐  ┌────────────┐                      │
│  │ Telegram   │  │ Dashboard  │                      │
│  │ (alerts)   │  │ (Flask)    │                      │
│  └────────────┘  └────────────┘                      │
└──────────────────────────────────────────────────────┘
```

Components:
- **Price Feeds:** Reused from paper trader — websocket + REST batch fetchers for all 4 exchanges. Read-only, no auth needed.
- **Strategy Engine:** EU Main logic — spread detection, entry filters (momentum, MA, volume, breakout guard, OB depth, blacklist), exit rules. Identical to paper trader.
- **Exchange Executor:** New component. Authenticated order placement via raw aiohttp + HMAC-SHA256 signing. One class per exchange.
- **Risk Manager:** Pre-trade checks + continuous monitoring. Kill switch, drawdown, position limits.
- **Position Tracker:** Tracks open positions with real exchange order IDs. Reconciles with actual exchange state every 5 minutes.
- **Telegram:** Trade alerts, periodic summaries, emergency notifications, command listener.
- **Dashboard:** Flask server with equity curve, positions, order audit log, exchange health, paper-vs-live comparison.

## Exchange Executor

### Interface

```python
class ExchangeExecutor:
    async def place_market_order(symbol, side, size_usd) -> OrderResult
    async def get_order_status(order_id) -> OrderStatus
    async def cancel_order(order_id) -> bool
    async def get_balance() -> dict
    async def get_open_positions() -> list
    async def close_position(symbol, side, size_usd) -> OrderResult
```

Four implementations: `OKXExecutor`, `BybitExecutor`, `MEXCExecutor`, `BloFinExecutor`.

### Authentication

| Exchange | Signature | Extra |
|---|---|---|
| OKX | HMAC-SHA256 of timestamp+method+path+body, base64 encoded | Requires passphrase header |
| Bybit | HMAC-SHA256 of timestamp+api_key+recv_window+params | recv_window=5000 |
| MEXC | HMAC-SHA256 of query string | Timestamp in query params |
| BloFin | HMAC-SHA256 similar to OKX | Requires passphrase |

### OrderResult

```python
@dataclass
class OrderResult:
    success: bool
    order_id: str
    exchange: str
    symbol: str
    side: str              # "buy" or "sell"
    size_usd: float        # requested
    filled_usd: float      # actual
    fill_price: float      # average fill price
    fees_usd: float        # actual fees charged
    timestamp: float
    error: str = ""        # if success=False
```

### API Keys

Stored as environment variables:
```
OKX_API_KEY, OKX_API_SECRET, OKX_PASSPHRASE
BYBIT_API_KEY, BYBIT_API_SECRET
MEXC_API_KEY, MEXC_API_SECRET
BLOFIN_API_KEY, BLOFIN_API_SECRET, BLOFIN_PASSPHRASE
```

Keys must have trade permission but NOT withdrawal permission.

## Trade Execution Flow

### Entry

1. Strategy detects spread above threshold + all filters pass.
2. Pre-trade checks: risk manager, balance, OB depth.
3. Execute both legs simultaneously via `asyncio.gather()`.
4. Evaluate results:
   - **Both filled:** Create LivePosition, Telegram notification.
   - **One filled, one failed:** Immediately close the filled leg via retry escalation. Telegram alert.
   - **One filled, one partial:** Close excess from larger leg to match sizes. Proceed with matched size.
   - **Both failed:** Log and move on.

### Exit

1. Spread converged below 0.15% OR 30-minute timeout.
2. Execute both closing legs simultaneously.
3. Evaluate results:
   - **Both filled:** Calculate real P&L, Telegram notification.
   - **One leg failed:** Enter retry escalation (see below).
   - **Partial close:** Retry remainder.

### Exit Failure Escalation (Fully Autonomous)

```
Retry 1-3:   market order at 1s, 2s, 5s intervals
             │
             ├─ Check WHY via API:
             │   Exchange errors → mark DOWN
             │   Insufficient margin → reduce size, retry
             │   Network timeout → retry
             │
Retry 4-6:   market orders at 10s intervals
             │
After 60s:   try limit order at 2% worse than market
             │
After 2min:  accept single-leg exposure temporarily
             │   Mark position as "degraded"
             │   Halt new trades if degraded > 50% equity
             │   Retry every 30s until exchange responds
             │   Telegram: "🚨 DEGRADED — retrying"
             │
Exchange recovers → auto-close immediately
             │   Telegram: "✅ RECOVERED"
```

The bot never gives up and never requires manual intervention.

## Risk Management

### Pre-Trade Checks (all must pass)

1. **Drawdown gate:** equity < starting_equity × 0.90 → HALT
2. **Position count:** open_positions >= 3 → skip
3. **Per-trade size:** capped at $25 per leg
4. **Balance check:** exchange USDT < $25 → skip that exchange
5. **Exchange health:** last successful API call > 30s → mark DOWN
6. **Degraded check:** stuck single-leg positions → halt if > 50% equity
7. **Concentration:** max 80% of balance per exchange
8. **Aged positions:** max 30% of equity in positions > 10 min old

### Kill Switch (10% Drawdown)

Starting equity $200, kills at $180. When triggered:
1. Cancel all pending orders.
2. Close all open positions (via retry escalation).
3. 1-hour cooldown — no new trades.
4. After cooldown: resume only if equity > $180.
5. If equity dropped further during close-out: extend cooldown.
6. Telegram: full report.

### Scaling Thresholds

| Equity | Max Trade | Max Positions | Kill Switch |
|---|---|---|---|
| $200 | $25 | 3 | $180 |
| $400 | $50 | 4 | $360 |
| $1,000 | $100 | 6 | $900 |
| $10,000 | $500 | 10 | $9,000 |

Manually updated at each scaling step after reviewing performance.

### Continuous Monitoring

- **Balance reconciliation:** every 5 min, compare bot state to actual exchange positions. Mismatch > 5% → alert + pause.
- **Heartbeat:** write timestamp to file every 60s. Separate cron checks staleness, alerts via Telegram if > 3 min.

## Strategy Configuration

### From EU Main (unchanged)

```
entry_spread_pct:        0.90%
exit_spread_pct:         0.15%
momentum_filter:         True
ma_filter:               True
pair_preference:          True
min_volume_usd:          50,000
max_hold_minutes:        30
defi_spread_premium:     N/A (DeFi disabled)
```

### Adjusted for Live

```
allowed_exchanges:       [OKX, MEXC, Bybit, BloFin]
max_position_usd:        25
position_size_pct:       0.125
max_positions:           3
starting_capital:        200
```

### Removed (Paper-Only Simulation)

```
leg_fail_chance           # real legs either fail or don't
partial_fill_chance       # exchange reports actual fill
flash_miss_rate           # real execution is real
competition_decay         # real OB depth IS the competition
exec_delay_drift          # real latency is measured
ob_staleness_penalty      # real prices are real
```

### Added (Live-Only)

```
order_timeout_seconds:   5
balance_refresh_seconds: 30
reconcile_interval:      300
cooldown_after_kill:     3600
max_retries_close:       unlimited
heartbeat_interval:      60
DRY_RUN:                 True     # flip to False when ready
```

## Strategy Features Carried Over from Paper Trader

- Breakout guard (block entry when 70%+ of recent ticks widen)
- Symbol blacklisting (tiered loss thresholds, 6-hour cooldown)
- Delisted symbol protection (hardcoded block list)
- Stale price detection (ignore quotes > 30s old)
- OB depth sizing (walk L2 orderbook, cap to available liquidity)
- Consumed liquidity tracking (decay-based recovery)
- Dynamic exit baseline (per-pair rolling spread average)
- Relative spread detection (trade deviations from baseline)
- Exchange concentration limit (scaled to 80% of per-exchange balance)
- Aged position capital reservation (max 30% in positions > 10 min)
- Strategy advisor reports (12-hour digest)
- OB empty book cache (skip known-empty books for N cycles)
- Rate limit manager (track per-exchange API budgets, auto-throttle)

## Telegram Integration

### Trade Notifications

```
🟢 OPEN #1 ETHUSDT
  SHORT OKX @ $3,026.80 ($25.00)
  LONG  Bybit @ $3,000.30 ($25.00)
  Spread: 0.88% | Fees est: $0.10
  Open positions: 1/3 | Equity: $200.00

🔴 CLOSE #1 ETHUSDT (+$0.11, +0.43%)
  Duration: 8m 22s | Exit spread: 0.07%
  Actual fees: $0.09
  Equity: $200.11 | Drawdown: 0.00%
```

### Periodic Summaries

- Every 30 min: status (equity, open positions, P&L since last)
- Every 12 hours: strategy advisor report
- Daily at midnight: full report with paper-vs-live comparison

### Alert Levels

- ℹ️ INFO: dry-run order, exchange recovered
- ⚠️ WARNING: exchange degraded, drawdown approaching threshold
- 🚨 EMERGENCY: kill switch, stuck leg, balance mismatch, bot offline

### Telegram Commands

- `/status` — equity, open positions, drawdown
- `/stop` — halt trading, close all positions
- `/start` — resume after manual stop
- `/dryrun` — toggle dry-run mode
- `/balance` — actual balances on all 4 exchanges

### Heartbeat Watchdog

Separate cron script (independent of bot process):
- Checks heartbeat file every 2 minutes
- Alerts via Telegram if file > 3 minutes old
- Catches crashes and stuck processes

## Dashboard

Flask server on same Lightsail instance, port 8080. HTTP basic auth for security.

### Panels

- **Equity curve** — lightweight-charts, same as paper trader
- **Open positions** — live spread, unrealized P&L, duration, exchange
- **Closed positions** — actual fill prices, fees, P&L per trade
- **Exchange balances** — available USDT per exchange + health status indicator
- **Order audit log** — every order placed with exchange order ID, fill details, latency ms
- **Reconciliation status** — last check time, match/mismatch state
- **Paper vs live comparison** — side-by-side divergences in fills, timing, missed trades
- **Config panel** — current parameters
- **Kill switch controls** — manual stop/start, DRY_RUN toggle, drawdown bar with threshold

## State Persistence

### Files

```
/app/data/
  real_state.json          # portfolio: equity, cash, trade history
  real_positions.json      # open positions with exchange order IDs
  real_balance_cache.json  # last known exchange balances
  heartbeat                # timestamp for watchdog
  real_trader.log          # rotating log
```

Positions saved every cycle. Full state every 30 seconds.

### Startup Sequence

1. Load state from disk (or initialize fresh).
2. Load API keys from environment.
3. Test auth on all 4 exchanges (read-only balance query).
   - Any exchange fails → disable it, continue with remaining.
   - ALL fail → abort, Telegram alert.
4. Reconcile loaded positions with actual exchange positions.
   - Orphaned on exchange → close them.
   - In state but not on exchange → remove from state.
5. Start websocket price feeds.
6. If DRY_RUN=True: log prominently.
7. Enter main loop.
8. Telegram: "🟢 Real Trader online"

## Deployment

### Infrastructure

```
AWS Lightsail Singapore ($5/mo)
Ubuntu 22.04, 1 vCPU, 1GB RAM, 2TB transfer

/app/
  real_trader.py
  dashboard.py
  start.sh
  requirements.txt        # aiohttp, flask
  .env                    # API keys (chmod 600)
  data/                   # persistent state

systemd service           # auto-restart on crash
crontab                   # heartbeat watchdog
```

### start.sh

Same pattern as paper trader: run bot + dashboard in parallel, if either dies kill the other and let systemd restart.

## Out of Scope for V1

- Withdrawal/transfer automation between exchanges
- Funding payment tracking
- Multiple strategy instances (only EU Main)
- DeFi exchanges (Hyperliquid, dYdX, Vertex)
- Limit orders (market orders only for $25 sizes)
- Dashboard charts beyond equity curve
