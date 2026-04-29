# Self-Healing Shadow Paper Trader — Design Spec

**Date:** 2026-04-29
**Status:** Design approved, ready for implementation plan
**Source documents:**
- `Bouwgids voor een Cross-Exchange Spread Convergence Bot` (PDF, 28 Apr 2026) — foundation
- `Self-Healing en Zelfverbeterende Spread Convergence Bot` (PDF, 29 Apr 2026) — heal layer

## Goal

Build a runnable greenfield paper-trading bot that implements the convergence-arbitrage strategy from the two source PDFs end-to-end, with a thin self-healing layer baked in from day one. The bot is a "shadow" in the sense that it observes live public market data and simulates trades — it never places real orders. It is independent of the live `real_trader.py` and the Railway paper trader.

The MVP must:
1. Connect to live L2 orderbook WebSockets on five exchanges (Binance, Bybit, Bitget, Gate.io, MEXC) using native per-exchange WS clients.
2. Compute a rolling Z-score on the cross-exchange spread for `BTC/USDT:USDT` perp on every pair (10 combinations).
3. Emit fee-adjusted entry/exit signals.
4. Simulate concurrent two-leg execution with idempotent client order IDs.
5. Drive a per-trade state machine with explicit allowed transitions.
6. Persist everything (signals, fills, aborts, state transitions, errors) to a SQLite journal.
7. Run five self-heal mechanisms continuously: heartbeat monitor, economic health check, reconciliation saga, rolling Hurst canary, atomic state writes.

Out of scope for MVP: Bayesian optimisation / WFO, LLM-assisted journal review, HMM regime detection, spoofing/flash-crash anomaly detection, API latency monitor, dashboard UI, multi-symbol trading, hot-reload config, real order placement.

## Project location

```
/Users/vandenboogaard/Claude projects/Claude Paperclip/self-healing-shadow/
```

A new sibling directory next to the existing `crypto-scanner/`, `dashboard-vercel/`, and `prod-snapshot/`. Branch: `master`. No worktree (single-session thin slice).

## File layout

```
self-healing-shadow/
├── paper_trader.py           # entry point: CLI, asyncio event loop, wires everything
├── config.yaml               # exchanges, symbols, Z-score params, fees, thresholds
├── requirements.txt
├── README.md                 # how to run, what it does
├── core/
│   ├── __init__.py
│   ├── data_feed.py          # native WS clients (one per exchange), in-memory L2 books
│   ├── spread_engine.py      # mid-prices, log-spread, rolling SMA/StdDev, Z-score
│   ├── signal_engine.py      # entry/exit signal + fee-adjusted profitability gate
│   ├── execution_sim.py      # simulated fills, idempotent clientOrderId, fee/slip model
│   ├── state_machine.py      # PositionState enum, ALLOWED_TRANSITIONS, Position class
│   └── trade_journal.py      # SQLite sink (incl. ABORTED/REJECTED entries)
├── heal/
│   ├── __init__.py
│   ├── heartbeat.py          # WS staleness → exchange DEGRADED
│   ├── health_check.py       # 4-metric economic probe every 5 min
│   ├── reconciliation.py     # 5-step saga (Detect → Verify → Rollback → Diagnose → Heal)
│   ├── hurst_canary.py       # rolling Hurst exponent per spread, suppress when H>0.5
│   └── atomic_writes.py      # write_state / read_state with .tmp + os.replace
├── data/                     # SQLite + state json (gitignored)
│   └── .gitkeep
└── tests/
    ├── __init__.py
    ├── test_spread_engine.py
    ├── test_state_machine.py
    ├── test_execution_sim.py
    ├── test_trade_journal.py
    ├── test_heartbeat.py
    ├── test_health_check.py
    ├── test_reconciliation.py
    ├── test_hurst_canary.py
    └── test_atomic_writes.py
```

Module size targets: 50–250 LOC each. Total project: ~1500–1800 LOC of source + ~600–800 LOC of tests. Dependencies point downward only (`paper_trader.py` → `core/*` → `heal/*`); nothing imports upward.

## Data flow

One async event loop with the following concurrent tasks (all `asyncio.create_task` from `paper_trader.py:main`):

1. **Per-exchange WS feeders** (5 tasks). Each calls into `core/data_feed.py:WSClient.run()` with the exchange's L2 endpoint. On every message: stamps `last_ws_update[exchange] = utcnow()`, refreshes `book[exchange][symbol]`, pushes `(exchange, symbol)` onto an `asyncio.Queue`.
2. **Spread evaluator** (1 task). Consumes the queue. For each tick, computes the mid-price for the affected exchange and recomputes the rolling Z-score against every other exchange that has a fresh book for the same symbol. Emits `EntrySignal` and `ExitSignal` events.
3. **Heartbeat monitor** (1 task, 1 Hz). Scans `last_ws_update`. Marks exchange `DEGRADED` if `age > 10s`; restores to `HEALTHY` on next message. Used as a hard gate by `SignalEngine`.
4. **Health check** (1 task, every 5 min). Runs the four economic probes. Returns a `HealthStatus` with one of: `CONTINUE`, `REOPTIMIZE_PARAMETERS`, `FORCE_EXIT_OLDEST`, `RECONCILE_IMMEDIATELY`, `ENTER_SAFE_MODE`. The main loop dispatches on the action.
5. **Position manager** (1 task per open position). Drives the per-trade FSM, watches Z-score for exit/stop-loss, calls reconciliation saga on simulated leg-fail.
6. **Journal flusher** (1 task, 1 Hz). Drains the journal write queue inside a single SQLite transaction (`BEGIN ... COMMIT`) per the burst-fill atomicity guidance in PDF2 §5.2.
7. **Atomic state checkpoint** (1 task, every 30 s). Calls `heal/atomic_writes.py:write_state()` to dump in-memory state to `data/state.json`.

The spread evaluator is reactive (queue-driven), not polled. There is no `POLL_INTERVAL` constant.

## State machine

### Per-trade FSM (`core/state_machine.py`)

States (10): `SCANNING`, `SIGNAL_DETECTED`, `PROFITABILITY_CHECK`, `EXECUTING`, `RECONCILING`, `POSITION_OPEN`, `MONITORING`, `CLOSING`, `EMERGENCY_EXIT`, `ROLLBACK`. Plus a sentinel `SAFE_MODE` at the bot level (not a position state — it pauses new entries).

Transitions (the canonical happy path plus error branches):

| From                    | To                              | Trigger                                  |
|-------------------------|---------------------------------|------------------------------------------|
| `SCANNING`              | `SIGNAL_DETECTED`               | `\|Z\| ≥ entry_z`                          |
| `SIGNAL_DETECTED`       | `PROFITABILITY_CHECK`           | always (immediate)                       |
| `PROFITABILITY_CHECK`   | `SCANNING`                      | net profit < `min_net_profit_usd`        |
| `PROFITABILITY_CHECK`   | `EXECUTING`                     | net profit ≥ threshold                   |
| `EXECUTING`             | `RECONCILING`                   | always (after `asyncio.gather` returns)  |
| `RECONCILING`           | `POSITION_OPEN`                 | both legs filled                         |
| `RECONCILING`           | `ROLLBACK`                      | one leg failed                           |
| `POSITION_OPEN`         | `MONITORING`                    | always (immediate)                       |
| `MONITORING`            | `CLOSING`                       | `\|Z\| ≤ exit_z`                           |
| `MONITORING`            | `EMERGENCY_EXIT`                | `\|Z\| ≥ stop_loss_z`                      |
| `CLOSING`               | `SCANNING`                      | both close legs filled                   |
| `EMERGENCY_EXIT`        | `SCANNING`                      | both close legs filled                   |
| `ROLLBACK`              | `SCANNING`                      | rollback simulated, bot enters SAFE_MODE |

Implementation: `ALLOWED_TRANSITIONS: dict[PositionState, set[PositionState]]` is a module-level constant. `Position.transition(new_state, reason)` validates against it and raises `InvalidTransition` on violation. Every transition stamps `state_history.append((old, new, ts, reason))` and writes a `STATE_TRANSITION` journal entry.

### Exchange health states (`heal/heartbeat.py` + `heal/reconciliation.py`)

`HEALTHY` ↔ `DEGRADED` (auto, stale WS) ↔ `QUARANTINED` (auto, ≥ 5 consecutive simulated leg-failures attributed to that exchange; exponential backoff `wait = min(base * 2^n + uniform(0, base), max_wait)` before retry).

## The five heal modules

### 1. `heal/heartbeat.py`

```python
class HeartbeatMonitor:
    last_update: dict[str, datetime]
    exchange_status: dict[str, Literal["HEALTHY", "DEGRADED"]]

    def on_ws_message(exchange: str) -> None
    async def monitor_loop() -> None              # 1 Hz; flips DEGRADED on age > stale_threshold_sec
    def is_pair_tradeable(a: str, b: str) -> bool # both HEALTHY
```

Hard gate: `SignalEngine` calls `is_pair_tradeable` before emitting any entry signal. Recovery is automatic — next message flips back to `HEALTHY` and logs `[HEALED]`.

### 2. `heal/health_check.py`

```python
@dataclass
class HealthStatus:
    is_healthy: bool
    issues: list[str]
    recommended_action: Literal[
        "CONTINUE", "REOPTIMIZE_PARAMETERS", "FORCE_EXIT_OLDEST",
        "RECONCILE_IMMEDIATELY", "ENTER_SAFE_MODE"
    ]

class TradingHealthCheck:
    async def run_check() -> HealthStatus
    async def monitor_loop() -> None              # every 300 s
```

Four metrics:
- **Trade drought** — `journal.last_successful_trade()` > 72 h → `REOPTIMIZE_PARAMETERS` (logged-only in MVP; emits `WANT_REOPTIMIZE` event for future Bayesian opt module).
- **Error rate** — `journal.count_errors(since=1h)` > 20 → `ENTER_SAFE_MODE`.
- **Capital depletion** — any exchange's simulated free balance < `max_position_usd` → `FORCE_EXIT_OLDEST`.
- **Orphaned positions** — any `Position` stuck in `RECONCILING` for > 60 s → `RECONCILE_IMMEDIATELY`.

Action dispatch table is in `paper_trader.py:on_health_status`.

### 3. `heal/reconciliation.py`

```python
class ReconciliationSaga:
    async def run(position: Position,
                  leg_a: SimOrder | Exception,
                  leg_b: SimOrder | Exception) -> SagaResult
```

Five steps, all journaled with `event_type="SAGA_STEP"`:
1. **Detect** — caller passes the two results.
2. **Verify** — `ExecutionSim.fetch_order(client_order_id)` with 3 s timeout; idempotent. Prevents double-rollback when a "failure" was actually just a lost response.
3. **Rollback** — if confirmed unfilled, simulate market close on the filled leg using current best bid/ask + slippage.
4. **Diagnose** — classify the exception: `InsufficientBalance`, `RateLimitExceeded`, `NetworkError`, `Unknown`.
5. **Heal** — apply policy:
   - `RateLimitExceeded` → halve internal rate-limiter for 15 min.
   - `InsufficientBalance` → recompute balance, halve `max_position_usd` for that exchange.
   - `NetworkError` → increment exchange's `consecutive_failures`; mark `DEGRADED` immediately; if count ≥ 5 then `QUARANTINED` with exponential backoff.
   - `Unknown` → log, mark `DEGRADED`.

Saga always ends by setting bot-level `SAFE_MODE = True` until manually cleared via a config flag or CLI command.

### 4. `heal/hurst_canary.py`

```python
class HurstCanary:
    windows: dict[tuple[str, str], collections.deque]   # 200-period per pair

    def update(pair: tuple[str, str], spread_value: float) -> None
    def is_pair_safe(pair: tuple[str, str]) -> bool     # H < 0.5
```

Pure function `compute_hurst(series: list[float]) -> float` (R/S analysis or `hurst` library's `compute_Hc`). Called by `SignalEngine` after the heartbeat gate. If `H ≥ 0.5` for a pair, suppress new entries on that pair (existing positions continue to monitor for exit normally).

### 5. `heal/atomic_writes.py`

```python
def write_state(path: Path, payload: dict) -> None        # .tmp + os.replace
def read_state(path: Path) -> dict                        # default {} on missing/corrupt
```

POSIX-atomic on the same filesystem. `read_state` never raises — it returns `{}` and logs an error so startup survives corruption.

## Simulated execution

`core/execution_sim.py` (~250 LOC):

```python
@dataclass
class SimOrder:
    client_order_id: str          # UUID4, mandatory
    exchange: str
    symbol: str
    side: Literal["buy", "sell"]
    size_usd: float
    state: Literal["pending", "filled", "rejected", "rolled_back"]
    filled_price: float | None
    fee_paid: float | None
    slippage_bps: float | None
    created_at: datetime
    filled_at: datetime | None

class ExecutionSim:
    simulated_balance: dict[str, float]                       # per-exchange paper USDT
    ledger: dict[str, SimOrder]                               # by client_order_id

    async def create_order(exchange, symbol, side, size_usd,
                           client_order_id) -> SimOrder       # raises on simulated failure
    async def fetch_order(client_order_id) -> SimOrder | None
    def starting_balances_from_config(cfg) -> dict[str, float]
```

`create_order` flow:
1. Roll uniform `[0,1)` against `sim_leg_failure_rate` (default 0.0). On hit → raise `Exception("simulated_failure: NetworkError")` to exercise the saga in tests.
2. Walk the live order book to compute volume-weighted fill price (PDF1 §6.4).
3. If insufficient depth → raise `InsufficientLiquidity`.
4. Apply taker fee from per-exchange config.
5. Decrement `simulated_balance[exchange]` by `size_usd + fee`.
6. Record `SimOrder` keyed by `client_order_id` in `ledger`.
7. Return filled order.

Fills are instant (no latency simulation in MVP).

## Trade journal

`core/trade_journal.py` (~200 LOC). Single SQLite table:

```sql
CREATE TABLE journal (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    ts            TEXT NOT NULL,
    event_type    TEXT NOT NULL,       -- ENTRY_SIGNAL | ENTRY_ABORTED | FILL | EXIT
                                       -- | STATE_TRANSITION | EXCHANGE_DEGRADED
                                       -- | HEALTH_CHECK | SAGA_STEP | ERROR
    pair          TEXT,                -- e.g. "MEXC_BINANCE"
    symbol        TEXT,
    payload_json  TEXT NOT NULL,
    z_score       REAL,
    spread_bps    REAL,
    expected_pnl_usd REAL
);
CREATE INDEX idx_ts ON journal(ts);
CREATE INDEX idx_event ON journal(event_type);
```

Public API: `log(event_type, payload, **kwargs)` is non-blocking (queue push). The 1 Hz flusher task drains the queue inside one transaction. Helper queries: `last_successful_trade()`, `count_errors(since=...)`, `count_aborts(reason=...)`.

Aborted entries are journaled with `event_type="ENTRY_ABORTED"` and a reason field — required by the design (PDF2 §3.1 explicitly says non-taken trades must be logged).

## Configuration (`config.yaml`)

```yaml
exchanges:
  binance:  { taker_fee_bps: 5,   maker_fee_bps: 2,   starting_balance_usd: 1000 }
  bybit:    { taker_fee_bps: 6,   maker_fee_bps: 1,   starting_balance_usd: 1000 }
  bitget:   { taker_fee_bps: 6,   maker_fee_bps: 2,   starting_balance_usd: 1000 }
  gateio:   { taker_fee_bps: 5,   maker_fee_bps: 1.5, starting_balance_usd: 1000 }
  mexc:     { taker_fee_bps: 2,   maker_fee_bps: 0,   starting_balance_usd: 1000 }

symbols: ["BTC/USDT:USDT"]

strategy:
  lookback_window: 200
  entry_z: 2.0
  exit_z: 0.5
  stop_loss_z: 4.0
  min_net_profit_usd: 5.0
  max_position_usd: 100
  max_concurrent_positions: 5

heal:
  ws_stale_threshold_sec: 10
  health_check_interval_sec: 300
  hurst_window: 200
  hurst_threshold: 0.5
  sim_leg_failure_rate: 0.0           # raise in tests to exercise saga
  saga_quarantine_after_failures: 5
  backoff_base_sec: 1
  backoff_max_sec: 60

paths:
  data_dir: "./data"
  journal_db: "./data/journal.sqlite"
  state_file: "./data/state.json"
```

Loaded once at startup. Hot-reload is out of MVP scope.

## Data feed (native WS, no ccxt.pro)

`core/data_feed.py` exposes one `WSClient` abstract base and five concrete implementations. Each subscribes to L2 orderbook depth on the exchange's documented public futures WS endpoint:

| Exchange | Endpoint                                      | Subscription topic        |
|----------|-----------------------------------------------|---------------------------|
| Binance  | `wss://fstream.binance.com/stream`            | `btcusdt@depth20@100ms`   |
| Bybit    | `wss://stream.bybit.com/v5/public/linear`     | `orderbook.50.BTCUSDT`    |
| Bitget   | `wss://ws.bitget.com/v2/ws/public`            | `books`, `BTCUSDT`        |
| Gate.io  | `wss://fx-ws.gateio.ws/v4/ws/usdt`            | `futures.order_book`      |
| MEXC     | `wss://contract.mexc.com/ws`                  | `sub.depth`, `BTC_USDT`   |

Each client:
- Maintains a websocket with auto-reconnect (exponential backoff via `heal/reconciliation.py` policy).
- Normalises incoming books into `OrderBook(bids: list[(price, size)], asks: list[(price, size)], ts: datetime)`.
- Calls `HeartbeatMonitor.on_ws_message(exchange_name)` on every message.
- Pushes `(exchange, symbol)` to the spread-evaluator queue.

~50 LOC per exchange + ~50 LOC for the base class.

## Testing

`pytest` + `pytest-asyncio`. TDD discipline: every module written test-first per `superpowers:test-driven-development`.

Test order (matches build order):
1. `test_spread_engine.py` — synthetic price series → known Z-score values; log-spread monotonicity.
2. `test_state_machine.py` — every valid transition succeeds; every invalid transition raises `InvalidTransition`.
3. `test_execution_sim.py` — fee math; book-walk fill price; simulated failure injection; idempotent `fetch_order`.
4. `test_trade_journal.py` — schema, helper queries, transaction batching.
5. `test_heartbeat.py` — staleness threshold, healing, `is_pair_tradeable` correctness.
6. `test_atomic_writes.py` — write/read round-trip; corrupt-file recovery returns `{}`.
7. `test_hurst_canary.py` — known mean-reverting series → H<0.5; random walk → H≈0.5.
8. `test_health_check.py` — fake journal rows trip each metric → assert correct `recommended_action`.
9. `test_reconciliation.py` — inject failure, assert all 5 saga steps execute in order, assert correct heal action per diagnosis class. `freezegun` for deterministic timestamps.

Coverage target: ≥ 80% on `core/` and `heal/`. The async event loop and live WS integration are **not** covered by unit tests — verified by manual smoke run only.

## Dependencies (`requirements.txt`)

```
websockets>=12.0
pyyaml>=6.0
numpy>=1.26
pandas>=2.1
hurst>=0.0.5
pytest>=8.0
pytest-asyncio>=0.23
freezegun>=1.4
```

No `ccxt`, no `ccxt.pro`. Pure native WS via `websockets`.

## How to run

```bash
cd self-healing-shadow
pip install -r requirements.txt
python paper_trader.py --config config.yaml
# Tail the journal:
sqlite3 data/journal.sqlite "select ts, event_type, pair, payload_json from journal order by id desc limit 20;"
```

## Out of scope (deferred)

- Bayesian Optimisation / Walk-Forward Optimisation (Optuna)
- LLM-assisted weekly review of the journal
- HMM regime detection
- Spoofing / flash-crash / thin-orderbook anomaly detection
- API latency monitor
- Dashboard / web UI
- Multi-symbol trading (config supports it; main loop assumes one symbol for the slice)
- Hot-reload config
- Real order placement on any exchange
- Graceful shutdown handler that closes simulated positions on SIGTERM (PDF2 checklist #7) — added in next iteration

Each of these can be added later without redesigning the core because the journal schema, state machine, and heal interfaces leave room for them.

## Implementation order

1. `requirements.txt`, `config.yaml`, `paper_trader.py` skeleton.
2. `core/spread_engine.py` (TDD).
3. `core/state_machine.py` (TDD).
4. `core/execution_sim.py` (TDD).
5. `core/trade_journal.py` (TDD).
6. `heal/atomic_writes.py` (TDD).
7. `heal/heartbeat.py` (TDD).
8. `heal/hurst_canary.py` (TDD).
9. `heal/health_check.py` (TDD).
10. `heal/reconciliation.py` (TDD).
11. `core/data_feed.py` — native WS clients, no unit tests; manual smoke verification.
12. `core/signal_engine.py` — wires Z-score + heartbeat gate + Hurst gate + profitability gate. Light unit tests on the gating logic.
13. `paper_trader.py` — wires all tasks, dispatches health actions, runs the loop.
14. Manual smoke run for ~10 min, observe journal.
