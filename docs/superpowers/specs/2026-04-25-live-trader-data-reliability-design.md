# Live Trader Data Reliability — Phase 0 Design Spec

## Context

The live trader (`real_trader.py`) is running real money on MEXC, OKX, BloFin, and Bybit. Recurring production bugs — orphan positions, asymmetric leg fills, audit-log fill prices wrong, total-trades counts diverging from reality, exchange/bot-state drift — share a common root cause: the bot's data is not trustworthy. State lives in three places (in-memory dataclasses, flat files, exchange APIs) that disagree, no schema enforces shape at boundaries, no continuous reconciliation catches drift, and the dashboard derives numbers via a parallel computation path.

This spec is **Phase 0** of a larger roadmap to make the trading bot self-improving. Until the bot's data is reliable, every higher-level loop (runtime auto-correct, offline auto-debug agents, autonomous code deployment) would be improving against a distorted mirror. Phase 0 builds the observation layer; later phases automate decisions on top of it.

Scope: **live trader only**. Paper trader migration is deferred.

## Goals

- Single, schema-validated, transactional source of truth for live-trader state.
- Continuous reconciliation between the bot's state and exchange ground truth.
- Self-consistency invariants checked every cycle, with violations surfaced as structured events.
- Replay tests that lock in known production bugs as fixtures, preventing silent regression.
- Safe migration and rollout with shadow-mode validation and a clear backout path.

## Non-Goals (Phase 0)

- **Acting on detected mismatches.** Phase 0 only writes events and alerts. Auto-correction (orphan close, phantom drop, size adjust) is Phase 1.
- **Halting trading on invariant violations.** Phase 1.
- **Migrating the paper trader.** Deferred.
- **Offline agent reading the new store and proposing PRs.** Phase 2.
- **Autonomous code deployment.** Phase 3, gated on Phase 0+1 producing trustworthy signals.
- **Always-on production recording.** On-demand only for now.
- **Schema migration framework.** Ship v1 only; add migrations when needed.
- **WebSocket order-book message validation.** Read-only feed, lower priority.

## Architecture

A new data-reliability layer wraps the existing `real_trader.py`. Five new modules, two existing files modified, one persistent SQLite store.

```
┌────────────────────────────────────────────────────────────┐
│                       real_trader.py                       │
│  (writes through schemas, no longer owns persistent state) │
└──────────┬─────────────────────────────────────────┬───────┘
           │                                         │
           ▼                                         ▼
   ┌──────────────┐                          ┌──────────────┐
   │  schemas.py  │  pydantic models for     │ state_store  │
   │  validation  │  every boundary crossing │   (SQLite)   │
   └──────────────┘                          └──────┬───────┘
                                                     │
       ┌─────────────────────┬─────────────────────┬─┴────────────────┐
       ▼                     ▼                     ▼                  ▼
┌─────────────┐      ┌──────────────┐     ┌──────────────┐    ┌──────────────┐
│ reconciler  │      │ invariants   │     │ dashboard.py │    │ alerts       │
│  (per-trade │      │  (rules over │     │  (reads only │    │  (Telegram,  │
│  + 5min)    │ →    │  state_store)│ →   │  state_store)│    │   severity)  │
└──────┬──────┘      └──────────────┘     └──────────────┘    └──────────────┘
       │
       ▼
  exchange APIs
   (truth source)
```

### Components

- **`state_store`** — SQLite-backed canonical state. Single source of truth for positions, fills, audit log, balances, exchange health, and reconciliation events. Both `real_trader` and `dashboard` read/write through it. All writes serialized through one async writer to avoid SQLite write contention.
- **`schemas.py`** — pydantic v2 models. Every value crossing a layer (exchange response → bot, bot → state_store, state_store → dashboard) is parsed and validated. Rejects malformed data at the boundary instead of silently storing it wrong.
- **`reconciler`** — pulls exchange truth (positions, fills, balances), diffs against `state_store`, writes a `ReconciliationEvent` row for any mismatch. Two triggers: per-trade (after every order placement) and a 5-minute periodic sweep.
- **`invariants`** — pure functions that assert self-consistency rules over `state_store`. Run continuously (every poll cycle, ~1s); violations write `reconciliation_events` rows with `source='invariants'`.
- **`alerts`** — Telegram notifier with severity levels (info / warn / error / critical) wired off reconciliation events and invariant violations.

### What the bot stops doing

- Holding positions/fills/audit log in flat files (`state.json`, `trade_history.csv`).
- Letting the dashboard recompute totals from raw files.
- Trusting exchange responses without parsing.

## Data Model (SQLite Schema)

Six tables. Timestamps stored as INTEGER epoch milliseconds. Money/sizes stored as REAL USD. Foreign keys enforced.

```sql
-- Positions: one row per spread arbitrage trade (covers both legs)
CREATE TABLE positions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT    NOT NULL,
    exchange_a      TEXT    NOT NULL,
    exchange_b      TEXT    NOT NULL,
    side_a          TEXT    NOT NULL CHECK (side_a IN ('buy','sell')),
    side_b          TEXT    NOT NULL CHECK (side_b IN ('buy','sell')),
    size_usd_a      REAL    NOT NULL,
    size_usd_b      REAL    NOT NULL,
    entry_spread_pct REAL   NOT NULL,
    exit_spread_pct REAL,
    status          TEXT    NOT NULL CHECK (status IN
                       ('opening','open','closing','closed','degraded','failed')),
    opened_at       INTEGER NOT NULL,
    closed_at       INTEGER,
    realized_pnl_usd REAL,
    UNIQUE (symbol, exchange_a, exchange_b, opened_at)
);
CREATE INDEX idx_positions_status ON positions(status);
CREATE INDEX idx_positions_symbol ON positions(symbol);

-- Fills: one row per actual exchange execution. Linked to position.
CREATE TABLE fills (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    position_id     INTEGER NOT NULL REFERENCES positions(id),
    exchange        TEXT    NOT NULL,
    leg             TEXT    NOT NULL CHECK (leg IN ('a','b')),
    intent          TEXT    NOT NULL CHECK (intent IN ('entry','exit')),
    order_id        TEXT    NOT NULL,
    side            TEXT    NOT NULL CHECK (side IN ('buy','sell')),
    size_usd        REAL    NOT NULL CHECK (size_usd > 0),
    fill_price      REAL    NOT NULL CHECK (fill_price > 0),
    fees_usd        REAL    NOT NULL,
    filled_at       INTEGER NOT NULL,
    raw_response    TEXT    NOT NULL
);
CREATE INDEX idx_fills_position ON fills(position_id);
CREATE UNIQUE INDEX idx_fills_exchange_order ON fills(exchange, order_id);

-- Audit log: every event the bot wants to record (not just fills)
CREATE TABLE audit_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp       INTEGER NOT NULL,
    event_type      TEXT    NOT NULL,
    severity        TEXT    NOT NULL CHECK (severity IN ('info','warn','error','critical')),
    position_id     INTEGER REFERENCES positions(id),
    exchange        TEXT,
    symbol          TEXT,
    message         TEXT    NOT NULL,
    details         TEXT
);
CREATE INDEX idx_audit_ts ON audit_log(timestamp);
CREATE INDEX idx_audit_position ON audit_log(position_id);

-- Balances: snapshot of per-exchange USDT balances after every reconcile
CREATE TABLE balances (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    exchange        TEXT    NOT NULL,
    asset           TEXT    NOT NULL,
    available_usd   REAL    NOT NULL,
    locked_usd      REAL    NOT NULL,
    snapshot_at     INTEGER NOT NULL
);
CREATE INDEX idx_balances_exchange_ts ON balances(exchange, snapshot_at);

-- Exchange health: rolling status per exchange
CREATE TABLE exchange_health (
    exchange        TEXT    PRIMARY KEY,
    status          TEXT    NOT NULL CHECK (status IN ('ok','degraded','down')),
    last_ok_at      INTEGER,
    last_error_at   INTEGER,
    last_error_msg  TEXT,
    consecutive_errors INTEGER NOT NULL DEFAULT 0
);

-- Reconciliation events: every mismatch found by reconciler or invariants
CREATE TABLE reconciliation_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp       INTEGER NOT NULL,
    source          TEXT    NOT NULL CHECK (source IN ('reconciler','invariants')),
    category        TEXT    NOT NULL,
    severity        TEXT    NOT NULL CHECK (severity IN ('info','warn','error','critical')),
    exchange        TEXT,
    symbol          TEXT,
    position_id     INTEGER REFERENCES positions(id),
    expected        TEXT,
    actual          TEXT,
    resolution      TEXT NOT NULL DEFAULT 'unresolved'
                       CHECK (resolution IN ('unresolved','manual','auto','stale')),
    notes           TEXT
);
CREATE INDEX idx_recon_ts ON reconciliation_events(timestamp);
CREATE INDEX idx_recon_unresolved ON reconciliation_events(resolution) WHERE resolution='unresolved';
```

### Schema notes

- `positions` covers both legs in one row — a "trade" in this strategy is a spread, not a single fill.
- `fills` is the granular ledger; reconciliation requires every claimed leg to resolve to a real fill row with a real `order_id`.
- `raw_response` on fills preserves the exchange's full JSON so the original exchange truth is always recoverable.
- `reconciliation_events.resolution` defaults to `'unresolved'`. Phase 0 only ever transitions to `'manual'` (human acts) or `'stale'` (no longer relevant). `'auto'` is reserved for Phase 1.
- The `(exchange, order_id)` unique index makes fill ingestion idempotent — re-running reconciliation never double-counts.

## Reconciliation Loop

Two triggers, one comparison engine, structured event output.

### Triggers

1. **Per-trade reconcile** — fires immediately after every order placement (entry or exit), scoped to the affected `(exchange, symbol)`. Runs as a non-blocking asyncio task so the trading loop is not gated on it. Target latency: result within ~3s of order placement.
2. **Periodic sweep** — every 5 minutes, full reconcile across all 4 exchanges. Catches drift on positions that didn't trigger a per-trade event (exchange-side liquidations, manual intervention, partial fills detected late).

### Per-exchange comparison

Single function, called from both triggers:

```
For exchange E, optional symbol filter:
  1. Pull from exchange:
       positions    = E.get_open_positions()
       fills_recent = E.get_recent_fills(since=last_known_ts)
       balance      = E.get_balance()
  2. Pull from state_store:
       sp_positions = state_store.open_positions(exchange=E, symbol?)
       sp_fills     = state_store.fills(exchange=E, since=last_known_ts)
       sp_balance   = state_store.latest_balance(exchange=E)
  3. Validate exchange responses through schemas.
     Any parse failure → critical event, skip exchange this round.
  4. Diff:
       a. Positions on exchange not in state_store     → category='phantom_position'
       b. Positions in state_store not on exchange     → category='orphan_leg'
                                                          or 'closed_externally'
       c. Size mismatch                                → category='size_mismatch'
       d. Fills on exchange not in state_store         → ingest if linkable;
                                                          else 'unlinked_fill'
       e. Balance drift > $0.01                        → category='balance_drift'
  5. Each diff → reconciliation_events row with severity, expected (JSON), actual (JSON).
  6. Update exchange_health (ok / degraded / down).
  7. Snapshot balance to balances table.
```

### Severity rules

| Category | Severity | Rationale |
|---|---|---|
| `exchange_unreachable` | error first occurrence; critical after 3 consecutive | Bot is flying blind on that exchange |
| `phantom_position` | error | Real open position the bot doesn't know about |
| `orphan_leg` | error | One leg missing — directional exposure |
| `size_mismatch` | warn | Likely partial fill not yet ingested |
| `unlinked_fill` | warn | Fill we can't tie to a position — investigate |
| `balance_drift` | info if absolute drift ≤ $1 AND ≤ 1% of balance; warn otherwise | Material vs rounding |
| `unparseable_response` | critical | Exchange returned data we can't model |

### Concurrency model

- Per-trade reconciles run concurrently across symbols (asyncio tasks).
- Only one periodic sweep at a time per exchange (lock).
- All writes to `state_store` go through a single async writer.

### Failure handling

- Reconciler exception ≠ trader crash. Caught, logged to `audit_log`, written as `reconciliation_event` (severity=error), exchange marked degraded.
- 3 consecutive reconciler failures for an exchange → severity=critical → Telegram → exchange marked `down` → trader's existing exchange-health logic skips it for new trades.

## Invariants (Self-Consistency Rules)

Pure functions over `state_store`. Run end-of-cycle (~1s cadence) since they're cheap SQL queries. Violations write `reconciliation_events` rows with `source='invariants'`. Each `(category, position_id)` is rate-limited to one event per 60s window to avoid spam.

| # | Invariant | Severity |
|---|---|---|
| 1 | Every `open`/`opening` position has exactly 2 entry fills | error |
| 2 | Every `closed` position has exactly 2 entry fills AND 2 exit fills | error |
| 3 | Every fill has `fill_price > 0` AND `size_usd > 0` | critical |
| 4 | No two `open` positions with same `(symbol, exchange_a, side_a)` or `(symbol, exchange_b, side_b)` | error |
| 5 | No `open` position older than `max_hold_minutes + 5` (35 min, where `max_hold_minutes=30` per the live trader EU spec) | warn |
| 6 | No `opening` or `closing` position older than 60s | error |
| 7 | Every `audit_log` entry with non-null `position_id` references existing position | warn |
| 8 | Every fill's `position_id` references existing position | error |
| 9 | `count(positions WHERE status IN ('opening','open','closing','degraded'))` matches in-memory tracker count | error |
| 10 | Per-exchange: `sum(open size_usd) ≤ available_usd + locked_usd` from latest balance | warn |
| 11 | No `unresolved` reconciliation_event older than 30 min | warn |
| 12 | Every exchange marked `ok` has `last_ok_at` within last 5 min | error |

### Why this set

Each invariant maps to a real bug or class of failure already observed:
- #1, #4 → asymmetric legs (the leg-symmetry problem).
- #3 → the fill-price-zero / wrong-fill-price audit log bug.
- #5, #6 → stuck/orphan positions like MEGAUSDT.
- #9 → trade-count miscount (in-memory vs persisted).
- #10 → catches sub-$1 phantom legs and oversized exposure.
- #11 → ensures reconciliation events don't get ignored.
- #12 → catches the silent "exchange thinks healthy but isn't" failure.

### Execution

- One module `invariants.py` exports `check_all() -> list[Violation]`.
- Called from the trader's main loop at end-of-cycle.
- Single read-only SQL transaction per invariant. All 12 should run in well under 100ms total at current scale.

## Schema Validation at Boundaries

Pydantic v2 models. Every value crossing a layer is parsed and validated; failures are events, not silent corruption.

### Boundaries

```
exchange API → ExchangeOrderResponse → state_store    (writes)
state_store  → PositionRecord/FillRecord → trader     (reads, in-memory ops)
state_store  → DashboardView → dashboard.py           (read-only queries)
trader       → AuditEntry → state_store               (audit writes)
```

### Core models (sketch)

```python
# schemas.py
from pydantic import BaseModel, Field, field_validator
from typing import Literal, Optional

class ExchangeOrderResponse(BaseModel):
    exchange: Literal["OKX","BYBIT","MEXC","BLOFIN"]
    success: bool
    order_id: str = Field(min_length=1)
    symbol: str
    side: Literal["buy","sell"]
    requested_size_usd: float = Field(gt=0)
    filled_size_usd: float = Field(ge=0)
    fill_price: float = Field(ge=0)
    fees_usd: float = Field(ge=0)
    timestamp_ms: int
    raw: dict

    @field_validator("fill_price")
    @classmethod
    def price_required_when_filled(cls, v, info):
        if info.data.get("filled_size_usd", 0) > 0 and v <= 0:
            raise ValueError("fill_price must be > 0 when filled_size_usd > 0")
        return v

class PositionRecord(BaseModel):
    id: int
    symbol: str
    exchange_a: str
    exchange_b: str
    side_a: Literal["buy","sell"]
    side_b: Literal["buy","sell"]
    size_usd_a: float = Field(gt=0)
    size_usd_b: float = Field(gt=0)
    status: Literal["opening","open","closing","closed","degraded","failed"]
    opened_at_ms: int
    closed_at_ms: Optional[int] = None
    realized_pnl_usd: Optional[float] = None

class FillRecord(BaseModel):
    id: int
    position_id: int
    exchange: str
    leg: Literal["a","b"]
    intent: Literal["entry","exit"]
    order_id: str
    side: Literal["buy","sell"]
    size_usd: float = Field(gt=0)
    fill_price: float = Field(gt=0)
    fees_usd: float = Field(ge=0)
    filled_at_ms: int

class AuditEntry(BaseModel):
    timestamp_ms: int
    event_type: str
    severity: Literal["info","warn","error","critical"]
    position_id: Optional[int] = None
    exchange: Optional[str] = None
    symbol: Optional[str] = None
    message: str
    details: Optional[dict] = None

class BalanceSnapshot(BaseModel):
    exchange: str
    asset: str = "USDT"
    available_usd: float = Field(ge=0)
    locked_usd: float = Field(ge=0)
    snapshot_at_ms: int

class ReconciliationEvent(BaseModel):
    timestamp_ms: int
    source: Literal["reconciler","invariants"]
    category: str
    severity: Literal["info","warn","error","critical"]
    exchange: Optional[str] = None
    symbol: Optional[str] = None
    position_id: Optional[int] = None
    expected: Optional[dict] = None
    actual: Optional[dict] = None
    notes: Optional[str] = None
```

### Per-exchange normalizers

Each `ExchangeExecutor` has a `_normalize_order_response(raw_json) -> ExchangeOrderResponse` method. Per-exchange quirks (MEXC partial-fill semantics, OKX `tgtCcy`, BloFin response shape) are translated here into one common model. Quirks are contained at the executor edge, not leaked into the trading code.

### Validation failure handling

| Where | Action on validation failure |
|---|---|
| Exchange response | `audit_log` entry severity=error + `reconciliation_event` category='unparseable_response'. Do NOT proceed to call the fill successful. Mark exchange degraded after N failures. |
| state_store read | Critical bug — schema-violating row exists. Log critical, surface immediately, do not silently coerce. |
| Trader → state_store write | Caller bug. Raise; supervisor catches, writes audit entry, alerts. |

### Dashboard implication

`dashboard.py` no longer parses CSVs or JSON files. It runs SQL queries that materialize into `PositionRecord` / `FillRecord` / `AuditEntry` instances. Same models, single computation path — eliminates the class of bug where dashboard derives a different total than the bot's own logs.

## Golden Fixtures and Replay Tests

Goal: every bug we've debugged becomes a test that fails the same way, so regressions can't re-ship silently.

### Three layers

#### 1. Capture from production

A `--record` flag on the live trader writes every exchange response (REST + WS), every state_store mutation, and every reconciliation event to a timestamped JSONL file under `recordings/YYYY-MM-DD/`. Off by default; turned on for short windows when investigating an issue or proactively building the fixture corpus.

```
recordings/2026-04-25/
  exchange_responses.jsonl   # every raw API response with timestamp + endpoint
  state_mutations.jsonl      # every write to state_store
  recon_events.jsonl         # every reconciliation_event
  meta.json                  # bot version, config, runtime metadata
```

#### 2. Curated fixtures

`tests/fixtures/` directory with hand-picked scenarios derived from recordings or hand-crafted. Each fixture is a directory:

```
tests/fixtures/
  orphan_leg_megausdt/
    scenario.md              # English: what happened, why it matters
    exchange_responses.jsonl
    expected_events.json     # which reconciliation_events should fire
    expected_invariants.json # which invariants should fail
  asymmetric_fill_blofin_silent/
  zero_fill_price_audit/
  trade_count_mismatch/
  exchange_unreachable/
```

Initial corpus (Phase 0 deliverable):

| Fixture | Source bug |
|---|---|
| `orphan_leg_megausdt` | MEGAUSDT stuck open on MEXC (P1175) |
| `asymmetric_fill_blofin_silent` | MEXC fills, BloFin "succeeds" but no leg appears (P1058, P1130) |
| `zero_fill_price_audit` | Fill recorded with `fill_price=0` but marked successful (P980) |
| `trade_count_mismatch` | Total/attempted counts diverge from positions row count (P1180, P1213) |
| `exchange_unreachable` | Exchange returns errors for 60s; health should flip to `down` |

#### 3. Replay test harness

```python
# tests/test_replay.py (sketch)

@pytest.mark.parametrize("fixture_dir", discover_fixtures())
def test_fixture_replay(fixture_dir, fresh_state_store):
    scenario = load_scenario(fixture_dir)

    # Replay exchange responses through the real reconciler + invariants,
    # using the fresh sqlite store. Trader main loop is NOT running —
    # we feed inputs directly to the data layer.
    for response in scenario.exchange_responses:
        ingest_exchange_response(fresh_state_store, response)

    actual_events = fresh_state_store.reconciliation_events()
    actual_invariant_failures = invariants.check_all(fresh_state_store)

    assert_events_match(actual_events, scenario.expected_events)
    assert_invariants_match(actual_invariant_failures, scenario.expected_invariants)
```

### Test characteristics

- Real SQLite store in a temp directory — no mocking the data layer.
- Mock only the network: `ExchangeExecutor` replaced with a `ReplayExecutor` returning scripted responses from the fixture.
- Each fixture is independent and seeded fresh; tests parallelizable.
- Snapshot diffs: when a fixture's expected events change intentionally, regenerate via a `--update-snapshots` flag.

### Coverage gates

- All Phase 0 fixtures must pass before the data-reliability layer ships.
- Pre-deploy CI gate: `pytest tests/test_replay.py` must be green.
- Adding a fixture for any new production bug becomes a hard step in the bug-fix workflow.

## Migration

The bot is running with real money. The cutover must be safe.

### Migration script

```
migrate_to_sqlite.py
  1. Read current state.json + trade_history.csv + audit logs.
  2. Parse each row through the new pydantic schemas
     (PositionRecord, FillRecord, AuditEntry).
  3. Validation pass:
       - Rows that pass schema → insert into SQLite.
       - Rows that FAIL schema → write to migration_quarantine.jsonl with reason.
  4. Print summary: N positions migrated, M fills migrated, K rows quarantined.
  5. Exit non-zero if quarantine count > 5% of total.
```

The quarantine output is itself useful — it surfaces exactly the wrong-fill-price / zero-size / orphan-record bugs already silently sitting in the existing files. Manual review of quarantine becomes a one-time data-cleanup pass.

## Rollout Sequence

1. **Build behind a flag.** New modules ship into the live trader codebase but gated by `USE_SQLITE_STATE=false`. Old file-based path remains active.
2. **Shadow mode (1–3 days).** With flag still off, turn on a "shadow writer" that mirrors every state mutation into SQLite alongside the file writes. Reconciler + invariants run against the shadow store and **emit events but do not alert**. Lets us see the event stream the new layer produces without changing production behavior.
3. **Migration dry-run.** Run `migrate_to_sqlite.py` against live data, review quarantine, fix any data-cleanup issues.
4. **Cutover deploy.** Stop the bot. Run real migration. Set `USE_SQLITE_STATE=true`. Restart. Bot now reads/writes SQLite; files become read-only legacy. Reconciler + invariants now alert.
5. **Watch window (24–72h).** Observe Telegram alerts, dashboard reconciliation panel, exchange divergence rate. Tune severity thresholds. Quiet false positives.
6. **Decommission file path.** Delete shadow-writer code and file-based persistence once cutover is stable.

### Backout

If something goes wrong post-cutover: stop bot, set `USE_SQLITE_STATE=false`, restart from last `state.json` snapshot (kept for the watch window). SQLite store retained as audit material.

## Observability Deliverables

- **Dashboard panel** — reconciliation event log (filterable by severity / category / exchange), invariant status board (12 rules, green/red), exchange divergence rate over time.
- **Telegram alert routing** — `info` silenced, `warn` digest hourly, `error` immediate, `critical` immediate + repeat every 5 min until acknowledged.
- **Migration quarantine report** — written to `docs/superpowers/specs/` as a one-time artifact when cutover happens.

## Phase 0 Done Criteria

- All five subsystems shipped: `state_store`, `schemas`, `reconciler`, `invariants`, `alerts`.
- All 12 invariants implemented and running every cycle.
- All 5 starter fixtures pass in the replay harness; CI gate active.
- Live trader fully cut over to SQLite; files no longer authoritative.
- Reconciliation panel live in dashboard.
- 72h watch window passed with alert volume manageable: zero critical false positives, warn-rate stable and triaged.
- Migration quarantine reviewed and any real data issues triaged.

## Roadmap Beyond Phase 0

| Phase | Scope |
|---|---|
| Phase 1 | Runtime auto-correct — orphan close, phantom drop, size adjust. Halt-on-violation policy. |
| Phase 2 | Offline agent reads `state_store` and proposes PRs (B-mode autonomy). |
| Phase 3 | Autonomous code deployment with hard guardrails (capital cap, file allowlist, auto-rollback on PnL deviation). |
| Later | Paper trader migration to SQLite. Always-on capture. Schema migration framework. WS message validation. |
