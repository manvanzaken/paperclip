# Live Trader Data Reliability — Plan 1 of 3 (Foundation)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a SQLite-backed canonical state store and pydantic schema layer for the live trader, plus a migration tool that brings existing file-based state into the new store with a quarantine pass for invalid rows.

**Architecture:** One SQLite database file holds positions, fills, audit log, balances, exchange health, and reconciliation events. Pydantic v2 models validate every value at every boundary. Migration script reads existing `state.json` + `trade_history.csv` + audit logs, runs each row through schema validation, and inserts passing rows or quarantines failures.

**Tech Stack:** Python 3.11+, SQLite (stdlib `sqlite3`), pydantic v2, pytest.

**Working directory:** `.claude/worktrees/tender-germain/deploy-live/` (the live trader worktree). All file paths in this plan are relative to that directory unless absolute.

**Spec:** `docs/superpowers/specs/2026-04-25-live-trader-data-reliability-design.md`

**Out of scope (deferred to Plans 2 and 3):** Reconciler, invariants, alerts, replay-test harness, golden fixtures, shadow-mode wiring, dashboard updates, feature-flagged cutover.

---

## File Structure

| File | Responsibility |
|---|---|
| `state_store.py` | Schema DDL embedded as constant; connection helpers (WAL, FK pragma); CRUD for all 6 tables. Single module to match existing flat-file style of the trading bot. |
| `schemas.py` | Pydantic v2 models: `ExchangeOrderResponse`, `PositionRecord`, `FillRecord`, `AuditEntry`, `BalanceSnapshot`, `ExchangeHealthRecord`, `ReconciliationEvent`. |
| `migrate_to_sqlite.py` | One-time migration: read existing files, validate via schemas, insert valid rows, write invalid rows to `migration_quarantine.jsonl` with reason. |
| `tests/test_state_store.py` | Unit tests for connection + every CRUD operation, against a temp-file SQLite DB. |
| `tests/test_schemas.py` | Unit tests for every pydantic model — happy path, edge cases, validator failures. |
| `tests/test_migration.py` | Tests for migration: clean input, mixed input, malformed input → quarantine, threshold-fail. |
| `tests/conftest.py` | Shared pytest fixtures: `fresh_db` (temp SQLite path with schema applied). |

**Note on file size:** the existing trading bot uses single large files (e.g. `real_trader.py` ~6000 lines). Matching that style, `state_store.py` and `schemas.py` are intentionally one file each rather than packages. If `state_store.py` exceeds ~800 lines during execution, the executor may split per-table CRUD into modules — that judgment call is theirs.

---

## Task Sequence

Each task follows TDD: write the failing test, run it red, write minimal implementation, run it green, commit. Tasks are ordered so that each builds on the prior. Tasks 1–2 set up the foundation; tasks 3–9 add CRUD per table; tasks 10–13 cover migration.

The remainder of this plan is structured as follows:

- **Tasks 1–2** — Schema + connection (the bedrock).
- **Tasks 3–9** — Pydantic models and CRUD per table, paired so each model lands with its table.
- **Tasks 10–13** — Migration script.

I will write tasks in the order above. Each task is fully self-contained: file paths, exact test code, exact implementation code, exact commands.

---

## Task 1: SQLite schema and connection

**Files:**
- Create: `state_store.py`
- Create: `tests/test_state_store.py`
- Create: `tests/conftest.py`

- [ ] **Step 1.1: Write the failing test for connection + schema bootstrap**

`tests/conftest.py`:
```python
import os
import tempfile
import pytest
from state_store import open_db, init_schema

@pytest.fixture
def fresh_db():
    """Yield a path to a fresh SQLite DB with the schema applied."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    init_schema(path)
    yield path
    os.remove(path)
```

`tests/test_state_store.py`:
```python
import sqlite3
from state_store import open_db, init_schema

def test_init_schema_creates_all_tables(fresh_db):
    conn = open_db(fresh_db)
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    tables = {row[0] for row in cur}
    assert tables >= {
        "positions", "fills", "audit_log",
        "balances", "exchange_health", "reconciliation_events",
    }
    conn.close()

def test_open_db_enables_foreign_keys(fresh_db):
    conn = open_db(fresh_db)
    cur = conn.execute("PRAGMA foreign_keys")
    assert cur.fetchone()[0] == 1
    conn.close()

def test_open_db_uses_wal_mode(fresh_db):
    conn = open_db(fresh_db)
    cur = conn.execute("PRAGMA journal_mode")
    assert cur.fetchone()[0].lower() == "wal"
    conn.close()
```

- [ ] **Step 1.2: Run the test and confirm failure**

Run: `pytest tests/test_state_store.py -v`
Expected: ImportError or ModuleNotFoundError for `state_store`.

- [ ] **Step 1.3: Implement `state_store.py` with schema DDL and connection helpers**

`state_store.py`:
```python
"""Canonical state store for the live trader.

SQLite-backed. Single source of truth for positions, fills, audit log,
balances, exchange health, and reconciliation events.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS positions (
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
CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status);
CREATE INDEX IF NOT EXISTS idx_positions_symbol ON positions(symbol);

CREATE TABLE IF NOT EXISTS fills (
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
CREATE INDEX IF NOT EXISTS idx_fills_position ON fills(position_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_fills_exchange_order ON fills(exchange, order_id);

CREATE TABLE IF NOT EXISTS audit_log (
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
CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit_log(timestamp);
CREATE INDEX IF NOT EXISTS idx_audit_position ON audit_log(position_id);

CREATE TABLE IF NOT EXISTS balances (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    exchange        TEXT    NOT NULL,
    asset           TEXT    NOT NULL,
    available_usd   REAL    NOT NULL,
    locked_usd      REAL    NOT NULL,
    snapshot_at     INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_balances_exchange_ts ON balances(exchange, snapshot_at);

CREATE TABLE IF NOT EXISTS exchange_health (
    exchange        TEXT    PRIMARY KEY,
    status          TEXT    NOT NULL CHECK (status IN ('ok','degraded','down')),
    last_ok_at      INTEGER,
    last_error_at   INTEGER,
    last_error_msg  TEXT,
    consecutive_errors INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS reconciliation_events (
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
CREATE INDEX IF NOT EXISTS idx_recon_ts ON reconciliation_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_recon_unresolved
    ON reconciliation_events(resolution) WHERE resolution='unresolved';
"""


def open_db(path: str | Path) -> sqlite3.Connection:
    """Open a SQLite connection with WAL + foreign keys enabled."""
    conn = sqlite3.connect(str(path), isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.row_factory = sqlite3.Row
    return conn


def init_schema(path: str | Path) -> None:
    """Create the database file (if missing) and apply the schema."""
    conn = open_db(path)
    try:
        conn.executescript(SCHEMA_DDL)
    finally:
        conn.close()
```

- [ ] **Step 1.4: Run tests and confirm green**

Run: `pytest tests/test_state_store.py -v`
Expected: 3 passed.

- [ ] **Step 1.5: Commit**

```bash
git add state_store.py tests/test_state_store.py tests/conftest.py
git commit -m "feat(state_store): add SQLite schema and connection helpers"
```

---

## Task 2: Pydantic schemas — core models

**Files:**
- Create: `schemas.py`
- Create: `tests/test_schemas.py`

- [ ] **Step 2.1: Write failing tests for `PositionRecord` and `FillRecord`**

`tests/test_schemas.py`:
```python
import pytest
from pydantic import ValidationError
from schemas import PositionRecord, FillRecord


def test_position_record_happy_path():
    p = PositionRecord(
        id=1, symbol="ORDIUSDT",
        exchange_a="MEXC", exchange_b="BLOFIN",
        side_a="buy", side_b="sell",
        size_usd_a=25.0, size_usd_b=25.0,
        status="open", opened_at_ms=1700000000000,
    )
    assert p.symbol == "ORDIUSDT"
    assert p.closed_at_ms is None


def test_position_record_rejects_zero_size():
    with pytest.raises(ValidationError):
        PositionRecord(
            id=1, symbol="X",
            exchange_a="A", exchange_b="B",
            side_a="buy", side_b="sell",
            size_usd_a=0, size_usd_b=25.0,
            status="open", opened_at_ms=1,
        )


def test_position_record_rejects_invalid_status():
    with pytest.raises(ValidationError):
        PositionRecord(
            id=1, symbol="X",
            exchange_a="A", exchange_b="B",
            side_a="buy", side_b="sell",
            size_usd_a=25.0, size_usd_b=25.0,
            status="bogus", opened_at_ms=1,
        )


def test_fill_record_happy_path():
    f = FillRecord(
        id=1, position_id=1, exchange="MEXC",
        leg="a", intent="entry", order_id="abc123",
        side="buy", size_usd=25.0, fill_price=1.234,
        fees_usd=0.01, filled_at_ms=1700000000000,
    )
    assert f.order_id == "abc123"


def test_fill_record_rejects_zero_fill_price():
    with pytest.raises(ValidationError):
        FillRecord(
            id=1, position_id=1, exchange="MEXC",
            leg="a", intent="entry", order_id="abc",
            side="buy", size_usd=25.0, fill_price=0,
            fees_usd=0.01, filled_at_ms=1,
        )


def test_fill_record_rejects_zero_size():
    with pytest.raises(ValidationError):
        FillRecord(
            id=1, position_id=1, exchange="MEXC",
            leg="a", intent="entry", order_id="abc",
            side="buy", size_usd=0, fill_price=1.0,
            fees_usd=0.01, filled_at_ms=1,
        )
```

- [ ] **Step 2.2: Run and confirm failure**

Run: `pytest tests/test_schemas.py -v`
Expected: ImportError for `schemas`.

- [ ] **Step 2.3: Implement core models**

`schemas.py`:
```python
"""Pydantic v2 models for every value crossing a layer boundary."""
from __future__ import annotations

from typing import Literal, Optional
from pydantic import BaseModel, Field, field_validator


class PositionRecord(BaseModel):
    id: int
    symbol: str
    exchange_a: str
    exchange_b: str
    side_a: Literal["buy", "sell"]
    side_b: Literal["buy", "sell"]
    size_usd_a: float = Field(gt=0)
    size_usd_b: float = Field(gt=0)
    entry_spread_pct: float = 0.0
    exit_spread_pct: Optional[float] = None
    status: Literal["opening", "open", "closing", "closed", "degraded", "failed"]
    opened_at_ms: int
    closed_at_ms: Optional[int] = None
    realized_pnl_usd: Optional[float] = None


class FillRecord(BaseModel):
    id: int
    position_id: int
    exchange: str
    leg: Literal["a", "b"]
    intent: Literal["entry", "exit"]
    order_id: str = Field(min_length=1)
    side: Literal["buy", "sell"]
    size_usd: float = Field(gt=0)
    fill_price: float = Field(gt=0)
    fees_usd: float = Field(ge=0)
    filled_at_ms: int
```

- [ ] **Step 2.4: Run tests and confirm green**

Run: `pytest tests/test_schemas.py -v`
Expected: 6 passed.

- [ ] **Step 2.5: Commit**

```bash
git add schemas.py tests/test_schemas.py
git commit -m "feat(schemas): add PositionRecord and FillRecord pydantic models"
```

---

## Task 3: Pydantic schemas — remaining models

**Files:**
- Modify: `schemas.py`
- Modify: `tests/test_schemas.py`

- [ ] **Step 3.1: Append failing tests for remaining models**

Append to `tests/test_schemas.py`:
```python
from schemas import (
    AuditEntry, BalanceSnapshot, ExchangeHealthRecord,
    ReconciliationEvent, ExchangeOrderResponse,
)


def test_audit_entry_happy_path():
    a = AuditEntry(
        timestamp_ms=1, event_type="entry_attempt",
        severity="info", message="ok",
    )
    assert a.severity == "info"
    assert a.position_id is None


def test_audit_entry_rejects_invalid_severity():
    with pytest.raises(ValidationError):
        AuditEntry(timestamp_ms=1, event_type="x", severity="oops", message="m")


def test_balance_snapshot_rejects_negative():
    with pytest.raises(ValidationError):
        BalanceSnapshot(
            exchange="MEXC", asset="USDT",
            available_usd=-1.0, locked_usd=0.0, snapshot_at_ms=1,
        )


def test_exchange_health_record_happy_path():
    h = ExchangeHealthRecord(
        exchange="MEXC", status="ok", consecutive_errors=0,
    )
    assert h.last_ok_at_ms is None


def test_reconciliation_event_happy_path():
    e = ReconciliationEvent(
        timestamp_ms=1, source="reconciler",
        category="orphan_leg", severity="error",
    )
    assert e.expected is None


def test_exchange_order_response_requires_price_when_filled():
    with pytest.raises(ValidationError):
        ExchangeOrderResponse(
            exchange="MEXC", success=True, order_id="x",
            symbol="ORDIUSDT", side="buy",
            requested_size_usd=25.0, filled_size_usd=25.0,
            fill_price=0.0, fees_usd=0.01,
            timestamp_ms=1, raw={},
        )


def test_exchange_order_response_zero_price_ok_when_unfilled():
    r = ExchangeOrderResponse(
        exchange="MEXC", success=False, order_id="x",
        symbol="ORDIUSDT", side="buy",
        requested_size_usd=25.0, filled_size_usd=0.0,
        fill_price=0.0, fees_usd=0.0,
        timestamp_ms=1, raw={"err": "rejected"},
    )
    assert r.success is False
```

- [ ] **Step 3.2: Run and confirm failure**

Run: `pytest tests/test_schemas.py -v`
Expected: 7 new failures (ImportError for new symbols).

- [ ] **Step 3.3: Append models to `schemas.py`**

Append to `schemas.py`:
```python
class AuditEntry(BaseModel):
    timestamp_ms: int
    event_type: str
    severity: Literal["info", "warn", "error", "critical"]
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


class ExchangeHealthRecord(BaseModel):
    exchange: str
    status: Literal["ok", "degraded", "down"]
    last_ok_at_ms: Optional[int] = None
    last_error_at_ms: Optional[int] = None
    last_error_msg: Optional[str] = None
    consecutive_errors: int = Field(ge=0, default=0)


class ReconciliationEvent(BaseModel):
    timestamp_ms: int
    source: Literal["reconciler", "invariants"]
    category: str
    severity: Literal["info", "warn", "error", "critical"]
    exchange: Optional[str] = None
    symbol: Optional[str] = None
    position_id: Optional[int] = None
    expected: Optional[dict] = None
    actual: Optional[dict] = None
    notes: Optional[str] = None
    resolution: Literal["unresolved", "manual", "auto", "stale"] = "unresolved"


class ExchangeOrderResponse(BaseModel):
    exchange: Literal["OKX", "BYBIT", "MEXC", "BLOFIN"]
    success: bool
    order_id: str = Field(min_length=1)
    symbol: str
    side: Literal["buy", "sell"]
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
```

- [ ] **Step 3.4: Run tests and confirm green**

Run: `pytest tests/test_schemas.py -v`
Expected: 13 passed.

- [ ] **Step 3.5: Commit**

```bash
git add schemas.py tests/test_schemas.py
git commit -m "feat(schemas): add audit, balance, health, recon, and order-response models"
```

---

## Task 4: state_store CRUD — positions

**Files:**
- Modify: `state_store.py`
- Modify: `tests/test_state_store.py`

- [ ] **Step 4.1: Write failing tests for `insert_position`, `get_position`, `update_position_status`, `close_position`, `list_open_positions`**

Append to `tests/test_state_store.py`:
```python
from schemas import PositionRecord
from state_store import (
    insert_position, get_position, update_position_status,
    close_position, list_open_positions,
)


def _sample_position(**overrides):
    base = dict(
        symbol="ORDIUSDT", exchange_a="MEXC", exchange_b="BLOFIN",
        side_a="buy", side_b="sell",
        size_usd_a=25.0, size_usd_b=25.0,
        entry_spread_pct=0.012,
        status="opening", opened_at_ms=1700000000000,
    )
    base.update(overrides)
    return base


def test_insert_position_returns_id_and_round_trips(fresh_db):
    conn = open_db(fresh_db)
    pid = insert_position(conn, **_sample_position())
    assert pid > 0
    rec = get_position(conn, pid)
    assert isinstance(rec, PositionRecord)
    assert rec.symbol == "ORDIUSDT"
    assert rec.status == "opening"
    conn.close()


def test_update_position_status(fresh_db):
    conn = open_db(fresh_db)
    pid = insert_position(conn, **_sample_position())
    update_position_status(conn, pid, "open")
    assert get_position(conn, pid).status == "open"
    conn.close()


def test_close_position_records_exit(fresh_db):
    conn = open_db(fresh_db)
    pid = insert_position(conn, **_sample_position(status="open"))
    close_position(conn, pid, closed_at_ms=1700000060000,
                   exit_spread_pct=0.0014, realized_pnl_usd=0.42)
    rec = get_position(conn, pid)
    assert rec.status == "closed"
    assert rec.closed_at_ms == 1700000060000
    assert rec.realized_pnl_usd == 0.42
    conn.close()


def test_list_open_positions_filters_by_status(fresh_db):
    conn = open_db(fresh_db)
    pid_open = insert_position(conn, **_sample_position(
        symbol="A", opened_at_ms=1, status="open"))
    insert_position(conn, **_sample_position(
        symbol="B", opened_at_ms=2, status="closed"))
    insert_position(conn, **_sample_position(
        symbol="C", opened_at_ms=3, status="opening"))
    open_ids = {p.id for p in list_open_positions(conn)}
    assert pid_open in open_ids
    assert len(open_ids) == 2  # 'open' and 'opening' both count
    conn.close()


def test_get_position_returns_none_for_missing(fresh_db):
    conn = open_db(fresh_db)
    assert get_position(conn, 999) is None
    conn.close()
```

- [ ] **Step 4.2: Run and confirm failure**

Run: `pytest tests/test_state_store.py -v -k position`
Expected: ImportError for new symbols.

- [ ] **Step 4.3: Implement positions CRUD**

Append to `state_store.py`:
```python
from schemas import PositionRecord


_OPEN_STATUSES = ("opening", "open", "closing", "degraded")


def insert_position(
    conn: sqlite3.Connection, *,
    symbol: str, exchange_a: str, exchange_b: str,
    side_a: str, side_b: str,
    size_usd_a: float, size_usd_b: float,
    entry_spread_pct: float,
    status: str, opened_at_ms: int,
) -> int:
    cur = conn.execute(
        """INSERT INTO positions
           (symbol, exchange_a, exchange_b, side_a, side_b,
            size_usd_a, size_usd_b, entry_spread_pct, status, opened_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (symbol, exchange_a, exchange_b, side_a, side_b,
         size_usd_a, size_usd_b, entry_spread_pct, status, opened_at_ms),
    )
    return cur.lastrowid


def get_position(conn: sqlite3.Connection, position_id: int) -> Optional[PositionRecord]:
    row = conn.execute(
        "SELECT * FROM positions WHERE id=?", (position_id,)
    ).fetchone()
    if row is None:
        return None
    return _row_to_position(row)


def update_position_status(
    conn: sqlite3.Connection, position_id: int, status: str
) -> None:
    conn.execute(
        "UPDATE positions SET status=? WHERE id=?", (status, position_id)
    )


def close_position(
    conn: sqlite3.Connection, position_id: int, *,
    closed_at_ms: int, exit_spread_pct: float, realized_pnl_usd: float,
) -> None:
    conn.execute(
        """UPDATE positions
           SET status='closed', closed_at=?, exit_spread_pct=?, realized_pnl_usd=?
           WHERE id=?""",
        (closed_at_ms, exit_spread_pct, realized_pnl_usd, position_id),
    )


def list_open_positions(conn: sqlite3.Connection) -> list[PositionRecord]:
    placeholders = ",".join("?" * len(_OPEN_STATUSES))
    rows = conn.execute(
        f"SELECT * FROM positions WHERE status IN ({placeholders}) ORDER BY id",
        _OPEN_STATUSES,
    ).fetchall()
    return [_row_to_position(r) for r in rows]


def _row_to_position(row: sqlite3.Row) -> PositionRecord:
    return PositionRecord(
        id=row["id"], symbol=row["symbol"],
        exchange_a=row["exchange_a"], exchange_b=row["exchange_b"],
        side_a=row["side_a"], side_b=row["side_b"],
        size_usd_a=row["size_usd_a"], size_usd_b=row["size_usd_b"],
        entry_spread_pct=row["entry_spread_pct"],
        exit_spread_pct=row["exit_spread_pct"],
        status=row["status"], opened_at_ms=row["opened_at"],
        closed_at_ms=row["closed_at"],
        realized_pnl_usd=row["realized_pnl_usd"],
    )
```

Add at top of `state_store.py` near other imports:
```python
from typing import Optional
```

- [ ] **Step 4.4: Run tests and confirm green**

Run: `pytest tests/test_state_store.py -v`
Expected: all green.

- [ ] **Step 4.5: Commit**

```bash
git add state_store.py tests/test_state_store.py
git commit -m "feat(state_store): add positions CRUD"
```

---

## Task 5: state_store CRUD — fills

**Files:**
- Modify: `state_store.py`
- Modify: `tests/test_state_store.py`

- [ ] **Step 5.1: Write failing tests for `insert_fill`, `list_fills_for_position`, `list_recent_fills`, idempotency on `(exchange, order_id)`**

Append to `tests/test_state_store.py`:
```python
from state_store import insert_fill, list_fills_for_position, list_recent_fills


def _sample_fill(position_id, **overrides):
    base = dict(
        position_id=position_id, exchange="MEXC", leg="a", intent="entry",
        order_id="ord-1", side="buy",
        size_usd=25.0, fill_price=1.234, fees_usd=0.01,
        filled_at_ms=1700000000500, raw_response='{"x":1}',
    )
    base.update(overrides)
    return base


def test_insert_fill_links_to_position(fresh_db):
    conn = open_db(fresh_db)
    pid = insert_position(conn, **_sample_position(status="open"))
    fid = insert_fill(conn, **_sample_fill(pid))
    assert fid > 0
    fills = list_fills_for_position(conn, pid)
    assert len(fills) == 1
    assert fills[0].order_id == "ord-1"
    conn.close()


def test_insert_fill_rejects_duplicate_exchange_order(fresh_db):
    conn = open_db(fresh_db)
    pid = insert_position(conn, **_sample_position(status="open"))
    insert_fill(conn, **_sample_fill(pid))
    with pytest.raises(sqlite3.IntegrityError):
        insert_fill(conn, **_sample_fill(pid))
    conn.close()


def test_list_recent_fills_filters_by_timestamp(fresh_db):
    conn = open_db(fresh_db)
    pid = insert_position(conn, **_sample_position(status="open"))
    insert_fill(conn, **_sample_fill(pid, order_id="o1", filled_at_ms=100))
    insert_fill(conn, **_sample_fill(pid, order_id="o2", filled_at_ms=200))
    insert_fill(conn, **_sample_fill(pid, order_id="o3", filled_at_ms=300))
    recent = list_recent_fills(conn, exchange="MEXC", since_ms=150)
    assert {f.order_id for f in recent} == {"o2", "o3"}
    conn.close()
```

(Note: `import sqlite3, pytest` at the top of the test file; add if not present.)

- [ ] **Step 5.2: Run and confirm failure**

Run: `pytest tests/test_state_store.py -v -k fill`
Expected: ImportError for `insert_fill` etc.

- [ ] **Step 5.3: Implement fills CRUD**

Append to `state_store.py`:
```python
from schemas import FillRecord


def insert_fill(
    conn: sqlite3.Connection, *,
    position_id: int, exchange: str, leg: str, intent: str,
    order_id: str, side: str,
    size_usd: float, fill_price: float, fees_usd: float,
    filled_at_ms: int, raw_response: str,
) -> int:
    cur = conn.execute(
        """INSERT INTO fills
           (position_id, exchange, leg, intent, order_id, side,
            size_usd, fill_price, fees_usd, filled_at, raw_response)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (position_id, exchange, leg, intent, order_id, side,
         size_usd, fill_price, fees_usd, filled_at_ms, raw_response),
    )
    return cur.lastrowid


def list_fills_for_position(
    conn: sqlite3.Connection, position_id: int
) -> list[FillRecord]:
    rows = conn.execute(
        "SELECT * FROM fills WHERE position_id=? ORDER BY filled_at",
        (position_id,),
    ).fetchall()
    return [_row_to_fill(r) for r in rows]


def list_recent_fills(
    conn: sqlite3.Connection, *, exchange: str, since_ms: int
) -> list[FillRecord]:
    rows = conn.execute(
        "SELECT * FROM fills WHERE exchange=? AND filled_at>=? ORDER BY filled_at",
        (exchange, since_ms),
    ).fetchall()
    return [_row_to_fill(r) for r in rows]


def _row_to_fill(row: sqlite3.Row) -> FillRecord:
    return FillRecord(
        id=row["id"], position_id=row["position_id"],
        exchange=row["exchange"], leg=row["leg"], intent=row["intent"],
        order_id=row["order_id"], side=row["side"],
        size_usd=row["size_usd"], fill_price=row["fill_price"],
        fees_usd=row["fees_usd"], filled_at_ms=row["filled_at"],
    )
```

- [ ] **Step 5.4: Run tests and confirm green**

Run: `pytest tests/test_state_store.py -v`
Expected: all green.

- [ ] **Step 5.5: Commit**

```bash
git add state_store.py tests/test_state_store.py
git commit -m "feat(state_store): add fills CRUD with idempotent (exchange, order_id)"
```

---

## Task 6: state_store CRUD — audit_log

**Files:**
- Modify: `state_store.py`
- Modify: `tests/test_state_store.py`

- [ ] **Step 6.1: Write failing tests for `write_audit`, `list_audit_for_position`, `list_audit_recent`**

Append to `tests/test_state_store.py`:
```python
from state_store import write_audit, list_audit_for_position, list_audit_recent


def test_write_audit_round_trips(fresh_db):
    conn = open_db(fresh_db)
    write_audit(conn, timestamp_ms=1, event_type="entry_attempt",
                severity="info", message="hi")
    rows = list_audit_recent(conn, since_ms=0, limit=10)
    assert len(rows) == 1
    assert rows[0].message == "hi"
    conn.close()


def test_audit_filters_by_position(fresh_db):
    conn = open_db(fresh_db)
    pid = insert_position(conn, **_sample_position(status="open"))
    write_audit(conn, timestamp_ms=1, event_type="entry_attempt",
                severity="info", message="for-pos", position_id=pid)
    write_audit(conn, timestamp_ms=2, event_type="heartbeat",
                severity="info", message="other")
    rows = list_audit_for_position(conn, pid)
    assert len(rows) == 1
    assert rows[0].message == "for-pos"
    conn.close()


def test_audit_serializes_details_dict(fresh_db):
    conn = open_db(fresh_db)
    write_audit(conn, timestamp_ms=1, event_type="x",
                severity="warn", message="m", details={"k": "v"})
    rows = list_audit_recent(conn, since_ms=0, limit=10)
    assert rows[0].details == {"k": "v"}
    conn.close()
```

- [ ] **Step 6.2: Run and confirm failure**

Run: `pytest tests/test_state_store.py -v -k audit`
Expected: ImportError.

- [ ] **Step 6.3: Implement audit_log CRUD**

Append to `state_store.py`:
```python
import json
from schemas import AuditEntry


def write_audit(
    conn: sqlite3.Connection, *,
    timestamp_ms: int, event_type: str, severity: str, message: str,
    position_id: Optional[int] = None,
    exchange: Optional[str] = None,
    symbol: Optional[str] = None,
    details: Optional[dict] = None,
) -> int:
    cur = conn.execute(
        """INSERT INTO audit_log
           (timestamp, event_type, severity, position_id, exchange, symbol, message, details)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (timestamp_ms, event_type, severity, position_id, exchange, symbol,
         message, json.dumps(details) if details is not None else None),
    )
    return cur.lastrowid


def list_audit_for_position(
    conn: sqlite3.Connection, position_id: int
) -> list[AuditEntry]:
    rows = conn.execute(
        "SELECT * FROM audit_log WHERE position_id=? ORDER BY timestamp",
        (position_id,),
    ).fetchall()
    return [_row_to_audit(r) for r in rows]


def list_audit_recent(
    conn: sqlite3.Connection, *, since_ms: int, limit: int = 100
) -> list[AuditEntry]:
    rows = conn.execute(
        "SELECT * FROM audit_log WHERE timestamp>=? ORDER BY timestamp DESC LIMIT ?",
        (since_ms, limit),
    ).fetchall()
    return [_row_to_audit(r) for r in rows]


def _row_to_audit(row: sqlite3.Row) -> AuditEntry:
    details = json.loads(row["details"]) if row["details"] else None
    return AuditEntry(
        timestamp_ms=row["timestamp"], event_type=row["event_type"],
        severity=row["severity"], position_id=row["position_id"],
        exchange=row["exchange"], symbol=row["symbol"],
        message=row["message"], details=details,
    )
```

- [ ] **Step 6.4: Run tests and confirm green**

Run: `pytest tests/test_state_store.py -v`
Expected: all green.

- [ ] **Step 6.5: Commit**

```bash
git add state_store.py tests/test_state_store.py
git commit -m "feat(state_store): add audit_log CRUD with JSON details"
```

---

## Task 7: state_store CRUD — balances and exchange_health

**Files:**
- Modify: `state_store.py`
- Modify: `tests/test_state_store.py`

- [ ] **Step 7.1: Write failing tests**

Append to `tests/test_state_store.py`:
```python
from state_store import (
    snapshot_balance, latest_balance,
    upsert_exchange_health, get_exchange_health,
)


def test_balance_snapshot_and_latest(fresh_db):
    conn = open_db(fresh_db)
    snapshot_balance(conn, exchange="MEXC", asset="USDT",
                     available_usd=50.0, locked_usd=0.0, snapshot_at_ms=100)
    snapshot_balance(conn, exchange="MEXC", asset="USDT",
                     available_usd=49.5, locked_usd=0.5, snapshot_at_ms=200)
    snap = latest_balance(conn, exchange="MEXC")
    assert snap.snapshot_at_ms == 200
    assert snap.available_usd == 49.5
    conn.close()


def test_latest_balance_returns_none_for_missing(fresh_db):
    conn = open_db(fresh_db)
    assert latest_balance(conn, exchange="OKX") is None
    conn.close()


def test_exchange_health_upsert(fresh_db):
    conn = open_db(fresh_db)
    upsert_exchange_health(conn, exchange="MEXC", status="ok",
                           last_ok_at_ms=100, consecutive_errors=0)
    h = get_exchange_health(conn, "MEXC")
    assert h.status == "ok"
    upsert_exchange_health(conn, exchange="MEXC", status="degraded",
                           last_error_at_ms=200, last_error_msg="timeout",
                           consecutive_errors=2)
    h = get_exchange_health(conn, "MEXC")
    assert h.status == "degraded"
    assert h.consecutive_errors == 2
    conn.close()
```

- [ ] **Step 7.2: Run and confirm failure**

Run: `pytest tests/test_state_store.py -v -k "balance or health"`
Expected: ImportError.

- [ ] **Step 7.3: Implement balances and exchange_health CRUD**

Append to `state_store.py`:
```python
from schemas import BalanceSnapshot, ExchangeHealthRecord


def snapshot_balance(
    conn: sqlite3.Connection, *,
    exchange: str, asset: str,
    available_usd: float, locked_usd: float, snapshot_at_ms: int,
) -> int:
    cur = conn.execute(
        """INSERT INTO balances
           (exchange, asset, available_usd, locked_usd, snapshot_at)
           VALUES (?, ?, ?, ?, ?)""",
        (exchange, asset, available_usd, locked_usd, snapshot_at_ms),
    )
    return cur.lastrowid


def latest_balance(
    conn: sqlite3.Connection, *, exchange: str, asset: str = "USDT"
) -> Optional[BalanceSnapshot]:
    row = conn.execute(
        """SELECT * FROM balances WHERE exchange=? AND asset=?
           ORDER BY snapshot_at DESC LIMIT 1""",
        (exchange, asset),
    ).fetchone()
    if row is None:
        return None
    return BalanceSnapshot(
        exchange=row["exchange"], asset=row["asset"],
        available_usd=row["available_usd"], locked_usd=row["locked_usd"],
        snapshot_at_ms=row["snapshot_at"],
    )


def upsert_exchange_health(
    conn: sqlite3.Connection, *,
    exchange: str, status: str,
    last_ok_at_ms: Optional[int] = None,
    last_error_at_ms: Optional[int] = None,
    last_error_msg: Optional[str] = None,
    consecutive_errors: int = 0,
) -> None:
    conn.execute(
        """INSERT INTO exchange_health
           (exchange, status, last_ok_at, last_error_at, last_error_msg, consecutive_errors)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(exchange) DO UPDATE SET
             status=excluded.status,
             last_ok_at=COALESCE(excluded.last_ok_at, exchange_health.last_ok_at),
             last_error_at=COALESCE(excluded.last_error_at, exchange_health.last_error_at),
             last_error_msg=COALESCE(excluded.last_error_msg, exchange_health.last_error_msg),
             consecutive_errors=excluded.consecutive_errors""",
        (exchange, status, last_ok_at_ms, last_error_at_ms,
         last_error_msg, consecutive_errors),
    )


def get_exchange_health(
    conn: sqlite3.Connection, exchange: str
) -> Optional[ExchangeHealthRecord]:
    row = conn.execute(
        "SELECT * FROM exchange_health WHERE exchange=?", (exchange,)
    ).fetchone()
    if row is None:
        return None
    return ExchangeHealthRecord(
        exchange=row["exchange"], status=row["status"],
        last_ok_at_ms=row["last_ok_at"], last_error_at_ms=row["last_error_at"],
        last_error_msg=row["last_error_msg"],
        consecutive_errors=row["consecutive_errors"],
    )
```

- [ ] **Step 7.4: Run tests and confirm green**

Run: `pytest tests/test_state_store.py -v`
Expected: all green.

- [ ] **Step 7.5: Commit**

```bash
git add state_store.py tests/test_state_store.py
git commit -m "feat(state_store): add balances and exchange_health CRUD"
```

---

## Task 8: state_store CRUD — reconciliation_events

**Files:**
- Modify: `state_store.py`
- Modify: `tests/test_state_store.py`

- [ ] **Step 8.1: Write failing tests**

Append to `tests/test_state_store.py`:
```python
from state_store import (
    write_recon_event, list_unresolved_recon_events,
    resolve_recon_event,
)


def test_write_recon_event_round_trips(fresh_db):
    conn = open_db(fresh_db)
    eid = write_recon_event(
        conn, timestamp_ms=1, source="reconciler",
        category="orphan_leg", severity="error",
        exchange="MEXC", symbol="ORDIUSDT",
        expected={"size": 25}, actual={"size": 0},
    )
    assert eid > 0
    events = list_unresolved_recon_events(conn)
    assert len(events) == 1
    e = events[0]
    assert e.expected == {"size": 25}
    assert e.actual == {"size": 0}
    assert e.resolution == "unresolved"
    conn.close()


def test_resolve_recon_event(fresh_db):
    conn = open_db(fresh_db)
    eid = write_recon_event(
        conn, timestamp_ms=1, source="invariants",
        category="size_mismatch", severity="warn",
    )
    resolve_recon_event(conn, eid, resolution="manual", notes="closed by user")
    events = list_unresolved_recon_events(conn)
    assert len(events) == 0
    conn.close()


def test_list_unresolved_filters_by_severity(fresh_db):
    conn = open_db(fresh_db)
    write_recon_event(conn, timestamp_ms=1, source="reconciler",
                      category="balance_drift", severity="info")
    write_recon_event(conn, timestamp_ms=2, source="reconciler",
                      category="orphan_leg", severity="error")
    errs = list_unresolved_recon_events(conn, min_severity="error")
    assert len(errs) == 1
    assert errs[0].category == "orphan_leg"
    conn.close()
```

- [ ] **Step 8.2: Run and confirm failure**

Run: `pytest tests/test_state_store.py -v -k recon`
Expected: ImportError.

- [ ] **Step 8.3: Implement reconciliation_events CRUD**

Append to `state_store.py`:
```python
from schemas import ReconciliationEvent

_SEVERITY_ORDER = {"info": 0, "warn": 1, "error": 2, "critical": 3}


def write_recon_event(
    conn: sqlite3.Connection, *,
    timestamp_ms: int, source: str, category: str, severity: str,
    exchange: Optional[str] = None,
    symbol: Optional[str] = None,
    position_id: Optional[int] = None,
    expected: Optional[dict] = None,
    actual: Optional[dict] = None,
    notes: Optional[str] = None,
) -> int:
    cur = conn.execute(
        """INSERT INTO reconciliation_events
           (timestamp, source, category, severity, exchange, symbol, position_id,
            expected, actual, notes)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (timestamp_ms, source, category, severity, exchange, symbol, position_id,
         json.dumps(expected) if expected is not None else None,
         json.dumps(actual) if actual is not None else None,
         notes),
    )
    return cur.lastrowid


def list_unresolved_recon_events(
    conn: sqlite3.Connection, *, min_severity: str = "info"
) -> list[ReconciliationEvent]:
    threshold = _SEVERITY_ORDER[min_severity]
    rows = conn.execute(
        "SELECT * FROM reconciliation_events WHERE resolution='unresolved' ORDER BY timestamp"
    ).fetchall()
    out = []
    for r in rows:
        if _SEVERITY_ORDER[r["severity"]] >= threshold:
            out.append(_row_to_recon_event(r))
    return out


def resolve_recon_event(
    conn: sqlite3.Connection, event_id: int, *,
    resolution: str, notes: Optional[str] = None,
) -> None:
    conn.execute(
        "UPDATE reconciliation_events SET resolution=?, notes=COALESCE(?, notes) WHERE id=?",
        (resolution, notes, event_id),
    )


def _row_to_recon_event(row: sqlite3.Row) -> ReconciliationEvent:
    return ReconciliationEvent(
        timestamp_ms=row["timestamp"], source=row["source"],
        category=row["category"], severity=row["severity"],
        exchange=row["exchange"], symbol=row["symbol"],
        position_id=row["position_id"],
        expected=json.loads(row["expected"]) if row["expected"] else None,
        actual=json.loads(row["actual"]) if row["actual"] else None,
        notes=row["notes"], resolution=row["resolution"],
    )
```

- [ ] **Step 8.4: Run tests and confirm green**

Run: `pytest tests/test_state_store.py -v`
Expected: all green.

- [ ] **Step 8.5: Commit**

```bash
git add state_store.py tests/test_state_store.py
git commit -m "feat(state_store): add reconciliation_events CRUD"
```

---

## Task 9: state_store — async writer wrapper

The spec requires that all writes serialize through a single async writer to avoid SQLite write contention. Reads can stay synchronous.

**Files:**
- Modify: `state_store.py`
- Modify: `tests/test_state_store.py`

- [ ] **Step 9.1: Write failing test**

Append to `tests/test_state_store.py`:
```python
import asyncio
from state_store import AsyncStateStore


def test_async_writer_serializes_writes(fresh_db):
    async def _go():
        store = AsyncStateStore(fresh_db)
        await store.start()
        try:
            # Two concurrent writers, both should succeed without contention
            async def w(symbol, t):
                return await store.insert_position(
                    symbol=symbol, exchange_a="MEXC", exchange_b="BLOFIN",
                    side_a="buy", side_b="sell",
                    size_usd_a=25.0, size_usd_b=25.0,
                    entry_spread_pct=0.01, status="opening", opened_at_ms=t,
                )
            ids = await asyncio.gather(w("A", 1), w("B", 2), w("C", 3))
            assert len(set(ids)) == 3
            opens = await store.list_open_positions()
            assert {p.symbol for p in opens} == {"A", "B", "C"}
        finally:
            await store.stop()

    asyncio.run(_go())
```

- [ ] **Step 9.2: Run and confirm failure**

Run: `pytest tests/test_state_store.py -v -k async`
Expected: ImportError for `AsyncStateStore`.

- [ ] **Step 9.3: Implement minimal async wrapper**

Append to `state_store.py`:
```python
import asyncio


class AsyncStateStore:
    """Serializes all writes through a single asyncio lock; reads are direct.

    Holds one connection. Callers should `await start()` before use and
    `await stop()` at shutdown.
    """

    def __init__(self, path: str | Path):
        self._path = path
        self._conn: Optional[sqlite3.Connection] = None
        self._write_lock = asyncio.Lock()

    async def start(self) -> None:
        self._conn = open_db(self._path)

    async def stop(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("AsyncStateStore not started")
        return self._conn

    # Writes (serialized)
    async def insert_position(self, **kwargs) -> int:
        async with self._write_lock:
            return insert_position(self.conn, **kwargs)

    async def update_position_status(self, position_id: int, status: str) -> None:
        async with self._write_lock:
            update_position_status(self.conn, position_id, status)

    async def close_position(self, position_id: int, **kwargs) -> None:
        async with self._write_lock:
            close_position(self.conn, position_id, **kwargs)

    async def insert_fill(self, **kwargs) -> int:
        async with self._write_lock:
            return insert_fill(self.conn, **kwargs)

    async def write_audit(self, **kwargs) -> int:
        async with self._write_lock:
            return write_audit(self.conn, **kwargs)

    async def snapshot_balance(self, **kwargs) -> int:
        async with self._write_lock:
            return snapshot_balance(self.conn, **kwargs)

    async def upsert_exchange_health(self, **kwargs) -> None:
        async with self._write_lock:
            upsert_exchange_health(self.conn, **kwargs)

    async def write_recon_event(self, **kwargs) -> int:
        async with self._write_lock:
            return write_recon_event(self.conn, **kwargs)

    async def resolve_recon_event(self, event_id: int, **kwargs) -> None:
        async with self._write_lock:
            resolve_recon_event(self.conn, event_id, **kwargs)

    # Reads (direct, no lock — SQLite supports concurrent reads in WAL mode)
    async def get_position(self, position_id: int):
        return get_position(self.conn, position_id)

    async def list_open_positions(self):
        return list_open_positions(self.conn)

    async def list_fills_for_position(self, position_id: int):
        return list_fills_for_position(self.conn, position_id)

    async def list_recent_fills(self, *, exchange: str, since_ms: int):
        return list_recent_fills(self.conn, exchange=exchange, since_ms=since_ms)

    async def latest_balance(self, *, exchange: str, asset: str = "USDT"):
        return latest_balance(self.conn, exchange=exchange, asset=asset)

    async def get_exchange_health(self, exchange: str):
        return get_exchange_health(self.conn, exchange)

    async def list_unresolved_recon_events(self, *, min_severity: str = "info"):
        return list_unresolved_recon_events(self.conn, min_severity=min_severity)
```

- [ ] **Step 9.4: Run tests and confirm green**

Run: `pytest tests/test_state_store.py -v`
Expected: all green.

- [ ] **Step 9.5: Commit**

```bash
git add state_store.py tests/test_state_store.py
git commit -m "feat(state_store): add AsyncStateStore async writer wrapper"
```

---

## Task 10: Migration script — read existing files

**Files:**
- Create: `migrate_to_sqlite.py`
- Create: `tests/test_migration.py`
- Create: `tests/fixtures/sample_state.json`
- Create: `tests/fixtures/sample_trade_history.csv`

- [ ] **Step 10.1: Write fixture inputs**

`tests/fixtures/sample_state.json`:
```json
{
  "open_positions": [
    {
      "symbol": "ORDIUSDT",
      "exchange_a": "MEXC", "exchange_b": "BLOFIN",
      "side_a": "buy", "side_b": "sell",
      "size_usd_a": 25.0, "size_usd_b": 25.0,
      "entry_spread_pct": 0.012,
      "opened_at_ms": 1700000000000,
      "fills": [
        {"exchange":"MEXC","leg":"a","intent":"entry","order_id":"m-1",
         "side":"buy","size_usd":25.0,"fill_price":1.234,"fees_usd":0.01,
         "filled_at_ms":1700000000500,"raw":"{}"},
        {"exchange":"BLOFIN","leg":"b","intent":"entry","order_id":"b-1",
         "side":"sell","size_usd":25.0,"fill_price":1.249,"fees_usd":0.01,
         "filled_at_ms":1700000000600,"raw":"{}"}
      ]
    }
  ]
}
```

`tests/fixtures/sample_trade_history.csv`:
```csv
symbol,exchange_a,exchange_b,side_a,side_b,size_usd_a,size_usd_b,entry_spread_pct,exit_spread_pct,opened_at_ms,closed_at_ms,realized_pnl_usd
WIFUSDT,OKX,MEXC,buy,sell,25.0,25.0,0.011,0.001,1699999000000,1699999600000,0.30
```

- [ ] **Step 10.2: Write failing test for migration of clean input**

`tests/test_migration.py`:
```python
import os
import tempfile
from pathlib import Path
from state_store import open_db, init_schema
from migrate_to_sqlite import migrate

FIXTURES = Path(__file__).parent / "fixtures"


def test_migrate_clean_input(tmp_path):
    db = tmp_path / "out.db"
    init_schema(db)
    quarantine = tmp_path / "q.jsonl"
    summary = migrate(
        state_json_path=FIXTURES / "sample_state.json",
        trade_history_csv_path=FIXTURES / "sample_trade_history.csv",
        db_path=db,
        quarantine_path=quarantine,
    )
    assert summary["positions_migrated"] == 2  # 1 open + 1 closed
    assert summary["fills_migrated"] == 2
    assert summary["quarantined"] == 0

    conn = open_db(db)
    rows = conn.execute("SELECT symbol, status FROM positions ORDER BY id").fetchall()
    assert [(r["symbol"], r["status"]) for r in rows] == [
        ("ORDIUSDT", "open"), ("WIFUSDT", "closed"),
    ]
    conn.close()
```

- [ ] **Step 10.3: Run and confirm failure**

Run: `pytest tests/test_migration.py -v`
Expected: ImportError for `migrate_to_sqlite`.

- [ ] **Step 10.4: Implement migration (clean-input path)**

`migrate_to_sqlite.py`:
```python
"""One-time migration from file-based state to the SQLite state_store.

Reads `state.json` (open positions + their fills) and `trade_history.csv`
(closed positions). Validates each row through pydantic schemas. Inserts
passing rows; writes failures to a quarantine JSONL file with reason.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from schemas import PositionRecord, FillRecord
from state_store import (
    open_db, insert_position, insert_fill,
)


def migrate(
    *,
    state_json_path: Path,
    trade_history_csv_path: Path,
    db_path: Path,
    quarantine_path: Path,
) -> dict[str, int]:
    summary = {"positions_migrated": 0, "fills_migrated": 0, "quarantined": 0}
    quarantine_lines: list[str] = []

    conn = open_db(db_path)
    try:
        # Open positions from state.json
        if state_json_path.exists():
            with open(state_json_path) as f:
                state = json.load(f)
            for raw in state.get("open_positions", []):
                pid_or_skip = _migrate_open_position(conn, raw, quarantine_lines)
                if pid_or_skip is not None:
                    summary["positions_migrated"] += 1
                    summary["fills_migrated"] += _migrate_fills(
                        conn, pid_or_skip, raw.get("fills", []), quarantine_lines
                    )

        # Closed positions from trade_history.csv
        if trade_history_csv_path.exists():
            with open(trade_history_csv_path, newline="") as f:
                for row in csv.DictReader(f):
                    if _migrate_closed_position(conn, row, quarantine_lines):
                        summary["positions_migrated"] += 1
    finally:
        conn.close()

    if quarantine_lines:
        with open(quarantine_path, "w") as f:
            f.write("\n".join(quarantine_lines))
    summary["quarantined"] = len(quarantine_lines)
    return summary


def _migrate_open_position(conn, raw: dict, quarantine: list[str]) -> int | None:
    try:
        # Validate the shape we expect — id is auto-assigned, so omit
        PositionRecord(
            id=0, symbol=raw["symbol"],
            exchange_a=raw["exchange_a"], exchange_b=raw["exchange_b"],
            side_a=raw["side_a"], side_b=raw["side_b"],
            size_usd_a=raw["size_usd_a"], size_usd_b=raw["size_usd_b"],
            entry_spread_pct=raw.get("entry_spread_pct", 0.0),
            status="open", opened_at_ms=raw["opened_at_ms"],
        )
    except (ValidationError, KeyError) as e:
        quarantine.append(json.dumps({
            "kind": "open_position", "row": raw, "reason": str(e),
        }))
        return None
    pid = insert_position(
        conn, symbol=raw["symbol"],
        exchange_a=raw["exchange_a"], exchange_b=raw["exchange_b"],
        side_a=raw["side_a"], side_b=raw["side_b"],
        size_usd_a=raw["size_usd_a"], size_usd_b=raw["size_usd_b"],
        entry_spread_pct=raw.get("entry_spread_pct", 0.0),
        status="open", opened_at_ms=raw["opened_at_ms"],
    )
    return pid


def _migrate_fills(conn, position_id: int, raws: list[dict], quarantine: list[str]) -> int:
    n = 0
    for raw in raws:
        try:
            FillRecord(
                id=0, position_id=position_id,
                exchange=raw["exchange"], leg=raw["leg"], intent=raw["intent"],
                order_id=raw["order_id"], side=raw["side"],
                size_usd=raw["size_usd"], fill_price=raw["fill_price"],
                fees_usd=raw["fees_usd"], filled_at_ms=raw["filled_at_ms"],
            )
        except (ValidationError, KeyError) as e:
            quarantine.append(json.dumps({
                "kind": "fill", "position_id": position_id, "row": raw, "reason": str(e),
            }))
            continue
        insert_fill(
            conn, position_id=position_id,
            exchange=raw["exchange"], leg=raw["leg"], intent=raw["intent"],
            order_id=raw["order_id"], side=raw["side"],
            size_usd=raw["size_usd"], fill_price=raw["fill_price"],
            fees_usd=raw["fees_usd"], filled_at_ms=raw["filled_at_ms"],
            raw_response=raw.get("raw", "{}"),
        )
        n += 1
    return n


def _migrate_closed_position(conn, row: dict[str, Any], quarantine: list[str]) -> bool:
    try:
        # CSV gives strings; coerce numerics.
        coerced = {
            "symbol": row["symbol"],
            "exchange_a": row["exchange_a"], "exchange_b": row["exchange_b"],
            "side_a": row["side_a"], "side_b": row["side_b"],
            "size_usd_a": float(row["size_usd_a"]),
            "size_usd_b": float(row["size_usd_b"]),
            "entry_spread_pct": float(row["entry_spread_pct"]),
            "exit_spread_pct": float(row["exit_spread_pct"]) if row.get("exit_spread_pct") else None,
            "opened_at_ms": int(row["opened_at_ms"]),
            "closed_at_ms": int(row["closed_at_ms"]) if row.get("closed_at_ms") else None,
            "realized_pnl_usd": float(row["realized_pnl_usd"]) if row.get("realized_pnl_usd") else None,
        }
        PositionRecord(id=0, status="closed", **coerced)
    except (ValidationError, ValueError, KeyError) as e:
        quarantine.append(json.dumps({
            "kind": "closed_position", "row": row, "reason": str(e),
        }))
        return False
    pid = insert_position(
        conn, symbol=coerced["symbol"],
        exchange_a=coerced["exchange_a"], exchange_b=coerced["exchange_b"],
        side_a=coerced["side_a"], side_b=coerced["side_b"],
        size_usd_a=coerced["size_usd_a"], size_usd_b=coerced["size_usd_b"],
        entry_spread_pct=coerced["entry_spread_pct"],
        status="closed", opened_at_ms=coerced["opened_at_ms"],
    )
    conn.execute(
        """UPDATE positions SET closed_at=?, exit_spread_pct=?, realized_pnl_usd=?
           WHERE id=?""",
        (coerced["closed_at_ms"], coerced["exit_spread_pct"],
         coerced["realized_pnl_usd"], pid),
    )
    return True
```

- [ ] **Step 10.5: Run tests and confirm green**

Run: `pytest tests/test_migration.py -v`
Expected: 1 passed.

- [ ] **Step 10.6: Commit**

```bash
git add migrate_to_sqlite.py tests/test_migration.py tests/fixtures/sample_state.json tests/fixtures/sample_trade_history.csv
git commit -m "feat(migration): migrate clean state.json + trade_history.csv to SQLite"
```

---

## Task 11: Migration — quarantine path for malformed rows

**Files:**
- Modify: `tests/test_migration.py`
- Create: `tests/fixtures/malformed_state.json`
- Create: `tests/fixtures/malformed_trade_history.csv`

- [ ] **Step 11.1: Write failing test**

`tests/fixtures/malformed_state.json` (a position with `size_usd_a=0`, a fill with `fill_price=0`):
```json
{
  "open_positions": [
    {
      "symbol": "BAD", "exchange_a": "MEXC", "exchange_b": "BLOFIN",
      "side_a": "buy", "side_b": "sell",
      "size_usd_a": 0, "size_usd_b": 25.0,
      "entry_spread_pct": 0.01, "opened_at_ms": 1, "fills": []
    },
    {
      "symbol": "OK", "exchange_a": "MEXC", "exchange_b": "BLOFIN",
      "side_a": "buy", "side_b": "sell",
      "size_usd_a": 25.0, "size_usd_b": 25.0,
      "entry_spread_pct": 0.01, "opened_at_ms": 2,
      "fills": [
        {"exchange":"MEXC","leg":"a","intent":"entry","order_id":"x",
         "side":"buy","size_usd":25.0,"fill_price":0,"fees_usd":0,
         "filled_at_ms":3,"raw":"{}"}
      ]
    }
  ]
}
```

`tests/fixtures/malformed_trade_history.csv` (one bad row, one good):
```csv
symbol,exchange_a,exchange_b,side_a,side_b,size_usd_a,size_usd_b,entry_spread_pct,exit_spread_pct,opened_at_ms,closed_at_ms,realized_pnl_usd
BAD,MEXC,BLOFIN,buy,sell,not-a-number,25.0,0.01,0.001,1,2,0.0
GOOD,OKX,MEXC,buy,sell,25.0,25.0,0.01,0.001,3,4,0.30
```

Append to `tests/test_migration.py`:
```python
def test_migrate_quarantines_malformed(tmp_path):
    db = tmp_path / "out.db"
    init_schema(db)
    quarantine = tmp_path / "q.jsonl"
    summary = migrate(
        state_json_path=FIXTURES / "malformed_state.json",
        trade_history_csv_path=FIXTURES / "malformed_trade_history.csv",
        db_path=db,
        quarantine_path=quarantine,
    )
    # 1 valid open + 1 valid closed = 2 positions; 0 valid fills
    assert summary["positions_migrated"] == 2
    assert summary["fills_migrated"] == 0
    # 1 bad open position + 1 bad fill + 1 bad closed = 3 quarantined
    assert summary["quarantined"] == 3
    lines = [json.loads(l) for l in quarantine.read_text().splitlines()]
    kinds = sorted(l["kind"] for l in lines)
    assert kinds == ["closed_position", "fill", "open_position"]
```

(Add `import json` to the test file if not present.)

- [ ] **Step 11.2: Run and confirm green** (the quarantine logic was already implemented in Task 10)

Run: `pytest tests/test_migration.py -v`
Expected: 2 passed.

- [ ] **Step 11.3: Commit**

```bash
git add tests/test_migration.py tests/fixtures/malformed_state.json tests/fixtures/malformed_trade_history.csv
git commit -m "test(migration): cover quarantine path for malformed rows"
```

---

## Task 12: Migration — threshold-based exit code

**Files:**
- Modify: `migrate_to_sqlite.py`
- Modify: `tests/test_migration.py`

- [ ] **Step 12.1: Write failing test**

Append to `tests/test_migration.py`:
```python
import pytest


def test_migrate_raises_when_quarantine_exceeds_threshold(tmp_path):
    db = tmp_path / "out.db"
    init_schema(db)
    quarantine = tmp_path / "q.jsonl"
    with pytest.raises(SystemExit) as excinfo:
        migrate(
            state_json_path=FIXTURES / "malformed_state.json",
            trade_history_csv_path=FIXTURES / "malformed_trade_history.csv",
            db_path=db,
            quarantine_path=quarantine,
            quarantine_threshold_pct=10.0,  # 3 quarantined / 5 total = 60% > 10%
        )
    assert excinfo.value.code != 0


def test_migrate_succeeds_when_quarantine_under_threshold(tmp_path):
    db = tmp_path / "out.db"
    init_schema(db)
    quarantine = tmp_path / "q.jsonl"
    summary = migrate(
        state_json_path=FIXTURES / "sample_state.json",
        trade_history_csv_path=FIXTURES / "sample_trade_history.csv",
        db_path=db,
        quarantine_path=quarantine,
        quarantine_threshold_pct=5.0,
    )
    assert summary["quarantined"] == 0
```

- [ ] **Step 12.2: Run and confirm failure**

Run: `pytest tests/test_migration.py -v -k threshold`
Expected: TypeError on unexpected `quarantine_threshold_pct`.

- [ ] **Step 12.3: Add threshold parameter to `migrate`**

Replace the signature and end of `migrate()` in `migrate_to_sqlite.py` to accept and enforce `quarantine_threshold_pct`. New signature and tail:

```python
import sys


def migrate(
    *,
    state_json_path: Path,
    trade_history_csv_path: Path,
    db_path: Path,
    quarantine_path: Path,
    quarantine_threshold_pct: float | None = None,
) -> dict[str, int]:
    # ... existing body up to the writing-quarantine block ...
    # (keep all existing logic; only the tail changes)

    if quarantine_lines:
        with open(quarantine_path, "w") as f:
            f.write("\n".join(quarantine_lines))
    summary["quarantined"] = len(quarantine_lines)

    if quarantine_threshold_pct is not None:
        total = (
            summary["positions_migrated"]
            + summary["fills_migrated"]
            + summary["quarantined"]
        )
        if total > 0:
            pct = 100.0 * summary["quarantined"] / total
            if pct > quarantine_threshold_pct:
                sys.stderr.write(
                    f"Migration aborted: {summary['quarantined']} of {total} rows "
                    f"({pct:.1f}%) quarantined, exceeding threshold "
                    f"{quarantine_threshold_pct:.1f}%\n"
                )
                raise SystemExit(2)

    return summary
```

- [ ] **Step 12.4: Run tests and confirm green**

Run: `pytest tests/test_migration.py -v`
Expected: all green.

- [ ] **Step 12.5: Commit**

```bash
git add migrate_to_sqlite.py tests/test_migration.py
git commit -m "feat(migration): add quarantine-threshold gate"
```

---

## Task 13: Migration — CLI entry point and dry-run

**Files:**
- Modify: `migrate_to_sqlite.py`
- Modify: `tests/test_migration.py`

- [ ] **Step 13.1: Write failing test for `--dry-run`**

Append to `tests/test_migration.py`:
```python
import subprocess
import sys


def test_cli_dry_run_does_not_write_db(tmp_path):
    db = tmp_path / "out.db"
    init_schema(db)
    quarantine = tmp_path / "q.jsonl"
    r = subprocess.run(
        [sys.executable, "migrate_to_sqlite.py",
         "--state", str(FIXTURES / "sample_state.json"),
         "--csv", str(FIXTURES / "sample_trade_history.csv"),
         "--db", str(db),
         "--quarantine", str(quarantine),
         "--dry-run"],
        capture_output=True, text=True, cwd=Path(__file__).parent.parent,
    )
    assert r.returncode == 0, r.stderr
    conn = open_db(db)
    n = conn.execute("SELECT COUNT(*) FROM positions").fetchone()[0]
    conn.close()
    assert n == 0  # dry-run wrote nothing
```

- [ ] **Step 13.2: Run and confirm failure**

Run: `pytest tests/test_migration.py -v -k dry_run`
Expected: missing CLI / argparse not yet implemented.

- [ ] **Step 13.3: Add CLI to `migrate_to_sqlite.py`**

Append to `migrate_to_sqlite.py`:
```python
import argparse


def _main():
    p = argparse.ArgumentParser(description="Migrate file-based state to SQLite.")
    p.add_argument("--state", required=True, type=Path)
    p.add_argument("--csv", required=True, type=Path)
    p.add_argument("--db", required=True, type=Path)
    p.add_argument("--quarantine", required=True, type=Path)
    p.add_argument("--threshold", type=float, default=5.0,
                   help="Abort if quarantined rows exceed this %% (default 5.0)")
    p.add_argument("--dry-run", action="store_true",
                   help="Run validation pass only; do not insert into the DB")
    args = p.parse_args()

    if args.dry_run:
        # Use an in-memory DB so all inserts are discarded
        import tempfile, os
        fd, tmp = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        try:
            init_schema_path = Path(tmp)
            from state_store import init_schema as _init
            _init(init_schema_path)
            summary = migrate(
                state_json_path=args.state,
                trade_history_csv_path=args.csv,
                db_path=init_schema_path,
                quarantine_path=args.quarantine,
                quarantine_threshold_pct=args.threshold,
            )
        finally:
            os.remove(tmp)
        print(f"DRY RUN summary: {summary}")
        return

    summary = migrate(
        state_json_path=args.state,
        trade_history_csv_path=args.csv,
        db_path=args.db,
        quarantine_path=args.quarantine,
        quarantine_threshold_pct=args.threshold,
    )
    print(f"Migration summary: {summary}")


if __name__ == "__main__":
    _main()
```

- [ ] **Step 13.4: Run tests and confirm green**

Run: `pytest tests/test_migration.py -v`
Expected: all green.

- [ ] **Step 13.5: Commit**

```bash
git add migrate_to_sqlite.py tests/test_migration.py
git commit -m "feat(migration): add CLI entry point with --dry-run"
```

---

## Plan 1 Done Criteria

- [ ] All 13 tasks complete; every commit message above shipped.
- [ ] `pytest tests/test_state_store.py tests/test_schemas.py tests/test_migration.py -v` is fully green.
- [ ] `state_store.py`, `schemas.py`, `migrate_to_sqlite.py` exist in the worktree.
- [ ] Live trader runtime is **unchanged** by this plan — Plan 1 ships only the data layer and migration tool. No `real_trader.py` modifications yet.

---

## What Plan 2 Will Build

Plan 2 (Detection layer) builds on this foundation:
- `reconciler.py` — per-trade and 5-min sweep, using `AsyncStateStore` reads + writes.
- `invariants.py` — 12 self-consistency rules over `state_store`.
- `alerts.py` — Telegram severity routing.
- Per-exchange `_normalize_order_response` methods on each `ExchangeExecutor`.
- Replay-test harness + 5 golden fixtures.

Plan 3 (Cutover) adds the `USE_SQLITE_STATE` feature flag, shadow-mode writer, dashboard updates, and the production cutover sequence.
