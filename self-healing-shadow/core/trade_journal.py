"""SQLite-backed structured trade journal.

`log()` is non-blocking — it appends to an in-memory queue. The 1 Hz
flusher task in `paper_trader.py` calls `flush()` to drain the queue
inside a single `BEGIN ... COMMIT` block, which gives the burst-fill
atomicity property described in PDF2 §5.2.

Writes happen on the calling thread; SQLite is opened with
`check_same_thread=False` since the bot is single-threaded asyncio.
"""

from __future__ import annotations

import json
import sqlite3
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional


_SCHEMA = """
CREATE TABLE IF NOT EXISTS journal (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    ts               TEXT NOT NULL,
    event_type       TEXT NOT NULL,
    pair             TEXT,
    symbol           TEXT,
    payload_json     TEXT NOT NULL,
    z_score          REAL,
    spread_bps       REAL,
    expected_pnl_usd REAL
);
CREATE INDEX IF NOT EXISTS idx_ts    ON journal(ts);
CREATE INDEX IF NOT EXISTS idx_event ON journal(event_type);
"""


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class TradeJournal:
    def __init__(self, db_path: str | Path) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
        self.conn.executescript(_SCHEMA)
        self.conn.commit()
        self._queue: deque[tuple[Any, ...]] = deque()

    def close(self) -> None:
        self.flush()
        self.conn.close()

    # --- write side ----------------------------------------------------

    def log(
        self,
        *,
        event_type: str,
        payload: dict,
        pair: Optional[str] = None,
        symbol: Optional[str] = None,
        z_score: Optional[float] = None,
        spread_bps: Optional[float] = None,
        expected_pnl_usd: Optional[float] = None,
    ) -> None:
        self._queue.append(
            (
                _utcnow_iso(),
                event_type,
                pair,
                symbol,
                json.dumps(payload),
                z_score,
                spread_bps,
                expected_pnl_usd,
            )
        )

    def flush(self) -> None:
        if not self._queue:
            return
        rows = list(self._queue)
        self._queue.clear()
        with self.conn:  # transaction
            self.conn.executemany(
                """
                INSERT INTO journal
                (ts, event_type, pair, symbol, payload_json, z_score, spread_bps, expected_pnl_usd)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

    # --- read side -----------------------------------------------------

    def last_successful_trade(self) -> Optional[datetime]:
        cur = self.conn.execute(
            "SELECT ts FROM journal WHERE event_type='FILL' ORDER BY id DESC LIMIT 1"
        )
        row = cur.fetchone()
        if row is None:
            return None
        return datetime.fromisoformat(row[0])

    def count_errors(self, *, since: timedelta) -> int:
        threshold = (datetime.now(timezone.utc) - since).isoformat()
        cur = self.conn.execute(
            "SELECT COUNT(*) FROM journal WHERE event_type='ERROR' AND ts >= ?",
            (threshold,),
        )
        return int(cur.fetchone()[0])

    def count_aborts(self, *, reason: Optional[str] = None) -> int:
        if reason is None:
            cur = self.conn.execute(
                "SELECT COUNT(*) FROM journal WHERE event_type='ENTRY_ABORTED'"
            )
            return int(cur.fetchone()[0])
        # Filter via JSON LIKE — fine for our small write rate.
        cur = self.conn.execute(
            """
            SELECT COUNT(*)
            FROM journal
            WHERE event_type='ENTRY_ABORTED'
              AND payload_json LIKE ?
            """,
            (f'%"reason": "{reason}"%',),
        )
        return int(cur.fetchone()[0])
