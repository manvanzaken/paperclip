from __future__ import annotations
import gzip
import json
import sqlite3
import time
import uuid
from pathlib import Path
from typing import List, Optional


SCHEMA = """
CREATE TABLE IF NOT EXISTS opportunities (
    id TEXT PRIMARY KEY,
    triangle_id TEXT NOT NULL,
    exchange TEXT NOT NULL,
    anchor TEXT NOT NULL,
    legs_json TEXT NOT NULL,
    opened_at INTEGER NOT NULL,
    closed_at INTEGER,
    lifetime_ms INTEGER,
    open_net_edge_pct REAL,
    close_net_edge_pct REAL,
    peak_net_edge_pct REAL,
    peak_executable_profit_usd REAL,
    peak_executable_size_usd REAL,
    peak_at INTEGER,
    bottleneck_leg_at_peak INTEGER,
    ws_update_count INTEGER,
    median_book_age_ms REAL,
    closed_reason TEXT,
    snapshots_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_opp_exchange_opened ON opportunities(exchange, opened_at);
CREATE INDEX IF NOT EXISTS idx_opp_triangle_opened ON opportunities(triangle_id, opened_at);
CREATE INDEX IF NOT EXISTS idx_opp_profit ON opportunities(peak_executable_profit_usd DESC);

CREATE TABLE IF NOT EXISTS scan_runs (
    run_id TEXT PRIMARY KEY,
    started_at INTEGER NOT NULL,
    ended_at INTEGER,
    config_json TEXT NOT NULL,
    exchanges TEXT NOT NULL,
    triangle_count INTEGER NOT NULL
);
"""


class SqliteStore:
    def __init__(self, path: Path | str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.con = sqlite3.connect(self.path)
        self.con.row_factory = sqlite3.Row
        self.con.executescript(SCHEMA)
        self.con.commit()

    def insert_opportunity(self, o: dict) -> None:
        self.con.execute(
            """INSERT OR REPLACE INTO opportunities
               (id, triangle_id, exchange, anchor, legs_json, opened_at, closed_at, lifetime_ms,
                open_net_edge_pct, close_net_edge_pct, peak_net_edge_pct,
                peak_executable_profit_usd, peak_executable_size_usd, peak_at,
                bottleneck_leg_at_peak, ws_update_count, median_book_age_ms,
                closed_reason, snapshots_json)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                o["id"], o["triangle_id"], o["exchange"], o["anchor"],
                json.dumps(o["legs"]),
                o["opened_at"], o.get("closed_at"), o.get("lifetime_ms"),
                o.get("open_net_edge_pct"), o.get("close_net_edge_pct"), o.get("peak_net_edge_pct"),
                o.get("peak_executable_profit_usd"), o.get("peak_executable_size_usd"),
                o.get("peak_at"), o.get("bottleneck_leg_at_peak"),
                o.get("ws_update_count"), o.get("median_book_age_ms"),
                o.get("closed_reason"),
                json.dumps(o.get("snapshots", [])),
            ),
        )
        self.con.commit()

    def list_opportunities(self, exchange: Optional[str] = None) -> List[dict]:
        if exchange:
            cur = self.con.execute(
                "SELECT * FROM opportunities WHERE exchange = ? ORDER BY opened_at DESC", (exchange,)
            )
        else:
            cur = self.con.execute("SELECT * FROM opportunities ORDER BY opened_at DESC")
        return [dict(r) for r in cur.fetchall()]

    def start_scan_run(self, config: dict, exchanges: List[str], triangle_count: int) -> str:
        rid = str(uuid.uuid4())
        self.con.execute(
            """INSERT INTO scan_runs (run_id, started_at, ended_at, config_json, exchanges, triangle_count)
               VALUES (?, ?, NULL, ?, ?, ?)""",
            (rid, int(time.time() * 1000), json.dumps(config), ",".join(exchanges), triangle_count),
        )
        self.con.commit()
        return rid

    def end_scan_run(self, run_id: str) -> None:
        self.con.execute(
            "UPDATE scan_runs SET ended_at = ? WHERE run_id = ?",
            (int(time.time() * 1000), run_id),
        )
        self.con.commit()

    def list_scan_runs(self) -> List[dict]:
        cur = self.con.execute("SELECT * FROM scan_runs ORDER BY started_at DESC")
        return [dict(r) for r in cur.fetchall()]

    def rebuild_from_jsonl(self, data_dir: Path | str) -> int:
        self.con.execute("DELETE FROM opportunities")
        count = 0
        for p in sorted(Path(data_dir).glob("events-*.jsonl*")):
            opener = gzip.open if p.suffix == ".gz" else open
            with opener(p, "rt") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    evt = json.loads(line)
                    if evt.get("type") == "OpportunityClosed":
                        self.insert_opportunity(evt["opportunity"])
                        count += 1
        return count

    def close(self) -> None:
        self.con.close()
