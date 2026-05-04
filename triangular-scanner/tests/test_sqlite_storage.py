import json
import sqlite3
from pathlib import Path
import pytest
from triscan.storage.sqlite import SqliteStore


def _opp_dict():
    return {
        "id": "abc", "triangle_id": "binance:USDT:A|B|C", "exchange": "binance", "anchor": "USDT",
        "legs": [["A/USDT", "BUY"], ["B/A", "BUY"], ["B/USDT", "SELL"]],
        "opened_at": 1, "closed_at": 100, "lifetime_ms": 99,
        "open_net_edge_pct": 0.2, "close_net_edge_pct": 0.05, "peak_net_edge_pct": 0.3,
        "peak_executable_profit_usd": 5.0, "peak_executable_size_usd": 1000.0,
        "peak_at": 50, "bottleneck_leg_at_peak": 1,
        "ws_update_count": 7, "median_book_age_ms": 12.0, "closed_reason": "edge_decay",
    }


def test_insert_opportunity_and_indexes(tmp_path):
    store = SqliteStore(tmp_path / "t.db")
    store.insert_opportunity(_opp_dict())
    rows = store.list_opportunities(exchange="binance")
    assert len(rows) == 1
    assert rows[0]["id"] == "abc"
    con = sqlite3.connect(tmp_path / "t.db")
    idx = {r[1] for r in con.execute("PRAGMA index_list(opportunities)").fetchall()}
    assert "idx_opp_exchange_opened" in idx
    assert "idx_opp_triangle_opened" in idx
    assert "idx_opp_profit" in idx
    store.close()


def test_scan_runs_records_config_snapshot(tmp_path):
    store = SqliteStore(tmp_path / "t.db")
    rid = store.start_scan_run({"hello": "world"}, ["binance"], 42)
    assert isinstance(rid, str)
    store.end_scan_run(rid)
    rows = store.list_scan_runs()
    assert len(rows) == 1
    assert rows[0]["triangle_count"] == 42
    assert json.loads(rows[0]["config_json"])["hello"] == "world"
    assert rows[0]["ended_at"] is not None
    store.close()


def test_rebuild_from_jsonl(tmp_path):
    jsonl = tmp_path / "events-2026-05-04.jsonl"
    jsonl.write_text(json.dumps({"type": "OpportunityClosed", "opportunity": _opp_dict()}) + "\n")
    store = SqliteStore(tmp_path / "t.db")
    store.rebuild_from_jsonl(tmp_path)
    assert len(store.list_opportunities()) == 1
    store.close()
