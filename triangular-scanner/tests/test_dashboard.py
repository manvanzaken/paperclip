import json
from pathlib import Path
import pytest
from flask import Flask
from triscan.dashboard import bp


def _app(data_dir):
    app = Flask(__name__)
    app.config["TRISCAN_DATA_DIR"] = str(data_dir)
    app.register_blueprint(bp)
    return app


def test_live_503_when_no_status(tmp_path):
    rv = _app(tmp_path).test_client().get("/api/triangular/live")
    assert rv.status_code == 503


def test_live_returns_status_when_present(tmp_path):
    (tmp_path / "triscan_status.json").write_text(json.dumps(
        {"ts": "2026-05-04T12:00:00Z", "confirmed": [], "candidates": [],
         "ws_subscriptions_per_exchange": {}, "triangle_count_per_exchange": {},
         "last_tier1_poll_per_exchange": {}}))
    rv = _app(tmp_path).test_client().get("/api/triangular/live")
    assert rv.status_code == 200
    assert rv.get_json()["confirmed"] == []


def test_history_empty_when_no_db(tmp_path):
    rv = _app(tmp_path).test_client().get("/api/triangular/history")
    assert rv.status_code == 200
    body = rv.get_json()
    assert body["opportunities"] == []
    assert body["summary"]["total"] == 0


def test_history_returns_grouped_rows(tmp_path):
    from triscan.storage.sqlite import SqliteStore
    store = SqliteStore(str(tmp_path / "triscan.db"))
    import time
    now_ms = int(time.time() * 1000)
    opp = {
        "id": "uuid-1", "triangle_id": "mexc:USDT-BTC-ETH-USDT", "exchange": "mexc", "anchor": "USDT",
        "legs": [["BTC/USDT","BUY"],["ETH/BTC","BUY"],["ETH/USDT","SELL"]],
        "opened_at": now_ms - 1000,
        "closed_at": now_ms,
        "lifetime_ms": 1000,
        "open_net_edge_pct": 0.18, "close_net_edge_pct": 0.10,
        "peak_net_edge_pct": 0.24, "peak_executable_profit_usd": 5.6,
        "peak_executable_size_usd": 2333.0,
        "peak_at": now_ms - 500, "bottleneck_leg_at_peak": 1,
        "ws_update_count": 12, "median_book_age_ms": 18.0, "closed_reason": "edge_decay",
    }
    store.insert_opportunity(opp)
    store.close()
    rv = _app(tmp_path).test_client().get("/api/triangular/history?hours=999999")
    body = rv.get_json()
    assert body["summary"]["total"] == 1
    assert body["opportunities"][0]["exchange"] == "mexc"
    assert body["opportunities"][0]["n"] == 1
