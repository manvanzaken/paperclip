from __future__ import annotations
import sqlite3
import time
from pathlib import Path
from flask import Blueprint, Response, current_app, jsonify, request

bp = Blueprint("triangular", __name__)


def _data_dir() -> Path:
    return Path(current_app.config["TRISCAN_DATA_DIR"])


@bp.get("/api/triangular/live")
def live():
    p = _data_dir() / "triscan_status.json"
    if not p.exists():
        return jsonify({"error": "scanner not running (no status file)"}), 503
    try:
        return Response(p.read_text(), mimetype="application/json")
    except OSError as exc:
        return jsonify({"error": str(exc)}), 500


@bp.get("/api/triangular/history")
def history():
    hours = int(request.args.get("hours", 24))
    exchange = request.args.get("exchange")
    db_path = _data_dir() / "triscan.db"
    if not db_path.exists():
        return jsonify({"opportunities": [], "summary": {"total": 0, "total_profit": 0.0}}), 200
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    try:
        # opened_at is stored as int milliseconds in our schema; compute cutoff in ms
        cutoff_ms = int((time.time() - hours * 3600) * 1000)
        where = "WHERE opened_at > ?"
        params: list = [cutoff_ms]
        if exchange:
            where += " AND exchange = ?"
            params.append(exchange)
        rows = con.execute(
            f"""SELECT triangle_id, exchange,
                       COUNT(*) AS n,
                       SUM(peak_executable_profit_usd) AS profit_seen,
                       AVG(lifetime_ms)               AS avg_lifetime_ms,
                       MAX(peak_net_edge_pct)         AS max_edge_pct
                FROM opportunities
                {where}
                GROUP BY triangle_id, exchange
                ORDER BY profit_seen DESC
                LIMIT 50""",
            params,
        ).fetchall()
        summary = con.execute(
            f"""SELECT COUNT(*) AS total,
                       COALESCE(SUM(peak_executable_profit_usd), 0) AS total_profit
                FROM opportunities {where}""",
            params,
        ).fetchone()
        return jsonify({
            "opportunities": [dict(r) for r in rows],
            "summary": dict(summary),
        })
    finally:
        con.close()


@bp.get("/api/triangular/stream")
def stream():
    p = _data_dir() / "events.jsonl"

    def gen():
        last_pos = 0
        while True:
            if not p.exists():
                yield ":scanner not running\n\n"
                time.sleep(2.0)
                continue
            with p.open("r") as f:
                f.seek(last_pos)
                for line in f:
                    yield f"data: {line.rstrip()}\n\n"
                last_pos = f.tell()
            time.sleep(0.5)

    return Response(gen(), mimetype="text/event-stream")
