"""Standalone Flask app hosting the triangular blueprint with a sample tab UI.
Use for local UI development. Production integration via copy-paste snippets
in docs/dashboard-integration.md."""
import argparse
import os
from flask import Flask, render_template_string
from triscan.dashboard import bp

INDEX_HTML = """
<!doctype html>
<html><head><meta charset="utf-8"><title>Triangular Scanner Dev</title>
<style>
body{font-family:system-ui,sans-serif;margin:1rem;background:#0a0a0a;color:#ddd}
table{border-collapse:collapse;width:100%}
th,td{padding:.4rem .6rem;text-align:left;border-bottom:1px solid #333;font-variant-numeric:tabular-nums}
th{background:#1a1a1a}
.confirmed{color:#4ade80}.candidate{color:#fbbf24}
.tabs{display:flex;gap:1rem;border-bottom:1px solid #333;margin-bottom:1rem}
.tabs button{background:none;color:#999;border:none;padding:.5rem 0;cursor:pointer;border-bottom:2px solid transparent}
.tabs button.active{color:#fff;border-bottom-color:#4ade80}
.muted{color:#666;font-size:.9em}
</style></head><body>
<h2>Triangular Arb Scanner</h2>
<div class="tabs">
  <button class="tab active" data-tab="live">Live</button>
  <button class="tab" data-tab="history">History (24h)</button>
</div>
<div id="tab-live"><div id="live-status" class="muted">loading\u2026</div>
<table id="live-table"><thead><tr>
  <th>State</th><th>Exchange</th><th>Triangle</th><th>Net %</th>
  <th>Profit $</th><th>Size $</th><th>Age (ms)</th>
</tr></thead><tbody></tbody></table></div>
<div id="tab-history" style="display:none"><div id="history-summary" class="muted"></div>
<table id="hist-table"><thead><tr>
  <th>Triangle</th><th>Exchange</th><th>#</th><th>Profit Seen $</th>
  <th>Avg Lifetime ms</th><th>Max Edge %</th>
</tr></thead><tbody></tbody></table></div>
<script>
function td(text) { const e = document.createElement("td"); e.textContent = String(text); return e; }
function tr(cells, className) {
  const e = document.createElement("tr");
  if (className) e.className = className;
  for (const c of cells) e.appendChild(td(c));
  return e;
}
function setRows(tbody, rows) { tbody.replaceChildren(...rows); }

document.querySelectorAll(".tab").forEach(b => b.onclick = () => {
  document.querySelectorAll(".tab").forEach(x => x.classList.remove("active"));
  b.classList.add("active");
  document.getElementById("tab-live").style.display    = b.dataset.tab === "live"    ? "" : "none";
  document.getElementById("tab-history").style.display = b.dataset.tab === "history" ? "" : "none";
  if (b.dataset.tab === "history") loadHistory();
});

async function loadLive() {
  const r = await fetch("/api/triangular/live");
  const status = document.getElementById("live-status");
  const tbody = document.querySelector("#live-table tbody");
  if (r.status === 503) { status.textContent = "scanner not running"; tbody.replaceChildren(); return; }
  const j = await r.json();
  const subs = j.ws_subscriptions_per_exchange || {};
  status.textContent = "subs: " + JSON.stringify(subs) + "  ts: " + (j.ts || "");
  const items = [
    ...(j.confirmed  || []).map(x => ({...x, state: "confirmed"})),
    ...(j.candidates || []).map(x => ({...x, state: "candidate"})),
  ];
  setRows(tbody, items.map(it => tr([
    it.state, it.exchange, it.triangle_id,
    (+it.net_edge_pct).toFixed(4),
    (+it.profit_usd).toFixed(2),
    (+it.size_usd).toFixed(0),
    it.age_ms || 0,
  ], it.state)));
}
async function loadHistory() {
  const r = await fetch("/api/triangular/history?hours=24");
  const j = await r.json();
  document.getElementById("history-summary").textContent =
    "total: " + (j.summary.total || 0) +
    "  profit-seen: $" + (+(j.summary.total_profit || 0)).toFixed(2);
  setRows(document.querySelector("#hist-table tbody"),
    (j.opportunities || []).map(o => tr([
      o.triangle_id, o.exchange, o.n,
      (+o.profit_seen).toFixed(2),
      (+o.avg_lifetime_ms).toFixed(0),
      (+o.max_edge_pct).toFixed(4),
    ])));
}
loadLive(); setInterval(loadLive, 1000);
</script>
</body></html>
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default=os.environ.get("TRISCAN_DATA_DIR", "./data"))
    ap.add_argument("--port", type=int, default=3010)
    args = ap.parse_args()
    app = Flask(__name__)
    app.config["TRISCAN_DATA_DIR"] = args.data_dir
    app.register_blueprint(bp)
    app.add_url_rule("/", "index", lambda: render_template_string(INDEX_HTML))
    app.run(host="127.0.0.1", port=args.port, debug=False)


if __name__ == "__main__":
    main()
