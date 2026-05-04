# Dashboard Integration Guide

How to surface triangular-scanner outputs as a tab inside the existing paper-trader dashboard (`local_dashboard.py`).

---

## 1. Mounting the blueprint

Add the following to your `local_dashboard.py` (or whichever Flask app file bootstraps the paper-trader dashboard), alongside the existing app setup:

```python
from triscan.dashboard import bp as triangular_bp
app.config["TRISCAN_DATA_DIR"] = os.environ.get(
    "TRISCAN_DATA_DIR", "./triangular-scanner/data"
)
app.register_blueprint(triangular_bp)
```

The blueprint registers three routes under `/api/triangular/`:

| Route | Method | Description |
|---|---|---|
| `/api/triangular/live` | GET | Current confirmed + candidate opportunities (from `triscan_status.json`) |
| `/api/triangular/history` | GET | Grouped history from SQLite (`?hours=24&exchange=mexc`) |
| `/api/triangular/stream` | GET | Server-Sent Events tail of `events.jsonl` |

---

## 2. Adding the tab to existing HTML

Copy these blocks into your dashboard HTML. The CSS goes in `<style>`, the tab divs and script go in `<body>`.

**CSS** (add to existing `<style>` block):

```css
.confirmed{color:#4ade80}.candidate{color:#fbbf24}
.tabs{display:flex;gap:1rem;border-bottom:1px solid #333;margin-bottom:1rem}
.tabs button{background:none;color:#999;border:none;padding:.5rem 0;cursor:pointer;border-bottom:2px solid transparent}
.tabs button.active{color:#fff;border-bottom-color:#4ade80}
.muted{color:#666;font-size:.9em}
```

**Tab buttons** (add alongside existing navigation):

```html
<div class="tabs">
  <button class="tab active" data-tab="live">Live</button>
  <button class="tab" data-tab="history">History (24h)</button>
</div>
```

**Tab content panels**:

```html
<div id="tab-live"><div id="live-status" class="muted">loading…</div>
<table id="live-table"><thead><tr>
  <th>State</th><th>Exchange</th><th>Triangle</th><th>Net %</th>
  <th>Profit $</th><th>Size $</th><th>Age (ms)</th>
</tr></thead><tbody></tbody></table></div>
<div id="tab-history" style="display:none"><div id="history-summary" class="muted"></div>
<table id="hist-table"><thead><tr>
  <th>Triangle</th><th>Exchange</th><th>#</th><th>Profit Seen $</th>
  <th>Avg Lifetime ms</th><th>Max Edge %</th>
</tr></thead><tbody></tbody></table></div>
```

**JavaScript** (add before closing `</body>`):

```html
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
```

Note: all DOM updates use `textContent` and `createElement` — not `innerHTML` — so user-supplied strings from the API cannot inject markup.

---

## 3. Running both processes

The scanner and dashboard are separate processes that communicate through shared files in the data directory.

**Scanner** (writes `triscan_status.json`, `triscan.db`, `events.jsonl`):

```bash
cd triangular-scanner
caffeinate -i .venv/bin/python scan.py scan --config config.yaml
```

**Dashboard** (reads those files via the blueprint):

```bash
# From the bot root (deploy-live/):
LOCAL_MODE=1 DATA_DIR=./data TRISCAN_DATA_DIR=../triangular-scanner/data \
  caffeinate -i python3 local_dashboard.py
```

The shared state files:

| File | Written by | Read by |
|---|---|---|
| `data/triscan_status.json` | scanner (every 1 s) | `/api/triangular/live` |
| `data/triscan.db` | scanner (on close) | `/api/triangular/history` |
| `data/events.jsonl` | scanner (on open/close) | `/api/triangular/stream` |

If the scanner is not running, `/api/triangular/live` returns HTTP 503 and the UI shows "scanner not running". The history endpoint returns an empty result set. This is graceful — the dashboard stays usable for the convergence bot even when the scanner is stopped.

Use `caffeinate -i` on macOS to prevent sleep during long-running scans.

---

## 4. Boundary statement

> **Important**
>
> The triangular blueprint is **strictly read-only**. There is no scanner-process control surface — no start/stop/restart endpoints. To change scanner config or restart, manage the scanner process directly. This boundary is intentional: the dashboard already runs alongside the live convergence trading bot, and any bug in scanner-control routes could destabilize the live trading dashboard. Keep it read-only.
