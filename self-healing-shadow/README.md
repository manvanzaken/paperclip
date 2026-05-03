# Self-Healing Shadow Paper Trader

Greenfield convergence-arbitrage paper trader with a thin self-heal layer.

Watches public L2 orderbook WebSockets on five exchanges (Binance, Bybit, Bitget,
Gate.io, MEXC), computes a rolling Z-score on the cross-exchange spread, and
simulates concurrent two-leg execution. No real orders are ever placed.

Spec: [`docs/superpowers/specs/2026-04-29-self-healing-shadow-paper-trader-design.md`](../docs/superpowers/specs/2026-04-29-self-healing-shadow-paper-trader-design.md)

## Run (foreground)

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python paper_trader.py --config config.yaml
```

## Run as a daemon (24/7, auto-restart on crash)

```bash
cp com.vandenboogaard.selfhealingshadow.plist ~/Library/LaunchAgents/
launchctl bootstrap gui/$(id -u) \
    ~/Library/LaunchAgents/com.vandenboogaard.selfhealingshadow.plist
```

Stop:
```bash
launchctl bootout gui/$(id -u)/com.vandenboogaard.selfhealingshadow
```

Status:
```bash
launchctl print gui/$(id -u)/com.vandenboogaard.selfhealingshadow | grep -E "state|pid"
```

Tail the journal:
```bash
sqlite3 data/journal.sqlite "select ts, event_type, pair, payload_json from journal order by id desc limit 20;"
```

## Tests

```bash
pytest -v
```
