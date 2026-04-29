# Self-Healing Shadow Paper Trader

Greenfield convergence-arbitrage paper trader with a thin self-heal layer.

Watches public L2 orderbook WebSockets on five exchanges (Binance, Bybit, Bitget,
Gate.io, MEXC), computes a rolling Z-score on the cross-exchange spread, and
simulates concurrent two-leg execution. No real orders are ever placed.

Spec: [`docs/superpowers/specs/2026-04-29-self-healing-shadow-paper-trader-design.md`](../docs/superpowers/specs/2026-04-29-self-healing-shadow-paper-trader-design.md)

## Run

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python paper_trader.py --config config.yaml
```

Tail the journal:
```bash
sqlite3 data/journal.sqlite "select ts, event_type, pair, payload_json from journal order by id desc limit 20;"
```

## Tests

```bash
pytest -v
```
