# Triangular Arbitrage Scanner

Read-only scanner for triangular arbitrage opportunities on MEXC, Binance, Gate, KuCoin, Bybit (spot, USDT/USDC anchors).

## Run

```
cd triangular-scanner
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp config.example.yaml config.yaml
python scan.py --config config.yaml
```
