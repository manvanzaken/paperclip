#!/bin/bash
# Start the BBO trader. MODE=paper (default) needs no keys; MODE=live needs the venue API keys in the
# environment (see .env.example) and refuses to start while the legacy real_trader heartbeat is fresh.
set -euo pipefail
cd "$(dirname "$0")"
export MODE="${MODE:-paper}"
export DATA_DIR="${DATA_DIR:-./data}"
mkdir -p "$DATA_DIR"
exec .venv/bin/python -m bbo_trader.main
