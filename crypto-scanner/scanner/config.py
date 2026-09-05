from __future__ import annotations

import os
from pathlib import Path

import yaml

BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = BASE_DIR / "config.yaml"
EXAMPLE_CONFIG_PATH = BASE_DIR / "config.example.yaml"
TOKENS_CACHE_PATH = BASE_DIR / "tokens.json"

DEFAULTS = {
    "scan_interval_seconds": 30,
    "min_spread_pct": 1.5,
    "min_liquidity_usd": 5000,
    "max_tokens": 50,
    "binance": {"api_key": "", "api_secret": ""},
    "gate": {"api_key": "", "api_secret": ""},
    "mexc": {"api_key": "", "api_secret": ""},
    "kucoin": {"api_key": "", "api_secret": "", "passphrase": ""},
    "bybit": {"api_key": "", "api_secret": ""},
    "fees": {
        "binance_spot_pct": 0.1,
        "gate_spot_pct": 0.2,
        "mexc_spot_pct": 0.2,
        "kucoin_spot_pct": 0.1,
        "bybit_spot_pct": 0.1,
        "slippage_pct": 0.5,
        "gas_eth_usd": 5.0,
        "gas_bsc_usd": 0.10,
        "gas_sol_usd": 0.001,
    },
    "telegram": {"enabled": False, "bot_token": "", "chat_id": ""},
    "discord": {"enabled": False, "webhook_url": ""},
}


def _deep_merge(base: dict, override: dict) -> dict:
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_config(path: Path | str | None = None) -> dict:
    config_path = Path(path) if path else DEFAULT_CONFIG_PATH

    if config_path.exists():
        with open(config_path) as f:
            user_config = yaml.safe_load(f) or {}
    elif EXAMPLE_CONFIG_PATH.exists():
        user_config = {}
    else:
        user_config = {}

    config = _deep_merge(DEFAULTS, user_config)

    # Allow env var overrides for API keys
    env_mappings = {
        "BINANCE_API_KEY": ("binance", "api_key"),
        "BINANCE_API_SECRET": ("binance", "api_secret"),
        "GATE_API_KEY": ("gate", "api_key"),
        "GATE_API_SECRET": ("gate", "api_secret"),
        "MEXC_API_KEY": ("mexc", "api_key"),
        "MEXC_API_SECRET": ("mexc", "api_secret"),
        "KUCOIN_API_KEY": ("kucoin", "api_key"),
        "KUCOIN_API_SECRET": ("kucoin", "api_secret"),
        "KUCOIN_PASSPHRASE": ("kucoin", "passphrase"),
        "BYBIT_API_KEY": ("bybit", "api_key"),
        "BYBIT_API_SECRET": ("bybit", "api_secret"),
        "TELEGRAM_BOT_TOKEN": ("telegram", "bot_token"),
        "TELEGRAM_CHAT_ID": ("telegram", "chat_id"),
        "DISCORD_WEBHOOK_URL": ("discord", "webhook_url"),
    }
    for env_var, (section, key) in env_mappings.items():
        val = os.environ.get(env_var)
        if val:
            config[section][key] = val

    return config
