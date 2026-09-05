import json
from pathlib import Path

from bbo_trader.config import load_config, Config, VenueConfig


def _write_venues(tmp_path: Path) -> Path:
    p = tmp_path / "venues.json"
    p.write_text(json.dumps({
        "mexc": {"role": "trade", "taker_fee_pct": 0.02, "maker_fee_pct": 0.0,
                 "rate_limits": {"orders": 20, "cancels": 20, "window_s": 2.0, "reserve": 4, "shared": False},
                 "max_topics": 30, "min_requote_ms": 500},
        "blofin": {"role": "trade", "taker_fee_pct": 0.06, "maker_fee_pct": 0.02,
                   "rate_limits": {"orders": 30, "cancels": 30, "window_s": 10.0, "reserve": 6, "shared": True},
                   "max_topics": 50, "min_requote_ms": 1000},
        "okx": {"role": "quote_only", "taker_fee_pct": 0.05, "maker_fee_pct": 0.02,
                "rate_limits": {"orders": 60, "cancels": 60, "window_s": 2.0, "reserve": 6, "shared": False},
                "max_topics": 50, "min_requote_ms": 500, "symbol_whitelist": ["BTCUSDT"]},
    }))
    return p


def test_defaults_and_env(tmp_path):
    venues = _write_venues(tmp_path)
    blocked = tmp_path / "blocked.json"
    blocked.write_text(json.dumps(["OMGUSDT"]))
    cfg = load_config(env={
        "DATA_DIR": str(tmp_path / "data"), "VENUES_FILE": str(venues), "BLOCKED_FILE": str(blocked),
        "MAX_POSITION_USD": "40", "TM_EXIT_ENABLED": "false", "MEXC_API_KEY": "k", "MEXC_API_SECRET": "s",
    })
    assert isinstance(cfg, Config)
    assert cfg.mode == "paper"                      # default
    assert cfg.max_position_usd == 40.0
    assert cfg.tm_exit_enabled is False
    assert cfg.min_edge_pct == 0.05                 # untouched default
    assert cfg.blocked_symbols == frozenset({"OMGUSDT"})
    assert cfg.trade_venues == ["mexc", "blofin"]
    assert cfg.feed_venues == ["mexc", "blofin", "okx"]
    mexc = cfg.venue("mexc")
    assert isinstance(mexc, VenueConfig)
    assert mexc.api_key == "k" and mexc.api_secret == "s"
    assert mexc.rate_limits.orders == 20 and mexc.rate_limits.shared is False
    assert cfg.venue("blofin").rate_limits.shared is True
    assert cfg.venue("okx").symbol_whitelist == ("BTCUSDT",)
    assert cfg.venue("blofin").min_requote_ms == 1000


def test_bot_config_json_overrides_env_but_not_mode(tmp_path):
    venues = _write_venues(tmp_path)
    data = tmp_path / "data"
    data.mkdir()
    (data / "bot_config.json").write_text(json.dumps({"MAX_POSITION_USD": 12.5, "MODE": "live"}))
    cfg = load_config(env={"DATA_DIR": str(data), "VENUES_FILE": str(venues), "MAX_POSITION_USD": "40"})
    assert cfg.max_position_usd == 12.5             # file wins over env
    assert cfg.mode == "paper"                      # MODE is process-level only


def test_missing_venues_file_raises(tmp_path):
    try:
        load_config(env={"DATA_DIR": str(tmp_path), "VENUES_FILE": str(tmp_path / "nope.json")})
    except FileNotFoundError:
        return
    raise AssertionError("expected FileNotFoundError")
