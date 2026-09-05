import json
from pathlib import Path

import pytest

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
    blocked = tmp_path / "blocked.json"
    blocked.write_text(json.dumps([]))
    data = tmp_path / "data"
    data.mkdir()
    (data / "bot_config.json").write_text(json.dumps({"MAX_POSITION_USD": 12.5, "MODE": "live"}))
    cfg = load_config(env={
        "DATA_DIR": str(data), "VENUES_FILE": str(venues), "BLOCKED_FILE": str(blocked),
        "MAX_POSITION_USD": "40",
    })
    assert cfg.max_position_usd == 12.5             # file wins over env
    assert cfg.mode == "paper"                      # MODE is process-level only


def test_missing_venues_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError, match="venues file not found"):
        load_config(env={"DATA_DIR": str(tmp_path), "VENUES_FILE": str(tmp_path / "nope.json")})


def test_mode_is_normalized_and_validated(tmp_path):
    venues = _write_venues(tmp_path)
    blocked = tmp_path / "blocked.json"
    blocked.write_text(json.dumps([]))
    base_env = {"DATA_DIR": str(tmp_path / "data"), "VENUES_FILE": str(venues), "BLOCKED_FILE": str(blocked)}

    cfg = load_config(env={**base_env, "MODE": "Paper "})
    assert cfg.mode == "paper"

    cfg = load_config(env={**base_env, "MODE": "LIVE"})
    assert cfg.mode == "live"

    with pytest.raises(ValueError, match="MODE must be"):
        load_config(env={**base_env, "MODE": "dry"})


def test_unknown_role_raises(tmp_path):
    venues = tmp_path / "venues.json"
    venues.write_text(json.dumps({
        "mexc": {"role": "Trade", "taker_fee_pct": 0.02, "maker_fee_pct": 0.0,
                 "rate_limits": {"orders": 20, "cancels": 20, "window_s": 2.0, "reserve": 4, "shared": False},
                 "max_topics": 30, "min_requote_ms": 500},
    }))
    blocked = tmp_path / "blocked.json"
    blocked.write_text(json.dumps([]))
    with pytest.raises(ValueError, match="unknown role"):
        load_config(env={
            "DATA_DIR": str(tmp_path / "data"), "VENUES_FILE": str(venues), "BLOCKED_FILE": str(blocked),
        })


def test_shared_string_false_is_false(tmp_path):
    venues = tmp_path / "venues.json"
    venues.write_text(json.dumps({
        "mexc": {"role": "trade", "taker_fee_pct": 0.02, "maker_fee_pct": 0.0,
                 "rate_limits": {"orders": 20, "cancels": 20, "window_s": 2.0, "reserve": 4, "shared": "false"},
                 "max_topics": 30, "min_requote_ms": 500},
        "blofin": {"role": "trade", "taker_fee_pct": 0.06, "maker_fee_pct": 0.02,
                   "rate_limits": {"orders": 30, "cancels": 30, "window_s": 10.0, "reserve": 6, "shared": "true"},
                   "max_topics": 50, "min_requote_ms": 1000},
    }))
    blocked = tmp_path / "blocked.json"
    blocked.write_text(json.dumps([]))
    cfg = load_config(env={
        "DATA_DIR": str(tmp_path / "data"), "VENUES_FILE": str(venues), "BLOCKED_FILE": str(blocked),
    })
    assert cfg.venue("mexc").rate_limits.shared is False
    assert cfg.venue("blofin").rate_limits.shared is True


def test_missing_blocked_file_raises(tmp_path):
    venues = _write_venues(tmp_path)
    with pytest.raises(FileNotFoundError, match="blocked symbols file not found"):
        load_config(env={
            "DATA_DIR": str(tmp_path / "data"), "VENUES_FILE": str(venues),
            "BLOCKED_FILE": str(tmp_path / "nope_blocked.json"),
        })


def test_unknown_bot_config_keys_are_ignored_and_secrets_hidden(tmp_path):
    venues = _write_venues(tmp_path)
    blocked = tmp_path / "blocked.json"
    blocked.write_text(json.dumps([]))
    data = tmp_path / "data"
    data.mkdir()
    (data / "bot_config.json").write_text(json.dumps(
        {"NOT_A_FIELD": 1, "venues": "pwned", "blocked_symbols": ["INJECTED"]}
    ))
    cfg = load_config(env={
        "DATA_DIR": str(data), "VENUES_FILE": str(venues), "BLOCKED_FILE": str(blocked),
        "MEXC_API_SECRET": "secret-value", "TELEGRAM_TOKEN": "TELEGRAM-SECRET",
    })
    assert cfg.trade_venues == ["mexc", "blofin"]
    assert cfg.blocked_symbols == frozenset()
    assert "secret-value" not in repr(cfg.venue("mexc"))
    assert cfg.telegram_token == "TELEGRAM-SECRET"        # loaded from env...
    assert "TELEGRAM-SECRET" not in repr(cfg)              # ...but never printed


def test_venue_lookup_keyerror(tmp_path):
    venues = _write_venues(tmp_path)
    blocked = tmp_path / "blocked.json"
    blocked.write_text(json.dumps([]))
    cfg = load_config(env={
        "DATA_DIR": str(tmp_path / "data"), "VENUES_FILE": str(venues), "BLOCKED_FILE": str(blocked),
    })
    with pytest.raises(KeyError):
        cfg.venue("nope")


def test_invalid_json_names_file(tmp_path):
    venues = tmp_path / "venues.json"
    venues.write_text("{")
    with pytest.raises(ValueError, match=r"venues\.json: invalid JSON"):
        load_config(env={"DATA_DIR": str(tmp_path / "data"), "VENUES_FILE": str(venues)})


def test_blocklist_must_be_list_of_strings(tmp_path):
    venues = _write_venues(tmp_path)
    blocked = tmp_path / "blocked.json"

    blocked.write_text(json.dumps("ABC"))
    with pytest.raises(ValueError, match="expected a JSON list"):
        load_config(env={
            "DATA_DIR": str(tmp_path / "data"), "VENUES_FILE": str(venues), "BLOCKED_FILE": str(blocked),
        })

    blocked.write_text(json.dumps([1, 2]))
    with pytest.raises(ValueError, match="expected a JSON list"):
        load_config(env={
            "DATA_DIR": str(tmp_path / "data"), "VENUES_FILE": str(venues), "BLOCKED_FILE": str(blocked),
        })


def test_venues_must_be_object(tmp_path):
    venues = tmp_path / "venues.json"
    venues.write_text(json.dumps([]))
    blocked = tmp_path / "blocked.json"
    blocked.write_text(json.dumps([]))
    with pytest.raises(ValueError, match="expected a JSON object"):
        load_config(env={
            "DATA_DIR": str(tmp_path / "data"), "VENUES_FILE": str(venues), "BLOCKED_FILE": str(blocked),
        })


def test_shared_must_be_boolean(tmp_path):
    venues = tmp_path / "venues.json"
    blocked = tmp_path / "blocked.json"
    blocked.write_text(json.dumps([]))
    base_env = {"DATA_DIR": str(tmp_path / "data"), "VENUES_FILE": str(venues), "BLOCKED_FILE": str(blocked)}

    venues.write_text(json.dumps({
        "mexc": {"role": "trade", "taker_fee_pct": 0.02, "maker_fee_pct": 0.0,
                 "rate_limits": {"orders": 20, "cancels": 20, "window_s": 2.0, "reserve": 4, "shared": None},
                 "max_topics": 30, "min_requote_ms": 500},
    }))
    with pytest.raises(ValueError, match="shared must be true or false"):
        load_config(env=base_env)

    venues.write_text(json.dumps({
        "mexc": {"role": "trade", "taker_fee_pct": 0.02, "maker_fee_pct": 0.0,
                 "rate_limits": {"orders": 20, "cancels": 20, "window_s": 2.0, "reserve": 4, "shared": "maybe"},
                 "max_topics": 30, "min_requote_ms": 500},
    }))
    with pytest.raises(ValueError, match="shared must be true or false"):
        load_config(env=base_env)


def test_bad_mode_reported_before_missing_files(tmp_path):
    with pytest.raises(ValueError, match="MODE must be"):
        load_config(env={
            "MODE": "lve", "DATA_DIR": str(tmp_path / "data"), "VENUES_FILE": str(tmp_path / "nope.json"),
        })


def test_rate_limits_are_validated(tmp_path):
    blocked = tmp_path / "blocked.json"
    blocked.write_text(json.dumps([]))
    venues = tmp_path / "venues.json"
    base = {"role": "trade", "taker_fee_pct": 0.02, "maker_fee_pct": 0.0, "max_topics": 30, "min_requote_ms": 500}
    env = {"DATA_DIR": str(tmp_path / "data"), "VENUES_FILE": str(venues), "BLOCKED_FILE": str(blocked)}
    venues.write_text(json.dumps({"mexc": {**base, "rate_limits": {"orders": 20, "cancels": 20, "window_s": 2.0, "reserve": 20, "shared": False}}}))
    with pytest.raises(ValueError, match="reserve must be in"):
        load_config(env=env)
    venues.write_text(json.dumps({"mexc": {**base, "rate_limits": {"orders": 20, "cancels": 20, "window_s": 0, "reserve": 4, "shared": False}}}))
    with pytest.raises(ValueError, match="must be positive"):
        load_config(env=env)
