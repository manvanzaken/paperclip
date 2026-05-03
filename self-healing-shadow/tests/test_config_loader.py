"""Tests for the config loader with overrides."""

import yaml

from paper_trader import load_config_with_overrides


def write_yaml(path, payload):
    with open(path, "w", encoding="utf-8") as fh:
        yaml.safe_dump(payload, fh)


class TestNoOverrides:
    def test_returns_base_config_when_overrides_missing(self, tmp_path):
        base = tmp_path / "config.yaml"
        write_yaml(base, {
            "strategy": {"entry_z": 2.0, "min_net_profit_usd": 0.5},
            "exchanges": {"mexc": {"taker_fee_bps": 2}},
        })
        cfg = load_config_with_overrides(base, tmp_path / "missing.yaml")
        assert cfg["strategy"]["entry_z"] == 2.0
        assert cfg["strategy"]["min_net_profit_usd"] == 0.5


class TestOverrideMerging:
    def test_overrides_replace_specific_strategy_keys(self, tmp_path):
        base = tmp_path / "config.yaml"
        write_yaml(base, {
            "strategy": {"entry_z": 2.0, "exit_z": 0.5, "min_net_profit_usd": 0.5},
        })
        overrides = tmp_path / "overrides.yaml"
        write_yaml(overrides, {
            "strategy": {"entry_z": 1.7, "min_net_profit_usd": 0.30},
        })
        cfg = load_config_with_overrides(base, overrides)
        assert cfg["strategy"]["entry_z"] == 1.7
        assert cfg["strategy"]["min_net_profit_usd"] == 0.30
        # Untouched key preserved.
        assert cfg["strategy"]["exit_z"] == 0.5

    def test_per_exchange_overrides_merge(self, tmp_path):
        base = tmp_path / "config.yaml"
        write_yaml(base, {
            "exchanges": {
                "mexc": {"taker_fee_bps": 2, "starting_balance_usd": 1000},
                "bybit": {"taker_fee_bps": 6, "starting_balance_usd": 1000},
            },
        })
        overrides = tmp_path / "overrides.yaml"
        write_yaml(overrides, {
            "exchanges": {
                "bybit": {"max_position_usd": 250},
            },
        })
        cfg = load_config_with_overrides(base, overrides)
        assert cfg["exchanges"]["bybit"]["max_position_usd"] == 250
        # mexc untouched, bybit's other keys preserved.
        assert cfg["exchanges"]["mexc"]["taker_fee_bps"] == 2
        assert cfg["exchanges"]["bybit"]["taker_fee_bps"] == 6

    def test_empty_overrides_file_is_noop(self, tmp_path):
        base = tmp_path / "config.yaml"
        write_yaml(base, {"strategy": {"entry_z": 2.0}})
        overrides = tmp_path / "overrides.yaml"
        overrides.write_text("")
        cfg = load_config_with_overrides(base, overrides)
        assert cfg["strategy"]["entry_z"] == 2.0
