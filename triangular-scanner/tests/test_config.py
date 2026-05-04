from pathlib import Path
import pytest
from pydantic import ValidationError
from triscan.config import load_config, Config


def test_load_example_config(tmp_path):
    cfg_path = Path(__file__).parents[1] / "config.example.yaml"
    cfg = load_config(cfg_path)
    assert isinstance(cfg, Config)
    assert cfg.scanner.tier1_interval_sec == 5
    assert cfg.scanner.tier1_threshold_pct == 0.5
    assert "USDT" in cfg.filters.anchors
    assert cfg.exchanges["mexc"].enabled is True
    assert cfg.exchanges["mexc"].taker_fee_pct == 0.05
    assert cfg.storage.data_dir.endswith("data")


def test_load_config_rejects_unknown_anchor(tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text("""
scanner: {tier1_interval_sec: 5, tier1_threshold_pct: 0.5, tier2_threshold_pct: 0.1, min_profit_usd: 1.0, cooldown_sec: 60, max_size_cap_usd: 10000, max_ws_subscriptions_per_exchange: 50, triangle_refresh_hours: 6, sample_every_n_updates: 0}
filters: {anchors: [], min_24h_volume_usd: 1000000}
exchanges: {}
storage: {data_dir: ./data, jsonl_retention_days: 30, sqlite_path: ./data/triscan.db}
output: {console: {enabled: true, refresh_ms: 500, show_candidates: true, top_n: 20}, log_level: INFO}
""")
    with pytest.raises(ValidationError, match="anchors"):
        load_config(bad)
