from __future__ import annotations
from pathlib import Path
from typing import Dict, List
from pydantic import BaseModel, Field, field_validator
import yaml


class ScannerConfig(BaseModel):
    tier1_interval_sec: int
    tier1_threshold_pct: float
    tier2_threshold_pct: float
    min_profit_usd: float
    cooldown_sec: int
    max_size_cap_usd: float
    max_ws_subscriptions_per_exchange: int
    triangle_refresh_hours: int
    sample_every_n_updates: int = 0


class FiltersConfig(BaseModel):
    anchors: List[str]
    min_24h_volume_usd: float

    @field_validator("anchors")
    @classmethod
    def _non_empty(cls, v):
        if not v:
            raise ValueError("anchors must be non-empty")
        return v


class ExchangeConfig(BaseModel):
    enabled: bool
    taker_fee_pct: float
    rest_url: str
    ws_url: str = ""


class StorageConfig(BaseModel):
    data_dir: str
    jsonl_retention_days: int
    sqlite_path: str


class ConsoleConfig(BaseModel):
    enabled: bool
    refresh_ms: int
    show_candidates: bool
    top_n: int


class OutputConfig(BaseModel):
    console: ConsoleConfig
    log_level: str = "INFO"


class Config(BaseModel):
    scanner: ScannerConfig
    filters: FiltersConfig
    exchanges: Dict[str, ExchangeConfig]
    storage: StorageConfig
    output: OutputConfig


def load_config(path: Path | str) -> Config:
    text = Path(path).read_text()
    raw = yaml.safe_load(text)
    return Config(**raw)
