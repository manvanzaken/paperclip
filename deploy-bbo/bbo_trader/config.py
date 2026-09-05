"""Configuration: environment defaults → DATA_DIR/bot_config.json overrides → venues.json registry.

MODE and DATA_DIR are process-level (env only). Everything else can be overridden by the
dashboard-editable bot_config.json (restart required to apply)."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Mapping


@dataclass(frozen=True)
class RateLimits:
    orders: int = 20
    cancels: int = 20
    window_s: float = 2.0
    reserve: int = 4
    shared: bool = False  # True: orders and cancels draw from one bucket (BloFin)


@dataclass(frozen=True)
class VenueConfig:
    name: str
    role: str  # "trade" | "quote_only" | "off"
    taker_fee_pct: float
    maker_fee_pct: float
    rate_limits: RateLimits = RateLimits()
    max_topics: int = 50
    min_requote_ms: int = 500
    staleness_override_s: float | None = None
    symbol_whitelist: tuple[str, ...] = ()
    # secrets: excluded from repr; anything that serializes a VenueConfig (asdict) must whitelist fields
    api_key: str = field(default="", repr=False)
    api_secret: str = field(default="", repr=False)
    passphrase: str = field(default="", repr=False)


@dataclass(frozen=True)
class Config:
    mode: str = "paper"
    data_dir: Path = Path("./data")
    venues_file: Path = Path("config/venues.json")
    blocked_file: Path = Path("config/blocked_symbols.json")
    # sizing
    max_position_usd: float = 25.0
    position_size_pct: float = 0.125
    min_position_usd: float = 10.0
    max_concurrent: int = 3
    max_resting_makers_per_venue: int = 2
    # edge
    min_edge_pct: float = 0.05
    tm_extra_edge_pct: float = 0.05
    slip_pct: float = 0.05
    exit_spread_pct: float = 0.15
    stop_pct: float = 1.5
    max_hold_min: float = 30.0
    min_fill_spread_pct: float = -0.10
    max_sane_spread_pct: float = 10.0
    stale_quote_s: float = 2.0
    tt_enabled: bool = True
    tm_entry_enabled: bool = True
    tm_exit_enabled: bool = True
    maker_venue_policy: str = "best_edge"
    exit_maker_venue_policy: str = "best_fee"
    # maker mechanics
    maker_ttl_s: float = 30.0
    improve_ticks: int = 0
    requote_ticks: int = 1
    edge_gone_ms: int = 300
    max_naked_ms: int = 1500
    max_leg_mismatch_pct: float = 5.0
    # gates
    touch_depth_mult: float = 1.0
    min_volume_usd: float = 50_000.0
    funding_block_s: float = 600.0
    mismatch_slow_pct: float = 10.0
    mismatch_slow_n: int = 300
    mismatch_fast_pct: float = 50.0
    mismatch_fast_n: int = 10
    failed_entry_cooldown_s: float = 60.0
    pair_strikes_to_blacklist: int = 2
    pair_blacklist_s: float = 86_400.0
    symbol_loss_blacklist_s: float = 21_600.0
    # ops
    halt_flag: str = "stop.flag"
    resume_flag: str = "start.flag"
    paper_capital_per_venue: float = 100.0
    sim_latency_ms: int = 150
    sim_taker_slip_bps: float = 2.0
    maker_top_level_frac: float = 0.5
    legacy_heartbeat_path: Path = Path("/app/data/heartbeat_live")
    legacy_heartbeat_max_age_s: float = 120.0
    # bearer token: excluded from repr; asdict() still exposes it — whitelist fields when serializing Config
    telegram_token: str = field(default="", repr=False)
    telegram_chat_id: str = ""
    venues: tuple[VenueConfig, ...] = ()
    blocked_symbols: frozenset[str] = frozenset()

    @property
    def trade_venues(self) -> list[str]:
        return [v.name for v in self.venues if v.role == "trade"]

    @property
    def feed_venues(self) -> list[str]:
        return [v.name for v in self.venues if v.role in ("trade", "quote_only")]

    def venue(self, name: str) -> VenueConfig:
        for v in self.venues:
            if v.name == name:
                return v
        raise KeyError(name)


_PROCESS_ONLY = {"MODE", "DATA_DIR", "VENUES_FILE", "BLOCKED_FILE"}


def _coerce(raw: object, target_type) -> object:
    if target_type is bool:
        return str(raw).strip().lower() in ("1", "true", "yes", "on")
    if target_type is int:
        return int(float(raw))
    if target_type is float:
        return float(raw)
    if target_type is Path:
        return Path(str(raw))
    return str(raw)


def _parse_bool(value: object, what: str) -> bool:
    # Stricter than _coerce: only a real JSON bool or the strings "true"/"false" (case-insensitive)
    # are accepted. Used for venues.json's rate_limits.shared, which must fail closed on nonsense
    # (e.g. null) rather than silently defaulting.
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip().lower() in ("true", "false"):
        return value.strip().lower() == "true"
    raise ValueError(f"{what}: rate_limits.shared must be true or false, got {value!r}")


def _read_json(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"{path}: invalid JSON: {e}") from e


def _load_venues(path: Path, env: Mapping[str, str]) -> tuple[VenueConfig, ...]:
    # Venue order follows venues.json key order (json.load preserves it); trade_venues/feed_venues keep that order.
    raw = _read_json(path)
    if not isinstance(raw, dict):
        raise ValueError(f"{path}: expected a JSON object of venue-name -> config")
    out = []
    for name, v in raw.items():
        rl = v.get("rate_limits") or {}
        prefix = name.upper()
        role = v.get("role", "off")
        # roles are compared verbatim (no case folding): venues.json is operator-authored and must be exact
        if role not in ("trade", "quote_only", "off"):
            raise ValueError(f"{name}: unknown role {role!r} (expected trade | quote_only | off)")
        limits = RateLimits(
            orders=int(rl.get("orders", 20)), cancels=int(rl.get("cancels", 20)),
            window_s=float(rl.get("window_s", 2.0)), reserve=int(rl.get("reserve", 4)),
            shared=_parse_bool(rl.get("shared", False), name))
        if not (limits.window_s > 0) or limits.orders <= 0 or limits.cancels <= 0:
            raise ValueError(f"{name}: rate_limits window_s, orders and cancels must be positive")
        if not (0 <= limits.reserve < min(limits.orders, limits.cancels)):
            raise ValueError(f"{name}: rate_limits.reserve must be in [0, min(orders, cancels)) — a reserve "
                             f"equal to the capacity would silently block every entry")
        out.append(VenueConfig(
            name=name,
            role=role,
            taker_fee_pct=float(v["taker_fee_pct"]),
            maker_fee_pct=float(v["maker_fee_pct"]),
            rate_limits=limits,
            max_topics=int(v.get("max_topics", 50)),
            min_requote_ms=int(v.get("min_requote_ms", 500)),
            staleness_override_s=(float(v["staleness_override_s"]) if v.get("staleness_override_s") is not None else None),
            symbol_whitelist=tuple(v.get("symbol_whitelist") or ()),
            api_key=env.get(f"{prefix}_API_KEY", ""),
            api_secret=env.get(f"{prefix}_API_SECRET", ""),
            passphrase=env.get(f"{prefix}_PASSPHRASE", ""),
        ))
    return tuple(out)


def load_config(env: Mapping[str, str] | None = None) -> Config:
    env = dict(os.environ if env is None else env)
    overrides: dict[str, object] = {}
    types = {f.name: f.type for f in fields(Config)}
    type_map = {"str": str, "int": int, "float": float, "bool": bool, "Path": Path}

    def field_type(name: str):
        t = types[name]
        return type_map.get(t if isinstance(t, str) else getattr(t, "__name__", "str"), str)

    # 1) environment
    for f in fields(Config):
        key = f.name.upper()
        if key in env and f.name not in ("venues", "blocked_symbols"):
            overrides[f.name] = _coerce(env[key], field_type(f.name))
    data_dir = Path(overrides.get("data_dir", Config.data_dir))
    # 2) dashboard-editable file (process-level keys ignored)
    bot_cfg = data_dir / "bot_config.json"
    if bot_cfg.exists():
        for k, v in _read_json(bot_cfg).items():
            # venues/blocked_symbols are overwritten from their files below anyway; skipping them here is
            # belt-and-braces so the dashboard file can never feed the registry, even after a reordering
            if k.upper() in _PROCESS_ONLY or k.lower() not in types or k.lower() in ("venues", "blocked_symbols"):
                continue
            overrides[k.lower()] = _coerce(v, field_type(k.lower()))
    # MODE is process-level only; validate and normalize so a typo (e.g. "Paper ", "dry") never
    # silently routes to live trading, since downstream code branches on `cfg.mode == "paper"`.
    # Checked before any registry/blocklist file I/O so a bad MODE is always the first error raised,
    # and no file access happens on a run that's going to fail anyway.
    mode = str(overrides.get("mode", Config.mode)).strip().lower()
    if mode not in ("paper", "live"):
        raise ValueError(f"MODE must be 'paper' or 'live', got {overrides.get('mode')!r}")
    overrides["mode"] = mode
    # 3) registry + blocked list
    venues_file = Path(overrides.get("venues_file", Config.venues_file))
    if not venues_file.exists():
        raise FileNotFoundError(f"venues file not found: {venues_file}")
    overrides["venues"] = _load_venues(venues_file, env)
    blocked_file = Path(overrides.get("blocked_file", Config.blocked_file))
    if not blocked_file.exists():
        raise FileNotFoundError(f"blocked symbols file not found: {blocked_file}")
    raw_blocked = _read_json(blocked_file)
    if not isinstance(raw_blocked, list) or not all(isinstance(s, str) for s in raw_blocked):
        raise ValueError(f"{blocked_file}: expected a JSON list of symbol strings")
    overrides["blocked_symbols"] = frozenset(raw_blocked)
    return Config(**overrides)
