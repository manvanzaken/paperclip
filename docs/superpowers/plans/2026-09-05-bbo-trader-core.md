# BBO Trader — Plan 1: Paper-Complete Core — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the new event-driven taker/maker convergence trader as a self-contained package `deploy-bbo/` and run it end-to-end in **paper mode** on live MEXC + BloFin best-bid/ask feeds — every quote update evaluates the symbol, TT (taker/taker) or TM (rest one leg post-only, hedge the other on fill) is chosen per opportunity, fills arrive as order events, state is persisted in the dashboard's schema.

**Architecture:** One asyncio process. Sharded public WebSocket feeds → in-memory `QuoteBoard` → pure edge math (`edge.py`) and gates (`strategy.py`) produce an `Intent` per quote update → `Executor` runs the TT flow or the `PeggedMaker` flow against a venue `Trading` protocol and consumes `OrderEvent`s from a `PrivateFeed` protocol → `PositionBook` state machine → atomic JSON state (`real_state.json`). In paper mode the `Trading`/`PrivateFeed` protocols are implemented by `SimVenue` over the real quotes; Plan 2 adds the live MEXC/BloFin implementations behind the same protocols, Plan 3 adds quote-only venues and further trade venues.

**Tech Stack:** Python 3.12+ (3.14 on the Mac), asyncio, aiohttp (WS + REST), stdlib json/logging/dataclasses, pytest + pytest-asyncio (`asyncio_mode=auto`). No other runtime dependencies.

**Spec:** `docs/superpowers/specs/2026-09-05-bbo-taker-maker-trader-design.md` (read it first; formulas and thresholds come from there).

**Scope of this plan (spec "Build order" step 1):** everything above the venue protocols + public feeds for MEXC and BloFin + `SimVenue`. **Not in this plan:** live trading adapters (Plan 2), quote-only venue parsers and additional trade venues (Plan 3), Telegram is minimal (send + 4 commands).

**Working directory for every command below:** `deploy-bbo/` inside this worktree (`/Users/vandenboogaard/Claude projects/Claude Paperclip/.claude/worktrees/trader-realtime-bid-ask-f22eff/deploy-bbo`). Run tests with `.venv/bin/python -m pytest -q`. Commit after every task with the message given.

---

## Conventions (apply to every task)

- **Symbols** are normalized `BASEUSDT` (e.g. `BTCUSDT`). Venue instrument ids live only inside `venues/*.py` (`to_instrument` / `to_symbol`).
- **Venue names** are lower-case (`"mexc"`, `"blofin"`). **Leg naming:** `a` = the venue we **sell/short** (higher bid), `b` = the venue we **buy/long**. A `Position` keeps `venue_a`, `venue_b`.
- **Percent units:** every spread, fee and edge is in **percent points** (0.06 = 0.06%). Fees in `venues.json` are percent per leg.
- **Quantities** are venue contracts (`float`); USD notional = `qty × price × contract_size`.
- **Time:** `time.time()` floats (seconds). Every class that needs time takes a `clock` callable so tests can inject a `FakeClock`.
- **Pure modules** (`edge.py`, `sizing.py`, `budget.py`, `quotes.py`, `discovery.py`) do no I/O, no logging, no asyncio.
- **Logging markers** are the spec's grep-able upper-case words as the first token of the message (`TM_POST`, `TT_ENTER`, ...).
- Tests never touch the network. Fixture messages for parsers are literal dicts captured from the venues' documented shapes (Plan 2 replaces them with live captures).

## File structure

```
deploy-bbo/
├── README.md
├── requirements.txt                 # aiohttp, pytest, pytest-asyncio
├── pytest.ini                       # asyncio_mode = auto, testpaths = tests
├── start.sh                         # .venv/bin/python -m bbo_trader.main
├── config/
│   ├── venues.json                  # venue registry (role, fees, rate limits, ws caps, requote interval)
│   └── blocked_symbols.json         # delisted/broken symbols (seeded from real_trader DELISTED_SYMBOLS)
├── bbo_trader/
│   ├── __init__.py
│   ├── config.py        # Config/VenueConfig/RateLimits + load_config (env → bot_config.json → venues.json)
│   ├── models.py        # BBO, VenueSpec, Fees, OrderAck, OrderEvent, Intent, Position, state constants
│   ├── quotes.py        # QuoteBoard
│   ├── edge.py          # PURE edge math, mode choice, maker price pegs, tick rounding
│   ├── sizing.py        # PURE USD→contracts, pair matching, hedge qty
│   ├── budget.py        # PURE TokenBucket + RateBudget
│   ├── positions.py     # transition table, PositionBook, StateStore
│   ├── risk.py          # MismatchGuard, RiskManager (halt flags, blacklists, funding gate, balances)
│   ├── strategy.py      # PairEvaluator: evaluate_entry / evaluate_resting / evaluate_exit / scan
│   ├── discovery.py     # PURE universe from specs
│   ├── metrics.py       # LatencyHist, Metrics, CoverageWatchdog
│   ├── notify.py        # Telegram send + command poll
│   ├── execution.py     # Executor: TT flow, PeggedMaker flow, hedging, flatten, degraded retry
│   ├── app.py           # App: wiring of quotes → strategy → executor, sweep, state save
│   ├── main.py          # entry point: config, logging, legacy guard, asyncio.run(App.run)
│   └── venues/
│       ├── __init__.py
│       ├── base.py      # protocols + Venue bundle + VenuePosition
│       ├── ws.py        # generic sharded WS runner (Backoff, chunking, app pings)
│       ├── mexc.py      # public: parsers, MexcPublic feed, MexcMarket REST
│       ├── blofin.py    # public: parsers, BlofinPublic feed, BlofinMarket REST
│       ├── sim.py       # SimVenue: Trading + PrivateFeed over real quotes (paper mode)
│       └── registry.py  # build_venues(cfg, ...) → dict[name, Venue]
└── tests/
    ├── conftest.py      # FakeClock, mk_bbo, mk_spec, sample Config
    ├── test_config.py, test_models.py, test_quotes.py, test_edge.py, test_sizing.py, test_budget.py,
    ├── test_positions.py, test_risk.py, test_strategy.py, test_discovery.py, test_metrics.py,
    ├── test_ws.py, test_mexc_public.py, test_blofin_public.py, test_sim.py,
    ├── test_execution_tt.py, test_execution_tm.py, test_notify.py, test_app.py
```

---

### Task 1: Scaffold, venv, config loader

**Files:**
- Create: `deploy-bbo/requirements.txt`, `deploy-bbo/pytest.ini`, `deploy-bbo/.gitignore`, `deploy-bbo/bbo_trader/__init__.py`, `deploy-bbo/bbo_trader/venues/__init__.py`, `deploy-bbo/config/venues.json`, `deploy-bbo/config/blocked_symbols.json`, `deploy-bbo/bbo_trader/config.py`
- Test: `deploy-bbo/tests/__init__.py`, `deploy-bbo/tests/test_config.py`

- [x] **Step 1: Create the folder, venv, and dependency files**

```bash
cd "/Users/vandenboogaard/Claude projects/Claude Paperclip/.claude/worktrees/trader-realtime-bid-ask-f22eff"
mkdir -p deploy-bbo/bbo_trader/venues deploy-bbo/config deploy-bbo/tests
cd deploy-bbo
python3 -m venv .venv
printf 'aiohttp>=3.9\npytest>=8.0\npytest-asyncio>=0.23\n' > requirements.txt
.venv/bin/pip install -q -r requirements.txt
printf '[pytest]\nasyncio_mode = auto\ntestpaths = tests\n' > pytest.ini
printf '.venv/\n__pycache__/\n*.pyc\ndata/\n.pytest_cache/\n' > .gitignore
touch bbo_trader/__init__.py bbo_trader/venues/__init__.py tests/__init__.py
```

Expected: pip finishes without errors; `.venv/bin/python -c "import aiohttp, pytest_asyncio"` prints nothing.

- [x] **Step 2: Write the venue registry and blocked-symbol seed**

`config/venues.json` — only `mexc` and `blofin` are `trade` in this plan; every other venue is `off` until Plan 3 gives it an adapter. Fees are the SpreadWatch tables (percent per leg, verify against live accounts). Rate limits carry ~10% headroom under the documented venue limits (MEXC 20/2 s → 18, BloFin 30/10 s → 27) because our window is measured at send time and the venue's at receive time.

```json
{
  "mexc":   {"role": "trade", "taker_fee_pct": 0.02, "maker_fee_pct": 0.00,
             "rate_limits": {"orders": 18, "cancels": 18, "window_s": 2.0, "reserve": 4, "shared": false},
             "max_topics": 30, "min_requote_ms": 500},
  "blofin": {"role": "trade", "taker_fee_pct": 0.06, "maker_fee_pct": 0.02,
             "rate_limits": {"orders": 27, "cancels": 27, "window_s": 10.0, "reserve": 6, "shared": true},
             "max_topics": 50, "min_requote_ms": 1000},
  "okx":    {"role": "off", "taker_fee_pct": 0.05, "maker_fee_pct": 0.02,
             "rate_limits": {"orders": 60, "cancels": 60, "window_s": 2.0, "reserve": 6, "shared": false},
             "max_topics": 50, "min_requote_ms": 500, "symbol_whitelist": []},
  "gate":   {"role": "off", "taker_fee_pct": 0.05, "maker_fee_pct": 0.02,
             "rate_limits": {"orders": 20, "cancels": 20, "window_s": 2.0, "reserve": 4, "shared": false},
             "max_topics": 25, "min_requote_ms": 500},
  "bitget": {"role": "off", "taker_fee_pct": 0.06, "maker_fee_pct": 0.02,
             "rate_limits": {"orders": 10, "cancels": 10, "window_s": 1.0, "reserve": 3, "shared": false},
             "max_topics": 50, "min_requote_ms": 500},
  "bingx":  {"role": "off", "taker_fee_pct": 0.05, "maker_fee_pct": 0.02,
             "rate_limits": {"orders": 10, "cancels": 10, "window_s": 1.0, "reserve": 3, "shared": false},
             "max_topics": 200, "min_requote_ms": 500},
  "kucoin": {"role": "off", "taker_fee_pct": 0.06, "maker_fee_pct": 0.02,
             "rate_limits": {"orders": 30, "cancels": 30, "window_s": 3.0, "reserve": 4, "shared": false},
             "max_topics": 50, "min_requote_ms": 500},
  "htx":    {"role": "off", "taker_fee_pct": 0.05, "maker_fee_pct": 0.02,
             "rate_limits": {"orders": 20, "cancels": 20, "window_s": 3.0, "reserve": 4, "shared": false},
             "max_topics": 30, "min_requote_ms": 500},
  "bybit":  {"role": "off", "taker_fee_pct": 0.055, "maker_fee_pct": 0.02,
             "rate_limits": {"orders": 10, "cancels": 10, "window_s": 1.0, "reserve": 2, "shared": false},
             "max_topics": 50, "min_requote_ms": 500},
  "binance": {"role": "off", "taker_fee_pct": 0.05, "maker_fee_pct": 0.02,
             "rate_limits": {"orders": 10, "cancels": 10, "window_s": 1.0, "reserve": 2, "shared": false},
             "max_topics": 50, "min_requote_ms": 500},
  "hyperliquid": {"role": "off", "taker_fee_pct": 0.045, "maker_fee_pct": 0.015,
             "rate_limits": {"orders": 10, "cancels": 10, "window_s": 1.0, "reserve": 2, "shared": false},
             "max_topics": 0, "min_requote_ms": 500, "staleness_override_s": 10.0}
}
```

`config/blocked_symbols.json` — copy the `DELISTED_SYMBOLS` set from `deploy-live/real_trader.py` (branch `claude/tender-germain`, lines ~285-320) as a JSON list. Generate it instead of retyping:

```bash
cd "/Users/vandenboogaard/Claude projects/Claude Paperclip/.claude/worktrees/trader-realtime-bid-ask-f22eff"
git show claude/tender-germain:deploy-live/real_trader.py | python3 -c '
import re, sys, json
src = sys.stdin.read()
block = src[src.index("DELISTED_SYMBOLS: set = {"):]
block = block[:block.index("}")]
syms = sorted(set(re.findall(r"\"([A-Z0-9]+USDT)\"", block)))
json.dump(syms, open("deploy-bbo/config/blocked_symbols.json", "w"), indent=0)
print(len(syms), "blocked symbols written")'
```

Expected: `143 blocked symbols written` (the count follows the legacy list; anything between 130 and 180 is fine).

- [x] **Step 3: Write the failing config tests**

`tests/test_config.py`:

```python
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
    venues.write_text(json.dumps({"mexc": {**base, "rate_limits": {"orders": 27, "cancels": 5, "window_s": 10, "reserve": 4, "shared": True}}}))
    with pytest.raises(ValueError, match="orders == cancels"):
        load_config(env=env)
```

- [x] **Step 4: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_config.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'bbo_trader.config'` (15 tests collected, all erroring at import)

- [x] **Step 5: Implement `bbo_trader/config.py`**

```python
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
    strike_decay_s: float = 21_600.0          # strikes older than this are forgotten
    symbol_loss_blacklist_s: float = 21_600.0
    symbol_loss_pct: float = -0.10            # a close worse than this % of size blacklists the symbol
    pair_stats_window_s: float = 86_400.0     # win-rate gate looks at closes inside this window (so a route can recover)
    pair_min_trades: int = 5                  # ...once it has at least this many closes in the window
    pair_min_win_rate: float = 0.30           # ...and blocks below this win rate
    balance_max_age_s: float = 120.0          # live: a balance cache older than this fails closed ("balance_unknown")
    funding_block_min_pct: float = 0.01       # funding gate blocks only above this net cost (percent points)
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
        if limits.shared and limits.orders != limits.cancels:
            raise ValueError(f"{name}: shared rate limits must set orders == cancels (cancels is ignored)")
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
```

- [x] **Step 6: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_config.py -q`
Expected: `15 passed`

- [x] **Step 7: Commit**

```bash
cd "/Users/vandenboogaard/Claude projects/Claude Paperclip/.claude/worktrees/trader-realtime-bid-ask-f22eff"
git add deploy-bbo
git commit -m "feat(bbo): scaffold deploy-bbo package with config loader and venue registry"
```

---

### Task 2: Core data types (`models.py`)

**Files:**
- Create: `deploy-bbo/bbo_trader/models.py`
- Test: `deploy-bbo/tests/conftest.py`, `deploy-bbo/tests/test_models.py`

- [x] **Step 1: Write shared test helpers**

`tests/conftest.py`:

```python
import time

import pytest

from bbo_trader.models import BBO, VenueSpec, Fees


class FakeClock:
    """Deterministic clock: call it like time.time(); advance with .tick()."""

    def __init__(self, start: float = 1_700_000_000.0):
        self.t = start

    def __call__(self) -> float:
        return self.t

    def tick(self, seconds: float) -> float:
        self.t += seconds
        return self.t


def mk_bbo(venue: str, symbol: str, bid: float, ask: float, bq: float = 1000.0, aq: float = 1000.0,
           ts: float | None = None, contract_size: float = 1.0, ts_exchange: float | None = None) -> BBO:
    """ts = local receive time (governs staleness); ts_exchange defaults to ts unless given."""
    t = time.time() if ts is None else ts
    return BBO(venue=venue, symbol=symbol, bid=bid, bid_qty=bq, ask=ask, ask_qty=aq,
               ts_exchange=t if ts_exchange is None else ts_exchange, ts_local=t, contract_size=contract_size)


def mk_spec(venue: str, symbol: str = "XYZUSDT", contract_size: float = 1.0, lot: float = 1.0,
            min_qty: float = 1.0, tick: float = 0.0001) -> VenueSpec:
    return VenueSpec(venue=venue, symbol=symbol, instrument=symbol, contract_size=contract_size,
                     lot=lot, min_qty=min_qty, tick=tick)


MEXC_FEES = Fees(taker=0.02, maker=0.00)
BLOFIN_FEES = Fees(taker=0.06, maker=0.02)


@pytest.fixture
def clock():
    return FakeClock()
```

- [x] **Step 2: Write the failing model tests**

`tests/test_models.py`:

```python
import json

import pytest

from bbo_trader.models import (BBO, OrderEvent, Intent, Position, OPEN, TT_ENTERING, CLOSED, NON_TERMINAL, none)
from tests.conftest import mk_bbo


def test_bbo_derived_values():
    q = mk_bbo("mexc", "XYZUSDT", bid=1.0, ask=1.002, bq=500, aq=200, contract_size=10)
    assert q.ok
    assert abs(q.mid - 1.001) < 1e-12
    assert abs(q.width_pct - (0.002 / 1.002 * 100)) < 1e-9
    assert q.touch_notional("sell") == 1.0 * 500 * 10      # selling hits the bid
    assert q.touch_notional("buy") == 1.002 * 200 * 10     # buying lifts the ask
    assert not mk_bbo("mexc", "XYZUSDT", bid=1.0, ask=0.99).ok  # crossed book is not ok


def test_order_event_terminal():
    assert OrderEvent("mexc", "c1", "o1", "filled").terminal
    assert not OrderEvent("mexc", "c1", "o1", "partial").terminal
    assert OrderEvent("mexc", "c1", "o1", "rejected", error="would cross").terminal


def test_intent_none_helper():
    i = none("below_edge")
    assert isinstance(i, Intent) and i.kind == "NONE" and i.reason == "below_edge"


def test_position_round_trip_and_dashboard_keys():
    p = Position(id=7, symbol="XYZUSDT", venue_a="blofin", venue_b="mexc", status=TT_ENTERING, mode="TT",
                 size_usd=25.0, entry_time=1_700_000_000.0)
    p.status = OPEN
    p.entry_price_a, p.entry_price_b = 1.01, 1.0
    p.client_ids["entry_a"] = "bp7-entry_a-1"
    d = p.to_dict()
    # dashboard-compatible aliases
    assert d["exchange_short"] == "blofin" and d["exchange_long"] == "mexc"
    assert d["entry_price_short"] == 1.01 and d["entry_price_long"] == 1.0
    assert d["instrument_short"] == "PERP" and d["status"] == "OPEN"
    assert d["entry_time"].startswith("2023-11-14T22:13:20")
    back = Position.from_dict(d)
    assert back == p


def test_touch_notional_rejects_unknown_side():
    q = mk_bbo("mexc", "XYZUSDT", 1.0, 1.001)
    with pytest.raises(ValueError, match="side must be"):
        q.touch_notional("SELL")


def test_position_json_round_trip_iso_fallback_and_unknown_keys():
    p = Position(id=1, symbol="XYZUSDT", venue_a="blofin", venue_b="mexc", status=OPEN, mode="TM",
                 entry_time=1_700_000_000.0)
    p.client_ids["maker"] = "c1"
    p.venue_position_ids["mexc"] = "12345"
    d = json.loads(json.dumps(p.to_dict()))
    back = Position.from_dict(d)
    assert back == p and back.client_ids is not d["client_ids"]        # no aliasing with the source dict
    assert d["exit_time"] is None                                       # open position: dashboard shows a dash
    d.pop("_entry_ts")
    d.pop("_exit_ts")                                                   # hand-repaired state file without shadow keys
    back2 = Position.from_dict(d)
    assert back2.entry_time == 1_700_000_000.0 and back2.exit_time == 0.0
    assert Position.from_dict({**d, "future_key": 1}).id == 1          # unknown keys ignored


def test_state_constants():
    assert CLOSED not in NON_TERMINAL and OPEN in NON_TERMINAL and len(NON_TERMINAL) == 8
```

- [x] **Step 3: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_models.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'bbo_trader.models'`

- [x] **Step 4: Implement `bbo_trader/models.py`**

```python
"""Core data types. Pure dataclasses, no I/O."""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone

# ---- position states -------------------------------------------------------
TT_ENTERING = "TT_ENTERING"
MAKER_RESTING = "MAKER_RESTING"
HEDGING = "HEDGING"
OPEN = "OPEN"
EXIT_MAKER_RESTING = "EXIT_MAKER_RESTING"
TT_EXITING = "TT_EXITING"
EXIT_HEDGING = "EXIT_HEDGING"
DEGRADED = "DEGRADED"
CLOSED = "CLOSED"
NON_TERMINAL = frozenset({TT_ENTERING, MAKER_RESTING, HEDGING, OPEN, EXIT_MAKER_RESTING, TT_EXITING, EXIT_HEDGING, DEGRADED})


@dataclass(frozen=True)
class BBO:
    venue: str
    symbol: str
    bid: float
    bid_qty: float      # contracts resting at the best bid
    ask: float
    ask_qty: float      # contracts resting at the best ask
    ts_exchange: float  # seconds, the venue's own timestamp (informational)
    ts_local: float     # seconds from the LOCAL clock at receipt — governs staleness; adapters must never
                        # stamp this from the venue clock (a future-dated ts_local would be fresh forever)
    contract_size: float = 1.0

    @property
    def ok(self) -> bool:
        return self.bid > 0 and self.ask > 0 and self.ask >= self.bid

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0

    @property
    def width_pct(self) -> float:
        return (self.ask - self.bid) / self.ask * 100.0 if self.ask > 0 else 0.0

    def touch_notional(self, side: str) -> float:
        """USD resting at the touch we would CROSS: 'sell' hits the bid, 'buy' lifts the ask."""
        if side == "sell":
            return self.bid * self.bid_qty * self.contract_size
        if side == "buy":
            return self.ask * self.ask_qty * self.contract_size
        raise ValueError(f"side must be 'buy' or 'sell', got {side!r}")


@dataclass(frozen=True)
class VenueSpec:
    venue: str
    symbol: str
    instrument: str       # venue-native id
    contract_size: float  # base units per contract
    lot: float            # size step (contracts)
    min_qty: float        # minimum order (contracts)
    tick: float           # price step


@dataclass(frozen=True)
class Fees:
    taker: float  # percent per leg
    maker: float  # percent per leg


@dataclass(frozen=True)
class OrderAck:
    ok: bool
    order_id: str = ""
    error: str = ""
    latency_ms: float = 0.0


@dataclass(frozen=True)
class OrderEvent:
    venue: str
    client_id: str
    order_id: str
    state: str            # ack | partial | filled | canceled | rejected
    filled_qty: float = 0.0   # cumulative contracts
    avg_price: float = 0.0    # cumulative average fill price
    fee: float = 0.0          # cumulative fee in USD (positive = cost)
    liquidity: str = ""       # maker | taker | ""
    position_id: str = ""     # venue position id (MEXC hedge mode)
    ts: float = 0.0
    error: str = ""

    @property
    def terminal(self) -> bool:
        return self.state in ("filled", "canceled", "rejected")


@dataclass(frozen=True)
class Intent:
    kind: str  # TT_ENTER | TM_ENTER | TT_EXIT | TM_EXIT | REQUOTE | CANCEL | UPGRADE_TT | NONE
    reason: str = ""
    symbol: str = ""
    venue_a: str = ""     # short venue
    venue_b: str = ""     # long venue
    maker_venue: str = ""
    rest_price: float = 0.0
    size_usd: float = 0.0
    edge_pct: float = 0.0
    spread_pct: float = 0.0
    ts: float = 0.0


def none(reason: str) -> Intent:
    return Intent(kind="NONE", reason=reason)


def _iso(ts: float) -> str | None:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts else None


def _ts_from(d: dict, private: str, iso_key: str) -> float:
    """Machine timestamp from the private shadow key, falling back to the dashboard's ISO string
    (a hand-repaired state file may carry only the latter; 0.0 would trigger an instant timeout exit)."""
    v = d.get(private)
    if v:
        return float(v)
    s = d.get(iso_key)
    return datetime.fromisoformat(s).timestamp() if isinstance(s, str) and s else 0.0


@dataclass
class Position:
    """One two-leg position through its whole life. `to_dict()` is the DASHBOARD view (legacy aliases,
    ISO times); the machine-readable timestamps travel in the private `_entry_ts`/`_exit_ts` keys and
    `from_dict()` prefers them. Unknown keys are ignored on load (forward compatibility)."""
    id: int
    symbol: str
    venue_a: str          # short leg venue
    venue_b: str          # long leg venue
    status: str
    mode: str             # entry mode: TT | TM
    size_usd: float = 0.0
    qty_a: float = 0.0    # target contracts per leg
    qty_b: float = 0.0
    filled_a: float = 0.0  # entry fills (contracts)
    filled_b: float = 0.0
    entry_price_a: float = 0.0
    entry_price_b: float = 0.0
    entry_fees_usd: float = 0.0
    exit_fees_usd: float = 0.0
    detect_spread_pct: float = 0.0
    entry_spread_pct: float = 0.0
    current_spread_pct: float = 0.0
    peak_spread_pct: float = 0.0
    entry_time: float = 0.0
    exit_time: float = 0.0
    exit_spread_pct: float = 0.0
    exit_price_a: float = 0.0
    exit_price_b: float = 0.0
    exit_filled_a: float = 0.0
    exit_filled_b: float = 0.0
    exit_reason: str = ""
    exit_mode: str = ""
    gross_pnl_usd: float = 0.0
    net_pnl_usd: float = 0.0
    # resting maker bookkeeping (entry or exit; one resting order at a time)
    maker_venue: str = ""
    maker_client_id: str = ""
    maker_order_id: str = ""
    maker_side: str = ""            # buy | sell
    maker_qty: float = 0.0
    maker_rest_price: float = 0.0
    maker_filled_qty: float = 0.0
    maker_avg_price: float = 0.0
    hedged_qty: float = 0.0         # maker contracts already hedged
    maker_fee_usd: float = 0.0      # cumulative fee on the resting order
    maker_cancel_sent: bool = False
    requote_pending: bool = False   # cancel+new requote in flight (venues without amend)
    requote_price: float = 0.0
    maker_posted_ts: float = 0.0
    maker_last_requote_ts: float = 0.0
    maker_fill_ts: float = 0.0
    edge_gone_since: float = 0.0
    upgrade_pending: bool = False
    # ids and analytics
    client_ids: dict = field(default_factory=dict)          # leg -> client id
    order_ids: dict = field(default_factory=dict)           # leg -> venue order id
    venue_position_ids: dict = field(default_factory=dict)  # venue -> position id
    fee_liquidity: dict = field(default_factory=dict)       # leg -> maker | taker
    latency_ms: dict = field(default_factory=dict)          # stage -> ms
    degraded_leg: str = ""    # a | b | both
    close_retry_count: int = 0
    last_close_attempt: float = 0.0
    signal_ts: float = 0.0

    def to_dict(self) -> dict:
        d = asdict(self)
        d.update({
            # dashboard-compatible aliases (real_trader schema)
            "exchange_short": self.venue_a, "exchange_long": self.venue_b,
            "instrument_short": "PERP", "instrument_long": "PERP",
            "entry_price_short": self.entry_price_a, "entry_price_long": self.entry_price_b,
            "exit_price_short": self.exit_price_a, "exit_price_long": self.exit_price_b,
            "order_id_short": self.order_ids.get("entry_a", ""), "order_id_long": self.order_ids.get("entry_b", ""),
            "order_id_close_short": self.order_ids.get("exit_a", ""), "order_id_close_long": self.order_ids.get("exit_b", ""),
            "entry_time": _iso(self.entry_time), "exit_time": _iso(self.exit_time),
            "fill_latency_short_ms": self.latency_ms.get("entry_a", 0.0),
            "fill_latency_long_ms": self.latency_ms.get("entry_b", 0.0),
        })
        d["_entry_ts"] = self.entry_time
        d["_exit_ts"] = self.exit_time
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "Position":
        names = {f for f in cls.__dataclass_fields__}
        kw = {k: (dict(v) if isinstance(v, dict) else v) for k, v in d.items() if k in names}  # no aliasing
        kw["entry_time"] = _ts_from(d, "_entry_ts", "entry_time")
        kw["exit_time"] = _ts_from(d, "_exit_ts", "exit_time")
        return cls(**kw)
```

- [x] **Step 5: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_models.py -q`
Expected: `7 passed`

- [x] **Step 6: Commit**

```bash
git add deploy-bbo/bbo_trader/models.py deploy-bbo/tests/conftest.py deploy-bbo/tests/test_models.py
git commit -m "feat(bbo): core data types (BBO, OrderEvent, Intent, Position) with dashboard aliases"
```

---

### Task 3: QuoteBoard (`quotes.py`)

**Files:**
- Create: `deploy-bbo/bbo_trader/quotes.py`
- Test: `deploy-bbo/tests/test_quotes.py`

- [x] **Step 1: Write the failing test**

`tests/test_quotes.py`:

```python
from bbo_trader.quotes import QuoteBoard
from tests.conftest import mk_bbo


def test_set_get_and_staleness():
    board = QuoteBoard(stale_s=2.0, overrides={"hyperliquid": 10.0})
    assert board.set(mk_bbo("mexc", "XYZUSDT", 1.0, 1.001, ts=100.0))
    assert board.set(mk_bbo("blofin", "XYZUSDT", 1.0, 1.002, ts=100.0))
    assert board.set(mk_bbo("hyperliquid", "XYZUSDT", 1.0, 1.003, ts=95.0))
    assert not board.set(mk_bbo("mexc", "ABCUSDT", 1.0, 0.9))          # crossed book ignored
    assert board.get("mexc", "XYZUSDT").ask == 1.001
    assert board.fresh("mexc", "XYZUSDT", now=101.5) is not None
    assert board.fresh("mexc", "XYZUSDT", now=102.5) is None            # 2.5 s old > 2.0
    assert board.fresh("hyperliquid", "XYZUSDT", now=104.0) is not None  # override 10 s
    assert board.fresh_venues("XYZUSDT", now=101.0) == ["blofin", "hyperliquid", "mexc"]
    assert board.fresh_venues("XYZUSDT", now=103.0) == ["hyperliquid"]
    assert board.fresh_counts(now=103.0) == {"mexc": 0, "blofin": 0, "hyperliquid": 1}
    assert board.symbols() == {"XYZUSDT"}
    # staleness is governed by ts_local, never by the venue clock
    assert board.set(mk_bbo("mexc", "PQRUSDT", 1.0, 1.001, ts=100.0, ts_exchange=1.0))
    assert board.fresh("mexc", "PQRUSDT", now=101.0) is not None
    assert board.fresh("mexc", "XYZUSDT", now=102.0) is not None   # boundary: age == stale_s is still fresh
    assert board.set(mk_bbo("mexc", "XYZUSDT", 2.0, 2.001, ts=110.0))  # newest quote overwrites
    assert board.get("mexc", "XYZUSDT").bid == 2.0
```

- [x] **Step 2: Run the test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_quotes.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'bbo_trader.quotes'`

- [x] **Step 3: Implement `bbo_trader/quotes.py`**

```python
"""QuoteBoard: latest best bid/ask per venue×symbol with staleness. Pure, no I/O."""
from __future__ import annotations

from .models import BBO


class QuoteBoard:
    def __init__(self, stale_s: float, overrides: dict[str, float] | None = None):
        self.stale_s = stale_s
        self.overrides = dict(overrides or {})
        self._q: dict[tuple[str, str], BBO] = {}
        self._venues_by_symbol: dict[str, set[str]] = {}

    def stale_for(self, venue: str) -> float:
        return self.overrides.get(venue, self.stale_s)

    def set(self, bbo: BBO) -> bool:
        """Store a quote. Crossed/empty books are ignored (returns False)."""
        if not bbo.ok:
            return False
        self._q[(bbo.venue, bbo.symbol)] = bbo
        self._venues_by_symbol.setdefault(bbo.symbol, set()).add(bbo.venue)
        return True

    def get(self, venue: str, symbol: str) -> BBO | None:
        return self._q.get((venue, symbol))

    def is_fresh(self, bbo: BBO, now: float) -> bool:
        return (now - bbo.ts_local) <= self.stale_for(bbo.venue)

    def fresh(self, venue: str, symbol: str, now: float) -> BBO | None:
        q = self._q.get((venue, symbol))
        return q if q is not None and self.is_fresh(q, now) else None

    def fresh_venues(self, symbol: str, now: float) -> list[str]:
        return sorted(v for v in self._venues_by_symbol.get(symbol, ())
                      if self.fresh(v, symbol, now) is not None)

    def fresh_counts(self, now: float) -> dict[str, int]:
        out: dict[str, int] = {}
        for (venue, _symbol), q in self._q.items():
            out.setdefault(venue, 0)
            if self.is_fresh(q, now):
                out[venue] += 1
        return out

    def symbols(self) -> set[str]:
        return set(self._venues_by_symbol)
```

- [x] **Step 4: Run the test to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_quotes.py -q`
Expected: `1 passed`

- [x] **Step 5: Commit**

```bash
git add deploy-bbo/bbo_trader/quotes.py deploy-bbo/tests/test_quotes.py
git commit -m "feat(bbo): QuoteBoard with per-venue staleness"
```

---

### Task 4: Edge math, mode selection, maker price pegs (`edge.py`)

**Files:**
- Create: `deploy-bbo/bbo_trader/edge.py`
- Test: `deploy-bbo/tests/test_edge.py`

The formulas are the spec's, verbatim. Read the spec section "Strategy: edge math and mode selection" before this task; the worked example (0.41% TT floor, 0.42% TM floor on MEXC↔BloFin) is encoded in the tests.

- [x] **Step 1: Write the failing tests**

`tests/test_edge.py`:

```python
import pytest

from bbo_trader.edge import (EdgeParams, evaluate_pair, maker_entry_price, maker_exit_price,
                             exit_spread_tt, needs_requote, best_fee_venue, round_up, round_down,
                             tick_decimals, spread_pct, tm_required_pct)
from bbo_trader.models import Fees
from tests.conftest import mk_bbo, MEXC_FEES, BLOFIN_FEES

P = EdgeParams(min_edge_pct=0.05, tm_extra_edge_pct=0.05, exit_spread_pct=0.15, slip_pct=0.05)


def test_tt_wins_when_spread_clears_all_costs():
    # spec worked example: TT needs spread_tt >= 0.41% on blofin(short)/mexc(long)
    qa = mk_bbo("blofin", "XYZUSDT", bid=1.0050, ask=1.0060)
    qb = mk_bbo("mexc", "XYZUSDT", bid=1.0000, ask=1.0008)
    pe = evaluate_pair(qa, qb, BLOFIN_FEES, MEXC_FEES, P)
    assert pe.spread_tt == pytest.approx(spread_pct(1.0050, 1.0008))
    assert pe.edge_tt == pytest.approx(pe.spread_tt - 0.08 - 0.28)
    assert pe.mode == "TT" and pe.maker_venue == "" and pe.edge == pytest.approx(pe.edge_tt)


def test_tm_on_wide_venue_when_tt_does_not_pay():
    # 0.31% raw spread, blofin book 0.2% wide: nothing for TT, TM on blofin clears
    qa = mk_bbo("blofin", "XYZUSDT", bid=1.0041, ask=1.0061)
    qb = mk_bbo("mexc", "XYZUSDT", bid=1.0000, ask=1.0010)
    pe = evaluate_pair(qa, qb, BLOFIN_FEES, MEXC_FEES, P)
    assert pe.edge_tt < P.min_edge_pct
    assert pe.edge_tm_a == pytest.approx(spread_pct(1.0061, 1.0010) - 0.04 - 0.28)
    assert pe.edge_tm_b == pytest.approx(spread_pct(1.0041, 1.0000) - 0.06 - 0.28)
    assert pe.mode == "TM" and pe.maker_venue == "blofin" and pe.edge == pytest.approx(pe.edge_tm_a)


def test_policy_forces_maker_venue_or_disables():
    qa = mk_bbo("blofin", "XYZUSDT", bid=1.0041, ask=1.0061)
    qb = mk_bbo("mexc", "XYZUSDT", bid=1.0000, ask=1.0010)
    forced = evaluate_pair(qa, qb, BLOFIN_FEES, MEXC_FEES, EdgeParams(0.05, 0.05, 0.15, 0.05, maker_venue_policy="mexc"))
    assert forced.mode == ""                     # mexc-maker edge 0.07 < 0.10
    off = evaluate_pair(qa, qb, BLOFIN_FEES, MEXC_FEES, EdgeParams(0.05, 0.05, 0.15, 0.05, tm_enabled=False))
    assert off.mode == ""
    foreign = evaluate_pair(qa, qb, BLOFIN_FEES, MEXC_FEES, EdgeParams(0.05, 0.05, 0.15, 0.05, maker_venue_policy="okx"))
    assert foreign.mode == ""


def test_maker_entry_price_joins_touch_or_floors_at_edge():
    qa = mk_bbo("blofin", "XYZUSDT", bid=1.0041, ask=1.0061)
    qb = mk_bbo("mexc", "XYZUSDT", bid=1.0000, ask=1.0010)
    # req = 0.05+0.05+0.02(maker blofin)+0.02(taker mexc)+0.28 = 0.42% over mexc ask 1.0010 -> 1.0052042
    assert maker_entry_price(qa, qb, BLOFIN_FEES, MEXC_FEES, P, "blofin", tick=0.0001) == pytest.approx(1.0061)
    qa_low = mk_bbo("blofin", "XYZUSDT", bid=1.0041, ask=1.0050)      # touch below the floor -> rest at floor
    assert maker_entry_price(qa_low, qb, BLOFIN_FEES, MEXC_FEES, P, "blofin", tick=0.0001) == pytest.approx(1.0053)
    # improve one tick: 1.0060 still above the floor and above the bid
    assert maker_entry_price(qa, qb, BLOFIN_FEES, MEXC_FEES, P, "blofin", tick=0.0001, improve_ticks=1) == pytest.approx(1.0060)
    # make on mexc (buy): cap = 1.0041 / 1.0044 = 0.99970... -> 0.9997 (below mexc bid, still < ask)
    assert maker_entry_price(qa, qb, BLOFIN_FEES, MEXC_FEES, P, "mexc", tick=0.0001) == pytest.approx(0.9997)
    # would-cross guard: improving one tick inside a one-tick-wide book would sit on the bid -> None
    qa_tight = mk_bbo("blofin", "XYZUSDT", bid=1.0053, ask=1.0054)
    assert maker_entry_price(qa_tight, qb, BLOFIN_FEES, MEXC_FEES, P, "blofin", tick=0.0001, improve_ticks=1) is None
    assert maker_entry_price(qa_tight, qb, BLOFIN_FEES, MEXC_FEES, P, "blofin", tick=0.0001) == pytest.approx(1.0054)
    assert maker_entry_price(qa, qb, BLOFIN_FEES, MEXC_FEES, P, "okx", tick=0.0001) is None


def test_maker_exit_price_both_sides():
    qa = mk_bbo("blofin", "XYZUSDT", bid=1.0020, ask=1.0030)
    qb = mk_bbo("mexc", "XYZUSDT", bid=1.0000, ask=1.0005)
    assert exit_spread_tt(qa, qb) == pytest.approx(spread_pct(1.0030, 1.0000))
    # rest BUY on blofin at <= mexc bid × 1.0015 = 1.0015 (below blofin bid, fine)
    assert maker_exit_price(qa, qb, 0.15, "blofin", tick=0.0001) == pytest.approx(1.0015)
    # rest SELL on mexc at >= blofin ask / 1.0015 = 1.001498 -> 1.0015 (above mexc ask)
    assert maker_exit_price(qa, qb, 0.15, "mexc", tick=0.0001) == pytest.approx(1.0015)
    # joining never crosses (px <= bid < ask); improving inside a one-tick book would -> None
    qa2 = mk_bbo("blofin", "XYZUSDT", bid=1.0014, ask=1.0015)
    assert maker_exit_price(qa2, qb, 0.15, "blofin", tick=0.0001) == pytest.approx(1.0014)
    assert maker_exit_price(qa2, qb, 0.15, "blofin", tick=0.0001, improve_ticks=1) is None
    assert maker_exit_price(qa, qb, 0.15, "okx", tick=0.0001) is None


def test_rounding_and_requote_helpers():
    assert tick_decimals(0.0001) == 4 and tick_decimals(1.0) == 0 and tick_decimals(0.5) == 1
    assert round_up(1.00001, 0.0001) == pytest.approx(1.0001)
    assert round_up(1.0001, 0.0001) == pytest.approx(1.0001)      # already on tick
    assert round_down(1.00019, 0.0001) == pytest.approx(1.0001)
    assert needs_requote(1.0000, 1.0001, 0.0001, 1)
    assert not needs_requote(1.0000, 1.00005, 0.0001, 1)
    assert not needs_requote(1.0000, 1.0001, 0.0001, 2)
    assert best_fee_venue("blofin", BLOFIN_FEES, "mexc", MEXC_FEES) == "blofin"   # 0.04 vs 0.02 saving
    assert best_fee_venue("mexc", MEXC_FEES, "blofin", BLOFIN_FEES) == "blofin"


def test_degenerate_quote_never_yields_zero_peg():
    # a glitched/near-zero quote on A must never yield a postable 0.0 peg: the lo=0.0 bound of the BUY
    # branches rejects it (the extra `0.0 < px` clause in _postable is belt-and-braces for the SELL branches)
    qa = mk_bbo("blofin", "XYZUSDT", bid=1e-9, ask=2e-9)
    qb = mk_bbo("mexc", "XYZUSDT", bid=1.0, ask=1.001)
    assert maker_exit_price(qa, qb, 0.15, "blofin", tick=0.0001) is None
    assert maker_entry_price(qa, qb, BLOFIN_FEES, MEXC_FEES, P, "mexc", tick=0.0001) is None


def test_requote_detects_one_tick_at_high_price():
    # absolute epsilon breaks down at 6-figure prices; tolerance must scale with tick, not price
    assert needs_requote(105123.45, 105123.46, 0.01, 1) is True
    assert needs_requote(105123.45, 105123.45, 0.01, 1) is False
    with pytest.raises(ValueError):
        needs_requote(105123.45, 105123.46, 0, 1)


def test_tick_decimals_and_rounding_extremes():
    assert tick_decimals(1e-8) == 8
    assert tick_decimals(2.5e-7) == 8
    assert tick_decimals(1e-13) == 13
    assert tick_decimals(100.0) == 0
    # on-grid prices must round-trip through round_up/round_down unchanged at tiny/large scales
    assert round_up(0.00012345, 1e-8) == pytest.approx(0.00012345)
    assert round_down(0.00012345, 1e-8) == pytest.approx(0.00012345)
    assert round_up(1000.000123, 1e-6) == pytest.approx(1000.000123)
    assert round_down(10000.0001, 0.0001) == pytest.approx(10000.0001)
    # these discriminate the relative epsilon from a fixed 1e-9 (which mis-rounds them by a full tick)
    assert round_up(963.443702, 1e-6) == pytest.approx(963.443702)
    assert round_up(306776.34, 0.01) == pytest.approx(306776.34)
    assert round_down(82940652.27, 0.01) == pytest.approx(82940652.27)
    # ...and the epsilon cap keeps extreme px/tick ratios from flipping the rounding direction
    assert round_up(65000.0, 1e-8) == pytest.approx(65000.0) and round_down(65000.0, 1e-8) == pytest.approx(65000.0)
    with pytest.raises(ValueError):
        tick_decimals(0)
    with pytest.raises(ValueError):
        tick_decimals(float("nan"))
    with pytest.raises(ValueError):
        needs_requote(1.0, 1.1, float("nan"), 1)


def test_tm_viable_when_tt_spread_negative():
    # blofin book is 0.6% wide with an inverted (negative) TT spread: TT can't fire, but making
    # on the wide side of blofin still clears the required edge against mexc's ask
    qa = mk_bbo("blofin", "XYZUSDT", bid=0.9995, ask=1.0055)
    qb = mk_bbo("mexc", "XYZUSDT", bid=1.0000, ask=1.0010)
    pe = evaluate_pair(qa, qb, BLOFIN_FEES, MEXC_FEES, P)
    assert pe.spread_tt < 0
    assert pe.mode == "TM"
    assert pe.maker_venue == "blofin"
    px = maker_entry_price(qa, qb, BLOFIN_FEES, MEXC_FEES, P, "blofin", tick=0.0001)
    assert px is not None and px > qa.bid


def test_make_on_b_would_cross_guards():
    # entry make-on-B (BUY on mexc): improving 1 tick inside a one-tick mexc book lands exactly on
    # the ask -> would cross, must reject even though the edge-required cap is not binding here
    qa = mk_bbo("blofin", "XYZUSDT", bid=1.0060, ask=1.0070)
    qb = mk_bbo("mexc", "XYZUSDT", bid=1.0009, ask=1.0010)
    assert maker_entry_price(qa, qb, BLOFIN_FEES, MEXC_FEES, P, "mexc", tick=0.0001, improve_ticks=1) is None
    # exit make-on-B (SELL on mexc): improving 1 tick inside a one-tick mexc book lands exactly on
    # the bid -> would cross, must reject even though the exit-target floor is not binding here
    qa2 = mk_bbo("blofin", "XYZUSDT", bid=1.0020, ask=1.0025)
    qb2 = mk_bbo("mexc", "XYZUSDT", bid=1.0014, ask=1.0015)
    assert maker_exit_price(qa2, qb2, 0.15, "mexc", tick=0.0001, improve_ticks=1) is None


def test_best_fee_venue_tie_goes_to_a():
    assert best_fee_venue("x", Fees(0.05, 0.02), "y", Fees(0.06, 0.03)) == "x"


def test_peg_properties_on_a_grid():
    """Deterministic sweep (no randomness): every postable peg maker_entry_price/maker_exit_price
    return must (a) sit strictly on the resting-order side of its own book (never cross) and
    (b) actually realize the edge/exit target it was pegged to meet."""
    ticks = (1e-6, 1e-4, 0.01)
    mids = (0.001, 1.0, 250.0, 65000.0)
    width_ticks_opts = (1, 3, 20)
    offset_ticks_opts = (-30, -5, 0, 5, 30)
    checked = 0
    produced: dict[tuple, int] = {}
    for tick in ticks:
        for mid in mids:
            if tick >= mid / 100:
                continue
            for width_ticks in width_ticks_opts:
                qb = mk_bbo("mexc", "XYZUSDT", bid=mid, ask=mid + width_ticks * tick)
                for offset_ticks in offset_ticks_opts:
                    qa_bid = qb.bid + offset_ticks * tick
                    qa_ask = qb.bid + (offset_ticks + width_ticks) * tick
                    if qa_bid <= 0 or qa_ask <= 0 or qb.bid <= 0 or qb.ask <= 0:
                        continue
                    qa = mk_bbo("blofin", "XYZUSDT", bid=qa_bid, ask=qa_ask)
                    for maker_venue in ("blofin", "mexc"):
                        for improve_ticks in (0, 1):
                            checked += 1
                            entry_px = maker_entry_price(qa, qb, BLOFIN_FEES, MEXC_FEES, P,
                                                          maker_venue, tick, improve_ticks)
                            if entry_px is not None:
                                produced[("entry", maker_venue, improve_ticks)] = produced.get(("entry", maker_venue, improve_ticks), 0) + 1
                                if maker_venue == "blofin":
                                    assert entry_px > qa.bid
                                    req = tm_required_pct(BLOFIN_FEES, MEXC_FEES, P)
                                    assert spread_pct(entry_px, qb.ask) >= req - 1e-9
                                else:
                                    assert entry_px < qb.ask
                                    req = tm_required_pct(MEXC_FEES, BLOFIN_FEES, P)
                                    assert spread_pct(qa.bid, entry_px) >= req - 1e-9
                            exit_px = maker_exit_price(qa, qb, 0.15, maker_venue, tick, improve_ticks)
                            if exit_px is not None:
                                produced[("exit", maker_venue, improve_ticks)] = produced.get(("exit", maker_venue, improve_ticks), 0) + 1
                                if maker_venue == "blofin":
                                    assert exit_px < qa.ask
                                    assert spread_pct(exit_px, qb.bid) <= 0.15 + 1e-9
                                else:
                                    assert exit_px > qb.bid
                                    assert spread_pct(qa.ask, exit_px) <= 0.15 + 1e-9
    assert checked > 0
    # every (phase, venue, improve) cell must have produced pegs, or the invariants above were vacuous
    assert len(produced) == 8 and min(produced.values()) > 0
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_edge.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'bbo_trader.edge'`

- [x] **Step 3: Implement `bbo_trader/edge.py`**

```python
"""Pure edge math for the two execution modes, maker price pegs and tick rounding.

All spreads, fees and edges are percent points. `qa` is the venue we SELL on (higher bid),
`qb` the venue we BUY on. See the spec section "Strategy: edge math and mode selection".

All entry points assume qa.ok and qb.ok (positive, uncrossed quotes) and qa.symbol == qb.symbol."""
from __future__ import annotations

import math
from dataclasses import dataclass, replace
from decimal import Decimal

from .models import BBO, Fees


@dataclass(frozen=True)
class EdgeParams:
    min_edge_pct: float
    tm_extra_edge_pct: float
    exit_spread_pct: float
    slip_pct: float
    tt_enabled: bool = True
    tm_enabled: bool = True
    maker_venue_policy: str = "best_edge"  # "best_edge" or a venue name


@dataclass(frozen=True)
class PairEval:
    symbol: str
    venue_a: str
    venue_b: str
    spread_tt: float
    edge_tt: float
    spread_tm_a: float   # make on A: rest SELL at ask_A, hedge BUY at ask_B
    edge_tm_a: float
    spread_tm_b: float   # make on B: rest BUY at bid_B, hedge SELL at bid_A
    edge_tm_b: float
    mode: str = ""       # "TT" | "TM" | ""
    maker_venue: str = ""
    edge: float = 0.0    # edge of the chosen mode


def spread_pct(sell_px: float, buy_px: float) -> float:
    return (sell_px - buy_px) / buy_px * 100.0


def exit_spread_tt(qa: BBO, qb: BBO) -> float:
    """What closing at market costs now: buy back A at its ask, sell B at its bid."""
    return spread_pct(qa.ask, qb.bid)


def _base_cost(fa: Fees, fb: Fees, p: EdgeParams) -> float:
    """Conservative exit fees (taker/taker) + exit target + slippage allowance."""
    return fa.taker + fb.taker + p.exit_spread_pct + p.slip_pct


def evaluate_pair(qa: BBO, qb: BBO, fa: Fees, fb: Fees, p: EdgeParams) -> PairEval:
    base = _base_cost(fa, fb, p)
    s_tt = spread_pct(qa.bid, qb.ask)
    s_tm_a = spread_pct(qa.ask, qb.ask)
    s_tm_b = spread_pct(qa.bid, qb.bid)
    pe = PairEval(qa.symbol, qa.venue, qb.venue,
                  s_tt, s_tt - (fa.taker + fb.taker) - base,
                  s_tm_a, s_tm_a - (fa.maker + fb.taker) - base,
                  s_tm_b, s_tm_b - (fa.taker + fb.maker) - base)
    return choose_mode(pe, p)


def choose_mode(pe: PairEval, p: EdgeParams) -> PairEval:
    """TT first (no fill risk); else TM on the venue picked by policy; else nothing."""
    if p.tt_enabled and pe.edge_tt >= p.min_edge_pct:
        return replace(pe, mode="TT", maker_venue="", edge=pe.edge_tt)
    if not p.tm_enabled:
        return replace(pe, mode="", maker_venue="", edge=0.0)
    if p.maker_venue_policy == "best_edge":
        mv, e = ((pe.venue_a, pe.edge_tm_a) if pe.edge_tm_a >= pe.edge_tm_b
                 else (pe.venue_b, pe.edge_tm_b))
    elif p.maker_venue_policy == pe.venue_a:
        mv, e = pe.venue_a, pe.edge_tm_a
    elif p.maker_venue_policy == pe.venue_b:
        mv, e = pe.venue_b, pe.edge_tm_b
    else:
        return replace(pe, mode="", maker_venue="", edge=0.0)
    if e >= p.min_edge_pct + p.tm_extra_edge_pct:
        return replace(pe, mode="TM", maker_venue=mv, edge=e)
    return replace(pe, mode="", maker_venue="", edge=0.0)


def tm_required_pct(maker_fees: Fees, taker_fees: Fees, p: EdgeParams) -> float:
    """Percent the maker fill must clear over the hedge touch to meet the TM edge."""
    return (p.min_edge_pct + p.tm_extra_edge_pct + maker_fees.maker + taker_fees.taker
            + _base_cost(maker_fees, taker_fees, p))


def tick_decimals(tick: float) -> int:
    if not (tick > 0):   # also rejects NaN
        raise ValueError(f"tick must be positive, got {tick!r}")
    return max(0, -Decimal(repr(tick)).normalize().as_tuple().exponent)


def _eps(px: float, tick: float) -> float:
    """Rounding tolerance in ticks: scales with px/tick (float error grows with the ratio) but is capped
    well below one tick so it can never flip a rounding direction."""
    return min(1e-3, max(1e-9, abs(px / tick) * 1e-12))


def round_up(px: float, tick: float) -> float:
    eps = _eps(px, tick)
    return round(math.ceil(px / tick - eps) * tick, tick_decimals(tick))


def round_down(px: float, tick: float) -> float:
    eps = _eps(px, tick)
    return round(math.floor(px / tick + eps) * tick, tick_decimals(tick))


def _postable(px: float, lo: float, hi: float) -> float | None:
    """A post-only price must sit strictly inside (lo, hi) and be positive."""
    return px if 0.0 < px and lo < px < hi else None


def maker_entry_price(qa: BBO, qb: BBO, fa: Fees, fb: Fees, p: EdgeParams, maker_venue: str,
                      tick: float, improve_ticks: int = 0) -> float | None:
    """Resting price for a TM entry. Assumes the caller already confirmed mode == "TM" for this
    maker venue (choose_mode); returns None only when no post-only price is available (it would
    cross, or is not positive)."""
    if maker_venue == qa.venue:  # rest SELL on A, hedge BUY at ask_B
        req = tm_required_pct(fa, fb, p)
        floor_px = qb.ask * (1.0 + req / 100.0)
        px = round_up(max(qa.ask - improve_ticks * tick, floor_px), tick)
        return _postable(px, qa.bid, float("inf"))
    if maker_venue == qb.venue:  # rest BUY on B, hedge SELL at bid_A
        req = tm_required_pct(fb, fa, p)
        cap_px = qa.bid / (1.0 + req / 100.0)
        px = round_down(min(qb.bid + improve_ticks * tick, cap_px), tick)
        return _postable(px, 0.0, qb.ask)
    return None


def maker_exit_price(qa: BBO, qb: BBO, exit_spread_pct: float, maker_venue: str, tick: float,
                     improve_ticks: int = 0) -> float | None:
    """Resting price for a TM exit of a position short A / long B (close = BUY A, SELL B).
    The fill must realize an exit spread <= target against the other venue's live touch."""
    if maker_venue == qa.venue:  # rest BUY on A; hedge SELL B at bid_B: (p - bid_B)/bid_B <= X
        cap_px = qb.bid * (1.0 + exit_spread_pct / 100.0)
        px = round_down(min(qa.bid + improve_ticks * tick, cap_px), tick)
        return _postable(px, 0.0, qa.ask)
    if maker_venue == qb.venue:  # rest SELL on B; hedge BUY A at ask_A: (ask_A - p)/p <= X
        floor_px = qa.ask / (1.0 + exit_spread_pct / 100.0)
        px = round_up(max(qb.ask - improve_ticks * tick, floor_px), tick)
        return _postable(px, qb.bid, float("inf"))
    return None


def needs_requote(working_px: float, new_px: float, tick: float, requote_ticks: int) -> bool:
    if not (tick > 0):   # also rejects NaN
        raise ValueError(f"tick must be positive, got {tick!r}")
    return abs(new_px - working_px) / tick >= requote_ticks - 1e-9


def best_fee_venue(venue_a: str, fa: Fees, venue_b: str, fb: Fees) -> str:
    """Venue where making saves the most (largest taker − maker gap); ties go to A."""
    return venue_a if (fa.taker - fa.maker) >= (fb.taker - fb.maker) else venue_b
```

- [x] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_edge.py -q`
Expected: `13 passed`

- [x] **Step 5: Commit**

```bash
git add deploy-bbo/bbo_trader/edge.py deploy-bbo/tests/test_edge.py
git commit -m "feat(bbo): pure edge math — TT/TM edges, mode rule, pegged maker prices"
```

---

### Task 5: Sizing (`sizing.py`)

**Files:**
- Create: `deploy-bbo/bbo_trader/sizing.py`
- Test: `deploy-bbo/tests/test_sizing.py`

- [x] **Step 1: Write the failing tests**

`tests/test_sizing.py`:

```python
import pytest

from bbo_trader.sizing import (contracts_for_usd, notional, size_pair, hedge_qty, hedge_plan, lots_floor,
                               excess_to_flatten)
from tests.conftest import mk_spec


def test_contracts_for_usd_respects_lot_and_min():
    s = mk_spec("mexc", contract_size=1.0, lot=1.0, min_qty=1.0)
    assert contracts_for_usd(25.0, 2.0, s) == 12.0
    assert contracts_for_usd(25.0, 2.0, mk_spec("mexc", contract_size=10.0)) == 1.0
    assert contracts_for_usd(25.0, 2.0, mk_spec("mexc", contract_size=10.0, min_qty=5.0)) == 0.0
    assert contracts_for_usd(25.0, 0.0031, mk_spec("mexc", lot=10.0)) == 8060.0   # 8064.5 -> lot 10
    assert contracts_for_usd(0.0, 2.0, s) == 0.0
    assert notional(12.0, 2.0, s) == 24.0
    # fractional lots stay on the lot grid; sub-unit contract sizes (BloFin BTC = 0.001) work
    assert contracts_for_usd(25.0, 65000.0, mk_spec("blofin", contract_size=0.001, lot=0.1, min_qty=0.1)) == 0.3
    assert contracts_for_usd(0.3 * 65.0, 65000.0, mk_spec("blofin", contract_size=0.001, lot=0.1, min_qty=0.1)) == 0.3
    assert contracts_for_usd(25.0, 65000.0, mk_spec("blofin", contract_size=0.001, lot=1.0, min_qty=1.0)) == 0.0
    # non-finite inputs fail closed instead of raising
    assert contracts_for_usd(float("nan"), 2.0, s) == 0.0 and contracts_for_usd(25.0, float("inf"), s) == 0.0
    assert lots_floor(2.3, mk_spec("x", lot=0.5, min_qty=0.5)) == 2.0 and lots_floor(0.3, s) == 0.0
    assert lots_floor(2.3, mk_spec("x", lot=float("nan"), min_qty=1.0)) == 0.0   # malformed venue metadata fails closed


def test_size_pair_matches_notionals_within_tolerance():
    sa = mk_spec("blofin", contract_size=1.0, lot=1.0, min_qty=1.0)
    sb = mk_spec("mexc", contract_size=10.0, lot=1.0, min_qty=1.0)
    loose = size_pair(25.0, 1.0, 1.0, sa, sb, max_mismatch_pct=5.0)
    assert loose is not None
    # b can only do multiples of $10 -> 2 contracts = $20; a shrinks 25 -> 21 ($21 vs $20 = 5.0%, allowed)
    assert loose.qty_b == 2.0 and loose.qty_a == 21.0 and loose.mismatch_pct == pytest.approx(5.0)
    tight = size_pair(25.0, 1.0, 1.0, sa, sb, max_mismatch_pct=1.0)
    assert tight.qty_a == 20.0 and tight.qty_b == 2.0
    assert tight.matched_usd == pytest.approx(20.0) and tight.mismatch_pct == pytest.approx(0.0)


def test_size_pair_returns_none_when_unexpressible():
    sa = mk_spec("blofin", contract_size=1.0, lot=1.0, min_qty=1.0)
    too_big = mk_spec("mexc", contract_size=100.0, lot=1.0, min_qty=1.0)   # one contract = $100 > $25
    assert size_pair(25.0, 1.0, 1.0, sa, too_big, 5.0) is None
    # min_qty on a: 30 contracts at $1 needed but only $25 -> None
    assert size_pair(25.0, 1.0, 1.0, mk_spec("blofin", min_qty=30.0), sa, 5.0) is None


def test_size_pair_coarse_vs_fine_lots_and_min_usd():
    fine = mk_spec("blofin", contract_size=1.0, lot=1.0, min_qty=1.0)          # $0.001 per contract
    coarse = mk_spec("mexc", contract_size=10000.0, lot=1.0, min_qty=1.0)      # $10 per contract
    legs = size_pair(25.0, 0.001, 0.001, fine, coarse, max_mismatch_pct=5.0)   # 4,000 single-lot steps before
    assert legs is not None and legs.qty_b == 2.0 and legs.matched_usd == pytest.approx(20.0)
    assert legs.qty_a == 21000.0 and legs.mismatch_pct == pytest.approx(5.0)
    # the matched notional can fall below the minimum position: enforce it here
    assert size_pair(25.0, 0.001, 0.001, fine, coarse, 5.0, min_usd=10.0) is not None   # matched $20 >= $10
    assert size_pair(25.0, 0.001, 0.001, fine, coarse, 5.0, min_usd=21.0) is None       # matched $20 < $21
    assert size_pair(12.0, 0.001, 0.001, fine, coarse, 5.0, min_usd=10.0).matched_usd == pytest.approx(10.0)
    assert size_pair(9.0, 0.001, 0.001, fine, coarse, 5.0, min_usd=10.0) is None        # one $10 contract does not fit $9
    # both legs shrink when neither is a multiple of the other
    a3 = mk_spec("a", contract_size=3.0, lot=1.0, min_qty=1.0)
    b7 = mk_spec("b", contract_size=7.0, lot=1.0, min_qty=1.0)
    legs = size_pair(25.0, 1.0, 1.0, a3, b7, max_mismatch_pct=5.0)
    assert legs is not None and legs.mismatch_pct <= 5.0 and legs.matched_usd == pytest.approx(21.0)
    assert (legs.qty_a, legs.qty_b) == (7.0, 3.0)


def test_hedge_plan_tracks_covered_and_residual():
    sm = mk_spec("blofin", contract_size=1.0, lot=1.0, min_qty=1.0)
    sh = mk_spec("mexc", contract_size=10.0, lot=1.0, min_qty=1.0)
    assert hedge_qty(20.0, 1.0, sm, 1.0, sh) == 2.0
    assert hedge_qty(7.0, 1.0, sm, 1.0, sh) == 0.0      # $7 < one $10 contract -> unhedgeable
    plan = hedge_plan(25.0, 1.0, sm, 1.0, sh)           # $25 of maker fill, $10 hedge contracts
    assert plan.hedge_qty == 2.0 and plan.covered_maker_qty == 20.0 and plan.residual_maker_qty == 5.0
    plan = hedge_plan(10.0, 1.0061, sm, 1.0010, sh)     # $10.06 fill -> 1 hedge contract ($10.01) covers 9.949
    assert plan.hedge_qty == 1.0 and plan.covered_maker_qty == pytest.approx(9.9493, abs=1e-3)
    assert plan.residual_maker_qty == pytest.approx(10.0 - plan.covered_maker_qty)
    none = hedge_plan(7.0, 1.0, sm, 1.0, sh)
    assert none.hedge_qty == 0.0 and none.covered_maker_qty == 0.0 and none.residual_maker_qty == 7.0


def test_excess_to_flatten_rules():
    sm = mk_spec("blofin", contract_size=1.0, lot=1.0, min_qty=1.0)
    assert excess_to_flatten(0.05, 9.95, sm, 5.0) == 0.0          # within tolerance -> accept
    assert excess_to_flatten(5.0, 20.0, sm, 5.0) == 5.0           # 25% of matched -> flatten all 5 lots
    assert excess_to_flatten(7.0, 0.0, sm, 5.0) == 7.0            # nothing matched -> flatten
    assert excess_to_flatten(0.4, 0.0, sm, 5.0) == 0.0            # below one lot: dust, cannot be sent
    assert excess_to_flatten(0.0, 20.0, sm, 5.0) == 0.0
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_sizing.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'bbo_trader.sizing'`

- [x] **Step 3: Implement `bbo_trader/sizing.py`**

```python
"""Pure sizing: USD notional → venue contracts, matched pair legs, hedge plans and residual handling."""
from __future__ import annotations

import math
from dataclasses import dataclass

from .models import VenueSpec


def lots_floor(qty: float, spec: VenueSpec) -> float:
    """Largest lot multiple <= qty; 0.0 when below the venue minimum (or qty is not a finite positive)."""
    if not math.isfinite(qty) or qty <= 0 or not (math.isfinite(spec.lot) and spec.lot > 0):
        return 0.0
    lots = qty / spec.lot
    n = round(math.floor(lots + 1e-9 * max(1.0, lots)) * spec.lot, 10)
    return n if n >= spec.min_qty - 1e-12 else 0.0


def contracts_for_usd(usd: float, price: float, spec: VenueSpec) -> float:
    """Largest lot-multiple not exceeding `usd` at `price`; 0.0 when below the venue minimum."""
    if not (math.isfinite(usd) and math.isfinite(price)) or usd <= 0 or price <= 0 or spec.contract_size <= 0:
        return 0.0
    return lots_floor(usd / (price * spec.contract_size), spec)


def notional(qty: float, price: float, spec: VenueSpec) -> float:
    return qty * price * spec.contract_size


@dataclass(frozen=True)
class LegSizes:
    qty_a: float
    qty_b: float
    notional_a: float
    notional_b: float

    @property
    def mismatch_pct(self) -> float:
        lo = min(self.notional_a, self.notional_b)
        return abs(self.notional_a - self.notional_b) / lo * 100.0 if lo > 0 else float("inf")

    @property
    def matched_usd(self) -> float:
        return min(self.notional_a, self.notional_b)


def size_pair(usd: float, px_a: float, px_b: float, spec_a: VenueSpec, spec_b: VenueSpec,
              max_mismatch_pct: float, min_usd: float = 0.0) -> LegSizes | None:
    """Contracts per leg for `usd` per leg. The larger leg is shrunk onto the smaller one's notional
    (closed-form jump per iteration, so coarse/fine lot combinations converge in a few steps) until the
    notionals match within `max_mismatch_pct`. None when a leg cannot be expressed or the matched
    notional ends up below `min_usd`."""
    qa = contracts_for_usd(usd, px_a, spec_a)
    qb = contracts_for_usd(usd, px_b, spec_b)
    if qa <= 0 or qb <= 0:
        return None
    tol = 1.0 + max_mismatch_pct / 100.0
    for _ in range(64):
        legs = LegSizes(qa, qb, notional(qa, px_a, spec_a), notional(qb, px_b, spec_b))
        if legs.mismatch_pct <= max_mismatch_pct:
            return legs if legs.matched_usd >= min_usd else None
        if legs.notional_a > legs.notional_b:
            nq = contracts_for_usd(legs.notional_b * tol, px_a, spec_a)
            qa = nq if nq < qa else round(qa - spec_a.lot, 10)      # jump, else guarantee progress
        else:
            nq = contracts_for_usd(legs.notional_a * tol, px_b, spec_b)
            qb = nq if nq < qb else round(qb - spec_b.lot, 10)
        if qa < spec_a.min_qty - 1e-12 or qb < spec_b.min_qty - 1e-12:
            return None
    return None


@dataclass(frozen=True)
class HedgePlan:
    hedge_qty: float            # contracts to send on the hedge venue (0.0 = nothing hedgeable yet)
    covered_maker_qty: float    # maker-venue contracts the hedge notional actually covers
    residual_maker_qty: float   # maker contracts still unhedged after this round


def hedge_plan(unhedged_maker_qty: float, px_maker: float, spec_maker: VenueSpec,
               px_hedge: float, spec_hedge: VenueSpec) -> HedgePlan:
    """Hedge-venue contracts for a maker fill, and how much of the fill they really cover (the hedge
    is floored to whole lots, so a residual below one hedge contract stays unhedged and must be
    tracked — never marked hedged)."""
    hq = contracts_for_usd(notional(unhedged_maker_qty, px_maker, spec_maker), px_hedge, spec_hedge)
    if hq <= 0:
        return HedgePlan(0.0, 0.0, unhedged_maker_qty)
    covered = min(unhedged_maker_qty, notional(hq, px_hedge, spec_hedge) / (px_maker * spec_maker.contract_size))
    return HedgePlan(hq, round(covered, 10), round(unhedged_maker_qty - covered, 10))


def hedge_qty(filled_qty: float, px_maker: float, spec_maker: VenueSpec,
              px_hedge: float, spec_hedge: VenueSpec) -> float:
    """Hedge-venue contracts matching a maker fill's notional; 0.0 = unhedgeable (below minimum).
    Simple-case shorthand for hedge_plan(...).hedge_qty; the executor uses hedge_plan."""
    return hedge_plan(filled_qty, px_maker, spec_maker, px_hedge, spec_hedge).hedge_qty


def excess_to_flatten(residual_qty: float, matched_qty: float, spec: VenueSpec, max_mismatch_pct: float) -> float:
    """Maker-venue contracts to flatten from an unhedged residual once the resting order is terminal:
    the residual (rounded down to lots) when nothing is matched or it exceeds the mismatch tolerance
    of the matched quantity. 0.0 means EITHER the residual is within tolerance (accept it as exposure)
    OR it is below the venue minimum and cannot be sent (exposure retained) — callers must tell the two
    apart with `residual <= matched × tolerance` when they report it."""
    if residual_qty <= 0:
        return 0.0
    if matched_qty > 0 and residual_qty <= matched_qty * max_mismatch_pct / 100.0:
        return 0.0
    return lots_floor(residual_qty, spec)
```

- [x] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_sizing.py -q`
Expected: `6 passed`

- [x] **Step 5: Commit**

```bash
git add deploy-bbo/bbo_trader/sizing.py deploy-bbo/tests/test_sizing.py
git commit -m "feat(bbo): pure sizing — contracts per venue, matched legs, hedge qty"
```

---

### Task 6: Rate budgets (`budget.py`)

**Files:**
- Create: `deploy-bbo/bbo_trader/budget.py`
- Test: `deploy-bbo/tests/test_budget.py`

- [x] **Step 1: Write the failing tests**

`tests/test_budget.py`:

```python
import pytest

from bbo_trader.budget import TokenBucket, RateBudget
from bbo_trader.config import RateLimits


def test_bucket_reserve_and_window():
    b = TokenBucket(capacity=5, window_s=2.0, reserve=2)
    now = 100.0
    assert b.available(now) == 3 and b.available(now, priority=True) == 5
    assert all(b.try_take(now) for _ in range(3))
    assert not b.try_take(now)                       # reserve protects the last 2
    assert b.try_take(now, priority=True) and b.try_take(now, priority=True)
    assert not b.try_take(now, priority=True)        # exhausted
    assert b.try_take(now + 2.01)                    # window rolled


def test_bucket_penalty_halves_capacity():
    b = TokenBucket(capacity=4, window_s=10.0, reserve=0)
    b.penalize(now=0.0, seconds=60.0)
    assert b.available(1.0) == 2
    assert b.available(61.0) == 4


def test_penalty_pauses_entries_but_never_priority_calls():
    # BloFin-like: 30 per 10 s shared, reserve 6; entries used 24, then a 429 lands
    b = TokenBucket(capacity=30, window_s=10.0, reserve=6)
    for _ in range(24):
        assert b.try_take(0.0)
    b.penalize(now=0.0, seconds=60.0)
    assert b.available(0.001) < 0 and not b.try_take(0.001)              # entries paused
    assert all(b.try_take(0.001, priority=True) for _ in range(6))       # hedges/closes keep the reserve
    assert not b.try_take(0.001, priority=True)                          # ... but never exceed the venue limit
    assert b.penalized(0.001) and not b.penalized(60.0)


def test_try_take_n_and_window_boundary():
    b = TokenBucket(capacity=3, window_s=2.0, reserve=0)
    assert b.try_take(0.0, n=2)
    assert not b.try_take(0.0, n=2)                  # only one token left
    assert b.try_take(0.0, n=1)
    assert not b.try_take(2.0)                       # a token taken exactly window_s ago still counts
    assert b.try_take(2.0000001)


def test_rate_budget_shared_vs_separate():
    shared = RateBudget(RateLimits(orders=3, cancels=3, window_s=10.0, reserve=1, shared=True))
    assert shared.try_take("order", 0.0) and shared.try_take("cancel", 0.0)
    assert not shared.try_take("order", 0.0)                       # one bucket: 2 used + reserve 1
    assert shared.try_take("cancel", 0.0, priority=True)           # priority may use the reserve
    separate = RateBudget(RateLimits(orders=1, cancels=1, window_s=10.0, reserve=0, shared=False))
    assert separate.try_take("order", 0.0) and separate.try_take("cancel", 0.0)
    assert not separate.try_take("amend", 0.0)                     # amend draws from orders
    assert separate.to_dict(0.0) == {"orders_free": 0, "cancels_free": 0, "orders_entry_free": 0, "cancels_entry_free": 0,
                                     "shared": False, "penalized": False}


def test_unknown_kind_raises():
    b = RateBudget(RateLimits(orders=5, cancels=5, window_s=1.0, reserve=0, shared=False))
    with pytest.raises(ValueError, match="unknown budget kind"):
        b.try_take("cancels", 0.0)


def test_to_dict_after_penalty_clamps_and_flags():
    b = RateBudget(RateLimits(orders=3, cancels=3, window_s=10.0, reserve=1, shared=True))
    assert b.try_take("order", 0.0) and b.try_take("order", 0.0)
    b.penalize(0.0)
    d = b.to_dict(0.5)
    assert d == {"orders_free": 1, "cancels_free": 1, "orders_entry_free": 0, "cancels_entry_free": 0,
                 "shared": True, "penalized": True}
    assert b.available("order", 0.5) < 0                          # entries see the halved capacity
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_budget.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'bbo_trader.budget'`

- [x] **Step 3: Implement `bbo_trader/budget.py`**

```python
"""Pure rate budgets: sliding-window token buckets with a reserve for risk-reducing calls."""
from __future__ import annotations

from collections import deque
from typing import Literal

from .config import RateLimits

Kind = Literal["order", "amend", "cancel"]


class TokenBucket:
    """Sliding-window budget. `reserve` tokens are only spendable by priority callers (hedges, closes,
    flattens, cancels). A penalty (after a 429) halves the capacity for NON-priority callers only:
    entries and requotes pause while risk-reducing calls keep the full window — the venue, not our
    bucket, is the last line of defence for those."""

    def __init__(self, capacity: int, window_s: float, reserve: int = 0):
        self.capacity = capacity
        self.window_s = window_s
        self.reserve = reserve
        self._stamps: deque[float] = deque()
        self._penalty_until = 0.0

    def _prune(self, now: float) -> None:
        # strict: a token taken exactly window_s ago still counts against "N per window"
        while self._stamps and self._stamps[0] < now - self.window_s:
            self._stamps.popleft()

    def penalized(self, now: float) -> bool:
        return now < self._penalty_until

    def available(self, now: float, priority: bool = False) -> int:
        """Tokens a caller may take now (may be negative after a penalty). Non-priority callers
        cannot touch the reserve and see the halved capacity while penalized."""
        self._prune(now)
        used = len(self._stamps)
        if priority:
            return self.capacity - used
        cap = self.capacity // 2 if self.penalized(now) else self.capacity
        return cap - used - self.reserve

    def try_take(self, now: float, n: int = 1, priority: bool = False) -> bool:
        if self.available(now, priority) < n:
            return False
        for _ in range(n):
            self._stamps.append(now)
        return True

    def penalize(self, now: float, seconds: float) -> None:
        """Halve non-priority capacity for `seconds` (after a 429 / 'too frequent')."""
        self._penalty_until = now + seconds


class RateBudget:
    """Per-venue budget. kind: 'order' | 'amend' (orders bucket) | 'cancel' (cancels bucket,
    or the same bucket when the venue shares one limit across trading endpoints — then `cancels` is
    ignored and config requires orders == cancels)."""

    def __init__(self, limits: RateLimits):
        self.shared = limits.shared
        self._orders = TokenBucket(limits.orders, limits.window_s, limits.reserve)
        self._cancels = (self._orders if limits.shared
                         else TokenBucket(limits.cancels, limits.window_s, limits.reserve))

    def _bucket(self, kind: Kind) -> TokenBucket:
        if kind == "cancel":
            return self._cancels
        if kind in ("order", "amend"):
            return self._orders
        raise ValueError(f"unknown budget kind: {kind!r}")

    def available(self, kind: Kind, now: float, priority: bool = False) -> int:
        return self._bucket(kind).available(now, priority)

    def try_take(self, kind: Kind, now: float, n: int = 1, priority: bool = False) -> bool:
        return self._bucket(kind).try_take(now, n, priority)

    def penalize(self, now: float, seconds: float = 60.0) -> None:
        self._orders.penalize(now, seconds)
        self._cancels.penalize(now, seconds)

    def to_dict(self, now: float) -> dict[str, object]:
        """`*_free` = what a priority call (hedge/close/cancel) may still take; `*_entry_free` = what an
        entry/requote may take (net of the reserve and any penalty) — the number that explains why entries stop."""
        return {"orders_free": max(0, self._orders.available(now, priority=True)),
                "cancels_free": max(0, self._cancels.available(now, priority=True)),
                "orders_entry_free": max(0, self._orders.available(now)),
                "cancels_entry_free": max(0, self._cancels.available(now)),
                "shared": self.shared,
                "penalized": self._orders.penalized(now) or self._cancels.penalized(now)}
```

- [x] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_budget.py -q`
Expected: `7 passed`

- [x] **Step 5: Commit**

```bash
git add deploy-bbo/bbo_trader/budget.py deploy-bbo/tests/test_budget.py
git commit -m "feat(bbo): per-venue token-bucket rate budgets with reserve"
```

---

### Task 7: Position lifecycle, book and state store (`positions.py`)

**Files:**
- Create: `deploy-bbo/bbo_trader/positions.py`
- Test: `deploy-bbo/tests/test_positions.py`

- [x] **Step 1: Write the failing tests**

`tests/test_positions.py`:

```python
import asyncio
import json
import threading

import pytest

from bbo_trader.models import (Position, TT_ENTERING, MAKER_RESTING, HEDGING, OPEN, EXIT_MAKER_RESTING,
                               TT_EXITING, CLOSED, DEGRADED)
from bbo_trader.positions import (transition, InvalidTransition, StateCorrupt, finalize_pnl, PositionBook,
                                  StateStore, build_state)


def test_transition_table():
    p = Position(1, "XYZUSDT", "blofin", "mexc", MAKER_RESTING, "TM")
    transition(p, HEDGING)
    transition(p, OPEN)
    transition(p, EXIT_MAKER_RESTING)
    transition(p, OPEN)                       # TTL/edge-gone returns to OPEN
    transition(p, TT_EXITING)
    with pytest.raises(InvalidTransition):
        transition(p, OPEN)                   # exiting cannot reopen
    transition(p, DEGRADED)
    transition(p, CLOSED)
    with pytest.raises(InvalidTransition):
        transition(p, OPEN)
    q = Position(2, "XYZUSDT", "blofin", "mexc", OPEN, "TT")
    transition(q, CLOSED)                     # reconcile: closed externally
    r = Position(3, "XYZUSDT", "blofin", "mexc", OPEN, "TT")
    transition(r, DEGRADED)                   # reconcile: held but unactionable


def test_finalize_pnl_two_legs():
    p = Position(1, "XYZUSDT", "blofin", "mexc", TT_EXITING, "TT", size_usd=25.0,
                 entry_price_a=1.01, entry_price_b=1.00, exit_price_a=1.002, exit_price_b=1.001,
                 entry_fees_usd=0.02, exit_fees_usd=0.02)
    finalize_pnl(p)
    # short leg: (1.01-1.002)/1.01*25 = 0.19802 ; long leg: (1.001-1.0)/1.0*25 = 0.025
    assert p.gross_pnl_usd == pytest.approx(0.19802 + 0.025, abs=1e-5)
    assert p.net_pnl_usd == pytest.approx(p.gross_pnl_usd - 0.04)
    assert p.exit_spread_pct == pytest.approx((1.002 - 1.001) / 1.001 * 100)


def test_book_lifecycle_and_totals():
    book = PositionBook(closed_keep=2)
    p1 = book.new("XYZUSDT", "blofin", "mexc", TT_ENTERING, "TT", size_usd=25.0)
    p2 = book.new("ABCUSDT", "blofin", "mexc", MAKER_RESTING, "TM", maker_venue="blofin")
    assert (p1.id, p2.id, book.next_id) == (1, 2, 3)
    assert book.for_symbol("XYZUSDT") == [p1] and book.get(2) is p2
    assert book.resting_counts() == {"blofin": 1}
    assert book.by_status(MAKER_RESTING) == [p2] and book.by_status(OPEN) == []
    p1.status = OPEN
    p1.entry_price_a, p1.entry_price_b, p1.exit_price_a, p1.exit_price_b = 1.0, 1.0, 0.99, 1.0
    transition(p1, TT_EXITING)
    book.close(p1, "convergence", now=200.0)
    assert p1.status == CLOSED and p1.exit_reason == "convergence" and p1.exit_time == 200.0
    assert book.total_trades == 1 and book.total_wins == 1 and book.total_pnl_usd == pytest.approx(0.25)
    book.close(p1, "convergence", now=201.0)          # late duplicate terminal event: idempotent
    assert book.total_trades == 1 and len(book.closed) == 1 and p1.exit_time == 200.0
    assert book.get(1) is None and book.get(1, include_closed=True) is p1
    book.discard(p2)                                  # cancelled maker, never a trade
    assert book.open == [] and book.total_trades == 1
    with pytest.raises(InvalidTransition):
        book.discard(p1)                              # closed positions cannot be discarded
    for i in range(3):                                # closed list is capped
        q = book.new("QQQUSDT", "blofin", "mexc", TT_ENTERING, "TT")
        q.status = TT_EXITING
        book.close(q, "timeout", now=300.0 + i, counts_as_trade=False)
    assert len(book.closed) == 2 and book.total_trades == 1
    book.mark_equity(100.0, now=0.0)
    book.mark_equity(90.0, now=10.0)                  # within 60 s -> no new history point
    assert book.peak_equity == 100.0 and book.max_drawdown_pct == pytest.approx(10.0)
    assert len(book.equity_history) == 1
    assert book.equity_history[0]["t"].startswith("1970-01-01T00:00:00") and book.equity_history[0]["v"] == 100.0
    assert PositionBook(closed_keep=0).closed_keep == 1


def test_state_store_roundtrip_and_dashboard_schema(tmp_path):
    book = PositionBook()
    p = book.new("XYZUSDT", "blofin", "mexc", OPEN, "TM", size_usd=20.0, entry_time=1_700_000_000.0)
    p.client_ids["maker"] = "bp1-maker-1"
    state = build_state(book, equity=210.0, cash=200.0, starting_capital=200.0, mode="paper",
                        risk_state={"pair_stats": {}, "venue_symbol_blacklist": ["blofin|HNTUSDT"],
                                    "symbol_blacklist": {}, "pair_strikes": {}, "pair_blacklist": {}, "halted": True},
                        balances={"mexc": {"available": 100.0}}, scanner=[], bbo={"latency": {}},
                        saved_at=1_700_000_100.0)
    for key in ("state_saved_at_ts", "cash", "equity", "open_positions", "closed_positions", "total_pnl_usd",
                "pair_stats", "blofin_risk_blacklist", "balance_cache", "spread_scanner", "dry_run",
                "kill_switch", "saved_at", "bbo", "risk", "order_audit_log", "equity_history"):
        assert key in state, key
    assert state["blofin_risk_blacklist"] == ["HNTUSDT"] and state["kill_switch"] is True   # mirrors the manual halt
    store = StateStore(tmp_path / "real_state.json")
    store.save(state)
    loaded = store.load()
    assert loaded["open_positions"][0]["exchange_short"] == "blofin"
    book2 = PositionBook()
    book2.load(loaded)
    assert book2.open[0] == p and book2.next_id == 2
    assert StateStore(tmp_path / "missing.json").load() is None
    store.save(state)                                  # second save keeps the previous file as .bak
    assert store.load_backup()["open_positions"][0]["id"] == 1


def test_closed_dicts_are_cached_and_reloaded():
    book = PositionBook(closed_keep=2)
    for i in range(3):
        q = book.new("QQQUSDT", "blofin", "mexc", TT_ENTERING, "TT", size_usd=10.0)
        q.status = TT_EXITING
        book.close(q, "timeout", now=100.0 + i)
    d = book.to_dict()
    assert [c["id"] for c in d["closed_positions"]] == [2, 3]            # capped like `closed`
    assert d["closed_positions"] is not book.to_dict()["closed_positions"]  # fresh list each call
    assert d["closed_positions"][0] == book.closed[0].to_dict()          # cached dict equals a fresh one
    book2 = PositionBook(closed_keep=2)
    book2.load(d)
    assert [c["id"] for c in book2.to_dict()["closed_positions"]] == [2, 3]


def test_corrupt_state_is_never_a_fresh_start(tmp_path):
    store = StateStore(tmp_path / "real_state.json")
    (tmp_path / "real_state.json").write_text('{"open_positions": [')     # truncated by a crash
    with pytest.raises(StateCorrupt):
        store.load()
    assert store.load_backup() is None                                    # no backup yet
    store.save({"a": 1})                                                  # fresh save after repair
    assert store.load() == {"a": 1}


def test_state_never_emits_nan_and_load_repairs_next_id(tmp_path):
    store = StateStore(tmp_path / "real_state.json")
    store.save({"equity": float("nan"), "nested": [float("inf"), 1.0]})
    text = (tmp_path / "real_state.json").read_text()
    assert "NaN" not in text and "Infinity" not in text
    assert json.loads(text) == {"equity": None, "nested": [None, 1.0]}
    book = PositionBook()
    book.load({"next_id": 1, "open_positions": [Position(7, "X", "a", "b", OPEN, "TT").to_dict()]})
    assert book.next_id == 8                                              # never reuse an id the file holds
    book.load({})                                                         # tolerant of missing keys
    assert book.open == [] and book.total_trades == 0


def test_state_file_is_never_absent_and_saves_do_not_race(tmp_path):
    store = StateStore(tmp_path / "real_state.json")
    store.save({"n": 1})
    store.save({"n": 2})
    assert store.load() == {"n": 2} and store.load_backup() == {"n": 1}
    assert not (tmp_path / "real_state.json.bak.tmp").exists()
    assert not [p for p in tmp_path.iterdir() if p.name.endswith(".tmp")]      # no stray temp files

    # a watcher thread must never observe the live file absent while saves run back to back
    absent = []
    stop = threading.Event()

    def watch():
        while not stop.is_set():
            if not (tmp_path / "real_state.json").exists():
                absent.append(1)
    w = threading.Thread(target=watch, daemon=True)
    w.start()
    for i in range(50):
        store.save({"n": 10 + i})

    async def concurrent():
        await asyncio.gather(store.save_async({"n": 3}), store.save_async({"n": 4}))
    asyncio.run(concurrent())
    stop.set()
    w.join()
    assert not absent
    assert store.load()["n"] in (3, 4) and (tmp_path / "real_state.json").exists()
    assert not [p for p in tmp_path.iterdir() if p.name.endswith(".tmp")]
    (tmp_path / "real_state.json").unlink()                                   # torn save: only the backup survives
    with pytest.raises(StateCorrupt, match="torn save"):
        store.load()


def test_non_object_state_and_stranded_closed_entries(tmp_path):
    store = StateStore(tmp_path / "real_state.json")
    (tmp_path / "real_state.json").write_text("[]")
    with pytest.raises(StateCorrupt, match="expected object"):
        store.load()
    closed = Position(3, "X", "a", "b", CLOSED, "TT", exit_reason="convergence").to_dict()
    live = Position(4, "X", "a", "b", OPEN, "TT").to_dict()
    book = PositionBook()
    book.load({"open_positions": [closed, live]})                             # a CLOSED entry under open_positions
    assert [p.id for p in book.open] == [4] and [p.id for p in book.closed] == [3] and book.next_id == 5
    weird = Position(5, "X", "a", "b", "FROM_THE_FUTURE", "TT").to_dict()      # unknown status -> DEGRADED, kept open
    live_under_closed = Position(6, "X", "a", "b", OPEN, "TT").to_dict()
    book.load({"open_positions": [weird], "closed_positions": [live_under_closed]})
    assert {p.id: p.status for p in book.open} == {5: DEGRADED, 6: OPEN} and book.closed == []
    stale = tmp_path / "real_state.json.123.456.tmp"
    stale.write_text("junk")
    StateStore(tmp_path / "real_state.json")                                  # start-up sweeps crash leftovers
    assert not stale.exists()
    store.save({"weird": {1, 2}})                                             # unserializable -> str fallback, no crash
    assert "weird" in store.load()
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_positions.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'bbo_trader.positions'`

- [x] **Step 3: Implement `bbo_trader/positions.py`**

```python
"""Position lifecycle: transition table, P&L finalization, PositionBook, StateStore, dashboard state."""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import threading
from datetime import datetime, timezone
from pathlib import Path

from .models import (Position, TT_ENTERING, MAKER_RESTING, HEDGING, OPEN, EXIT_MAKER_RESTING,
                     TT_EXITING, EXIT_HEDGING, DEGRADED, CLOSED, NON_TERMINAL)

log = logging.getLogger("bbo.state")

# OPEN -> CLOSED / DEGRADED exist for reconciliation: a position the venues no longer hold (closed
# externally, liquidated) is booked closed; one we hold but cannot act on becomes DEGRADED.
ALLOWED: dict[str, set[str]] = {
    TT_ENTERING: {OPEN, CLOSED, DEGRADED},
    MAKER_RESTING: {HEDGING, CLOSED, TT_ENTERING},
    HEDGING: {OPEN, CLOSED, DEGRADED},
    OPEN: {TT_EXITING, EXIT_MAKER_RESTING, CLOSED, DEGRADED},
    EXIT_MAKER_RESTING: {EXIT_HEDGING, TT_EXITING, OPEN},
    TT_EXITING: {CLOSED, DEGRADED},
    EXIT_HEDGING: {CLOSED, DEGRADED, TT_EXITING},
    DEGRADED: {CLOSED},
    CLOSED: set(),
}


class InvalidTransition(Exception):
    pass


class StateCorrupt(Exception):
    """The state file exists but cannot be read or parsed. Never treat this as a fresh start: the
    venues may still hold the positions the file described."""


def transition(pos: Position, new_status: str) -> None:
    if new_status not in ALLOWED.get(pos.status, set()):
        raise InvalidTransition(f"#{pos.id} {pos.status} -> {new_status}")
    pos.status = new_status


def finalize_pnl(pos: Position) -> None:
    """Gross = short leg (entry_a - exit_a)/entry_a + long leg (exit_b - entry_b)/entry_b, times matched USD."""
    gross = 0.0
    if pos.entry_price_a > 0 and pos.exit_price_a > 0:
        gross += (pos.entry_price_a - pos.exit_price_a) / pos.entry_price_a * pos.size_usd
    if pos.entry_price_b > 0 and pos.exit_price_b > 0:
        gross += (pos.exit_price_b - pos.entry_price_b) / pos.entry_price_b * pos.size_usd
    pos.gross_pnl_usd = gross
    pos.net_pnl_usd = gross - pos.entry_fees_usd - pos.exit_fees_usd
    if pos.exit_price_a > 0 and pos.exit_price_b > 0:
        pos.exit_spread_pct = (pos.exit_price_a - pos.exit_price_b) / pos.exit_price_b * 100.0


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


class PositionBook:
    """All non-terminal positions live in `open` (resting, hedging, open, exiting, degraded).

    Closed positions are immutable once closed, so their dashboard dicts are cached at close time:
    state saves (up to 2/s) must not re-serialize hundreds of closed positions. `close()` is
    idempotent — the cancel-after-fill / fill-after-cancel races the spec designs for may deliver a
    late terminal event after a position was already booked."""

    EQUITY_POINTS = 2000        # at the 60 s cadence ≈ 33 h of dashboard chart; keeps the state file small

    def __init__(self, closed_keep: int = 200):
        self.open: list[Position] = []
        self.closed: list[Position] = []
        self._closed_dicts: list[dict] = []
        self.next_id = 1
        self.total_trades = 0
        self.total_wins = 0
        self.total_pnl_usd = 0.0
        self.peak_equity = 0.0
        self.max_drawdown_pct = 0.0
        self.equity_history: list[dict] = []   # {"t": iso, "v": equity, "_ts": float} — the dashboard reads t/v
        self.audit: list[dict] = []            # 1000 kept in memory, the last 200 persisted
        self.closed_keep = max(1, closed_keep)
        self.dirty = False

    def new(self, symbol: str, venue_a: str, venue_b: str, status: str, mode: str, **kw) -> Position:
        pos = Position(id=self.next_id, symbol=symbol, venue_a=venue_a, venue_b=venue_b,
                       status=status, mode=mode, **kw)
        self.next_id += 1
        self.open.append(pos)
        self.dirty = True
        return pos

    def get(self, pos_id: int, include_closed: bool = False) -> Position | None:
        for p in self.open:
            if p.id == pos_id:
                return p
        if include_closed:
            for p in self.closed:
                if p.id == pos_id:
                    return p
        return None

    def for_symbol(self, symbol: str) -> list[Position]:
        return [p for p in self.open if p.symbol == symbol]

    def by_status(self, *statuses: str) -> list[Position]:
        return [p for p in self.open if p.status in statuses]

    def resting_counts(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for p in self.open:
            if p.status in (MAKER_RESTING, EXIT_MAKER_RESTING) and p.maker_venue:
                out[p.maker_venue] = out.get(p.maker_venue, 0) + 1
        return out

    def close(self, pos: Position, exit_reason: str, now: float, counts_as_trade: bool = True) -> None:
        if pos.status == CLOSED:
            return                                     # already booked: a late duplicate event
        transition(pos, CLOSED)
        pos.exit_reason = exit_reason
        pos.exit_time = now
        finalize_pnl(pos)
        if pos in self.open:
            self.open.remove(pos)
        self.closed.append(pos)
        self._closed_dicts.append(pos.to_dict())
        del self.closed[:-self.closed_keep]
        del self._closed_dicts[:-self.closed_keep]
        if counts_as_trade:
            self.total_trades += 1
            self.total_pnl_usd += pos.net_pnl_usd
            if pos.net_pnl_usd > 0:
                self.total_wins += 1
        self.dirty = True

    def discard(self, pos: Position) -> None:
        """Drop a position that never became a trade (nothing filled). Anything that may hold a leg on
        a venue must be closed, not discarded."""
        if pos.status not in (TT_ENTERING, MAKER_RESTING):
            raise InvalidTransition(f"#{pos.id} discard from {pos.status}")
        if pos in self.open:
            self.open.remove(pos)
        self.dirty = True

    def mark_equity(self, equity: float, now: float, every_s: float = 60.0) -> None:
        if equity > self.peak_equity:
            self.peak_equity = equity
        if self.peak_equity > 0:
            dd = (self.peak_equity - equity) / self.peak_equity * 100.0
            self.max_drawdown_pct = max(self.max_drawdown_pct, dd)
        last_ts = float(self.equity_history[-1].get("_ts") or 0.0) if self.equity_history else 0.0   # null-tolerant
        if not self.equity_history or now - last_ts >= every_s:
            self.equity_history.append({"t": _iso(now), "v": round(equity, 4), "_ts": now})
            del self.equity_history[:-self.EQUITY_POINTS]

    def audit_order(self, entry: dict) -> None:
        self.audit.append(entry)
        del self.audit[:-1000]

    def to_dict(self) -> dict:
        return {"next_id": self.next_id, "total_trades": self.total_trades, "total_wins": self.total_wins,
                "total_pnl_usd": self.total_pnl_usd, "peak_equity": self.peak_equity,
                "max_drawdown_pct": self.max_drawdown_pct, "equity_history": self.equity_history[-self.EQUITY_POINTS:],
                "open_positions": [p.to_dict() for p in self.open],
                "closed_positions": list(self._closed_dicts),
                "order_audit_log": self.audit[-200:]}

    def load(self, d: dict) -> None:
        self.total_trades = int(d.get("total_trades", 0))
        self.total_wins = int(d.get("total_wins", 0))
        self.total_pnl_usd = float(d.get("total_pnl_usd", 0.0))
        self.peak_equity = float(d.get("peak_equity", 0.0))
        self.max_drawdown_pct = float(d.get("max_drawdown_pct", 0.0))
        self.equity_history = list(d.get("equity_history", []))[-self.EQUITY_POINTS:]
        self.audit = list(d.get("order_audit_log", []))
        loaded_open = [Position.from_dict(x) for x in d.get("open_positions", [])]
        loaded_closed = [Position.from_dict(x) for x in d.get("closed_positions", [])]
        for p in loaded_open:
            if p.status not in NON_TERMINAL and p.status != CLOSED:
                # a status this build does not know (rollback, hand edit): the venues may still hold it
                log.error("STATE_UNKNOWN_STATUS #%s %r -> DEGRADED (reconcile can still book it)", p.id, p.status)
                p.status = DEGRADED
        stranded_closed = [p for p in loaded_open if p.status == CLOSED]          # CLOSED under open_positions
        stranded_open = [p for p in loaded_closed if p.status in NON_TERMINAL]    # live under closed_positions
        if stranded_closed or stranded_open:
            log.warning("STATE_REPARTITIONED %d closed entries under open, %d live entries under closed",
                        len(stranded_closed), len(stranded_open))
        self.open = [p for p in loaded_open if p.status != CLOSED] + stranded_open
        self.closed = ([p for p in loaded_closed if p.status not in NON_TERMINAL] + stranded_closed)[-self.closed_keep:]
        self._closed_dicts = [p.to_dict() for p in self.closed]
        highest = max((p.id for p in self.open + self.closed), default=0)
        self.next_id = max(int(d.get("next_id", 1)), highest + 1)   # never reuse an id the file still holds


def _sanitize(obj):
    """Replace non-finite floats with None so the dashboard's JSON.parse never chokes; coerce non-primitive keys."""
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, dict):
        return {(k if isinstance(k, (str, int, float, bool)) or k is None else str(k)): _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize(v) for v in obj]
    return obj


def dumps_state(state: dict) -> str:
    try:
        return json.dumps(state, allow_nan=False)
    except (ValueError, TypeError) as e:
        log.error("STATE_UNSERIALIZABLE %r — sanitizing (non-finite -> null, unknown types -> str)", e)
        return json.dumps(_sanitize(state), default=str)


class StateStore:
    """Atomic JSON state. The live file is NEVER absent: write a uniquely named tmp → flush+fsync →
    hard-link the current file to `.bak` → rename tmp over the live file. `save()` is serialized by a
    thread lock and `save_async()` additionally by an asyncio lock, so an overlapping shutdown save
    cannot race the sweep's save. Stale tmp files from a crash are removed at start-up."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self.bak = self.path.with_suffix(self.path.suffix + ".bak")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()
        self._tlock = threading.Lock()
        for stale in self.path.parent.glob(f"{self.path.name}.*.tmp"):
            stale.unlink(missing_ok=True)

    def save(self, state: dict) -> None:
        payload = dumps_state(state)
        uniq = f"{os.getpid()}.{threading.get_ident()}"
        tmp = self.path.with_suffix(f"{self.path.suffix}.{uniq}.tmp")
        link = self.path.with_suffix(f"{self.path.suffix}.{uniq}.bak.tmp")
        with self._tlock:
            with open(tmp, "w") as f:
                f.write(payload)
                f.flush()
                os.fsync(f.fileno())
            try:
                link.unlink(missing_ok=True)
                os.link(self.path, link)          # the live file itself is never unlinked
                os.replace(link, self.bak)
            except FileNotFoundError:
                pass                              # first save: nothing to back up
            os.replace(tmp, self.path)

    async def save_async(self, state: dict) -> None:
        """Serialize + write off the event loop (state must be a snapshot the loop no longer mutates)."""
        async with self._lock:
            await asyncio.to_thread(self.save, state)

    def _read(self, path: Path) -> dict | None:
        try:
            with open(path, "r") as f:
                data = json.load(f)
        except FileNotFoundError:
            return None
        except (json.JSONDecodeError, OSError) as e:
            raise StateCorrupt(f"{path}: {e}") from e
        if not isinstance(data, dict):
            raise StateCorrupt(f"{path}: top-level {type(data).__name__}, expected object")
        return data

    def load(self) -> dict | None:
        """None only when neither the state file nor a backup exists (fresh start). A missing live
        file next to a backup is a torn save, not a fresh start. Raises StateCorrupt otherwise."""
        d = self._read(self.path)
        if d is None and self.bak.exists():
            raise StateCorrupt(f"{self.path} missing but {self.bak} exists — torn save?")
        return d

    def load_backup(self) -> dict | None:
        return self._read(self.bak)


def build_state(book: PositionBook, *, equity: float, cash: float, starting_capital: float, mode: str,
                risk_state: dict, balances: dict, scanner: list, bbo: dict, saved_at: float) -> dict:
    """The real_trader dashboard schema plus `risk` and `bbo` sections. `cash` is realized-only
    (capital + realized P&L): the legacy dashboard shows `cash + Σ open net_pnl_usd`, so `equity`
    must not be passed as `cash` when it already includes unrealized P&L. `kill_switch` mirrors the
    manual halt so the dashboard's status light reflects `stop.flag` / `/stop`."""
    d = book.to_dict()
    d.update({
        "state_saved_at_ts": saved_at,
        "saved_at": _iso(saved_at),
        "cash": cash, "equity": equity, "starting_capital": starting_capital,
        "pair_stats": risk_state.get("pair_stats", {}),
        "blofin_risk_blacklist": sorted(k.split("|", 1)[1] for k in risk_state.get("venue_symbol_blacklist", [])
                                        if k.startswith("blofin|")),
        "symbol_blacklist": risk_state.get("symbol_blacklist", {}),
        "pair_failure_counts": {k: (v.get("n", 0) if isinstance(v, dict) else v)
                                for k, v in risk_state.get("pair_strikes", {}).items()},
        "pair_blacklist": risk_state.get("pair_blacklist", {}),
        "balance_cache": {k: dict(v) for k, v in balances.items()},
        "spread_scanner": scanner,
        "spread_histogram": {},
        "dry_run": mode != "live",
        "kill_switch": bool(risk_state.get("halted")),
        "mode": mode,
        "risk": risk_state,
        "bbo": bbo,
    })
    return d
```

- [x] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_positions.py -q`
Expected: `9 passed`

- [x] **Step 5: Commit**

```bash
git add deploy-bbo/bbo_trader/positions.py deploy-bbo/tests/test_positions.py
git commit -m "feat(bbo): position state machine, PositionBook, atomic StateStore, dashboard state schema"
```

---

### Task 8: Risk manager (`risk.py`)

**Files:**
- Create: `deploy-bbo/bbo_trader/risk.py`
- Modify: `deploy-bbo/tests/conftest.py` (append `make_cfg`)
- Test: `deploy-bbo/tests/test_risk.py`

- [ ] **Step 1: Append the `make_cfg` helper to `tests/conftest.py`**

Append at the end of the file:

```python
def make_cfg(tmp_path=None, **over):
    """A Config with mexc + blofin as trade venues, no file I/O."""
    from pathlib import Path
    from bbo_trader.config import Config, VenueConfig, RateLimits
    venues = (
        VenueConfig("mexc", "trade", 0.02, 0.00, RateLimits(20, 20, 2.0, 4, False), max_topics=30, min_requote_ms=500),
        VenueConfig("blofin", "trade", 0.06, 0.02, RateLimits(30, 30, 10.0, 6, True), max_topics=50, min_requote_ms=1000),
        VenueConfig("okx", "quote_only", 0.05, 0.02, RateLimits(), symbol_whitelist=("BTCUSDT",)),
    )
    kw = dict(venues=venues, data_dir=Path(tmp_path) if tmp_path is not None else Path("./data"))
    kw.update(over)
    return Config(**kw)
```

- [ ] **Step 2: Write the failing tests**

`tests/test_risk.py`:

```python
from dataclasses import replace

import pytest

from bbo_trader.config import VenueConfig, RateLimits
from bbo_trader.models import Position, OPEN
from bbo_trader.risk import MismatchGuard, RiskManager, pair_key, route_key
from tests.conftest import make_cfg


def test_mismatch_guard_slow_and_fast_tiers():
    g = MismatchGuard(slow_pct=10.0, slow_n=3, fast_pct=50.0, fast_n=2)
    assert not g.observe("k", 12.0)
    assert not g.observe("k", 12.0)
    assert not g.observe("k", 5.0)                      # reset by a sane quote
    assert not g.observe("k", 12.0)
    assert not g.observe("k", 12.0)
    assert g.observe("k", 12.0)                         # third consecutive > 10% -> blacklisted (slow tier)
    assert g.is_blacklisted("k") and g.blacklisted["k"]["tier"] == "slow"
    assert not g.observe("k", 12.0)                     # already blacklisted: no second transition
    g2 = MismatchGuard(10.0, 300, 50.0, 2)
    assert not g2.observe("f", 80.0)
    assert g2.observe("f", -80.0)                       # fast tier, sign-agnostic
    assert g2.blacklisted["f"]["tier"] == "fast" and g2.blacklisted["f"]["spread_pct"] == -80.0
    assert not g2.observe("n", float("nan"))            # NaN never counts as absurd
    with pytest.raises(ValueError):
        MismatchGuard(10.0, 0, 50.0, 2)                 # n=0 would blacklist every pair on its first quote
    with pytest.raises(ValueError):
        MismatchGuard(0.0, 3, 50.0, 2)


def test_entry_gates_in_order(tmp_path, clock):
    cfg = make_cfg(tmp_path, max_concurrent=2, blocked_symbols=frozenset({"BADUSDT"}))
    cfg = replace(cfg, venues=cfg.venues + (VenueConfig("gate", "trade", 0.05, 0.02, RateLimits(),
                                                        symbol_whitelist=("BTCUSDT",)),))
    r = RiskManager(cfg, clock)
    ok = lambda **kw: r.entry_allowed(kw.get("symbol", "XYZUSDT"), kw.get("a", "blofin"), kw.get("b", "mexc"),
                                      kw.get("size", 25.0), kw.get("open_count", 0))
    assert ok() == (True, "ok")
    assert ok(a="mexc", b="mexc") == (False, "invalid")
    assert ok(size=0.0) == (False, "invalid")
    assert ok(open_count=2) == (False, "max_concurrent")
    assert ok(symbol="BADUSDT") == (False, "blocked_symbol")
    r.set_cooldown("XYZUSDT")
    assert ok() == (False, "cooldown")
    clock.tick(61)
    assert ok() == (True, "ok")
    assert ok(a="okx", b="mexc") == (False, "venue_blocked")           # quote_only venue: never tradable
    assert ok(a="binance", b="mexc") == (False, "venue_blocked")       # unknown venue fails closed, no KeyError
    assert ok(a="gate", b="mexc") == (False, "venue_blocked")          # whitelist excludes XYZUSDT
    assert ok(symbol="BTCUSDT", a="gate", b="mexc") == (True, "ok")
    r.blacklist_venue_symbol("blofin", "XYZUSDT")
    assert ok() == (False, "venue_blocked")
    r.venue_symbol_blacklist.clear()
    assert not r.record_strike("XYZUSDT", "blofin", "mexc")
    assert r.record_strike("XYZUSDT", "blofin", "mexc")                 # 2 strikes -> 24 h blacklist
    assert ok() == (False, "pair_blacklist")
    assert ok(a="mexc", b="blofin") == (True, "ok")                     # other direction unaffected
    r.mismatch.blacklist(route_key("XYZUSDT", "mexc", "blofin"))
    assert ok(a="mexc", b="blofin") == (False, "mismatch")
    r.set_balance("mexc", available=20.0, total=100.0)
    assert ok(symbol="QQQUSDT") == (False, "balance")                  # 20 < 25 × 1.05
    r.set_balance("mexc", available=30.0, total=100.0)
    assert ok(symbol="QQQUSDT") == (True, "ok")
    r.halt("test")
    assert ok(symbol="QQQUSDT") == (False, "halted")


def test_balance_gate_fails_closed_in_live_mode(tmp_path, clock):
    r = RiskManager(make_cfg(tmp_path, mode="live"), clock)
    ok = lambda: r.entry_allowed("XYZUSDT", "blofin", "mexc", 25.0, 0)
    assert ok() == (False, "balance_unknown")                           # no cache yet
    r.set_balance("mexc", 100.0, 100.0)
    r.set_balance("blofin", 100.0, 100.0)
    assert ok() == (True, "ok")
    clock.tick(121)
    assert ok() == (False, "balance_unknown")                           # stale cache (> balance_max_age_s)
    r.set_balance("mexc", 100.0, 100.0)
    r.set_balance("blofin", 10.0, 100.0)
    assert ok() == (False, "balance")
    paper = RiskManager(make_cfg(tmp_path), clock)                      # paper: a missing cache is permissive
    assert paper.entry_allowed("XYZUSDT", "blofin", "mexc", 25.0, 0) == (True, "ok")


def test_halt_flags_are_consumed_and_stop_wins(tmp_path, clock):
    r = RiskManager(make_cfg(tmp_path), clock)
    assert r.check_flags() is None
    (tmp_path / "stop.flag").write_text("")
    assert r.check_flags() == "halt" and r.halted and r.halt_reason == "stop.flag"
    assert not (tmp_path / "stop.flag").exists()                        # consumed: the persisted `halted` is the truth
    assert r.check_flags() is None
    r.resume()                                                          # a Telegram /start must really resume
    assert r.check_flags() is None and not r.halted
    (tmp_path / "start.flag").write_text("")
    assert r.check_flags() == "resume_noop" and not r.halted            # start while running: consumed, reported
    (tmp_path / "stop.flag").write_text("")
    (tmp_path / "start.flag").write_text("")
    assert r.check_flags() == "halt" and r.halted                       # both present: stop wins
    assert not (tmp_path / "stop.flag").exists() and not (tmp_path / "start.flag").exists()
    (tmp_path / "start.flag").write_text("")
    assert r.check_flags() == "resume" and not r.halted


def test_undeletable_flag_never_disables_the_stop(tmp_path, clock):
    r = RiskManager(make_cfg(tmp_path), clock)
    (tmp_path / "start.flag").mkdir()                                   # a directory is not a flag
    (tmp_path / "stop.flag").write_text("")
    assert r.check_flags() == "halt" and r.halted                       # no exception, stop honoured
    assert r.check_flags() is None and r.halted                         # the directory does not resume the bot
    (tmp_path / "start.flag").rmdir()
    (tmp_path / "start.flag").write_text("")
    tmp_path.chmod(0o555)                                               # a real flag that cannot be unlinked
    try:
        assert r.check_flags() == "resume" and not r.halted             # honoured once...
        r.halt("telegram")
        assert r.check_flags() is None and r.halted                     # ...then ignored: it must not undo a later halt
    finally:
        tmp_path.chmod(0o755)


def test_funding_gate_net_of_both_legs(tmp_path, clock):
    r = RiskManager(make_cfg(tmp_path, funding_block_s=600.0), clock)
    now = clock()
    # short blofin receives +0.01%, long mexc pays +0.05% -> net -0.04% within window -> blocked
    r.set_funding("blofin", {"XYZUSDT": (0.0001, now + 300)})
    r.set_funding("mexc", {"XYZUSDT": (0.0005, now + 300)})
    assert r.funding_blocks("XYZUSDT", "blofin", "mexc")
    assert not r.funding_blocks("XYZUSDT", "mexc", "blofin")           # reversed legs: net +0.04%
    r.set_funding("mexc", {"XYZUSDT": (0.000101, now + 300)})          # 0.0001 % apart: below the threshold
    assert not r.funding_blocks("XYZUSDT", "blofin", "mexc")
    r.set_funding("mexc", {"XYZUSDT": (0.0005, now + 3600)})           # mexc settles outside the window
    assert not r.funding_blocks("XYZUSDT", "blofin", "mexc")
    assert not r.funding_blocks("NOFUNDUSDT", "blofin", "mexc")        # unknown -> allowed
    r.set_funding("mexc", {"XYZUSDT": (0.0005, now - 7200)})           # dead feed: stamp hours in the past
    assert not r.funding_blocks("XYZUSDT", "blofin", "mexc") and "mexc|XYZUSDT" in r.stale_funding
    r.set_funding("mexc", {"XYZUSDT": (0.0005, now + 300)})
    assert r.funding_blocks("XYZUSDT", "blofin", "mexc") and not r.stale_funding
    r.set_funding("mexc", {"XYZUSDT": ("bad", None)})                  # malformed feed value: logged, ignored
    assert r.funding["mexc|XYZUSDT"] == (0.0005, now + 300)


def test_record_close_win_rate_window_and_symbol_blacklist(tmp_path, clock):
    r = RiskManager(make_cfg(tmp_path), clock)
    key = pair_key("XYZUSDT", "blofin", "mexc")
    mk = lambda pnl: Position(1, "XYZUSDT", "blofin", "mexc", OPEN, "TT", size_usd=25.0, net_pnl_usd=pnl)
    r.record_close(mk(-0.05))                            # -0.2% of size -> 6 h symbol blacklist
    assert r.symbol_blacklist["XYZUSDT"] == clock() + 21_600.0
    r.record_close(mk(0.0))                              # zero P&L: neither win nor loss
    r.record_close(mk(0.01), counts_as_trade=False)      # reconciliation close: ignored
    assert r.pair_stats[key]["wins"] == 0 and r.pair_stats[key]["losses"] == 1
    assert r.pair_stats[key]["total_pnl"] == pytest.approx(-0.05)
    ok = lambda: r.entry_allowed("XYZUSDT", "blofin", "mexc", 25.0, 0)
    clock.tick(21_601)
    for pnl in (-0.01, -0.01, -0.01, 0.02):              # 1 win / 4 losses inside the window -> 20 % < 30 %
        r.record_close(mk(pnl))
    assert ok() == (False, "pair_win_rate")
    clock.tick(86_401)                                   # outcomes age out of the window: the route can recover
    assert ok() == (True, "ok")


def test_strikes_decay_and_state_roundtrip(tmp_path, clock):
    r = RiskManager(make_cfg(tmp_path), clock)
    key = pair_key("XYZUSDT", "blofin", "mexc")
    assert not r.record_strike("XYZUSDT", "blofin", "mexc")
    clock.tick(21_601)                                   # strike_decay_s: the old strike is forgotten
    assert not r.record_strike("XYZUSDT", "blofin", "mexc")
    assert r.pair_strikes[key]["n"] == 1
    r.record_close(Position(1, "ABCUSDT", "blofin", "mexc", OPEN, "TT", size_usd=25.0, net_pnl_usd=-0.05))
    r.mismatch.blacklist("QQQUSDT|blofin|mexc", 80.0)
    d = r.to_dict()
    r.record_close(Position(2, "ABCUSDT", "blofin", "mexc", OPEN, "TT", size_usd=25.0, net_pnl_usd=0.05))
    assert d["pair_stats"][pair_key("ABCUSDT", "blofin", "mexc")]["wins"] == 0    # to_dict() is a snapshot
    r2 = RiskManager(make_cfg(tmp_path), clock)
    r2.load(d)
    assert r2.symbol_blacklist == r.symbol_blacklist and r2.pair_strikes == r.pair_strikes
    assert r2.pair_stats[pair_key("ABCUSDT", "blofin", "mexc")]["losses"] == 1
    assert r2.mismatch.is_blacklisted("QQQUSDT|blofin|mexc") and r2.mismatch.blacklisted["QQQUSDT|blofin|mexc"]["spread_pct"] == 80.0
    clock.tick(30_000)
    r3 = RiskManager(make_cfg(tmp_path), clock)
    r3.load(d)
    assert r3.symbol_blacklist == {} and r3.pair_strikes == {}          # expired entries dropped on load


def test_load_tolerates_corrupt_and_legacy_state(tmp_path, clock):
    r = RiskManager(make_cfg(tmp_path), clock)
    now = clock()
    r.load({"pair_blacklist": {"k": "soon", "ok": now + 100}, "pair_strikes": {"k": None, "old": 1},
            "cooldowns": None, "pair_stats": {"p": {"wins": "x"}, "q": {"wins": 2, "losses": 1, "total_pnl": 0.1}},
            "mismatch_blacklist": ["legacy|a|b"], "venue_symbol_blacklist": "notalist", "halted": 1})
    assert r.pair_blacklist == {"ok": now + 100} and r.pair_strikes == {"old": {"n": 1, "ts": now}}
    assert r.cooldowns == {} and "p" not in r.pair_stats and r.pair_stats["q"]["recent"] == []
    assert r.mismatch.is_blacklisted("legacy|a|b") and r.venue_symbol_blacklist == set() and r.halted
    r.load("garbage")                                                   # not even an object: fresh state, no raise
    assert not r.halted and r.pair_blacklist == {}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_risk.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'bbo_trader.risk'`

- [ ] **Step 4: Implement `bbo_trader/risk.py`**

```python
"""Risk: manual halt, blacklists and strikes, mismatch guard, balances, funding gate.

Rules of this module:
- `entry_allowed` never raises and fails CLOSED on unknown input (unknown or quote-only venue,
  missing/stale balance cache in live mode); every rejection has a stable reason string that the
  strategy counts in the funnel.
- There is no automatic kill switch in v1 (user decision). Halting is manual: `stop.flag`/`start.flag`
  in DATA_DIR (edge-triggered, consumed on read, stop wins over a simultaneous start) or Telegram
  /stop and /start. The halt survives restarts through the persisted `halted` flag, not the files.
- One position per symbol is NOT gated here: the app drives a held symbol's position instead of
  re-evaluating it (`App.on_quote`), so a held symbol never reaches `entry_allowed`.
"""
from __future__ import annotations

import logging
import math
import time
from pathlib import Path

from .config import Config
from .models import Position

log = logging.getLogger("bbo.risk")

RECENT_OUTCOMES_KEEP = 20          # per route; the win-rate gate reads the ones inside pair_stats_window_s
FUNDING_STALE_GRACE_S = 60.0       # a settle stamp this far in the past is a late poll, further back a dead feed
BALANCE_HEADROOM = 1.05            # notional × 1.05 must be available (conservative: leverage is not modelled)


def pair_key(symbol: str, venue_a: str, venue_b: str) -> str:
    return f"{symbol}|{venue_a}>{venue_b}"


def route_key(symbol: str, venue_x: str, venue_y: str) -> str:
    """Direction-free key for a venue pair (mismatch guard)."""
    return f"{symbol}|" + "|".join(sorted((venue_x, venue_y)))


# ---- tolerant state parsers: a hand-edited or version-skewed `risk` section must never crash start-up ----
def _float_map(raw: object, now: float | None = None) -> dict[str, float]:
    """`{key: float}`; drops unparsable entries and, when `now` is given, expired ones."""
    out: dict[str, float] = {}
    if not isinstance(raw, dict):
        return out
    for k, v in raw.items():
        try:
            t = float(v)
        except (TypeError, ValueError):
            log.warning("RISK_STATE_DROP %r=%r", k, v)
            continue
        if math.isfinite(t) and (now is None or t > now):
            out[str(k)] = t
    return out


def _strikes_from(raw: object, now: float, decay_s: float) -> dict[str, dict]:
    out: dict[str, dict] = {}
    if not isinstance(raw, dict):
        return out
    for k, v in raw.items():
        try:
            if isinstance(v, dict):
                n, ts = int(v.get("n", 0)), float(v.get("ts", now))
            else:
                n, ts = int(v), now            # legacy bare count: starts decaying from this load
        except (TypeError, ValueError):
            log.warning("RISK_STATE_DROP pair_strikes %r=%r", k, v)
            continue
        if n > 0 and now - ts <= decay_s:
            out[str(k)] = {"n": n, "ts": ts}
    return out


def _stats_from(raw: object) -> dict[str, dict]:
    out: dict[str, dict] = {}
    if not isinstance(raw, dict):
        return out
    for k, v in raw.items():
        try:
            recent = [[float(ts), bool(won)] for ts, won in (v.get("recent") or [])]
            out[str(k)] = {"wins": int(v.get("wins", 0)), "losses": int(v.get("losses", 0)),
                           "total_pnl": float(v.get("total_pnl", 0.0)), "recent": recent[-RECENT_OUTCOMES_KEEP:]}
        except (TypeError, ValueError, AttributeError):
            log.warning("RISK_STATE_DROP pair_stats %r=%r", k, v)
    return out


def _mismatch_from(raw: object) -> dict[str, dict]:
    """Accepts the legacy bare list of keys or the current `{key: {ts, spread_pct, tier}}`. A key is
    never dropped over a bad detail: the blacklist is a safety list."""
    if isinstance(raw, list):
        return {str(k): {"ts": 0.0, "spread_pct": 0.0, "tier": "legacy"} for k in raw}
    out: dict[str, dict] = {}
    if isinstance(raw, dict):
        for k, v in raw.items():
            try:
                out[str(k)] = {"ts": float(v.get("ts", 0.0)), "spread_pct": float(v.get("spread_pct", 0.0)),
                               "tier": str(v.get("tier", ""))}
            except (TypeError, ValueError, AttributeError):
                out[str(k)] = {"ts": 0.0, "spread_pct": 0.0, "tier": "legacy"}
    return out


class MismatchGuard:
    """Same-ticker-different-asset detector: |raw mid spread| stays absurd for N consecutive quotes.
    Two tiers: slow (moderately absurd for long) and fast (grossly absurd for a few quotes). A hit is
    persistent for the deployment (spec) and records when/why so the dashboard can show it; lift one
    by hand by removing its key from `mismatch_blacklist` in the state file."""

    def __init__(self, slow_pct: float, slow_n: int, fast_pct: float, fast_n: int,
                 blacklisted: dict[str, dict] | None = None, clock=time.time):
        if not (slow_pct > 0.0 and fast_pct > 0.0) or slow_n < 1 or fast_n < 1:
            raise ValueError(f"mismatch thresholds must be positive: slow {slow_pct}%x{slow_n}, fast {fast_pct}%x{fast_n}")
        self.slow_pct, self.slow_n, self.fast_pct, self.fast_n = slow_pct, slow_n, fast_pct, fast_n
        self.blacklisted: dict[str, dict] = dict(blacklisted or {})   # key -> {"ts", "spread_pct", "tier"}
        self.clock = clock
        self._slow: dict[str, int] = {}
        self._fast: dict[str, int] = {}

    def observe(self, key: str, raw_spread_pct: float) -> bool:
        """Returns True when this observation newly blacklists the pair (NaN never counts as absurd)."""
        if key in self.blacklisted:
            return False
        a = abs(raw_spread_pct)
        self._slow[key] = self._slow.get(key, 0) + 1 if a > self.slow_pct else 0
        self._fast[key] = self._fast.get(key, 0) + 1 if a > self.fast_pct else 0
        tier = "fast" if self._fast[key] >= self.fast_n else "slow" if self._slow[key] >= self.slow_n else ""
        if not tier:
            return False
        self.blacklist(key, raw_spread_pct, tier)
        return True

    def blacklist(self, key: str, spread_pct: float = 0.0, tier: str = "manual") -> None:
        self.blacklisted[key] = {"ts": self.clock(), "spread_pct": spread_pct, "tier": tier}
        self._slow.pop(key, None)
        self._fast.pop(key, None)
        log.warning("MISMATCH_BLACKLIST %s spread=%.2f%% tier=%s", key, spread_pct, tier)

    def is_blacklisted(self, key: str) -> bool:
        return key in self.blacklisted


class RiskManager:
    def __init__(self, cfg: Config, clock=time.time):
        self.cfg = cfg
        self.clock = clock
        self._venues = {v.name: v for v in cfg.venues}
        self.halted = False
        self.halt_reason = ""
        self.pair_strikes: dict[str, dict] = {}         # key -> {"n": strikes, "ts": last strike}
        self.pair_blacklist: dict[str, float] = {}      # key -> until ts
        self.symbol_blacklist: dict[str, float] = {}    # symbol -> until ts
        self.cooldowns: dict[str, float] = {}           # symbol -> until ts
        self.venue_symbol_blacklist: set[str] = set()   # "venue|symbol"
        self.balances: dict[str, dict] = {}             # venue -> {"available","total","locked","ts"}
        self.funding: dict[str, tuple[float, float]] = {}  # "venue|symbol" -> (rate fraction, next_settle_ts)
        self.stale_funding: set[str] = set()            # "venue|symbol" whose settle stamp is in the past (dead feed)
        self.pair_stats: dict[str, dict] = {}           # key -> {"wins","losses","total_pnl","recent": [[ts, won],..]}
        self.mismatch = MismatchGuard(cfg.mismatch_slow_pct, cfg.mismatch_slow_n,
                                      cfg.mismatch_fast_pct, cfg.mismatch_fast_n, clock=clock)
        self._dead_flags: dict[Path, tuple[int, int]] = {}   # undeletable flag -> (inode, mtime_ns): ignored until replaced

    # ---- halt ----------------------------------------------------------------
    def halt(self, reason: str) -> None:
        self.halted, self.halt_reason = True, reason
        self._consume_flags()

    def resume(self) -> None:
        self.halted, self.halt_reason = False, ""
        self._consume_flags()          # a stale stop.flag must never re-halt a deliberate resume

    def _flag_path(self, name: str) -> Path:
        return self.cfg.data_dir / name

    @staticmethod
    def _signature(p: Path) -> tuple[int, int] | None:
        try:
            st = p.stat()
        except OSError:
            return None
        return st.st_ino, st.st_mtime_ns

    def _flag_present(self, p: Path) -> bool:
        try:
            if p in self._dead_flags:
                if self._signature(p) == self._dead_flags[p]:
                    return False       # the very file we could not unlink: keep ignoring it
                del self._dead_flags[p]    # gone or replaced: treat the new one as a fresh flag
            return p.is_file()         # a directory or a socket is not a flag
        except OSError as e:
            log.error("FLAG_CHECK_FAILED %s: %s", p, e)
            return False

    def _consume_flags(self) -> None:
        for name in (self.cfg.halt_flag, self.cfg.resume_flag):
            p = self._flag_path(name)
            try:
                p.unlink()
            except FileNotFoundError:
                pass
            except OSError as e:
                log.error("FLAG_UNLINK_FAILED %s: %s — ignoring this flag until it is removed by hand", p, e)
                sig = self._signature(p)
                if sig is not None:
                    self._dead_flags[p] = sig

    def check_flags(self) -> str | None:
        """Dashboard/operator flags in DATA_DIR, edge-triggered and consumed on read: `stop.flag` halts,
        `start.flag` resumes; both present → stop wins. Returns "halt" / "resume" on a transition,
        "halt_noop" / "resume_noop" when the flag asked for the state we are already in, else None."""
        has_stop = self._flag_present(self._flag_path(self.cfg.halt_flag))
        has_start = self._flag_present(self._flag_path(self.cfg.resume_flag))
        if not has_stop and not has_start:
            return None
        if has_stop:
            if has_start:
                log.warning("FLAGS %s and %s both present — stop wins", self.cfg.halt_flag, self.cfg.resume_flag)
            if self.halted:
                self._consume_flags()
                return "halt_noop"
            self.halt("stop.flag")
            return "halt"
        if not self.halted:
            self._consume_flags()
            return "resume_noop"
        self.resume()
        return "resume"

    # ---- gates ---------------------------------------------------------------
    def venue_symbol_blocked(self, venue: str, symbol: str) -> bool:
        if f"{venue}|{symbol}" in self.venue_symbol_blacklist:
            return True
        vc = self._venues.get(venue)
        if vc is None or vc.role != "trade":
            return True                # unknown or quote-only venue: fail closed, never raise from a gate
        return bool(vc.symbol_whitelist) and symbol not in vc.symbol_whitelist

    def _pair_win_rate_blocks(self, key: str, now: float) -> bool:
        st = self.pair_stats.get(key)
        if not st:
            return False
        recent = [won for ts, won in st.get("recent", []) if now - ts <= self.cfg.pair_stats_window_s]
        if len(recent) < self.cfg.pair_min_trades:
            return False
        return sum(1 for won in recent if won) / len(recent) < self.cfg.pair_min_win_rate

    def entry_allowed(self, symbol: str, venue_a: str, venue_b: str, size_usd: float,
                      open_count: int) -> tuple[bool, str]:
        """Cheap/global gates first, then per-route ones. The balance gate compares USDT `available`
        against NOTIONAL × 1.05 (leverage is not modelled: conservative). In live mode a missing or
        stale balance cache fails closed as "balance_unknown"; paper mode stays permissive."""
        now = self.clock()
        if self.halted:
            return False, "halted"
        if venue_a == venue_b or not size_usd > 0.0:
            return False, "invalid"
        if open_count >= self.cfg.max_concurrent:
            return False, "max_concurrent"
        if symbol in self.cfg.blocked_symbols:
            return False, "blocked_symbol"
        if self.cooldowns.get(symbol, 0.0) > now:
            return False, "cooldown"
        if self.symbol_blacklist.get(symbol, 0.0) > now:
            return False, "symbol_blacklist"
        for v in (venue_a, venue_b):
            if self.venue_symbol_blocked(v, symbol):
                return False, "venue_blocked"
        key = pair_key(symbol, venue_a, venue_b)
        if self.pair_blacklist.get(key, 0.0) > now:
            return False, "pair_blacklist"
        if self.mismatch.is_blacklisted(route_key(symbol, venue_a, venue_b)):
            return False, "mismatch"
        if self._pair_win_rate_blocks(key, now):
            return False, "pair_win_rate"
        for v in (venue_a, venue_b):
            bal = self.balances.get(v)
            if bal is None or now - bal.get("ts", 0.0) > self.cfg.balance_max_age_s:
                if self.cfg.mode == "live":
                    return False, "balance_unknown"
            elif bal.get("available", 0.0) < size_usd * BALANCE_HEADROOM:
                return False, "balance"
        return True, "ok"

    def funding_blocks(self, symbol: str, venue_a: str, venue_b: str) -> bool:
        """Rates are FRACTIONS as exchanges deliver them (0.0001 = 0.01 %); positive = longs pay shorts.
        Short on A receives A's rate, long on B pays B's rate. Blocks only when the settlements inside
        `funding_block_s` net to a cost above `funding_block_min_pct` (percent points): both v1 venues
        settle on the same 8-hour grid with near-identical rates, so a zero threshold would block half
        of all routes over a few thousandths of a basis point. A settle stamp already in the past means
        the feed is dead for that key: treated as unknown (allowed) and reported in `stale_funding`."""
        now = self.clock()
        net, in_window = 0.0, False
        for k, sign in ((f"{venue_a}|{symbol}", 1.0), (f"{venue_b}|{symbol}", -1.0)):
            f = self.funding.get(k)
            if f is None:
                continue
            rate, settle = f
            dt = settle - now
            if dt < -FUNDING_STALE_GRACE_S:
                if k not in self.stale_funding:
                    self.stale_funding.add(k)
                    log.warning("FUNDING_STALE %s settle=%.0f now=%.0f — gate disabled for this key", k, settle, now)
                continue
            self.stale_funding.discard(k)
            if 0.0 <= dt < self.cfg.funding_block_s:
                net += sign * rate
                in_window = True
        return in_window and net * 100.0 < -self.cfg.funding_block_min_pct

    # ---- bookkeeping ---------------------------------------------------------
    def record_strike(self, symbol: str, venue_a: str, venue_b: str) -> bool:
        """A failed/one-legged entry or failed hedge. Strikes older than `strike_decay_s` are forgotten;
        `pair_strikes_to_blacklist` strikes inside that window blacklist the route for `pair_blacklist_s`."""
        now = self.clock()
        key = pair_key(symbol, venue_a, venue_b)
        st = self.pair_strikes.get(key)
        n = (st["n"] if st is not None and now - st["ts"] <= self.cfg.strike_decay_s else 0) + 1
        if n >= self.cfg.pair_strikes_to_blacklist:
            self.pair_blacklist[key] = now + self.cfg.pair_blacklist_s
            self.pair_strikes.pop(key, None)
            log.warning("PAIR_BLACKLIST %s for %.0fs after %d strikes", key, self.cfg.pair_blacklist_s, n)
            return True
        self.pair_strikes[key] = {"n": n, "ts": now}
        return False

    def set_cooldown(self, symbol: str, seconds: float | None = None) -> None:
        self.cooldowns[symbol] = self.clock() + (self.cfg.failed_entry_cooldown_s if seconds is None else seconds)

    def blacklist_venue_symbol(self, venue: str, symbol: str) -> None:
        self.venue_symbol_blacklist.add(f"{venue}|{symbol}")

    def record_close(self, pos: Position, counts_as_trade: bool = True) -> None:
        """Bookkeeping for a closed position. `counts_as_trade=False` (reconciliation, never-filled entries)
        leaves the stats alone; a zero-P&L close is neither a win nor a loss."""
        if not counts_as_trade:
            return
        now = self.clock()
        key = pair_key(pos.symbol, pos.venue_a, pos.venue_b)
        st = self.pair_stats.setdefault(key, {"wins": 0, "losses": 0, "total_pnl": 0.0, "recent": []})
        st.setdefault("recent", [])
        st["total_pnl"] += pos.net_pnl_usd
        if pos.net_pnl_usd != 0.0:
            won = pos.net_pnl_usd > 0.0
            st["wins" if won else "losses"] += 1
            st["recent"] = (st["recent"] + [[now, won]])[-RECENT_OUTCOMES_KEEP:]
        if pos.size_usd > 0 and pos.net_pnl_usd / pos.size_usd * 100.0 < self.cfg.symbol_loss_pct:
            self.symbol_blacklist[pos.symbol] = now + self.cfg.symbol_loss_blacklist_s
            log.warning("SYMBOL_BLACKLIST %s for %.0fs after %+.4f on $%.2f", pos.symbol,
                        self.cfg.symbol_loss_blacklist_s, pos.net_pnl_usd, pos.size_usd)

    def set_balance(self, venue: str, available: float, total: float) -> None:
        self.balances[venue] = {"available": available, "total": total, "locked": max(0.0, total - available),
                                "ts": self.clock()}

    def set_funding(self, venue: str, rates: dict[str, tuple[float, float]]) -> None:
        """`rates`: symbol -> (rate as a FRACTION, next settlement unix ts)."""
        for symbol, val in rates.items():
            try:
                rate, settle = val
                self.funding[f"{venue}|{symbol}"] = (float(rate), float(settle))
            except (TypeError, ValueError):
                log.warning("FUNDING_BAD %s %s %r", venue, symbol, val)

    # ---- persistence ---------------------------------------------------------
    def to_dict(self) -> dict:
        """A snapshot: nothing here aliases live state (the saver serializes off the event loop)."""
        now = self.clock()
        return {"halted": self.halted, "halt_reason": self.halt_reason,
                "pair_strikes": {k: dict(v) for k, v in self.pair_strikes.items()
                                 if now - v.get("ts", 0.0) <= self.cfg.strike_decay_s},
                "pair_blacklist": {k: t for k, t in self.pair_blacklist.items() if t > now},
                "symbol_blacklist": {k: t for k, t in self.symbol_blacklist.items() if t > now},
                "cooldowns": {k: t for k, t in self.cooldowns.items() if t > now},
                "venue_symbol_blacklist": sorted(self.venue_symbol_blacklist),
                "pair_stats": {k: {**v, "recent": [list(r) for r in v.get("recent", [])]}
                               for k, v in self.pair_stats.items()},
                "mismatch_blacklist": {k: dict(v) for k, v in self.mismatch.blacklisted.items()},
                "stale_funding": sorted(self.stale_funding)}

    def load(self, d: object) -> None:
        """Tolerant: a malformed `risk` section degrades to empty risk state (with warnings), never to a
        crash loop under systemd Restart=always."""
        if not isinstance(d, dict):
            if d:
                log.warning("RISK_STATE_IGNORED not an object: %r", type(d).__name__)
            d = {}
        now = self.clock()
        self.halted = bool(d.get("halted", False))
        self.halt_reason = str(d.get("halt_reason") or "")
        self.pair_strikes = _strikes_from(d.get("pair_strikes"), now, self.cfg.strike_decay_s)
        self.pair_blacklist = _float_map(d.get("pair_blacklist"), now)
        self.symbol_blacklist = _float_map(d.get("symbol_blacklist"), now)
        self.cooldowns = _float_map(d.get("cooldowns"), now)
        raw_vs = d.get("venue_symbol_blacklist")
        self.venue_symbol_blacklist = {str(x) for x in raw_vs} if isinstance(raw_vs, list) else set()
        self.pair_stats = _stats_from(d.get("pair_stats"))
        self.mismatch.blacklisted = _mismatch_from(d.get("mismatch_blacklist"))
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_risk.py -q`
Expected: `9 passed`

- [ ] **Step 6: Commit**

```bash
git add deploy-bbo/bbo_trader/risk.py deploy-bbo/tests/test_risk.py deploy-bbo/tests/conftest.py
git commit -m "feat(bbo): risk manager — halt flags, strikes/blacklists, mismatch guard, funding gate"
```

---

### Task 9: Strategy — PairEvaluator (`strategy.py`)

**Files:**
- Create: `deploy-bbo/bbo_trader/strategy.py`
- Test: `deploy-bbo/tests/test_strategy.py`

- [ ] **Step 1: Write the failing tests**

`tests/test_strategy.py`:

```python
from collections import Counter
from dataclasses import replace

import pytest

from bbo_trader.config import VenueConfig, RateLimits
from bbo_trader.models import Fees, MAKER_RESTING, OPEN, EXIT_MAKER_RESTING
from bbo_trader.positions import PositionBook
from bbo_trader.quotes import QuoteBoard
from bbo_trader.risk import RiskManager
from bbo_trader.strategy import PairEvaluator
from tests.conftest import make_cfg, mk_bbo, mk_spec, MEXC_FEES, BLOFIN_FEES

SYM = "XYZUSDT"
FEES = {"mexc": MEXC_FEES, "blofin": BLOFIN_FEES, "gate": Fees(0.05, 0.02)}


def build(tmp_path, clock, **over):
    cfg = make_cfg(tmp_path, **over)
    board = QuoteBoard(cfg.stale_quote_s)
    specs = {v: {SYM: mk_spec(v, SYM)} for v in ("mexc", "blofin", "gate")}
    risk = RiskManager(cfg, clock)
    ev = PairEvaluator(cfg, board, FEES, specs, volumes={}, risk=risk, funnel=Counter(), clock=clock)
    return cfg, board, risk, ev


def test_entry_picks_tt_when_it_pays(tmp_path, clock):
    cfg, board, risk, ev = build(tmp_path, clock)
    now = clock()
    board.set(mk_bbo("blofin", SYM, 1.0050, 1.0060, ts=now))
    board.set(mk_bbo("mexc", SYM, 1.0000, 1.0008, ts=now))
    it = ev.evaluate_entry(SYM, equity=200.0, open_count=0, resting_counts={})
    assert it.kind == "TT_ENTER" and (it.venue_a, it.venue_b) == ("blofin", "mexc")
    assert it.size_usd == 25.0 and it.maker_venue == "" and it.ts == now
    assert ev.funnel["candidate"] == 1 and ev.funnel["below_edge"] == 1   # the reverse direction


def test_entry_picks_tm_with_price_and_respects_gates(tmp_path, clock):
    cfg, board, risk, ev = build(tmp_path, clock)
    now = clock()
    board.set(mk_bbo("blofin", SYM, 1.0041, 1.0061, ts=now))
    board.set(mk_bbo("mexc", SYM, 1.0000, 1.0010, ts=now))
    it = ev.evaluate_entry(SYM, 200.0, 0, {})
    assert it.kind == "TM_ENTER" and it.maker_venue == "blofin" and it.rest_price == pytest.approx(1.0061)
    # maker slots full on blofin -> nothing
    assert ev.evaluate_entry(SYM, 200.0, 0, {"blofin": 2}).kind == "NONE" and ev.funnel["maker_slots"] == 1
    # thin hedge touch (mexc ask qty) -> touch_depth
    board.set(mk_bbo("mexc", SYM, 1.0000, 1.0010, aq=5, ts=now))
    assert ev.evaluate_entry(SYM, 200.0, 0, {}).kind == "NONE" and ev.funnel["touch_depth"] == 1
    board.set(mk_bbo("mexc", SYM, 1.0000, 1.0010, ts=now))
    # low volume on one venue -> volume
    ev.volumes["mexc"] = {SYM: 1000.0}
    assert ev.evaluate_entry(SYM, 200.0, 0, {}).kind == "NONE" and ev.funnel["volume"] == 1
    ev.volumes.clear()
    # risk gate (max concurrent) is counted under its own reason
    assert ev.evaluate_entry(SYM, 200.0, 3, {}).kind == "NONE" and ev.funnel["max_concurrent"] == 1
    # tiny equity -> size below minimum
    assert ev.evaluate_entry(SYM, 40.0, 0, {}).reason == "size_below_min"
    # stale quote on one venue -> no pair
    clock.tick(5)
    assert ev.evaluate_entry(SYM, 200.0, 0, {}).reason == "no_pair"


def test_insane_spread_and_mismatch_guard(tmp_path, clock):
    cfg, board, risk, ev = build(tmp_path, clock, mismatch_fast_n=2)
    now = clock()
    board.set(mk_bbo("blofin", SYM, 2.0, 2.001, ts=now))
    board.set(mk_bbo("mexc", SYM, 1.0, 1.001, ts=now))
    assert ev.evaluate_entry(SYM, 200.0, 0, {}).kind == "NONE" and ev.funnel["insane"] == 2
    assert ev.evaluate_entry(SYM, 200.0, 0, {}).kind == "NONE"
    assert ev.funnel["mismatch_blacklisted"] == 1 and ev.funnel["mismatch"] == 2


def test_route_rule_takes_best_edge_across_three_venues(tmp_path, clock):
    cfg = make_cfg(tmp_path)
    cfg = replace(cfg, venues=cfg.venues + (VenueConfig("gate", "trade", 0.05, 0.02, RateLimits()),))
    board = QuoteBoard(cfg.stale_quote_s)
    specs = {v: {SYM: mk_spec(v, SYM)} for v in ("mexc", "blofin", "gate")}
    ev = PairEvaluator(cfg, board, FEES, specs, {}, RiskManager(cfg, clock), clock=clock)
    now = clock()
    board.set(mk_bbo("blofin", SYM, 1.0050, 1.0060, ts=now))
    board.set(mk_bbo("gate", SYM, 1.0070, 1.0080, ts=now))     # richer short venue than blofin
    board.set(mk_bbo("mexc", SYM, 1.0000, 1.0008, ts=now))
    it = ev.evaluate_entry(SYM, 200.0, 0, {})
    assert it.kind == "TT_ENTER" and it.venue_a == "gate" and it.venue_b == "mexc"


def test_resting_upgrade_requote_ttl_and_edge_gone(tmp_path, clock):
    cfg, board, risk, ev = build(tmp_path, clock)
    now = clock()
    book = PositionBook()
    pos = book.new(SYM, "blofin", "mexc", MAKER_RESTING, "TM", size_usd=25.0, maker_venue="blofin",
                   maker_rest_price=1.0061, maker_posted_ts=now)
    board.set(mk_bbo("blofin", SYM, 1.0041, 1.0061, ts=now))
    board.set(mk_bbo("mexc", SYM, 1.0000, 1.0010, ts=now))
    assert ev.evaluate_resting(pos).reason == "resting"
    # mexc ask drops one tick -> floor drops, but touch (1.0061) still binds -> no requote
    board.set(mk_bbo("mexc", SYM, 0.9999, 1.0009, ts=now))
    assert ev.evaluate_resting(pos).reason == "resting"
    # blofin ask moves to 1.0063 -> peg follows the touch -> REQUOTE (interval since post is 0, last requote 0)
    board.set(mk_bbo("blofin", SYM, 1.0041, 1.0063, ts=now))
    it = ev.evaluate_resting(pos)
    assert it.kind == "REQUOTE" and it.rest_price == pytest.approx(1.0063)
    pos.maker_last_requote_ts = now
    board.set(mk_bbo("blofin", SYM, 1.0041, 1.0065, ts=now))
    assert ev.evaluate_resting(pos).reason == "resting"          # blofin min_requote 1000 ms not elapsed
    clock.tick(1.1)
    board.set(mk_bbo("blofin", SYM, 1.0041, 1.0065, ts=clock()))
    board.set(mk_bbo("mexc", SYM, 0.9999, 1.0009, ts=clock()))
    assert ev.evaluate_resting(pos).kind == "REQUOTE"
    # TT edge appears -> upgrade
    board.set(mk_bbo("blofin", SYM, 1.0060, 1.0065, ts=clock()))
    assert ev.evaluate_resting(pos).kind == "UPGRADE_TT"
    # edge gone: improving one tick inside a one-tick-wide blofin book would sit on the bid (post-only
    # would cross) -> no valid price; TT disabled so the upgrade branch does not pre-empt it
    cfg2 = replace(cfg, improve_ticks=1, tt_enabled=False)
    ev2 = PairEvaluator(cfg2, board, FEES, ev.specs, {}, risk, clock=clock)
    board.set(mk_bbo("blofin", SYM, 1.0064, 1.0065, ts=clock()))
    board.set(mk_bbo("mexc", SYM, 1.0000, 1.0010, ts=clock()))
    assert ev2.evaluate_resting(pos).reason == "edge_gone_wait"
    clock.tick(0.35)
    board.set(mk_bbo("blofin", SYM, 1.0064, 1.0065, ts=clock()))
    board.set(mk_bbo("mexc", SYM, 1.0000, 1.0010, ts=clock()))
    assert ev2.evaluate_resting(pos).reason == "edge_gone"
    # TTL
    clock.tick(30)
    board.set(mk_bbo("blofin", SYM, 1.0041, 1.0061, ts=clock()))
    board.set(mk_bbo("mexc", SYM, 1.0000, 1.0010, ts=clock()))
    assert ev.evaluate_resting(pos).reason == "ttl"
    # stale quotes
    clock.tick(5)
    assert ev.evaluate_resting(pos).reason == "stale"


def test_exit_triggers_and_tm_exit(tmp_path, clock):
    cfg, board, risk, ev = build(tmp_path, clock)
    now = clock()
    book = PositionBook()
    pos = book.new(SYM, "blofin", "mexc", OPEN, "TT", size_usd=25.0, entry_spread_pct=0.45, entry_time=now)
    board.set(mk_bbo("blofin", SYM, 1.0020, 1.0030, ts=now))
    board.set(mk_bbo("mexc", SYM, 1.0000, 1.0005, ts=now))
    it = ev.evaluate_exit(pos, {})
    assert it.kind == "TM_EXIT" and it.maker_venue == "blofin" and it.rest_price == pytest.approx(1.0015)
    assert pos.current_spread_pct == pytest.approx((1.0020 - 1.0005) / 1.0005 * 100)
    assert ev.evaluate_exit(pos, {"blofin": 2}).reason == "maker_slots"
    # resting exit maker: requote when the mexc bid moves a tick
    pos.status, pos.maker_venue, pos.maker_rest_price = EXIT_MAKER_RESTING, "blofin", 1.0015
    board.set(mk_bbo("mexc", SYM, 1.0002, 1.0006, ts=now))
    it = ev.evaluate_exit(pos, {})
    assert it.kind == "REQUOTE" and it.rest_price == pytest.approx(1.0017)
    # convergence -> TT exit even while resting
    board.set(mk_bbo("blofin", SYM, 1.0008, 1.0010, ts=now))
    assert ev.evaluate_exit(pos, {}).reason == "convergence"
    # stop: entry-side spread widened beyond entry + 1.5
    board.set(mk_bbo("blofin", SYM, 1.0210, 1.0220, ts=now))
    board.set(mk_bbo("mexc", SYM, 1.0000, 1.0005, ts=now))
    assert ev.evaluate_exit(pos, {}).reason == "stop"
    # timeout
    board.set(mk_bbo("blofin", SYM, 1.0020, 1.0030, ts=now))
    clock.tick(31 * 60)
    board.set(mk_bbo("blofin", SYM, 1.0020, 1.0030, ts=clock()))
    board.set(mk_bbo("mexc", SYM, 1.0000, 1.0005, ts=clock()))
    assert ev.evaluate_exit(pos, {}).reason == "timeout"
    # tm exit disabled -> hold
    ev3 = PairEvaluator(replace(cfg, tm_exit_enabled=False, max_hold_min=1e9), board, FEES, ev.specs, {}, risk, clock=clock)
    pos.status = OPEN
    assert ev3.evaluate_exit(pos, {}).reason == "hold"
    # stale while resting -> cancel
    pos.status = EXIT_MAKER_RESTING
    clock.tick(5)
    assert ev.evaluate_exit(pos, {}).reason == "stale"


def test_scan_rows_for_dashboard(tmp_path, clock):
    cfg, board, risk, ev = build(tmp_path, clock)
    now = clock()
    board.set(mk_bbo("blofin", SYM, 1.0050, 1.0060, ts=now))
    board.set(mk_bbo("mexc", SYM, 1.0000, 1.0008, ts=now))
    board.set(mk_bbo("blofin", "ABCUSDT", 1.0, 1.001, ts=now))   # only one venue -> no row
    rows = ev.scan([SYM, "ABCUSDT"])
    assert len(rows) == 1
    r = rows[0]
    assert r["symbol"] == SYM and r["short_exchange"] == "blofin" and r["long_exchange"] == "mexc"
    assert r["fees_pct"] == pytest.approx(0.08) and r["is_candidate"] is True and r["mode"] == "TT"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_strategy.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'bbo_trader.strategy'`

- [ ] **Step 3: Implement `bbo_trader/strategy.py`**

```python
"""PairEvaluator: turns fresh quotes into Intents.

- evaluate_entry: every ordered pair of TRADE venues with a fresh quote for the symbol; gates run
  cheapest-first and every rejection increments the funnel; the best-edge route wins (spec "Route rule").
- evaluate_resting: manage an entry maker order (upgrade to TT, requote, cancel on TTL/edge-gone/stale).
- evaluate_exit: TT exit triggers (convergence/timeout/stop), TM exit posting and requoting.
- scan: best route per symbol for the dashboard's spread_scanner section.
Side effects are limited to funnel counts and the mismatch guard (entry) and the position's
current/peak spread and edge-gone timer (resting/exit)."""
from __future__ import annotations

import time
from collections import Counter
from itertools import permutations
from typing import Iterable

from .config import Config
from .edge import (EdgeParams, evaluate_pair, maker_entry_price, maker_exit_price, exit_spread_tt,
                   needs_requote, best_fee_venue, spread_pct)
from .models import BBO, Fees, Intent, Position, VenueSpec, none, OPEN, EXIT_MAKER_RESTING
from .quotes import QuoteBoard
from .risk import RiskManager, route_key


class PairEvaluator:
    def __init__(self, cfg: Config, board: QuoteBoard, fees: dict[str, Fees],
                 specs: dict[str, dict[str, VenueSpec]], volumes: dict[str, dict[str, float]],
                 risk: RiskManager, funnel: Counter | None = None,
                 trade_venues: list[str] | None = None, clock=time.time):
        self.cfg = cfg
        self.board = board
        self.fees = fees
        self.specs = specs
        self.volumes = volumes
        self.risk = risk
        self.funnel = funnel if funnel is not None else Counter()
        self.trade_venues = list(trade_venues if trade_venues is not None else cfg.trade_venues)
        self.clock = clock

    @property
    def params(self) -> EdgeParams:
        c = self.cfg
        return EdgeParams(c.min_edge_pct, c.tm_extra_edge_pct, c.exit_spread_pct, c.slip_pct,
                          c.tt_enabled, c.tm_entry_enabled, c.maker_venue_policy)

    def spec(self, venue: str, symbol: str) -> VenueSpec | None:
        return self.specs.get(venue, {}).get(symbol)

    def tick(self, venue: str, symbol: str) -> float:
        s = self.spec(venue, symbol)
        return s.tick if s is not None else 0.0001

    def size_for(self, equity: float) -> float:
        return min(self.cfg.max_position_usd, equity * self.cfg.position_size_pct)

    def _fresh_trade_quotes(self, symbol: str, now: float) -> list[BBO]:
        out = []
        for v in self.board.fresh_venues(symbol, now):
            if v in self.trade_venues and self.spec(v, symbol) is not None:
                out.append(self.board.fresh(v, symbol, now))
        return out

    # ---- entries -------------------------------------------------------------
    def evaluate_entry(self, symbol: str, equity: float, open_count: int,
                       resting_counts: dict[str, int]) -> Intent:
        now = self.clock()
        cfg = self.cfg
        size = self.size_for(equity)
        if size < cfg.min_position_usd:
            self.funnel["size_below_min"] += 1
            return none("size_below_min")
        quotes = self._fresh_trade_quotes(symbol, now)
        if len(quotes) < 2:
            self.funnel["no_pair"] += 1
            return none("no_pair")
        best: Intent | None = None
        for qa, qb in permutations(quotes, 2):
            raw = spread_pct(qa.mid, qb.mid)
            rk = route_key(symbol, qa.venue, qb.venue)
            if qa.venue < qb.venue and self.risk.mismatch.observe(rk, raw):   # once per unordered pair
                self.funnel["mismatch_blacklisted"] += 1
            if self.risk.mismatch.is_blacklisted(rk):
                self.funnel["mismatch"] += 1
                continue
            if abs(raw) > cfg.max_sane_spread_pct:
                self.funnel["insane"] += 1
                continue
            fa, fb = self.fees[qa.venue], self.fees[qb.venue]
            pe = evaluate_pair(qa, qb, fa, fb, self.params)
            if pe.mode == "":
                self.funnel["below_edge"] += 1
                continue
            need = size * cfg.touch_depth_mult
            if pe.mode == "TT":
                if qa.touch_notional("sell") < need or qb.touch_notional("buy") < need:
                    self.funnel["touch_depth"] += 1
                    continue
            else:
                hedge_touch = qb.touch_notional("buy") if pe.maker_venue == qa.venue else qa.touch_notional("sell")
                if hedge_touch < need:
                    self.funnel["touch_depth"] += 1
                    continue
                if resting_counts.get(pe.maker_venue, 0) >= cfg.max_resting_makers_per_venue:
                    self.funnel["maker_slots"] += 1
                    continue
            thin = False
            for v in (qa.venue, qb.venue):
                vol = self.volumes.get(v, {}).get(symbol)
                if vol is not None and vol < cfg.min_volume_usd:
                    thin = True
            if thin:
                self.funnel["volume"] += 1
                continue
            if self.risk.funding_blocks(symbol, qa.venue, qb.venue):
                self.funnel["funding"] += 1
                continue
            ok, reason = self.risk.entry_allowed(symbol, qa.venue, qb.venue, size, open_count)
            if not ok:
                self.funnel[reason] += 1
                continue
            px = 0.0
            if pe.mode == "TM":
                px = maker_entry_price(qa, qb, fa, fb, self.params, pe.maker_venue,
                                       self.tick(pe.maker_venue, symbol), cfg.improve_ticks)
                if px is None:
                    self.funnel["maker_price"] += 1
                    continue
            if best is None or pe.edge > best.edge_pct:
                best = Intent(kind="TT_ENTER" if pe.mode == "TT" else "TM_ENTER",
                              reason=f"edge={pe.edge:.3f}", symbol=symbol,
                              venue_a=qa.venue, venue_b=qb.venue, maker_venue=pe.maker_venue,
                              rest_price=px, size_usd=size, edge_pct=pe.edge, spread_pct=pe.spread_tt, ts=now)
        if best is None:
            return none("no_candidate")
        self.funnel["candidate"] += 1
        return best

    # ---- resting entry maker ----------------------------------------------------
    def evaluate_resting(self, pos: Position) -> Intent:
        now = self.clock()
        cfg = self.cfg
        qa = self.board.fresh(pos.venue_a, pos.symbol, now)
        qb = self.board.fresh(pos.venue_b, pos.symbol, now)
        base = dict(symbol=pos.symbol, venue_a=pos.venue_a, venue_b=pos.venue_b, maker_venue=pos.maker_venue, ts=now)
        if qa is None or qb is None:
            return Intent(kind="CANCEL", reason="stale", **base)
        if now - pos.maker_posted_ts >= cfg.maker_ttl_s:
            return Intent(kind="CANCEL", reason="ttl", **base)
        fa, fb = self.fees[pos.venue_a], self.fees[pos.venue_b]
        pe = evaluate_pair(qa, qb, fa, fb, self.params)
        if cfg.tt_enabled and pe.edge_tt >= cfg.min_edge_pct:
            return Intent(kind="UPGRADE_TT", reason=f"edge_tt={pe.edge_tt:.3f}", size_usd=pos.size_usd,
                          edge_pct=pe.edge_tt, spread_pct=pe.spread_tt, **base)
        px = maker_entry_price(qa, qb, fa, fb, self.params, pos.maker_venue,
                               self.tick(pos.maker_venue, pos.symbol), cfg.improve_ticks)
        if px is None:
            if pos.edge_gone_since == 0.0:
                pos.edge_gone_since = now
                return none("edge_gone_wait")
            if now - pos.edge_gone_since >= cfg.edge_gone_ms / 1000.0:
                return Intent(kind="CANCEL", reason="edge_gone", **base)
            return none("edge_gone_wait")
        pos.edge_gone_since = 0.0
        min_gap = cfg.venue(pos.maker_venue).min_requote_ms / 1000.0
        if (needs_requote(pos.maker_rest_price, px, self.tick(pos.maker_venue, pos.symbol), cfg.requote_ticks)
                and now - pos.maker_last_requote_ts >= min_gap):
            return Intent(kind="REQUOTE", reason="peg_moved", rest_price=px, **base)
        return none("resting")

    # ---- exits -------------------------------------------------------------------
    def exit_maker_venue(self, pos: Position) -> str:
        pol = self.cfg.exit_maker_venue_policy
        if pol == "best_fee":
            return best_fee_venue(pos.venue_a, self.fees[pos.venue_a], pos.venue_b, self.fees[pos.venue_b])
        return pol if pol in (pos.venue_a, pos.venue_b) else ""

    def evaluate_exit(self, pos: Position, resting_counts: dict[str, int]) -> Intent:
        now = self.clock()
        cfg = self.cfg
        qa = self.board.fresh(pos.venue_a, pos.symbol, now)
        qb = self.board.fresh(pos.venue_b, pos.symbol, now)
        base = dict(symbol=pos.symbol, venue_a=pos.venue_a, venue_b=pos.venue_b, ts=now)
        if qa is None or qb is None:
            if pos.status == EXIT_MAKER_RESTING:
                return Intent(kind="CANCEL", reason="stale", maker_venue=pos.maker_venue, **base)
            return none("stale")
        x = exit_spread_tt(qa, qb)
        s_now = spread_pct(qa.bid, qb.ask)
        pos.current_spread_pct = s_now
        pos.peak_spread_pct = max(pos.peak_spread_pct, s_now)
        reason = ""
        if x <= cfg.exit_spread_pct:
            reason = "convergence"
        elif now - pos.entry_time >= cfg.max_hold_min * 60.0:
            reason = "timeout"
        elif s_now >= pos.entry_spread_pct + cfg.stop_pct:
            reason = "stop"
        if reason:
            return Intent(kind="TT_EXIT", reason=reason, spread_pct=x, **base)
        if not cfg.tm_exit_enabled:
            return none("hold")
        mv = self.exit_maker_venue(pos)
        if not mv:
            return none("hold")
        px = maker_exit_price(qa, qb, cfg.exit_spread_pct, mv, self.tick(mv, pos.symbol), cfg.improve_ticks)
        if pos.status == OPEN:
            if px is None:
                return none("hold")
            if resting_counts.get(mv, 0) >= cfg.max_resting_makers_per_venue:
                return none("maker_slots")
            return Intent(kind="TM_EXIT", reason="take_profit", maker_venue=mv, rest_price=px, spread_pct=x, **base)
        # EXIT_MAKER_RESTING: keep the peg current
        if px is None or mv != pos.maker_venue:
            return Intent(kind="CANCEL", reason="edge_gone", maker_venue=pos.maker_venue, **base)
        min_gap = cfg.venue(mv).min_requote_ms / 1000.0
        if (needs_requote(pos.maker_rest_price, px, self.tick(mv, pos.symbol), cfg.requote_ticks)
                and now - pos.maker_last_requote_ts >= min_gap):
            return Intent(kind="REQUOTE", reason="peg_moved", maker_venue=mv, rest_price=px, **base)
        return none("resting")

    # ---- dashboard scanner -------------------------------------------------------
    def scan(self, symbols: Iterable[str], limit: int = 100) -> list[dict]:
        now = self.clock()
        rows = []
        for symbol in symbols:
            quotes = self._fresh_trade_quotes(symbol, now)
            best = None
            for qa, qb in permutations(quotes, 2):
                pe = evaluate_pair(qa, qb, self.fees[qa.venue], self.fees[qb.venue], self.params)
                score = max(pe.edge_tt, pe.edge_tm_a, pe.edge_tm_b)
                if best is None or score > best[0]:
                    best = (score, pe, qa, qb)
            if best is None:
                continue
            _score, pe, qa, qb = best
            fees = self.fees[qa.venue].taker + self.fees[qb.venue].taker
            rows.append({"symbol": symbol, "short_exchange": qa.venue, "long_exchange": qb.venue,
                         "short_instrument": "PERP", "long_instrument": "PERP",
                         "spread_pct": round(pe.spread_tt, 4), "fees_pct": round(fees, 4),
                         "net_spread_pct": round(pe.spread_tt - fees, 4),
                         "price_short": qa.bid, "price_long": qb.ask,
                         "edge_tt_pct": round(pe.edge_tt, 4),
                         "edge_tm_pct": round(max(pe.edge_tm_a, pe.edge_tm_b), 4),
                         "mode": pe.mode, "is_candidate": pe.mode != ""})
        rows.sort(key=lambda r: r["spread_pct"], reverse=True)
        return rows[:limit]
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_strategy.py -q`
Expected: `7 passed`

- [ ] **Step 5: Run the whole suite**

Run: `.venv/bin/python -m pytest -q`
Expected: `74 passed`

- [ ] **Step 6: Commit**

```bash
git add deploy-bbo/bbo_trader/strategy.py deploy-bbo/tests/test_strategy.py
git commit -m "feat(bbo): PairEvaluator — multi-venue entry routing, resting-maker management, exits, scanner"
```

---

### Task 10: Venue protocols and bundle (`venues/base.py`)

**Files:**
- Create: `deploy-bbo/bbo_trader/venues/base.py`

No behaviour to test on its own (protocols only); it is exercised by Tasks 11–16. Write it exactly as below — later tasks import these names.

- [ ] **Step 1: Write `bbo_trader/venues/base.py`**

```python
"""Venue protocols — everything the strategy and executor may ask of a venue — and the Venue bundle.

Implementations: venues/mexc.py, venues/blofin.py (public side in Plan 1, private + trading in Plan 2),
venues/sim.py (paper trading over any real public feed)."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Iterable, Protocol

from ..budget import RateBudget
from ..config import VenueConfig
from ..models import BBO, Fees, OrderAck, OrderEvent, VenueSpec


@dataclass(frozen=True)
class VenuePosition:
    venue: str
    symbol: str
    side: str          # long | short
    qty: float         # contracts (absolute)
    position_id: str = ""


class PublicFeed(Protocol):
    """Streams BBO updates for a set of symbols into the on_bbo callback it was built with."""
    async def run(self) -> None: ...
    def set_symbols(self, symbols: Iterable[str]) -> None: ...
    def set_specs(self, specs: dict[str, VenueSpec]) -> None: ...
    @property
    def connected(self) -> bool: ...


class MarketData(Protocol):
    """Public REST: contract specs, 24 h USD volumes, funding, and a BBO fallback for open positions."""
    async def fetch_specs(self) -> dict[str, VenueSpec]: ...
    async def fetch_volumes(self) -> dict[str, float]: ...
    async def fetch_funding(self) -> dict[str, tuple[float, float]]: ...
    async def fetch_bbo(self, symbol: str) -> BBO | None: ...


class Trading(Protocol):
    supports_amend: bool
    async def place_market(self, symbol: str, side: str, qty: float, reduce_only: bool, client_id: str) -> OrderAck: ...
    async def place_post_only(self, symbol: str, side: str, qty: float, price: float, reduce_only: bool,
                              client_id: str) -> OrderAck: ...
    async def cancel(self, symbol: str, client_id: str, order_id: str) -> bool: ...
    async def amend(self, symbol: str, client_id: str, order_id: str, new_price: float) -> OrderAck: ...
    async def query_order(self, symbol: str, client_id: str, order_id: str) -> OrderEvent | None: ...
    async def open_orders(self) -> list[OrderEvent]: ...
    async def positions(self) -> list[VenuePosition]: ...
    async def balance(self) -> dict[str, float]: ...
    async def set_leverage(self, symbol: str, leverage: int) -> None: ...


class PrivateFeed(Protocol):
    """Delivers OrderEvents (ack/partial/filled/canceled/rejected) to the registered handler."""
    def set_handler(self, on_event: Callable[[OrderEvent], None]) -> None: ...
    async def run(self) -> None: ...


@dataclass
class Venue:
    cfg: VenueConfig
    fees: Fees
    budget: RateBudget
    public: PublicFeed | None = None
    market: MarketData | None = None
    trading: Trading | None = None
    private: PrivateFeed | None = None
    specs: dict[str, VenueSpec] = field(default_factory=dict)
    volumes: dict[str, float] = field(default_factory=dict)

    @property
    def name(self) -> str:
        return self.cfg.name

    @property
    def tradeable(self) -> bool:
        return self.cfg.role == "trade" and self.trading is not None
```

- [ ] **Step 2: Import check and commit**

Run: `.venv/bin/python -c "from bbo_trader.venues.base import Venue, VenuePosition, Trading, PrivateFeed, PublicFeed, MarketData; print('ok')"`
Expected: `ok`

```bash
git add deploy-bbo/bbo_trader/venues/base.py
git commit -m "feat(bbo): venue protocols (PublicFeed, MarketData, Trading, PrivateFeed) and Venue bundle"
```

---

### Task 11: Generic sharded WebSocket runner (`venues/ws.py`)

**Files:**
- Create: `deploy-bbo/bbo_trader/venues/ws.py`
- Test: `deploy-bbo/tests/test_ws.py`

The runner is a port of SpreadWatch's `WSFeed` (uptime-keyed `Backoff`, per-shard connections, app-level pings). The test spins up a local `aiohttp.web` WebSocket server — no network.

- [ ] **Step 1: Write the failing tests**

`tests/test_ws.py`:

```python
import asyncio
import json

from aiohttp import web, WSMsgType

from bbo_trader.venues.ws import Backoff, chunk, WSAdapter, WSRunner


def test_chunk_and_backoff():
    assert chunk([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4], [5]]
    assert chunk([1, 2], 0) == [[1, 2]]
    b = Backoff(healthy_s=20.0, cap_s=30.0, base_s=1.0)
    assert [b.next(0.0) for _ in range(6)] == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0]
    assert b.next(25.0) == 1.0            # a healthy socket resets the ladder
    assert b.next(0.0) == 1.0 and b.next(0.0) == 2.0


async def test_runner_shards_subscribes_parses_and_ignores_non_json():
    subs = []

    async def handler(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        first = await ws.receive()
        subs.append(json.loads(first.data))
        await ws.send_str("pong")                                  # non-JSON frame must be ignored
        await ws.send_json({"channel": "x", "v": len(subs), "symbol": "A"})
        async for m in ws:
            if m.type == WSMsgType.TEXT and m.data == "ping":
                await ws.send_str("pong")
        return ws

    app = web.Application()
    app.router.add_get("/ws", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    items = []
    adapter = WSAdapter(name="t", url=f"ws://127.0.0.1:{port}/ws",
                        subscribe=lambda insts: [{"op": "sub", "args": insts}],
                        parse=lambda raw, state: [raw["v"]] if raw.get("channel") == "x" else [],
                        max_topics=2, ping=(0.05, "ping"))
    r = WSRunner(adapter, on_items=items.extend)
    r.set_instruments(["C", "A", "B"])
    task = asyncio.create_task(r.run())
    for _ in range(100):
        if len(items) >= 2:
            break
        await asyncio.sleep(0.02)
    assert sorted(items) == [1, 2] and r.connections == 2 and r.connected
    assert {tuple(s["args"]) for s in subs} == {("A", "B"), ("C",)}
    await r.stop()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await runner.cleanup()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_ws.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'bbo_trader.venues.ws'`

- [ ] **Step 3: Implement `bbo_trader/venues/ws.py`**

```python
"""Generic sharded WebSocket runner (ported from SpreadWatch's WSFeed, incl. its hard-won lessons):
one connection task per shard of `max_topics` instruments, uptime-keyed reconnect backoff, optional
app-level keepalive, non-JSON frames ignored, server closes logged with the socket's lifetime."""
from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Callable, Iterable

import aiohttp

log = logging.getLogger("bbo.ws")


class Backoff:
    """Escalates only for sockets that die FAST; a socket that lived healthy_s resets to base."""

    def __init__(self, healthy_s: float = 20.0, cap_s: float = 30.0, base_s: float = 1.0):
        self.healthy_s, self.cap_s, self.base_s = healthy_s, cap_s, base_s
        self._d = base_s

    def next(self, uptime_s: float) -> float:
        if uptime_s >= self.healthy_s:
            self._d = self.base_s
            return self.base_s
        d = self._d
        self._d = min(self._d * 2.0, self.cap_s)
        return d


def chunk(items: list, max_topics: int) -> list[list]:
    if not max_topics:
        return [list(items)]
    return [list(items[i:i + max_topics]) for i in range(0, len(items), max_topics)]


@dataclass
class WSAdapter:
    name: str
    url: str
    subscribe: Callable[[list[str]], list]        # -> messages (dict → JSON, str → text frame)
    parse: Callable[[dict, dict], list]           # (raw json, per-connection scratch) -> parsed items
    max_topics: int = 0
    ping: tuple[float, object] | None = None      # (interval_s, message) app-level keepalive
    text_ping_reply: tuple[str, str] | None = None  # (server text frame, our reply)
    heartbeat: float | None = 20.0                # aiohttp protocol ping


async def _send(ws, msg) -> None:
    if isinstance(msg, str):
        await ws.send_str(msg)
    else:
        await ws.send_json(msg)


class WSRunner:
    def __init__(self, adapter: WSAdapter, on_items: Callable[[list], None],
                 session_factory=aiohttp.ClientSession, sleep=asyncio.sleep):
        self.adapter = adapter
        self.on_items = on_items
        self._session_factory = session_factory
        self._sleep = sleep
        self._insts: list[str] = []
        self._reconnect = False
        self._stop = False
        self._connected: set[int] = set()

    def set_instruments(self, insts: Iterable[str]) -> None:
        new = sorted(set(insts))
        if new != self._insts:
            self._insts = new
            self._reconnect = True

    @property
    def connected(self) -> bool:
        return bool(self._connected)

    @property
    def connections(self) -> int:
        return len(self._connected)

    async def stop(self) -> None:
        self._stop = True

    async def run(self) -> None:
        while not self._stop:
            self._reconnect = False
            shards = chunk(self._insts, self.adapter.max_topics) if self._insts else []
            tasks = [asyncio.create_task(self._run_conn(i, s)) for i, s in enumerate(shards)]
            try:
                while not self._reconnect and not self._stop:
                    await self._sleep(0.5)
                if self._reconnect:
                    log.info("%s instruments changed, resharding (%d)", self.adapter.name, len(self._insts))
            finally:
                for t in tasks:
                    t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                self._connected.clear()

    async def _run_conn(self, conn_id: int, insts: list[str]) -> None:
        backoff = Backoff()
        a = self.adapter
        while not self._stop:
            opened = None
            loop = asyncio.get_running_loop()
            try:
                async with self._session_factory() as session:
                    async with session.ws_connect(a.url, heartbeat=a.heartbeat) as ws:
                        for m in a.subscribe(insts):
                            await _send(ws, m)
                        pinger = None
                        if a.ping:
                            interval, msg = a.ping

                            async def _pinger():
                                while True:
                                    await asyncio.sleep(interval)
                                    await _send(ws, msg)
                            pinger = asyncio.create_task(_pinger())
                        self._connected.add(conn_id)
                        opened = loop.time()
                        state: dict = {}
                        try:
                            async for msg in ws:
                                if msg.type != aiohttp.WSMsgType.TEXT:
                                    if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                        break
                                    continue
                                data = msg.data
                                if a.text_ping_reply and data == a.text_ping_reply[0]:
                                    await ws.send_str(a.text_ping_reply[1])
                                    continue
                                try:
                                    raw = json.loads(data)
                                except ValueError:
                                    continue
                                if not isinstance(raw, dict):
                                    continue
                                items = a.parse(raw, state)
                                if items:
                                    self.on_items(items)
                        finally:
                            if pinger:
                                pinger.cancel()
                        log.info("%s conn %d closed by server (%d insts, lived %.0fs)",
                                 a.name, conn_id, len(insts), loop.time() - opened)
            except asyncio.CancelledError:
                self._connected.discard(conn_id)
                raise
            except Exception as e:  # noqa: BLE001 — any transport error → reconnect with backoff
                log.warning("%s conn %d error: %r", a.name, conn_id, e)
            self._connected.discard(conn_id)
            uptime = (loop.time() - opened) if opened is not None else 0.0
            await self._sleep(backoff.next(uptime))
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_ws.py -q`
Expected: `2 passed`

- [ ] **Step 5: Commit**

```bash
git add deploy-bbo/bbo_trader/venues/ws.py deploy-bbo/tests/test_ws.py
git commit -m "feat(bbo): generic sharded WebSocket runner with uptime-keyed backoff"
```

---

### Task 12: MEXC public adapter (`venues/mexc.py`)

**Files:**
- Create: `deploy-bbo/bbo_trader/venues/mexc.py`
- Test: `deploy-bbo/tests/test_mexc_public.py`

Message shapes follow the MEXC contract API docs and SpreadWatch's live captures (`push.depth.full` levels are `[price, contracts, order_count]`). **Verified against live frames on 2026-09-05** with exactly this parser: subscription ack `{"channel":"rs.sub.depth.full","data":"success"}` (ignored), pushes `{"symbol":"BTC_USDT","data":{"cts":...,"asks":[[79666.5,166847,6],...],"bids":[...],"version":...},"channel":"push.depth.full","ts":...}`; measured 1.2–3 BBO updates/s per symbol. Plan 2 repeats the capture for the private channel.

- [ ] **Step 1: Write the failing tests**

`tests/test_mexc_public.py`:

```python
import pytest

from bbo_trader.venues import mexc


def test_instrument_mapping_and_subscribe():
    assert mexc.to_instrument("BTCUSDT") == "BTC_USDT" and mexc.to_symbol("BTC_USDT") == "BTCUSDT"
    assert mexc.subscribe(["BTC_USDT"]) == [{"method": "sub.depth.full", "param": {"symbol": "BTC_USDT", "limit": 5}}]


def test_parse_depth_top_level_with_contract_size():
    raw = {"channel": "push.depth.full", "symbol": "XYZ_USDT", "ts": 1700000000123,
           "data": {"bids": [[1.0041, 500, 3], [1.0040, 900, 5]], "asks": [[1.0061, 200, 2], [1.0062, 10, 1]], "version": 7}}
    out = mexc.parse_depth(raw, {"XYZ_USDT": 10.0}, now=42.0)
    assert len(out) == 1
    b = out[0]
    assert (b.venue, b.symbol, b.bid, b.bid_qty, b.ask, b.ask_qty) == ("mexc", "XYZUSDT", 1.0041, 500.0, 1.0061, 200.0)
    assert b.ts_exchange == pytest.approx(1700000000.123) and b.ts_local == 42.0 and b.contract_size == 10.0
    assert mexc.parse_depth({"channel": "pong", "data": 1}, {}, 0.0) == []
    assert mexc.parse_depth({"channel": "push.depth.full", "symbol": "X_USDT", "data": {"bids": [], "asks": []}}, {}, 0.0) == []


def test_parse_specs_tickers_funding():
    specs = mexc.parse_specs({"success": True, "data": [
        {"symbol": "XYZ_USDT", "quoteCoin": "USDT", "state": 0, "contractSize": 10, "volUnit": 1, "minVol": 1, "priceUnit": 0.0001},
        {"symbol": "OLD_USDT", "quoteCoin": "USDT", "state": 1, "contractSize": 1, "volUnit": 1, "minVol": 1, "priceUnit": 0.01},
        {"symbol": "BTC_USDC", "quoteCoin": "USDC", "state": 0, "contractSize": 1, "volUnit": 1, "minVol": 1, "priceUnit": 0.1}]})
    assert list(specs) == ["XYZUSDT"]
    s = specs["XYZUSDT"]
    assert (s.instrument, s.contract_size, s.lot, s.min_qty, s.tick) == ("XYZ_USDT", 10.0, 1.0, 1.0, 0.0001)
    vols = mexc.parse_tickers({"data": [{"symbol": "XYZ_USDT", "amount24": "123456.5", "bid1": 1.0, "ask1": 1.1},
                                        {"symbol": "ABC_USDC", "amount24": "1"}]})
    assert vols == {"XYZUSDT": 123456.5}
    fund = mexc.parse_funding({"data": [{"symbol": "XYZ_USDT", "fundingRate": "0.0001", "nextSettleTime": 1700003600000}]})
    assert fund == {"XYZUSDT": (0.0001, 1700003600.0)}
    b = mexc.parse_depth_rest({"data": {"bids": [[1.0, 5]], "asks": [[1.1, 6]], "timestamp": 1700000000000}}, "XYZ_USDT", 10.0, 1.0)
    assert b.bid == 1.0 and b.ask_qty == 6.0 and b.contract_size == 10.0 and b.symbol == "XYZUSDT"


def test_public_feed_wiring(clock):
    from bbo_trader.config import VenueConfig
    got = []
    feed = mexc.MexcPublic(VenueConfig("mexc", "trade", 0.02, 0.0, max_topics=30), got.append, clock=clock)
    feed.set_specs(mexc.parse_specs({"data": [{"symbol": "XYZ_USDT", "quoteCoin": "USDT", "state": 0, "contractSize": 10,
                                               "volUnit": 1, "minVol": 1, "priceUnit": 0.0001}]}))
    feed.set_symbols(["XYZUSDT"])
    assert feed._runner._insts == ["XYZ_USDT"]
    feed._emit(feed._parse({"channel": "push.depth.full", "symbol": "XYZ_USDT", "ts": 1000,
                            "data": {"bids": [[1.0, 1]], "asks": [[1.1, 1]]}}, {}))
    assert got[0].contract_size == 10.0 and got[0].ts_local == clock()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_mexc_public.py -q`
Expected: FAIL with `ImportError` (no module `bbo_trader.venues.mexc`)

- [ ] **Step 3: Implement `bbo_trader/venues/mexc.py`**

```python
"""MEXC USDT-M contract venue — public side: BBO from `sub.depth.full limit=5` (top level only),
REST specs / volumes / funding / depth fallback. Plan 2 adds MexcPrivate and MexcTrading here."""
from __future__ import annotations

import time
from typing import Callable, Iterable

import aiohttp

from ..config import VenueConfig
from ..models import BBO, VenueSpec
from .ws import WSAdapter, WSRunner

NAME = "mexc"
WS_URL = "wss://contract.mexc.com/edge"
REST = "https://contract.mexc.com"
HEADERS = {"User-Agent": "Mozilla/5.0 (bbo-trader/1.0)"}   # Cloudflare rejects requests without a UA


def to_instrument(symbol: str) -> str:
    return f"{symbol[:-4]}_USDT"


def to_symbol(instrument: str) -> str:
    return instrument.replace("_", "")


def subscribe(insts: list[str]) -> list[dict]:
    return [{"method": "sub.depth.full", "param": {"symbol": i, "limit": 5}} for i in insts]


def parse_depth(raw: dict, contract_size: dict[str, float], now: float) -> list[BBO]:
    """`push.depth.full` → one BBO from the top levels. Levels are [price, contracts, order_count]."""
    if raw.get("channel") != "push.depth.full":
        return []
    d = raw.get("data") or {}
    bids, asks = d.get("bids") or [], d.get("asks") or []
    if not bids or not asks:
        return []
    inst = str(raw.get("symbol", ""))
    return [BBO(NAME, to_symbol(inst), float(bids[0][0]), float(bids[0][1]), float(asks[0][0]),
                float(asks[0][1]), float(raw.get("ts") or 0) / 1000.0, now, contract_size.get(inst, 1.0))]


def parse_depth_rest(raw: dict, inst: str, contract_size: float, now: float) -> BBO | None:
    d = raw.get("data") or {}
    bids, asks = d.get("bids") or [], d.get("asks") or []
    if not bids or not asks:
        return None
    ts = float(d.get("timestamp") or d.get("ts") or 0) / 1000.0
    return BBO(NAME, to_symbol(inst), float(bids[0][0]), float(bids[0][1]), float(asks[0][0]),
               float(asks[0][1]), ts, now, contract_size)


def parse_specs(raw: dict) -> dict[str, VenueSpec]:
    """`/api/v1/contract/detail` → specs for enabled (state 0) USDT contracts."""
    out: dict[str, VenueSpec] = {}
    for c in raw.get("data") or []:
        if c.get("quoteCoin") != "USDT" or int(c.get("state", 0) or 0) != 0:
            continue
        inst = c.get("symbol") or ""
        sym = to_symbol(inst)
        if not inst or not sym.endswith("USDT"):
            continue
        out[sym] = VenueSpec(NAME, sym, inst, float(c.get("contractSize") or 1), float(c.get("volUnit") or 1),
                             float(c.get("minVol") or 1), float(c.get("priceUnit") or 0.0001))
    return out


def parse_tickers(raw: dict) -> dict[str, float]:
    """`/api/v1/contract/ticker` → 24 h turnover in USDT per symbol (`amount24`)."""
    return {to_symbol(t["symbol"]): float(t.get("amount24") or 0)
            for t in raw.get("data") or [] if str(t.get("symbol", "")).endswith("_USDT")}


def parse_funding(raw: dict) -> dict[str, tuple[float, float]]:
    """`/api/v1/contract/funding_rate` → symbol -> (rate, next settlement ts seconds)."""
    out = {}
    for f in raw.get("data") or []:
        inst = str(f.get("symbol", ""))
        if not inst.endswith("_USDT"):
            continue
        out[to_symbol(inst)] = (float(f.get("fundingRate") or 0), float(f.get("nextSettleTime") or 0) / 1000.0)
    return out


class MexcPublic:
    def __init__(self, cfg: VenueConfig, on_bbo: Callable[[BBO], None], clock=time.time,
                 session_factory=aiohttp.ClientSession):
        self.cfg = cfg
        self.on_bbo = on_bbo
        self.clock = clock
        self.contract_size: dict[str, float] = {}
        self._runner = WSRunner(
            WSAdapter(name=NAME, url=WS_URL, subscribe=subscribe, parse=self._parse,
                      max_topics=cfg.max_topics, ping=(15.0, {"method": "ping"}), heartbeat=None),
            on_items=self._emit, session_factory=session_factory)

    def _parse(self, raw: dict, state: dict) -> list[BBO]:
        return parse_depth(raw, self.contract_size, self.clock())

    def _emit(self, items: list[BBO]) -> None:
        for b in items:
            self.on_bbo(b)

    def set_specs(self, specs: dict[str, VenueSpec]) -> None:
        self.contract_size = {s.instrument: s.contract_size for s in specs.values()}

    def set_symbols(self, symbols: Iterable[str]) -> None:
        self._runner.set_instruments(to_instrument(s) for s in symbols)

    @property
    def connected(self) -> bool:
        return self._runner.connected

    async def run(self) -> None:
        await self._runner.run()


class MexcMarket:
    def __init__(self, session: aiohttp.ClientSession, clock=time.time, specs: dict[str, VenueSpec] | None = None):
        self.session = session
        self.clock = clock
        self.specs = specs or {}

    async def _get(self, path: str, timeout: float = 10.0) -> dict:
        async with self.session.get(REST + path, headers=HEADERS,
                                    timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            return await r.json(content_type=None)

    async def fetch_specs(self) -> dict[str, VenueSpec]:
        self.specs = parse_specs(await self._get("/api/v1/contract/detail"))
        return self.specs

    async def fetch_volumes(self) -> dict[str, float]:
        return parse_tickers(await self._get("/api/v1/contract/ticker"))

    async def fetch_funding(self) -> dict[str, tuple[float, float]]:
        return parse_funding(await self._get("/api/v1/contract/funding_rate"))

    async def fetch_bbo(self, symbol: str) -> BBO | None:
        inst = to_instrument(symbol)
        spec = self.specs.get(symbol)
        raw = await self._get(f"/api/v1/contract/depth/{inst}?limit=5", timeout=5.0)
        return parse_depth_rest(raw, inst, spec.contract_size if spec else 1.0, self.clock())
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_mexc_public.py -q`
Expected: `4 passed`

- [ ] **Step 5: Commit**

```bash
git add deploy-bbo/bbo_trader/venues/mexc.py deploy-bbo/tests/test_mexc_public.py
git commit -m "feat(bbo): MEXC public adapter — depth.full BBO feed, specs, volumes, funding"
```

---

### Task 13: BloFin public adapter (`venues/blofin.py`)

**Files:**
- Create: `deploy-bbo/bbo_trader/venues/blofin.py`
- Test: `deploy-bbo/tests/test_blofin_public.py`

**Verified against live frames on 2026-09-05** with exactly this parser: subscribe ack `{"event":"subscribe","arg":{...}}` (ignored), pushes `{"arg":{"channel":"books5","instId":"BTC-USDT"},"action":"snapshot","data":{"asks":[["79666.5","1233"],...],"bids":[...],"ts":"1788599375999"}}` (`data` is a dict); measured ~9 pushes/s per symbol even when the book is unchanged — expect ~10 evaluations/s per BloFin symbol.

- [ ] **Step 1: Write the failing tests**

`tests/test_blofin_public.py`:

```python
import pytest

from bbo_trader.venues import blofin


def test_instrument_mapping_and_subscribe():
    assert blofin.to_instrument("BTCUSDT") == "BTC-USDT" and blofin.to_symbol("BTC-USDT") == "BTCUSDT"
    assert blofin.subscribe(["A-USDT", "B-USDT"]) == [{"op": "subscribe", "args": [
        {"channel": "books5", "instId": "A-USDT"}, {"channel": "books5", "instId": "B-USDT"}]}]


def test_parse_books5_dict_and_list_shapes():
    raw = {"arg": {"channel": "books5", "instId": "XYZ-USDT"},
           "data": {"bids": [["1.0041", "500"], ["1.0040", "900"]], "asks": [["1.0061", "200"]], "ts": "1700000000123"}}
    out = blofin.parse_books5(raw, {"XYZ-USDT": 0.1}, now=7.0)
    assert len(out) == 1
    b = out[0]
    assert (b.symbol, b.bid, b.bid_qty, b.ask, b.ask_qty, b.contract_size) == ("XYZUSDT", 1.0041, 500.0, 1.0061, 200.0, 0.1)
    assert b.ts_exchange == pytest.approx(1700000000.123) and b.ts_local == 7.0
    raw_list = dict(raw, data=[raw["data"]])
    assert len(blofin.parse_books5(raw_list, {}, 0.0)) == 1
    assert blofin.parse_books5({"event": "subscribe", "arg": {"channel": "books5"}}, {}, 0.0) == []
    assert blofin.parse_books5({"arg": {"channel": "trades", "instId": "X-USDT"}, "data": []}, {}, 0.0) == []


def test_parse_instruments_tickers_funding_books():
    specs = blofin.parse_instruments({"code": "0", "data": [
        {"instId": "XYZ-USDT", "contractValue": "0.1", "lotSize": "1", "minSize": "1", "tickSize": "0.0001", "state": "live"},
        {"instId": "DEAD-USDT", "contractValue": "1", "lotSize": "1", "minSize": "1", "tickSize": "0.01", "state": "suspend"},
        {"instId": "BTC-USDC", "contractValue": "1", "lotSize": "1", "minSize": "1", "tickSize": "0.1", "state": "live"}]})
    assert list(specs) == ["XYZUSDT"]
    s = specs["XYZUSDT"]
    assert (s.instrument, s.contract_size, s.lot, s.min_qty, s.tick) == ("XYZ-USDT", 0.1, 1.0, 1.0, 0.0001)
    vols = blofin.parse_tickers({"data": [{"instId": "XYZ-USDT", "last": "2.0", "volCurrency24h": "1000", "bidPrice": "1.9", "askPrice": "2.1"}]})
    assert vols == {"XYZUSDT": 2000.0}
    fund = blofin.parse_funding({"data": [{"instId": "XYZ-USDT", "fundingRate": "-0.0002", "fundingTime": "1700003600000"}]})
    assert fund == {"XYZUSDT": (-0.0002, 1700003600.0)}
    b = blofin.parse_books_rest({"data": [{"bids": [["1.0", "5"]], "asks": [["1.1", "6"]], "ts": "1700000000000"}]}, "XYZ-USDT", 0.1, 1.0)
    assert b.bid == 1.0 and b.ask_qty == 6.0 and b.contract_size == 0.1


def test_public_feed_wiring(clock):
    from bbo_trader.config import VenueConfig
    got = []
    feed = blofin.BlofinPublic(VenueConfig("blofin", "trade", 0.06, 0.02, max_topics=50), got.append, clock=clock)
    feed.set_specs(blofin.parse_instruments({"data": [{"instId": "XYZ-USDT", "contractValue": "0.1", "lotSize": "1",
                                                        "minSize": "1", "tickSize": "0.0001", "state": "live"}]}))
    feed.set_symbols(["XYZUSDT"])
    assert feed._runner._insts == ["XYZ-USDT"]
    feed._emit(feed._parse({"arg": {"channel": "books5", "instId": "XYZ-USDT"},
                            "data": {"bids": [["1", "1"]], "asks": [["1.1", "1"]], "ts": "1000"}}, {}))
    assert got[0].contract_size == 0.1 and got[0].ts_local == clock()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_blofin_public.py -q`
Expected: FAIL with `ImportError` (no module `bbo_trader.venues.blofin`)

- [ ] **Step 3: Implement `bbo_trader/venues/blofin.py`**

```python
"""BloFin USDT swaps venue — public side: BBO from `books5` snapshots (top level only), REST
instruments / tickers / funding / books fallback. Plan 2 adds BlofinPrivate and BlofinTrading here.
Shapes as captured live by SpreadWatch (2026-08-23): books5 `data` is a DICT, keepalive is a bare
text `ping` answered with `pong`."""
from __future__ import annotations

import time
from typing import Callable, Iterable

import aiohttp

from ..config import VenueConfig
from ..models import BBO, VenueSpec
from .ws import WSAdapter, WSRunner

NAME = "blofin"
WS_URL = "wss://openapi.blofin.com/ws/public"
REST = "https://openapi.blofin.com"


def to_instrument(symbol: str) -> str:
    return f"{symbol[:-4]}-USDT"


def to_symbol(instrument: str) -> str:
    return instrument.replace("-", "")


def subscribe(insts: list[str]) -> list[dict]:
    return [{"op": "subscribe", "args": [{"channel": "books5", "instId": i} for i in insts]}]


def _rows(data) -> list[dict]:
    if isinstance(data, dict):
        return [data]
    return [d for d in (data or []) if isinstance(d, dict)]


def parse_books5(raw: dict, contract_size: dict[str, float], now: float) -> list[BBO]:
    if (raw.get("arg") or {}).get("channel") != "books5" or "data" not in raw:
        return []
    inst = str(raw["arg"].get("instId", ""))
    out = []
    for d in _rows(raw["data"]):
        bids, asks = d.get("bids") or [], d.get("asks") or []
        if not bids or not asks:
            continue
        out.append(BBO(NAME, to_symbol(inst), float(bids[0][0]), float(bids[0][1]), float(asks[0][0]),
                       float(asks[0][1]), float(d.get("ts") or 0) / 1000.0, now, contract_size.get(inst, 1.0)))
    return out


def parse_books_rest(raw: dict, inst: str, contract_size: float, now: float) -> BBO | None:
    rows = _rows(raw.get("data"))
    if not rows:
        return None
    d = rows[0]
    bids, asks = d.get("bids") or [], d.get("asks") or []
    if not bids or not asks:
        return None
    return BBO(NAME, to_symbol(inst), float(bids[0][0]), float(bids[0][1]), float(asks[0][0]),
               float(asks[0][1]), float(d.get("ts") or 0) / 1000.0, now, contract_size)


def parse_instruments(raw: dict) -> dict[str, VenueSpec]:
    out: dict[str, VenueSpec] = {}
    for c in raw.get("data") or []:
        inst = str(c.get("instId", ""))
        if not inst.endswith("-USDT") or str(c.get("state", "live")) != "live":
            continue
        sym = to_symbol(inst)
        out[sym] = VenueSpec(NAME, sym, inst, float(c.get("contractValue") or 1), float(c.get("lotSize") or 1),
                             float(c.get("minSize") or 1), float(c.get("tickSize") or 0.0001))
    return out


def parse_tickers(raw: dict) -> dict[str, float]:
    """`/api/v1/market/tickers` → 24 h USD volume ≈ volCurrency24h × last."""
    out = {}
    for t in raw.get("data") or []:
        inst = str(t.get("instId", ""))
        if not inst.endswith("-USDT"):
            continue
        out[to_symbol(inst)] = float(t.get("volCurrency24h") or 0) * float(t.get("last") or 0)
    return out


def parse_funding(raw: dict) -> dict[str, tuple[float, float]]:
    out = {}
    for f in raw.get("data") or []:
        inst = str(f.get("instId", ""))
        if not inst.endswith("-USDT"):
            continue
        out[to_symbol(inst)] = (float(f.get("fundingRate") or 0), float(f.get("fundingTime") or 0) / 1000.0)
    return out


class BlofinPublic:
    def __init__(self, cfg: VenueConfig, on_bbo: Callable[[BBO], None], clock=time.time,
                 session_factory=aiohttp.ClientSession):
        self.cfg = cfg
        self.on_bbo = on_bbo
        self.clock = clock
        self.contract_size: dict[str, float] = {}
        self._runner = WSRunner(
            WSAdapter(name=NAME, url=WS_URL, subscribe=subscribe, parse=self._parse,
                      max_topics=cfg.max_topics, ping=(25.0, "ping"), heartbeat=None),
            on_items=self._emit, session_factory=session_factory)

    def _parse(self, raw: dict, state: dict) -> list[BBO]:
        return parse_books5(raw, self.contract_size, self.clock())

    def _emit(self, items: list[BBO]) -> None:
        for b in items:
            self.on_bbo(b)

    def set_specs(self, specs: dict[str, VenueSpec]) -> None:
        self.contract_size = {s.instrument: s.contract_size for s in specs.values()}

    def set_symbols(self, symbols: Iterable[str]) -> None:
        self._runner.set_instruments(to_instrument(s) for s in symbols)

    @property
    def connected(self) -> bool:
        return self._runner.connected

    async def run(self) -> None:
        await self._runner.run()


class BlofinMarket:
    def __init__(self, session: aiohttp.ClientSession, clock=time.time, specs: dict[str, VenueSpec] | None = None):
        self.session = session
        self.clock = clock
        self.specs = specs or {}

    async def _get(self, path: str, timeout: float = 10.0) -> dict:
        async with self.session.get(REST + path, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            return await r.json(content_type=None)

    async def fetch_specs(self) -> dict[str, VenueSpec]:
        self.specs = parse_instruments(await self._get("/api/v1/market/instruments"))
        return self.specs

    async def fetch_volumes(self) -> dict[str, float]:
        return parse_tickers(await self._get("/api/v1/market/tickers"))

    async def fetch_funding(self) -> dict[str, tuple[float, float]]:
        return parse_funding(await self._get("/api/v1/market/funding-rate"))

    async def fetch_bbo(self, symbol: str) -> BBO | None:
        inst = to_instrument(symbol)
        spec = self.specs.get(symbol)
        raw = await self._get(f"/api/v1/market/books?instId={inst}&size=5", timeout=5.0)
        return parse_books_rest(raw, inst, spec.contract_size if spec else 1.0, self.clock())
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_blofin_public.py -q`
Expected: `4 passed`

- [ ] **Step 5: Commit**

```bash
git add deploy-bbo/bbo_trader/venues/blofin.py deploy-bbo/tests/test_blofin_public.py
git commit -m "feat(bbo): BloFin public adapter — books5 BBO feed, instruments, tickers, funding"
```

---

### Task 14: Paper venue (`venues/sim.py`)

**Files:**
- Create: `deploy-bbo/bbo_trader/venues/sim.py`
- Test: `deploy-bbo/tests/test_sim.py`

- [ ] **Step 1: Write the failing tests**

`tests/test_sim.py`:

```python
import asyncio

import pytest

from bbo_trader.quotes import QuoteBoard
from bbo_trader.venues.sim import SimVenue
from tests.conftest import make_cfg, mk_bbo, mk_spec, BLOFIN_FEES

SYM = "XYZUSDT"


def build(tmp_path, clock):
    cfg = make_cfg(tmp_path, sim_latency_ms=1, sim_taker_slip_bps=2.0, maker_top_level_frac=0.5)
    board = QuoteBoard(cfg.stale_quote_s)
    sim = SimVenue("blofin", cfg, BLOFIN_FEES, board, {SYM: mk_spec("blofin", SYM, contract_size=10.0)}, clock=clock)
    events = []
    sim.set_handler(events.append)
    return cfg, board, sim, events


async def test_market_order_fills_at_touch_with_slip_and_taker_fee(tmp_path, clock):
    cfg, board, sim, events = build(tmp_path, clock)
    board.set(mk_bbo("blofin", SYM, 1.0000, 1.0010, ts=clock()))
    ack = await sim.place_market(SYM, "buy", 2.0, False, "c1")
    assert ack.ok and ack.order_id == "sim-1"
    await asyncio.sleep(0.02)
    ev = events[-1]
    assert ev.state == "filled" and ev.client_id == "c1" and ev.liquidity == "taker"
    assert ev.avg_price == pytest.approx(1.0010 * 1.0002) and ev.filled_qty == 2.0
    assert ev.fee == pytest.approx(2.0 * ev.avg_price * 10.0 * 0.06 / 100)
    assert (await sim.positions())[0].side == "long" and (await sim.positions())[0].qty == 2.0
    assert (await sim.balance())["available"] == pytest.approx(100.0 - ev.fee)
    assert (await sim.place_market("NOPEUSDT", "buy", 1.0, False, "c2")).ok is False   # no quote


async def test_post_only_rests_then_fills_when_touch_crosses(tmp_path, clock):
    cfg, board, sim, events = build(tmp_path, clock)
    board.set(mk_bbo("blofin", SYM, 1.0041, 1.0061, ts=clock()))
    ack = await sim.place_post_only(SYM, "sell", 4.0, 1.0061, False, "m1")
    assert ack.ok and events[-1].state == "ack"           # ack event pushed before the return value
    sim.on_quote(mk_bbo("blofin", SYM, 1.0061, 1.0062, bq=4.0, ts=clock()))   # not live yet (latency)
    assert events[-1].state == "ack"
    clock.tick(0.01)
    sim.on_quote(mk_bbo("blofin", SYM, 1.0061, 1.0062, bq=4.0, ts=clock()))   # crosses: 50% of 4 -> 2
    assert events[-1].state == "partial" and events[-1].filled_qty == 2.0 and events[-1].liquidity == "maker"
    assert events[-1].fee == pytest.approx(2.0 * 1.0061 * 10.0 * 0.02 / 100)
    sim.on_quote(mk_bbo("blofin", SYM, 1.0040, 1.0060, ts=clock()))            # no cross -> nothing
    assert events[-1].state == "partial"
    sim.on_quote(mk_bbo("blofin", SYM, 1.0065, 1.0066, bq=1000.0, ts=clock()))
    assert events[-1].state == "filled" and events[-1].filled_qty == 4.0 and events[-1].avg_price == pytest.approx(1.0061)
    assert await sim.open_orders() == []
    assert not await sim.cancel(SYM, "m1", ack.order_id)                       # already terminal
    assert (await sim.positions())[0].side == "short"


async def test_post_only_would_cross_is_rejected(tmp_path, clock):
    cfg, board, sim, events = build(tmp_path, clock)
    board.set(mk_bbo("blofin", SYM, 1.0041, 1.0061, ts=clock()))
    ack = await sim.place_post_only(SYM, "sell", 1.0, 1.0041, False, "m1")
    assert not ack.ok and "cross" in ack.error and events[-1].state == "rejected"
    ack2 = await sim.place_post_only(SYM, "buy", 1.0, 1.0061, False, "m2")
    assert not ack2.ok and events[-1].state == "rejected" and events[-1].client_id == "m2"


async def test_cancel_amend_query(tmp_path, clock):
    cfg, board, sim, events = build(tmp_path, clock)
    board.set(mk_bbo("blofin", SYM, 1.0041, 1.0061, ts=clock()))
    ack = await sim.place_post_only(SYM, "buy", 3.0, 1.0040, False, "m1")
    assert ack.ok and len(await sim.open_orders()) == 1
    assert (await sim.amend(SYM, "m1", ack.order_id, 1.0041)).ok
    assert not (await sim.amend(SYM, "m1", ack.order_id, 1.0061)).ok             # would cross
    q = await sim.query_order(SYM, "m1", ack.order_id)
    assert q.state == "ack" and q.filled_qty == 0.0
    assert await sim.cancel(SYM, "m1", ack.order_id)
    assert events[-1].state == "canceled" and events[-1].filled_qty == 0.0
    assert await sim.open_orders() == [] and await sim.query_order(SYM, "zzz", "") is None
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_sim.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'bbo_trader.venues.sim'`

- [ ] **Step 3: Implement `bbo_trader/venues/sim.py`**

```python
"""SimVenue: paper trading over any real public feed. Implements the Trading + PrivateFeed protocols.

Fill model (conservative — the SpreadWatch `post_hedge` rule):
- market orders fill fully at the current touch ± SIM_TAKER_SLIP_BPS after SIM_LATENCY_MS (liquidity "taker");
- post-only orders that would cross the book are rejected like a real venue; resting orders become live
  after the latency and fill only when the OPPOSITE touch crosses the resting price on a quote newer
  than the post, for at most MAKER_TOP_LEVEL_FRAC × that touch's quantity per quote (liquidity "maker");
- cancel/amend are immediate; positions are signed contracts per symbol (one-way accounting).
Order events are pushed synchronously to the registered handler, so an "ack" event can reach the
executor before the REST-style OrderAck returns — exactly what live private WebSockets do."""
from __future__ import annotations

import asyncio
import itertools
import math
import time
from dataclasses import dataclass
from typing import Callable

from ..config import Config
from ..models import BBO, Fees, OrderAck, OrderEvent, VenueSpec
from ..quotes import QuoteBoard
from .base import VenuePosition

TERMINAL = ("filled", "canceled", "rejected")


@dataclass
class SimOrder:
    client_id: str
    order_id: str
    symbol: str
    side: str
    qty: float
    price: float          # 0.0 for market orders
    post_only: bool
    reduce_only: bool
    live_ts: float        # when the order is considered to have reached the venue
    state: str = "ack"
    filled: float = 0.0
    avg_price: float = 0.0
    fee: float = 0.0
    liquidity: str = ""

    @property
    def remaining(self) -> float:
        return round(self.qty - self.filled, 10)


class SimVenue:
    supports_amend = True

    def __init__(self, name: str, cfg: Config, fees: Fees, board: QuoteBoard,
                 specs: dict[str, VenueSpec], clock=time.time):
        self.name = name
        self.cfg = cfg
        self.fees = fees
        self.board = board
        self.specs = specs
        self.clock = clock
        self.latency_s = cfg.sim_latency_ms / 1000.0
        self.slip = cfg.sim_taker_slip_bps / 10_000.0
        self.frac = cfg.maker_top_level_frac
        self._orders: dict[str, SimOrder] = {}
        self._pos: dict[str, float] = {}
        self._cash = cfg.paper_capital_per_venue
        self._handler: Callable[[OrderEvent], None] | None = None
        self._seq = itertools.count(1)
        self._tasks: set[asyncio.Task] = set()

    # ---- PrivateFeed ----------------------------------------------------------
    def set_handler(self, on_event: Callable[[OrderEvent], None]) -> None:
        self._handler = on_event

    async def run(self) -> None:
        await asyncio.Event().wait()   # events are pushed synchronously from fills

    # ---- internals ------------------------------------------------------------
    def _spec(self, symbol: str) -> VenueSpec:
        return self.specs.get(symbol) or VenueSpec(self.name, symbol, symbol, 1.0, 1.0, 1.0, 0.0001)

    def _event(self, o: SimOrder) -> OrderEvent:
        return OrderEvent(self.name, o.client_id, o.order_id, o.state, o.filled, o.avg_price, o.fee,
                          o.liquidity, "", self.clock())

    def _emit(self, o: SimOrder, state: str | None = None) -> None:
        if state:
            o.state = state
        if self._handler:
            self._handler(self._event(o))

    def _fill(self, o: SimOrder, qty: float, price: float, liquidity: str) -> None:
        cs = self._spec(o.symbol).contract_size
        notional = qty * price * cs
        o.avg_price = (o.avg_price * o.filled + price * qty) / (o.filled + qty)
        o.filled = round(o.filled + qty, 10)
        rate = self.fees.taker if liquidity == "taker" else self.fees.maker
        fee = notional * rate / 100.0
        o.fee += fee
        o.liquidity = liquidity
        self._cash -= fee
        signed = qty if o.side == "buy" else -qty
        self._pos[o.symbol] = round(self._pos.get(o.symbol, 0.0) + signed, 10)
        self._emit(o, "filled" if o.remaining <= 1e-12 else "partial")

    def _spawn(self, coro) -> None:
        t = asyncio.get_running_loop().create_task(coro)
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)

    async def _fill_market_later(self, o: SimOrder, price: float) -> None:
        await asyncio.sleep(self.latency_s)
        if o.state in TERMINAL:
            return
        self._fill(o, o.qty, price, "taker")

    # ---- Trading --------------------------------------------------------------
    async def place_market(self, symbol: str, side: str, qty: float, reduce_only: bool, client_id: str) -> OrderAck:
        q = self.board.get(self.name, symbol)
        if q is None or not q.ok:
            return OrderAck(False, error="no quote")
        if qty <= 0:
            return OrderAck(False, error="qty must be > 0")
        o = SimOrder(client_id, f"sim-{next(self._seq)}", symbol, side, qty, 0.0, False, reduce_only,
                     self.clock() + self.latency_s)
        self._orders[client_id] = o
        price = q.ask * (1.0 + self.slip) if side == "buy" else q.bid * (1.0 - self.slip)
        self._spawn(self._fill_market_later(o, price))
        return OrderAck(True, o.order_id, latency_ms=self.latency_s * 1000.0)

    async def place_post_only(self, symbol: str, side: str, qty: float, price: float, reduce_only: bool,
                              client_id: str) -> OrderAck:
        q = self.board.get(self.name, symbol)
        if q is None or not q.ok:
            return OrderAck(False, error="no quote")
        o = SimOrder(client_id, f"sim-{next(self._seq)}", symbol, side, qty, price, True, reduce_only,
                     self.clock() + self.latency_s)
        self._orders[client_id] = o
        if (side == "sell" and price <= q.bid) or (side == "buy" and price >= q.ask):
            self._emit(o, "rejected")
            return OrderAck(False, o.order_id, error="post-only would cross")
        self._emit(o, "ack")
        return OrderAck(True, o.order_id, latency_ms=self.latency_s * 1000.0)

    def on_quote(self, bbo: BBO) -> None:
        """Called by the app for every stored quote of this venue: fill resting orders the touch crossed."""
        if bbo.venue != self.name:
            return
        for o in list(self._orders.values()):
            if o.symbol != bbo.symbol or not o.post_only or o.state not in ("ack", "partial"):
                continue
            if bbo.ts_local < o.live_ts:
                continue
            if o.side == "sell" and bbo.bid >= o.price:
                touch_qty = bbo.bid_qty
            elif o.side == "buy" and bbo.ask <= o.price:
                touch_qty = bbo.ask_qty
            else:
                continue
            lot = self._spec(o.symbol).lot
            cap = max(lot, math.floor(self.frac * touch_qty / lot) * lot)
            qty = min(o.remaining, cap)
            if qty > 0:
                self._fill(o, qty, o.price, "maker")

    async def cancel(self, symbol: str, client_id: str, order_id: str) -> bool:
        o = self._orders.get(client_id)
        if o is None or o.state in TERMINAL:
            return False
        self._emit(o, "canceled")
        return True

    async def amend(self, symbol: str, client_id: str, order_id: str, new_price: float) -> OrderAck:
        o = self._orders.get(client_id)
        if o is None or not o.post_only or o.state not in ("ack", "partial"):
            return OrderAck(False, error="not resting")
        q = self.board.get(self.name, symbol)
        if q and ((o.side == "sell" and new_price <= q.bid) or (o.side == "buy" and new_price >= q.ask)):
            return OrderAck(False, o.order_id, error="post-only would cross")
        o.price = new_price
        o.live_ts = self.clock() + self.latency_s
        return OrderAck(True, o.order_id)

    async def query_order(self, symbol: str, client_id: str, order_id: str) -> OrderEvent | None:
        o = self._orders.get(client_id)
        return self._event(o) if o else None

    async def open_orders(self) -> list[OrderEvent]:
        return [self._event(o) for o in self._orders.values() if o.post_only and o.state in ("ack", "partial")]

    async def positions(self) -> list[VenuePosition]:
        return [VenuePosition(self.name, s, "long" if q > 0 else "short", abs(q))
                for s, q in self._pos.items() if abs(q) > 1e-12]

    async def balance(self) -> dict[str, float]:
        return {"available": self._cash, "total": self._cash}

    async def set_leverage(self, symbol: str, leverage: int) -> None:
        return None
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_sim.py -q`
Expected: `4 passed`

- [ ] **Step 5: Run the whole suite and commit**

Run: `.venv/bin/python -m pytest -q`
Expected: `88 passed`

```bash
git add deploy-bbo/bbo_trader/venues/sim.py deploy-bbo/tests/test_sim.py
git commit -m "feat(bbo): SimVenue — paper trading with conservative maker fill model"
```

---

### Task 15: Metrics (`metrics.py`)

**Files:**
- Create: `deploy-bbo/bbo_trader/metrics.py`
- Test: `deploy-bbo/tests/test_metrics.py`

- [ ] **Step 1: Write the failing tests**

`tests/test_metrics.py`:

```python
from bbo_trader.metrics import LatencyHist, Metrics, CoverageWatchdog
from bbo_trader.quotes import QuoteBoard
from tests.conftest import mk_bbo


def test_latency_hist_percentiles():
    h = LatencyHist()
    for v in [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]:
        h.add(v)
    d = h.to_dict()
    assert d["n"] == 10 and d["p50"] == 60.0 and d["p95"] == 100.0 and d["max"] == 100.0
    assert LatencyHist().to_dict() == {"n": 0, "p50": 0.0, "p95": 0.0, "max": 0.0}


def test_metrics_record_and_funnel():
    m = Metrics()
    m.record("submit_to_ack", 120.0)
    m.funnel["below_edge"] += 2
    d = m.to_dict()
    assert d["latency_ms"]["submit_to_ack"]["n"] == 1 and d["funnel"] == {"below_edge": 2}


def test_coverage_watchdog_counts(clock):
    board = QuoteBoard(2.0)
    board.set(mk_bbo("mexc", "AUSDT", 1, 1.1, ts=clock()))
    board.set(mk_bbo("mexc", "BUSDT", 1, 1.1, ts=clock()))
    board.set(mk_bbo("blofin", "AUSDT", 1, 1.1, ts=clock() - 5))    # stale
    w = CoverageWatchdog(board, ["mexc", "blofin"], floor=1, clock=clock)
    assert w.check() == {"mexc": 2, "blofin": 0}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_metrics.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'bbo_trader.metrics'`

- [ ] **Step 3: Implement `bbo_trader/metrics.py`**

```python
"""Latency histograms, the rejection funnel, event-loop lag sampling and the feed coverage watchdog."""
from __future__ import annotations

import asyncio
import logging
import time
from collections import Counter, deque

log = logging.getLogger("bbo.metrics")


class LatencyHist:
    def __init__(self, maxlen: int = 1000):
        self._v: deque[float] = deque(maxlen=maxlen)

    def add(self, ms: float) -> None:
        self._v.append(float(ms))

    def _pct(self, q: float) -> float:
        if not self._v:
            return 0.0
        s = sorted(self._v)
        return s[min(len(s) - 1, int(q * len(s)))]

    @property
    def count(self) -> int:
        return len(self._v)

    def to_dict(self) -> dict:
        return {"n": self.count, "p50": round(self._pct(0.50), 1), "p95": round(self._pct(0.95), 1),
                "max": round(max(self._v), 1) if self._v else 0.0}


class Metrics:
    def __init__(self):
        self.hists: dict[str, LatencyHist] = {}
        self.funnel: Counter = Counter()
        self.loop_lag = LatencyHist(600)
        self.started = time.time()

    def record(self, stage: str, ms: float) -> None:
        self.hists.setdefault(stage, LatencyHist()).add(ms)

    async def sample_loop_lag(self, interval_s: float = 1.0) -> None:
        loop = asyncio.get_running_loop()
        while True:
            t0 = loop.time()
            await asyncio.sleep(interval_s)
            lag_ms = (loop.time() - t0 - interval_s) * 1000.0
            self.loop_lag.add(max(0.0, lag_ms))
            if lag_ms > 50.0:
                log.warning("LOOP_LAG %.0f ms", lag_ms)

    def to_dict(self) -> dict:
        return {"latency_ms": {k: h.to_dict() for k, h in self.hists.items()},
                "funnel": dict(self.funnel), "loop_lag_ms": self.loop_lag.to_dict(),
                "uptime_s": round(time.time() - self.started)}


class CoverageWatchdog:
    """Logs fresh-quote counts per venue every interval and warns when a venue is under its floor."""

    def __init__(self, board, venues: list[str], floor: int = 10, interval_s: float = 60.0, clock=time.time):
        self.board, self.venues, self.floor, self.interval_s, self.clock = board, venues, floor, interval_s, clock
        self.last: dict[str, int] = {}

    def check(self) -> dict[str, int]:
        counts = self.board.fresh_counts(self.clock())
        self.last = {v: counts.get(v, 0) for v in self.venues}
        log.info("FEED_COVERAGE %s", " ".join(f"{v}={n}" for v, n in self.last.items()))
        for v, n in self.last.items():
            if n < self.floor:
                log.warning("FEED_COVERAGE_LOW %s fresh=%d floor=%d", v, n, self.floor)
        return self.last

    async def run(self) -> None:
        while True:
            await asyncio.sleep(self.interval_s)
            self.check()
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_metrics.py -q`
Expected: `3 passed`

- [ ] **Step 5: Commit**

```bash
git add deploy-bbo/bbo_trader/metrics.py deploy-bbo/tests/test_metrics.py
git commit -m "feat(bbo): latency histograms, funnel, loop-lag sampler, coverage watchdog"
```

---

### Task 16: Executor — TT flow and PeggedMaker flow (`execution.py`)

**Files:**
- Create: `deploy-bbo/bbo_trader/execution.py`
- Test: `deploy-bbo/tests/test_execution_tt.py`, `deploy-bbo/tests/test_execution_tm.py`

This is the heart of the trader. Read the spec sections "Execution" and "Position state machine" first. The tests drive the executor with two `SimVenue`s (blofin contract size 1, mexc contract size 10 → $25 sizes to $20 matched), 1 ms simulated latency and no-op retry sleeps. `Harness` in the TT test file is reused by the TM tests and by the App test in Task 19.

- [ ] **Step 1: Write the failing TT-flow tests**

`tests/test_execution_tt.py`:

```python
import asyncio

import pytest

from bbo_trader.budget import RateBudget
from bbo_trader.execution import Executor
from bbo_trader.metrics import Metrics
from bbo_trader.models import Intent, OPEN, CLOSED, MAKER_RESTING
from bbo_trader.positions import PositionBook
from bbo_trader.quotes import QuoteBoard
from bbo_trader.risk import RiskManager
from bbo_trader.venues.base import Venue
from bbo_trader.venues.sim import SimVenue
from tests.conftest import make_cfg, mk_bbo, mk_spec, MEXC_FEES, BLOFIN_FEES

SYM = "XYZUSDT"


class Harness:
    """Two SimVenues (blofin, mexc) wired to an Executor; real time, 1 ms sim latency, no-op sleeps."""

    def __init__(self, tmp_path, **over):
        self.cfg = make_cfg(tmp_path, sim_latency_ms=1, sim_taker_slip_bps=0.0, maker_top_level_frac=0.5, **over)
        self.board = QuoteBoard(self.cfg.stale_quote_s)
        self.book = PositionBook()
        self.risk = RiskManager(self.cfg)
        self.metrics = Metrics()
        self.venues = {}
        for name, fees, cs in (("blofin", BLOFIN_FEES, 1.0), ("mexc", MEXC_FEES, 10.0)):
            vc = self.cfg.venue(name)
            specs = {SYM: mk_spec(name, SYM, contract_size=cs)}
            sim = SimVenue(name, self.cfg, fees, self.board, specs)
            self.venues[name] = Venue(vc, fees, RateBudget(vc.rate_limits), trading=sim, private=sim, specs=specs)
        self.notes = []

        async def notify(text):
            self.notes.append(text)

        async def no_sleep(_s):
            await asyncio.sleep(0)

        self.ex = Executor(self.cfg, self.venues, self.board, self.book, self.risk, self.metrics,
                           notify=notify, sleep=no_sleep)
        for v in self.venues.values():
            v.private.set_handler(self.ex.on_order_event)

    def quote(self, venue, bid, ask, bq=1000.0, aq=1000.0):
        b = mk_bbo(venue, SYM, bid, ask, bq=bq, aq=aq, contract_size=self.venues[venue].specs[SYM].contract_size)
        self.board.set(b)
        self.venues[venue].trading.on_quote(b)
        return b

    def sim(self, venue) -> SimVenue:
        return self.venues[venue].trading


async def test_enter_tt_opens_matched_position(tmp_path):
    h = Harness(tmp_path)
    h.quote("blofin", 1.0050, 1.0060)
    h.quote("mexc", 1.0000, 1.0008)
    intent = Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0, spread_pct=0.42, edge_pct=0.06, ts=0.0)
    pos = await h.ex.enter_tt(intent)
    assert pos is not None and pos.status == OPEN and pos.mode == "TT"
    assert pos.filled_a == 20.0 and pos.filled_b == 2.0            # $20 matched: mexc contracts are $10 each
    assert pos.entry_price_a == pytest.approx(1.0050) and pos.entry_price_b == pytest.approx(1.0008)
    assert pos.size_usd == pytest.approx(min(20 * 1.0050, 2 * 1.0008 * 10))
    assert pos.entry_spread_pct == pytest.approx((1.0050 - 1.0008) / 1.0008 * 100)
    assert pos.entry_fees_usd == pytest.approx(20 * 1.0050 * 0.06 / 100 + 2 * 1.0008 * 10 * 0.02 / 100)
    assert pos.fee_liquidity == {"entry_a": "taker", "entry_b": "taker"}
    assert "entry_a" in pos.latency_ms and h.metrics.hists["submit_to_fill"].count == 2
    assert (await h.sim("blofin").positions())[0].side == "short" and (await h.sim("mexc").positions())[0].side == "long"
    await asyncio.sleep(0.01)                                    # notification task runs
    assert h.notes and h.notes[0].startswith("OPEN #1")


async def test_enter_tt_one_leg_fails_flattens_and_records(tmp_path):
    h = Harness(tmp_path)
    h.quote("blofin", 1.0050, 1.0060)
    h.board.set(mk_bbo("mexc", SYM, 1.0000, 1.0008, contract_size=10.0))   # board has mexc for sizing...
    h.sim("mexc").board = QuoteBoard(2.0)                                  # ...but the venue itself has no quote → reject
    intent = Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0, spread_pct=0.42, ts=0.0)
    assert await h.ex.enter_tt(intent) is None
    assert h.book.open == [] and len(h.book.closed) == 1
    closed = h.book.closed[0]
    assert closed.status == CLOSED and closed.exit_reason == "failed_entry"
    assert closed.exit_price_a > 0 and closed.exit_fees_usd > 0 and closed.net_pnl_usd < 0   # round-trip fees
    assert await h.sim("blofin").positions() == []                       # flat again
    assert h.risk.pair_strikes["XYZUSDT|blofin>mexc"]["n"] == 1 and "XYZUSDT" in h.risk.cooldowns


async def test_exit_tt_closes_and_books_pnl(tmp_path):
    h = Harness(tmp_path)
    h.quote("blofin", 1.0050, 1.0060)
    h.quote("mexc", 1.0000, 1.0008)
    pos = await h.ex.enter_tt(Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0, spread_pct=0.42))
    h.quote("blofin", 1.0010, 1.0012)          # converged: buy back at 1.0012
    h.quote("mexc", 1.0009, 1.0011)            # sell long at 1.0009
    await h.ex.exit_tt(pos, "convergence")
    assert pos.status == CLOSED and pos.exit_reason == "convergence" and pos.exit_mode == "TT"
    assert pos.exit_price_a == pytest.approx(1.0012) and pos.exit_price_b == pytest.approx(1.0009)
    assert pos.gross_pnl_usd == pytest.approx(((1.0050 - 1.0012) / 1.0050 + (1.0009 - 1.0008) / 1.0008) * pos.size_usd)
    assert pos.net_pnl_usd == pytest.approx(pos.gross_pnl_usd - pos.entry_fees_usd - pos.exit_fees_usd)
    assert h.book.total_trades == 1 and h.book.total_pnl_usd == pytest.approx(pos.net_pnl_usd)
    assert await h.sim("blofin").positions() == [] and await h.sim("mexc").positions() == []
    assert h.risk.pair_stats["XYZUSDT|blofin>mexc"]["wins"] + h.risk.pair_stats["XYZUSDT|blofin>mexc"]["losses"] == 1


async def test_budget_exhaustion_rejects_entry_but_reserve_allows_close(tmp_path):
    h = Harness(tmp_path)
    h.quote("blofin", 1.0050, 1.0060)
    h.quote("mexc", 1.0000, 1.0008)
    bud = h.venues["blofin"].budget
    now = h.ex.clock()
    while bud.try_take("order", now):
        pass                                                     # burn the non-reserved blofin budget
    assert await h.ex.enter_tt(Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0)) is None
    assert h.metrics.funnel["budget"] == 1 and h.book.open == []  # blofin rejected → mexc leg flattened → failed_entry
    assert h.book.closed[0].exit_reason == "failed_entry"
```

- [ ] **Step 2: Write the failing TM-flow tests**

`tests/test_execution_tm.py`:

```python
import asyncio

import pytest

from bbo_trader.models import Intent, OPEN, CLOSED, MAKER_RESTING, HEDGING, EXIT_MAKER_RESTING
from tests.test_execution_tt import Harness, SYM


async def settle():
    for _ in range(5):
        await asyncio.sleep(0.005)


async def test_tm_entry_rests_fills_hedges_opens(tmp_path):
    h = Harness(tmp_path)
    h.quote("blofin", 1.0041, 1.0061)
    h.quote("mexc", 1.0000, 1.0010)
    intent = Intent("TM_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                    rest_price=1.0061, size_usd=25.0, spread_pct=0.31, edge_pct=0.19, ts=0.0)
    pos = await h.ex.enter_tm(intent)
    assert pos is not None and pos.status == MAKER_RESTING and pos.maker_side == "sell"
    assert pos.maker_qty == 20.0 and pos.qty_b == 2.0 and pos.maker_order_id.startswith("sim-")
    assert len(await h.sim("blofin").open_orders()) == 1
    await asyncio.sleep(0.005)                                   # order becomes live at the venue
    h.quote("blofin", 1.0061, 1.0062, bq=100.0)                  # buyer lifts our ask: 50% of 100 ≥ 20 → full fill
    await settle()
    assert pos.status == OPEN and pos.mode == "TM"
    assert pos.filled_a == 20.0 and pos.entry_price_a == pytest.approx(1.0061)
    assert pos.filled_b == 2.0 and pos.entry_price_b == pytest.approx(1.0010)   # hedge bought mexc ask
    assert pos.hedged_qty == 20.0 and pos.fee_liquidity["maker"] == "maker"
    assert pos.entry_fees_usd == pytest.approx(20 * 1.0061 * 0.02 / 100 + 2 * 1.0010 * 10 * 0.02 / 100)
    assert pos.entry_spread_pct == pytest.approx((1.0061 - 1.0010) / 1.0010 * 100)
    assert h.metrics.hists["fill_to_hedged"].count == 1 and h.metrics.hists["post_to_first_fill"].count == 1
    assert (await h.sim("blofin").positions())[0].side == "short" and (await h.sim("mexc").positions())[0].qty == 2.0


async def test_tm_partial_fill_cancels_remainder_and_opens_partial(tmp_path):
    h = Harness(tmp_path)
    h.quote("blofin", 1.0041, 1.0061)
    h.quote("mexc", 1.0000, 1.0010)
    pos = await h.ex.enter_tm(Intent("TM_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                     rest_price=1.0061, size_usd=25.0))
    await asyncio.sleep(0.005)
    h.quote("blofin", 1.0061, 1.0062, bq=20.0)                   # 50% of 20 → 10 contracts = $10.06 → 1 mexc contract
    await settle()
    assert pos.status == OPEN
    assert pos.filled_a == 10.0 and pos.filled_b == 1.0 and pos.hedged_qty == 10.0
    assert pos.size_usd == pytest.approx(min(10 * 1.0061, 1 * 1.0010 * 10))
    assert await h.sim("blofin").open_orders() == []              # remainder cancelled on first fill
    assert pos.maker_cancel_sent
    # the $10.01 hedge contract covered 9.95 maker contracts; the 0.05 residual is within tolerance
    assert (await h.sim("blofin").positions())[0].qty == 10.0


async def test_tm_residual_beyond_tolerance_is_flattened(tmp_path):
    h = Harness(tmp_path, max_position_usd=31.0)
    h.quote("blofin", 1.0000, 1.0020)
    h.quote("mexc", 1.0000, 1.0010)
    pos = await h.ex.enter_tm(Intent("TM_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                     rest_price=1.0020, size_usd=31.0))
    assert pos.maker_qty == 30.0 and pos.qty_b == 3.0              # $30.06 vs 3 × $10.01
    await asyncio.sleep(0.005)
    h.quote("blofin", 1.0020, 1.0021, bq=50.0)                   # 50% of 50 -> 25 contracts fill, then cancel
    await settle()
    assert pos.status == OPEN
    # $25.05 filled -> 2 mexc contracts ($20.02) cover ~19.98 maker contracts; residual ~5 > 5% -> flattened
    assert pos.filled_b == 2.0 and pos.filled_a == pytest.approx(20.0, abs=0.2)
    assert (await h.sim("blofin").positions())[0].qty == pytest.approx(pos.filled_a)
    assert pos.exit_fees_usd > 0                                 # the flatten paid a taker fee


async def test_tm_cancel_with_nothing_filled_discards(tmp_path):
    h = Harness(tmp_path)
    h.quote("blofin", 1.0041, 1.0061)
    h.quote("mexc", 1.0000, 1.0010)
    pos = await h.ex.enter_tm(Intent("TM_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                     rest_price=1.0061, size_usd=25.0))
    await h.ex.cancel_maker(pos, "ttl")
    await settle()
    assert h.book.open == [] and h.book.closed == [] and h.metrics.funnel["maker_cancelled"] == 1
    assert await h.sim("blofin").positions() == []


async def test_upgrade_to_tt_after_cancel(tmp_path):
    h = Harness(tmp_path)
    h.quote("blofin", 1.0041, 1.0061)
    h.quote("mexc", 1.0000, 1.0010)
    pos = await h.ex.enter_tm(Intent("TM_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                     rest_price=1.0061, size_usd=25.0))
    h.quote("blofin", 1.0055, 1.0061)                            # TT now pays; strategy would emit UPGRADE_TT
    await h.ex.upgrade_to_tt(pos)
    await settle()
    assert pos.status == OPEN and pos.mode == "TT"
    assert pos.entry_price_a == pytest.approx(1.0055) and pos.fee_liquidity["entry_a"] == "taker"


async def test_requote_amends_resting_price(tmp_path):
    h = Harness(tmp_path)
    h.quote("blofin", 1.0041, 1.0061)
    h.quote("mexc", 1.0000, 1.0010)
    pos = await h.ex.enter_tm(Intent("TM_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                     rest_price=1.0061, size_usd=25.0))
    await h.ex.requote(pos, 1.0063)
    assert pos.maker_rest_price == 1.0063
    assert (await h.sim("blofin").open_orders())[0].client_id == pos.maker_client_id
    assert h.sim("blofin")._orders[pos.maker_client_id].price == 1.0063


async def test_tm_exit_take_profit_and_tt_exit_while_resting(tmp_path):
    h = Harness(tmp_path)
    h.quote("blofin", 1.0050, 1.0060)
    h.quote("mexc", 1.0000, 1.0008)
    pos = await h.ex.enter_tt(Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0))
    assert pos.status == OPEN
    # rest a reduce-only buy on blofin at 1.0015 (take-profit against mexc bid 1.0000 × 1.0015)
    h.quote("blofin", 1.0020, 1.0030)
    h.quote("mexc", 1.0000, 1.0005)
    assert await h.ex.exit_tm(pos, Intent("TM_EXIT", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin", rest_price=1.0015))
    assert pos.status == EXIT_MAKER_RESTING and pos.maker_side == "buy" and pos.maker_qty == 20.0
    await asyncio.sleep(0.005)
    h.quote("blofin", 1.0010, 1.0015, aq=100.0)                  # seller hits our bid → fill 20
    await settle()
    assert pos.status == CLOSED and pos.exit_reason == "take_profit" and pos.exit_mode == "TM"
    assert pos.exit_price_a == pytest.approx(1.0015) and pos.exit_price_b == pytest.approx(1.0000)   # hedge sold mexc bid
    assert pos.fee_liquidity["maker"] == "maker" and pos.net_pnl_usd > 0
    assert await h.sim("blofin").positions() == [] and await h.sim("mexc").positions() == []

    # second position: resting exit maker, then a TT exit (convergence) cancels it and closes at market
    h.quote("blofin", 1.0050, 1.0060)
    h.quote("mexc", 1.0000, 1.0008)
    pos2 = await h.ex.enter_tt(Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0))
    h.quote("blofin", 1.0020, 1.0030)
    h.quote("mexc", 1.0000, 1.0005)
    assert await h.ex.exit_tm(pos2, Intent("TM_EXIT", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin", rest_price=1.0015))
    h.quote("blofin", 1.0004, 1.0006)                            # converged
    h.quote("mexc", 1.0003, 1.0005)
    await h.ex.exit_tt(pos2, "convergence")
    await settle()
    assert pos2.status == CLOSED and pos2.exit_reason == "convergence" and pos2.exit_mode == "TM+TT"
    assert pos2.exit_price_a == pytest.approx(1.0006) and pos2.exit_price_b == pytest.approx(1.0003)
    assert await h.sim("blofin").positions() == [] and await h.sim("mexc").positions() == []
    assert h.book.total_trades == 2
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_execution_tt.py tests/test_execution_tm.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'bbo_trader.execution'`

- [ ] **Step 4: Implement `bbo_trader/execution.py`**

```python
"""Executor: turns Intents into orders and OrderEvents into position transitions.

Two flows, used identically for entries (open both legs) and exits (reduce-only on both legs):
- TakerTaker: both legs at market in parallel; each leg awaits its terminal OrderEvent from the private
  feed, falling back to REST `query_order` polling after EVENT_GRACE_S; a market order with no terminal
  state after POLL_MAX_S is assumed filled at the submitted size (MEXC status lag); a one-leg failure
  flattens the filled leg (retry ladder → DEGRADED).
- PeggedMaker: a post-only order rests on the maker venue; the FIRST fill event cancels the remainder and
  every fill event hedges the unhedged delta at market on the other venue; the strategy drives
  requote / cancel / upgrade through intents; when the resting order is terminal and everything is
  hedged the position opens (entry) or closes (exit). Hedge failure → flatten the maker fill.

Order events are matched by clientOrderId, so an event may arrive before the REST ack returns.
All venue calls go through the per-venue RateBudget; hedges, flattens, closes and cancels use the reserve."""
from __future__ import annotations

import asyncio
import itertools
import logging
import time
from dataclasses import dataclass
from typing import Awaitable, Callable

from .config import Config
from .edge import spread_pct
from .metrics import Metrics
from .models import (Intent, OrderEvent, Position, TT_ENTERING, MAKER_RESTING, HEDGING, OPEN,
                     EXIT_MAKER_RESTING, TT_EXITING, EXIT_HEDGING, DEGRADED)
from .positions import PositionBook, transition
from .quotes import QuoteBoard
from .risk import RiskManager
from .sizing import size_pair, hedge_plan, excess_to_flatten, lots_floor, notional
from .venues.base import Venue

log = logging.getLogger("bbo.exec")

EVENT_GRACE_S = 1.5
POLL_S = 0.3
POLL_MAX_S = 5.0
HEDGE_RETRIES = 3
HEDGE_RETRY_S = 0.2
FLATTEN_LADDER_S = (1.0, 2.0, 5.0, 10.0, 10.0, 10.0)
DEGRADED_RETRY_S = 30.0


@dataclass
class LegTrack:
    pos_id: int
    leg: str
    venue: str
    symbol: str
    qty: float
    submitted_ts: float
    last: OrderEvent | None = None
    done: asyncio.Future | None = None


class Executor:
    def __init__(self, cfg: Config, venues: dict[str, Venue], board: QuoteBoard, book: PositionBook,
                 risk: RiskManager, metrics: Metrics, notify: Callable[[str], Awaitable[None]] | None = None,
                 clock=time.time, sleep=asyncio.sleep):
        self.cfg = cfg
        self.venues = venues
        self.board = board
        self.book = book
        self.risk = risk
        self.metrics = metrics
        self.notify = notify
        self.clock = clock
        self._sleep = sleep
        self._tracks: dict[str, LegTrack] = {}
        self._seq = itertools.count(1)
        self._locks: dict[int, asyncio.Lock] = {}
        self._tasks: set[asyncio.Task] = set()
        self.busy: set[str] = set()   # symbols with an entry in flight before the position exists

    # ---- plumbing ----------------------------------------------------------------
    def _new_cid(self, pos: Position, leg: str) -> str:
        cid = f"b{self.cfg.mode[0]}{pos.id}-{leg}-{next(self._seq)}"
        return cid[:32]

    def _lock(self, pos_id: int) -> asyncio.Lock:
        return self._locks.setdefault(pos_id, asyncio.Lock())

    def _spawn(self, coro) -> None:
        t = asyncio.get_running_loop().create_task(self._guard(coro))
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)

    async def _guard(self, coro) -> None:
        try:
            await coro
        except Exception:  # noqa: BLE001 — never let one event kill the loop
            log.exception("EXEC_TASK_ERROR")

    def _say(self, text: str) -> None:
        if self.notify is not None:
            self._spawn(self.notify(text))

    def _note_rate_limit(self, venue: str, error: str) -> None:
        e = (error or "").lower()
        if "429" in e or "too frequent" in e or "rate limit" in e or "too many" in e:
            self.venues[venue].budget.penalize(self.clock())
            log.warning("RATE_LIMIT %s: %s", venue, error)

    def _spec(self, venue: str, symbol: str):
        return self.venues[venue].specs.get(symbol)

    # ---- order events ------------------------------------------------------------
    def on_order_event(self, ev: OrderEvent) -> None:
        tr = self._tracks.get(ev.client_id)
        if tr is None:
            log.info("ORDER_EVENT_UNKNOWN %s %s %s", ev.venue, ev.client_id, ev.state)
            return
        tr.last = ev
        pos = self.book.get(tr.pos_id)
        if pos is not None:
            if ev.order_id:
                pos.order_ids[tr.leg] = ev.order_id
            if ev.position_id:
                pos.venue_position_ids[ev.venue] = ev.position_id
            if ev.liquidity:
                pos.fee_liquidity[tr.leg] = ev.liquidity
            if tr.leg == "maker":
                self._on_maker_event(pos, tr, ev)
        if ev.terminal and tr.done is not None and not tr.done.done():
            tr.done.set_result(ev)

    # ---- taker leg -----------------------------------------------------------------
    async def _place_taker(self, pos: Position, venue: str, leg: str, side: str, qty: float,
                           reduce_only: bool, priority: bool = False) -> OrderEvent:
        v = self.venues[venue]
        now = self.clock()
        if not v.budget.try_take("order", now, priority=priority):
            self.metrics.funnel["budget"] += 1
            return OrderEvent(venue, "", "", "rejected", error="budget", ts=now)
        cid = self._new_cid(pos, leg)
        tr = LegTrack(pos.id, leg, venue, pos.symbol, qty, now, done=asyncio.get_running_loop().create_future())
        self._tracks[cid] = tr
        pos.client_ids[leg] = cid
        t0 = asyncio.get_running_loop().time()
        ack = await v.trading.place_market(pos.symbol, side, qty, reduce_only, cid)
        ack_ms = (asyncio.get_running_loop().time() - t0) * 1000.0
        self.metrics.record("submit_to_ack", ack_ms)
        self.book.audit_order({"ts": now, "pos": pos.id, "leg": leg, "venue": venue, "side": side, "qty": qty,
                               "type": "market", "reduce_only": reduce_only, "client_id": cid,
                               "ok": ack.ok, "error": ack.error})
        if not ack.ok:
            self._note_rate_limit(venue, ack.error)
            log.warning("ORDER_REJECTED #%d %s %s: %s", pos.id, venue, leg, ack.error)
            return OrderEvent(venue, cid, "", "rejected", error=ack.error or "rejected", ts=self.clock())
        if ack.order_id:
            pos.order_ids[leg] = ack.order_id
        ev = await self._await_terminal(tr, v, cid, ack.order_id, qty)
        pos.latency_ms[leg] = (asyncio.get_running_loop().time() - t0) * 1000.0
        self.metrics.record("submit_to_fill", pos.latency_ms[leg])
        return ev

    async def _await_terminal(self, tr: LegTrack, v: Venue, cid: str, order_id: str, qty: float) -> OrderEvent:
        try:
            return await asyncio.wait_for(asyncio.shield(tr.done), timeout=EVENT_GRACE_S)
        except asyncio.TimeoutError:
            pass
        loop = asyncio.get_running_loop()
        deadline = loop.time() + POLL_MAX_S
        while loop.time() < deadline:
            if tr.done.done():
                return tr.done.result()
            ev = await v.trading.query_order(tr.symbol, cid, order_id)
            if ev is not None and ev.terminal:
                return ev
            await self._sleep(POLL_S)
        if tr.done.done():
            return tr.done.result()
        log.warning("FILL_ASSUMED %s %s: no terminal state after %.1fs, trusting submitted qty",
                    v.name, cid, POLL_MAX_S)
        last = tr.last
        return OrderEvent(v.name, cid, order_id, "filled", filled_qty=qty,
                          avg_price=last.avg_price if last else 0.0, fee=last.fee if last else 0.0,
                          liquidity="taker", ts=self.clock())

    # ---- TT entry ------------------------------------------------------------------
    async def enter_tt(self, intent: Intent) -> Position | None:
        a, b, sym = intent.venue_a, intent.venue_b, intent.symbol
        qa, qb = self.board.get(a, sym), self.board.get(b, sym)
        spec_a, spec_b = self._spec(a, sym), self._spec(b, sym)
        if qa is None or qb is None or spec_a is None or spec_b is None:
            return None
        legs = size_pair(intent.size_usd, qa.bid, qb.ask, spec_a, spec_b, self.cfg.max_leg_mismatch_pct,
                         min_usd=self.cfg.min_position_usd)
        if legs is None:
            self.metrics.funnel["size_fail"] += 1
            return None
        now = self.clock()
        pos = self.book.new(sym, a, b, TT_ENTERING, "TT", size_usd=legs.matched_usd, qty_a=legs.qty_a,
                            qty_b=legs.qty_b, detect_spread_pct=intent.spread_pct, entry_time=now,
                            signal_ts=intent.ts)
        if intent.ts:
            self.metrics.record("detect_to_submit", (now - intent.ts) * 1000.0)
        log.info("TT_ENTER #%d %s short %s / long %s spread=%.3f%% edge=%.3f%% size=$%.2f",
                 pos.id, sym, a, b, intent.spread_pct, intent.edge_pct, legs.matched_usd)
        return await self._tt_legs(pos)

    async def _tt_legs(self, pos: Position) -> Position | None:
        a, b, sym = pos.venue_a, pos.venue_b, pos.symbol
        spec_a, spec_b = self._spec(a, sym), self._spec(b, sym)
        ev_a, ev_b = await asyncio.gather(
            self._place_taker(pos, a, "entry_a", "sell", pos.qty_a, False),
            self._place_taker(pos, b, "entry_b", "buy", pos.qty_b, False))
        ok_a = ev_a.state == "filled" and ev_a.filled_qty > 0
        ok_b = ev_b.state == "filled" and ev_b.filled_qty > 0
        now = self.clock()
        if ok_a and ok_b:
            pos.filled_a, pos.filled_b = ev_a.filled_qty, ev_b.filled_qty
            pos.entry_price_a, pos.entry_price_b = ev_a.avg_price, ev_b.avg_price
            pos.entry_fees_usd = ev_a.fee + ev_b.fee
            pos.size_usd = min(notional(pos.filled_a, pos.entry_price_a, spec_a),
                               notional(pos.filled_b, pos.entry_price_b, spec_b))
            pos.entry_spread_pct = (spread_pct(pos.entry_price_a, pos.entry_price_b)
                                    if pos.entry_price_a > 0 and pos.entry_price_b > 0 else pos.detect_spread_pct)
            pos.peak_spread_pct = abs(pos.entry_spread_pct)
            pos.entry_time = now
            transition(pos, OPEN)
            if pos.signal_ts:
                self.metrics.record("signal_to_open", (now - pos.signal_ts) * 1000.0)
            log.info("OPEN #%d %s TT fill_spread=%.3f%% size=$%.2f fees=$%.4f lat=%.0f/%.0fms",
                     pos.id, sym, pos.entry_spread_pct, pos.size_usd, pos.entry_fees_usd,
                     pos.latency_ms.get("entry_a", 0), pos.latency_ms.get("entry_b", 0))
            self._say(f"OPEN #{pos.id} {sym} {pos.mode} short {a} / long {b} spread {pos.entry_spread_pct:.3f}% ${pos.size_usd:.2f}")
            if pos.entry_spread_pct < self.cfg.min_fill_spread_pct:
                log.warning("FILL_QUALITY_ABORT #%d %s fill_spread=%.3f%% < %.3f%%", pos.id, sym,
                            pos.entry_spread_pct, self.cfg.min_fill_spread_pct)
                self.risk.set_cooldown(sym)
                await self.exit_tt(pos, "fill_quality_abort")
                return None
            self.book.dirty = True
            return pos
        if ok_a != ok_b:
            leg = "a" if ok_a else "b"
            ev = ev_a if ok_a else ev_b
            log.warning("LEG_DESYNC #%d %s: leg %s filled, other failed (%s)", pos.id, sym, leg,
                        (ev_b if ok_a else ev_a).error)
            if leg == "a":
                pos.filled_a, pos.entry_price_a = ev.filled_qty, ev.avg_price
                pos.size_usd = notional(ev.filled_qty, ev.avg_price, spec_a)
            else:
                pos.filled_b, pos.entry_price_b = ev.filled_qty, ev.avg_price
                pos.size_usd = notional(ev.filled_qty, ev.avg_price, spec_b)
            pos.entry_fees_usd = ev.fee
            self.risk.record_strike(sym, a, b)
            self.risk.set_cooldown(sym)
            if await self._flatten_leg(pos, leg, ev.filled_qty):
                self._close(pos, "failed_entry")
            else:
                transition(pos, DEGRADED)
                pos.degraded_leg = leg
                pos.last_close_attempt = self.clock()
            return None
        log.warning("ENTRY_FAILED #%d %s both legs: %s / %s", pos.id, sym, ev_a.error, ev_b.error)
        self.risk.record_strike(sym, a, b)
        self.risk.set_cooldown(sym)
        self.book.discard(pos)
        return None

    async def _flatten_leg(self, pos: Position, leg: str, qty: float) -> bool:
        """Market-close one entry leg (reduce-only) with the retry ladder. Records exit price/fees."""
        venue = pos.venue_a if leg == "a" else pos.venue_b
        side = "buy" if leg == "a" else "sell"
        for i, delay in enumerate(FLATTEN_LADDER_S):
            ev = await self._place_taker(pos, venue, f"flat_{leg}{i}", side, qty, True, priority=True)
            if ev.state == "filled" and ev.filled_qty > 0:
                self._accumulate_leg(pos, venue, "exit", ev)
                log.info("FLATTEN #%d %s %s ok qty=%s px=%s", pos.id, venue, leg, ev.filled_qty, ev.avg_price)
                return True
            log.warning("FLATTEN #%d %s attempt %d failed: %s", pos.id, venue, i + 1, ev.error)
            await self._sleep(delay)
        return False

    # ---- TM entry (PeggedMaker) -------------------------------------------------------
    async def enter_tm(self, intent: Intent) -> Position | None:
        a, b, sym, mv = intent.venue_a, intent.venue_b, intent.symbol, intent.maker_venue
        qa, qb = self.board.get(a, sym), self.board.get(b, sym)
        spec_a, spec_b = self._spec(a, sym), self._spec(b, sym)
        if qa is None or qb is None or spec_a is None or spec_b is None or mv not in (a, b):
            return None
        px_a = intent.rest_price if mv == a else qa.bid
        px_b = intent.rest_price if mv == b else qb.ask
        legs = size_pair(intent.size_usd, px_a, px_b, spec_a, spec_b, self.cfg.max_leg_mismatch_pct,
                         min_usd=self.cfg.min_position_usd)
        if legs is None:
            self.metrics.funnel["size_fail"] += 1
            return None
        now = self.clock()
        pos = self.book.new(sym, a, b, MAKER_RESTING, "TM", size_usd=legs.matched_usd, qty_a=legs.qty_a,
                            qty_b=legs.qty_b, detect_spread_pct=intent.spread_pct, entry_time=now,
                            signal_ts=intent.ts, maker_venue=mv, maker_side="sell" if mv == a else "buy",
                            maker_qty=legs.qty_a if mv == a else legs.qty_b, maker_rest_price=intent.rest_price)
        if intent.ts:
            self.metrics.record("detect_to_submit", (now - intent.ts) * 1000.0)
        if not await self._post_maker(pos, reduce_only=False):
            self.book.discard(pos)
            return None
        return pos

    async def _post_maker(self, pos: Position, reduce_only: bool) -> bool:
        v = self.venues[pos.maker_venue]
        now = self.clock()
        if not v.budget.try_take("order", now):
            self.metrics.funnel["budget"] += 1
            return False
        cid = self._new_cid(pos, "maker")
        self._tracks[cid] = LegTrack(pos.id, "maker", v.name, pos.symbol, pos.maker_qty, now)
        pos.maker_client_id = cid
        pos.client_ids["maker"] = cid
        pos.maker_posted_ts = now
        pos.maker_last_requote_ts = now
        pos.maker_cancel_sent = False
        pos.maker_filled_qty = 0.0
        pos.hedged_qty = 0.0
        pos.maker_fee_usd = 0.0
        pos.maker_avg_price = 0.0
        ack = await v.trading.place_post_only(pos.symbol, pos.maker_side, pos.maker_qty, pos.maker_rest_price,
                                              reduce_only, cid)
        self.book.audit_order({"ts": now, "pos": pos.id, "leg": "maker", "venue": v.name, "side": pos.maker_side,
                               "qty": pos.maker_qty, "price": pos.maker_rest_price, "type": "post_only",
                               "reduce_only": reduce_only, "client_id": cid, "ok": ack.ok, "error": ack.error})
        if not ack.ok:
            self._note_rate_limit(v.name, ack.error)
            if "102127" in (ack.error or ""):
                self.risk.blacklist_venue_symbol(v.name, pos.symbol)
            self.metrics.funnel["maker_rejected"] += 1
            log.info("TM_POST_REJECTED #%d %s %s @%s: %s", pos.id, v.name, pos.maker_side, pos.maker_rest_price, ack.error)
            return False
        if ack.order_id:
            pos.maker_order_id = ack.order_id
            pos.order_ids["maker"] = ack.order_id
        log.info("TM_POST #%d %s %s %s qty=%s @%s reduce_only=%s", pos.id, pos.symbol, v.name, pos.maker_side,
                 pos.maker_qty, pos.maker_rest_price, reduce_only)
        return True

    async def requote(self, pos: Position, new_price: float) -> None:
        v = self.venues[pos.maker_venue]
        now = self.clock()
        if pos.maker_cancel_sent or pos.status not in (MAKER_RESTING, EXIT_MAKER_RESTING):
            return
        if v.trading.supports_amend:
            if not v.budget.try_take("amend", now):
                return
            ack = await v.trading.amend(pos.symbol, pos.maker_client_id, pos.maker_order_id, new_price)
            if ack.ok:
                log.info("TM_REQUOTE #%d %s %s -> %s", pos.id, v.name, pos.maker_rest_price, new_price)
                pos.maker_rest_price = new_price
                pos.maker_last_requote_ts = now
            else:
                self._note_rate_limit(v.name, ack.error)
            return
        if v.budget.available("cancel", now) < 1 or v.budget.available("order", now) < 1:
            return
        pos.requote_pending = True
        pos.requote_price = new_price
        await self._cancel_maker_order(pos, priority=False)

    async def cancel_maker(self, pos: Position, reason: str) -> None:
        log.info("TM_CANCEL #%d %s reason=%s", pos.id, pos.maker_venue, reason)
        await self._cancel_maker_order(pos, priority=True)

    async def upgrade_to_tt(self, pos: Position) -> None:
        if pos.status != MAKER_RESTING:
            return
        log.info("UPGRADE_TT #%d %s", pos.id, pos.symbol)
        pos.upgrade_pending = True
        await self._cancel_maker_order(pos, priority=True)

    async def _cancel_maker_order(self, pos: Position, priority: bool) -> bool:
        if pos.maker_cancel_sent:
            return True
        v = self.venues[pos.maker_venue]
        now = self.clock()
        if not v.budget.try_take("cancel", now, priority=priority):
            return False
        pos.maker_cancel_sent = True
        ok = await v.trading.cancel(pos.symbol, pos.maker_client_id, pos.maker_order_id)
        if not ok:
            # already terminal at the venue (filled or gone): pull the terminal state ourselves
            ev = await v.trading.query_order(pos.symbol, pos.maker_client_id, pos.maker_order_id)
            if ev is not None and ev.terminal:
                self.on_order_event(ev)
        return ok

    def _on_maker_event(self, pos: Position, tr: LegTrack, ev: OrderEvent) -> None:
        if ev.state in ("ack", "rejected"):
            return
        if ev.filled_qty > pos.maker_filled_qty + 1e-12:
            if pos.maker_fill_ts == 0.0:
                pos.maker_fill_ts = self.clock()
                self.metrics.record("post_to_first_fill", (pos.maker_fill_ts - pos.maker_posted_ts) * 1000.0)
            pos.maker_filled_qty = ev.filled_qty
            pos.maker_avg_price = ev.avg_price
            pos.maker_fee_usd = ev.fee
            log.info("TM_FILL #%d %s %s filled=%s/%s @%s", pos.id, pos.symbol, pos.maker_venue,
                     ev.filled_qty, pos.maker_qty, ev.avg_price)
        self._spawn(self._hedge_delta(pos, terminal=ev.terminal))

    def _hedge_side(self, pos: Position, phase: str) -> str:
        maker_is_a = pos.maker_venue == pos.venue_a
        if phase == "entry":
            return "buy" if maker_is_a else "sell"     # maker sold on A → buy B; maker bought on B → sell A
        return "sell" if maker_is_a else "buy"         # maker bought back on A → sell B; maker sold on B → buy A

    async def _hedge_delta(self, pos: Position, terminal: bool) -> None:
        """Hedge whatever the resting order has filled but we have not yet covered. The hedge is
        floored to whole hedge-venue lots, so `hedged_qty` only advances by the maker quantity the
        hedge notional really covers; the residual waits for more fills and, once the resting order
        is terminal, is flattened on the maker venue when it exceeds the mismatch tolerance."""
        async with self._lock(pos.id):
            if pos.status not in (MAKER_RESTING, HEDGING, EXIT_MAKER_RESTING, EXIT_HEDGING):
                return
            phase = "entry" if pos.status in (MAKER_RESTING, HEDGING) else "exit"
            mv = pos.maker_venue
            hv = pos.venue_b if mv == pos.venue_a else pos.venue_a
            spec_m, spec_h = self._spec(mv, pos.symbol), self._spec(hv, pos.symbol)
            unhedged = round(pos.maker_filled_qty - pos.hedged_qty, 10)
            if unhedged > 1e-12:
                if pos.status == MAKER_RESTING:
                    transition(pos, HEDGING)
                elif pos.status == EXIT_MAKER_RESTING:
                    transition(pos, EXIT_HEDGING)
                if not terminal:
                    await self._cancel_maker_order(pos, priority=True)   # first fill cancels the remainder
                side = self._hedge_side(pos, phase)
                q_h = self.board.get(hv, pos.symbol)
                px_h = ((q_h.ask if side == "buy" else q_h.bid) if q_h else pos.maker_avg_price) or pos.maker_avg_price
                plan = hedge_plan(unhedged, pos.maker_avg_price, spec_m, px_h, spec_h)
                if plan.hedge_qty > 0:
                    t_fill = pos.maker_fill_ts or self.clock()
                    ev = None
                    for attempt in range(HEDGE_RETRIES):
                        ev = await self._place_taker(pos, hv, f"hedge_{phase}{attempt}", side, plan.hedge_qty,
                                                     phase == "exit", priority=True)
                        if ev.state == "filled" and ev.filled_qty > 0:
                            break
                        await self._sleep(HEDGE_RETRY_S)
                    if ev is None or not (ev.state == "filled" and ev.filled_qty > 0):
                        log.error("HEDGE_FAILED #%d %s on %s — flattening maker fill", pos.id, pos.symbol, hv)
                        self.risk.record_strike(pos.symbol, pos.venue_a, pos.venue_b)
                        await self._flatten_maker_fill(pos, lots_floor(unhedged, spec_m), phase)
                        pos.hedged_qty = pos.maker_filled_qty
                    else:
                        pos.hedged_qty = round(pos.hedged_qty + plan.covered_maker_qty, 10)
                        self._accumulate_leg(pos, hv, phase, ev)
                        self.metrics.record("fill_to_hedged", (self.clock() - t_fill) * 1000.0)
                        log.info("TM_HEDGE #%d %s %s %s qty=%s @%s covers=%s maker", pos.id, pos.symbol, hv, side,
                                 ev.filled_qty, ev.avg_price, plan.covered_maker_qty)
                residual = round(pos.maker_filled_qty - pos.hedged_qty, 10)
                if residual > 1e-12 and terminal:
                    to_flat = excess_to_flatten(residual, pos.hedged_qty, spec_m, self.cfg.max_leg_mismatch_pct)
                    if to_flat > 0:
                        log.warning("RESIDUAL #%d %s %s maker contracts unhedged (matched %s) — flattening %s",
                                    pos.id, pos.symbol, residual, pos.hedged_qty, to_flat)
                        await self._flatten_maker_fill(pos, to_flat, phase)
                    elif residual <= pos.hedged_qty * self.cfg.max_leg_mismatch_pct / 100.0:
                        log.info("RESIDUAL_ACCEPTED #%d %s %s maker contracts within tolerance", pos.id, pos.symbol, residual)
                    else:
                        log.warning("RESIDUAL_RETAINED #%d %s %s maker contracts below the venue minimum — cannot be sent, exposure retained",
                                    pos.id, pos.symbol, residual)
                    pos.hedged_qty = pos.maker_filled_qty
            tr = self._tracks.get(pos.maker_client_id)
            maker_terminal = tr is not None and tr.last is not None and tr.last.terminal
            if maker_terminal and round(pos.maker_filled_qty - pos.hedged_qty, 10) <= 1e-12:
                await self._finalize_maker(pos, phase)

    async def _flatten_maker_fill(self, pos: Position, qty: float, phase: str) -> None:
        """Undo an unhedgeable/unhedged maker fill on the maker venue itself (reduce-only market)."""
        if qty <= 0:
            return
        side = "buy" if pos.maker_side == "sell" else "sell"
        for i, delay in enumerate(FLATTEN_LADDER_S[:3]):
            ev = await self._place_taker(pos, pos.maker_venue, f"mflat{i}", side, qty, True, priority=True)
            if ev.state == "filled" and ev.filled_qty > 0:
                pos.exit_fees_usd += ev.fee
                pos.maker_filled_qty = round(pos.maker_filled_qty - qty, 10)   # netted out
                log.info("FLATTEN #%d maker leg %s qty=%s", pos.id, pos.maker_venue, qty)
                return
            await self._sleep(delay)
        log.error("FLATTEN_FAILED #%d maker leg %s qty=%s — DEGRADED", pos.id, pos.maker_venue, qty)
        transition(pos, DEGRADED)
        pos.degraded_leg = "a" if pos.maker_venue == pos.venue_a else "b"
        pos.last_close_attempt = self.clock()

    def _accumulate_leg(self, pos: Position, venue: str, phase: str, ev: OrderEvent) -> None:
        is_a = venue == pos.venue_a
        if phase == "entry":
            q_attr, p_attr = ("filled_a", "entry_price_a") if is_a else ("filled_b", "entry_price_b")
            pos.entry_fees_usd += ev.fee
        else:
            q_attr, p_attr = ("exit_filled_a", "exit_price_a") if is_a else ("exit_filled_b", "exit_price_b")
            pos.exit_fees_usd += ev.fee
        q0, p0 = getattr(pos, q_attr), getattr(pos, p_attr)
        q1 = q0 + ev.filled_qty
        setattr(pos, p_attr, (p0 * q0 + ev.avg_price * ev.filled_qty) / q1 if q1 > 0 else 0.0)
        setattr(pos, q_attr, round(q1, 10))

    def _apply_maker_leg(self, pos: Position, phase: str) -> None:
        is_a = pos.maker_venue == pos.venue_a
        if phase == "entry":
            if is_a:
                pos.filled_a, pos.entry_price_a = pos.maker_filled_qty, pos.maker_avg_price
            else:
                pos.filled_b, pos.entry_price_b = pos.maker_filled_qty, pos.maker_avg_price
            pos.entry_fees_usd += pos.maker_fee_usd
        else:
            if is_a:
                pos.exit_filled_a, pos.exit_price_a = pos.maker_filled_qty, pos.maker_avg_price
            else:
                pos.exit_filled_b, pos.exit_price_b = pos.maker_filled_qty, pos.maker_avg_price
            pos.exit_fees_usd += pos.maker_fee_usd
        pos.fee_liquidity["maker"] = "maker"

    async def _finalize_maker(self, pos: Position, phase: str) -> None:
        now = self.clock()
        spec_a, spec_b = self._spec(pos.venue_a, pos.symbol), self._spec(pos.venue_b, pos.symbol)
        if phase == "entry":
            if pos.maker_filled_qty <= 1e-12:
                if pos.requote_pending and pos.status == MAKER_RESTING:
                    pos.requote_pending = False
                    pos.maker_rest_price = pos.requote_price
                    if await self._post_maker(pos, reduce_only=False):
                        return
                if pos.upgrade_pending and pos.status == MAKER_RESTING:
                    pos.upgrade_pending = False
                    pos.mode = "TT"
                    transition(pos, TT_ENTERING)
                    await self._tt_legs(pos)
                    return
                self.metrics.funnel["maker_cancelled"] += 1
                self.book.discard(pos)
                return
            self._apply_maker_leg(pos, "entry")
            pos.size_usd = min(notional(pos.filled_a, pos.entry_price_a, spec_a),
                               notional(pos.filled_b, pos.entry_price_b, spec_b))
            pos.entry_spread_pct = (spread_pct(pos.entry_price_a, pos.entry_price_b)
                                    if pos.entry_price_a > 0 and pos.entry_price_b > 0 else pos.detect_spread_pct)
            pos.peak_spread_pct = abs(pos.entry_spread_pct)
            pos.entry_time = now
            if pos.status == MAKER_RESTING:
                transition(pos, HEDGING)
            transition(pos, OPEN)
            if pos.signal_ts:
                self.metrics.record("signal_to_open", (now - pos.signal_ts) * 1000.0)
            log.info("OPEN #%d %s TM maker=%s fill_spread=%.3f%% size=$%.2f fees=$%.4f", pos.id, pos.symbol,
                     pos.maker_venue, pos.entry_spread_pct, pos.size_usd, pos.entry_fees_usd)
            self._say(f"OPEN #{pos.id} {pos.symbol} TM maker {pos.maker_venue} spread {pos.entry_spread_pct:.3f}% ${pos.size_usd:.2f}")
            self.book.dirty = True
            return
        # exit phase
        if pos.maker_filled_qty <= 1e-12:
            if pos.requote_pending and pos.status == EXIT_MAKER_RESTING:
                pos.requote_pending = False
                pos.maker_rest_price = pos.requote_price
                if await self._post_maker(pos, reduce_only=True):
                    return
            if pos.exit_reason:
                transition(pos, TT_EXITING)
                await self._close_remainder_tt(pos)
                return
            if pos.status in (EXIT_MAKER_RESTING, EXIT_HEDGING):
                if pos.status == EXIT_HEDGING:
                    transition(pos, TT_EXITING)
                    await self._close_remainder_tt(pos)
                    return
                transition(pos, OPEN)
                pos.maker_venue = ""
            return
        self._apply_maker_leg(pos, "exit")
        if pos.status == EXIT_MAKER_RESTING:
            transition(pos, EXIT_HEDGING)
        rem_a = round(pos.filled_a - pos.exit_filled_a, 10)
        rem_b = round(pos.filled_b - pos.exit_filled_b, 10)
        if rem_a > 1e-9 or rem_b > 1e-9:
            transition(pos, TT_EXITING)
            await self._close_remainder_tt(pos)
            return
        self._close(pos, pos.exit_reason or "take_profit")

    # ---- exits ---------------------------------------------------------------------
    async def exit_tm(self, pos: Position, intent: Intent) -> bool:
        if pos.status != OPEN or intent.maker_venue not in (pos.venue_a, pos.venue_b):
            return False
        mv = intent.maker_venue
        pos.maker_venue = mv
        pos.maker_side = "buy" if mv == pos.venue_a else "sell"
        pos.maker_qty = round((pos.filled_a - pos.exit_filled_a) if mv == pos.venue_a else (pos.filled_b - pos.exit_filled_b), 10)
        pos.maker_rest_price = intent.rest_price
        pos.exit_reason = ""
        pos.exit_mode = "TM"
        pos.maker_fill_ts = 0.0
        pos.upgrade_pending = False
        pos.requote_pending = False
        pos.edge_gone_since = 0.0
        if pos.maker_qty <= 0:
            return False
        transition(pos, EXIT_MAKER_RESTING)
        if not await self._post_maker(pos, reduce_only=True):
            transition(pos, OPEN)
            pos.maker_venue = ""
            return False
        return True

    async def exit_tt(self, pos: Position, reason: str) -> None:
        pos.exit_reason = reason
        if pos.status == EXIT_MAKER_RESTING:
            pos.exit_mode = "TM+TT"
            await self._cancel_maker_order(pos, priority=True)   # finalize closes the remainder TT
            return
        if pos.status != OPEN:
            return
        pos.exit_mode = pos.exit_mode or "TT"
        transition(pos, TT_EXITING)
        log.info("TT_EXIT #%d %s reason=%s", pos.id, pos.symbol, reason)
        await self._close_remainder_tt(pos)

    async def _close_remainder_tt(self, pos: Position) -> None:
        rem_a = round(pos.filled_a - pos.exit_filled_a, 10)
        rem_b = round(pos.filled_b - pos.exit_filled_b, 10)
        jobs = []
        if rem_a > 1e-9:
            jobs.append(("a", self._place_taker(pos, pos.venue_a, "exit_a", "buy", rem_a, True, priority=True)))
        if rem_b > 1e-9:
            jobs.append(("b", self._place_taker(pos, pos.venue_b, "exit_b", "sell", rem_b, True, priority=True)))
        results = await asyncio.gather(*(j[1] for j in jobs)) if jobs else []
        failed = ""
        for (leg, _), ev in zip(jobs, results):
            if ev.state == "filled" and ev.filled_qty > 0:
                self._accumulate_leg(pos, pos.venue_a if leg == "a" else pos.venue_b, "exit", ev)
            else:
                failed += leg
        if failed:
            log.warning("CLOSE_DEGRADED #%d %s legs=%s", pos.id, pos.symbol, failed)
            transition(pos, DEGRADED)
            pos.degraded_leg = "both" if failed == "ab" else failed
            pos.close_retry_count += 1
            pos.last_close_attempt = self.clock()
            return
        self._close(pos, pos.exit_reason or "convergence")

    async def retry_degraded(self) -> None:
        now = self.clock()
        for pos in list(self.book.by_status(DEGRADED)):
            if now - pos.last_close_attempt < DEGRADED_RETRY_S:
                continue
            pos.last_close_attempt = now
            pos.close_retry_count += 1
            rem_a = round(pos.filled_a - pos.exit_filled_a, 10)
            rem_b = round(pos.filled_b - pos.exit_filled_b, 10)
            ok = True
            if rem_a > 1e-9:
                ev = await self._place_taker(pos, pos.venue_a, f"retry_a{pos.close_retry_count}", "buy", rem_a, True, priority=True)
                ok &= ev.state == "filled" and ev.filled_qty > 0
                if ok:
                    self._accumulate_leg(pos, pos.venue_a, "exit", ev)
            if rem_b > 1e-9:
                ev = await self._place_taker(pos, pos.venue_b, f"retry_b{pos.close_retry_count}", "sell", rem_b, True, priority=True)
                good = ev.state == "filled" and ev.filled_qty > 0
                ok &= good
                if good:
                    self._accumulate_leg(pos, pos.venue_b, "exit", ev)
            if ok:
                self._close(pos, pos.exit_reason or "recovered")

    def _close(self, pos: Position, reason: str) -> None:
        self.book.close(pos, reason, self.clock())
        self.risk.record_close(pos)
        log.info("CLOSE #%d %s reason=%s pnl=$%+.4f gross=$%+.4f fees=$%.4f exit_spread=%.3f%%", pos.id, pos.symbol,
                 reason, pos.net_pnl_usd, pos.gross_pnl_usd, pos.entry_fees_usd + pos.exit_fees_usd, pos.exit_spread_pct)
        self._say(f"CLOSE #{pos.id} {pos.symbol} {reason} pnl ${pos.net_pnl_usd:+.4f}")

    async def cancel_all_resting(self) -> None:
        for pos in list(self.book.by_status(MAKER_RESTING, EXIT_MAKER_RESTING)):
            await self._cancel_maker_order(pos, priority=True)
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_execution_tt.py tests/test_execution_tm.py -q`
Expected: `11 passed`

- [ ] **Step 6: Run the whole suite and commit**

Run: `.venv/bin/python -m pytest -q`
Expected: `102 passed`

```bash
git add deploy-bbo/bbo_trader/execution.py deploy-bbo/tests/test_execution_tt.py deploy-bbo/tests/test_execution_tm.py
git commit -m "feat(bbo): Executor — parallel TT legs with event-driven fills, PeggedMaker post/peg/hedge, flatten, degraded retry"
```

---

### Task 17: Universe discovery (`discovery.py`)

**Files:**
- Create: `deploy-bbo/bbo_trader/discovery.py`
- Test: `deploy-bbo/tests/test_discovery.py`

- [ ] **Step 1: Write the failing test**

`tests/test_discovery.py`:

```python
from bbo_trader.discovery import build_universe, symbols_for_venue, symbols_for_quote_venue
from tests.conftest import mk_spec


def test_universe_requires_two_trade_venues_and_applies_filters():
    specs = {
        "mexc": {s: mk_spec("mexc", s) for s in ("AUSDT", "BUSDT", "CUSDT", "BADUSDT")},
        "blofin": {s: mk_spec("blofin", s) for s in ("AUSDT", "BUSDT", "BADUSDT")},
        "okx": {s: mk_spec("okx", s) for s in ("AUSDT", "CUSDT")},
    }
    uni = build_universe(specs, ["mexc", "blofin", "okx"], blocked={"BADUSDT"}, whitelists={"okx": ("CUSDT",)})
    assert uni == {"AUSDT": ["blofin", "mexc"], "BUSDT": ["blofin", "mexc"], "CUSDT": ["mexc", "okx"]}
    assert symbols_for_venue(uni, "okx") == ["CUSDT"]
    assert symbols_for_venue(uni, "mexc") == ["AUSDT", "BUSDT", "CUSDT"]
    assert symbols_for_quote_venue(uni, specs["okx"]) == ["AUSDT", "CUSDT"]
    assert build_universe(specs, ["mexc"], set(), {}) == {}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_discovery.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'bbo_trader.discovery'`

- [ ] **Step 3: Implement `bbo_trader/discovery.py`**

```python
"""Universe construction. Pure: symbols listed on >= 2 trade venues after blocked list and per-venue whitelists."""
from __future__ import annotations

from .models import VenueSpec


def build_universe(specs: dict[str, dict[str, VenueSpec]], trade_venues: list[str], blocked: frozenset[str] | set[str],
                   whitelists: dict[str, tuple[str, ...]]) -> dict[str, list[str]]:
    listed: dict[str, list[str]] = {}
    for v in trade_venues:
        wl = set(whitelists.get(v) or ())
        for sym in specs.get(v, {}):
            if sym in blocked or (wl and sym not in wl):
                continue
            listed.setdefault(sym, []).append(v)
    return {s: sorted(vs) for s, vs in sorted(listed.items()) if len(vs) >= 2}


def symbols_for_venue(universe: dict[str, list[str]], venue: str) -> list[str]:
    """Trade venue: the universe symbols it is a leg for."""
    return sorted(s for s, vs in universe.items() if venue in vs)


def symbols_for_quote_venue(universe: dict[str, list[str]], venue_specs: dict[str, VenueSpec]) -> list[str]:
    """Quote-only venue: every universe symbol it lists (feeds the scanner, never a leg)."""
    return sorted(s for s in universe if s in venue_specs)
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_discovery.py -q`
Expected: `1 passed`

- [ ] **Step 5: Commit**

```bash
git add deploy-bbo/bbo_trader/discovery.py deploy-bbo/tests/test_discovery.py
git commit -m "feat(bbo): universe from per-venue specs (>=2 trade venues, blocked list, whitelists)"
```

---

### Task 18: Telegram notifications and commands (`notify.py`)

**Files:**
- Create: `deploy-bbo/bbo_trader/notify.py`
- Test: `deploy-bbo/tests/test_notify.py`

- [ ] **Step 1: Write the failing tests**

`tests/test_notify.py`:

```python
from bbo_trader.notify import parse_commands, Telegram


def test_parse_commands_filters_chat_and_advances_offset():
    updates = [
        {"update_id": 10, "message": {"chat": {"id": 42}, "text": "/stop"}},
        {"update_id": 11, "message": {"chat": {"id": 99}, "text": "/start"}},        # someone else
        {"update_id": 12, "message": {"chat": {"id": 42}, "text": "hello"}},
        {"update_id": 13, "message": {"chat": {"id": 42}, "text": "/close_all now"}},
    ]
    cmds, nxt = parse_commands(updates, "42", offset=0)
    assert cmds == ["/stop", "/close_all"] and nxt == 14
    assert parse_commands([], "42", 14) == ([], 14)


async def test_disabled_telegram_is_a_noop():
    t = Telegram("", "")
    assert not t.enabled and await t.send("x") is False and await t.poll_commands() == []
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_notify.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'bbo_trader.notify'`

- [ ] **Step 3: Implement `bbo_trader/notify.py`**

```python
"""Telegram: send messages and poll the operator commands (/stop, /start, /close_all, /status)."""
from __future__ import annotations

import logging

import aiohttp

log = logging.getLogger("bbo.notify")
COMMANDS = ("/stop", "/start", "/close_all", "/status")


def parse_commands(updates: list[dict], chat_id: str, offset: int) -> tuple[list[str], int]:
    """Pure: Telegram getUpdates payload → (commands from our chat, next offset)."""
    cmds: list[str] = []
    nxt = offset
    for u in updates:
        uid = int(u.get("update_id", 0))
        nxt = max(nxt, uid + 1)
        msg = u.get("message") or {}
        if str((msg.get("chat") or {}).get("id", "")) != str(chat_id):
            continue
        text = str(msg.get("text", "")).strip().split()[0] if msg.get("text") else ""
        if text in COMMANDS:
            cmds.append(text)
    return cmds, nxt


class Telegram:
    def __init__(self, token: str, chat_id: str, session_factory=aiohttp.ClientSession):
        self.token, self.chat_id = token, chat_id
        self.enabled = bool(token and chat_id)
        self._session_factory = session_factory
        self._offset = 0

    async def send(self, text: str) -> bool:
        if not self.enabled:
            return False
        try:
            async with self._session_factory() as s:
                async with s.post(f"https://api.telegram.org/bot{self.token}/sendMessage",
                                  json={"chat_id": self.chat_id, "text": text[:4000]},
                                  timeout=aiohttp.ClientTimeout(total=10)) as r:
                    return r.status == 200
        except Exception as e:  # noqa: BLE001
            log.debug("telegram send failed: %r", e)
            return False

    async def poll_commands(self) -> list[str]:
        if not self.enabled:
            return []
        try:
            async with self._session_factory() as s:
                async with s.get(f"https://api.telegram.org/bot{self.token}/getUpdates",
                                 params={"offset": self._offset, "timeout": 0},
                                 timeout=aiohttp.ClientTimeout(total=10)) as r:
                    data = await r.json(content_type=None)
        except Exception as e:  # noqa: BLE001
            log.debug("telegram poll failed: %r", e)
            return []
        cmds, self._offset = parse_commands(data.get("result") or [], self.chat_id, self._offset)
        return cmds
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_notify.py -q`
Expected: `2 passed`

- [ ] **Step 5: Commit**

```bash
git add deploy-bbo/bbo_trader/notify.py deploy-bbo/tests/test_notify.py
git commit -m "feat(bbo): Telegram send + /stop /start /close_all /status command polling"
```

---

### Task 19: Venue registry, App wiring, entry point (`venues/registry.py`, `app.py`, `main.py`)

**Files:**
- Create: `deploy-bbo/bbo_trader/venues/registry.py`, `deploy-bbo/bbo_trader/app.py`, `deploy-bbo/bbo_trader/main.py`
- Test: `deploy-bbo/tests/test_app.py`

`App` is the only place where quotes, strategy and executor meet: `on_bbo` stores the quote, feeds the paper venue's fill model, then either drives the symbol's existing position (requote/cancel/upgrade/exit intents) or evaluates an entry. The sweep (every 500 ms) handles timers, halt flags, degraded retries, heartbeat and state saves. The end-to-end test runs quotes through the whole pipeline with `SimVenue`s — no network.

- [ ] **Step 1: Write the failing end-to-end tests**

`tests/test_app.py`:

```python
import asyncio
import json
from collections import Counter

import pytest

from bbo_trader.app import App
from bbo_trader.main import legacy_bot_running
from bbo_trader.models import OPEN, CLOSED, MAKER_RESTING
from bbo_trader.positions import StateStore, StateCorrupt
from bbo_trader.strategy import PairEvaluator
from tests.conftest import mk_bbo
from tests.test_execution_tt import Harness, SYM


async def settle(n=8):
    for _ in range(n):
        await asyncio.sleep(0.005)


def build_app(tmp_path, **over) -> App:
    h = Harness(tmp_path, **over)
    fees = {n: v.fees for n, v in h.venues.items()}
    ev = PairEvaluator(h.cfg, h.board, fees, {}, {}, h.risk, h.metrics.funnel)
    app = App(h.cfg, h.venues, h.board, h.book, h.risk, h.metrics, h.ex, ev, StateStore(tmp_path / "real_state.json"))
    for v in h.venues.values():
        v.private.set_handler(h.ex.on_order_event)
    app.apply_universe()                      # specs already on the Venue bundles → universe = {SYM}
    app.harness = h
    return app


def bbo(venue, bid, ask, cs, **kw):
    return mk_bbo(venue, SYM, bid, ask, contract_size=cs, **kw)


async def test_quote_to_tt_position_to_exit_and_state_file(tmp_path):
    app = build_app(tmp_path)
    assert app.universe == {SYM: ["blofin", "mexc"]}
    app.on_bbo(bbo("mexc", 1.0000, 1.0008, 10.0))
    app.on_bbo(bbo("blofin", 1.0050, 1.0060, 1.0))          # TT edge appears → entry task
    assert SYM in app._pending_entries
    await settle()
    pos = app.book.open[0]
    assert pos.status == OPEN and pos.mode == "TT" and not app._pending_entries
    app.on_bbo(bbo("blofin", 1.0010, 1.0012, 1.0))          # converged → exit task
    app.on_bbo(bbo("mexc", 1.0009, 1.0011, 10.0))
    await settle()
    assert pos.status == CLOSED and pos.exit_reason == "convergence" and app.book.total_trades == 1
    await app.sweep_once()
    state = json.loads((tmp_path / "real_state.json").read_text())
    assert state["closed_positions"][0]["exit_reason"] == "convergence" and state["bbo"]["mode"] == "paper"
    assert state["spread_scanner"][0]["symbol"] == SYM and (tmp_path / "heartbeat_paper").exists()
    assert state["equity"] == pytest.approx(200.0 + pos.net_pnl_usd)


async def test_quote_to_tm_position_and_halt_cancels_resting(tmp_path):
    app = build_app(tmp_path)
    app.on_bbo(bbo("mexc", 1.0000, 1.0010, 10.0))
    app.on_bbo(bbo("blofin", 1.0041, 1.0061, 1.0))          # TM edge on blofin
    await settle()
    pos = app.book.open[0]
    assert pos.status == MAKER_RESTING and pos.maker_venue == "blofin" and pos.maker_rest_price == pytest.approx(1.0061)
    (tmp_path / "stop.flag").write_text("")
    await app.sweep_once()                                   # halt → cancel resting
    await settle()
    assert app.risk.halted and app.book.open == [] and app.metrics.funnel["maker_cancelled"] == 1
    app.on_bbo(bbo("blofin", 1.0041, 1.0062, 1.0))          # no new entries while halted
    assert not app._pending_entries and app.metrics.funnel["halted"] >= 1
    (tmp_path / "start.flag").write_text("")
    await app.sweep_once()
    assert not app.risk.halted


async def test_tm_fill_through_app_and_close_all(tmp_path):
    app = build_app(tmp_path)
    app.on_bbo(bbo("mexc", 1.0000, 1.0010, 10.0))
    app.on_bbo(bbo("blofin", 1.0041, 1.0061, 1.0))
    await settle()
    pos = app.book.open[0]
    await asyncio.sleep(0.005)
    app.on_bbo(bbo("blofin", 1.0061, 1.0062, 1.0, bq=100.0))   # our resting ask is lifted → hedge → OPEN
    await settle()
    assert pos.status == OPEN and pos.mode == "TM" and pos.fee_liquidity["maker"] == "maker"
    await app.handle_command("/close_all")
    await settle()
    assert pos.status == CLOSED and pos.exit_reason == "halt" and app.risk.halted


def test_legacy_guard(tmp_path):
    hb = tmp_path / "heartbeat_live"
    assert not legacy_bot_running(hb, 120.0, now=1000.0)
    hb.write_text("1")
    import os
    os.utime(hb, (1000.0, 1000.0))
    assert legacy_bot_running(hb, 120.0, now=1050.0)
    assert not legacy_bot_running(hb, 120.0, now=1200.0)


async def test_live_refuses_corrupt_state_and_paper_falls_back_to_backup(tmp_path):
    (tmp_path / "real_state.json").write_text("{not json")
    app = build_app(tmp_path)                                  # paper: no backup -> fresh start, no exception
    app.load_state()
    assert app.book.open == []
    live = build_app(tmp_path, mode="live")
    with pytest.raises(StateCorrupt):
        live.load_state()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_app.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'bbo_trader.app'`

- [ ] **Step 3: Implement `bbo_trader/venues/registry.py`**

```python
"""Build Venue bundles from the registry: a public feed + market data for every venue with an adapter,
SimVenue trading in paper mode, live trading adapters (Plan 2) via LIVE_ADAPTERS in live mode."""
from __future__ import annotations

import logging
import time
from typing import Callable

import aiohttp

from ..budget import RateBudget
from ..config import Config, VenueConfig
from ..models import BBO, Fees
from ..quotes import QuoteBoard
from .base import Venue
from .blofin import BlofinPublic, BlofinMarket
from .mexc import MexcPublic, MexcMarket
from .sim import SimVenue

log = logging.getLogger("bbo.registry")

PUBLIC_ADAPTERS: dict[str, tuple[type, type]] = {"mexc": (MexcPublic, MexcMarket), "blofin": (BlofinPublic, BlofinMarket)}
# name -> factory(cfg: VenueConfig, session, clock) -> (Trading, PrivateFeed); populated by Plan 2
LIVE_ADAPTERS: dict[str, Callable] = {}


def build_venues(cfg: Config, on_bbo: Callable[[BBO], None], board: QuoteBoard,
                 session: aiohttp.ClientSession, clock=time.time) -> dict[str, Venue]:
    out: dict[str, Venue] = {}
    for vc in cfg.venues:
        if vc.role == "off":
            continue
        if vc.name not in PUBLIC_ADAPTERS:
            log.warning("VENUE_SKIPPED %s: no public adapter yet", vc.name)
            continue
        pub_cls, mkt_cls = PUBLIC_ADAPTERS[vc.name]
        fees = Fees(vc.taker_fee_pct, vc.maker_fee_pct)
        v = Venue(vc, fees, RateBudget(vc.rate_limits), public=pub_cls(vc, on_bbo, clock=clock),
                  market=mkt_cls(session, clock=clock))
        if vc.role == "trade":
            if cfg.mode == "paper":
                sim = SimVenue(vc.name, cfg, fees, board, v.specs, clock=clock)
                v.trading, v.private = sim, sim
            else:
                if vc.name not in LIVE_ADAPTERS:
                    raise RuntimeError(f"no live trading adapter for {vc.name} (Plan 2)")
                if not vc.api_key:
                    raise RuntimeError(f"{vc.name}: API key missing for live mode")
                v.trading, v.private = LIVE_ADAPTERS[vc.name](vc, session, clock)
        out[vc.name] = v
    return out
```

- [ ] **Step 4: Implement `bbo_trader/app.py`**

```python
"""App: wires quotes → strategy → executor, runs the periodic sweep, persists state, handles operator commands."""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Awaitable, Callable

from .config import Config
from .discovery import build_universe, symbols_for_venue
from .execution import Executor
from .metrics import Metrics, CoverageWatchdog
from .models import BBO, Intent, Position, MAKER_RESTING, OPEN, EXIT_MAKER_RESTING
from .positions import PositionBook, StateStore, StateCorrupt, build_state
from .quotes import QuoteBoard
from .risk import RiskManager
from .strategy import PairEvaluator
from .venues.base import Venue
from .venues.sim import SimVenue

log = logging.getLogger("bbo.app")


class App:
    def __init__(self, cfg: Config, venues: dict[str, Venue], board: QuoteBoard, book: PositionBook,
                 risk: RiskManager, metrics: Metrics, executor: Executor, evaluator: PairEvaluator,
                 store: StateStore, telegram=None, clock=time.time):
        self.cfg, self.venues, self.board, self.book = cfg, venues, board, book
        self.risk, self.metrics, self.executor, self.evaluator = risk, metrics, executor, evaluator
        self.store, self.telegram, self.clock = store, telegram, clock
        self.universe: dict[str, list[str]] = {}
        self._pending_entries: set[str] = set()
        self._tasks: set[asyncio.Task] = set()
        self._last_state_save = 0.0
        self._scanner: list[dict] = []
        self._last_scan = 0.0
        self.watchdog = CoverageWatchdog(board, list(venues), clock=clock)
        self.running = True

    # ---- helpers ------------------------------------------------------------------
    def _spawn(self, coro: Awaitable) -> None:
        async def guard():
            try:
                await coro
            except Exception:  # noqa: BLE001
                log.exception("APP_TASK_ERROR")
        t = asyncio.get_running_loop().create_task(guard())
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)

    def equity(self) -> float:
        if self.cfg.mode == "paper":
            return self.cfg.paper_capital_per_venue * len(self.cfg.trade_venues) + self.book.total_pnl_usd
        total = sum(b.get("total", 0.0) for b in self.risk.balances.values())
        return total if total > 0 else self.cfg.paper_capital_per_venue * len(self.cfg.trade_venues)

    # ---- quote path ----------------------------------------------------------------
    def on_bbo(self, bbo: BBO) -> None:
        if not self.board.set(bbo):
            return
        v = self.venues.get(bbo.venue)
        if v is not None and isinstance(v.trading, SimVenue):
            v.trading.on_quote(bbo)
        self.on_quote(bbo.symbol)

    def on_quote(self, symbol: str) -> None:
        positions = self.book.for_symbol(symbol)
        if positions:                     # one position per symbol: a held symbol is driven, never re-evaluated
            for pos in positions:
                self._drive(pos)
            return
        if symbol in self._pending_entries or symbol not in self.universe:
            return
        intent = self.evaluator.evaluate_entry(symbol, self.equity(), len(self.book.open), self.book.resting_counts())
        if intent.kind in ("TT_ENTER", "TM_ENTER"):
            log.info("INTENT %s %s %s/%s edge=%.3f%% maker=%s", intent.kind, symbol, intent.venue_a, intent.venue_b,
                     intent.edge_pct, intent.maker_venue or "-")
            self._pending_entries.add(symbol)
            self._spawn(self._enter(intent))

    async def _enter(self, intent: Intent) -> None:
        try:
            if intent.kind == "TT_ENTER":
                await self.executor.enter_tt(intent)
            else:
                await self.executor.enter_tm(intent)
        finally:
            self._pending_entries.discard(intent.symbol)

    def _drive(self, pos: Position) -> None:
        if pos.status == MAKER_RESTING:
            it = self.evaluator.evaluate_resting(pos)
        elif pos.status in (OPEN, EXIT_MAKER_RESTING):
            it = self.evaluator.evaluate_exit(pos, self.book.resting_counts())
        else:
            return
        if it.kind == "NONE":
            return
        if it.kind == "REQUOTE":
            self._spawn(self.executor.requote(pos, it.rest_price))
        elif it.kind == "CANCEL":
            self._spawn(self.executor.cancel_maker(pos, it.reason))
        elif it.kind == "UPGRADE_TT":
            self._spawn(self.executor.upgrade_to_tt(pos))
        elif it.kind == "TT_EXIT":
            self._spawn(self.executor.exit_tt(pos, it.reason))
        elif it.kind == "TM_EXIT":
            self._spawn(self.executor.exit_tm(pos, it))

    # ---- market data / universe ---------------------------------------------------------
    async def refresh_market_data(self) -> None:
        async def one(v: Venue):
            specs = await v.market.fetch_specs()
            v.specs.clear()
            v.specs.update(specs)
            v.public.set_specs(v.specs)
            v.market.specs = v.specs
            try:
                v.volumes.clear()
                v.volumes.update(await v.market.fetch_volumes())
            except Exception as e:  # noqa: BLE001
                log.warning("VOLUMES_FAILED %s: %r", v.name, e)
            try:
                self.risk.set_funding(v.name, await v.market.fetch_funding())
            except Exception as e:  # noqa: BLE001
                log.warning("FUNDING_FAILED %s: %r", v.name, e)
        results = await asyncio.gather(*(one(v) for v in self.venues.values() if v.market), return_exceptions=True)
        for v, r in zip([v for v in self.venues.values() if v.market], results):
            if isinstance(r, Exception):
                log.warning("SPECS_FAILED %s: %r", v.name, r)
        self.apply_universe()

    def apply_universe(self) -> None:
        specs = {name: v.specs for name, v in self.venues.items()}
        self.evaluator.specs = specs
        self.evaluator.volumes = {name: v.volumes for name, v in self.venues.items()}
        self.universe = build_universe(specs, self.cfg.trade_venues, self.cfg.blocked_symbols,
                                       {v.cfg.name: v.cfg.symbol_whitelist for v in self.venues.values()})
        for name, v in self.venues.items():
            if v.public is not None and v.cfg.role == "trade":
                v.public.set_symbols(symbols_for_venue(self.universe, name))
        log.info("UNIVERSE %d symbols on >=2 trade venues", len(self.universe))

    # ---- sweep / state -------------------------------------------------------------------
    async def sweep_once(self) -> None:
        now = self.clock()
        flag = self.risk.check_flags()
        if flag == "halt":
            log.warning("HALT reason=%s — cancelling resting orders, entries paused", self.risk.halt_reason)
            await self.executor.cancel_all_resting()
        elif flag == "resume":
            log.info("RESUME entries re-enabled")
        elif flag:
            log.info("FLAG %s consumed (already in that state)", flag)
        for pos in list(self.book.open):
            self._drive(pos)
        await self.executor.retry_degraded()
        self._heartbeat(now)
        if self.book.dirty or now - self._last_state_save >= 5.0:
            await self.save_state(now)

    def _heartbeat(self, now: float) -> None:
        try:
            self.cfg.data_dir.mkdir(parents=True, exist_ok=True)
            (self.cfg.data_dir / f"heartbeat_{self.cfg.mode}").write_text(str(int(now)))
        except OSError as e:
            log.debug("heartbeat write failed: %r", e)

    def bbo_section(self, now: float) -> dict:
        return {"mode": self.cfg.mode, "universe": len(self.universe), "metrics": self.metrics.to_dict(),
                "coverage": self.board.fresh_counts(now),
                "budget": {n: v.budget.to_dict(now) for n, v in self.venues.items()},
                "resting_makers": self.book.resting_counts(), "halted": self.risk.halted,
                "pending_entries": sorted(self._pending_entries)}

    async def save_state(self, now: float) -> None:
        cash = self.equity()                     # realized only (capital + realized P&L); no mark-to-market in v1
        self.book.mark_equity(cash, now)
        if now - self._last_scan >= 1.0:
            self._scanner = self.evaluator.scan(self.universe, 100)
            self._last_scan = now
        state = build_state(self.book, equity=cash, cash=cash,
                            starting_capital=self.cfg.paper_capital_per_venue * len(self.cfg.trade_venues),
                            mode=self.cfg.mode, risk_state=self.risk.to_dict(), balances=self.risk.balances,
                            scanner=self._scanner, bbo=self.bbo_section(now), saved_at=now)
        self.book.dirty = False
        self._last_state_save = now
        await self.store.save_async(state)       # snapshot built above; serialization runs off the loop

    def load_state(self) -> None:
        try:
            d = self.store.load()
        except StateCorrupt as e:
            if self.cfg.mode == "live":
                raise                            # never start live on a corrupt file: the venues may hold its positions
            log.error("STATE_CORRUPT %s — paper mode: trying the backup", e)
            try:
                d = self.store.load_backup()
            except StateCorrupt as e2:
                log.error("STATE_CORRUPT backup unusable too (%s) — fresh start", e2)
                d = None
        if not d:
            log.info("STATE fresh start")
            return
        self.book.load(d)
        self.risk.load(d.get("risk") or {})
        log.info("STATE loaded: %d open, %d closed, pnl=$%.2f", len(self.book.open), len(self.book.closed), self.book.total_pnl_usd)

    # ---- operator commands ---------------------------------------------------------------
    async def handle_command(self, cmd: str) -> None:
        if cmd == "/stop":
            self.risk.halt("telegram")
            await self.executor.cancel_all_resting()
        elif cmd == "/start":
            self.risk.resume()
        elif cmd == "/close_all":
            self.risk.halt("close_all")
            await self.executor.cancel_all_resting()
            for pos in list(self.book.by_status(OPEN)):
                await self.executor.exit_tt(pos, "halt")
        elif cmd == "/status" and self.telegram is not None:
            await self.telegram.send(f"{self.cfg.mode} equity ${self.equity():.2f} open={len(self.book.open)} "
                                     f"trades={self.book.total_trades} pnl=${self.book.total_pnl_usd:+.2f} "
                                     f"halted={self.risk.halted} coverage={self.board.fresh_counts(self.clock())}")

    # ---- periodic loops ------------------------------------------------------------------
    async def _loop(self, interval_s: float, fn: Callable[[], Awaitable[None]]) -> None:
        while self.running:
            try:
                await fn()
            except Exception:  # noqa: BLE001
                log.exception("LOOP_ERROR %s", getattr(fn, "__name__", fn))
            await asyncio.sleep(interval_s)

    async def _refresh_balances(self) -> None:
        for v in self.venues.values():
            if v.trading is not None:
                bal = await v.trading.balance()
                self.risk.set_balance(v.name, bal.get("available", 0.0), bal.get("total", 0.0))

    async def _quote_fallback(self) -> None:
        """Open positions must never depend on WS health: pull a REST BBO for stale legs."""
        now = self.clock()
        for pos in list(self.book.open):
            for venue in (pos.venue_a, pos.venue_b):
                v = self.venues.get(venue)
                if v is None or v.market is None or self.board.fresh(venue, pos.symbol, now) is not None:
                    continue
                bbo = await v.market.fetch_bbo(pos.symbol)
                if bbo is not None:
                    self.on_bbo(bbo)

    async def _poll_telegram(self) -> None:
        if self.telegram is None:
            return
        for cmd in await self.telegram.poll_commands():
            log.info("COMMAND %s", cmd)
            await self.handle_command(cmd)

    async def _watchdog(self) -> None:
        self.watchdog.check()

    async def run(self) -> None:
        self.load_state()
        await self.refresh_market_data()
        for v in self.venues.values():
            if v.private is not None:
                v.private.set_handler(self.executor.on_order_event)
        await self._refresh_balances()
        tasks = [asyncio.create_task(v.public.run()) for v in self.venues.values() if v.public is not None]
        tasks += [asyncio.create_task(v.private.run()) for v in self.venues.values() if v.private is not None]
        tasks += [asyncio.create_task(self.metrics.sample_loop_lag()),
                  asyncio.create_task(self._loop(0.5, self.sweep_once)),
                  asyncio.create_task(self._loop(1.0, self._quote_fallback)),
                  asyncio.create_task(self._loop(30.0, self._refresh_balances)),
                  asyncio.create_task(self._loop(60.0, self._watchdog)),
                  asyncio.create_task(self._loop(3600.0, self.refresh_market_data)),
                  asyncio.create_task(self._loop(5.0, self._poll_telegram))]
        if self.telegram is not None:
            await self.telegram.send(f"BBO trader started [{self.cfg.mode}] venues={list(self.venues)} universe={len(self.universe)}")
        try:
            while self.running:
                await asyncio.sleep(0.5)
        finally:
            await self.shutdown()
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def shutdown(self) -> None:
        log.info("SHUTDOWN cancelling resting orders and saving state")
        try:
            await self.executor.cancel_all_resting()
        finally:
            await self.save_state(self.clock())
```

- [ ] **Step 5: Implement `bbo_trader/main.py`**

```python
"""Entry point: config → logging → legacy-bot guard → build everything → App.run()."""
from __future__ import annotations

import asyncio
import logging
import signal
import sys
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

import aiohttp

from .app import App
from .config import Config, load_config
from .execution import Executor
from .metrics import Metrics
from .notify import Telegram
from .positions import PositionBook, StateStore, StateCorrupt
from .quotes import QuoteBoard
from .risk import RiskManager
from .strategy import PairEvaluator
from .venues.registry import build_venues

log = logging.getLogger("bbo")


def setup_logging(data_dir: Path, mode: str) -> None:
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s %(message)s", "%Y-%m-%dT%H:%M:%S")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    root.addHandler(sh)
    data_dir.mkdir(parents=True, exist_ok=True)
    fh = RotatingFileHandler(data_dir / f"bbo_trader_{mode}.log", maxBytes=10 * 1024 * 1024, backupCount=5)
    fh.setFormatter(fmt)
    root.addHandler(fh)


def legacy_bot_running(path: Path, max_age_s: float, now: float) -> bool:
    """True when the old real_trader's heartbeat file is fresh — both bots would race for the same balances."""
    try:
        return now - path.stat().st_mtime < max_age_s
    except FileNotFoundError:
        return False


async def run(cfg: Config) -> int:
    if cfg.mode == "live" and legacy_bot_running(cfg.legacy_heartbeat_path, cfg.legacy_heartbeat_max_age_s, time.time()):
        log.critical("REFUSED: legacy bot heartbeat %s is fresh — stop realtrader.service first", cfg.legacy_heartbeat_path)
        return 2
    board = QuoteBoard(cfg.stale_quote_s, {v.name: v.staleness_override_s for v in cfg.venues if v.staleness_override_s})
    book = PositionBook()
    risk = RiskManager(cfg)
    metrics = Metrics()
    store = StateStore(cfg.data_dir / "real_state.json")
    telegram = Telegram(cfg.telegram_token, cfg.telegram_chat_id) if cfg.telegram_token else None
    async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0 (bbo-trader/1.0)"}) as session:
        app_ref: dict[str, App] = {}

        def on_bbo(bbo):
            a = app_ref.get("app")
            if a is not None:
                a.on_bbo(bbo)

        venues = build_venues(cfg, on_bbo, board, session)
        fees = {n: v.fees for n, v in venues.items()}
        evaluator = PairEvaluator(cfg, board, fees, {}, {}, risk, metrics.funnel)
        executor = Executor(cfg, venues, board, book, risk, metrics,
                            notify=(telegram.send if telegram else None))
        app = App(cfg, venues, board, book, risk, metrics, executor, evaluator, store, telegram)
        app_ref["app"] = app
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: setattr(app, "running", False))
        log.info("=== BBO trader starting [%s] venues=%s ===", cfg.mode, list(venues))
        try:
            await app.run()
        except StateCorrupt as e:
            log.critical("REFUSED: state file corrupt (%s) — inspect real_state.json / .bak, repair, then restart", e)
            return 3
    return 0


def main() -> None:
    cfg = load_config()
    setup_logging(cfg.data_dir, cfg.mode)
    sys.exit(asyncio.run(run(cfg)))


if __name__ == "__main__":
    main()
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_app.py -q`
Expected: `5 passed`

- [ ] **Step 7: Run the whole suite and commit**

Run: `.venv/bin/python -m pytest -q`
Expected: `110 passed`

```bash
git add deploy-bbo/bbo_trader/venues/registry.py deploy-bbo/bbo_trader/app.py deploy-bbo/bbo_trader/main.py deploy-bbo/tests/test_app.py
git commit -m "feat(bbo): venue registry, App wiring (quotes → strategy → executor, sweep, state), entry point"
```

---

### Task 20: Run scripts, README, first paper run

**Files:**
- Create: `deploy-bbo/start.sh`, `deploy-bbo/bbotrader.service`, `deploy-bbo/README.md`, `deploy-bbo/.env.example`

- [ ] **Step 1: Write `start.sh`**

```bash
#!/bin/bash
# Start the BBO trader. MODE=paper (default) needs no keys; MODE=live needs the venue API keys in the
# environment (see .env.example) and refuses to start while the legacy real_trader heartbeat is fresh.
set -euo pipefail
cd "$(dirname "$0")"
export MODE="${MODE:-paper}"
export DATA_DIR="${DATA_DIR:-./data}"
mkdir -p "$DATA_DIR"
exec .venv/bin/python -m bbo_trader.main
```

Run: `chmod +x start.sh`

- [ ] **Step 2: Write the systemd unit `bbotrader.service` (Tokyo, Plan 2 deploys it)**

```ini
[Unit]
Description=BBO taker/maker trader
After=network-online.target
Wants=network-online.target

[Service]
User=ubuntu
WorkingDirectory=/app/deploy-bbo
EnvironmentFile=/app/deploy-bbo/.env
Environment=DATA_DIR=/app/data_bbo
ExecStart=/app/deploy-bbo/.venv/bin/python -m bbo_trader.main
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

- [ ] **Step 3: Write `.env.example`**

```
MODE=paper
DATA_DIR=./data
# live only:
MEXC_API_KEY=
MEXC_API_SECRET=
BLOFIN_API_KEY=
BLOFIN_API_SECRET=
BLOFIN_PASSPHRASE=
TELEGRAM_TOKEN=
TELEGRAM_CHAT_ID=
```

- [ ] **Step 4: Write `README.md`**

```markdown
# BBO Trader (deploy-bbo)

Event-driven cross-venue convergence trader that needs only real-time best bid/ask from each venue.
Every quote update re-evaluates the symbol; opportunities are taken taker/taker when the spread pays for
it immediately, otherwise taker/maker (rest one leg post-only, hedge the other at market on fill).
Spec: `docs/superpowers/specs/2026-09-05-bbo-taker-maker-trader-design.md`.

## Run (paper mode, no keys)

    python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
    ./start.sh                     # MODE=paper DATA_DIR=./data

State: `data/real_state.json` (same schema as the legacy dashboard reads — run `dashboard.py` with
`DATA_DIR=<this data dir>`). Log: `data/bbo_trader_paper.log`. Flags: `data/stop.flag` halts entries and
cancels resting orders, `data/start.flag` resumes. Telegram: `/stop /start /close_all /status`.

## Tests

    .venv/bin/python -m pytest -q

## Layout

`bbo_trader/edge.py` (pure edge math) · `strategy.py` (gates → intents) · `execution.py` (TT + pegged
maker flows) · `positions.py` (state machine + state file) · `venues/` (protocols, WS runner, MEXC,
BloFin, SimVenue, registry) · `app.py` (wiring + sweep) · `main.py` (entry point).

## Config

Environment variables override `config.py` defaults; `DATA_DIR/bot_config.json` overrides both
(restart to apply). Venues, fees and rate limits: `config/venues.json`. Blocked symbols:
`config/blocked_symbols.json`.
```

- [ ] **Step 5: First paper run against live feeds (manual acceptance, ~10 minutes)**

```bash
cd deploy-bbo && MODE=paper DATA_DIR=./data ./start.sh
```

Expected within 2 minutes in the log: `UNIVERSE <n> symbols` with n ≥ 300, `FEED_COVERAGE mexc=<hundreds> blofin=<hundreds>`, `INTENT ...` lines, no `LOOP_LAG` warnings, no tracebacks. Within 10 minutes: at least one `TM_POST` or `TT_ENTER`; `data/real_state.json` updates every ≤ 5 s (`state_saved_at_ts`). Stop with Ctrl-C: expect `SHUTDOWN cancelling resting orders and saving state`.

If MEXC shows `FEED_COVERAGE mexc=0`: capture 15 s of raw frames with a probe (`aiohttp` `ws_connect` to `wss://contract.mexc.com/edge`, send one `sub.depth.full` message, print the first 10 frames) and compare with `parse_depth` in `venues/mexc.py`; same for BloFin (`books5`). Fix the parser and its fixture test — never the strategy.

- [ ] **Step 6: Commit**

```bash
git add deploy-bbo/start.sh deploy-bbo/bbotrader.service deploy-bbo/README.md deploy-bbo/.env.example
git commit -m "feat(bbo): run script, systemd unit, README, env example"
```

---

## Plan self-review (done while writing)

- **Task 1 amended after code review (2026-09-05):** MODE normalized/validated (`paper`|`live`, else `ValueError`), venue `role` validated, `shared` coerced like other bools, secrets excluded from `repr`, missing blocklist file fails closed, JSON errors name the file; venues.json must be an object and the blocklist a list of strings; `shared` must be a real boolean; Telegram token hidden from repr; MODE checked before any file I/O; 11 tests added (14 total). **Task 2 amended after code review:** `touch_notional` rejects unknown sides, `from_dict` falls back to the ISO timestamps and copies dicts (no aliasing), `NON_TERMINAL` is a frozenset; 3 tests added (7 total). **Task 3 amended after code review:** `mk_bbo` gained a `ts_exchange` kwarg and the quotes test now proves `ts_local` governs staleness, the boundary is inclusive and newer quotes overwrite (mutation-checked). **Task 4 amended after code review:** pegs must be strictly positive and strictly inside the book (`_postable`), `needs_requote` tolerance scales with the tick, `tick_decimals` from the shortest repr (rejects non-positive ticks), relative rounding epsilon, `tm_required_pct(maker_fees, taker_fees, p)`; 7 tests added incl. a deterministic peg property sweep (13 total). **Task 7 amended after code review:** `close()` is idempotent (late duplicate terminal events), `discard()` only from TT_ENTERING/MAKER_RESTING, `OPEN → CLOSED/DEGRADED` for reconciliation, equity history uses the dashboard's `t`/`v` keys (2,000 points), `StateStore` fsyncs and keeps a `.bak`, raises `StateCorrupt` instead of silently starting fresh (live refuses to start, paper falls back to the backup), non-finite floats are sanitized, `next_id` is repaired from the ids in the file, `kill_switch` mirrors the manual halt; `App.save_state` is async (serialization off the loop) and passes realized-only `cash`; the live file is never absent (hard-link `.bak`, unique tmp, lock-serialized saves, a missing file next to a backup is a torn save), non-object JSON is corrupt, CLOSED entries under `open_positions` are routed to `closed`; 4 positions tests + 1 app test added. **Task 6 amended after code review:** a 429 penalty halves capacity for non-priority callers only (hedges/closes/cancels keep the full window, per spec), unknown budget kinds raise, the window boundary is exclusive, `to_dict` clamps at 0 and reports `shared`/`penalized`; `config.py` validates rate limits (`0 <= reserve < min(orders, cancels)`, positive window) and `venues.json` carries 10% headroom; 4 budget tests + 1 config test added. **Task 5 amended after code review:** `size_pair` shrinks the larger leg with a closed-form jump (coarse/fine lot pairs no longer time out) and enforces `min_usd`; `contracts_for_usd`/`lots_floor` fail closed on non-finite inputs; new `hedge_plan` returns hedge qty + covered maker qty + residual, `excess_to_flatten` decides what residual to flatten; 2 tests added (6 total). **Task 16 amended accordingly:** `_hedge_delta` advances `hedged_qty` only by the covered maker quantity and, once the resting order is terminal, flattens residuals beyond `MAX_LEG_MISMATCH_PCT`; both `size_pair` calls pass `min_usd=cfg.min_position_usd`; 1 TM test added (11 total). **Task 7 amended before implementation (from the Task 2 review):** closed positions' dashboard dicts are cached at close time and `closed_keep` defaults to 200, so a state save never re-serializes hundreds of closed positions; 1 test added (5 total). **Task 8 amended after code review (2026-09-05):** flag files are edge-triggered and consumed on read (a Telegram `/start` really resumes; stop wins over a simultaneous start; an undeletable flag is logged and ignored, never raises), gates never raise and fail closed (unknown or quote-only venue → `venue_blocked`; live mode with a missing/stale balance cache → `balance_unknown`; `invalid` for same-venue or non-positive size), the pair win-rate gate reads outcomes inside `pair_stats_window_s` (so a route can recover) and ignores zero-P&L and `counts_as_trade=False` closes, strikes decay after `strike_decay_s` (`pair_strikes` values are `{n, ts}`; `build_state` maps them back to bare counts for the dashboard), the funding gate needs a net cost above `funding_block_min_pct` and reports dead feeds in `stale_funding`, the mismatch guard validates its thresholds, logs `MISMATCH_BLACKLIST` and records when/why, `to_dict()` is a true snapshot and `load()` tolerates corrupt or legacy values; Config gained `strike_decay_s`, `symbol_loss_pct`, `pair_stats_window_s`, `pair_min_trades`, `pair_min_win_rate`, `balance_max_age_s`, `funding_block_min_pct`; Task 19's `sweep_once` logs the no-op flag results; 4 tests added (9 total). Carry-forward notes: Task 19 must normalize `coverage` over the configured venues and reject quotes with `ts_local > now + 1 s` (`QUOTE_TS_SKEW`); Task 7 should cache closed positions' dicts so state saves do not re-serialize 500 closed positions. The code blocks above are the amended versions.

- **Spec coverage:** decisions 1–10 → Tasks 1 (registry, roles), 3/12/13 (BBO-only feeds), 9/19 (event-driven, 500 ms sweep), 16 (event-driven fills, REST fallback, TT priority, PeggedMaker for entry and exit, rate budgets with reserve, flatten ladder, degraded retry), 8/19 (manual halt via flags and Telegram, no kill switch), 14/19 (paper mode over real feeds), 7/19 (dashboard-schema state file, `DATA_DIR`), 19/20 (legacy heartbeat guard, systemd unit). Mismatch guard, funding gate, touch-depth guard, volume gate, win-rate gate, cooldowns and strikes → Tasks 8–9. Latency metrics and coverage watchdog → Task 15/19. Not in this plan by design: live adapters (Plan 2), quote-only venues and further trade venues (Plan 3), the 48 h paper soak (operational, after Task 20).
- **Placeholder scan:** none.
- **Type consistency:** every module in Tasks 3–19 was executed together against the tests shown (71 passed) before being pasted here; names match across tasks (`Intent.kind` values, `Position` fields, `Venue` bundle, `Trading` protocol).

## What comes next

- **Plan 2 — live MEXC + BloFin:** `MexcPrivate`/`MexcTrading`, `BlofinPrivate`/`BlofinTrading` (signing, post-only `type=2` / `post_only`, `externalOid`/`clientOrderId`, private WS login and order/deal channels → `OrderEvent`, amend for BloFin, hedge-mode side codes and `positionId` capture for MEXC), registration in `LIVE_ADAPTERS`, reconciliation on startup and every 60 s, venue conformance suite with live-captured fixtures, Tokyo deployment.
- **Plan 3 — more venues:** port SpreadWatch's public parsers as quote-only feeds behind `WSRunner`, scanner rows for quote-only routes, then one trade adapter per venue the user has keys for (OKX first, with its 10-pair whitelist).
