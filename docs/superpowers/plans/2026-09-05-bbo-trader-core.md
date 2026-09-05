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
├── requirements.txt                 # aiohttp (runtime only); requirements-dev.txt adds pytest, pytest-asyncio
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
printf 'aiohttp>=3.9\n' > requirements.txt
printf -- '-r requirements.txt\npytest>=8.0\npytest-asyncio>=0.23\n' > requirements-dev.txt
.venv/bin/pip install -q -r requirements-dev.txt
printf '[pytest]\nasyncio_mode = auto\ntestpaths = tests\n' > pytest.ini
printf '.venv/\n__pycache__/\n*.pyc\ndata/\n.pytest_cache/\n' > .gitignore
touch bbo_trader/__init__.py bbo_trader/venues/__init__.py tests/__init__.py
```

Expected: pip finishes without errors; `.venv/bin/python -c "import aiohttp, pytest_asyncio"` prints nothing.

- [x] **Step 2: Write the venue registry and blocked-symbol seed**

`config/venues.json` — only `mexc` and `blofin` are `trade` in this plan; every other venue is `off` until Plan 3 gives it an adapter. Fees are the SpreadWatch tables (percent per leg, verify against live accounts). Rate limits carry ~10% headroom under the documented venue limits (MEXC 20/2 s → 18, BloFin 30/10 s → 27) because our window is measured at send time and the venue's at receive time.

```json
{
  "mexc": {
    "role": "trade",
    "taker_fee_pct": 0.02,
    "maker_fee_pct": 0.0,
    "rate_limits": {
      "orders": 18,
      "cancels": 18,
      "window_s": 2.0,
      "reserve": 4,
      "shared": false
    },
    "max_topics": 30,
    "min_requote_ms": 500,
    "staleness_override_s": 5.0
  },
  "blofin": {
    "role": "trade",
    "taker_fee_pct": 0.06,
    "maker_fee_pct": 0.02,
    "rate_limits": {
      "orders": 27,
      "cancels": 27,
      "window_s": 10.0,
      "reserve": 6,
      "shared": true
    },
    "max_topics": 50,
    "min_requote_ms": 1000
  },
  "okx": {
    "role": "off",
    "taker_fee_pct": 0.05,
    "maker_fee_pct": 0.02,
    "rate_limits": {
      "orders": 60,
      "cancels": 60,
      "window_s": 2.0,
      "reserve": 6,
      "shared": false
    },
    "max_topics": 50,
    "min_requote_ms": 500,
    "symbol_whitelist": []
  },
  "gate": {
    "role": "off",
    "taker_fee_pct": 0.05,
    "maker_fee_pct": 0.02,
    "rate_limits": {
      "orders": 20,
      "cancels": 20,
      "window_s": 2.0,
      "reserve": 4,
      "shared": false
    },
    "max_topics": 25,
    "min_requote_ms": 500
  },
  "bitget": {
    "role": "off",
    "taker_fee_pct": 0.06,
    "maker_fee_pct": 0.02,
    "rate_limits": {
      "orders": 10,
      "cancels": 10,
      "window_s": 1.0,
      "reserve": 3,
      "shared": false
    },
    "max_topics": 50,
    "min_requote_ms": 500
  },
  "bingx": {
    "role": "off",
    "taker_fee_pct": 0.05,
    "maker_fee_pct": 0.02,
    "rate_limits": {
      "orders": 10,
      "cancels": 10,
      "window_s": 1.0,
      "reserve": 3,
      "shared": false
    },
    "max_topics": 200,
    "min_requote_ms": 500
  },
  "kucoin": {
    "role": "off",
    "taker_fee_pct": 0.06,
    "maker_fee_pct": 0.02,
    "rate_limits": {
      "orders": 30,
      "cancels": 30,
      "window_s": 3.0,
      "reserve": 4,
      "shared": false
    },
    "max_topics": 50,
    "min_requote_ms": 500
  },
  "htx": {
    "role": "off",
    "taker_fee_pct": 0.05,
    "maker_fee_pct": 0.02,
    "rate_limits": {
      "orders": 20,
      "cancels": 20,
      "window_s": 3.0,
      "reserve": 4,
      "shared": false
    },
    "max_topics": 30,
    "min_requote_ms": 500
  },
  "bybit": {
    "role": "off",
    "taker_fee_pct": 0.055,
    "maker_fee_pct": 0.02,
    "rate_limits": {
      "orders": 10,
      "cancels": 10,
      "window_s": 1.0,
      "reserve": 2,
      "shared": false
    },
    "max_topics": 50,
    "min_requote_ms": 500
  },
  "binance": {
    "role": "off",
    "taker_fee_pct": 0.05,
    "maker_fee_pct": 0.02,
    "rate_limits": {
      "orders": 10,
      "cancels": 10,
      "window_s": 1.0,
      "reserve": 2,
      "shared": false
    },
    "max_topics": 50,
    "min_requote_ms": 500
  },
  "hyperliquid": {
    "role": "off",
    "taker_fee_pct": 0.045,
    "maker_fee_pct": 0.015,
    "rate_limits": {
      "orders": 10,
      "cancels": 10,
      "window_s": 1.0,
      "reserve": 2,
      "shared": false
    },
    "max_topics": 0,
    "min_requote_ms": 500,
    "staleness_override_s": 10.0
  }
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


def test_max_topics_must_be_non_negative(tmp_path):
    blocked = tmp_path / "blocked.json"
    blocked.write_text(json.dumps([]))
    venues = tmp_path / "venues.json"
    env = {"DATA_DIR": str(tmp_path / "data"), "VENUES_FILE": str(venues), "BLOCKED_FILE": str(blocked)}
    venues.write_text(json.dumps({"mexc": {"role": "trade", "taker_fee_pct": 0.02, "maker_fee_pct": 0.0, "max_topics": -1}}))
    with pytest.raises(ValueError, match="max_topics must be >= 0"):
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
    coverage_floor: int = 10                  # FEED_COVERAGE_LOW below max(floor, coverage_frac × the venue's high-water mark)
    coverage_frac: float = 0.5
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
        max_topics = int(v.get("max_topics", 50))
        if max_topics < 0:
            raise ValueError(f"{name}: max_topics must be >= 0 (0 = every topic on one connection) — a negative "
                             f"value would create zero shards and silently subscribe to nothing")
        out.append(VenueConfig(
            name=name,
            role=role,
            taker_fee_pct=float(v["taker_fee_pct"]),
            maker_fee_pct=float(v["maker_fee_pct"]),
            rate_limits=limits,
            max_topics=max_topics,
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
Expected: `16 passed`

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
    stop_ref_spread_pct: float | None = None   # divergence-stop reference on the (bid_A − ask_B) basis; None until set
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
    maker_booked_qty: float = 0.0   # how much of the resting order's fill is already booked on the leg
    maker_booked_fee: float = 0.0
    pnl_adjust_usd: float = 0.0     # realized P&L from unwinds outside the booked legs (maker-fill flattens)
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
                                    "symbol_blacklist": {}, "pair_strikes": {"k": {"n": 2, "ts": 1.0}, "legacy": 1}, "pair_blacklist": {}, "halted": True},
                        balances={"mexc": {"available": 100.0}}, scanner=[], bbo={"latency": {}},
                        saved_at=1_700_000_100.0)
    for key in ("state_saved_at_ts", "cash", "equity", "open_positions", "closed_positions", "total_pnl_usd",
                "pair_stats", "blofin_risk_blacklist", "balance_cache", "spread_scanner", "dry_run",
                "kill_switch", "saved_at", "bbo", "risk", "order_audit_log", "equity_history"):
        assert key in state, key
    assert state["pair_failure_counts"] == {"k": 2, "legacy": 1}     # dashboard wants bare counts
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
    EXIT_MAKER_RESTING: {EXIT_HEDGING, TT_EXITING, OPEN, CLOSED},   # CLOSED: venue removed (paper) / reconciliation
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
    pos.net_pnl_usd = gross - pos.entry_fees_usd - pos.exit_fees_usd + pos.pnl_adjust_usd
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

- [x] **Step 1: Append the `make_cfg` helper to `tests/conftest.py`**

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

- [x] **Step 2: Write the failing tests**

`tests/test_risk.py`:

```python
import os
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
    (tmp_path / "stop.flag").write_text("")
    r.resume()                                                          # /start must not eat a stop written meanwhile
    assert r.check_flags() == "halt" and r.halted
    (tmp_path / "stop.flag").write_text("")
    assert r.check_flags() == "halt_noop" and r.halted and not (tmp_path / "stop.flag").exists()


def test_non_file_and_undeletable_flags(tmp_path, clock, caplog):
    r = RiskManager(make_cfg(tmp_path), clock)
    (tmp_path / "start.flag").mkdir()                                   # a directory is not a start flag
    (tmp_path / "stop.flag").write_text("")
    assert r.check_flags() == "halt" and r.halted                       # no exception, stop honoured
    assert r.check_flags() is None and r.halted                         # the directory does not resume the bot
    assert "FLAG_NOT_A_FILE" in caplog.text
    (tmp_path / "start.flag").rmdir()
    (tmp_path / "stop.flag").mkdir()                                    # ...but a directory named stop.flag still halts
    r.resume()
    assert r.check_flags() == "halt" and r.halted and r.halt_reason == "stop.flag"
    assert r.check_flags() is None and r.halted                         # cannot be unlinked: remembered, not re-processed
    (tmp_path / "stop.flag").rmdir()
    (tmp_path / "start.flag").write_text("")
    tmp_path.chmod(0o555)                                               # a real flag that cannot be unlinked
    try:
        assert r.check_flags() == "resume" and not r.halted             # honoured once...
        if os.geteuid() != 0:
            assert r._dead_flags                                        # (root can always unlink: guard is vacuous there)
        r.halt("telegram")
        assert r.check_flags() is None and r.halted                     # ...then ignored: it must not undo a later halt
    finally:
        tmp_path.chmod(0o755)
    os.symlink(tmp_path / "nope", tmp_path / "stop.flag")               # a dangling symlink is still a stop
    r.resume()
    assert r.check_flags() == "halt" and r.halted and not (tmp_path / "stop.flag").is_symlink()


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
    r.set_funding("mexc", {"XYZUSDT": (float("nan"), now + 300)})      # NaN would silently disable the gate
    assert r.funding["mexc|XYZUSDT"] == (0.0005, now + 300)


def test_record_close_win_rate_window_and_symbol_blacklist(tmp_path, clock):
    r = RiskManager(make_cfg(tmp_path), clock)
    key = pair_key("XYZUSDT", "blofin", "mexc")
    mk = lambda pnl: Position(1, "XYZUSDT", "blofin", "mexc", OPEN, "TT", size_usd=25.0, net_pnl_usd=pnl)
    r.record_close(mk(-0.05))                            # -0.2% of size -> 6 h symbol blacklist
    assert r.symbol_blacklist["XYZUSDT"] == clock() + 21_600.0
    r.record_close(mk(0.0))                              # zero P&L: neither win nor loss
    r.record_close(mk(0.01), counts_as_trade=False)      # reconciliation close: not a trade
    assert r.pair_stats[key]["wins"] == 0 and r.pair_stats[key]["losses"] == 1
    assert r.pair_stats[key]["total_pnl"] == pytest.approx(-0.05)
    r.symbol_blacklist.clear()
    r.record_close(mk(-0.05), counts_as_trade=False)     # ...but a real loss still blacklists the symbol
    assert "XYZUSDT" in r.symbol_blacklist and r.pair_stats[key]["losses"] == 1
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
    r.load({"pair_blacklist": {"k": "soon", "ok": now + 100},
            "pair_strikes": {"k": None, "old": 1, "immortal": {"n": 1, "ts": float("inf")}, "inf": float("inf")},
            "cooldowns": None,
            "pair_stats": {"p": {"wins": "x"}, "q": {"wins": 2, "losses": 1, "total_pnl": 0.1},
                           "r": {"wins": 1, "losses": 4, "total_pnl": float("inf"),
                                 "recent": [[now, False], [float("inf"), False], ["x", 1, 2], [now, True]]},
                           "s": {"wins": 1, "losses": 4, "recent": 5},          # non-iterable: entry dropped, route kept
                           "t": {"wins": float("inf")}},                        # int(inf): OverflowError must not escape
            "mismatch_blacklist": ["legacy|a|b"], "venue_symbol_blacklist": "notalist", "halted": 1})
    assert r.pair_blacklist == {"ok": now + 100} and r.pair_strikes == {"old": {"n": 1, "ts": now}}
    assert r.cooldowns == {} and "p" not in r.pair_stats and r.pair_stats["q"]["recent"] == []
    assert r.pair_stats["r"]["recent"] == [[now, False], [now, True]] and r.pair_stats["r"]["total_pnl"] == 0.0
    assert r.pair_stats["s"]["recent"] == [] and r.pair_stats["s"]["losses"] == 4 and "t" not in r.pair_stats
    assert r.mismatch.is_blacklisted("legacy|a|b") and r.venue_symbol_blacklist == set() and r.halted
    r.load("garbage")                                                   # not even an object: fresh state, no raise
    assert not r.halted and r.pair_blacklist == {}
```

- [x] **Step 3: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_risk.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'bbo_trader.risk'`

- [x] **Step 4: Implement `bbo_trader/risk.py`**

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
        except (TypeError, ValueError, OverflowError):   # OverflowError: int(inf) from a JSON `Infinity`
            log.warning("RISK_STATE_DROP pair_strikes %r=%r", k, v)
            continue
        if n > 0 and math.isfinite(ts) and now - ts <= decay_s:   # an inf/NaN stamp would make a strike immortal
            out[str(k)] = {"n": n, "ts": ts}
    return out


def _stats_from(raw: object) -> dict[str, dict]:
    """One bad `recent` entry drops that entry, not the route (dropping the route would unblock it)."""
    out: dict[str, dict] = {}
    if not isinstance(raw, dict):
        return out
    for k, v in raw.items():
        try:
            recent = []
            raw_recent = v.get("recent") or []
            if not isinstance(raw_recent, (list, tuple)):
                log.warning("RISK_STATE_DROP pair_stats %r recent %r", k, raw_recent)
                raw_recent = []
            for item in raw_recent:
                try:
                    ts, won = item
                    ts = float(ts)
                    if math.isfinite(ts):
                        recent.append([ts, bool(won)])
                    else:
                        log.warning("RISK_STATE_DROP pair_stats %r recent %r", k, item)
                except (TypeError, ValueError, OverflowError):
                    log.warning("RISK_STATE_DROP pair_stats %r recent %r", k, item)
            total = float(v.get("total_pnl", 0.0))
            out[str(k)] = {"wins": int(v.get("wins", 0)), "losses": int(v.get("losses", 0)),
                           "total_pnl": total if math.isfinite(total) else 0.0, "recent": recent[-RECENT_OUTCOMES_KEEP:]}
        except (TypeError, ValueError, AttributeError, OverflowError):
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
        self._consume_flags(self.cfg.halt_flag, self.cfg.resume_flag)   # stop wins: a pending start is void

    def resume(self) -> None:
        self.halted, self.halt_reason = False, ""
        self._consume_flags(self.cfg.resume_flag)   # only the start flag: a stop.flag written meanwhile is still honoured

    def _flag_path(self, name: str) -> Path:
        return self.cfg.data_dir / name

    @staticmethod
    def _signature(p: Path) -> tuple[int, int] | None:
        try:
            st = p.lstat()             # lstat: a dangling symlink named stop.flag is still a stop
        except OSError:
            return None
        return st.st_ino, st.st_mtime_ns

    def _flag_present(self, p: Path, files_only: bool) -> bool:
        """`files_only=False` (stop): any path counts — a directory named stop.flag still means stop.
        `files_only=True` (start): only a regular file resumes; anything else is logged once and ignored."""
        try:
            sig = self._signature(p)
            if sig is None:
                self._dead_flags.pop(p, None)
                return False
            if p in self._dead_flags:
                if sig == self._dead_flags[p]:
                    return False       # the very path we could not unlink: keep ignoring it
                del self._dead_flags[p]    # replaced: treat the new one as a fresh flag
            if files_only and not p.is_file():
                log.error("FLAG_NOT_A_FILE %s is not a regular file — ignored until replaced", p)
                self._dead_flags[p] = sig
                return False
            return True
        except OSError as e:
            log.error("FLAG_CHECK_FAILED %s: %s", p, e)
            return False

    def _consume_flags(self, *names: str) -> None:
        for name in names:
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
        has_stop = self._flag_present(self._flag_path(self.cfg.halt_flag), files_only=False)
        has_start = self._flag_present(self._flag_path(self.cfg.resume_flag), files_only=True)
        if not has_stop and not has_start:
            return None
        if has_stop:
            if has_start:
                log.warning("FLAGS %s and %s both present — stop wins", self.cfg.halt_flag, self.cfg.resume_flag)
            if self.halted:
                self._consume_flags(self.cfg.halt_flag, self.cfg.resume_flag)
                return "halt_noop"
            self.halt("stop.flag")
            return "halt"
        if not self.halted:
            self._consume_flags(self.cfg.resume_flag)
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
        `funding_block_s` net to a cost above `funding_block_min_pct` (percent points): the venues settle
        on 4 h / 8 h grids (MEXC `collectCycle`, BloFin `fundingInterval`) with near-identical rates, so a
        zero threshold would block half of all routes over a few thousandths of a basis point. A settle stamp already in the past means
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
        """Bookkeeping for a closed position. `counts_as_trade=False` (Plan 2 reconciliation closes) leaves the
        win-rate stats alone; a zero-P&L close is neither a win nor a loss."""
        now = self.clock()
        if counts_as_trade:
            key = pair_key(pos.symbol, pos.venue_a, pos.venue_b)
            st = self.pair_stats.setdefault(key, {"wins": 0, "losses": 0, "total_pnl": 0.0, "recent": []})
            st.setdefault("recent", [])
            st["total_pnl"] += pos.net_pnl_usd
            if pos.net_pnl_usd != 0.0:
                won = pos.net_pnl_usd > 0.0
                st["wins" if won else "losses"] += 1
                st["recent"] = (st["recent"] + [[now, won]])[-RECENT_OUTCOMES_KEEP:]
        # a real loss blacklists the symbol even when the close does not count as a trade (reconciliation)
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
                rate, settle = float(rate), float(settle)
                if not (math.isfinite(rate) and math.isfinite(settle)):
                    raise ValueError("non-finite")      # a NaN rate would silently disable the gate for this route
                self.funding[f"{venue}|{symbol}"] = (rate, settle)
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
                "stale_funding": sorted(self.stale_funding)}      # diagnostic only: recomputed, not restored by load()

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

- [x] **Step 5: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_risk.py -q`
Expected: `9 passed`

- [x] **Step 6: Commit**

```bash
git add deploy-bbo/bbo_trader/risk.py deploy-bbo/tests/test_risk.py deploy-bbo/tests/conftest.py
git commit -m "feat(bbo): risk manager — halt flags, strikes/blacklists, mismatch guard, funding gate"
```

---

### Task 9: Strategy — PairEvaluator (`strategy.py`)

**Files:**
- Create: `deploy-bbo/bbo_trader/strategy.py`
- Test: `deploy-bbo/tests/test_strategy.py`

- [x] **Step 1: Write the failing tests**

`tests/test_strategy.py`:

```python
from collections import Counter
from dataclasses import replace

import pytest

from bbo_trader.config import VenueConfig, RateLimits
from bbo_trader.models import Fees, MAKER_RESTING, OPEN, EXIT_MAKER_RESTING
from bbo_trader.positions import PositionBook
from bbo_trader.quotes import QuoteBoard
from bbo_trader.risk import RiskManager, route_key
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
    assert ev.funnel["evaluated"] == 1 and ev.funnel["volume_unknown"] == 1   # no volumes at all: once per evaluation


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
    assert ev.evaluate_entry(SYM, 200.0, 0, {}).kind == "NONE" and ev.funnel["insane"] == 1   # once per pair
    assert ev.evaluate_entry(SYM, 200.0, 0, {}).kind == "NONE"
    assert ev.funnel["mismatch_blacklisted"] == 1 and ev.funnel["mismatch"] == 1


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
    # tm exit disabled -> hold (OPEN) / cancel (a resting exit maker is never orphaned)
    ev3 = PairEvaluator(replace(cfg, tm_exit_enabled=False, max_hold_min=1e9), board, FEES, ev.specs, {}, risk, clock=clock)
    pos.status = OPEN
    assert ev3.evaluate_exit(pos, {}).reason == "hold"
    pos.status = EXIT_MAKER_RESTING
    assert ev3.evaluate_exit(pos, {}) == ev3.evaluate_exit(pos, {})
    assert ev3.evaluate_exit(pos, {}).kind == "CANCEL" and ev3.evaluate_exit(pos, {}).reason == "tm_exit_disabled"
    ev4 = PairEvaluator(replace(cfg, exit_maker_venue_policy="gate", max_hold_min=1e9), board, FEES, ev.specs, {}, risk, clock=clock)
    assert ev4.evaluate_exit(pos, {}).reason == "no_maker_venue"
    # stale while resting -> cancel (with the time stop out of the way)
    ev5 = PairEvaluator(replace(cfg, max_hold_min=1e9), board, FEES, ev.specs, {}, risk, clock=clock)
    clock.tick(5)
    assert ev5.evaluate_exit(pos, {}).reason == "stale"


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


def test_timeout_exit_fires_on_stale_quotes_and_unknown_venue_never_raises(tmp_path, clock):
    cfg, board, risk, ev = build(tmp_path, clock)
    now = clock()
    book = PositionBook()
    pos = book.new(SYM, "blofin", "mexc", OPEN, "TT", size_usd=25.0, entry_spread_pct=0.45, entry_time=now)
    assert ev.evaluate_exit(pos, {}).reason == "stale"                  # no quotes at all
    clock.tick(31 * 60)
    it = ev.evaluate_exit(pos, {})
    assert it.kind == "TT_EXIT" and it.reason == "timeout"              # a market close needs no quote
    pos.status = EXIT_MAKER_RESTING
    assert ev.evaluate_exit(pos, {}).reason == "timeout"                # the executor cancels the maker first
    # a venue that left the registry: never a KeyError, never an intent the executor cannot carry out
    ev2 = PairEvaluator(cfg, board, {"blofin": BLOFIN_FEES}, ev.specs, {}, risk, clock=clock)
    pos.status = OPEN
    assert ev2.evaluate_exit(pos, {}).reason == "venue_unknown"
    pos.status = MAKER_RESTING
    assert ev2.evaluate_resting(pos).reason == "venue_unknown"


def test_upgrade_needs_touch_depth_and_requote_waits_for_inflight(tmp_path, clock):
    cfg, board, risk, ev = build(tmp_path, clock)
    now = clock()
    book = PositionBook()
    pos = book.new(SYM, "blofin", "mexc", MAKER_RESTING, "TM", size_usd=25.0, maker_venue="blofin",
                   maker_rest_price=1.0061, maker_posted_ts=now)
    board.set(mk_bbo("blofin", SYM, 1.0060, 1.0065, bq=1.0, ts=now))       # TT edge, but a $1 bid touch
    board.set(mk_bbo("mexc", SYM, 1.0000, 1.0010, ts=now))
    it = ev.evaluate_resting(pos)
    assert it.kind != "UPGRADE_TT" and ev.funnel["upgrade_depth"] == 1
    board.set(mk_bbo("blofin", SYM, 1.0060, 1.0065, ts=now))
    assert ev.evaluate_resting(pos).kind == "UPGRADE_TT"
    pos.maker_cancel_sent = True                                           # a cancel is in flight: no upgrade either
    assert ev.evaluate_resting(pos).reason == "in_flight"
    pos.maker_cancel_sent = False
    board.set(mk_bbo("blofin", SYM, 1.0041, 1.0065, ts=now))               # peg moved by 4 ticks
    pos.requote_pending = True                                             # cancel+new requote in flight
    assert ev.evaluate_resting(pos).reason == "in_flight"
    pos.requote_pending = False
    assert ev.evaluate_resting(pos).kind == "REQUOTE"


def test_route_rule_prefers_certain_tt_over_wide_book_tm(tmp_path, clock):
    cfg = make_cfg(tmp_path)
    cfg = replace(cfg, venues=cfg.venues + (VenueConfig("gate", "trade", 0.05, 0.02, RateLimits()),))
    board = QuoteBoard(cfg.stale_quote_s)
    specs = {v: {SYM: mk_spec(v, SYM)} for v in ("mexc", "blofin", "gate")}
    ev = PairEvaluator(cfg, board, FEES, specs, {}, RiskManager(cfg, clock), clock=clock)
    now = clock()
    board.set(mk_bbo("mexc", SYM, 1.0000, 1.0005, ts=now))
    board.set(mk_bbo("gate", SYM, 1.0048, 1.0052, ts=now))                 # tight book: certain TT vs mexc
    board.set(mk_bbo("blofin", SYM, 1.0030, 1.0075, ts=now))               # 0.45 % wide book: fat-looking TM edge
    it = ev.evaluate_entry(SYM, 200.0, 0, {})
    assert it.kind == "TT_ENTER" and (it.venue_a, it.venue_b) == ("gate", "mexc")


def test_thin_tt_touch_falls_back_to_tm_and_depth_is_checked_per_side(tmp_path, clock):
    cfg, board, risk, ev = build(tmp_path, clock)
    now = clock()
    board.set(mk_bbo("blofin", SYM, 1.0050, 1.0060, bq=1.0, ts=now))       # TT pays, but selling into a $1 bid
    board.set(mk_bbo("mexc", SYM, 1.0000, 1.0008, ts=now))
    it = ev.evaluate_entry(SYM, 200.0, 0, {})
    assert it.kind == "TM_ENTER" and it.maker_venue == "blofin" and ev.funnel["tt_depth_fallback"] == 1
    assert it.spread_pct == pytest.approx((1.0060 - 1.0008) / 1.0008 * 100)   # the TM spread, not the TT one
    ev_tt = PairEvaluator(replace(cfg, tm_entry_enabled=False), board, FEES, ev.specs, {}, risk, clock=clock)
    assert ev_tt.evaluate_entry(SYM, 200.0, 0, {}).reason == "no_candidate" and ev_tt.funnel["touch_depth"] == 1
    board.set(mk_bbo("blofin", SYM, 1.0050, 1.0060, aq=1.0, ts=now))       # thin ASK on the short venue is irrelevant
    assert ev_tt.evaluate_entry(SYM, 200.0, 0, {}).kind == "TT_ENTER"
    board.set(mk_bbo("blofin", SYM, 1.0050, 1.0060, ts=now))
    board.set(mk_bbo("mexc", SYM, 1.0000, 1.0008, aq=1.0, ts=now))         # buying a $1 ask on the long venue
    assert ev_tt.evaluate_entry(SYM, 200.0, 0, {}).reason == "no_candidate" and ev_tt.funnel["touch_depth"] == 2
    board.set(mk_bbo("mexc", SYM, 1.0000, 1.0008, ts=now))
    assert ev_tt.evaluate_entry(SYM, 1000.0, 0, {}).size_usd == 25.0       # capped at max_position_usd
    # funding gate is wired: an unfavourable net inside the window blocks the route
    risk.set_funding("blofin", {SYM: (0.0001, now + 300)})
    risk.set_funding("mexc", {SYM: (0.0005, now + 300)})
    assert ev_tt.evaluate_entry(SYM, 200.0, 0, {}).reason == "no_candidate" and ev_tt.funnel["funding"] == 1
    # edge_gone timer resets when a price reappears
    book = PositionBook()
    pos = book.new(SYM, "blofin", "mexc", MAKER_RESTING, "TM", size_usd=25.0, maker_venue="blofin",
                   maker_rest_price=1.0061, maker_posted_ts=now)
    ev6 = PairEvaluator(replace(cfg, improve_ticks=1, tt_enabled=False), board, FEES, ev.specs, {}, risk, clock=clock)
    board.set(mk_bbo("blofin", SYM, 1.0064, 1.0065, ts=now))               # one-tick book: no postable price
    board.set(mk_bbo("mexc", SYM, 1.0000, 1.0010, ts=now))
    assert ev6.evaluate_resting(pos).reason == "edge_gone_wait" and pos.edge_gone_since == now
    board.set(mk_bbo("blofin", SYM, 1.0041, 1.0065, ts=now))               # price back -> timer reset
    ev6.evaluate_resting(pos)
    assert pos.edge_gone_since == 0.0
    clock.tick(0.35)
    board.set(mk_bbo("blofin", SYM, 1.0064, 1.0065, ts=clock()))
    board.set(mk_bbo("mexc", SYM, 1.0000, 1.0010, ts=clock()))
    assert ev6.evaluate_resting(pos).reason == "edge_gone_wait"            # not an immediate cancel


def test_tm_exit_hedge_depth_and_stop_uses_tt_basis(tmp_path, clock):
    cfg, board, risk, ev = build(tmp_path, clock)
    now = clock()
    book = PositionBook()
    pos = book.new(SYM, "blofin", "mexc", OPEN, "TT", size_usd=25.0, entry_spread_pct=0.45, entry_time=now)
    board.set(mk_bbo("blofin", SYM, 1.0020, 1.0030, ts=now))
    board.set(mk_bbo("mexc", SYM, 1.0000, 1.0005, bq=1.0, ts=now))         # hedge = SELL mexc into a $1 bid
    assert ev.evaluate_exit(pos, {}).reason == "hedge_depth"
    board.set(mk_bbo("mexc", SYM, 1.0000, 1.0005, ts=now))
    assert ev.evaluate_exit(pos, {}).kind == "TM_EXIT"
    # a TM-entered position: entry_spread_pct (0.75, maker basis) is one touch width above the TT basis
    tm = book.new(SYM, "blofin", "mexc", OPEN, "TM", size_usd=25.0, entry_spread_pct=0.75, entry_time=now)
    board.set(mk_bbo("blofin", SYM, 1.0050, 1.0070, ts=now))
    board.set(mk_bbo("mexc", SYM, 1.0000, 1.0005, ts=now))
    assert ev.evaluate_exit(tm, {}).kind != "TT_EXIT"
    assert tm.stop_ref_spread_pct == pytest.approx((1.0050 - 1.0005) / 1.0005 * 100)   # 0.4498 on the TT basis
    board.set(mk_bbo("blofin", SYM, 1.0210, 1.0230, ts=now))               # s_now 2.049: >= 0.45 + 1.5, < 0.75 + 1.5
    assert ev.evaluate_exit(tm, {}).reason == "stop"
    # a late first evaluation (restart, stale symbol) must not anchor the stop to an already diverged spread
    late = book.new(SYM, "blofin", "mexc", OPEN, "TM", size_usd=25.0, entry_spread_pct=0.75, entry_time=now)
    board.set(mk_bbo("blofin", SYM, 1.0500, 1.0510, ts=now))               # s_now 4.95 %
    assert ev.evaluate_exit(late, {}).reason == "stop" and late.stop_ref_spread_pct == 0.75
    # a resting exit maker with a cancel in flight gets no new intent
    tm.status, tm.maker_venue, tm.maker_rest_price, tm.maker_cancel_sent = EXIT_MAKER_RESTING, "blofin", 1.0015, True
    board.set(mk_bbo("blofin", SYM, 1.0020, 1.0030, ts=now))
    assert ev.evaluate_exit(tm, {}).reason == "in_flight"


def test_scan_skips_blacklisted_and_insane_pairs_and_sorts_by_edge(tmp_path, clock):
    cfg, board, risk, ev = build(tmp_path, clock)
    now = clock()
    for v in ("blofin", "mexc"):
        for sym in ("BIGUSDT", "WIDEUSDT", "BADUSDT", "MISUSDT"):
            ev.specs[v][sym] = mk_spec(v, sym)
    board.set(mk_bbo("blofin", SYM, 1.0050, 1.0060, ts=now))
    board.set(mk_bbo("mexc", SYM, 1.0000, 1.0008, ts=now))
    board.set(mk_bbo("blofin", "BIGUSDT", 1.0500, 1.0510, ts=now))       # richer edge -> first row
    board.set(mk_bbo("mexc", "BIGUSDT", 1.0000, 1.0008, ts=now))
    board.set(mk_bbo("blofin", "WIDEUSDT", 1.0030, 1.0090, ts=now))      # small TT spread, wide book: big TM edge
    board.set(mk_bbo("mexc", "WIDEUSDT", 1.0000, 1.0008, ts=now))
    board.set(mk_bbo("blofin", "BADUSDT", 2.0, 2.001, ts=now))           # insane 2x: never a scanner row
    board.set(mk_bbo("mexc", "BADUSDT", 1.0, 1.001, ts=now))
    board.set(mk_bbo("blofin", "MISUSDT", 1.0050, 1.0060, ts=now))
    board.set(mk_bbo("mexc", "MISUSDT", 1.0000, 1.0008, ts=now))
    risk.mismatch.blacklist(route_key("MISUSDT", "blofin", "mexc"))
    rows = ev.scan([SYM, "BIGUSDT", "WIDEUSDT", "BADUSDT", "MISUSDT"])
    assert [r["symbol"] for r in rows] == ["BIGUSDT", "WIDEUSDT", SYM]     # by edge, not by TT spread
    assert rows[1]["spread_pct"] < rows[2]["spread_pct"] and rows[1]["edge_pct"] > rows[2]["edge_pct"]
    assert rows[2]["price_short"] == 1.0050 and rows[2]["price_long"] == 1.0008


def test_halt_posts_no_new_exit_makers_and_cancels_resting_ones(tmp_path, clock):
    cfg, board, risk, ev = build(tmp_path, clock)
    now = clock()
    book = PositionBook()
    pos = book.new(SYM, "blofin", "mexc", OPEN, "TT", size_usd=25.0, entry_spread_pct=0.45, entry_time=now)
    board.set(mk_bbo("blofin", SYM, 1.0020, 1.0030, ts=now))
    board.set(mk_bbo("mexc", SYM, 1.0000, 1.0005, ts=now))
    assert ev.evaluate_exit(pos, {}).kind == "TM_EXIT"
    risk.halt("test")
    assert ev.evaluate_exit(pos, {}).reason == "hold"                  # halted: no new orders at any venue
    pos.status, pos.maker_venue, pos.maker_rest_price = EXIT_MAKER_RESTING, "blofin", 1.0015
    it = ev.evaluate_exit(pos, {})
    assert it.kind == "CANCEL" and it.reason == "halted"                # ...and resting exit makers come off
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_strategy.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'bbo_trader.strategy'`

- [x] **Step 3: Implement `bbo_trader/strategy.py`**

```python
"""PairEvaluator: turns fresh quotes into Intents.

- evaluate_entry: every ordered pair of TRADE venues with a fresh quote for the symbol; gates run
  cheapest-first and every rejection increments the funnel. Routes are ranked TT before TM, then by edge
  (spec "Route rule" + TT priority): a certain taker/taker fill beats a wider-looking maker edge, which is
  inflated by the maker venue's touch width and carries fill risk.
  A TT route whose taker legs would sweep a thin touch falls back to TM on the same pair.
- evaluate_resting: manage an entry maker order — upgrade to TT (same touch-depth gate as an entry),
  requote, cancel on TTL / edge gone / stale quotes; nothing while a cancel or requote is in flight.
- evaluate_exit: the time stop fires even on stale quotes (a market close needs no quote); TT exit
  triggers (convergence, divergence stop on the (bid_A − ask_B) basis); TM exit posting gated on the
  hedge touch; requoting. Exit makers have no TTL by design: they rest until convergence/timeout/stop
  or until the peg disappears.
- scan: best route per symbol for the dashboard's spread_scanner (trade venues only in Plan 1;
  quote-only venues join the scanner with Plan 3), skipping mismatch-blacklisted and insane pairs.
Never raises on a position whose venue left the registry: returns NONE/"venue_unknown" and logs once
(the App refuses to start live with such a position, see App.load_state).
Side effects are limited to funnel counts and the mismatch guard (entry) and the position's
current/peak spread, stop reference and edge-gone timer (resting/exit)."""
from __future__ import annotations

import logging
import time
from collections import Counter
from dataclasses import replace
from functools import cached_property
from itertools import permutations
from typing import Iterable

from .config import Config, VenueConfig
from .edge import (EdgeParams, evaluate_pair, choose_mode, maker_entry_price, maker_exit_price,
                   exit_spread_tt, needs_requote, best_fee_venue, spread_pct)
from .models import BBO, Fees, Intent, Position, VenueSpec, none, OPEN, EXIT_MAKER_RESTING
from .quotes import QuoteBoard
from .risk import RiskManager, route_key

log = logging.getLogger("bbo.strategy")

# only reachable if `fees` and `cfg.venues` disagree; mirrors the VenueConfig default rather than duplicating it
_DEFAULT_MIN_REQUOTE_MS = VenueConfig.__dataclass_fields__["min_requote_ms"].default


def raw_mid_spread_pct(qa: BBO, qb: BBO) -> float:
    """Direction-free |mid_A − mid_B| / min(mid) in percent: the mismatch guard's and sanity gate's input."""
    lo = min(qa.mid, qb.mid)
    if not lo > 0.0:
        return float("inf")
    return abs(qa.mid - qb.mid) / lo * 100.0


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
        self._venue_cfgs = {v.name: v for v in cfg.venues}
        self._warned: set[int] = set()

    @cached_property
    def params(self) -> EdgeParams:          # cfg is frozen; an evaluator is never re-configured in place
        c = self.cfg
        return EdgeParams(c.min_edge_pct, c.tm_extra_edge_pct, c.exit_spread_pct, c.slip_pct,
                          c.tt_enabled, c.tm_entry_enabled, c.maker_venue_policy)

    @cached_property
    def _tm_params(self) -> EdgeParams:
        return replace(self.params, tt_enabled=False)

    def spec(self, venue: str, symbol: str) -> VenueSpec | None:
        return self.specs.get(venue, {}).get(symbol)

    def tick(self, venue: str, symbol: str) -> float:
        s = self.spec(venue, symbol)
        return s.tick if s is not None else 0.0001

    def size_for(self, equity: float) -> float:
        return min(self.cfg.max_position_usd, equity * self.cfg.position_size_pct)

    def _min_gap_s(self, venue: str) -> float:
        vc = self._venue_cfgs.get(venue)
        return (vc.min_requote_ms if vc is not None else _DEFAULT_MIN_REQUOTE_MS) / 1000.0

    def _venue_unknown(self, pos: Position) -> bool:
        missing = {v for v in (pos.venue_a, pos.venue_b, pos.maker_venue) if v and v not in self.fees}
        if not missing:
            return False
        if pos.id not in self._warned:
            self._warned.add(pos.id)
            log.error("VENUE_UNKNOWN #%d %s: %s not in the registry — position cannot be managed",
                      pos.id, pos.symbol, sorted(missing))
        return True

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
        self.funnel["evaluated"] += 1
        size = self.size_for(equity)
        if size < cfg.min_position_usd:
            self.funnel["size_below_min"] += 1
            return none("size_below_min")
        quotes = self._fresh_trade_quotes(symbol, now)
        if len(quotes) < 2:
            self.funnel["no_pair"] += 1
            return none("no_pair")
        need = size * cfg.touch_depth_mult
        best: Intent | None = None
        best_rank: tuple[int, float] | None = None
        volume_unknown: set[str] = set()
        for qa, qb in permutations(quotes, 2):
            first = qa.venue < qb.venue                 # pair-symmetric gates count once per unordered pair
            rk = route_key(symbol, qa.venue, qb.venue)
            raw = raw_mid_spread_pct(qa, qb)
            if first and self.risk.mismatch.observe(rk, raw):
                self.funnel["mismatch_blacklisted"] += 1
            if self.risk.mismatch.is_blacklisted(rk):
                if first:
                    self.funnel["mismatch"] += 1
                continue
            if raw > cfg.max_sane_spread_pct:
                if first:
                    self.funnel["insane"] += 1
                continue
            fa, fb = self.fees[qa.venue], self.fees[qb.venue]
            pe = evaluate_pair(qa, qb, fa, fb, self.params)
            if pe.mode == "":
                self.funnel["below_edge"] += 1
                continue
            if pe.mode == "TT" and (qa.touch_notional("sell") < need or qb.touch_notional("buy") < need):
                pe = choose_mode(pe, self._tm_params)   # the taker legs would sweep a thin touch: try TM here
                if pe.mode == "":
                    self.funnel["touch_depth"] += 1
                    continue
                self.funnel["tt_depth_fallback"] += 1
            if pe.mode == "TM":
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
                if vol is None:
                    volume_unknown.add(v)                # the gate fails open, but visibly
                elif vol < cfg.min_volume_usd:
                    thin = True
                    break
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
            rank = (1 if pe.mode == "TT" else 0, pe.edge)   # TT always beats TM; edge decides within a mode
            if best_rank is None or rank > best_rank:
                best_rank = rank
                if pe.mode == "TT":
                    spread = pe.spread_tt
                else:
                    spread = pe.spread_tm_a if pe.maker_venue == qa.venue else pe.spread_tm_b
                best = Intent(kind="TT_ENTER" if pe.mode == "TT" else "TM_ENTER",
                              reason=f"edge={pe.edge:.3f}", symbol=symbol,
                              venue_a=qa.venue, venue_b=qb.venue, maker_venue=pe.maker_venue,
                              rest_price=px, size_usd=size, edge_pct=pe.edge, spread_pct=spread, ts=now)
        if volume_unknown:
            self.funnel["volume_unknown"] += 1
        if best is None:
            return none("no_candidate")
        self.funnel["candidate"] += 1
        return best

    # ---- resting entry maker ----------------------------------------------------
    def evaluate_resting(self, pos: Position) -> Intent:
        now = self.clock()
        cfg = self.cfg
        if self._venue_unknown(pos):
            return none("venue_unknown")
        qa = self.board.fresh(pos.venue_a, pos.symbol, now)
        qb = self.board.fresh(pos.venue_b, pos.symbol, now)
        base = dict(symbol=pos.symbol, venue_a=pos.venue_a, venue_b=pos.venue_b, maker_venue=pos.maker_venue, ts=now)
        if pos.requote_pending or pos.maker_cancel_sent:
            return none("in_flight")                         # the executor is mid-cancel: any new intent is moot
        if self.risk.halted:                                 # the halt edge's cancel may have failed: nothing rests while halted
            return Intent(kind="CANCEL", reason="halted", **base)
        if qa is None or qb is None:
            return Intent(kind="CANCEL", reason="stale", **base)
        if now - pos.maker_posted_ts >= cfg.maker_ttl_s:
            return Intent(kind="CANCEL", reason="ttl", **base)
        fa, fb = self.fees[pos.venue_a], self.fees[pos.venue_b]
        pe = evaluate_pair(qa, qb, fa, fb, self.params)
        if cfg.tt_enabled and pe.edge_tt >= cfg.min_edge_pct:
            need = pos.size_usd * cfg.touch_depth_mult
            if qa.touch_notional("sell") >= need and qb.touch_notional("buy") >= need:
                return Intent(kind="UPGRADE_TT", reason=f"edge_tt={pe.edge_tt:.3f}", size_usd=pos.size_usd,
                              edge_pct=pe.edge_tt, spread_pct=pe.spread_tt, **base)
            self.funnel["upgrade_depth"] += 1             # a TT edge on a thin touch: keep resting instead
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
        if (needs_requote(pos.maker_rest_price, px, self.tick(pos.maker_venue, pos.symbol), cfg.requote_ticks)
                and now - pos.maker_last_requote_ts >= self._min_gap_s(pos.maker_venue)):
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
        base = dict(symbol=pos.symbol, venue_a=pos.venue_a, venue_b=pos.venue_b, ts=now)
        resting = pos.status == EXIT_MAKER_RESTING
        if self._venue_unknown(pos):
            return none("venue_unknown")
        if now - pos.entry_time >= cfg.max_hold_min * 60.0:   # a market close on both legs needs no quote
            return Intent(kind="TT_EXIT", reason="timeout", spread_pct=pos.current_spread_pct, **base)
        qa = self.board.fresh(pos.venue_a, pos.symbol, now)
        qb = self.board.fresh(pos.venue_b, pos.symbol, now)
        if qa is None or qb is None:
            if resting:
                return Intent(kind="CANCEL", reason="stale", maker_venue=pos.maker_venue, **base)
            return none("stale")
        x = exit_spread_tt(qa, qb)
        s_now = spread_pct(qa.bid, qb.ask)
        pos.current_spread_pct = s_now
        pos.peak_spread_pct = max(pos.peak_spread_pct, s_now)
        if pos.stop_ref_spread_pct is None:
            # the stop reference must sit on the same (bid_A − ask_B) basis as s_now: a TM fill's
            # entry_spread_pct is one touch width higher and would loosen the stop by that width. For TM the
            # entry spread is the looser bound, so min() can only tighten — it protects against a late first
            # evaluation (restart, stale symbol) anchoring the stop to an already diverged spread.
            pos.stop_ref_spread_pct = pos.entry_spread_pct if pos.mode == "TT" else min(s_now, pos.entry_spread_pct)
        reason = ""
        if x <= cfg.exit_spread_pct:
            reason = "convergence"
        elif s_now >= pos.stop_ref_spread_pct + cfg.stop_pct:
            reason = "stop"
        if reason:
            return Intent(kind="TT_EXIT", reason=reason, spread_pct=x, **base)
        mv = self.exit_maker_venue(pos) if (cfg.tm_exit_enabled and not self.risk.halted) else ""
        if not mv:                                       # halted: no NEW orders at any venue, resting exit makers come off
            if resting:                                  # never orphan a resting exit maker
                why = "halted" if self.risk.halted else "tm_exit_disabled" if not cfg.tm_exit_enabled else "no_maker_venue"
                return Intent(kind="CANCEL", reason=why, maker_venue=pos.maker_venue, **base)
            return none("hold")
        px = maker_exit_price(qa, qb, cfg.exit_spread_pct, mv, self.tick(mv, pos.symbol), cfg.improve_ticks)
        if pos.status == OPEN:                           # post a take-profit maker?
            if px is None:
                return none("hold")
            hedge_touch = qb.touch_notional("sell") if mv == pos.venue_a else qa.touch_notional("buy")
            if hedge_touch < pos.size_usd * cfg.touch_depth_mult:
                return none("hedge_depth")
            if resting_counts.get(mv, 0) >= cfg.max_resting_makers_per_venue:
                return none("maker_slots")
            return Intent(kind="TM_EXIT", reason="take_profit", maker_venue=mv, rest_price=px, spread_pct=x, **base)
        if not resting:
            return none("hold")
        # EXIT_MAKER_RESTING: keep the peg current
        if pos.requote_pending or pos.maker_cancel_sent:
            return none("in_flight")
        if mv != pos.maker_venue:
            return Intent(kind="CANCEL", reason="venue_changed", maker_venue=pos.maker_venue, **base)
        if px is None:
            return Intent(kind="CANCEL", reason="edge_gone", maker_venue=pos.maker_venue, **base)
        if (needs_requote(pos.maker_rest_price, px, self.tick(mv, pos.symbol), cfg.requote_ticks)
                and now - pos.maker_last_requote_ts >= self._min_gap_s(mv)):
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
                if self.risk.mismatch.is_blacklisted(route_key(symbol, qa.venue, qb.venue)):
                    continue
                if raw_mid_spread_pct(qa, qb) > self.cfg.max_sane_spread_pct:
                    continue
                pe = evaluate_pair(qa, qb, self.fees[qa.venue], self.fees[qb.venue], self.params)
                score = max(pe.edge_tt, pe.edge_tm_a, pe.edge_tm_b)
                if best is None or score > best[0]:
                    best = (score, pe, qa, qb)
            if best is None:
                continue
            score, pe, qa, qb = best
            fees = self.fees[qa.venue].taker + self.fees[qb.venue].taker
            rows.append({"symbol": symbol, "short_exchange": qa.venue, "long_exchange": qb.venue,
                         "short_instrument": "PERP", "long_instrument": "PERP",
                         "spread_pct": round(pe.spread_tt, 4), "fees_pct": round(fees, 4),
                         "net_spread_pct": round(pe.spread_tt - fees, 4),
                         "price_short": qa.bid, "price_long": qb.ask,
                         "edge_pct": round(score, 4), "edge_tt_pct": round(pe.edge_tt, 4),
                         "edge_tm_pct": round(max(pe.edge_tm_a, pe.edge_tm_b), 4),
                         "mode": pe.mode, "is_candidate": pe.mode != ""})
        rows.sort(key=lambda r: r["edge_pct"], reverse=True)
        return rows[:limit]
```

- [x] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_strategy.py -q`
Expected: `14 passed`

- [x] **Step 5: Run the whole suite**

Run: `.venv/bin/python -m pytest -q`
Expected: `82 passed`

- [x] **Step 6: Commit**

```bash
git add deploy-bbo/bbo_trader/strategy.py deploy-bbo/tests/test_strategy.py
git commit -m "feat(bbo): PairEvaluator — multi-venue entry routing, resting-maker management, exits, scanner"
```

---

### Task 10: Venue protocols and bundle (`venues/base.py`)

**Files:**
- Create: `deploy-bbo/bbo_trader/venues/base.py`

No behaviour to test on its own (protocols only); it is exercised by Tasks 11–16. Write it exactly as below — later tasks import these names.

- [x] **Step 1: Write `bbo_trader/venues/base.py`**

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


class VenueError(RuntimeError):
    """A venue REST call failed: HTTP status, non-JSON body, or an error envelope. Venues report most errors
    as HTTP 200 plus an envelope (MEXC `{"success": false, ...}`, BloFin `{"code": "152002", ...}`), so the
    adapters check the envelope and raise this; callers treat it as UNKNOWN, never as an empty result."""


@dataclass(frozen=True)
class VenuePosition:
    venue: str
    symbol: str
    side: str          # long | short
    qty: float         # contracts (absolute)
    position_id: str = ""


class PublicFeed(Protocol):
    """Streams BBO updates for a set of symbols into the on_bbo callback it was built with.
    `set_specs` must run before quotes flow: a BBO carries the contract size of its spec, and an instrument
    without a known spec yields no BBO at all (sizes span 1e-5 … 1e7; a default would lie)."""
    async def run(self) -> None: ...
    def set_symbols(self, symbols: Iterable[str]) -> None: ...
    def set_specs(self, specs: dict[str, VenueSpec]) -> None: ...
    @property
    def connected(self) -> bool: ...


class MarketData(Protocol):
    """Public REST: contract specs, 24 h USD volumes, funding, and a BBO fallback for open positions.
    `specs` is set by `fetch_specs` and re-aliased by the App to the Venue bundle's dict; `fetch_bbo`
    uses it for the contract size."""
    specs: dict[str, VenueSpec]
    async def fetch_specs(self) -> dict[str, VenueSpec]: ...
    async def fetch_volumes(self) -> dict[str, float]: ...
    async def fetch_funding(self) -> dict[str, tuple[float, float]]: ...   # symbol -> (rate FRACTION, settle unix SECONDS)
    async def fetch_bbo(self, symbol: str) -> BBO | None: ...


class Trading(Protocol):
    """Order entry. Units and vocabulary (the executor relies on all of these):
    - `qty` is in CONTRACTS (never USD), `price` in the quote currency, `side` is "buy" | "sell"
      (a VenuePosition's `side` is "long" | "short" — a different vocabulary).
    - `OrderAck.ok` means ACCEPTED by the venue, not filled; fills arrive as OrderEvents on the PrivateFeed
      (or via `query_order`). `cancel` returns an OrderAck too: ok=False with `error` when the venue refused
      or the transport failed — the executor then queries the order and may retry.
    - `balance()` returns {"available": USDT, "total": USDT}; the risk gate reads exactly those keys."""
    supports_amend: bool
    async def place_market(self, symbol: str, side: str, qty: float, reduce_only: bool, client_id: str) -> OrderAck: ...
    async def place_post_only(self, symbol: str, side: str, qty: float, price: float, reduce_only: bool,
                              client_id: str) -> OrderAck: ...
    async def cancel(self, symbol: str, client_id: str, order_id: str) -> OrderAck: ...
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
    # shared by reference with the SimVenue and re-aliased onto `market.specs`: mutate in place (clear/update),
    # never rebind, or the paper venue silently falls back to contract size 1.0
    specs: dict[str, VenueSpec] = field(default_factory=dict)
    volumes: dict[str, float] = field(default_factory=dict)

    @property
    def name(self) -> str:
        return self.cfg.name

    @property
    def tradeable(self) -> bool:
        # fills are event-driven: without a private feed a resting maker's fill would go unnoticed until its TTL
        return self.cfg.role == "trade" and self.trading is not None and self.private is not None
```

- [x] **Step 2: Import check and commit**

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

- [x] **Step 1: Write the failing tests**

`tests/test_ws.py`:

```python
import asyncio
import json
import logging

from aiohttp import web, WSMsgType

from bbo_trader.venues.ws import Backoff, chunk, WSAdapter, WSRunner


def test_chunk_and_backoff():
    assert chunk([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4], [5]]
    assert chunk([1, 2], 0) == [[1, 2]]
    b = Backoff(healthy_s=20.0, cap_s=30.0, base_s=1.0)
    assert [b.next(0.0) for _ in range(6)] == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0]
    assert b.next(25.0) == 1.0            # a healthy socket resets the ladder
    assert b.next(0.0) == 1.0 and b.next(0.0) == 2.0


async def _serve(handler):
    app = web.Application()
    app.router.add_get("/ws", handler)
    runner = web.AppRunner(app, shutdown_timeout=0.2)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    return site._server.sockets[0].getsockname()[1], runner


def _fast_sleep(delays):
    async def sleep(d):               # records the runner's requested delays, waits (almost) nothing
        delays.append(d)
        await asyncio.sleep(0.005)
    return sleep


async def _until(pred, n=200):
    for _ in range(n):
        if pred():
            return True
        await asyncio.sleep(0.01)
    return pred()


async def _shutdown(r, task, runner):
    await r.stop()
    await asyncio.wait_for(task, 2.0)     # stop() ends run() and tears down every connection
    assert r.connections == 0
    await runner.cleanup()


async def test_runner_shards_subscribes_parses_and_ignores_non_json():
    subs, pings = [], []

    async def handler(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        first = await ws.receive()
        subs.append(json.loads(first.data))
        await ws.send_str("pong")                                  # non-JSON frame must be ignored
        await ws.send_json({"channel": "x", "v": len(subs), "symbol": "A"})
        async for m in ws:
            if m.type == WSMsgType.TEXT and m.data == "ping":
                pings.append(1)
                await ws.send_str("pong")
        return ws

    port, runner = await _serve(handler)
    items = []
    adapter = WSAdapter(name="t", url=f"ws://127.0.0.1:{port}/ws",
                        subscribe=lambda insts: [{"op": "sub", "args": insts}],
                        parse=lambda raw, state: [raw["v"]] if raw.get("channel") == "x" else [],
                        max_topics=2, ping=(0.02, "ping"))
    r = WSRunner(adapter, on_items=items.extend)
    r.set_instruments(["C", "A", "B"])
    task = asyncio.create_task(r.run())
    assert await _until(lambda: len(items) >= 2 and len(pings) >= 2)
    assert sorted(items) == [1, 2] and r.connections == 2 and r.connected
    assert {tuple(s["args"]) for s in subs} == {("A", "B"), ("C",)}
    r.set_instruments(["A", "B", "C", "D"])                        # reshard while running
    assert await _until(lambda: {tuple(s["args"]) for s in subs} >= {("A", "B"), ("C", "D")})
    await _shutdown(r, task, runner)


async def test_consumer_exception_costs_one_frame_not_the_socket(caplog):
    conns = []

    async def handler(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        conns.append(1)
        await ws.receive()
        for v in range(1, 30):
            await ws.send_json({"v": v})
            await asyncio.sleep(0.005)
        async for _ in ws:
            pass
        return ws

    port, runner = await _serve(handler)
    got = []

    def parse(raw, state):
        if raw["v"] == 2:
            raise KeyError("boom")                                # a parser / consumer bug on one frame
        return [raw["v"]]
    r = WSRunner(WSAdapter("t", f"ws://127.0.0.1:{port}/ws", lambda i: ["sub"], parse), on_items=got.extend)
    r.set_instruments(["A"])
    task = asyncio.create_task(r.run())
    with caplog.at_level(logging.ERROR, logger="bbo.ws"):
        assert await _until(lambda: 10 in got)
    assert 2 not in got and 1 in got and len(conns) == 1 and r.connected   # same socket, one quote lost
    assert "dropped frame #1" in caplog.text
    await _shutdown(r, task, runner)


async def test_quiet_but_ponging_socket_is_kept_at_shipped_defaults():
    conns = []

    async def handler(request):                                    # answers protocol pings, pushes nothing
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        conns.append(1)
        async for _ in ws:
            pass
        return ws

    port, runner = await _serve(handler)
    a = WSAdapter("t", f"ws://127.0.0.1:{port}/ws", lambda i: ["sub"], lambda raw, st: [])
    # a receive timeout under the heartbeat churns idle sockets; above it PONGs reset it so it can never fire
    assert a.receive_timeout is None and a.heartbeat == 20.0 and a.data_timeout == 120.0
    r = WSRunner(a, on_items=lambda items: None)
    r.set_instruments(["A"])
    task = asyncio.create_task(r.run())
    assert await _until(lambda: r.connected)
    await asyncio.sleep(0.5)
    assert len(conns) == 1 and r.connected                        # no receive-timeout churn on an idle healthy shard
    await _shutdown(r, task, runner)


async def test_dead_subscription_and_dead_keepalive_are_dropped_and_reconnected(caplog):
    conns = []

    async def handler(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        conns.append(1)
        await ws.receive()
        await ws.send_json({"v": len(conns)})
        async for _ in ws:                                        # socket stays alive and pongs, data stops
            pass
        return ws

    port, runner = await _serve(handler)
    delays, got = [], []
    a = WSAdapter("t", f"ws://127.0.0.1:{port}/ws", lambda i: ["sub"], lambda raw, st: [raw["v"]], data_timeout=0.1)
    r = WSRunner(a, on_items=got.extend, sleep=_fast_sleep(delays))
    r.set_instruments(["A"])
    task = asyncio.create_task(r.run())
    with caplog.at_level(logging.WARNING, logger="bbo.ws"):
        assert await _until(lambda: len(conns) >= 3)              # dead subscriptions are dropped and reopened
    assert "no data for" in caplog.text
    assert [d for d in delays if d != 0.5][:2] == [1.0, 2.0]      # the backoff ladder, not a hot loop
    await _shutdown(r, task, runner)
    conns.clear()

    async def listening(request):                                  # a venue that answers the close handshake
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        conns.append(1)
        async for _ in ws:
            pass
        return ws
    port, runner = await _serve(listening)
    a2 = WSAdapter("t", f"ws://127.0.0.1:{port}/ws", lambda i: ["sub"], lambda raw, st: [raw["v"]],
                   ping=(0.01, object()))                         # unserializable keepalive
    r2 = WSRunner(a2, on_items=got.extend, sleep=_fast_sleep([]))
    r2.set_instruments(["A"])
    task2 = asyncio.create_task(r2.run())
    with caplog.at_level(logging.WARNING, logger="bbo.ws"):
        assert await _until(lambda: len(conns) >= 2)              # a dead keepalive drops the socket visibly
    assert "keepalive failed" in caplog.text
    await _shutdown(r2, task2, runner)


async def test_server_close_reconnects_with_backoff_ladder():
    conns, delays = [], []

    async def handler(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        conns.append(1)
        await ws.receive()
        await ws.close()                                          # venue drops us right after the subscribe
        return ws

    port, runner = await _serve(handler)
    r = WSRunner(WSAdapter("t", f"ws://127.0.0.1:{port}/ws", lambda i: ["sub"], lambda raw, st: []),
                 on_items=lambda items: None, sleep=_fast_sleep(delays))
    r.set_instruments(["A"])
    task = asyncio.create_task(r.run())
    assert await _until(lambda: len(conns) >= 4)
    assert [d for d in delays if d != 0.5][:3] == [1.0, 2.0, 4.0]
    await _shutdown(r, task, runner)
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_ws.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'bbo_trader.venues.ws'`

- [x] **Step 3: Implement `bbo_trader/venues/ws.py`**

```python
"""Generic sharded WebSocket runner (ported from SpreadWatch's WSFeed, incl. its hard-won lessons):
one connection task per shard of `max_topics` instruments, uptime-keyed reconnect backoff, optional
app-level keepalive, non-JSON frames ignored, server closes logged with the socket's lifetime.

Unlike SpreadWatch's pure bookstore write, `on_items` here is the trading brain (App.on_bbo → evaluate
→ spawn), so the runner never lets a consumer or parse exception take the socket down: a bad frame
costs one quote and is logged with escalating sparsity. Liveness has two layers: aiohttp's protocol
heartbeat (default 20 s; a missing PONG drops a wedged TCP socket within ~30 s) and a data watchdog
(no frame that parsed to items for `data_timeout` → the subscription is dead even though the socket
pongs → reconnect). A failing keepalive drops the socket visibly, and teardown is bounded so a venue
that keeps streaming while we leave cannot park a shard."""
from __future__ import annotations

import asyncio
import contextlib
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
    heartbeat: float | None = 20.0                # aiohttp protocol ping; a missing PONG drops the socket in ~1.5×
    receive_timeout: float | None = None          # per-frame receive timeout — only meaningful with heartbeat=None
    #                                               (PONG frames reset it, so above the heartbeat it can never fire)
    data_timeout: float | None = 120.0            # no parsed data frame for this long → dead subscription: reconnect


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
        """Ends `run()` and tears down every connection at the next loop tick. The App cancels the run task
        instead (same effect through `run()`'s finally); `stop()` serves embedding and tests."""
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

    async def _watchdog(self, ws, conn_id: int, timeout: float, last_data: list[float]) -> None:
        """Dead-subscription detector: the socket may keep ponging while the venue stopped pushing."""
        loop = asyncio.get_running_loop()
        while True:
            await asyncio.sleep(timeout / 4.0)
            idle = loop.time() - last_data[0]
            if idle >= timeout:
                log.warning("%s conn %d no data for %.0fs — reconnecting", self.adapter.name, conn_id, idle)
                with contextlib.suppress(Exception):
                    await ws.close()
                return

    async def _pinger(self, ws, conn_id: int, interval: float, msg) -> None:
        """Keepalive. A failure must be visible AND must drop the socket: a silently dead
        pinger means MEXC kills the connection 60 s later for no logged reason."""
        try:
            while True:
                await asyncio.sleep(interval)
                await _send(ws, msg)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            log.warning("%s conn %d keepalive failed: %r", self.adapter.name, conn_id, e)
            with contextlib.suppress(Exception):
                await ws.close()

    async def _run_conn(self, conn_id: int, insts: list[str]) -> None:
        backoff = Backoff()
        a = self.adapter
        closes = bad = 0                 # cumulative for the shard: the log sparsity below self-throttles
        while not self._stop:
            opened = None
            loop = asyncio.get_running_loop()
            try:
                async with self._session_factory() as session:
                    ws = await session.ws_connect(
                        a.url, heartbeat=a.heartbeat,
                        timeout=aiohttp.ClientWSTimeout(ws_receive=a.receive_timeout, ws_close=10.0))
                    try:
                        for m in a.subscribe(insts):
                            await _send(ws, m)
                        helpers = []
                        if a.ping:
                            interval, msg = a.ping
                            helpers.append(asyncio.create_task(self._pinger(ws, conn_id, interval, msg)))
                        last_data = [loop.time()]
                        if a.data_timeout:
                            helpers.append(asyncio.create_task(self._watchdog(ws, conn_id, a.data_timeout, last_data)))
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
                                try:
                                    items = a.parse(raw, state)
                                    if items:
                                        last_data[0] = loop.time()
                                        self.on_items(items)
                                except Exception:  # noqa: BLE001 — a bad frame or a consumer
                                    bad += 1     # bug costs one quote, never the socket
                                    if bad in (1, 10, 100) or bad % 1000 == 0:
                                        log.exception("%s conn %d dropped frame #%d", a.name, conn_id, bad)
                        finally:
                            for h in helpers:
                                h.cancel()
                            for h in helpers:
                                with contextlib.suppress(asyncio.CancelledError, Exception):
                                    await h
                        closes += 1      # escalating sparsity: a venue that starts flapping hours in stays visible
                        log.log(logging.INFO if closes in (1, 10, 100) or closes % 1000 == 0 else logging.DEBUG,
                                "%s conn %d closed #%d (%d insts, lived %.0fs, %d bad frames total)%s",
                                a.name, conn_id, closes, len(insts), loop.time() - opened, bad,
                                f" — {ws.exception()!r}" if ws.exception() is not None else " by server")
                    finally:
                        # ws.close() restarts its ws_close timeout for every non-CLOSE frame,
                        # so a venue still streaming while we leave can park this task forever.
                        with contextlib.suppress(Exception):
                            await asyncio.wait_for(ws.close(), 5.0)
            except asyncio.CancelledError:
                self._connected.discard(conn_id)
                raise
            except Exception as e:  # noqa: BLE001 — any transport error → reconnect with backoff
                log.warning("%s conn %d error: %r", a.name, conn_id, e)
            self._connected.discard(conn_id)
            uptime = (loop.time() - opened) if opened is not None else 0.0
            await self._sleep(backoff.next(uptime))
```

- [x] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_ws.py -q`
Expected: `6 passed`

- [x] **Step 5: Commit**

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

- [x] **Step 1: Write the failing tests**

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
    assert b.ts_exchange == pytest.approx(1700000000.123, abs=1e-6) and b.ts_local == 42.0 and b.contract_size == 10.0
    assert mexc.parse_depth({"channel": "pong", "data": 1}, {}, 0.0) == []
    assert mexc.parse_depth({"channel": "push.depth.full", "symbol": "X_USDT", "data": {"bids": [], "asks": []}}, {}, 0.0) == []


def test_parse_specs_tickers_funding():
    specs = mexc.parse_specs({"success": True, "data": [
        {"symbol": "XYZ_USDT", "quoteCoin": "USDT", "state": 0, "contractSize": 10, "volUnit": 1, "minVol": 5, "priceUnit": 0.0001},
        {"symbol": "OLD_USDT", "quoteCoin": "USDT", "state": 1, "contractSize": 1, "volUnit": 1, "minVol": 1, "priceUnit": 0.01},
        {"symbol": "BTC_USDC", "quoteCoin": "USDC", "state": 0, "contractSize": 1, "volUnit": 1, "minVol": 1, "priceUnit": 0.1}]})
    assert list(specs) == ["XYZUSDT"]
    s = specs["XYZUSDT"]
    assert (s.instrument, s.contract_size, s.lot, s.min_qty, s.tick) == ("XYZ_USDT", 10.0, 1.0, 5.0, 0.0001)   # lot ≠ min
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


# ---- REST client + robustness (fake session; shapes frozen from live captures on 2026-09-05) ----------------
import json
import logging

from bbo_trader.venues.base import VenueError


class _Resp:
    def __init__(self, status, body):
        self.status, self._body = status, body

    async def text(self):
        return self._body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class _Session:
    def __init__(self, status=200, body="{}"):
        self.status, self.body, self.calls = status, body, []

    def get(self, url, **kw):
        self.calls.append((url, kw))
        return _Resp(self.status, self.body)


LIVE_DETAIL_ROW = {"symbol": "BTC_USDT", "quoteCoin": "USDT", "settleCoin": "USDT", "contractSize": 0.0001, "priceUnit": 0.1,
                   "volUnit": 1, "minVol": 1, "maxVol": 400000, "state": 0, "apiAllowed": True, "takerFeeRate": 0.0002,
                   "makerFeeRate": 0, "isNew": False, "isHot": True, "openingTime": 0}
LIVE_TICKER_ROW = {"contractId": 10, "symbol": "BTC_USDT", "lastPrice": 79805, "bid1": 79804.9, "ask1": 79805,
                   "volume24": 229640964, "amount24": 1828276834.31004, "fundingRate": 1.8e-05, "timestamp": 1788624278796}
LIVE_FUNDING_ROW = {"symbol": "BTC_USDT", "fundingRate": 1.8e-05, "collectCycle": 8, "nextSettleTime": 1788652800000,
                    "timestamp": 1788624281857}
LIVE_DEPTH_REST = {"success": True, "code": 0, "data": {"cts": None, "asks": [[79805, 99907, 7], [79805.1, 8556, 4]],
                                                        "bids": [[79804.9, 276151, 5], [79804.8, 9060, 2]],
                                                        "version": 41535524017, "timestamp": 1788624282021}}
LIVE_PUSH = {"symbol": "BTC_USDT", "data": {"cts": 1788624282010, "asks": [[79805, 99907, 7]], "bids": [[79804.9, 276151, 5]],
                                            "version": 41535524017}, "channel": "push.depth.full", "ts": 1788624282021}


def test_live_shapes_round_trip():
    specs = mexc.parse_specs({"success": True, "code": 0, "data": [LIVE_DETAIL_ROW]})
    assert specs["BTCUSDT"].contract_size == 0.0001 and specs["BTCUSDT"].tick == 0.1 and specs["BTCUSDT"].lot == 1.0
    assert mexc.parse_tickers({"success": True, "data": [LIVE_TICKER_ROW]}) == {"BTCUSDT": 1828276834.31004}
    assert mexc.parse_funding({"success": True, "data": [LIVE_FUNDING_ROW]}) == {"BTCUSDT": (1.8e-05, 1788652800.0)}
    b = mexc.parse_depth(LIVE_PUSH, {"BTC_USDT": 0.0001}, now=1.0)[0]
    assert b.bid == 79804.9 and b.ask_qty == 99907.0 and b.ts_exchange == pytest.approx(1788624282.010, abs=1e-6)   # cts, not ts
    assert b.touch_notional("buy") == pytest.approx(99907 * 79805 * 0.0001)
    r = mexc.parse_depth_rest(LIVE_DEPTH_REST, "BTC_USDT", 0.0001, 2.0)
    assert r.bid_qty == 276151.0 and r.ts_exchange == pytest.approx(1788624282.021, abs=1e-6) and r.contract_size == 0.0001


def test_specs_skip_api_disallowed_and_isolate_bad_rows(caplog):
    rows = [LIVE_DETAIL_ROW, dict(LIVE_DETAIL_ROW, symbol="FATCOIN_USDT", apiAllowed=False),
            dict(LIVE_DETAIL_ROW, symbol="BAD_USDT", contractSize="n/a"), dict(LIVE_DETAIL_ROW, symbol="NUL_USDT", priceUnit=None),
            "not-a-row", dict(LIVE_DETAIL_ROW, symbol="OK2_USDT")]
    with caplog.at_level(logging.WARNING, logger="bbo.mexc"):
        specs = mexc.parse_specs({"success": True, "data": rows})
    assert list(specs) == ["BTCUSDT", "OK2USDT"] and "SPEC_ROWS_DROPPED mexc 3 of 6" in caplog.text
    assert mexc.parse_tickers({"data": [dict(LIVE_TICKER_ROW, symbol="BAD_USDT", amount24=None), LIVE_TICKER_ROW, 5]}) == {"BTCUSDT": 1828276834.31004}
    assert mexc.parse_funding({"data": [dict(LIVE_FUNDING_ROW, symbol="BAD_USDT", nextSettleTime="x"), LIVE_FUNDING_ROW]}) == {"BTCUSDT": (1.8e-05, 1788652800.0)}


def test_unknown_instrument_and_malformed_frames_yield_nothing(caplog):
    assert mexc.parse_depth(LIVE_PUSH, {}, 1.0) == []                                    # no contract size → no quote
    assert mexc.parse_depth(dict(LIVE_PUSH, data=[LIVE_PUSH["data"]]), {"BTC_USDT": 1.0}, 1.0) == []
    assert mexc.parse_depth(dict(LIVE_PUSH, data="junk"), {"BTC_USDT": 1.0}, 1.0) == []
    assert mexc.parse_depth({"channel": "push.depth.full", "symbol": "BTC_USDT", "data": {"bids": [[1, 1]], "asks": []}},
                            {"BTC_USDT": 1.0}, 1.0) == []                                 # one-sided book
    assert mexc.parse_depth({"channel": "push.depth.full", "symbol": "BTC_USDT", "ts": 5000,
                             "data": {"bids": [["1.5", "2"]], "asks": [["1.6", "3"]]}}, {"BTC_USDT": 1.0}, 1.0)[0].bid == 1.5
    with pytest.raises(ValueError):                                                        # a NaN price must never become a BBO
        mexc.parse_depth({"channel": "push.depth.full", "symbol": "BTC_USDT", "ts": 5000,
                          "data": {"bids": [[float("nan"), 2]], "asks": [[1.6, 3]]}}, {"BTC_USDT": 1.0}, 1.0)
    from bbo_trader.config import VenueConfig
    got = []
    feed = mexc.MexcPublic(VenueConfig("mexc", "trade", 0.02, 0.0, max_topics=30), got.append)
    state = {}
    with caplog.at_level(logging.WARNING, logger="bbo.mexc"):
        for _ in range(12):
            assert feed._parse({"channel": "rs.error", "data": "Contract [BOGUS_USDT] not exists", "ts": 1}, state) == []
    assert state["errors"] == 12 and caplog.text.count("rs.error") == 2                  # logged at #1 and #10
    with pytest.raises(ValueError):
        mexc.to_instrument("BTCUSDC")                                                    # never map to the wrong contract
    with pytest.raises(ValueError):
        mexc.to_instrument("USDT")


async def test_rest_client_raises_on_error_envelopes_and_never_returns_an_empty_universe():
    for status, body in ((200, json.dumps({"success": False, "code": 510, "message": "request frequency"})),
                         (404, json.dumps({"success": False, "code": 404, "message": "Not Found"})),
                         (429, "Too Many Requests"), (200, "<html>Cloudflare</html>"), (200, json.dumps([1, 2])),
                         (500, json.dumps({"success": True, "code": 0, "data": [LIVE_DETAIL_ROW]}))):   # status alone must fail
        m = mexc.MexcMarket(_Session(status, body))
        with pytest.raises(VenueError):
            await m.fetch_specs()
        with pytest.raises(VenueError):
            await m.fetch_volumes()
    m = mexc.MexcMarket(_Session(200, json.dumps({"success": True, "code": 0, "data": []})))
    with pytest.raises(VenueError, match="no usable contracts"):
        await m.fetch_specs()                                    # an empty spec set must never reach the App
    m = mexc.MexcMarket(_Session(200, json.dumps({"success": True, "code": 0, "data": [LIVE_DETAIL_ROW]})), clock=lambda: 7.0)
    assert list(await m.fetch_specs()) == ["BTCUSDT"]
    m.session = _Session(200, json.dumps(LIVE_DEPTH_REST))
    b = await m.fetch_bbo("BTCUSDT")
    assert b.contract_size == 0.0001 and b.ts_local == 7.0
    url, kw = m.session.calls[0]
    assert url.endswith("/api/v1/contract/depth/BTC_USDT?limit=5") and kw["timeout"].total == 5.0 and "User-Agent" in kw["headers"]
    assert await m.fetch_bbo("NOPEUSDT") is None                 # no spec → no fabricated contract size
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_mexc_public.py -q`
Expected: FAIL with `ImportError` (no module `bbo_trader.venues.mexc`)

- [x] **Step 3: Implement `bbo_trader/venues/mexc.py`**

```python
"""MEXC USDT-M contract venue — public side: BBO from `sub.depth.full limit=5` (top level only),
REST specs / volumes / funding / depth fallback. Plan 2 adds MexcPrivate and MexcTrading here.

Verified against the live API on 2026-09-05: errors come back as HTTP 200 + {"success": false, "code",
"message"} (so `_get` checks the envelope, not just the status); `state` is 0 for every contract while
`apiAllowed` is false for ~30 live ones whose orders the API rejects; contract sizes span 1e-5 … 1e7, so an
instrument without a known size is dropped rather than given a default; `collectCycle` is 4 h for about
half the book; a bad subscription answers `{"channel": "rs.error", ...}` on an otherwise healthy socket."""
from __future__ import annotations

import json
import logging
import math
import time
from typing import Callable, Iterable

import aiohttp

from ..config import VenueConfig
from ..models import BBO, VenueSpec
from .base import VenueError
from .ws import WSAdapter, WSRunner

log = logging.getLogger("bbo.mexc")

NAME = "mexc"
WS_URL = "wss://contract.mexc.com/edge"
REST = "https://contract.mexc.com"
HEADERS = {"User-Agent": "Mozilla/5.0 (bbo-trader/1.0)"}   # Cloudflare rejects requests without a UA


def to_instrument(symbol: str) -> str:
    if len(symbol) <= 4 or not symbol.endswith("USDT"):
        raise ValueError(f"mexc: not a USDT symbol: {symbol!r}")
    return f"{symbol[:-4]}_USDT"


def to_symbol(instrument: str) -> str:
    return instrument.replace("_", "")


def subscribe(insts: list[str]) -> list[dict]:
    return [{"method": "sub.depth.full", "param": {"symbol": i, "limit": 5}} for i in insts]


def _num(x: object) -> float:
    """Strict numeric field: missing/empty/non-finite raise so the caller drops the row instead of guessing."""
    if x is None or x == "":
        raise ValueError("missing numeric field")
    f = float(x)
    if not math.isfinite(f):
        raise ValueError("non-finite numeric field")
    return f


def _top(d: dict, inst: str, contract_size: float, ts_ms: object, now: float) -> BBO | None:
    bids, asks = d.get("bids") or [], d.get("asks") or []
    if not bids or not asks:
        return None
    return BBO(NAME, to_symbol(inst), _num(bids[0][0]), _num(bids[0][1]), _num(asks[0][0]), _num(asks[0][1]),
               float(ts_ms or 0) / 1000.0, now, contract_size)


def parse_depth(raw: dict, contract_size: dict[str, float], now: float) -> list[BBO]:
    """`push.depth.full` → one BBO from the top levels. Levels are [price, contracts, order_count]. An
    instrument without a known contract size yields nothing: touch_notional would be wrong by up to 1e4×."""
    if raw.get("channel") != "push.depth.full":
        return []
    d = raw.get("data")
    if not isinstance(d, dict):
        return []
    inst = str(raw.get("symbol", ""))
    cs = contract_size.get(inst)
    if cs is None:
        return []
    b = _top(d, inst, cs, d.get("cts") or raw.get("ts"), now)     # book time when present, push time otherwise
    return [b] if b is not None else []


def parse_depth_rest(raw: dict, inst: str, contract_size: float, now: float) -> BBO | None:
    d = raw.get("data")
    if not isinstance(d, dict):
        return None
    return _top(d, inst, contract_size, d.get("timestamp") or d.get("ts"), now)


def parse_specs(raw: dict) -> dict[str, VenueSpec]:
    """`/api/v1/contract/detail` → specs for enabled, API-tradable USDT contracts. One malformed row costs
    that row, never the refresh."""
    out: dict[str, VenueSpec] = {}
    rows = raw.get("data") or []
    dropped = 0
    for c in rows:
        try:
            if c.get("quoteCoin") != "USDT" or int(c.get("state", 0) or 0) != 0 or not c.get("apiAllowed", True):
                continue
            inst = str(c.get("symbol") or "")
            if not inst.endswith("_USDT"):
                continue
            sym = to_symbol(inst)
            out[sym] = VenueSpec(NAME, sym, inst, _num(c.get("contractSize")), _num(c.get("volUnit")),
                                 _num(c.get("minVol")), _num(c.get("priceUnit")))
        except (TypeError, ValueError, AttributeError):
            dropped += 1
    if dropped:
        log.warning("SPEC_ROWS_DROPPED mexc %d of %d", dropped, len(rows))
    return out


def parse_tickers(raw: dict) -> dict[str, float]:
    """`/api/v1/contract/ticker` → 24 h turnover in USDT per symbol (`amount24`; `volume24` is contracts)."""
    out: dict[str, float] = {}
    for t in raw.get("data") or []:
        try:
            inst = str(t.get("symbol", ""))
            if not inst.endswith("_USDT"):
                continue
            out[to_symbol(inst)] = _num(t.get("amount24"))
        except (TypeError, ValueError, AttributeError):
            continue
    return out


def parse_funding(raw: dict) -> dict[str, tuple[float, float]]:
    """`/api/v1/contract/funding_rate` → symbol -> (rate FRACTION, next settlement unix SECONDS)."""
    out: dict[str, tuple[float, float]] = {}
    for f in raw.get("data") or []:
        try:
            inst = str(f.get("symbol", ""))
            if not inst.endswith("_USDT"):
                continue
            out[to_symbol(inst)] = (_num(f.get("fundingRate")), _num(f.get("nextSettleTime")) / 1000.0)
        except (TypeError, ValueError, AttributeError):
            continue
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
                      max_topics=cfg.max_topics, ping=(15.0, {"method": "ping"})),
            on_items=self._emit, session_factory=session_factory)

    def _parse(self, raw: dict, state: dict) -> list[BBO]:
        if raw.get("channel") == "rs.error":        # a rejected topic on an otherwise healthy socket
            n = state["errors"] = state.get("errors", 0) + 1
            if n in (1, 10, 100) or n % 1000 == 0:
                log.warning("mexc rs.error #%d: %.200s", n, raw.get("data"))
            return []
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
        """GET + envelope check: MEXC reports errors as HTTP 200 + {"success": false}; a swallowed error would
        look like an empty market (no contracts, no volumes) to the caller."""
        async with self.session.get(REST + path, headers=HEADERS,
                                    timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            body = await r.text()
            if r.status != 200:
                raise VenueError(f"mexc {path} HTTP {r.status}: {body[:200]}")
        try:
            d = json.loads(body)
        except ValueError as e:
            raise VenueError(f"mexc {path} non-JSON body: {body[:200]}") from e
        if not isinstance(d, dict) or d.get("success") is not True:
            raise VenueError(f"mexc {path} error envelope: {str(d)[:200]}")
        return d

    async def fetch_specs(self) -> dict[str, VenueSpec]:
        specs = parse_specs(await self._get("/api/v1/contract/detail"))
        if not specs:
            raise VenueError("mexc contract/detail returned no usable contracts")   # never hand out an empty universe
        self.specs = specs
        return self.specs

    async def fetch_volumes(self) -> dict[str, float]:
        return parse_tickers(await self._get("/api/v1/contract/ticker"))

    async def fetch_funding(self) -> dict[str, tuple[float, float]]:
        return parse_funding(await self._get("/api/v1/contract/funding_rate"))

    async def fetch_bbo(self, symbol: str) -> BBO | None:
        spec = self.specs.get(symbol)
        if spec is None:
            return None                              # no spec → no contract size → no honest quote
        raw = await self._get(f"/api/v1/contract/depth/{spec.instrument}?limit=5", timeout=5.0)
        return parse_depth_rest(raw, spec.instrument, spec.contract_size, self.clock())
```

- [x] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_mexc_public.py -q`
Expected: `8 passed`

- [x] **Step 5: Commit**

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

- [x] **Step 1: Write the failing tests**

`tests/test_blofin_public.py`:

```python
import pytest

from bbo_trader.venues import blofin


def test_instrument_mapping_and_subscribe():
    assert blofin.to_instrument("BTCUSDT") == "BTC-USDT" and blofin.to_symbol("BTC-USDT") == "BTCUSDT"
    # one message per instrument: BloFin validates a subscribe message atomically (one bad instId → nothing subscribed)
    assert blofin.subscribe(["A-USDT", "B-USDT"]) == [{"op": "subscribe", "args": [{"channel": "books5", "instId": "A-USDT"}]},
                                                      {"op": "subscribe", "args": [{"channel": "books5", "instId": "B-USDT"}]}]
    assert all(len(m["args"]) == 1 for m in blofin.subscribe([f"S{i}-USDT" for i in range(50)]))


def test_parse_books5_dict_and_list_shapes():
    raw = {"arg": {"channel": "books5", "instId": "XYZ-USDT"},
           "data": {"bids": [["1.0041", "500"], ["1.0040", "900"]], "asks": [["1.0061", "200"]], "ts": "1700000000123"}}
    out = blofin.parse_books5(raw, {"XYZ-USDT": 0.1}, now=7.0)
    assert len(out) == 1
    b = out[0]
    assert (b.symbol, b.bid, b.bid_qty, b.ask, b.ask_qty, b.contract_size) == ("XYZUSDT", 1.0041, 500.0, 1.0061, 200.0, 0.1)
    assert b.ts_exchange == pytest.approx(1700000000.123, abs=1e-6) and b.ts_local == 7.0
    raw_list = dict(raw, data=[raw["data"]])
    assert len(blofin.parse_books5(raw_list, {"XYZ-USDT": 0.1}, 0.0)) == 1
    assert blofin.parse_books5(raw_list, {}, 0.0) == []                      # unknown instrument: no contract size, no quote
    assert blofin.parse_books5({"event": "subscribe", "arg": {"channel": "books5"}}, {}, 0.0) == []
    assert blofin.parse_books5({"arg": {"channel": "trades", "instId": "X-USDT"}, "data": []}, {}, 0.0) == []


def test_parse_instruments_tickers_funding_books():
    specs = blofin.parse_instruments({"code": "0", "data": [
        {"instId": "XYZ-USDT", "contractValue": "0.1", "lotSize": "1", "minSize": "5", "tickSize": "0.0001", "state": "live"},
        {"instId": "DEAD-USDT", "contractValue": "1", "lotSize": "1", "minSize": "1", "tickSize": "0.01", "state": "suspend"},
        {"instId": "BTC-USDC", "contractValue": "1", "lotSize": "1", "minSize": "1", "tickSize": "0.1", "state": "live"}]})
    assert list(specs) == ["XYZUSDT"]
    s = specs["XYZUSDT"]
    assert (s.instrument, s.contract_size, s.lot, s.min_qty, s.tick) == ("XYZ-USDT", 0.1, 1.0, 5.0, 0.0001)   # lot ≠ min
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


# ---- REST client + robustness (fake session; shapes frozen from live captures on 2026-09-05) ----------------
import json
import logging

from bbo_trader.venues.base import VenueError


class _Resp:
    def __init__(self, status, body):
        self.status, self._body = status, body

    async def text(self):
        return self._body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class _Session:
    def __init__(self, status=200, body="{}"):
        self.status, self.body, self.calls = status, body, []

    def get(self, url, **kw):
        self.calls.append((url, kw))
        return _Resp(self.status, self.body)


LIVE_INSTRUMENT = {"instId": "BTC-USDT", "baseCurrency": "BTC", "quoteCurrency": "USDT", "contractValue": "0.001", "listTime": "1673517600000", "expireTime": "4918462709025", "maxLeverage": "150", "assetClass": "Crypto", "minSize": "0.1", "lotSize": "0.1", "tickSize": "0.1", "instType": "SWAP", "contractType": "linear", "maxLimitSize": "1000000", "maxMarketSize": "100000", "state": "live", "thresholdX": "0.02", "thresholdY": "0.01", "thresholdZ": "0.02", "settleCurrency": "USDT", "offTime": ""}   # verbatim /api/v1/market/instruments row
LIVE_TICKER = {"instId": "BTC-USDT", "last": "80032.6", "lastSize": "12", "askPrice": "80032.8", "askSize": "6416", "bidPrice": "80032.7", "bidSize": "4310", "high24h": "80163.9", "open24h": "79681.8", "low24h": "79356.4", "volCurrency24h": "1166.606", "vol24h": "1166606", "ts": "1788627625240"}   # verbatim /api/v1/market/tickers row
LIVE_FUNDING = {"instId": "HOLO-USDT", "fundingRate": "0.000077429202847014", "fundingTime": "1788638400000",
                "fundingInterval": "4", "fundingIntervalUnit": "hour", "fundingRateCap": "0.03", "fundingRateFloor": "-0.03"}
LIVE_BOOKS_REST = {"code": "0", "msg": "success", "data": [{"asks": [["79738.8", "5976"], ["79738.9", "256"]],
                                                            "bids": [["79738.7", "6316"], ["79738.6", "209"]], "ts": "1788625095610"}]}
LIVE_PUSH = {"arg": {"channel": "books5", "instId": "BTC-USDT"}, "action": "snapshot",
             "data": {"asks": [["79666.5", "1233"]], "bids": [["79666.4", "980"]], "ts": "1788599375999"}}


def test_live_shapes_round_trip():
    specs = blofin.parse_instruments({"code": "0", "msg": "success", "data": [LIVE_INSTRUMENT]})
    s = specs["BTCUSDT"]
    assert (s.contract_size, s.lot, s.min_qty, s.tick) == tuple(float(LIVE_INSTRUMENT[k]) for k in ("contractValue", "lotSize", "minSize", "tickSize"))
    assert s.contract_size == 0.001 and s.lot == 0.1                                       # BloFin's BTC: 0.001 BTC, 0.1-contract lots
    assert blofin.parse_tickers({"code": "0", "data": [LIVE_TICKER]}) == {
        "BTCUSDT": pytest.approx(float(LIVE_TICKER["volCurrency24h"]) * float(LIVE_TICKER["last"]))}   # base units × last
    assert float(LIVE_TICKER["volCurrency24h"]) == pytest.approx(float(LIVE_TICKER["vol24h"]) * float(LIVE_INSTRUMENT["contractValue"]), rel=1e-6)
    assert blofin.parse_funding({"code": "0", "data": [LIVE_FUNDING]}) == {"HOLOUSDT": (pytest.approx(7.7429202847014e-05), 1788638400.0)}
    b = blofin.parse_books5(LIVE_PUSH, {"BTC-USDT": 0.0001}, now=1.0)[0]
    assert b.bid == 79666.4 and b.ask_qty == 1233.0 and b.ts_exchange == pytest.approx(1788599375.999, abs=1e-6)
    assert b.touch_notional("buy") == pytest.approx(1233 * 79666.5 * 0.0001)
    r = blofin.parse_books_rest(LIVE_BOOKS_REST, "BTC-USDT", 0.0001, 2.0)
    assert r.bid_qty == 6316.0 and r.ts_exchange == pytest.approx(1788625095.610, abs=1e-6) and r.contract_size == 0.0001


def test_instruments_isolate_bad_rows_and_unknown_instruments_yield_nothing(caplog):
    rows = [LIVE_INSTRUMENT, dict(LIVE_INSTRUMENT, instId="BAD-USDT", contractValue="n/a"),
            dict(LIVE_INSTRUMENT, instId="NUL-USDT", tickSize=None), "not-a-row", dict(LIVE_INSTRUMENT, instId="OK2-USDT"),
            dict(LIVE_INSTRUMENT, instId="BTC-USDC", quoteCurrency="USDC"),
            dict(LIVE_INSTRUMENT, instId="SPY-USDT", assetClass="Stocks"),          # stock perp: gaps when Wall St is closed
            dict(LIVE_INSTRUMENT, instId="INV-USDT", contractType="inverse"),
            dict(LIVE_INSTRUMENT, instId="NAN-USDT", contractValue=float("nan"))]   # json.loads accepts a bare NaN
    with caplog.at_level(logging.WARNING, logger="bbo.blofin"):
        specs = blofin.parse_instruments({"code": "0", "data": rows})
    assert list(specs) == ["BTCUSDT", "OK2USDT"] and "SPEC_ROWS_DROPPED blofin 4 of 9" in caplog.text
    # the REAL subscribe ack for a KNOWN instrument carries arg.channel == books5 but no data
    assert blofin.parse_books5({"event": "subscribe", "arg": {"channel": "books5", "instId": "BTC-USDT"}}, {"BTC-USDT": 0.001}, 1.0) == []
    assert blofin.parse_books5(dict(LIVE_PUSH, action="update"), {"BTC-USDT": 0.001}, 1.0) == []   # only snapshots are a top of book
    with pytest.raises(ValueError):                                                        # a NaN price must never become a BBO
        blofin.parse_books5(dict(LIVE_PUSH, data={"bids": [[float("nan"), "1"]], "asks": [["1", "1"]], "ts": "1"}), {"BTC-USDT": 1.0}, 1.0)
    assert blofin.parse_tickers({"data": [dict(LIVE_TICKER, instId="BAD-USDT", last=None), LIVE_TICKER, 5]}) == {
        "BTCUSDT": pytest.approx(float(LIVE_TICKER["volCurrency24h"]) * float(LIVE_TICKER["last"]))}
    assert blofin.parse_funding({"data": [dict(LIVE_FUNDING, instId="BAD-USDT", fundingTime=""), LIVE_FUNDING]}) == {"HOLOUSDT": (pytest.approx(7.7429202847014e-05), 1788638400.0)}
    assert blofin.parse_books5(LIVE_PUSH, {}, 1.0) == []                                  # no contract size → no quote
    assert blofin.parse_books5(dict(LIVE_PUSH, data="junk"), {"BTC-USDT": 1.0}, 1.0) == []
    assert blofin.parse_books5(dict(LIVE_PUSH, data={"bids": [], "asks": [["1", "1"]], "ts": "1"}), {"BTC-USDT": 1.0}, 1.0) == []
    from bbo_trader.config import VenueConfig
    feed = blofin.BlofinPublic(VenueConfig("blofin", "trade", 0.06, 0.02, max_topics=50), lambda b: None)
    state = {}
    with caplog.at_level(logging.WARNING, logger="bbo.blofin"):
        for _ in range(12):
            assert feed._parse({"event": "error", "code": "60018", "msg": "Wrong URL or channel:books5,instId:BOGUS-USDT doesn't exist"}, state) == []
    assert state["errors"] == 12 and caplog.text.count("error event") == 2
    with pytest.raises(ValueError):
        blofin.to_instrument("BTCUSDC")
    with pytest.raises(ValueError):
        blofin.to_instrument("USDT")
    a = feed._runner.adapter                                       # WS wiring, verified live: no server pings, idle close ~32 s
    assert a.url == "wss://openapi.blofin.com/ws/public" and a.max_topics == 50
    assert a.ping == (25.0, "ping") and a.data_timeout == 120.0 and a.text_ping_reply is None


async def test_rest_client_raises_on_error_envelopes_and_never_returns_an_empty_universe():
    for status, body in ((200, json.dumps({"code": "152002", "msg": "Parameter instId error."})),
                         (200, json.dumps({"code": "429", "msg": "Too Many Requests"})),
                         (503, "<html>maintenance</html>"), (200, "not json"), (200, json.dumps([1])),
                         (500, json.dumps({"code": "0", "msg": "success", "data": [LIVE_INSTRUMENT]}))):   # status alone must fail
        m = blofin.BlofinMarket(_Session(status, body))
        with pytest.raises(VenueError):
            await m.fetch_specs()
        with pytest.raises(VenueError):
            await m.fetch_funding()
    m = blofin.BlofinMarket(_Session(200, json.dumps({"code": "0", "msg": "success", "data": []})))
    with pytest.raises(VenueError, match="no usable instruments"):
        await m.fetch_specs()
    m = blofin.BlofinMarket(_Session(200, json.dumps({"code": "0", "msg": "success", "data": [LIVE_INSTRUMENT]})), clock=lambda: 7.0)
    assert list(await m.fetch_specs()) == ["BTCUSDT"]
    m.session = _Session(200, json.dumps(LIVE_BOOKS_REST))
    b = await m.fetch_bbo("BTCUSDT")
    assert b.contract_size == float(LIVE_INSTRUMENT["contractValue"]) and b.ts_local == 7.0   # the spec's size, not a default
    url, kw = m.session.calls[0]
    assert url.endswith("/api/v1/market/books?instId=BTC-USDT&size=5") and kw["timeout"].total == 5.0 and "User-Agent" in kw["headers"]
    assert await m.fetch_bbo("NOPEUSDT") is None
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_blofin_public.py -q`
Expected: FAIL with `ImportError` (no module `bbo_trader.venues.blofin`)

- [x] **Step 3: Implement `bbo_trader/venues/blofin.py`**

```python
"""BloFin USDT swaps venue — public side: BBO from `books5` snapshots (top level only), REST
instruments / tickers / funding / books fallback. Plan 2 adds BlofinPrivate and BlofinTrading here.

Shapes as captured live (SpreadWatch 2026-08-23, re-verified 2026-09-05): books5 `data` is a DICT on the
WS and a one-element LIST on REST, prices/sizes are strings, keepalive is a bare text `ping` answered with
`pong`. Errors come back as HTTP 200 + {"code": "152002", "msg": ...} with no `data` (so `_get` checks
`code == "0"`). A subscribe MESSAGE is validated atomically: one unknown instId answers {"event": "error",
"code": "60012", ...} and subscribes NONE of the other args in that message while the socket stays open —
hence one message per instrument. `fundingTime` is the UPCOMING settlement and `fundingInterval` is 4 or
8 hours (three 1 h contracts); every instrument is `state: live` today (delistings simply vanish from the
list), every row is `instType: SWAP` (`expireTime` is a far-future per-instrument value, 2028…2126, not a
settlement date), contract values span 1e-4 … 1e7, and the USDT set includes stock/index/commodity perps
(`assetClass` Stocks/Indices/Commodities), which are excluded: their underlying markets close."""
from __future__ import annotations

import json
import logging
import math
import time
from typing import Callable, Iterable

import aiohttp

from ..config import VenueConfig
from ..models import BBO, VenueSpec
from .base import VenueError
from .ws import WSAdapter, WSRunner

log = logging.getLogger("bbo.blofin")

NAME = "blofin"
WS_URL = "wss://openapi.blofin.com/ws/public"
REST = "https://openapi.blofin.com"
HEADERS = {"User-Agent": "Mozilla/5.0 (bbo-trader/1.0)"}


def to_instrument(symbol: str) -> str:
    if len(symbol) <= 4 or not symbol.endswith("USDT"):
        raise ValueError(f"blofin: not a USDT symbol: {symbol!r}")
    return f"{symbol[:-4]}-USDT"


def to_symbol(instrument: str) -> str:
    return instrument.replace("-", "")


def subscribe(insts: list[str]) -> list[dict]:
    """One message per instrument (verified live 2026-09-05): BloFin validates a subscribe message atomically,
    so a single delisted instId in a batched message would black out the whole shard — silently, because
    our own text pings keep the empty socket alive."""
    return [{"op": "subscribe", "args": [{"channel": "books5", "instId": i}]} for i in insts]


def _num(x: object) -> float:
    """Strict numeric field: missing/empty/non-finite raise so the caller drops the row instead of guessing."""
    if x is None or x == "":
        raise ValueError("missing numeric field")
    f = float(x)
    if not math.isfinite(f):
        raise ValueError("non-finite numeric field")
    return f


def _rows(data) -> list[dict]:
    if isinstance(data, dict):
        return [data]
    return [d for d in (data or []) if isinstance(d, dict)]


def _top(d: dict, inst: str, contract_size: float, now: float) -> BBO | None:
    bids, asks = d.get("bids") or [], d.get("asks") or []
    if not bids or not asks:
        return None
    return BBO(NAME, to_symbol(inst), _num(bids[0][0]), _num(bids[0][1]), _num(asks[0][0]), _num(asks[0][1]),
               float(d.get("ts") or 0) / 1000.0, now, contract_size)


def parse_books5(raw: dict, contract_size: dict[str, float], now: float) -> list[BBO]:
    """books5 push → one BBO per row. An instrument without a known contract size yields nothing. books5 is
    snapshot-only; the sibling `books` channel sends `action: "update"` deltas with partial sides, so anything
    but a snapshot is ignored rather than read as a top of book."""
    if (raw.get("arg") or {}).get("channel") != "books5" or "data" not in raw:
        return []
    if raw.get("action") not in (None, "snapshot"):
        return []
    inst = str(raw["arg"].get("instId", ""))
    cs = contract_size.get(inst)
    if cs is None:
        return []
    out = []
    for d in _rows(raw["data"]):
        b = _top(d, inst, cs, now)
        if b is not None:
            out.append(b)
    return out


def parse_books_rest(raw: dict, inst: str, contract_size: float, now: float) -> BBO | None:
    rows = _rows(raw.get("data"))
    return _top(rows[0], inst, contract_size, now) if rows else None


def parse_instruments(raw: dict) -> dict[str, VenueSpec]:
    """`/api/v1/market/instruments` → specs for live USDT swaps. One malformed row costs that row only."""
    out: dict[str, VenueSpec] = {}
    rows = raw.get("data") or []
    dropped = 0
    for c in rows:
        try:
            inst = str(c.get("instId", ""))
            if not inst.endswith("-USDT") or str(c.get("state", "live")) != "live":
                continue
            if c.get("contractType", "linear") != "linear" or c.get("assetClass", "Crypto") != "Crypto":
                continue        # inverse contracts size the other way; equity/commodity perps gap when their market is closed
            sym = to_symbol(inst)
            out[sym] = VenueSpec(NAME, sym, inst, _num(c.get("contractValue")), _num(c.get("lotSize")),
                                 _num(c.get("minSize")), _num(c.get("tickSize")))
        except (TypeError, ValueError, AttributeError):
            dropped += 1
    if dropped:
        log.warning("SPEC_ROWS_DROPPED blofin %d of %d", dropped, len(rows))
    return out


def parse_tickers(raw: dict) -> dict[str, float]:
    """`/api/v1/market/tickers` → 24 h USD volume ≈ volCurrency24h (base units) × last."""
    out: dict[str, float] = {}
    for t in raw.get("data") or []:
        try:
            inst = str(t.get("instId", ""))
            if not inst.endswith("-USDT"):
                continue
            out[to_symbol(inst)] = _num(t.get("volCurrency24h")) * _num(t.get("last"))
        except (TypeError, ValueError, AttributeError):
            continue
    return out


def parse_funding(raw: dict) -> dict[str, tuple[float, float]]:
    """`/api/v1/market/funding-rate` → symbol -> (rate FRACTION, upcoming settlement unix SECONDS)."""
    out: dict[str, tuple[float, float]] = {}
    for f in raw.get("data") or []:
        try:
            inst = str(f.get("instId", ""))
            if not inst.endswith("-USDT"):
                continue
            out[to_symbol(inst)] = (_num(f.get("fundingRate")), _num(f.get("fundingTime")) / 1000.0)
        except (TypeError, ValueError, AttributeError):
            continue
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
                      max_topics=cfg.max_topics, ping=(25.0, "ping")),
            on_items=self._emit, session_factory=session_factory)

    def _parse(self, raw: dict, state: dict) -> list[BBO]:
        if raw.get("event") == "error":              # the whole subscribe message was refused; the socket stays open
            n = state["errors"] = state.get("errors", 0) + 1
            if n in (1, 10, 100) or n % 1000 == 0:
                log.warning("blofin error event #%d: code=%s %.200s", n, raw.get("code"), raw.get("msg"))
            return []
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
        """GET + envelope check: BloFin reports errors as HTTP 200 + {"code": "<non-zero>", "msg"} without `data`."""
        async with self.session.get(REST + path, headers=HEADERS,
                                    timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            body = await r.text()
            if r.status != 200:
                raise VenueError(f"blofin {path} HTTP {r.status}: {body[:200]}")
        try:
            d = json.loads(body)
        except ValueError as e:
            raise VenueError(f"blofin {path} non-JSON body: {body[:200]}") from e
        if not isinstance(d, dict) or str(d.get("code")) != "0":
            raise VenueError(f"blofin {path} error envelope: {str(d)[:200]}")
        return d

    async def fetch_specs(self) -> dict[str, VenueSpec]:
        specs = parse_instruments(await self._get("/api/v1/market/instruments"))
        if not specs:
            raise VenueError("blofin market/instruments returned no usable instruments")
        self.specs = specs
        return self.specs

    async def fetch_volumes(self) -> dict[str, float]:
        return parse_tickers(await self._get("/api/v1/market/tickers"))

    async def fetch_funding(self) -> dict[str, tuple[float, float]]:
        return parse_funding(await self._get("/api/v1/market/funding-rate"))

    async def fetch_bbo(self, symbol: str) -> BBO | None:
        spec = self.specs.get(symbol)
        if spec is None:
            return None
        raw = await self._get(f"/api/v1/market/books?instId={spec.instrument}&size=5", timeout=5.0)
        return parse_books_rest(raw, spec.instrument, spec.contract_size, self.clock())
```

- [x] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_blofin_public.py -q`
Expected: `7 passed`

- [x] **Step 5: Commit**

```bash
git add deploy-bbo/bbo_trader/venues/blofin.py deploy-bbo/tests/test_blofin_public.py
git commit -m "feat(bbo): BloFin public adapter — books5 BBO feed, instruments, tickers, funding"
```

---

### Task 14: Paper venue (`venues/sim.py`)

**Files:**
- Create: `deploy-bbo/bbo_trader/venues/sim.py`
- Test: `deploy-bbo/tests/test_sim.py`, `deploy-bbo/tests/test_venue_protocols.py`

- [x] **Step 1: Write the failing tests**

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
    assert not (await sim.cancel(SYM, "m1", ack.order_id)).ok                  # already terminal
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
    assert (await sim.cancel(SYM, "m1", ack.order_id)).ok
    assert events[-1].state == "canceled" and events[-1].filled_qty == 0.0
    assert await sim.open_orders() == [] and await sim.query_order(SYM, "zzz", "") is None


async def test_taker_fills_at_the_touch_that_exists_after_the_latency(tmp_path, clock):
    cfg, board, sim, events = build(tmp_path, clock)
    board.set(mk_bbo("blofin", SYM, 1.0000, 1.0010, ts=clock()))
    ack = await sim.place_market(SYM, "buy", 2.0, False, "c1")
    assert ack.ok and events[-1].state == "ack"                         # live venues push new/ack first
    board.set(mk_bbo("blofin", SYM, 1.0100, 1.0110, ts=clock()))        # the market gaps during the latency
    await asyncio.sleep(0.02)
    assert events[-1].state == "filled" and events[-1].avg_price == pytest.approx(1.0110 * 1.0002)   # spread decay is real
    clock.tick(5)                                                        # board stale -> no fills from fantasy prices
    assert (await sim.place_market(SYM, "sell", 1.0, False, "c2")).error == "no quote"
    assert (await sim.place_post_only(SYM, "sell", 1.0, 1.02, False, "m9")).error == "no quote"


async def test_maker_cap_is_per_distinct_touch_and_sub_lot_touch_fills_nothing(tmp_path, clock):
    cfg, board, sim, events = build(tmp_path, clock)
    board.set(mk_bbo("blofin", SYM, 1.0041, 1.0061, ts=clock()))
    await sim.place_post_only(SYM, "sell", 100.0, 1.0061, False, "m1")
    clock.tick(0.01)
    for _ in range(10):                                                  # an unchanged book re-pushed 10x is not new flow
        sim.on_quote(mk_bbo("blofin", SYM, 1.0061, 1.0062, bq=20.0, ts=clock()))
    assert events[-1].state == "partial" and events[-1].filled_qty == 10.0
    sim.on_quote(mk_bbo("blofin", SYM, 1.0061, 1.0062, bq=0.5, ts=clock()))      # sub-lot touch: nothing
    assert events[-1].filled_qty == 10.0
    sim.on_quote(mk_bbo("blofin", SYM, 1.0061, 1.0062, bq=30.0, ts=clock()))     # a new touch: 15 more
    assert events[-1].filled_qty == 25.0
    sim.on_quote(mk_bbo("mexc", SYM, 1.0070, 1.0071, bq=1000.0, ts=clock()))     # another venue's quote is ignored
    assert events[-1].filled_qty == 25.0
    assert sim._resting[SYM] and len(sim._orders) == 1
    await sim.cancel(SYM, "m1", "sim-1")
    assert sim._resting.get(SYM) == {} and await sim.open_orders() == []


async def test_reduce_only_is_clamped_and_rejected_when_flat(tmp_path, clock):
    cfg, board, sim, events = build(tmp_path, clock)
    board.set(mk_bbo("blofin", SYM, 1.0000, 1.0010, ts=clock()))
    await sim.place_market(SYM, "buy", 5.0, False, "c1")
    await asyncio.sleep(0.02)
    await sim.place_market(SYM, "sell", 8.0, True, "c2")                # reduce-only 8 against a long 5
    await asyncio.sleep(0.02)
    assert events[-1].state == "filled" and events[-1].filled_qty == 5.0 and await sim.positions() == []
    await sim.place_market(SYM, "sell", 3.0, True, "c3")                # nothing left to reduce
    await asyncio.sleep(0.02)
    assert events[-1].state == "rejected" and events[-1].error == "nothing to reduce" and await sim.positions() == []


async def test_realized_pnl_and_fees_flow_into_balance(tmp_path, clock):
    cfg, board, sim, events = build(tmp_path, clock)
    board.set(mk_bbo("blofin", SYM, 1.0000, 1.0010, ts=clock()))
    await sim.place_market(SYM, "buy", 2.0, False, "c1")                # 2 x 10 contracts at 1.0010*(1+2bps)
    await asyncio.sleep(0.02)
    entry = events[-1].avg_price
    board.set(mk_bbo("blofin", SYM, 2.0000, 2.0010, ts=clock()))
    await sim.place_market(SYM, "sell", 2.0, True, "c2")
    await asyncio.sleep(0.02)
    exit_px = events[-1].avg_price
    fees = sum(e.fee for e in events if e.state == "filled")
    assert (await sim.balance())["available"] == pytest.approx(100.0 + (exit_px - entry) * 2.0 * 10.0 - fees)


async def test_cancel_inside_latency_prevents_the_fill_and_amend_resets_the_clock(tmp_path, clock):
    cfg, board, sim, events = build(tmp_path, clock)
    board.set(mk_bbo("blofin", SYM, 1.0000, 1.0010, ts=clock()))
    ack = await sim.place_market(SYM, "buy", 2.0, False, "c1")
    assert (await sim.cancel(SYM, "c1", ack.order_id)).ok
    await asyncio.sleep(0.02)
    assert events[-1].state == "canceled" and await sim.positions() == []
    assert (await sim.cancel(SYM, "nope", "x")).error == "unknown order"
    assert not (await sim.place_post_only(SYM, "sell", 0.0, 1.0010, False, "m0")).ok       # qty guard
    ack = await sim.place_post_only(SYM, "sell", 4.0, 1.0012, False, "m1")
    clock.tick(0.01)
    assert (await sim.amend(SYM, "m1", ack.order_id, 1.0011)).ok
    sim.on_quote(mk_bbo("blofin", SYM, 1.0011, 1.0013, bq=100.0, ts=clock()))              # inside the new latency window
    assert events[-1].state == "ack"
    clock.tick(0.01)
    sim.on_quote(mk_bbo("blofin", SYM, 1.0011, 1.0013, bq=100.0, ts=clock()))
    assert events[-1].state == "filled" and events[-1].avg_price == 1.0011


async def test_flip_realizes_pnl_on_the_closed_part_and_reanchors_the_average(tmp_path, clock):
    cfg, board, sim, events = build(tmp_path, clock)
    board.set(mk_bbo("blofin", SYM, 1.0000, 1.0000, ts=clock()))            # zero-width book: fills at the touch
    sim.slip = 0.0
    await sim.place_market(SYM, "buy", 5.0, False, "c1")                     # long 5 @ 1.0
    await asyncio.sleep(0.02)
    board.set(mk_bbo("blofin", SYM, 1.2000, 1.2000, ts=clock()))
    await sim.place_market(SYM, "sell", 8.0, False, "c2")                    # not reduce-only: flips to short 3 @ 1.2
    await asyncio.sleep(0.02)
    p = (await sim.positions())[0]
    assert p.side == "short" and p.qty == 3.0 and sim._avg[SYM] == pytest.approx(1.2)
    fees = sum(e.fee for e in events if e.state == "filled")
    assert (await sim.balance())["available"] == pytest.approx(100.0 + (1.2 - 1.0) * 5 * 10.0 - fees)   # P&L on the 5 closed
    board.set(mk_bbo("blofin", SYM, 1.1000, 1.1000, ts=clock()))
    await sim.place_market(SYM, "buy", 3.0, True, "c3")                      # cover the short: +0.1 x 3 x 10
    await asyncio.sleep(0.02)
    fees = sum(e.fee for e in events if e.state == "filled")
    assert await sim.positions() == [] and (await sim.balance())["available"] == pytest.approx(100.0 + 10.0 + 3.0 - fees)
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_sim.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'bbo_trader.venues.sim'`

- [x] **Step 3: Implement `bbo_trader/venues/sim.py`**

```python
"""SimVenue: paper trading over any real public feed. Implements the Trading + PrivateFeed protocols.

Fill model (conservative — the SpreadWatch `post_hedge` rule):
- market orders are acked at once and fill fully after SIM_LATENCY_MS at the touch that exists THEN
  (± SIM_TAKER_SLIP_BPS): the market moves during the latency, so paper sees real spread decay;
- post-only orders that would cross the book are rejected like a real venue; resting orders become live
  after the latency and fill only when the OPPOSITE touch crosses the resting price on a quote newer than
  the post, for at most MAKER_TOP_LEVEL_FRAC × that touch's quantity — granted once per DISTINCT touch
  (an unchanged book re-pushed ten times a second is not new flow) and never for a sub-lot touch;
- reduce-only fills are clamped to the open position; a close against a flat position is rejected with
  error "nothing to reduce", as MEXC and BloFin do;
- every quote used for a fill must be FRESH (a stale board means "no quote"); cancel/amend are immediate;
- positions are signed contracts per symbol with an average cost basis; realized P&L and fees flow into
  `balance()` (no margin model: available == total). `position_id` is always "" (one-way accounting), so
  a clean paper run says nothing about MEXC hedge-mode position ids.
Order events are pushed synchronously to the registered handler, so an "ack" event can reach the
executor before the REST-style OrderAck returns — exactly what live private WebSockets do."""
from __future__ import annotations

import asyncio
import itertools
import logging
import time
from collections import deque
from dataclasses import dataclass
from typing import Callable

from ..config import Config
from ..models import BBO, Fees, OrderAck, OrderEvent, VenueSpec
from ..quotes import QuoteBoard
from ..sizing import lots_floor
from .base import VenuePosition

log = logging.getLogger("bbo.sim")

TERMINAL = ("filled", "canceled", "rejected")
MAX_ORDERS = 5000      # terminal orders kept for query_order; the oldest are dropped beyond this


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
    error: str = ""
    last_touch: tuple[float, float] = (0.0, 0.0)   # (price, qty) of the touch that last filled this order

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
        self._resting: dict[str, dict[str, SimOrder]] = {}   # symbol -> client_id -> live post-only order
        self._terminal_ids: deque[str] = deque()
        self._pos: dict[str, float] = {}                    # symbol -> signed contracts
        self._avg: dict[str, float] = {}                    # symbol -> average entry price
        self._cash = cfg.paper_capital_per_venue
        self._handler: Callable[[OrderEvent], None] | None = None
        self._seq = itertools.count(1)
        self._tasks: set[asyncio.Task] = set()
        self._warned_specs: set[str] = set()

    # ---- PrivateFeed ----------------------------------------------------------
    def set_handler(self, on_event: Callable[[OrderEvent], None]) -> None:
        self._handler = on_event

    async def run(self) -> None:
        await asyncio.Event().wait()   # events are pushed synchronously from fills

    # ---- internals ------------------------------------------------------------
    def _spec(self, symbol: str) -> VenueSpec:
        s = self.specs.get(symbol)
        if s is None:
            if symbol not in self._warned_specs:
                self._warned_specs.add(symbol)
                log.warning("SIM_SPEC_MISSING %s %s: contract size 1.0 assumed — fees and P&L may be off by that factor",
                            self.name, symbol)
            return VenueSpec(self.name, symbol, symbol, 1.0, 1.0, 1.0, 0.0001)
        return s

    def _fresh(self, symbol: str) -> BBO | None:
        q = self.board.fresh(self.name, symbol, self.clock())
        return q if q is not None and q.ok else None

    def _event(self, o: SimOrder) -> OrderEvent:
        return OrderEvent(self.name, o.client_id, o.order_id, o.state, o.filled, o.avg_price, o.fee,
                          o.liquidity, "", self.clock(), o.error)

    def _emit(self, o: SimOrder, state: str | None = None) -> None:
        if state:
            o.state = state
        if o.state in TERMINAL:
            self._resting.get(o.symbol, {}).pop(o.client_id, None)
            self._terminal_ids.append(o.client_id)
            while len(self._orders) > MAX_ORDERS and self._terminal_ids:
                self._orders.pop(self._terminal_ids.popleft(), None)
        if self._handler:
            self._handler(self._event(o))

    def _book_position(self, symbol: str, signed: float, price: float, cs: float) -> None:
        """Signed contracts with an average cost basis; a reducing fill realizes P&L into cash."""
        pos = self._pos.get(symbol, 0.0)
        avg = self._avg.get(symbol, 0.0)
        new = round(pos + signed, 10)
        if pos == 0.0 or (pos > 0) == (signed > 0):                  # opening or adding
            self._avg[symbol] = (avg * abs(pos) + price * abs(signed)) / abs(new)
        else:                                                         # reducing, maybe flipping
            closed = min(abs(signed), abs(pos))
            self._cash += (price - avg) * closed * cs * (1.0 if pos > 0 else -1.0)
            if new != 0.0 and (new > 0) != (pos > 0):
                self._avg[symbol] = price                             # the flipped remainder opens here
        if abs(new) < 1e-12:
            self._pos.pop(symbol, None)
            self._avg.pop(symbol, None)
        else:
            self._pos[symbol] = new

    def _fill(self, o: SimOrder, qty: float, price: float, liquidity: str) -> None:
        cs = self._spec(o.symbol).contract_size
        if o.reduce_only:
            pos = self._pos.get(o.symbol, 0.0)
            room = max(0.0, -pos) if o.side == "buy" else max(0.0, pos)
            if room <= 1e-12:
                o.error = "nothing to reduce"
                self._emit(o, "canceled" if o.filled > 0 else "rejected")
                return
            if qty > room:
                log.warning("SIM_REDUCE_CLAMPED %s %s %s %.10g -> %.10g", self.name, o.symbol, o.side, qty, room)
                qty = room
                o.qty = round(o.filled + qty, 10)                     # the venue fills what exists, then the order is done
        notional = qty * price * cs
        o.avg_price = (o.avg_price * o.filled + price * qty) / (o.filled + qty)
        o.filled = round(o.filled + qty, 10)
        rate = self.fees.taker if liquidity == "taker" else self.fees.maker
        fee = notional * rate / 100.0
        o.fee += fee
        o.liquidity = liquidity
        self._cash -= fee
        self._book_position(o.symbol, qty if o.side == "buy" else -qty, price, cs)
        self._emit(o, "filled" if o.remaining <= 1e-12 else "partial")

    def _spawn(self, coro) -> None:
        async def guard():
            try:
                await coro
            except Exception:  # noqa: BLE001
                log.exception("SIM_TASK_ERROR %s", self.name)
        t = asyncio.get_running_loop().create_task(guard())
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)

    def _taker_price(self, symbol: str, side: str) -> float | None:
        q = self._fresh(symbol)
        if q is None:
            return None
        return q.ask * (1.0 + self.slip) if side == "buy" else q.bid * (1.0 - self.slip)

    async def _fill_market_later(self, o: SimOrder, submit_price: float) -> None:
        await asyncio.sleep(self.latency_s)
        if o.state in TERMINAL:
            return
        # the market moves during the latency: fill at the touch that exists NOW (submit price if the feed died)
        self._fill(o, o.qty, self._taker_price(o.symbol, o.side) or submit_price, "taker")

    # ---- Trading --------------------------------------------------------------
    async def place_market(self, symbol: str, side: str, qty: float, reduce_only: bool, client_id: str) -> OrderAck:
        q = self._fresh(symbol)
        if q is None:
            return OrderAck(False, error="no quote")
        if qty <= 0:
            return OrderAck(False, error="qty must be > 0")
        o = SimOrder(client_id, f"sim-{next(self._seq)}", symbol, side, qty, 0.0, False, reduce_only,
                     self.clock() + self.latency_s)
        self._orders[client_id] = o
        self._emit(o, "ack")                                          # live venues push new/ack before the fill
        price = q.ask * (1.0 + self.slip) if side == "buy" else q.bid * (1.0 - self.slip)
        self._spawn(self._fill_market_later(o, price))
        return OrderAck(True, o.order_id, latency_ms=self.latency_s * 1000.0)

    async def place_post_only(self, symbol: str, side: str, qty: float, price: float, reduce_only: bool,
                              client_id: str) -> OrderAck:
        q = self._fresh(symbol)
        if q is None:
            return OrderAck(False, error="no quote")
        if qty <= 0:
            return OrderAck(False, error="qty must be > 0")
        o = SimOrder(client_id, f"sim-{next(self._seq)}", symbol, side, qty, price, True, reduce_only,
                     self.clock() + self.latency_s)
        self._orders[client_id] = o
        if (side == "sell" and price <= q.bid) or (side == "buy" and price >= q.ask):
            o.error = "post-only would cross"
            self._emit(o, "rejected")
            return OrderAck(False, o.order_id, error="post-only would cross")
        self._resting.setdefault(symbol, {})[client_id] = o
        self._emit(o, "ack")
        return OrderAck(True, o.order_id, latency_ms=self.latency_s * 1000.0)

    def on_quote(self, bbo: BBO) -> None:
        """Called by the app for every stored quote of this venue: fill resting orders the touch crossed."""
        if bbo.venue != self.name:
            return
        for o in list(self._resting.get(bbo.symbol, {}).values()):
            if o.state not in ("ack", "partial") or bbo.ts_local < o.live_ts:
                continue
            if o.side == "sell" and bbo.bid >= o.price:
                touch = (bbo.bid, bbo.bid_qty)
            elif o.side == "buy" and bbo.ask <= o.price:
                touch = (bbo.ask, bbo.ask_qty)
            else:
                continue
            if touch == o.last_touch:
                continue                                              # the same book re-pushed: no new flow
            o.last_touch = touch
            cap = lots_floor(self.frac * touch[1], self._spec(o.symbol))   # whole lots; a sub-lot touch fills nothing
            qty = min(o.remaining, cap)
            if qty > 0:
                self._fill(o, qty, o.price, "maker")

    async def cancel(self, symbol: str, client_id: str, order_id: str) -> OrderAck:
        o = self._orders.get(client_id)
        if o is None:
            return OrderAck(False, order_id, error="unknown order")
        if o.state in TERMINAL:
            return OrderAck(False, o.order_id, error="terminal")
        self._emit(o, "canceled")
        return OrderAck(True, o.order_id)

    async def amend(self, symbol: str, client_id: str, order_id: str, new_price: float) -> OrderAck:
        o = self._orders.get(client_id)
        if o is None or not o.post_only or o.state not in ("ack", "partial"):
            return OrderAck(False, order_id, error="not resting")
        q = self._fresh(symbol)
        if q is None:
            return OrderAck(False, o.order_id, error="no quote")
        if (o.side == "sell" and new_price <= q.bid) or (o.side == "buy" and new_price >= q.ask):
            return OrderAck(False, o.order_id, error="post-only would cross")
        o.price = new_price
        o.live_ts = self.clock() + self.latency_s
        o.last_touch = (0.0, 0.0)
        return OrderAck(True, o.order_id)

    async def query_order(self, symbol: str, client_id: str, order_id: str) -> OrderEvent | None:
        o = self._orders.get(client_id)
        return self._event(o) if o else None

    async def open_orders(self) -> list[OrderEvent]:
        return [self._event(o) for by_sym in self._resting.values() for o in by_sym.values()
                if o.state in ("ack", "partial")]

    async def positions(self) -> list[VenuePosition]:
        return [VenuePosition(self.name, s, "long" if q > 0 else "short", abs(q))
                for s, q in self._pos.items() if abs(q) > 1e-12]

    async def balance(self) -> dict[str, float]:
        return {"available": self._cash, "total": self._cash}

    async def set_leverage(self, symbol: str, leverage: int) -> None:
        return None
```

- [x] **Step 3b: Write the protocol conformance test**

`tests/test_venue_protocols.py` (pins every adapter to the protocols in `venues/base.py`; goes green immediately and fails on any signature drift in Plan 2/3 adapters):

```python
"""Structural conformance: every adapter must match the venue protocols exactly (names, parameter
order, async-ness, properties, data members). Protocols are not runtime_checkable and nothing else
would catch a drifting signature before a live order call."""
import inspect

from bbo_trader.venues import base
from bbo_trader.venues.blofin import BlofinMarket, BlofinPublic
from bbo_trader.venues.mexc import MexcMarket, MexcPublic
from bbo_trader.venues.sim import SimVenue

IMPLEMENTATIONS = {
    base.PublicFeed: [MexcPublic, BlofinPublic],
    base.MarketData: [MexcMarket, BlofinMarket],
    base.Trading: [SimVenue],
    base.PrivateFeed: [SimVenue],
}
INSTANCES = {MexcMarket: lambda: MexcMarket(None), BlofinMarket: lambda: BlofinMarket(None)}   # for instance attrs


def test_adapters_conform_to_protocols():
    for proto, impls in IMPLEMENTATIONS.items():
        for impl in impls:
            for name, member in vars(proto).items():
                if name.startswith("_"):
                    continue
                assert hasattr(impl, name), (proto.__name__, impl.__name__, name)
                mine = getattr(impl, name)
                if isinstance(member, property):
                    assert isinstance(mine, property), (impl.__name__, name)
                    continue
                if callable(member):
                    assert list(inspect.signature(member).parameters) == list(inspect.signature(mine).parameters), \
                        (impl.__name__, name)
                    assert inspect.iscoroutinefunction(member) == inspect.iscoroutinefunction(mine), (impl.__name__, name)
            for attr in getattr(proto, "__annotations__", {}):          # data members: supports_amend, specs
                obj = INSTANCES[impl]() if impl in INSTANCES else impl
                assert hasattr(obj, attr), (proto.__name__, impl.__name__, attr)
```

Run: `.venv/bin/python -m pytest tests/test_venue_protocols.py -q`
Expected: `1 passed`

- [x] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_sim.py -q`
Expected: `10 passed`

- [x] **Step 5: Run the whole suite and commit**

Run: `.venv/bin/python -m pytest -q`
Expected: `114 passed`

```bash
git add deploy-bbo/bbo_trader/venues/sim.py deploy-bbo/tests/test_sim.py deploy-bbo/tests/test_venue_protocols.py
git commit -m "feat(bbo): SimVenue — paper trading with conservative maker fill model"
```

---

### Task 15: Metrics (`metrics.py`)

**Files:**
- Create: `deploy-bbo/bbo_trader/metrics.py`
- Test: `deploy-bbo/tests/test_metrics.py`

- [x] **Step 1: Write the failing tests**

`tests/test_metrics.py`:

```python
import asyncio
import logging

from bbo_trader import metrics as metrics_mod
from bbo_trader.metrics import LatencyHist, Metrics, CoverageWatchdog
from bbo_trader.quotes import QuoteBoard
from tests.conftest import mk_bbo


def test_latency_hist_percentiles_window_and_lifetime():
    h = LatencyHist()
    for v in [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]:
        h.add(v)
    d = h.to_dict()
    assert d["n"] == 10 and d["p50"] == 60.0 and d["p95"] == 100.0 and d["max"] == 100.0   # floor(q·n) convention
    assert LatencyHist().to_dict() == {"n": 0, "total": 0, "p50": 0.0, "p95": 0.0, "max": 0.0, "max_ever": 0.0}
    small = LatencyHist(3)
    for v in (1000.0, 1.0, 2.0, 3.0, 4.0):
        small.add(v)
    d = small.to_dict()
    assert d["n"] == 3 and d["max"] == 4.0 and d["total"] == 5 and d["max_ever"] == 1000.0   # window vs lifetime
    small.add(-5.0)                                              # a backwards clock step is clamped, NaN ignored
    small.add(float("nan"))
    assert small.to_dict()["n"] == 3 and small.to_dict()["total"] == 6 and min(small._v) == 0.0


def test_metrics_record_and_funnel_and_shape():
    t = [100.0]
    m = Metrics(mono=lambda: t[0])
    m.record("submit_to_ack", 120.0)
    m.record("submit_to_ack", 80.0)
    m.funnel["below_edge"] += 2
    t[0] = 160.0
    d = m.to_dict()
    assert d["latency_ms"]["submit_to_ack"]["n"] == 2 and d["latency_ms"]["submit_to_ack"]["p50"] == 120.0
    assert d["funnel"] == {"below_edge": 2} and d["uptime_s"] == 60
    assert set(d) == {"latency_ms", "funnel", "loop_lag_ms", "uptime_s", "started_at"}


async def test_loop_lag_sampler_measures_and_summarizes(monkeypatch, caplog):
    m = Metrics()
    real_sleep = asyncio.sleep

    async def slow_sleep(_s):                                     # the loop is "busy": every sleep overruns by ~30 ms
        await real_sleep(0.03)
    monkeypatch.setattr(metrics_mod.asyncio, "sleep", slow_sleep)
    with caplog.at_level(logging.WARNING, logger="bbo.metrics"):
        task = asyncio.create_task(m.sample_loop_lag(interval_s=0.001, warn_ms=10.0, summary_s=0.05))
        await real_sleep(0.2)
        task.cancel()
    d = m.to_dict()["loop_lag_ms"]
    assert d["n"] >= 3 and d["p50"] >= 20.0                        # lag is measured against the requested interval
    lines = [r for r in caplog.records if "LOOP_LAG" in r.getMessage()]
    assert 1 <= len(lines) <= 4 and "over 10 ms" in lines[0].getMessage()   # summarized, not one line per sample


def test_coverage_watchdog_counts_relative_floor_and_grace(clock, caplog):
    board = QuoteBoard(2.0)
    board.set(mk_bbo("mexc", "AUSDT", 1, 1.1, ts=clock()))
    board.set(mk_bbo("mexc", "BUSDT", 1, 1.1, ts=clock()))
    board.set(mk_bbo("blofin", "AUSDT", 1, 1.1, ts=clock() - 5))    # stale
    w = CoverageWatchdog(board, ["mexc", "blofin"], floor=1, clock=clock)
    with caplog.at_level(logging.WARNING, logger="bbo.metrics"):
        assert w.check() == {"mexc": 2, "blofin": 0}                # a dead venue reads 0, never disappears
    assert "FEED_COVERAGE_LOW" not in caplog.text                    # quiet during the start-up grace
    clock.tick(61)
    for i in range(400):
        board.set(mk_bbo("mexc", f"S{i}USDT", 1, 1.1, ts=clock()))
    with caplog.at_level(logging.WARNING, logger="bbo.metrics"):
        w.check()
    assert "FEED_COVERAGE_LOW blofin fresh=0" in caplog.text and "FEED_COVERAGE_LOW mexc" not in caplog.text
    caplog.clear()
    clock.tick(3)                                                    # mexc: 390 of 400 go stale -> under 50 % of its high-water mark
    for i in range(10):
        board.set(mk_bbo("mexc", f"S{i}USDT", 1, 1.1, ts=clock()))
    with caplog.at_level(logging.WARNING, logger="bbo.metrics"):
        assert w.check()["mexc"] == 10
    assert "FEED_COVERAGE_LOW mexc fresh=10 threshold=200" in caplog.text
    assert w.to_dict() == {"fresh": {"mexc": 10, "blofin": 0}, "high": {"mexc": 400, "blofin": 0}, "floor": 1, "frac": 0.5}
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_metrics.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'bbo_trader.metrics'`

- [x] **Step 3: Implement `bbo_trader/metrics.py`**

```python
"""Latency histograms, the rejection funnel, event-loop lag sampling and the feed coverage watchdog.

Everything here lands in the state file's `bbo` section for the dashboard, so the numbers say what they
are: histogram `n`/`max` describe the sliding window, `total`/`max_ever` the process lifetime."""
from __future__ import annotations

import asyncio
import logging
import math
import time
from collections import Counter, deque

log = logging.getLogger("bbo.metrics")


class LatencyHist:
    def __init__(self, maxlen: int = 1000):
        self._v: deque[float] = deque(maxlen=maxlen)
        self._total = 0
        self._max_ever = 0.0

    def add(self, ms: float) -> None:
        ms = float(ms)
        if not math.isfinite(ms):
            return
        ms = max(0.0, ms)                 # a backwards clock step is not a negative latency
        self._v.append(ms)
        self._total += 1
        self._max_ever = max(self._max_ever, ms)

    def _pct(self, q: float) -> float:
        """Index floor(q·n) of the sorted window — one rank above nearest-rank, so p95 == max while n <= 20."""
        if not self._v:
            return 0.0
        s = sorted(self._v)
        return s[min(len(s) - 1, int(q * len(s)))]

    @property
    def count(self) -> int:
        return len(self._v)

    def to_dict(self) -> dict:
        return {"n": self.count, "total": self._total, "p50": round(self._pct(0.50), 1),
                "p95": round(self._pct(0.95), 1), "max": round(max(self._v), 1) if self._v else 0.0,
                "max_ever": round(self._max_ever, 1)}


class Metrics:
    def __init__(self, mono=time.monotonic):
        self.hists: dict[str, LatencyHist] = {}
        self.funnel: Counter = Counter()
        self.loop_lag = LatencyHist(600)
        self._mono = mono
        self.started_mono = mono()
        self.started = time.time()

    def record(self, stage: str, ms: float) -> None:
        self.hists.setdefault(stage, LatencyHist()).add(ms)

    async def sample_loop_lag(self, interval_s: float = 1.0, warn_ms: float = 50.0, summary_s: float = 60.0) -> None:
        """Samples the event-loop lag every `interval_s`; one LOOP_LAG summary line per `summary_s` at most
        (a lag storm must not bury the trading log under one warning per second)."""
        loop = asyncio.get_running_loop()
        over = samples = 0
        worst = 0.0
        window_start = loop.time()
        while True:
            t0 = loop.time()
            await asyncio.sleep(interval_s)
            lag_ms = max(0.0, (loop.time() - t0 - interval_s) * 1000.0)
            self.loop_lag.add(lag_ms)
            samples += 1
            if lag_ms > warn_ms:
                over += 1
                worst = max(worst, lag_ms)
            if loop.time() - window_start >= summary_s:
                if over:
                    log.warning("LOOP_LAG %d of %d samples over %.0f ms in the last %.0fs, worst %.0f ms",
                                over, samples, warn_ms, loop.time() - window_start, worst)
                over = samples = 0
                worst = 0.0
                window_start = loop.time()

    def to_dict(self) -> dict:
        return {"latency_ms": {k: h.to_dict() for k, h in self.hists.items()},
                "funnel": dict(self.funnel), "loop_lag_ms": self.loop_lag.to_dict(),
                "uptime_s": round(self._mono() - self.started_mono), "started_at": self.started}


class CoverageWatchdog:
    """Fresh-quote counts per CONFIGURED venue (a dead venue reads 0, it never disappears). Warns when a venue
    is under max(floor, frac × its own high-water mark): an absolute floor alone is silent when 390 of 400
    symbols go stale and permanently noisy on a 10-pair venue. Quiet for `grace_s` after start so the first
    minute of connecting does not raise an alarm. The App schedules `check()`; `to_dict()` goes to the state file."""

    def __init__(self, board, venues: list[str], floor: int = 10, frac: float = 0.5, grace_s: float = 60.0,
                 clock=time.time):
        self.board, self.venues, self.floor, self.frac, self.grace_s, self.clock = board, list(venues), floor, frac, grace_s, clock
        self.last: dict[str, int] = {v: 0 for v in self.venues}
        self.high: dict[str, int] = {v: 0 for v in self.venues}
        self._t0 = clock()

    def check(self) -> dict[str, int]:
        now = self.clock()
        counts = self.board.fresh_counts(now)
        self.last = {v: counts.get(v, 0) for v in self.venues}
        log.info("FEED_COVERAGE %s", " ".join(f"{v}={n}" for v, n in self.last.items()))
        for v, n in self.last.items():
            self.high[v] = max(self.high[v], n)
            threshold = max(float(self.floor), self.frac * self.high[v])
            if n < threshold and now - self._t0 >= self.grace_s:
                log.warning("FEED_COVERAGE_LOW %s fresh=%d threshold=%.0f (floor=%d, high=%d)", v, n, threshold,
                            self.floor, self.high[v])
        return self.last

    def to_dict(self) -> dict:
        return {"fresh": dict(self.last), "high": dict(self.high), "floor": self.floor, "frac": self.frac}
```

- [x] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_metrics.py -q`
Expected: `4 passed`

- [x] **Step 5: Commit**

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

- [x] **Step 1: Write the failing TT-flow tests**

`tests/test_execution_tt.py`:

```python
import asyncio

import pytest

from bbo_trader.budget import RateBudget
from bbo_trader.execution import Executor
from bbo_trader.metrics import Metrics
from bbo_trader.models import Intent, OPEN, CLOSED, MAKER_RESTING, HEDGING, OrderAck, OrderEvent
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


async def test_retry_degraded_books_a_leg_the_venue_no_longer_holds(tmp_path, monkeypatch):
    from bbo_trader import execution
    h = Harness(tmp_path)
    h.quote("blofin", 1.0050, 1.0060)
    h.quote("mexc", 1.0000, 1.0008)
    pos = await h.ex.enter_tt(Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0, spread_pct=0.42))
    h.sim("mexc")._pos.clear()                                     # the venue lost our long (accounting drift)
    h.sim("mexc")._avg.clear()
    await h.ex.exit_tt(pos, "test")                                # mexc refuses: nothing to reduce -> booked flat
    assert pos.status == CLOSED and pos.exit_filled_b == pos.filled_b and pos.exit_reason == "test"
    assert await h.sim("blofin").positions() == [] and await h.sim("mexc").positions() == []
    monkeypatch.setattr(execution, "DEGRADED_RETRY_S", 0.0)
    monkeypatch.setattr(execution, "MAX_CLOSE_RETRIES", 1)         # a leg that can never close stops retrying
    h.quote("blofin", 1.0050, 1.0060)
    pos2 = await h.ex.enter_tt(Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0, spread_pct=0.42))
    h.sim("mexc")._pos.clear()
    h.sim("mexc")._pos[SYM] = -1.0                                 # venue holds a SHORT where we book a long: not flat, not closable
    await h.ex.exit_tt(pos2, "test")
    assert pos2.status == "DEGRADED"
    await h.ex.retry_degraded()
    await h.ex.retry_degraded()
    await asyncio.sleep(0.01)                                      # notification task runs
    assert pos2.status == "DEGRADED" and pos2.close_retry_count == 2 and any("DEGRADED_STUCK" in n for n in h.notes)


async def settle(n=10):
    for _ in range(n):
        await asyncio.sleep(0.005)


async def test_partial_close_stays_degraded_until_the_remainder_is_closed(tmp_path, monkeypatch):
    from bbo_trader import execution
    h = Harness(tmp_path)
    h.quote("blofin", 1.0050, 1.0060)
    h.quote("mexc", 1.0000, 1.0008)
    pos = await h.ex.enter_tt(Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0, spread_pct=0.42))
    sim = h.sim("blofin")
    real_pm = sim.place_market

    async def fills_four(symbol, side, qty, reduce_only, client_id):            # a thin book: 4 of the 20
        return await real_pm(symbol, side, min(qty, 4.0), reduce_only, client_id)
    sim.place_market = fills_four
    h.quote("blofin", 1.0010, 1.0012)
    h.quote("mexc", 1.0009, 1.0011)
    await h.ex.exit_tt(pos, "convergence")
    assert pos.status == "DEGRADED" and pos.degraded_leg == "a" and pos.exit_filled_a == 4.0
    assert (await sim.positions())[0].qty == 16.0                            # the remainder is still owned, not abandoned
    sim.place_market = real_pm
    monkeypatch.setattr(execution, "DEGRADED_RETRY_S", 0.0)
    await h.ex.retry_degraded()
    assert pos.status == CLOSED and pos.exit_filled_a == 20.0 and await sim.positions() == []


async def test_flatten_leg_continues_after_partial_fills(tmp_path):
    h = Harness(tmp_path)
    h.quote("blofin", 1.0050, 1.0060)
    h.board.set(mk_bbo("mexc", SYM, 1.0000, 1.0008, contract_size=10.0))
    h.sim("mexc").board = QuoteBoard(2.0)                                     # the mexc leg is rejected: no quote
    sim = h.sim("blofin")
    real_pm = sim.place_market

    async def partial(symbol, side, qty, reduce_only, client_id):
        return await real_pm(symbol, side, min(qty, 6.0) if reduce_only else qty, reduce_only, client_id)
    sim.place_market = partial
    assert await h.ex.enter_tt(Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0)) is None
    closed = h.book.closed[-1]
    assert closed.status == CLOSED and closed.exit_reason == "failed_entry" and closed.exit_filled_a == 20.0
    assert await sim.positions() == []                                        # the ladder flattened 6+6+6+2


async def test_venue_exception_on_one_leg_flattens_the_other(tmp_path, monkeypatch):
    from bbo_trader import execution
    monkeypatch.setattr(execution, "EVENT_GRACE_S", 0.01)
    monkeypatch.setattr(execution, "POLL_MAX_S", 0.05)
    h = Harness(tmp_path)
    h.quote("blofin", 1.0050, 1.0060)
    h.quote("mexc", 1.0000, 1.0008)

    async def boom(*a, **k):
        raise RuntimeError("REST timeout")
    h.sim("mexc").place_market = boom                                         # in doubt: polled, then treated as rejected
    assert await h.ex.enter_tt(Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0)) is None
    await asyncio.sleep(0.01)
    assert h.book.open == [] and h.book.closed[-1].exit_reason == "failed_entry"
    assert await h.sim("blofin").positions() == [] and any("ORDER_UNRESOLVED" in n for n in h.notes)


async def test_hedge_failure_with_successful_flatten_closes_as_a_round_trip(tmp_path, caplog):
    h = Harness(tmp_path)
    h.quote("blofin", 1.0041, 1.0061)
    h.quote("mexc", 1.0000, 1.0010)
    pos = await h.ex.enter_tm(Intent("TM_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                     rest_price=1.0061, size_usd=25.0))
    await asyncio.sleep(0.005)
    h.sim("mexc").board = QuoteBoard(2.0)                                     # the hedge venue cannot see a quote
    h.quote("blofin", 1.0061, 1.0062, bq=100.0)                               # full 20-contract maker fill
    await settle()
    assert pos.status == CLOSED and pos.exit_reason == "hedge_unwound"
    assert await h.sim("blofin").positions() == [] and await h.sim("mexc").positions() == []
    assert pos.net_pnl_usd == pytest.approx(pos.pnl_adjust_usd - pos.entry_fees_usd - pos.exit_fees_usd)
    assert pos.entry_fees_usd > 0 and pos.exit_fees_usd > 0 and pos.pnl_adjust_usd < 0     # bought back one tick higher
    assert "EXEC_TASK_ERROR" not in caplog.text and h.book.open == []


async def test_exit_hedge_failure_closes_the_remainder_taker(tmp_path, caplog, monkeypatch):
    from bbo_trader import execution
    h = Harness(tmp_path)
    h.quote("blofin", 1.0050, 1.0060)
    h.quote("mexc", 1.0000, 1.0008)
    pos = await h.ex.enter_tt(Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0))
    h.quote("blofin", 1.0020, 1.0030)
    h.quote("mexc", 1.0000, 1.0005)
    assert await h.ex.exit_tm(pos, Intent("TM_EXIT", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                          rest_price=1.0015))
    await asyncio.sleep(0.005)
    real_board = h.sim("mexc").board
    h.sim("mexc").board = QuoteBoard(2.0)                                     # hedge venue unreachable
    h.quote("blofin", 1.0010, 1.0015, aq=100.0)                               # the exit maker fills: short leg closed
    await settle()
    assert pos.status == "DEGRADED" and pos.degraded_leg == "b" and pos.exit_filled_a == 20.0
    assert "FLATTEN" not in caplog.text and "HEDGE_FAILED" in caplog.text     # no futile re-opening order on blofin
    assert await h.sim("blofin").positions() == []
    h.sim("mexc").board = real_board
    monkeypatch.setattr(execution, "DEGRADED_RETRY_S", 0.0)
    await h.ex.retry_degraded()
    assert pos.status == CLOSED and await h.sim("mexc").positions() == []


async def test_cancel_failure_during_hedge_is_retried_by_the_sweep(tmp_path, monkeypatch):
    from bbo_trader import execution
    monkeypatch.setattr(execution, "HEDGING_SWEEP_AFTER_S", 0.0)
    h = Harness(tmp_path)
    h.quote("blofin", 1.0041, 1.0061)
    h.quote("mexc", 1.0000, 1.0010)
    pos = await h.ex.enter_tm(Intent("TM_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                     rest_price=1.0061, size_usd=25.0))
    await asyncio.sleep(0.005)
    sim = h.sim("blofin")
    real_cancel, real_query = sim.cancel, sim.query_order

    async def bad_cancel(*a, **k):
        return OrderAck(False, error="boom")

    async def no_query(*a, **k):
        return None
    sim.cancel, sim.query_order = bad_cancel, no_query
    h.quote("blofin", 1.0061, 1.0062, bq=20.0)                                # partial fill of 10: hedged, cancel fails
    await settle()
    assert pos.status == HEDGING and pos.hedged_qty > 9.0 and len(await sim.open_orders()) == 1
    sim.cancel, sim.query_order = real_cancel, real_query
    await h.ex.retry_degraded()                                               # the sweep retries the cancel
    await settle()
    assert pos.status == OPEN and await sim.open_orders() == [] and pos.filled_a == 10.0


async def test_one_legged_unwind_books_the_traded_leg(tmp_path, monkeypatch):
    from bbo_trader import execution
    h = Harness(tmp_path)
    h.quote("blofin", 1.0041, 1.0061)
    h.quote("mexc", 1.0000, 1.0010)
    pos = await h.ex.enter_tm(Intent("TM_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                     rest_price=1.0061, size_usd=25.0))
    await asyncio.sleep(0.005)
    h.quote("blofin", 1.0061, 1.0062, bq=100.0)
    h.sim("mexc").board = QuoteBoard(2.0)                                     # the hedge fails...
    real_pm = h.sim("blofin").place_market

    async def no_market(*a, **k):
        return OrderAck(False, error="venue down")
    h.sim("blofin").place_market = no_market                                  # ...and so does the flatten
    await settle()
    assert pos.status == "DEGRADED" and pos.filled_a == 20.0 and pos.size_usd == pytest.approx(20 * 1.0061)
    h.sim("blofin").place_market = real_pm
    monkeypatch.setattr(execution, "DEGRADED_RETRY_S", 0.0)
    await h.ex.retry_degraded()
    assert pos.status == CLOSED and await h.sim("blofin").positions() == []
    assert pos.gross_pnl_usd == pytest.approx((1.0061 - pos.exit_price_a) / 1.0061 * pos.size_usd)   # not zeroed


async def test_stray_fill_on_a_superseded_maker_order_is_unwound(tmp_path, monkeypatch):
    from bbo_trader import execution
    h = Harness(tmp_path)
    h.quote("blofin", 1.0041, 1.0061)
    h.quote("mexc", 1.0000, 1.0010)
    sim = h.sim("blofin")
    sim.supports_amend = False                                                # MEXC-style cancel+new requote
    pos = await h.ex.enter_tm(Intent("TM_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                     rest_price=1.0061, size_usd=25.0))
    old_cid = pos.maker_client_id
    await h.ex.requote(pos, 1.0063)
    await settle()
    assert pos.maker_client_id != old_cid and pos.status == MAKER_RESTING
    sim._pos[SYM] = -12.0                                                     # the venue really filled the cancelled order
    h.ex.on_order_event(OrderEvent("blofin", old_cid, "sim-1", "filled", filled_qty=12.0, avg_price=1.0061,
                                   fee=0.0024, liquidity="maker", ts=h.ex.clock()))
    await settle()
    assert pos.status == "DEGRADED" and pos.filled_a == 12.0 and pos.maker_filled_qty == 0.0   # never on the live order's counters
    assert await sim.open_orders() == []                                      # the live order was cancelled by the degrade
    monkeypatch.setattr(execution, "DEGRADED_RETRY_S", 0.0)
    await h.ex.retry_degraded()
    assert pos.status == CLOSED and await sim.positions() == [] and await h.sim("mexc").positions() == []


async def test_close_is_idempotent_for_risk_stats(tmp_path):
    h = Harness(tmp_path)
    h.quote("blofin", 1.0050, 1.0060)
    h.quote("mexc", 1.0000, 1.0008)
    pos = await h.ex.enter_tt(Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0))
    h.quote("blofin", 1.0010, 1.0012)
    h.quote("mexc", 1.0009, 1.0011)
    await h.ex.exit_tt(pos, "convergence")
    st = dict(h.risk.pair_stats["XYZUSDT|blofin>mexc"])
    h.ex._close(pos, "convergence")                                           # a racing second close
    assert h.risk.pair_stats["XYZUSDT|blofin>mexc"] == st and h.book.total_trades == 1
    assert pos.id not in h.ex._locks and not any(t.pos_id == pos.id for t in h.ex._tracks.values())


async def test_in_doubt_order_with_a_known_partial_books_the_partial(tmp_path, monkeypatch):
    from bbo_trader import execution
    from bbo_trader.execution import LegTrack
    monkeypatch.setattr(execution, "EVENT_GRACE_S", 0.01)
    monkeypatch.setattr(execution, "POLL_MAX_S", 0.02)
    h = Harness(tmp_path)
    loop = asyncio.get_running_loop()
    tr = LegTrack(1, "entry_b", "mexc", SYM, 2.0, 0.0, done=loop.create_future())
    tr.last = OrderEvent("mexc", "cid", "", "partial", filled_qty=1.0, avg_price=1.0008, fee=0.002)
    ev = await h.ex._await_terminal(tr, h.venues["mexc"], "cid", "", 2.0, assume_filled=False)
    assert ev.state == "filled" and ev.filled_qty == 1.0 and ev.error == "in_doubt"          # book what the feed showed
    tr2 = LegTrack(1, "entry_b", "mexc", SYM, 2.0, 0.0, done=loop.create_future())
    ev2 = await h.ex._await_terminal(tr2, h.venues["mexc"], "cid2", "", 2.0, assume_filled=False)
    assert ev2.state == "rejected" and ev2.error == "in_doubt"                               # nothing known: never assumed filled
    await asyncio.sleep(0.01)
    assert sum("ORDER_UNRESOLVED" in n for n in h.notes) == 2
```

- [x] **Step 2: Write the failing TM-flow tests**

`tests/test_execution_tm.py`:

```python
import asyncio

import pytest

from bbo_trader.models import Intent, OPEN, CLOSED, MAKER_RESTING, HEDGING, EXIT_MAKER_RESTING, OrderAck
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


async def test_failed_cancel_leaves_nothing_in_flight(tmp_path):
    h = Harness(tmp_path)
    h.quote("blofin", 1.0041, 1.0061)
    h.quote("mexc", 1.0000, 1.0010)
    pos = await h.ex.enter_tm(Intent("TM_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                     rest_price=1.0061, size_usd=25.0))
    sim = h.sim("blofin")
    real_cancel = sim.cancel

    async def bad_cancel(*a, **k):
        return OrderAck(False, error="boom")

    async def no_query(*a, **k):
        return None
    sim.cancel, sim.query_order = bad_cancel, no_query
    await h.ex.cancel_maker(pos, "test")
    assert pos.status == MAKER_RESTING and not pos.maker_cancel_sent          # transport failure: retry allowed
    sim.supports_amend = False
    await h.ex.requote(pos, 1.0063)                                          # cancel+new path fails the same way
    assert not pos.requote_pending and not pos.maker_cancel_sent and pos.maker_rest_price == 1.0061
    await h.ex.upgrade_to_tt(pos)
    assert not pos.upgrade_pending and not pos.maker_cancel_sent
    sim.cancel = real_cancel
    await h.ex.cancel_maker(pos, "test")                                     # the venue is back: cancel goes through
    assert pos.maker_cancel_sent


async def test_exit_maker_rejected_when_venue_is_flat_books_the_leg_and_closes(tmp_path):
    h = Harness(tmp_path)
    h.quote("blofin", 1.0041, 1.0061)
    h.quote("mexc", 1.0000, 1.0010)
    pos = await h.ex.enter_tm(Intent("TM_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                     rest_price=1.0061, size_usd=25.0))
    await asyncio.sleep(0.005)
    h.quote("blofin", 1.0061, 1.0062, bq=100.0)
    await settle()
    assert pos.status == OPEN
    h.sim("blofin")._pos.clear()                                   # the venue lost our short (accounting drift)
    h.sim("blofin")._avg.clear()
    h.quote("blofin", 1.0020, 1.0030)
    h.quote("mexc", 1.0000, 1.0005)
    assert await h.ex.exit_tm(pos, Intent("TM_EXIT", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                          rest_price=1.0015))
    assert pos.status == EXIT_MAKER_RESTING
    await asyncio.sleep(0.005)
    h.quote("blofin", 1.0010, 1.0015, aq=100.0)                    # our reduce-only bid would fill: venue says nothing to reduce
    await settle()
    assert pos.status == CLOSED and pos.exit_reason == "venue_flat" and pos.exit_filled_a == pos.filled_a
    assert await h.sim("mexc").positions() == [] and pos.exit_filled_b == pos.filled_b


async def test_stray_fill_during_a_flatten_does_not_close_the_position(tmp_path, monkeypatch):
    from bbo_trader import execution
    from bbo_trader.models import OrderEvent
    from bbo_trader.quotes import QuoteBoard
    h = Harness(tmp_path)
    h.quote("blofin", 1.0041, 1.0061)
    h.quote("mexc", 1.0000, 1.0010)
    sim = h.sim("blofin")
    sim.supports_amend = False
    pos = await h.ex.enter_tm(Intent("TM_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                     rest_price=1.0061, size_usd=25.0))
    old_cid = pos.maker_client_id
    await h.ex.requote(pos, 1.0063)
    await settle()
    await asyncio.sleep(0.005)
    h.sim("mexc").board = QuoteBoard(2.0)                          # hedge venue unreachable: the fill will be flattened
    h.quote("blofin", 1.0063, 1.0064, bq=100.0)                    # the live order fills 20 (flatten now in flight)
    sim._pos[SYM] = sim._pos.get(SYM, 0.0) - 12.0                  # ...and the cancelled order's fill lands at the venue
    h.ex.on_order_event(OrderEvent("blofin", old_cid, "sim-1", "filled", filled_qty=12.0, avg_price=1.0061,
                                   fee=0.0024, liquidity="maker", ts=h.ex.clock()))
    await settle()
    assert pos.status == "DEGRADED" and pos.filled_a - pos.exit_filled_a == pytest.approx(12.0)   # not hedge_unwound
    monkeypatch.setattr(execution, "DEGRADED_RETRY_S", 0.0)
    await h.ex.retry_degraded()
    assert pos.status == CLOSED and await sim.positions() == [] and h.book.open == []


async def test_stray_fill_during_the_tt_upgrade_is_not_clobbered(tmp_path, monkeypatch):
    from bbo_trader import execution
    from bbo_trader.models import OrderEvent
    h = Harness(tmp_path)
    h.quote("blofin", 1.0041, 1.0061)
    h.quote("mexc", 1.0000, 1.0010)
    sim = h.sim("blofin")
    pos = await h.ex.enter_tm(Intent("TM_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                     rest_price=1.0061, size_usd=25.0))
    old_cid = pos.maker_client_id
    h.quote("blofin", 1.0060, 1.0065)                              # a TT edge: upgrade
    await h.ex.upgrade_to_tt(pos)
    for _ in range(50):                                            # the cancel finalizes into TT legs in flight
        if pos.status == "TT_ENTERING":
            break
        await asyncio.sleep(0.001)
    assert pos.status == "TT_ENTERING"
    sim._pos[SYM] = sim._pos.get(SYM, 0.0) - 12.0                  # the cancelled maker order filled after all
    h.ex.on_order_event(OrderEvent("blofin", old_cid, "sim-1", "filled", filled_qty=12.0, avg_price=1.0061,
                                   fee=0.0024, liquidity="maker", ts=h.ex.clock()))
    await settle()
    assert pos.status == "DEGRADED" and pos.filled_a >= 12.0       # the TT fill was ADDED to the stray, not written over it
    monkeypatch.setattr(execution, "DEGRADED_RETRY_S", 0.0)
    for _ in range(3):
        await h.ex.retry_degraded()
        await settle()
    assert pos.status == CLOSED and await sim.positions() == [] and await h.sim("mexc").positions() == []


async def test_straggler_on_a_cancelled_exit_maker_is_booked_as_an_exit_fill(tmp_path, monkeypatch):
    from bbo_trader import execution
    from bbo_trader.models import OrderEvent
    h = Harness(tmp_path)
    h.quote("blofin", 1.0050, 1.0060)
    h.quote("mexc", 1.0000, 1.0008)
    pos = await h.ex.enter_tt(Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0))
    h.quote("blofin", 1.0020, 1.0030)
    h.quote("mexc", 1.0000, 1.0005)
    assert await h.ex.exit_tm(pos, Intent("TM_EXIT", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                          rest_price=1.0015))
    cid = pos.maker_client_id
    await h.ex.cancel_maker(pos, "test")
    await settle()
    assert pos.status == OPEN and pos.maker_venue == ""
    sim = h.sim("blofin")
    sim._pos[SYM] += 12.0                                          # the venue filled the cancelled reduce-only buy: short 20 -> 8
    h.ex.on_order_event(OrderEvent("blofin", cid, "sim-2", "filled", filled_qty=12.0, avg_price=1.0015,
                                   fee=0.0024, liquidity="maker", ts=h.ex.clock()))
    await settle()
    assert pos.status == "DEGRADED" and pos.exit_filled_a == 12.0 and pos.filled_a == 20.0 and pos.filled_b == 2.0
    monkeypatch.setattr(execution, "DEGRADED_RETRY_S", 0.0)
    await h.ex.retry_degraded()
    assert pos.status == CLOSED and await sim.positions() == [] and await h.sim("mexc").positions() == []
    assert abs(pos.gross_pnl_usd) < 0.2                            # no fabricated P&L from a mis-booked leg


async def test_stray_fills_after_close_or_discard_alert_a_human(tmp_path):
    from bbo_trader.models import OrderEvent
    h = Harness(tmp_path)
    h.quote("blofin", 1.0041, 1.0061)
    h.quote("mexc", 1.0000, 1.0010)
    pos = await h.ex.enter_tm(Intent("TM_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                     rest_price=1.0061, size_usd=25.0))
    cid = pos.maker_client_id
    await h.ex.cancel_maker(pos, "test")
    await settle()
    assert pos not in h.book.open                                   # nothing filled: discarded
    h.ex.on_order_event(OrderEvent("blofin", cid, "sim-1", "filled", filled_qty=25.0, avg_price=1.0061, fee=0.005,
                                   liquidity="maker", ts=h.ex.clock()))
    await asyncio.sleep(0.01)
    assert any("STRAY_FILL_NO_POSITION" in n for n in h.notes)
    pos2 = await h.ex.enter_tm(Intent("TM_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                      rest_price=1.0061, size_usd=25.0))
    cid2 = pos2.maker_client_id
    await asyncio.sleep(0.005)
    h.quote("blofin", 1.0061, 1.0062, bq=100.0)
    await settle()
    h.quote("blofin", 1.0010, 1.0012)
    h.quote("mexc", 1.0009, 1.0011)
    await h.ex.exit_tt(pos2, "convergence")
    assert pos2.status == CLOSED
    h.ex.on_order_event(OrderEvent("blofin", cid2, "sim-2", "filled", filled_qty=25.0, avg_price=1.0061, fee=0.005,
                                   liquidity="maker", ts=h.ex.clock()))               # 5 more than we ever booked
    await asyncio.sleep(0.01)
    assert any("STRAY_FILL_AFTER_CLOSE" in n for n in h.notes) and pos2.filled_a == 20.0   # books untouched, human alerted


async def test_desync_with_a_stray_on_the_failed_leg_degrades_instead_of_closing(tmp_path, monkeypatch):
    from bbo_trader import execution
    from bbo_trader.models import OrderEvent
    h = Harness(tmp_path)
    h.quote("blofin", 1.0041, 1.0061)
    h.quote("mexc", 1.0000, 1.0010)
    sim = h.sim("blofin")
    pos = await h.ex.enter_tm(Intent("TM_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                     rest_price=1.0061, size_usd=25.0))
    old_cid = pos.maker_client_id
    h.quote("blofin", 1.0060, 1.0065)
    real_pm = sim.place_market

    async def reject_opening_orders(symbol, side, qty, reduce_only, client_id):   # the blofin TT leg is refused
        if not reduce_only:
            return OrderAck(False, error="venue busy")
        return await real_pm(symbol, side, qty, reduce_only, client_id)
    sim.place_market = reject_opening_orders
    await h.ex.upgrade_to_tt(pos)
    for _ in range(50):
        if pos.status == "TT_ENTERING":
            break
        await asyncio.sleep(0.001)
    assert pos.status == "TT_ENTERING"
    sim._pos[SYM] = sim._pos.get(SYM, 0.0) - 12.0                  # the cancelled maker order filled on the refused leg's venue
    h.ex.on_order_event(OrderEvent("blofin", old_cid, "sim-1", "filled", filled_qty=12.0, avg_price=1.0061,
                                   fee=0.0024, liquidity="maker", ts=h.ex.clock()))
    await settle()
    assert pos.status == "DEGRADED" and pos.filled_a == 12.0 and pos.exit_filled_b == pos.filled_b == 2.0
    assert pos in h.book.open                                      # the mexc leg was flattened, the blofin stray is still owned
    monkeypatch.setattr(execution, "DEGRADED_RETRY_S", 0.0)
    for _ in range(2):
        await h.ex.retry_degraded()
        await settle()
    assert pos.status == CLOSED and await sim.positions() == [] and await h.sim("mexc").positions() == []
```

- [x] **Step 3: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_execution_tt.py tests/test_execution_tm.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'bbo_trader.execution'`

- [x] **Step 4: Implement `bbo_trader/execution.py`**

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
  hedged the position opens (entry) or closes (exit). Entry hedge failure → flatten the maker fill; exit
  hedge failure → the remainder closes taker/taker.

Failure discipline: a venue exception leaves an order IN DOUBT (polled, then reported as rejected with error
"in_doubt" — never assumed filled); every close path decides on the REMAINING quantity, not on "some fill
arrived"; a fill on a superseded maker order (fill-after-cancel) is booked on the leg and the position handed
to `retry_degraded`, never written onto the live order's counters; a reduce-only order refused with
"nothing to reduce" books the leg closed once the venue confirms it holds nothing (LEG_FLAT_AT_VENUE).

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
from .models import (Intent, OrderAck, OrderEvent, Position, TT_ENTERING, MAKER_RESTING, HEDGING, OPEN,
                     EXIT_MAKER_RESTING, TT_EXITING, EXIT_HEDGING, DEGRADED, CLOSED)
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
MAX_CLOSE_RETRIES = 40           # ~20 min of DEGRADED retries, then the position waits for a human
HEDGING_SWEEP_AFTER_S = 2.0      # a HEDGING position whose maker order is still live this long gets its cancel retried
HEDGING_STUCK_S = 60.0           # cancel sent, no terminal event, the order query silent this long → DEGRADED (a human is told)
FEE_MISMATCH_TOL = 0.20          # spec: a fill fee > 20 % off the configured rate for its liquidity type is a FEE_MISMATCH
FEE_ALERT_EVERY_S = 3600.0       # one FEE_MISMATCH Telegram warning per venue per hour
TM_EXIT_SLIP_TOL_PCT = 0.05      # a TM exit realized this far above EXIT_SPREAD_PCT is `tm_exit_slipped`, not a take-profit
#                                  (the sweep runs inside retry_degraded, which the App calls from its 500 ms sweep)
MAX_CID_LEN = 32                 # MEXC externalOid / BloFin clientOrderId
STALE_CANCEL_COOLDOWN_S = 5.0    # a maker cancelled for stale quotes must not be re-posted on the very next quote
MAX_TRACKS = 5000                # maker tracks outlive their position (fill-after-cancel alerts); the OLDEST are dropped —
#                                  a live position's tracks are always among the newest, so the bound is safe in practice


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
    acked: bool = False       # the venue accepted the order at some point (a later "rejected" is post-rest)
    filled_seen: float = 0.0  # cumulative fill already processed for THIS order (stray-fill deltas)
    fee_seen: float = 0.0
    phase: str = ""           # maker orders: "entry" | "exit" as posted — survives pos.maker_venue being cleared


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
        self._notify_tasks: set[asyncio.Task] = set()   # never in the shutdown drain: a hung Telegram must not hold up order legs
        self.stopping = False          # set by App.shutdown(): no NEW order may rest or open once we are going down
        self._naked_alerted: set[int] = set()
        self._last_query: dict[int, float] = {}
        self._fee_alert_ts: dict[str, float] = {}

    # ---- plumbing ----------------------------------------------------------------
    def _new_cid(self, pos: Position, leg: str) -> str:
        cid = f"b{self.cfg.mode[0]}{pos.id}-{leg}-{next(self._seq)}"
        if len(cid) > MAX_CID_LEN:
            log.error("CID_TRUNCATED %s — idempotency key no longer unique", cid)
        return cid[:MAX_CID_LEN]

    def _forget(self, pos: Position) -> None:
        """Drop the taker tracks and the lock of a finished position. Maker tracks are kept (bounded) so a
        fill-after-cancel on a closed position is still recognised and raised to a human."""
        for cid in [c for c, t in self._tracks.items() if t.pos_id == pos.id and t.leg != "maker"]:
            del self._tracks[cid]
        while len(self._tracks) > MAX_TRACKS:
            self._tracks.pop(next(iter(self._tracks)))
        self._locks.pop(pos.id, None)
        self._naked_alerted.discard(pos.id)
        self._last_query.pop(pos.id, None)

    @staticmethod
    def _maker_phase(pos: Position) -> str:
        entry_side = "sell" if pos.maker_venue == pos.venue_a else "buy"
        return "entry" if pos.maker_side == entry_side else "exit"

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
            t = asyncio.get_running_loop().create_task(self._guard(self.notify(text)))
            self._notify_tasks.add(t)
            t.add_done_callback(self._notify_tasks.discard)

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
            if ev.filled_qty > 0:
                log.error("ORDER_EVENT_UNKNOWN %s %s %s filled=%s — an order we do not track has filled: reconcile by hand",
                          ev.venue, ev.client_id, ev.state, ev.filled_qty)
                self._say(f"UNKNOWN_FILL {ev.venue} {ev.client_id}: {ev.filled_qty} contracts")
            else:
                log.info("ORDER_EVENT_UNKNOWN %s %s %s", ev.venue, ev.client_id, ev.state)
            return
        tr.last = ev
        if ev.state != "rejected":
            tr.acked = True
        pos = self.book.get(tr.pos_id, include_closed=True)
        if pos is None and ev.filled_qty > tr.filled_seen + 1e-12:      # a discarded position's order filled after all
            log.error("STRAY_FILL_NO_POSITION #%d %s %s: %s contracts on %s — reconcile by hand",
                      tr.pos_id, tr.symbol, tr.venue, ev.filled_qty - tr.filled_seen, ev.client_id)
            self._say(f"STRAY_FILL_NO_POSITION #{tr.pos_id} {tr.symbol} {tr.venue}: {ev.filled_qty - tr.filled_seen} contracts")
            tr.filled_seen = ev.filled_qty
        if pos is not None:
            if ev.order_id:
                pos.order_ids[tr.leg] = ev.order_id
                if tr.leg == "maker" and ev.client_id == pos.maker_client_id:
                    pos.maker_order_id = ev.order_id
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
        """Market order + its terminal event. `OrderAck.ok=False` is a definitive venue rejection (live adapters
        raise on transport/envelope failures instead); an exception leaves the order IN DOUBT — the venue is
        polled for it and, if it stays unknown, the leg is reported rejected with error "in_doubt" rather than
        assumed filled. Callers then flatten/retry with reduce-only orders, which the venue refuses with
        "nothing to reduce" if the doubted order did fill after all (→ LEG_FLAT_AT_VENUE)."""
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
        in_doubt = False
        try:
            ack = await v.trading.place_market(pos.symbol, side, qty, reduce_only, cid)
        except Exception as e:  # noqa: BLE001 — the venue may or may not hold this order
            log.error("ORDER_IN_DOUBT #%d %s %s: %r — polling the venue", pos.id, venue, leg, e)
            self._note_rate_limit(venue, repr(e))
            ack = OrderAck(True, "", error=f"in_doubt: {e!r}")
            in_doubt = True
        ack_ms = (asyncio.get_running_loop().time() - t0) * 1000.0
        self.metrics.record("submit_to_ack", ack_ms)
        self.book.audit_order({"ts": now, "pos": pos.id, "leg": leg, "venue": venue, "side": side, "qty": qty,
                               "type": "market", "reduce_only": reduce_only, "client_id": cid,
                               "ok": ack.ok and not in_doubt, "error": ack.error})
        if not ack.ok:
            self._note_rate_limit(venue, ack.error)
            log.warning("ORDER_REJECTED #%d %s %s: %s", pos.id, venue, leg, ack.error)
            return OrderEvent(venue, cid, "", "rejected", error=ack.error or "rejected", ts=self.clock())
        if ack.order_id:
            pos.order_ids[leg] = ack.order_id
        ev = await self._await_terminal(tr, v, cid, ack.order_id, qty, assume_filled=not in_doubt)
        pos.latency_ms[leg] = (asyncio.get_running_loop().time() - t0) * 1000.0
        if ev.state == "filled":
            self.metrics.record("submit_to_fill", pos.latency_ms[leg])
        return ev

    async def _await_terminal(self, tr: LegTrack, v: Venue, cid: str, order_id: str, qty: float,
                              assume_filled: bool = True) -> OrderEvent:
        try:
            return await asyncio.wait_for(asyncio.shield(tr.done), timeout=EVENT_GRACE_S)
        except asyncio.TimeoutError:
            pass
        loop = asyncio.get_running_loop()
        deadline = loop.time() + POLL_MAX_S
        while loop.time() < deadline:
            if tr.done.done():
                return tr.done.result()
            try:
                ev = await v.trading.query_order(tr.symbol, cid, order_id)
            except Exception as e:  # noqa: BLE001
                log.debug("query_order failed %s %s: %r", v.name, cid, e)
                ev = None
            if ev is not None and ev.terminal:
                return ev
            await self._sleep(POLL_S)
        if tr.done.done():
            return tr.done.result()
        if not assume_filled:
            last = tr.last
            if last is not None and last.filled_qty > 0:      # the feed showed a partial: book what we know
                log.error("ORDER_UNRESOLVED %s %s: in doubt, %s filled so far — booking that, the rest is unknown",
                          v.name, cid, last.filled_qty)
                self._say(f"ORDER_UNRESOLVED {v.name} {cid}: {last.filled_qty} filled, remainder unknown — check the venue")
                return OrderEvent(v.name, cid, order_id, "filled", filled_qty=last.filled_qty, avg_price=last.avg_price,
                                  fee=last.fee, liquidity="taker", ts=self.clock(), error="in_doubt")
            log.error("ORDER_UNRESOLVED %s %s: in doubt and unknown to the venue after %.1fs — treated as rejected; "
                      "reconcile by hand if the venue shows it", v.name, cid, POLL_MAX_S)
            self._say(f"ORDER_UNRESOLVED {v.name} {cid}: check the venue for a stray {tr.leg} order")
            return OrderEvent(v.name, cid, order_id, "rejected", error="in_doubt", ts=self.clock())
        log.warning("FILL_ASSUMED %s %s: no terminal state after %.1fs, trusting submitted qty",
                    v.name, cid, POLL_MAX_S)
        last = tr.last
        return OrderEvent(v.name, cid, order_id, "filled", filled_qty=qty,
                          avg_price=last.avg_price if last else 0.0, fee=last.fee if last else 0.0,
                          liquidity="taker", ts=self.clock())

    def _as_event(self, result, venue: str) -> OrderEvent:
        if isinstance(result, OrderEvent):
            return result
        log.error("LEG_EXCEPTION %s: %r", venue, result)
        return OrderEvent(venue, "", "", "rejected", error=f"exception: {result!r}", ts=self.clock())

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
        res = await asyncio.gather(
            self._place_taker(pos, a, "entry_a", "sell", pos.qty_a, False),
            self._place_taker(pos, b, "entry_b", "buy", pos.qty_b, False), return_exceptions=True)
        ev_a, ev_b = self._as_event(res[0], a), self._as_event(res[1], b)
        ok_a = ev_a.state == "filled" and ev_a.filled_qty > 0
        ok_b = ev_b.state == "filled" and ev_b.filled_qty > 0
        now = self.clock()
        if ok_a:                                   # accumulate: a stray maker fill may already sit on the leg
            self._accumulate_leg(pos, a, "entry", ev_a)
        if ok_b:
            self._accumulate_leg(pos, b, "entry", ev_b)
        if pos.status == DEGRADED:                 # degraded while the legs were in flight: retry_degraded owns it
            self._resize_from_legs(pos, spec_a, spec_b)
            self.book.dirty = True
            return None
        if ok_a and ok_b:
            self._resize_from_legs(pos, spec_a, spec_b)
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
            self._resize_from_legs(pos, spec_a, spec_b)
            self.risk.record_strike(sym, a, b)
            self.risk.set_cooldown(sym)
            rem = round((pos.filled_a - pos.exit_filled_a) if leg == "a" else (pos.filled_b - pos.exit_filled_b), 10)
            if await self._flatten_leg(pos, leg, rem) and pos.status != DEGRADED:
                rem_a = round(pos.filled_a - pos.exit_filled_a, 10)     # a stray maker fill may sit on the OTHER leg
                rem_b = round(pos.filled_b - pos.exit_filled_b, 10)
                if rem_a <= 1e-9 and rem_b <= 1e-9:
                    self._close(pos, "failed_entry")
                else:
                    log.error("DESYNC_NOT_FLAT #%d %s rem_a=%s rem_b=%s — DEGRADED", pos.id, sym, rem_a, rem_b)
                    transition(pos, DEGRADED)
                    pos.degraded_leg = "a" if rem_a > 1e-9 else "b"
                    pos.last_close_attempt = self.clock()
                    self.book.dirty = True
            elif pos.status != DEGRADED:
                transition(pos, DEGRADED)
                pos.degraded_leg = leg
                pos.last_close_attempt = self.clock()
                self.book.dirty = True
            return None
        log.warning("ENTRY_FAILED #%d %s both legs: %s / %s", pos.id, sym, ev_a.error, ev_b.error)
        self.risk.record_strike(sym, a, b)
        self.risk.set_cooldown(sym)
        if pos.filled_a > 1e-12 or pos.filled_b > 1e-12:       # something (a stray maker fill) is on the books
            self._resize_from_legs(pos, spec_a, spec_b)
            if pos.status != DEGRADED:
                transition(pos, DEGRADED)
                pos.degraded_leg = "a" if pos.filled_a > 1e-12 else "b"
                pos.last_close_attempt = self.clock()
                self.book.dirty = True
            return None
        self.book.discard(pos)
        self._forget(pos)
        return None

    async def _flatten_leg(self, pos: Position, leg: str, qty: float) -> bool:
        """Market-close one entry leg (reduce-only) with the retry ladder, continuing with whatever is left
        after a partial fill. Records exit price/fees; False leaves the remainder to retry_degraded."""
        venue = pos.venue_a if leg == "a" else pos.venue_b
        side = "buy" if leg == "a" else "sell"
        remaining = round(qty, 10)
        for i, delay in enumerate(FLATTEN_LADDER_S):
            ev = await self._place_taker(pos, venue, f"flat_{leg}{i}", side, remaining, True, priority=True)
            if ev.state == "filled" and ev.filled_qty > 0:
                self._accumulate_leg(pos, venue, "exit", ev)
                remaining = round(remaining - ev.filled_qty, 10)
                if remaining <= 1e-9:
                    log.info("FLATTEN #%d %s %s ok qty=%s px=%s", pos.id, venue, leg, qty, ev.avg_price)
                    return True
                log.warning("FLATTEN_PARTIAL #%d %s %s filled=%s remaining=%s", pos.id, venue, leg, ev.filled_qty, remaining)
                continue
            if "nothing to reduce" in (ev.error or "") and await self._leg_flat_at_venue(pos, venue):
                self._book_leg_flat(pos, leg, venue)
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
            self._forget(pos)
            return None
        return pos

    async def _post_maker(self, pos: Position, reduce_only: bool, keep_posted_ts: bool = False) -> bool:
        if self.stopping:                      # a requote or entry landing mid-shutdown must not leave a fresh order resting
            log.info("STOPPING #%d %s: maker order not posted", pos.id, pos.symbol)
            return False
        v = self.venues[pos.maker_venue]
        now = self.clock()
        if not v.budget.try_take("order", now):
            self.metrics.funnel["budget"] += 1
            return False
        cid = self._new_cid(pos, "maker")
        self._tracks[cid] = LegTrack(pos.id, "maker", v.name, pos.symbol, pos.maker_qty, now,
                                     phase="exit" if reduce_only else "entry")
        pos.maker_client_id = cid
        pos.client_ids["maker"] = cid
        if not (keep_posted_ts and pos.maker_posted_ts > 0.0):
            pos.maker_posted_ts = now          # a cancel+new requote keeps the original TTL clock
        pos.maker_last_requote_ts = now
        pos.maker_cancel_sent = False
        pos.maker_filled_qty = 0.0
        pos.hedged_qty = 0.0
        pos.maker_fee_usd = 0.0
        pos.maker_avg_price = 0.0
        pos.maker_booked_qty = 0.0
        pos.maker_booked_fee = 0.0
        try:
            ack = await v.trading.place_post_only(pos.symbol, pos.maker_side, pos.maker_qty, pos.maker_rest_price,
                                                  reduce_only, cid)
        except Exception as e:  # noqa: BLE001 — in doubt: the venue may hold a resting order we did not see acked
            log.error("TM_POST_IN_DOUBT #%d %s: %r — querying the venue", pos.id, v.name, e)
            ack = await self._resolve_doubtful_post(v, pos, cid)
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

    async def _resolve_doubtful_post(self, v: Venue, pos: Position, cid: str) -> OrderAck:
        try:
            ev = await v.trading.query_order(pos.symbol, cid, "")
        except Exception as e:  # noqa: BLE001
            log.error("TM_POST_UNRESOLVED #%d %s %s: %r — check the venue for a stray resting order", pos.id, v.name, cid, e)
            self._say(f"TM_POST_UNRESOLVED #{pos.id} {pos.symbol} {v.name}: check for a stray resting order {cid}")
            return OrderAck(False, error="in_doubt")
        if ev is None or ev.state == "rejected":
            return OrderAck(False, error="in_doubt: not at venue")
        return OrderAck(True, ev.order_id)          # it exists (resting or already filled): events will follow

    async def requote(self, pos: Position, new_price: float) -> None:
        v = self.venues[pos.maker_venue]
        now = self.clock()
        if pos.maker_cancel_sent or pos.status not in (MAKER_RESTING, EXIT_MAKER_RESTING):
            return
        if v.trading.supports_amend:
            if not v.budget.try_take("amend", now):
                self.metrics.funnel["requote_budget"] += 1      # invisible starvation otherwise (shared BloFin bucket)
                return
            pos.maker_last_requote_ts = now    # stamped on the attempt: a failing amend must not retry every quote
            try:
                ack = await v.trading.amend(pos.symbol, pos.maker_client_id, pos.maker_order_id, new_price)
            except Exception as e:  # noqa: BLE001
                ack = OrderAck(False, pos.maker_order_id, error=f"exception: {e!r}")
            if ack.ok:
                log.info("TM_REQUOTE #%d %s %s -> %s", pos.id, v.name, pos.maker_rest_price, new_price)
                pos.maker_rest_price = new_price
            else:
                self._note_rate_limit(v.name, ack.error)
                log.info("TM_REQUOTE_FAILED #%d %s: %s", pos.id, v.name, ack.error)
            return
        if v.budget.available("cancel", now) < 1 or v.budget.available("order", now) < 1:
            self.metrics.funnel["requote_budget"] += 1
            return
        pos.requote_pending = True
        pos.requote_price = new_price
        pos.maker_last_requote_ts = now        # on the attempt (see the amend path)
        await self._cancel_maker_order(pos, priority=False)   # a failed cancel clears requote_pending itself

    async def cancel_maker(self, pos: Position, reason: str) -> None:
        log.info("TM_CANCEL #%d %s reason=%s", pos.id, pos.maker_venue, reason)
        if reason == "stale":
            self.risk.set_cooldown(pos.symbol, STALE_CANCEL_COOLDOWN_S)
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
        try:
            ack = await v.trading.cancel(pos.symbol, pos.maker_client_id, pos.maker_order_id)
        except Exception as e:  # noqa: BLE001
            ack = OrderAck(False, pos.maker_order_id, error=f"exception: {e!r}")
        if ack.ok:
            return True
        self._note_rate_limit(v.name, ack.error)
        # already terminal at the venue (filled or gone)? pull the terminal state ourselves
        try:
            ev = await v.trading.query_order(pos.symbol, pos.maker_client_id, pos.maker_order_id)
        except Exception as e:  # noqa: BLE001
            log.debug("query_order failed %s %s: %r", v.name, pos.maker_client_id, e)
            ev = None
        if ev is not None and ev.terminal:
            self.on_order_event(ev)
            return False
        # the order may still be live (transport error, venue hiccup): nothing is in flight, let the strategy retry
        log.warning("CANCEL_FAILED #%d %s %s: %s — will retry", pos.id, v.name, pos.maker_client_id, ack.error)
        pos.maker_cancel_sent = False
        pos.requote_pending = False
        pos.upgrade_pending = False
        return False

    def _on_maker_event(self, pos: Position, tr: LegTrack, ev: OrderEvent) -> None:
        if ev.state == "ack" or (ev.state == "rejected" and not tr.acked):
            return          # a pre-rest (post-only would cross) rejection is handled by _post_maker off the OrderAck
        delta = round(ev.filled_qty - tr.filled_seen, 10)
        fee_delta = max(0.0, ev.fee - tr.fee_seen)
        tr.filled_seen, tr.fee_seen = max(tr.filled_seen, ev.filled_qty), max(tr.fee_seen, ev.fee)
        if ev.client_id != pos.maker_client_id or pos.status not in (MAKER_RESTING, HEDGING, EXIT_MAKER_RESTING, EXIT_HEDGING):
            # fill-after-cancel on a superseded order, or a position no longer in the maker flow: never write it
            # onto the live order's counters — book the real exposure on the leg (the track remembers the venue
            # and the phase the order was posted in) and hand the position to retry_degraded
            if delta > 1e-12 and pos.status != CLOSED:
                leg = "a" if tr.venue == pos.venue_a else "b"
                why = "superseded order" if ev.client_id != pos.maker_client_id else pos.status
                log.error("TM_STRAY_FILL #%d %s %s: %s contracts @%s on %s (%s) — booked on the %s %s leg for unwinding",
                          pos.id, pos.symbol, tr.venue, delta, ev.avg_price, ev.client_id, why, tr.phase, leg)
                self._accumulate_leg(pos, tr.venue, tr.phase,
                                     OrderEvent(ev.venue, ev.client_id, ev.order_id, ev.state, delta, ev.avg_price, fee_delta))
                self.book.dirty = True
                if pos.status != DEGRADED:
                    self._spawn(self._degrade(pos, leg, "stray maker fill"))
            elif delta > 1e-12:
                log.error("TM_STRAY_FILL_AFTER_CLOSE #%d %s %s: %s contracts on %s — reconcile by hand",
                          pos.id, pos.symbol, tr.venue, delta, ev.client_id)
                self._say(f"STRAY_FILL_AFTER_CLOSE #{pos.id} {pos.symbol} {tr.venue}: {delta} contracts")
            return
        if ev.state == "rejected":
            log.warning("TM_REJECTED_WHILE_RESTING #%d %s %s: %s", pos.id, pos.symbol, pos.maker_venue, ev.error)
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

    async def _degrade(self, pos: Position, leg: str, why: str) -> None:
        """Hand a maker-flow position to retry_degraded: cancel anything still resting, book the live order's
        fill on the leg, walk the state machine to DEGRADED."""
        async with self._lock(pos.id):
            if pos.status in (CLOSED, DEGRADED):
                return
            log.error("DEGRADE #%d %s leg %s: %s", pos.id, pos.symbol, leg, why)
            if pos.status in (MAKER_RESTING, EXIT_MAKER_RESTING):
                await self._cancel_maker_order(pos, priority=True)
                if pos.status in (MAKER_RESTING, EXIT_MAKER_RESTING):      # a fill event may have moved it meanwhile
                    transition(pos, HEDGING if pos.status == MAKER_RESTING else EXIT_HEDGING)
            if pos.status in (HEDGING, EXIT_HEDGING):
                self._apply_maker_leg(pos, self._maker_phase(pos))
            if pos.status != DEGRADED:
                transition(pos, DEGRADED)
            pos.degraded_leg = leg
            pos.last_close_attempt = self.clock()
            self.book.dirty = True
            self._say(f"DEGRADED #{pos.id} {pos.symbol}: {why}")

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
                if not terminal:                                        # first fill cancels the remainder
                    if not await self._cancel_maker_order(pos, priority=True) and not pos.maker_cancel_sent:
                        await self._sleep(HEDGE_RETRY_S)
                        if not await self._cancel_maker_order(pos, priority=True) and not pos.maker_cancel_sent:
                            log.error("HEDGE_CANCEL_FAILED #%d %s: remainder still resting on %s — the sweep retries",
                                      pos.id, pos.symbol, mv)
                side = self._hedge_side(pos, phase)
                q_h = self.board.fresh(hv, pos.symbol, self.clock())
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
                        self.risk.record_strike(pos.symbol, pos.venue_a, pos.venue_b)
                        if phase == "entry":
                            log.error("HEDGE_FAILED #%d %s on %s — flattening the maker fill", pos.id, pos.symbol, hv)
                            await self._flatten_maker_fill(pos, lots_floor(unhedged, spec_m))
                        else:                          # an exit maker fill IS progress: the rest closes taker/taker
                            log.error("HEDGE_FAILED #%d %s exit hedge on %s — the remainder closes taker/taker",
                                      pos.id, pos.symbol, hv)
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
                    if to_flat > 0 and phase == "entry":
                        log.warning("RESIDUAL #%d %s %s maker contracts unhedged (matched %s) — flattening %s",
                                    pos.id, pos.symbol, residual, pos.hedged_qty, to_flat)
                        await self._flatten_maker_fill(pos, to_flat)
                    elif to_flat > 0:
                        log.warning("RESIDUAL #%d %s %s exit maker contracts unhedged — the remainder closes taker/taker",
                                    pos.id, pos.symbol, residual)
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

    async def _flatten_maker_fill(self, pos: Position, qty: float) -> None:
        """Undo an unhedgeable/unhedged ENTRY maker fill on the maker venue itself (reduce-only market). The
        round trip is netted out of the maker leg; its realized P&L lands in `pnl_adjust_usd`."""
        if qty <= 0:
            return
        side = "buy" if pos.maker_side == "sell" else "sell"
        sign = 1.0 if pos.maker_side == "sell" else -1.0
        spec_m = self._spec(pos.maker_venue, pos.symbol)
        cs = spec_m.contract_size if spec_m is not None else 1.0
        remaining = round(qty, 10)
        for i, delay in enumerate(FLATTEN_LADDER_S[:3]):
            ev = await self._place_taker(pos, pos.maker_venue, f"mflat{i}", side, remaining, True, priority=True)
            if ev.state == "filled" and ev.filled_qty > 0:
                # the part of the fill never booked on the leg is realized here; a part already booked (defensive:
                # bookings and flattens share the position lock, so this should not happen) is booked as the leg's
                # exit fill so the legs stay consistent
                unbooked = max(0.0, min(ev.filled_qty, round(pos.maker_filled_qty - pos.maker_booked_qty, 10)))
                booked_part = round(ev.filled_qty - unbooked, 10)
                if unbooked > 0:
                    pos.pnl_adjust_usd += sign * (pos.maker_avg_price - ev.avg_price) * unbooked * cs
                if booked_part > 1e-12:
                    self._accumulate_leg(pos, pos.maker_venue, "exit",
                                         OrderEvent(ev.venue, ev.client_id, ev.order_id, "filled", booked_part, ev.avg_price, 0.0, "taker"))
                    pos.maker_booked_qty = round(pos.maker_booked_qty - booked_part, 10)
                pos.exit_fees_usd += ev.fee
                pos.maker_filled_qty = round(pos.maker_filled_qty - ev.filled_qty, 10)   # netted out
                remaining = round(remaining - ev.filled_qty, 10)
                log.info("FLATTEN #%d maker leg %s qty=%s @%s", pos.id, pos.maker_venue, ev.filled_qty, ev.avg_price)
                if remaining <= 1e-9:
                    return
                continue
            await self._sleep(delay)
        log.error("FLATTEN_FAILED #%d maker leg %s qty=%s — DEGRADED", pos.id, pos.maker_venue, remaining)
        if pos.status in (MAKER_RESTING, HEDGING, EXIT_MAKER_RESTING, EXIT_HEDGING):
            self._apply_maker_leg(pos, self._maker_phase(pos))     # what is left of the fill must be on the books
        if pos.status in (MAKER_RESTING, EXIT_MAKER_RESTING):
            transition(pos, HEDGING if pos.status == MAKER_RESTING else EXIT_HEDGING)
        if pos.status != DEGRADED:
            transition(pos, DEGRADED)
        pos.degraded_leg = "a" if pos.maker_venue == pos.venue_a else "b"
        pos.last_close_attempt = self.clock()
        self.book.dirty = True

    def _check_fee(self, pos: Position, venue: str, ev: OrderEvent) -> None:
        """Spec: a fill whose fee is more than FEE_MISMATCH_TOL off the configured rate for its liquidity type is a
        FEE_MISMATCH — the edge math runs on those rates, so a changed schedule must be seen, not absorbed."""
        spec = self._spec(venue, pos.symbol)
        if spec is None or ev.filled_qty <= 0 or ev.avg_price <= 0 or ev.liquidity not in ("maker", "taker"):
            return
        fees = self.venues[venue].fees
        rate = fees.maker if ev.liquidity == "maker" else fees.taker
        expected = notional(ev.filled_qty, ev.avg_price, spec) * rate / 100.0
        if abs(ev.fee - expected) <= max(FEE_MISMATCH_TOL * expected, 1e-6):
            return
        self.metrics.funnel["fee_mismatch"] += 1
        log.warning("FEE_MISMATCH %s %s %s fill: fee $%.6f, configured %.3f%% => $%.6f (%s)", venue, pos.symbol, ev.liquidity,
                    ev.fee, rate, expected, ev.client_id)
        now = self.clock()
        if now - self._fee_alert_ts.get(venue, float("-inf")) >= FEE_ALERT_EVERY_S:
            self._fee_alert_ts[venue] = now
            self._say(f"FEE_MISMATCH {venue} {ev.liquidity}: paid ${ev.fee:.6f} vs configured {rate:.3f}% (${expected:.6f}) on {pos.symbol}")

    def _accumulate_leg(self, pos: Position, venue: str, phase: str, ev: OrderEvent) -> None:
        self._check_fee(pos, venue, ev)
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
        """Book the resting order's fill and fee on its leg — incrementally, so it can be called at finalize, at
        degrade time and after a stray fill without double counting or masking a later fill."""
        dq = max(0.0, round(pos.maker_filled_qty - pos.maker_booked_qty, 10))
        dfee = max(0.0, pos.maker_fee_usd - pos.maker_booked_fee)
        if dq > 1e-12 or dfee > 0:
            self._accumulate_leg(pos, pos.maker_venue, phase,
                                 OrderEvent(pos.maker_venue, pos.maker_client_id, pos.maker_order_id, "filled",
                                            dq, pos.maker_avg_price, dfee, "maker"))
        pos.maker_booked_qty = max(pos.maker_booked_qty, pos.maker_filled_qty)
        pos.maker_booked_fee = max(pos.maker_booked_fee, pos.maker_fee_usd)
        pos.fee_liquidity["maker"] = "maker"

    def _resize_from_legs(self, pos: Position, spec_a, spec_b) -> None:
        """Matched size from the legs actually on the books; a one-legged unwind still has a real size."""
        sizes = [n for n in (notional(pos.filled_a, pos.entry_price_a, spec_a) if spec_a else 0.0,
                             notional(pos.filled_b, pos.entry_price_b, spec_b) if spec_b else 0.0) if n > 0]
        pos.size_usd = min(sizes) if sizes else pos.size_usd

    async def _finalize_maker(self, pos: Position, phase: str) -> None:
        now = self.clock()
        spec_a, spec_b = self._spec(pos.venue_a, pos.symbol), self._spec(pos.venue_b, pos.symbol)
        if phase == "entry":
            if pos.status == DEGRADED:
                self._apply_maker_leg(pos, "entry")         # whatever filled must be on the books for retry_degraded
                self._resize_from_legs(pos, spec_a, spec_b)
                return
            if pos.maker_filled_qty <= 1e-12:
                if pos.status == HEDGING:                   # the fill was flattened: a round trip, not a discard...
                    self._apply_maker_leg(pos, "entry")
                    rem_a = round(pos.filled_a - pos.exit_filled_a, 10)
                    rem_b = round(pos.filled_b - pos.exit_filled_b, 10)
                    if rem_a <= 1e-9 and rem_b <= 1e-9:
                        self._close(pos, "hedge_unwound")
                    else:                                   # ...unless something else (a stray fill) sits on a leg
                        log.error("UNWOUND_BUT_NOT_FLAT #%d %s rem_a=%s rem_b=%s — DEGRADED", pos.id, pos.symbol, rem_a, rem_b)
                        transition(pos, DEGRADED)
                        pos.degraded_leg = "a" if rem_a > 1e-9 else "b"
                        pos.last_close_attempt = self.clock()
                        self.book.dirty = True
                    return
                if pos.requote_pending and pos.status == MAKER_RESTING:
                    pos.requote_pending = False
                    pos.upgrade_pending = False        # an upgrade asked for mid-requote is stale: re-evaluate fresh
                    pos.maker_rest_price = pos.requote_price
                    if await self._post_maker(pos, reduce_only=False, keep_posted_ts=True):
                        return
                if pos.upgrade_pending and pos.status == MAKER_RESTING:
                    pos.upgrade_pending = False
                    if self.stopping:              # no NEW position may open during shutdown: the discard below
                        log.info("STOPPING #%d %s: TT upgrade dropped", pos.id, pos.symbol)
                    else:
                        pos.mode = "TT"
                        qa, qb = self.board.get(pos.venue_a, pos.symbol), self.board.get(pos.venue_b, pos.symbol)
                        if qa is not None and qb is not None and spec_a is not None and spec_b is not None:
                            legs = size_pair(pos.size_usd, qa.bid, qb.ask, spec_a, spec_b, self.cfg.max_leg_mismatch_pct,
                                             min_usd=self.cfg.min_position_usd)     # re-size at the CURRENT touch
                            if legs is not None:
                                pos.qty_a, pos.qty_b = legs.qty_a, legs.qty_b
                        transition(pos, TT_ENTERING)
                        await self._tt_legs(pos)
                        return
                self.metrics.funnel["maker_cancelled"] += 1
                self.book.discard(pos)
                self._forget(pos)
                return
            self._apply_maker_leg(pos, "entry")
            self._resize_from_legs(pos, spec_a, spec_b)
            pos.entry_spread_pct = (spread_pct(pos.entry_price_a, pos.entry_price_b)
                                    if pos.entry_price_a > 0 and pos.entry_price_b > 0 else pos.detect_spread_pct)
            pos.peak_spread_pct = abs(pos.entry_spread_pct)
            pos.entry_time = now
            if pos.status == MAKER_RESTING:
                transition(pos, HEDGING)
            transition(pos, OPEN)
            if pos.signal_ts:                     # includes the resting time: kept apart from the TT figure
                self.metrics.record("signal_to_open_tm", (now - pos.signal_ts) * 1000.0)
            log.info("OPEN #%d %s TM maker=%s fill_spread=%.3f%% size=$%.2f fees=$%.4f", pos.id, pos.symbol,
                     pos.maker_venue, pos.entry_spread_pct, pos.size_usd, pos.entry_fees_usd)
            self._say(f"OPEN #{pos.id} {pos.symbol} TM maker {pos.maker_venue} spread {pos.entry_spread_pct:.3f}% ${pos.size_usd:.2f}")
            self.book.dirty = True
            if pos.entry_spread_pct < self.cfg.min_fill_spread_pct:
                # same guard as the TT legs: a maker that filled because the book moved through it, hedged at a
                # market that had already gone, is the legacy bot's inverted-fill loss — close it, cool the symbol
                log.warning("FILL_QUALITY_ABORT #%d %s TM fill_spread=%.3f%% < %.3f%%", pos.id, pos.symbol,
                            pos.entry_spread_pct, self.cfg.min_fill_spread_pct)
                self.risk.set_cooldown(pos.symbol)
                self._spawn(self.exit_tt(pos, "fill_quality_abort"))   # spawned: we hold the position lock here
            return
        # exit phase
        if pos.status == DEGRADED:
            self._apply_maker_leg(pos, "exit")
            return
        if pos.maker_filled_qty <= 1e-12:
            tr = self._tracks.get(pos.maker_client_id)
            if (tr is not None and tr.last is not None and tr.last.state == "rejected"
                    and "nothing to reduce" in (tr.last.error or "")
                    and await self._leg_flat_at_venue(pos, pos.maker_venue)):
                # the venue holds nothing on the maker leg: book it closed at the mark, close the other leg TT
                self._book_leg_flat(pos, "a" if pos.maker_venue == pos.venue_a else "b", pos.maker_venue)
                pos.requote_pending = False
                pos.exit_reason = pos.exit_reason or "venue_flat"
                transition(pos, TT_EXITING)
                await self._close_remainder_tt(pos)
                return
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
        self._close(pos, pos.exit_reason or self._tm_exit_reason(pos))

    def _tm_exit_reason(self, pos: Position) -> str:
        """Label a completed TM exit by what it REALIZED: the maker leg rested at the target but the hedge crossed a
        market that may have moved — a spread far above EXIT_SPREAD_PCT is a slipped exit, not a take-profit."""
        if pos.exit_price_a > 0 and pos.exit_price_b > 0:
            realized = spread_pct(pos.exit_price_a, pos.exit_price_b)
            if realized > self.cfg.exit_spread_pct + TM_EXIT_SLIP_TOL_PCT:
                self.metrics.funnel["tm_exit_slipped"] += 1
                return "tm_exit_slipped"
        return "take_profit"

    # ---- exits ---------------------------------------------------------------------
    async def exit_tm(self, pos: Position, intent: Intent) -> bool:
        """Rest the exit's maker leg. `_post_maker` zeroes the maker counters (`maker_filled_qty`, `hedged_qty`,
        `maker_booked_qty`, fees) on every post, so on an EXIT_MAKER_RESTING position they describe the EXIT fill —
        `App._adopt_transients` relies on that to tell a filled exit maker from an unfilled one."""
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
        if pos.status not in (OPEN, EXIT_MAKER_RESTING):
            log.info("EXIT_IGNORED #%d %s status=%s reason=%s (in flight; the next evaluation retries)",
                     pos.id, pos.symbol, pos.status, reason)
            return
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
        results = await asyncio.gather(*(j[1] for j in jobs), return_exceptions=True) if jobs else []
        for (leg, _), r in zip(jobs, results):
            venue = pos.venue_a if leg == "a" else pos.venue_b
            ev = self._as_event(r, venue)
            if ev.state == "filled" and ev.filled_qty > 0:
                self._accumulate_leg(pos, venue, "exit", ev)
            elif "nothing to reduce" in (ev.error or "") and await self._leg_flat_at_venue(pos, venue):
                self._book_leg_flat(pos, leg, venue)
        failed = ""
        for leg, _ in jobs:                       # decide on what is LEFT, not on "some fill arrived"
            rem = round((pos.filled_a - pos.exit_filled_a) if leg == "a" else (pos.filled_b - pos.exit_filled_b), 10)
            if rem > 1e-9:
                failed += leg
        if failed:
            log.warning("CLOSE_DEGRADED #%d %s legs=%s", pos.id, pos.symbol, failed)
            if pos.status != DEGRADED:
                transition(pos, DEGRADED)
            pos.degraded_leg = "both" if failed == "ab" else failed
            pos.close_retry_count += 1
            pos.last_close_attempt = self.clock()
            self.book.dirty = True
            return
        self._close(pos, pos.exit_reason or "convergence")

    async def _leg_flat_at_venue(self, pos: Position, venue: str) -> bool:
        """A reduce-only close was refused with "nothing to reduce": does the venue really hold no position?"""
        try:
            held = await self.venues[venue].trading.positions()
        except Exception as e:  # noqa: BLE001
            log.warning("POSITIONS_QUERY_FAILED %s: %r", venue, e)
            return False
        return not any(p.symbol == pos.symbol for p in held)

    def _book_leg_flat(self, pos: Position, leg: str, venue: str) -> None:
        """The venue holds nothing for this leg although we do: book it closed at the mark so the position can
        finish, and say so loudly — our accounting and the venue disagreed (Plan 2 reconciliation owns this)."""
        q = self.board.get(venue, pos.symbol)
        mark = q.mid if q is not None else (pos.entry_price_a if leg == "a" else pos.entry_price_b)
        log.error("LEG_FLAT_AT_VENUE #%d %s leg %s on %s: venue holds no position — booked closed at mark %s",
                  pos.id, pos.symbol, leg, venue, mark)
        q_attr, p_attr, total = (("exit_filled_a", "exit_price_a", pos.filled_a) if leg == "a"
                                 else ("exit_filled_b", "exit_price_b", pos.filled_b))
        done, px = getattr(pos, q_attr), getattr(pos, p_attr)     # the remainder at the mark, a partial exit fill kept
        rem = max(0.0, round(total - done, 10))
        setattr(pos, p_attr, (px * done + mark * rem) / total if total > 0 else mark)
        setattr(pos, q_attr, total)

    def _check_naked(self, pos: Position, now: float) -> None:
        """Spec: naked exposure is bounded by MAX_NAKED_MS — alert (once per position) when a maker fill has sat
        unhedged longer than that; the hedge/flatten ladder keeps running, this is the operator's signal."""
        unhedged = round(pos.maker_filled_qty - pos.hedged_qty, 10)
        if unhedged <= 1e-12 or not pos.maker_fill_ts or pos.id in self._naked_alerted:
            return
        naked_ms = (now - pos.maker_fill_ts) * 1000.0
        if naked_ms > self.cfg.max_naked_ms:
            self._naked_alerted.add(pos.id)
            self.metrics.funnel["naked_exposure"] += 1
            log.warning("NAKED_EXPOSURE #%d %s: %s maker contracts on %s unhedged for %.0f ms (> %d ms)", pos.id, pos.symbol,
                        unhedged, pos.maker_venue, naked_ms, self.cfg.max_naked_ms)
            self._say(f"NAKED_EXPOSURE #{pos.id} {pos.symbol}: {unhedged} contracts unhedged for {naked_ms:.0f} ms")

    async def _sweep_stuck_hedging(self, now: float) -> None:
        """A HEDGING/EXIT_HEDGING position whose maker order is still live: retry the cancel (the one inside
        _hedge_delta failed) so no more fills arrive. If the cancel WAS sent but no terminal event ever came (private
        feed dropped, venue status lag — both venues do this), pull the order state ourselves and feed it in; a venue
        silent past HEDGING_STUCK_S hands the position to retry_degraded, which needs no event. Also the naked-exposure
        alarm."""
        for pos in list(self.book.by_status(HEDGING, EXIT_HEDGING)):
            self._check_naked(pos, now)
            tr = self._tracks.get(pos.maker_client_id)
            live = tr is not None and (tr.last is None or not tr.last.terminal)
            since = pos.maker_fill_ts or pos.maker_posted_ts
            if not live or now - since < HEDGING_SWEEP_AFTER_S:
                continue
            if not pos.maker_cancel_sent:
                log.warning("HEDGING_SWEEP #%d %s: maker order still live on %s — cancelling", pos.id, pos.symbol, pos.maker_venue)
                await self._cancel_maker_order(pos, priority=True)
                continue
            if now - self._last_query.get(pos.id, float("-inf")) >= HEDGING_SWEEP_AFTER_S:
                self._last_query[pos.id] = now
                v = self.venues[pos.maker_venue]
                try:
                    ev = await v.trading.query_order(pos.symbol, pos.maker_client_id, pos.maker_order_id)
                except Exception as e:  # noqa: BLE001
                    log.debug("query_order failed %s %s: %r", v.name, pos.maker_client_id, e)
                    ev = None
                if ev is not None and ev.terminal:
                    log.warning("HEDGING_SWEEP #%d %s: no terminal event for %s — pulled %s from the venue", pos.id, pos.symbol,
                                pos.maker_client_id, ev.state)
                    self.on_order_event(ev)
                    continue
            if now - since >= HEDGING_STUCK_S:
                log.error("HEDGING_STUCK #%d %s: maker order %s on %s reported nothing for %.0fs — DEGRADED, closing what the books show",
                          pos.id, pos.symbol, pos.maker_client_id, pos.maker_venue, now - since)
                await self._degrade(pos, "both", f"maker order silent for {now - since:.0f}s")

    def adopt_restored(self, pos: Position) -> None:
        """Own a position restored mid-flight (the task behind it died with the process). Whatever its resting order
        had filled goes on its leg FIRST — a HEDGING/EXIT_HEDGING save predates `_finalize_maker`, a MAKER_RESTING
        save can predate the hedge task — then retry_degraded closes what the books show. In paper the venue holds
        nothing (booked flat via `nothing to reduce`); live reconciliation (Plan 2) must confirm the venue side and
        cancel or adopt the resting order itself. Specs are usually not loaded yet at this point, so the size is only
        re-derived from the legs when they are."""
        if pos.maker_venue and pos.status in (MAKER_RESTING, HEDGING, EXIT_MAKER_RESTING, EXIT_HEDGING):
            self._apply_maker_leg(pos, "entry" if pos.status in (MAKER_RESTING, HEDGING) else "exit")
            self._resize_from_legs(pos, self._spec(pos.venue_a, pos.symbol), self._spec(pos.venue_b, pos.symbol))
        if pos.status == MAKER_RESTING:                   # the state machine walks resting -> hedging -> degraded
            transition(pos, HEDGING)
        elif pos.status == EXIT_MAKER_RESTING:
            transition(pos, EXIT_HEDGING)
        if pos.status != DEGRADED:
            transition(pos, DEGRADED)
        pos.degraded_leg, pos.last_close_attempt = "both", 0.0
        self.book.dirty = True

    async def retry_degraded(self) -> None:
        now = self.clock()
        await self._sweep_stuck_hedging(now)
        for pos in list(self.book.by_status(DEGRADED)):
            if now - pos.last_close_attempt < DEGRADED_RETRY_S:
                continue
            if pos.close_retry_count >= MAX_CLOSE_RETRIES:
                if pos.close_retry_count == MAX_CLOSE_RETRIES:
                    pos.close_retry_count += 1
                    log.error("DEGRADED_STUCK #%d %s: %d close attempts failed — manual intervention needed",
                              pos.id, pos.symbol, MAX_CLOSE_RETRIES)
                    self._say(f"DEGRADED_STUCK #{pos.id} {pos.symbol}: close it by hand")
                continue
            pos.last_close_attempt = now
            pos.close_retry_count += 1
            ok = True
            for leg, venue, side in (("a", pos.venue_a, "buy"), ("b", pos.venue_b, "sell")):
                rem = round((pos.filled_a - pos.exit_filled_a) if leg == "a" else (pos.filled_b - pos.exit_filled_b), 10)
                if rem <= 1e-9:
                    continue
                ev = await self._place_taker(pos, venue, f"retry_{leg}{pos.close_retry_count}", side, rem, True, priority=True)
                if ev.state == "filled" and ev.filled_qty > 0:
                    self._accumulate_leg(pos, venue, "exit", ev)
                elif "nothing to reduce" in (ev.error or "") and await self._leg_flat_at_venue(pos, venue):
                    self._book_leg_flat(pos, leg, venue)
                rem = round((pos.filled_a - pos.exit_filled_a) if leg == "a" else (pos.filled_b - pos.exit_filled_b), 10)
                if rem > 1e-9:                    # a partial close is not a close
                    ok = False
            if ok:
                self._close(pos, pos.exit_reason or "recovered")

    def _close(self, pos: Position, reason: str) -> None:
        if pos.status == CLOSED:
            return                                # book.close is idempotent; the risk stats must be too
        self.book.close(pos, reason, self.clock())
        self.risk.record_close(pos)
        log.info("CLOSE #%d %s reason=%s pnl=$%+.4f gross=$%+.4f fees=$%.4f adj=$%+.4f exit_spread=%.3f%%", pos.id,
                 pos.symbol, reason, pos.net_pnl_usd, pos.gross_pnl_usd, pos.entry_fees_usd + pos.exit_fees_usd,
                 pos.pnl_adjust_usd, pos.exit_spread_pct)
        self._say(f"CLOSE #{pos.id} {pos.symbol} {reason} pnl ${pos.net_pnl_usd:+.4f}")
        self._forget(pos)

    async def cancel_all_resting(self) -> None:
        """Halt: cancel every live maker order, including one left resting by a failed cancel while hedging."""
        for pos in list(self.book.by_status(MAKER_RESTING, EXIT_MAKER_RESTING, HEDGING, EXIT_HEDGING)):
            tr = self._tracks.get(pos.maker_client_id)
            if pos.status in (HEDGING, EXIT_HEDGING) and (tr is None or (tr.last is not None and tr.last.terminal)):
                continue
            await self._cancel_maker_order(pos, priority=True)
```

- [x] **Step 5: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_execution_tt.py tests/test_execution_tm.py -q`
Expected: `29 passed`

- [x] **Step 6: Run the whole suite and commit**

Run: `.venv/bin/python -m pytest -q`
Expected: `147 passed`

```bash
git add deploy-bbo/bbo_trader/execution.py deploy-bbo/tests/test_execution_tt.py deploy-bbo/tests/test_execution_tm.py
git commit -m "feat(bbo): Executor — parallel TT legs with event-driven fills, PeggedMaker post/peg/hedge, flatten, degraded retry"
```

---

### Task 17: Universe discovery (`discovery.py`)

**Files:**
- Create: `deploy-bbo/bbo_trader/discovery.py`
- Test: `deploy-bbo/tests/test_discovery.py`

- [x] **Step 1: Write the failing test**

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
    assert build_universe(specs, ["mexc", "blofin", "okx"], {"BADUSDT"}, {"okx": ()}) == {
        "AUSDT": ["blofin", "mexc", "okx"], "BUSDT": ["blofin", "mexc"], "CUSDT": ["mexc", "okx"]}   # empty whitelist = unrestricted
    del specs["blofin"]["BUSDT"]                                                                     # delisted on one venue -> drops out
    assert "BUSDT" not in build_universe(specs, ["mexc", "blofin", "okx"], {"BADUSDT"}, {"okx": ("CUSDT",)})
```

- [x] **Step 2: Run the test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_discovery.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'bbo_trader.discovery'`

- [x] **Step 3: Implement `bbo_trader/discovery.py`**

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

- [x] **Step 4: Run the test to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_discovery.py -q`
Expected: `1 passed`

- [x] **Step 5: Commit**

```bash
git add deploy-bbo/bbo_trader/discovery.py deploy-bbo/tests/test_discovery.py
git commit -m "feat(bbo): universe from per-venue specs (>=2 trade venues, blocked list, whitelists)"
```

---

### Task 18: Telegram notifications and commands (`notify.py`)

**Files:**
- Create: `deploy-bbo/bbo_trader/notify.py`
- Test: `deploy-bbo/tests/test_notify.py`

- [x] **Step 1: Write the failing tests**

`tests/test_notify.py`:

```python
import json
import logging

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
    # nothing Telegram can send may wedge the poller: every update advances the offset
    for text in (" ", "\n", "\t", "\xa0", "　", None, 7):
        assert parse_commands([{"update_id": 5, "message": {"chat": {"id": 42}, "text": text}}], "42", 0) == ([], 6)
    assert parse_commands([{"update_id": 5, "edited_message": {"chat": {"id": 42}, "text": "/stop"}}], "42", 0) == ([], 6)
    assert parse_commands([{"update_id": 5, "message": "junk"}, "junk", {"update_id": "x"}], "42", 0) == ([], 6)
    assert parse_commands([{"update_id": 5, "message": {"chat": "junk", "text": "/stop"}},
                           {"update_id": 6, "message": {"chat": {"id": 42}, "text": "/stop"}}], "42", 0) == (["/stop"], 7)
    # the command menu in a group sends /stop@botname; phones capitalize
    assert parse_commands([{"update_id": 5, "message": {"chat": {"id": 42}, "text": "/Stop@bbo_bot"}}], "42", 0) == (["/stop"], 6)
    # a chat id with stray whitespace (env file) still matches
    assert parse_commands([{"update_id": 5, "message": {"chat": {"id": 42}, "text": "/stop"}}], " 42 ", 0) == (["/stop"], 6)
    # updates dated before process start are consumed but not executed (no replay of a stale /close_all)
    old = {"update_id": 5, "message": {"chat": {"id": 42}, "text": "/close_all", "date": 1000}}
    new = {"update_id": 6, "message": {"chat": {"id": 42}, "text": "/close_all", "date": 3000}}
    assert parse_commands([old, new], "42", 0, not_before=2000) == (["/close_all"], 7)


async def test_disabled_telegram_is_a_noop(caplog):
    t = Telegram("", "")
    assert not t.enabled and await t.send("x") is False and await t.poll_commands() == []
    with caplog.at_level(logging.WARNING, logger="bbo.notify"):
        half = Telegram("tok", "")                              # half-configured: say so once at start-up
    assert not half.enabled and "TELEGRAM_DISABLED" in caplog.text


class _Resp:
    def __init__(self, status, body):
        self.status, self._body = status, body

    async def json(self, content_type=None):
        return json.loads(self._body)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class _Session:
    def __init__(self, status=200, body="{}", raise_with=None):
        self.status, self.body, self.raise_with, self.calls = status, body, raise_with, []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    def get(self, url, **kw):
        self.calls.append(("get", url, kw))
        if self.raise_with:
            raise self.raise_with
        return _Resp(self.status, self.body)

    def post(self, url, **kw):
        self.calls.append(("post", url, kw))
        if self.raise_with:
            raise self.raise_with
        return _Resp(self.status, self.body)


async def test_telegram_failures_are_visible_once_and_never_leak_the_token(caplog):
    token = "123456:SECRET-TOKEN-VALUE"
    sess = _Session(200, json.dumps({"ok": False, "error_code": 401, "description": "Unauthorized"}))
    t = Telegram(token, " 42 ", session_factory=lambda: sess)
    assert t.chat_id == "42"
    with caplog.at_level(logging.DEBUG, logger="bbo.notify"):
        assert await t.poll_commands() == []
        assert await t.poll_commands() == []
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1 and "Unauthorized" in warnings[0].getMessage()     # once at WARNING, then DEBUG
    caplog.clear()
    boom = _Session(raise_with=ValueError(f"https://api.telegram.org/bot{token}/sendMessage is bad"))
    t2 = Telegram(token, "42", session_factory=lambda: boom)
    with caplog.at_level(logging.DEBUG, logger="bbo.notify"):
        assert await t2.send("x") is False and await t2.poll_commands() == []
    assert "TELEGRAM_FAILING" in caplog.text and token not in caplog.text          # the URL carries the token: never logged
    sess3 = _Session(500, "{}")
    t3 = Telegram(token, "42", session_factory=lambda: sess3)
    with caplog.at_level(logging.WARNING, logger="bbo.notify"):
        assert await t3.send("x") is False
    assert "HTTP 500" in caplog.text


async def test_telegram_polls_advance_the_offset_and_skip_pre_start_updates():
    now = [5000.0]
    first = json.dumps({"ok": True, "result": [
        {"update_id": 10, "message": {"chat": {"id": 42}, "text": "/close_all", "date": 4000}},   # before start: ignored
        {"update_id": 11, "message": {"chat": {"id": 42}, "text": "/status", "date": 5001}}]})
    sess = _Session(200, first)
    t = Telegram("tok", "42", session_factory=lambda: sess, clock=lambda: now[0])
    assert await t.poll_commands() == ["/status"] and t._offset == 12
    sess.body = json.dumps({"ok": True, "result": []})
    assert await t.poll_commands() == [] and sess.calls[-1][2]["params"]["offset"] == 12
    sess.body = json.dumps({"ok": True, "result": "not-a-list"})
    assert await t.poll_commands() == [] and t._offset == 12
    long_sess = _Session(200, json.dumps({"ok": True, "result": {}}))
    t2 = Telegram("tok", "42", session_factory=lambda: long_sess)
    assert await t2.send("y" * 5000) is True
    assert len(long_sess.calls[-1][2]["json"]["text"]) == 4000 and long_sess.calls[-1][2]["json"]["text"].endswith("...")
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_notify.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'bbo_trader.notify'`

- [x] **Step 3: Implement `bbo_trader/notify.py`**

```python
"""Telegram: send messages and poll the operator commands (/stop, /start, /close_all, /status).

This is the only remote control path of a bot without an automatic kill switch, so the poller must never
wedge: every update advances the offset whatever its shape, a malformed message costs nothing, commands are
matched case-insensitively and with a `@botname` suffix, the configured chat id is normalized, updates dated
before process start are ignored (a /close_all sent during an outage must not replay against reloaded
positions), and the first failure per process is a WARNING that never contains the token."""
from __future__ import annotations

import logging
import time

import aiohttp

log = logging.getLogger("bbo.notify")
COMMANDS = ("/stop", "/start", "/close_all", "/status")
MAX_TEXT = 4000


def parse_commands(updates: list[dict], chat_id: str, offset: int, not_before: float = 0.0) -> tuple[list[str], int]:
    """Pure: Telegram getUpdates payload → (commands from our chat, next offset). Never raises on a payload
    Telegram can produce; every update with an id advances the offset."""
    cmds: list[str] = []
    nxt = offset
    want = str(chat_id).strip()
    for u in updates:
        if not isinstance(u, dict):
            continue
        try:
            uid = int(u.get("update_id", 0))
        except (TypeError, ValueError):
            continue
        nxt = max(nxt, uid + 1)
        msg = u.get("message")
        if not isinstance(msg, dict):
            continue                                    # edited_message / channel_post / callback_query: ignored
        chat = msg.get("chat")
        if not isinstance(chat, dict) or str(chat.get("id", "")).strip() != want:
            continue
        try:
            dated = float(msg.get("date") or 0.0)
        except (TypeError, ValueError):
            dated = 0.0
        if not_before and dated and dated < not_before:
            continue
        parts = str(msg.get("text") or "").strip().split()
        text = parts[0].split("@")[0].lower() if parts else ""
        if text in COMMANDS:
            cmds.append(text)
    return cmds, nxt


class Telegram:
    def __init__(self, token: str, chat_id: str, session_factory=aiohttp.ClientSession, clock=time.time):
        self.token = (token or "").strip()
        self.chat_id = str(chat_id or "").strip()
        self.enabled = bool(self.token and self.chat_id)
        if bool(self.token) != bool(self.chat_id):
            log.warning("TELEGRAM_DISABLED: set both TELEGRAM_TOKEN and TELEGRAM_CHAT_ID (only one is set)")
        self._session_factory = session_factory
        self._offset = 0
        self._started = clock()          # updates dated before this are ignored: no replay of a stale /close_all
        #                                  (Telegram dates are whole seconds; a command sent in the start-up second may be
        #                                  dropped — the safe side of the bias: a missed /stop costs a re-send, a replayed
        #                                  /close_all costs money)
        self._warned = False

    def _fail(self, what: str) -> None:
        """First failure per process at WARNING (a dead token must not be silent), the rest at DEBUG. Never the
        exception repr: the request URL carries the bot token."""
        if not self._warned:
            self._warned = True
            log.warning("TELEGRAM_FAILING %s — further failures logged at DEBUG", what)
        else:
            log.debug("telegram %s", what)

    async def send(self, text: str) -> bool:
        if not self.enabled:
            return False
        if len(text) > MAX_TEXT:
            text = text[:MAX_TEXT - 3] + "..."
        try:
            async with self._session_factory() as s:
                async with s.post(f"https://api.telegram.org/bot{self.token}/sendMessage",
                                  json={"chat_id": self.chat_id, "text": text},
                                  timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status != 200:
                        self._fail(f"sendMessage HTTP {r.status}")
                        return False
                    return True
        except Exception as e:  # noqa: BLE001
            self._fail(f"sendMessage {type(e).__name__}")
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
            self._fail(f"getUpdates {type(e).__name__}")
            return []
        if not isinstance(data, dict) or data.get("ok") is not True:
            desc = data.get("description") if isinstance(data, dict) else type(data).__name__
            self._fail(f"getUpdates ok=false: {str(desc)[:120]}")
            return []
        result = data.get("result")
        if not isinstance(result, list):
            return []
        try:
            cmds, self._offset = parse_commands(result, self.chat_id, self._offset, not_before=self._started)
        except Exception:  # noqa: BLE001 — belt and braces: a parse bug must never freeze the offset
            log.exception("TELEGRAM_PARSE_ERROR")
            ids = [int(u.get("update_id", 0)) for u in result if isinstance(u, dict) and str(u.get("update_id", "")).isdigit()]
            self._offset = max([self._offset] + [i + 1 for i in ids])
            return []
        return cmds
```

- [x] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_notify.py -q`
Expected: `4 passed`

- [x] **Step 5: Commit**

```bash
git add deploy-bbo/bbo_trader/notify.py deploy-bbo/tests/test_notify.py
git commit -m "feat(bbo): Telegram send + /stop /start /close_all /status command polling"
```

---

### Task 19: Venue registry, App wiring, entry point (`venues/registry.py`, `app.py`, `main.py`)

**Files:**
- Create: `deploy-bbo/bbo_trader/venues/registry.py`, `deploy-bbo/bbo_trader/app.py`, `deploy-bbo/bbo_trader/main.py`
- Test: `deploy-bbo/tests/test_app.py`, `deploy-bbo/tests/test_registry.py`

`App` is the only place where quotes, strategy and executor meet: `on_bbo` stores the quote, feeds the paper venue's fill model, then either drives the symbol's existing position (requote/cancel/upgrade/exit intents) or evaluates an entry. The sweep (every 500 ms) handles timers, halt flags, degraded retries, heartbeat and state saves. The end-to-end test runs quotes through the whole pipeline with `SimVenue`s — no network.

- [x] **Step 1: Write the failing end-to-end tests**

`tests/test_app.py`:

```python
import asyncio
import json
from collections import Counter
from dataclasses import replace

import pytest

from bbo_trader.app import App, VenueMissing
from bbo_trader.main import legacy_bot_running
from bbo_trader.models import OPEN, CLOSED, MAKER_RESTING, EXIT_MAKER_RESTING, Intent
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
    assert state["spread_scanner"][0]["symbol"] == SYM and (tmp_path / "bbo_heartbeat_paper").exists()
    assert app._market_data_interval() == 3600.0 and state["bbo"]["connected"] == {}
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


def test_legacy_guard(tmp_path, monkeypatch):
    hb = tmp_path / "heartbeat_live"
    assert not legacy_bot_running(hb, 120.0, now=1000.0)
    hb.write_text("1")
    import os
    os.utime(hb, (1000.0, 1000.0))
    assert legacy_bot_running(hb, 120.0, now=1050.0)
    assert not legacy_bot_running(hb, 120.0, now=1200.0)
    import pathlib

    def denied(self, *a, **k):
        raise PermissionError("denied")
    monkeypatch.setattr(pathlib.Path, "stat", denied)
    assert legacy_bot_running(hb, 120.0, now=1200.0)               # unreadable: assume the legacy bot runs, a human decides


async def test_live_refuses_corrupt_state_and_paper_falls_back_to_backup(tmp_path):
    (tmp_path / "real_state.json").write_text("{not json")
    app = build_app(tmp_path)                                  # paper: no backup -> fresh start, no exception
    app.load_state()
    assert app.book.open == []
    live = build_app(tmp_path, mode="live")
    with pytest.raises(StateCorrupt):
        live.load_state()


async def test_venue_missing_check_and_drive_guard(tmp_path, caplog):
    app = build_app(tmp_path)
    pos = app.book.new(SYM, "gate", "mexc", OPEN, "TT", size_usd=25.0, entry_time=app.clock())
    app._check_position_venues()                                   # paper: booked closed, not counted as a trade
    assert pos.status == CLOSED and pos.exit_reason == "venue_removed" and app.book.total_trades == 0
    app.book.new(SYM, "gate", "mexc", OPEN, "TT", size_usd=25.0, entry_time=app.clock())
    app.cfg = replace(app.cfg, mode="live")
    with pytest.raises(VenueMissing):
        app._check_position_venues()                               # live: refuse to start, the venue holds it
    app.cfg = replace(app.cfg, mode="paper")
    app._check_position_venues()
    app.book.new(SYM, "blofin", "mexc", OPEN, "TT", size_usd=25.0, entry_time=app.clock())
    app.evaluator.evaluate_exit = lambda *a, **k: 1 / 0            # one broken position...
    await app.sweep_once()                                         # ...stops neither the sweep nor the heartbeat
    assert (tmp_path / "bbo_heartbeat_paper").exists() and "DRIVE_ERROR" in caplog.text


async def test_quotes_from_the_future_are_refused(tmp_path, caplog):
    app = build_app(tmp_path)
    app.on_bbo(bbo("mexc", 1.0000, 1.0008, 10.0, ts=app.clock() + 5.0))      # a receive time that would never go stale
    assert app.board.get("mexc", SYM) is None and "QUOTE_TS_SKEW" in caplog.text
    app.on_bbo(bbo("mexc", 1.0000, 1.0008, 10.0, ts=app.clock() + 0.5))      # within the tolerance: accepted
    assert app.board.get("mexc", SYM) is not None


def _slow(sim, delay=0.05):
    real_pm = sim.place_market

    async def slow_pm(*a, **k):
        await asyncio.sleep(delay)
        return await real_pm(*a, **k)
    sim.place_market = slow_pm


async def test_shutdown_drains_inflight_entries_before_saving(tmp_path):
    app = build_app(tmp_path)
    _slow(app.harness.sim("mexc"))
    app.on_bbo(bbo("mexc", 1.0000, 1.0008, 10.0))
    app.on_bbo(bbo("blofin", 1.0050, 1.0060, 1.0))
    assert SYM in app._pending_entries                              # the entry is in flight
    app.running = False
    await app.shutdown()                                            # drains it, then saves
    pos = app.book.open[0]
    assert pos.status == OPEN and pos.filled_a == 20.0 and pos.filled_b == 2.0 and not app._pending_entries
    state = json.loads((tmp_path / "real_state.json").read_text())
    assert state["open_positions"][0]["status"] == OPEN and state["open_positions"][0]["filled_a"] == 20.0
    app.on_bbo(bbo("blofin", 1.0070, 1.0080, 1.0))                # no new entries once stopping
    assert not app._pending_entries


async def test_restored_transient_positions_are_adopted(tmp_path, monkeypatch):
    from bbo_trader import execution
    app = build_app(tmp_path)
    now = app.clock()
    legs = dict(size_usd=25.0, filled_a=20.0, filled_b=2.0, entry_price_a=1.005, entry_price_b=1.0008, entry_time=now)
    for st in ("TT_ENTERING", "HEDGING", "EXIT_HEDGING", "TT_EXITING"):
        app.book.new(SYM, "blofin", "mexc", st, "TT", **legs)
    app.book.new(SYM, "blofin", "mexc", MAKER_RESTING, "TM", size_usd=25.0, maker_venue="blofin", maker_client_id="gone-1",
                 entry_time=now)
    app.book.new(SYM, "blofin", "mexc", EXIT_MAKER_RESTING, "TT", maker_venue="blofin", maker_client_id="gone-2", **legs)
    await app.save_state(now)
    app2 = build_app(tmp_path)
    app2.load_state()
    app2._adopt_transients()                                        # run() does this after the first market-data refresh
    assert sorted(p.status for p in app2.book.open) == ["DEGRADED"] * 4 + [OPEN]   # resting entry discarded, exit maker reopened
    reopened = [p for p in app2.book.open if p.status == OPEN][0]
    assert reopened.maker_client_id == "" and reopened.maker_venue == ""
    monkeypatch.setattr(execution, "DEGRADED_RETRY_S", 0.0)
    app2.harness.quote("blofin", 1.0050, 1.0060)
    app2.harness.quote("mexc", 1.0000, 1.0008)
    await app2.sweep_once()                                         # retry_degraded owns them: the venues hold nothing -> booked flat
    await settle()
    assert not [p for p in app2.book.open if p.status == "DEGRADED"]


async def test_close_all_covers_inflight_entries(tmp_path):
    app = build_app(tmp_path)
    _slow(app.harness.sim("mexc"))
    app.on_bbo(bbo("mexc", 1.0000, 1.0008, 10.0))
    app.on_bbo(bbo("blofin", 1.0050, 1.0060, 1.0))
    assert SYM in app._pending_entries
    await app.handle_command("/close_all")                          # drains the entry first, then closes it
    await settle()
    pos = app.book.closed[-1]
    assert pos.status == CLOSED and pos.exit_reason == "halt" and app.risk.halted and app.book.open == []
    assert await app.harness.sim("blofin").positions() == [] and await app.harness.sim("mexc").positions() == []


async def test_dying_task_is_reported_and_stops_the_process(tmp_path, caplog):
    app = build_app(tmp_path)

    async def boom():
        raise RuntimeError("feed died")
    t = app._supervise("public:test", asyncio.create_task(boom()))
    await asyncio.gather(t, return_exceptions=True)
    await asyncio.sleep(0.01)
    assert not app.running and "TASK_DIED public:test" in caplog.text


def test_data_dir_collision_with_the_legacy_bot_is_refused(tmp_path):
    from bbo_trader.main import data_dir_collides
    from tests.conftest import make_cfg
    legacy = tmp_path / "legacy"
    legacy.mkdir()
    assert not data_dir_collides(make_cfg(tmp_path / "own", legacy_heartbeat_path=legacy / "heartbeat_live"))
    assert data_dir_collides(make_cfg(legacy, legacy_heartbeat_path=legacy / "heartbeat_live"))


async def test_quote_fallback_refreshes_before_the_stale_boundary(tmp_path):
    import time
    app = build_app(tmp_path)
    h = app.harness
    h.quote("blofin", 1.0050, 1.0060)
    h.quote("mexc", 1.0000, 1.0008)
    await h.ex.enter_tt(Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0))
    calls = []

    class FakeMarket:
        specs = {}

        async def fetch_bbo(self, symbol):
            calls.append(symbol)
            return mk_bbo("mexc", SYM, 1.0001, 1.0009, contract_size=10.0)
    app.venues["mexc"].market = FakeMarket()
    app._last_rest["mexc"] = app.clock()                            # connection already warm
    await app._quote_fallback()
    await settle()
    assert calls == []                                              # fresh leg: no REST call
    app.clock = lambda: time.time() + 1.2                           # the mexc leg is 1.2 s old: past half of the 2 s budget
    await app._quote_fallback()
    await settle()
    assert calls == [SYM] and app.board.get("mexc", SYM).bid == 1.0001   # refreshed before it could read stale


async def test_restored_maker_fills_are_booked_not_discarded(tmp_path, monkeypatch):
    """A save can land between a maker fill and its hedge (MAKER_RESTING with maker_filled_qty), during HEDGING before
    _finalize_maker booked the fill on the leg, or with an exit maker partly filled: none of these may be discarded,
    reopened as if unfilled, or closed as if nothing was held."""
    from bbo_trader import execution
    app = build_app(tmp_path)
    now = app.clock()
    app.book.new(SYM, "blofin", "mexc", MAKER_RESTING, "TM", size_usd=25.0, maker_venue="blofin", maker_side="sell",
                 maker_client_id="gone-1", maker_filled_qty=12.0, maker_avg_price=1.006, maker_fee_usd=0.002, entry_time=now)
    app.book.new(SYM, "blofin", "mexc", "HEDGING", "TM", size_usd=25.0, maker_venue="blofin", maker_side="sell",
                 maker_client_id="gone-2", maker_filled_qty=20.0, maker_avg_price=1.006, hedged_qty=20.0,
                 filled_b=2.0, entry_price_b=1.0008, entry_time=now)          # hedge leg booked, maker leg not yet
    app.book.new(SYM, "blofin", "mexc", EXIT_MAKER_RESTING, "TT", size_usd=25.0, maker_venue="blofin", maker_side="buy",
                 maker_client_id="gone-3", maker_filled_qty=5.0, maker_avg_price=1.001, filled_a=20.0, filled_b=2.0,
                 entry_price_a=1.005, entry_price_b=1.0008, entry_time=now)
    await app.save_state(now)
    app2 = build_app(tmp_path)
    app2.load_state()
    app2._adopt_transients()
    p1, p2, p3 = sorted(app2.book.open, key=lambda p: p.id)
    assert [p.status for p in (p1, p2, p3)] == ["DEGRADED"] * 3
    assert p1.filled_a == 12.0 and p1.entry_price_a == pytest.approx(1.006) and p1.entry_fees_usd == pytest.approx(0.002)
    assert p1.size_usd == pytest.approx(12.0 * 1.006)                    # sized by what it really holds (specs known by then)
    assert p2.filled_a == 20.0 and p2.filled_b == 2.0                    # the maker leg is on the books before retry_degraded looks
    assert p3.exit_filled_a == 5.0 and p3.filled_a == 20.0               # the exit fill reduced leg a: 15 remain to close
    monkeypatch.setattr(execution, "DEGRADED_RETRY_S", 0.0)
    app2.harness.quote("blofin", 1.0050, 1.0060)
    app2.harness.quote("mexc", 1.0000, 1.0008)
    await app2.sweep_once()                                              # paper: the venues hold nothing -> booked flat
    await settle()
    assert app2.book.open == [] and app2.book.total_trades == 3


async def test_quote_fallback_abandons_a_hung_fetch_within_the_budget(tmp_path, caplog):
    import time
    app = build_app(tmp_path, stale_quote_s=0.4)                         # refresh at 0.2 s, give the call the full 0.4 s
    h = app.harness
    h.quote("blofin", 1.0050, 1.0060)
    h.quote("mexc", 1.0000, 1.0008)
    await h.ex.enter_tt(Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0))
    calls = []

    class HungMarket:
        specs = {}

        async def fetch_bbo(self, symbol):
            calls.append(symbol)
            await asyncio.sleep(10)
    app.venues["blofin"].market = HungMarket()
    app.clock = lambda: time.time() + 0.3                                # the blofin leg is 0.3 s old: past half the budget
    t0 = time.time()
    await app._quote_fallback()                                          # spawns the fetch; the tick itself never blocks
    assert time.time() - t0 < 0.05 and len(app._fallback_tasks) == 1
    await asyncio.sleep(0.05)
    await app._quote_fallback()                                          # the next tick while the fetch is in flight: no duplicate
    await asyncio.gather(*app._fallback_tasks)
    assert time.time() - t0 < 0.7 and calls == [SYM]                    # abandoned at the bound, not at the adapter's 5 s
    assert app.metrics.funnel["fallback_timeout"] == 1 and "QUOTE_FALLBACK_TIMEOUT" in caplog.text
    assert not app._fallback_inflight


async def test_shutdown_settles_cancelled_makers_before_saving(tmp_path):
    app = build_app(tmp_path)
    app.on_bbo(bbo("mexc", 1.0000, 1.0010, 10.0))
    app.on_bbo(bbo("blofin", 1.0041, 1.0061, 1.0))                       # TM entry rests on blofin
    await settle()
    assert app.book.open[0].status == MAKER_RESTING
    app.running = False
    await app.shutdown()                                                 # the cancel ack is an order event: wait for it, then save
    state = json.loads((tmp_path / "real_state.json").read_text())
    assert state["open_positions"] == [] and app.book.open == []         # not a MAKER_RESTING a restart would report as stuck


async def test_no_new_maker_orders_once_stopping(tmp_path):
    app = build_app(tmp_path)
    app.on_bbo(bbo("mexc", 1.0000, 1.0008, 10.0))
    app.on_bbo(bbo("blofin", 1.0050, 1.0060, 1.0))
    await settle()
    pos = app.book.open[0]
    assert pos.status == OPEN
    app.running = False
    app.evaluator.evaluate_exit = lambda *a, **k: Intent("TM_EXIT", symbol=SYM, venue_a="blofin", venue_b="mexc",
                                                         maker_venue="blofin", rest_price=1.0050)
    app._drive(pos)
    await settle()
    assert pos.status == OPEN                                            # no new resting order while shutting down...
    app.evaluator.evaluate_exit = lambda *a, **k: Intent("TT_EXIT", reason="time_stop", symbol=SYM, venue_a="blofin", venue_b="mexc")
    app._drive(pos)
    await settle()
    assert pos.status == CLOSED                                          # ...but a taker exit still goes through


async def test_hung_notify_does_not_hold_the_drain(tmp_path, caplog):
    import time
    app = build_app(tmp_path)

    async def hung(text):
        await asyncio.sleep(10)
    app.executor.notify = hung
    app.executor._say("hello")
    await asyncio.sleep(0)                                               # the send is now sitting in its 10 s sleep
    t0 = time.time()
    await app._drain(2.0)
    assert time.time() - t0 < 0.5 and "DRAIN_TIMEOUT" not in caplog.text
    sends = list(app.executor._notify_tasks)
    assert len(sends) == 1                                               # its own set: the drain waits for order tasks only
    for t in sends:
        t.cancel()
    await asyncio.gather(*sends, return_exceptions=True)


async def test_market_data_refresh_keeps_held_specs_and_retries_fast_when_empty(tmp_path, caplog):
    from tests.conftest import mk_spec
    app = build_app(tmp_path)
    h = app.harness
    h.quote("blofin", 1.0050, 1.0060)
    h.quote("mexc", 1.0000, 1.0008)
    await h.ex.enter_tt(Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0))
    other = mk_spec("blofin", "OTHERUSDT")

    class DelistingMarket:
        specs = {}

        async def fetch_specs(self):
            return {"OTHERUSDT": other}                                  # SYM delisted while we hold it

        async def fetch_volumes(self):
            return {"OTHERUSDT": 1e6}

    class Public:
        connected = True
        symbols = None

        def set_specs(self, specs):
            pass

        def set_symbols(self, syms):
            self.symbols = list(syms)
    v = app.venues["blofin"]
    v.market, v.public = DelistingMarket(), Public()
    await app.refresh_market_data()
    assert SYM in v.specs and "OTHERUSDT" in v.specs and SYM in v.public.symbols   # kept its spec, stays subscribed
    assert app.universe == {SYM: ["blofin", "mexc"]} and app._market_data_interval() == 3600.0
    app.venues["mexc"].specs.clear()                                     # nothing on two trade venues any more
    await app.refresh_market_data()
    assert app.universe == {} and app._market_data_interval() == 60.0 and "UNIVERSE_EMPTY" in caplog.text


async def test_stale_cancel_puts_the_symbol_on_a_short_cooldown(tmp_path):
    import time
    app = build_app(tmp_path)
    app.on_bbo(bbo("mexc", 1.0000, 1.0010, 10.0))
    app.on_bbo(bbo("blofin", 1.0041, 1.0061, 1.0))
    await settle()
    pos = app.book.open[0]
    assert pos.status == MAKER_RESTING
    app.evaluator.clock = lambda: time.time() + 3.0                      # every leg reads stale to the strategy
    app._drive(pos)
    await settle()
    assert app.book.open == [] and app.metrics.funnel["maker_cancelled"] == 1
    assert 4.0 < app.risk.cooldowns[SYM] - time.time() <= 5.0            # STALE_CANCEL_COOLDOWN_S, not the 60 s entry-failure one


async def test_quote_fallback_keeps_one_warm_rest_connection_per_venue(tmp_path):
    import time
    from tests.conftest import mk_spec
    app = build_app(tmp_path)
    h = app.harness
    h.quote("blofin", 1.0050, 1.0060)
    h.quote("mexc", 1.0000, 1.0008)
    await h.ex.enter_tt(Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0))
    other = "ABCUSDT"
    for name, cs in (("blofin", 1.0), ("mexc", 10.0)):
        app.venues[name].specs[other] = mk_spec(name, other, contract_size=cs)
    app.book.new(other, "blofin", "mexc", OPEN, "TT", size_usd=25.0, filled_a=20.0, filled_b=2.0, entry_time=app.clock())
    seen, inflight, peak = [], [0], [0]

    class SlowMarket:
        specs = {}

        async def fetch_bbo(self, symbol):
            inflight[0] += 1
            peak[0] = max(peak[0], inflight[0])
            await asyncio.sleep(0.02)
            inflight[0] -= 1
            seen.append(symbol)
            return mk_bbo("blofin", symbol, 1.0050, 1.0060, ts=app.clock())
    app.venues["blofin"].market = SlowMarket()
    for sym in (SYM, other):
        app.board.set(mk_bbo("blofin", sym, 1.0050, 1.0060, ts=app.clock()))      # both blofin legs fresh...
    await app._quote_fallback()
    await settle()
    assert seen == [SYM]                                                        # ...one warm-up call for the venue anyway
    await app._quote_fallback()
    await settle()
    assert seen == [SYM]                                                        # warm within REST_WARM_S: nothing
    t1 = time.time() + 1.5                                                      # both legs read 1.5 s old: past half the budget
    app.clock = lambda: t1
    await app._quote_fallback()
    await asyncio.gather(*app._fallback_tasks)
    assert sorted(seen[1:]) == [other, SYM] and peak[0] == 1                    # both refreshed, one call at a time
    assert app.board.get("blofin", SYM).ts_local == t1 and app.metrics.funnel["fallback_ok"] == 3


async def _resting_tm(app):
    app.on_bbo(bbo("mexc", 1.0000, 1.0010, 10.0))
    app.on_bbo(bbo("blofin", 1.0041, 1.0061, 1.0))
    await settle()
    pos = app.book.open[0]
    assert pos.status == MAKER_RESTING
    return pos


def _count_calls(obj, name):
    calls = []
    orig = getattr(obj, name)

    async def wrapped(*a, **k):
        calls.append(a)
        return await orig(*a, **k)
    setattr(obj, name, wrapped)
    return calls


async def test_shutdown_does_not_repost_a_requoted_maker(tmp_path):
    """A requote sends the cancel and returns; its `canceled` event lands during shutdown and finalize would post the
    new price — a fresh order resting at the venue after the sweep that was supposed to clear it."""
    app = build_app(tmp_path)
    pos = await _resting_tm(app)
    app.harness.sim("blofin").supports_amend = False                            # the cancel + new-order requote path
    posts = _count_calls(app.harness.sim("blofin"), "place_post_only")
    app.running = False
    app._spawn(app.executor.requote(pos, 1.0063))
    await app.shutdown()
    assert posts == [] and app.book.open == []                                  # discarded, nothing re-posted
    state = json.loads((tmp_path / "real_state.json").read_text())
    assert state["open_positions"] == [] and app.harness.sim("blofin")._resting.get(SYM, {}) == {}


async def test_shutdown_drops_a_pending_tt_upgrade(tmp_path):
    app = build_app(tmp_path)
    pos = await _resting_tm(app)
    markets = _count_calls(app.harness.sim("blofin"), "place_market") + _count_calls(app.harness.sim("mexc"), "place_market")
    app.running = False
    app._spawn(app.executor.upgrade_to_tt(pos))
    await app.shutdown()
    assert markets == [] and app.book.open == [] and app.book.closed == []      # no new position opens on the way down


async def test_shutdown_flushes_pending_alerts(tmp_path):
    app = build_app(tmp_path)
    delivered = []

    async def slow_notify(text):
        await asyncio.sleep(0.1)
        delivered.append(text)
    app.executor.notify = slow_notify
    app.running = False
    app.executor._say("DEGRADED #1 XYZUSDT: check the venue by hand")
    await app.shutdown()
    assert delivered == ["DEGRADED #1 XYZUSDT: check the venue by hand"]         # the alert survives the process exit


# ---- final-review round --------------------------------------------------------------------------------------

async def test_restored_exit_maker_on_a_removed_venue_is_closed_in_paper(tmp_path):
    app = build_app(tmp_path)
    app.book.new(SYM, "gate", "mexc", EXIT_MAKER_RESTING, "TT", size_usd=25.0, maker_venue="gate", entry_time=app.clock())
    app._check_position_venues()                       # EXIT_MAKER_RESTING -> CLOSED must be legal, or this crash-loops under systemd
    assert app.book.open == [] and app.book.closed[-1].exit_reason == "venue_removed"


async def _stuck_hedging(app):
    """A partial maker fill hedged, the remainder cancelled — but the venue's `canceled` event never arrives."""
    sim = app.harness.sim("blofin")
    orig, dropped = sim._handler, []

    def lossy(ev):
        if ev.state == "canceled":
            dropped.append(ev)
            return
        orig(ev)
    sim._handler = lossy
    pos = await _resting_tm(app)
    await asyncio.sleep(0.005)
    app.on_bbo(bbo("blofin", 1.0061, 1.0062, 1.0, bq=20.0))        # 50 % of 20 -> 10 of the 20 contracts fill
    await settle()
    assert pos.status == "HEDGING" and pos.maker_cancel_sent and dropped and pos.hedged_qty >= 9.9   # residual waits for the terminal
    return pos


async def test_hedging_without_a_terminal_event_is_rescued_by_the_sweep(tmp_path, caplog):
    import time
    app = build_app(tmp_path)
    pos = await _stuck_hedging(app)
    await app.executor.retry_degraded()                             # too early: nothing to do yet
    assert pos.status == "HEDGING"
    app.executor.clock = lambda: time.time() + 3.0                  # past HEDGING_SWEEP_AFTER_S
    await app.executor.retry_degraded()                             # the sweep asks the venue and feeds the terminal state in
    await settle()
    assert pos.status == OPEN and pos.filled_a == 10.0 and pos.filled_b == 1.0 and "pulled canceled" in caplog.text


async def test_hedging_on_a_silent_venue_degrades_after_stuck_s(tmp_path, caplog, monkeypatch):
    import time
    from bbo_trader import execution
    app = build_app(tmp_path)
    pos = await _stuck_hedging(app)
    sim = app.harness.sim("blofin")

    async def silent(*a, **k):
        return None
    sim.query_order = silent
    app.executor.clock = lambda: time.time() + 61.0                 # past HEDGING_STUCK_S
    await app.executor.retry_degraded()
    await settle()
    assert pos.status == "DEGRADED" and "HEDGING_STUCK" in caplog.text and pos.filled_a == 10.0
    assert any("DEGRADED #1" in n for n in app.harness.notes)
    monkeypatch.setattr(execution, "DEGRADED_RETRY_S", 0.0)
    app.harness.quote("blofin", 1.0061, 1.0062)
    app.harness.quote("mexc", 1.0000, 1.0010)
    await app.executor.retry_degraded()                             # retry_degraded now owns it: both legs closed
    await settle()
    assert pos.status == CLOSED and await sim.positions() == [] and await app.harness.sim("mexc").positions() == []


async def test_naked_exposure_alert_fires_once(tmp_path, caplog):
    app = build_app(tmp_path)
    now = app.clock()
    pos = app.book.new(SYM, "blofin", "mexc", "HEDGING", "TM", size_usd=25.0, maker_venue="blofin", maker_side="sell",
                       maker_client_id="m-1", maker_filled_qty=10.0, hedged_qty=0.0, maker_fill_ts=now - 2.0, entry_time=now)
    await app.executor.retry_degraded()
    await asyncio.sleep(0.01)
    assert f"NAKED_EXPOSURE #{pos.id}" in caplog.text and app.metrics.funnel["naked_exposure"] == 1
    assert any("NAKED_EXPOSURE" in n for n in app.harness.notes)
    await app.executor.retry_degraded()
    assert app.metrics.funnel["naked_exposure"] == 1                 # once per position


async def test_fee_mismatch_is_logged_and_alerted_once_per_venue_per_hour(tmp_path, caplog):
    from dataclasses import replace as dc_replace
    from tests.conftest import MEXC_FEES
    app = build_app(tmp_path)
    h = app.harness
    app.venues["mexc"].fees = dc_replace(MEXC_FEES, taker=0.05)       # we believe 0.05 %; the venue charges 0.02 %
    h.quote("blofin", 1.0050, 1.0060)
    h.quote("mexc", 1.0000, 1.0008)
    pos = await h.ex.enter_tt(Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0))
    await asyncio.sleep(0.01)
    assert "FEE_MISMATCH mexc" in caplog.text and "FEE_MISMATCH blofin" not in caplog.text
    assert app.metrics.funnel["fee_mismatch"] == 1 and sum("FEE_MISMATCH" in n for n in h.notes) == 1
    await h.ex.exit_tt(pos, "test")                                  # the exit fill mismatches too: counted, not re-alerted
    await asyncio.sleep(0.01)
    assert app.metrics.funnel["fee_mismatch"] == 2 and sum("FEE_MISMATCH" in n for n in h.notes) == 1


async def test_fill_quality_abort_covers_tt_and_tm_entries(tmp_path, caplog):
    app = build_app(tmp_path, min_fill_spread_pct=0.50)             # every realized entry spread below 0.50 % is refused
    h = app.harness
    h.quote("blofin", 1.0050, 1.0060)
    h.quote("mexc", 1.0000, 1.0008)
    assert await h.ex.enter_tt(Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0)) is None
    await settle()
    tt = app.book.closed[-1]
    assert tt.exit_reason == "fill_quality_abort" and app.risk.cooldowns.get(SYM, 0) > app.clock()
    assert await h.sim("blofin").positions() == [] and await h.sim("mexc").positions() == []
    (tmp_path / "tm").mkdir()
    app = build_app(tmp_path / "tm", min_fill_spread_pct=0.60)      # fresh books (the TT loss blacklisted the symbol); TM realizes 0.51 %
    h = app.harness
    pos = await _resting_tm(app)                                     # TM: the maker fills, the hedge lands, the spread is poor
    await asyncio.sleep(0.005)
    app.on_bbo(bbo("blofin", 1.0061, 1.0062, 1.0, bq=100.0))
    await settle()
    assert pos.status == CLOSED and pos.exit_reason == "fill_quality_abort" and "FILL_QUALITY_ABORT #%d %s TM" % (pos.id, SYM) in caplog.text
    assert await h.sim("blofin").positions() == [] and await h.sim("mexc").positions() == []


async def test_tm_exit_reason_labels_slipped_exits(tmp_path):
    app = build_app(tmp_path)
    pos = app.book.new(SYM, "blofin", "mexc", OPEN, "TT", size_usd=25.0, exit_price_a=1.0010, exit_price_b=1.0000)
    assert app.executor._tm_exit_reason(pos) == "take_profit"        # realized 0.10 % <= 0.15 % target
    pos.exit_price_a = 1.0060                                         # realized 0.60 %: the hedge crossed a market that had moved
    assert app.executor._tm_exit_reason(pos) == "tm_exit_slipped" and app.metrics.funnel["tm_exit_slipped"] == 1


async def test_requote_is_not_double_fired_before_the_task_runs(tmp_path):
    import time
    app = build_app(tmp_path)
    pos = await _resting_tm(app)
    requotes = _count_calls(app.executor, "requote")
    t1 = time.time() + 1.5                                            # past blofin's 1 s min requote gap
    app.clock = app.evaluator.clock = lambda: t1
    app.on_bbo(bbo("blofin", 1.0043, 1.0065, 1.0))                   # the peg moved: one requote
    app.on_bbo(bbo("blofin", 1.0043, 1.0065, 1.0))                   # the same book again before that task ran
    await settle()
    assert len(requotes) == 1 and pos.maker_rest_price != 1.0061


async def test_halt_cancels_a_resting_entry_even_if_the_sweep_cancel_failed(tmp_path):
    app = build_app(tmp_path)
    pos = await _resting_tm(app)
    app.risk.halt("test")                                             # the halt edge's cancel_all_resting did not reach this order
    app._drive(pos)
    await settle()
    assert app.book.open == [] and app.metrics.funnel["maker_cancelled"] == 1


async def test_book_leg_flat_accumulates_a_partial_exit(tmp_path):
    app = build_app(tmp_path)
    h = app.harness
    h.quote("blofin", 1.0050, 1.0060)
    pos = app.book.new(SYM, "blofin", "mexc", OPEN, "TT", size_usd=25.0, filled_a=20.0, exit_filled_a=5.0, exit_price_a=1.0010,
                       entry_time=app.clock())
    app.executor._book_leg_flat(pos, "a", "blofin")
    assert pos.exit_filled_a == 20.0 and pos.exit_price_a == pytest.approx((1.0010 * 5 + 1.0055 * 15) / 20)   # mark = mid
```

- [x] **Step 1b: Write the registry tests**

`tests/test_registry.py`:

```python
from dataclasses import replace

import pytest

from bbo_trader.config import VenueConfig, RateLimits
from bbo_trader.quotes import QuoteBoard
from bbo_trader.venues.registry import build_venues
from bbo_trader.venues.sim import SimVenue
from tests.conftest import make_cfg


def test_paper_wiring_skips_off_and_adapterless_venues(tmp_path, caplog):
    cfg = make_cfg(tmp_path)
    cfg = replace(cfg, venues=cfg.venues + (VenueConfig("gate", "off", 0.05, 0.02, RateLimits()),))
    venues = build_venues(cfg, lambda b: None, QuoteBoard(cfg.stale_quote_s), session=None)
    assert set(venues) == {"mexc", "blofin"} and "VENUE_SKIPPED okx" in caplog.text   # okx: no adapter yet; gate: off
    for v in venues.values():
        assert isinstance(v.trading, SimVenue) and v.private is v.trading and v.tradeable
        assert v.trading.specs is v.specs                          # the sim shares the bundle's spec dict (mutated in place)
        assert v.public is not None and v.market is not None and v.public.contract_size == {}


def test_live_mode_refuses_and_lists_every_problem(tmp_path):
    cfg = make_cfg(tmp_path, mode="live")
    with pytest.raises(RuntimeError) as e:
        build_venues(cfg, lambda b: None, QuoteBoard(2.0), session=None)
    msg = str(e.value)
    assert msg.startswith("live mode refused") and "mexc" in msg and "blofin" in msg and "Plan 2" in msg
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_app.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'bbo_trader.app'`

- [x] **Step 3: Implement `bbo_trader/venues/registry.py`**

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
    problems: list[str] = []
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
                    problems.append(f"no live trading adapter for {vc.name} (Plan 2)")
                elif not (vc.api_key and vc.api_secret):
                    problems.append(f"{vc.name}: API key/secret missing for live mode")
                else:
                    v.trading, v.private = LIVE_ADAPTERS[vc.name](vc, session, clock)
        out[vc.name] = v
    if problems:                                  # every problem at once: one restart per finding is not acceptable
        raise RuntimeError("live mode refused: " + "; ".join(problems))
    return out
```

- [x] **Step 4: Implement `bbo_trader/app.py`**

```python
"""App: wires quotes → strategy → executor, runs the periodic sweep, persists state, handles operator commands.

Process-boundary discipline (the soak restarts under systemd): shutdown DRAINS in-flight order tasks before it
cancels resting orders, waits for those cancels to settle, then saves, so the state file does not lag the venues;
every status that can be persisted is owned by some loop after a restart (`_adopt_transients`, which books any
maker fill before handing the position over); a task that dies stops the process loudly (`TASK_DIED`) so systemd
restarts it with fresh sockets instead of letting it run blind."""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Awaitable, Callable

from .config import Config
from .discovery import build_universe, symbols_for_venue
from .execution import Executor
from .metrics import Metrics, CoverageWatchdog
from .models import (BBO, Intent, Position, MAKER_RESTING, OPEN, EXIT_MAKER_RESTING, TT_ENTERING, HEDGING,
                     EXIT_HEDGING, TT_EXITING, DEGRADED)
from .positions import PositionBook, StateStore, StateCorrupt, build_state, transition
from .quotes import QuoteBoard
from .risk import RiskManager
from .strategy import PairEvaluator
from .venues.base import Venue, VenueError
from .venues.sim import SimVenue

log = logging.getLogger("bbo.app")

SHUTDOWN_DRAIN_S = 15.0        # worst case per taker leg: EVENT_GRACE_S 1.5 + POLL_MAX_S 5, twice
SHUTDOWN_CANCEL_S = 10.0       # a hung venue must not cost us the final state save
SHUTDOWN_SETTLE_S = 5.0        # cancel acks arrive as order events; a save before them persists a status a restart calls stuck
NOTIFY_FLUSH_S = 3.0           # after the save: let alerts raised during the drain reach Telegram before the session closes
CLOSE_ALL_DRAIN_S = 10.0       # /close_all lets in-flight entries land first, or they open behind our back
QUOTE_REFRESH_FRAC = 0.5       # REST-refresh an open position's leg once its quote is past half the staleness budget
REST_WARM_S = 10.0             # while positions are open, touch each venue's REST at least this often so the pooled
                               # connection stays warm (aiohttp drops it after 15 s idle): a fallback on a cold connection
                               # pays a new TLS handshake, and bursts of those from an IP already streaming 8+ WS shards
                               # to the same host showed SYN-retransmit tails of 1-5 s (probe 2026-09-05) — past the budget
_MAKER_FLOW = (MAKER_RESTING, HEDGING, EXIT_MAKER_RESTING, EXIT_HEDGING)
_STOPPING_REFUSES = ("REQUOTE", "TM_EXIT", "UPGRADE_TT")   # no NEW order may rest or open once we are shutting down
UNIVERSE_RETRY_S = 60.0        # market-data refresh cadence while the universe is empty (start-up blip)
MARKET_DATA_S = 3600.0
_ORPHANED_ON_RESTART = (TT_ENTERING, HEDGING, EXIT_HEDGING, TT_EXITING)


class VenueMissing(RuntimeError):
    """A restored open position references a venue the registry cannot trade on."""


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
        self._notify_tasks: set[asyncio.Task] = set()
        self._fallback_tasks: set[asyncio.Task] = set()
        self._fallback_inflight: set[tuple[str, str]] = set()
        self._rest_locks: dict[str, asyncio.Lock] = {}      # one fallback REST call per venue at a time: reuse the warm connection
        self._last_rest: dict[str, float] = {}
        self._last_state_save = 0.0
        self._scanner: list[dict] = []
        self._last_scan = 0.0
        self.watchdog = CoverageWatchdog(board, list(venues), floor=cfg.coverage_floor, frac=cfg.coverage_frac, clock=clock)
        self.running = True

    # ---- helpers ------------------------------------------------------------------
    def _spawn_into(self, coro: Awaitable, tasks: set, label: str) -> None:
        async def guard():
            try:
                await coro
            except Exception:  # noqa: BLE001
                log.exception("%s", label)
        t = asyncio.get_running_loop().create_task(guard())
        tasks.add(t)
        t.add_done_callback(tasks.discard)

    def _spawn(self, coro: Awaitable) -> None:
        """Order tasks (entries, exits, hedges): the shutdown drain waits for these."""
        self._spawn_into(coro, self._tasks, "APP_TASK_ERROR")

    def _notify(self, text: str) -> None:
        """Telegram sends live in their own set: the shutdown drain waits for order legs, never for a hung send;
        `shutdown()` flushes them after the save."""
        if self.telegram is not None:
            self._spawn_into(self.telegram.send(text), self._notify_tasks, "NOTIFY_ERROR")

    @property
    def n_trade_venues(self) -> int:
        return len([v for v in self.venues.values() if v.tradeable])

    def starting_capital(self) -> float:
        return self.cfg.paper_capital_per_venue * self.n_trade_venues

    def equity(self) -> float:
        if self.cfg.mode == "paper":
            return self.starting_capital() + self.book.total_pnl_usd
        total = sum(b.get("total", 0.0) for b in self.risk.balances.values())
        return total if total > 0 else self.starting_capital()

    def _supervise(self, name: str, t: asyncio.Task) -> asyncio.Task:
        """A long-lived task must never die quietly: stop the process so systemd restarts it with fresh sockets."""
        def done(task: asyncio.Task) -> None:
            if task.cancelled():
                return
            exc = task.exception()
            if exc is not None:
                log.critical("TASK_DIED %s: %r — stopping so systemd restarts with fresh sockets", name, exc, exc_info=exc)
                self._notify(f"TASK_DIED {name}: {exc!r} — restarting")
                self.running = False
        t.add_done_callback(done)
        return t

    async def _drain(self, timeout: float) -> None:
        """Wait for in-flight order tasks (entries, exits, hedges) so the book matches the venues before we act.
        Deadlines run on the loop clock, not `self.clock` (a frozen test clock must not spin this forever)."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            pending = [t for t in (self._tasks | self.executor._tasks) if not t.done()]
            if not pending:
                return
            left = deadline - loop.time()
            if left <= 0:
                log.error("DRAIN_TIMEOUT %d order tasks unfinished after %.0fs — state may lag the venues", len(pending), timeout)
                return
            await asyncio.wait(pending, timeout=min(left, 1.0))

    async def _settle_makers(self, timeout: float) -> None:
        """After cancel_all_resting: the acks arrive as order events and finalize in their own tasks. Wait until no
        position is in a maker-flow status (and nothing is in flight) so the saved state says OPEN/CLOSED, not a
        MAKER_RESTING a restart would adopt as stuck and an operator would be told to check by hand."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            pending = [t for t in (self._tasks | self.executor._tasks) if not t.done()]
            unsettled = self.book.by_status(*_MAKER_FLOW)
            if not pending and not unsettled:
                return
            if loop.time() >= deadline:
                log.warning("SHUTDOWN_UNSETTLED %d maker-flow positions, %d tasks still pending after %.0fs — saving anyway",
                            len(unsettled), len(pending), timeout)
                return
            await asyncio.sleep(0.05)

    # ---- quote path ----------------------------------------------------------------
    def on_bbo(self, bbo: BBO) -> None:
        if bbo.ts_local > self.clock() + 1.0:      # a receive time in the future would never go stale: refuse it
            n = self._skewed = getattr(self, "_skewed", 0) + 1
            if n in (1, 10, 100) or n % 1000 == 0:
                log.error("QUOTE_TS_SKEW #%d %s %s ts_local=%.3f now=%.3f", n, bbo.venue, bbo.symbol, bbo.ts_local, self.clock())
            return
        if not self.board.set(bbo):
            return
        v = self.venues.get(bbo.venue)
        if v is not None and isinstance(v.trading, SimVenue):
            v.trading.on_quote(bbo)
        self.on_quote(bbo.symbol)

    def on_quote(self, symbol: str) -> None:
        try:
            self._on_quote_unguarded(symbol)
        except Exception:  # noqa: BLE001 — an App bug must be named as such, not counted as a dropped venue frame
            log.exception("QUOTE_ERROR %s", symbol)

    def _on_quote_unguarded(self, symbol: str) -> None:
        positions = self.book.for_symbol(symbol)
        if positions:                     # one position per symbol: a held symbol is driven, never re-evaluated
            for pos in positions:
                self._drive(pos)
            return
        if not self.running or symbol in self._pending_entries or symbol not in self.universe:
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
        try:
            self._drive_unguarded(pos)
        except Exception:  # noqa: BLE001 — one bad position must not stop the sweep, the feed or the heartbeat
            log.exception("DRIVE_ERROR #%d %s", pos.id, pos.symbol)

    def _drive_unguarded(self, pos: Position) -> None:
        if pos.status == MAKER_RESTING:
            it = self.evaluator.evaluate_resting(pos)
        elif pos.status in (OPEN, EXIT_MAKER_RESTING):
            it = self.evaluator.evaluate_exit(pos, self.book.resting_counts())
        else:
            return
        if it.kind == "NONE":
            return
        if not self.running and it.kind in _STOPPING_REFUSES:
            log.info("STOPPING #%d %s: %s refused, cancel_all_resting/TT exits only", pos.id, pos.symbol, it.kind)
            return
        if it.kind == "REQUOTE":
            pos.maker_last_requote_ts = self.clock()     # stamped NOW: the next quote must not spawn a second requote
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
        """Hourly: specs (never replaced by an empty set — that would collapse the universe and unsubscribe
        every feed) and 24 h volumes. Funding has its own 5-minute loop (4 h settlement grids)."""
        held = {pos.symbol for pos in self.book.open}

        async def one(v: Venue):
            specs = await v.market.fetch_specs()
            if not specs:
                raise VenueError(f"{v.name}: empty spec set — keeping the {len(v.specs)} known contracts")
            for sym in held:                         # a delisted symbol with an open position keeps its spec
                if sym in v.specs and sym not in specs:
                    specs[sym] = v.specs[sym]
            v.specs.clear()
            v.specs.update(specs)
            v.public.set_specs(v.specs)
            v.market.specs = v.specs
            try:
                volumes = await v.market.fetch_volumes()
                v.volumes.clear()
                v.volumes.update(volumes)
            except Exception as e:  # noqa: BLE001 — the volume gate then fails open, counted as volume_unknown
                log.warning("VOLUMES_FAILED %s: %r", v.name, e)
        with_market = [v for v in self.venues.values() if v.market is not None]
        results = await asyncio.gather(*(one(v) for v in with_market), return_exceptions=True)
        for v, r in zip(with_market, results):
            if isinstance(r, Exception):
                log.warning("SPECS_FAILED %s: %r", v.name, r)
        self.apply_universe()
        if not self.universe:
            log.error("UNIVERSE_EMPTY — no symbol on two trade venues; retrying market data in %.0fs", UNIVERSE_RETRY_S)

    def _market_data_interval(self) -> float:
        return UNIVERSE_RETRY_S if not self.universe else MARKET_DATA_S

    async def _market_data_loop(self) -> None:
        """Hourly refresh, but a fast retry while the universe is empty (a start-up network blip must not cost an hour)."""
        while self.running:
            await asyncio.sleep(self._market_data_interval())
            if not self.running:
                return
            try:
                await self.refresh_market_data()
            except Exception:  # noqa: BLE001
                log.exception("LOOP_ERROR refresh_market_data")

    async def refresh_funding(self) -> None:
        for v in self.venues.values():
            if v.market is None:
                continue
            try:
                self.risk.set_funding(v.name, await v.market.fetch_funding())
            except Exception as e:  # noqa: BLE001
                log.warning("FUNDING_FAILED %s: %r", v.name, e)

    def apply_universe(self) -> None:
        specs = {name: v.specs for name, v in self.venues.items()}
        self.evaluator.specs = specs
        self.evaluator.volumes = {name: v.volumes for name, v in self.venues.items()}
        self.universe = build_universe(specs, self.cfg.trade_venues, self.cfg.blocked_symbols,
                                       {v.cfg.name: v.cfg.symbol_whitelist for v in self.venues.values()})
        held = {pos.symbol for pos in self.book.open}
        for name, v in self.venues.items():
            if v.public is not None and v.cfg.role == "trade":
                subscribed = set(symbols_for_venue(self.universe, name))
                subscribed |= {s for s in held if s in v.specs}      # open positions stay subscribed
                v.public.set_symbols(sorted(subscribed))
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

    def heartbeat_path(self):
        return self.cfg.data_dir / f"bbo_heartbeat_{self.cfg.mode}"     # never `heartbeat_live`: that is the legacy bot's

    def _heartbeat(self, now: float) -> None:
        try:
            self.cfg.data_dir.mkdir(parents=True, exist_ok=True)
            self.heartbeat_path().write_text(str(int(now)))
        except OSError as e:
            log.debug("heartbeat write failed: %r", e)

    def bbo_section(self, now: float) -> dict:
        return {"mode": self.cfg.mode, "universe": len(self.universe), "metrics": self.metrics.to_dict(),
                "coverage": self.watchdog.to_dict(),
                "connected": {n: bool(v.public.connected) for n, v in self.venues.items() if v.public is not None},
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
                            starting_capital=self.starting_capital(),
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
        self._check_position_venues()
        log.info("STATE loaded: %d open, %d closed, pnl=$%.2f halted=%s", len(self.book.open), len(self.book.closed),
                 self.book.total_pnl_usd, self.risk.halted)

    def _adopt_transients(self) -> None:
        """Every status that can be persisted must be driven by some loop after a restart. Positions saved mid-flight
        (SIGKILL, or a shutdown that could not drain) have no task behind them any more:
        - TT_ENTERING / HEDGING / EXIT_HEDGING / TT_EXITING, and a MAKER_RESTING / EXIT_MAKER_RESTING whose resting
          order had filled (or with anything on its legs) → DEGRADED via `Executor.adopt_restored`, which books the
          maker fill on its leg first; retry_degraded then closes what the books show (a leg the venue no longer
          holds is booked flat via `nothing to reduce`);
        - MAKER_RESTING with nothing filled per our books → discarded — the resting order died with the process in paper;
        - EXIT_MAKER_RESTING with nothing filled → OPEN with the maker fields cleared: the position is still held, the
          exit is re-decided.
        None of this asks the venue: a zero-fill transient closes as `recovered` on our books alone, and a resting
        order at a live venue is only reported. Live reconciliation (Plan 2) must query positions and open orders at
        start-up before trusting any of it. `run()` calls this AFTER the first market-data refresh so the specs are
        known and an adopted position's size is re-derived from the legs it really holds."""
        now = self.clock()
        for pos in list(self.book.open):
            maker_fill = pos.maker_filled_qty > 1e-12 or pos.maker_booked_qty > 1e-12
            on_legs = pos.filled_a > 1e-12 or pos.filled_b > 1e-12
            if pos.status in _ORPHANED_ON_RESTART or (pos.status == MAKER_RESTING and (maker_fill or on_legs)) \
                    or (pos.status == EXIT_MAKER_RESTING and maker_fill):
                log.error("STUCK_TRANSIENT #%d %s restored as %s with no owner (maker filled %s) -> DEGRADED; %s",
                          pos.id, pos.symbol, pos.status, pos.maker_filled_qty,
                          "check the venue for the resting order by hand" if self.cfg.mode == "live" else "paper")
                self.executor.adopt_restored(pos)
            elif pos.status == MAKER_RESTING:
                log.error("STUCK_RESTING #%d %s restored as MAKER_RESTING (%s resting order %s) — discarded; %s",
                          pos.id, pos.symbol, pos.maker_venue, pos.maker_client_id,
                          "check the venue for the order by hand" if self.cfg.mode == "live" else "paper: nothing is held")
                self.book.discard(pos)
            elif pos.status == EXIT_MAKER_RESTING:
                log.error("STUCK_EXIT_MAKER #%d %s restored as EXIT_MAKER_RESTING — reopened as OPEN, exit re-decided; %s",
                          pos.id, pos.symbol, "cancel the resting order at the venue by hand" if self.cfg.mode == "live" else "paper")
                transition(pos, OPEN)
                pos.maker_venue, pos.maker_client_id, pos.maker_order_id = "", "", ""
                pos.maker_cancel_sent = pos.requote_pending = pos.upgrade_pending = False
                pos.exit_mode = ""
                pos.entry_time = pos.entry_time or now
                self.book.dirty = True

    def _check_position_venues(self) -> None:
        """An open position on a venue that is no longer tradeable (role changed to off/quote_only, keys
        removed) cannot be managed. Live: refuse to start — the venue still holds it. Paper: book it closed."""
        now = self.clock()
        for pos in list(self.book.open):
            bad = sorted({v for v in (pos.venue_a, pos.venue_b, pos.maker_venue)
                          if v and not (v in self.venues and self.venues[v].tradeable)})
            if not bad:
                continue
            if self.cfg.mode == "live":
                raise VenueMissing(f"open position #{pos.id} {pos.symbol} on non-tradeable venue(s) {bad}: "
                                   f"re-enable the venue or flatten it by hand")
            log.error("VENUE_MISSING #%d %s on %s — paper mode: booked closed", pos.id, pos.symbol, bad)
            self.book.close(pos, "venue_removed", now, counts_as_trade=False)

    # ---- operator commands ---------------------------------------------------------------
    async def handle_command(self, cmd: str) -> None:
        if cmd == "/stop":
            self.risk.halt("telegram")
            await self.executor.cancel_all_resting()
        elif cmd == "/start":
            self.risk.resume()
        elif cmd == "/close_all":
            self.risk.halt("close_all")
            await self._drain(CLOSE_ALL_DRAIN_S)             # in-flight entries land first, or they open behind our back
            await self.executor.cancel_all_resting()
            for pos in list(self.book.by_status(OPEN, EXIT_MAKER_RESTING)):
                await self.executor.exit_tt(pos, "halt")
        elif cmd == "/status" and self.telegram is not None:
            await self.telegram.send(f"{self.cfg.mode} equity ${self.equity():.2f} open={len(self.book.open)} "
                                     f"trades={self.book.total_trades} pnl=${self.book.total_pnl_usd:+.2f} "
                                     f"halted={self.risk.halted} coverage={self.watchdog.last}")

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
                try:
                    bal = await v.trading.balance()
                except Exception as e:  # noqa: BLE001 — the risk gate fails closed on a missing/stale cache in live
                    log.warning("BALANCE_FAILED %s: %r", v.name, e)
                    continue
                self.risk.set_balance(v.name, bal.get("available", 0.0), bal.get("total", 0.0))

    async def _quote_fallback(self) -> None:
        """Open positions must never depend on WS health. Every 0.5 s, for each open position's leg: REST-refresh it
        once its quote is past half the staleness budget (a resting maker is cancelled the moment a leg reads stale,
        so refreshing only after the boundary always loses that race), and refresh one leg per venue every
        `REST_WARM_S` regardless so the pooled connection stays warm. Fetches run as their own tasks — one per leg in
        flight, one REST call per venue at a time so each reuses that warm connection — bounded by the venue's full
        staleness budget: a late answer is still a fresh quote (ts_local is stamped at receipt), so the bound only
        caps how long a hung call blocks the leg's retry, and a hung call never delays the tick or the other legs."""
        now = self.clock()
        warmed: set[str] = set()
        for pos in list(self.book.open):
            for venue in (pos.venue_a, pos.venue_b):
                v = self.venues.get(venue)
                if v is None or v.market is None or (venue, pos.symbol) in self._fallback_inflight:
                    continue
                q = self.board.get(venue, pos.symbol)
                age = (now - q.ts_local) if q is not None else float("inf")
                warm = venue not in warmed and now - self._last_rest.get(venue, 0.0) >= REST_WARM_S
                if age < self.board.stale_for(venue) * QUOTE_REFRESH_FRAC and not warm:
                    continue
                warmed.add(venue)
                self._last_rest[venue] = now
                self._fallback_inflight.add((venue, pos.symbol))
                self._spawn_into(self._fetch_quote(v, pos.symbol), self._fallback_tasks, "QUOTE_FALLBACK_ERROR")

    async def _fetch_quote(self, v: Venue, symbol: str) -> None:
        key = (v.name, symbol)
        budget = self.board.stale_for(v.name)
        lock = self._rest_locks.setdefault(v.name, asyncio.Lock())

        async def fetch():
            async with lock:
                return await v.market.fetch_bbo(symbol)
        try:
            bbo = await asyncio.wait_for(fetch(), budget)
        except asyncio.TimeoutError:
            n = self.metrics.funnel["fallback_timeout"] = self.metrics.funnel["fallback_timeout"] + 1
            if n in (1, 10, 100) or n % 1000 == 0:
                log.warning("QUOTE_FALLBACK_TIMEOUT #%d %s %s: no answer within %.1fs", n, v.name, symbol, budget)
            return
        except Exception as e:  # noqa: BLE001
            self.metrics.funnel["fallback_failed"] += 1
            log.warning("QUOTE_FALLBACK_FAILED %s %s: %r", v.name, symbol, e)
            return
        finally:
            self._fallback_inflight.discard(key)
        if bbo is not None:
            self.metrics.funnel["fallback_ok"] += 1
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
        self._adopt_transients()
        await self.refresh_funding()
        for v in self.venues.values():
            if v.private is not None:
                v.private.set_handler(self.executor.on_order_event)
        await self._refresh_balances()
        tasks = [self._supervise(f"public:{n}", asyncio.create_task(v.public.run()))
                 for n, v in self.venues.items() if v.public is not None]
        tasks += [self._supervise(f"private:{n}", asyncio.create_task(v.private.run()))
                  for n, v in self.venues.items() if v.private is not None]
        tasks += [self._supervise(name, asyncio.create_task(coro)) for name, coro in (
            ("loop_lag", self.metrics.sample_loop_lag()),
            ("sweep", self._loop(0.5, self.sweep_once)),
            ("quote_fallback", self._loop(0.5, self._quote_fallback)),
            ("balances", self._loop(30.0, self._refresh_balances)),
            ("watchdog", self._loop(60.0, self._watchdog)),
            ("market_data", self._market_data_loop()),
            ("funding", self._loop(300.0, self.refresh_funding)),
            ("telegram", self._loop(5.0, self._poll_telegram)))]
        banner = (f"BBO trader started [{self.cfg.mode}] venues={list(self.venues)} universe={len(self.universe)} "
                  f"open={len(self.book.open)} halted={self.risk.halted}")
        log.info(banner)
        if self.telegram is not None:
            await self.telegram.send(banner)
        try:
            while self.running:
                await asyncio.sleep(0.5)
        finally:
            await self.shutdown()
            stragglers = [*tasks, *self._fallback_tasks]
            for t in stragglers:
                t.cancel()
            await asyncio.gather(*stragglers, return_exceptions=True)

    async def shutdown(self) -> None:
        """Drain in-flight order tasks (an entry must land before we cancel and save, or the state file says we hold
        nothing while both legs sit at the venues), cancel every resting order with a timeout, wait for the cancels
        to settle, save, say so, then give pending alerts a moment to reach Telegram."""
        log.info("SHUTDOWN draining in-flight order tasks, cancelling resting orders, saving state")
        self.executor.stopping = True            # a requote/upgrade completing during the drain posts nothing new
        await self._drain(SHUTDOWN_DRAIN_S)
        try:
            await asyncio.wait_for(self.executor.cancel_all_resting(), SHUTDOWN_CANCEL_S)
            await self._settle_makers(SHUTDOWN_SETTLE_S)
        except Exception:  # noqa: BLE001 — a stuck venue must not cost us the save
            log.exception("SHUTDOWN cancel_all_resting failed — saving state anyway")
        finally:
            await self.save_state(self.clock())
        log.info("SHUTDOWN complete: %d open, trades=%d pnl=$%+.2f, state saved",
                 len(self.book.open), self.book.total_trades, self.book.total_pnl_usd)
        alerts = [t for t in (self._notify_tasks | self.executor._notify_tasks) if not t.done()]
        if alerts:                               # the DEGRADED / ORDER_UNRESOLVED raised during the drain must still go out
            await asyncio.wait(alerts, timeout=NOTIFY_FLUSH_S)
```

- [x] **Step 5: Implement `bbo_trader/main.py`**

```python
"""Entry point: config → logging → legacy-bot guard → build everything → App.run()."""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

import aiohttp

from .app import App, VenueMissing
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
    except OSError as e:                 # unreadable: assume the worst, a human decides
        log.error("legacy heartbeat %s unreadable (%r) — treating the legacy bot as running", path, e)
        return True


def data_dir_collides(cfg: Config) -> bool:
    """DATA_DIR must not be the legacy bot's data dir: our `real_state.json` would overwrite its position file."""
    try:
        return cfg.data_dir.resolve() == cfg.legacy_heartbeat_path.parent.resolve()
    except OSError:
        return False


async def run(cfg: Config) -> int:
    if data_dir_collides(cfg):
        log.critical("REFUSED: DATA_DIR %s is the legacy bot's data dir (%s) — give the BBO trader its own DATA_DIR",
                     cfg.data_dir, cfg.legacy_heartbeat_path.parent)
        return 5
    if cfg.mode == "live" and legacy_bot_running(cfg.legacy_heartbeat_path, cfg.legacy_heartbeat_max_age_s, time.time()):
        log.critical("REFUSED: legacy bot heartbeat %s is fresh — stop realtrader.service first", cfg.legacy_heartbeat_path)
        return 2
    board = QuoteBoard(cfg.stale_quote_s, {v.name: v.staleness_override_s for v in cfg.venues if v.staleness_override_s})
    book = PositionBook()
    risk = RiskManager(cfg)
    metrics = Metrics()
    store = StateStore(cfg.data_dir / "real_state.json")
    telegram = Telegram(cfg.telegram_token, cfg.telegram_chat_id) if (cfg.telegram_token or cfg.telegram_chat_id) else None
    async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0 (bbo-trader/1.0)"}) as session:
        app_ref: dict[str, App] = {}

        def on_bbo(bbo):
            a = app_ref.get("app")
            if a is not None:
                a.on_bbo(bbo)

        try:
            venues = build_venues(cfg, on_bbo, board, session)
        except RuntimeError as e:
            log.critical("REFUSED: %s", e)
            return 6
        fees = {n: v.fees for n, v in venues.items()}
        evaluator = PairEvaluator(cfg, board, fees, {}, {}, risk, metrics.funnel)
        executor = Executor(cfg, venues, board, book, risk, metrics,
                            notify=(telegram.send if telegram else None))
        app = App(cfg, venues, board, book, risk, metrics, executor, evaluator, store, telegram)
        app_ref["app"] = app
        loop = asyncio.get_running_loop()
        signals = {"n": 0}

        def on_signal() -> None:
            signals["n"] += 1
            if signals["n"] == 1:
                log.info("SIGNAL received — shutting down (a second signal forces exit)")
                app.running = False
            else:
                log.critical("SIGNAL received twice — forcing exit without a final save")
                os._exit(130)
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, on_signal)
        log.info("=== BBO trader starting [%s] venues=%s ===", cfg.mode, list(venues))
        try:
            await app.run()
        except StateCorrupt as e:
            log.critical("REFUSED: state file corrupt (%s) — inspect real_state.json / .bak, repair, then restart", e)
            return 3
        except VenueMissing as e:
            log.error("VENUE_MISSING %s", e)
            return 4
    return 0


def main() -> None:
    cfg = load_config()
    setup_logging(cfg.data_dir, cfg.mode)
    sys.exit(asyncio.run(run(cfg)))


if __name__ == "__main__":
    main()
```

- [x] **Step 6: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_app.py -q`
Expected: `13 passed`

- [x] **Step 7: Run the whole suite and commit**

Run: `.venv/bin/python -m pytest -q`
Expected: `188 passed`

```bash
git add deploy-bbo/bbo_trader/venues/registry.py deploy-bbo/bbo_trader/app.py deploy-bbo/bbo_trader/main.py deploy-bbo/tests/test_app.py deploy-bbo/tests/test_registry.py
git commit -m "feat(bbo): venue registry, App wiring (quotes → strategy → executor, sweep, state), entry point"
```

---

### Task 20: Run scripts, README, first paper run

**Files:**
- Create: `deploy-bbo/start.sh`, `deploy-bbo/bbotrader.service`, `deploy-bbo/README.md`, `deploy-bbo/.env.example`

- [x] **Step 1: Write `start.sh`**

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

- [x] **Step 2: Write the systemd unit `bbotrader.service` (Tokyo, Plan 2 deploys it)**

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

- [x] **Step 3: Write `.env.example`**

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

- [x] **Step 4: Write `README.md`**

```markdown
# BBO Trader (deploy-bbo)

Event-driven cross-venue convergence trader that needs only real-time best bid/ask from each venue.
Every quote update re-evaluates the symbol; opportunities are taken taker/taker when the spread pays for
it immediately, otherwise taker/maker (rest one leg post-only, hedge the other at market on fill).
Spec: `docs/superpowers/specs/2026-09-05-bbo-taker-maker-trader-design.md`.

## Run (paper mode, no keys)

    python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
    ./start.sh                     # MODE=paper DATA_DIR=./data

State: `data/real_state.json` (same schema as the legacy dashboard reads — that dashboard is `deploy-live/dashboard.py`,
a separate component on the legacy bot's branch; run it with `DATA_DIR=<this data dir>`). Log: `data/bbo_trader_paper.log`. Heartbeat: `data/bbo_heartbeat_paper` (never
`heartbeat_live`, which belongs to the legacy bot; DATA_DIR must not be the legacy bot's data dir — refused with exit 5). Flags: `data/stop.flag` halts entries and
cancels resting orders, `data/start.flag` resumes. Telegram: `/stop /start /close_all /status`.

## Tests

    .venv/bin/pip install -r requirements-dev.txt
    .venv/bin/python -m pytest -q

## Layout

`bbo_trader/config.py` (env → Config, venues.json) · `models.py` (BBO, Position, Intent, order events) ·
`quotes.py` (QuoteBoard, staleness) · `edge.py` (pure edge math) · `sizing.py` (lot sizing, hedge plan) ·
`budget.py` (per-venue rate budget) · `strategy.py` (gates → intents) · `risk.py` (halt flags, pair stats,
cooldowns, funding gate) · `execution.py` (TT + pegged maker flows) · `positions.py` (state machine + state
file) · `discovery.py` (universe) · `metrics.py` (latency, funnel, coverage watchdog) · `notify.py` (Telegram) ·
`venues/` (protocols, WS runner, MEXC, BloFin, SimVenue, registry) · `app.py` (wiring + sweep) · `main.py`
(entry point).

## Config

Environment variables override `config.py` defaults; `DATA_DIR/bot_config.json` overrides both
(restart to apply). Venues, fees and rate limits: `config/venues.json`. Blocked symbols:
`config/blocked_symbols.json`.

Per-venue `staleness_override_s` widens the quote-staleness budget (default `STALE_QUOTE_S`, 2 s) for feeds
that push only on change: MEXC's depth channel measured a p99 inter-update gap of 5.2 s (2026-09-05), so it
runs at 5 s — this also lets an entry be priced off a quote up to 5 s old on that venue. BloFin needs none.
Open positions' legs are REST-refreshed at half the budget; the state file's `bbo.metrics.funnel` counters
`fallback_ok` / `fallback_timeout` / `fallback_failed` show whether those refreshes answer in time.
```

- [x] **Step 5: First paper run against live feeds (manual acceptance, ~10 minutes)**

```bash
cd deploy-bbo && MODE=paper DATA_DIR=./data ./start.sh
```

Expected within 2 minutes in the log: `UNIVERSE <n> symbols` with n ≥ 300, `FEED_COVERAGE mexc=<hundreds> blofin=<hundreds>`, `INTENT ...` lines, no `LOOP_LAG` warnings, no tracebacks. Within 10 minutes: at least one `TM_POST` or `TT_ENTER`; `data/real_state.json` updates every ≤ 5 s (`state_saved_at_ts`). Watch `bbo.metrics.funnel`: `fallback_timeout` above ~30 % of `fallback_ok + fallback_timeout` means the REST quote refresh is losing to the stale-cancel (the Task 19 review measured 10 of 14 BloFin fallback calls exceeding 5 s in-app while the endpoint answers in ~300 ms in isolation — cause not yet known); TM entries cancelled `stale` should then be the first thing to investigate. Stop with Ctrl-C: expect `SHUTDOWN draining in-flight order tasks, cancelling resting orders, saving state` and then `SHUTDOWN complete: <n> open, trades=<n> pnl=$..., state saved`; a second Ctrl-C forces exit without a save.

If MEXC shows `FEED_COVERAGE mexc=0`: capture 15 s of raw frames with a probe (`aiohttp` `ws_connect` to `wss://contract.mexc.com/edge`, send one `sub.depth.full` message, print the first 10 frames) and compare with `parse_depth` in `venues/mexc.py`; same for BloFin (`books5`). Fix the parser and its fixture test — never the strategy.

- [x] **Step 6: Commit**

```bash
git add deploy-bbo/start.sh deploy-bbo/bbotrader.service deploy-bbo/README.md deploy-bbo/.env.example
git commit -m "feat(bbo): run script, systemd unit, README, env example"
```

---

## Plan self-review (done while writing)

- **Task 1 amended after code review (2026-09-05):** MODE normalized/validated (`paper`|`live`, else `ValueError`), venue `role` validated, `shared` coerced like other bools, secrets excluded from `repr`, missing blocklist file fails closed, JSON errors name the file; venues.json must be an object and the blocklist a list of strings; `shared` must be a real boolean; Telegram token hidden from repr; MODE checked before any file I/O; 11 tests added (14 total). **Task 2 amended after code review:** `touch_notional` rejects unknown sides, `from_dict` falls back to the ISO timestamps and copies dicts (no aliasing), `NON_TERMINAL` is a frozenset; 3 tests added (7 total). **Task 3 amended after code review:** `mk_bbo` gained a `ts_exchange` kwarg and the quotes test now proves `ts_local` governs staleness, the boundary is inclusive and newer quotes overwrite (mutation-checked). **Task 4 amended after code review:** pegs must be strictly positive and strictly inside the book (`_postable`), `needs_requote` tolerance scales with the tick, `tick_decimals` from the shortest repr (rejects non-positive ticks), relative rounding epsilon, `tm_required_pct(maker_fees, taker_fees, p)`; 7 tests added incl. a deterministic peg property sweep (13 total). **Task 7 amended after code review:** `close()` is idempotent (late duplicate terminal events), `discard()` only from TT_ENTERING/MAKER_RESTING, `OPEN → CLOSED/DEGRADED` for reconciliation, equity history uses the dashboard's `t`/`v` keys (2,000 points), `StateStore` fsyncs and keeps a `.bak`, raises `StateCorrupt` instead of silently starting fresh (live refuses to start, paper falls back to the backup), non-finite floats are sanitized, `next_id` is repaired from the ids in the file, `kill_switch` mirrors the manual halt; `App.save_state` is async (serialization off the loop) and passes realized-only `cash`; the live file is never absent (hard-link `.bak`, unique tmp, lock-serialized saves, a missing file next to a backup is a torn save), non-object JSON is corrupt, CLOSED entries under `open_positions` are routed to `closed`; 4 positions tests + 1 app test added. **Task 6 amended after code review:** a 429 penalty halves capacity for non-priority callers only (hedges/closes/cancels keep the full window, per spec), unknown budget kinds raise, the window boundary is exclusive, `to_dict` clamps at 0 and reports `shared`/`penalized`; `config.py` validates rate limits (`0 <= reserve < min(orders, cancels)`, positive window) and `venues.json` carries 10% headroom; 4 budget tests + 1 config test added. **Task 5 amended after code review:** `size_pair` shrinks the larger leg with a closed-form jump (coarse/fine lot pairs no longer time out) and enforces `min_usd`; `contracts_for_usd`/`lots_floor` fail closed on non-finite inputs; new `hedge_plan` returns hedge qty + covered maker qty + residual, `excess_to_flatten` decides what residual to flatten; 2 tests added (6 total). **Task 16 amended accordingly:** `_hedge_delta` advances `hedged_qty` only by the covered maker quantity and, once the resting order is terminal, flattens residuals beyond `MAX_LEG_MISMATCH_PCT`; both `size_pair` calls pass `min_usd=cfg.min_position_usd`; 1 TM test added (11 total). **Task 7 amended before implementation (from the Task 2 review):** closed positions' dashboard dicts are cached at close time and `closed_keep` defaults to 200, so a state save never re-serializes hundreds of closed positions; 1 test added (5 total). **Task 8 amended after code review (2026-09-05):** flag files are edge-triggered and consumed on read (a Telegram `/start` really resumes; stop wins over a simultaneous start; an undeletable flag is logged and ignored, never raises), gates never raise and fail closed (unknown or quote-only venue → `venue_blocked`; live mode with a missing/stale balance cache → `balance_unknown`; `invalid` for same-venue or non-positive size), the pair win-rate gate reads outcomes inside `pair_stats_window_s` (so a route can recover) and ignores zero-P&L and `counts_as_trade=False` closes, strikes decay after `strike_decay_s` (`pair_strikes` values are `{n, ts}`; `build_state` maps them back to bare counts for the dashboard), the funding gate needs a net cost above `funding_block_min_pct` and reports dead feeds in `stale_funding`, the mismatch guard validates its thresholds, logs `MISMATCH_BLACKLIST` and records when/why, `to_dict()` is a true snapshot and `load()` tolerates corrupt or legacy values; Config gained `strike_decay_s`, `symbol_loss_pct`, `pair_stats_window_s`, `pair_min_trades`, `pair_min_win_rate`, `balance_max_age_s`, `funding_block_min_pct`; Task 19's `sweep_once` logs the no-op flag results; 4 tests added (9 total). Re-review round: `resume()` consumes only `start.flag` (a stop written meanwhile is still honoured), a non-file `start.flag` is logged (`FLAG_NOT_A_FILE`) and ignored while any path named `stop.flag` halts, non-finite values are rejected at load (`pair_strikes`, `pair_stats`) and ingest (`set_funding`), a bad `recent` entry drops the entry not the route, the symbol-loss blacklist applies even to `counts_as_trade=False` closes, and `build_state`'s strike mapping is covered by the positions test. Carry-forward: Plan 2 reconciliation closes must call `record_close(pos, counts_as_trade=False)`. Verified live 2026-09-05: BloFin's `fundingTime` is the UPCOMING settlement (≈2 h ahead at probe time), so Task 13's `parse_funding` needs no change. Third round (approved): a non-iterable `recent` drops the entry not the route, `OverflowError` from `int(inf)` is caught at load, and flag signatures use `lstat` so a dangling symlink named `stop.flag` still halts. **Task 9 amended after code review (2026-09-05):** the time stop fires before the stale-quote gate (a market close needs no quote), a position whose venue left the registry yields NONE/`venue_unknown` (logged once) instead of a KeyError, `UPGRADE_TT` requires the same touch depth as a TT entry, routes are ranked TT-before-TM and then by surplus over the mode's bar (a TM edge is inflated by the maker venue's width), a TT route with a thin taker touch falls back to TM on the same pair (`tt_depth_fallback`), pair-symmetric gates (`insane`, `mismatch`) count once per unordered pair on a direction-free mid spread, `evaluated`/`volume_unknown`/`upgrade_depth` funnel counters, REQUOTE is suppressed while a requote is in flight, a resting exit maker is cancelled (never orphaned) when TM exits are disabled or the policy names a third venue (`tm_exit_disabled`/`no_maker_venue`/`venue_changed`), TM exits require hedge-touch depth (`hedge_depth`), the divergence stop uses a `stop_ref_spread_pct` on the (bid_A − ask_B) basis (new Position field, Task 2), the scanner skips mismatch-blacklisted/insane pairs and sorts by the winning edge, `params` is a cached_property; 6 tests added (13 total). Cross-task: Task 16's `requote` stamps `maker_last_requote_ts` on the attempt and a cancel+new requote keeps the TTL clock (`keep_posted_ts`); Task 19's `_drive` is guarded per position (`DRIVE_ERROR`) and `App.load_state` runs `_check_position_venues` — live refuses to start (`VenueMissing`, exit 4) and paper books the position closed as `venue_removed`; 1 app test added. Quote-only venues in the scanner are Plan 3. **Task 10 amended after code review (2026-09-05):** `MarketData` declares `specs`, the `Trading` docstring fixes units (contracts / quote currency / "buy"|"sell"), the meaning of `OrderAck.ok` and the `balance()` keys, `PublicFeed` states that specs must precede quotes, `fetch_funding` documents fraction + unix seconds, `Venue.specs` is documented as a shared alias (mutate in place), `Venue.tradeable` also requires the private feed, and `Trading.cancel` returns an `OrderAck`: Task 14's `SimVenue.cancel` returns `OrderAck(False, error="terminal")`/`OrderAck(True, order_id)`, Task 16's `_cancel_maker_order` notes rate limits on cancel errors and, when the order is not terminal at the venue, clears `maker_cancel_sent`/`requote_pending`/`upgrade_pending` so the strategy can retry (`CANCEL_FAILED`); the cancel+new requote stamps `maker_last_requote_ts` on the attempt; Task 14 gains `tests/test_venue_protocols.py` (structural conformance of every adapter to the protocols); 1 executor test added. Plan 2 notes: `VenuePosition` may gain `entry_price` for reconciliation P&L; `place_market` has no `position_id` — MexcTrading keeps its own symbol→positionId map (one position per symbol). **Task 11 amended after code review (2026-09-05):** a parse/consumer exception costs one frame, never the socket (`dropped frame #n` logged at 1/10/100/…), `WSAdapter.receive_timeout` (default 10 s) drops a silent socket via `aiohttp.ClientWSTimeout(ws_receive=…)`, a failing keepalive is logged (`keepalive failed`) and closes the socket, `ws.close()` on teardown is bounded to 5 s (a venue that keeps streaming while we leave would park the shard), the pinger is awaited after cancel, repeated server closes log at DEBUG after the first; Tasks 12/13 no longer pass `heartbeat=None` (SpreadWatch runs both venues with aiohttp's 20 s protocol ping); `config.py` rejects `max_topics < 0` (zero shards); 3 ws tests + 1 config test added. Re-review round: `receive_timeout` defaults to None (under the 20 s heartbeat it churned healthy idle sockets; above it PONGs reset it so it could never fire) — liveness is aiohttp's heartbeat for wedged TCP plus a `data_timeout` watchdog (120 s without a frame that parsed to items → `no data for` → reconnect) for dead subscriptions; `closes`/`bad` are cumulative per shard with escalating log sparsity; a shipped-defaults test keeps a quiet-but-ponging socket; 1 ws test added. Third round (approved): the shipped-defaults test pins `receive_timeout is None`/`heartbeat == 20`/`data_timeout == 120`, and a close caused by a heartbeat timeout logs the socket's exception instead of "by server". **Tasks 12/13 amended after the Task 12 code review (2026-09-05, verified against both live APIs):** `_get` raises `VenueError` (new, `venues/base.py`) on a non-200 status, a non-JSON body or an error ENVELOPE (MEXC `success != true`, BloFin `code != "0"` — both venues report errors as HTTP 200), and `fetch_specs` raises rather than returning an empty set; `parse_specs` also requires `apiAllowed` (MEXC: `state` is 0 for every contract, `apiAllowed` false for ~30 live ones); every row parser isolates a malformed row (`SPEC_ROWS_DROPPED`) and uses strict numerics (no `or` defaults); an instrument without a known contract size yields no BBO (sizes span 1e-5…1e7) and `fetch_bbo` returns None without a spec; `to_instrument` rejects non-USDT symbols; MEXC `ts_exchange` prefers `data.cts`; rejected subscriptions (`rs.error` / `event: error`) are logged with escalating sparsity; BloFin sends a User-Agent. Task 19: `refresh_market_data` never applies an empty spec set, funding has its own 5-minute loop (`refresh_funding`; both venues run 4 h and 8 h grids), `_quote_fallback` fetches legs concurrently with a per-call guard (`QUOTE_FALLBACK_FAILED`). Live-shape fixtures + fake-session REST tests: 4 MEXC and 3 BloFin tests added. Re-review (approved): timestamp assertions use `abs=1e-6`, malformed rows carry distinct symbols so a fabricated zero cannot hide behind the good row, a status-only failure case is covered, and the `PublicFeed` docstring states that unknown instruments yield no BBO. **Task 13 amended after code review (2026-09-05, verified live):** `subscribe()` sends ONE message per instrument — BloFin validates a subscribe message atomically, so one delisted instId in a batched message subscribed nothing for the whole shard while our text pings kept the empty socket alive; `parse_books5` ignores non-snapshot actions; `parse_instruments` also requires `contractType == linear` and `assetClass == Crypto` (BloFin lists equity/index/commodity USDT perps that gap when their market is closed); both adapters' `_top` use strict `_num` for levels; the BloFin fixture is a verbatim live BTC-USDT row (contract value 0.001, 0.1-contract lots) with assertions derived from it; tests pin the WS wiring (url, 50 topics, 25 s text ping, 120 s data timeout), the real subscribe ack for a known instrument, `action: update` frames, a JSON NaN literal, and the too-short-symbol guard. Re-review (approved): NaN prices are pinned to raise in both adapters, the synthetic spec rows use lot ≠ min so a transposed `VenueSpec` argument fails, the fixture uses BloFin's real `Stocks` label, and the docstring describes `expireTime` correctly (per-instrument far-future values; `instType` is always SWAP). **Task 14 amended after code review (2026-09-05):** the paper venue no longer flatters the strategy — taker orders fill at the touch that exists AFTER the latency (paper now shows spread decay) and push an `ack` event first, the maker cap is granted once per DISTINCT touch (an unchanged book re-pushed 10×/s is not new flow) and a sub-lot touch fills nothing, reduce-only fills are clamped to the open position (a close against a flat position is rejected with `nothing to reduce`, like the real venues), every fill needs a FRESH quote, resting orders are indexed per symbol (`on_quote` no longer scans every order ever placed) and terminal orders are bounded (`MAX_ORDERS`), positions carry an average cost so realized P&L and fees flow into `balance()`, `place_post_only` rejects `qty <= 0`, `cancel` distinguishes unknown from terminal, `amend` refuses without a fresh quote, the fill task is exception-guarded and a missing spec is logged once; 5 tests added (9 total). Task 16 accordingly: `retry_degraded` books a leg closed at the mark when the venue confirms it holds no position after a `nothing to reduce` rejection (`LEG_FLAT_AT_VENUE`) and stops after `MAX_CLOSE_RETRIES` (`DEGRADED_STUCK`, Telegram) instead of spinning. Re-review (approved) + follow-ups: the sim's maker cap uses `lots_floor`, a flip test pins the average re-anchor; Task 16's `_on_maker_event` no longer swallows a POST-rest rejection (`LegTrack.acked`; a resting reduce-only exit maker refused with `nothing to reduce` books the leg flat after the venue confirms it and closes the other leg TT, exit reason `venue_flat`); 2 executor tests added. **Task 15 amended after code review (2026-09-05):** histograms report lifetime `total`/`max_ever` beside the windowed `n`/`max` and clamp negative/NaN samples, `LOOP_LAG` is one summary line per `summary_s` (not one per second), uptime is monotonic, `CoverageWatchdog` warns under max(`coverage_floor`, `coverage_frac` × the venue's high-water mark) after a start-up grace, has no `run()` (the App schedules `check()`), and its `to_dict()` (fresh/high/floor) is what the App persists as `coverage`; Config gained `coverage_floor`/`coverage_frac`; 1 metrics test added. **Task 16 amended after code review (2026-09-05):** every close path decides on the REMAINING quantity (a partial reduce-only fill leaves the position DEGRADED with the remainder owned; `_flatten_leg` continues its ladder with what is left); a venue exception on `place_market` leaves the order IN DOUBT — polled, then reported rejected as `in_doubt` (never assumed filled, `ORDER_UNRESOLVED` to Telegram) — and `_tt_legs`/`_close_remainder_tt` gather with `return_exceptions`; `place_post_only`, `amend`, `cancel`, `query_order` exceptions are caught (`TM_POST_IN_DOUBT` queries the venue); a fill on a superseded maker order or on a position outside the maker flow is a `TM_STRAY_FILL`: booked on the leg (`LegTrack.filled_seen` delta) and the position is handed to `retry_degraded` via `_degrade` (cancels the live order, walks MAKER_RESTING→HEDGING→DEGRADED), never written onto the live order's counters; `_finalize_maker` is total over HEDGING (a flattened fill closes as `hedge_unwound`) and DEGRADED (books the fill and resizes); an exit-phase hedge failure no longer sends a re-opening flatten — the remainder closes taker/taker; maker-fill flattens realize their P&L into `Position.pnl_adjust_usd` (new, Task 2; `finalize_pnl` adds it, Task 7) and continue on partial fills; `_apply_maker_leg` books incrementally (`maker_booked_qty/fee`); a cancel that fails inside `_hedge_delta` is retried and `_sweep_stuck_hedging` (run from `retry_degraded`) plus `cancel_all_resting` cover HEDGING positions with a live order; `_close` is idempotent and prunes tracks/locks; `exit_tt` only acts on OPEN/EXIT_MAKER_RESTING; hedges size off fresh quotes; the TT upgrade re-sizes at the current touch; `submit_to_fill` only records fills; `busy` removed; CID truncation logged; 9 tests added (23 total). Second round: maker `LegTrack`s carry `phase` and venue as posted (a late fill on a cancelled exit maker whose `maker_venue` was cleared books on the right leg in the right phase), stray fills are booked additively without touching `maker_booked_qty` (the live order is booked later by finalize/degrade), `_tt_legs` accumulates its fills and defers to a concurrent degrade (a stray during the TT upgrade is added, not overwritten; a both-legs-failed entry with something on the books degrades instead of discarding), `hedge_unwound` requires flat legs (`UNWOUND_BUT_NOT_FLAT` → DEGRADED), a flatten that nets already-booked quantity books it as the leg's exit fill, an in-doubt order with a known partial books the partial, maker tracks outlive their position (bounded by `MAX_TRACKS`) so `STRAY_FILL_AFTER_CLOSE`/`STRAY_FILL_NO_POSITION`/`UNKNOWN_FILL` reach Telegram, DEGRADED transitions mark the book dirty, `EXIT_IGNORED` is logged; 5 tests added (28 total). Plan 2 acceptance criteria for the live `Trading` adapters: raise (never `ok=False`) on transport/envelope failures, and support `query_order` by client id — the in-doubt path depends on both. Third round: the LEG_DESYNC branch closes `failed_entry` only when BOTH legs net flat (`DESYNC_NOT_FLAT` → DEGRADED otherwise) and the both-legs-failed-with-a-stray branch resizes; 1 test added (29 total). **Task 17 reviewed (2026-09-05, approved):** the test also pins that an empty whitelist means unrestricted and that a symbol delisted on one venue drops out. Notes for later plans: Plan 3's quote-only wiring must pass `symbols_for_quote_venue` the venue's SPEC MAP (a venue name silently yields `[]`); when Plan 4 flips OKX to `trade`, `apply_universe`/config should warn on an empty `symbol_whitelist` for a venue known to need one (OKX-EEA's 10 pairs), otherwise every OKX leg is rejected one-legged. **Task 18 amended after code review (2026-09-05):** the poller can no longer wedge — `parse_commands` never raises (a whitespace-only message used to raise `IndexError` before the offset advanced, so Telegram redelivered it forever and every operator command went dark for 24 h), every update with an id advances the offset, commands match case-insensitively and with a `@botname` suffix, the chat id is normalized (whitespace), updates dated before process start are consumed but not executed (no replay of a stale `/close_all` against reloaded positions), the first failure per process (HTTP status, `ok:false` with Telegram's description, or an exception TYPE — never the repr, the URL carries the token) is a WARNING and later ones DEBUG, a half configuration (one of token/chat id) warns at start-up (`main.py` builds the client when either is set), long messages are truncated with an ellipsis; fake-session tests cover a 401 body, a raising session, the offset across polls and the pre-start filter; 2 tests added (4 total). Re-review (approved): a non-dict `chat` is skipped like a non-dict `message`, and the deliberate sub-second bias of the pre-start filter is documented. **Task 19 amended after code review (2026-09-05, review included a 4-minute live paper smoke run: universe 364, coverage mexc 287 / blofin 364, a full TT round trip, clean shutdown):** `shutdown()` DRAINS in-flight order tasks (`_drain`, `SHUTDOWN_DRAIN_S`) before cancelling resting orders (bounded by `SHUTDOWN_CANCEL_S`) and saving, and logs `SHUTDOWN complete`; `_adopt_transients` runs at load — TT_ENTERING/HEDGING/EXIT_HEDGING/TT_EXITING → DEGRADED (`STUCK_TRANSIENT`), a restored MAKER_RESTING is discarded (`STUCK_RESTING`), a restored EXIT_MAKER_RESTING reopens as OPEN (`STUCK_EXIT_MAKER`); `/close_all` drains first and also exits EXIT_MAKER_RESTING positions; the strategy posts no new exit maker while halted and cancels resting ones (`halted`); every long-lived task is supervised (`TASK_DIED` → Telegram → `running=False` so systemd restarts); the heartbeat is `bbo_heartbeat_{mode}` and `main.py` refuses a DATA_DIR equal to the legacy bot's data dir (exit 5), wraps `build_venues` (exit 6; the registry now collects every live-mode problem into one message and requires key AND secret), treats an unreadable legacy heartbeat as running, and force-exits on a second signal; market data retries every 60 s while the universe is empty (`UNIVERSE_EMPTY`); `_quote_fallback` runs every 0.5 s and refreshes a leg at HALF the staleness budget (before, it always lost the race to `evaluate_resting`'s stale-cancel: 12 of 14 TM entries in the smoke run died `stale`), a stale-cancel puts the symbol on a 5 s cooldown (`STALE_CANCEL_COOLDOWN_S`), `on_quote` is guarded (`QUOTE_ERROR`), `QUOTE_TS_SKEW` rejects quotes from the future, equity/starting capital count tradeable venues, open-position symbols stay subscribed and keep their specs across a refresh, the banner and `STATE loaded` report `halted`/open, `bbo_section` reports per-venue `connected`, balance refresh is guarded; MEXC gets `staleness_override_s: 5.0` in `config/venues.json` (its depth feed pushes only on change; ~80 of 364 symbols read stale at 2 s); 6 App tests + `tests/test_registry.py` (2) + 1 strategy test added. Deferred: Telegram session reuse; Plan 2 must cancel/adopt resting orders at start-up (reconciliation). Re-review round (approved; all seven findings resolved, follow-ups folded in): a restored MAKER_RESTING / EXIT_MAKER_RESTING whose resting order had filled — and a HEDGING/EXIT_HEDGING saved before `_finalize_maker` booked the fill — go through `Executor.adopt_restored`, which books the maker fill on its leg and walks the position to DEGRADED (before, the fill was discarded or the position closed as `recovered` with the fill orphaned at the venue); only a fill-less MAKER_RESTING is discarded and only a fill-less EXIT_MAKER_RESTING reopens; `_quote_fallback` bounds each REST call to the same half-budget it fires at (`asyncio.wait_for`; the adapters' 5 s timeout let one hung request sit through the whole budget — 10 of 14 BloFin fallback calls timed out in the instrumented run) with one fetch per leg in flight and `fallback_ok`/`fallback_timeout`/`fallback_failed` funnel counters; `shutdown()` waits for cancelled makers to settle (`_settle_makers`, `SHUTDOWN_SETTLE_S`) before saving, so a clean restart no longer reports `STUCK_EXIT_MAKER` for an order the bot itself cancelled; `_drive` refuses REQUOTE/TM_EXIT/UPGRADE_TT once stopping (`STOPPING`); Telegram sends live in their own task sets (`Executor._notify_tasks`, `App._notify`) so a hung send cannot eat the drain budget; `_adopt_transients` documents that nothing asks the venue (Plan 2 reconciliation); tests added for all of these plus the stale-cancel cooldown, the 60 s empty-universe retry, held-symbol spec preservation and the unreadable legacy heartbeat (174 total). MEXC's 5 s staleness override was measured, not assumed: median quote age 0.5–1.5 s, p90 2.5–3.5 s, inter-update p99 5.24 s over 30k updates; BloFin median 0.02–0.27 s, none needed. Open question for the paper soak: why in-app BloFin REST fallback calls exceed 5 s when the endpoint answers in ~300 ms in isolation (shared aiohttp session; `fresh` vs `shared` sessions measured equal). Follow-up round (both reviews approved, findings folded in): the open question was answered by a probe — with 8 BloFin books5 shards streaming from the same IP, a REST call on a NEW connection has p90 1.4 s / max 4.8 s (SYN-retransmit-shaped tail; bursts of new connections are what suffer) while a call on an already-open keep-alive connection stays ~300 ms even at 10–20 s idle gaps — so `_quote_fallback` now spawns per-leg fetch tasks (the tick never blocks; `_fallback_inflight` dedupe is load-bearing), serializes them per venue on `_rest_locks` so every call reuses the warm pooled connection, touches each venue with open positions every `REST_WARM_S` (10 s) to keep that connection alive, and bounds a call at the venue's FULL staleness budget (a late answer is still a fresh quote — `ts_local` is stamped at receipt — so the half-budget bound of the previous round was too tight: 6 of 22 fallbacks abandoned in the reviewer's run); `Executor.stopping` (set first thing in `shutdown()`) makes `_post_maker` refuse and the TT-upgrade branch discard, so a requote or upgrade whose cancel event lands during the drain can no longer leave a fresh order resting or open a new position (`STOPPING`); `shutdown()` flushes pending Telegram alerts for `NOTIFY_FLUSH_S` after the save (they were dropped when the session closed); `_drain`/`_settle_makers` deadlines run on the loop clock; `_adopt_transients` runs from `run()` AFTER the first market-data refresh so an adopted position's size is re-derived from its legs with known specs (`load_state` no longer adopts); `exit_tm` documents that `_post_maker` resets the maker counters; the README lists every module and names the legacy dashboard's location. Tests: +4 (warm connection + one-call-at-a-time, no re-post on shutdown, TT upgrade dropped on shutdown, alerts flushed) → 178 total. **Task 20 acceptance run (2026-09-05, paper, 9 min, live public feeds, commit 84ea4ba40):** universe 364; coverage mexc 358→362, blofin 267→349 (BloFin ramps over several minutes; no FEED_COVERAGE_LOW); 22 INTENT / 22 TM_POST / 211 TM_REQUOTE / 2 TM_FILL / 14 TM_CANCEL (13 ttl, 1 stale — down from 8 of 12 in the pre-fix run); 7 round trips (6 TT convergence, 1 TM take_profit), net −$0.001; fallback_ok 108 / fallback_timeout 4 (3.6 %, down from 27 %); loop lag p50 1.1 ms / max 21 ms; no tracebacks, no ERROR lines, state saved every ≤ 5 s, `bbo_heartbeat_paper` written; SIGINT → both SHUTDOWN lines, 0 open, exit 0. Soak watch-list: maker TTL cancels dominate (13 of 14) — tune `maker_ttl_s`/peg aggressiveness from soak data, not from the historical live trades. **Final whole-implementation review (2026-09-05; merge gated on two fixes, both landed with the review's Important items):** `ALLOWED[EXIT_MAKER_RESTING]` gains `CLOSED` (a restored exit maker on a venue removed from the registry raised `InvalidTransition` from `_check_position_venues` — a crash loop under `Restart=always` in paper; live raised `VenueMissing` first); `_sweep_stuck_hedging` rescues a HEDGING/EXIT_HEDGING position whose cancel was sent but whose terminal event never arrived (private feed drop, venue status lag): it pulls `query_order` every `HEDGING_SWEEP_AFTER_S` and feeds a terminal state in, and after `HEDGING_STUCK_S` (60 s) of silence degrades the position (`HEDGING_STUCK`, Telegram) so retry_degraded owns it; spec items that had no code — `MAX_NAKED_MS` (`NAKED_EXPOSURE` alert once per position when a maker fill sits unhedged past `max_naked_ms`, funnel `naked_exposure`) and `FEE_MISMATCH` (`_check_fee` on every booked fill: fee > 20 % off the configured rate for its liquidity type → log + funnel `fee_mismatch` + one Telegram per venue per hour); the post-fill spread-quality guard (`MIN_FILL_SPREAD_PCT`) now covers TM entries too (the soak's dominant loss was a maker filled by a market moving through it, hedged after the spread had gone: #4 NOMUSDT −0.45 % fill, −$0.15) — the abort is spawned because `_finalize_maker` holds the position lock; a completed TM exit is labelled by what it realized (`take_profit` only within `TM_EXIT_SLIP_TOL_PCT` of `EXIT_SPREAD_PCT`, else `tm_exit_slipped`, funnel-counted); requotes no longer double-fire (the App stamps `maker_last_requote_ts` synchronously before spawning; budget-denied requotes count as `requote_budget`); `evaluate_resting` cancels a resting entry while halted (`halted`), mirroring `evaluate_exit`; `_book_leg_flat` accumulates the remainder at the mark instead of overwriting a partial exit fill; TM `signal_to_open` is recorded as `signal_to_open_tm` (it includes the resting time); `requirements.txt` is runtime-only (`requirements-dev.txt` adds pytest). Tests: +10 (188 total), each proven red against the previous commit. **Plan 2 carry-forwards from the final review:** the spec's ARM → prewarm `set_leverage` step has no caller (protocol + SimVenue no-op only) — implement it with the live adapters and persist the set of symbols already set; seed `Executor._seq` from a persisted counter so client ids stay unique across restarts; reuse one aiohttp session for Telegram; pin the Python version Tokyo runs (the venv here is 3.14, the spec says 3.12); state saves can reach 2/s (sweep + dirty flag) against the spec's 1/s — harmless, documented; funnel counters mix per-pair and per-direction semantics (`insane`/`mismatch` vs `below_edge`) and `no_candidate` is never counted; `finalize_pnl` prices both legs at the smaller notional (conservative by up to `max_leg_mismatch_pct`); watch BloFin shards for `no data for 127s` reconnects during the soak. **Task 9 re-review round (approved):** nothing is emitted for a resting maker while a cancel or requote is in flight (`in_flight`, covers REQUOTE and UPGRADE_TT), `_finalize_maker` clears a stale `upgrade_pending` when it re-posts a requote, a TM position's stop reference is `min(s_now, entry_spread_pct)` (a late first evaluation cannot anchor to a diverged spread), `stop_ref_spread_pct` is `None` until set, routes rank by (TT-first, edge), `volume_unknown` counts once per evaluation, the OPEN take-profit branch is guarded on `status == OPEN`, and the scanner test has a wide-book symbol whose edge order differs from its spread order. Carry-forward notes: Task 19 must normalize `coverage` over the configured venues and reject quotes with `ts_local > now + 1 s` (`QUOTE_TS_SKEW`); Task 7 should cache closed positions' dicts so state saves do not re-serialize 500 closed positions. The code blocks above are the amended versions.

- **Spec coverage:** decisions 1–10 → Tasks 1 (registry, roles), 3/12/13 (BBO-only feeds), 9/19 (event-driven, 500 ms sweep), 16 (event-driven fills, REST fallback, TT priority, PeggedMaker for entry and exit, rate budgets with reserve, flatten ladder, degraded retry), 8/19 (manual halt via flags and Telegram, no kill switch), 14/19 (paper mode over real feeds), 7/19 (dashboard-schema state file, `DATA_DIR`), 19/20 (legacy heartbeat guard, systemd unit). Mismatch guard, funding gate, touch-depth guard, volume gate, win-rate gate, cooldowns and strikes → Tasks 8–9. Latency metrics and coverage watchdog → Task 15/19. Not in this plan by design: live adapters (Plan 2), quote-only venues and further trade venues (Plan 3), the 48 h paper soak (operational, after Task 20).
- **Placeholder scan:** none.
- **Type consistency:** every module in Tasks 3–19 was executed together against the tests shown (71 passed) before being pasted here; names match across tasks (`Intent.kind` values, `Position` fields, `Venue` bundle, `Trading` protocol).

## What comes next

- **Plan 2 — live MEXC + BloFin:** `MexcPrivate`/`MexcTrading`, `BlofinPrivate`/`BlofinTrading` (signing, post-only `type=2` / `post_only`, `externalOid`/`clientOrderId`, private WS login and order/deal channels → `OrderEvent`, amend for BloFin, hedge-mode side codes and `positionId` capture for MEXC), registration in `LIVE_ADAPTERS`, reconciliation on startup and every 60 s, venue conformance suite with live-captured fixtures, Tokyo deployment.
- **Plan 3 — more venues:** port SpreadWatch's public parsers as quote-only feeds behind `WSRunner`, scanner rows for quote-only routes, then one trade adapter per venue the user has keys for (OKX first, with its 10-pair whitelist).
