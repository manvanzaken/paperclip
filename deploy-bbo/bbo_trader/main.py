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
