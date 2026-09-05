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
from .venues.base import Venue, VenueError
from .venues.sim import SimVenue

log = logging.getLogger("bbo.app")


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
        self._last_state_save = 0.0
        self._scanner: list[dict] = []
        self._last_scan = 0.0
        self.watchdog = CoverageWatchdog(board, list(venues), floor=cfg.coverage_floor, frac=cfg.coverage_frac, clock=clock)
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
        """Hourly: specs (never replaced by an empty set — that would collapse the universe and unsubscribe
        every feed) and 24 h volumes. Funding has its own 5-minute loop (4 h settlement grids)."""
        async def one(v: Venue):
            specs = await v.market.fetch_specs()
            if not specs:
                raise VenueError(f"{v.name}: empty spec set — keeping the {len(v.specs)} known contracts")
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
        results = await asyncio.gather(*(one(v) for v in self.venues.values() if v.market), return_exceptions=True)
        for v, r in zip([v for v in self.venues.values() if v.market], results):
            if isinstance(r, Exception):
                log.warning("SPECS_FAILED %s: %r", v.name, r)
        self.apply_universe()

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
                "coverage": self.watchdog.to_dict(),
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
        self._check_position_venues()
        log.info("STATE loaded: %d open, %d closed, pnl=$%.2f", len(self.book.open), len(self.book.closed), self.book.total_pnl_usd)

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
            await self.executor.cancel_all_resting()
            for pos in list(self.book.by_status(OPEN)):
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
                bal = await v.trading.balance()
                self.risk.set_balance(v.name, bal.get("available", 0.0), bal.get("total", 0.0))

    async def _quote_fallback(self) -> None:
        """Open positions must never depend on WS health: pull a REST BBO for stale legs (concurrently, each
        guarded — one hung endpoint must not delay the other legs)."""
        now = self.clock()

        async def one(v: Venue, symbol: str) -> None:
            try:
                bbo = await v.market.fetch_bbo(symbol)
            except Exception as e:  # noqa: BLE001
                log.warning("QUOTE_FALLBACK_FAILED %s %s: %r", v.name, symbol, e)
                return
            if bbo is not None:
                self.on_bbo(bbo)
        jobs = []
        for pos in list(self.book.open):
            for venue in (pos.venue_a, pos.venue_b):
                v = self.venues.get(venue)
                if v is None or v.market is None or self.board.fresh(venue, pos.symbol, now) is not None:
                    continue
                jobs.append(one(v, pos.symbol))
        if jobs:
            await asyncio.gather(*jobs)

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
        await self.refresh_funding()
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
                  asyncio.create_task(self._loop(300.0, self.refresh_funding)),
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