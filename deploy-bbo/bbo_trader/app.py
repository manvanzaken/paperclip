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
CLOSE_ALL_DRAIN_S = 10.0       # /close_all lets in-flight entries land first, or they open behind our back
QUOTE_REFRESH_FRAC = 0.5       # REST-refresh an open position's leg at half the staleness budget, and give the call that long
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
        self._fallback_inflight: set[tuple[str, str]] = set()
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

    def _notify(self, text: str) -> None:
        """Telegram sends live in their own set: the shutdown drain waits for order legs, never for a hung send."""
        if self.telegram is None:
            return

        async def guard():
            try:
                await self.telegram.send(text)
            except Exception:  # noqa: BLE001
                log.exception("NOTIFY_ERROR")
        t = asyncio.get_running_loop().create_task(guard())
        self._notify_tasks.add(t)
        t.add_done_callback(self._notify_tasks.discard)

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
        """Wait for in-flight order tasks (entries, exits, hedges) so the book matches the venues before we act."""
        deadline = self.clock() + timeout
        while True:
            pending = [t for t in (self._tasks | self.executor._tasks) if not t.done()]
            if not pending:
                return
            left = deadline - self.clock()
            if left <= 0:
                log.error("DRAIN_TIMEOUT %d order tasks unfinished after %.0fs — state may lag the venues", len(pending), timeout)
                return
            await asyncio.wait(pending, timeout=min(left, 1.0))

    async def _settle_makers(self, timeout: float) -> None:
        """After cancel_all_resting: the acks arrive as order events and finalize in their own tasks. Wait until no
        position is in a maker-flow status (and nothing is in flight) so the saved state says OPEN/CLOSED, not a
        MAKER_RESTING a restart would adopt as stuck and an operator would be told to check by hand."""
        deadline = self.clock() + timeout
        while True:
            pending = [t for t in (self._tasks | self.executor._tasks) if not t.done()]
            unsettled = self.book.by_status(*_MAKER_FLOW)
            if not pending and not unsettled:
                return
            if self.clock() >= deadline:
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
        self._adopt_transients()
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
        start-up before trusting any of it."""
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
        """Open positions must never depend on WS health: REST-refresh a leg at HALF the staleness budget (a resting
        maker is cancelled the moment a leg reads stale, so refreshing only after the boundary always loses that
        race) and give the call that same half — an answer after the boundary is useless, and the adapters' 5 s
        timeout let one hung request sit through the whole budget. One fetch per leg in flight; the next 0.5 s tick
        retries after a timeout on a fresh connection."""
        now = self.clock()

        async def one(v: Venue, symbol: str, budget: float) -> None:
            key = (v.name, symbol)
            self._fallback_inflight.add(key)
            try:
                bbo = await asyncio.wait_for(v.market.fetch_bbo(symbol), budget)
            except asyncio.TimeoutError:
                n = self.metrics.funnel["fallback_timeout"] = self.metrics.funnel["fallback_timeout"] + 1
                if n in (1, 10, 100) or n % 1000 == 0:
                    log.warning("QUOTE_FALLBACK_TIMEOUT #%d %s %s: no answer within %.1fs (leg reads stale at %.1fs)",
                                n, v.name, symbol, budget, self.board.stale_for(v.name))
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
        jobs = []
        for pos in list(self.book.open):
            for venue in (pos.venue_a, pos.venue_b):
                v = self.venues.get(venue)
                if v is None or v.market is None or (venue, pos.symbol) in self._fallback_inflight:
                    continue
                q = self.board.get(venue, pos.symbol)
                age = (now - q.ts_local) if q is not None else float("inf")
                budget = self.board.stale_for(venue) * QUOTE_REFRESH_FRAC
                if age < budget:
                    continue
                jobs.append(one(v, pos.symbol, budget))
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
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def shutdown(self) -> None:
        """Drain in-flight order tasks (an entry must land before we cancel and save, or the state file says we hold
        nothing while both legs sit at the venues), cancel every resting order with a timeout, wait for the cancels
        to settle, save, and say so."""
        log.info("SHUTDOWN draining in-flight order tasks, cancelling resting orders, saving state")
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
