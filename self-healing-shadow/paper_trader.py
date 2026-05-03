"""Self-healing shadow paper trader — entry point.

Wires the data feeds, spread engine, signal engine, execution sim, and
the five heal modules into one async event loop. No real orders ever
leave this process.

Run with::

    python paper_trader.py --config config.yaml [--minutes N]

Use ``--minutes`` to time-box the run for smoke tests; omit for an
indefinite run (Ctrl-C to stop).
"""

from __future__ import annotations

import argparse
import asyncio
import itertools
import logging
import signal
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import yaml

from core.data_feed import WS_CLIENTS
from core.execution_sim import (
    ExecutionSim,
    InsufficientLiquidity,
    SimOrder,
    SimulatedFailure,
)
from core.orderbook import OrderBook
from core.signal_engine import EntrySignal, ExitSignal, SignalEngine  # noqa: F401
from core.spread_engine import SpreadEngine, log_spread, mid_price
from core.state_machine import Position, PositionState
from core.trade_journal import TradeJournal
from heal.atomic_writes import write_state
from heal.health_check import HealthStatus, TradingHealthCheck
from heal.heartbeat import HeartbeatMonitor
from heal.hurst_canary import HurstCanary
from heal.reconciliation import ReconciliationSaga


log = logging.getLogger("paper_trader")


# ---------------------------------------------------------------------
# Configuration loader.
# ---------------------------------------------------------------------


def load_config(path: str | Path) -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


# ---------------------------------------------------------------------
# The bot.
# ---------------------------------------------------------------------


class PaperTrader:
    def __init__(self, cfg: dict) -> None:
        self.cfg = cfg
        self.symbol = cfg["symbols"][0]
        self.exchanges = list(cfg["exchanges"].keys())

        # Wire shared state.
        self.books: dict[str, OrderBook] = {}
        self.heartbeat = HeartbeatMonitor(
            stale_threshold_sec=cfg["heal"]["ws_stale_threshold_sec"],
        )
        self.canary = HurstCanary(
            window=cfg["heal"]["hurst_window"],
            threshold=cfg["heal"]["hurst_threshold"],
        )
        self.journal = TradeJournal(cfg["paths"]["journal_db"])
        self.spread_engine = SpreadEngine(
            lookback_window=cfg["strategy"]["lookback_window"],
        )
        self.execution = ExecutionSim(
            starting_balances={
                ex: float(c["starting_balance_usd"])
                for ex, c in cfg["exchanges"].items()
            },
            taker_fees_bps={
                ex: float(c["taker_fee_bps"])
                for ex, c in cfg["exchanges"].items()
            },
            sim_leg_failure_rate=cfg["heal"]["sim_leg_failure_rate"],
        )
        self.saga = ReconciliationSaga(
            sim=self.execution,
            journal=self.journal,
            quarantine_after=cfg["heal"]["saga_quarantine_after_failures"],
        )
        self.signal_engine = SignalEngine(
            heartbeat=self.heartbeat,
            canary=self.canary,
            journal=self.journal,
            spread_engine=self.spread_engine,
            entry_z=cfg["strategy"]["entry_z"],
            exit_z=cfg["strategy"]["exit_z"],
            stop_loss_z=cfg["strategy"]["stop_loss_z"],
            min_net_profit_usd=cfg["strategy"]["min_net_profit_usd"],
            max_position_usd=cfg["strategy"]["max_position_usd"],
            recovery_fraction=cfg["strategy"].get("recovery_fraction", 0.5),
            taker_fees_bps={
                ex: float(c["taker_fee_bps"])
                for ex, c in cfg["exchanges"].items()
            },
            is_quarantined=self.saga.exchange_quarantined,
        )
        self.health = TradingHealthCheck(
            journal=self.journal,
            get_balances=lambda: dict(self.execution.simulated_balance),
            min_order_usd=cfg["strategy"]["max_position_usd"],
            get_open_positions=lambda: list(self.open_positions.values()),
            interval_sec=cfg["heal"]["health_check_interval_sec"],
        )

        # Eval queue: (exchange_with_new_book, symbol).
        self._eval_queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue()
        self.open_positions: dict[str, Position] = {}
        self._safe_mode = False
        self._stop = asyncio.Event()
        self._max_concurrent = cfg["strategy"]["max_concurrent_positions"]
        self._max_hold = timedelta(minutes=cfg["strategy"].get("max_hold_min", 30))

    # --- runtime --------------------------------------------------------

    async def run(self, *, minutes: Optional[int] = None) -> None:
        log.info(
            "starting paper trader: %d exchanges, symbol=%s",
            len(self.exchanges), self.symbol,
        )
        tasks = [
            asyncio.create_task(self._run_ws(ex), name=f"ws-{ex}")
            for ex in self.exchanges
        ]
        tasks += [
            asyncio.create_task(self._spread_loop(),         name="spread"),
            asyncio.create_task(self._heartbeat_loop(),      name="heartbeat"),
            asyncio.create_task(self._health_loop(),         name="health"),
            asyncio.create_task(self._position_monitor_loop(), name="position-monitor"),
            asyncio.create_task(self._journal_flusher(),     name="journal-flush"),
            asyncio.create_task(self._checkpoint_loop(),     name="checkpoint"),
        ]
        if minutes is not None:
            tasks.append(asyncio.create_task(self._timer(minutes), name="timer"))
        try:
            await self._stop.wait()
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.journal.flush()
            self.journal.close()

    async def _timer(self, minutes: int) -> None:
        await asyncio.sleep(minutes * 60)
        log.info("timer expired after %d min — stopping", minutes)
        self._stop.set()

    def request_stop(self) -> None:
        self._stop.set()

    # --- per-exchange WS feeder ----------------------------------------

    async def _run_ws(self, exchange: str) -> None:
        client_cls = WS_CLIENTS[exchange]
        client = client_cls(
            symbol=self.symbol,
            on_book=self._on_book,
            on_message=self.heartbeat.on_ws_message,
        )
        await client.run()

    async def _on_book(self, exchange: str, symbol: str, book: OrderBook) -> None:
        self.books[exchange] = book
        # Mirror into the execution simulator so create_order can walk it.
        self.execution.set_book(exchange, symbol, book)
        await self._eval_queue.put((exchange, symbol))

    # --- spread evaluator ----------------------------------------------

    async def _spread_loop(self) -> None:
        while True:
            exchange, symbol = await self._eval_queue.get()
            try:
                await self._evaluate_pairs(exchange, symbol)
            except Exception as e:
                log.exception("spread loop error: %s", e)
                self.journal.log(event_type="ERROR", payload={"where": "spread_loop", "err": str(e)})

    async def _evaluate_pairs(self, exchange: str, symbol: str) -> None:
        book_changed = self.books.get(exchange)
        if book_changed is None or book_changed.best_bid is None or book_changed.best_ask is None:
            return
        mid_changed = mid_price(bid=book_changed.best_bid, ask=book_changed.best_ask)
        if mid_changed is None:
            return  # crossed book; skip this tick

        for other in self.exchanges:
            if other == exchange:
                continue
            book_other = self.books.get(other)
            if book_other is None or book_other.best_bid is None or book_other.best_ask is None:
                continue
            mid_other = mid_price(bid=book_other.best_bid, ask=book_other.best_ask)
            if mid_other is None:
                continue  # crossed book on the other side
            spread = log_spread(price_a=mid_changed, price_b=mid_other)
            pair = (exchange, other)
            self.spread_engine.update(pair=pair, spread=spread)
            self.canary.update(pair, spread)

            z = self.spread_engine.zscore(pair)
            if z is None:
                continue

            # Check exit on any open position for this pair.
            await self._maybe_exit_position(pair, z)

            # Try to open a new one if we have headroom and are not in safe mode.
            if self._safe_mode:
                continue
            if len(self.open_positions) >= self._max_concurrent:
                continue
            if self._has_open_for_pair(pair):
                continue

            sig = self.signal_engine.evaluate(
                pair=pair,
                symbol=symbol,
                z_score=z,
                current_spread_log=spread,
                book_a=book_changed,
                book_b=book_other,
            )
            if isinstance(sig, EntrySignal):
                await self._open_position(sig)

    def _has_open_for_pair(self, pair: tuple[str, str]) -> bool:
        # Canonical pair comparison: (a, b) and (b, a) are the same trade.
        canonical = tuple(sorted(pair))
        for p in self.open_positions.values():
            if tuple(sorted(p.pair)) == canonical:
                return True
        return False

    # --- position lifecycle --------------------------------------------

    async def _open_position(self, sig: EntrySignal) -> None:
        pid = f"t-{uuid.uuid4().hex[:8]}"
        a, b = sig.pair
        position = Position(id=pid, pair=sig.pair, symbol=sig.symbol, entry_z=sig.z_score)
        self.open_positions[pid] = position

        position.transition(PositionState.SIGNAL_DETECTED, reason=f"z={sig.z_score:.2f}")
        position.transition(PositionState.PROFITABILITY_CHECK, reason="net>=min")
        position.transition(PositionState.EXECUTING, reason="placing legs")

        cid_a = f"{pid}-A-{uuid.uuid4().hex[:6]}"
        cid_b = f"{pid}-B-{uuid.uuid4().hex[:6]}"
        position.leg_a_order_id = cid_a
        position.leg_b_order_id = cid_b

        results = await asyncio.gather(
            self.execution.create_order(
                exchange=a, symbol=sig.symbol, side=sig.side_a,
                size_usd=sig.size_usd, client_order_id=cid_a,
            ),
            self.execution.create_order(
                exchange=b, symbol=sig.symbol, side=sig.side_b,
                size_usd=sig.size_usd, client_order_id=cid_b,
            ),
            return_exceptions=True,
        )
        leg_a, leg_b = results
        position.transition(PositionState.RECONCILING, reason="checking legs")

        result = await self.saga.run(
            position_id=pid,
            exchange_a=a, exchange_b=b,
            leg_a=leg_a, leg_b=leg_b,
            client_order_id_a=cid_a, client_order_id_b=cid_b,
        )

        if result.both_filled:
            position.transition(PositionState.POSITION_OPEN, reason="both filled")
            position.opened_at = datetime.now(timezone.utc)
            position.transition(PositionState.MONITORING, reason="awaiting convergence")
            self.journal.log(
                event_type="FILL",
                payload={
                    "position_id": pid,
                    "size_usd": sig.size_usd,
                    "side_a": sig.side_a,
                    "side_b": sig.side_b,
                },
                pair=f"{a}_{b}",
                symbol=sig.symbol,
                z_score=sig.z_score,
            )
            self.saga.note_success(a)
            self.saga.note_success(b)
        else:
            position.transition(PositionState.ROLLBACK, reason=str(result.diagnosis))
            position.transition(PositionState.SCANNING, reason="rolled back")
            self.open_positions.pop(pid, None)
            self._safe_mode = True
            self.journal.log(
                event_type="ERROR",
                payload={
                    "position_id": pid,
                    "diagnosis": result.diagnosis.value if result.diagnosis else "UNKNOWN",
                    "rolled_back_leg": result.rolled_back_leg,
                },
            )
            log.warning("entered SAFE_MODE due to saga failure on position %s", pid)

    async def _maybe_exit_position(self, pair: tuple[str, str], z_score: float) -> None:
        canonical = tuple(sorted(pair))
        for pid, p in list(self.open_positions.items()):
            if tuple(sorted(p.pair)) != canonical:
                continue
            if p.state is not PositionState.MONITORING:
                continue
            sig = self.signal_engine.evaluate_exit(z_score=z_score, stop_loss_hit=False)
            if sig is None:
                continue
            await self._close_position(pid, p, sig, z_score)

    async def _position_monitor_loop(self) -> None:
        """Once per second, re-check exits and max-hold for every open position.

        This guarantees positions are evaluated even when the WS feed for
        their pair goes quiet (otherwise an exit can be missed for a long
        time during low-volume periods).
        """
        while True:
            try:
                await self._tick_open_positions()
            except Exception as e:
                log.exception("position monitor: %s", e)
                self.journal.log(event_type="ERROR", payload={"where": "position_monitor", "err": str(e)})
            await asyncio.sleep(1.0)

    async def _tick_open_positions(self) -> None:
        now = datetime.now(timezone.utc)
        for pid, p in list(self.open_positions.items()):
            if p.state is not PositionState.MONITORING:
                continue
            # Max-hold timeout: close regardless of Z.
            if p.opened_at is not None and (now - p.opened_at) > self._max_hold:
                z = self.spread_engine.zscore(p.pair) or 0.0
                await self._close_position(pid, p, ExitSignal(reason="max_hold"), z)
                continue
            # Z-based exit re-check using the latest rolling Z.
            z = self.spread_engine.zscore(p.pair)
            if z is None:
                continue
            sig = self.signal_engine.evaluate_exit(z_score=z, stop_loss_hit=False)
            if sig is not None:
                await self._close_position(pid, p, sig, z)

    async def _close_position(
        self, pid: str, p: Position, sig: ExitSignal, z_score: float,
    ) -> None:
        if sig.reason == "convergence":
            p.transition(PositionState.CLOSING, reason=f"z={z_score:.2f} converged")
        else:
            p.transition(PositionState.EMERGENCY_EXIT, reason=f"z={z_score:.2f} stop")

        # Close legs at current book.
        a, b = p.pair
        original_a = await self.execution.fetch_order(p.leg_a_order_id) if p.leg_a_order_id else None
        original_b = await self.execution.fetch_order(p.leg_b_order_id) if p.leg_b_order_id else None
        size = original_a.size_usd if original_a else self.cfg["strategy"]["max_position_usd"]
        opp_a = "sell" if (original_a and original_a.side == "buy") else "buy"
        opp_b = "sell" if (original_b and original_b.side == "buy") else "buy"

        cid_close_a = f"{pid}-CA-{uuid.uuid4().hex[:6]}"
        cid_close_b = f"{pid}-CB-{uuid.uuid4().hex[:6]}"
        try:
            await asyncio.gather(
                self.execution.create_order(
                    exchange=a, symbol=p.symbol, side=opp_a,
                    size_usd=size, client_order_id=cid_close_a,
                ),
                self.execution.create_order(
                    exchange=b, symbol=p.symbol, side=opp_b,
                    size_usd=size, client_order_id=cid_close_b,
                ),
                return_exceptions=False,
            )
        except (SimulatedFailure, InsufficientLiquidity) as e:
            self.journal.log(event_type="ERROR", payload={"where": "close", "err": str(e), "pid": pid})

        p.transition(PositionState.SCANNING, reason="closed")
        self.open_positions.pop(pid, None)
        self.journal.log(
            event_type="EXIT",
            payload={"position_id": pid, "reason": sig.reason},
            pair=f"{a}_{b}",
            symbol=p.symbol,
            z_score=z_score,
        )

    # --- supporting tasks ----------------------------------------------

    async def _heartbeat_loop(self) -> None:
        await self.heartbeat.monitor_loop()

    async def _health_loop(self) -> None:
        await self.health.monitor_loop(self._on_health_status)

    def _on_health_status(self, status: HealthStatus) -> None:
        self.journal.log(
            event_type="HEALTH_CHECK",
            payload={"is_healthy": status.is_healthy, "issues": status.issues, "action": status.recommended_action},
        )
        if status.recommended_action == "ENTER_SAFE_MODE":
            self._safe_mode = True
            log.warning("HealthCheck -> SAFE_MODE")
        elif status.recommended_action == "FORCE_EXIT_OLDEST":
            log.warning("HealthCheck -> FORCE_EXIT_OLDEST (advisory in MVP)")
        elif status.recommended_action == "CONTINUE" and self._safe_mode:
            # Auto-clear: a fully healthy check after a saga-triggered
            # SAFE_MODE means the underlying issue resolved (e.g. exchange
            # came back, rate limit cleared). Without this the bot would
            # be permanently locked out of new entries after one failure.
            self._safe_mode = False
            log.info("HealthCheck CONTINUE -> SAFE_MODE cleared")

    async def _journal_flusher(self) -> None:
        while True:
            await asyncio.sleep(1.0)
            try:
                self.journal.flush()
            except Exception as e:
                log.error("journal flush failed: %s", e)

    async def _checkpoint_loop(self) -> None:
        path = Path(self.cfg["paths"]["state_file"])
        while True:
            await asyncio.sleep(30.0)
            try:
                payload = {
                    "balances": self.execution.simulated_balance,
                    "open_positions": [
                        {"id": p.id, "pair": list(p.pair), "state": p.state.name}
                        for p in self.open_positions.values()
                    ],
                    "safe_mode": self._safe_mode,
                }
                write_state(path, payload)
            except Exception as e:
                log.error("checkpoint failed: %s", e)


# ---------------------------------------------------------------------
# CLI.
# ---------------------------------------------------------------------


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--config", default="config.yaml")
    p.add_argument("--minutes", type=int, default=None,
                   help="time-box the run (omit for indefinite)")
    args = p.parse_args()

    _setup_logging()
    cfg = load_config(args.config)
    bot = PaperTrader(cfg)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, bot.request_stop)
    try:
        loop.run_until_complete(bot.run(minutes=args.minutes))
    finally:
        loop.close()


if __name__ == "__main__":
    main()
