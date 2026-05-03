"""Daily adaptive parameter tuner.

Looks at the last 24 hours of journal events and applies bounded
nudges to a `params_overrides.yaml` that is merged on top of the
user's `config.yaml` at startup. After writing overrides the bot
restarts via launchd to pick up the new values.

Six rules (see design doc, Step 2 plan):

  1. ``profit_below_threshold`` aborts > 90 % of signals → lower
     ``min_net_profit_usd`` by 10 %.
  2. Zero fills in 24 h AND Hurst-rejection rate < 30 % → lower
     ``entry_z`` by 0.1.
  3. > 20 fills in 24 h → raise ``entry_z`` by 0.1.
  4. ``hurst_unsafe`` aborts > 30 % of signals → raise ``entry_z``
     by 0.1.
  5. An exchange suffered ≥ 3 saga failures in 24 h → halve that
     exchange's ``max_position_usd``.
  6. An exchange had ≥ 5 fills and zero saga failures in 24 h →
     restore that exchange's ``max_position_usd`` by 1.2× (capped).

All changes pass through ``GUARDRAILS`` so no parameter can drift
out of bounds. Every nudge writes a ``PARAM_NUDGE`` journal entry
with the rule that fired, the before value, and the after value.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any

import yaml

from core.trade_journal import TradeJournal
from heal.atomic_writes import write_state

log = logging.getLogger(__name__)


# (floor, ceiling) — every nudge is clamped to this range.
GUARDRAILS: dict[str, tuple[float, float]] = {
    "min_net_profit_usd":         (-5.0, 20.0),
    "entry_z":                    (1.5, 3.5),
    "exchange_max_position_usd":  (25.0, 5000.0),
}


@dataclass
class NudgeResult:
    changes: dict[str, float] = field(default_factory=dict)


class AdaptiveTuner:
    def __init__(
        self,
        *,
        journal: TradeJournal,
        base_config: dict,
        overrides_path: Path,
        window: timedelta = timedelta(hours=24),
        interval_sec: float = 86400.0,
    ) -> None:
        self._journal = journal
        self._base = base_config
        self.overrides_path = Path(overrides_path)
        self._window = window
        self._interval_sec = interval_sec

    # --- public API ----------------------------------------------------

    async def run_once(self) -> NudgeResult:
        """Evaluate all rules against the last `window` of journal data
        and write any resulting nudges to `overrides_path`."""
        cur = self._effective_config()
        stats = self._read_stats()

        result = NudgeResult()

        # Rule 1: aborts dominated by profit_below_threshold -> lower threshold.
        # Use a multiplicative step for positive values (faster convergence
        # at high thresholds) and an additive step at zero/negative (so we
        # can keep nudging past zero without ever losing forward progress).
        sig = stats["signals"]
        if sig > 0:
            ratio = stats["aborts_profit"] / sig
            if ratio > 0.90:
                old = cur["strategy"]["min_net_profit_usd"]
                step = old * 0.10 if old > 0.10 else 0.05
                new = self._clamp("min_net_profit_usd", old - step)
                if new != old:
                    self._record(result, "strategy.min_net_profit_usd", old, new, rule="rule_1_profit_aborts_dominate")

        # Rule 2: no fills + low hurst block -> lower entry_z.
        if stats["fills"] == 0:
            hurst_ratio = stats["aborts_hurst"] / sig if sig > 0 else 0.0
            if hurst_ratio < 0.30:
                old = cur["strategy"]["entry_z"]
                new = self._clamp("entry_z", old - 0.1)
                if new != old:
                    self._record(result, "strategy.entry_z", old, new, rule="rule_2_no_fills_lower_z")

        # Rule 3: too many fills -> raise entry_z (be more selective).
        if stats["fills"] > 20:
            old = cur["strategy"]["entry_z"]
            new = self._clamp("entry_z", old + 0.1)
            if new != old and "strategy.entry_z" not in result.changes:
                self._record(result, "strategy.entry_z", old, new, rule="rule_3_too_many_fills_raise_z")

        # Rule 4: hurst aborts dominate -> raise entry_z (wait for stronger signal).
        if sig > 0:
            hurst_ratio = stats["aborts_hurst"] / sig
            if hurst_ratio > 0.30:
                old = cur["strategy"]["entry_z"]
                new = self._clamp("entry_z", old + 0.1)
                if new != old and "strategy.entry_z" not in result.changes:
                    self._record(result, "strategy.entry_z", old, new, rule="rule_4_hurst_dominates_raise_z")

        # Rule 5 + 6: per-exchange position-size nudges.
        for ex, fails in stats["saga_failures_per_exchange"].items():
            if fails >= 3:
                old = cur["exchanges"].get(ex, {}).get("max_position_usd")
                if old is None:
                    continue
                new = self._clamp("exchange_max_position_usd", old * 0.5)
                if new != old:
                    self._record(
                        result, f"exchanges.{ex}.max_position_usd",
                        old, new, rule="rule_5_shrink_failing_exchange",
                    )
        for ex, fills in stats["fills_per_exchange"].items():
            if fills >= 5 and stats["saga_failures_per_exchange"].get(ex, 0) == 0:
                old = cur["exchanges"].get(ex, {}).get("max_position_usd")
                base_size = self._base["exchanges"].get(ex, {}).get(
                    "max_position_usd",
                    GUARDRAILS["exchange_max_position_usd"][1],
                )
                if old is None or old >= base_size:
                    continue
                new = min(self._clamp("exchange_max_position_usd", old * 1.2), base_size)
                if new != old:
                    key = f"exchanges.{ex}.max_position_usd"
                    if key not in result.changes:
                        self._record(result, key, old, new, rule="rule_6_restore_recovered_exchange")

        # Persist if anything changed.
        if result.changes:
            self._write_overrides(result.changes)
        return result

    async def monitor_loop(self) -> None:
        """Run forever, evaluating once per `interval_sec` (default 24h)."""
        import asyncio
        while True:
            try:
                await self.run_once()
            except Exception as e:
                log.exception("adaptive tuner: %s", e)
                self._journal.log(
                    event_type="ERROR",
                    payload={"where": "adaptive_tuner", "err": str(e)},
                )
            await asyncio.sleep(self._interval_sec)

    # --- internals -----------------------------------------------------

    def _effective_config(self) -> dict:
        """Base config merged with any existing overrides — so a second
        run compounds on the previous run's adjustments."""
        from paper_trader import _deep_merge
        if not self.overrides_path.exists():
            return _deep_merge({}, self._base)
        overlay = yaml.safe_load(self.overrides_path.read_text()) or {}
        return _deep_merge(self._base, overlay)

    def _read_stats(self) -> dict[str, Any]:
        """Aggregate journal counts over the lookback window."""
        from datetime import datetime, timezone
        threshold = (datetime.now(timezone.utc) - self._window).isoformat()
        c = self._journal.conn

        def count_event(event_type: str) -> int:
            row = c.execute(
                "SELECT COUNT(*) FROM journal WHERE event_type=? AND ts >= ?",
                (event_type, threshold),
            ).fetchone()
            return int(row[0])

        def count_aborts_with_reason(reason: str) -> int:
            row = c.execute(
                """SELECT COUNT(*) FROM journal
                   WHERE event_type='ENTRY_ABORTED'
                     AND ts >= ?
                     AND payload_json LIKE ?""",
                (threshold, f'%"reason": "{reason}"%'),
            ).fetchone()
            return int(row[0])

        # Per-exchange saga failures: count Heal steps that mark/quarantine.
        saga_per_ex: dict[str, int] = {}
        for (payload_json,) in c.execute(
            """SELECT payload_json FROM journal
               WHERE event_type='SAGA_STEP' AND ts >= ?
                 AND payload_json LIKE '%"step": "Heal"%'""",
            (threshold,),
        ):
            import json
            try:
                p = json.loads(payload_json)
            except json.JSONDecodeError:
                continue
            ex = p.get("exchange")
            if ex:
                saga_per_ex[ex] = saga_per_ex.get(ex, 0) + 1

        # Per-exchange fills: count by parsing pair "a_b" -> credit both.
        fills_per_ex: dict[str, int] = {}
        for (pair,) in c.execute(
            """SELECT pair FROM journal
               WHERE event_type='FILL' AND ts >= ? AND pair IS NOT NULL""",
            (threshold,),
        ):
            for ex in pair.split("_"):
                fills_per_ex[ex] = fills_per_ex.get(ex, 0) + 1

        return {
            "signals":                    count_event("ENTRY_SIGNAL"),
            "fills":                      count_event("FILL"),
            "aborts_profit":              count_aborts_with_reason("profit_below_threshold"),
            "aborts_hurst":               count_aborts_with_reason("hurst_unsafe"),
            "aborts_quarantined":         count_aborts_with_reason("exchange_quarantined"),
            "saga_failures_per_exchange": saga_per_ex,
            "fills_per_exchange":         fills_per_ex,
        }

    def _clamp(self, key: str, value: float) -> float:
        lo, hi = GUARDRAILS[key]
        return max(lo, min(hi, value))

    def _record(
        self,
        result: NudgeResult,
        path: str,
        old: float,
        new: float,
        *,
        rule: str,
    ) -> None:
        result.changes[path] = float(new)
        self._journal.log(
            event_type="PARAM_NUDGE",
            payload={"path": path, "old": old, "new": new, "rule": rule},
        )

    def _write_overrides(self, changes: dict[str, float]) -> None:
        """Apply `changes` (dot-paths) to the existing overrides and
        write atomically."""
        existing: dict = {}
        if self.overrides_path.exists():
            try:
                existing = yaml.safe_load(self.overrides_path.read_text()) or {}
            except yaml.YAMLError:
                existing = {}

        for dot_path, value in changes.items():
            keys = dot_path.split(".")
            d = existing
            for k in keys[:-1]:
                d = d.setdefault(k, {})
            d[keys[-1]] = value

        # write_state is for JSON; for YAML do the same atomic rename.
        tmp = self.overrides_path.with_suffix(self.overrides_path.suffix + ".tmp")
        tmp.write_text(yaml.safe_dump(existing))
        import os
        os.replace(tmp, self.overrides_path)
