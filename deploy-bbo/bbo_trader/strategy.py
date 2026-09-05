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
