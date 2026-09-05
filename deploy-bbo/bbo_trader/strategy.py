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
