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
    board.set(mk_bbo("blofin", SYM, 1.0041, 1.0065, ts=now))               # peg moved by 4 ticks
    pos.requote_pending = True                                             # cancel+new requote in flight
    assert ev.evaluate_resting(pos).reason == "resting"
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


def test_scan_skips_blacklisted_and_insane_pairs_and_sorts_by_edge(tmp_path, clock):
    cfg, board, risk, ev = build(tmp_path, clock)
    now = clock()
    for v in ("blofin", "mexc"):
        for sym in ("BIGUSDT", "BADUSDT", "MISUSDT"):
            ev.specs[v][sym] = mk_spec(v, sym)
    board.set(mk_bbo("blofin", SYM, 1.0050, 1.0060, ts=now))
    board.set(mk_bbo("mexc", SYM, 1.0000, 1.0008, ts=now))
    board.set(mk_bbo("blofin", "BIGUSDT", 1.0500, 1.0510, ts=now))       # richer edge -> first row
    board.set(mk_bbo("mexc", "BIGUSDT", 1.0000, 1.0008, ts=now))
    board.set(mk_bbo("blofin", "BADUSDT", 2.0, 2.001, ts=now))           # insane 2x: never a scanner row
    board.set(mk_bbo("mexc", "BADUSDT", 1.0, 1.001, ts=now))
    board.set(mk_bbo("blofin", "MISUSDT", 1.0050, 1.0060, ts=now))
    board.set(mk_bbo("mexc", "MISUSDT", 1.0000, 1.0008, ts=now))
    risk.mismatch.blacklist(route_key("MISUSDT", "blofin", "mexc"))
    rows = ev.scan([SYM, "BIGUSDT", "BADUSDT", "MISUSDT"])
    assert [r["symbol"] for r in rows] == ["BIGUSDT", SYM]
    assert rows[1]["price_short"] == 1.0050 and rows[1]["price_long"] == 1.0008 and rows[0]["edge_pct"] > rows[1]["edge_pct"]
