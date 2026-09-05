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
