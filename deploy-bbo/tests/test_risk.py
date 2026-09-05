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
