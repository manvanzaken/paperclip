from bbo_trader.models import Position, OPEN
from bbo_trader.risk import MismatchGuard, RiskManager, pair_key, route_key
from tests.conftest import make_cfg


def test_mismatch_guard_slow_and_fast_tiers():
    g = MismatchGuard(slow_pct=10.0, slow_n=3, fast_pct=50.0, fast_n=2)
    assert not g.observe("k", 12.0) and not g.observe("k", 12.0)
    assert not g.observe("k", 5.0)                      # reset by a sane quote
    assert not g.observe("k", 12.0) and not g.observe("k", 12.0)
    assert g.observe("k", 12.0)                         # third consecutive > 10% -> blacklisted
    assert g.is_blacklisted("k")
    g2 = MismatchGuard(10.0, 300, 50.0, 2)
    assert not g2.observe("f", 80.0) and g2.observe("f", -80.0)   # fast tier, sign-agnostic


def test_entry_gates_in_order(tmp_path, clock):
    cfg = make_cfg(tmp_path, max_concurrent=2, blocked_symbols=frozenset({"BADUSDT"}))
    r = RiskManager(cfg, clock)
    ok = lambda **kw: r.entry_allowed(kw.get("symbol", "XYZUSDT"), kw.get("a", "blofin"), kw.get("b", "mexc"),
                                      kw.get("size", 25.0), kw.get("open_count", 0))
    assert ok() == (True, "ok")
    assert ok(open_count=2) == (False, "max_concurrent")
    assert ok(symbol="BADUSDT") == (False, "blocked_symbol")
    r.set_cooldown("XYZUSDT")
    assert ok() == (False, "cooldown")
    clock.tick(61)
    assert ok() == (True, "ok")
    assert ok(a="okx", b="mexc") == (False, "venue_blocked")           # okx whitelist excludes XYZUSDT
    assert ok(symbol="BTCUSDT", a="okx", b="mexc") == (True, "ok")
    r.blacklist_venue_symbol("blofin", "XYZUSDT")
    assert ok() == (False, "venue_blocked")
    r.venue_symbol_blacklist.clear()
    assert not r.record_strike("XYZUSDT", "blofin", "mexc")
    assert r.record_strike("XYZUSDT", "blofin", "mexc")                 # 2 strikes -> 24 h blacklist
    assert ok() == (False, "pair_blacklist")
    assert ok(a="mexc", b="blofin") == (True, "ok")                     # other direction unaffected
    r.mismatch.blacklisted.add(route_key("XYZUSDT", "mexc", "blofin"))
    assert ok(a="mexc", b="blofin") == (False, "mismatch")
    r.pair_stats[pair_key("ABCUSDT", "blofin", "mexc")] = {"wins": 1, "losses": 4, "total_pnl": -1.0}
    assert ok(symbol="ABCUSDT") == (False, "pair_win_rate")
    r.set_balance("mexc", available=20.0, total=100.0)
    assert ok(symbol="QQQUSDT") == (False, "balance")                  # 20 < 25 × 1.05
    r.set_balance("mexc", available=30.0, total=100.0)
    assert ok(symbol="QQQUSDT") == (True, "ok")
    r.halt("test")
    assert ok(symbol="QQQUSDT") == (False, "halted")


def test_halt_flags(tmp_path, clock):
    cfg = make_cfg(tmp_path)
    r = RiskManager(cfg, clock)
    assert r.check_flags() is None
    (tmp_path / "stop.flag").write_text("")
    assert r.check_flags() == "halt" and r.halted
    assert r.check_flags() is None                      # idempotent
    (tmp_path / "start.flag").write_text("")
    assert r.check_flags() == "resume" and not r.halted
    assert not (tmp_path / "stop.flag").exists() and not (tmp_path / "start.flag").exists()


def test_funding_gate_net_of_both_legs(tmp_path, clock):
    r = RiskManager(make_cfg(tmp_path, funding_block_s=600.0), clock)
    now = clock()
    # short blofin receives +0.01%, long mexc pays +0.05% -> net -0.04% within window -> blocked
    r.set_funding("blofin", {"XYZUSDT": (0.0001, now + 300)})
    r.set_funding("mexc", {"XYZUSDT": (0.0005, now + 300)})
    assert r.funding_blocks("XYZUSDT", "blofin", "mexc")
    assert not r.funding_blocks("XYZUSDT", "mexc", "blofin")           # reversed legs: net +0.04%
    r.set_funding("mexc", {"XYZUSDT": (0.0005, now + 3600)})           # mexc settles outside the window
    assert not r.funding_blocks("XYZUSDT", "blofin", "mexc")
    assert not r.funding_blocks("NOFUNDUSDT", "blofin", "mexc")        # unknown -> allowed


def test_record_close_and_persistence(tmp_path, clock):
    r = RiskManager(make_cfg(tmp_path), clock)
    p = Position(1, "XYZUSDT", "blofin", "mexc", OPEN, "TT", size_usd=25.0, net_pnl_usd=-0.05)
    r.record_close(p)                                    # -0.2% of size -> 6 h symbol blacklist
    assert r.symbol_blacklist["XYZUSDT"] == clock() + 21_600.0
    assert r.pair_stats[pair_key("XYZUSDT", "blofin", "mexc")] == {"wins": 0, "losses": 1, "total_pnl": -0.05}
    d = r.to_dict()
    r2 = RiskManager(make_cfg(tmp_path), clock)
    r2.load(d)
    assert r2.symbol_blacklist == r.symbol_blacklist and r2.pair_stats == r.pair_stats
    clock.tick(30_000)
    assert RiskManager(make_cfg(tmp_path), clock).load(d) is None
    r3 = RiskManager(make_cfg(tmp_path), clock)
    r3.load(d)
    assert r3.symbol_blacklist == {}                     # expired entries dropped on load
