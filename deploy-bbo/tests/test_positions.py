import pytest

from bbo_trader.models import (Position, TT_ENTERING, MAKER_RESTING, HEDGING, OPEN, EXIT_MAKER_RESTING,
                               TT_EXITING, CLOSED, DEGRADED)
from bbo_trader.positions import (transition, InvalidTransition, finalize_pnl, PositionBook, StateStore,
                                  build_state)


def test_transition_table():
    p = Position(1, "XYZUSDT", "blofin", "mexc", MAKER_RESTING, "TM")
    transition(p, HEDGING)
    transition(p, OPEN)
    transition(p, EXIT_MAKER_RESTING)
    transition(p, OPEN)                       # TTL/edge-gone returns to OPEN
    transition(p, TT_EXITING)
    with pytest.raises(InvalidTransition):
        transition(p, OPEN)                   # exiting cannot reopen
    transition(p, DEGRADED)
    transition(p, CLOSED)
    with pytest.raises(InvalidTransition):
        transition(p, OPEN)


def test_finalize_pnl_two_legs():
    p = Position(1, "XYZUSDT", "blofin", "mexc", TT_EXITING, "TT", size_usd=25.0,
                 entry_price_a=1.01, entry_price_b=1.00, exit_price_a=1.002, exit_price_b=1.001,
                 entry_fees_usd=0.02, exit_fees_usd=0.02)
    finalize_pnl(p)
    # short leg: (1.01-1.002)/1.01*25 = 0.19802 ; long leg: (1.001-1.0)/1.0*25 = 0.025
    assert p.gross_pnl_usd == pytest.approx(0.19802 + 0.025, abs=1e-5)
    assert p.net_pnl_usd == pytest.approx(p.gross_pnl_usd - 0.04)
    assert p.exit_spread_pct == pytest.approx((1.002 - 1.001) / 1.001 * 100)


def test_book_lifecycle_and_totals():
    book = PositionBook(closed_keep=2)
    p1 = book.new("XYZUSDT", "blofin", "mexc", TT_ENTERING, "TT", size_usd=25.0)
    p2 = book.new("ABCUSDT", "blofin", "mexc", MAKER_RESTING, "TM", maker_venue="blofin")
    assert (p1.id, p2.id, book.next_id) == (1, 2, 3)
    assert book.for_symbol("XYZUSDT") == [p1] and book.get(2) is p2
    assert book.resting_counts() == {"blofin": 1}
    p1.status = OPEN
    p1.entry_price_a, p1.entry_price_b, p1.exit_price_a, p1.exit_price_b = 1.0, 1.0, 0.99, 1.0
    transition(p1, TT_EXITING)
    book.close(p1, "convergence", now=200.0)
    assert p1.status == CLOSED and p1.exit_reason == "convergence" and p1.exit_time == 200.0
    assert book.total_trades == 1 and book.total_wins == 1 and book.total_pnl_usd == pytest.approx(0.25)
    book.discard(p2)                                  # cancelled maker, never a trade
    assert book.open == [] and book.total_trades == 1
    for i in range(3):                                # closed list is capped
        q = book.new("QQQUSDT", "blofin", "mexc", TT_ENTERING, "TT")
        q.status = TT_EXITING
        book.close(q, "timeout", now=300.0 + i, counts_as_trade=False)
    assert len(book.closed) == 2 and book.total_trades == 1
    book.mark_equity(100.0, now=0.0)
    book.mark_equity(90.0, now=10.0)                  # within 60 s -> no new history point
    assert book.peak_equity == 100.0 and book.max_drawdown_pct == pytest.approx(10.0)
    assert len(book.equity_history) == 1


def test_state_store_roundtrip_and_dashboard_schema(tmp_path):
    book = PositionBook()
    p = book.new("XYZUSDT", "blofin", "mexc", OPEN, "TM", size_usd=20.0, entry_time=1_700_000_000.0)
    p.client_ids["maker"] = "bp1-maker-1"
    state = build_state(book, equity=210.0, cash=200.0, starting_capital=200.0, mode="paper",
                        risk_state={"pair_stats": {}, "venue_symbol_blacklist": ["blofin|HNTUSDT"],
                                    "symbol_blacklist": {}, "pair_strikes": {}, "pair_blacklist": {}},
                        balances={"mexc": {"available": 100.0}}, scanner=[], bbo={"latency": {}},
                        saved_at=1_700_000_100.0)
    for key in ("state_saved_at_ts", "cash", "equity", "open_positions", "closed_positions", "total_pnl_usd",
                "pair_stats", "blofin_risk_blacklist", "balance_cache", "spread_scanner", "dry_run",
                "kill_switch", "saved_at", "bbo", "risk", "order_audit_log", "equity_history"):
        assert key in state, key
    assert state["blofin_risk_blacklist"] == ["HNTUSDT"] and state["kill_switch"] is False
    store = StateStore(tmp_path / "real_state.json")
    store.save(state)
    loaded = store.load()
    assert loaded["open_positions"][0]["exchange_short"] == "blofin"
    book2 = PositionBook()
    book2.load(loaded)
    assert book2.open[0] == p and book2.next_id == 2
    assert StateStore(tmp_path / "missing.json").load() is None


def test_closed_dicts_are_cached_and_reloaded():
    book = PositionBook(closed_keep=2)
    for i in range(3):
        q = book.new("QQQUSDT", "blofin", "mexc", TT_ENTERING, "TT", size_usd=10.0)
        q.status = TT_EXITING
        book.close(q, "timeout", now=100.0 + i)
    d = book.to_dict()
    assert [c["id"] for c in d["closed_positions"]] == [2, 3]            # capped like `closed`
    assert d["closed_positions"] is not book.to_dict()["closed_positions"]  # fresh list each call
    assert d["closed_positions"][0] == book.closed[0].to_dict()          # cached dict equals a fresh one
    book2 = PositionBook(closed_keep=2)
    book2.load(d)
    assert [c["id"] for c in book2.to_dict()["closed_positions"]] == [2, 3]
