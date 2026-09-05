from bbo_trader.models import (BBO, OrderEvent, Intent, Position, OPEN, TT_ENTERING, none)
from tests.conftest import mk_bbo


def test_bbo_derived_values():
    q = mk_bbo("mexc", "XYZUSDT", bid=1.0, ask=1.002, bq=500, aq=200, contract_size=10)
    assert q.ok
    assert abs(q.mid - 1.001) < 1e-12
    assert abs(q.width_pct - (0.002 / 1.002 * 100)) < 1e-9
    assert q.touch_notional("sell") == 1.0 * 500 * 10      # selling hits the bid
    assert q.touch_notional("buy") == 1.002 * 200 * 10     # buying lifts the ask
    assert not mk_bbo("mexc", "XYZUSDT", bid=1.0, ask=0.99).ok  # crossed book is not ok


def test_order_event_terminal():
    assert OrderEvent("mexc", "c1", "o1", "filled").terminal
    assert not OrderEvent("mexc", "c1", "o1", "partial").terminal
    assert OrderEvent("mexc", "c1", "o1", "rejected", error="would cross").terminal


def test_intent_none_helper():
    i = none("below_edge")
    assert isinstance(i, Intent) and i.kind == "NONE" and i.reason == "below_edge"


def test_position_round_trip_and_dashboard_keys():
    p = Position(id=7, symbol="XYZUSDT", venue_a="blofin", venue_b="mexc", status=TT_ENTERING, mode="TT",
                 size_usd=25.0, entry_time=1_700_000_000.0)
    p.status = OPEN
    p.entry_price_a, p.entry_price_b = 1.01, 1.0
    p.client_ids["entry_a"] = "bp7-entry_a-1"
    d = p.to_dict()
    # dashboard-compatible aliases
    assert d["exchange_short"] == "blofin" and d["exchange_long"] == "mexc"
    assert d["entry_price_short"] == 1.01 and d["entry_price_long"] == 1.0
    assert d["instrument_short"] == "PERP" and d["status"] == "OPEN"
    assert d["entry_time"].startswith("2023-11-14T22:13:20")
    back = Position.from_dict(d)
    assert back == p
