import pytest

from bbo_trader.sizing import contracts_for_usd, notional, size_pair, hedge_qty
from tests.conftest import mk_spec


def test_contracts_for_usd_respects_lot_and_min():
    s = mk_spec("mexc", contract_size=1.0, lot=1.0, min_qty=1.0)
    assert contracts_for_usd(25.0, 2.0, s) == 12.0
    assert contracts_for_usd(25.0, 2.0, mk_spec("mexc", contract_size=10.0)) == 1.0
    assert contracts_for_usd(25.0, 2.0, mk_spec("mexc", contract_size=10.0, min_qty=5.0)) == 0.0
    assert contracts_for_usd(25.0, 0.0031, mk_spec("mexc", lot=10.0)) == 8060.0   # 8064.5 -> lot 10
    assert contracts_for_usd(0.0, 2.0, s) == 0.0
    assert notional(12.0, 2.0, s) == 24.0


def test_size_pair_matches_notionals_within_tolerance():
    sa = mk_spec("blofin", contract_size=1.0, lot=1.0, min_qty=1.0)
    sb = mk_spec("mexc", contract_size=10.0, lot=1.0, min_qty=1.0)
    loose = size_pair(25.0, 1.0, 1.0, sa, sb, max_mismatch_pct=5.0)
    assert loose is not None
    # b can only do multiples of $10 -> 2 contracts = $20; a shrinks 25 -> 21 ($21 vs $20 = 5.0%, allowed)
    assert loose.qty_b == 2.0 and loose.qty_a == 21.0 and loose.mismatch_pct == pytest.approx(5.0)
    tight = size_pair(25.0, 1.0, 1.0, sa, sb, max_mismatch_pct=1.0)
    assert tight.qty_a == 20.0 and tight.qty_b == 2.0
    assert tight.matched_usd == pytest.approx(20.0) and tight.mismatch_pct == pytest.approx(0.0)


def test_size_pair_returns_none_when_unexpressible():
    sa = mk_spec("blofin", contract_size=1.0, lot=1.0, min_qty=1.0)
    too_big = mk_spec("mexc", contract_size=100.0, lot=1.0, min_qty=1.0)   # one contract = $100 > $25
    assert size_pair(25.0, 1.0, 1.0, sa, too_big, 5.0) is None
    # min_qty on a: 30 contracts at $1 needed but only $25 -> None
    assert size_pair(25.0, 1.0, 1.0, mk_spec("blofin", min_qty=30.0), sa, 5.0) is None


def test_hedge_qty_and_unhedgeable_partial():
    sm = mk_spec("blofin", contract_size=1.0, lot=1.0, min_qty=1.0)
    sh = mk_spec("mexc", contract_size=10.0, lot=1.0, min_qty=1.0)
    assert hedge_qty(20.0, 1.0, sm, 1.0, sh) == 2.0
    assert hedge_qty(7.0, 1.0, sm, 1.0, sh) == 0.0      # $7 < one $10 contract -> unhedgeable
