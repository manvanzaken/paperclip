import pytest

from bbo_trader.sizing import (contracts_for_usd, notional, size_pair, hedge_qty, hedge_plan, lots_floor,
                               excess_to_flatten)
from tests.conftest import mk_spec


def test_contracts_for_usd_respects_lot_and_min():
    s = mk_spec("mexc", contract_size=1.0, lot=1.0, min_qty=1.0)
    assert contracts_for_usd(25.0, 2.0, s) == 12.0
    assert contracts_for_usd(25.0, 2.0, mk_spec("mexc", contract_size=10.0)) == 1.0
    assert contracts_for_usd(25.0, 2.0, mk_spec("mexc", contract_size=10.0, min_qty=5.0)) == 0.0
    assert contracts_for_usd(25.0, 0.0031, mk_spec("mexc", lot=10.0)) == 8060.0   # 8064.5 -> lot 10
    assert contracts_for_usd(0.0, 2.0, s) == 0.0
    assert notional(12.0, 2.0, s) == 24.0
    # fractional lots stay on the lot grid; sub-unit contract sizes (BloFin BTC = 0.001) work
    assert contracts_for_usd(25.0, 65000.0, mk_spec("blofin", contract_size=0.001, lot=0.1, min_qty=0.1)) == 0.3
    assert contracts_for_usd(0.3 * 65.0, 65000.0, mk_spec("blofin", contract_size=0.001, lot=0.1, min_qty=0.1)) == 0.3
    assert contracts_for_usd(25.0, 65000.0, mk_spec("blofin", contract_size=0.001, lot=1.0, min_qty=1.0)) == 0.0
    # non-finite inputs fail closed instead of raising
    assert contracts_for_usd(float("nan"), 2.0, s) == 0.0 and contracts_for_usd(25.0, float("inf"), s) == 0.0
    assert lots_floor(2.3, mk_spec("x", lot=0.5, min_qty=0.5)) == 2.0 and lots_floor(0.3, s) == 0.0


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


def test_size_pair_coarse_vs_fine_lots_and_min_usd():
    fine = mk_spec("blofin", contract_size=1.0, lot=1.0, min_qty=1.0)          # $0.001 per contract
    coarse = mk_spec("mexc", contract_size=10000.0, lot=1.0, min_qty=1.0)      # $10 per contract
    legs = size_pair(25.0, 0.001, 0.001, fine, coarse, max_mismatch_pct=5.0)   # 4,000 single-lot steps before
    assert legs is not None and legs.qty_b == 2.0 and legs.matched_usd == pytest.approx(20.0)
    assert legs.qty_a == 21000.0 and legs.mismatch_pct == pytest.approx(5.0)
    # the matched notional can fall below the minimum position: enforce it here
    assert size_pair(25.0, 0.001, 0.001, fine, coarse, 5.0, min_usd=10.0) is not None   # matched $20 >= $10
    assert size_pair(25.0, 0.001, 0.001, fine, coarse, 5.0, min_usd=21.0) is None       # matched $20 < $21
    assert size_pair(12.0, 0.001, 0.001, fine, coarse, 5.0, min_usd=10.0).matched_usd == pytest.approx(10.0)
    assert size_pair(9.0, 0.001, 0.001, fine, coarse, 5.0, min_usd=10.0) is None        # one $10 contract does not fit $9
    # both legs shrink when neither is a multiple of the other
    a3 = mk_spec("a", contract_size=3.0, lot=1.0, min_qty=1.0)
    b7 = mk_spec("b", contract_size=7.0, lot=1.0, min_qty=1.0)
    legs = size_pair(25.0, 1.0, 1.0, a3, b7, max_mismatch_pct=5.0)
    assert legs is not None and legs.mismatch_pct <= 5.0 and legs.matched_usd == pytest.approx(21.0)
    assert (legs.qty_a, legs.qty_b) == (7.0, 3.0)


def test_hedge_plan_tracks_covered_and_residual():
    sm = mk_spec("blofin", contract_size=1.0, lot=1.0, min_qty=1.0)
    sh = mk_spec("mexc", contract_size=10.0, lot=1.0, min_qty=1.0)
    assert hedge_qty(20.0, 1.0, sm, 1.0, sh) == 2.0
    assert hedge_qty(7.0, 1.0, sm, 1.0, sh) == 0.0      # $7 < one $10 contract -> unhedgeable
    plan = hedge_plan(25.0, 1.0, sm, 1.0, sh)           # $25 of maker fill, $10 hedge contracts
    assert plan.hedge_qty == 2.0 and plan.covered_maker_qty == 20.0 and plan.residual_maker_qty == 5.0
    plan = hedge_plan(10.0, 1.0061, sm, 1.0010, sh)     # $10.06 fill -> 1 hedge contract ($10.01) covers 9.949
    assert plan.hedge_qty == 1.0 and plan.covered_maker_qty == pytest.approx(9.9493, abs=1e-3)
    assert plan.residual_maker_qty == pytest.approx(10.0 - plan.covered_maker_qty)
    none = hedge_plan(7.0, 1.0, sm, 1.0, sh)
    assert none.hedge_qty == 0.0 and none.covered_maker_qty == 0.0 and none.residual_maker_qty == 7.0


def test_excess_to_flatten_rules():
    sm = mk_spec("blofin", contract_size=1.0, lot=1.0, min_qty=1.0)
    assert excess_to_flatten(0.05, 9.95, sm, 5.0) == 0.0          # within tolerance -> accept
    assert excess_to_flatten(5.0, 20.0, sm, 5.0) == 5.0           # 25% of matched -> flatten all 5 lots
    assert excess_to_flatten(7.0, 0.0, sm, 5.0) == 7.0            # nothing matched -> flatten
    assert excess_to_flatten(0.4, 0.0, sm, 5.0) == 0.0            # below one lot: dust, cannot be sent
    assert excess_to_flatten(0.0, 20.0, sm, 5.0) == 0.0
