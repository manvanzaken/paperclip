from decimal import Decimal
import pytest
from triscan.models import LegSide, BookLevel, Book
from triscan.pricing import (
    gross_multiplier,
    net_multiplier,
    simulate_cycle_through_book,
    binary_search_executable_size,
)


def test_gross_multiplier_profitable_triangle():
    legs = [
        ("BTC/USDT", LegSide.BUY,  Decimal("60000")),
        ("ETH/BTC",  LegSide.BUY,  Decimal("0.05")),
        ("ETH/USDT", LegSide.SELL, Decimal("3001")),
    ]
    m = gross_multiplier(legs)
    assert m == pytest.approx(3001 / 3000, rel=1e-9)


def test_gross_multiplier_unprofitable_triangle():
    legs = [
        ("BTC/USDT", LegSide.BUY,  Decimal("60000")),
        ("ETH/BTC",  LegSide.BUY,  Decimal("0.05")),
        ("ETH/USDT", LegSide.SELL, Decimal("2999")),
    ]
    m = gross_multiplier(legs)
    assert m < 1.0


def test_net_multiplier_applies_three_fees():
    gross = 1.01
    net = net_multiplier(gross, fee_pct=Decimal("0.10"))
    expected = 1.01 * (1 - 0.001) ** 3
    assert net == pytest.approx(expected, rel=1e-9)


def _make_book(symbol, bids, asks, seq=1):
    return Book(
        exchange="x",
        symbol=symbol,
        bids=[BookLevel(Decimal(str(p)), Decimal(str(s))) for p, s in bids],
        asks=[BookLevel(Decimal(str(p)), Decimal(str(s))) for p, s in asks],
        ts_ms=1,
        seq=seq,
    )


def test_simulate_cycle_returns_output_and_bottleneck():
    legs = [
        ("BTC/USDT", LegSide.BUY,  _make_book("BTC/USDT", [], [(60000, 0.001)])),
        ("ETH/BTC",  LegSide.BUY,  _make_book("ETH/BTC",  [], [(0.05, 1.0)])),
        ("ETH/USDT", LegSide.SELL, _make_book("ETH/USDT", [(3001, 100)], [])),
    ]
    result = simulate_cycle_through_book(legs, input_size=Decimal("100"))
    assert result.output_quote == pytest.approx(60.02, rel=1e-6)
    assert result.input_consumed == pytest.approx(60.0, rel=1e-9)
    assert result.bottleneck_leg == 0
    assert result.fully_filled is False


def test_simulate_cycle_fully_filled():
    legs = [
        ("BTC/USDT", LegSide.BUY,  _make_book("BTC/USDT", [], [(60000, 1.0)])),
        ("ETH/BTC",  LegSide.BUY,  _make_book("ETH/BTC",  [], [(0.05, 100.0)])),
        ("ETH/USDT", LegSide.SELL, _make_book("ETH/USDT", [(3001, 1000.0)], [])),
    ]
    result = simulate_cycle_through_book(legs, input_size=Decimal("100"))
    assert result.fully_filled is True
    assert result.input_consumed == pytest.approx(100.0, rel=1e-9)


def test_binary_search_finds_max_size_under_threshold():
    legs = [
        ("BTC/USDT", LegSide.BUY,  _make_book("BTC/USDT", [], [(60000, 10.0)])),
        ("ETH/BTC",  LegSide.BUY,  _make_book("ETH/BTC",  [], [(0.05, 10000.0)])),
        ("ETH/USDT", LegSide.SELL, _make_book("ETH/USDT", [(3010, 10000.0)], [])),
    ]
    size = binary_search_executable_size(
        legs, fee_pct=Decimal("0.10"), tier2_threshold_pct=0.0,
        max_size_cap_usd=Decimal("10000"),
    )
    assert size > 0
    assert size <= 10000


def test_binary_search_returns_zero_when_no_profitable_size():
    legs = [
        ("BTC/USDT", LegSide.BUY,  _make_book("BTC/USDT", [], [(60000, 10.0)])),
        ("ETH/BTC",  LegSide.BUY,  _make_book("ETH/BTC",  [], [(0.05, 10000.0)])),
        ("ETH/USDT", LegSide.SELL, _make_book("ETH/USDT", [(2999, 10000.0)], [])),
    ]
    size = binary_search_executable_size(
        legs, fee_pct=Decimal("0.10"), tier2_threshold_pct=0.1,
        max_size_cap_usd=Decimal("10000"),
    )
    assert size == 0
