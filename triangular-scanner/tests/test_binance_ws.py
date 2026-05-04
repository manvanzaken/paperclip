import json
from decimal import Decimal
from pathlib import Path
import pytest
from triscan.sources.binance import BinanceSource, _book_from_snapshot, _apply_diff


def _load(name):
    return json.loads(Path(__file__).parent.joinpath("fixtures", name).read_text())


def test_book_from_snapshot_sorts_correctly():
    snap = _load("binance_depth_snapshot.json")
    book = _book_from_snapshot("binance", "BTC/USDT", snap)
    assert book.best_bid().price == Decimal("60000.0")
    assert book.bids[1].price == Decimal("59990.0")
    assert book.best_ask().price == Decimal("60001.0")
    assert book.seq == 100


def test_apply_diff_updates_levels():
    snap = _load("binance_depth_snapshot.json")
    book = _book_from_snapshot("binance", "BTC/USDT", snap)
    diff = _load("binance_depth_diff.json")
    _apply_diff(book, diff)
    assert book.bids[0].price == Decimal("60000.0")
    assert book.bids[0].size == Decimal("0.5")
    assert book.best_ask().price == Decimal("60002.0")
    assert book.seq == 102
