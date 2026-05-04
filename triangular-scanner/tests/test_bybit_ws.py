import json
from decimal import Decimal
from pathlib import Path
from triscan.sources.bybit import _book_from_bybit_snapshot, _apply_bybit_delta


def test_bybit_book_from_snapshot():
    raw = json.loads(Path(__file__).parent.joinpath("fixtures", "bybit_book_snapshot.json").read_text())
    book = _book_from_bybit_snapshot("bybit", "BTC/USDT", raw)
    assert book.best_bid().price == Decimal("60000")
    assert book.seq == 1


def test_bybit_apply_delta():
    snap = json.loads(Path(__file__).parent.joinpath("fixtures", "bybit_book_snapshot.json").read_text())
    book = _book_from_bybit_snapshot("bybit", "BTC/USDT", snap)
    delta = json.loads(Path(__file__).parent.joinpath("fixtures", "bybit_book_delta.json").read_text())
    _apply_bybit_delta(book, delta)
    assert book.bids[0].size == Decimal("0.5")
    assert book.best_ask().price == Decimal("60002")
    assert book.seq == 2
