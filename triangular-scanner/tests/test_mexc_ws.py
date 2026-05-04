import json
from decimal import Decimal
from pathlib import Path
from triscan.sources.mexc import _book_from_mexc_snapshot


def test_mexc_book_from_snapshot():
    raw = json.loads(Path(__file__).parent.joinpath("fixtures", "mexc_depth.json").read_text())
    book = _book_from_mexc_snapshot("mexc", "BTC/USDT", raw)
    assert book.best_bid().price == Decimal("60000")
    assert book.best_ask().price == Decimal("60001")
    assert book.seq == 1700000000000
