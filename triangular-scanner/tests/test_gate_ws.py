import json
from decimal import Decimal
from pathlib import Path
from triscan.sources.gate import _book_from_gate_snapshot


def test_gate_book_from_snapshot():
    raw = json.loads(Path(__file__).parent.joinpath("fixtures", "gate_orderbook.json").read_text())
    book = _book_from_gate_snapshot("gate", "BTC/USDT", raw["result"])
    assert book.best_bid().price == Decimal("60000")
    assert book.best_ask().price == Decimal("60001")
