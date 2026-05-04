import json
from decimal import Decimal
from pathlib import Path
from triscan.sources.kucoin import _book_from_kucoin_snapshot, _apply_kucoin_diff


def test_kucoin_book_from_snapshot():
    raw = json.loads(Path(__file__).parent.joinpath("fixtures", "kucoin_l2_snapshot.json").read_text())
    book = _book_from_kucoin_snapshot("kucoin", "BTC/USDT", raw["data"])
    assert book.best_bid().price == Decimal("60000")
    assert book.seq == 100


def test_kucoin_apply_diff_updates_levels_and_seq():
    snap = json.loads(Path(__file__).parent.joinpath("fixtures", "kucoin_l2_snapshot.json").read_text())
    book = _book_from_kucoin_snapshot("kucoin", "BTC/USDT", snap["data"])
    diff = json.loads(Path(__file__).parent.joinpath("fixtures", "kucoin_l2_diff.json").read_text())
    _apply_kucoin_diff(book, diff["data"])
    assert book.bids[0].size == Decimal("0.5")
    assert book.best_ask().price == Decimal("60002")
    assert book.seq == 102
