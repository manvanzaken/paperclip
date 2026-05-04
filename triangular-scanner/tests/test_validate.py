from decimal import Decimal
import pytest
from triscan.models import Book, BookLevel, Quote
from triscan.validate import compare_book_to_quote, BookMismatch


def test_compare_book_matches_within_tolerance():
    book = Book("x", "BTC/USDT",
                bids=[BookLevel(Decimal("60000"), Decimal("1"))],
                asks=[BookLevel(Decimal("60001"), Decimal("1"))],
                ts_ms=1, seq=1)
    quote = Quote("x", "BTC/USDT", bid=Decimal("60000.5"), ask=Decimal("60000.5"), ts_ms=2)
    res = compare_book_to_quote(book, quote, max_bp=1.0)
    assert isinstance(res, BookMismatch) is False or res is None


def test_compare_book_flags_outside_tolerance():
    book = Book("x", "BTC/USDT",
                bids=[BookLevel(Decimal("60000"), Decimal("1"))],
                asks=[BookLevel(Decimal("60001"), Decimal("1"))],
                ts_ms=1, seq=1)
    quote = Quote("x", "BTC/USDT", bid=Decimal("60100"), ask=Decimal("60101"), ts_ms=2)
    res = compare_book_to_quote(book, quote, max_bp=1.0)
    assert isinstance(res, BookMismatch)
    assert res.field in ("bid", "ask")
