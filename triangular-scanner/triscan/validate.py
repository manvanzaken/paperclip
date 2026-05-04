from __future__ import annotations
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional
from .models import Book, Quote


@dataclass
class BookMismatch:
    symbol: str
    field: str
    book_value: Decimal
    quote_value: Decimal
    diff_bp: float


def compare_book_to_quote(book: Book, quote: Quote, max_bp: float = 1.0) -> Optional[BookMismatch]:
    if not book.bids or not book.asks:
        return BookMismatch(book.symbol, "bid", Decimal(0), quote.bid, float("inf"))
    book_bid = book.best_bid().price
    book_ask = book.best_ask().price

    def _bp(a: Decimal, b: Decimal) -> float:
        if b == 0:
            return float("inf")
        return float(abs(a - b) / b * Decimal(10000))

    if _bp(book_bid, quote.bid) > max_bp:
        return BookMismatch(book.symbol, "bid", book_bid, quote.bid, _bp(book_bid, quote.bid))
    if _bp(book_ask, quote.ask) > max_bp:
        return BookMismatch(book.symbol, "ask", book_ask, quote.ask, _bp(book_ask, quote.ask))
    return None
