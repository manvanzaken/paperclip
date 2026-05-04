from decimal import Decimal
from triscan.models import Quote, BookLevel, Book, Triangle, LegSide, OpportunityState


def test_quote_construction():
    q = Quote(exchange="binance", symbol="BTC/USDT", bid=Decimal("60000"), ask=Decimal("60001"), ts_ms=1)
    assert q.bid < q.ask


def test_book_best_bid_ask():
    b = Book(
        exchange="binance",
        symbol="BTC/USDT",
        bids=[BookLevel(Decimal("60000"), Decimal("1.0"))],
        asks=[BookLevel(Decimal("60001"), Decimal("1.0"))],
        ts_ms=1,
        seq=42,
    )
    assert b.best_bid().price == Decimal("60000")
    assert b.best_ask().price == Decimal("60001")


def test_triangle_signature_is_stable():
    t = Triangle(
        exchange="binance",
        anchor="USDT",
        legs=(("BTC/USDT", LegSide.BUY), ("ETH/BTC", LegSide.BUY), ("ETH/USDT", LegSide.SELL)),
    )
    assert t.id == "binance:USDT:BTC/USDT@BUY|ETH/BTC@BUY|ETH/USDT@SELL"


def test_opportunity_state_enum():
    assert OpportunityState.IDLE.value == "idle"
    assert OpportunityState.CANDIDATE.value == "candidate"
    assert OpportunityState.CONFIRMED.value == "confirmed"
