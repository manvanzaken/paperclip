import pytest

from bbo_trader.budget import TokenBucket, RateBudget
from bbo_trader.config import RateLimits


def test_bucket_reserve_and_window():
    b = TokenBucket(capacity=5, window_s=2.0, reserve=2)
    now = 100.0
    assert b.available(now) == 3 and b.available(now, priority=True) == 5
    assert all(b.try_take(now) for _ in range(3))
    assert not b.try_take(now)                       # reserve protects the last 2
    assert b.try_take(now, priority=True) and b.try_take(now, priority=True)
    assert not b.try_take(now, priority=True)        # exhausted
    assert b.try_take(now + 2.01)                    # window rolled


def test_bucket_penalty_halves_capacity():
    b = TokenBucket(capacity=4, window_s=10.0, reserve=0)
    b.penalize(now=0.0, seconds=60.0)
    assert b.available(1.0) == 2
    assert b.available(61.0) == 4


def test_penalty_pauses_entries_but_never_priority_calls():
    # BloFin-like: 30 per 10 s shared, reserve 6; entries used 24, then a 429 lands
    b = TokenBucket(capacity=30, window_s=10.0, reserve=6)
    for _ in range(24):
        assert b.try_take(0.0)
    b.penalize(now=0.0, seconds=60.0)
    assert b.available(0.001) < 0 and not b.try_take(0.001)              # entries paused
    assert all(b.try_take(0.001, priority=True) for _ in range(6))       # hedges/closes keep the reserve
    assert not b.try_take(0.001, priority=True)                          # ... but never exceed the venue limit
    assert b.penalized(0.001) and not b.penalized(60.0)


def test_try_take_n_and_window_boundary():
    b = TokenBucket(capacity=3, window_s=2.0, reserve=0)
    assert b.try_take(0.0, n=2)
    assert not b.try_take(0.0, n=2)                  # only one token left
    assert b.try_take(0.0, n=1)
    assert not b.try_take(2.0)                       # a token taken exactly window_s ago still counts
    assert b.try_take(2.0000001)


def test_rate_budget_shared_vs_separate():
    shared = RateBudget(RateLimits(orders=3, cancels=3, window_s=10.0, reserve=1, shared=True))
    assert shared.try_take("order", 0.0) and shared.try_take("cancel", 0.0)
    assert not shared.try_take("order", 0.0)                       # one bucket: 2 used + reserve 1
    assert shared.try_take("cancel", 0.0, priority=True)           # priority may use the reserve
    separate = RateBudget(RateLimits(orders=1, cancels=1, window_s=10.0, reserve=0, shared=False))
    assert separate.try_take("order", 0.0) and separate.try_take("cancel", 0.0)
    assert not separate.try_take("amend", 0.0)                     # amend draws from orders
    assert separate.to_dict(0.0) == {"orders_free": 0, "cancels_free": 0, "orders_entry_free": 0, "cancels_entry_free": 0,
                                     "shared": False, "penalized": False}


def test_unknown_kind_raises():
    b = RateBudget(RateLimits(orders=5, cancels=5, window_s=1.0, reserve=0, shared=False))
    with pytest.raises(ValueError, match="unknown budget kind"):
        b.try_take("cancels", 0.0)


def test_to_dict_after_penalty_clamps_and_flags():
    b = RateBudget(RateLimits(orders=3, cancels=3, window_s=10.0, reserve=1, shared=True))
    assert b.try_take("order", 0.0) and b.try_take("order", 0.0)
    b.penalize(0.0)
    d = b.to_dict(0.5)
    assert d == {"orders_free": 1, "cancels_free": 1, "orders_entry_free": 0, "cancels_entry_free": 0,
                 "shared": True, "penalized": True}
    assert b.available("order", 0.5) < 0                          # entries see the halved capacity
