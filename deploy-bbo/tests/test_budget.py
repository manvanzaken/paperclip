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


def test_rate_budget_shared_vs_separate():
    shared = RateBudget(RateLimits(orders=3, cancels=3, window_s=10.0, reserve=1, shared=True))
    assert shared.try_take("order", 0.0) and shared.try_take("cancel", 0.0)
    assert not shared.try_take("order", 0.0)                       # one bucket: 2 used + reserve 1
    assert shared.try_take("cancel", 0.0, priority=True)           # priority may use the reserve
    separate = RateBudget(RateLimits(orders=1, cancels=1, window_s=10.0, reserve=0, shared=False))
    assert separate.try_take("order", 0.0) and separate.try_take("cancel", 0.0)
    assert not separate.try_take("amend", 0.0)                     # amend draws from orders
    assert separate.to_dict(0.0) == {"orders_free": 0, "cancels_free": 0}
