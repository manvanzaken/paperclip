import time

import pytest

from bbo_trader.models import BBO, VenueSpec, Fees


class FakeClock:
    """Deterministic clock: call it like time.time(); advance with .tick()."""

    def __init__(self, start: float = 1_700_000_000.0):
        self.t = start

    def __call__(self) -> float:
        return self.t

    def tick(self, seconds: float) -> float:
        self.t += seconds
        return self.t


def mk_bbo(venue: str, symbol: str, bid: float, ask: float, bq: float = 1000.0, aq: float = 1000.0,
           ts: float | None = None, contract_size: float = 1.0, ts_exchange: float | None = None) -> BBO:
    """ts = local receive time (governs staleness); ts_exchange defaults to ts unless given."""
    t = time.time() if ts is None else ts
    return BBO(venue=venue, symbol=symbol, bid=bid, bid_qty=bq, ask=ask, ask_qty=aq,
               ts_exchange=t if ts_exchange is None else ts_exchange, ts_local=t, contract_size=contract_size)


def mk_spec(venue: str, symbol: str = "XYZUSDT", contract_size: float = 1.0, lot: float = 1.0,
            min_qty: float = 1.0, tick: float = 0.0001) -> VenueSpec:
    return VenueSpec(venue=venue, symbol=symbol, instrument=symbol, contract_size=contract_size,
                     lot=lot, min_qty=min_qty, tick=tick)


MEXC_FEES = Fees(taker=0.02, maker=0.00)
BLOFIN_FEES = Fees(taker=0.06, maker=0.02)


@pytest.fixture
def clock():
    return FakeClock()


def make_cfg(tmp_path=None, **over):
    """A Config with mexc + blofin as trade venues, no file I/O."""
    from pathlib import Path
    from bbo_trader.config import Config, VenueConfig, RateLimits
    venues = (
        VenueConfig("mexc", "trade", 0.02, 0.00, RateLimits(20, 20, 2.0, 4, False), max_topics=30, min_requote_ms=500),
        VenueConfig("blofin", "trade", 0.06, 0.02, RateLimits(30, 30, 10.0, 6, True), max_topics=50, min_requote_ms=1000),
        VenueConfig("okx", "quote_only", 0.05, 0.02, RateLimits(), symbol_whitelist=("BTCUSDT",)),
    )
    kw = dict(venues=venues, data_dir=Path(tmp_path) if tmp_path is not None else Path("./data"))
    kw.update(over)
    return Config(**kw)
