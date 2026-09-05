from dataclasses import replace

import pytest

from bbo_trader.config import VenueConfig, RateLimits
from bbo_trader.quotes import QuoteBoard
from bbo_trader.venues.registry import build_venues
from bbo_trader.venues.sim import SimVenue
from tests.conftest import make_cfg


def test_paper_wiring_skips_off_and_adapterless_venues(tmp_path, caplog):
    cfg = make_cfg(tmp_path)
    cfg = replace(cfg, venues=cfg.venues + (VenueConfig("gate", "off", 0.05, 0.02, RateLimits()),))
    venues = build_venues(cfg, lambda b: None, QuoteBoard(cfg.stale_quote_s), session=None)
    assert set(venues) == {"mexc", "blofin"} and "VENUE_SKIPPED okx" in caplog.text   # okx: no adapter yet; gate: off
    for v in venues.values():
        assert isinstance(v.trading, SimVenue) and v.private is v.trading and v.tradeable
        assert v.trading.specs is v.specs                          # the sim shares the bundle's spec dict (mutated in place)
        assert v.public is not None and v.market is not None and v.public.contract_size == {}


def test_live_mode_refuses_and_lists_every_problem(tmp_path):
    cfg = make_cfg(tmp_path, mode="live")
    with pytest.raises(RuntimeError) as e:
        build_venues(cfg, lambda b: None, QuoteBoard(2.0), session=None)
    msg = str(e.value)
    assert msg.startswith("live mode refused") and "mexc" in msg and "blofin" in msg and "Plan 2" in msg
