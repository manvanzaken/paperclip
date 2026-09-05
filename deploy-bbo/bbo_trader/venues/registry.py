"""Build Venue bundles from the registry: a public feed + market data for every venue with an adapter,
SimVenue trading in paper mode, live trading adapters (Plan 2) via LIVE_ADAPTERS in live mode."""
from __future__ import annotations

import logging
import time
from typing import Callable

import aiohttp

from ..budget import RateBudget
from ..config import Config, VenueConfig
from ..models import BBO, Fees
from ..quotes import QuoteBoard
from .base import Venue
from .blofin import BlofinPublic, BlofinMarket
from .mexc import MexcPublic, MexcMarket
from .sim import SimVenue

log = logging.getLogger("bbo.registry")

PUBLIC_ADAPTERS: dict[str, tuple[type, type]] = {"mexc": (MexcPublic, MexcMarket), "blofin": (BlofinPublic, BlofinMarket)}
# name -> factory(cfg: VenueConfig, session, clock) -> (Trading, PrivateFeed); populated by Plan 2
LIVE_ADAPTERS: dict[str, Callable] = {}


def build_venues(cfg: Config, on_bbo: Callable[[BBO], None], board: QuoteBoard,
                 session: aiohttp.ClientSession, clock=time.time) -> dict[str, Venue]:
    out: dict[str, Venue] = {}
    problems: list[str] = []
    for vc in cfg.venues:
        if vc.role == "off":
            continue
        if vc.name not in PUBLIC_ADAPTERS:
            log.warning("VENUE_SKIPPED %s: no public adapter yet", vc.name)
            continue
        pub_cls, mkt_cls = PUBLIC_ADAPTERS[vc.name]
        fees = Fees(vc.taker_fee_pct, vc.maker_fee_pct)
        v = Venue(vc, fees, RateBudget(vc.rate_limits), public=pub_cls(vc, on_bbo, clock=clock),
                  market=mkt_cls(session, clock=clock))
        if vc.role == "trade":
            if cfg.mode == "paper":
                sim = SimVenue(vc.name, cfg, fees, board, v.specs, clock=clock)
                v.trading, v.private = sim, sim
            else:
                if vc.name not in LIVE_ADAPTERS:
                    problems.append(f"no live trading adapter for {vc.name} (Plan 2)")
                elif not (vc.api_key and vc.api_secret):
                    problems.append(f"{vc.name}: API key/secret missing for live mode")
                else:
                    v.trading, v.private = LIVE_ADAPTERS[vc.name](vc, session, clock)
        out[vc.name] = v
    if problems:                                  # every problem at once: one restart per finding is not acceptable
        raise RuntimeError("live mode refused: " + "; ".join(problems))
    return out
