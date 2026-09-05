"""Structural conformance: every adapter must match the venue protocols exactly (names, parameter
order, async-ness, properties, data members). Protocols are not runtime_checkable and nothing else
would catch a drifting signature before a live order call."""
import inspect

from bbo_trader.venues import base
from bbo_trader.venues.blofin import BlofinMarket, BlofinPublic
from bbo_trader.venues.mexc import MexcMarket, MexcPublic
from bbo_trader.venues.sim import SimVenue

IMPLEMENTATIONS = {
    base.PublicFeed: [MexcPublic, BlofinPublic],
    base.MarketData: [MexcMarket, BlofinMarket],
    base.Trading: [SimVenue],
    base.PrivateFeed: [SimVenue],
}
INSTANCES = {MexcMarket: lambda: MexcMarket(None), BlofinMarket: lambda: BlofinMarket(None)}   # for instance attrs


def test_adapters_conform_to_protocols():
    for proto, impls in IMPLEMENTATIONS.items():
        for impl in impls:
            for name, member in vars(proto).items():
                if name.startswith("_"):
                    continue
                assert hasattr(impl, name), (proto.__name__, impl.__name__, name)
                mine = getattr(impl, name)
                if isinstance(member, property):
                    assert isinstance(mine, property), (impl.__name__, name)
                    continue
                if callable(member):
                    assert list(inspect.signature(member).parameters) == list(inspect.signature(mine).parameters), \
                        (impl.__name__, name)
                    assert inspect.iscoroutinefunction(member) == inspect.iscoroutinefunction(mine), (impl.__name__, name)
            for attr in getattr(proto, "__annotations__", {}):          # data members: supports_amend, specs
                obj = INSTANCES[impl]() if impl in INSTANCES else impl
                assert hasattr(obj, attr), (proto.__name__, impl.__name__, attr)