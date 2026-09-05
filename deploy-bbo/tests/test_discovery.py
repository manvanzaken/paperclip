from bbo_trader.discovery import build_universe, symbols_for_venue, symbols_for_quote_venue
from tests.conftest import mk_spec


def test_universe_requires_two_trade_venues_and_applies_filters():
    specs = {
        "mexc": {s: mk_spec("mexc", s) for s in ("AUSDT", "BUSDT", "CUSDT", "BADUSDT")},
        "blofin": {s: mk_spec("blofin", s) for s in ("AUSDT", "BUSDT", "BADUSDT")},
        "okx": {s: mk_spec("okx", s) for s in ("AUSDT", "CUSDT")},
    }
    uni = build_universe(specs, ["mexc", "blofin", "okx"], blocked={"BADUSDT"}, whitelists={"okx": ("CUSDT",)})
    assert uni == {"AUSDT": ["blofin", "mexc"], "BUSDT": ["blofin", "mexc"], "CUSDT": ["mexc", "okx"]}
    assert symbols_for_venue(uni, "okx") == ["CUSDT"]
    assert symbols_for_venue(uni, "mexc") == ["AUSDT", "BUSDT", "CUSDT"]
    assert symbols_for_quote_venue(uni, specs["okx"]) == ["AUSDT", "CUSDT"]
    assert build_universe(specs, ["mexc"], set(), {}) == {}
