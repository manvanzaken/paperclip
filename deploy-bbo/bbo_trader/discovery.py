"""Universe construction. Pure: symbols listed on >= 2 trade venues after blocked list and per-venue whitelists."""
from __future__ import annotations

from .models import VenueSpec


def build_universe(specs: dict[str, dict[str, VenueSpec]], trade_venues: list[str], blocked: frozenset[str] | set[str],
                   whitelists: dict[str, tuple[str, ...]]) -> dict[str, list[str]]:
    listed: dict[str, list[str]] = {}
    for v in trade_venues:
        wl = set(whitelists.get(v) or ())
        for sym in specs.get(v, {}):
            if sym in blocked or (wl and sym not in wl):
                continue
            listed.setdefault(sym, []).append(v)
    return {s: sorted(vs) for s, vs in sorted(listed.items()) if len(vs) >= 2}


def symbols_for_venue(universe: dict[str, list[str]], venue: str) -> list[str]:
    """Trade venue: the universe symbols it is a leg for."""
    return sorted(s for s, vs in universe.items() if venue in vs)


def symbols_for_quote_venue(universe: dict[str, list[str]], venue_specs: dict[str, VenueSpec]) -> list[str]:
    """Quote-only venue: every universe symbol it lists (feeds the scanner, never a leg)."""
    return sorted(s for s in universe if s in venue_specs)
