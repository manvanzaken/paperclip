"""QuoteBoard: latest best bid/ask per venue×symbol with staleness. Pure, no I/O."""
from __future__ import annotations

from .models import BBO


class QuoteBoard:
    def __init__(self, stale_s: float, overrides: dict[str, float] | None = None):
        self.stale_s = stale_s
        self.overrides = dict(overrides or {})
        self._q: dict[tuple[str, str], BBO] = {}
        self._venues_by_symbol: dict[str, set[str]] = {}

    def stale_for(self, venue: str) -> float:
        return self.overrides.get(venue, self.stale_s)

    def set(self, bbo: BBO) -> bool:
        """Store a quote. Crossed/empty books are ignored (returns False)."""
        if not bbo.ok:
            return False
        self._q[(bbo.venue, bbo.symbol)] = bbo
        self._venues_by_symbol.setdefault(bbo.symbol, set()).add(bbo.venue)
        return True

    def get(self, venue: str, symbol: str) -> BBO | None:
        return self._q.get((venue, symbol))

    def is_fresh(self, bbo: BBO, now: float) -> bool:
        return (now - bbo.ts_local) <= self.stale_for(bbo.venue)

    def fresh(self, venue: str, symbol: str, now: float) -> BBO | None:
        q = self._q.get((venue, symbol))
        return q if q is not None and self.is_fresh(q, now) else None

    def fresh_venues(self, symbol: str, now: float) -> list[str]:
        return sorted(v for v in self._venues_by_symbol.get(symbol, ())
                      if self.fresh(v, symbol, now) is not None)

    def fresh_counts(self, now: float) -> dict[str, int]:
        out: dict[str, int] = {}
        for (venue, _symbol), q in self._q.items():
            out.setdefault(venue, 0)
            if self.is_fresh(q, now):
                out[venue] += 1
        return out

    def symbols(self) -> set[str]:
        return set(self._venues_by_symbol)
