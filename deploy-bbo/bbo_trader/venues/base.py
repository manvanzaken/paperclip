"""Venue protocols — everything the strategy and executor may ask of a venue — and the Venue bundle.

Implementations: venues/mexc.py, venues/blofin.py (public side in Plan 1, private + trading in Plan 2),
venues/sim.py (paper trading over any real public feed)."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Iterable, Protocol

from ..budget import RateBudget
from ..config import VenueConfig
from ..models import BBO, Fees, OrderAck, OrderEvent, VenueSpec


@dataclass(frozen=True)
class VenuePosition:
    venue: str
    symbol: str
    side: str          # long | short
    qty: float         # contracts (absolute)
    position_id: str = ""


class PublicFeed(Protocol):
    """Streams BBO updates for a set of symbols into the on_bbo callback it was built with.
    `set_specs` must run before quotes flow: a BBO carries the contract size of its spec (1.0 if unknown)."""
    async def run(self) -> None: ...
    def set_symbols(self, symbols: Iterable[str]) -> None: ...
    def set_specs(self, specs: dict[str, VenueSpec]) -> None: ...
    @property
    def connected(self) -> bool: ...


class MarketData(Protocol):
    """Public REST: contract specs, 24 h USD volumes, funding, and a BBO fallback for open positions.
    `specs` is set by `fetch_specs` and re-aliased by the App to the Venue bundle's dict; `fetch_bbo`
    uses it for the contract size."""
    specs: dict[str, VenueSpec]
    async def fetch_specs(self) -> dict[str, VenueSpec]: ...
    async def fetch_volumes(self) -> dict[str, float]: ...
    async def fetch_funding(self) -> dict[str, tuple[float, float]]: ...   # symbol -> (rate FRACTION, settle unix SECONDS)
    async def fetch_bbo(self, symbol: str) -> BBO | None: ...


class Trading(Protocol):
    """Order entry. Units and vocabulary (the executor relies on all of these):
    - `qty` is in CONTRACTS (never USD), `price` in the quote currency, `side` is "buy" | "sell"
      (a VenuePosition's `side` is "long" | "short" — a different vocabulary).
    - `OrderAck.ok` means ACCEPTED by the venue, not filled; fills arrive as OrderEvents on the PrivateFeed
      (or via `query_order`). `cancel` returns an OrderAck too: ok=False with `error` when the venue refused
      or the transport failed — the executor then queries the order and may retry.
    - `balance()` returns {"available": USDT, "total": USDT}; the risk gate reads exactly those keys."""
    supports_amend: bool
    async def place_market(self, symbol: str, side: str, qty: float, reduce_only: bool, client_id: str) -> OrderAck: ...
    async def place_post_only(self, symbol: str, side: str, qty: float, price: float, reduce_only: bool,
                              client_id: str) -> OrderAck: ...
    async def cancel(self, symbol: str, client_id: str, order_id: str) -> OrderAck: ...
    async def amend(self, symbol: str, client_id: str, order_id: str, new_price: float) -> OrderAck: ...
    async def query_order(self, symbol: str, client_id: str, order_id: str) -> OrderEvent | None: ...
    async def open_orders(self) -> list[OrderEvent]: ...
    async def positions(self) -> list[VenuePosition]: ...
    async def balance(self) -> dict[str, float]: ...
    async def set_leverage(self, symbol: str, leverage: int) -> None: ...


class PrivateFeed(Protocol):
    """Delivers OrderEvents (ack/partial/filled/canceled/rejected) to the registered handler."""
    def set_handler(self, on_event: Callable[[OrderEvent], None]) -> None: ...
    async def run(self) -> None: ...


@dataclass
class Venue:
    cfg: VenueConfig
    fees: Fees
    budget: RateBudget
    public: PublicFeed | None = None
    market: MarketData | None = None
    trading: Trading | None = None
    private: PrivateFeed | None = None
    # shared by reference with the SimVenue and re-aliased onto `market.specs`: mutate in place (clear/update),
    # never rebind, or the paper venue silently falls back to contract size 1.0
    specs: dict[str, VenueSpec] = field(default_factory=dict)
    volumes: dict[str, float] = field(default_factory=dict)

    @property
    def name(self) -> str:
        return self.cfg.name

    @property
    def tradeable(self) -> bool:
        # fills are event-driven: without a private feed a resting maker's fill would go unnoticed until its TTL
        return self.cfg.role == "trade" and self.trading is not None and self.private is not None
