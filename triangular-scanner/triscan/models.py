from __future__ import annotations
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, List, Optional, Tuple


class LegSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OpportunityState(str, Enum):
    IDLE = "idle"
    CANDIDATE = "candidate"
    CONFIRMED = "confirmed"


@dataclass(frozen=True)
class Quote:
    exchange: str
    symbol: str
    bid: Decimal
    ask: Decimal
    ts_ms: int


@dataclass(frozen=True)
class BookLevel:
    price: Decimal
    size: Decimal


@dataclass
class Book:
    exchange: str
    symbol: str
    bids: List[BookLevel]   # sorted descending by price
    asks: List[BookLevel]   # sorted ascending by price
    ts_ms: int
    seq: int

    def best_bid(self) -> BookLevel:
        return self.bids[0]

    def best_ask(self) -> BookLevel:
        return self.asks[0]


@dataclass(frozen=True)
class Triangle:
    exchange: str
    anchor: str
    legs: Tuple[Tuple[str, LegSide], Tuple[str, LegSide], Tuple[str, LegSide]]

    def __post_init__(self):
        if len(self.legs) != 3:
            raise ValueError("triangle must have exactly 3 legs")

    @property
    def id(self) -> str:
        legs_str = "|".join(f"{sym}@{side.value}" for sym, side in self.legs)
        return f"{self.exchange}:{self.anchor}:{legs_str}"

    @property
    def symbols(self) -> List[str]:
        return [sym for sym, _ in self.legs]


@dataclass
class LiveStatus:
    ts: str
    confirmed: list
    candidates: list
    ws_subscriptions_per_exchange: dict
    triangle_count_per_exchange: dict
    last_tier1_poll_per_exchange: dict


@dataclass
class Opportunity:
    id: str
    triangle_id: str
    exchange: str
    anchor: str
    legs: Tuple[Tuple[str, LegSide], Tuple[str, LegSide], Tuple[str, LegSide]]
    opened_at: int
    closed_at: Optional[int] = None
    lifetime_ms: Optional[int] = None
    open_net_edge_pct: float = 0.0
    close_net_edge_pct: float = 0.0
    peak_net_edge_pct: float = 0.0
    peak_executable_profit_usd: float = 0.0
    peak_executable_size_usd: float = 0.0
    peak_at: Optional[int] = None
    bottleneck_leg_at_peak: Optional[int] = None  # 0, 1, or 2
    ws_update_count: int = 0
    median_book_age_ms: float = 0.0
    closed_reason: Optional[str] = None  # edge_decay | book_thinned | ws_disconnect | manual_stop
    snapshots: List[dict] = field(default_factory=list)
