from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
from decimal import Decimal
from typing import Awaitable, Callable, Dict, Iterable, List, Optional

from ..models import Quote, Book
from ..enumerator import Market


@dataclass
class Source(ABC):
    name: str
    taker_fee_pct: float
    rest_url: str
    ws_url: str

    @abstractmethod
    async def fetch_markets(self) -> List[Market]:
        """Return current spot markets (with 24h quote volume in USD)."""

    @abstractmethod
    async def fetch_tickers(self, symbols: Iterable[str]) -> Dict[str, Quote]:
        """Return latest bid/ask for each symbol."""

    @abstractmethod
    async def subscribe_book(
        self,
        symbol: str,
        on_update: Callable[[Book], Awaitable[None]],
        on_failure: Optional[Callable[[], Awaitable[None]]] = None,
    ) -> Callable[[], Awaitable[None]]:
        """Subscribe to L2 book updates for `symbol`. Returns a cancel coroutine."""

    @abstractmethod
    async def close(self) -> None:
        """Release any held resources."""
