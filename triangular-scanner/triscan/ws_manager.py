from __future__ import annotations
import asyncio
import logging
from collections import defaultdict
from typing import Awaitable, Callable, Dict
from .models import Book
from .sources.base import Source

log = logging.getLogger(__name__)


class WsManager:
    """Reference-counted L2 book subscriptions for one Source.

    `acquire(symbol, on_update)` returns True on success, False if backpressure
    rejected the new subscription (cap hit). The same `on_update` callback is
    invoked for every subscriber of that symbol — the manager fan-outs.
    """

    def __init__(self, source: Source, max_subscriptions: int):
        self.source = source
        self.max_subscriptions = max_subscriptions
        self._refcount: Dict[str, int] = defaultdict(int)
        self._cancellers: Dict[str, Callable[[], Awaitable[None]]] = {}
        self._fanout: Dict[str, list] = defaultdict(list)
        self._lock = asyncio.Lock()

    def refcount(self, symbol: str) -> int:
        return self._refcount[symbol]

    async def acquire(self, symbol: str, on_update: Callable[[Book], Awaitable[None]]) -> bool:
        async with self._lock:
            if symbol in self._cancellers:
                self._refcount[symbol] += 1
                self._fanout[symbol].append(on_update)
                return True
            if len(self._cancellers) >= self.max_subscriptions:
                log.warning("ws cap reached on %s (%d) — refusing %s",
                            self.source.name, self.max_subscriptions, symbol)
                return False

            async def fanout(book: Book):
                for cb in list(self._fanout[symbol]):
                    try:
                        await cb(book)
                    except Exception as e:
                        log.warning("ws callback error %s/%s: %s", self.source.name, symbol, e)

            cancel = await self.source.subscribe_book(symbol, fanout)
            self._cancellers[symbol] = cancel
            self._refcount[symbol] = 1
            self._fanout[symbol] = [on_update]
            return True

    async def release(self, symbol: str) -> None:
        async with self._lock:
            if self._refcount[symbol] <= 0:
                return
            self._refcount[symbol] -= 1
            if self._refcount[symbol] == 0:
                cancel = self._cancellers.pop(symbol, None)
                self._fanout.pop(symbol, None)
                if cancel is not None:
                    try:
                        await cancel()
                    except Exception as e:
                        log.warning("cancel failed %s/%s: %s", self.source.name, symbol, e)
