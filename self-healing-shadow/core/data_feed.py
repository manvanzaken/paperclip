"""Native WebSocket clients for the five exchanges.

Each `WSClient` subclass owns one connection and one subscription. On
every message it normalises the payload into an `OrderBook`, stamps the
heartbeat, refreshes a shared `book` cache, and pushes (exchange, symbol)
onto a queue the spread evaluator consumes.

This module is NOT covered by unit tests (live WS — verified by manual
smoke run). Each subclass should stay small enough to read in one go;
that is the substitute for unit tests here.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Awaitable, Callable, Optional

import websockets

from .orderbook import OrderBook

log = logging.getLogger(__name__)

# Symbol on each exchange differs slightly from the canonical ccxt name.
# Map ccxt-style "BTC/USDT:USDT" to the per-exchange wire format.
_SYMBOL_MAP = {
    "binance": "btcusdt",
    "bybit":   "BTCUSDT",
    "bitget":  "BTCUSDT",
    "gateio":  "BTC_USDT",
    "mexc":    "BTC_USDT",
}


def wire_symbol(exchange: str, ccxt_symbol: str) -> str:
    """Convert "BTC/USDT:USDT" -> the exchange's wire symbol."""
    if ccxt_symbol != "BTC/USDT:USDT":
        # MVP only supports BTC/USDT perp.
        raise ValueError(f"unsupported symbol: {ccxt_symbol}")
    return _SYMBOL_MAP[exchange]


OnBookCallback = Callable[[str, str, OrderBook], Awaitable[None]]


class WSClient:
    """Base class. Subclasses implement endpoint, subscribe message,
    and parse_message."""

    name: str = "base"
    endpoint: str = ""

    def __init__(
        self,
        *,
        symbol: str,
        on_book: OnBookCallback,
        on_message: Callable[[str], None],
    ) -> None:
        self.symbol = symbol
        self._on_book = on_book
        self._on_message = on_message

    def subscribe_message(self) -> dict:
        raise NotImplementedError

    def parse(self, raw: dict) -> Optional[OrderBook]:
        raise NotImplementedError

    async def run(self) -> None:
        backoff = 1.0
        while True:
            try:
                async with websockets.connect(self.endpoint, ping_interval=20) as ws:
                    sub = self.subscribe_message()
                    if sub:
                        await ws.send(json.dumps(sub))
                    backoff = 1.0
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        ob = self.parse(msg)
                        if ob is None:
                            continue
                        self._on_message(self.name)
                        await self._on_book(self.name, self.symbol, ob)
            except (
                websockets.ConnectionClosed,
                OSError,
                asyncio.TimeoutError,
            ) as e:
                log.warning("[%s] WS dropped: %s — reconnect in %.1fs", self.name, e, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)


# ---------------------------------------------------------------------
# Per-exchange concrete clients.
# ---------------------------------------------------------------------


class BinanceWS(WSClient):
    name = "binance"
    endpoint = "wss://fstream.binance.com/stream"

    def subscribe_message(self) -> dict:
        sym = wire_symbol(self.name, self.symbol)
        return {
            "method": "SUBSCRIBE",
            "params": [f"{sym}@depth20@100ms"],
            "id": 1,
        }

    def parse(self, raw: dict) -> Optional[OrderBook]:
        data = raw.get("data") or raw
        bids = data.get("b") or data.get("bids")
        asks = data.get("a") or data.get("asks")
        if not bids or not asks:
            return None
        return OrderBook(
            bids=[(float(p), float(s)) for p, s in bids[:20]],
            asks=[(float(p), float(s)) for p, s in asks[:20]],
            ts=datetime.now(timezone.utc),
        )


class BybitWS(WSClient):
    name = "bybit"
    endpoint = "wss://stream.bybit.com/v5/public/linear"

    def subscribe_message(self) -> dict:
        sym = wire_symbol(self.name, self.symbol)
        return {"op": "subscribe", "args": [f"orderbook.50.{sym}"]}

    def parse(self, raw: dict) -> Optional[OrderBook]:
        topic = raw.get("topic", "")
        if not topic.startswith("orderbook"):
            return None
        data = raw.get("data") or {}
        bids = data.get("b")
        asks = data.get("a")
        if not bids or not asks:
            return None
        return OrderBook(
            bids=[(float(p), float(s)) for p, s in bids[:20]],
            asks=[(float(p), float(s)) for p, s in asks[:20]],
            ts=datetime.now(timezone.utc),
        )


class BitgetWS(WSClient):
    name = "bitget"
    endpoint = "wss://ws.bitget.com/v2/ws/public"

    def subscribe_message(self) -> dict:
        sym = wire_symbol(self.name, self.symbol)
        return {
            "op": "subscribe",
            "args": [{"instType": "USDT-FUTURES", "channel": "books", "instId": sym}],
        }

    def parse(self, raw: dict) -> Optional[OrderBook]:
        if raw.get("action") not in ("snapshot", "update"):
            return None
        data_list = raw.get("data") or []
        if not data_list:
            return None
        d = data_list[0]
        bids = d.get("bids")
        asks = d.get("asks")
        if not bids or not asks:
            return None
        return OrderBook(
            bids=[(float(p), float(s)) for p, s in bids[:20]],
            asks=[(float(p), float(s)) for p, s in asks[:20]],
            ts=datetime.now(timezone.utc),
        )


class GateIoWS(WSClient):
    name = "gateio"
    endpoint = "wss://fx-ws.gateio.ws/v4/ws/usdt"

    def subscribe_message(self) -> dict:
        sym = wire_symbol(self.name, self.symbol)
        return {
            "time": int(time.time()),
            "channel": "futures.order_book",
            "event": "subscribe",
            "payload": [sym, "20", "100ms"],
        }

    def parse(self, raw: dict) -> Optional[OrderBook]:
        if raw.get("channel") != "futures.order_book":
            return None
        result = raw.get("result")
        if not isinstance(result, dict):
            return None
        bids = result.get("bids")
        asks = result.get("asks")
        if not bids or not asks:
            return None
        return OrderBook(
            bids=[(float(b["p"]), float(b["s"])) for b in bids[:20]],
            asks=[(float(a["p"]), float(a["s"])) for a in asks[:20]],
            ts=datetime.now(timezone.utc),
        )


class MexcWS(WSClient):
    name = "mexc"
    endpoint = "wss://contract.mexc.com/edge"

    def subscribe_message(self) -> dict:
        sym = wire_symbol(self.name, self.symbol)
        return {
            "method": "sub.depth",
            "param": {"symbol": sym},
        }

    def parse(self, raw: dict) -> Optional[OrderBook]:
        if raw.get("channel") != "push.depth":
            return None
        data = raw.get("data") or {}
        bids = data.get("bids")
        asks = data.get("asks")
        if not bids or not asks:
            return None
        return OrderBook(
            bids=[(float(p), float(s)) for p, s, *_ in bids[:20]],
            asks=[(float(p), float(s)) for p, s, *_ in asks[:20]],
            ts=datetime.now(timezone.utc),
        )


WS_CLIENTS: dict[str, type[WSClient]] = {
    "binance": BinanceWS,
    "bybit":   BybitWS,
    "bitget":  BitgetWS,
    "gateio":  GateIoWS,
    "mexc":    MexcWS,
}
