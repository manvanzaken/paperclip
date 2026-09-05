from __future__ import annotations

import aiohttp

from scanner.models import PriceQuote
from scanner.rate_limiter import get_limiter

BASE_URL = "https://api.binance.com"


async def get_all_symbols(session: aiohttp.ClientSession) -> list[dict]:
    """Fetch exchange info to get all traded symbols."""
    limiter = get_limiter("binance")
    await limiter.acquire()
    async with session.get(f"{BASE_URL}/api/v3/exchangeInfo") as resp:
        data = await resp.json()
        return data.get("symbols", [])


async def get_book_tickers(
    session: aiohttp.ClientSession, symbols: list[str] | None = None
) -> list[dict]:
    """Fetch bid/ask for symbols. If None, fetches all."""
    limiter = get_limiter("binance")
    await limiter.acquire()
    params = {}
    if symbols:
        # Binance supports fetching individual or all
        if len(symbols) == 1:
            params["symbol"] = symbols[0]
        else:
            import json
            params["symbols"] = json.dumps(symbols)
    async with session.get(f"{BASE_URL}/api/v3/ticker/bookTicker", params=params) as resp:
        data = await resp.json()
        if isinstance(data, dict):
            return [data]
        return data


async def get_24h_tickers(
    session: aiohttp.ClientSession, symbols: list[str] | None = None
) -> list[dict]:
    """Fetch 24h stats (volume, price change) for symbols."""
    limiter = get_limiter("binance")
    await limiter.acquire()
    params = {"type": "MINI"}
    if symbols:
        if len(symbols) == 1:
            params["symbol"] = symbols[0]
        else:
            import json
            params["symbols"] = json.dumps(symbols)
    async with session.get(f"{BASE_URL}/api/v3/ticker/24hr", params=params) as resp:
        data = await resp.json()
        if isinstance(data, dict):
            return [data]
        return data


async def get_price(
    session: aiohttp.ClientSession, symbol: str
) -> float | None:
    """Get current price for a single symbol (e.g. BTCUSDT)."""
    limiter = get_limiter("binance")
    await limiter.acquire()
    async with session.get(f"{BASE_URL}/api/v3/ticker/price", params={"symbol": symbol}) as resp:
        if resp.status != 200:
            return None
        data = await resp.json()
        return float(data.get("price", 0))


async def fetch_prices(
    session: aiohttp.ClientSession,
    symbols: list[str],
    volume_data: dict[str, float] | None = None,
) -> list[PriceQuote]:
    """Fetch bid/ask prices for Binance symbols and return PriceQuotes.

    Args:
        symbols: Binance trading pair symbols like ["MYTOKENUSDT", "ANOTHERUSDT"]
        volume_data: Optional pre-fetched {symbol: volume_usd} map
    """
    if not symbols:
        return []

    book_data = await get_book_tickers(session, symbols)
    volume_map = volume_data or {}

    # If no volume data provided, fetch it
    if not volume_map:
        tickers_24h = await get_24h_tickers(session, symbols)
        for t in tickers_24h:
            sym = t.get("symbol", "")
            vol = float(t.get("quoteVolume", 0))
            volume_map[sym] = vol

    # Get USDT/USD rate (approximately 1:1, but we need BTC/ETH rates for conversion)
    btc_price = await get_price(session, "BTCUSDT")
    eth_price = await get_price(session, "ETHUSDT")

    quotes = []
    for item in book_data:
        sym = item.get("symbol", "")
        bid = float(item.get("bidPrice", 0))
        ask = float(item.get("askPrice", 0))

        if bid == 0 and ask == 0:
            continue

        # Convert to USD based on quote currency
        multiplier = 1.0
        if sym.endswith("BTC") and btc_price:
            multiplier = btc_price
        elif sym.endswith("ETH") and eth_price:
            multiplier = eth_price

        price_usd = ((bid + ask) / 2) * multiplier

        # Extract base symbol
        base = sym
        for suffix in ("USDT", "USDC", "BUSD", "BTC", "ETH", "FDUSD"):
            if sym.endswith(suffix):
                base = sym[: -len(suffix)]
                break

        quotes.append(
            PriceQuote(
                source="binance",
                source_detail="binance_spot",
                symbol=base,
                price_usd=price_usd,
                bid=bid * multiplier,
                ask=ask * multiplier,
                volume_24h_usd=volume_map.get(sym, 0),
                timestamp=__import__("time").time(),
                quote_currency=sym.replace(base, "") if base != sym else "USDT",
            )
        )

    return quotes
