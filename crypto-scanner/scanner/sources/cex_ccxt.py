from __future__ import annotations

import asyncio
import logging
import time

import ccxt.async_support as ccxt

from scanner.config import load_config
from scanner.models import PriceQuote, Token
from scanner.rate_limiter import get_limiter

logger = logging.getLogger(__name__)

# Exchanges to scan (all support public ticker without API keys)
EXCHANGE_CLASSES = {
    "gate": ccxt.gateio,
    "mexc": ccxt.mexc,
    "kucoin": ccxt.kucoin,
    "bybit": ccxt.bybit,
}


def _create_exchange(name: str, config: dict) -> ccxt.Exchange | None:
    """Create a CCXT exchange instance."""
    exchange_cls = EXCHANGE_CLASSES.get(name)
    if not exchange_cls:
        return None

    exchange_config = config.get(name, {})
    params = {"enableRateLimit": True}

    api_key = exchange_config.get("api_key", "")
    if api_key:
        params["apiKey"] = api_key
        params["secret"] = exchange_config.get("api_secret", "")
        if name == "kucoin":
            params["password"] = exchange_config.get("passphrase", "")

    return exchange_cls(params)


async def fetch_ticker(
    exchange: ccxt.Exchange,
    exchange_name: str,
    symbol: str,
) -> PriceQuote | None:
    """Fetch ticker for a symbol on a specific exchange."""
    limiter = get_limiter(f"ccxt_{exchange_name}")
    await limiter.acquire()

    pair = f"{symbol}/USDT"
    try:
        ticker = await exchange.fetch_ticker(pair)
    except (ccxt.BadSymbol, ccxt.ExchangeNotAvailable):
        return None
    except Exception as e:
        logger.debug(f"{exchange_name}: {pair} error: {e}")
        return None

    if not ticker or not ticker.get("last"):
        return None

    return PriceQuote(
        source=exchange_name,
        source_detail=f"{exchange_name}_spot",
        symbol=symbol,
        price_usd=float(ticker["last"]),
        bid=float(ticker["bid"]) if ticker.get("bid") else None,
        ask=float(ticker["ask"]) if ticker.get("ask") else None,
        volume_24h_usd=float(ticker.get("quoteVolume", 0) or 0),
        timestamp=time.time(),
    )


async def fetch_all_cex_prices(
    session,  # Not used by CCXT, but kept for interface consistency
    tokens: list[Token],
    config: dict | None = None,
) -> list[PriceQuote]:
    """Fetch prices for all tokens across all configured CEX exchanges."""
    if config is None:
        config = load_config()

    exchanges: dict[str, ccxt.Exchange] = {}
    for name in EXCHANGE_CLASSES:
        ex = _create_exchange(name, config)
        if ex:
            exchanges[name] = ex

    if not exchanges:
        return []

    quotes = []
    sem = asyncio.Semaphore(3)  # Limit concurrent exchange requests

    async def fetch_one(exchange_name: str, exchange: ccxt.Exchange, symbol: str):
        async with sem:
            quote = await fetch_ticker(exchange, exchange_name, symbol)
            if quote:
                quotes.append(quote)

    tasks = []
    for token in tokens:
        for name, exchange in exchanges.items():
            tasks.append(fetch_one(name, exchange, token.symbol))

    await asyncio.gather(*tasks, return_exceptions=True)

    # Close exchanges
    for exchange in exchanges.values():
        try:
            await exchange.close()
        except Exception:
            pass

    return quotes
