from __future__ import annotations

import asyncio
import logging
import time

import aiohttp

from scanner.models import FeeEstimate, Opportunity, PriceQuote, Token
from scanner.fee_estimator import estimate_fees
from scanner.sources import binance, dexscreener

logger = logging.getLogger(__name__)


async def fetch_all_prices(
    session: aiohttp.ClientSession,
    tokens: list[Token],
) -> dict[str, list[PriceQuote]]:
    """Fetch prices for all tokens from all sources concurrently.

    Returns: {symbol: [PriceQuote, ...]}
    """
    result: dict[str, list[PriceQuote]] = {}

    # 1. Batch-fetch Binance prices for all tokens at once
    binance_symbols = [t.binance_symbol for t in tokens if t.binance_symbol]
    try:
        binance_quotes = await binance.fetch_prices(session, binance_symbols)
        for q in binance_quotes:
            result.setdefault(q.symbol, []).append(q)
    except Exception as e:
        logger.warning(f"Binance fetch failed: {e}")

    # 2. Fetch DEX prices per token (with concurrency limit)
    sem = asyncio.Semaphore(5)  # Limit concurrent DEX requests

    async def fetch_dex(token: Token):
        async with sem:
            try:
                quotes = await dexscreener.fetch_prices(
                    session, token.symbol, token.contract_addresses
                )
                for q in quotes:
                    result.setdefault(q.symbol, []).append(q)
            except Exception as e:
                logger.debug(f"DexScreener fetch failed for {token.symbol}: {e}")

    # 3. Fetch CEX prices via CCXT if available
    try:
        from scanner.sources.cex_ccxt import fetch_all_cex_prices
        cex_quotes = await fetch_all_cex_prices(session, tokens)
        for q in cex_quotes:
            result.setdefault(q.symbol, []).append(q)
    except ImportError:
        pass  # CCXT source not yet implemented
    except Exception as e:
        logger.debug(f"CCXT fetch failed: {e}")

    # 4. Fetch Jupiter prices for Solana tokens
    try:
        from scanner.sources.jupiter import fetch_prices as jupiter_fetch
        solana_tokens = [t for t in tokens if "solana" in t.contract_addresses]
        for token in solana_tokens:
            async with sem:
                try:
                    quotes = await jupiter_fetch(session, token)
                    for q in quotes:
                        result.setdefault(q.symbol, []).append(q)
                except Exception as e:
                    logger.debug(f"Jupiter fetch failed for {token.symbol}: {e}")
    except ImportError:
        pass  # Jupiter source not yet implemented

    await asyncio.gather(*[fetch_dex(t) for t in tokens if t.contract_addresses])

    return result


def find_opportunities(
    prices: dict[str, list[PriceQuote]],
    tokens: list[Token],
    config: dict,
) -> list[Opportunity]:
    """Compare all price pairs for each token and find arbitrage spreads.

    Only returns opportunities where net spread exceeds min_spread_pct.
    """
    min_spread = config.get("min_spread_pct", 1.5)
    min_liquidity = config.get("min_liquidity_usd", 5000)
    opportunities = []

    token_map = {t.symbol: t for t in tokens}

    for symbol, quotes in prices.items():
        if len(quotes) < 2:
            continue

        token = token_map.get(symbol)
        if not token:
            continue

        # Compare every pair of quotes
        for i, q1 in enumerate(quotes):
            for q2 in quotes[i + 1:]:
                # Determine buy/sell direction
                buy_price = q1.effective_buy_price
                sell_price = q2.effective_sell_price

                if buy_price <= 0 or sell_price <= 0:
                    continue

                # Try both directions
                for buy_q, sell_q in [(q1, q2), (q2, q1)]:
                    bp = buy_q.effective_buy_price
                    sp = sell_q.effective_sell_price

                    if bp <= 0 or sp <= 0 or sp <= bp:
                        continue

                    gross_spread_pct = ((sp - bp) / bp) * 100

                    # Check liquidity on both sides
                    buy_liq = buy_q.liquidity_usd or buy_q.volume_24h_usd
                    sell_liq = sell_q.liquidity_usd or sell_q.volume_24h_usd
                    min_side_liq = min(buy_liq, sell_liq) if buy_liq and sell_liq else 0

                    if min_side_liq < min_liquidity and min_side_liq > 0:
                        continue

                    # Estimate fees
                    fees = estimate_fees(buy_q, sell_q, config)
                    net_spread_pct = gross_spread_pct - fees.total_cost_pct

                    if net_spread_pct < min_spread:
                        continue

                    # Recommended trade size: 10% of min-side liquidity, capped at $10k
                    rec_size = min(min_side_liq * 0.1, 10000) if min_side_liq else 0

                    opportunities.append(
                        Opportunity(
                            token=token,
                            buy_quote=buy_q,
                            sell_quote=sell_q,
                            gross_spread_pct=round(gross_spread_pct, 2),
                            fees=fees,
                            net_spread_pct=round(net_spread_pct, 2),
                            recommended_size_usd=round(rec_size, 2),
                            timestamp=time.time(),
                        )
                    )

    return opportunities
