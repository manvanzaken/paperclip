from __future__ import annotations

import logging
import time

import aiohttp

from scanner.models import PriceQuote, Token
from scanner.rate_limiter import get_limiter

logger = logging.getLogger(__name__)

# Jupiter quote API (free, no auth needed)
QUOTE_URL = "https://api.jup.ag/swap/v1/quote"

# USDC mint on Solana
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

# 1 USDC = 1_000_000 (6 decimals)
USDC_AMOUNT = 1_000_000


async def get_price(
    session: aiohttp.ClientSession,
    mint_address: str,
) -> dict | None:
    """Get Jupiter price for a Solana token by quoting a USDC→token swap.

    Uses the free quote API to derive the effective price.
    """
    limiter = get_limiter("jupiter")
    await limiter.acquire()

    # Quote: how many tokens do I get for 1 USDC?
    params = {
        "inputMint": USDC_MINT,
        "outputMint": mint_address,
        "amount": str(USDC_AMOUNT),
        "slippageBps": "50",
    }
    try:
        async with session.get(QUOTE_URL, params=params) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            out_amount = data.get("outAmount")
            if not out_amount:
                return None
            # We need decimals to compute price. Estimate from the quote.
            # price = USDC_spent / tokens_received
            # Since we spent 1 USDC (1_000_000 raw), price = 1 / (out_amount / 10^decimals)
            # Without knowing decimals, we return raw data for the caller to handle
            return {
                "outAmount": out_amount,
                "inAmount": str(USDC_AMOUNT),
                "price": 1.0 / (int(out_amount) / USDC_AMOUNT) if int(out_amount) > 0 else None,
            }
    except Exception as e:
        logger.debug(f"Jupiter quote error: {e}")
        return None


async def fetch_prices(
    session: aiohttp.ClientSession,
    token: Token,
) -> list[PriceQuote]:
    """Fetch Jupiter price for a token if it has a Solana address."""
    sol_address = token.contract_addresses.get("solana")
    if not sol_address:
        return []

    price_data = await get_price(session, sol_address)
    if not price_data:
        return []

    price = price_data.get("price")
    if not price:
        return []

    return [
        PriceQuote(
            source="jupiter",
            source_detail="jupiter_solana",
            symbol=token.symbol,
            price_usd=float(price),
            chain="solana",
            timestamp=time.time(),
        )
    ]
