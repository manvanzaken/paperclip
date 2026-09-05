from __future__ import annotations

import aiohttp

from scanner.models import PriceQuote
from scanner.rate_limiter import get_limiter

BASE_URL = "https://api.dexscreener.com"

# Map DexScreener chain IDs to our normalized names
CHAIN_MAP = {
    "ethereum": "ethereum",
    "bsc": "bsc",
    "solana": "solana",
    "base": "base",
    "arbitrum": "arbitrum",
    "polygon": "polygon",
    "avalanche": "avalanche",
    "optimism": "optimism",
}


async def search_token(
    session: aiohttp.ClientSession, query: str
) -> list[dict]:
    """Search DexScreener for a token by name or symbol."""
    limiter = get_limiter("dexscreener")
    await limiter.acquire()
    async with session.get(f"{BASE_URL}/latest/dex/search", params={"q": query}) as resp:
        if resp.status != 200:
            return []
        data = await resp.json()
        return data.get("pairs", [])


async def get_token_pairs(
    session: aiohttp.ClientSession, address: str
) -> list[dict]:
    """Get all DEX pairs for a token by contract address."""
    limiter = get_limiter("dexscreener")
    await limiter.acquire()
    async with session.get(f"{BASE_URL}/tokens/v1/{address}") as resp:
        if resp.status != 200:
            return []
        data = await resp.json()
        if isinstance(data, list):
            return data
        return data.get("pairs", []) if isinstance(data, dict) else []


async def get_pairs_by_chain(
    session: aiohttp.ClientSession, chain: str, address: str
) -> list[dict]:
    """Get DEX pairs for a token on a specific chain."""
    limiter = get_limiter("dexscreener")
    await limiter.acquire()
    async with session.get(f"{BASE_URL}/latest/dex/tokens/{address}") as resp:
        if resp.status != 200:
            return []
        data = await resp.json()
        pairs = data.get("pairs", [])
        return [p for p in pairs if p.get("chainId") == chain]


async def fetch_prices(
    session: aiohttp.ClientSession,
    symbol: str,
    contract_addresses: dict[str, str],
) -> list[PriceQuote]:
    """Fetch DEX prices for a token across all chains where it has known addresses.

    Args:
        symbol: Token symbol (e.g. "MYTOKEN")
        contract_addresses: {"ethereum": "0x...", "bsc": "0x...", "solana": "..."}
    """
    quotes = []

    for chain, address in contract_addresses.items():
        try:
            pairs = await get_token_pairs(session, address)
        except Exception:
            continue

        if not pairs:
            # Try chain-specific endpoint
            try:
                pairs = await get_pairs_by_chain(session, chain, address)
            except Exception:
                continue

        # Pick the highest-liquidity pair per chain
        best_pair = None
        best_liquidity = 0
        for pair in pairs:
            liq = pair.get("liquidity", {}).get("usd", 0) or 0
            if liq > best_liquidity:
                best_liquidity = liq
                best_pair = pair

        if best_pair and best_pair.get("priceUsd"):
            price = float(best_pair["priceUsd"])
            volume = float(best_pair.get("volume", {}).get("h24", 0) or 0)
            liquidity = float(best_pair.get("liquidity", {}).get("usd", 0) or 0)
            chain_id = best_pair.get("chainId", chain)
            dex_name = best_pair.get("dexId", "unknown")

            quotes.append(
                PriceQuote(
                    source="dexscreener",
                    source_detail=f"dexscreener_{chain_id}_{dex_name}",
                    symbol=symbol,
                    price_usd=price,
                    volume_24h_usd=volume,
                    liquidity_usd=liquidity,
                    chain=chain_id,
                    pair_address=best_pair.get("pairAddress"),
                    timestamp=__import__("time").time(),
                )
            )

    return quotes
