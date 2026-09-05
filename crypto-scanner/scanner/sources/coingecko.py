from __future__ import annotations

import aiohttp

from scanner.rate_limiter import get_limiter

BASE_URL = "https://api.coingecko.com/api/v3"

# CoinGecko platform IDs to our chain names
PLATFORM_MAP = {
    "ethereum": "ethereum",
    "binance-smart-chain": "bsc",
    "solana": "solana",
    "base": "base",
    "arbitrum-one": "arbitrum",
    "polygon-pos": "polygon",
    "avalanche": "avalanche",
    "optimistic-ethereum": "optimism",
}


async def search_token(
    session: aiohttp.ClientSession, query: str
) -> list[dict]:
    """Search CoinGecko for a token. Returns list of {id, name, symbol, ...}."""
    limiter = get_limiter("coingecko")
    await limiter.acquire()
    async with session.get(f"{BASE_URL}/search", params={"query": query}) as resp:
        if resp.status != 200:
            return []
        data = await resp.json()
        return data.get("coins", [])


async def get_coin_info(
    session: aiohttp.ClientSession, coingecko_id: str
) -> dict | None:
    """Get detailed coin info including contract addresses on all platforms."""
    limiter = get_limiter("coingecko")
    await limiter.acquire()
    params = {
        "localization": "false",
        "tickers": "false",
        "market_data": "false",
        "community_data": "false",
        "developer_data": "false",
    }
    async with session.get(f"{BASE_URL}/coins/{coingecko_id}", params=params) as resp:
        if resp.status != 200:
            return None
        return await resp.json()


async def get_contract_addresses(
    session: aiohttp.ClientSession, coingecko_id: str
) -> dict[str, str]:
    """Get contract addresses for a token across all chains.

    Returns: {"ethereum": "0x...", "bsc": "0x...", "solana": "..."}
    """
    info = await get_coin_info(session, coingecko_id)
    if not info:
        return {}

    platforms = info.get("platforms", {})
    detail_platforms = info.get("detail_platforms", {})

    addresses = {}
    for platform_id, address in platforms.items():
        if not address:
            continue
        chain = PLATFORM_MAP.get(platform_id)
        if chain:
            addresses[chain] = address

    # Also check detail_platforms for more info
    for platform_id, detail in detail_platforms.items():
        if not detail:
            continue
        address = detail.get("contract_address", "")
        if not address:
            continue
        chain = PLATFORM_MAP.get(platform_id)
        if chain and chain not in addresses:
            addresses[chain] = address

    return addresses


async def find_coingecko_id(
    session: aiohttp.ClientSession, symbol: str, name: str = ""
) -> str | None:
    """Try to find the CoinGecko ID for a token by symbol/name.

    Returns the best matching coingecko_id or None.
    """
    results = await search_token(session, symbol)
    if not results:
        return None

    # Exact symbol match preferred
    symbol_lower = symbol.lower()
    for coin in results:
        if coin.get("symbol", "").lower() == symbol_lower:
            return coin["id"]

    # If name is provided, try name match
    if name:
        name_lower = name.lower()
        for coin in results:
            if name_lower in coin.get("name", "").lower():
                return coin["id"]

    # Fall back to first result
    return results[0]["id"] if results else None
