from __future__ import annotations

import json
import logging
from pathlib import Path

import aiohttp

from scanner.config import TOKENS_CACHE_PATH
from scanner.models import Token
from scanner.sources import binance, coingecko, dexscreener

logger = logging.getLogger(__name__)


async def discover_from_binance(session: aiohttp.ClientSession) -> list[str]:
    """Get token symbols from Binance that are likely Alpha tokens.

    Binance Alpha tokens are typically newer, smaller-cap listings.
    We identify them by looking at all USDT pairs and filtering for
    tokens that are not in the major/established list.
    """
    exchange_info = await binance.get_all_symbols(session)

    # Collect all base assets that have USDT pairs and are trading
    symbols = set()
    for s in exchange_info:
        if (
            s.get("status") == "TRADING"
            and s.get("quoteAsset") == "USDT"
            and s.get("isSpotTradingAllowed", False)
        ):
            symbols.add(s["baseAsset"])

    # Filter out well-known majors (these are not "Alpha" tokens)
    majors = {
        "BTC", "ETH", "BNB", "SOL", "XRP", "ADA", "DOGE", "DOT", "AVAX",
        "LINK", "MATIC", "UNI", "SHIB", "LTC", "ATOM", "FIL", "APT",
        "NEAR", "OP", "ARB", "SUI", "SEI", "TIA", "JUP", "WIF",
        "PEPE", "FLOKI", "BONK", "INJ", "TRX", "ETC", "BCH", "ALGO",
        "FTM", "MANA", "SAND", "AXS", "GALA", "ENJ", "IMX", "RNDR",
        "FET", "AGIX", "OCEAN", "ROSE", "HBAR", "VET", "ICP", "EGLD",
        "XLM", "EOS", "NEO", "AAVE", "MKR", "SNX", "CRV", "COMP",
        "YFI", "SUSHI", "1INCH", "BAL", "LDO", "RPL", "SSV", "PENDLE",
        "GMX", "DYDX", "STX", "ORDI", "RUNE", "OSMO", "PYTH",
        # Stablecoins
        "USDT", "USDC", "BUSD", "DAI", "TUSD", "FDUSD", "USDP",
    }

    alpha_candidates = symbols - majors
    return sorted(alpha_candidates)


async def enrich_token(
    session: aiohttp.ClientSession, symbol: str
) -> Token | None:
    """Look up a symbol on CoinGecko and DexScreener to build a Token object."""
    # Find CoinGecko ID
    cg_id = await coingecko.find_coingecko_id(session, symbol)
    if not cg_id:
        logger.debug(f"No CoinGecko match for {symbol}")
        return None

    # Get contract addresses
    addresses = await coingecko.get_contract_addresses(session, cg_id)

    # Verify at least one address has DEX liquidity via DexScreener
    verified_addresses = {}
    for chain, addr in addresses.items():
        pairs = await dexscreener.get_token_pairs(session, addr)
        if pairs:
            verified_addresses[chain] = addr

    if not verified_addresses:
        logger.debug(f"{symbol} ({cg_id}): no DEX pairs found")
        # Still include it — it may have cross-CEX arb opportunities
        verified_addresses = addresses

    return Token(
        symbol=symbol,
        name=symbol,  # Could fetch from CoinGecko but not critical
        binance_symbol=f"{symbol}USDT",
        contract_addresses=verified_addresses,
        coingecko_id=cg_id,
        cex_listings=["binance"],
    )


async def discover_tokens(
    session: aiohttp.ClientSession,
    max_tokens: int = 50,
    symbols_override: list[str] | None = None,
) -> list[Token]:
    """Full discovery pipeline: find Alpha tokens and enrich with addresses.

    Args:
        session: aiohttp session
        max_tokens: Maximum tokens to process (CoinGecko rate limits)
        symbols_override: If provided, use these symbols instead of auto-discovery
    """
    if symbols_override:
        symbols = symbols_override
    else:
        symbols = await discover_from_binance(session)
        logger.info(f"Found {len(symbols)} candidate Alpha tokens on Binance")

    # Limit to avoid rate limit exhaustion (CoinGecko is 10 req/min)
    symbols = symbols[:max_tokens]

    tokens = []
    for i, symbol in enumerate(symbols):
        logger.info(f"Enriching {symbol} ({i+1}/{len(symbols)})...")
        try:
            token = await enrich_token(session, symbol)
            if token:
                tokens.append(token)
                logger.info(
                    f"  ✓ {symbol}: {len(token.contract_addresses)} chains, "
                    f"CoinGecko ID: {token.coingecko_id}"
                )
            else:
                logger.info(f"  ✗ {symbol}: not found on CoinGecko")
        except Exception as e:
            logger.warning(f"  ✗ {symbol}: error - {e}")

    logger.info(f"Discovery complete: {len(tokens)}/{len(symbols)} tokens enriched")
    return tokens


def save_tokens(tokens: list[Token], path: Path | None = None):
    """Cache discovered tokens to disk."""
    cache_path = path or TOKENS_CACHE_PATH
    data = []
    for t in tokens:
        data.append({
            "symbol": t.symbol,
            "name": t.name,
            "binance_symbol": t.binance_symbol,
            "contract_addresses": t.contract_addresses,
            "coingecko_id": t.coingecko_id,
            "cex_listings": t.cex_listings,
        })
    with open(cache_path, "w") as f:
        json.dump(data, f, indent=2)
    logger.info(f"Saved {len(tokens)} tokens to {cache_path}")


def load_tokens(path: Path | None = None) -> list[Token]:
    """Load cached tokens from disk."""
    cache_path = path or TOKENS_CACHE_PATH
    if not cache_path.exists():
        return []
    with open(cache_path) as f:
        data = json.load(f)
    return [
        Token(
            symbol=d["symbol"],
            name=d.get("name", d["symbol"]),
            binance_symbol=d.get("binance_symbol"),
            contract_addresses=d.get("contract_addresses", {}),
            coingecko_id=d.get("coingecko_id"),
            cex_listings=d.get("cex_listings", ["binance"]),
        )
        for d in data
    ]
