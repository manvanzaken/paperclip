#!/usr/bin/env python3
"""Run a single scan cycle: fetch prices, compare, and display opportunities.

Usage:
    python scan_once.py                    # Scan all cached tokens
    python scan_once.py --min-spread 2.0   # Only show 2%+ net spread
    python scan_once.py --top 10           # Show top 10 only
"""

import argparse
import asyncio
import logging
import sys

import aiohttp

from scanner.alerts.console import display
from scanner.config import load_config
from scanner.discovery import load_tokens
from scanner.opportunity import rank_opportunities
from scanner.price_engine import fetch_all_prices, find_opportunities

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


async def main():
    parser = argparse.ArgumentParser(description="Scan for price inefficiencies")
    parser.add_argument("--min-spread", type=float, default=None, help="Min net spread percent")
    parser.add_argument("--top", type=int, default=20, help="Show top N opportunities")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    config = load_config()
    if args.min_spread is not None:
        config["min_spread_pct"] = args.min_spread

    # Load cached tokens
    tokens = load_tokens()
    if not tokens:
        logger.error(
            "No tokens found. Run 'python discover_tokens.py' first to build tokens.json"
        )
        sys.exit(1)

    logger.info(f"Scanning {len(tokens)} tokens...")

    async with aiohttp.ClientSession() as session:
        # Fetch prices from all sources
        prices = await fetch_all_prices(session, tokens)

        tokens_with_prices = sum(1 for v in prices.values() if len(v) >= 2)
        total_quotes = sum(len(v) for v in prices.values())
        logger.info(f"Fetched {total_quotes} quotes, {tokens_with_prices} tokens with 2+ sources")

        # Find and rank opportunities
        opportunities = find_opportunities(prices, tokens, config)
        ranked = rank_opportunities(opportunities)

        # Display
        display(ranked[: args.top])

        if not ranked:
            logger.info(
                f"No opportunities found above {config['min_spread_pct']}% net spread. "
                "Try lowering --min-spread or running discover_tokens.py to add more tokens."
            )


if __name__ == "__main__":
    asyncio.run(main())
