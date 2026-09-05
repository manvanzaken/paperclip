#!/usr/bin/env python3
"""Discover Binance Alpha tokens and map them to contract addresses.

Run this first (or periodically) to build/refresh tokens.json.
The main scanner reads from this cache.

Usage:
    python discover_tokens.py                      # Auto-discover from Binance
    python discover_tokens.py --symbols ACE,PIXEL  # Discover specific tokens
    python discover_tokens.py --max 20             # Limit to 20 tokens
"""

import argparse
import asyncio
import logging
import sys

import aiohttp

from scanner.config import load_config
from scanner.discovery import discover_tokens, save_tokens

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


async def main():
    parser = argparse.ArgumentParser(description="Discover Binance Alpha tokens")
    parser.add_argument(
        "--symbols",
        type=str,
        help="Comma-separated list of symbols to discover (e.g. ACE,PIXEL,MANTA)",
    )
    parser.add_argument(
        "--max",
        type=int,
        default=None,
        help="Maximum tokens to process (default: from config)",
    )
    args = parser.parse_args()

    config = load_config()
    max_tokens = args.max or config.get("max_tokens", 50)

    symbols_override = None
    if args.symbols:
        symbols_override = [s.strip().upper() for s in args.symbols.split(",")]

    async with aiohttp.ClientSession() as session:
        tokens = await discover_tokens(
            session,
            max_tokens=max_tokens,
            symbols_override=symbols_override,
        )

        if not tokens:
            logger.warning("No tokens discovered. Check your network or try --symbols.")
            sys.exit(1)

        save_tokens(tokens)
        logger.info(f"\nDone! {len(tokens)} tokens saved to tokens.json")
        logger.info("You can now run: python scan_once.py")


if __name__ == "__main__":
    asyncio.run(main())
