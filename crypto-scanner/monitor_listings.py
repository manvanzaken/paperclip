#!/usr/bin/env python3
"""Monitor Binance announcements for new Alpha token listings.

Polls the Binance announcement API every N seconds and alerts when
new listing announcements are detected. Optionally auto-discovers
tokens and runs an immediate price scan.

Usage:
    python monitor_listings.py                     # Poll every 60s, Alpha only
    python monitor_listings.py --interval 30       # Poll every 30s
    python monitor_listings.py --all-listings      # Include non-Alpha listings
    python monitor_listings.py --auto-scan         # Auto-scan new tokens
    python monitor_listings.py --show-recent       # Show recent announcements and exit
"""

import argparse
import asyncio
import logging
import signal
import sys

import aiohttp
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from scanner.announcements import (
    check_new_announcements,
    get_all_recent_announcements,
)
from scanner.config import load_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)
console = Console()

_running = True


def _handle_signal(sig, frame):
    global _running
    _running = False


async def show_recent():
    """Display recent Binance announcements."""
    async with aiohttp.ClientSession() as session:
        announcements = await get_all_recent_announcements(session)

    table = Table(title="Recent Binance Listing Announcements")
    table.add_column("Alpha", width=5, justify="center")
    table.add_column("Symbols", width=15, style="cyan")
    table.add_column("Title", width=70)

    for ann in announcements:
        alpha_mark = "[green]YES[/green]" if ann.is_alpha else "[dim]no[/dim]"
        symbols = ", ".join(ann.symbols) if ann.symbols else "[dim]-[/dim]"
        table.add_row(alpha_mark, symbols, ann.title)

    console.print(table)


async def auto_scan_tokens(symbols: list[str], config: dict):
    """Auto-discover and scan newly listed tokens."""
    from scanner.discovery import discover_tokens, save_tokens, load_tokens
    from scanner.price_engine import fetch_all_prices, find_opportunities
    from scanner.opportunity import rank_opportunities
    from scanner.alerts.console import display

    logger.info(f"Auto-scanning new tokens: {', '.join(symbols)}")

    async with aiohttp.ClientSession() as session:
        # Discover and enrich
        new_tokens = await discover_tokens(session, symbols_override=symbols)
        if not new_tokens:
            logger.warning("Could not enrich any of the new tokens")
            return

        # Merge with existing cache
        existing = load_tokens()
        existing_symbols = {t.symbol for t in existing}
        for t in new_tokens:
            if t.symbol not in existing_symbols:
                existing.append(t)
        save_tokens(existing)

        # Scan just the new tokens
        prices = await fetch_all_prices(session, new_tokens)
        opportunities = find_opportunities(prices, new_tokens, config)
        ranked = rank_opportunities(opportunities)
        display(ranked, title="NEW LISTING SCAN")


async def send_listing_alert(symbols: list[str], title: str, config: dict):
    """Send alerts for new listings via configured channels."""
    # Telegram
    tg = config.get("telegram", {})
    if tg.get("enabled") and tg.get("bot_token"):
        try:
            import aiohttp as _aiohttp
            message = (
                f"*NEW BINANCE ALPHA LISTING*\n\n"
                f"*Tokens:* {', '.join(symbols)}\n"
                f"*Title:* {title}\n\n"
                f"ACT NOW — check DEX prices!"
            )
            async with _aiohttp.ClientSession() as s:
                url = f"https://api.telegram.org/bot{tg['bot_token']}/sendMessage"
                await s.post(url, json={
                    "chat_id": tg["chat_id"],
                    "text": message,
                    "parse_mode": "Markdown",
                })
        except Exception as e:
            logger.warning(f"Telegram alert failed: {e}")

    # Discord
    dc = config.get("discord", {})
    if dc.get("enabled") and dc.get("webhook_url"):
        try:
            import aiohttp as _aiohttp
            async with _aiohttp.ClientSession() as s:
                await s.post(dc["webhook_url"], json={
                    "username": "Listing Monitor",
                    "embeds": [{
                        "title": "NEW BINANCE ALPHA LISTING",
                        "description": f"**Tokens:** {', '.join(symbols)}\n{title}",
                        "color": 0xFF0000,
                    }],
                })
        except Exception as e:
            logger.warning(f"Discord alert failed: {e}")


async def monitor_loop(
    interval: int,
    alpha_only: bool,
    auto_scan: bool,
    config: dict,
):
    """Main monitoring loop."""
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    console.print(Panel(
        f"Monitoring Binance announcements every {interval}s\n"
        f"Filter: {'Alpha only' if alpha_only else 'All listings'}\n"
        f"Auto-scan: {'enabled' if auto_scan else 'disabled'}\n"
        f"Press Ctrl+C to stop",
        title="Listing Monitor",
        border_style="green",
    ))

    # First run: seed the seen list so we don't alert on old announcements
    async with aiohttp.ClientSession() as session:
        initial = await check_new_announcements(session, alpha_only=False)
        if initial:
            logger.info(f"Seeded {len(initial)} existing announcements (won't re-alert)")

    while _running:
        try:
            async with aiohttp.ClientSession() as session:
                new = await check_new_announcements(session, alpha_only=alpha_only)

            if new:
                for ann in new:
                    console.print()
                    console.print(Panel(
                        f"[bold red]NEW LISTING DETECTED[/bold red]\n\n"
                        f"[bold]{ann.title}[/bold]\n\n"
                        f"Symbols: [cyan]{', '.join(ann.symbols)}[/cyan]\n"
                        f"Alpha: {'Yes' if ann.is_alpha else 'No'}\n"
                        f"URL: {ann.url}",
                        title="ALERT",
                        border_style="red",
                    ))

                    # Send external alerts
                    await send_listing_alert(ann.symbols, ann.title, config)

                    # Auto-scan if enabled
                    if auto_scan and ann.symbols:
                        await auto_scan_tokens(ann.symbols, config)

        except Exception as e:
            logger.error(f"Monitor error: {e}")

        if _running:
            await asyncio.sleep(interval)


async def main():
    parser = argparse.ArgumentParser(description="Monitor Binance Alpha listings")
    parser.add_argument("--interval", type=int, default=60, help="Poll interval in seconds")
    parser.add_argument("--all-listings", action="store_true", help="Include non-Alpha listings")
    parser.add_argument("--auto-scan", action="store_true", help="Auto-scan newly listed tokens")
    parser.add_argument("--show-recent", action="store_true", help="Show recent announcements and exit")
    args = parser.parse_args()

    config = load_config()

    if args.show_recent:
        await show_recent()
        return

    await monitor_loop(
        interval=args.interval,
        alpha_only=not args.all_listings,
        auto_scan=args.auto_scan,
        config=config,
    )


if __name__ == "__main__":
    asyncio.run(main())
