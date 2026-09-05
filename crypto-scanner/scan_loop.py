#!/usr/bin/env python3
"""Continuously scan for price inefficiencies with adaptive frequency.

Features:
  - Volatility detection: automatically speeds up scanning during price spikes
  - Inventory awareness: sizes opportunities based on your actual holdings
  - Rebalancing alerts: warns when a venue is running low
  - Multi-channel alerts: console, Telegram, Discord

Usage:
    python scan_loop.py                          # Default settings
    python scan_loop.py --interval 15            # Base interval 15s
    python scan_loop.py --min-spread 2.0         # Only show 2%+ net spread
    python scan_loop.py --no-volatility          # Disable adaptive frequency
    python scan_loop.py --no-inventory           # Disable inventory tracking
"""

import argparse
import asyncio
import logging
import signal
import sys

import aiohttp
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from scanner.alerts.console import display
from scanner.config import load_config
from scanner.discovery import load_tokens
from scanner.opportunity import rank_opportunities
from scanner.price_engine import fetch_all_prices, find_opportunities
from scanner.volatility import VolatilityTracker
from scanner.inventory import InventoryManager

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
    logger.info("\nShutting down...")
    _running = False


def display_volatility_signals(signals, tracker):
    """Display volatility alerts."""
    if not signals:
        return

    for sig in signals:
        style = {
            "price_spike": "bold red",
            "volume_spike": "bold yellow",
            "spread_widening": "bold green",
        }.get(sig.signal_type, "bold")

        console.print(f"  [{style}]VOLATILITY[/{style}] {sig.detail}")

    if tracker.is_fast_mode:
        console.print(
            f"  [bold cyan]FAST MODE[/bold cyan] active — "
            f"scanning every {tracker.fast_interval}s "
            f"({tracker.cycles_remaining_fast} cycles remaining)"
        )


def display_inventory_status(inventory, ranked):
    """Display inventory-aware sizing for top opportunities."""
    if not ranked:
        return

    sized = [inventory.size_opportunity(opp) for opp in ranked[:5]]
    executable = [s for s in sized if s.executable_size_usd > 0]

    if executable:
        console.print("\n  [bold]Executable (with your inventory):[/bold]")
        for s in executable:
            opp = s.opportunity
            limit_note = ""
            if s.limited_by == "buy_side":
                limit_note = f" [dim](limited by {opp.buy_quote.source_detail} USDT)[/dim]"
            elif s.limited_by == "sell_side":
                limit_note = f" [dim](limited by {opp.sell_quote.source_detail} {opp.token.symbol})[/dim]"

            console.print(
                f"    {opp.token.symbol}: up to "
                f"[green]${s.executable_size_usd:,.0f}[/green] "
                f"@ {opp.net_spread_pct:.2f}% net{limit_note}"
            )
    else:
        console.print(
            "\n  [dim]No opportunities executable with current inventory. "
            "Pre-position tokens on both sides to enable instant arb.[/dim]"
        )

    # Rebalancing alerts
    rebalance = inventory.check_rebalancing()
    if rebalance:
        console.print("\n  [bold yellow]Rebalancing needed:[/bold yellow]")
        for alert in rebalance:
            style = "red" if alert.severity == "high" else "yellow"
            console.print(f"    [{style}]{alert.detail}[/{style}]")


async def main():
    parser = argparse.ArgumentParser(description="Continuous arbitrage scanner")
    parser.add_argument("--interval", type=int, default=None, help="Base scan interval in seconds")
    parser.add_argument("--min-spread", type=float, default=None, help="Min net spread percent")
    parser.add_argument("--top", type=int, default=10, help="Show top N opportunities")
    parser.add_argument("--no-volatility", action="store_true", help="Disable volatility-adaptive scanning")
    parser.add_argument("--no-inventory", action="store_true", help="Disable inventory tracking")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    config = load_config()
    base_interval = args.interval or config.get("scan_interval_seconds", 30)
    if args.min_spread is not None:
        config["min_spread_pct"] = args.min_spread

    tokens = load_tokens()
    if not tokens:
        logger.error("No tokens found. Run 'python discover_tokens.py' first.")
        sys.exit(1)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    # Initialize volatility tracker
    use_volatility = not args.no_volatility
    tracker = VolatilityTracker(
        normal_interval=base_interval,
        fast_interval=max(10, base_interval // 3),
    ) if use_volatility else None

    # Initialize inventory manager
    use_inventory = not args.no_inventory
    inventory = None
    if use_inventory:
        inventory = InventoryManager()
        if not inventory.load():
            logger.info("Edit inventory.yaml with your actual holdings to enable sizing")
            inventory = None

    # Load optional alert modules
    alert_modules = []
    try:
        if config.get("telegram", {}).get("enabled"):
            from scanner.alerts.telegram import send_alerts
            alert_modules.append(("telegram", send_alerts))
            logger.info("Telegram alerts enabled")
    except ImportError:
        pass
    try:
        if config.get("discord", {}).get("enabled"):
            from scanner.alerts.discord import send_alerts
            alert_modules.append(("discord", send_alerts))
            logger.info("Discord alerts enabled")
    except ImportError:
        pass

    # Status display
    features = []
    if tracker:
        features.append("volatility detection")
    if inventory:
        features.append("inventory tracking")
    if alert_modules:
        features.append(f"{', '.join(n for n, _ in alert_modules)} alerts")

    console.print(Panel(
        f"Scanning {len(tokens)} tokens, base interval {base_interval}s\n"
        f"Min spread: {config.get('min_spread_pct', 1.5)}%\n"
        f"Features: {', '.join(features) if features else 'none'}\n"
        f"Press Ctrl+C to stop",
        title="Arbitrage Scanner",
        border_style="green",
    ))

    cycle = 0
    async with aiohttp.ClientSession() as session:
        while _running:
            cycle += 1
            try:
                # Determine interval
                if tracker:
                    interval = tracker.recommended_interval
                    mode = "FAST" if tracker.is_fast_mode else "normal"
                else:
                    interval = base_interval
                    mode = "normal"

                logger.info(f"--- Cycle {cycle} ({mode}, {interval}s) ---")

                # Fetch and analyze
                prices = await fetch_all_prices(session, tokens)
                opportunities = find_opportunities(prices, tokens, config)
                ranked = rank_opportunities(opportunities)

                # Display opportunities
                display(ranked[:args.top])

                # Volatility detection
                if tracker:
                    signals = tracker.update(prices)
                    display_volatility_signals(signals, tracker)

                # Inventory sizing
                if inventory and ranked:
                    display_inventory_status(inventory, ranked)

                # Send external alerts for top opportunities
                for name, send_fn in alert_modules:
                    try:
                        await send_fn(ranked[:5], config)
                    except Exception as e:
                        logger.warning(f"{name} alert failed: {e}")

            except Exception as e:
                logger.error(f"Scan cycle {cycle} failed: {e}")

            if _running:
                await asyncio.sleep(interval)


if __name__ == "__main__":
    asyncio.run(main())
