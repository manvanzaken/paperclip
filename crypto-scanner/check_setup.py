#!/usr/bin/env python3
"""Verify API connectivity and configuration.

Run this after setting up config.yaml to check that everything works.

Usage:
    python check_setup.py
"""

import asyncio
import logging
import sys

import aiohttp
from rich.console import Console
from rich.table import Table

from scanner.config import load_config, DEFAULT_CONFIG_PATH, TOKENS_CACHE_PATH

console = Console()
logging.basicConfig(level=logging.WARNING)


async def check_binance(session: aiohttp.ClientSession) -> tuple[bool, str]:
    try:
        async with session.get("https://api.binance.com/api/v3/ping") as resp:
            if resp.status == 200:
                return True, "Connected (no key needed)"
            return False, f"HTTP {resp.status}"
    except Exception as e:
        return False, str(e)


async def check_dexscreener(session: aiohttp.ClientSession) -> tuple[bool, str]:
    try:
        async with session.get(
            "https://api.dexscreener.com/latest/dex/search",
            params={"q": "PEPE"},
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                pairs = len(data.get("pairs", []))
                return True, f"Connected, {pairs} pairs found (no key needed)"
            return False, f"HTTP {resp.status}"
    except Exception as e:
        return False, str(e)


async def check_coingecko(session: aiohttp.ClientSession) -> tuple[bool, str]:
    try:
        async with session.get("https://api.coingecko.com/api/v3/ping") as resp:
            if resp.status == 200:
                return True, "Connected (free tier, 10 req/min)"
            return False, f"HTTP {resp.status}"
    except Exception as e:
        return False, str(e)


async def check_jupiter(session: aiohttp.ClientSession) -> tuple[bool, str]:
    try:
        # Test Jupiter quote API (free, no auth)
        sol_mint = "So11111111111111111111111111111111111111112"
        usdc_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
        async with session.get(
            "https://api.jup.ag/swap/v1/quote",
            params={
                "inputMint": usdc_mint,
                "outputMint": sol_mint,
                "amount": "1000000",
                "slippageBps": "50",
            },
        ) as resp:
            if resp.status == 200:
                return True, "Connected via quote API (no key needed)"
            elif resp.status == 401:
                return True, "API requires auth (optional — DexScreener covers Solana)"
            return False, f"HTTP {resp.status}"
    except Exception as e:
        return False, str(e)


async def check_ccxt_exchange(name: str, config: dict) -> tuple[bool, str]:
    try:
        import ccxt.async_support as ccxt

        exchange_map = {
            "gate": ccxt.gateio,
            "mexc": ccxt.mexc,
            "kucoin": ccxt.kucoin,
            "bybit": ccxt.bybit,
        }

        cls = exchange_map.get(name)
        if not cls:
            return False, "Unknown exchange"

        exchange = cls({"enableRateLimit": True})
        try:
            ticker = await exchange.fetch_ticker("BTC/USDT")
            price = ticker.get("last", "?")
            return True, f"Connected, BTC=${price}"
        finally:
            await exchange.close()
    except ImportError:
        return False, "ccxt not installed"
    except Exception as e:
        return False, str(e)


async def main():
    config = load_config()

    console.print("\n[bold]Binance Alpha Arbitrage Scanner — Setup Check[/bold]\n")

    # Check config
    config_exists = DEFAULT_CONFIG_PATH.exists()
    tokens_exist = TOKENS_CACHE_PATH.exists()

    table = Table(title="Configuration")
    table.add_column("Item", width=25)
    table.add_column("Status", width=10)
    table.add_column("Details", width=50)

    table.add_row(
        "config.yaml",
        "[green]OK[/green]" if config_exists else "[yellow]MISSING[/yellow]",
        str(DEFAULT_CONFIG_PATH) if config_exists else "Using defaults (copy config.example.yaml → config.yaml)",
    )
    table.add_row(
        "tokens.json",
        "[green]OK[/green]" if tokens_exist else "[yellow]MISSING[/yellow]",
        str(TOKENS_CACHE_PATH) if tokens_exist else "Run: python discover_tokens.py",
    )

    if tokens_exist:
        from scanner.discovery import load_tokens
        tokens = load_tokens()
        table.add_row("Cached tokens", "[green]OK[/green]", f"{len(tokens)} tokens loaded")

    console.print(table)
    console.print()

    # Check API connectivity
    api_table = Table(title="API Connectivity")
    api_table.add_column("Source", width=20)
    api_table.add_column("Status", width=10)
    api_table.add_column("Details", width=55)

    async with aiohttp.ClientSession() as session:
        checks = [
            ("Binance", check_binance(session)),
            ("DexScreener", check_dexscreener(session)),
            ("CoinGecko", check_coingecko(session)),
            ("Jupiter", check_jupiter(session)),
        ]

        for name, coro in checks:
            ok, msg = await coro
            status = "[green]OK[/green]" if ok else "[red]FAIL[/red]"
            api_table.add_row(name, status, msg)

    # Check CCXT exchanges
    for name in ["gate", "mexc", "kucoin", "bybit"]:
        ok, msg = await check_ccxt_exchange(name, config)
        status = "[green]OK[/green]" if ok else "[yellow]SKIP[/yellow]"
        api_table.add_row(f"{name.title()} (CCXT)", status, msg)

    console.print(api_table)
    console.print()

    # Check alert config
    alert_table = Table(title="Alert Channels")
    alert_table.add_column("Channel", width=20)
    alert_table.add_column("Status", width=10)
    alert_table.add_column("Details", width=55)

    tg = config.get("telegram", {})
    if tg.get("enabled") and tg.get("bot_token"):
        alert_table.add_row("Telegram", "[green]Configured[/green]", f"Chat ID: {tg.get('chat_id', '?')}")
    else:
        alert_table.add_row("Telegram", "[dim]Disabled[/dim]", "Set telegram.enabled=true in config.yaml")

    dc = config.get("discord", {})
    if dc.get("enabled") and dc.get("webhook_url"):
        alert_table.add_row("Discord", "[green]Configured[/green]", "Webhook URL set")
    else:
        alert_table.add_row("Discord", "[dim]Disabled[/dim]", "Set discord.enabled=true in config.yaml")

    console.print(alert_table)

    console.print("\n[bold]Next steps:[/bold]")
    if not tokens_exist:
        console.print("  1. python discover_tokens.py --symbols ACE,PIXEL,MANTA")
        console.print("  2. python scan_once.py")
    else:
        console.print("  1. python scan_once.py")
        console.print("  2. python scan_loop.py")
    console.print()


if __name__ == "__main__":
    asyncio.run(main())
