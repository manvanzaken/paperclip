from __future__ import annotations

from datetime import datetime

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text

from scanner.models import Opportunity

console = Console()


def display(opportunities: list[Opportunity], title: str = "Binance Alpha Arbitrage Scanner"):
    """Display opportunities as a rich formatted table."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if not opportunities:
        console.print(
            Panel(
                f"[dim]{now}[/dim]\n\nNo opportunities found above threshold.",
                title=title,
                border_style="dim",
            )
        )
        return

    table = Table(
        title=f"{title}  [dim]{now}[/dim]",
        show_lines=True,
        padding=(0, 1),
    )

    table.add_column("Score", justify="center", style="bold", width=6)
    table.add_column("Token", style="cyan", width=10)
    table.add_column("Buy @", width=28)
    table.add_column("Sell @", width=28)
    table.add_column("Gross", justify="right", width=8)
    table.add_column("Net", justify="right", width=8)
    table.add_column("Fees", justify="right", width=8)
    table.add_column("Size", justify="right", width=10)

    for opp in opportunities:
        # Score color
        if opp.score >= 70:
            score_style = "bold green"
        elif opp.score >= 40:
            score_style = "yellow"
        else:
            score_style = "dim"

        # Net spread color
        if opp.net_spread_pct >= 5:
            net_style = "bold green"
        elif opp.net_spread_pct >= 2:
            net_style = "green"
        else:
            net_style = "yellow"

        # Format buy/sell info
        buy_info = _format_quote(opp.buy_quote)
        sell_info = _format_quote(opp.sell_quote)

        # Format size
        size_str = f"${opp.recommended_size_usd:,.0f}" if opp.recommended_size_usd else "-"

        table.add_row(
            Text(f"{opp.score:.0f}", style=score_style),
            opp.token.symbol,
            buy_info,
            sell_info,
            f"{opp.gross_spread_pct:.2f}%",
            Text(f"{opp.net_spread_pct:.2f}%", style=net_style),
            f"{opp.fees.total_cost_pct:.2f}%",
            size_str,
        )

    console.print()
    console.print(table)
    console.print()

    # Summary
    best = opportunities[0]
    console.print(
        f"  [bold]Top opportunity:[/bold] {best.token.symbol} — "
        f"Buy at {best.buy_quote.source_detail} (${best.buy_quote.effective_buy_price:.6f}) → "
        f"Sell at {best.sell_quote.source_detail} (${best.sell_quote.effective_sell_price:.6f}) — "
        f"Net {best.net_spread_pct:.2f}%"
    )
    console.print()


def _format_quote(quote) -> str:
    """Format a price quote for table display."""
    price_str = _format_price(quote.price_usd)
    source = quote.source_detail.replace("_", " ").title()

    liq = quote.liquidity_usd
    if liq:
        if liq >= 1_000_000:
            liq_str = f"${liq/1_000_000:.1f}M liq"
        elif liq >= 1000:
            liq_str = f"${liq/1000:.0f}K liq"
        else:
            liq_str = f"${liq:.0f} liq"
    else:
        vol = quote.volume_24h_usd
        if vol >= 1_000_000:
            liq_str = f"${vol/1_000_000:.1f}M vol"
        elif vol >= 1000:
            liq_str = f"${vol/1000:.0f}K vol"
        else:
            liq_str = f"${vol:.0f} vol"

    return f"{price_str}\n[dim]{source} | {liq_str}[/dim]"


def _format_price(price: float) -> str:
    """Format price with appropriate decimal places."""
    if price >= 100:
        return f"${price:,.2f}"
    elif price >= 1:
        return f"${price:.4f}"
    elif price >= 0.01:
        return f"${price:.6f}"
    else:
        return f"${price:.8f}"
