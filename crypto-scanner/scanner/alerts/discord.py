from __future__ import annotations

import logging

import aiohttp

from scanner.models import Opportunity

logger = logging.getLogger(__name__)


async def send_alerts(
    opportunities: list[Opportunity],
    config: dict,
) -> None:
    """Send top opportunities to Discord via webhook."""
    dc_config = config.get("discord", {})
    webhook_url = dc_config.get("webhook_url", "")

    if not webhook_url or not opportunities:
        return

    embeds = []
    for opp in opportunities[:5]:
        color = 0x00FF00 if opp.net_spread_pct >= 5 else 0xFFFF00
        embeds.append({
            "title": f"{opp.token.symbol} — Net {opp.net_spread_pct:.2f}%",
            "color": color,
            "fields": [
                {
                    "name": "Buy",
                    "value": f"${opp.buy_quote.price_usd:.6f}\n{opp.buy_quote.source_detail}",
                    "inline": True,
                },
                {
                    "name": "Sell",
                    "value": f"${opp.sell_quote.price_usd:.6f}\n{opp.sell_quote.source_detail}",
                    "inline": True,
                },
                {
                    "name": "Details",
                    "value": (
                        f"Gross: {opp.gross_spread_pct:.2f}%\n"
                        f"Fees: {opp.fees.total_cost_pct:.2f}%\n"
                        f"Score: {opp.score:.0f}/100"
                    ),
                    "inline": True,
                },
            ],
        })

    payload = {
        "username": "Arbitrage Scanner",
        "embeds": embeds,
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(webhook_url, json=payload) as resp:
            if resp.status not in (200, 204):
                body = await resp.text()
                logger.warning(f"Discord webhook error {resp.status}: {body}")
