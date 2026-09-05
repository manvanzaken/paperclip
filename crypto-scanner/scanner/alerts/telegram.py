from __future__ import annotations

import logging

import aiohttp

from scanner.models import Opportunity

logger = logging.getLogger(__name__)

API_URL = "https://api.telegram.org"


async def send_alerts(
    opportunities: list[Opportunity],
    config: dict,
) -> None:
    """Send top opportunities to Telegram."""
    tg_config = config.get("telegram", {})
    bot_token = tg_config.get("bot_token", "")
    chat_id = tg_config.get("chat_id", "")

    if not bot_token or not chat_id:
        return

    if not opportunities:
        return

    message = _format_message(opportunities)

    async with aiohttp.ClientSession() as session:
        url = f"{API_URL}/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }
        async with session.post(url, json=payload) as resp:
            if resp.status != 200:
                body = await resp.text()
                logger.warning(f"Telegram API error {resp.status}: {body}")


def _format_message(opportunities: list[Opportunity]) -> str:
    lines = ["*Arbitrage Alert*\n"]

    for opp in opportunities[:5]:
        lines.append(
            f"*{opp.token.symbol}* — Net {opp.net_spread_pct:.2f}% (Score: {opp.score:.0f})\n"
            f"  Buy: ${opp.buy_quote.price_usd:.6f} @ {opp.buy_quote.source_detail}\n"
            f"  Sell: ${opp.sell_quote.price_usd:.6f} @ {opp.sell_quote.source_detail}\n"
            f"  Gross: {opp.gross_spread_pct:.2f}% | Fees: {opp.fees.total_cost_pct:.2f}%"
        )

    return "\n".join(lines)
