from __future__ import annotations

from scanner.models import Opportunity


def score_opportunity(opp: Opportunity) -> float:
    """Score an opportunity 0-100 based on profitability and reliability.

    Factors:
    - Net spread (primary driver, ~60% weight)
    - Liquidity depth (higher = more executable, ~25% weight)
    - Volume confirmation (higher = more reliable price, ~15% weight)
    """
    # Net spread score: 1.5% = 20, 5% = 60, 10%+ = 90
    spread_score = min(90, max(0, (opp.net_spread_pct - 0.5) * 12))

    # Liquidity score: based on min-side liquidity
    buy_liq = opp.buy_quote.liquidity_usd or opp.buy_quote.volume_24h_usd or 0
    sell_liq = opp.sell_quote.liquidity_usd or opp.sell_quote.volume_24h_usd or 0
    min_liq = min(buy_liq, sell_liq) if buy_liq and sell_liq else max(buy_liq, sell_liq)

    if min_liq >= 100000:
        liq_score = 100
    elif min_liq >= 50000:
        liq_score = 80
    elif min_liq >= 10000:
        liq_score = 50
    elif min_liq >= 5000:
        liq_score = 30
    else:
        liq_score = 10

    # Volume score
    buy_vol = opp.buy_quote.volume_24h_usd
    sell_vol = opp.sell_quote.volume_24h_usd
    min_vol = min(buy_vol, sell_vol) if buy_vol and sell_vol else max(buy_vol, sell_vol)

    if min_vol >= 500000:
        vol_score = 100
    elif min_vol >= 100000:
        vol_score = 70
    elif min_vol >= 10000:
        vol_score = 40
    else:
        vol_score = 15

    # CEX-CEX trades are more reliable than CEX-DEX
    reliability_bonus = 5 if not opp.buy_quote.chain and not opp.sell_quote.chain else 0

    final = (spread_score * 0.60) + (liq_score * 0.25) + (vol_score * 0.15) + reliability_bonus
    return round(min(100, max(0, final)), 1)


def rank_opportunities(
    opportunities: list[Opportunity],
    min_score: float = 0,
) -> list[Opportunity]:
    """Score and sort opportunities by score descending."""
    for opp in opportunities:
        opp.score = score_opportunity(opp)

    filtered = [o for o in opportunities if o.score >= min_score]
    filtered.sort(key=lambda o: o.score, reverse=True)
    return filtered
