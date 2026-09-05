from __future__ import annotations

from scanner.models import FeeEstimate, PriceQuote

# Default trading fees by source
DEFAULT_FEES: dict[str, float] = {
    "binance": 0.1,
    "gate": 0.2,
    "mexc": 0.2,
    "kucoin": 0.1,
    "bybit": 0.1,
    "dexscreener": 0.3,  # DEX swap fees (typical 0.3% for Uniswap-style)
    "jupiter": 0.25,     # Jupiter aggregator finds best route
}

# Gas costs by chain (USD estimates)
DEFAULT_GAS: dict[str, float] = {
    "ethereum": 5.0,
    "bsc": 0.10,
    "solana": 0.001,
    "base": 0.05,
    "arbitrum": 0.10,
    "polygon": 0.01,
    "avalanche": 0.10,
    "optimism": 0.05,
}

# Estimated withdrawal fees (USD) for CEX → external
DEFAULT_WITHDRAWAL_FEES: dict[str, float] = {
    "binance": 1.0,
    "gate": 1.5,
    "mexc": 1.0,
    "kucoin": 1.0,
    "bybit": 1.0,
}


def estimate_fees(
    buy_quote: PriceQuote,
    sell_quote: PriceQuote,
    config: dict,
) -> FeeEstimate:
    """Estimate total cost of an arbitrage trade between two venues.

    Accounts for:
    - Trading fees on both sides
    - Withdrawal fee (if cross-venue transfer needed)
    - Gas fee (if DEX involved)
    - Slippage estimate
    """
    fees_config = config.get("fees", {})

    # Trading fees
    buy_fee = fees_config.get(
        f"{buy_quote.source}_spot_pct",
        DEFAULT_FEES.get(buy_quote.source, 0.2),
    )
    sell_fee = fees_config.get(
        f"{sell_quote.source}_spot_pct",
        DEFAULT_FEES.get(sell_quote.source, 0.2),
    )

    # Withdrawal fee: needed if tokens must move between venues
    withdrawal_usd = 0.0
    if buy_quote.source != sell_quote.source:
        # You need to move tokens from buy venue to sell venue
        withdrawal_usd = DEFAULT_WITHDRAWAL_FEES.get(buy_quote.source, 1.0)

    # Gas fee: if either side is a DEX
    gas_usd = 0.0
    for q in (buy_quote, sell_quote):
        if q.chain:  # DEX trade
            gas_usd += fees_config.get(
                f"gas_{q.chain}_usd",
                DEFAULT_GAS.get(q.chain, 1.0),
            )

    # Slippage
    slippage_pct = fees_config.get("slippage_pct", 0.5)

    return FeeEstimate(
        buy_trading_fee_pct=buy_fee,
        sell_trading_fee_pct=sell_fee,
        withdrawal_fee_usd=withdrawal_usd,
        gas_fee_usd=gas_usd,
        slippage_pct=slippage_pct,
    )
