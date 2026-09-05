from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass
class Token:
    symbol: str                             # e.g. "MYTOKEN"
    name: str
    binance_symbol: str | None = None       # e.g. "MYTOKENUSDT"
    contract_addresses: dict[str, str] = field(default_factory=dict)  # {"ethereum": "0x...", "bsc": "0x..."}
    coingecko_id: str | None = None
    cex_listings: list[str] = field(default_factory=list)  # ["binance", "gate", "mexc"]


@dataclass
class PriceQuote:
    source: str             # "binance", "gate", "dexscreener", "jupiter"
    source_detail: str      # "binance_spot", "dexscreener_ethereum", "jupiter_solana"
    symbol: str
    price_usd: float
    bid: float | None = None
    ask: float | None = None
    volume_24h_usd: float = 0.0
    liquidity_usd: float | None = None
    timestamp: float = field(default_factory=time.time)
    chain: str | None = None            # "ethereum", "bsc", "solana", None for CEX
    pair_address: str | None = None     # DEX pair contract
    quote_currency: str = "USD"         # original quote before normalization

    @property
    def effective_buy_price(self) -> float:
        """Price you'd pay to buy (ask or last)."""
        return self.ask if self.ask else self.price_usd

    @property
    def effective_sell_price(self) -> float:
        """Price you'd receive selling (bid or last)."""
        return self.bid if self.bid else self.price_usd


@dataclass
class FeeEstimate:
    buy_trading_fee_pct: float = 0.0
    sell_trading_fee_pct: float = 0.0
    withdrawal_fee_usd: float = 0.0
    gas_fee_usd: float = 0.0
    slippage_pct: float = 0.0

    @property
    def total_cost_pct(self) -> float:
        return self.buy_trading_fee_pct + self.sell_trading_fee_pct + self.slippage_pct

    def total_cost_usd(self, trade_size_usd: float) -> float:
        pct_cost = (self.total_cost_pct / 100) * trade_size_usd
        return pct_cost + self.withdrawal_fee_usd + self.gas_fee_usd


@dataclass
class Opportunity:
    token: Token
    buy_quote: PriceQuote
    sell_quote: PriceQuote
    gross_spread_pct: float
    fees: FeeEstimate
    net_spread_pct: float
    recommended_size_usd: float = 0.0
    score: float = 0.0
    timestamp: float = field(default_factory=time.time)

    @property
    def direction(self) -> str:
        return f"Buy@{self.buy_quote.source_detail} → Sell@{self.sell_quote.source_detail}"
