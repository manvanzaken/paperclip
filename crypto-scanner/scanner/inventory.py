"""Inventory tracking system for pre-positioned arbitrage.

Instead of transferring tokens between venues for each trade (slow, expensive),
maintain inventory on both sides and trade simultaneously. This module tracks
your holdings, calculates when rebalancing is needed, and sizes opportunities
based on available inventory.

Inventory file: inventory.yaml
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import yaml

from scanner.config import BASE_DIR
from scanner.models import Opportunity

logger = logging.getLogger(__name__)

INVENTORY_PATH = BASE_DIR / "inventory.yaml"

EXAMPLE_INVENTORY = """\
# Inventory tracking for pre-positioned arbitrage.
# Record your actual holdings across venues here.
# The scanner will use this to size opportunities and flag rebalancing needs.

# Your holdings by venue and token
holdings:
  # CEX holdings
  binance:
    USDT: 5000
    # MUBARAK: 100000   # example: 100k MUBARAK tokens
  gate:
    USDT: 2000
  mexc:
    USDT: 2000
  kucoin:
    USDT: 1000
  bybit:
    USDT: 1000

  # DEX holdings (by chain)
  dexscreener_bsc:
    USDT: 3000
    # MUBARAK: 50000
  dexscreener_ethereum:
    USDT: 2000
  dexscreener_solana:
    USDT: 1000

# Rebalancing thresholds
rebalancing:
  # Alert when venue balance drops below this fraction of initial
  min_balance_ratio: 0.2
  # Alert when imbalance between buy/sell side exceeds this ratio
  max_imbalance_ratio: 3.0
  # Minimum USDT to keep on each venue for gas/fees
  min_usdt_reserve: 100
"""


@dataclass
class VenueBalance:
    venue: str          # "binance", "gate", "dexscreener_bsc", etc.
    token: str          # "USDT", "MUBARAK", etc.
    amount: float


@dataclass
class RebalanceAlert:
    venue: str
    token: str
    current: float
    needed: float
    severity: str       # "low", "medium", "high"
    detail: str


@dataclass
class SizedOpportunity:
    """An opportunity with actual executable size based on inventory."""
    opportunity: Opportunity
    max_buy_amount_usd: float    # How much you can buy given your balance
    max_sell_amount_tokens: float # How many tokens you can sell
    executable_size_usd: float   # Min of buy/sell capacity
    limited_by: str              # "buy_side", "sell_side", "neither"


class InventoryManager:
    """Track and manage inventory across trading venues."""

    def __init__(self, path: Path | None = None):
        self.path = path or INVENTORY_PATH
        self.holdings: dict[str, dict[str, float]] = {}
        self.rebalancing_config: dict = {
            "min_balance_ratio": 0.2,
            "max_imbalance_ratio": 3.0,
            "min_usdt_reserve": 100,
        }
        self._initial_holdings: dict[str, dict[str, float]] = {}

    def load(self) -> bool:
        """Load inventory from YAML file. Returns True if loaded successfully."""
        if not self.path.exists():
            logger.info(
                f"No inventory file found at {self.path}. "
                "Creating example. Edit it with your actual holdings."
            )
            self._create_example()
            return False

        try:
            with open(self.path) as f:
                data = yaml.safe_load(f) or {}

            self.holdings = data.get("holdings", {})
            self.rebalancing_config = data.get("rebalancing", self.rebalancing_config)

            # Store initial state for rebalancing alerts
            self._initial_holdings = {
                venue: dict(tokens)
                for venue, tokens in self.holdings.items()
            }

            total = sum(
                sum(tokens.values())
                for tokens in self.holdings.values()
            )
            venues = len(self.holdings)
            logger.info(f"Loaded inventory: {venues} venues, {total:,.0f} total units")
            return True

        except Exception as e:
            logger.error(f"Failed to load inventory: {e}")
            return False

    def _create_example(self):
        """Create example inventory file."""
        with open(self.path, "w") as f:
            f.write(EXAMPLE_INVENTORY)
        logger.info(f"Created example inventory at {self.path}")

    def get_balance(self, venue: str, token: str) -> float:
        """Get current balance for a token on a venue."""
        # Normalize venue name
        venue = self._normalize_venue(venue)
        return self.holdings.get(venue, {}).get(token, 0)

    def get_usdt_balance(self, venue: str) -> float:
        """Get USDT balance on a venue."""
        venue = self._normalize_venue(venue)
        return self.holdings.get(venue, {}).get("USDT", 0)

    def _normalize_venue(self, source_detail: str) -> str:
        """Normalize a source_detail to a venue key matching inventory.yaml."""
        # "binance_spot" -> "binance"
        # "dexscreener_bsc_pancakeswap" -> "dexscreener_bsc"
        # "gate_spot" -> "gate"
        if source_detail.endswith("_spot"):
            return source_detail[:-5]
        parts = source_detail.split("_")
        if parts[0] == "dexscreener" and len(parts) >= 2:
            return f"dexscreener_{parts[1]}"
        return source_detail

    def size_opportunity(self, opp: Opportunity) -> SizedOpportunity:
        """Calculate executable size based on actual inventory."""
        buy_venue = self._normalize_venue(opp.buy_quote.source_detail)
        sell_venue = self._normalize_venue(opp.sell_quote.source_detail)

        # Buy side: how much USDT do we have to buy?
        usdt_available = self.get_balance(buy_venue, "USDT")
        reserve = self.rebalancing_config.get("min_usdt_reserve", 100)
        buy_capacity_usd = max(0, usdt_available - reserve)

        # Sell side: how many tokens do we have to sell?
        token_balance = self.get_balance(sell_venue, opp.token.symbol)
        sell_price = opp.sell_quote.effective_sell_price
        sell_capacity_usd = token_balance * sell_price if sell_price > 0 else 0

        # Executable size is the minimum of both sides
        executable = min(buy_capacity_usd, sell_capacity_usd)

        if executable <= 0:
            limited_by = "buy_side" if buy_capacity_usd <= 0 else "sell_side"
        elif buy_capacity_usd < sell_capacity_usd:
            limited_by = "buy_side"
        elif sell_capacity_usd < buy_capacity_usd:
            limited_by = "sell_side"
        else:
            limited_by = "neither"

        return SizedOpportunity(
            opportunity=opp,
            max_buy_amount_usd=buy_capacity_usd,
            max_sell_amount_tokens=token_balance,
            executable_size_usd=executable,
            limited_by=limited_by,
        )

    def check_rebalancing(self) -> list[RebalanceAlert]:
        """Check if any venues need rebalancing."""
        alerts = []
        min_ratio = self.rebalancing_config.get("min_balance_ratio", 0.2)
        min_reserve = self.rebalancing_config.get("min_usdt_reserve", 100)

        for venue, tokens in self.holdings.items():
            for token, current in tokens.items():
                initial = self._initial_holdings.get(venue, {}).get(token, current)

                if initial <= 0:
                    continue

                ratio = current / initial

                # Check if balance is critically low
                if token == "USDT" and current < min_reserve:
                    alerts.append(RebalanceAlert(
                        venue=venue,
                        token=token,
                        current=current,
                        needed=min_reserve,
                        severity="high",
                        detail=f"{venue} USDT below reserve (${current:.0f} < ${min_reserve:.0f})",
                    ))
                elif ratio < min_ratio:
                    severity = "high" if ratio < 0.1 else "medium"
                    alerts.append(RebalanceAlert(
                        venue=venue,
                        token=token,
                        current=current,
                        needed=initial * 0.5,
                        severity=severity,
                        detail=(
                            f"{venue} {token} at {ratio:.0%} of initial "
                            f"({current:,.0f} / {initial:,.0f})"
                        ),
                    ))

        return alerts

    def simulate_trade(
        self, opp: Opportunity, size_usd: float
    ) -> dict[str, dict[str, float]]:
        """Simulate a trade and return the resulting balance changes.

        Does NOT modify actual holdings — call apply_trade() to commit.
        Returns: {venue: {token: delta}} showing what would change.
        """
        buy_venue = self._normalize_venue(opp.buy_quote.source_detail)
        sell_venue = self._normalize_venue(opp.sell_quote.source_detail)
        buy_price = opp.buy_quote.effective_buy_price
        sell_price = opp.sell_quote.effective_sell_price

        if buy_price <= 0 or sell_price <= 0:
            return {}

        tokens_bought = size_usd / buy_price
        revenue = tokens_bought * sell_price

        return {
            buy_venue: {
                "USDT": -size_usd,
                opp.token.symbol: +tokens_bought,
            },
            sell_venue: {
                "USDT": +revenue,
                opp.token.symbol: -tokens_bought,
            },
        }

    def apply_trade(self, deltas: dict[str, dict[str, float]]):
        """Apply trade deltas to holdings and save."""
        for venue, changes in deltas.items():
            if venue not in self.holdings:
                self.holdings[venue] = {}
            for token, delta in changes.items():
                current = self.holdings[venue].get(token, 0)
                self.holdings[venue][token] = current + delta

        self._save()

    def _save(self):
        """Save current holdings back to YAML."""
        data = {
            "holdings": self.holdings,
            "rebalancing": self.rebalancing_config,
        }
        with open(self.path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)

    def summary(self) -> dict:
        """Get summary of all holdings."""
        total_usdt = 0
        venue_totals = {}
        for venue, tokens in self.holdings.items():
            usdt = tokens.get("USDT", 0)
            total_usdt += usdt
            venue_totals[venue] = {
                "usdt": usdt,
                "tokens": {k: v for k, v in tokens.items() if k != "USDT"},
            }
        return {
            "total_usdt": total_usdt,
            "venues": venue_totals,
        }
