"""Volatility spike detector with adaptive scan frequency.

Tracks price and volume history across scan cycles. When a token's
price moves sharply or volume spikes, it signals the scan loop to
increase frequency — these are the moments when spreads blow out.
"""

from __future__ import annotations

import time
from collections import defaultdict, deque
from dataclasses import dataclass

from scanner.models import PriceQuote


@dataclass
class VolatilitySignal:
    symbol: str
    signal_type: str        # "price_spike", "volume_spike", "spread_widening"
    magnitude: float        # How many X above baseline
    detail: str
    timestamp: float


class VolatilityTracker:
    """Track price/volume history and detect spikes."""

    def __init__(
        self,
        price_spike_pct: float = 5.0,       # Alert if price moves >5% in window
        volume_spike_multiplier: float = 3.0, # Alert if volume >3x baseline
        spread_spike_pct: float = 3.0,       # Alert if cross-venue spread >3%
        history_window: int = 60,            # Keep last 60 data points
        fast_interval: int = 10,             # Scan every 10s during spikes
        normal_interval: int = 30,           # Normal scan interval
        cooldown_cycles: int = 20,           # Stay fast for 20 cycles after spike
    ):
        self.price_spike_pct = price_spike_pct
        self.volume_spike_multiplier = volume_spike_multiplier
        self.spread_spike_pct = spread_spike_pct
        self.history_window = history_window
        self.fast_interval = fast_interval
        self.normal_interval = normal_interval
        self.cooldown_cycles = cooldown_cycles

        # History: {symbol: deque of (timestamp, avg_price, total_volume)}
        self._price_history: dict[str, deque] = defaultdict(
            lambda: deque(maxlen=history_window)
        )
        self._volume_history: dict[str, deque] = defaultdict(
            lambda: deque(maxlen=history_window)
        )

        # Fast mode tracking
        self._fast_mode_until_cycle: int = 0
        self._current_cycle: int = 0

    def update(
        self,
        prices: dict[str, list[PriceQuote]],
    ) -> list[VolatilitySignal]:
        """Update with new price data and return any detected signals."""
        self._current_cycle += 1
        signals = []
        now = time.time()

        for symbol, quotes in prices.items():
            if not quotes:
                continue

            # Calculate current aggregates
            current_prices = [q.price_usd for q in quotes if q.price_usd > 0]
            if not current_prices:
                continue

            avg_price = sum(current_prices) / len(current_prices)
            total_volume = sum(q.volume_24h_usd for q in quotes)

            # --- Price spike detection ---
            price_hist = self._price_history[symbol]
            if len(price_hist) >= 3:
                # Compare to average of last N readings
                recent_prices = [p for _, p, _ in price_hist]
                baseline_price = sum(recent_prices) / len(recent_prices)

                if baseline_price > 0:
                    price_change_pct = abs(avg_price - baseline_price) / baseline_price * 100

                    if price_change_pct >= self.price_spike_pct:
                        direction = "UP" if avg_price > baseline_price else "DOWN"
                        signals.append(VolatilitySignal(
                            symbol=symbol,
                            signal_type="price_spike",
                            magnitude=price_change_pct,
                            detail=(
                                f"{symbol} moved {price_change_pct:.1f}% {direction} "
                                f"(${baseline_price:.6f} -> ${avg_price:.6f})"
                            ),
                            timestamp=now,
                        ))

            # --- Volume spike detection ---
            vol_hist = self._volume_history[symbol]
            if len(vol_hist) >= 3:
                recent_vols = [v for _, v in vol_hist]
                baseline_vol = sum(recent_vols) / len(recent_vols)

                if baseline_vol > 0:
                    vol_multiplier = total_volume / baseline_vol

                    if vol_multiplier >= self.volume_spike_multiplier:
                        signals.append(VolatilitySignal(
                            symbol=symbol,
                            signal_type="volume_spike",
                            magnitude=vol_multiplier,
                            detail=(
                                f"{symbol} volume {vol_multiplier:.1f}x baseline "
                                f"(${baseline_vol:,.0f} -> ${total_volume:,.0f})"
                            ),
                            timestamp=now,
                        ))

            # --- Cross-venue spread detection ---
            if len(current_prices) >= 2:
                min_p = min(current_prices)
                max_p = max(current_prices)
                if min_p > 0:
                    spread_pct = (max_p - min_p) / min_p * 100
                    if spread_pct >= self.spread_spike_pct:
                        # Find which venues
                        min_q = min(quotes, key=lambda q: q.price_usd if q.price_usd > 0 else float("inf"))
                        max_q = max(quotes, key=lambda q: q.price_usd)
                        signals.append(VolatilitySignal(
                            symbol=symbol,
                            signal_type="spread_widening",
                            magnitude=spread_pct,
                            detail=(
                                f"{symbol} spread {spread_pct:.1f}% "
                                f"({min_q.source_detail} ${min_p:.6f} vs "
                                f"{max_q.source_detail} ${max_p:.6f})"
                            ),
                            timestamp=now,
                        ))

            # Record history
            price_hist.append((now, avg_price, total_volume))
            vol_hist.append((now, total_volume))

        # Activate fast mode if any signals
        if signals:
            self._fast_mode_until_cycle = self._current_cycle + self.cooldown_cycles

        return signals

    @property
    def is_fast_mode(self) -> bool:
        """Whether we should be scanning at fast frequency."""
        return self._current_cycle < self._fast_mode_until_cycle

    @property
    def recommended_interval(self) -> int:
        """Current recommended scan interval in seconds."""
        return self.fast_interval if self.is_fast_mode else self.normal_interval

    @property
    def cycles_remaining_fast(self) -> int:
        """How many more fast-mode cycles remain."""
        remaining = self._fast_mode_until_cycle - self._current_cycle
        return max(0, remaining)
