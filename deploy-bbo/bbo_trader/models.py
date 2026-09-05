"""Core data types. Pure dataclasses, no I/O."""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone

# ---- position states -------------------------------------------------------
TT_ENTERING = "TT_ENTERING"
MAKER_RESTING = "MAKER_RESTING"
HEDGING = "HEDGING"
OPEN = "OPEN"
EXIT_MAKER_RESTING = "EXIT_MAKER_RESTING"
TT_EXITING = "TT_EXITING"
EXIT_HEDGING = "EXIT_HEDGING"
DEGRADED = "DEGRADED"
CLOSED = "CLOSED"
NON_TERMINAL = frozenset({TT_ENTERING, MAKER_RESTING, HEDGING, OPEN, EXIT_MAKER_RESTING, TT_EXITING, EXIT_HEDGING, DEGRADED})


@dataclass(frozen=True)
class BBO:
    venue: str
    symbol: str
    bid: float
    bid_qty: float      # contracts resting at the best bid
    ask: float
    ask_qty: float      # contracts resting at the best ask
    ts_exchange: float  # seconds
    ts_local: float     # seconds, when we received it
    contract_size: float = 1.0

    @property
    def ok(self) -> bool:
        return self.bid > 0 and self.ask > 0 and self.ask >= self.bid

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0

    @property
    def width_pct(self) -> float:
        return (self.ask - self.bid) / self.ask * 100.0 if self.ask > 0 else 0.0

    def touch_notional(self, side: str) -> float:
        """USD resting at the touch we would CROSS: 'sell' hits the bid, 'buy' lifts the ask."""
        if side == "sell":
            return self.bid * self.bid_qty * self.contract_size
        if side == "buy":
            return self.ask * self.ask_qty * self.contract_size
        raise ValueError(f"side must be 'buy' or 'sell', got {side!r}")


@dataclass(frozen=True)
class VenueSpec:
    venue: str
    symbol: str
    instrument: str       # venue-native id
    contract_size: float  # base units per contract
    lot: float            # size step (contracts)
    min_qty: float        # minimum order (contracts)
    tick: float           # price step


@dataclass(frozen=True)
class Fees:
    taker: float  # percent per leg
    maker: float  # percent per leg


@dataclass(frozen=True)
class OrderAck:
    ok: bool
    order_id: str = ""
    error: str = ""
    latency_ms: float = 0.0


@dataclass(frozen=True)
class OrderEvent:
    venue: str
    client_id: str
    order_id: str
    state: str            # ack | partial | filled | canceled | rejected
    filled_qty: float = 0.0   # cumulative contracts
    avg_price: float = 0.0    # cumulative average fill price
    fee: float = 0.0          # cumulative fee in USD (positive = cost)
    liquidity: str = ""       # maker | taker | ""
    position_id: str = ""     # venue position id (MEXC hedge mode)
    ts: float = 0.0
    error: str = ""

    @property
    def terminal(self) -> bool:
        return self.state in ("filled", "canceled", "rejected")


@dataclass(frozen=True)
class Intent:
    kind: str  # TT_ENTER | TM_ENTER | TT_EXIT | TM_EXIT | REQUOTE | CANCEL | UPGRADE_TT | NONE
    reason: str = ""
    symbol: str = ""
    venue_a: str = ""     # short venue
    venue_b: str = ""     # long venue
    maker_venue: str = ""
    rest_price: float = 0.0
    size_usd: float = 0.0
    edge_pct: float = 0.0
    spread_pct: float = 0.0
    ts: float = 0.0


def none(reason: str) -> Intent:
    return Intent(kind="NONE", reason=reason)


def _iso(ts: float) -> str | None:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts else None


def _ts_from(d: dict, private: str, iso_key: str) -> float:
    """Machine timestamp from the private shadow key, falling back to the dashboard's ISO string
    (a hand-repaired state file may carry only the latter; 0.0 would trigger an instant timeout exit)."""
    v = d.get(private)
    if v:
        return float(v)
    s = d.get(iso_key)
    return datetime.fromisoformat(s).timestamp() if isinstance(s, str) and s else 0.0


@dataclass
class Position:
    """One two-leg position through its whole life. `to_dict()` is the DASHBOARD view (legacy aliases,
    ISO times); the machine-readable timestamps travel in the private `_entry_ts`/`_exit_ts` keys and
    `from_dict()` prefers them. Unknown keys are ignored on load (forward compatibility)."""
    id: int
    symbol: str
    venue_a: str          # short leg venue
    venue_b: str          # long leg venue
    status: str
    mode: str             # entry mode: TT | TM
    size_usd: float = 0.0
    qty_a: float = 0.0    # target contracts per leg
    qty_b: float = 0.0
    filled_a: float = 0.0  # entry fills (contracts)
    filled_b: float = 0.0
    entry_price_a: float = 0.0
    entry_price_b: float = 0.0
    entry_fees_usd: float = 0.0
    exit_fees_usd: float = 0.0
    detect_spread_pct: float = 0.0
    entry_spread_pct: float = 0.0
    current_spread_pct: float = 0.0
    peak_spread_pct: float = 0.0
    entry_time: float = 0.0
    exit_time: float = 0.0
    exit_spread_pct: float = 0.0
    exit_price_a: float = 0.0
    exit_price_b: float = 0.0
    exit_filled_a: float = 0.0
    exit_filled_b: float = 0.0
    exit_reason: str = ""
    exit_mode: str = ""
    gross_pnl_usd: float = 0.0
    net_pnl_usd: float = 0.0
    # resting maker bookkeeping (entry or exit; one resting order at a time)
    maker_venue: str = ""
    maker_client_id: str = ""
    maker_order_id: str = ""
    maker_side: str = ""            # buy | sell
    maker_qty: float = 0.0
    maker_rest_price: float = 0.0
    maker_filled_qty: float = 0.0
    maker_avg_price: float = 0.0
    hedged_qty: float = 0.0         # maker contracts already hedged
    maker_fee_usd: float = 0.0      # cumulative fee on the resting order
    maker_cancel_sent: bool = False
    requote_pending: bool = False   # cancel+new requote in flight (venues without amend)
    requote_price: float = 0.0
    maker_posted_ts: float = 0.0
    maker_last_requote_ts: float = 0.0
    maker_fill_ts: float = 0.0
    edge_gone_since: float = 0.0
    upgrade_pending: bool = False
    # ids and analytics
    client_ids: dict = field(default_factory=dict)          # leg -> client id
    order_ids: dict = field(default_factory=dict)           # leg -> venue order id
    venue_position_ids: dict = field(default_factory=dict)  # venue -> position id
    fee_liquidity: dict = field(default_factory=dict)       # leg -> maker | taker
    latency_ms: dict = field(default_factory=dict)          # stage -> ms
    degraded_leg: str = ""    # a | b | both
    close_retry_count: int = 0
    last_close_attempt: float = 0.0
    signal_ts: float = 0.0

    def to_dict(self) -> dict:
        d = asdict(self)
        d.update({
            # dashboard-compatible aliases (real_trader schema)
            "exchange_short": self.venue_a, "exchange_long": self.venue_b,
            "instrument_short": "PERP", "instrument_long": "PERP",
            "entry_price_short": self.entry_price_a, "entry_price_long": self.entry_price_b,
            "exit_price_short": self.exit_price_a, "exit_price_long": self.exit_price_b,
            "order_id_short": self.order_ids.get("entry_a", ""), "order_id_long": self.order_ids.get("entry_b", ""),
            "order_id_close_short": self.order_ids.get("exit_a", ""), "order_id_close_long": self.order_ids.get("exit_b", ""),
            "entry_time": _iso(self.entry_time), "exit_time": _iso(self.exit_time),
            "fill_latency_short_ms": self.latency_ms.get("entry_a", 0.0),
            "fill_latency_long_ms": self.latency_ms.get("entry_b", 0.0),
        })
        d["_entry_ts"] = self.entry_time
        d["_exit_ts"] = self.exit_time
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "Position":
        names = {f for f in cls.__dataclass_fields__}
        kw = {k: (dict(v) if isinstance(v, dict) else v) for k, v in d.items() if k in names}  # no aliasing
        kw["entry_time"] = _ts_from(d, "_entry_ts", "entry_time")
        kw["exit_time"] = _ts_from(d, "_exit_ts", "exit_time")
        return cls(**kw)
