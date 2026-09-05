# Live Trader Canonical Types — Design Spec

> Symbol registry + order-lifecycle normalization for the live trader, prerequisite to adding new exchanges beyond the current four (OKX, Bybit, MEXC, BloFin).

**Author:** Claude (drafted from analysis session 2026-04-27)
**Status:** Approved for implementation
**Scope:** `deploy-live/` only; scanner unaffected
**Hyperliquid:** out of scope for v1

---

## 1. Background

A normalization audit of the live trader (see analysis session 2026-04-27) identified that the trader is ~60% of the way to a clean canonical layer. Two gaps must close before a multi-exchange rollout:

1. **Order lifecycle is collapsed to `success: bool`** in `ExchangeOrderResponse` (`schemas.py:85`). Partial fills and full fills are indistinguishable at the schema level. Silent zero-fill failures (BloFin, MEXC) are caught ad-hoc per normalizer with no structured rejection reason.

2. **Symbol normalization is replicated in three places** (`normalizers.py:64`, `live_exchange_fetcher.py:153`, `live_exchange_fetcher.py:207`, plus inline in `real_trader.py:3038`). The `_NORMALIZERS` dict has both `"Bybit"` and `"BYBIT"` as keys — already inconsistent. There is no instrument registry; lot/tick/min-notional enforcement exists only on OKX (`real_trader.py:550–599`).

Adding a fifth exchange today multiplies both gaps. This spec consolidates them into a single canonical layer.

## 2. Goals

- One `OrderStatus` enum used by every executor, fill record, and reconciler event.
- Every `ExchangeOrderResponse` carries `requested_size_usd`, `filled_size_usd`, `status`, and structured `rejection_code` + `rejection_message` when applicable.
- Partial fills are first-class. v1 strategy reaction: **accept the reduced size**.
- One `InstrumentRegistry` per exchange, refreshed at startup + hourly, holding lot/tick/min-notional/contract-size.
- One pair of functions: `to_canonical(exchange, raw) → CanonicalSymbol` and `to_venue(exchange, canonical) → str`. All venue-specific string manipulation lives in `symbols.py`.
- Lot/tick enforcement on **every** exchange (closes the OKX-only gap).
- Adding an exchange = registering one normalizer pair + one instrument fetcher; no string manipulation outside `symbols.py`.

## 3. Non-goals

- Scanner data model changes. Scanner stays as a research/discovery tool with its own `PriceQuote`.
- Multi-quote-currency (USDC, EUR). v1 is USDT-quoted only.
- Spot vs perp disambiguation in canonical form. Bot uses one type per exchange (Bybit=SPOT, others=perp). Registry tracks the type but canonical symbol stays `{BASE}USDT`.
- FIX-grade order state machine (`PENDING_CANCEL`, etc.). Bot is market-only.
- Order modify/cancel state. Deferred.
- Maker fills / passive orders. Deferred.
- Funding payment schema. Tracked separately on `LivePosition` today; defer canonical schema.
- Multi-asset balance positions. v1 stays USDT-only.
- Cross-venue option/future symbology.
- Hyperliquid integration into the registry. Today it appears in `real_trader.py:3038` only; deferred to a follow-up plan.

## 4. Architectural decisions (locked)

These were resolved in the design session and are not open questions for implementers.

- **D1 — Live-trader-only scope.** Scanner unaffected.
- **D2 — Partial-fill v1 reaction = `accept_reduced`.** When `status=PARTIALLY_FILLED`, the position opens at the actual filled size. Strategy does not chase the remainder. Reconciler emits a `partial_fill_accepted` info event for visibility.
- **D3 — Keep `OrderStatus.SUBMITTED`** for forward-compat with future limit orders. Currently rare (bot is market-only) but reserved.
- **D4 — Hyperliquid out of scope.** Registry has no Hyperliquid normalizer; existing inline code in `real_trader.py:3038` left untouched.
- **D5 — Instrument refresh failure on boot is a hard fail.** If any exchange's `/instruments` endpoint fails on startup, the bot does not start. Safer than degraded mode for live capital.
- **D6 — Registry is in-memory only.** No SQLite persistence in v1. Adds startup latency on the order of seconds; acceptable.
- **D7 — Stale-on-open-position policy.** When `instrument_registry_stale` fires for a symbol with an open position, the bot keeps the position but blocks new entries on that symbol until refresh succeeds.
- **D8 — Tolerance for "≈ filled" = 1%.** `filled_size_usd >= requested_size_usd * 0.99` → FILLED. Configurable per-venue is a follow-up if needed.

## 5. Canonical schema additions

### 5.1 OrderStatus + RejectionCode

`schemas.py` (additive):

```python
class OrderStatus(str, Enum):
    SUBMITTED        = "submitted"          # sent, no ack yet (rare for market)
    FILLED           = "filled"             # >= 99% of requested size
    PARTIALLY_FILLED = "partially_filled"   # > 0 but < 99% of requested
    REJECTED         = "rejected"           # exchange refused
    CANCELLED        = "cancelled"          # we or they cancelled before fill
    UNKNOWN          = "unknown"            # response unparseable; treat as pending

class RejectionCode(str, Enum):
    INSUFFICIENT_BALANCE = "insufficient_balance"
    MIN_NOTIONAL         = "min_notional"
    LOT_SIZE             = "lot_size"
    PRICE_OUT_OF_BAND    = "price_out_of_band"
    RATE_LIMITED         = "rate_limited"
    COMPLIANCE_BLOCKED   = "compliance_blocked"   # OKX 51155 etc.
    SILENT_ZERO_FILL     = "silent_zero_fill"     # BloFin/MEXC pattern
    DUPLICATE_CLIENT_ID  = "duplicate_client_id"
    UNKNOWN              = "unknown"
```

### 5.2 ExchangeOrderResponse — modified

```python
class ExchangeOrderResponse(BaseModel):
    exchange: Literal["OKX","BYBIT","MEXC","BLOFIN"]
    order_id: str
    side: Literal["buy","sell"]
    requested_size_usd: float = Field(gt=0)        # NEW
    filled_size_usd: float = Field(ge=0)            # tightened: was optional
    fill_price: float | None = None
    fees_usd: float = 0.0
    status: OrderStatus                             # NEW (replaces success: bool)
    rejection_code: RejectionCode | None = None     # NEW
    rejection_message: str | None = None            # NEW (capped 500 chars)
    raw: dict
    received_at_ms: int                             # NEW
    exchange_ts_ms: int | None = None               # NEW

    @model_validator(mode="after")
    def _consistency(self) -> "ExchangeOrderResponse":
        if self.status == OrderStatus.FILLED:
            assert (self.requested_size_usd - self.filled_size_usd) / self.requested_size_usd < 0.01
        if self.status == OrderStatus.PARTIALLY_FILLED:
            assert 0 < self.filled_size_usd < self.requested_size_usd
        if self.status in (OrderStatus.REJECTED, OrderStatus.CANCELLED):
            assert self.filled_size_usd == 0
            assert self.rejection_code is not None
        return self

    @property
    def success(self) -> bool:
        """Transitional shim — kept until all callers migrate to .status."""
        return self.status in (OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED)
```

### 5.3 FillRecord — modified

```python
class FillRecord(BaseModel):
    # ... existing fields ...
    requested_size_usd: float = Field(gt=0)         # NEW
    status: OrderStatus                              # NEW
    rejection_code: RejectionCode | None = None      # NEW
    is_taker: bool = True                            # NEW (placeholder)
```

### 5.4 SQLite migration

Additive columns on `fills` and `order_responses` tables: `requested_size_usd`, `status`, `rejection_code`, `rejection_message`, `received_at_ms`, `exchange_ts_ms`. All `DEFAULT NULL`. Backfill `status='filled'` for legacy rows where `success=1`, `status='rejected'` where `success=0`.

## 6. Per-exchange status mapping

Single source of truth: `normalizers.py`. Tested against captured fixtures.

| Exchange | Field(s) read | Mapping |
|---|---|---|
| **OKX** | `state` | `filled` → FILLED; `partially_filled` → PARTIALLY_FILLED; `canceled` → CANCELLED; `mmp_canceled` → CANCELLED; `live` → SUBMITTED; otherwise UNKNOWN |
| | error code 51155 | REJECTED + COMPLIANCE_BLOCKED |
| **Bybit** | `orderStatus` | `Filled` → FILLED; `PartiallyFilled` → PARTIALLY_FILLED; `Cancelled` / `PartiallyFilledCanceled` → CANCELLED; `Rejected` → REJECTED; `New` → SUBMITTED |
| **MEXC** | `status` + `cummulativeQuoteQty` | `FILLED`+qty>0 → FILLED; `PARTIALLY_FILLED`+qty>0 → PARTIALLY_FILLED; `FILLED`+qty=0 → REJECTED + SILENT_ZERO_FILL; `CANCELED` → CANCELLED |
| **BloFin** | `state` + `filledSize` | `filled`+>0 → FILLED; `partially_filled`+>0 → PARTIALLY_FILLED; `filled`+0 → REJECTED + SILENT_ZERO_FILL; `canceled` → CANCELLED |

Classifier (after exchange-specific extraction):

```python
def classify(requested_usd, filled_usd, raw_status) -> OrderStatus:
    if filled_usd <= 0:
        return REJECTED if raw_status indicates failure else CANCELLED
    if filled_usd >= requested_usd * 0.99:   # D8 tolerance
        return FILLED
    return PARTIALLY_FILLED
```

## 7. Rejection-code mapping

`map_rejection(exchange, raw) -> tuple[RejectionCode, str]` in `normalizers.py`. Initial table:

| Pattern | RejectionCode |
|---|---|
| OKX `51155` | COMPLIANCE_BLOCKED |
| OKX `51008` / Bybit `110007` / MEXC `30005` | INSUFFICIENT_BALANCE |
| OKX `51000` lot-size error / MEXC `30001` | LOT_SIZE |
| Any HTTP 429 / Retry-After header present | RATE_LIMITED |
| BloFin `state=filled, filledSize=0` | SILENT_ZERO_FILL |
| MEXC `status=FILLED, cummulativeQuoteQty=0` | SILENT_ZERO_FILL |
| Anything else | UNKNOWN (preserve raw text in `rejection_message`) |

Extend the table as we observe new codes in production.

## 8. Symbol registry

### 8.1 Canonical form

```
CanonicalSymbol = "{BASE}USDT"   # uppercase ASCII, no separators
```

Rules:
- USDT-quoted only (v1).
- BASE is the exchange-native base name (e.g., `BTC`, `PEPE`, `1000PEPE`).
- `1000PEPE` and `PEPE` are **different canonical symbols**.

### 8.2 Types

`symbols.py` (new):

```python
ExchangeName   = Literal["OKX", "BYBIT", "MEXC", "BLOFIN"]
InstrumentType = Literal["spot", "perp"]

@dataclass(frozen=True)
class CanonicalSymbol:
    value: str
    def __post_init__(self):
        if not (self.value.endswith("USDT") and self.value.isascii() and self.value.isupper()):
            raise ValueError(f"Invalid canonical: {self.value}")
    @property
    def base(self) -> str:
        return self.value[:-4]

@dataclass(frozen=True)
class Instrument:
    canonical: CanonicalSymbol
    exchange: ExchangeName
    venue_symbol: str            # e.g. "BTC-USDT-SWAP"
    instrument_type: InstrumentType
    contract_size: Decimal
    lot_size: Decimal
    tick_size: Decimal
    min_notional_usd: Decimal
    is_tradeable: bool
    refreshed_at_ms: int

class InstrumentRegistry:
    def to_canonical(self, exchange: ExchangeName, raw_symbol: str) -> CanonicalSymbol: ...
    def to_venue(self, exchange: ExchangeName, canonical: CanonicalSymbol) -> str: ...
    def get(self, exchange: ExchangeName, canonical: CanonicalSymbol) -> Instrument | None: ...
    def supports(self, exchange: ExchangeName, canonical: CanonicalSymbol) -> bool: ...
    def round_size(self, exchange, canonical, raw_size: Decimal) -> Decimal: ...
    def round_price(self, exchange, canonical, raw_price: Decimal) -> Decimal: ...
    async def refresh(self, exchange: ExchangeName) -> None: ...
    async def refresh_all(self) -> None: ...
```

### 8.3 Per-exchange conversion

| Exchange | venue → canonical | canonical → venue |
|---|---|---|
| OKX | strip `-USDT-SWAP` / `-USDT` → upper | `{BASE}-USDT-SWAP` (perp) or `{BASE}-USDT` (spot) |
| Bybit | passthrough | passthrough |
| MEXC | strip `_` | passthrough |
| BloFin | strip `-` | `{BASE}-USDT` |

### 8.4 Refresh strategy

- **Startup:** `await registry.refresh_all()` before the trading loop opens. **Hard-fail boot if any exchange refresh fails (D5).**
- **Hourly:** background `asyncio` task refreshes each exchange independently. Failures emit `instrument_registry_stale` health event; do not crash.
- **On REJECTED + LOT_SIZE:** synchronous refresh for that exchange before next entry on that symbol.
- **TTL:** `Instrument.refreshed_at_ms` exposed; reconciler raises `instrument_metadata_stale` if any in-use instrument is >2h old.
- **Stale-with-open-position (D7):** keep the position; block new entries on that symbol until refresh succeeds.
- **Persistence:** in-memory only (D6).

### 8.5 Per-exchange instrument endpoints

| Exchange | Endpoint | Notes |
|---|---|---|
| OKX | `GET /api/v5/public/instruments?instType=SWAP` | already used in `OKXExecutor._load_contract_specs` |
| Bybit | `GET /v5/market/instruments-info?category=spot` | bot is SPOT-only on Bybit |
| MEXC | `GET /api/v3/exchangeInfo` (spot) or contract endpoint | confirm path used by bot today |
| BloFin | `GET /api/v1/market/instruments` | |

### 8.6 Lot/tick enforcement

`InstrumentRegistry.round_size` becomes the single rounding entry point. Each `ExchangeExecutor.place_market_order` calls:

```python
size_native = registry.round_size(exchange_name, canonical, raw_native_size)
notional = size_native * mark_price
if notional < instrument.min_notional_usd:
    return ExchangeOrderResponse(
        status=REJECTED,
        rejection_code=MIN_NOTIONAL,
        rejection_message=f"notional {notional} < min {instrument.min_notional_usd}",
        ...
    )
```

This replaces the OKX-only `_compute_sz` (`real_trader.py:550–599`). MEXC, Bybit, BloFin executors get the same protection without duplicated logic. **This closes the highest-priority gap from the audit.**

## 9. Reusable components from current code

These exist today and integrate cleanly without modification:

- `schemas.py` Pydantic suite — extends additively.
- `state_store.py` SQLite layer — exchange-agnostic.
- `reconciler.py` + `ExchangeFetcher` Protocol — adding a venue is registering a normalizer + implementing fills.
- `OKXExecutor._load_contract_specs` — template for the lot-rounding pattern, ported into `InstrumentRegistry`.
- Per-exchange normalizer pattern in `normalizers.py` — extension is mechanical.

## 10. Test strategy

- `tests/test_symbols.py`: round-trip per exchange; cross-exchange consistency (same base on 4 venues → same canonical); casing-insensitive registry lookup; invalid canonical raises.
- `tests/test_instrument_registry.py`: refresh from recorded fixtures; `round_size` snaps to lot; min-notional rejects sub-threshold; stale-data health event after TTL.
- `tests/test_normalizers.py` (extended): one test per exchange per `OrderStatus` from real captured response fixtures.
- `tests/test_partial_fill.py` (new): 50% fill scales `LivePosition.size_usd`; silent zero-fill emits `unparseable_response`; 99.5% fill treated as FILLED.
- `tests/fixtures/orders/{venue}/{status}.json`: captured response fixtures for each exchange × each status.

## 11. Rollout phases

This spec is implemented by `2026-04-27-canonical-types.md`, which sequences the work into a single plan with three phases:

- **Phase A — additive** (no behavior change): enums, new schema fields, registry skeleton, symbol consolidation in `live_exchange_fetcher.py`.
- **Phase B — migration**: OKX lot-rounding moved to registry; executor signatures take `requested_usd`; status enum populated end-to-end.
- **Phase C — enforcement**: lot/tick on MEXC/Bybit/BloFin (warn → enforce); reconciler partial-fill check; cleanup of `success` shim.

## 12. Future work (explicitly deferred)

- Hyperliquid in registry.
- Multi-quote-currency (USDC pairs).
- Order modify/cancel state in the lifecycle enum.
- Maker order support + `is_taker` becomes meaningful.
- `FundingPayment` canonical schema.
- Persistent registry cache for faster boot.
- Strategy-side partial-fill chase (v1 is `accept_reduced`).
- Per-venue configurable filled-tolerance (D8 is currently global 1%).
