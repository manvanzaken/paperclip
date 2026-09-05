# Live Trader Canonical Types — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land canonical `OrderStatus` enum + `InstrumentRegistry` so adding new exchanges beyond the current four (OKX, Bybit, MEXC, BloFin) requires only registering a normalizer + an instrument fetcher — no string manipulation, no per-venue lot-size code, no `success: bool` collapse.

**Architecture:** A new `symbols.py` module owns all venue-symbol conversion and instrument metadata. `schemas.py` gains additive `OrderStatus` / `RejectionCode` enums and the new fields on `ExchangeOrderResponse` / `FillRecord`. Each `ExchangeExecutor.place_market_order` is migrated to take `requested_usd` and return a fully-populated status-bearing response. Lot/tick enforcement is centralized in `InstrumentRegistry.round_size`, replacing OKX's bespoke `_compute_sz` and adding the same protection to MEXC/Bybit/BloFin. The `success: bool` shim survives until all callers migrate, then is removed in the cleanup task.

**Tech Stack:** Python 3.11+, asyncio, SQLite (existing `state_store`), Pydantic v2, pytest, httpx (existing).

**Working directory:** `.claude/worktrees/canonical-types/` — a new worktree branched off the current `master` after the data-reliability-cutover work merges. All paths in this plan are relative to `deploy-live/` within that worktree unless absolute.

**Spec:** `docs/superpowers/specs/2026-04-27-canonical-types-design.md`

**Builds on:**
- Data-reliability foundation, detection, and cutover plans (already merged).
- Plan 3 wiring of `LiveExchangeFetcher` and the normalizer integration into executors.

**Out of scope (per spec §3 and §12):**
- Hyperliquid integration (D4).
- Scanner data model.
- Multi-quote-currency (USDC, EUR pairs).
- Order modify/cancel state in lifecycle enum.
- Maker order support.
- Funding payment canonical schema.
- Persistent registry cache.
- Strategy-side partial-fill chase (v1 = `accept_reduced`, D2).

---

## Architectural Decisions Locked in Spec

These were resolved in the design session and are not open questions for implementers. Quoted for visibility — full rationale in spec §4.

- **D1** — Live-trader-only scope; scanner unaffected.
- **D2** — Partial-fill v1 reaction = `accept_reduced`. No chase.
- **D3** — Keep `OrderStatus.SUBMITTED` for forward-compat with future limit orders.
- **D4** — Hyperliquid out of scope.
- **D5** — Instrument refresh failure on boot → hard-fail. Bot does not start.
- **D6** — Registry is in-memory only. No SQLite persistence in v1.
- **D7** — Stale-with-open-position → keep position, block new entries on that symbol.
- **D8** — Filled tolerance = 1% globally. `filled >= requested * 0.99` → FILLED.

---

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `symbols.py` | Create | `CanonicalSymbol`, `Instrument`, `InstrumentRegistry`. Per-exchange `to_canonical` / `to_venue` private functions. |
| `schemas.py` | Modify | Add `OrderStatus`, `RejectionCode`. New fields on `ExchangeOrderResponse` and `FillRecord`. Keep `success` as derived `@property` shim. |
| `normalizers.py` | Modify | Replace `_strip_dash` callers with registry calls. Populate new status + rejection fields. Add `map_rejection`. |
| `live_exchange_fetcher.py` | Modify | Replace `_strip_swap`, MEXC `_` strip, and the `Bybit`/`BYBIT` dual key with registry calls. |
| `real_trader.py` | Modify | Migrate `ExchangeExecutor.place_market_order` signature to take `requested_usd` and return status-bearing response. Move `OKXExecutor._compute_sz` logic into `InstrumentRegistry.round_size`. Add lot/tick + min-notional checks to MEXC/Bybit/BloFin executors. Add registry initialization at startup with hard-fail. Add hourly refresh task. |
| `state_store.py` | Modify | Additive columns on `fills` and `order_responses` tables. Backfill helper. |
| `reconciler.py` | Modify | New `partial_fill_accepted` info event. New `instrument_metadata_stale` warning. New `unparseable_response` flow uses structured `rejection_code`. |
| `tests/test_symbols.py` | Create | Round-trip per exchange; cross-exchange consistency; casing; invalid canonical. |
| `tests/test_instrument_registry.py` | Create | Refresh from fixtures; `round_size`; min-notional; TTL. |
| `tests/test_partial_fill.py` | Create | Partial fill scales position; silent zero-fill emits event; 99.5% treated as FILLED. |
| `tests/test_normalizers.py` | Modify | One test per exchange per `OrderStatus`. |
| `tests/fixtures/instruments/{venue}.json` | Create | Recorded `/instruments` responses for each exchange. |
| `tests/fixtures/orders/{venue}/{status}.json` | Create | Recorded order responses per exchange × status. |
| `start.sh` | Modify | No new flags, but document the registry hard-fail behavior in startup logs. |

**File-size guidance:** `symbols.py` should stay under 300 lines (per-exchange functions + types + registry). `real_trader.py` net delta target: under +200 lines (gains lot enforcement on 3 venues, loses bespoke `_compute_sz`).

---

## Task Sequence

19 tasks in 3 phases.

- **Phase A — Additive (Tasks 1–6):** No behavior change. New types, registry skeleton, symbol consolidation in `live_exchange_fetcher.py`.
- **Phase B — Migration (Tasks 7–12):** Executor signatures take `requested_usd`. Status enum populated end-to-end. OKX lot-rounding moves to registry. Schema migration.
- **Phase C — Enforcement (Tasks 13–19):** Lot/tick on MEXC/Bybit/BloFin (warn → enforce). Partial-fill reconciler check. `success` shim removed.

---

## Phase A — Additive

### Task 1: Add `OrderStatus` and `RejectionCode` enums
- [ ] Add both enums to `schemas.py` per spec §5.1.
- [ ] No callers yet. Existing code unaffected.
- [ ] Test: import + value enumeration.

### Task 2: Extend `ExchangeOrderResponse` with new fields (additive)
- [ ] Add `requested_size_usd`, `status`, `rejection_code`, `rejection_message`, `received_at_ms`, `exchange_ts_ms` per spec §5.2.
- [ ] All new fields default to `None` except `received_at_ms` (default = current time).
- [ ] Add `model_validator` from spec but make it lenient initially (skip checks if `status is None`).
- [ ] Add `success` property shim (returns `True` if `status` ∈ {FILLED, PARTIALLY_FILLED}, else falls back to a private `_legacy_success` field for transitional rows).
- [ ] Test: legacy construction still works; new construction validates.

### Task 3: Extend `FillRecord` with new fields (additive)
- [ ] Add `requested_size_usd`, `status`, `rejection_code`, `is_taker` per spec §5.3.
- [ ] All optional/defaulted; existing tests unchanged.

### Task 4: SQLite migration — additive columns
- [ ] Add columns to `fills` and `order_responses` per spec §5.4. All `DEFAULT NULL`.
- [ ] Backfill `status='filled'` where `success=1`, `status='rejected'` where `success=0`.
- [ ] Add `upsert_fill_with_status` helper; old `upsert_fill` keeps working.
- [ ] Test: migration applied to fixture DB; legacy rows readable.

### Task 5: Create `symbols.py` skeleton with `CanonicalSymbol` + `Instrument`
- [ ] New file per spec §8.2.
- [ ] `CanonicalSymbol.__post_init__` validates form.
- [ ] `Instrument` is a frozen dataclass with `Decimal` fields.
- [ ] Per-exchange private functions `_to_canonical_okx`, `_to_canonical_bybit`, etc. per spec §8.3.
- [ ] `_to_venue_okx`, etc.
- [ ] Registry stub with `to_canonical` / `to_venue` only (no refresh, no metadata yet).
- [ ] Test: round-trip per exchange; cross-exchange consistency on BTC/ETH/1000PEPE/BONK; invalid canonical raises.

### Task 6: Replace duplicate symbol-stripping with registry calls
- [ ] In `live_exchange_fetcher.py`: replace `_strip_swap`, `_strip_dash` callers, MEXC `_` replace, with `registry.to_canonical(exchange, raw)`.
- [ ] Kill the `Bybit`/`BYBIT` duplicate key in `_NORMALIZERS` — registry is case-insensitive at the boundary.
- [ ] In `normalizers.py`: replace `_strip_dash` callers with registry.
- [ ] Behavior must be byte-identical. Add a snapshot test capturing canonical outputs for representative venue inputs and asserting unchanged.
- [ ] **Phase A complete:** ship; observe one trading day; confirm zero diff in canonical symbol outputs vs production logs.

---

## Phase B — Migration

### Task 7: Implement `InstrumentRegistry.refresh()` per exchange
- [ ] OKX: port logic from `OKXExecutor._load_contract_specs` (`real_trader.py:527–599`) into a private `_refresh_okx`.
- [ ] Bybit: implement `_refresh_bybit` against `/v5/market/instruments-info?category=spot`.
- [ ] MEXC: implement `_refresh_mexc` against the bot's existing instrument endpoint.
- [ ] BloFin: implement `_refresh_blofin`.
- [ ] All four populate `Instrument` with `lot_size`, `tick_size`, `min_notional_usd`, `contract_size`, `is_tradeable`.
- [ ] Add fixtures under `tests/fixtures/instruments/`.
- [ ] Test: `refresh()` from fixture populates registry correctly; `get()` returns expected `Instrument`.

### Task 8: Add `round_size`, `round_price`, `supports`, `min_notional_check` to registry
- [ ] `round_size` snaps a raw `Decimal` size to `lot_size` (floor for buys, by default).
- [ ] `round_price` snaps to `tick_size`.
- [ ] `supports(exchange, canonical)` returns `is_tradeable`.
- [ ] Test: each helper against fixture data.

### Task 9: Wire registry into bot startup with hard-fail (D5)
- [ ] In `real_trader.py` startup: instantiate `InstrumentRegistry`, call `await registry.refresh_all()` before the trading loop opens.
- [ ] Any exception → log + `sys.exit(1)`. Document in startup logs.
- [ ] Add hourly refresh `asyncio` task. Per-exchange failure emits `instrument_registry_stale` health event; does not crash.
- [ ] Test: refresh-failure path exits cleanly; hourly task survives one exchange outage.

### Task 10: Migrate `OKXExecutor._compute_sz` callers to `registry.round_size`
- [ ] Replace `_compute_sz` body with a call to `registry.round_size`.
- [ ] Keep behavior identical. Snapshot test: run a day of historical orders through both implementations; sizes must match exactly.
- [ ] If matched, delete `_compute_sz`. If not, halt and investigate.

### Task 11: Migrate executor signature: `place_market_order(symbol, side, requested_usd) -> ExchangeOrderResponse`
- [ ] Update `ExchangeExecutor` ABC and all four concrete classes.
- [ ] Threads the original USD intent through to `ExchangeOrderResponse.requested_size_usd`.
- [ ] Existing callers updated. Tests updated.

### Task 12: Populate `OrderStatus` end-to-end via normalizers
- [ ] Update each normalizer in `normalizers.py` per spec §6.
- [ ] Add `map_rejection(exchange, raw) -> tuple[RejectionCode, str]` per spec §7.
- [ ] Tighten the `model_validator` on `ExchangeOrderResponse` — `status` now required.
- [ ] All `ExchangeOrderResponse` instances now carry status. `.success` shim still works for unmigrated callers.
- [ ] Test: one fixture per exchange × per status; assert mapping.
- [ ] **Phase B complete:** ship; deploy to paper-trader first; verify all 4 executors emit valid `OrderStatus` for one trading day. Diff sizes from Task 10 must remain zero.

---

## Phase C — Enforcement

### Task 13: Add lot/tick + min-notional check to MEXC executor (warn-only)
- [ ] In `MEXCExecutor.place_market_order`: call `registry.round_size` before submission; check `min_notional_usd`.
- [ ] Behind `SIZE_ENFORCEMENT=warn` env var (default).
- [ ] In `warn` mode: log `WARN size would have been rounded from X to Y` but submit the original. Log every divergence.
- [ ] Test: warn path emits structured log; original behavior preserved.

### Task 14: Add lot/tick + min-notional check to Bybit and BloFin executors (warn-only)
- [ ] Mirror Task 13 for `BybitExecutor` and `BloFinExecutor`.
- [ ] Run all three in warn mode for 24h.
- [ ] Review divergence logs. Any unexpected pattern → halt and investigate.

### Task 15: Flip `SIZE_ENFORCEMENT=enforce`
- [ ] Default flips to `enforce`. Sizes are rounded before submission. Sub-threshold orders return `REJECTED + MIN_NOTIONAL` locally without hitting the venue.
- [ ] Test: rejected orders return correct enum; never reach the network.

### Task 16: Partial-fill handling in `LivePosition` open path (D2 = accept_reduced)
- [ ] When `status == PARTIALLY_FILLED`: scale `LivePosition.size_usd` to actual `filled_size_usd`; do not chase.
- [ ] Emit `partial_fill_accepted` info event via reconciler.
- [ ] Test: 50% fill scales position correctly; no second order placed.

### Task 17: Reconciler check — silent partial fills
- [ ] After T+30s, any open `LivePosition` whose summed fills < `requested_size_usd * 0.99` raises `partial_fill_unhandled` warning.
- [ ] Should be rare given Task 16, but catches paths that bypass `LivePosition` accounting.
- [ ] Test: simulate orphan partial fill; assert event raised.

### Task 18: Stale-on-open-position policy (D7)
- [ ] `instrument_registry_stale` for a symbol with an open position → block new entries on that symbol via `registry.supports()` returning False until refresh succeeds.
- [ ] Existing positions untouched.
- [ ] Test: simulate stale + open position; new entry attempt blocked; existing position unaffected.

### Task 19: Remove `success` shim
- [ ] Delete `ExchangeOrderResponse.success` property and `_legacy_success` field.
- [ ] All callers must use `.status`. Failing build = remaining caller.
- [ ] Backfilled DB rows already have `status` populated from Task 4.
- [ ] **Phase C complete:** ship; one week clean live data; close the plan.

---

## Validation Gates

End-of-phase checks before proceeding:

- **End of Phase A:** zero diff between pre- and post-consolidation canonical symbol outputs across one production trading day's logs. Tests green.
- **End of Phase B:** OKX size diff (Task 10) is zero. All four executors emit valid `OrderStatus` in paper for one day. `model_validator` strict mode active.
- **End of Phase C:** no `partial_fill_unhandled` events for 7 consecutive days. No `MIN_NOTIONAL` rejections that would have succeeded under the old code path. `success` references in repo = 0 (verified by grep).

---

## Backout

Each phase is independently revertible:

- **Phase A:** revert symbol consolidation; old stripping functions return. Schema additions are additive; harmless.
- **Phase B:** revert executor signature change + status population. `success` shim survives, callers still work. SQLite columns stay (they are nullable).
- **Phase C:** revert `SIZE_ENFORCEMENT` flip to `warn`; revert partial-fill scaling; the `success` shim resurrection requires reverting Task 19 explicitly.

A full backout = revert all three phases. SQLite schema migration is additive only and stays in place.

---

## Open Operational Questions (resolve before Task 9)

These do not block Phase A but must be answered before the registry goes live in startup:

1. **Refresh failure timeout.** How long does `refresh_all()` wait per exchange before declaring failure? Recommend 10s per exchange, 60s total.
2. **Refresh concurrency.** Refresh all four exchanges in parallel or serially? Recommend parallel via `asyncio.gather` to keep boot under 30s.
3. **MEXC instrument endpoint confirmation.** Spec §8.5 lists `/api/v3/exchangeInfo` (spot) but the bot uses MEXC for perp. Confirm exact endpoint by inspecting current `MEXCExecutor` code before Task 7.

---

## Future Plans (not part of this one)

- Hyperliquid registry integration.
- Multi-quote-currency support (USDC, EUR pairs).
- Persistent registry cache for sub-second boot.
- Strategy-side partial-fill chase (replaces D2 accept_reduced).
- `FundingPayment` canonical schema.
- Maker order lifecycle support.
