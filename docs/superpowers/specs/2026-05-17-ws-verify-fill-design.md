# WS Verify-Fill — Design Spec

**Date:** 2026-05-17
**Scope:** Replace REST verify-fill polling with WS push-notification subscription on MEXC and BloFin
**Status:** Phase 0 in progress

## Problem

Live trader per-leg latency on Tokyo is ~900–1900 ms (median ~1000 ms). Breakdown of a single MEXC fill:

| Stage | Latency | Movable? |
|---|---|---|
| REST POST submit (Tokyo → MEXC matching engine) | ~200 ms | No — geographic |
| Verify-fill REST GET polling (`/api/v1/private/order/get/{id}`, ~300 ms cadence × 2–3 polls) | ~600–800 ms | **Yes — replace with WS push** |

Same breakdown applies to BloFin. **Total addressable latency: ~1000–1600 ms per round-trip** (both legs combined).

This matters because median spread opportunity duration on the bot's universe is ~5 s, so verify-fill polling consumes 20–30 % of every entry window. Decayed fills are the #1 contributor to losing trades (Cause #1 in the trade-loss analysis from this session).

## Non-goals

- **Order placement via WS**: MEXC and BloFin private WS channels are read-only — they push order/account state but cannot accept order-submit messages. Confirmed against `mexcdevelop.github.io/apidocs/contract_v1_en/` and BloFin's `wss://openapi.blofin.com/ws/private` docs. REST POST submit stays.
- **Market data WS**: Already in place (`book_cache` populated by public WS feeds).
- **Strategy or universe changes**: Out of scope; this spec is purely a latency reduction.

## Design

### New component: `OrderStateTracker` (one instance per exchange)

A long-lived async background task per exchange that:

1. Maintains an authenticated WS connection to the exchange's private channel:
   - MEXC: `wss://contract.mexc.com/edge`
   - BloFin: `wss://openapi.blofin.com/ws/private`
2. Performs login handshake using exchange-specific signing (MEXC: `HMAC-SHA256(secret, apiKey + reqTime)`; BloFin: existing scheme from `BloFinExecutor`).
3. Subscribes to order-state push channels (MEXC: `personal.filter` with `filter=order`; BloFin: `orders`).
4. Maintains `dict[order_id → asyncio.Event + fill_dict]` — registers pending orders, sets event when push arrives.
5. Exposes `await_fill(order_id, timeout) -> dict` — caller awaits, returns fill dict.
6. On disconnect: reconnects with exponential backoff; on reconnect, REST-fetches state of all registered pending orders to recover any missed push events.

### Integration with existing executors

```python
# OLD (real_trader.py:1576 for MEXC, :2036 for BloFin)
async def _verify_fill(self, order_id, timeout=10.0):
    while time.time() - start < timeout:
        resp = await self._rest_get_order(order_id)
        if resp['state'] == 'FILLED':
            return resp
        await asyncio.sleep(0.3)
    raise TimeoutError

# NEW
async def _verify_fill(self, order_id, timeout=10.0):
    if not self._ws_verify_enabled:                # feature flag
        return await self._verify_fill_legacy(order_id, timeout)

    self._tracker.register_pending(order_id)
    try:
        return await self._tracker.await_fill(order_id, timeout=WS_PUSH_TIMEOUT_SEC)
    except WSPushMissed:                            # fallback if WS dies or times out
        return await self._verify_fill_legacy(order_id, timeout)
```

### Failure modes + mitigations

| Failure | Detection | Mitigation |
|---|---|---|
| Auth handshake bug | First-connect failure | Feature flag default-OFF. Per-exchange independent flags. |
| Push event missed (network drop, deserialize error) | `await_fill` timeout > `WS_PUSH_TIMEOUT_SEC` (1.5 s) | Fallback to legacy REST polling. Log every fallback as a metric. |
| WS reconnect drops in-flight events | Reconnect handler triggers | On reconnect, REST sweep `get_open_orders` for all registered IDs. |
| Race: push arrives during REST fallback | Both paths set the same `Event` | Whichever fires first wins. Other is silently ignored. Outcome is identical. |
| Auth token expiry mid-session | Login response includes expiry; some exchanges silently disconnect | Track expiry, re-auth proactively (or re-login on every reconnect). |
| Single WS connection becomes bottleneck | N/A (low order rate; 1 trade/min max) | None needed at current volume. |

### Feature flags (env-overridable)

| Flag | Default | Effect |
|---|---|---|
| `ENABLE_WS_VERIFY_FILL_MEXC` | `false` | Use WS push for MEXC verify-fill |
| `ENABLE_WS_VERIFY_FILL_BLOFIN` | `false` | Use WS push for BloFin verify-fill |
| `WS_PUSH_TIMEOUT_SEC` | `1.5` | How long to wait for WS push before falling back to REST |

All defaults preserve current behavior. Activation is per-exchange and reversible via config flip + restart.

## Phased rollout

| Phase | What | Verifiable end-state | Deployable? |
|---|---|---|---|
| **0** | Standalone PoC: MEXC WS auth + subscribe + receive 1 push event for a known REST-submitted $1 throwaway order. Measure WS-push latency vs REST-poll latency. | Push received with timing data captured. Auth/subscription contract verified. | No — research script |
| **1** | `MEXCOrderStateTracker` class + integration into `MEXCExecutor._verify_fill` behind `ENABLE_WS_VERIFY_FILL_MEXC=false` flag. Unit + integration tests. | Bot runs with flag OFF (byte-identical to current). Activating flag uses WS. Latency improves. Fallback path tested. | Yes — flag-gated |
| **2** | `BloFinOrderStateTracker` (clone of MEXC pattern with BloFin signing). Integration behind `ENABLE_WS_VERIFY_FILL_BLOFIN=false`. | Same as Phase 1 for BloFin. | Yes — flag-gated |
| **3** | Tokyo deploy. Both flags ON. Instrumented logging compares per-leg latency before/after. Run 24 h. | Latency reduction confirmed. No regressions in fill/abort/orphan rates. | Yes — production |

## Open questions (to resolve during Phase 0)

1. **MEXC subscription scope**: subscribe per-symbol or wildcard? Per-symbol minimizes noise but requires re-subscribing on new symbols. Wildcard is simpler but receives every push.
2. **BloFin auth format**: confirm vs `BloFinExecutor._sign` whether WS uses identical signing or a variant.
3. **Push event field names**: does MEXC's `personal.order` push include `vol_filled` / `deal_avg_price`? If so we can replace REST polling entirely; if not we still need a REST fetch after push.
4. **Latency expectation**: measured during Phase 0. If WS push is <100 ms, the refactor is clearly worth it. If WS push is >300 ms, the savings shrink.

## Testing

- Unit tests for `OrderStateTracker.register_pending` / `await_fill` / push event correlation
- Unit test: fallback fires when no push within `WS_PUSH_TIMEOUT_SEC`
- Unit test: reconnect handler issues REST sweep for pending orders
- Integration test (live, $1 throwaway): full round-trip via WS-enabled path
- Regression test: flag-OFF behavior identical to current `_verify_fill`

## Rollback

Each phase rolls back via flag flip + systemd restart. No data migration. No schema changes. No persistent state changes.
