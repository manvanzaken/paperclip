# WS Verify-Fill — Implementation Plan

**Spec:** `docs/superpowers/specs/2026-05-17-ws-verify-fill-design.md`
**Date:** 2026-05-17
**Active phase:** 0

---

## Phase 0 — Standalone PoC (this session)

**Goal:** Empirically verify MEXC WS private-channel contract works as documented and measure push latency. **No production code touched.**

### Tasks
1. Write `deploy-live/scripts/poc_mexc_ws_verify.py` standalone script
2. Implement WS login using `HMAC-SHA256(secret, apiKey + reqTime)`
3. Subscribe to `personal.filter` with `filter=order`, single-symbol scope
4. Submit a $1.5 throwaway market order via existing `MEXCExecutor.place_order` path on a deep-book pair (BTCUSDT)
5. Capture timestamps:
   - `t_submit_returned` (REST POST `/api/v1/private/order/submit` response received with order_id)
   - `t_ws_push_received` (push event for that order_id arrives)
   - `t_rest_poll_filled` (parallel REST polling sees status=FILLED)
6. Compute: `ws_push_latency_ms = t_ws_push_received - t_submit_returned`
7. Compute: `rest_poll_latency_ms = t_rest_poll_filled - t_submit_returned`
8. Immediately close position via reduce-only market order
9. Print results table + push event JSON for documentation

### Pre-flight checks before execution
- Verify Tokyo bot is NOT mid-trade (no in-flight orders on MEXC)
- Verify Tokyo account has at least $5 free margin
- Confirm BTCUSDT contract spec exists in `MEXCExecutor._contract_specs`

### Verification
- WS connection establishes within 5 s
- Login response is `success`
- Subscribe response is `success`
- Push event arrives for the submitted order
- Push event includes `state=FILLED` (or equivalent) and fill price/volume

### Output deliverable
A short results file documenting:
- Auth/subscription contract (any deviations from docs)
- Latency comparison (WS push vs REST poll)
- Sample push event JSON (sanitized)
- Recommendation: proceed to Phase 1 (or adjust design)

### Rollback / cleanup
- Script is standalone, not deployed
- Throwaway position closed immediately after measurement
- Total cost: ~$0.005 in fees

---

## Phase 1 — MEXC tracker + integration (FUTURE SESSION)

**Goal:** Production MEXC WS verify-fill behind feature flag.

### Tasks (high-level — refine after Phase 0 findings)
1. Add `MEXCOrderStateTracker` class in `real_trader.py` near `MEXCExecutor`
2. Lifecycle: created at startup if `ENABLE_WS_VERIFY_FILL_MEXC=true`, started as background task, gracefully closed on shutdown
3. Add `WSPushMissed` exception class
4. Modify `MEXCExecutor._verify_fill` to use tracker when flag enabled, with `WSPushMissed` → legacy fallback
5. Rename current `_verify_fill` body to `_verify_fill_legacy` (no change to logic)
6. Add tests:
   - Tracker register/await/correlation
   - Reconnect handler issues REST sweep
   - Fallback fires on push timeout
   - Race: REST fallback + WS push both fire → no double-process
7. Update env-config docs

### Files touched
- `real_trader.py`: ~150 lines added (new class), ~10 lines modified (`_verify_fill`)
- `deploy-live/tests/test_mexc_ws_tracker.py`: new
- `setup.md` or `deployment.md`: document new env flag

### Verification
- All existing tests pass (regression)
- New tests pass
- Bot starts with flag OFF — behavior byte-identical
- Bot starts with flag ON — connects to WS, subscribes, processes pushes
- Synthetic push-timeout test triggers legacy fallback correctly

### Deploy
- Push to Tokyo with flag OFF (no behavior change)
- Manually flip flag to ON via `bot_config_live.json` after restart
- Watch logs for `[ws-tracker] fill received order_id=...` events vs `[ws-tracker] fallback REST poll` events

---

## Phase 2 — BloFin tracker + integration (FUTURE SESSION)

**Goal:** Clone Phase 1 pattern for BloFin.

### Tasks (high-level)
1. Resolve BloFin WS login signing format vs `BloFinExecutor._sign` (likely identical)
2. Add `BloFinOrderStateTracker` (subclass-or-clone of MEXC pattern)
3. Modify `BloFinExecutor._verify_fill` to use tracker behind `ENABLE_WS_VERIFY_FILL_BLOFIN`
4. Tests parallel to Phase 1
5. Deploy + flag-flip

---

## Phase 3 — Deploy + measure (FUTURE SESSION)

**Goal:** Confirm latency reduction in production.

### Tasks
1. Both feature flags ON in Tokyo `bot_config_live.json`
2. Add structured log line for every verify-fill outcome:
   ```
   [verify-fill] exchange=MEXC order_id=... source=ws_push latency_ms=42
   [verify-fill] exchange=MEXC order_id=... source=rest_fallback latency_ms=687
   ```
3. Run for 24 h with normal trading
4. Aggregate metrics:
   - WS push vs REST fallback ratio (target: >95% WS)
   - Latency distribution per source
   - Total per-leg latency before/after (target: −500 ms median)
5. Compare entry-decay metrics (actual_entry_spread vs detection_spread) before/after — does reduced latency improve fill quality?

### Success criteria
- WS push success rate >95% per exchange
- Median per-leg latency reduced by ≥400 ms
- No new categories of trade failures introduced
- Fill quality (post-fill spread decay) improved or unchanged

### Rollback
- Flip both flags to OFF in config + restart
- Code remains in place behind flags (no code rollback needed)

---

## Risk log

| Risk | Phase | Mitigation |
|---|---|---|
| MEXC auth signing differs from docs | 0 | PoC catches this before any production code |
| Push event lacks fill price/volume | 0 | If true, fall back to "push triggers REST fetch" instead of "push replaces REST" |
| BloFin WS auth turns out to be different from REST | 2 | Same PoC approach as MEXC before integration |
| Concurrent flag flip during open positions | 1, 2 | Lifecycle: tracker connects on startup only; flag flip requires restart |
| WS becomes unhealthy without disconnect | 1, 2 | Heartbeat / ping timer in tracker; force-reconnect if no message in 30s |
