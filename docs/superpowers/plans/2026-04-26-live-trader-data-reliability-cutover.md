# Live Trader Data Reliability — Plan 3 of 3 (Cutover & Decommission)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire Plan 2's detection layer into `real_trader.py` and `dashboard.py`, ship a feature-flagged shadow-mode rollout, perform the safe cutover from file-based persistence to SQLite as canonical state, and decommission the legacy file path after a watch window.

**Architecture:** A new `live_exchange_fetcher.py` adapts the bot's existing `ExchangeExecutor` classes to the `ExchangeFetcher` Protocol Plan 2 expects. Each `ExchangeExecutor.place_market_order` is wrapped to call its matching normalizer, returning the validated `ExchangeOrderResponse` alongside (initially) the legacy `OrderResult`. A new `shadow_writer.py` mirrors every state mutation into SQLite while the file-based path remains authoritative. A new `USE_SQLITE_STATE` env flag flips reads from files to SQLite on cutover. `AlertDispatcher` is initialized once at startup; reconciler triggers fire after every order placement plus on a periodic sweep task; `invariants.check_all` runs at end-of-cycle. The dashboard gains a reconciliation events panel and an invariant status board.

**Tech Stack:** Python 3.11+, asyncio, SQLite (Plan 1's `state_store`), pydantic v2 (Plan 1's `schemas`), pytest, httpx (for `TelegramSink`).

**Working directory:** `.claude/worktrees/data-reliability-cutover/` — a new worktree branched off `claude/data-reliability-detection` (Plan 2's branch). All file paths in this plan are relative to `deploy-live/` within that worktree unless absolute.

**Spec:** `docs/superpowers/specs/2026-04-25-live-trader-data-reliability-design.md`

**Builds on:**
- Plan 1: `docs/superpowers/plans/2026-04-25-live-trader-data-reliability-foundation.md` (state_store, schemas, migration)
- Plan 2: `docs/superpowers/plans/2026-04-26-live-trader-data-reliability-detection.md` (normalizers, reconciler, invariants, alerts, replay tests)
- Plan 2 wiring notes: `deploy-live/PLAN2_NOTES.md`

**Out of scope (deferred to later phases per spec):**
- Auto-correction on reconciliation events (orphan close, phantom drop, size adjust) — Phase 1.
- Halt-on-invariant-violation policy — Phase 1.
- Paper trader migration to SQLite — deferred.
- Always-on production recording — on-demand only for Phase 0.
- WebSocket order-book message validation — read-only feed, lower priority.

---

## Architectural Decisions Made Inline

User authorized me to decide the open behavioral choices left by Plan 2 (recorded in `PLAN2_NOTES.md` §8) and a handful of new ones. These are baked into the tasks below; recording them here so the rationale is visible.

### From PLAN2_NOTES §8

- **8a — MEXC normalizer fallback semantics.** Change to `success=False` on the bad-data path (mirroring BloFin). Rationale: the silent-USD-mismatch is exactly the bug class Phase 0 is designed to eliminate. Better to fail loud than to record a misleading USD figure. Implemented in Task 1.
- **8b — Reconciler write idempotency.** Update existing unresolved row + bump `repeat_count`/`last_seen_ms` instead of inserting duplicates. Rationale: dedup at the DB level prevents row blow-up; AlertDispatcher's 60s window already covers Telegram dedup but the DB shouldn't grow unbounded for the same condition. Schema gets two new columns. Implemented in Task 2.
- **8c — Stale OK invariant gating.** Gate on a runtime flag `sweep_loop_started` (set when `start_periodic_sweep` schedules its first iteration). Rationale: avoids noisy alerts during local dev / startup window; correctness preserved in production where the sweep always starts. Implemented in Task 3.
- **8d — Unreachable-exchange masks checks.** Add an `unchecked_exchange` info-severity event whenever a reconcile cycle skips diff because of `ConnectionError`. Rationale: makes the gap visible without escalating; underlying `exchange_unreachable` already alerts at error/critical. Implemented in Task 4.
- **8e — Event-loop blocking in reconcile.** Wrap `ExchangeFetcher` calls in `loop.run_in_executor` from inside `reconcile_exchange` — keep SQLite I/O synchronous on the loop thread (it's microseconds at this scale). Rationale: pragmatic. Full async refactor is large; the actual blocking is HTTP, not SQLite. Stalls drop from ~400ms to ~5ms per sweep cycle. Implemented in Task 5.
- **8f — AsyncStateStore connection sharing.** No change required given 8e's resolution (SQLite stays single-thread). Documented inline.
- **8g — Replay coverage gaps.** Add a `size_mismatch` golden fixture in Task 16. Skip the normalizer-twin fixture for `asymmetric_fill_blofin_silent` (current coverage is acceptable; defer to a future plan).

### New decisions for Plan 3

- **Shadow writer is opt-in via `SHADOW_SQLITE=true`, default off.** Cutover sequence sets it on for the watch window, then `USE_SQLITE_STATE=true` for the flip, then both off after decommission. Two flags to keep the three rollout states clean.
- **Reads from SQLite are governed by `USE_SQLITE_STATE`.** Writes are mirrored whenever `SHADOW_SQLITE` is on (regardless of read flag). When `USE_SQLITE_STATE=true`, SQLite is also the source of truth and file writes still happen for the duration of the watch window (belt-and-braces).
- **Telegram credentials come from existing `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` env vars.** No new secrets. `TelegramSink` is gated on these being non-empty; otherwise dispatcher uses `ConsoleSink` only.
- **Reconciliation events panel is a new dashboard route, not inline.** Keeps the existing dashboard UI stable during cutover. Linked from the main page.
- **Migration is run manually under tmux with the bot stopped.** Not automated. The trader's start.sh checks the SQLite file exists before starting if `USE_SQLITE_STATE=true`; aborts loudly otherwise.
- **Backout = stop bot, set `USE_SQLITE_STATE=false`, restart.** SQLite file kept on disk as audit material (not deleted on backout).
- **Decommission (Task 22) requires explicit human approval after watch window.** Not driven by an automated metric.

---

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `live_exchange_fetcher.py` | Create | Concrete `ExchangeFetcher` Protocol implementation that delegates to the trader's existing `ExchangeExecutor` classes. |
| `shadow_writer.py` | Create | Mirror writes from file-based path to SQLite when `SHADOW_SQLITE=true`. |
| `dashboard_recon.py` | Create | Dashboard route + templates for the reconciliation event log + invariant status board. |
| `dashboard.py` | Modify | Mount the new recon route; switch read source to SQLite when `USE_SQLITE_STATE=true` (with file fallback). |
| `real_trader.py` | Modify | Wire normalizers into each `ExchangeExecutor.place_market_order`; wire reconciler triggers, invariants, alerts at startup; gate everything behind feature flags. |
| `normalizers.py` | Modify | Apply 8a fix (MEXC fallback → success=False). |
| `state_store.py` | Modify | Add `repeat_count` + `last_seen_ms` columns to `reconciliation_events`; helper `upsert_recon_event`. |
| `reconciler.py` | Modify | Apply 8b (upsert), 8c (sweep_loop_started gate), 8d (unchecked_exchange event), 8e (run_in_executor for fetcher calls). |
| `invariants.py` | Modify | Apply 8c (gate `_check_stale_ok_exchange_health` on `sweep_loop_started` flag). |
| `requirements.txt` | Modify | Add `httpx>=0.27`. |
| `start.sh` | Modify | Pre-start guard: if `USE_SQLITE_STATE=true`, abort if SQLite file missing. |
| `tests/test_live_exchange_fetcher.py` | Create | Tests against fake `ExchangeExecutor`. |
| `tests/test_shadow_writer.py` | Create | Tests for mirror writes; idempotency; failure isolation. |
| `tests/test_dashboard_recon.py` | Create | Render tests for the new dashboard route. |
| `tests/test_normalizers.py` | Modify | Update MEXC fallback test to expect `success=False`. |
| `tests/test_reconciler.py` | Modify | Add tests for upsert idempotency, unchecked_exchange events, sweep-gating. |
| `tests/test_invariants.py` | Modify | Add tests verifying `_check_stale_ok_exchange_health` is suppressed when `sweep_loop_started=False`. |
| `tests/fixtures/replay/size_mismatch_partial_fill/` | Create | Golden fixture for the size-mismatch detection path. |
| `.github/workflows/replay-gate.yml` | Create | CI gate that runs `pytest tests/test_replay.py`. |
| `docs/superpowers/specs/2026-04-XX-cutover-runbook.md` | Create at cutover | One-time artifact: migration quarantine summary + watch-window notes. |

**File-size guidance:** `real_trader.py` is 3,413 lines today. Plan 3 must add ~150 lines and remove ~50 (legacy persist helpers eventually). Net delta target: under +120 lines. New modules should each stay under 250 lines.

---

## Task Sequence

22 tasks. Numeric prefix indicates phase boundary.

- **Tasks 1–5** — Apply Plan 2 review decisions (8a–8e) inline as small commits.
- **Tasks 6–8** — `LiveExchangeFetcher` + `shadow_writer` + normalizer wiring (no behavior change yet).
- **Tasks 9–13** — Wire reconciler triggers, invariants, alerts into `real_trader.py` behind flags.
- **Tasks 14–16** — Dashboard recon panel + size_mismatch fixture + CI gate.
- **Tasks 17–19** — Shadow-mode rollout: deploy, observe, tune.
- **Tasks 20–21** — Cutover runbook + watch window.
- **Task 22** — Decommission file path.

---

## Task 1: MEXC normalizer — fail loud on missing USD field (8a)

**Files:**
- Modify: `normalizers.py`
- Modify: `tests/test_normalizers.py`

- [ ] **1.1** Update the existing test `test_mexc_normalizer_*_missing_usd_field_*` (or add one if absent) to assert `r.success is False` and `r.filled_size_usd == 0.0` when `cummulativeQuoteQty` is `"0"` but `executedQty > 0` and `status == "FILLED"`.
- [ ] **1.2** Run `pytest tests/test_normalizers.py -v` — confirm red for the new assertion.
- [ ] **1.3** In `normalize_mexc_order`, when `cummulativeQuoteQty` parses to `0.0` and `executedQty > 0` and `status in _MEXC_FILLED_STATUSES`, set `filled_size_usd=0.0` and `success=False`. Do NOT fall back to `executedQty`.
- [ ] **1.4** Run tests — green.
- [ ] **1.5** Commit: `fix(normalizers): MEXC fails loud when cummulativeQuoteQty missing on FILLED`

---

## Task 2: Recon event upsert (8b)

**Files:**
- Modify: `state_store.py`
- Modify: `reconciler.py`
- Modify: `tests/test_state_store.py`
- Modify: `tests/test_reconciler.py`

- [ ] **2.1** In `state_store.py`, extend the `reconciliation_events` schema DDL with `repeat_count INTEGER NOT NULL DEFAULT 1` and `last_seen_ms INTEGER NOT NULL DEFAULT 0`. Add a partial unique index: `CREATE UNIQUE INDEX IF NOT EXISTS uniq_recon_unresolved_key ON reconciliation_events(source, category, COALESCE(exchange,''), COALESCE(symbol,''), COALESCE(position_id,-1)) WHERE resolution='unresolved';`
- [ ] **2.2** Add a one-shot in-place migration function `_migrate_recon_event_columns(conn)` invoked from `init_schema` that adds columns if missing and backfills `last_seen_ms = timestamp` on existing rows.
- [ ] **2.3** Add helper `upsert_recon_event(conn, *, ...) -> tuple[int, bool]` returning `(event_id, was_insert)`. On conflict with the partial unique index, UPDATE `last_seen_ms` and `repeat_count = repeat_count + 1` instead of inserting; return existing id and `False`.
- [ ] **2.4** Add tests in `test_state_store.py` for: insert path, conflict-update path, resolution='manual' breaks the dedup (a new identical event after resolution can be inserted).
- [ ] **2.5** In `reconciler.py`, replace `write_recon_event` calls with `upsert_recon_event`. Update `test_reconciler.py` for any tests that asserted row-count semantics; assert `repeat_count` instead.
- [ ] **2.6** Run tests — green.
- [ ] **2.7** Commit: `feat(state-store): upsert reconciliation events to dedupe unresolved repeats`

---

## Task 3: Sweep-loop-started gate for stale-OK invariant (8c)

**Files:**
- Modify: `invariants.py`
- Modify: `reconciler.py`
- Modify: `tests/test_invariants.py`

- [ ] **3.1** In `invariants.py`, add module-level mutable state `_runtime = {"sweep_loop_started": False}` and exported helpers `mark_sweep_started()` / `is_sweep_started()`. Gate `_check_stale_ok_exchange_health` to short-circuit (return `[]`) when `is_sweep_started() is False`.
- [ ] **3.2** In `reconciler.start_periodic_sweep`, call `invariants.mark_sweep_started()` before entering the loop.
- [ ] **3.3** Add `tests/test_invariants.py` cases: stale-OK invariant returns empty when not started; returns violations when started; idempotent across multiple starts.
- [ ] **3.4** Run tests — green.
- [ ] **3.5** Commit: `feat(invariants): gate stale-OK exchange-health check on sweep loop running`

---

## Task 4: `unchecked_exchange` event on connection failure (8d)

**Files:**
- Modify: `reconciler.py`
- Modify: `tests/test_reconciler.py`

- [ ] **4.1** In `reconcile_exchange`'s `ConnectionError` branch, after writing the existing `exchange_unreachable` event, also `upsert_recon_event(category='unchecked_exchange', severity='info', exchange=ex, notes='diff checks skipped this cycle')`.
- [ ] **4.2** Add test: simulate fetcher raising `ConnectionError`; assert both `exchange_unreachable` (error) and `unchecked_exchange` (info) rows present.
- [ ] **4.3** Run tests — green.
- [ ] **4.4** Commit: `feat(reconciler): emit info-severity unchecked_exchange when diff is skipped`

---

## Task 5: Off-load fetcher I/O to executor (8e)

**Files:**
- Modify: `reconciler.py`
- Modify: `tests/test_reconciler.py`

- [ ] **5.1** Convert `reconcile_exchange` to `async def`. Wrap each `fetcher.get_*` call with `await asyncio.get_running_loop().run_in_executor(None, lambda: fetcher.get_X(...))`. Keep SQLite calls synchronous (single-thread loop, microsecond duration).
- [ ] **5.2** Update `start_periodic_sweep` and `schedule_per_trade_reconcile` to `await reconcile_exchange(...)` (both already operate inside asyncio Tasks).
- [ ] **5.3** Update tests that call `reconcile_exchange(...)` directly to `await` it; mark them `@pytest.mark.asyncio`.
- [ ] **5.4** Run `pytest tests/test_reconciler.py -v` — green. Verify `pytest-asyncio` is in `requirements.txt`; add if missing.
- [ ] **5.5** Commit: `perf(reconciler): off-load fetcher HTTP to executor; reconcile_exchange is async`

---

## Task 6: `LiveExchangeFetcher` adapter

**Files:**
- Create: `live_exchange_fetcher.py`
- Create: `tests/test_live_exchange_fetcher.py`

- [ ] **6.1** Write tests against a `FakeExecutor` that records method calls and returns canned data:
  - `get_open_positions("MEXC")` → list of (symbol, side, size_usd) tuples normalized from the executor's position-listing response.
  - `get_recent_fills("BLOFIN", since_ms=...)` → list of dicts compatible with `ExchangeOrderResponse`.
  - `get_balance("OKX")` → `BalanceSnapshot` with available + locked.
  - All three methods raise `ConnectionError` when the fake executor raises any subclass of `aiohttp.ClientError` or `asyncio.TimeoutError`.
- [ ] **6.2** Run tests — red.
- [ ] **6.3** Implement `LiveExchangeFetcher` taking `executors: Dict[str, ExchangeExecutor]` in its constructor. Each method delegates to the right executor. Methods are SYNC (the fetcher is called inside `run_in_executor` per Task 5) and use `asyncio.run` ONLY if delegation requires it; prefer wrapping each executor's existing method via a small sync adapter if the executor's method is `async def`.

  Implementation note: the trader's `ExchangeExecutor` methods are mostly `async def`. The fetcher methods should accept the executor's underlying HTTP session and call the same private helpers synchronously, OR — simpler — keep the fetcher itself async (`async def get_open_positions(...)`) and update the Protocol in `reconciler.py` to match. **Do the second.** Update `ExchangeFetcher` Protocol to declare async methods; remove `run_in_executor` from Task 5 and use `await fetcher.get_X(...)` directly. Re-verify Task 5 tests.
- [ ] **6.4** Add per-exchange normalization of position/fill/balance responses (re-use `normalizers.py` where possible; for positions and balances, map raw exchange JSON to the small dicts/snapshots expected by the reconciler).
- [ ] **6.5** Run tests — green.
- [ ] **6.6** Commit: `feat(live-fetcher): ExchangeFetcher adapter delegating to ExchangeExecutor`

**Note:** Task 6.3's late refactor of Task 5 (sync→async fetcher Protocol) is intentional — Task 5 explored the executor approach and Task 6 reveals that an async Protocol is cleaner once concrete fetcher methods are written. This pattern (decision deferred until concrete usage forces it) is cheaper than premature design.

---

## Task 7: Normalizer wiring inside `ExchangeExecutor.place_market_order`

**Files:**
- Modify: `real_trader.py`

- [ ] **7.1** For each of the four `ExchangeExecutor` subclasses (MEXC at line ~571, OKX ~396, BloFin ~922, Bybit ~741, plus the dry-run shim ~1107), find the point where the raw HTTP JSON has been parsed but before the legacy `OrderResult` is constructed.
- [ ] **7.2** Insert `normalized: ExchangeOrderResponse = normalize_<ex>_order(raw, requested_size_usd=size_usd)`. Wrap in `try / except ValidationError as e:` — on validation error, write a recon event `category='unparseable_response', severity='critical'`, log loudly, and force `OrderResult.success=False`.
- [ ] **7.3** Attach the normalized response to the legacy `OrderResult` as a new attribute `OrderResult.normalized: Optional[ExchangeOrderResponse]`. Existing code paths that consume `OrderResult` continue working unchanged.
- [ ] **7.4** Add an import block at the top of `real_trader.py` for the new modules (gated on `try / except ImportError` so the bot can still run if Plan 3 modules aren't yet deployed — defensive during rollout).
- [ ] **7.5** Smoke-run `pytest tests/` — all existing tests still pass.
- [ ] **7.6** Commit: `feat(executor): attach normalized ExchangeOrderResponse to OrderResult`

---

## Task 8: `shadow_writer.py` — mirror writes to SQLite

**Files:**
- Create: `shadow_writer.py`
- Create: `tests/test_shadow_writer.py`
- Modify: `real_trader.py` (initialize + use)

- [ ] **8.1** Tests:
  - `test_shadow_writer_disabled_does_nothing` — when `SHADOW_SQLITE=false`, no writes.
  - `test_shadow_writer_mirrors_position_open` — when on, calls to `mirror_position_open(...)` insert into `positions`.
  - `test_shadow_writer_mirrors_position_close` — updates the position row with `closed_at`, `realized_pnl_usd`, `status='closed'`.
  - `test_shadow_writer_mirrors_fill` — inserts into `fills` linked to the position.
  - `test_shadow_writer_failure_is_isolated` — if the SQLite write raises, the function logs+swallows; never raises (must not break the trader path).
- [ ] **8.2** Implement `ShadowWriter` class with methods `mirror_position_open(pos)`, `mirror_position_close(pos, pnl)`, `mirror_fill(fill)`, `mirror_audit(entry)`. Constructor takes `enabled: bool`, `db_path: str`. All methods early-return when `enabled=False`.
- [ ] **8.3** In `real_trader.py`'s startup, instantiate `ShadowWriter(enabled=os.environ.get("SHADOW_SQLITE","false")=="true", db_path=DATA_DIR/"state.db")`. Hold on the trader instance.
- [ ] **8.4** In each existing position-open / position-close / fill-record / audit-write path in `real_trader.py`, add `self.shadow.mirror_*(...)` immediately after the existing file write.
- [ ] **8.5** Run tests — green.
- [ ] **8.6** Commit: `feat(shadow): mirror file-based state writes into SQLite when SHADOW_SQLITE=true`

---

## Task 9: Initialize `state_store` and `AlertDispatcher` at startup

**Files:**
- Modify: `real_trader.py`

- [ ] **9.1** At trader construction (`LiveTrader.__init__` ~line 2375), open the SQLite connection: `self.state_conn = state_store.open_db(str(DATA_DIR/"state.db")); state_store.init_schema(str(DATA_DIR/"state.db"))`. Always; the connection is harmless when neither flag is on (only the shadow writer + recon code will use it).
- [ ] **9.2** Build the `AlertDispatcher`: `self.alerts = AlertDispatcher(dedup_window_s=60.0)`. Add `ConsoleSink()` always; add `TelegramSink(...)` only when `TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID` are set; severity routing per spec (`info` silenced, `warn` digest hourly, `error` immediate, `critical` repeat-every-5-min).
- [ ] **9.3** **Hourly digest implementation:** since `AlertDispatcher` from Plan 2 doesn't natively digest, add a small `DigestSink` wrapper that batches `warn` events, flushes every 3600s on a background task spawned at startup. Tests in `tests/test_alerts.py`.
- [ ] **9.4** Wire shutdown cleanup: cancel digest task, await dispatcher flush, close `state_conn`.
- [ ] **9.5** Smoke test by setting `DRY_RUN=true` and starting the trader locally; check that an `info`-severity event from `ConsoleSink` appears.
- [ ] **9.6** Commit: `feat(real-trader): initialize state_store + alert dispatcher at startup`

---

## Task 10: Reconciler triggers wired into order placement

**Files:**
- Modify: `real_trader.py`

- [ ] **10.1** At trader startup, instantiate `self.fetcher = LiveExchangeFetcher(self.executors)`.
- [ ] **10.2** Spawn periodic sweep: `self.sweep_task = start_periodic_sweep(self.state_conn, self.fetcher, exchanges=EXCHANGES, interval_s=RECONCILE_INTERVAL_SEC, alert_dispatcher=self.alerts)`. Store handle for shutdown cancellation.
- [ ] **10.3** In `open_position` (line ~2123), after each leg's order placement returns, call `schedule_per_trade_reconcile(self.state_conn, self.fetcher, exchange=ex, symbol=sym, alert_dispatcher=self.alerts)`. Discard the returned task handle.
- [ ] **10.4** Same in `close_position` (line ~2218).
- [ ] **10.5** Gate steps 10.2–10.4 on `os.environ.get("SHADOW_SQLITE","false")=="true" or os.environ.get("USE_SQLITE_STATE","false")=="true"` — reconciler only runs when at least one of the two flags is on. Avoids load on instances we haven't migrated.
- [ ] **10.6** Add an integration test: spin a trader with mock executors, place a fake order, assert a reconciliation tick runs against the mock fetcher within 5s.
- [ ] **10.7** Commit: `feat(real-trader): wire reconciler triggers behind feature flag`

---

## Task 11: Invariants run at end-of-cycle

**Files:**
- Modify: `real_trader.py`

- [ ] **11.1** Add a small helper on the trader: `def _run_invariants_pass(self):` that calls `invariants.check_all(self.state_conn)` plus `invariants.check_inmem_consistency(self.state_conn, in_memory_open_count=len(self.portfolio.open_positions()))`.
- [ ] **11.2** For each violation: `upsert_recon_event(...)` then `await self.alerts.dispatch(event)`. Re-use the helper Plan 2's notes suggested (`_violation_to_event`); add it to `invariants.py` if not present.
- [ ] **11.3** Call `_run_invariants_pass` at the end of the main poll cycle, gated on the same flag as Task 10.5.
- [ ] **11.4** Add a `RateLimiter` instance on the trader (re-use the one from Plan 2's `invariants.py`) so the same `(category, position_id)` is not dispatched twice in 60s.
- [ ] **11.5** Smoke test: run trader with a fake violation seeded directly in SQLite; confirm exactly one alert dispatch per minute.
- [ ] **11.6** Commit: `feat(real-trader): run invariant checks each cycle and dispatch alerts`

---

## Task 12: `start.sh` pre-start guard

**Files:**
- Modify: `start.sh`

- [ ] **12.1** At top of `start.sh`, add:
  ```bash
  if [ "${USE_SQLITE_STATE:-false}" = "true" ] && [ ! -f "${DATA_DIR:-/data}/state.db" ]; then
      echo "FATAL: USE_SQLITE_STATE=true but ${DATA_DIR:-/data}/state.db missing" >&2
      exit 1
  fi
  ```
- [ ] **12.2** Manual verify by running `USE_SQLITE_STATE=true bash start.sh` against a directory without the DB; confirm fatal exit.
- [ ] **12.3** Commit: `chore(start): abort startup if USE_SQLITE_STATE=true without state.db`

---

## Task 13: `requirements.txt` updates

**Files:**
- Modify: `requirements.txt`

- [ ] **13.1** Append `httpx>=0.27,<1` (for `TelegramSink`).
- [ ] **13.2** Append `pytest-asyncio>=0.23` if not present (for async tests added in Task 5).
- [ ] **13.3** Run `pip install -r requirements.txt` in the worktree's venv; verify clean install.
- [ ] **13.4** Commit: `chore(deps): add httpx and pytest-asyncio`

---

## Task 14: Dashboard recon panel

**Files:**
- Create: `dashboard_recon.py`
- Modify: `dashboard.py`
- Create: `tests/test_dashboard_recon.py`

- [ ] **14.1** Tests for `dashboard_recon`:
  - `/recon/events` returns paginated event list with severity filter.
  - `/recon/invariants` returns each of the 12 invariants with last-known status (green if no unresolved event in 5 min, red otherwise).
  - When `USE_SQLITE_STATE=false` AND the state.db is missing, the route returns a friendly empty state, not a 500.
- [ ] **14.2** Implement `dashboard_recon.py` as a small Flask blueprint (matching the existing `dashboard.py` framework — verify which framework it uses; Plan 2's notes suggest Flask. If it's something else, mirror that.).
- [ ] **14.3** Mount the blueprint in `dashboard.py`. Add a navigation link in the existing main template.
- [ ] **14.4** When `USE_SQLITE_STATE=true`, `dashboard.py`'s existing routes that read positions / fills / audit also switch to reading from SQLite via `state_store` query helpers. Add a thin `_data_source()` selector at the top.
- [ ] **14.5** Run tests — green.
- [ ] **14.6** Commit: `feat(dashboard): reconciliation events panel + invariant status board`

---

## Task 15: CI gate for replay tests

**Files:**
- Create: `.github/workflows/replay-gate.yml`

- [ ] **15.1** Workflow triggered on `pull_request` and `push` to the live-trader paths. Steps: checkout, set up Python 3.11, `pip install -r deploy-live/requirements.txt`, `cd deploy-live && pytest tests/test_replay.py -v`.
- [ ] **15.2** Block merge on failure (require status check in branch protection — note as a TODO for the user; can't be set programmatically without admin token).
- [ ] **15.3** Commit: `ci(replay): gate live-trader changes on golden-fixture replay tests`

---

## Task 16: `size_mismatch_partial_fill` golden fixture

**Files:**
- Create: `tests/fixtures/replay/size_mismatch_partial_fill/`
  - `scenario.md`
  - `exchange_responses.jsonl`
  - `expected_events.json`
  - `expected_invariants.json`

- [ ] **16.1** Author the fixture: bot opens a $25 position on MEXC; MEXC partially fills $18.50 then "completes" with stale data; reconciler diff yields `size_mismatch` warn event.
- [ ] **16.2** Run `pytest tests/test_replay.py -v -k size_mismatch_partial_fill` — confirm green.
- [ ] **16.3** Commit: `test(replay): add size_mismatch_partial_fill golden fixture`

---

## Task 17: Shadow-mode deploy

**Manual procedure (no code changes).**

- [ ] **17.1** Merge the worktree branch into `master` (PR review optional; user has been autonomously approved).
- [ ] **17.2** SSH to the live trader Lightsail host. Set `SHADOW_SQLITE=true` (leave `USE_SQLITE_STATE` unset/false). Restart the bot.
- [ ] **17.3** Verify in logs: `state_store opened`, `shadow writer enabled`, `reconciler sweep started`, `alerts dispatcher initialized`.
- [ ] **17.4** Tail Telegram for the next 60 minutes — should be no new alerts (info-only is silenced; warn digest takes 60min). If error or critical fires, investigate before proceeding.
- [ ] **17.5** Note any false-positive categories in a scratch file; tune severity thresholds in a small follow-up commit if needed.

---

## Task 18: Shadow-mode watch (24–72h)

**Observation, not code.**

- [ ] **18.1** Daily check (at minimum): `sqlite3 /data/state.db "SELECT category, severity, COUNT(*) FROM reconciliation_events WHERE timestamp >= strftime('%s','now','-24 hours')*1000 GROUP BY category, severity ORDER BY 3 DESC"`.
- [ ] **18.2** Compare position counts: `sqlite3 /data/state.db "SELECT COUNT(*) FROM positions WHERE status IN ('open','opening')"` vs the live trader's in-memory count from logs. Should match exactly.
- [ ] **18.3** If divergence > 0 at any check: do NOT proceed to cutover. Investigate the divergence in a separate plan.
- [ ] **18.4** After 72h with zero critical events and matching counts, mark watch window passed. Capture the daily-counts output as `docs/superpowers/specs/2026-04-XX-cutover-runbook.md` (date filled in at cutover).

---

## Task 19: Migration dry-run

**Files:**
- Run: `migrate_to_sqlite.py` (Plan 1 deliverable)

- [ ] **19.1** SSH to host. With bot still running, copy `state.json`, `trade_history.csv`, audit logs to a scratch directory.
- [ ] **19.2** Run `python migrate_to_sqlite.py --input scratch/ --output scratch/state.db --dry-run`. Review the quarantine output (`migration_quarantine.jsonl`).
- [ ] **19.3** If quarantine count > 5%: STOP. Open an investigation; the quarantine is itself the bug surface from years of file-based persistence. Triage manually before cutover. Add a one-time data-cleanup commit if needed.
- [ ] **19.4** If quarantine count ≤ 5% (warnings ok, no errors), proceed to Task 20.

---

## Task 20: Cutover

**Manual procedure. Coordinated downtime ~5 minutes.**

- [ ] **20.1** Pre-flight: confirm Task 18 watch window passed; confirm Task 19 dry-run quarantine acceptable; confirm a known-good rollback target tag (`git tag pre-sqlite-cutover`).
- [ ] **20.2** Stop the bot (`./stop.sh` or equivalent).
- [ ] **20.3** Run real migration: `python migrate_to_sqlite.py --input /data --output /data/state.db`. Should complete in < 30s for current data volume.
- [ ] **20.4** Inspect the migration summary (positions / fills / audit / quarantine counts). Save to the runbook.
- [ ] **20.5** Set `USE_SQLITE_STATE=true` (keep `SHADOW_SQLITE=true` for belt-and-braces during the watch window).
- [ ] **20.6** Start the bot. Verify in logs: `using SQLite as canonical state`, `start.sh pre-start guard passed`.
- [ ] **20.7** Verify dashboard renders against SQLite (position list matches, recon panel populated).
- [ ] **20.8** Verify Telegram alerts route correctly: trigger a synthetic `warn` event by inserting a test reconciliation_event row, confirm digest accumulates it (or restart digest interval to ≤ 60s for the test).

---

## Task 21: Post-cutover watch (24–72h)

**Observation only.**

- [ ] **21.1** Same daily-check SQL as Task 18.
- [ ] **21.2** Confirm zero `critical` events; warns triaged; no manual operator interventions required.
- [ ] **21.3** Confirm dashboard queries serving from SQLite are < 100ms (the existing file-based dashboard was effectively instant; SQLite should match).
- [ ] **21.4** If any backout is needed during this window: `USE_SQLITE_STATE=false`, restart, file-based path resumes; SQLite kept for audit. Document the reason in the runbook and stop here.

---

## Task 22: Decommission file path

**Requires explicit human approval after Task 21 passes cleanly.**

**Files:**
- Modify: `real_trader.py` (remove file-based persistence helpers)
- Modify: `shadow_writer.py` (delete; the mirror is no longer meaningful)
- Modify: `dashboard.py` (remove file-fallback branch from `_data_source()`)

- [ ] **22.1** Wait for explicit go-ahead from the user. Do not auto-proceed even if Task 21 passes.
- [ ] **22.2** Remove `state.json` writers, `trade_history.csv` writers, file-based audit-log writers from `real_trader.py`. Replace with direct `state_store.write_*` calls.
- [ ] **22.3** Delete `shadow_writer.py` and its tests. Delete the `SHADOW_SQLITE` environment-variable branch.
- [ ] **22.4** Simplify `dashboard.py`'s `_data_source()` to always use SQLite. Remove file-reading code.
- [ ] **22.5** Move the migration script from active-use to `scripts/archived/`.
- [ ] **22.6** Run `pytest deploy-live/tests/ -v` — all green.
- [ ] **22.7** Deploy to production. Watch for one cycle to confirm steady-state.
- [ ] **22.8** Commit: `chore(decommission): remove file-based persistence; SQLite is sole source of truth`
- [ ] **22.9** Tag the release: `git tag phase-0-complete`. Phase 0 done.

---

## Definition of Done

- All 22 tasks checked.
- `pytest deploy-live/tests/` all green.
- `pytest deploy-live/tests/test_replay.py` is gating CI.
- Live trader reads + writes SQLite; no file-based persistence remains.
- Dashboard reconciliation panel live and in active use.
- 72h watch window passed both pre- and post-cutover with zero critical false positives.
- Cutover runbook artifact written to `docs/superpowers/specs/`.
- Phase 0 done criteria from spec all satisfied.

---

## Roadmap Beyond Plan 3 (recap from spec)

| Phase | Scope |
|---|---|
| Phase 1 | Runtime auto-correct — orphan close, phantom drop, size adjust. Halt-on-violation policy. |
| Phase 2 | Offline agent reads `state_store` and proposes PRs (B-mode autonomy). |
| Phase 3 | Autonomous code deployment with hard guardrails (capital cap, file allowlist, auto-rollback on PnL deviation). |
