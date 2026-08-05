# SER-SLICE-2 Performance Implementation Audit

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_2_PERFORMANCE_IMPLEMENTATION_AUDIT
MODE=IMPLEMENTATION_AUDIT
BASELINE_HEAD=6a94b62c (+ uncommitted working-tree fixes for AR-1-001..005)
VERDICT=APPROVE_WITH_MINOR_REVISIONS
STATE_WRITE_ALLOWED=no
DIAGRAM_WRITE_ALLOWED=no
```

## Scope

Current working-tree source read in full:

- `src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap`
- `src/ortec/serial/core/zcl_abapgit_ortec_ser_cost.clas.abap`
- `src/ortec/serial/core/zcl_abapgit_ortec_ser_planner.clas.abap`
- `src/ortec/serial/rfc/zabapgit_ortec_serial.fugr.z_abapgit_ortec_ser_batch.abap`

Approved context read: `serialization_adaptive_batch_design.md`,
`serialization_slice_2_performance_scan.md`,
`serialization_slice_2_hook_audit_adversarial.md`. Also consulted the
official ABAP Keyword Documentation (`ABENLOGEXP_BOOLE`, `ABAPWAIT_UP_TO`)
to verify two claims empirically rather than by assumption (see PS-001 and
Inspection Point B below) — this is current, authoritative evidence, not a
static guess.

## §9 limit-constant classification

| # | Constant | Value | Classification | Evidence |
|---|---|---|---|---|
| 1 | `c_max_batch_rows` | 25 | **ENFORCED** | `serialize()` passes it as `iv_row_limit` to `build_initial_batches`; planner enforces `lines( <ls_best>-items ) < iv_row_limit` as a hard cap before adding a work item to a batch (zcl_abapgit_ortec_ser_planner.clas.abap, `build_initial_batches`). |
| 2 | `c_max_batch_input_bytes_est` | 8388608 | **ENFORCED** (for its declared, narrow purpose: an *advisory planning* gate) | `serialize()` passes it as `iv_byte_limit`; planner enforces `<ls_best>-total_est_bytes + ls_work_item-est_bytes <= iv_byte_limit` on the **estimated** bytes only. Never consulted as a dispatch admission gate — consistent with its own doc comment. |
| 3 | `c_max_actual_batch_bytes` | 12582912 | **DEFERRED_BY_APPROVED_SCOPE** | `before_dispatch` has a real, structurally-complete gate (`IF lv_actual_bytes > c_max_actual_batch_bytes AND lines(...) > 1` → recursive split), but `lv_actual_bytes` is hardcoded to `0` immediately above it, with an explicit, non-silent code comment citing the SER-SLICE-2 scope boundary (batch-scoped prefetch extraction on `ZCL_ABAPGIT_ORTEC_SER_PREF*` does not exist yet). Confirmed **not silently broken**: every object still gets a correct, if unbatched-prefetch, serialization (a safe "prefetch miss"), matching the design doc's own disclosure. |
| 4 | `c_max_pre_dispatch_splits` | 3 | **DECLARED_ONLY** | Grep across `src/ortec/serial/**` finds zero references outside its own declaration and a sibling doc-comment. No counter increment, no log call, no read anywhere. Matches its own doc: "TELEMETRY/WARNING COUNTER ONLY... never a reason to dispatch an over-limit group" — correctly not classified as BROKEN since no enforcement was ever claimed to exist yet. |
| 5 | `c_max_drain_wait_s` | 300 | **ENFORCED** | `check_timeouts`: `IF lv_elapsed >= ( c_batch_rfc_timeout_s + c_max_drain_wait_s ). <ls_t>-state = c_state_abandoned.` — the T→X transition is live code, reachable from the poll loop every 5s. |
| 6 | `c_max_in_flight_bytes` | 104857600 | **DECLARED_ONLY** | Grep finds zero references outside the declaration. Only a **row/dispatch-count** budget is enforced (`<ls_ctx>-in_flight` vs `<ls_ctx>-worker_count`, incremented in `dispatch_batch`, decremented in `release_in_flight_budget`) — no byte-based in-flight budget exists anywhere. Precisely: *count* is enforced, *bytes* are not tracked at all for this constant's stated purpose. |
| 7 | `c_max_object_output_bytes` | 20971520 | **DECLARED_ONLY** | `on_end_of_batch` reads `ls_row-output_bytes` (populated by the RFC worker) only to feed `update_estimate`'s EWMA — it is never compared against `c_max_object_output_bytes` anywhere. Zero consumer confirmed by grep and by full read of `on_end_of_batch`. |
| 8 | `c_oversized_threshold` | 3 | **DECLARED_ONLY** | Zero code references outside declaration/doc-comment. The class's own doc explicitly discloses this as a planner-side refinement "not implemented this slice... reserved for a future slice, not silently dropped" — same disclosed-gap character as #4, so DECLARED_ONLY rather than BROKEN. |
| 9 | `c_batch_rfc_timeout_s` | 300 | **ENFORCED** | `check_timeouts`: `IF lv_elapsed >= c_batch_rfc_timeout_s. <ls_d>-state = c_state_timed_out.` — the A→T transition is live and reachable every poll cycle. |
| 10 | `c_max_retries` | 2 | **ENFORCED** | `check_timeouts`: `IF <ls_d>-attempt < c_max_retries. before_dispatch(... attempt+1 ...) ELSE. route_to_sequential_fallback(...).` — retry-vs-fallback decision is live and correctly bounded (each bisection half in `handle_receive_failure` gets attempt+1, never resets). |

Counts: **ENFORCED=5, PARTIALLY_ENFORCED=0, DECLARED_ONLY=4, DEFERRED_BY_APPROVED_SCOPE=1, BROKEN=0.**

## Inspection points A–E

**A — circuit-breaker gate cost.** `mt_broken_runs` is `HASHED TABLE ... WITH UNIQUE KEY table_line`; `before_dispatch`'s `line_exists( mt_broken_runs[ table_line = iv_run_id ] )` is an O(1) hash lookup, evaluated once per `before_dispatch` invocation (i.e. once per dispatch/retry/bisection, not per object or per session size). **No O(n) or worse cost confirmed — clean.**

**B — per-object `MATCH_OBJ_PATTERNS` cost/gating.** In `serialize()`'s partition loop the call sits as the last operand of `is_i18n_params-main_language_only = abap_false AND it_wo_translation_patterns IS NOT INITIAL AND match_obj_patterns(...)`. Per ABAP Keyword Documentation `ABENLOGEXP_BOOLE` ("the remaining logical expressions are no longer evaluated... functional methods... that do not need to be evaluated... are not executed"), AND is short-circuited left-to-right — the method is **only invoked when both preconditions hold**, i.e. only when the feature is actually configured. `match_obj_patterns` itself (`zcl_abapgit_i18n_params.clas.abap`) is a plain in-memory `LOOP` over the pattern table with an `EXIT` on first match and no SQL/RFC — O(pattern-count) per object, worst case. Same call already exists on the standard `run_parallel`/`run_sequential` hot path (per AR-1-001's own evidence), so this reproduces existing cost, it does not add a new asymptotic class. **Confirmed correct and zero-cost-when-unused.**

**C — `NEXT_TASK_NAME` counter.** `mv_next_task_seq = mv_next_task_seq + 1.` is a scalar CLASS-DATA increment. ABAP has no OS-level threads within one internal session (RFC callbacks are processed synchronously at `WAIT`/`RECEIVE` roll-in points, never pre-emptively), so there is no real concurrent-mutation hazard and no locking is needed or present. **No cost or contention concern.**

**D — PS-001 and PS-003 reassessed precisely**

- **PS-001 (fixed 5-second poll).** Re-verified against the current implementation (`serialize()`'s `DO` loop: `WAIT UP TO 5 SECONDS.` with no `UNTIL`/`FOR ASYNCHRONOUS TASKS` qualifier) and against the ABAP Keyword Documentation for the exact statement used: `ABAPWAIT_UP_TO` states explicitly **"The variant shown here does not wait for callback routines."** — i.e. this specific plain form of `WAIT UP TO n SECONDS` always blocks the full `n` seconds and does **not** return early when an aRFC callback completes (only `WAIT FOR ASYNCHRONOUS TASKS UNTIL log_exp UP TO sec SECONDS` has that early-return property). This **upgrades PS-001 from a static suspicion to a documentation-confirmed fact**: every poll round costs a full, unavoidable 5 seconds even if the awaited batch finished in milliseconds. It is also a real drift from the design doc's own §5.3 pseudocode, which specified `WAIT UNTIL <SPBT_INSTANCE or lv_free_slot indicator changes> UP TO 5 SECONDS` (an event-aware wait), not a blind sleep. Impact is bounded and linear, not unbounded: total avoidable overhead ≈ `5s × number_of_refill_rounds`, where rounds ≈ `total_batches / concurrent_workers` — for 40,000 objects at 25 rows/batch and e.g. 10 workers, ≈160 rounds ≈ up to ~13 minutes of pure added latency on top of real work, non-catastrophic but real and easily avoidable. **Recommendation: fix before enabling this path broadly** — switch to `WAIT FOR ASYNCHRONOUS TASKS UNTIL <a per-run "something changed" flag, e.g. toggled by ON_END_OF_BATCH/CHECK_TIMEOUTS> UP TO 5 SECONDS`, matching the design's own original intent. This is a MAJOR, non-blocking finding (does not affect correctness, does not grow super-linearly), not BLOCK_PRODUCTION_SCALE.
- **PS-003 (`mt_resolved` growth).** `mt_resolved` holds one small row (run_id + obj_type + obj_name, ~60 bytes) per object **actually requested in the current run (K)**, not per object in the repository (N), and is purged in full at `purge_run_state` once the run's own dispatches all reach a terminal state. This is `HASHED TABLE`-backed set-membership, deliberately kept for the entire run's duration as the belt-and-suspenders duplicate-merge guard closing AR-1-004/cycle-2's counterexample — removing it early would reopen that exact risk. Growth is proportional to **K** (this run's own object count), never to total stored repository objects, so it does not violate the "no repository-wide reads for incremental work" rule. Even at K≈1,000,000 (a pathological single call), footprint is tens of MB, not GB. **Recommendation: accept as a documented, K-proportional design trade-off — no fix required**, unless a future measurement on a truly extreme single-call K shows real memory pressure, in which case a per-batch-drain-triggered partial purge (once every dispatch tied to a fully-merged batch is terminal) would be the natural follow-up, not a redesign.

**E — EXPORT/IMPORT DATA BUFFER wire format.** RFC worker: one `EXPORT data = ls_serialization TO DATA BUFFER ls_result-files_xstring` **per object**, appended as its own row into `ET_RESULT` (no cross-object concatenation into one shared buffer). Orchestrator: `merge_into_mt_files` does one `IMPORT ... FROM DATA BUFFER is_result-files_xstring` per object, then `LOOP AT ls_serialization-files ... APPEND INITIAL LINE ... <ls_return>-file = ls_file` — a single structure copy per file, no repeated/quadratic concatenation of a growing XSTRING. Each object's payload is copied the minimum necessary number of times (worker-side EXPORT, RFC marshal, orchestrator-side IMPORT, one structure assignment into the accumulator) — no evidence of an avoidable extra full-payload duplication. **Clean.**

## Adversarial findings (AR-1-001..005) — closure verification

All five re-verified directly against current source, not taken on faith from the earlier audit doc:

- **AR-1-001** (i18n parity): FIXED. `wo_translation_patterns` now threaded through `ty_run_context`; `serialize()`'s partition loop and `route_to_sequential_fallback` both recompute `main_language_only` via the same short-circuited `match_obj_patterns` check as the standard path.
- **AR-1-002** (dead circuit breaker): FIXED. `before_dispatch` is confirmed the single choke point for every dispatch source (initial, queue-drain, timeout-retry, receive-failure bisection all call `before_dispatch`, never `dispatch_batch` directly) and checks `mt_broken_runs` first.
- **AR-1-003** (task-name collision): FIXED. `next_task_name` now uses a session-wide monotonic `mv_next_task_seq`, independent of `run_id` content.
- **AR-1-004** (silent resolved-without-output): FIXED. `merge_into_mt_files` returns `rv_merged`; `on_end_of_batch` checks it, logs, routes the single object to `route_to_sequential_fallback` (which owns the `mt_resolved` insert), and `CONTINUE`s — never marks it resolved via the batch path.
- **AR-1-005** (avoidable wait on empty work): FIXED. `serialize()`'s `DO` loop checks the exit condition **before** the first `WAIT UP TO 5 SECONDS`.

No regressions found in any of the five areas.

## Mandatory scale scenarios

```text
small (1-20 objects)              NOT EXECUTED this audit (static/code review only)
medium (>=5,000, multi-batch)     NOT EXECUTED — no measured trace or IT8 run available in ALLOWED_CONTEXT
large (>=40,000, cold/warm)       NOT EXECUTED — design doc's own T-DRAIN (&sect;5.1b) validation suite is
                                   specified but no execution artifact/log was found or supplied
shared branches                   N/A to this slice (no branch/prefetch-sharing surface in scope)
incremental store (~100/1M keys)  N/A — this slice has no persistent object-store dimension
interrupted attempt + retry       PARTIALLY COVERED by design/unit-level evidence (check_timeouts retry/
                                   fallback logic read and traced by hand) but NOT executed live
```

Per this mode's own instruction, these are reported as unexecuted, not converted into measured results. The
static/code-level analysis above is sufficient to clear the named §9 constants and inspection points A-E
precisely (each has a concrete, quoted call site or an equally concrete absence), but does **not** substitute
for the still-outstanding live T-DRAIN + medium/large IT8 validation the design doc itself requires "before
SLICE 2 is considered DONE" (§5.1b). The feature remains gated off by default (`is_serial_batch_active` =
false), which bounds production exposure until that validation runs.

## Verdict

```text
VERDICT=APPROVE_WITH_MINOR_REVISIONS
REASON=All five prior MAJOR adversarial findings (AR-1-001..005) are confirmed fixed in the current
  working tree. All ten &sect;9 limit constants behave exactly as their own doc comments claim - no
  BROKEN constant, no undisclosed silent gap. The four DECLARED_ONLY constants and one
  DEFERRED_BY_APPROVED_SCOPE constant are all explicitly, non-silently disclosed as SER-SLICE-2 scope
  boundaries in both the design doc and the source comments, and none compromise correctness (every
  object still serializes correctly via safe fallback paths). Inspection points A/B/C/E are all clean.
  One MAJOR, non-blocking, now DOCUMENTATION-CONFIRMED (not merely suspected) finding remains open:
  PS-001's plain "WAIT UP TO 5 SECONDS" does not return early on aRFC callback completion (per
  ABAPWAIT_UP_TO), costing real, bounded, linear-in-round-count added latency at scale, and drifting
  from the design doc's own event-aware wait intent. PS-003 is reassessed as an acceptable, K-proportional,
  explicitly documented trade-off requiring no fix.
REQUIRED_FIX_BEFORE_WIDE_ROLLOUT=Switch the poll loop's WAIT UP TO 5 SECONDS to WAIT FOR ASYNCHRONOUS
  TASKS UNTIL <a run-scoped "something changed" indicator> UP TO 5 SECONDS (or equivalent) so batch
  completion is observed immediately rather than up to 5s late; execute the design's own T-DRAIN suite
  (&sect;5.1b) plus a >=5,000-object and a >=40,000-object IT8 run before enabling IS_SERIAL_BATCH_ACTIVE
  outside controlled validation.
ACCEPTED_AS_DOCUMENTED_TRADE_OFF=PS-003 (mt_resolved run-sized growth); c_max_pre_dispatch_splits,
  c_max_in_flight_bytes, c_max_object_output_bytes, c_oversized_threshold (DECLARED_ONLY, disclosed
  follow-up-slice scope); c_max_actual_batch_bytes (DEFERRED_BY_APPROVED_SCOPE, disclosed missing
  batch-scoped prefetch-extraction prerequisite).
BLOCKING_FINDINGS=NONE
NEXT=Fix PS-001's wait mechanism and run the design's own T-DRAIN + medium/large IT8 validation before
  flipping IS_SERIAL_BATCH_ACTIVE on for production repositories; re-run this audit's mandatory scale
  scenarios with real measured evidence at that point.
```
