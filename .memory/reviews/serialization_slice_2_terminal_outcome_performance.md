# SER-SLICE-2 — Terminal-Outcome Fix Implementation Performance Audit

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_2_TERMINAL_OUTCOME_PERF_AUDIT
MODE=IMPLEMENTATION_AUDIT
BASELINE_COMMIT=4907442934b61d5bd7355ca165ca956cda1dc744
SOURCE_SCOPE=src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap;
  src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.testclasses.abap
EVIDENCE=current-source call-chain analysis only (no git commands run
  this pass, per task envelope's FORBIDDEN_CHANGES; no scoped
  ortec-abapgit-performance-scan output available for this slice; no
  SAT/ST05 trace available) + existing unit tests
  (queued_failures_block_return, drain_fail_marks_batch,
  terminal_counts_isolated - all at expected_count 1-2, no scale
  scenario).
VERDICT=PASS_WITH_MINOR_FINDINGS
SUPERSEDES=an earlier pass of this same report (verdict
  FAIL_IMPLEMENTATION_PERFORMANCE, finding PERF-1 against a
  `LOOP AT mt_resolved/mt_failed WHERE run_id = ...` O(N) implementation
  of COUNT_TERMINAL_OBJECTS) - CURRENT source already contains exactly
  the O(1)-counter fix that earlier report's REQUIRED_FIX recommended
  (TY_RUN_CONTEXT-TERMINAL_COUNT/FAILED_COUNT, maintained at the
  MARK_OBJECT_SUCCESS/MARK_OBJECT_FAILURES sites). PERF-1 is CLOSED;
  verified against the current file, not re-derived from the stale text
  below (kept for history only).
```

## Scope of "the terminal-outcome fix" (historical evidence, not re-verified this pass)

The pre-existing completion predicate (inline in `SERIALIZE`'s old `DO`
loop exit check, per the earlier pass of this report) was:

```abap
IF NOT ( <ls_ctx> IS ASSIGNED AND lines( <ls_ctx>-queue ) > 0 )
   AND NOT line_exists( mt_dispatch[ run_id = lv_run_id state = c_state_awaiting ] )
   AND NOT line_exists( mt_dispatch[ run_id = lv_run_id state = c_state_timed_out ] ).
  EXIT.
ENDIF.
```

This checked **dispatch/batch state only** — O(number of this run's
`mt_dispatch` rows), i.e. O(batches), not O(objects). The new
`IS_RUN_COMPLETE` (current source, lines ~1346-1367) keeps the same
dispatch-state checks and **adds**:

```abap
IF count_terminal_objects( iv_run_id ) <> <ls_ctx>-expected_count.
  rv_complete = abap_false.
ENDIF.
```

where `count_terminal_objects` (lines ~930-937) is:

```abap
METHOD count_terminal_objects.
  LOOP AT mt_resolved INTO DATA(ls_success) WHERE run_id = iv_run_id.
    rv_count = rv_count + 1.
  ENDLOOP.
  LOOP AT mt_failed INTO DATA(ls_failed) WHERE run_id = iv_run_id.
    rv_count = rv_count + 1.
  ENDLOOP.
ENDMETHOD.
```

This is a genuinely new terminal-object-count check that did not exist in
any form before this fix — confirmed by the diff (no equivalent
object-level count predicate existed in the pre-fix completion check).

**CURRENT SOURCE (verified this pass, lines ~957-969 of
`zcl_abapgit_ortec_ser_orch.clas.abap`) no longer contains this shape:**

```abap
METHOD count_terminal_objects.
  ASSIGN mt_run_context[ run_id = iv_run_id ] TO FIELD-SYMBOL(<ls_ctx>).
  IF <ls_ctx> IS ASSIGNED.
    rv_count = <ls_ctx>-terminal_count.
  ENDIF.
ENDMETHOD.

METHOD count_failed_objects.
  ASSIGN mt_run_context[ run_id = iv_run_id ] TO FIELD-SYMBOL(<ls_ctx>).
  IF <ls_ctx> IS ASSIGNED.
    rv_count = <ls_ctx>-failed_count.
  ENDIF.
ENDMETHOD.
```

Both are now O(1) reads of dedicated `ty_run_context` counters
(`TERMINAL_COUNT`, `FAILED_COUNT`), maintained incrementally at the
single-object insert sites in `MARK_OBJECT_SUCCESS` (increments
`terminal_count` only on a genuinely new resolution; decrements
`failed_count` without touching `terminal_count` when recovering a
previously-failed object — no double count) and `MARK_OBJECT_FAILURES`
(increments both `terminal_count` and `failed_count` once per object,
guarded by the same existing `mt_resolved`/`mt_failed` duplicate checks).
This is exactly the fix the earlier pass of this report required — the
finding below is retained for history and marked CLOSED, not re-derived.

## Finding PERF-1 — CLOSED (was BLOCKING in the prior pass of this report)

```text
ID       PERF-1
SEVERITY BLOCKING
PATH     zcl_abapgit_ortec_ser_orch.clas.abap ->
         COUNT_TERMINAL_OBJECTS, called from IS_RUN_COMPLETE, called as
         the "WAIT FOR ASYNCHRONOUS TASKS UNTIL is_run_complete(...)"
         condition in WAIT_FOR_RUN_COMPLETION.
OBSERVED_CALL_SHAPE
  MT_RESOLVED and MT_FAILED are HASHED TABLEs keyed by
  RUN_ID+OBJ_TYPE+OBJ_NAME (the full key). COUNT_TERMINAL_OBJECTS filters
  by RUN_ID ALONE - a partial-key WHERE clause on a hashed table cannot
  use the hash function (which requires all key components), so both
  LOOP...WHERE statements degrade to a full LINEAR SCAN of the ENTIRE
  table's current row count for this run, i.e. O(objects resolved/failed
  so far this run) per call - growing throughout the run, one row added
  per completed OBJECT (not per batch).
  IS_RUN_COMPLETE is the logexp of "WAIT FOR ASYNCHRONOUS TASKS UNTIL
  ... UP TO c_batch_rfc_timeout_s SECONDS" - per the ABAP Keyword
  Documentation for that statement, the UNTIL condition is (re-)evaluated
  around callback delivery, i.e. at least once per completed batch
  callback (O(batches) = O(N / c_max_batch_rows) evaluations for a run of
  N objects at the 25-row batch cap), plus the two additional direct
  calls in WAIT_FOR_RUN_COMPLETION itself (before/after WAIT).
  Net shape: O(N / 25) evaluations x O(N) scan each = O(N^2 / 25) row
  comparisons for one run - a NEW quadratic-in-object-count cost that
  did not exist in the pre-fix completion predicate (which was O(batches)
  only, never O(objects)).
EXPECTED_PRODUCTION_CARDINALITY
  1 object; 1,000 objects; 40,000 objects (mode-mandated large scenario)
ESTIMATED_SQL_CALLS  0 (this is a pure in-memory ABAP internal-table
  defect, not a DB/HTTP one - does not itself violate the SQL/HTTP-per-
  object hard-fail categories)
ESTIMATED_HTTP_CALLS 0
ESTIMATED_MEMORY_IMPACT
  Negligible extra memory (no new retained data), but real, measurable
  additional CPU-bound elapsed time inside the SAME synchronous work
  process that is also driving the async RFC dispatch/receive loop:
    N=1        : ~1 comparison/call, no observable effect.
    N=1,000    : ~40 evaluations x ~1,000 scan = ~40,000 comparisons,
                 negligible (sub-millisecond class).
    N=5,000 (mode-mandated medium)
               : ~200 evaluations x up to 5,000 scan = up to ~1,000,000
                 row comparisons per run - measurable but likely still
                 small versus actual serialization time.
    N=40,000 (mode-mandated large)
               : ~1,600 evaluations x up to 40,000 scan = up to
                 ~64,000,000 row comparisons per run, ALL incurred inside
                 the poll loop that is meant to be near-free bookkeeping.
                 This is pure ABAP LOOP AT ... WHERE interpreter overhead
                 (not a single tight kernel op), so at real production
                 scale this can add non-trivial wall-clock seconds that
                 were not present before this fix and were not part of
                 the Stage-A IT8 measurement (that evidence covers only a
                 ~458-CLAS sample, well below where this shape starts to
                 matter) - it directly erodes the very serialization
                 speedup (46.3% total-time reduction measured on the
                 small sample) this slice exists to deliver, and gets
                 WORSE the more objects a single run contains, which is
                 exactly the scenario SER-SLICE-2 was built for.
WHY_IT_MATTERS
  This is precisely the pattern the abap-performance-patterns skill
  warns against (section 9, "Hashed lookup patterns" / section 6's
  complexity target "database calls = O(number of batches or graph
  levels), not O(number of nodes)" - the equivalent in-memory analogue
  applies here: a hot, repeatedly-evaluated predicate must not re-scan
  O(objects) state on every poll). It is also a direct violation of this
  task's own INV-2 ("no new per-object linear scans... introduced beyond
  pre-existing shape") - the pre-fix completion predicate never scanned
  per-object state at all, only per-batch dispatch state.
REQUIRED_FIX
  Maintain the terminal count incrementally instead of recomputing it by
  scanning MT_RESOLVED/MT_FAILED. MARK_OBJECT_SUCCESS and
  MARK_OBJECT_FAILURES already run exactly once per object (guarded by
  the existing duplicate checks) - add e.g. TY_RUN_CONTEXT-TERMINAL_COUNT
  (or separate RESOLVED_COUNT/FAILED_COUNT fields, matching
  COUNT_FAILED_OBJECTS's own separate need), incremented at those two
  existing single-object insert sites, so IS_RUN_COMPLETE becomes an O(1)
  comparison against MT_RUN_CONTEXT fields instead of an O(N) table scan.
  This preserves the exact same correctness contract (COUNT_TERMINAL_
  OBJECTS's current semantics are unchanged, just not recomputed by
  scanning) and does not touch INV-1/INV-3.
REGRESSION_TEST_OR_MEASUREMENT
  No scale test exists today (see below) - the current source's
  TERMINAL_COUNT/FAILED_COUNT counters are exercised functionally by
  `queued_failures_block_return`, `drain_fail_marks_batch`, and
  `terminal_counts_isolated` (1:1 tracking against MARK_OBJECT_SUCCESS/
  MARK_OBJECT_FAILURES, cross-run isolation), but none of these are scale
  tests. A real SAT/ST05 trace at >=5,000 and >=40,000 objects is still
  recommended to confirm the O(1) counter path in practice, but is no
  longer required to accept this specific fix - the call shape itself is
  now provably O(1), independent of measurement.
STATUS   CLOSED - current source already implements REQUIRED_FIX.
```

## Other INV checks (verified against current source)

```text
INV-1 (terminal-outcome fix must preserve production-scale behavior
  shape) - PASS. SQL/HTTP/dispatch shape is unchanged; the completion-
  check shape (COUNT_TERMINAL_OBJECTS/COUNT_FAILED_OBJECTS) is now O(1)
  per call, matching the pre-fix predicate's O(batches)-only cost class
  (it adds a constant-time field read, not a new scan dimension).
INV-2 (no new per-object linear scans beyond pre-existing shape) - PASS.
  MARK_OBJECT_SUCCESS/MARK_OBJECT_FAILURES use FULL-key (O(1)) hashed
  lookups on MT_RESOLVED/MT_FAILED and are bounded per call to one
  dispatch/bisection (<= c_max_batch_rows = 25) or, via
  MARK_QUEUED_FAILURES, to one run's still-queued batches only.
  COUNT_TERMINAL_OBJECTS/COUNT_FAILED_OBJECTS read MT_RUN_CONTEXT
  counters, no table scan at all.
INV-3 (queue-failure accounting bounded to queued items only) - PASS.
  MARK_QUEUED_FAILURES loops only over <ls_ctx>-queue (the batches still
  pending, never yet dispatched) and calls MARK_OBJECT_FAILURES per
  batch, itself O(batch items) with O(1) hashed-key duplicate checks
  (MT_RESOLVED/MT_FAILED accessed via their FULL key here) - no full-run
  or session-wide scan is introduced by the queue-failure path.
```

## MINOR (pre-existing, not introduced by this fix) — residual finding PERF-2

```text
ID       PERF-2
SEVERITY MINOR (not blocking; not introduced by the audited fix)
PATH     IS_RUN_COMPLETE ->
         line_exists( mt_dispatch[ run_id = iv_run_id state = c_state_awaiting ] )
OBSERVED_CALL_SHAPE
  MT_DISPATCH is keyed only by TASK_NAME; this RUN_ID+STATE filter is a
  partial-key scan, O(this run's own MT_DISPATCH row count) per call,
  i.e. O(batches) = O(N / c_max_batch_rows), NOT O(objects). Called once
  per ON END OF TASK wake-up (the WAIT ... UNTIL condition) plus twice
  directly in WAIT_FOR_RUN_COMPLETION - net O(batches^2), not O(N^2).
WHY_IT_MATTERS
  Same class of issue PERF-1 was, but one dimension smaller (batches, not
  objects) and PRE-EXISTING (part of the earlier Stage-A
  WAIT_FOR_RUN_COMPLETION redesign, not this terminal-outcome fix) - out
  of INV-2's "beyond pre-existing shape" scope, so not blocking here.
REQUIRED_FIX (not required for this fix's acceptance, tracked for a
  future slice)
  Same pattern already proven in this fix: add an AWAITING-count field to
  TY_RUN_CONTEXT, maintained at DISPATCH_BATCH (+1) and
  RELEASE_IN_FLIGHT_BUDGET (-1, mirroring IN_FLIGHT's own maintenance),
  so IS_RUN_COMPLETE becomes fully O(1).
```

## Other pre-existing, unaffected shape (not re-flagged)

```text
- ON_END_OF_BATCH's per-result-row `line_exists(mt_resolved[full key])` /
  `line_exists(mt_failed[full key])` and OBJECT_KEY_SETS_EQUAL's
  per-object-key `it_result[ obj_type = ... obj_name = ... ]` lookups use
  FULL keys (hashed O(1)) or are bounded to one batch's own row count
  (<= c_max_batch_rows = 25) - O(N) in total across the whole run, not
  O(N^2). Pre-existing, unaffected by this fix, not flagged.
- DRAIN_QUEUE/BEFORE_DISPATCH/DISPATCH_BATCH's own per-dispatch work is
  unchanged by this fix and remains O(1) per dispatch plus the
  already-approved bisection-on-oversized-batch recursion (terminates on
  strictly decreasing size, per the design doc) - not in scope for this
  fix, not re-flagged.
```

## Mandatory scale scenarios

```text
small (1-20 objects)      : COVERED by existing unit tests
  (expected_count = 1 or 2 throughout the testclasses include) - PASS.
medium (>=5,000 mixed)     : NOT EXECUTED. No fixture or trace exists.
  Terminal-outcome accounting itself is now O(1) per object regardless of
  N (static estimate, not measured); PERF-2 (residual, batch-count-bound)
  estimate: up to ~200^2 = 40,000 dispatch-row comparisons for ~200
  batches - negligible.
large (>=40,000)           : NOT EXECUTED. No fixture or trace exists.
  Terminal-outcome accounting remains O(1) per object (static estimate,
  not measured); PERF-2 estimate: up to ~1,600^2 = 2,560,000 dispatch-row
  comparisons for ~1,600 batches - small relative to actual serialization
  work, but the next candidate for the same counter treatment if batch
  counts grow further.
shared branches / incremental store / interrupted-attempt-and-retry
  : NOT APPLICABLE to this class (no persistence, no branch/shared-object
    model here - this is the in-session batch orchestrator, not the
    object-store layer Variant B's checks target).
```

All non-small scenarios above are explicitly marked NOT EXECUTED per the
mode's own instruction not to convert static estimates into measured
results.

## Verdict rationale

`PASS_WITH_MINOR_FINDINGS`: the terminal-outcome accounting fix under
audit (TY_RUN_CONTEXT-TERMINAL_COUNT/FAILED_COUNT, maintained at
MARK_OBJECT_SUCCESS/MARK_OBJECT_FAILURES) is O(1) per object, satisfies
INV-1/INV-2/INV-3, and closes the BLOCKING PERF-1 finding from the prior
pass of this report. The only remaining cost (PERF-2) is pre-existing,
batch-count-bound (not object-count-bound), and out of this fix's scope
per INV-2's "beyond pre-existing shape" carve-out - explicitly stated
here per AC-2, not blocking per AC-1.
