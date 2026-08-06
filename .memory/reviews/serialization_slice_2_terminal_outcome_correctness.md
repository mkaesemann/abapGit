# Design Review

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_2_TERMINAL_OUTCOME_CORRECTNESS
BASELINE_COMMIT=4907442934b61d5bd7355ca165ca956cda1dc744
MODE=READ_ONLY_REVIEW
SOURCE_SCOPE=src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap;
  src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.testclasses.abap
```

## Verdict
APPROVE_WITH_MINOR_REVISIONS

## Confidence
High

## Strengths

- `assert_successful_run` correctly enforces INV-1: success requires
  `iv_wait_result` not in `{4,8}` AND `count_terminal_objects =
  expected_count` AND `count_failed_objects = 0` - every other branch raises
  `zcx_abapgit_exception` before `rt_files` is ever assigned in `SERIALIZE`.
  `is_run_complete` (the sole path to `iv_wait_result = 0`) independently
  re-derives the same `terminal_count = expected_count` condition, so the
  two checks cannot disagree.
- INV-3 holds structurally at both levels: `merge_into_mt_files` only
  appends to `<ls_ctx>-files` after a successful `IMPORT` (`merge_fails_*`
  tests confirm zero/unchanged rows on bad payload or missing context), and
  `SERIALIZE`'s `CATCH` block unconditionally `CLEAR`s `rt_files` before
  re-raising, so a discarded run can never leak partial output to the
  caller.
- `mark_queued_failures` gives the queued-object helper-failure path (AC-2)
  an explicit terminal-failure representation instead of leaving it in the
  queue forever. `queued_failures_block_return` and `drain_fail_marks_batch`
  (AC-3) exercise this end-to-end into `assert_successful_run`, asserting
  both the terminal/failed counts and that the resulting failure message is
  the "object(s) failed" variant, not the generic "missing batch result
  condition" one - this is real coverage of the *distinction* the
  acceptance criteria call for, not just a happy-path smoke test.
  `terminal_counts_isolated` and `fallback_fail_marks_failed` round out
  coverage of the terminal-accounting primitives.
- `dispatch_batch` (AC-1) is structurally sound: the `MT_DISPATCH` row and
  `in_flight` increment happen before `CALL FUNCTION ... STARTING NEW TASK`
  so a callback can never arrive before its own row exists; the
  `resource_failure` retry loop (`DO`/`EXIT`/`CONTINUE`, retry ≤5× with 1s
  backoff) is well-formed with a single `EXIT` per path; and the post-loop
  `IF lv_subrc <> 0` cleanup (`DELETE mt_dispatch`, release in-flight
  budget, `route_to_sequential_fallback`) converts a failed dispatch attempt
  into a terminal outcome rather than an orphaned AWAITING row. Live
  `SAPDiagnose(action="syntax")` against the active system object confirms
  zero errors (two pre-existing, unrelated ABAP Doc/shorttext warnings
  only) - AC-1's activation-readiness is confirmed, not just assumed from a
  clean local `get_errors`.
- `before_dispatch`'s circuit-breaker gate is the single choke point for
  every dispatch source (initial batches, queue drain, receive-failure
  bisection), matching the documented AR-1-002 requirement -
  `breaker_gates_before_dispatch` confirms no `MT_DISPATCH` row is created
  once a run is tripped.
- Idempotency is consistent across every mark/merge entry point
  (`mark_object_success`, `mark_object_failures`, the merge branch in
  `on_end_of_batch`): each checks `mt_resolved`/`mt_failed` membership
  first, so a duplicate object appearing in two dispatches cannot
  double-count `terminal_count` or flip an already-resolved object.

## Issues

### DR-001
- Type: correctness
- Severity: major
- Evidence: [zcl_abapgit_ortec_ser_orch.clas.abap](src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap) -
  `dispatch_batch`, `route_to_sequential_fallback`, `drain_queue`,
  `merge_into_mt_files`, and `mark_queued_failures` each do `ASSIGN
  mt_run_context[ run_id = ... ] ... IF sy-subrc <> 0. RETURN. ENDIF.`
  (or `IF NOT <ls_ctx> IS ASSIGNED. RETURN. ENDIF.`) with no explicit
  terminal-failure marking on the miss. For `dispatch_batch` and
  `route_to_sequential_fallback` specifically, `it_object_keys` is an
  independent input parameter, not derived from the context - a missing
  context there silently drops real, caller-supplied work items with zero
  accounting (neither `mt_resolved` nor `mt_failed`).
- Why it matters: this is exactly the callback-helper-failure shape INV-2
  forbids ("not only incomplete wait state"). It is unreachable *today* by
  convention only - `purge_run_state`/`discard_run_state` are the only
  writers that delete `mt_run_context` rows, and both also unconditionally
  clear this run's `mt_dispatch` rows in the same synchronous call, so no
  live dispatch should ever be able to reach these guards with a missing
  context. But nothing in the type system or method contracts *enforces*
  that pairing across the ~5 call sites that assume it, which directly
  contradicts this class's own stated design principle ("RUN_ID is part of
  the key... enforced by construction... not by convention" - see the
  class-level ABAP Doc). If the pairing is ever broken by a future change,
  the affected object(s) never reach a terminal state, `is_run_complete`
  (which itself first checks `mt_run_context` existence and returns
  `abap_false` if absent) can never report done, and the run's only escape
  is the 300s `c_batch_rfc_timeout_s` wait-budget exhaustion followed by
  `discard_run_state` and a visible exception - INV-1/INV-3 still hold (no
  false success, no partial return), so this is not exploitable into an
  incorrect success, but it silently downgrades an explicit, fast failure
  into a slow, generic timeout with a much less informative error message.
- Fix: at minimum, add a one-line comment at each guard cross-referencing
  the "context and dispatch rows are removed together" invariant it relies
  on (some sites already gesture at this, several don't). Preferably, have
  `dispatch_batch` and `route_to_sequential_fallback` call
  `mark_object_failures( it_object_keys )` before returning when the
  context lookup misses, so terminal accounting stays explicit-by-
  construction even if the invariant is ever violated elsewhere.

### DR-002
- Type: maintainability
- Severity: minor
- Evidence: `before_dispatch`, `dispatch_batch`, `handle_receive_failure`,
  and `route_to_sequential_fallback` all declare `RAISING
  zcx_abapgit_exception`, but no statement in any of their current bodies
  actually raises it - `route_to_sequential_fallback` only ever `CATCH`es
  `zcx_abapgit_exception` from `zcl_abapgit_objects=>serialize` and converts
  it to `mark_object_failures`, never re-raising; the other three only
  reach `route_to_sequential_fallback`/recursion, never a bare `RAISE`.
- Why it matters: the class-level ABAP Doc (e.g. `before_dispatch`'s
  `@raising ... Propagated from DISPATCH_BATCH or
  ROUTE_TO_SEQUENTIAL_FALLBACK`) describes a propagation path that does not
  currently exist, which could mislead a future maintainer into believing
  the `TRY`/`CATCH` blocks around these calls in `on_end_of_batch` are
  load-bearing when they are presently unreachable dead code.
- Fix: either note explicitly in the docs that these are currently
  unreachable defensive safety nets (not active propagation paths today),
  or drop `RAISING` from the signatures until a real raising path exists.

### DR-003
- Type: maintainability
- Severity: minor
- Evidence: `zcl_abapgit_ortec_ser_orch.clas.abap`, `on_end_of_batch`:
  `DATA lv_msg TYPE c LENGTH 100.` is declared but never referenced
  anywhere in that method (the only other `lv_msg` uses belong to the
  separate `dispatch_batch` method's `MESSAGE lv_msg` pattern).
- Why it matters: harmless dead local variable, a copy-paste leftover; will
  surface as an ATC/lint finding.
- Fix: remove the unused declaration from `on_end_of_batch`.

## Required revisions
None. All three issues above are non-blocking for activation or for the
terminal-outcome-correctness acceptance criteria; DR-001 is rated major
because it contradicts a documented design principle of this class, but it
has no live trigger path today and does not threaten INV-1/INV-3.

## Optional improvements
- Apply the DR-001 fix (explicit accounting or cross-reference comments) to
  make the "context and dispatch rows removed together" invariant
  construction-enforced rather than convention-enforced.
- Apply the DR-002/DR-003 documentation and dead-code cleanups.
- The hard actual-bytes admission gate (`c_max_actual_batch_bytes`,
  `before_dispatch`) remains structurally present but inert this slice
  (`lv_actual_bytes` is hardcoded to 0 pending batch-scoped prefetch
  extraction). This is an already-disclosed, previously-reviewed scope
  boundary from earlier slice documentation, not a regression introduced by
  this source, and is out of scope for a terminal-outcome-correctness
  review; flagged here only so the pending
  `ortec-abapgit-performance-review` handoff does not treat it as newly
  discovered.


### DR-003
- Type: maintainability
- Severity: minor
- Evidence: `SERIALIZE`'s initial dispatch loop calls `before_dispatch(...)` unconditionally in the `IF lv_ready < iv_max_processes` branch but gates the sibling `ELSEIF <ls_ctx> IS ASSIGNED` queue-append branch on context presence.
- Why it matters: harmless today (`<ls_ctx>` is always assigned by this point), but the asymmetry is confusing and slightly inconsistent with DR-001's concern.
- Fix: gate both branches the same way, or drop the redundant check if the invariant is asserted elsewhere.

## Required revisions

- DR-001 should be addressed (or explicitly accepted as a documented residual risk) before calling Stage A terminal-accounting hardening complete.

## Optional improvements

- DR-002, DR-003.
