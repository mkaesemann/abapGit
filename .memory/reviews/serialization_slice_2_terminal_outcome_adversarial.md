# SER-SLICE-2 Terminal Outcome Adversarial Review

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_2_TERMINAL_OUTCOME_ADVERSARIAL
CYCLE=3
DATE=2026-08-06
MODE=READ_ONLY_REVIEW
VERDICT=APPROVE
```

## Scope

Allowed context read:
- `.memory/handoffs/serialization-slice-2.md`
- `.memory/logs/serialization_adaptive_batch_design.md`
- `.memory/logs/serialization_slice_2_it8_validation_plan.md`

Source reviewed:
- `src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap`
- `src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.testclasses.abap`

Forbidden paths were not read. Productive source, `.memory/state.md`, archive paths, diagrams, and editor memory were not modified. No git commands were run. Local VS Code diagnostics for both scoped source files report no errors, so this review found no current local activation blocker in `DISPATCH_BATCH`.

## Evidence Matrix

```text
E-CTX-001
  .memory/handoffs/serialization-slice-2.md
  Stage A supersedes the older T/X drain model with fail-fast semantics:
  success only when complete; WAIT 4/8 discard partial results and raise a
  visible ZCX_ABAPGIT_EXCEPTION. The handoff also records that queued failures
  must be explicit rather than hidden as mere incompleteness.

E-CTX-002
  .memory/logs/serialization_adaptive_batch_design.md
  Current-source supersedure confirms fail-fast WAIT is authoritative and late
  callbacks after DISCARD_RUN_STATE are safe through unknown-task RECEIVE and
  discard. Historical drain text is not the active contract.

E-CTX-003
  .memory/logs/serialization_slice_2_it8_validation_plan.md
  Required IT8 closeout includes visible error/no-partial-success behavior,
  late callback safety, callback/run-registry isolation, output parity, and
  ORCH unit coverage for queued-failure semantics.

E-SRC-001
  src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap, SERIALIZE
  SERIALIZE creates the run context, partitions work, dispatches/queues planned
  batches, waits, calls ASSERT_SUCCESSFUL_RUN before copying files, and on
  ZCX_ABAPGIT_EXCEPTION discards run state, clears RT_FILES, and reraises.

E-SRC-002
  src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap, ASSERT_SUCCESSFUL_RUN
  ASSERT_SUCCESSFUL_RUN raises for missing context, WAIT timeout, WAIT 4 or
  terminal-count mismatch, and any failed-count > 0.

E-SRC-003
  src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap, MARK_OBJECT_SUCCESS / MARK_OBJECT_FAILURES
  Success and failure terminal markers are deduplicated per RUN_ID + object key.
  A failure increments both TERMINAL_COUNT and FAILED_COUNT and records the
  first failed identity; a later success removes a prior failure only for the
  same run/object and adjusts FAILED_COUNT.

E-SRC-004
  src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap, MARK_QUEUED_FAILURES
  MARK_QUEUED_FAILURES marks every object in the current queue as terminally
  failed and clears the queue.

E-SRC-005
  src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap, ON_END_OF_BATCH
  ON_END_OF_BATCH catches DRAIN_QUEUE exceptions on receive-failure,
  result-mismatch, and success branches; each catch logs when possible and
  calls MARK_QUEUED_FAILURES for the run.

E-SRC-006
  src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap, DRAIN_QUEUE
  DRAIN_QUEUE reads the first queued batch, raises the test seam before
  BEFORE_DISPATCH, calls BEFORE_DISPATCH, and deletes the queue entry only
  after BEFORE_DISPATCH returns. Therefore a pre-dispatch DRAIN_QUEUE exception
  leaves the selected batch still visible to MARK_QUEUED_FAILURES.

E-SRC-007
  src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap, IS_RUN_COMPLETE / WAIT_FOR_RUN_COMPLETION
  IS_RUN_COMPLETE requires no queued batches, no awaiting dispatch, and
  terminal count equal to EXPECTED_COUNT. WAIT_FOR_RUN_COMPLETION drains once,
  waits on that completion predicate, and interprets incomplete WAIT outcomes
  as 4 or 8 for ASSERT_SUCCESSFUL_RUN.

E-SRC-008
  src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap, ON_END_OF_BATCH unknown-task branch
  Unknown or already-discarded task names are RECEIVEd with exceptions mapped
  to SY-SUBRC and then discarded without touching run state.

E-TEST-001
  src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.testclasses.abap, QUEUED_FAILURES_BLOCK_RETURN
  The test seeds queued work, calls MARK_QUEUED_FAILURES, proves terminal_count
  reaches expected_count, failed_count is explicit, IS_RUN_COMPLETE is true,
  and ASSERT_SUCCESSFUL_RUN rejects via object(s)-failed rather than the
  missing-result path.

E-TEST-002
  src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.testclasses.abap, DRAIN_FAIL_MARKS_BATCH
  The test forces DRAIN_QUEUE to raise before dispatch, then marks queued
  failures and proves the selected object is MT_FAILED, terminal_count reaches
  expected_count, failed_count is explicit, IS_RUN_COMPLETE is true, and
  ASSERT_SUCCESSFUL_RUN rejects via object(s)-failed rather than the
  missing-result path. This would fail if DRAIN_QUEUE deleted the selected
  batch before the exception point.

E-TEST-003
  src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.testclasses.abap, TERMINAL_COUNTS_ISOLATED
  The test proves terminal and failure counters are isolated per RUN_ID for two
  runs with overlapping static registry state.
```

## Prior Response Verification

```text
ACCEPTED_AND_FIXED=prior DISPATCH_BATCH activation blocker
PROOF=Current DISPATCH_BATCH has complete retry/fallback control flow and local
diagnostics for both scoped files report no errors. This is local-source proof,
not a substitute for owner IT8 activation.

ACCEPTED_AND_FIXED=merge success without output
PROOF=MERGE_INTO_MT_FILES returns ABAP_FALSE on missing run context or failed
IMPORT. ON_END_OF_BATCH reacts to that false return by per-object fallback and,
if fallback cannot produce output, MARK_OBJECT_FAILURES. ASSERT_SUCCESSFUL_RUN
rejects failed objects before RT_FILES is copied out.

ACCEPTED_AND_FIXED=fail-fast partial-result contract
PROOF=SERIALIZE copies <ls_ctx>-files to RT_FILES only after
ASSERT_SUCCESSFUL_RUN. Timeout, missing terminal count, and failed objects all
raise; the CATCH path discards state and clears RT_FILES.

REJECTED_WITH_PROOF=AR-2-001 from the previous artifact
PROOF=The prior finding claimed DRAIN_QUEUE deletes the first queued batch before
BEFORE_DISPATCH can raise, so MARK_QUEUED_FAILURES would miss it. Current source
does the opposite: BEFORE_DISPATCH is called first, and DELETE <ls_ctx>-queue
INDEX 1 happens only afterward. The test seam raises before BEFORE_DISPATCH and
before DELETE, leaving the selected batch in the queue for MARK_QUEUED_FAILURES.
```

## Findings

```text
No open BLOCKER or MAJOR findings.
```

## Hostile Checks

```text
ID=AR-3-NF-001
CLAIM=A run can return success after any expected object remains non-terminal or failed.
RESULT=REJECTED_WITH_PROOF
COUNTEREXAMPLE_TESTED=One success plus one missing terminal marker; one success plus one failed marker; queued failure converted to explicit failed marker.
EVIDENCE=E-SRC-001,E-SRC-002,E-SRC-007,E-TEST-001,E-TEST-002
IMPACT=INV-1 closed for scoped local source.
REQUIRED_CHANGE=None.
RETEST=ORCH unit tests plus owner IT8 fail-fast visible-error/no-partial-result cases.

ID=AR-3-NF-002
CLAIM=Callback-side DRAIN_QUEUE exceptions hide behind mere incompleteness.
RESULT=REJECTED_WITH_PROOF
COUNTEREXAMPLE_TESTED=DRAIN_FAIL_MARKS_BATCH forces a pre-dispatch drain failure while the selected queued batch is still in the queue, then proves the affected object is explicitly MT_FAILED and ASSERT_SUCCESSFUL_RUN raises object(s)-failed rather than missing-result.
EVIDENCE=E-SRC-004,E-SRC-005,E-SRC-006,E-TEST-001,E-TEST-002
IMPACT=INV-2 and AC-2 closed for the current pre-dispatch drain-failure seam.
REQUIRED_CHANGE=None.
RETEST=Keep DRAIN_FAIL_MARKS_BATCH; add live IT8 callback-path exercise if a future seam can drive ON_END_OF_BATCH without real RFC dependence.

ID=AR-3-NF-003
CLAIM=Unknown or late callbacks after DISCARD_RUN_STATE corrupt a later run.
RESULT=REJECTED_WITH_PROOF
COUNTEREXAMPLE_TESTED=Current source resolves callbacks by globally unique TASK_NAME, not by object identity or run-local sequence. After DISCARD_RUN_STATE deletes the task row, ON_END_OF_BATCH uses the unknown-task RECEIVE-and-discard branch and does not merge into any run context.
EVIDENCE=E-CTX-002,E-SRC-008
IMPACT=INV-3 closed by source structure; final closure still requires IT8 late-callback validation per the plan.
REQUIRED_CHANGE=None.
RETEST=Execute the validation-plan late-callback case after import.

ID=AR-3-NF-004
CLAIM=The current scoped source still has an activation blocker in DISPATCH_BATCH.
RESULT=REJECTED_WITH_PROOF
COUNTEREXAMPLE_TESTED=Local VS Code diagnostics for the main and testclasses includes report no errors. DISPATCH_BATCH has a bounded resource-failure retry loop, deletes the pending dispatch row when STARTING NEW TASK fails, releases in-flight budget, and routes to sequential fallback.
EVIDENCE=local diagnostics 2026-08-06,E-SRC-001
IMPACT=AC-1 closed at local-diagnostic level; owner IT8 activation remains the authoritative runtime closure.
REQUIRED_CHANGE=None.
RETEST=Owner IT8 import/activation and ABAP Unit.

ID=AR-3-NF-005
CLAIM=The current fix depends on broad, unstated assumptions.
RESULT=REJECTED_WITH_PROOF
COUNTEREXAMPLE_TESTED=The terminal-outcome proof is local to the run context counters, explicit failed set, completion predicate, and assertion ordering. It does not depend on output parity, performance assumptions, or the historical timeout/drain lifecycle.
EVIDENCE=E-CTX-001,E-CTX-002,E-SRC-001,E-SRC-002,E-SRC-003,E-SRC-007
IMPACT=INV-4 closed for the reviewed terminal-outcome behavior.
REQUIRED_CHANGE=None.
RETEST=Do not broaden the claim beyond scoped source; retain separate IT8 parity/performance validations from the plan.
```

## Acceptance Status

```text
AC-1 no activation blocker remains in dispatch_batch = PASS_LOCAL_DIAGNOSTICS_ONLY
AC-2 drain_queue exception accounting explicit and terminal = PASS
AC-3 hostile review finds zero blocker/major correctness gaps in scoped source = PASS
AC-4 test coverage meaningfully exercises explicit queued-failure semantics = PASS
```

## Verdict

```text
VERDICT=APPROVE
OPEN_BLOCKER=0
OPEN_MAJOR=0
OPEN_MINOR=0
CLOSED=AR-3-NF-001,AR-3-NF-002,AR-3-NF-003,AR-3-NF-004,AR-3-NF-005
RESIDUAL_RISK=Local diagnostics are not live IT8 activation; late-callback safety still needs the validation-plan runtime case after import.
NEXT=Proceed to owner IT8 import/activation, ORCH ABAP Unit, and the validation-plan fail-fast/late-callback cases.
```
