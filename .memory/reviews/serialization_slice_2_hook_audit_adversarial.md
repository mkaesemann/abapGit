# SER-SLICE-2 Independent Adversarial Implementation Audit

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_2_INDEPENDENT_ADVERSARIAL_AUDIT
CYCLE=1
BASELINE_HEAD=6a94b62c
STATUS=FAIL
VERDICT=REVISE
OPEN_BLOCKER=0
OPEN_MAJOR=4
OPEN_MINOR=2
STATE_WRITE_ALLOWED=no
DIAGRAM_WRITE_ALLOWED=no
```

## Scope

Read-only audit of exactly the requested implementation surface:

- `src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap`
- `src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.testclasses.abap`
- `src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.locals_imp.abap`
- `src/ortec/serial/core/zcl_abapgit_ortec_ser_cost.clas.abap`
- `src/ortec/serial/core/zcl_abapgit_ortec_ser_planner.clas.abap`
- `src/ortec/serial/rfc/zabapgit_ortec_serial.fugr.z_abapgit_ortec_ser_batch.abap`
- `src/objects/core/zcl_abapgit_serialize.clas.abap`
- `src/ortec/git/zcl_abapgit_ortec_git_switch.clas.abap`

Approved context read:

- `.memory/logs/serialization_adaptive_batch_design.md`
- `.memory/handoffs/serialization-slice-2.md`

## Evidence Matrix

```text
E1=src/objects/core/zcl_abapgit_serialize.clas.abap:635-683, run_parallel recomputes per-object main_language_only via match_obj_patterns before RFC dispatch.
E2=src/objects/core/zcl_abapgit_serialize.clas.abap:709-739, run_sequential recomputes per-object main_language_only via match_obj_patterns before local serialization.
E3=src/objects/core/zcl_abapgit_serialize.clas.abap:787-803, standard hook delegates to ORCH with only ms_i18n_params, not mt_wo_translation_patterns.
E4=src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap:598-693, ORCH serialize partitions, dispatches, waits, drains queue; no mt_broken_runs read/gate.
E5=src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap:1120-1144, record_task_outcome writes mt_broken_runs after breaker ratio is exceeded.
E6=src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap:389,1143,1162, mt_broken_runs production use is declaration/write/purge only; no production read found.
E7=src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap:886-970, dispatch_batch inserts one mt_dispatch row, increments in_flight, retries resource_failure, and falls back on final task-start failure.
E8=src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap:914-924, task_name is built from only the first 8 hex chars of run_id plus per-run dispatch_seq; INSERT result is not checked.
E9=src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap:811-828, merge_into_mt_files silently RETURNs if run context is missing or IMPORT from files_xstring fails.
E10=src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap:763-806, on_end_of_batch inserts mt_resolved after merge_into_mt_files returns, without knowing whether merge imported/appended files.
E11=src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap:670-693, ORCH always executes WAIT UP TO 5 SECONDS before detecting an empty queue/no dispatch state.
E12=src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.testclasses.abap:1-230, unit tests cover set equality, breaker write behavior, purge, budget floor, and no-parallel parity only; no actual dispatch_batch/on_end_of_batch RFC path or T-DRAIN execution.
E13=src/ortec/git/zcl_abapgit_ortec_git_switch.clas.abap:162,232-237, serial batch flag defaults OFF and is returned directly.
E14=src/objects/core/zcl_abapgit_serialize.clas.abap:787-803, with serial batch flag OFF, the hook body is unreachable and standard path continues below unchanged.
E15=src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap:618-634, WAPA/ECTC/ECTD and lv_max=1 are partitioned to forced sequential before any batch is built.
E16=src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap:857-884, retry/refill/initial dispatches funnel through before_dispatch; no secondary admission path bypassing the partitioned object_keys.
E17=src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap:974-1048, check_timeouts handles A->T retry/fallback and T->X abandonment with TRY/CATCH around RAISING calls.
E18=src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap:704-810, on_end_of_batch uses RECEIVE EXCEPTIONS and catches zcx_abapgit_exception from fallback/retry calls.
```

## Findings

```text
ID=AR-1-001
SEVERITY=MAJOR
CLAIM=The ORTEC hook preserves standard serialization semantics for every object it delegates to the batch orchestrator.
COUNTEREXAMPLE=Configure a repository with main_language_only = abap_false and an object-without-translation pattern matching CLAS ZCL_X. The standard run_parallel/run_sequential paths recompute lv_main_language_only/ls_i18n_params-main_language_only per object via zcl_abapgit_i18n_params=>match_obj_patterns before serialization. The hook passes only ms_i18n_params to ORCH, and ORCH/worker reuse that single run-level flag for every object. The matching object is therefore serialized with translations in batch mode where the standard path would force main-language-only.
EVIDENCE=E1,E2,E3,src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap:920-947,src/ortec/serial/rfc/zabapgit_ortec_serial.fugr.z_abapgit_ortec_ser_batch.abap:73-81
IMPACT=correctness/parity
REQUIRED_CHANGE=Pass the without-translation pattern table, or precompute per-object main_language_only into the batch input, and use the per-object value in both batch RFC and route_to_sequential_fallback.
RETEST=Unit or IT8 test with main_language_only=false plus a matching without-translation pattern proves standard and batch paths pass the same i18n flag for matching and non-matching objects.
```

```text
ID=AR-1-002
SEVERITY=MAJOR
CLAIM=The per-run circuit breaker stops new batch dispatches and routes remaining unresolved work to fallback once mt_broken_runs is tripped.
COUNTEREXAMPLE=record_task_outcome receives five confirmed failures at an 80-100% failure ratio and inserts the run id into mt_broken_runs. serialize then continues draining <ls_ctx>-queue and calls before_dispatch for queued batches because neither serialize, before_dispatch, dispatch_batch, check_timeouts, nor handle_receive_failure checks line_exists( mt_broken_runs[ table_line = lv_run_id ] ). The breaker trips but has no production effect.
EVIDENCE=E4,E5,E6
IMPACT=correctness/resilience/performance
REQUIRED_CHANGE=Before every new dispatch source (initial queue drain, refill/queue drain, timeout retry, receive-failure bisection), check mt_broken_runs for the run and route all not-yet-resolved work to route_to_sequential_fallback instead of dispatching more RFC tasks.
RETEST=Focused test: call record_task_outcome to trip the breaker, leave queued eligible objects for that run, execute the queue-drain path, and assert no new mt_dispatch rows are inserted and remaining keys become mt_resolved through fallback.
```

```text
ID=AR-1-003
SEVERITY=MAJOR
CLAIM=task_name is globally unique across all runs in the internal session, so callbacks cannot cross-correlate.
COUNTEREXAMPLE=Two different run ids with the same first 8 hex characters and dispatch_seq = 1 both build the identical task name SER-<same8>-1. mt_dispatch is keyed only by task_name and dispatch_batch does not check the INSERT result. A second run can therefore either fail to record its dispatch row while still starting an RFC task, or an older retained abandoned row can receive a callback intended for the newer run.
EVIDENCE=E8,E7
IMPACT=identity/cross-run isolation
REQUIRED_CHANGE=Make task_name collision-proof within char40, for example by using a session-wide monotonic dispatch id or by checking INSERT sy-subrc and regenerating a unique suffix before CALL FUNCTION. Do not rely on 32-bit GUID truncation plus per-run sequence.
RETEST=Friend test seeds mt_run_context for two synthetic run ids sharing the first 8 hex characters, calls dispatch_batch or an extracted task-name builder twice, and proves the second dispatch either produces a distinct task name or fails before RFC start.
```

```text
ID=AR-1-004
SEVERITY=MAJOR
CLAIM=A successful ET_RESULT row is either merged exactly once or safely treated as failed/fallback; it cannot be silently resolved without output.
COUNTEREXAMPLE=on_end_of_batch accepts a result set whose keys match object_keys and whose row has rc = 0. merge_into_mt_files then IMPORTs files_xstring; if the buffer is malformed/empty or the run context is already missing, it RETURNs silently. on_end_of_batch does not inspect that outcome and still inserts mt_resolved for the object. The object is now marked resolved with no files appended and will never be routed to fallback.
EVIDENCE=E9,E10
IMPACT=correctness/data-loss
REQUIRED_CHANGE=Make merge_into_mt_files return success/failure or raise a local handled exception; only insert mt_resolved after a successful import/append, otherwise log and route that object to sequential fallback.
RETEST=Friend test supplies a matching rc=0 ET_RESULT row with invalid files_xstring and asserts mt_resolved is not inserted and fallback/logging is invoked.
```

```text
ID=AR-1-005
SEVERITY=MINOR
CLAIM=Enabling serial batch does not add avoidable latency when no objects are batch-eligible.
COUNTEREXAMPLE=When it_tadir is empty, or every object is forced sequential (WAPA, ECTC/ECTD, or iv_max_processes=1 before the hook guard can apply in future callers), lt_batches is empty and no dispatch exists. serialize still enters DO, executes WAIT UP TO 5 SECONDS, then exits after proving queue/dispatch are empty.
EVIDENCE=E11,E15
IMPACT=performance/UX
REQUIRED_CHANGE=Skip the poll loop when there is no queued work and no awaiting/timed-out dispatch for the run.
RETEST=Unit test with all-forced or empty input asserts serialize returns without entering the 5-second wait path.
```

```text
ID=AR-1-006
SEVERITY=MINOR
CLAIM=Correctness-critical public/effectively reachable paths are covered by executable tests.
COUNTEREXAMPLE=The test include only covers pure helper behavior and direct static table manipulation. There is no executable test for dispatch_batch's CALL FUNCTION path, on_end_of_batch normal RECEIVE merge, RECEIVE failure bisection, T/D/X late-callback draining, or the partial T-DRAIN seam. A hostile regression in the actual aRFC lifecycle can pass the current tests.
EVIDENCE=E12,E7,E17,E18
IMPACT=test-coverage/regression-risk
REQUIRED_CHANGE=Add an IT8/live-RFC validation test or report using IV_TEST_DELAY_S plus reduced timeout seams, and at minimum friend tests for failure-safe merge and task-name uniqueness.
RETEST=Validation artifact shows T-DRAIN late callback drain, receive failure fallback, no duplicate merge, and no ST22 dumps on live RFC execution.
```

## Focus-Area Closure Notes

```text
RACE_STATIC_TABLES=PASS_WITH_SCOPE: ABAP aRFC callbacks are processed at WAIT/RECEIVE points, and serialize's poll loop only mutates after WAIT returns; no preemptive same-session race was found. Open defects are logic/identity defects, not thread preemption.
DUPLICATE_LATE_CALLBACK=PASS_EXCEPT_AR-1-004: T/X callbacks are drained and not merged; mt_resolved blocks duplicate normal result rows. Silent merge failure can still mark resolved without output.
CIRCUIT_BREAKER=FAIL: AR-1-002.
PURGE_CONTEXT=PASS: purge_run_state does not purge while A/T rows exist; X rows intentionally lose context only after fallback/resolution has already happened. No A/T context-loss trace found.
BEFORE_DISPATCH_RECURSION=PASS_FOR_CURRENT_SCOPE: recursion halves strictly decreasing object lists and threads iv_batch_id/iv_attempt unchanged; current actual bytes are disclosed as always zero because buffers are not implemented.
RESOURCE_FAILURE_CLEANUP=PASS: final task-start failure deletes mt_dispatch, releases in_flight, and routes fallback. route_to_sequential_fallback catches object exceptions.
TASK_NAME_UNIQUENESS=FAIL: AR-1-003.
WAPA_NO_PARALLEL=PASS: all batch entries originate from the serialize partition, and retries use existing dispatch object_keys only.
FEATURE_OFF_EQUIVALENCE=PASS: is_serial_batch_active defaults false and hook body is unreachable when false; standard path below remains unchanged.
CALLBACK_EXCEPTION_SAFETY=PASS_WITH_GAP: reachable RAISING calls from on_end_of_batch/check_timeouts are caught; RECEIVE uses EXCEPTIONS. No uncaught zcx_abapgit_exception path found in callback handling.
OBJECT_KEY_SET_GUARD=PASS_FOR_EXTRANEOUS_ROWS: result rows for objects not requested cause set mismatch and fallback. AR-1-004 covers malformed payload after identity match.
TEST_COVERAGE=FAIL: AR-1-006.
```

## Verdict

```text
VERDICT=REVISE
REASON=Four open MAJOR findings remain: i18n parity regression, dead circuit breaker, non-unique callback task identity, and silent resolved-without-output merge failure.
NEXT=Fix AR-1-001..AR-1-004 before enabling IS_SERIAL_BATCH_ACTIVE outside controlled IT8 validation.
```