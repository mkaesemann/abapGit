# SER-SLICE-2 — Combined IT8 Activation and Validation Checklist

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SERIALIZATION_SER_SLICE_2_IT8_VALIDATION_PLAN
STATUS=NOT_YET_EXECUTED — this is the plan, not a completed run
```

Perform in this exact dependency order. Do not claim SER-SLICE-2 complete
until every item below passes.

## 1. Import and activate (dependency order)

1. `ZAOG_SER_BATCH_RESULT` (TABL), `ZAOG_SER_BATCH_RESULT_TT` (TTYP)
2. `ZAOG_SER_TADIR` (TABL), `ZAOG_SER_TADIR_TT` (TTYP)
3. `ZCL_ABAPGIT_ORTEC_SER_COST`, `ZCL_ABAPGIT_ORTEC_SER_PLANNER`,
   `ZCL_ABAPGIT_ORTEC_SER_PROV_GEN` (independent of each other)
4. `ZCL_ABAPGIT_ORTEC_SER_ORCH` (depends on 1-3)
5. `ZABAPGIT_ORTEC_SERIAL` (FUGR) / `Z_ABAPGIT_ORTEC_SER_BATCH` (FUNC)
   (depends on 1-2)
6. `ZCL_ABAPGIT_ORTEC_GIT_SWITCH` (new `IS_SERIAL_BATCH_ACTIVE` flag)
7. `ZCL_ABAPGIT_SERIALIZE` (new `IS_NO_PARALLEL` visibility + minimal hook)
   — depends on 4 and 6

All 7 must activate with zero syntax errors before any test below runs.

## 2. ABAP Unit suites

```text
ZCL_ABAPGIT_ORTEC_SER_COST      - PASS required (14 methods, EWMA/default-
                                   family logic, no RFC dependency)
ZCL_ABAPGIT_ORTEC_SER_PLANNER   - PASS required (LPT/refill logic, no RFC)
ZCL_ABAPGIT_ORTEC_SER_ORCH      - PASS required (object_key_sets_equal,
                                   breaker sliding-window, purge guard/
                                   retention, in-flight-budget floor - all
                                   testable without live RFC via LOCAL
                                   FRIENDS access to private static state)
```

## 3. ATC

Re-run `SAPDiagnose(action="atc")` on all 4 core classes plus the FUGR -
must return zero findings (this agent already confirmed ORCH/COST/
PLANNER clean before this Phase 2 pass began; PROV_GEN was already
clean; re-verify after this commit's additive changes since ORCH's body
grew substantially).

## 4. T-DRAIN (PARTIAL SEAM ONLY - see disclosed limitation)

```text
STATUS=BLOCKED_ON_FOLLOW_UP_WORK
REASON=IV_TEST_DELAY_S exists on Z_ABAPGIT_ORTEC_SER_BATCH (default 0,
  test-only), but C_BATCH_RFC_TIMEOUT_S/C_MAX_DRAIN_WAIT_S have no
  test-time override, so a real T-DRAIN-1..8 run would take 600s+ per
  case. Owner decision needed: either accept a long-running IT8 test, or
  authorize a follow-up contract change to make these two constants
  test-overridable (e.g. a test-only class-method setter, mirroring how
  other ORTEC feature flags in ZCL_ABAPGIT_ORTEC_GIT_SWITCH work) before
  T-DRAIN-1..8 can be executed practically.
```

## 5. Callback/run-registry isolation (manual or scripted IT8 test)

- Serialize two DIFFERENT repositories with overlapping object names
  (e.g. both containing a `CLAS ZCL_FOO`) in the SAME session with
  `IS_SERIAL_BATCH_ACTIVE` on. Confirm both repos' results are correct
  and neither's `MT_RESOLVED`/`MT_RUN_CONTEXT` rows leaked into the
  other (per-run_id isolation, sect 5.1).
- Confirm a late/abandoned dispatch from an EARLIER run never merges
  into a LATER run's output (drain-and-discard path, sect 5.5).

## 6. Output parity (mandatory, hard design requirement)

Serialize the SAME repository twice - once with `IS_SERIAL_BATCH_ACTIVE`
OFF (existing standard/sequential-parallel path) and once ON (new batch
path) - and byte-for-byte diff the resulting file sets. Any difference is
a BLOCKING correctness defect. Repeat for a repository containing:

- a mix of CLAS/INTF (batch-eligible)
- at least one WAPA object (must produce IDENTICAL output either way -
  this is the WAPA-exclusion regression this session's review caught and
  fixed; confirm empirically here, not just by code review)
- at least one object type in `IS_NO_PARALLEL`'s denylist (ECTC/ECTD, if
  available) or `IV_MAX_PROCESSES = 1` forced-sequential case

## 7. WAPA exclusion (explicit, in addition to output parity above)

Confirm via a debugger breakpoint or log trace that no WAPA `TADIR` row
ever appears in any `Z_ABAPGIT_ORTEC_SER_BATCH` dispatch's `IT_TADIR` -
it must always be resolved via `ROUTE_TO_SEQUENTIAL_FALLBACK` instead.

## 8. Performance comparison

For a large repository (hundreds+ objects), compare total wall-clock
elapsed time for `IS_SERIAL_BATCH_ACTIVE` OFF vs ON. Given the disclosed
prefetch-buffer limitation (buffers are always empty this slice), the
batch path is NOT expected to outperform the existing prefetch-enabled
standard parallel path yet - the primary expected win this slice is
reduced RFC task-dispatch overhead (fewer, larger `STARTING NEW TASK`
calls) per SAT trace evidence in the design doc section 1, not prefetch
acceleration. Record actual numbers; do not assume the win materializes
without measuring it.

## Sign-off

SER-SLICE-2 may be marked complete only when ALL of sections 1-3, 5, 6,
and 7 pass, AND section 4 (T-DRAIN) is either executed successfully or
the owner has explicitly accepted the disclosed limitation and decided
how to proceed. Section 8 is informational, not a blocking gate.
