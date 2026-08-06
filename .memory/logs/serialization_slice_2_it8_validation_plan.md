# SER-SLICE-2 — Combined IT8 Activation and Validation Checklist

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SERIALIZATION_SER_SLICE_2_IT8_VALIDATION_PLAN
STATUS=NOT_YET_EXECUTED — this is the plan, not a completed run
LOCAL_2026_08_06_STATE=Terminal-outcome semantics are locally review-clean
  again, but full IT8 validation is still pending. One SAPDiagnose dry-run
  against the current ORCH source advanced beyond the repaired main-include
  control-flow issues and then failed on the live system's stale
  `zcl_abapgit_ortec_ser_orch` testclasses include (`C_STATE_ABANDONED`
  still present there). Therefore this plan must begin by importing /
  activating the CURRENT local class pool before any live syntax/unit/ATC
  result is treated as meaningful.
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
7. `ZCL_ABAPGIT_SERIALIZE` (`IS_NO_PARALLEL` REVERTED back to its original
   PRIVATE instance form - NO standard public API change - plus the
   minimal hook block) - depends on 4 and 6

All 7 must activate with zero syntax errors before any test below runs.

### 1a. Minimal-hook proof (new, Phase 3)

Diff the ACTIVE `ZCL_ABAPGIT_SERIALIZE` source against the nearest
pre-SER-SLICE-2 commit touching that file (`ada103d5` at the time of
writing) and confirm the ONLY residual difference is the one `IF
zcl_abapgit_ortec_git_switch=>is_serial_batch_active( ) = abap_true ...
ENDIF.` block inside `SERIALIZE()` - no visibility/staticness change,
no other statement touched. This is a repeat of the Phase 0/1 diff
already performed locally; re-confirm it against the ACTUAL IT8-active
version after import, not just the git working tree.

## 2. ABAP Unit suites

```text
ZCL_ABAPGIT_ORTEC_SER_COST      - PASS required (14 methods, EWMA/default-
                                   family logic, no RFC dependency)
ZCL_ABAPGIT_ORTEC_SER_PLANNER   - PASS required (LPT/refill logic, no RFC)
ZCL_ABAPGIT_ORTEC_SER_ORCH      - PASS required (object_key_sets_equal,
                                   breaker sliding-window, purge guard/
                                   retention, in-flight-budget floor,
                                   no-parallel parity, next_task_name
                                   uniqueness, breaker gate at
                                   before_dispatch, merge failure
                                   fallback (3 cases), queued-failure
                                   explicit terminal accounting, and the
                                   drain-queue pre-dispatch failure window
                                   - all testable
                                   without live RFC via LOCAL FRIENDS
                                   access to private static state)
```

## 3. ATC

Re-run `SAPDiagnose(action="atc")` on all 4 core classes plus the FUGR -
must return zero findings (this agent already confirmed ORCH/COST/
PLANNER clean before this Phase 2 pass began; PROV_GEN was already
clean; re-verify after this commit's additive changes since ORCH's body
grew substantially).

## 4. Fail-fast WAIT/error contract (supersedes the old T-DRAIN gate)

```text
STATUS=REQUIRED_FOR_STAGE_A_IT8_CLOSEOUT
REASON=Stage A intentionally replaced the old T/X/abandon/drain model
  with a fail-fast WAIT contract. The obsolete long-running T-DRAIN gate
  is therefore SUPERSEDED, not still pending. What now needs owner IT8
  validation is the new visible-error/no-partial-success contract below.
```

Required IT8 cases:

- feature ON, complete run -> normal success, full output returned
- feature ON, induced missing-result condition (or equivalent forced
  incomplete terminal state) -> visible ZCX_ABAPGIT_EXCEPTION, NO partial
  result accepted
- feature ON, callback-side helper failure while draining queued work
  before dispatch completion -> visible failed-object or explicit fatal
  outcome, NOT a generic unexplained missing-result condition
- feature ON, induced timeout/no-completion case -> visible
  ZCX_ABAPGIT_EXCEPTION, NO partial result accepted
- late callback after DISCARD_RUN_STATE -> no dump, RECEIVE+discard only,
  no merge into any later run
- feature OFF regression -> unchanged baseline behavior


## 5. Callback/run-registry isolation (manual or scripted IT8 test)

- Serialize two DIFFERENT repositories with overlapping object names
  (e.g. both containing a `CLAS ZCL_FOO`) in the SAME session with
  `IS_SERIAL_BATCH_ACTIVE` on. Confirm both repos' results are correct
  and neither's `MT_RESOLVED`/`MT_RUN_CONTEXT` rows leaked into the
  other (per-run_id isolation, sect 5.1).
- Confirm a late callback from an EARLIER, discarded run never merges
  into a LATER run's output (unknown-task RECEIVE+discard path).

## 6. Output parity (mandatory, hard design requirement)

Serialize the SAME repository twice - once with `IS_SERIAL_BATCH_ACTIVE`
OFF (existing standard/sequential-parallel path) and once ON (new batch
path) - and byte-for-byte diff the resulting file sets. Any difference is
a BLOCKING correctness defect. Repeat for a repository containing:

- a mix of CLAS/INTF (batch-eligible)
- at least one WAPA object (must produce IDENTICAL output either way -
  WAPA is now batch-eligible only as a singleton batch, not forced-
  sequential-only; confirm both parity and singleton-batch handling
  empirically here, not just by code review)
- at least one object type in `IS_NO_PARALLEL`'s denylist (ECTC/ECTD, if
  available) or `IV_MAX_PROCESSES = 1` forced-sequential case
- **CLAS-only baseline** (single object type, simplest possible parity
  check, easiest to diff and triage first if anything fails)
- **mixed CLAS/INTF/DTEL/DOMA** (a realistic cross-type repository slice)
  to exercise the planner's per-type cost-estimate grouping and confirm
  DTEL/DOMA (typically small/fast objects) do not get mis-batched
  relative to larger CLAS objects
- **at least one object whose path matches a configured
  `MT_WO_TRANSLATION_PATTERNS` entry**, with the repository's own
  `MAIN_LANGUAGE_ONLY` set to `abap_false` - confirms the AR-1-001 fix
  (per-object i18n override) empirically, not just by code review

### 6a. Feature-off regression (new, Phase 3)

Before testing the feature ON at all, first confirm `IS_SERIAL_BATCH_
ACTIVE` defaults to `abap_false` (`GET_INSTANCE`/direct getter check),
and run the EXISTING standard serialization ABAP Unit suite plus a
manual repository pull with the feature left at its default OFF value -
results must be byte-identical to the pre-SER-SLICE-2 baseline (this
proves the minimal hook is truly inert when off, per the Phase 1 side-
effect ledger review already performed locally).

## 7. WAPA exclusion (explicit, in addition to output parity above)

Confirm via a debugger breakpoint or trace that every WAPA `TADIR` row
appears in the batch RFC path only as a SINGLETON `IT_TADIR` (exactly
one row, object type WAPA) - never mixed with another WAPA and never
mixed with any non-WAPA object.

## 8. Performance comparison

For a large repository (hundreds+ objects), compare total wall-clock
elapsed time for `IS_SERIAL_BATCH_ACTIVE` OFF vs ON. Given the disclosed
prefetch-buffer limitation (buffers are always empty this slice), the
batch path is NOT expected to outperform the existing prefetch-enabled
standard parallel path yet - the primary expected win this slice is
reduced RFC task-dispatch overhead (fewer, larger `STARTING NEW TASK`
calls) per SAT trace evidence in the design doc section 1, not prefetch
acceleration. Record actual numbers where available; do not assume the
win materializes without measuring it:

- total wall-clock elapsed time (OFF vs ON)
- RFC task/dispatch count (should be roughly `object_count /
  c_max_batch_rows`, not one per object)
- total batch count and average objects-per-batch
- poll-loop "wait tail" time (with the PS-001 fix, this should now be
  near-zero for the common case instead of up to 5s per round - measure
  to confirm the fix actually helps in practice, not just in theory)
- DB time (if measurable via ST05/SAT) - expected negligible for this
  slice (no new SQL hot path)
- peak memory (informal, e.g. via SAT or a rough estimate) - to sanity-
  check PS-003's "K-proportional, not N-proportional" analysis on a real
  large-K single run

## Sign-off

SER-SLICE-2 may be marked complete only when ALL of sections 1, 1a, 2,
3, 4, 5, 6, 6a, and 7 pass. Section 8 is informational, not a blocking
gate.
