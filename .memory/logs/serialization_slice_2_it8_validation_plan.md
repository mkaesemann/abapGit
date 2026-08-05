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
                                   fallback (3 cases) - all testable
                                   without live RFC via LOCAL FRIENDS
                                   access to private static state)
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
  case.
```

### T-DRAIN owner-decision options (Phase 3 recommendation)

**Option A - run it for real, no code change (RECOMMENDED).**
Execute T-DRAIN-1..8 against the real 300s/300s constants as-is. Total
cost ≈8 cases x up to 600s ≈ 80 minutes of real wall-clock time, once,
as a one-time SER-SLICE-2 validation gate (not part of routine CI).
Zero risk to the already-approved "HARD BOUND, compile-time CONSTANTS,
never a variable" design guarantee for `C_BATCH_RFC_TIMEOUT_S`/
`C_MAX_DRAIN_WAIT_S` (see their own ABAP Doc). No new contract surface.

**Option B - convert the two constants to a variable get/set pair
(NOT RECOMMENDED without explicit owner sign-off).** Would let a test
shrink the timeouts directly, but converts a value the design doc calls
a "HARD BOUND" into something mutable at runtime - this weakens an
already-approved correctness property of the activated contract, which
this mode's own rules treat as requiring a real design decision, not an
implementation-level convenience.

**Option C - add a separate, explicit TEST-MODE override (viable
alternative to A if repeated re-validation is expected).** Mirror the
existing `ZCL_ABAPGIT_ORTEC_GIT_SWITCH` feature-flag pattern: add a new,
narrowly-scoped test-only static flag (e.g.
`IS_SER_TEST_MODE_ACTIVE`/`SET_SER_TEST_MODE_ACTIVE` plus paired
test-only timeout setters) that ONLY takes effect when a test explicitly
arms it; `C_BATCH_RFC_TIMEOUT_S`/`C_MAX_DRAIN_WAIT_S` themselves stay
untouched, real, compile-time constants for production - CHECK_TIMEOUTS
would read an "effective timeout" through one small private helper that
consults the test-mode override first. This preserves the hard-bound
production guarantee while making repeat T-DRAIN runs fast, but is a
NEW contract addition (even if test-only) and therefore requires the
same design-review gate as any other productive ABAP change - NOT
implemented in this pass.

**Recommendation: Option A.** This is a one-time gate for a feature that
is default-OFF and not yet enabled anywhere; the 80-minute one-time cost
is acceptable and avoids any change to an already-approved hard-bound
correctness property. Revisit Option C only if SER-SLICE-3+ work is
expected to require re-running T-DRAIN repeatedly.


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
3, 5, 6, 6a, and 7 pass, AND section 4 (T-DRAIN) is either executed per
Option A/B/C above or the owner has explicitly accepted the disclosed
limitation and decided how to proceed. Section 8 is informational, not
a blocking gate.
