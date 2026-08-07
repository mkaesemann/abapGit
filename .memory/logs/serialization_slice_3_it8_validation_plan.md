# SER-SLICE-3 — consolidated IT8 validation plan (DOMA/DTEL provider)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_3_IT8_VALIDATION_PLAN
SCOPE=DOMA/DTEL batch prefetch provider only (CLAS/INTF and all other
  families are DEFERRED, not part of this validation pass - see
  serialization_slice_3_clas_intf.md and serialization_slice_3_object_
  ranking.md)
STATUS=SUPERSEDED_BY_PARITY_INCIDENT_RETEST - the original IT8 run using
  this plan surfaced a real output-parity failure (Feature ON produced 2
  files instead of 113 for a real repository). Root cause, fixes, and
  reviews are recorded in
  `.memory/incidents/serialization_slice_3_dtel_doma_parity.md`. THIS
  PLAN NOW REQUIRES A FULL RE-RUN (not just a delta) against the fixed
  source, using the EXACT SAME repository/branch/scope as the incident
  screenshots, before SER-SLICE-3 can be considered SAP-validated.
```

## 0. Mandatory parity retest (do this FIRST, before re-running sections 1-7 below)

```text
SCOPE=the exact same repository/branch/filter scope as the incident
  screenshots (OS4 6.0, development/6.0.x, /LOT/OS)
STEPS=
  1. Activate the corrected DDIC/classes/includes in dependency order
     (section 1 below, unchanged).
  2. Run Feature OFF (mv_serial_batch_active = abap_false) - record the
     exact file count and full file list/paths for this scope. This is
     now the REQUIRED baseline (previously implicitly assumed correct -
     now must be explicitly re-captured for a byte-level comparison).
  3. Run Feature ON (mv_serial_batch_active = abap_true) for the SAME
     scope - record the exact file count and full file list/paths.
  4. Compare: same requested object set, same generated file set, same
     paths/filenames, byte-identical payloads, same item metadata, no
     duplicate or missing files. "More than 2 files" is NOT the
     acceptance criterion - full parity with step 2's baseline is.
  5. If parity holds, proceed to sections 1-7 below (ABAP Unit/ATC/
     functional runs) as originally planned. If parity FAILS again,
     STOP and report the new failure shape (file counts, which specific
     objects differ) rather than assuming it is the same root cause.
```

Do not report SER-SLICE-3 as SAP-validated/complete until every step below
passes. This plan assumes SER-SLICE-2's own final IT8 validation (ATC/ABAP
Unit/feature-OFF/feature-ON-after-RPERF-fix, all PASS,
`.memory/handoffs/serialization-slice-2.md`) remains valid - do not re-run
that gate unless new contradicting evidence appears.

## 1. Manual object creation and activation order

```text
1. TABL ZAOG_SER_DD_BENTRY (structure) - see serialization_slice_3_doma_
   dtel.md for exact fields; XML source already exists in
   src/ortec/serial/core/zaog_ser_dd_bentry.tabl.xml
2. TTYP ZAOG_SER_DD_BENTRY_TT (STANDARD TABLE OF ZAOG_SER_DD_BENTRY);
   XML: src/ortec/serial/core/zaog_ser_dd_bentry_tt.ttyp.xml
3. TABL ZAOG_SER_DD_BHDR (structure); XML: src/ortec/serial/core/
   zaog_ser_dd_bhdr.tabl.xml
4. CLAS ZCL_ABAPGIT_ORTEC_SER_PREF_EXT (main + testclasses include) -
   activate AFTER steps 1-3 (references the new DDIC types)
5. CLAS ZCL_ABAPGIT_OBJECT_DOMA (standard-abapGit class, minimal seam
   change only)
6. CLAS ZCL_ABAPGIT_ORTEC_SER_ORCH (main + testclasses include)
7. FUGR ZABAPGIT_ORTEC_SERIAL / FUNC Z_ABAPGIT_ORTEC_SER_BATCH (the
   IV_PREFETCH_BUFFER_DD injection block)
EXPECTED_ACTIVE_VERSIONS=all 7 objects show a clean activation with no
  syntax errors once steps 1-3 (new DDIC) exist; no other object in the
  repository needs to change for this to activate cleanly.
STANDARD_HOOK_CHANGED=NO (ZCL_ABAPGIT_SERIALIZE~SERIALIZE itself is
  unchanged this slice - only ZCL_ABAPGIT_OBJECT_DOMA gained an internal,
  smallest-possible seam, exactly mirroring the already-IT8-validated
  ZCL_ABAPGIT_OBJECT_DTEL seam)
```

## 2. ABAP Unit

```text
CLASSES_AND_METHODS=
  ZCL_ABAPGIT_ORTEC_SER_PREF_EXT (testclasses include):
    ltcl_doma_parity: active_with_fixed_values, active_without_fixed_
      values, nonexistent_or_inactive, exists_matches_serialize,
      output_is_stable (pre-existing, SER-SLICE-1 baseline - must still
      pass unchanged), PROVIDER_HIT_MATCHES_BASELINE,
      PROVIDER_HIT_NO_FIXED_VALUES (NEW, this slice's mandatory parity
      proof - both must pass byte-identical)
    ltcl_dd_batch_wire (all NEW this slice): extract_no_dd_objects_empty,
      batch_round_trip_finds_data, reject_unknown_version,
      reject_count_mismatch, reject_duplicate_entries,
      no_cross_batch_leakage, doma_miss_when_not_prepared,
      reject_corrupt_import, unexpected_entry_ignored,
      empty_payload_is_hit
    plus every existing ltcl_* test class in this file (DTEL/ENHS/FUGR/
      PROG/SMIM/TOBJ/TRAN prepare/get/extract_for_object tests) - these
      must show NO regression from the additive mt_doma/prepare_doma
      changes.
  ZCL_ABAPGIT_ORTEC_SER_ORCH (testclasses include): ALL existing methods
      (see serialization-slice-2.md's own IT8 plan for the full list -
      not reproduced here) PLUS the 4 NEW this slice:
      split_depth_below_cap_false, split_depth_at_cap_true,
      split_depth_above_cap_true, before_dispatch_dd_buf_empty.
EXPECTED_COUNTS=13 new/extended test methods this slice (2 in
  ltcl_doma_parity, 10 in ltcl_dd_batch_wire including the corrected
  count after DR-003/DR-004 fixes, wait - recount: extract_no_dd_objects_
  empty, batch_round_trip_finds_data, reject_unknown_version,
  reject_count_mismatch, reject_duplicate_entries, no_cross_batch_
  leakage, doma_miss_when_not_prepared, reject_corrupt_import,
  unexpected_entry_ignored, empty_payload_is_hit = 10 in ltcl_dd_batch_
  wire, plus 2 in ltcl_doma_parity, plus 4 in ORCH testclasses = 16 new
  test methods total this slice), all must show GREEN with zero skips.
REQUIRED_SLICE_2_REGRESSION_SUITES=terminal-outcome accounting, no-
  partial-successful-result, MERGE_INTO_MT_FILES coverage, queue pre-
  dispatch failure, feature-OFF behavior, WAPA singleton planning, cross-
  run isolation, visible error propagation, RPERF fix behavior (all
  pre-existing in ZCL_ABAPGIT_ORTEC_SER_ORCH's testclasses include,
  unmodified logic - must still pass to prove this slice introduced no
  regression to SER-SLICE-2's own validated contract).
PROVIDER_SPECIFIC_SUITES=ltcl_doma_parity + ltcl_dd_batch_wire (above).
```

## 3. ATC

```text
PACKAGES_OR_CLASSES=ZCL_ABAPGIT_ORTEC_SER_PREF_EXT, ZCL_ABAPGIT_OBJECT_
  DOMA, ZCL_ABAPGIT_ORTEC_SER_ORCH, Z_ABAPGIT_ORTEC_SER_BATCH (function
  group ZABAPGIT_ORTEC_SERIAL), plus the 3 new DDIC objects.
CHECKS_ESPECIALLY_RELEVANT=
  - internal-table keys: HASHED TABLE WITH UNIQUE KEY domname (mt_doma)
    correctness, no accidental duplicate-key risk from the new INSERT
    pattern in inject_batch_from_buffer.
  - exception contracts: zcx_abapgit_exception raised (not a bare
    cx_root) from every inject_batch_from_buffer failure path; the RFC
    function module's TRY/CATCH must not have an empty/silent-swallow ATC
    finding flagged as a real concern (it is intentional per design - the
    ##NO_HANDLER pragma should already be present; confirm ATC accepts
    the pragma rather than flagging it).
  - memory: EXPORT ... COMPRESSION ON usage, no obvious oversized static
    buffer risk.
  - SQL: the 4 new FOR ALL ENTRIES SELECTs in prepare_doma - confirm ATC
    raises no "SELECT * anti-pattern" finding beyond the already-accepted
    PS-001 minor (performance scan), and no missing-WHERE/full-table-scan
    finding.
```

## 4. Functional test runs

```text
RUN=Feature OFF baseline
  REPOSITORY=any repository containing at least one DOMA and one DTEL
    object with fixed values/translations (or reuse the universal SAP
    Basis fixtures XFELD/CHAR30/MANDT directly via a scoped serialize
    call, matching ltcl_doma_parity's own approach)
  SWITCHES=is_serial_batch_active=FALSE (default OFF is already
    IT8-validated per SER-SLICE-2; confirm it remains FALSE-safe with
    the new DOMA seam present but never exercised via the batch path)
  EXPECTED=byte-identical output to the pre-SER-SLICE-3 baseline
  METRICS=none beyond pass/fail
  STOP_CONDITION=any output diff -> STOP, do not proceed to the next run

RUN=Feature ON, generic batch, DD provider naturally disabled (empty cache)
  SWITCHES=is_serial_batch_active=TRUE, is_serial_prefetch_active=TRUE
    (default), but do NOT prime mt_doma via prepare() outside the normal
    ORCH SERIALIZE flow - this is simply the normal batch path with
    ZERO DOMA/DTEL objects in the repository scope, proving the DD buffer
    stays legitimately empty (0 bytes, no DB access) for non-DD batches
  EXPECTED=identical behavior/output to SER-SLICE-2's own already-
    validated feature-ON baseline (this slice must not regress that)

RUN=DOMA/DTEL provider OFF versus ON (the mandatory parity comparison)
  A. is_serial_prefetch_active=FALSE - every DOMA/DTEL object in scope
     takes the standard, unchanged per-object path (both inside and
     outside the adaptive batch orchestrator)
  B. is_serial_prefetch_active=TRUE, is_serial_batch_active=TRUE, a
     repository/object-set scope containing REAL DOMA and DTEL objects
     (prefer objects with: an active version, multiple languages, fixed
     values, missing text in at least one language, no fixed values at
     all, and at least one domain/data-element that legitimately has no
     active version) processed through the adaptive batch path so the
     new extract_for_batch/inject_batch_from_buffer round trip is
     genuinely exercised end to end via the real RFC call (not just the
     in-process unit tests)
  EXPECTED=A and B produce BYTE-IDENTICAL output for every object
  METRICS=wall time, object count, RFC batch count, PROVIDER_HIT/MISS/
    FALLBACK per object (currently NOT yet exposed as a queryable metric
    outside ABAP Unit assertions - see OPEN ITEM below), output parity
  STOP_CONDITION=any output diff -> STOP, treat DOMA/DTEL provider as
    FIX_AND_RETEST, do not proceed to mixed-batch or WAPA runs

RUN=mixed DOMA/DTEL batch (real multi-object batch through one RFC
  dispatch)
  SCOPE=a repository/object-set scope with BOTH DOMA and DTEL objects
    small enough to land in one adaptive batch (<= c_max_batch_rows = 25)
  EXPECTED=both object types resolve correctly from the SAME
    iv_prefetch_buffer_dd buffer, byte-identical to feature-OFF for both

RUN=WAPA singleton (regression only - WAPA has no DD provider interaction)
  EXPECTED=unchanged from SER-SLICE-2's own validated WAPA singleton
    behavior - this slice must not have altered WAPA's partitioning or
    batching in any way (source confirms it did not)

RUN=provider miss/fallback
  SCOPE=an object set where DOMA/DTEL objects exist but are NOT prefetch-
    populated (e.g. is_serial_prefetch_active=TRUE but is_serial_batch_
    active=FALSE, so the seam's provider check runs but mt_doma/mt_dtel
    are never populated by an adaptive-batch dispatch) - proves the
    seam's MISS path (unchanged standard SELECT/DDIF_DOMA_GET call)
    still produces correct output with zero provider involvement
  EXPECTED=identical output to feature-OFF

RUN=corrupt/unknown provider buffer via test seam
  METHOD=since there is no existing IV_TEST_* seam for corrupting
    iv_prefetch_buffer_dd end-to-end over a REAL aRFC call (the local
    ABAP Unit coverage only exercises inject_batch_from_buffer directly,
    not via a live CALL FUNCTION ... STARTING NEW TASK), this run
    requires either: (a) a temporary debugger-assisted corruption of the
    buffer between BEFORE_DISPATCH's computation and the CALL FUNCTION
    statement, or (b) accepting the ABAP Unit-level REJECT_CORRUPT_IMPORT
    coverage as sufficient proof for this specific behavior and skipping
    a live-corruption run. RECOMMENDATION: accept (b) unless the owner
    specifically wants a live aRFC-boundary fuzz test - record whichever
    choice is made in the final sign-off.
  EXPECTED=the whole batch still completes; only the DD prefetch
    optimization silently degrades to MISS for every object in that one
    buffer; no dump, no partial-success violation, no visible error
    solely due to the corrupt buffer

RUN=failure path with visible abapGit error (regression only)
  EXPECTED=unchanged from SER-SLICE-2's own validated fail-fast contract -
    this slice never touches the terminal-outcome/WAIT/error contract,
    only adds an ADDITIONAL, independently-catchable failure mode
    (corrupt DD buffer) that must NEVER propagate as a batch-level error
```

## 5. Performance comparison

```text
METRICS=wall time, object count, RFC batch count, PROVIDER_HIT/MISS/
  FALLBACK (see OPEN ITEM below - not yet exposed), output parity,
  optional focused SAT/ST05 DB/API call counts for a DOMA/DTEL-heavy
  repository scope (compare per-object DDIF_DOMA_GET/DD04L-DD04T SELECT
  counts feature-OFF vs the bulk prepare_doma/prepare_dtel call counts
  feature-ON)
BASELINE=SER-SLICE-2's own CLAS-only measurements remain authoritative
  for CLAS (TOTAL_RUNTIME_REDUCTION_PERCENT=46.3,
  SERIALIZATION_RUNTIME_REDUCTION_PERCENT=55.7,
  RFC_TASK_REDUCTION_PERCENT=95.9, AVERAGE_OBJECTS_PER_BATCH=24.16) - DO
  NOT re-measure these unless the owner wants a fresh combined trace;
  this slice adds a NEW DOMA/DTEL-specific measurement, not a repeat of
  the CLAS one.
OPEN_ITEM=PROVIDER_HIT/MISS/FALLBACK counters are NOT YET wired into
  ZAOG_SER_BATCH_RESULT or any queryable per-run structure this slice
  (design §6 describes the intended shape but it was not implemented -
  disclosed, not silently dropped). Until a future slice adds this, HIT/
  MISS/FALLBACK can only be inferred indirectly via a SAT/ST05 trace
  (fewer DDIF_DOMA_GET/DD04L-DD04T SELECT calls under the ON scenario
  proves HIT occurred) rather than read directly from a counter. Record
  this as a known limitation in the decision matrix below, not as a
  failure.
```

## 6. Memory/safety checks

```text
- No SYSTEM_NO_ROLL, RPERF_ILLEGAL_STATEMENT, TIME_OUT, or
  DBSQL_STMNT_TOO_LARGE introduced by the 4 new bulk SELECTs in
  prepare_doma (unlikely given the existing DTEL/MSAG bulk-read
  precedent and the SER-SLICE-2 batch-row cap, but confirm via a live
  run against a repository with an unusually large number of distinct
  domains in one serialize() call).
- No cross-batch provider leakage - already proven at the unit-test level
  (no_cross_batch_leakage); confirm no live counter-evidence contradicts
  this over a real multi-batch IT8 run.
- No partial successful output - the corrupt-buffer degrade-to-MISS path
  must never let one object succeed while a sibling in the SAME batch
  silently uses stale/wrong cached data; already proven structurally
  (CLEAR mt_doma/mt_dtel happens only AFTER all validation passes, so a
  rejected buffer never populates the cache at all).
- No stale static provider state after a second run - mt_doma is cleared
  by the SAME existing clear() call site DTEL already relies on
  (zcl_abapgit_serialize's own cleanup block, unchanged this slice).
```

## 7. Decision matrix

```text
DOMA/DTEL provider:
  ACCEPT if: all Section 4 parity runs pass byte-identical, ABAP Unit/ATC
    both green, no dump/memory-safety issue in Section 6.
  FIX_AND_RETEST if: any parity run shows a byte difference (treat as a
    NEW DR-level finding, do not assume it is DR-001 recurring without
    checking) or any ABAP Unit test fails live that passed only via
    local get_errors this session.
  DISABLE_PROVIDER_KEEP_GENERIC_BATCH if: parity holds but a live SAT/
    ST05 trace shows NO measurable DB/API-call reduction for a realistic
    DOMA/DTEL-heavy repository (i.e. the provider is neutral or worse) -
    is_serial_prefetch_active can be flipped off without losing the
    adaptive batch orchestration itself.
  REMOVE_OR_DEFER if: a genuine correctness regression is found that
    cannot be fixed within the existing design's scope.
```

## 8. Owner action required

```text
OWNER_ACTION_REQUIRED=RUN_PARITY_RETEST_THEN_CONSOLIDATED_IT8_VALIDATION
NEXT=run section 0 (mandatory parity retest) FIRST against the fixed
  source (import the 3 corrections in this pass: CLEAR_DD_CACHE +
  unconditional worker-side clear, ROUTE_TO_SEQUENTIAL_FALLBACK's
  zero-file guard, MERGE_INTO_MT_FILES's empty-import guard, plus the
  original prepare()/extract_for_batch fixes). Only once section 0 shows
  byte-identical Feature ON/OFF output for the incident's own repository
  scope, proceed to create the 3 new DDIC objects (section 1),
  import/activate in the stated order, run ABAP Unit + ATC (sections
  2-3), execute the remaining functional/performance runs (sections 4-5),
  confirm section 6, then report back using the decision matrix (section
  7) so this file's STATUS can be updated to SAP_VALIDATED_COMPLETE (or
  FIX_AND_RETEST with the specific failing case attached).
```
