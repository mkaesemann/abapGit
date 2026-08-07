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

# SER-SLICE-3 — consolidated IT8 validation plan (final two-path architecture)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_3_IT8_VALIDATION_PLAN
STATUS=CONSOLIDATED_FOR_FULL_SER_SLICE_3_SCOPE
SCOPE=repository-scoped adaptive batch setting, CLAS/INTF batch
  provider, MSAG batch provider, DOMA/DTEL provider (owner-debug-
  validated, re-included here only for completeness), removal of the
  legacy ORTEC non-batch (Path 3) optimization path.
```

This supersedes the DOMA/DTEL-only plan previously recorded in this file
(preserved below in section 9 as a historical reference for the exact
DOMA/DTEL DDIC activation order and test list - still accurate, just no
longer the complete scope). The DOMA/DTEL parity incident that plan was
built around is `SUPERSEDED_FALSE_ORACLE` - see
`.memory/incidents/serialization_slice_3_dtel_doma_parity.md`. Do not
re-run the DOMA/DTEL parity retest as a standalone gate; validate it
together with the rest of this plan.

## 1. Manual object creation and activation order (owner action required)

```text
OWNER_ACTION_REQUIRED=CREATE_GLOBAL_OBJECTS

1. TABL ZAOG_SER_DD_BENTRY, TTYP ZAOG_SER_DD_BENTRY_TT, TABL
   ZAOG_SER_DD_BHDR - DOMA/DTEL envelope, unchanged from the prior DOMA/
   DTEL slice, package same as ZAOG_SER_BATCH_RESULT. XML sources exist:
   src/ortec/serial/core/zaog_ser_dd_b{entry,entry_tt,hdr}.tabl.xml.
2. TABL ZAOG_SER_ENV_BHDR (structure: WIRE_FORMAT_VERSION INT4,
   PROVIDER_ID CHAR8, BATCH_ID CHAR32, OBJECT_COUNT INT4) - generic
   envelope header, package same as ZAOG_SER_BATCH_RESULT. XML:
   src/ortec/serial/core/zaog_ser_env_bhdr.tabl.xml.
3. TABL ZAOG_SER_ENV_BENTRY (structure: OBJ_TYPE TROBJTYPE, OBJ_NAME
   SOBJ_NAME, STATE CHAR1 [P/M/F], ACTUAL_BYTES INT4) - generic envelope
   entry, same package. XML: src/ortec/serial/core/zaog_ser_env_bentry.
   tabl.xml.
4. TTYP ZAOG_SER_ENV_BENTRY_TT (STANDARD TABLE OF ZAOG_SER_ENV_BENTRY),
   same package. XML: src/ortec/serial/core/zaog_ser_env_bentry_tt.
   ttyp.xml.
5. CLAS ZCL_ABAPGIT_ORTEC_SER_PREF_EXT (main + testclasses) - references
   ZAOG_SER_DD_* (steps 1) - activate after step 1.
6. CLAS ZCL_ABAPGIT_ORTEC_SER_PREF_OO (main + testclasses, NEW
   testclasses include this run) - references ZAOG_SER_ENV_* (steps 2-4)
   - activate after steps 2-4.
7. CLAS ZCL_ABAPGIT_ORTEC_SER_PREF (main + NEW testclasses include this
   run) - references ZAOG_SER_ENV_* (steps 2-4) - activate after steps
   2-4 (can activate in parallel with step 6, no cross-dependency).
8. CLAS ZCL_ABAPGIT_OBJECT_DOMA - standard-abapGit class, minimal seam,
   unchanged this run (already IT8-debug-validated).
9. CLAS ZCL_ABAPGIT_PERSISTENCE_ORTEC (main + testclasses) - new
   get_repo_use_serial_batch/set_repo_use_serial_batch methods and
   ty_repo_config-use_serial_batch field, no new DDIC dependency (XML-
   persisted, self-describing).
10. CLAS ZCL_ABAPGIT_ORTEC_GIT_SWITCH (main + NEW testclasses include
    this run) - references ZCL_ABAPGIT_PERSISTENCE_ORTEC (step 9) -
    activate after step 9.
11. CLAS ZCL_ABAPGIT_SERIALIZE - standard-abapGit class, constructor gains
    iv_repo_url OPTIONAL, routing reads the new per-repo setting -
    references ZCL_ABAPGIT_ORTEC_GIT_SWITCH (step 10).
12. CLAS ZCL_ABAPGIT_REPO - 3 call sites pass iv_repo_url - references
    step 11.
13. CLAS ZCL_ABAPGIT_GUI_PAGE_SETT_REPO - new checkbox - references step
    10.
14. CLAS ZCL_ABAPGIT_ORTEC_SER_ORCH (main + testclasses) - references
    steps 5-7, 10 (unconditional prepare()/clear()/set_serial_prefetch_
    active pairing, new iv_prefetch_buffer_oo_batch/iv_prefetch_buffer_
    msag parameters).
15. FUGR ZABAPGIT_ORTEC_SERIAL / FUNC Z_ABAPGIT_ORTEC_SER_BATCH - new
    IV_PREFETCH_BUFFER_OO_BATCH/IV_PREFETCH_BUFFER_MSAG parameters,
    CLAS/INTF/MSAG CASE branches - activate after step 14.

EXPECTED_ACTIVE_VERSIONS=all objects activate cleanly once steps 1-4
  (new DDIC) exist. No object outside this list needs to change.
STANDARD_HOOK_CHANGED=YES, minimal: ZCL_ABAPGIT_SERIALIZE gained one
  OPTIONAL constructor parameter (iv_repo_url) and its existing routing
  IF-condition now passes that parameter to is_serial_batch_active - no
  other standard-class change. ZCL_ABAPGIT_REPO's 3 call sites gained one
  additional EXPORTING line each.
```

## 2. ABAP Unit

```text
NEW/EXTENDED TEST CLASSES this run:
  zcl_abapgit_persistence_ortec.clas.testclasses.abap:
    ltcl_serial_batch_setting (roundtrip_set_then_get,
    default_off_unknown_url, repo_a_on_repo_b_off_isolated, teardown)
  zcl_abapgit_ortec_git_switch.clas.testclasses.abap (NEW FILE):
    ltcl_serial_batch_switch (roundtrip_via_switch,
    default_off_unconfigured_repo, reads_per_repo_not_global,
    no_url_falls_back_to_test_seam, teardown)
    ltcl_serial_prefetch_switch (prefetch_default_off, wapa_default_off,
    wapa_delegates_to_prefetch, teardown)
  zcl_abapgit_ortec_ser_pref_oo.clas.testclasses.abap (NEW FILE):
    ltcl_oo_batch_wire (21 methods - small/large class, interface,
    compo/subco/both/missing-optional HIT rules, multi-language,
    namespaced name, mixed CLAS+INTF, all-miss, neither-present-MISS,
    partial-data-no-cross-contam, reject unknown version/duplicate/
    count-mismatch/corrupt, cross-batch isolation, full round-trip,
    extract-empty-when-no-objects/not-prepared)
  zcl_abapgit_ortec_ser_pref.clas.testclasses.abap (NEW FILE):
    ltcl_msag_batch_wire-equivalent (15 methods, see
    serialization_slice_3_msag.md for the exact list, including
    inject_does_not_touch_dokil - proves the DOKIL scope boundary)
  zcl_abapgit_ortec_ser_orch.clas.testclasses.abap:
    + before_dispatch_oo_buf_empty (mirrors before_dispatch_dd_buf_empty
    for the new CLAS/INTF buffer)
  All PRE-EXISTING ltcl_dd_batch_wire (DOMA/DTEL), ltcl_doma_parity, and
  every existing ORCH terminal-outcome/no-partial-success/MERGE_INTO_MT_
  FILES/WAPA-singleton test - unmodified logic, must show NO regression.
EXPECTED_COUNTS=roughly 4 + 5 + 3 + 21 + 15 + 1 = 49 new/changed test
  methods this run (Phases 3+4+6+7 combined), all must show GREEN.
```

## 3. ATC

```text
PACKAGES_OR_CLASSES=every class/DDIC object listed in section 1.
CHECKS_ESPECIALLY_RELEVANT=
  - HASHED TABLE key correctness in the new ZAOG_SER_ENV_BENTRY_TT-based
    caches (mt_classtx/mt_compotx/mt_subcotx for OO, mt_msag for MSAG) -
    no accidental duplicate-key silent-drop risk from inject_batch_from_
    buffer's repopulation loop.
  - exception contracts: zcx_abapgit_exception raised (not bare cx_root)
    from every inject_batch_from_buffer failure path across all 3
    providers (DD/OO/MSAG); the RFC worker's ##NO_HANDLER TRY/CATCH
    pragma should not be flagged as a real concern (it is intentional).
  - CLASS-DATA visibility: mv_serial_prefetch_active's default-value
    flip and is_wapa_active's new one-line delegation should not trigger
    any "unreachable code"/"always true" false positive - if ATC flags
    is_wapa_active as suspicious for delegating to another method's own
    flag, this is a false positive to document, not a real finding.
  - SQL: no new "SELECT * anti-pattern" or missing-WHERE findings beyond
    the already-accepted PS-001 (performance scan, DOMA bulk-read).
```

## 4. Path A verification (repository setting OFF)

```text
SETTING=Use ORTEC Adaptive Batch Serialization = OFF (default, or
  explicitly unchecked and saved) for the test repository.
TRACE/DEBUG=confirm via ST05/debugger breakpoints (or, at minimum, a
  live SAT trace filtered to the relevant classes) that NONE of the
  following execute during a Stage/serialize of this repository:
  - ZCL_ABAPGIT_ORTEC_SER_PREF=>PREPARE/CLEAR/EXTRACT_FOR_BATCH
  - ZCL_ABAPGIT_ORTEC_SER_PREF_EXT=>PREPARE/CLEAR/EXTRACT_FOR_BATCH
  - ZCL_ABAPGIT_ORTEC_SER_PREF_OO=>PREPARE/CLEAR/EXTRACT_FOR_BATCH
  - ZCL_ABAPGIT_ORTEC_WAPA=>SERIALIZE (WAPA objects must use the
    standard cl_o2_api_application path instead)
  - ZCL_ABAPGIT_ORTEC_SER_ORCH=>SERIALIZE / the batch RFC
  This is the direct live proof for Finding F-1/F-2's fix - local static
  review (serialization_final_two_path_audit.md, Phase 7 record) proved
  this by source deletion/diff; this is the missing live confirmation.
EXPECTED_OUTPUT=byte-identical to the pre-SER-SLICE-3 standard-path
  baseline for this repository (same file set, paths, content).
STAGE_BEHAVIOR=correct MODIFIED/unchanged classification (the original
  false-MODIFIED symptom that motivated removing Path 3 must NOT
  reappear - if it does, it was NOT solely a Path-3 artifact and this is
  a genuine new finding, not a re-run of the superseded incident).
```

## 5. Path B verification (repository setting ON)

```text
SETTING=Use ORTEC Adaptive Batch Serialization = ON for the test
  repository, at least 2 parallel processes available.
SCOPE=a repository containing DOMA, DTEL, CLAS, INTF, MSAG, and at least
  one WAPA object, large enough to produce multiple batches.
EXPECTED=
  - ZCL_ABAPGIT_ORTEC_SER_ORCH=>SERIALIZE executes; the classic
    sequential/parallel loop in ZCL_ABAPGIT_SERIALIZE is NOT reached for
    this repository's serialize calls (single delegation point, already
    proven by source read).
  - DOMA/DTEL: PROVIDER_HIT for objects with an active version + text/
    fixed-value rows, PROVIDER_MISS for domains/data-elements with no
    active version - byte-identical output to Path A for the same scope.
  - CLAS/INTF: PROVIDER_HIT for classes/interfaces with at least one of
    SEOCLASSTX/SEOCOMPOTX/SEOSUBCOTX translation/description rows,
    PROVIDER_MISS otherwise (a MISS here is normal, not a defect) -
    byte-identical serialized .clas.abap/.intf.abap/.testclasses.abap/
    etc. file SET and CONTENT to Path A for the same scope (compare
    actual file bytes, not just the Stage MODIFIED list - the whole
    point of the superseded incident is that the Stage list is not a
    valid oracle).
  - MSAG: PROVIDER_HIT for message classes with T100/T100T rows,
    PROVIDER_MISS otherwise - byte-identical output; long-text
    documentation (DOKIL) is a disclosed non-goal for the batch buffer
    and must still serialize correctly via each object's own per-object
    fallback (this was never part of any batch path, on or off).
  - WAPA: exactly one ZCL_ABAPGIT_ORTEC_WAPA=>SERIALIZE call per WAPA
    object, always as a singleton batch (never combined with another
    WAPA or any non-WAPA object in the same RFC dispatch).
  - No partial successful output for any batch (fail-fast contract from
    SER-SLICE-2, unmodified this run).
  - No provider cache leakage between this run and either a prior run in
    the same session or a concurrent run for a DIFFERENT repository with
    the setting OFF (Path A must never see stale ORTEC state - proven
    locally by ORCH's unconditional clear() + set_serial_prefetch_active
    (abap_false) on every exit path; needs live confirmation across at
    least 2 consecutive Stage operations, one ON then one OFF).
METRICS=wall time, object count, RFC batch count, PROVIDER_HIT/MISS/
  FALLBACK per object type (already exposed via ZAOG_SER_BATCH_RESULT-
  provider_hit/provider_miss/provider_fallback, unchanged field shape,
  now populated for DOMA/DTEL/CLAS/INTF/MSAG), output parity, optional
  SAT/ST05 DB-call-count comparison (expect materially fewer SEOCLASSTX/
  SEOCOMPOTX/SEOSUBCOTX/T100/T100T single-row SELECTs under Path B for a
  CLAS/INTF/MSAG-heavy scope vs Path A).
```

## 6. Mandatory-family validation matrix

```text
DTEL   - implemented provider (batch). Fixture: existing SER-SLICE-1/3
         DOMA/DTEL test repository scope. Expected calls: PREPARE once
         per run, EXTRACT_FOR_BATCH once per dispatch. Parity: byte-
         identical Path A/B output. Metrics: PROVIDER_HIT/MISS counters.
         Stop condition: any byte diff -> STOP, do not proceed further.
TABL   - deferred (DESIGN_REQUIRED). No fixture/scope this run - a
         future dedicated slice.
TTYP   - deferred (DESIGN_REQUIRED, pair with TABL). No fixture this run.
PROG   - deferred (GENERIC_BATCH_ONLY, narrow-slice extension). No
         fixture this run - PROG source reading is generic and
         unaffected by this run's changes either way.
DOMA   - implemented provider (batch), owner-debug-validated already.
         Fixture/expected/metrics: same as DTEL above.
CLAS   - implemented provider (batch), THIS RUN's primary new scope.
         Fixture: a small class (1-2 methods), a large class (10+
         methods/params with texts), and a class with NO translation/
         description data at all (proves MISS is handled). Expected
         calls: PREPARE once, EXTRACT_FOR_BATCH once per dispatch.
         Parity criterion: byte-identical serialized file content
         (not just Stage MODIFIED list). Metrics: PROVIDER_HIT/MISS,
         SEOCLASSTX/SEOCOMPOTX/SEOSUBCOTX SELECT-count reduction via
         optional SAT/ST05. Stop condition: any byte diff -> STOP.
FUGR   - deferred (MEASURE_FIRST). Optional: a real SAT/ST05 trace on a
         FUGR-heavy repository MAY be captured opportunistically during
         this IT8 pass (not required) to seed a future MEASURE_FIRST
         decision - not a pass/fail gate this run.
MSAG   - implemented provider (batch), THIS RUN's second new scope.
         Fixture: a message class with several messages/languages, and
         a message class the provider legitimately MISSes. Expected
         calls/parity/metrics: same shape as CLAS above. Stop condition:
         any byte diff -> STOP.
INTF   - implemented provider (batch, shares CLAS's implementation).
         Fixture: at least one interface in the mixed CLAS/INTF scope
         above. Same parity criterion as CLAS.
WAPA   - already-optimized (singleton batch, gate corrected this run).
         Fixture: at least one WAPA object. Expected: exactly one
         ZCL_ABAPGIT_ORTEC_WAPA=>SERIALIZE call, never batched with
         anything else. Parity: unchanged from SER-SLICE-2's own
         validated WAPA behavior.
```

## 7. Practical metrics

```text
REQUIRE_ONLY=wall time, object count, RFC batch count, provider
  HIT/MISS/FALLBACK, serialized output parity, optional focused SAT/ST05
  calls. Do not fabricate any metric not actually captured.
```

## 8. Safety

```text
CHECK=no SYSTEM_NO_ROLL, no RPERF_ILLEGAL_STATEMENT, no TIME_OUT, no
  DBSQL_STMNT_TOO_LARGE, no partial successful output, no stale provider
  state in a second run, no ORTEC optimization on Path A, no double
  serialization (single delegation point in ZCL_ABAPGIT_SERIALIZE,
  proven by source read - confirm live no dual execution for the same
  object).
DO_NOT_REQUIRE=testing Path 3 after removal - it is no longer reachable
  by any repository setting or code path; do not attempt to recreate it.
```

## 9. Historical reference — original DOMA/DTEL-only plan (superseded in
scope, activation-order detail for the DD envelope remains accurate)

The original DOMA/DTEL-specific manual object list, ABAP Unit test list,
ATC scope, functional runs, and decision matrix from the earlier pass of
this file are preserved verbatim in
`.memory/logs/serialization_slice_3_doma_dtel.md` and
`.memory/incidents/serialization_slice_3_dtel_doma_parity.md` - not
duplicated here again to keep this file the single current authority for
the FULL SER-SLICE-3 scope. Section 1 above already includes the DD
envelope's exact DDIC objects and activation position within the larger
combined order.

## 10. Decision matrix

```text
ACCEPT_SER_SLICE_3 if: sections 4-6 all pass byte-identical parity where
  specified, ABAP Unit/ATC both green, section 8 safety checks clean.
FIX_AND_RETEST if: any Path A trace shows an ORTEC method executing, any
  Path B parity run shows a byte difference, or any ABAP Unit test fails
  live that passed only via local get_errors this session.
PARTIAL_ACCEPT if: DOMA/DTEL/CLAS/INTF/MSAG all pass but WAPA singleton
  behavior regresses - treat WAPA as FIX_AND_RETEST in isolation, the
  rest of SER-SLICE-3 may still be accepted.
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
