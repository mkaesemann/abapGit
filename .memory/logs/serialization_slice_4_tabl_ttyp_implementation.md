# SER-SLICE-4 Package A (TABL) — implementation log

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_4_PACKAGE_A_TABL_IMPLEMENTATION
BASELINE_HEAD=8c9e5df4f9dd4fdaa4e05103cc0ac1e773758a32
STATUS=IMPLEMENTED_LOCAL_NOT_IT8_VALIDATED
```

## Correction applied vs. the approved design doc

The design doc's &sect;3 specified NEW GLOBAL DDIC structures
(`ZAOG_SER_TABL_TX_BROW`/`_TT`, `ZAOG_SER_TABL_EX_BROW`/`_TT`). Verified
against the ACTUAL, ALREADY-IMPLEMENTED DOMA/DTEL convention in
`ZCL_ABAPGIT_ORTEC_SER_PREF_EXT` (`ty_doma_cache_tt` is a PRIVATE ABAP
TYPES table reused DIRECTLY as the EXPORT/IMPORT wire payload type - no
separate DDIC "wire row" object exists per provider) - this design-doc
detail was corrected during implementation: **NO new DDIC objects were
created**. TABL's payload rows (`ty_tabl_text_cache_tt`/
`ty_tabl_extras_cache_tt`) are PRIVATE ABAP TYPES, exported/imported
directly, exactly mirroring the DOMA/DTEL/CLAS/INTF/MSAG precedent. Only
the pre-existing generic `ZAOG_SER_ENV_BHDR`/`BENTRY`/`BENTRY_TT` (from
SER-SLICE-3, already on IT8's creation manifest) are reused - zero new
global objects for this package.

## Files changed

```text
src/ortec/serial/zcl_abapgit_ortec_ser_pref_ext.clas.abap
  + PRIVATE TYPES ty_tabl_text_lang/_tt, ty_tabl_text_cache/_tt (nested-
    by-tabname, PF-001 O(1) shape), ty_tabl_extras_cache/_tt, ty_tabl_keys
  + CLASS-DATA mt_tabl_text, mt_tabl_extras
  + collect_keys gained et_tabl EXPORTING param + WHEN 'TABL' branch
  + PREPARE gained the prepare_tabl( ) call; CLEAR gained the 2 new
    CLEAR lines
  + PRIVATE prepare_tabl (decision-free bulk DD02T/TDDAT read, TT-001-
    fixed skip condition: ddlanguage IS INITIAL only, never ddtext;
    TT-002-fixed unconditional per-name extras pre-insert)
  + PUBLIC get_tabl_i18n / get_tabl_extras (TT-002/TT-005-fixed: both
    keyed on the SAME mt_tabl_extras "checked" marker)
  + PUBLIC extract_for_batch_tabl / inject_batch_from_buffer_tabl /
    clear_tabl_cache (TT-003-fixed names, TT-004-fixed full validation
    sequence: provider_id/entry-type-state/duplicate-entries/payload-to-
    P-entry correlation/initial-language-rejection, all before any cache
    mutation; PF-001-fixed O(1) full-key lookups throughout, no LOOP
    AT ... WHERE partial-key scan anywhere)
src/ortec/serial/zcl_abapgit_ortec_ser_pref_ext.clas.testclasses.abap
  + ltcl_tabl_batch_wire (new LOCAL FRIEND test class, 11 test methods:
    extract_no_tabl_objects_empty, batch_round_trip_finds_data (real
    SFLIGHT fixture), checked_empty_is_hit_not_miss (TT-002/005),
    empty_ddtext_row_is_kept (TT-001), extras_absent_row_is_checked,
    reject_unknown_provider_id, reject_duplicate_extras,
    reject_extras_without_p_entry, reject_initial_language,
    no_cross_batch_leakage, clear_tabl_cache_clears_both)
src/objects/tabl/zcl_abapgit_object_tabl.clas.abap
  + seam in zif_abapgit_object~serialize's read_extras call site
  + seam in serialize_texts's language-discovery query
  (both gated by is_serial_prefetch_active, unchanged fallback on MISS,
  DD03P/DD05M/DD08V/DD12V/DD17V/DD35V/DD36M/IDoc/longtexts untouched)
src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap
  SHARED PREREQUISITE (bundled - see rationale below):
  + RESTORED lv_prefetch_buffer_oo_batch/_msag computation and RFC-
    forwarding in before_dispatch/dispatch_batch (a prior owner commit
    had silently dropped both - see serialization_slice_3_owner_test_
    rework.md's "Separate, undocumented drift" section)
  + NEW sum_provider_buffer_bytes (pure, TYPE int8, testable overflow-
    safe aggregate byte sum across all 6 provider buffer slots -
    tabl/prog/fugr slots present now, populated as each package lands)
  + lv_actual_bytes changed TYPE i -> TYPE int8
  PACKAGE A:
  + lv_prefetch_buffer_tabl computation, threaded into
    sum_provider_buffer_bytes(...) and dispatch_batch(...)
  + dispatch_batch signature/RFC call gained iv_prefetch_buffer_tabl
src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.testclasses.abap
  + before_dispatch_msag_buf_empty, before_dispatch_tabl_buf_empty
  + byte_sum_all_buffers_empty/one_buffer_populated/
    all_buffers_populated/below_limit/exactly_at_limit/above_limit/
    overflow_boundary (7 tests for sum_provider_buffer_bytes)
src/ortec/serial/rfc/zabapgit_ortec_serial.fugr.z_abapgit_ortec_ser_batch.abap
  + IV_PREFETCH_BUFFER_TABL parameter; unconditional clear_tabl_cache()
    then conditional inject_batch_from_buffer_tabl() with swallowed
    zcx_abapgit_exception; CASE gained WHEN 'TABL' telemetry branch
src/ortec/serial/rfc/zabapgit_ortec_serial.fugr.xml
  + RSIMP/RSFDO rows for IV_PREFETCH_BUFFER_TABL
```

## Why the shared prerequisite (Phase 2) and Package A are bundled in one
commit

Both edits land in the SAME statements of `before_dispatch`/
`dispatch_batch` (the byte-sum call and the `dispatch_batch(...)`
argument list) - Package A's own `iv_buffer_tabl`/`iv_prefetch_buffer_tabl`
term was added to the SAME lines Phase 2 introduced
(`sum_provider_buffer_bytes(...)`, the `dispatch_batch(...)` call).
Splitting into two commits would require hand-reconstructing an
intermediate, artificial state that never actually existed in the working
tree. Per the mission's own commit-strategy exception ("combine only
where separation would create a non-compilable or misleading state"),
these are committed together. Packages B and C each add ONE further,
independent line to the SAME sum/call and can be committed separately on
top.

## Validation

```text
GET_ERRORS=CLEAN on all 7 touched files
METHOD_NAME_LENGTH_SCAN=CLEAN (exhaustive PowerShell scan across all 6
  ABAP files touched by this package, zero names > 30 chars)
LIVE_SYNTAX_DRY_RUN=NOT_RUN this session (no live SAP connectivity)
ABAP_UNIT=NOT_RUN_LIVE this session
DDIC_CREATED=NONE (see correction above)
```

## Disclosed residual (IT8-only)

Direct serialized-output parity for a real TABL object (feature OFF vs.
feature ON+batch OFF vs. feature ON+batch ON, byte-identical `.tabl.xml`)
was not run this session (no live connectivity) - same disclosed boundary
DOMA/DTEL/CLAS/INTF/MSAG had before their own IT8 passes. See the
consolidated IT8 validation plan.
