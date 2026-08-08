# SER-SLICE-4 Package B (PROG) — implementation log

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_4_PACKAGE_B_PROG_IMPLEMENTATION
BASELINE_HEAD=commit "ORTEC: Enforce aggregate provider byte admission and
  add TABL batch provider" (Package A, already in git HEAD)
STATUS=IMPLEMENTED_LOCAL_NOT_IT8_VALIDATED
```

## Correction applied vs. the approved design doc

The design doc's &sect;2 specified a NEW GLOBAL DDIC structure/table type
(`ZAOG_SER_PROG_BROW`/`_TT`). Per the CORRECTION instruction (same
rationale as Package A) and after confirming the EXISTING PRIVATE
`ty_prog_lang_cache_tt` (`program` + `tpool_i18n`, `HASHED TABLE WITH
UNIQUE KEY program`) already has the byte-identical shape the design's
own DDIC proposal described: **no new DDIC objects were created.** The
wire payload reuses `ty_prog_lang_cache_tt` directly, exported/imported
under the field name `prog`, exactly mirroring Package A's TABL
precedent (which reused `ty_tabl_text_cache_tt`/`ty_tabl_extras_cache_tt`
directly). Only the pre-existing generic
`ZAOG_SER_ENV_BHDR`/`BENTRY`/`BENTRY_TT` envelope is reused - zero new
global objects for this package.

Because `ty_prog_lang_cache_tt` is `HASHED TABLE WITH UNIQUE KEY
program`, a duplicate `program` row cannot exist in a well-formed export
(same reasoning Package A documented for `ty_tabl_text_cache_tt`/
`ty_tabl_extras_cache_tt`) - the design doc's separate "duplicate prog
payload row" check (written against a proposed STANDARD-table DDIC wire
type) is therefore structurally unreachable and was not implemented;
this mirrors Package A's own established precedent exactly. The
"duplicate entry in ENTRIES" check (generic `zaog_ser_env_bentry_tt`,
a STANDARD table) IS implemented, since that table can genuinely carry
duplicates.

## Files changed

```text
src/ortec/serial/zcl_abapgit_ortec_ser_pref_ext.clas.abap
  + PUBLIC extract_for_batch_prog / inject_batch_from_buffer_prog /
    clear_prog_cache (design.md &sect;3/&sect;4/&sect;6, PR-001/PR-002/
    PR-003/PR-005-fixed names and validation sequence). No new TYPES, no
    new CLASS-DATA - reuses the EXISTING PRIVATE ty_prog_lang_cache_tt/
    mt_prog_langs/mv_language unchanged. No changes to PREPARE/CLEAR/
    collect_keys/prepare_prog_langs/get_prog_tpool_languages - verified
    already fully wired for PROG from a prior slice.
src/ortec/serial/zcl_abapgit_ortec_ser_pref_ext.clas.testclasses.abap
  + LOCAL FRIENDS line gained ltcl_prog_batch_wire
  + ltcl_prog_batch_wire (new LOCAL FRIEND test class, 8 test methods:
    extract_no_prog_objects_empty, batch_round_trip_finds_data (real
    SAPLSCFG fixture), checked_empty_is_hit_not_miss, reject_unknown_
    provider_id, reject_prog_without_p_entry, reject_initial_language
    (two-step PR-005 lifecycle regression: valid inject then a malformed
    language=space inject, proves no stale HIT survives), no_cross_
    batch_leakage, clear_prog_cache_clears)
src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap
  + before_dispatch: new lv_prefetch_buffer_prog computation via
    extract_for_batch_prog( it_object_keys ), threaded into
    sum_provider_buffer_bytes(...) (iv_buffer_prog parameter already
    existed on that method's signature, unused until now) and into
    dispatch_batch(...) (iv_prefetch_buffer_prog)
  + dispatch_batch signature gained iv_prefetch_buffer_prog TYPE xstring
    OPTIONAL (plus doc comment); its RFC CALL FUNCTION EXPORTING list
    gained iv_prefetch_buffer_prog = iv_prefetch_buffer_prog
src/ortec/serial/rfc/zabapgit_ortec_serial.fugr.z_abapgit_ortec_ser_batch.abap
  + IV_PREFETCH_BUFFER_PROG parameter (interface comment); unconditional
    clear_prog_cache() then conditional inject_batch_from_buffer_prog()
    with swallowed zcx_abapgit_exception, positioned after the existing
    TABL block; CASE ls_tadir-object gained a WHEN 'PROG' telemetry
    branch calling get_prog_tpool_languages(...) for provider_hit/
    provider_miss
src/ortec/serial/rfc/zabapgit_ortec_serial.fugr.xml
  + RSIMP/RSFDO rows for IV_PREFETCH_BUFFER_PROG, mirroring
    IV_PREFETCH_BUFFER_TABL exactly
```

## `mt_prog_langs`/PREPARE/CLEAR/collect_keys wiring check (task item 1b)

CONFIRMED already fully wired before this package, unchanged by it:
- `collect_keys` already has an `et_prog TYPE ty_prog_keys` EXPORTING
  parameter, populated from `it_tadir` for `object = 'PROG'`.
- `PREPARE` already calls `prepare_prog_langs( it_programs = lt_prog
  iv_language = iv_language )` unconditionally, using `collect_keys`'s
  `et_prog` output.
- `CLEAR` already has `CLEAR mt_prog_langs.`.
- `prepare_prog_langs` unconditionally pre-inserts one `mt_prog_langs`
  row per requested program (even with zero D010TINF rows), which is
  exactly the "checked, possibly-empty HIT" contract `extract_for_batch_
  prog`/`get_prog_tpool_languages` rely on.

No deviation found; no changes made to any of these four methods.

## Validation

```text
GET_ERRORS=CLEAN on all 5 touched files (2 CLAS main includes, 1 CLAS
  testclasses include, 1 RFC FUNCTION source, 1 FUGR XML)
METHOD_NAME_LENGTH_SCAN=CLEAN (exhaustive PowerShell scan across all 4
  ABAP files touched by this package, zero names > 30 chars; new names
  explicitly measured: extract_for_batch_prog=22,
  inject_batch_from_buffer_prog=29, clear_prog_cache=16,
  extract_no_prog_objects_empty=29, batch_round_trip_finds_data=27,
  checked_empty_is_hit_not_miss=29, reject_unknown_provider_id=26,
  reject_prog_without_p_entry=27, reject_initial_language=23,
  no_cross_batch_leakage=22, clear_prog_cache_clears=23)
VALIDATION_SEQUENCE_ORDER=CONFIRMED matches design.md &sect;4 exactly:
  wire_format_version -> provider_id -> object_count -> language-initial
  (PR-005) -> entry type/state loop (PR-002) -> duplicate ENTRIES ->
  payload-to-P-entry correlation -> CLEAR+INSERT mt_prog_langs ->
  unconditional mv_language assignment (PR-005)
LIVE_SYNTAX_DRY_RUN=NOT_RUN this session (no live SAP connectivity)
ABAP_UNIT=NOT_RUN_LIVE this session
DDIC_CREATED=NONE (see correction above)
```

## Disclosed residual (IT8-only)

Direct serialized-output parity for a real PROG object with extra-
language translations (feature OFF vs. feature ON+batch OFF vs. feature
ON+batch ON, byte-identical `.prog.xml` I18N_TPOOL section) was not run
this session (no live connectivity) - same disclosed boundary
DOMA/DTEL/CLAS/INTF/MSAG/TABL had before their own IT8 passes. PROVIDER_
HIT/PROVIDER_MISS telemetry for real PROG objects under the batch path
is the practical IT8 metric per design.md &sect;11 (currently always
PROVIDER_MISS under the batch path before this package; this package's
whole point is to make them PROVIDER_HIT).
