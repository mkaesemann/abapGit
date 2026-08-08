# SER-SLICE-4 Package C — FUGR batch provider design

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_4_PACKAGE_C_FUGR_DESIGN
BASELINE_HEAD=e5e10d62137033801739b8a53c69bdff9cfa650e
STATUS=DESIGN_DRAFT_CYCLE_3_AWAITING_REVIEW
CYCLE=3
CYCLE_1_REVIEW=.memory/reviews/serialization_slice_4_fugr_adversarial.md
  (REVISE_AND_REVIEW_ONCE, 1 BLOCKER + 2 MAJOR: FG-001..FG-003)
CYCLE_2_REVIEW=same artifact, "Cycle 2" section (FG-001..FG-003 all
  CLOSED; new FG-004 MAJOR: section 6a's seam discarded get_prog_tpool_
  languages's own rv_found and always skipped the D010TINF fallback,
  silently losing translations on any real MISS)
CYCLE_3_FIXES=FG-004 (section 6a now assigns lv_fugr_i18n_prefetched
  from get_prog_tpool_languages's actual RETURNING rv_found instead of
  hardcoding abap_true, verified against the real method body in
  zcl_abapgit_ortec_ser_pref_ext.clas.abap which CLEARs et_tpool_i18n
  and returns abap_false on any miss)
PERFORMANCE_GATE_REVIEW=.memory/reviews/serialization_slice_4_performance.md
  (REVISE_AND_REVIEW_ONCE, 0 BLOCKER for this package, PF-002 MINOR:
  prepare_fugr's new TFDIR bulk SELECT never declared/guarded its
  lt_funcnames driver table; PF-003 MINOR: sect 8 wording could be
  misread as implying c_max_actual_batch_bytes bounds a single
  oversized-singleton FUGR, when the real safety net there is the
  separate post-hoc adaptive-shrink mechanism)
PERFORMANCE_FIXES=PF-002 (added explicit lt_funcnames population +
  empty-driver guard before the TFDIR SELECT, sect 6, matching every
  sibling FOR ALL ENTRIES read's own guard convention); PF-003
  (clarified sect 8's wording on the singleton-vs-batch admission-check
  scope)
```

## 0. Evidence base (CONFIRMED_SOURCE unless marked otherwise)

- `zcl_abapgit_object_fugr.clas.abap` `zif_abapgit_object~serialize`
  (CONFIRMED_SOURCE, ~line 1503): `serialize_xml` (header/short text) ->
  `serialize_functions` (per-function-module metadata+source) ->
  `serialize_includes` (TOP/UXX/generated/customer includes) ->
  `serialize_texts` (program text pool i18n, delegates to PROG's own
  logic via the shared main-program name) -> `serialize_dynpros`/
  `serialize_cua` if `subc = 'F'` -> `serialize_function_docs`
  (longtexts, per function module and per function exception class).
- `serialize_xml` (CONFIRMED_SOURCE, ~line 1125-1216): builds the FUGR
  header. Already has an ORTEC seam: `get_fugr_areat( iv_area iv_language
  )` on HIT supplies `TLIBT-AREAT` (function-group short text); on MISS,
  `SELECT SINGLE areat FROM tlibt WHERE ... AND spras = iv_language`.
  Also calls `RS_GET_ALL_INCLUDES` for the include list (unaffected by
  any prefetch, kernel/generated-object-directory read, not a DB SELECT).
- `functions` (CONFIRMED_SOURCE, ~line 572): already has an ORTEC seam:
  `get_fugr_enlfdir( iv_area )` on HIT supplies the group's
  `ENLFDIR`-shaped function-module directory rows (`ty_enlfdir_tt`); on
  MISS, `SELECT * FROM enlfdir WHERE area = ...` plus
  `RS_FUNCTION_POOL_CONTENTS` for the module NAME list itself (kernel
  read, unaffected).
- `serialize_functions` (CONFIRMED_SOURCE, ~line 937-1030): per function
  module, `CALL FUNCTION 'RPY_FUNCTIONMODULE_READ_NEW'` (kernel-level,
  returns import/export/changing/tables/exception interface signature,
  documentation, AND source - "fm RPY_FUNCTIONMODULE_READ does not
  support source code lines longer than 72 characters" per the code's own
  comment, hence `RPY_FUNCTIONMODULE_READ_NEW`'s `new_source`/`source`
  dual-table output). Already has an ORTEC seam:
  `get_fugr_func_metadata( iv_funcname )` on HIT supplies
  `exception_classes` (a single `abap_bool`, derived today from `SELECT
  SINGLE exten3 FROM enlfdir WHERE funcname = ...` on MISS). Also a
  version-dependent `SELECT SINGLE rfcscope rfcvers FROM ('TFDIR')`
  (dynamic OSQL, guarded by `CATCH cx_sy_dynamic_osql_semantics` for
  releases where these fields do not exist) - NOT currently prefetched,
  same low-risk DD0xL-style shape as the others, candidate for inclusion
  (&sect;1).
- `ZCL_ABAPGIT_ORTEC_SER_PREF_EXT` already owns three FUGR-scoped
  CLASS-DATA caches, all populated once per whole dispatch by
  `prepare_fugr` (called from the class's own `PREPARE` entry point, same
  single main-process call site as PROG's `prepare_prog_langs`,
  CONFIRMED_SOURCE):
  - `mt_fugr_areat TYPE ty_fugr_areat_cache_tt` (`HASHED ... UNIQUE KEY
    area`; row = `area TYPE tlibt-area / areat TYPE tlibt-areat`)
  - `mt_fugr_enlfdir TYPE ty_fugr_enlfdir_cache_tt` (`HASHED ... UNIQUE
    KEY area`; row = `area TYPE enlfdir-area / enlfdir TYPE ty_enlfdir_tt`
    - i.e. ALL of a group's ENLFDIR rows nested under one area key)
  - `mt_fugr_func_meta TYPE ty_fugr_func_meta_tt` (`HASHED ... UNIQUE KEY
    funcname`; row = `funcname TYPE rs38l_fnam / exception_classes TYPE
    abap_bool`)
  `collect_keys` (CONFIRMED_SOURCE) also derives each FUGR's main program
  name via `get_fugr_main_program` and inserts it into the SAME
  `et_prog`/`mt_prog_langs` collection Package B (PROG) already covers.
  **FG-003 correction (cycle 1 finding)**: this does NOT mean FUGR's own
  text-pool i18n is automatically optimized - `zcl_abapgit_object_fugr
  .clas.abap`'s OWN `serialize_texts` (CONFIRMED_SOURCE, ~line 1083-1118)
  is a SEPARATE method body from PROG's, and does its OWN independent
  `SELECT DISTINCT language FROM d010tinf WHERE r3state = 'A' AND prog =
  iv_prog_name ...` query - it NEVER calls `get_prog_tpool_languages` at
  all. So although `mt_prog_langs` correctly caches the FUGR main
  program's language list (via `collect_keys`), FUGR's serializer never
  reads it. This is a real, small, currently-unclaimed optimization
  opportunity, addressed explicitly in &sect;7 below (NOT assumed "free"
  via Package B as cycle 1 incorrectly claimed).
- **Same root-cause gap as Package B (CONFIRMED_SOURCE, identical
  mechanism)**: `before_dispatch` never populates the generic
  `iv_prefetch_buffer_ext` (only `_dd`/`_oo_batch`/`_msag` are computed),
  so `mt_fugr_areat`/`mt_fugr_enlfdir`/`mt_fugr_func_meta` are NEVER
  populated inside an RFC worker today - every FUGR object serialized via
  the adaptive batch/RFC path (the current IT8-validated default) is an
  unconditional MISS on all three existing seams, falling back to the
  exact same per-object `SELECT SINGLE`/`SELECT *` calls the code always
  had. This is the SAME disclosed, pre-existing architecture gap
  documented in Package B &sect;0, not a new defect.
- Longtext serialization (`serialize_function_docs`) has NO ORTEC
  prefetch hook (CONFIRMED_SOURCE, `zcl_abapgit_longtexts.clas.abap` grep
  is empty) - out of scope, unchanged, disclosed non-goal, same as every
  other package.
- Includes (`serialize_includes`, `RS_GET_ALL_INCLUDES`) and the actual
  function-module SOURCE (`RPY_FUNCTIONMODULE_READ_NEW`'s `source`/
  `new_source`) are kernel-level generated-source reads, structurally
  identical in risk profile to PROG's `RPY_PROGRAM_READ` (Package B
  &sect;1) - no DB-bulk equivalent exists, and FUGR objects can be large
  (multiple function modules, each with potentially large source) -
  MEDIUM-HIGH payload risk, HYPOTHESIS (source-derived: function group
  source is drawn from the same underlying REPOSRC/kernel generation
  mechanism as PROG's, no repository-specific measurement performed).

## 1. Decision

**IMPLEMENT_METADATA_AND_DIRECTORY_PROVIDER (Option B).**

Scope: extend all THREE existing, already-correct FUGR caches
(`mt_fugr_areat`, `mt_fugr_enlfdir`, `mt_fugr_func_meta`) plus the
version-dependent `TFDIR-RFCSCOPE`/`RFCVERS` pair (&sect;0, new small
addition, same low-risk DD0xL shape) into ONE combined
`extract_for_batch_fugr`/`inject_batch_from_buffer_fugr` pair on
`ZCL_ABAPGIT_ORTEC_SER_PREF_EXT`, reusing the generic
`ZAOG_SER_ENV_BHDR`/`ZAOG_SER_ENV_BENTRY`/`ZAOG_SER_ENV_BENTRY_TT`
envelope (same rationale as Package B &sect;2). Two small changes to
`zcl_abapgit_object_fugr.clas.abap` are required: (a) one new accessor
call added to `serialize_functions`'s existing `TRY ... CATCH
cx_sy_dynamic_osql_semantics` block for the new `TFDIR` fields (&sect;6);
(b) a new prefetch seam in `serialize_texts` for the text-pool language
list, mirroring PROG's own seam (FG-003 fix, &sect;7). All three
ORIGINAL `get_fugr_*` calls (`get_fugr_areat`/`get_fugr_enlfdir`/
`get_fugr_func_metadata`) are UNCHANGED beyond `get_fugr_func_metadata`'s
additive new OPTIONAL output parameters (&sect;6) - they already read
these CLASS-DATA caches by table key and are agnostic to how the caches
were populated.

**Explicitly OUT OF SCOPE this slice (Option C, full source/include
provider) - MEASURE_FIRST, not implemented:**

- Function-module SOURCE (`RPY_FUNCTIONMODULE_READ_NEW`'s source output)
  and includes (`RS_GET_ALL_INCLUDES`/`serialize_includes`'s own content
  reads) remain on the STANDARD per-object path, unchanged. Rationale:
  (a) no DB-bulk equivalent exists for kernel-generated source, matching
  Package B's PROG-source non-goal exactly; (b) FUGR groups can contain
  many function modules with non-trivial source each, so a naive
  "concatenate every function's source into one wire buffer" would risk
  exactly the failure mode the mission's Common Provider Constraints
  explicitly forbid ("Do not concatenate per-object EXPORT buffers...
  bounded rows/bytes... actual-byte admission") without a much deeper,
  separately-designed byte-budget/splitting scheme than this metadata-only
  provider needs; (c) no live SAT/ST05 trace has been run this pass to
  prove FUGR source reads are a measured bottleneck (evidence rule: "Do
  not claim runtime dominance without measurement") - matches the prior
  discovery ranking's own `MEASURE_FIRST` disposition for FUGR, which
  this design NARROWS to specifically the source/include piece rather
  than the whole family, since the metadata/directory piece is now shown
  to have a concrete, low-risk, already-proven-correct existing seam to
  extend.
- Screens/statuses (`serialize_dynpros`/`serialize_cua`, `subc = 'F'`
  guard) - REJECT, same reasoning as Package B's dynpro/CUA non-goal (no
  existing seam, lower prevalence, no evidence of benefit).
- Longtexts (`serialize_function_docs`) - REJECT, shared serializer, out
  of scope for every package (&sect;0).
- Function-group text-pool i18n - **FG-003 fix**: NOT automatically
  covered by Package B (cycle 1's claim was FALSE, &sect;0) - `zcl_
  abapgit_object_fugr`'s own `serialize_texts` never calls `get_prog_
  tpool_languages`. This design ADDS that seam explicitly (&sect;7) -
  it is IN scope, not excluded, and does not depend on Package B being
  implemented (the underlying `get_prog_tpool_languages` accessor
  already exists in current source today; Package B only adds a batch-
  envelope layer on top of it, which this new FUGR seam benefits from
  automatically once Package B ships, exactly like PROG's own consumer
  already does).

A future MEASURE_FIRST follow-up slice may revisit Option C once a real
SAT/ST05 trace on a representative repository's FUGR-heavy serialize run
exists - not authorized by this design.

## 2. Wire envelope

Reuses `ZAOG_SER_ENV_BHDR`/`ZAOG_SER_ENV_BENTRY`/`ZAOG_SER_ENV_BENTRY_TT`.
Three NEW DDIC row/table-type pairs (package `$ZAOG_SER`), one per
existing cache, kept SEPARATE rather than one mega-structure per the
mission's "do not design a universal untyped mega-envelope" rule - each
type is a direct mirror of an existing, already-typed PRIVATE cache row:

```abap
TYPES: BEGIN OF zaog_ser_fugr_areat_brow,   " ZAOG_SER_FUGR_AT_BROW
         area  TYPE tlibt-area,
         areat TYPE tlibt-areat,
       END OF zaog_ser_fugr_areat_brow.
TYPES zaog_ser_fugr_areat_brow_tt TYPE STANDARD TABLE OF
  zaog_ser_fugr_areat_brow WITH DEFAULT KEY.  " ZAOG_SER_FUGR_AT_BROW_TT

TYPES: BEGIN OF zaog_ser_fugr_enlfdir_brow, " ZAOG_SER_FUGR_ED_BROW
         area    TYPE enlfdir-area,
         enlfdir TYPE zcl_abapgit_ortec_ser_pref_ext=>ty_enlfdir_tt,
       END OF zaog_ser_fugr_enlfdir_brow.
TYPES zaog_ser_fugr_enlfdir_brow_tt TYPE STANDARD TABLE OF
  zaog_ser_fugr_enlfdir_brow WITH DEFAULT KEY. " ZAOG_SER_FUGR_ED_BROW_TT

TYPES: BEGIN OF zaog_ser_fugr_func_brow,    " ZAOG_SER_FUGR_FN_BROW
         funcname          TYPE rs38l_fnam,
         exception_classes TYPE abap_bool,
         rfcscope          TYPE c LENGTH 1,  " FG-001 fix: release-stable
                                              " primitive type, matching
                                              " zcl_abapgit_object_fugr's
                                              " OWN existing ty_function-
                                              " rfcscope field EXACTLY
                                              " (CONFIRMED_SOURCE) -
                                              " NEVER typed against
                                              " tfdir-rfcscope, which
                                              " would make this DDIC
                                              " structure's own
                                              " activation depend on a
                                              " field that may not exist
                                              " on older releases
         rfcvers           TYPE c LENGTH 10, " FG-001 fix: same
                                              " rationale, matches
                                              " ty_function-rfcvers
                                              " exactly
         rfc_fields_valid  TYPE abap_bool,   " abap_true iff this
                                              " release's TFDIR actually
                                              " has RFCSCOPE/RFCVERS -
                                              " mirrors the existing
                                              " TRY/CATCH cx_sy_dynamic_
                                              " osql_semantics release
                                              " gate so the worker never
                                              " has to re-probe release
                                              " capability itself
       END OF zaog_ser_fugr_func_brow.
TYPES zaog_ser_fugr_func_brow_tt TYPE STANDARD TABLE OF
  zaog_ser_fugr_func_brow WITH DEFAULT KEY.  " ZAOG_SER_FUGR_FN_BROW_TT
```

`ZAOG_SER_ENV_BHDR-PROVIDER_ID = 'SER_FUGR'`.

`ENTRIES` uses `obj_type = 'FUGR'` for every row (one entry per
function GROUP, not per function module) - a group's own state is `P`
(HIT) iff EITHER `mt_fugr_areat` OR `mt_fugr_enlfdir` has a row for its
area (mirrors the CLAS/INTF "any of three caches" HIT rule exactly); a
group with enlfdir but zero function modules (a legally empty/new group)
is still `P` with an empty `enlfdir` table - the valid-empty-payload case,
not a MISS.

## 3. `extract_for_batch_fugr` (decision-free pseudocode)

```abap
METHOD extract_for_batch_fugr.
  DATA lt_entries  TYPE zaog_ser_env_bentry_tt.
  DATA lt_areat    TYPE zaog_ser_fugr_areat_brow_tt.
  DATA lt_enlfdir  TYPE zaog_ser_fugr_enlfdir_brow_tt.
  DATA lt_func     TYPE zaog_ser_fugr_func_brow_tt.
  DATA ls_hdr      TYPE zaog_ser_env_bhdr.
  DATA lv_any_hit  TYPE abap_bool.

  IF mv_language IS INITIAL.                 " PR-003-style fix (same as
                                              " Package B): ZCL_ABAPGIT_
                                              " ORTEC_SER_PREF_EXT has no
                                              " mv_prepared field - PREPARE
                                              " sets mv_language
                                              " unconditionally, so this
                                              " is the real prepared-state
                                              " signal
    RETURN.
  ENDIF.

  LOOP AT it_object_keys INTO DATA(ls_tadir) WHERE object = 'FUGR'.
    DATA(lv_area) = CONV tlibt-area( ls_tadir-obj_name ).
    DATA(lv_found) = abap_false.

    READ TABLE mt_fugr_areat INTO DATA(ls_areat) WITH TABLE KEY area = lv_area.
    IF sy-subrc = 0.
      APPEND VALUE #( area = ls_areat-area areat = ls_areat-areat ) TO lt_areat.
      lv_found = abap_true.
    ENDIF.

    READ TABLE mt_fugr_enlfdir INTO DATA(ls_enlfdir) WITH TABLE KEY area = lv_area.
    IF sy-subrc = 0.
      APPEND VALUE #( area = ls_enlfdir-area enlfdir = ls_enlfdir-enlfdir ) TO lt_enlfdir.
      lv_found = abap_true.
      " per-function-module metadata for every FM in THIS group only -
      " mirrors the existing extract_for_object FUGR branch's own nested
      " LOOP AT ls_enlfdir-enlfdir exactly.
      LOOP AT ls_enlfdir-enlfdir INTO DATA(ls_fm).
        READ TABLE mt_fugr_func_meta INTO DATA(ls_meta)
          WITH TABLE KEY funcname = ls_fm-funcname.
        IF sy-subrc = 0.
          APPEND VALUE #( funcname = ls_meta-funcname
                           exception_classes = ls_meta-exception_classes
                           rfcscope = ls_meta-rfcscope   " &sect;6 new field
                           rfcvers  = ls_meta-rfcvers
                           rfc_fields_valid = ls_meta-rfc_fields_valid )
            TO lt_func.
        ENDIF.
      ENDLOOP.
    ENDIF.

    DATA(ls_entry) = VALUE zaog_ser_env_bentry(
      obj_type = ls_tadir-object obj_name = ls_tadir-obj_name ).
    IF lv_found = abap_true.
      ls_entry-state        = 'P'.
      ls_entry-actual_bytes = xstrlen( extract_for_object( ls_tadir ) ).
      lv_any_hit = abap_true.
    ELSE.
      ls_entry-state        = 'M'.
      ls_entry-actual_bytes = 0.
    ENDIF.
    APPEND ls_entry TO lt_entries.
  ENDLOOP.

  IF lt_entries IS INITIAL OR lv_any_hit = abap_false.
    CLEAR rv_buffer.
    RETURN.
  ENDIF.

  ls_hdr-wire_format_version = 1.
  ls_hdr-provider_id         = 'SER_FUGR'.
  ls_hdr-object_count        = lines( lt_entries ).

  EXPORT hdr = ls_hdr entries = lt_entries
         areat = lt_areat enlfdir = lt_enlfdir func = lt_func
         language = mv_language
    TO DATA BUFFER rv_buffer COMPRESSION ON.
ENDMETHOD.
```

## 4. `inject_batch_from_buffer_fugr` (decision-free pseudocode - same
validation sequence as Package B &sect;4/CLAS-INTF precedent: unknown
version / object_count mismatch / duplicate entry / corrupt IMPORT all
reject the WHOLE buffer)

```abap
METHOD inject_batch_from_buffer_fugr.
  " ... identical IMPORT + wire_format_version + object_count +
  " duplicate-entries validation sequence as &sect;4 of the PROG design
  " (serialization_slice_4_prog_design.md), operating on
  " areat/enlfdir/func instead of prog ...
  CLEAR mt_fugr_areat.
  CLEAR mt_fugr_enlfdir.
  CLEAR mt_fugr_func_meta.
  LOOP AT lt_areat INTO DATA(ls_areat).
    INSERT VALUE ty_fugr_areat_cache( area = ls_areat-area areat = ls_areat-areat )
      INTO TABLE mt_fugr_areat.
  ENDLOOP.
  LOOP AT lt_enlfdir INTO DATA(ls_enlfdir).
    INSERT VALUE ty_fugr_enlfdir_cache( area = ls_enlfdir-area enlfdir = ls_enlfdir-enlfdir )
      INTO TABLE mt_fugr_enlfdir.
  ENDLOOP.
  LOOP AT lt_func INTO DATA(ls_func).
    INSERT VALUE ty_fugr_func_meta( funcname          = ls_func-funcname
                                     exception_classes = ls_func-exception_classes
                                     rfcscope          = ls_func-rfcscope
                                     rfcvers           = ls_func-rfcvers
                                     rfc_fields_valid  = ls_func-rfc_fields_valid )
      INTO TABLE mt_fugr_func_meta.               " FG-002 fix: ONE cache,
                                                    " ONE insert - rfcscope/
                                                    " rfcvers/rfc_fields_
                                                    " valid are now plain
                                                    " additive fields on
                                                    " the EXISTING mt_fugr_
                                                    " func_meta row (&sect;6),
                                                    " never a separate
                                                    " mt_fugr_tfdir cache
  ENDLOOP.
  IF lv_language IS NOT INITIAL.
    mv_language = lv_language.
  ENDIF.
ENDMETHOD.

METHOD clear_fugr_cache.
  CLEAR mt_fugr_areat.
  CLEAR mt_fugr_enlfdir.
  CLEAR mt_fugr_func_meta.                 " FG-002 fix: rfcscope/rfcvers/
                                            " rfc_fields_valid are fields
                                            " ON this table, cleared with
                                            " it - no separate
                                            " mt_fugr_tfdir to clear
ENDMETHOD.
```

## 5. Required behavior mapping

Identical shape to Package B &sect;5 (unknown version/corrupt/duplicate ->
reject whole buffer, worker swallows and falls back to full MISS for
FUGR only; missing entry -> per-cache `rv_found = abap_false`, unchanged
existing fallback; unexpected entry -> ignored by exact-key lookup; empty
payload -> valid HIT, not a MISS - e.g. a function group that legitimately
has zero function modules yet).

## 6. New small addition: `TFDIR-RFCSCOPE`/`RFCVERS` prefetch

Currently NOT prefetched at all (&sect;0) - `serialize_functions` does a
per-function-module dynamic `SELECT SINGLE ... FROM ('TFDIR')` guarded by
a release-dependent `TRY/CATCH cx_sy_dynamic_osql_semantics`. Bulk
equivalent, added to `prepare_fugr` (existing method, extended, not
replaced):

```abap
METHOD prepare_fugr.                       " existing method, ADD at the
                                            " end, after the existing
                                            " ENLFDIR/TLIBT/func_meta
                                            " bulk reads
  " ... existing bulk reads unchanged ...

  " FG-001 fix: release-stable LOCAL target structure, declared with the
  " SAME primitive types as zcl_abapgit_object_fugr's own ty_function-
  " rfcscope/rfcvers (TYPE c LENGTH 1 / TYPE c LENGTH 10) - NEVER typed
  " from TFDIR's own fields, so this declaration activates identically
  " on every release regardless of whether TFDIR itself has RFCSCOPE/
  " RFCVERS. The TRY/CATCH below protects only the DYNAMIC SELECT's
  " runtime execution (cx_sy_dynamic_osql_semantics fires when the
  " SELECT's field list names a column TFDIR does not have on this
  " release) - it was NEVER able to protect a DDIC/type declaration
  " referencing a missing field, which is why cycle 1's ty_fugr_func_
  " meta/ZAOG_SER_FUGR_FN_BROW typed against tfdir-rfcscope/tfdir-
  " rfcvers directly was a real activation-time defect (FG-001).
  TYPES: BEGIN OF ty_tfdir_rfc_row,
           funcname TYPE rs38l_fnam,
           rfcscope TYPE c LENGTH 1,
           rfcvers  TYPE c LENGTH 10,
         END OF ty_tfdir_rfc_row.
  DATA lt_tfdir TYPE STANDARD TABLE OF ty_tfdir_rfc_row WITH DEFAULT KEY.
  DATA lt_funcnames TYPE STANDARD TABLE OF rs38l_fnam WITH DEFAULT KEY.

  " PF-002 fix (performance design gate): explicit driver-table
  " population + empty-driver guard, matching the sibling convention
  " every other FOR-ALL-ENTRIES read in this slice already follows
  " (prepare_tabl's "IF it_names IS INITIAL. RETURN. ENDIF.",
  " prepare_prog_langs'/prepare_fugr's own existing it_programs/it_areas
  " guards) - a free early-exit for a batch whose function groups
  " resolved zero function modules this run, and keeps this bulk read's
  " own driver-table handling consistent with every sibling read.
  lt_funcnames = VALUE #( FOR ls_meta IN mt_fugr_func_meta
                           ( ls_meta-funcname ) ).
  IF lt_funcnames IS INITIAL.
    RETURN.
  ENDIF.

  TRY.
      SELECT funcname, rfcscope, rfcvers
        FROM ('TFDIR')
        FOR ALL ENTRIES IN @lt_funcnames
        WHERE funcname = @lt_funcnames-table_line
        INTO CORRESPONDING FIELDS OF TABLE @lt_tfdir.
      LOOP AT lt_tfdir INTO DATA(ls_tfdir).
        READ TABLE mt_fugr_func_meta ASSIGNING FIELD-SYMBOL(<ls_meta>)
          WITH TABLE KEY funcname = ls_tfdir-funcname.
        IF sy-subrc = 0.
          <ls_meta>-rfcscope = ls_tfdir-rfcscope.
          <ls_meta>-rfcvers  = ls_tfdir-rfcvers.
          <ls_meta>-rfc_fields_valid = abap_true.
        ENDIF.
      ENDLOOP.
    CATCH cx_sy_dynamic_osql_semantics.
      " release does not have RFCSCOPE/RFCVERS on TFDIR at all - every
      " mt_fugr_func_meta row keeps rfc_fields_valid = abap_false
      " (initial), and the worker-side consumer (&sect;7 of the ORIGINAL
      " doc, see cross-reference note below) must fall back to the
      " existing per-object dynamic SELECT exactly as today, never treat
      " rfc_fields_valid = abap_false as a hard error.
  ENDTRY.
ENDMETHOD.
```

`ty_fugr_func_meta` (PRIVATE type, EXISTING) gains `rfcscope TYPE c
LENGTH 1`, `rfcvers TYPE c LENGTH 10`, `rfc_fields_valid TYPE abap_bool`
- **FG-001/FG-002 fix**: release-stable primitive types (NEVER `tfdir-
rfcscope`/`tfdir-rfcvers`), added DIRECTLY to the EXISTING type (no
separate cache, matching &sect;4's single-cache-shape fix) - purely
additive fields, no existing field renamed/removed, so the EXISTING
`get_fugr_func_metadata` single-object accessor and its EXISTING callers
remain source-compatible; `get_fugr_func_metadata`'s signature gains two
new OPTIONAL EXPORTING parameters (`ev_rfcscope`/`ev_rfcvers`) plus
`ev_rfc_fields_valid`, additive only.

`serialize_functions`'s existing block:

```abap
TRY.
    SELECT SINGLE rfcscope rfcvers INTO CORRESPONDING FIELDS OF ls_function FROM ('TFDIR')
      WHERE funcname = <ls_func>-funcname.
  CATCH cx_sy_dynamic_osql_semantics ##NO_HANDLER.
ENDTRY.
```

becomes (mirrors the existing `exception_classes` prefetch-guard
structure immediately above it in the same method):

```abap
DATA lv_rfc_prefetched TYPE abap_bool.
IF zcl_abapgit_ortec_git_switch=>is_serial_prefetch_active( ) = abap_true.
  zcl_abapgit_ortec_ser_pref_ext=>get_fugr_func_metadata(
    EXPORTING iv_funcname = <ls_func>-funcname
    IMPORTING es_metadata = ls_metadata
              ev_rfc_fields_valid = lv_rfc_prefetched ).
  IF lv_rfc_prefetched = abap_true.
    ls_function-rfcscope = ls_metadata-rfcscope.
    ls_function-rfcvers  = ls_metadata-rfcvers.
  ENDIF.
ENDIF.
IF lv_rfc_prefetched = abap_false.
  TRY.
      SELECT SINGLE rfcscope rfcvers INTO CORRESPONDING FIELDS OF ls_function FROM ('TFDIR')
        WHERE funcname = <ls_func>-funcname.
    CATCH cx_sy_dynamic_osql_semantics ##NO_HANDLER.
  ENDTRY.
ENDIF.
```

`lv_rfc_prefetched = abap_false` on a release with no RFCSCOPE/RFCVERS
falls through to the UNCHANGED dynamic-SQL block, which itself safely
no-ops via its own existing `CATCH` - no double-execution risk, no new
release-detection logic invented (reuses the existing guard).

## 6a. FUGR text-pool i18n seam (FG-003 fix - NEW this cycle)

**Correction of a false cycle-1 claim**: `zcl_abapgit_object_fugr
.clas.abap`'s own `serialize_texts` (CONFIRMED_SOURCE, ~line 1083-1118)
does NOT call `get_prog_tpool_languages` - it independently runs `SELECT
DISTINCT language FROM d010tinf WHERE r3state = 'A' AND prog =
iv_prog_name AND language &lt;&gt; mv_language`, filters via `mo_i18n_params->
trim_saplang_keyed_table`, then `READ TEXTPOOL iv_prog_name LANGUAGE
&lt;lang&gt; INTO lt_tpool` per language - structurally near-identical to
PROG's own `serialize_texts`, but a genuinely separate method body with
no prefetch seam at all today. Since `collect_keys` ALREADY inserts each
FUGR's main program into `et_prog`/`mt_prog_langs` (CONFIRMED_SOURCE,
&sect;0), the SAME already-existing `get_prog_tpool_languages` accessor
(present in current source regardless of whether Package B's OWN batch
envelope is ever implemented) can be consumed here with zero new SQL:

```abap
" replacing the "SELECT DISTINCT language ... FROM d010tinf" query in
" zcl_abapgit_object_fugr's OWN serialize_texts:
DATA lv_fugr_i18n_prefetched TYPE abap_bool.
IF zcl_abapgit_ortec_git_switch=>is_serial_prefetch_active( ) = abap_true.
  lv_fugr_i18n_prefetched = zcl_abapgit_ortec_ser_pref_ext=>get_prog_tpool_languages(
    EXPORTING iv_program    = CONV #( iv_prog_name )
              iv_language   = mv_language
    IMPORTING et_tpool_i18n = lt_tpool_i18n ).
                                              " FG-004 FIX: the method's
                                              " OWN RETURNING rv_found
                                              " MUST gate the fallback
                                              " below - a MISS (program
                                              " not prepared, or a
                                              " rejected/absent Package B
                                              " batch buffer in an RFC
                                              " worker) clears
                                              " et_tpool_i18n internally
                                              " and returns abap_false;
                                              " treating that as a HIT
                                              " (cycle-2's bug) would
                                              " silently serialize ZERO
                                              " translations for a FUGR
                                              " that genuinely has them.
                                              " PROG's OWN serialize_texts
                                              " can safely ignore this
                                              " RETURNING value only
                                              " because IT never has a
                                              " separate fallback branch
                                              " to choose between - this
                                              " FUGR seam SPECIFICALLY
                                              " needs the real value
                                              " because &sect;0's D010TINF
                                              " SELECT fallback exists
                                              " right below and must run
                                              " on a genuine MISS.
ENDIF.
IF lv_fugr_i18n_prefetched = abap_false.
  " unchanged: SELECT DISTINCT language FROM d010tinf ...
ENDIF.
mo_i18n_params->trim_saplang_keyed_table(                 " UNCHANGED,
  EXPORTING iv_lang_field_name = 'LANGUAGE'                " runs
  CHANGING  ct_tab             = lt_tpool_i18n ).           " regardless
SORT lt_tpool_i18n BY language ASCENDING.                  " of which
" ... unchanged LOOP AT ... READ TEXTPOOL ... ii_xml->add ...          " branch
                                                            " supplied rows
```

**FG-004 fix note**: `get_prog_tpool_languages`'s signature already
declares `RETURNING VALUE(rv_found) TYPE abap_bool` (Package B &sect;0,
existing method, unchanged) - cycle 2's pseudocode discarded this return
value and unconditionally set `lv_fugr_i18n_prefetched = abap_true`,
which meant ANY miss (program never prepared, `is_serial_prefetch_active`
transiently true but `PREPARE` not yet run, or - in an RFC worker - a
rejected/absent/corrupt Package B batch buffer) would skip the D010TINF
fallback entirely and silently produce ZERO translations instead of the
correct set. The fix above simply assigns the method's own return value
instead of hardcoding `abap_true` - no other line changes.

This is a SMALL, additive, low-risk change scoped entirely to FUGR's own
`serialize_texts` - it consumes an accessor that ALREADY EXISTS in
current source (`get_prog_tpool_languages`, Package B's design only adds
a BATCH-ENVELOPE layer on top of it), so this seam provides SOME benefit
(main-process/sequential-path cache reuse) even if Package B is never
implemented, and FULL benefit (RFC-worker-populated cache) once Package
B's batch envelope ships - no ordering dependency between the two
packages is required, but implementing BOTH together maximizes the win.
Required test (added to &sect;10): a FUGR whose main program has
extra-language text-pool translations serializes an identical `I18N_
TPOOL` XML section before and after this seam is added, under feature
OFF, feature ON+batch OFF, and feature ON+batch ON.

## 7. Worker wiring

New `iv_prefetch_buffer_fugr` parameter, threaded exactly like Package
B's `iv_prefetch_buffer_prog` (`before_dispatch` computes it once,
`dispatch_batch` threads it through, RFC worker does unconditional
`clear_fugr_cache( )` then conditional
`inject_batch_from_buffer_fugr( iv_prefetch_buffer_fugr )` inside a
swallowed `TRY/CATCH zcx_abapgit_exception`). Worker `CASE
ls_tadir-object` telemetry gains:

```abap
WHEN 'FUGR'.
  IF zcl_abapgit_ortec_ser_pref_ext=>get_fugr_areat(
       iv_area = CONV #( ls_tadir-obj_name ) iv_language = iv_language ) = abap_true
     OR zcl_abapgit_ortec_ser_pref_ext=>get_fugr_enlfdir(
       iv_area = CONV #( ls_tadir-obj_name ) ) = abap_true.
    ls_result-provider_hit = 1.
  ELSE.
    ls_result-provider_miss = 1.
  ENDIF.
```

## 8. Memory bounds

- `enlfdir`/`func` payload size scales with the number of function
  modules per group (typically tens, occasionally hundreds for very
  large groups) times a small fixed row size each - SOURCE_DERIVED
  ESTIMATE: still expected to be well under `c_max_actual_batch_bytes`
  per dispatch even for a batch containing several large groups, but
  MEDIUM risk category (higher than PROG's small text-pool-language-list
  payload).
- **PF-003 fix (performance design gate)**: the split-and-recurse
  `c_max_actual_batch_bytes` admission check is the safety bound ONLY
  for a MULTI-object batch (`before_dispatch`'s real split condition is
  `IF lv_actual_bytes > c_max_actual_batch_bytes AND lines(
  it_object_keys ) > 1` - CONFIRMED_SOURCE) - a SINGLE pathologically
  large function group whose own payload alone exceeds the cap cannot be
  split further and is dispatched as an unsplittable singleton batch
  regardless. The real safety net for that specific case is the
  SEPARATE, already-existing, pre-approved post-hoc adaptive-shrink
  mechanism (`c_max_object_output_bytes`/oversized-result handling in
  `ZCL_ABAPGIT_ORTEC_SER_ORCH`, unchanged, applies uniformly to every
  object type including this provider's three new ones with zero code
  change needed) - NOT `c_max_actual_batch_bytes` itself, which this
  section's cycle-1/2/3 wording could be misread as implying.
- No source/include bytes are ever included in this payload (&sect;1
  scope boundary) - this is the deliberate choice that keeps this
  provider's memory profile bounded and metadata-only, unlike a
  hypothetical Option C.

## 9. ORCH wiring / actual-byte admission

Same pattern as Package B &sect;6: `before_dispatch` gains
`lv_prefetch_buffer_fugr = zcl_abapgit_ortec_ser_pref_ext=>
extract_for_batch_fugr( it_object_keys )`, summed into the SAME
`lv_actual_bytes` total the DD/PROG buffers already contribute to (see
Shared Infrastructure &sect;3 for the exact combined-sum requirement
across every provider buffer, not just DD) before the existing
`c_max_actual_batch_bytes` check; `dispatch_batch` gains
`iv_prefetch_buffer_fugr TYPE xstring OPTIONAL`.

## 10. Required test design

```text
small FUGR (1 function module, no extra metadata) - HIT
large FUGR (many function modules, e.g. 50+) - HIT, full enlfdir/func
  round-trips byte-identical
multiple function modules - per-FM func_meta entries independently
  correct, no cross-FM contamination
module texts - **FG-003 fix**: NOW in scope via &sect;6a's new seam - a
  FUGR main program with extra-language text-pool translations produces
  a byte-identical `I18N_TPOOL` XML section under feature OFF, feature
  ON+batch OFF, and feature ON+batch ON; a FUGR main program with ZERO
  extra-language translations correctly produces no `I18N_TPOOL` section
  in all three cases (mirrors PROG's own "present but empty" test,
  Package B &sect;10)
TOP/multiple/customer includes - N/A, out of scope (&sect;1 Option C)
languages/missing optional data - a function group with no TFDIR
  RFCSCOPE/RFCVERS on this release (rfc_fields_valid = abap_false for
  every func_meta row) still round-trips correctly and the consumer falls
  back to the unchanged dynamic SELECT
namespaces (/NS/SAPLZFOO) round trip through area/obj_name key
inactive version - N/A, ENLFDIR/TLIBT/TFDIR reads are not active/
  inactive-version-sensitive the way TABL/PROG source is (unaffected)
stable file order - N/A this package (file/include ordering belongs to
  Option C, out of scope)
mixed small/large batch - some FUGR groups with few FMs, some with many,
  same dispatch, independent correctness
oversized singleton - a single FUGR group whose OWN enlfdir/func payload
  exceeds c_max_actual_batch_bytes dispatches with NO fugr buffer
  attached (existing structural fallback, &sect;8), never a hard error
hit/miss/fallback - one test per &sect;5 row
cross-object/cross-batch isolation - two sequential worker-style
  inject_batch_from_buffer_fugr calls with different area sets; second
  call's clear-first leaves zero trace of the first
direct output parity - compare a real live FUGR's serialized short text/
  ENLFDIR-derived function list/exception_classes/rfcscope/rfcvers XML
  output under feature ON+batch OFF vs feature ON+batch ON vs feature OFF
  - all three byte-identical; IT8-only validation step, same disclosed
  boundary as Package B &sect;10
reject unknown wire version / reject duplicate entries / reject
  object_count mismatch / reject corrupt IMPORT - one test per &sect;5 row
```

## 11. Performance model

```text
representative object count: source-derived estimate only - FUGR is
  common but typically far less numerous than PROG/TABL in a repository
  (HYPOTHESIS, general composition knowledge)
calls/object today (RFC-batch path): 1 SELECT SINGLE areat FROM tlibt +
  1 SELECT * FROM enlfdir + 1 SELECT SINGLE exten3 FROM enlfdir (per FM,
  not per group) + 1 dynamic SELECT SINGLE rfcscope/rfcvers FROM tfdir
  (per FM) - ALL currently MISS under the RFC-batch path per &sect;0's
  root-cause finding, i.e. ALL of these currently execute on every batch
  run despite the seams existing
expected bulk call count: all of the above collapse into the ALREADY-
  EXISTING prepare_fugr bulk reads plus &sect;6's one new bulk TFDIR read
  - no new SQL shape invented beyond &sect;6
rows/batch: 1 areat + 1 enlfdir (nested) + N func rows (N = function
  modules in that group) per HIT group
bytes/batch: MEDIUM, source-derived estimate (&sect;8) - the largest
  metadata-only payload of the three packages in this slice, still far
  below TABL/TTYP's or a hypothetical source-inclusive design's risk
main/worker retained bytes: bounded by the three existing caches, same
  bound in both processes, no new unbounded structure
copy count: 1 (main) -&gt; export buffer -&gt; 1 (worker), same shape as every
  other provider in this codebase
fallback risk: LOW - every failure mode degrades to the pre-existing,
  already-correct per-object read
expected benefit: MODERATE-HIGH for repositories with FUGR-heavy content
  and RFC-batch active (currently ZERO benefit, matching Package B's
  "fix a dead optimization path" framing) - HIGHER absolute per-object
  call-count reduction than PROG (4 distinct query shapes vs PROG's 1),
  but on a lower-prevalence object type
provider-OFF vs provider-ON comparison: same PROVIDER_HIT/PROVIDER_MISS
  IT8 metric as Package B; FUGR objects with batch active should flip
  from always-MISS to mostly-HIT
```
