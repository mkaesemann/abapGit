# SER-SLICE-4 Package B — PROG batch provider design

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_4_PACKAGE_B_PROG_DESIGN
BASELINE_HEAD=e5e10d62137033801739b8a53c69bdff9cfa650e
STATUS=DESIGN_DRAFT_CYCLE_3_AWAITING_REVIEW
CYCLE=3
CYCLE_1_REVIEW=.memory/reviews/serialization_slice_4_prog_adversarial.md
  (REVISE_AND_REVIEW_ONCE, 0 BLOCKER + 3 MAJOR + 1 MINOR: PR-001..PR-004)
CYCLE_2_REVIEW=same artifact, "Cycle 2" section (PR-001..PR-004 all
  CLOSED; new PR-005 MAJOR: a malformed buffer with initial `language`
  could be injected and silently ride on a pooled worker's stale prior
  mv_language, serving HITs it never established itself)
CYCLE_3_FIXES=PR-005 (inject_batch_from_buffer_prog now rejects the
  whole buffer when the imported `language` is initial - extract_for_
  batch_prog can only ever export a non-initial mv_language per its own
  &sect;3 guard, so an initial language is itself proof of corruption;
  mv_language assignment is now unconditional, no more silent-skip
  branch, sect 4/10)
```

## 0. Evidence base (CONFIRMED_SOURCE unless marked otherwise)

- `zcl_abapgit_object_prog.clas.abap` `zif_abapgit_object~serialize`: calls
  `serialize_program` (shared helper in
  `zcl_abapgit_objects_program.clas.abap`), then `serialize_texts`
  (CONFIRMED_SOURCE), then `serialize_longtexts`.
- `zcl_abapgit_objects_program.clas.abap` `serialize_program`
  (CONFIRMED_SOURCE): main source via `CALL FUNCTION 'RPY_PROGRAM_READ'`
  (kernel-level read, no DB-table bulk equivalent exists), progdir via
  `zcl_abapgit_factory=>get_sap_report( )->read_progdir`/`read_report`
  (handles the "inactive version present -&gt; still serialize active source"
  case), then `serialize_dynpros`/`serialize_cua` for module-pool-only
  programs (`subc = '1'` or `'M'`).
- `serialize_texts` (CONFIRMED_SOURCE, `zcl_abapgit_object_prog.clas.abap`
  lines ~97-149): already has a working ORTEC single-object seam -
  `zcl_abapgit_ortec_git_switch=>is_serial_prefetch_active( )` guards a
  call to `zcl_abapgit_ortec_ser_pref_ext=>get_prog_tpool_languages(
  iv_program iv_language )`, which returns `et_tpool_i18n` (the LIST of
  translation languages this program has, from a bulk `D010TINF` prefetch
  built once per whole-repo dispatch by `prepare_prog_langs`). On HIT, the
  method still does `READ TEXTPOOL &lt;prog&gt; LANGUAGE &lt;lang&gt; INTO lt_tpool`
  for EVERY remaining language - this is a KERNEL TEXT-POOL BUFFER READ,
  not a DB SELECT, and has no DB-bulk equivalent; only the `D010TINF`
  language-discovery SELECT is avoided by the prefetch. On MISS, the
  method falls back to `SELECT DISTINCT language FROM d010tinf WHERE
  r3state = 'A' AND prog = ... AND language &lt;&gt; mv_language AND language
  IN lt_language_filter` (unchanged).
- `ZCL_ABAPGIT_ORTEC_SER_PREF_EXT` already owns the CLASS-DATA cache
  `mt_prog_langs TYPE ty_prog_lang_cache_tt` (`HASHED TABLE ... UNIQUE KEY
  program`, row shape `program TYPE d010tinf-prog / tpool_i18n TYPE
  ty_tpool_i18n_tt` where `ty_tpool_i18n_tt` is `STANDARD TABLE OF
  zif_abapgit_lang_definitions=>ty_i18n_tpool`), populated once per whole
  dispatch by `prepare_prog_langs` (called from the class's own `PREPARE`
  entry point, which in turn is called once by
  `ZCL_ABAPGIT_ORTEC_SER_ORCH=>serialize` in the MAIN process before any
  RFC dispatch - CONFIRMED_SOURCE, `zcl_abapgit_ortec_ser_orch.clas.abap`
  lines ~866-877).
- **Root cause of the current zero-benefit-under-RFC-batch gap
  (CONFIRMED_SOURCE, the single most important finding of this design)**:
  `ZCL_ABAPGIT_ORTEC_SER_PREF_EXT` also exposes a GENERIC single-object
  combined buffer, `EXTRACT_FOR_OBJECT`/`INJECT_FROM_BUFFER`, which DOES
  already include `prog_langs` in its `EXPORT ... TO DATA BUFFER` list
  (confirmed in `extract_for_object`/`inject_from_buffer`, lines
  ~1109-1300). However `ZCL_ABAPGIT_ORTEC_SER_ORCH=>before_dispatch`
  NEVER calls `extract_for_object`/populates `iv_prefetch_buffer_ext` -
  it only computes `iv_prefetch_buffer_dd`
  (`ser_pref_ext=&gt;extract_for_batch`, DOMA/DTEL only),
  `iv_prefetch_buffer_oo_batch` (`ser_pref_oo=&gt;extract_for_batch`,
  CLAS/INTF only), and `iv_prefetch_buffer_msag`
  (`ser_pref=&gt;extract_for_batch`, MSAG only) - see the method's own
  doc comment: *"ZCL_ABAPGIT_ORTEC_SER_PREF/_EXT/_OO only expose
  EXTRACT_FOR_OBJECT (one object at a time); their EXPORT/IMPORT wire
  format cannot be safely combined for multiple objects by simple
  concatenation... this method always passes INITIAL (empty) prefetch
  buffers"* (comment predates SER-SLICE-3's `_dd`/`_oo_batch`/`_msag`
  additions but the base `iv_prefetch_buffer`/`iv_prefetch_buffer_ext`
  params remain permanently unpopulated - grep-confirmed: `before_dispatch`
  never assigns either). The RFC worker function
  (`z_abapgit_ortec_ser_batch`) only calls
  `ser_pref_ext=&gt;inject_from_buffer` `IF iv_prefetch_buffer_ext IS NOT
  INITIAL` - since that parameter is always initial in the current call
  chain, `mt_prog_langs` (and `mt_fugr_areat`/`mt_fugr_enlfdir`/
  `mt_fugr_func_meta`/`mt_enhs`/`mt_smim_*`/`mt_tobj`/`mt_tran`) are
  **NEVER populated inside an RFC worker process today**.
- **Consequence (CONFIRMED_SOURCE + logical inference, not yet measured)**:
  with `is_serial_batch_active` at its current IT8-validated default ON
  (`.memory/state.md`), every PROG object serialized through the adaptive
  batch/RFC path today calls `get_prog_tpool_languages` inside a worker
  whose `mt_prog_langs` is empty, so it is an **unconditional MISS**, and
  falls back to the exact same `SELECT DISTINCT language FROM d010tinf`
  the code always had - i.e. the existing PROG seam currently provides
  ZERO benefit whenever the batch/RFC path is active, and only helps on
  the sequential/non-batch-dispatch path (feature ON, batch OFF) where
  `prepare()` populated the SAME (main) process's cache. This is a
  **disclosed, pre-existing limitation of the SER-SLICE-2/3 architecture**,
  not a new defect introduced by this design - HYPOTHESIS (reasoning
  basis: this design's own source-grep evidence above; no live SAT trace
  performed this pass, consistent with the "do not claim runtime
  dominance without measurement" evidence rule).
- Longtext serialization (`zcl_abapgit_longtexts.clas.abap`) has NO ORTEC
  prefetch hook at all (CONFIRMED_SOURCE, grep found zero matches) - out
  of scope for this package, unchanged, a disclosed non-goal.
- `serialize_dynpros`/`serialize_cua` (module-pool screens/menus) also
  have no prefetch seam (CONFIRMED_SOURCE, not found in the discovery
  grep) - out of scope, unchanged.

## 1. Decision

**IMPLEMENT_METADATA_TEXT_PROVIDER.**

Scope: extend the EXISTING `mt_prog_langs` cache (already correct, already
built once per dispatch by `prepare_prog_langs`, already consumed
unchanged by `zcl_abapgit_object_prog.clas.abap`'s `get_prog_tpool_languages`
call) with a batch-scoped `extract_for_batch`/`inject_batch_from_buffer`
pair on `ZCL_ABAPGIT_ORTEC_SER_PREF_EXT`, reusing the generic
`ZAOG_SER_ENV_BHDR`/`ZAOG_SER_ENV_BENTRY`/`ZAOG_SER_ENV_BENTRY_TT` envelope
established for CLAS/INTF (Phase 4) and MSAG (Phase 6) - mirroring those
two providers' exact mechanics, NOT the older bespoke-header DOMA/DTEL
shape. No change to `zcl_abapgit_object_prog.clas.abap` or
`zcl_abapgit_objects_program.clas.abap` source is required - the existing
`get_prog_tpool_languages` call already reads `mt_prog_langs` by table key
and is agnostic to how that cache was populated (single-object PREPARE in
the main process vs. batch injection in an RFC worker).

**Explicitly OUT OF SCOPE (REJECTED for this slice, not a silent gap):**

- Main source (`RPY_PROGRAM_READ`) batching - REJECT. `RPY_PROGRAM_READ`
  is a kernel-level, non-DB-table read (generated program byte-code
  source lines); there is no bulk equivalent, and a hand-rolled
  `REPOSRC`/`REPOTEXT`-level bulk read would have to re-implement
  `RPY_PROGRAM_READ`'s own inactive-vs-active fallback, lowercase
  handling, and 255-char line reconstruction - a fundamentally different
  and much higher-risk undertaking than a DDIC metadata bulk read, with
  no existing prefetch seam to extend (unlike `prog_langs`). Matches the
  discovery ranking's own prior conclusion (`GENERIC_BATCH_ONLY`).
- Dynpros (`serialize_dynpros`)/CUA (`serialize_cua`) - REJECT, no
  existing seam, module-pool-only (`subc = '1'`/`'M'`), lower prevalence
  than the base source/text-pool path, no evidence of benefit.
- Longtexts - REJECT (shared serializer, out of scope for every package
  in this slice, see &sect;0).
- The actual `READ TEXTPOOL` content itself - REJECT/cannot be batched;
  it is a kernel text-pool buffer read (like `RPY_PROGRAM_READ`), not a
  DB SELECT, and already only runs for languages the (now-batchable)
  language-LIST step says exist.

## 2. Wire envelope

Reuses `ZAOG_SER_ENV_BHDR`/`ZAOG_SER_ENV_BENTRY`/`ZAOG_SER_ENV_BENTRY_TT`
(no new DDIC header/entry types). One NEW DDIC row/table-type pair for the
PROG payload, package `$ZAOG_SER` (same as `ZAOG_SER_ENV_*`):

```abap
TYPES: BEGIN OF zaog_ser_prog_brow,        " structure: ZAOG_SER_PROG_BROW
         program    TYPE d010tinf-prog,
         tpool_i18n TYPE zif_abapgit_lang_definitions=>ty_i18n_tpools,
       END OF zaog_ser_prog_brow.
TYPES zaog_ser_prog_brow_tt TYPE STANDARD TABLE OF zaog_ser_prog_brow
  WITH DEFAULT KEY.                        " table type: ZAOG_SER_PROG_BROW_TT
```

This is the byte-identical shape of the existing PRIVATE
`ty_prog_lang_cache` row minus the hashed-key wrapper (a plain standard
table for wire transport; the worker re-inserts into its own
`mt_prog_langs` HASHED TABLE on receipt) - no new field, no new
derivation, purely a transport-shape mirror of data that is already
proven correct by the pre-existing single-object seam.

`ZAOG_SER_ENV_BHDR-PROVIDER_ID = 'SER_PROG'` (8 chars, fits `CHAR8`).

## 3. `extract_for_batch_prog` (decision-free pseudocode, on
`ZCL_ABAPGIT_ORTEC_SER_PREF_EXT`, mirrors `ser_pref_oo`'s CLAS/INTF
`extract_for_batch` structurally - **PR-001/PR-003 FIX**: renamed from
cycle-1's bare `extract_for_batch` (which collides with the EXISTING
DOMA/DTEL method of that exact name on the SAME class) and no longer
references a nonexistent `mv_prepared` field - `ZCL_ABAPGIT_ORTEC_SER_
PREF_EXT` has NO `mv_prepared` CLASS-DATA (CONFIRMED_SOURCE, grep
confirmed only `mv_language` exists); its OWN `PREPARE` method sets
`mv_language = iv_language` unconditionally as its first step
(CONFIRMED_SOURCE, `zcl_abapgit_ortec_ser_pref_ext.clas.abap` `METHOD
prepare`), and the class's own existing code comments this as the
prepared-state indicator ("a worker session that never called PREPARE()
has MV_LANGUAGE initial") - `mv_language IS INITIAL` is the correct,
source-consistent guard, mirrored below)

```abap
METHOD extract_for_batch_prog.             " NEW public class-method
  DATA lt_entries TYPE zaog_ser_env_bentry_tt.
  DATA lt_prog    TYPE zaog_ser_prog_brow_tt.
  DATA ls_hdr     TYPE zaog_ser_env_bhdr.
  DATA lv_any_hit TYPE abap_bool.

  IF mv_language IS INITIAL.               " PR-003 fix: PREPARE never
                                            " ran this session/worker
    RETURN.
  ENDIF.

  LOOP AT it_object_keys INTO DATA(ls_tadir) WHERE object = 'PROG'.
    DATA(ls_entry) = VALUE zaog_ser_env_bentry(
      obj_type = ls_tadir-object
      obj_name = ls_tadir-obj_name ).

    READ TABLE mt_prog_langs INTO DATA(ls_prog)
      WITH TABLE KEY program = CONV d010tinf-prog( ls_tadir-obj_name ).
    IF sy-subrc = 0.
      " A present-but-genuinely-empty tpool_i18n (program exists in no
      " extra language) is still a real HIT, not a MISS - mirrors the
      " DD/OO_BATCH "valid empty payload" distinction (provider_
      " contract.md &sect;3).
      APPEND VALUE #( program = ls_prog-program
                       tpool_i18n = ls_prog-tpool_i18n ) TO lt_prog.
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
  ls_hdr-provider_id         = 'SER_PROG'.
  ls_hdr-object_count        = lines( lt_entries ).

  EXPORT hdr      = ls_hdr
         entries  = lt_entries
         prog     = lt_prog
         language = mv_language
    TO DATA BUFFER rv_buffer COMPRESSION ON.
ENDMETHOD.
```

`READ TABLE mt_prog_langs ... WITH TABLE KEY program` inside a `LOOP AT
it_object_keys` is O(1) per object (hashed key), so this is O(n) overall
for a dispatch of n PROG objects, no new SQL.

## 4. `inject_batch_from_buffer_prog` (decision-free pseudocode,
mirrors `ser_pref_oo`'s CLAS/INTF `inject_batch_from_buffer` validation
sequence - unknown-version / object_count-mismatch / duplicate-entry /
corrupt-IMPORT all reject the WHOLE buffer, never a partial import -
**PR-001/PR-002/PR-003 FIX**: renamed from cycle-1's bare
`inject_batch_from_buffer` (collides with the EXISTING DOMA/DTEL method
name on the SAME class - the cycle-1 doc's OWN section 9 already
required a suffix but sections 3/4/6 never actually used one, a real
internal contradiction the review caught); removed the invented `mv_
prepared` field per &sect;3; and added `provider_id`/entry-type/entry-
state/duplicate-payload/payload-to-entry correlation validation the
cycle-1 pseudocode omitted, matching Package A &sect;6's TT-004 fix)

```abap
METHOD inject_batch_from_buffer_prog.
  DATA ls_hdr     TYPE zaog_ser_env_bhdr.
  DATA lt_entries TYPE zaog_ser_env_bentry_tt.
  DATA lt_prog    TYPE zaog_ser_prog_brow_tt.
  DATA lt_entries_sorted TYPE STANDARD TABLE OF zaog_ser_env_bentry WITH DEFAULT KEY.
  DATA lt_prog_sorted TYPE STANDARD TABLE OF zaog_ser_prog_brow WITH DEFAULT KEY.
  DATA lv_lines_before TYPE i.
  DATA lv_language TYPE spras.

  CHECK iv_buffer IS NOT INITIAL.

  TRY.
      IMPORT hdr = ls_hdr entries = lt_entries prog = lt_prog
             language = lv_language FROM DATA BUFFER iv_buffer.
    CATCH cx_root INTO DATA(lx_import).
      zcx_abapgit_exception=>raise(
        |ORTEC PROG batch prefetch buffer is corrupt: { lx_import->get_text( ) }| ).
  ENDTRY.
  IF sy-subrc <> 0.
    zcx_abapgit_exception=>raise( 'ORTEC PROG batch prefetch buffer: IMPORT failed' ).
  ENDIF.
  IF ls_hdr-wire_format_version <> 1.
    zcx_abapgit_exception=>raise(
      |ORTEC PROG batch prefetch buffer: unknown wire_format_version { ls_hdr-wire_format_version }| ).
  ENDIF.
  IF ls_hdr-provider_id <> 'SER_PROG'.               " PR-002 fix
    zcx_abapgit_exception=>raise(
      'ORTEC PROG batch prefetch buffer: unexpected provider_id' ).
  ENDIF.
  IF ls_hdr-object_count <> lines( lt_entries ).
    zcx_abapgit_exception=>raise(
      'ORTEC PROG batch prefetch buffer: object_count does not match ENTRIES' ).
  ENDIF.
  IF lv_language IS INITIAL.                            " PR-005 fix
    zcx_abapgit_exception=>raise(
      'ORTEC PROG batch prefetch buffer: missing language' ).
  ENDIF.

  " PR-002 fix: every entry must be a known, PROG-typed, P/M state -
  " reject anything else as corrupt/foreign, never treat it as a normal
  " per-object MISS.
  LOOP AT lt_entries INTO DATA(ls_check_entry).
    IF ls_check_entry-obj_type <> 'PROG' OR
       ( ls_check_entry-state <> 'P' AND ls_check_entry-state <> 'M' ).
      zcx_abapgit_exception=>raise(
        'ORTEC PROG batch prefetch buffer: unexpected entry type or state' ).
    ENDIF.
  ENDLOOP.

  lt_entries_sorted = CORRESPONDING #( lt_entries ).
  SORT lt_entries_sorted BY obj_type obj_name.
  lv_lines_before = lines( lt_entries_sorted ).
  DELETE ADJACENT DUPLICATES FROM lt_entries_sorted COMPARING obj_type obj_name.
  IF lines( lt_entries_sorted ) <> lv_lines_before.
    zcx_abapgit_exception=>raise(
      'ORTEC PROG batch prefetch buffer: duplicate entry in ENTRIES' ).
  ENDIF.

  " PR-002 fix: reject duplicate PROG payload rows (same program twice)
  " BEFORE any INSERT - a HASHED-table INSERT would otherwise silently
  " collapse the duplicate instead of rejecting the whole buffer.
  lt_prog_sorted = CORRESPONDING #( lt_prog ).
  SORT lt_prog_sorted BY program.
  DATA(lv_prog_lines_before) = lines( lt_prog_sorted ).
  DELETE ADJACENT DUPLICATES FROM lt_prog_sorted COMPARING program.
  IF lines( lt_prog_sorted ) <> lv_prog_lines_before.
    zcx_abapgit_exception=>raise(
      'ORTEC PROG batch prefetch buffer: duplicate prog payload row' ).
  ENDIF.

  " PR-002 fix: canonical object-key correlation - every 'P' entry must
  " have EXACTLY ONE prog payload row, and every prog payload row must
  " correspond to a 'P' entry (never an unauthorized/unexpected key).
  DATA(lt_p_entries) = lt_entries.
  DELETE lt_p_entries WHERE state <> 'P'.
  IF lines( lt_prog_sorted ) <> lines( lt_p_entries ).
    zcx_abapgit_exception=>raise(
      'ORTEC PROG batch prefetch buffer: prog payload does not match P entries 1:1' ).
  ENDIF.
  LOOP AT lt_prog_sorted INTO DATA(ls_prog_check).
    READ TABLE lt_p_entries TRANSPORTING NO FIELDS
      WITH KEY obj_name = ls_prog_check-program.
    IF sy-subrc <> 0.
      zcx_abapgit_exception=>raise(
        'ORTEC PROG batch prefetch buffer: prog payload key not in P entries' ).
    ENDIF.
  ENDLOOP.

  CLEAR mt_prog_langs.                     " unconditional clear-first,
                                            " same rationale as
                                            " CLEAR_DD_CACHE/CLEAR_OO_CACHE
  LOOP AT lt_prog INTO DATA(ls_prog).
    INSERT VALUE ty_prog_lang_cache( program = ls_prog-program
                                      tpool_i18n = ls_prog-tpool_i18n )
      INTO TABLE mt_prog_langs.
  ENDLOOP.

  mv_language = lv_language.               " PR-005 fix: unconditional -
                                            " &sect;4's own new guard
                                            " above already proved
                                            " lv_language is non-initial,
                                            " so there is no longer an
                                            " "IF lv_language IS NOT
                                            " INITIAL" branch that could
                                            " silently skip establishing
                                            " this batch's own prepared-
                                            " state and let a malformed/
                                            " language-less buffer ride
                                            " on a PRIOR dispatch's
                                            " stale mv_language (PR-005,
                                            " cycle-2 adversarial finding)
ENDMETHOD.

METHOD clear_prog_cache.                   " NEW, narrow clear, mirrors
                                            " CLEAR_OO_CACHE/CLEAR_DD_CACHE
  CLEAR mt_prog_langs.
ENDMETHOD.
```

## 5. Required behavior mapping

```text
unknown wire_format_version / corrupt IMPORT / object_count mismatch /
  duplicate entry
  -> reject WHOLE buffer via zcx_abapgit_exception; RFC worker catches
     and swallows it (##NO_HANDLER pattern, matching DD/OO_BATCH/MSAG),
     treats as full MISS for PROG this dispatch only - never aborts the
     batch.
missing object entry (PROG obj_name not in ENTRIES)
  -> PROVIDER MISS; get_prog_tpool_languages already returns rv_found =
     abap_false in this case (unchanged existing contract), caller falls
     back to the existing SELECT DISTINCT ... FROM d010tinf.
unexpected object entry (present in ENTRIES/prog but not in this
  dispatch's own it_tadir)
  -> ignored deterministically - lookup is by exact (obj_type=PROG,
     obj_name) key via mt_prog_langs, never by position.
empty object payload (tpool_i18n empty - program has no extra-language
  translations at all)
  -> valid HIT with an empty language list; get_prog_tpool_languages
     returns rv_found = abap_true with et_tpool_i18n empty, exactly what
     a correctly-prefetched "no translations" program looks like today.
```

## 6. Actual-byte admission and ORCH wiring

New `ZCL_ABAPGIT_ORTEC_SER_ORCH` parameter `iv_prefetch_buffer_prog`,
threaded exactly like `iv_prefetch_buffer_msag`:

```text
BEFORE_DISPATCH (new line, alongside the existing _dd/_oo_batch/_msag
  computations):
  DATA(lv_prefetch_buffer_prog) =
    zcl_abapgit_ortec_ser_pref_ext=>extract_for_batch_prog( it_object_keys ).
    " NOTE: DD's OWN extract_for_batch (DOMA/DTEL, existing/unsuffixed)
    " and PROG's NEW extract_for_batch_prog are two DIFFERENT public
    " methods on the SAME class ZCL_ABAPGIT_ORTEC_SER_PREF_EXT - see
    " &sect;9 naming note for why they must NOT share a name/overload
    " (PR-001 fix: cycle 1's pseudocode incorrectly called the bare,
    " already-taken `extract_for_batch` name).
  " lv_actual_bytes admission check gains
  " + xstrlen( lv_prefetch_buffer_prog ) to its existing
  " xstrlen( lv_prefetch_buffer_dd ) sum (see &sect;7 of this design for
  " the exact combined-sum requirement, consistent with the Package
  " C/shared-infra note that ALL provider buffers must be summed before
  " the SAME c_max_actual_batch_bytes gate, not just the DD buffer).
DISPATCH_BATCH gains iv_prefetch_buffer_prog TYPE xstring OPTIONAL,
  threaded through unchanged to the RFC CALL FUNCTION, mirroring
  iv_prefetch_buffer_msag exactly.
```

RFC worker (`z_abapgit_ortec_ser_batch`) gains, in the SAME position as
the existing DD/OO_BATCH/MSAG blocks:

```abap
zcl_abapgit_ortec_ser_pref_ext=>clear_prog_cache( ).
IF iv_prefetch_buffer_prog IS NOT INITIAL.
  TRY.
      zcl_abapgit_ortec_ser_pref_ext=>inject_batch_from_buffer_prog( iv_prefetch_buffer_prog ).
    CATCH zcx_abapgit_exception ##NO_HANDLER.
  ENDTRY.
ENDIF.
```

and the worker's own `CASE ls_tadir-object` HIT/MISS reporting gains:

```abap
WHEN 'PROG'.
  IF zcl_abapgit_ortec_ser_pref_ext=>get_prog_tpool_languages(
       iv_program = CONV #( ls_tadir-obj_name )
       iv_language = iv_language
       IMPORTING et_tpool_i18n = DATA(lt_ignored) ) = abap_true.
    ls_result-provider_hit = 1.
  ELSE.
    ls_result-provider_miss = 1.
  ENDIF.
```

(this is purely a HIT/MISS TELEMETRY probe identical in shape to the
DOMA/DTEL/CLAS/INTF cases already in the worker - the REAL call inside
`zcl_abapgit_object_prog.clas.abap`'s own `serialize_texts` is unchanged
and happens later, in the normal serialization call below this CASE.)

## 7. Memory bounds

- `mt_prog_langs` row size is bounded by the number of extra languages a
  program has translations for (typically single digits) times a handful
  of small fields (`ty_i18n_tpool` - language + textpool table, itself
  small: TEXTPOOL entries are short key/value pairs) - SOURCE_DERIVED
  ESTIMATE, not measured: a batch of even a few thousand PROG objects
  with, say, 5 extra languages each and modest text pools should still
  produce a buffer in the low single-digit megabytes, well under
  `c_max_actual_batch_bytes` (12582912 bytes) even before compression
  (`EXPORT ... COMPRESSION ON`).
- No new retained state beyond the existing `mt_prog_langs` (already
  exists, already bounded by the same PREPARE-time collection as today).
- Worker-side memory: one extra `CLEAR`+`LOOP AT ... INSERT` pass over
  `lt_prog` at inject time - O(n) in the batch's own PROG object count,
  no larger than the batch itself.

## 8. Fallback and cleanup

- Every path above degrades to the PRE-EXISTING per-object
  `get_prog_tpool_languages` MISS branch (unchanged `SELECT DISTINCT
  language FROM d010tinf`) - there is no new failure mode that does not
  already exist today.
- `clear_prog_cache` is called UNCONDITIONALLY first on every worker
  invocation (never only on successful inject), matching the DD/OO_BATCH/
  MSAG precedent exactly - a pooled/reused RFC worker session must never
  carry a PRIOR dispatch's `mt_prog_langs` content into a batch whose OWN
  PROG buffer is legitimately empty.
- Feature OFF (`is_serial_prefetch_active = abap_false`): `serialize_texts`
  never calls `get_prog_tpool_languages` at all (unchanged existing
  guard) - this provider has zero code-path reachability, exactly like
  every other provider in this codebase.

## 9. Naming note (must be resolved by the implementer, not left TBD)

`ZCL_ABAPGIT_ORTEC_SER_PREF_EXT` will host THREE unrelated
`extract_for_batch`/`inject_batch_from_buffer`-shaped method pairs once
this design and Package A (TABL/TTYP, same class) are both implemented:
DOMA/DTEL (existing), PROG (this package), TABL/TTYP (Package A). ABAP
does not support method overloading by parameter type alone, and the
existing DOMA/DTEL pair already occupies the exact names
`extract_for_batch`/`inject_batch_from_buffer`. This design's methods
MUST be named distinctly:

```text
extract_for_batch_prog / inject_batch_from_buffer_prog / clear_prog_cache
```

(Package A's design in
`.memory/logs/serialization_slice_4_tabl_ttyp_design.md` &sect;9 uses the
analogous `_tabl` suffix for the same reason (`_tabl`, not `_tabl_ttyp` -
that design's own cycle-2 revision shortened it to fit ABAP's 30-
character method-name limit) - this is a REQUIRED, decision-free naming
convention for this slice, not a style preference, because a bare
rename of the existing DOMA/DTEL pair to add a suffix would be an
unnecessary, unrelated, higher-risk change to already IT8-validated
code, which SER-SLICE-4 must not touch. **PR-001 cycle-1 finding**: the
rule above was already stated correctly in cycle 1's own section 9, but
sections 3/4/6's pseudocode still called the bare, already-taken
`extract_for_batch`/`inject_batch_from_buffer` names - an internal
contradiction the adversarial review caught; cycle 2 fixed sections
3/4/6 to actually use `extract_for_batch_prog`/
`inject_batch_from_buffer_prog` throughout.)

## 10. Required test design

```text
small program (no extra languages) - HIT with empty tpool_i18n, matches
  existing single-object rv_found=abap_true/et_tpool_i18n empty contract
large program (many extra languages, e.g. 10+) - HIT, full tpool_i18n
  round-trips byte-identical
multiple languages round trip (2-3 languages, distinct textpool content
  per language, order preserved after SORT BY language)
no languages (program only exists in main language) - HIT with empty
  list (not a MISS - distinct from "program not in this batch at all")
with/without documentation - N/A for this narrow slice (documentation is
  the shared longtexts path, explicitly out of scope, &sect;1)
namespaces (/NS/ZPROGRAM) round trip through obj_name/program key
inactive version present - N/A for prog_langs (D010TINF language
  discovery does not depend on active/inactive source state; unaffected
  by this provider, unchanged from today)
mixed small/large batch (some PROG objects with 0 languages, some with
  many, in the same dispatch) - per-entry state independent, no
  cross-contamination
oversized singleton - N/A, tpool_i18n payload sizes are far below any
  realistic singleton-exceeds-limit scenario (source-derived estimate,
  &sect;7); if a future measurement contradicts this, the existing
  c_max_actual_batch_bytes split-and-recurse logic already handles it
  structurally without any code change specific to PROG
hit/miss/fallback - covered by &sect;5's required behavior mapping,
  one test per row
cross-batch isolation - two sequential worker-style
  inject_batch_from_buffer_prog calls with DIFFERENT program sets; the
  second call's clear-first must leave zero trace of the first call's
  programs
direct output parity - compare the .prog.xml I18N_TPOOL section produced
  via a real live PROG object under (a) feature ON, batch OFF (prepare-
  populated main-process cache) vs (b) feature ON, batch ON (RFC-worker-
  injected cache) vs (c) feature OFF (no prefetch at all) - all three
  must be byte-identical; this is the IT8-only validation step (local
  tests can only prove the extract/inject round trip is lossless for the
  provider's own cached data, not full file-output parity - same
  disclosed boundary CLAS/INTF had, &sect;"Local validation" of
  `serialization_slice_3_clas_intf.md`)
reject unknown wire version / reject duplicate entries / reject
  object_count mismatch / reject corrupt IMPORT - one test per &sect;5 row
reject unexpected provider_id / reject non-PROG entry type or invalid
  entry state / reject duplicate prog payload row / reject prog payload
  row without a matching P entry / reject P entry without a matching
  prog payload row - one test per PR-002's added validation (cycle-2 fix)
failed-inject leaves cache clean - a rejected (corrupt/mismatched) buffer
  must leave `mt_prog_langs` exactly as `clear_prog_cache` left it
  (empty), never partially populated from the rejected IMPORT (PR-004
  fix)
reject initial language (PR-005 fix, cycle 3) - a buffer whose imported
  `language` is initial must be rejected in full, even if `hdr`/`entries`/
  `prog` are otherwise well-formed; add a worker-lifecycle test: inject a
  valid `language = 'E'` buffer, then (worker-style) `clear_prog_cache`
  followed by injecting a second, malformed buffer with `language`
  initial - the second inject must raise and leave `mt_prog_langs` empty;
  a subsequent `get_prog_tpool_languages( iv_language = 'E' )` call must
  return a clean MISS, never a stale HIT from the first buffer's data
```

## 11. Performance model

```text
representative object count: source-derived estimate only, not measured
  this pass - PROG is one of the most prevalent abapGit object types in
  any real repository (HYPOTHESIS, general ABAP repository composition
  knowledge, not a repository-specific count)
calls/object today (RFC-batch path): 1 CALL FUNCTION 'RPY_PROGRAM_READ'
  (unaffected by this design) + 1 SELECT DISTINCT ... FROM d010tinf per
  object (THIS is what the provider eliminates) + 1 READ TEXTPOOL per
  extra language (unaffected)
expected bulk call count: the d010tinf per-object SELECT is entirely
  replaced by the ALREADY-EXISTING single bulk prepare_prog_langs SELECT
  (no new SQL introduced by this design at all - the only NEW cost is the
  EXPORT/IMPORT wire-transport step itself)
rows/batch: 1 row per PROG object with &gt;=1 extra-language translation in
  the dispatch (HIT rows only; MISS objects still get an ENTRIES row but
  no payload row)
bytes/batch: source-derived estimate, low (&sect;7)
main/worker retained bytes: bounded by mt_prog_langs, same bound in both
  processes
copy count: 1 (main process cache) -&gt; export buffer -&gt; 1 (worker process
  cache) - identical shape to DD/OO_BATCH/MSAG, no additional copy
fallback risk: LOW - every failure mode degrades to the pre-existing,
  already-correct per-object SELECT, never to wrong data
expected benefit: MODERATE for repositories with heavy PROG i18n
  translation usage under the RFC-batch path (currently ZERO benefit per
  &sect;0's root-cause finding); LOW/NONE for repositories with few or no
  extra-language PROG translations, since the eliminated cost (a single
  SELECT DISTINCT language FROM d010tinf, typically a handful of rows) is
  already cheap in absolute terms - this is fundamentally a
  fix-a-currently-dead-optimization-path package, not a new large win
provider-OFF vs provider-ON comparison: with is_serial_prefetch_active=
  abap_true and is_serial_batch_active=abap_true (current IT8-validated
  default), OFF means the RFC worker's mt_prog_langs stays empty forever
  (today's actual behavior) and ON means it is correctly populated -
  the practical metric to gather at IT8 is PROVIDER_HIT/PROVIDER_MISS
  counts on real PROG objects with translations, which are CURRENTLY
  ALWAYS PROVIDER_MISS under the batch path (this design's whole point is
  to make them PROVIDER_HIT)
```
