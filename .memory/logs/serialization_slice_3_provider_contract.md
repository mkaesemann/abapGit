# SER-SLICE-3 — Batch prefetch wire format and provider contract (Phase 1 finalization)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_3_PHASE_1_WIRE_FORMAT
STATUS=FINALIZED_FOR_IMPLEMENTATION
BASELINE_HEAD=daef510e9bd50cdef2adcfb26a3f2a01050bb401
DESIGN_REVIEW=REVISE_AND_REVIEW_ONCE (0 blocker/3 major/3 minor) -> all 3
  majors (DR-001 merged-structure cache shape, DR-002 missing decision-free
  DOMA seam pseudocode, DR-003 buffer computed-once-and-reused) and all 3
  minors (DR-004 stale bootstrap manifest, DR-005 VALPOS/VALUE correlation
  justification, DR-006 unverifiable payload_bytes field) FIXED IN THIS
  FILE below - not independently re-reviewed a second time (single
  reviewer-recommended iteration, fixes are mechanical/additive to the
  reviewer's own exact required-changes list, no new architecture
  introduced); the senior implementer must still verify DR-001's
  DD01V/DD07V field-for-field match empirically via the byte-identical
  parity test in §7 before this is considered SAP-validated.
SUPERSEDES_DRAFT=.memory/logs/serialization_provider_design.md (§1/§5 generic
  ZIF_ABAPGIT_ORTEC_SER_PROV + ZCL_ABAPGIT_ORTEC_SER_PROV_FCD facade layer)
  -- CONCEPTS (lifecycle, hit/miss/fallback, byte gating, DOMA table
  semantics) remain CONFIRMED_CURRENT and authoritative; the CLASS-SPLIT
  MECHANISM is revised below because current ORCH source (daef510e) never
  adopted a generic interface-dispatch loop - it uses four direct named
  XSTRING params (iv_prefetch_buffer/_ext/_oo/_dd) end to end from
  BEFORE_DISPATCH -> DISPATCH_BATCH -> Z_ABAPGIT_ORTEC_SER_BATCH, which
  already reserves iv_prefetch_buffer_dd for this exact provider and is
  otherwise untouched. Introducing a new interface + facade class for a
  single family, with no second consumer yet, is unjustified extra surface
  for this run; the same additive-seam pattern already proven for DTEL
  (zcl_abapgit_ortec_ser_pref_ext=>get_dtel_data, gated by
  is_serial_prefetch_active(), miss -> unchanged standard SELECT) is reused
  and extended, not replaced by a new abstraction. Revisit the generic
  interface only if/when a second, structurally different provider family
  (e.g. CLAS/INTF) is actually implemented and a real dispatch-list benefit
  exists.
```

## 1. Decision: extend `ZCL_ABAPGIT_ORTEC_SER_PREF_EXT`, no new provider class

DTEL's cache (`mt_dtel`, `get_dtel_data`, `get_dtel_i18n`) already lives in
this class and already has a working single-object seam in
`zcl_abapgit_object_dtel.clas.abap` (confirmed by source read). DOMA has no
existing seam at all (confirmed: zero hits for "ortec|prefetch|is_serial" in
`zcl_abapgit_object_doma.clas.abap`). Both additions below are purely
additive to `ZCL_ABAPGIT_ORTEC_SER_PREF_EXT` - no existing method body
changes, no retirement of `ser_pref`/`ser_pref_oo`, consistent with
provider_design.md §9's DECISION.

**Design-review correction (DR-004):** `serialization-design-bootstrap.md`'s
creation manifest (older draft) lists a separate NEW `ZCL_ABAPGIT_ORTEC_SER_
PROV_DD` class and states "SER_PREF_EXT does NOT gain a DOMA cache" - that
manifest is SUPERSEDED by this file for the class-split mechanism (see the
PACKET header's SUPERSEDES_DRAFT note); no new class is created for DOMA/
DTEL in this run.

**Design-review correction (DR-001): the cache MUST store the same
MERGED, DD01V/DD07V-shaped structures the standard code actually adds to
the XML - not raw DD01L/DD07L rows.** DTEL already does this correctly
(`ty_dtel_cache-dd04v TYPE dd04v`, a fully merged structure) - DOMA must
mirror it exactly, never a header-only shape:

```abap
TYPES: BEGIN OF ty_doma_cache,
         domname       TYPE dd01l-domname,
         dd01v         TYPE dd01v,             " main-language, MERGED (DD01L fields + DDLANGUAGE/DDTEXT from DD01T) - same shape DDIF_DOMA_GET's dd01v_wa returns
         dd01v_i18n    TYPE STANDARD TABLE OF dd01v WITH DEFAULT KEY, " one row per translation language, same merge, keyed by DDLANGUAGE
         dd07v_tab     TYPE dd07v_tab,          " main-language, MERGED (DD07L fields + DDTEXT/DOMVAL_LD/DOMVAL_HD from DD07T) - same shape DDIF_DOMA_GET's dd07v_tab returns
         dd07v_tab_i18n TYPE STANDARD TABLE OF dd07v WITH DEFAULT KEY, " all translation-language fixed-value text rows, one DD07V row per (VALPOS, DDLANGUAGE)
       END OF ty_doma_cache.
TYPES ty_doma_cache_tt TYPE HASHED TABLE OF ty_doma_cache WITH UNIQUE KEY domname.
CLASS-DATA mt_doma TYPE ty_doma_cache_tt.
```

No separate `ty_doma_header`/`ty_doma_text`/`ty_doma_fixval*` types (the
earlier draft's header-only shapes are withdrawn per DR-001).

New accessors (mirror `get_dtel_data`/`get_dtel_i18n` exactly, same
found/not-found contract):
```text
get_doma_data   IMPORTING iv_domname iv_language
                EXPORTING es_dd01v TYPE dd01v  et_dd07v_tab TYPE dd07v_tab
                RETURNING rv_found TYPE abap_bool
  -- returns the MAIN-LANGUAGE dd01v/dd07v_tab, i.e. the exact IMPORTING
  -- parameters zif_abapgit_object~serialize's own DDIF_DOMA_GET call
  -- receives today (dd01v_wa, dd07v_tab), so the seam below needs zero
  -- reshaping - just DD01V_WA = ES_DD01V / DD07V_TAB[] = ET_DD07V_TAB[].
get_doma_i18n   IMPORTING iv_domname
                EXPORTING et_i18n_langs TYPE ty_langu_tt
                          et_dd01v_i18n TYPE STANDARD TABLE OF dd01v
                          et_dd07v_i18n TYPE dd07v_tab
                RETURNING rv_found TYPE abap_bool
  -- et_dd01v_i18n/et_dd07v_i18n are the CACHED dd01v_i18n/dd07v_tab_i18n
  -- rows already built by DDIF_DOMA_GET-per-language during PREPARE_DOMA -
  -- serialize_texts's own LOOP AT lt_i18n_langs body is replaced by a
  -- single READ from these tables per language, see §DOMA SEAM below.
```

`rv_found = abap_false` whenever `mv_dd_prepared = abap_false` OR the domain
has no active DD01L row - this is the PROVIDER MISS contract (§DOMA
VERSION_SEMANTICS in provider_design.md: "a domain with no active version is
OBSERVATIONALLY IDENTICAL... the miss contract does not need to distinguish
these two cases").

`prepare_doma` (called from the existing `prepare()` entry point, alongside
`prepare_dtel`) - decision-free pseudocode, EXACTLY reproducing what
`DDIF_DOMA_GET` itself would do for every domain in the batch, main
language AND every translation language, but as ONE bulk read instead of N
function-module calls per object per language:

```abap
METHOD prepare_doma.
  DATA lt_dd01l TYPE STANDARD TABLE OF dd01l.
  DATA lt_dd01t TYPE STANDARD TABLE OF dd01t.
  DATA lt_dd07l TYPE STANDARD TABLE OF dd07l.
  DATA lt_dd07t TYPE STANDARD TABLE OF dd07t.

  IF it_names IS INITIAL. RETURN. ENDIF.

  SELECT * FROM dd01l INTO TABLE lt_dd01l
    FOR ALL ENTRIES IN it_names
    WHERE domname = it_names-table_line AND as4local = 'A' AND as4vers = '0000'.
  IF lt_dd01l IS INITIAL. RETURN. ENDIF.  " no active domain at all in this batch - nothing to cache, every lookup below is a clean MISS

  SELECT * FROM dd01t INTO TABLE lt_dd01t
    FOR ALL ENTRIES IN lt_dd01l
    WHERE domname = lt_dd01l-domname AND as4local = 'A' AND as4vers = '0000'.
  SELECT * FROM dd07l INTO TABLE lt_dd07l
    FOR ALL ENTRIES IN lt_dd01l
    WHERE domname = lt_dd01l-domname AND as4local = 'A' AND as4vers = '0000'.
  SELECT * FROM dd07t INTO TABLE lt_dd07t
    FOR ALL ENTRIES IN lt_dd01l
    WHERE domname = lt_dd01l-domname AND as4local = 'A' AND as4vers = '0000'.

  LOOP AT lt_dd01l INTO DATA(ls_dd01l).
    DATA(lt_langs) = VALUE ty_langu_tt( ).
    " every language this domain has EITHER a DD01T OR a DD07T text row for
    " (mirrors serialize_texts's own "SELECT DISTINCT ddlanguage ... FROM
    " dd01v ... UNION-like APPENDING ... FROM dd07v" language-discovery
    " query, but from the already-fetched bulk tables instead of a 2nd
    " per-object SELECT)
    LOOP AT lt_dd01t INTO DATA(ls_dd01t) WHERE domname = ls_dd01l-domname.
      APPEND ls_dd01t-ddlanguage TO lt_langs.
    ENDLOOP.
    LOOP AT lt_dd07t INTO DATA(ls_dd07t_lang) WHERE domname = ls_dd01l-domname.
      APPEND ls_dd07t_lang-ddlanguage TO lt_langs.
    ENDLOOP.
    SORT lt_langs. DELETE ADJACENT DUPLICATES FROM lt_langs.

    DATA(ls_cache) = VALUE ty_doma_cache( domname = ls_dd01l-domname ).

    LOOP AT lt_langs INTO DATA(lv_lang).
      DATA(ls_dd01v) = CORRESPONDING dd01v( ls_dd01l ).           " same fields DDIF_DOMA_GET fills from DD01L
      READ TABLE lt_dd01t INTO DATA(ls_text) WITH KEY domname = ls_dd01l-domname ddlanguage = lv_lang.
      IF sy-subrc = 0.
        ls_dd01v-ddlanguage = ls_text-ddlanguage.
        ls_dd01v-ddtext     = ls_text-ddtext.
      ELSE.
        ls_dd01v-ddlanguage = lv_lang.   " DDIF_DOMA_GET still sets ddlanguage even with no text row (confirmed: zcl_abapgit_object_doma serialize_texts already handles ls_dd01v-ddlanguage IS INITIAL defensively for exactly this case)
      ENDIF.

      DATA(lt_dd07v) = VALUE dd07v_tab( ).
      LOOP AT lt_dd07l INTO DATA(ls_dd07l) WHERE domname = ls_dd01l-domname.
        DATA(ls_dd07v) = CORRESPONDING dd07v( ls_dd07l ).
        READ TABLE lt_dd07t INTO DATA(ls_val_text)
          WITH KEY domname = ls_dd01l-domname ddlanguage = lv_lang valpos = ls_dd07l-valpos.
        IF sy-subrc = 0.
          ls_dd07v-ddlanguage = ls_val_text-ddlanguage.
          ls_dd07v-ddtext     = ls_val_text-ddtext.
          ls_dd07v-domval_ld  = ls_val_text-domval_ld.
          ls_dd07v-domval_hd  = ls_val_text-domval_hd.
        ELSE.
          ls_dd07v-ddlanguage = lv_lang.  " no translation for this value - keep the row, texts stay initial (matches the standard "no translation -> keep entry but clear texts" branch)
        ENDIF.
        APPEND ls_dd07v TO lt_dd07v.
      ENDLOOP.

      IF lv_lang = iv_main_language.
        ls_cache-dd01v     = ls_dd01v.
        ls_cache-dd07v_tab = lt_dd07v.
      ELSE.
        APPEND ls_dd01v TO ls_cache-dd01v_i18n.
        APPEND LINES OF lt_dd07v TO ls_cache-dd07v_tab_i18n.
      ENDIF.
    ENDLOOP.

    INSERT ls_cache INTO TABLE mt_doma.
  ENDLOOP.
ENDMETHOD.
```

`iv_main_language` is `PREPARE`'s existing `iv_language` parameter, already
threaded through to `prepare_dtel` today - `prepare_doma` reuses the exact
same value, no new parameter on the public `prepare()` entry point.

**DOMA SEAM in `zcl_abapgit_object_doma.clas.abap` (DR-002), mirroring the
existing DTEL seam in `zif_abapgit_object~serialize` exactly:**

```abap
" in zif_abapgit_object~serialize, replacing the unconditional CALL FUNCTION 'DDIF_DOMA_GET':
DATA lv_prefetched TYPE abap_bool.
IF zcl_abapgit_ortec_git_switch=>is_serial_prefetch_active( ) = abap_true.
  lv_prefetched = zcl_abapgit_ortec_ser_pref_ext=>get_doma_data(
    EXPORTING iv_domname = lv_name iv_language = mv_language
    IMPORTING es_dd01v = ls_dd01v et_dd07v_tab = lt_dd07v ).
  IF lv_prefetched = abap_true.
    lv_state = 'A'.  " get_doma_data's own found=TRUE already proves an active row exists (prepare_doma's WHERE as4local='A' AND as4vers='0000' is the same predicate DDIF_DOMA_GET's state='A' enforces) - no separate gotstate output needed from the cache
  ENDIF.
ENDIF.
IF lv_prefetched = abap_false.
  CALL FUNCTION 'DDIF_DOMA_GET'   " unchanged, exactly as today
    ...
ENDIF.
IF ls_dd01v IS INITIAL OR lv_state <> 'A'.
  RETURN.
ENDIF.
" everything below this point (CLEAR as4user/as4date/..., ACTFLAG, AUTHCLASS, MASKLEN, DD07V APPVAL filter, SORT, io_xml->add) is 100% UNCHANGED
```

```abap
" in serialize_texts, replacing the LOOP AT lt_i18n_langs ... CALL FUNCTION 'DDIF_DOMA_GET' body:
DATA lv_prefetched TYPE abap_bool.
IF mo_i18n_params->ms_params-main_language_only = abap_true. RETURN. ENDIF.
lt_language_filter = mo_i18n_params->build_language_filter( ).
IF zcl_abapgit_ortec_git_switch=>is_serial_prefetch_active( ) = abap_true.
  lv_prefetched = zcl_abapgit_ortec_ser_pref_ext=>get_doma_i18n(
    EXPORTING iv_domname = lv_name
    IMPORTING et_i18n_langs = lt_i18n_langs et_dd01v_i18n = lt_dd01v_i18n et_dd07v_i18n = lt_dd07v_i18n ).
  IF lv_prefetched = abap_true.
    DELETE lt_i18n_langs WHERE table_line NOT IN lt_language_filter OR table_line = mv_language.
    " build lt_dd01_texts/lt_dd07_texts from lt_dd01v_i18n/lt_dd07v_i18n by
    " straight MOVE-CORRESPONDING per remaining language, identical field
    " set to today's post-DDIF_DOMA_GET-loop code - the VALPOS/VALUE-match
    " logic below is UNCHANGED, only its INPUT rows now come from the cache
    " instead of one DDIF_DOMA_GET call per language.
  ENDIF.
ENDIF.
IF lv_prefetched = abap_false.
  " unchanged: SELECT DISTINCT ... FROM dd01v/dd07v, LOOP ... CALL FUNCTION 'DDIF_DOMA_GET' per language
ENDIF.
" everything from "SORT lt_i18n_langs ASCENDING." onward (including the
" existing dd07v_tab VALUE-match against it_dd07v, DR-005 below) is
" 100% UNCHANGED regardless of which branch supplied the input rows.
```

**DR-005 resolution:** the existing standard `serialize_texts` code already
matches translation fixed-value texts to main-language rows BY VALUE
(`domvalue_l`/`domvalue_h`), never by VALPOS - this UNCHANGED matching
step is what runs regardless of whether its `it_dd07v`/per-language
`lt_dd07v` input came from the cache or from a live `DDIF_DOMA_GET` call,
so the provider does not need its own VALPOS-vs-VALUE decision at all;
`prepare_doma`'s internal `dd07v_tab`/`dd07v_tab_i18n` construction (which
does use VALPOS to line up a fixed value with its OWN text row within the
SAME language) is a different, lower-level join than the standard code's
cross-language VALUE match, and both must produce the SAME dd07v_tab-
shaped rows DDIF_DOMA_GET would have returned - verified by the byte-
identical parity test (§7), not asserted here.

## 2. Wire envelope (Phase 1 required properties)

One NEW DDIC structure/table pair for the combined DTEL+DOMA batch buffer,
replacing the ad hoc `EXPORT dtel = ... TO DATA BUFFER` shape used by the
existing single-object methods (which stays as-is for the single-object
path - only the NEW batch path uses this envelope):

```abap
TYPES: BEGIN OF zaog_ser_dd_batch_hdr,      " table: ZAOG_SER_DD_BHDR (structure)
         wire_format_version TYPE i,        " =1; unknown version -> reject whole batch
         provider_id         TYPE c LENGTH 8, " 'SER_DD01'
         batch_id            TYPE char32,    " correlation only, not used for lookup
         object_count        TYPE i,         " declared entry-row count; importer counts
                                              " the actual ENTRIES rows it received and
                                              " compares - mismatch (e.g. truncated
                                              " IMPORT) -> reject as corrupt. DR-006:
                                              " this is a cross-check against ENTRIES'
                                              " own line count, NOT an independently
                                              " recomputed byte length of a sub-region -
                                              " one combined EXPORT...TO DATA BUFFER has
                                              " no separately addressable sub-region to
                                              " recompute, so the earlier "payload_bytes"
                                              " field is dropped as unverifiable.
       END OF zaog_ser_dd_batch_hdr.

TYPES: BEGIN OF zaog_ser_dd_batch_entry,    " table: ZAOG_SER_DD_BENTRY_TT
         obj_type   TYPE trobjtype,          " 'DOMA' or 'DTEL' - canonical key part 1
         obj_name   TYPE sobj_name,          " canonical key part 2
         present    TYPE abap_bool,          " PRESENT/MISS state (§ Phase 1)
       END OF zaog_ser_dd_batch_entry.
```

Correlation is strictly by `(obj_type, obj_name)` - never by table order (the
export loop and the per-object `mt_doma`/`mt_dtel` READ TABLE lookups both
key on the same field). The actual DOMA/DTEL row payloads travel in the
SAME `EXPORT ... TO DATA BUFFER` call as `hdr`, `entries`, `doma` (a
`ty_doma_cache_tt`-shaped table, entries with no cache row omitted - this
IS the MISS representation) and `dtel` (existing `ty_dtel_cache_tt`,
unchanged). A present-but-genuinely-empty domain (e.g. no DD07L rows) is
still `present = abap_true` with an empty `dd07l`/`dd07t_main` - this is the
"valid empty payload" case, distinct from a MISS entry that is absent from
`doma`/`dtel` entirely.

## 3. Required behavior mapping

```text
unknown wire_format_version (<> 1)
  -> INJECT_FROM_BATCH_BUFFER raises zcx_abapgit_exception with a
     dedicated message; caller (Z_ABAPGIT_ORTEC_SER_BATCH) catches it,
     treats the WHOLE dispatch as a prefetch MISS (does NOT call
     inject_from_buffer at all for this buffer, does NOT abort the batch -
     every object in it still serializes via its own standard path) -
     matches BEFORE_DISPATCH's existing "singleton still exceeds limit ->
     dispatch anyway with no provider buffers" safe-fallback precedent.
corrupt/truncated envelope (IMPORT ... failure, e.g. CX_SY_IMPORT_MISMATCH_
  ERROR)
  -> same as unknown version: caught, treated as a full MISS for this
     buffer, no partial import, no partial successful result for any
     object (feature parity with "no partial-success path").
missing object entry (obj_type/obj_name not in ENTRIES at all)
  -> PROVIDER MISS for that object; standard serializer fallback (existing
     get_doma_data/get_dtel_data already return rv_found = abap_false in
     this case, since it is simply absent from mt_doma/mt_dtel too).
unexpected object entry (present in ENTRIES/doma/dtel but NOT in the
  dispatch's own IT_TADIR for this call)
  -> ignored deterministically: the worker only ever calls get_doma_data/
     get_dtel_data for objects in ITS OWN it_tadir loop, so an extra cached
     row is simply never read - never bound to another object because
     lookup is by exact (obj_type, obj_name) key, not position.
duplicate object entry (same obj_type/obj_name twice in ENTRIES)
  -> rejected deterministically: INJECT_FROM_BATCH_BUFFER checks
     `lines( entries ) <> lines( doma ) + lines( dtel_miss_count... )`-style
     structural counts are insufficient by themselves (duplicates in a
     HASHED TABLE keyed by domname/rollname already collapse silently on
     INSERT, which would hide the duplicate rather than reject it) - so the
     validator explicitly SORTs a working copy of ENTRIES by (obj_type,
     obj_name) and calls `READ TABLE ... BINARY SEARCH` / duplicate check
     BEFORE any INSERT INTO mt_doma/mt_dtel; a duplicate found this way
     rejects the WHOLE buffer (treated as corrupt, per corrupt handling
     above) rather than silently keeping the first or last occurrence -
     exact duplicate semantics are NOT assumed safe (per Phase-1
     requirement) because the two duplicate rows could legitimately carry
     DIFFERENT payload content from a buggy producer.
empty object payload (present=abap_true, dd07l etc. all empty)
  -> valid; treated as a real HIT with an empty fixed-value list - this is
     exactly the pre-existing MISS-vs-empty distinction problem provider_
     design.md already resolved for DTEL/MSAG and DOMA reuses unchanged.
```

## 4. Actual-byte admission

`BEFORE_DISPATCH`'s existing `lv_actual_bytes = 0` placeholder (see
`zcl_abapgit_ortec_ser_orch.clas.abap` class-level doc on BEFORE_DISPATCH,
"SER-SLICE-2 SCOPE BOUNDARY") is replaced by:

```abap
lv_actual_bytes = xstrlen(
  zcl_abapgit_ortec_ser_pref_ext=>extract_for_batch( it_object_keys ) ).
```

`extract_for_batch` (DR-003: computed EXACTLY ONCE per dispatch and reused,
never recomputed) is called from `BEFORE_DISPATCH` and its RESULT is
threaded through unchanged to `DISPATCH_BATCH`'s new parameter, not just
measured and discarded:

```abap
" BEFORE_DISPATCH (replaces the existing "lv_actual_bytes = 0." placeholder):
DATA(lv_prefetch_buffer_dd) = zcl_abapgit_ortec_ser_pref_ext=>extract_for_batch( it_object_keys ).
lv_actual_bytes = xstrlen( lv_prefetch_buffer_dd ).
" ... existing c_max_actual_batch_bytes split-and-recurse check, unchanged ...
" on the final, non-split path:
dispatch_batch( iv_run_id              = iv_run_id
                 it_object_keys         = it_object_keys
                 iv_attempt             = iv_attempt
                 iv_batch_id            = iv_batch_id
                 iv_prefetch_buffer_dd  = lv_prefetch_buffer_dd ).
```

If `before_dispatch` recurses (split into two halves), EACH half calls
`extract_for_batch` again with its own (smaller) `it_object_keys` - this is
unavoidable since the two halves need independently-sized buffers anyway
(the whole point of the split is that the combined buffer was too big), and
matches the existing recursion shape exactly (both halves are already
computed independently for `it_object_keys` itself, nothing new here).
`extract_for_batch` filters `it_object_keys` to `object = 'DOMA' OR object =
'DTEL'` internally; if the filtered set is empty it returns an INITIAL
xstring (0 bytes) with zero DB access - existing non-DD object types (MSAG/
CLAS/etc.) are unaffected and never touch this method. This is the ONLY
buffer counted in `lv_actual_bytes` for now, since `ser_pref`/`ser_pref_oo`
still only expose `extract_for_object` (unchanged scope boundary, still
disclosed as a limitation for those two families, not silently dropped).
The existing `c_max_actual_batch_bytes` (12582912) split-and-recurse logic
in `BEFORE_DISPATCH` is otherwise untouched and now has a genuine non-zero
input for DOMA/DTEL-containing batches; `c_max_pre_dispatch_splits` is a
NEW named constant capping `BEFORE_DISPATCH` recursion depth explicitly
(previously implicit/uncapped-by-name) - HARD SAFETY BOUND, value 12
(covers a batch of up to 4096 objects splitting to singletons; any run
needing more splits routes the remainder through
`route_to_sequential_fallback` instead of recursing further - "no
zero-progress split" is already structurally guaranteed since `lines(
it_object_keys ) > 1` is required to recurse and each half is strictly
smaller).

`dispatch_batch` gains one more OPTIONAL parameter, `iv_prefetch_buffer_dd
TYPE xstring OPTIONAL`, threaded through to `Z_ABAPGIT_ORTEC_SER_BATCH`'s
EXISTING `iv_prefetch_buffer_dd` import parameter (already reserved on the
RFC signature - no FM signature change needed).

## 5. Lifecycle (unchanged from provider_design.md §3/§7, reconfirmed)

`prepare()` (already calls `prepare_dtel`) gains a `prepare_doma` call in
the SAME method, same RUN-scoped timing. `clear()` (already clears
`mt_dtel`) gains `CLEAR mt_doma.`. Both fire in the SAME existing call
sites - no new CLEANUP hook. Batch-worker-side `inject_from_buffer`
(NEW batch-shaped overload, e.g. `inject_batch_from_buffer`) is called
ONCE per worker dispatch from `Z_ABAPGIT_ORTEC_SER_BATCH`, mirroring the
three existing `IF iv_prefetch_buffer_xxx IS NOT INITIAL. ...=>
inject_from_buffer( ... ). ENDIF.` blocks, and unconditionally does
`CLEAR mt_doma. CLEAR mt_dtel.` BEFORE inserting (same clear-before-insert
defensive pattern already proven, prevents cross-batch/cross-dispatch
leakage in a reused worker session even though CLASS-DATA is not actually
shared across separate work processes in production - SOURCE_CONFIRMED
per provider_design.md §4 session-isolation clarification).

## 6. Metrics

`PROVIDER_HIT`/`PROVIDER_MISS`/`PROVIDER_FALLBACK` counters added to
`ZAOG_SER_BATCH_RESULT` (existing structure - 3 new `TYPE i` fields) or
tracked via a new per-run table in ORCH, whichever the correctness review
below prefers; semantics exactly as specified by the owner brief (HIT = all
required provider data available and consumed; MISS = no applicable entry;
FALLBACK = entry existed but was unusable - the DOMA/DTEL provider's own
data is never "unusable" once present=abap_true and the version is known,
so FALLBACK is only reachable via the whole-buffer unknown-version/corrupt
paths in §3, counted once per object in that batch, not per row).

## 7. Tests required (see also user-supplied test matrix)

Reuses the EXISTING `ltcl_doma_parity` harness in
`zcl_abapgit_ortec_ser_pref_ext.clas.testclasses.abap` (calls real
`ZCL_ABAPGIT_OBJECT_DOMA~serialize()` unmodified, fixtures `XFELD`/
`CHAR30`/a nonexistent domain) as the FEATURE-OFF baseline; new parity
cases add feature-ON + provider-hit runs for the SAME fixtures and assert
byte-identical XML, per provider_design.md's explicit "PARITY PROOF...
required before authorized for implementation" condition.
