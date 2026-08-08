# SER-SLICE-4 Package A — TABL and TTYP batch provider design

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_4_PACKAGE_A_TABL_TTYP_DESIGN
BASELINE_HEAD=e5e10d62137033801739b8a53c69bdff9cfa650e
STATUS=DESIGN_DRAFT_CYCLE_3_AWAITING_REVIEW
CYCLE=3
CYCLE_1_REVIEW=.memory/reviews/serialization_slice_4_tabl_ttyp_adversarial.md
  (REVISE_AND_REVIEW_ONCE, 3 BLOCKER + 1 MAJOR: TT-001..TT-004)
CYCLE_2_REVIEW=same artifact, "Cycle 2" section (REVISE_AND_REVIEW_ONCE,
  TT-001/003/004 CLOSED, TT-002 reopened as a stale &sect;3 P/M rule +
  new TT-005 MAJOR i18n-checked-empty contradiction)
CYCLE_3_FIXES=TT-002/TT-005 (rewrote &sect;3's canonical P/M rule: P now
  means "prepare_tabl processed this table at all", derived from the
  SAME mt_tabl_extras checked-marker &sect;5's get_tabl_extras already
  used - M is defensive-only; aligned get_tabl_i18n's rv_found contract
  in &sect;5 to the SAME marker instead of "table has zero cached text
  rows", removing the last mv_prepared-flavored leftover wording)
PERFORMANCE_GATE_REVIEW=.memory/reviews/serialization_slice_4_performance.md
  (REVISE_AND_REVIEW_ONCE, PF-001 BLOCKER: mt_tabl_text's flat composite-
  keyed HASHED TABLE made get_tabl_i18n/extract_for_batch_tabl do an
  O(N_tabl^2)-shape partial-key LOOP AT...WHERE scan across a run;
  cycle-1/2/3 pseudocode also still referenced a nonexistent mv_prepared
  field, missed by 3 rounds of adversarial review because none of them
  were scoped to check this)
PERFORMANCE_FIXES=PF-001 (restructured mt_tabl_text to ONE nested row per
  tabname, mirroring mt_fugr_enlfdir's already-proven O(1) full-key-READ
  shape, sect 4/5/6 - eliminates the scan entirely); also fixed the
  leftover mv_prepared references to the real mv_language-initial signal
  (sect 6, matching Packages B/C's own PR-003/FG-family fix) and added
  the PR-005-style "reject initial language" guard to
  inject_batch_from_buffer_tabl (sect 6)
```

## 0. Evidence base (CONFIRMED_SOURCE unless marked otherwise)

### TABL

- `zif_abapgit_object~serialize` (CONFIRMED_SOURCE,
  `zcl_abapgit_object_tabl.clas.abap` ~line 863-1013): ONE
  `CALL FUNCTION 'DDIF_TABL_GET'` (main language) returns `gotstate`,
  `dd02v_wa` (header+text), `dd09l_wa` (technical settings), and TABLES
  `dd03p_tab` (fields, merged/derived - see &sect;1 risk analysis),
  `dd05m_tab` (foreign-key check-field pairs), `dd08v_tab` (foreign
  keys), `dd12v_tab`/`dd17v_tab` (indexes/index fields), `dd35v_tab`/
  `dd36m_tab` (search help assignments) - ALL in ONE FM call, i.e.
  **the current main-language cost is already O(1) FM calls per object,
  not O(sub-tables)** - this is structurally different from DOMA/DTEL/
  MSAG/PROG/FUGR, where the ORIGINAL problem was "N per-object FM/SELECT
  calls instead of 1 bulk read"; for TABL's main-language path there is
  no such N-calls-per-sub-table problem to eliminate in the first place.
  After the call: `CLEAR` on volatile fields (`as4user`/`as4date`/
  `as4time` on `dd02v`/`dd09l`/`dd12v`), numeric-field defensive clears,
  `ACTFLAG` release-dependent clear, `.INCLUDE`-inherited-FK/search-help
  removal (`DELETE ls_internal-dd08v WHERE noinherit = 'N'`, `DELETE
  ls_internal-dd35v WHERE shlpinher = abap_true`), then
  `clear_dd03p_fields` (removes nested-structure rows `depth &lt;&gt; '00'`
  and `.INCLUDE`-derived admin rows `adminfield &lt;&gt; '0'`, clears several
  language/data-element-sourced DD03P fields).
- `serialize_texts` (CONFIRMED_SOURCE, ~line 521-568): for every extra
  language (`SELECT DISTINCT ddlanguage FROM dd02v WHERE tabname = ...`),
  calls `CALL FUNCTION 'DDIF_TABL_GET'` **again, the FULL heavyweight FM,
  with EVERY TABLES parameter it accepts left unfilled** - only
  `dd02v_wa` is requested, and only `ls_dd02v-ddlanguage`/implicitly
  `ddtext` (via `MOVE-CORRESPONDING ls_dd02v TO &lt;ls_dd02_text&gt;`) are
  actually used. **This IS a genuine, avoidable, per-extra-language
  waste, structurally identical to the DOMA/DTEL/PROG/MSAG i18n
  patterns already solved in SER-SLICE-3** - the correct, low-risk
  target for this package.
- `read_extras` (CONFIRMED_SOURCE, ~line 451-456): `SELECT SINGLE * FROM
  tddat WHERE tabname = iv_tabname` (table authorization group) +
  `get_abap_language_version( )` (shared helper, dynamic `SELECT SINGLE
  abap_language_version FROM dd02l`, release-dependent). Called
  UNCONDITIONALLY once per object (not language-gated) at the end of
  `zif_abapgit_object~serialize`. A plain, single-key `SELECT SINGLE`
  against a flat table with no derived/merged fields - LOW RISK, directly
  bulk-readable via `FOR ALL ENTRIES`, same risk class as TRAN's existing
  `TSTCA`/TOBJ's existing `TDDAT` prefetch precedent already proven in
  `ZCL_ABAPGIT_ORTEC_SER_PREF_EXT`.
- `serialize_idoc_segment`/`is_idoc_segment` (CONFIRMED_SOURCE): IDoc
  segment detection + `SEGMENT_READ`/`SEGMENTDEFINITION_READ` kernel FM
  calls - rare feature (only applies to actual IDoc segment structures),
  no DB-bulk equivalent (kernel IDoc repository API, not a DDIC table
  read) - out of scope, unchanged.
- Longtexts (`zcl_abapgit_factory=>get_longtexts( )->serialize`) - no
  ORTEC hook anywhere (&sect;0 of every other package in this slice) -
  out of scope, unchanged, disclosed non-goal.
- **DD03P risk analysis (the central finding of this package,
  CONFIRMED_SOURCE + domain reasoning)**: `dd03p_tab` is not a plain
  1:1 mirror of a single master table. `clear_dd03p_fields`'s own logic
  proves this: it must explicitly `DELETE ... WHERE depth &lt;&gt; '00'` (drop
  nested-structure rows) and `DELETE ... WHERE adminfield &lt;&gt; '0'` (drop
  rows synthesized from `.INCLUDE`/`.APPEND`), i.e. `DDIF_TABL_GET`
  returns an ALREADY-FLATTENED view of every field a table logically has,
  including fields contributed by (possibly multi-level, possibly
  conditional `.INCLUDE ... IF ...`) includes and appends - this
  flattening is performed by SAP's own nametab/structure-generation
  machinery at call time, not stored pre-flattened in any single DDIC
  master table. Reproducing it via naive bulk `SELECT ... FOR ALL
  ENTRIES` reads would require reimplementing recursive include/append
  resolution (multi-level nesting, conditional includes, enhancement-
  category interactions) - this is meaningfully harder and higher-risk
  than DOMA's DD07V derivation (a bounded, non-recursive fixed-value/
  text join) or CLAS/INTF's description-table joins, and edges toward
  "replacement serializer" territory, which the mission's Common
  Discovery section explicitly gates behind "explicit adversarial
  approval" and generally discourages ("prefer feeding data to unchanged
  serializers via ORTEC-only lookup seams"). Per-field TECHNICAL
  attributes (INTTYPE/LENG/OUTPUTLEN/CONVEXIT/LOWERCASE/DECIMALS) for a
  ROLLNAME-based field ARE already available, pre-resolved and
  IT8-validated, in the EXISTING `mt_dtel` cache's `dd04v` structure
  (SER-SLICE-3), and for a DOMNAME-based field in the EXISTING `mt_doma`
  cache's `dd01v` - so the ATTRIBUTE-RESOLUTION half of the problem is
  NOT the blocker; the FLATTENING/STRUCTURE-EXPANSION half is. **This
  design does not attempt DD03P (or TTYP's structurally analogous DD43V
  key-field list) reconstruction this slice** - see &sect;1 for the exact
  scope boundary and &sect;12 for the explicit named follow-up.

### TTYP

- `zif_abapgit_object~serialize` (CONFIRMED_SOURCE,
  `zcl_abapgit_object_ttyp.clas.abap` ~line 181-241): ONE
  `CALL FUNCTION 'DDIF_TTYP_GET'` (main language, `state = 'A'`) returns
  `dd40v_wa` (header+text+row-kind/access-mode), TABLES `dd42v_tab`
  (line-type/reference-type resolution - DERIVED, analogous risk to
  DD03P: resolves the referenced TABL/STRU/DTEL/domain's own attributes)
  and `dd43v_tab` (key field list with technical attributes - DERIVED,
  same risk class as DD03P for a table-type's key fields). `ls_extra-
  abap_language_version` via the SAME shared `get_abap_language_version`
  helper TABL uses. `serialize_longtexts` at the end (shared, out of
  scope).
- **TTYP has NO per-language/i18n loop at all in current source**
  (CONFIRMED_SOURCE - no second `DDIF_TTYP_GET` call, no `SELECT DISTINCT
  ddlanguage`-style query anywhere in the file) - unlike TABL/DOMA/DTEL/
  PROG/MSAG, there is no existing "N per-extra-language waste" for a
  narrow provider to eliminate, because the standard abapGit TTYP
  serializer does not currently serialize per-language TTYP header
  text translations at all.
- **Consequence**: TTYP has neither (a) an i18n loop to optimize (unlike
  TABL) nor (b) a safe, non-derived bulk-readable sub-structure of
  comparable value to TABL's TDDAT (DD40V/DD42V/DD43V are ALL either the
  header itself or DERIVED/resolved structures) - there is no
  identified, evidence-backed, low-risk batching opportunity for TTYP
  this slice.

## 1. Decision

```text
TABL: IMPLEMENT_PARTIAL_PROVIDER
TTYP: DEFER
```

**TABL scope (IMPLEMENTED this slice):**

1. Per-extra-language TABL header text (`DD02T`-equivalent), eliminating
   `serialize_texts`'s current full-`DDIF_TABL_GET`-per-language waste -
   mirrors the DOMA/DTEL/PROG i18n pattern exactly.
2. Per-object `TDDAT` (table authorization group) prefetch, eliminating
   `read_extras`'s current per-object `SELECT SINGLE` - mirrors the
   already-proven TOBJ/TRAN low-risk metadata-prefetch pattern.

**TABL scope EXCLUDED this slice (not a silent gap, explicit
evidence-based boundary, &sect;0):**

- The main-language `DD02V`/`DD09L`/`DD03P`/`DD05M`/`DD08V`/`DD12V`/
  `DD17V`/`DD35V`/`DD36M` bundle - REMAINS on the standard, unchanged,
  per-object `DDIF_TABL_GET` call. Rationale: (a) this call is ALREADY
  O(1) per object (not O(sub-tables)), so there is no per-sub-table N+1
  problem analogous to DOMA/DTEL/MSAG/PROG/FUGR to fix; (b) the ONE
  sub-table that would be genuinely valuable to prefetch at scale
  (DD03P, since large tables can have many fields) carries the
  structure-flattening derivation risk in &sect;0 that this design
  declines to accept without a dedicated, separately-adversarially-
  reviewed follow-up (&sect;12); (c) batching the OUTER structures
  (DD09L/DD08V/DD12V/DD17V/DD35V/DD36M) alone, while LEAVING the SAME
  per-object `DDIF_TABL_GET` call in place to still fetch DD03P, would
  provide ZERO net FM-call reduction (the expensive call still happens
  once per object either way) for ADDED complexity and risk - a
  clearly poor risk/benefit trade, matching the mission's explicit
  allowance for a negative recommendation when evidence shows this.
- `read_extras`'s `get_abap_language_version` (dynamic SQL against
  `DD02L`, release-dependent field existence) - left unbatched; it is a
  single-field dynamic SELECT with its own `CATCH
  cx_sy_dynamic_osql_semantics` release guard already, LOW absolute
  cost, and bundling it into &sect;3's TDDAT batch would mix a
  release-conditional dynamic-SQL path into an otherwise simple static
  bulk read for marginal benefit - EXCLUDED as not worth the added
  complexity (SOURCE_DERIVED judgment call, not a hard technical
  blocker like DD03P).
- IDoc segment handling, longtexts - out of scope (&sect;0).

**TTYP scope: no provider implemented this slice.** TTYP participates in
the generic (no-op) batch dispatch path exactly like every other
currently-unimplemented family - always a clean, correct PROVIDER_MISS,
falling back to the unchanged standard `DDIF_TTYP_GET` call, which is
already O(1) per object. Revisit only if (a) standard abapGit later adds
a genuine per-language TTYP loop analogous to TABL's, giving this
provider something to attach to, or (b) a real SAT/ST05 trace on a
TTYP-heavy repository shows meaningful per-object `DDIF_TTYP_GET`
overhead that a bulk multi-object read could plausibly reduce WITHOUT
touching DD42V/DD43V's derived content (e.g. batching only DD40V header/
text, DD42V/DD43V still per-object) - not designed here, no evidence
exists yet (UNKNOWN, not HYPOTHESIS - no reasoning basis has been
constructed for this alternative).

**Combined-vs-separate envelope question (mission &sect;"TABL/TTYP")**:
moot for this slice - since TTYP has no implemented provider, there is
nothing to combine or separate. If a future TTYP provider is authorized,
&sect;12 records the recommended approach (a SEPARATE payload table
within the SAME reused generic envelope class, following this package's
own TABL precedent, not a merged DD03P/DD43V-style structure given their
different derivation risk profiles).

## 2. Provider home

Extends `ZCL_ABAPGIT_ORTEC_SER_PREF_EXT` (already houses DOMA/DTEL,
the other classic-DDIC family, and per Package B/C also PROG/FUGR) -
consistent with `serialization_slice_3_provider_contract.md` &sect;1's
"extend existing prefetch classes, no new provider class per family"
convention.

## 3. Wire envelope

Reuses `ZAOG_SER_ENV_BHDR`/`ZAOG_SER_ENV_BENTRY`/`ZAOG_SER_ENV_BENTRY_TT`
(same generic envelope as Packages B/C). Two NEW DDIC row/table-type
pairs (package `$ZAOG_SER`):

```abap
TYPES: BEGIN OF zaog_ser_tabl_text_brow,    " ZAOG_SER_TABL_TX_BROW
         tabname    TYPE dd02l-tabname,
         ddlanguage TYPE dd02t-ddlanguage,
         ddtext     TYPE dd02t-ddtext,
       END OF zaog_ser_tabl_text_brow.
TYPES zaog_ser_tabl_text_brow_tt TYPE STANDARD TABLE OF
  zaog_ser_tabl_text_brow WITH DEFAULT KEY. " ZAOG_SER_TABL_TX_BROW_TT

TYPES: BEGIN OF zaog_ser_tabl_extras_brow, " ZAOG_SER_TABL_EX_BROW
         tabname TYPE tddat-tabname,
         tddat   TYPE tddat,
       END OF zaog_ser_tabl_extras_brow.
TYPES zaog_ser_tabl_extras_brow_tt TYPE STANDARD TABLE OF
  zaog_ser_tabl_extras_brow WITH DEFAULT KEY. " ZAOG_SER_TABL_EX_BROW_TT
```

`ZAOG_SER_ENV_BHDR-PROVIDER_ID = 'SER_TABL'`. One `ENTRIES` row per TABL
object with `obj_type = 'TABL'`. **TT-005 FIX (cycle 3 - canonical P/M
rule, supersedes cycle 1's text/TDDAT-content-based wording)**: `state =
'P'` iff `prepare_tabl` processed this table at all - i.e. iff an
`mt_tabl_extras` row exists for it (per &sect;4's unconditional
pre-insert, this is true for EVERY TABL object in the run once `PREPARE`
has executed, regardless of whether its text list or TDDAT content is
actually non-empty). `state = 'M'` is reachable ONLY defensively (a TABL
object in `it_object_keys` that `PREPARE` never saw at all, structurally
unreachable in correct operation since `collect_keys`/`PREPARE` always
cover the FULL run's `it_tadir` - &sect;6). A table with genuinely zero
extra-language translations AND zero `TDDAT` row is therefore a REAL,
valid `P` (checked-and-empty on both counts), NOT an `M` - `M` no longer
means "nothing to prefetch", it means "never checked" (defensive-only).
This single rule is now the ONLY source of truth for TABL's HIT/MISS
semantics; &sect;5/&sect;7/&sect;11 are aligned to it below.

## 4. `prepare_tabl` (NEW private method on `ZCL_ABAPGIT_ORTEC_SER_PREF_EXT`,
called from the existing `PREPARE` entry point, decision-free bulk-read
mirror of what `serialize_texts`/`read_extras` do today)

```abap
METHOD prepare_tabl.
  DATA lt_dd02t TYPE STANDARD TABLE OF dd02t.
  DATA lt_tddat TYPE STANDARD TABLE OF tddat.

  IF it_names IS INITIAL. RETURN. ENDIF.

  " Text: every language THIS table has a DD02T row for, in the language
  " filter's scope is resolved by the CALLER (zcl_abapgit_object_tabl's
  " own mo_i18n_params->build_language_filter( )) at CONSUME time, not
  " here - PREPARE always caches ALL languages present in DD02T for a
  " table in this batch, exactly mirroring how mt_dtel/mt_doma cache ALL
  " languages and let the per-object consumer filter afterward (DD01V_
  " I18N/DTEL_I18N precedent, provider_contract.md &sect;1).
  SELECT * FROM dd02t INTO TABLE lt_dd02t
    FOR ALL ENTRIES IN it_names
    WHERE tabname = it_names-table_line
      AND ddlanguage <> iv_main_language.       " main language already
                                                  " serialized directly by
                                                  " the object's own first
                                                  " DDIF_TABL_GET call -
                                                  " never needed from
                                                  " this cache
  LOOP AT lt_dd02t INTO DATA(ls_dd02t).
    IF ls_dd02t-ddlanguage IS INITIAL.
      CONTINUE.                                  " TT-001 FIX: mirrors
                                                   " serialize_texts's
                                                   " EXACT real skip
                                                   " condition (`sy-subrc
                                                   " <> 0 OR ls_dd02v-
                                                   " ddlanguage IS
                                                   " INITIAL`) - NOT a
                                                   " DDTEXT-initial check.
                                                   " An empty-but-PRESENT
                                                   " DDTEXT on a real
                                                   " DD02T row is a VALID
                                                   " text row and MUST be
                                                   " kept (the original
                                                   " cycle-1 pseudocode
                                                   " incorrectly dropped
                                                   " it - adversarial
                                                   " finding TT-001).
    ENDIF.
    " PF-001 FIX (performance design gate, cycle 4): nest ALL of one
    " table's text rows under a SINGLE hashed-by-tabname cache row,
    " mirroring mt_fugr_enlfdir's already-proven shape (one row per
    " AREA nesting every ENLFDIR row) instead of a flat composite-keyed
    " HASHED TABLE keyed on (tabname, ddlanguage) - a LOOP AT ... WHERE
    " tabname = ... against a HASHED table's full composite key is a
    " PARTIAL-KEY condition, which ABAP cannot serve via the hash index
    " and instead executes as a full LINEAR SCAN of the ENTIRE cache -
    " since PREPARE populates this cache ONCE for the whole run (every
    " TABL object, not just one batch), a per-object GET_TABL_I18N call
    " scanning the FULL cache on every call is an O(N_tabl^2) shape
    " across a run of N_tabl objects, not O(N_tabl) - a real, silent
    " CPU/response-time regression invisible to any SQL trace. The fix
    " below performs one O(1) hashed-full-key READ/INSERT per DD02T row
    " DURING prepare (a single linear pass over this PREPARE call's own
    " bulk-SELECT result, NOT the accumulating global cache), then every
    " later consumer does one O(1) full-key READ - no scan anywhere.
    READ TABLE mt_tabl_text ASSIGNING FIELD-SYMBOL(<ls_text_cache>)
      WITH TABLE KEY tabname = ls_dd02t-tabname.
    IF sy-subrc <> 0.
      INSERT VALUE ty_tabl_text_cache( tabname = ls_dd02t-tabname )
        INTO TABLE mt_tabl_text ASSIGNING <ls_text_cache>.
    ENDIF.
    APPEND VALUE #( ddlanguage = ls_dd02t-ddlanguage
                     ddtext     = ls_dd02t-ddtext )
      TO <ls_text_cache>-texts.
  ENDLOOP.

  " TDDAT: unconditional per-object read today, trivially bulk-able.
  " TT-002 FIX: pre-insert ONE extras row per requested name BEFORE
  " overlaying TDDAT content - mirrors the EXISTING TOBJ precedent
  " (prepare_tobj/get_tobj_data, same class) of representing "checked,
  " nothing found" as a real cached fact distinct from "never checked
  " at all". Without this pre-insert, a table with no TDDAT row would
  " be indistinguishable in mt_tabl_extras from a table PREPARE never
  " processed, and GET_TABL_EXTRAS (&sect;5) could not correctly answer
  " "definitely has no authorization group" versus "not prepared".
  LOOP AT it_names INTO DATA(lv_tabname_extras).
    INSERT VALUE ty_tabl_extras_cache( tabname = lv_tabname_extras )
      INTO TABLE mt_tabl_extras.
  ENDLOOP.

  SELECT * FROM tddat INTO TABLE lt_tddat
    FOR ALL ENTRIES IN it_names
    WHERE tabname = it_names-table_line.
  LOOP AT lt_tddat INTO DATA(ls_tddat).
    READ TABLE mt_tabl_extras ASSIGNING FIELD-SYMBOL(<ls_extras_upd>)
      WITH TABLE KEY tabname = ls_tddat-tabname.
    IF sy-subrc = 0.
      <ls_extras_upd>-tddat = ls_tddat.
    ENDIF.
  ENDLOOP.
ENDMETHOD.
```

New PRIVATE types/CLASS-DATA:

```abap
TYPES: BEGIN OF ty_tabl_text_lang,             " PF-001 fix: nested row
         ddlanguage TYPE dd02t-ddlanguage,
         ddtext     TYPE dd02t-ddtext,
       END OF ty_tabl_text_lang.
TYPES ty_tabl_text_lang_tt TYPE STANDARD TABLE OF ty_tabl_text_lang
  WITH DEFAULT KEY.

TYPES: BEGIN OF ty_tabl_text_cache,            " PF-001 fix: ONE row per
         tabname TYPE dd02l-tabname,           " tabname (was: one row
         texts   TYPE ty_tabl_text_lang_tt,    " per tabname+language)
       END OF ty_tabl_text_cache.
TYPES ty_tabl_text_cache_tt TYPE HASHED TABLE OF ty_tabl_text_cache
  WITH UNIQUE KEY tabname.                     " full-key O(1) lookup,
                                                " mirrors mt_fugr_enlfdir

TYPES: BEGIN OF ty_tabl_extras_cache,
         tabname TYPE tddat-tabname,
         tddat   TYPE tddat,
       END OF ty_tabl_extras_cache.
TYPES ty_tabl_extras_cache_tt TYPE HASHED TABLE OF ty_tabl_extras_cache
  WITH UNIQUE KEY tabname.

CLASS-DATA mt_tabl_text   TYPE ty_tabl_text_cache_tt.
CLASS-DATA mt_tabl_extras TYPE ty_tabl_extras_cache_tt.
```

`collect_keys` gains a `WHEN 'TABL'. INSERT CONV dd02l-tabname(
ls_tadir-obj_name ) INTO TABLE et_tabl.` branch (new `et_tabl TYPE
ty_tabl_keys` output, `HASHED TABLE OF dd02l-tabname WITH UNIQUE KEY
table_line`), and `PREPARE`'s body gains `prepare_tabl( it_names =
lt_tabl iv_main_language = iv_language ).` alongside the existing
`prepare_dtel`/`prepare_doma`/... calls. `CLEAR` (public) gains `CLEAR
mt_tabl_text. CLEAR mt_tabl_extras.`.

## 5. Single-object accessors (NEW seam in `zcl_abapgit_object_tabl.clas.abap`
- required BEFORE the batch envelope can help anything, mirrors the
DOMA seam pattern exactly, `serialization_slice_3_provider_contract.md`
&sect;1 "DOMA SEAM")

```abap
CLASS-METHODS get_tabl_i18n
  IMPORTING iv_tabname       TYPE dd02l-tabname
            iv_language      TYPE spras
  EXPORTING et_i18n_langs    TYPE zcl_abapgit_ortec_ser_pref=>ty_langu_tt
            et_dd02_texts    TYPE STANDARD TABLE OF dd02v
  RETURNING VALUE(rv_found)  TYPE abap_bool.

CLASS-METHODS get_tabl_extras
  IMPORTING iv_tabname       TYPE tddat-tabname
  EXPORTING es_tddat         TYPE tddat
  RETURNING VALUE(rv_found)  TYPE abap_bool.
```

`get_tabl_i18n` reads `mt_tabl_text` via ONE full-key `READ TABLE ...
WITH TABLE KEY tabname = iv_tabname` (O(1), PF-001 fix - see &sect;4's
nested-cache-shape rationale), builds `et_i18n_langs` (distinct sorted
languages) and `et_dd02_texts` (one `dd02v`-shaped row per language, from
the found row's nested `texts` table, `ddlanguage`/`ddtext` populated,
every other field left initial - `serialize_texts` only ever reads
`ddlanguage`/implicitly-via-`MOVE-CORRESPONDING` `ddtext` from its
existing `ls_dd02v` local variable, so an initial-elsewhere `dd02v` row
is a byte-for-byte-equivalent substitute for what a real `DDIF_TABL_GET`
call would have produced for THIS consumer, exactly as the DOMA seam's
own `get_doma_data` contract already established: "no reshaping needed,
just field-for-field substitution of what the consumer already reads").
`rv_found = abap_true` IFF an `mt_tabl_extras` row exists for
`iv_tabname` (TT-005 FIX: the SAME checked-marker &sect;3/&sect;5's
`get_tabl_extras` uses, NOT "the table has zero cached text rows" as
cycle 1/2 stated - that wording made a checked-but-genuinely-empty
translation list indistinguishable from "never prepared", contradicting
&sect;3's own canonical P rule). `et_i18n_langs`/`et_dd02_texts` are
simply whatever `mt_tabl_text` rows exist for this table - empty is a
perfectly valid result once `rv_found = abap_true` (mirrors DOMA/PROG's
"present but empty" pattern exactly, now correctly gated by the SAME
prepared-marker as `get_tabl_extras` rather than by the text cache's own
content).

**`get_tabl_extras` contract (TT-002 FIX - revised from cycle 1):**
reads `mt_tabl_extras` BY TABLE KEY `tabname` (`READ TABLE ... WITH
TABLE KEY tabname = iv_tabname`, O(1)). `rv_found = abap_true` IFF a row
exists in `mt_tabl_extras` for `iv_tabname` - which, per &sect;4's
pre-insert fix, is true for EVERY table name `prepare_tabl` ever
processed (one row is UNCONDITIONALLY inserted per name before `TDDAT`
content is overlaid), REGARDLESS of whether a real `TDDAT` row was found
for it. `es_tddat` is either the real cached `TDDAT` row or remains
INITIAL (a table with no authorization group assigned). This correctly
and unambiguously reproduces the standard code's own contract (`SELECT
SINGLE * FROM tddat` on a genuinely absent row silently leaves the
target initial, `sy-subrc` never checked by the caller, always a valid
empty outcome) WITHOUT conflating two DIFFERENT facts that cycle 1's
design confused:

```text
"no TDDAT row for this table" (a real, cached, CHECKED fact) ->
  rv_found = abap_true, es_tddat INITIAL
"this table was never part of the current run's PREPARE at all" (e.g.
  is_serial_prefetch_active is OFF, or PREPARE never ran, or a corrupt
  buffer was rejected) -> rv_found = abap_false
```

Practical consequence: once `PREPARE`/`prepare_tabl` has run for a given
run, EVERY TABL object in that run's `it_tadir` has an `mt_tabl_extras`
row (found = true) - `rv_found = abap_false` is now reachable ONLY via
"not prepared at all", never via "prepared, but this specific table had
no TDDAT row" (&sect;6/&sect;7 build on this simplification).

Seam in `zif_abapgit_object~serialize`'s `read_extras` call site:

```abap
" replacing "ls_internal-extras = read_extras( lv_name )." :
DATA lv_extras_prefetched TYPE abap_bool.
IF zcl_abapgit_ortec_git_switch=>is_serial_prefetch_active( ) = abap_true.
  lv_extras_prefetched = zcl_abapgit_ortec_ser_pref_ext=>get_tabl_extras(
    EXPORTING iv_tabname = lv_name
    IMPORTING es_tddat   = ls_internal-extras-tddat ).
  IF lv_extras_prefetched = abap_true.
    ls_internal-extras-abap_language_version = get_abap_language_version( ).
                                              " UNCHANGED - &sect;1 explicitly
                                              " excludes this field from
                                              " batching
  ENDIF.
ENDIF.
IF lv_extras_prefetched = abap_false.
  ls_internal-extras = read_extras( lv_name ).   " unchanged fallback
ENDIF.
```

Seam in `serialize_texts` (mirrors DOMA's own `serialize_texts` seam
structure exactly - the OUTER language-discovery query is the ONLY part
replaced, the per-language `LOOP` body's field assembly is otherwise
unchanged):

```abap
" replacing the "SELECT DISTINCT ddlanguage ... FROM dd02v" query and the
" subsequent per-language "CALL FUNCTION 'DDIF_TABL_GET'" loop body:
DATA lv_i18n_prefetched TYPE abap_bool.
IF zcl_abapgit_ortec_git_switch=>is_serial_prefetch_active( ) = abap_true.
  lv_i18n_prefetched = zcl_abapgit_ortec_ser_pref_ext=>get_tabl_i18n(
    EXPORTING iv_tabname = lv_name iv_language = mv_language
    IMPORTING et_i18n_langs = cs_internal-i18n_langs
              et_dd02_texts = cs_internal-dd02_texts ).
  IF lv_i18n_prefetched = abap_true.
    DELETE cs_internal-i18n_langs WHERE table_line NOT IN lt_language_filter
                                      OR table_line = mv_language.
    DELETE cs_internal-dd02_texts WHERE ddlanguage NOT IN lt_language_filter
                                      OR ddlanguage = mv_language.
  ENDIF.
ENDIF.
IF lv_i18n_prefetched = abap_false.
  " unchanged: SELECT DISTINCT ddlanguage ..., LOOP ... CALL FUNCTION
  " 'DDIF_TABL_GET' per language
ENDIF.
SORT cs_internal-i18n_langs ASCENDING.
SORT cs_internal-dd02_texts BY ddlanguage ASCENDING.  " UNCHANGED, runs
                                                        " regardless of
                                                        " which branch
                                                        " supplied rows
```

## 6. `extract_for_batch_tabl` / `inject_batch_from_buffer_tabl`
(decision-free pseudocode - same validation sequence as Packages B/C
&sect;4: unknown version / object_count mismatch / duplicate entry /
corrupt IMPORT reject the WHOLE buffer. **TT-003 NAMING FIX**: cycle 1's
`extract_for_batch_tabl_ttyp`/`inject_batch_from_buffer_tabl_ttyp` names
were adopted for future-proofing, but
`inject_batch_from_buffer_tabl_ttyp` is 34 characters, exceeding ABAP's
30-character global method-name limit (an implementation following that
name literally cannot compile). Renamed to the shorter, still-unique
`_tabl` suffix (`extract_for_batch_tabl` = 22 chars,
`inject_batch_from_buffer_tabl` = 29 chars, `clear_tabl_cache` = 16
chars, all &lt;= 30) - TTYP has no payload this slice (&sect;1 DEFER), so
reserving a longer `_tabl_ttyp` suffix for a hypothetical future TTYP
payload bought nothing except the compile failure; a future TTYP
provider can define its OWN suffixed methods when/if authorized,
exactly like every other family in this slice already does.)

**TT-002 consequence for extract**: since `prepare_tabl` (&sect;4, fixed)
now UNCONDITIONALLY inserts one `mt_tabl_extras` row per name it
processes, EVERY TABL object that was part of this run's `PREPARE` call
always has an extras row - `lv_found` degenerates to "was this object
part of `PREPARE` at all", not "did either cache have interesting
content". The pseudocode below reflects this (no separate `lv_found`
flag needed - state is `P` whenever the extras READ succeeds, which is
always true post-PREPARE for objects in `it_tadir`; a defensive `M`
fallback remains for the structurally-impossible-in-correct-operation
case where `it_object_keys` contains a TABL object PREPARE never saw).

```abap
METHOD extract_for_batch_tabl.
  DATA lt_entries TYPE zaog_ser_env_bentry_tt.
  DATA lt_text    TYPE zaog_ser_tabl_text_brow_tt.
  DATA lt_extras  TYPE zaog_ser_tabl_extras_brow_tt.
  DATA ls_hdr     TYPE zaog_ser_env_bhdr.
  DATA lv_any_hit TYPE abap_bool.

  IF mv_language IS INITIAL.                 " fixed: no mv_prepared
                                              " field exists on this
                                              " class (same mv_language-
                                              " initial signal Packages
                                              " B/C use, see their own
                                              " &sect;3 fixes) - this
                                              " was flagged as an
                                              " internal consistency
                                              " note by the performance
                                              " design gate and is fixed
                                              " here alongside PF-001
    RETURN.
  ENDIF.

  LOOP AT it_object_keys INTO DATA(ls_tadir) WHERE object = 'TABL'.
    DATA(lv_tabname) = CONV dd02l-tabname( ls_tadir-obj_name ).

    " PF-001 fix: ONE full-key READ (O(1) hashed lookup, mirrors
    " mt_fugr_enlfdir's own extract_for_batch_fugr pattern exactly) then
    " a plain LOOP over the found row's OWN nested texts table - no
    " WHERE-scan of the global cache anywhere in this method.
    READ TABLE mt_tabl_text INTO DATA(ls_text_cache)
      WITH TABLE KEY tabname = lv_tabname.
    IF sy-subrc = 0.
      LOOP AT ls_text_cache-texts INTO DATA(ls_lang).
        APPEND VALUE #( tabname    = lv_tabname
                         ddlanguage = ls_lang-ddlanguage
                         ddtext     = ls_lang-ddtext ) TO lt_text.
      ENDLOOP.
    ENDIF.

    DATA(ls_entry) = VALUE zaog_ser_env_bentry(
      obj_type = ls_tadir-object obj_name = ls_tadir-obj_name ).

    READ TABLE mt_tabl_extras INTO DATA(ls_extras)
      WITH TABLE KEY tabname = lv_tabname.
    IF sy-subrc = 0.                              " always true here in
                                                    " correct operation,
                                                    " TT-002 fix - see
                                                    " note above
      APPEND VALUE #( tabname = ls_extras-tabname tddat = ls_extras-tddat )
        TO lt_extras.
      ls_entry-state        = 'P'.
      ls_entry-actual_bytes = xstrlen( extract_for_object( ls_tadir ) ).
      lv_any_hit = abap_true.
    ELSE.
      ls_entry-state        = 'M'.                 " defensive only -
                                                     " should be
                                                     " unreachable if
                                                     " PREPARE always
                                                     " covers this run's
                                                     " full it_tadir
      ls_entry-actual_bytes = 0.
    ENDIF.
    APPEND ls_entry TO lt_entries.
  ENDLOOP.

  IF lt_entries IS INITIAL OR lv_any_hit = abap_false.
    CLEAR rv_buffer.
    RETURN.
  ENDIF.

  ls_hdr-wire_format_version = 1.
  ls_hdr-provider_id         = 'SER_TABL'.
  ls_hdr-object_count        = lines( lt_entries ).

  EXPORT hdr = ls_hdr entries = lt_entries
         tabl_text = lt_text tabl_extras = lt_extras
         language = mv_language
    TO DATA BUFFER rv_buffer COMPRESSION ON.
ENDMETHOD.
```

**TT-004 FIX - `inject_batch_from_buffer_tabl` payload-key validation**
(extends the Package B &sect;4 baseline sequence - IMPORT, version check,
`entries` duplicate/`object_count` check - with TABL-specific checks the
cycle-1 design omitted):

```abap
METHOD inject_batch_from_buffer_tabl.
  " ... IMPORT hdr/entries/tabl_text/tabl_extras/language, exactly as
  " Package B &sect;4's baseline sequence (including its own object_
  " count check and, per that same fix, an "IF lv_language IS INITIAL.
  " zcx_abapgit_exception=>raise(...). ENDIF." guard immediately after -
  " PR-005's fix applies identically here: extract_for_batch_tabl can
  " only ever export a non-initial mv_language per its own &sect;6
  " guard, so an initial language is itself proof of corruption) ...

  IF ls_hdr-provider_id <> 'SER_TABL'.
    zcx_abapgit_exception=>raise(
      'ORTEC TABL batch prefetch buffer: unexpected provider_id' ).
  ENDIF.

  " every ENTRIES row must be TABL with a known state - reject anything
  " else as corrupt (TT-004: an unexpected object type or state is NOT
  " a normal MISS, it is evidence the buffer does not belong to this
  " provider or was corrupted in transit).
  LOOP AT lt_entries INTO DATA(ls_check_entry).
    IF ls_check_entry-obj_type <> 'TABL' OR
       ( ls_check_entry-state <> 'P' AND ls_check_entry-state <> 'M' ).
      zcx_abapgit_exception=>raise(
        'ORTEC TABL batch prefetch buffer: unexpected entry type or state' ).
    ENDIF.
  ENDLOOP.

  " duplicate-payload rejection (TT-004): text keyed by (tabname,
  " ddlanguage), extras keyed by tabname - a HASHED-table INSERT would
  " otherwise silently collapse a duplicate instead of rejecting the
  " whole buffer, exactly the same risk class the DD/OO_BATCH ENTRIES
  " duplicate check already guards against for a DIFFERENT table.
  DATA(lt_text_sorted) = lt_text.
  SORT lt_text_sorted BY tabname ddlanguage.
  DATA(lv_text_lines_before) = lines( lt_text_sorted ).
  DELETE ADJACENT DUPLICATES FROM lt_text_sorted COMPARING tabname ddlanguage.
  IF lines( lt_text_sorted ) <> lv_text_lines_before.
    zcx_abapgit_exception=>raise(
      'ORTEC TABL batch prefetch buffer: duplicate text payload row' ).
  ENDIF.

  DATA(lt_extras_sorted) = lt_extras.
  SORT lt_extras_sorted BY tabname.
  DATA(lv_extras_lines_before) = lines( lt_extras_sorted ).
  DELETE ADJACENT DUPLICATES FROM lt_extras_sorted COMPARING tabname.
  IF lines( lt_extras_sorted ) <> lv_extras_lines_before.
    zcx_abapgit_exception=>raise(
      'ORTEC TABL batch prefetch buffer: duplicate extras payload row' ).
  ENDIF.

  " canonical object-key correlation (TT-004): per &sect;6's extract
  " invariant, EVERY 'P' entry has EXACTLY ONE extras payload row (the
  " unconditional pre-insert in &sect;4 guarantees this on the producer
  " side) - enforce the SAME invariant on the consumer side, and reject
  " any extras/text row whose tabname is not a 'P' entry (an unexpected/
  " unauthorized payload key).
  DATA(lt_p_entries) = lt_entries.
  DELETE lt_p_entries WHERE state <> 'P'.
  IF lines( lt_extras_sorted ) <> lines( lt_p_entries ).
    zcx_abapgit_exception=>raise(
      'ORTEC TABL batch prefetch buffer: extras payload does not match P entries 1:1' ).
  ENDIF.
  LOOP AT lt_extras_sorted INTO DATA(ls_extras_check).
    READ TABLE lt_p_entries TRANSPORTING NO FIELDS
      WITH KEY obj_name = ls_extras_check-tabname.
    IF sy-subrc <> 0.
      zcx_abapgit_exception=>raise(
        'ORTEC TABL batch prefetch buffer: extras payload key not in P entries' ).
    ENDIF.
  ENDLOOP.
  LOOP AT lt_text_sorted INTO DATA(ls_text_check).
    READ TABLE lt_p_entries TRANSPORTING NO FIELDS
      WITH KEY obj_name = ls_text_check-tabname.
    IF sy-subrc <> 0.
      zcx_abapgit_exception=>raise(
        'ORTEC TABL batch prefetch buffer: text payload key not in P entries' ).
    ENDIF.
  ENDLOOP.

  CLEAR mt_tabl_text.
  CLEAR mt_tabl_extras.
  LOOP AT lt_text INTO DATA(ls_text_ins).
    " PF-001 fix: insert into the nested-by-tabname cache shape (&sect;4)
    " - one O(1) full-key READ/INSERT per wire row, never a WHERE-scan.
    READ TABLE mt_tabl_text ASSIGNING FIELD-SYMBOL(<ls_text_ins_cache>)
      WITH TABLE KEY tabname = ls_text_ins-tabname.
    IF sy-subrc <> 0.
      INSERT VALUE ty_tabl_text_cache( tabname = ls_text_ins-tabname )
        INTO TABLE mt_tabl_text ASSIGNING <ls_text_ins_cache>.
    ENDIF.
    APPEND VALUE #( ddlanguage = ls_text_ins-ddlanguage
                     ddtext     = ls_text_ins-ddtext )
      TO <ls_text_ins_cache>-texts.
  ENDLOOP.
  LOOP AT lt_extras INTO DATA(ls_extras_ins).
    INSERT VALUE ty_tabl_extras_cache( tabname = ls_extras_ins-tabname
                                        tddat   = ls_extras_ins-tddat )
      INTO TABLE mt_tabl_extras.
  ENDLOOP.
  mv_language = lv_language.                 " fixed: no mv_prepared
                                              " field (same fix as
                                              " Package B/C); unconditional
                                              " per PR-005's precedent -
                                              " lv_language is validated
                                              " non-initial earlier in
                                              " this method (add the SAME
                                              " "IF lv_language IS
                                              " INITIAL. raise ...
                                              " ENDIF." guard as PROG
                                              " &sect;4/PR-005, placed
                                              " immediately after the
                                              " object_count check, before
                                              " any cache mutation)
ENDMETHOD.

METHOD clear_tabl_cache.
  CLEAR mt_tabl_text.
  CLEAR mt_tabl_extras.
ENDMETHOD.
```

## 7. Required behavior mapping

Identical shape to Packages B/C &sect;5 (unknown version/corrupt/
duplicate -&gt; reject whole buffer; missing entry -&gt; MISS; unexpected entry
-&gt; ignored by exact-key lookup), PLUS &sect;6's TT-004 additions
(unexpected `provider_id`/entry type/state, duplicate payload rows, and
payload-to-entry correlation all independently reject the WHOLE buffer).
One TABL-specific clarification (TT-002 fix): a table present in
`ENTRIES` as `P` but absent from `tabl_text` (has a checked, empty
authorization-group extras row but zero extra-language translations) is
a valid HIT for `get_tabl_extras` (`rv_found = abap_true`, real or
initial `es_tddat`) and a valid "checked, no translations" case for
`get_tabl_i18n` (`rv_found = abap_true`, `et_i18n_langs` empty) - NEITHER
is a MISS, since &sect;4's unconditional extras pre-insert means every
`P` entry always has real, checked data for BOTH sub-features, whether or
not either happens to be non-empty for that specific table.

## 8. ORCH wiring

New `iv_prefetch_buffer_tabl` parameter, threaded through exactly like
Packages B/C's own new parameters (method names use the `_tabl` suffix
per &sect;6's TT-003 fix; the RFC interface parameter name was already
`_tabl`, unaffected by that rename). Same
`before_dispatch`/`dispatch_batch`/RFC-worker wiring pattern as Packages
B &sect;6 / C &sect;9 - summed into the SAME combined `lv_actual_bytes`
total (Shared Infrastructure &sect;3). Worker `CASE ls_tadir-object`
telemetry gains:

```abap
WHEN 'TABL'.
  IF zcl_abapgit_ortec_ser_pref_ext=>get_tabl_i18n(
       iv_tabname = CONV #( ls_tadir-obj_name ) iv_language = iv_language )
       = abap_true
     OR zcl_abapgit_ortec_ser_pref_ext=>get_tabl_extras(
       iv_tabname = CONV #( ls_tadir-obj_name ) ) = abap_true.
    ls_result-provider_hit = 1.
  ELSE.
    ls_result-provider_miss = 1.
  ENDIF.
```

## 9. Naming note

See Package B &sect;9 - `ZCL_ABAPGIT_ORTEC_SER_PREF_EXT` will host FOUR
distinct `extract_for_batch*`/`inject_batch_from_buffer*` pairs once
Packages A/B/C are all implemented (DOMA/DTEL unsuffixed/existing, `_prog`,
`_fugr`, `_tabl` - **TT-003 FIX**: `_tabl`, not `_tabl_ttyp` - see
&sect;6 for why the longer suffix does not fit ABAP's 30-character
method-name limit for the `inject_batch_from_buffer_*` name) - all four
MUST keep distinct names within that limit; this is a mandatory,
decision-free implementation constraint, not a style choice. Before
final implementation, run a method-name-length scan (e.g. `Select-String
-Pattern '^\s*METHODS?\s+(\w+)'` over every touched file, per this
repository's own documented 30-char pitfall) across ALL new method names
introduced by Packages A/B/C together, not just the ones this design
happened to name explicitly.

## 10. Memory bounds

- `mt_tabl_text` row count is bounded by (tables in batch) x (extra
  languages per table) - typically small (most tables are maintained in
  1-3 languages) - SOURCE_DERIVED ESTIMATE, LOW risk, smaller per-row
  footprint than PROG's tpool_i18n (a single CHAR/text field vs a whole
  text pool table).
- `mt_tabl_extras` is bounded 1:1 by table count in the batch, one small
  fixed-size `TDDAT` row each - LOW risk, smallest payload of any
  provider in this slice.
- No DD03P/DD05M/DD08V/DD12V/DD17V/DD35V/DD36M data is ever held in this
  provider's cache or wire buffer (&sect;1 scope boundary) - this is what
  keeps this provider's memory profile bounded and low-risk, in exchange
  for the deliberately narrower win.

## 11. Required test design

```text
transparent table with 1 extra-language translation - HIT, i18n text
  round-trips byte-identical
structure (TABL, tabclass=INTTAB or similar) - N/A distinction for THIS
  provider (i18n text/TDDAT prefetch applies uniformly regardless of
  tabclass; the excluded DD03P/etc. bundle is where tabclass-specific
  behavior would matter, out of scope)
include/append - N/A for this provider (excluded, &sect;1); a table WITH
  includes/appends still gets its i18n text/TDDAT correctly prefetched,
  since those two sub-features are entirely independent of field-level
  structure - explicit test: an include-bearing table's i18n/extras HIT
  is unaffected by its own DD03P complexity
technical settings - N/A, excluded (&sect;1), unaffected, standard path
  unchanged
foreign key/search help - N/A, excluded (&sect;1), unaffected
currency/quantity refs - N/A, excluded (&sect;1) - these live in DD03P/
  DD08V-adjacent structures, unaffected by this provider
languages/missing texts - table with ZERO extra-language DD02T rows -
  valid HIT-eligible-but-empty i18n_langs (mirrors DOMA/PROG's "present
  but empty" contract, &sect;5); table with an existing DD02T row for a
  language whose DDTEXT is EMPTY-but-the-row-genuinely-exists
  (DDLANGUAGE populated, DDTEXT initial) - MUST be KEPT as a valid text
  row (TT-001 fix, &sect;4's corrected skip condition checks DDLANGUAGE,
  never DDTEXT) - this is now an EXPLICIT required test case, since
  cycle 1's design got this backwards
large field count - N/A for THIS provider (DD03P excluded, &sect;1); a
  large-field-count table's i18n/extras prefetch performance is
  independent of its own field count - explicit test confirms this
  (payload size scales with LANGUAGE count, not FIELD count)
namespaces (/NS/ZTABLE) round trip through tabname key
inactive version present - `prepare_tabl`'s `SELECT * FROM dd02t` has NO
  active/inactive filter (DD02T is a pure text table, not version-gated
  like DD01L/DD01T's `as4local`/`as4vers`) - CONFIRMED_SOURCE difference
  from DOMA; explicit test: a table with an inactive DDIC version present
  still gets correct MAIN-LANGUAGE behavior from the UNCHANGED standard
  DDIF_TABL_GET call (this provider never touches gotstate/active-version
  logic at all, &sect;1), and correct i18n/extras from this provider
  regardless of the main object's active/inactive state (TDDAT/DD02T are
  independent of table activation state)
TTYP standard/sorted/hashed, unique/non-unique key, reference/DDIC line
  type - N/A, TTYP has no provider this slice (&sect;1 DEFER) - explicit
  test only proves the NO-OP path: a TTYP object always reports
  PROVIDER_MISS and produces byte-identical output to today, whether the
  RFC-batch path is active or not
mixed TABL/TTYP batch - a dispatch containing BOTH TABL and TTYP objects:
  TABL entries get real HIT/MISS per &sect;7, TTYP entries are entirely
  absent from `entries` (never looped, `WHERE object = 'TABL'` in
  &sect;6) - confirms zero interference between the two object types in
  ONE shared dispatch
TDDAT checked-but-absent (TT-002) - a table with i18n text but NO TDDAT
  row: get_tabl_extras returns rv_found=true/es_tddat initial, NOT a
  MISS and NOT confused with "not prepared"; a table with NEITHER i18n
  text NOR a TDDAT row in an otherwise-non-empty batch: still a real `P`
  entry (checked-and-empty on both counts), distinguishable from a
  hypothetical un-prepared object only by mv_prepared/PREPARE having run
  at all - explicit tests for both cases, plus an all-checked-and-empty
  TABL batch (extract still returns a real buffer since lv_any_hit is
  driven by prepare having run for at least one object, not by any
  object having non-empty text/tddat content)
hit/miss/fallback - one test per &sect;7 row
corrupt/version mismatch - one test per &sect;6's TT-004 validation rows
  (unexpected provider_id, unexpected entry obj_type/state, duplicate
  text payload, duplicate extras payload, extras-payload-count mismatch
  vs P entries, extras/text payload key not in P entries) IN ADDITION TO
  the Package B &sect;4-style baseline rows (unknown version, object_
  count mismatch, duplicate ENTRIES row, corrupt IMPORT)
cross-batch isolation - two sequential worker-style
  inject_batch_from_buffer_tabl calls with different table sets; second
  call's clear-first (`clear_tabl_cache`) leaves zero trace of the first
direct serialized-output parity - compare a real live TABL object's
  `.tabl.xml` I18N text section and (if applicable) the authorization-
  group-derived extras output under feature ON+batch OFF vs feature
  ON+batch ON vs feature OFF - all three byte-identical; IT8-only
  validation step, same disclosed boundary as Packages B/C &sect;10
```

## 12. Explicit named follow-up (NOT authorized by this design)

A future, SEPARATELY adversarially-reviewed slice may attempt full
DD03P (TABL fields) / DD43V (TTYP key fields) bulk reconstruction,
reusing `mt_dtel`/`mt_doma` for per-field technical-attribute resolution
(&sect;0's synergy observation) - but ONLY after a dedicated discovery
pass specifically into SAP's structure/include/append flattening
semantics (multi-level nesting, conditional `.INCLUDE ... IF ...`,
enhancement-category interactions) proves a safe, decision-free bulk
algorithm exists, AND a byte-identical parity test (same rigor as
DOMA's DR-001 fix) passes against a representative sample of real
include/append-bearing tables BEFORE any IT8 validation is claimed. This
design explicitly does NOT authorize that work and does NOT estimate its
size/risk beyond "meaningfully higher than every other provider in this
slice" (HYPOTHESIS, reasoning basis in &sect;0).
