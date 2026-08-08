# SER-SLICE-4 — Implementation-readiness audit (Packages A/B/C)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_4_IMPLEMENTATION_READINESS
BASELINE_HEAD=e5e10d62137033801739b8a53c69bdff9cfa650e
STATUS=READY_AWAITING_OWNER_IMPLEMENTATION_DECISION
GATES=ADVERSARIAL(A/B/C)=APPROVE, CORRECTNESS=APPROVE_WITH_MINOR_REVISIONS
  (CG-001 REJECTED_WITH_PROOF, CG-002/003 actioned), PERFORMANCE=APPROVE
  (PF-001/002/003 fixed and re-verified)
```

This audit converts the three approved designs
(`serialization_slice_4_tabl_ttyp_design.md`,
`serialization_slice_4_prog_design.md`,
`serialization_slice_4_fugr_design.md`) into discrete, ordered
implementation slices with exact anchors. Every slice below references
the design doc section that already contains the full decision-free
pseudocode - this audit does not repeat that pseudocode, it adds the
missing implementation-mechanics fields (COMMIT_BOUNDARY/STOP_IF/
VALIDATION) and pins exact file/method/anchor identifiers.

No slice below is authorized for execution by this document alone - per
the mission's hard boundaries, implementation requires a separate,
explicit owner GO decision (see `SER_SLICE_4_STATUS` in `.memory/state.md`).

## Slice 0 — shared ORCH correction (prerequisite for A/B/C's byte admission)

```text
FILE_OR_OBJECT=src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap
METHOD_OR_DDIC=BEFORE_DISPATCH
ANCHOR=the line `lv_actual_bytes = xstrlen( lv_prefetch_buffer_dd ).`
  (immediately after the existing DD extract_for_batch call)
ACTION=replace
CHANGE=after computing lv_prefetch_buffer_tabl/_prog/_fugr (Slices A4/B4/
  C4 below), change this single assignment to
  `lv_actual_bytes = xstrlen( lv_prefetch_buffer_dd )
     + xstrlen( lv_prefetch_buffer_oo_batch )
     + xstrlen( lv_prefetch_buffer_msag )
     + xstrlen( lv_prefetch_buffer_tabl )
     + xstrlen( lv_prefetch_buffer_prog )
     + xstrlen( lv_prefetch_buffer_fugr ).`
  (shared_infrastructure.md &sect;3 - CORRECTNESS GATE CG-001 independently
  re-confirmed via direct source read that lv_prefetch_buffer_oo_batch/
  _msag ARE already computed and forwarded to dispatch_batch/the RFC
  call today, only the byte-SUM omits them - this slice closes that gap
  for all SIX buffers in one change, not just the three new ones)
INVARIANTS=no double-counting; no omission; computed exactly once per
  before_dispatch invocation (including each recursive split call,
  matching the existing DD/OO_BATCH/MSAG recomputation-per-split shape,
  &sect;4 shared infra)
SQL_OR_API_SHAPE=none - pure arithmetic, no new DB access
MEMORY_BOUNDS=no new retained state
ERROR_FALLBACK=none - this is an admission-check input, not itself a
  failure path
TESTS=a batch whose COMBINED six-buffer size exceeds
  c_max_actual_batch_bytes while NO SINGLE buffer alone exceeds it must
  now trigger the split-and-recurse path (regression test proving the
  pre-fix gap: construct such a batch, assert split occurs)
VALIDATION=local get_errors clean + ABAP Unit on before_dispatch's
  existing split-behavior tests, extended with the new combined-size
  case; final IT8 activation/ATC/unit pass required per the "every
  slice needs real IT8 validation" binding invariant
COMMIT_BOUNDARY=bundle with whichever of Slice A4/B4/C4 is implemented
  FIRST (this line cannot compile/be meaningful in isolation before at
  least one new lv_prefetch_buffer_* local exists) - do not commit this
  change alone
STOP_IF=any existing DD/OO_BATCH/MSAG regression test starts failing
  after this change (would indicate the pre-existing split threshold
  behavior was implicitly depended upon elsewhere)
```

## Package A (TABL) slices

### Slice A1 — DDIC objects

```text
FILE_OR_OBJECT=ZAOG_SER_TABL_TX_BROW (structure), ZAOG_SER_TABL_TX_BROW_TT
  (table type), ZAOG_SER_TABL_EX_BROW (structure), ZAOG_SER_TABL_EX_BROW_TT
  (table type) - package matches ZAOG_SER_ENV_BHDR's own package
METHOD_OR_DDIC=DDIC structures/table types (new objects)
ANCHOR=n/a (new objects)
ACTION=insert
CHANGE=exact field lists per serialization_slice_4_tabl_ttyp_design.md
  &sect;3 (ZAOG_SER_TABL_TX_BROW: tabname/ddlanguage/ddtext;
  ZAOG_SER_TABL_EX_BROW: tabname/tddat)
INVARIANTS=field types match dd02l-tabname/dd02t-ddlanguage/dd02t-ddtext/
  tddat-tabname/tddat (structure) exactly - no release-unstable field
  typing risk here (unlike FUGR's TFDIR fields, these are all stable,
  long-standing DDIC tables)
SQL_OR_API_SHAPE=n/a (DDIC only)
MEMORY_BOUNDS=n/a (DDIC only)
ERROR_FALLBACK=n/a (DDIC only)
TESTS=n/a (DDIC only, exercised by A2's tests)
VALIDATION=DDIC activation must succeed before any dependent class change
COMMIT_BOUNDARY=own commit, DDIC-only, before A2
STOP_IF=DDIC activation fails for any reason (e.g. a package/transport
  assignment conflict) - do not proceed to A2 until resolved
```

### Slice A2 — `ZCL_ABAPGIT_ORTEC_SER_PREF_EXT` provider core

```text
FILE_OR_OBJECT=src/ortec/serial/zcl_abapgit_ortec_ser_pref_ext.clas.abap
METHOD_OR_DDIC=new PRIVATE types (ty_tabl_text_lang/_tt,
  ty_tabl_text_cache/_tt, ty_tabl_extras_cache/_tt), new CLASS-DATA
  (mt_tabl_text, mt_tabl_extras), new methods (prepare_tabl,
  get_tabl_i18n [PUBLIC], get_tabl_extras [PUBLIC],
  extract_for_batch_tabl [PUBLIC], inject_batch_from_buffer_tabl
  [PUBLIC], clear_tabl_cache [PUBLIC]); extended existing methods
  (collect_keys gains WHEN 'TABL', PREPARE gains prepare_tabl( ) call,
  CLEAR gains CLEAR mt_tabl_text/mt_tabl_extras)
ANCHOR=collect_keys's existing `WHEN 'TRAN'.` branch (insert a new WHEN
  'TABL' branch immediately before or after it); PREPARE's existing
  `prepare_tran(...)` call line (insert prepare_tabl call adjacent);
  CLEAR's existing `CLEAR mv_language.` line (insert the two new CLEAR
  lines immediately before it, matching the method's existing ordering)
ACTION=insert
CHANGE=exact decision-free pseudocode in
  serialization_slice_4_tabl_ttyp_design.md &sect;4 (prepare_tabl,
  PF-001-fixed nested-cache shape), &sect;5 (get_tabl_i18n/
  get_tabl_extras, TT-002/TT-005-fixed contracts), &sect;6
  (extract_for_batch_tabl/inject_batch_from_buffer_tabl/
  clear_tabl_cache, TT-003/TT-004/PF-001-fixed)
INVARIANTS=method names <=30 chars (verified: extract_for_batch_tabl=22,
  inject_batch_from_buffer_tabl=29, clear_tabl_cache=16, get_tabl_i18n=13,
  get_tabl_extras=15, prepare_tabl=12 - correctness gate CG- exhaustive
  recount); zero name collision with existing DOMA/DTEL
  extract_for_batch/inject_batch_from_buffer/clear_dd_cache on the SAME
  class; O(1) full-key lookups only, no partial-key LOOP AT...WHERE scan
  anywhere (PF-001); canonical P/M rule per &sect;3 (P iff prepare_tabl
  processed the object); strict duplicate/unexpected-payload-key
  rejection per &sect;6 (TT-004)
SQL_OR_API_SHAPE=`SELECT * FROM dd02t INTO TABLE lt_dd02t FOR ALL ENTRIES
  IN it_names WHERE tabname = it_names-table_line AND ddlanguage <>
  iv_main_language.` and `SELECT * FROM tddat INTO TABLE lt_tddat FOR
  ALL ENTRIES IN it_names WHERE tabname = it_names-table_line.` - both
  guarded by `IF it_names IS INITIAL. RETURN. ENDIF.` at method entry
MEMORY_BOUNDS=&sect;10 of the design (LOW risk, smallest payload of any
  provider this slice - see shared_infrastructure.md &sect;3's combined-
  sum requirement, Slice 0)
ERROR_FALLBACK=&sect;7 of the design (unknown provider_id/entry type or
  state/duplicate payload/payload-entry mismatch all reject the WHOLE
  buffer; missing entry -> per-accessor rv_found = abap_false ->
  standard fallback in A3)
TESTS=full list in &sect;11 of the design (18 named cases incl. TT-001/
  TT-002/TT-004/TT-005/PF-001 regression cases)
VALIDATION=local get_errors clean on main + testclasses; live SAP
  syntax check (`SAPDiagnose action=syntax`) before ABAP Unit; ABAP Unit
  full pass; final IT8 activation
COMMIT_BOUNDARY=own commit (class + testclasses together), after A1,
  before A3
STOP_IF=any &sect;11 test fails and the root cause is a genuine
  DD02T/TDDAT semantic difference not covered by this design (escalate
  to design revision, do not patch around it silently)
```

### Slice A3 — `zcl_abapgit_object_tabl.clas.abap` seams

```text
FILE_OR_OBJECT=src/objects/tabl/zcl_abapgit_object_tabl.clas.abap
METHOD_OR_DDIC=zif_abapgit_object~serialize (read_extras call site),
  serialize_texts (language-discovery query + per-language loop)
ANCHOR=`ls_internal-extras = read_extras( lv_name ).` (serialize);
  `SELECT DISTINCT ddlanguage AS langu INTO TABLE cs_internal-i18n_langs
  ... FROM dd02v ...` through the `LOOP AT cs_internal-i18n_langs ...
  ENDLOOP.` block (serialize_texts)
ACTION=replace
CHANGE=exact decision-free pseudocode in
  serialization_slice_4_tabl_ttyp_design.md &sect;5 ("Seam in
  zif_abapgit_object~serialize's read_extras call site" and "Seam in
  serialize_texts")
INVARIANTS=feature OFF (`is_serial_prefetch_active = abap_false`) takes
  the EXACT unchanged standard path with zero new branches reachable;
  &sect;1's excluded-scope items (DD03P/etc., get_abap_language_version,
  IDoc segment, longtexts) remain 100% untouched by this seam
SQL_OR_API_SHAPE=no new SQL in this file - both branches call the A2
  accessors
MEMORY_BOUNDS=n/a (no new local state beyond existing method locals)
ERROR_FALLBACK=`lv_extras_prefetched`/`lv_i18n_prefetched` = abap_false
  on any MISS falls through to the UNCHANGED existing
  read_extras/SELECT DISTINCT+DDIF_TABL_GET-loop code, byte-for-byte
INVARIANTS=see above (duplicate key kept intentionally for schema
  completeness)
TESTS=the "direct serialized-output parity" row in &sect;11 (feature
  OFF vs feature ON+batch OFF vs feature ON+batch ON, byte-identical
  .tabl.xml)
VALIDATION=live SAP syntax check + real TABL object serialize
  before/after diff at IT8 (this is the ONLY slice that touches a
  standard-abapGit-adjacent object class file, so extra care per the
  binding invariant "any standard-abapGit-class change for an ORTEC
  hook must stay the smallest possible delegation")
COMMIT_BOUNDARY=own commit, after A2, before A4
STOP_IF=any byte-for-byte parity test fails - this is the highest-
  consequence slice in Package A (a real, widely-used object class) and
  must not be merged with any observed output difference
```

### Slice A4 — ORCH wiring (TABL-specific half of Slice 0)

```text
FILE_OR_OBJECT=src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap
METHOD_OR_DDIC=BEFORE_DISPATCH, DISPATCH_BATCH
ANCHOR=BEFORE_DISPATCH's existing `DATA(lv_prefetch_buffer_msag) =
  zcl_abapgit_ortec_ser_pref=>extract_for_batch( it_object_keys ).` line
  (insert the new `lv_prefetch_buffer_tabl` computation immediately
  after it); DISPATCH_BATCH's signature list (insert
  `iv_prefetch_buffer_tabl TYPE xstring OPTIONAL` after
  `iv_prefetch_buffer_msag`) and its RFC `CALL FUNCTION` EXPORTING list
  (insert `iv_prefetch_buffer_tabl = iv_prefetch_buffer_tabl` after
  `iv_prefetch_buffer_msag = iv_prefetch_buffer_msag`)
ACTION=insert
CHANGE=`DATA(lv_prefetch_buffer_tabl) =
  zcl_abapgit_ortec_ser_pref_ext=>extract_for_batch_tabl(
  it_object_keys ).` plus the two signature/call-list insertions above,
  plus this dispatch_batch call's own EXPORTING list gaining
  `iv_prefetch_buffer_tabl = lv_prefetch_buffer_tabl` at the
  BEFORE_DISPATCH -> DISPATCH_BATCH call site
INVARIANTS=see Slice 0 (combined byte-sum correctness); recursive split
  calls in BEFORE_DISPATCH recompute this buffer independently per half,
  matching existing DD/OO_BATCH/MSAG shape exactly
SQL_OR_API_SHAPE=none directly (delegates to A2's extract_for_batch_tabl)
MEMORY_BOUNDS=one xstring local per before_dispatch invocation/split level
ERROR_FALLBACK=an oversized singleton whose OWN tabl buffer alone
  exceeds c_max_actual_batch_bytes is dispatched anyway with all
  provider buffers empty (existing structural fallback, unchanged)
TESTS=Slice 0's combined-size test, extended to include a TABL-heavy
  batch
VALIDATION=ABAP Unit on BEFORE_DISPATCH/DISPATCH_BATCH's existing test
  suite, extended
COMMIT_BOUNDARY=bundle with Slice 0 and A5 (ORCH + RFC interface changes
  form one coherent, independently-importable unit)
STOP_IF=any existing DD/OO_BATCH/MSAG dispatch test regresses
```

### Slice A5 — RFC worker + interface

```text
FILE_OR_OBJECT=src/ortec/serial/rfc/zabapgit_ortec_serial.fugr.z_abapgit_ortec_ser_batch.abap,
  src/ortec/serial/rfc/zabapgit_ortec_serial.fugr.xml
METHOD_OR_DDIC=FUNCTION z_abapgit_ortec_ser_batch (interface + body)
ANCHOR=interface: after the existing
  `VALUE(IV_PREFETCH_BUFFER_MSAG) TYPE XSTRING OPTIONAL` line; body:
  after the existing `zcl_abapgit_ortec_ser_pref=>clear_msag_cache( ).`
  / `inject_batch_from_buffer` MSAG block; CASE ls_tadir-object: after
  the existing `WHEN 'CLAS' OR 'INTF'.`/`WHEN` MSAG-equivalent branch
ACTION=insert
CHANGE=new `IV_PREFETCH_BUFFER_TABL TYPE XSTRING OPTIONAL` interface
  parameter (+ matching RSIMP/RSFDO rows in the .fugr.xml, mirroring the
  MSAG parameter's own XML rows exactly); body gains
  `zcl_abapgit_ortec_ser_pref_ext=>clear_tabl_cache( ).` (unconditional)
  then `IF iv_prefetch_buffer_tabl IS NOT INITIAL. TRY.
  zcl_abapgit_ortec_ser_pref_ext=>inject_batch_from_buffer_tabl(
  iv_prefetch_buffer_tabl ). CATCH zcx_abapgit_exception ##NO_HANDLER.
  ENDTRY. ENDIF.`; CASE gains `WHEN 'TABL'.` per
  serialization_slice_4_tabl_ttyp_design.md &sect;8
INVARIANTS=unconditional clear-first on EVERY worker invocation (never
  only on successful inject) - matches DD/OO_BATCH/MSAG precedent
  exactly; swallowed exception never propagates (batch always completes)
SQL_OR_API_SHAPE=none (delegates to A2)
MEMORY_BOUNDS=see A2 &sect;10
ERROR_FALLBACK=see A2 &sect;7
TESTS=cross-batch isolation (&sect;11) exercised at the worker level;
  this is the ONE slice CG-002 flagged as never independently source-
  verified in any TABL adversarial review cycle - a mandatory pre-
  implementation spot-check (mirror the already-verified PROG/FUGR
  pattern in the SAME file) is required before this slice is considered
  implementation-ready, not just designed
VALIDATION=live SAP syntax check on the function group; ABAP Unit
  (worker-level tests require a real or mocked RFC call harness -
  mirror however DD/OO_BATCH/MSAG's own worker-level tests are
  structured in this codebase); final IT8 end-to-end batch-dispatch test
COMMIT_BOUNDARY=bundle with Slice 0 and A4
STOP_IF=the CG-002 pre-implementation spot-check reveals the worker
  body's real structure differs from the DD/OO_BATCH/MSAG pattern this
  slice assumes - revise this slice's ANCHOR/CHANGE before proceeding,
  do not implement against an unverified assumption
```

## Package B (PROG) slices

### Slice B1 — DDIC objects

```text
FILE_OR_OBJECT=ZAOG_SER_PROG_BROW (structure), ZAOG_SER_PROG_BROW_TT
  (table type)
METHOD_OR_DDIC=DDIC structures/table types (new objects)
ANCHOR=n/a (new objects)
ACTION=insert
CHANGE=exact field list per serialization_slice_4_prog_design.md &sect;2
  (program TYPE d010tinf-prog, tpool_i18n TYPE
  zif_abapgit_lang_definitions=>ty_i18n_tpools)
INVARIANTS=byte-identical shape to the EXISTING PRIVATE ty_prog_lang_cache
  row (minus the hashed-key wrapper) - no new field, no new derivation
SQL_OR_API_SHAPE=n/a (DDIC only)
MEMORY_BOUNDS=n/a
ERROR_FALLBACK=n/a
TESTS=n/a (exercised by B2's tests)
VALIDATION=DDIC activation must succeed before B2
COMMIT_BOUNDARY=own commit, DDIC-only, before B2
STOP_IF=DDIC activation fails
```

### Slice B2 — `ZCL_ABAPGIT_ORTEC_SER_PREF_EXT` provider core

```text
FILE_OR_OBJECT=src/ortec/serial/zcl_abapgit_ortec_ser_pref_ext.clas.abap
METHOD_OR_DDIC=new PUBLIC methods extract_for_batch_prog,
  inject_batch_from_buffer_prog, clear_prog_cache - NO new CLASS-DATA
  (reuses the EXISTING mt_prog_langs) and NO changes to PREPARE/CLEAR/
  collect_keys (already wired from SER-SLICE-3, correctness gate CG-003
  confirmed)
ANCHOR=insert the three new methods anywhere in the class's existing
  method-grouping convention (adjacent to the existing
  get_prog_tpool_languages/prepare_prog_langs methods is the natural
  placement, though not load-bearing)
ACTION=insert
CHANGE=exact decision-free pseudocode in
  serialization_slice_4_prog_design.md &sect;3 (extract_for_batch_prog,
  PR-001/PR-003-fixed), &sect;4 (inject_batch_from_buffer_prog,
  PR-001/PR-002/PR-003/PR-005-fixed, clear_prog_cache)
INVARIANTS=method names <=30 chars (extract_for_batch_prog=22,
  inject_batch_from_buffer_prog=29, clear_prog_cache=16); zero collision
  with existing DOMA/DTEL/TABL method names on the SAME class (Package
  A's Slice A2 and this slice touch the SAME class - verified zero
  overlap by the correctness gate's exhaustive method-name audit); guard
  is `IF mv_language IS INITIAL` (NOT a nonexistent mv_prepared field -
  PR-003); inject rejects a buffer with `lv_language IS INITIAL` as
  corrupt BEFORE any cache mutation (PR-005); strict duplicate/
  unexpected-payload-key rejection (PR-002)
SQL_OR_API_SHAPE=none - this slice adds ZERO new SQL (reuses
  prepare_prog_langs's existing bulk read verbatim, unchanged)
MEMORY_BOUNDS=&sect;7 of the design (LOW risk, source-derived estimate)
ERROR_FALLBACK=&sect;5 of the design
TESTS=full list in &sect;10 of the design (PR-002/PR-004/PR-005
  regression cases included)
VALIDATION=local get_errors clean; live SAP syntax check; ABAP Unit full
  pass; final IT8 activation
COMMIT_BOUNDARY=own commit (class + testclasses together), after B1
STOP_IF=any &sect;10 test fails
```

### Slice B3 — ORCH wiring (PROG-specific half of Slice 0)

```text
FILE_OR_OBJECT=src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap
METHOD_OR_DDIC=BEFORE_DISPATCH, DISPATCH_BATCH
ANCHOR=same as Slice A4, but for the PROG buffer (insert alongside/after
  the TABL insertion if both slices land together, or independently if
  Package B ships before Package A)
ACTION=insert
CHANGE=`DATA(lv_prefetch_buffer_prog) =
  zcl_abapgit_ortec_ser_pref_ext=>extract_for_batch_prog(
  it_object_keys ).` plus DISPATCH_BATCH signature/call-list insertions,
  per serialization_slice_4_prog_design.md &sect;6
INVARIANTS=see Slice 0
SQL_OR_API_SHAPE=none directly
MEMORY_BOUNDS=one xstring local per invocation/split level
ERROR_FALLBACK=same structural fallback as Slice A4
TESTS=Slice 0's combined-size test, extended for a PROG-heavy batch
VALIDATION=ABAP Unit extension
COMMIT_BOUNDARY=bundle with Slice 0 and B4
STOP_IF=any existing dispatch test regresses
```

### Slice B4 — RFC worker + interface

```text
FILE_OR_OBJECT=src/ortec/serial/rfc/zabapgit_ortec_serial.fugr.z_abapgit_ortec_ser_batch.abap,
  src/ortec/serial/rfc/zabapgit_ortec_serial.fugr.xml
METHOD_OR_DDIC=FUNCTION z_abapgit_ortec_ser_batch
ANCHOR=same anchor pattern as Slice A5, for the PROG buffer/branch
ACTION=insert
CHANGE=new `IV_PREFETCH_BUFFER_PROG` interface parameter; body gains
  unconditional `clear_prog_cache( )` then conditional
  `inject_batch_from_buffer_prog(...)`; CASE gains `WHEN 'PROG'.` per
  serialization_slice_4_prog_design.md &sect;6 - this slice's root-cause
  claim (before_dispatch never populates the OLD generic
  iv_prefetch_buffer_ext, so mt_prog_langs is currently always empty in
  an RFC worker) was independently CONFIRMED_SOURCE by this orchestrator
  via direct read of this exact file earlier in the design session, in
  addition to the cycle-2 adversarial review's own confirmation
INVARIANTS=same as Slice A5's pattern
SQL_OR_API_SHAPE=none (delegates to B2)
MEMORY_BOUNDS=see B2 &sect;7
ERROR_FALLBACK=see B2 &sect;5
TESTS=&sect;10's cross-batch isolation case at the worker level
VALIDATION=live SAP syntax check; ABAP Unit; final IT8 end-to-end
COMMIT_BOUNDARY=bundle with Slice 0 and B3
STOP_IF=none identified beyond the generic worker-source spot-check
  discipline (this slice, unlike A5, WAS independently source-verified
  across cycles 1-2 of its own adversarial review, so no open CG-002-
  style gap exists here)
```

## Package C (FUGR) slices

### Slice C1 — DDIC objects

```text
FILE_OR_OBJECT=ZAOG_SER_FUGR_AT_BROW/_TT, ZAOG_SER_FUGR_ED_BROW/_TT,
  ZAOG_SER_FUGR_FN_BROW/_TT (structures + table types)
METHOD_OR_DDIC=DDIC structures/table types (new objects)
ANCHOR=n/a (new objects)
ACTION=insert
CHANGE=exact field lists per serialization_slice_4_fugr_design.md
  &sect;2, POST-FIX (FG-001): `ZAOG_SER_FUGR_FN_BROW-RFCSCOPE TYPE C
  LENGTH 1`, `-RFCVERS TYPE C LENGTH 10` (release-stable primitives,
  NEVER tfdir-rfcscope/tfdir-rfcvers) - matches
  zcl_abapgit_object_fugr's own ty_function-rfcscope/rfcvers exactly
  (CONFIRMED_SOURCE)
INVARIANTS=activates identically on every release regardless of whether
  TFDIR itself has RFCSCOPE/RFCVERS on that release (FG-001's entire
  point)
SQL_OR_API_SHAPE=n/a (DDIC only)
MEMORY_BOUNDS=n/a
ERROR_FALLBACK=n/a
TESTS=n/a (exercised by C2's tests, including a release-without-these-
  fields simulation if feasible, else documented as IT8-only)
VALIDATION=DDIC activation must succeed before C2, on a release
  representative of the LOWEST supported release (to catch any
  remaining release-dependent typing issue)
COMMIT_BOUNDARY=own commit, DDIC-only, before C2
STOP_IF=DDIC activation fails on any supported release
```

### Slice C2 — `ZCL_ABAPGIT_ORTEC_SER_PREF_EXT` provider core

```text
FILE_OR_OBJECT=src/ortec/serial/zcl_abapgit_ortec_ser_pref_ext.clas.abap
METHOD_OR_DDIC=extended existing PRIVATE type ty_fugr_func_meta (gains
  rfcscope/rfcvers/rfc_fields_valid - additive, FG-002-fixed single-
  cache-shape), extended existing prepare_fugr (new TFDIR bulk read,
  PF-002-fixed with explicit driver-table guard), extended existing
  get_fugr_func_metadata (two new OPTIONAL EXPORTING params), new
  PUBLIC methods extract_for_batch_fugr, inject_batch_from_buffer_fugr,
  clear_fugr_cache
ANCHOR=ty_fugr_func_meta's own TYPES declaration (append 3 fields);
  prepare_fugr's method body, after its existing ENLFDIR/TLIBT/func_meta
  bulk reads (append the new TRY/CATCH TFDIR block); insert the three
  new PUBLIC methods adjacent to the existing get_fugr_*/prepare_fugr
  methods
ACTION=insert (append fields/new methods), replace (get_fugr_func_metadata
  signature only - additive OPTIONAL params, source-compatible with
  every existing caller)
CHANGE=exact decision-free pseudocode in
  serialization_slice_4_fugr_design.md &sect;2 (payload types),
  &sect;3 (extract_for_batch_fugr, mv_language-guard fixed),
  &sect;4 (inject_batch_from_buffer_fugr/clear_fugr_cache,
  FG-002-fixed single-cache-shape), &sect;6 (prepare_fugr TFDIR
  extension, FG-001/PF-002-fixed; get_fugr_func_metadata signature
  extension; serialize_functions consumer update - see Slice C3)
INVARIANTS=method names <=30 chars (extract_for_batch_fugr=22,
  inject_batch_from_buffer_fugr=29, clear_fugr_cache=16); zero
  collision with existing/Package A/B method names on the SAME class;
  guard is `IF mv_language IS INITIAL` (FG-family mv_prepared fix);
  ONE cache (mt_fugr_func_meta) carries rfcscope/rfcvers/
  rfc_fields_valid, no separate mt_fugr_tfdir (FG-002); TFDIR SELECT
  guarded by an explicit `IF lt_funcnames IS INITIAL. RETURN. ENDIF.`
  (PF-002)
SQL_OR_API_SHAPE=`SELECT funcname, rfcscope, rfcvers FROM ('TFDIR') FOR
  ALL ENTRIES IN @lt_funcnames WHERE funcname =
  @lt_funcnames-table_line INTO CORRESPONDING FIELDS OF TABLE @lt_tfdir.`
  inside `TRY ... CATCH cx_sy_dynamic_osql_semantics.` (release gate
  protects the DYNAMIC SELECT's runtime execution only, never a type
  declaration - FG-001)
MEMORY_BOUNDS=&sect;8 of the design (MEDIUM risk, PF-003-clarified
  oversized-singleton fallback mechanism)
ERROR_FALLBACK=&sect;5 of the design
TESTS=full list in &sect;10 of the design (FG-001..FG-004 regression
  cases, PF-002 empty-driver case included)
VALIDATION=local get_errors clean; live SAP syntax check on a release
  WITHOUT TFDIR-RFCSCOPE/RFCVERS if such a system is available (to
  directly exercise the CATCH branch), else IT8-only for that specific
  release-gap case; ABAP Unit full pass; final IT8 activation
COMMIT_BOUNDARY=own commit (class + testclasses together), after C1,
  before C3
STOP_IF=any &sect;10 test fails
```

### Slice C3 — `zcl_abapgit_object_fugr.clas.abap` seams

```text
FILE_OR_OBJECT=src/objects/zcl_abapgit_object_fugr.clas.abap
METHOD_OR_DDIC=serialize_functions (TFDIR RFCSCOPE/RFCVERS consumer
  block), serialize_texts (NEW seam, FG-003 fix)
ANCHOR=serialize_functions's existing
  `TRY. SELECT SINGLE rfcscope rfcvers INTO CORRESPONDING FIELDS OF
  ls_function FROM ('TFDIR') WHERE funcname = <ls_func>-funcname. CATCH
  cx_sy_dynamic_osql_semantics ##NO_HANDLER. ENDTRY.` block;
  serialize_texts's existing
  `SELECT DISTINCT language INTO CORRESPONDING FIELDS OF TABLE
  lt_tpool_i18n FROM d010tinf WHERE r3state = 'A' AND prog =
  iv_prog_name AND language <> mv_language ORDER BY language
  ##TOO_MANY_ITAB_FIELDS.` line
ACTION=replace
CHANGE=exact decision-free pseudocode in
  serialization_slice_4_fugr_design.md &sect;6 (serialize_functions
  consumer update, additive-optional-parameter call) and &sect;6a
  (serialize_texts seam, FG-004-fixed to correctly gate on
  get_prog_tpool_languages's real RETURNING rv_found)
INVARIANTS=feature OFF takes the exact unchanged standard path; the
  `mo_i18n_params->trim_saplang_keyed_table`/`SORT`/`READ TEXTPOOL`
  loop tail remains 100% unchanged regardless of which branch supplied
  `lt_tpool_i18n`'s rows (FG-004's own fix requirement)
SQL_OR_API_SHAPE=no new SQL in this file - both branches call C2's
  accessors (get_fugr_func_metadata / the PRE-EXISTING
  get_prog_tpool_languages, independent of whether Package B is ever
  implemented - correctness gate independently verified this dependency
  holds against live source)
MEMORY_BOUNDS=n/a (no new local state)
ERROR_FALLBACK=`lv_rfc_prefetched`/`lv_fugr_i18n_prefetched` = abap_false
  on any MISS falls through to the UNCHANGED existing dynamic-SQL/
  SELECT-DISTINCT code, byte-for-byte
TESTS=the "direct output parity" rows in &sect;10 (RFCSCOPE/RFCVERS
  output) and the new "module texts" row (&sect;10, FG-003 fix's
  I18N_TPOOL parity test)
VALIDATION=live SAP syntax check + real FUGR object serialize
  before/after diff at IT8 (same elevated care as TABL's Slice A3 -
  this touches a standard-abapGit-adjacent object class)
COMMIT_BOUNDARY=own commit, after C2, before C4
STOP_IF=any byte-for-byte parity test fails, especially the NEW
  I18N_TPOOL seam (FG-003/FG-004's own fix history shows this exact
  seam already produced two real defects during design review - treat
  its IT8 validation with proportionally higher scrutiny)
```

### Slice C4 — ORCH wiring (FUGR-specific half of Slice 0)

```text
FILE_OR_OBJECT=src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap
METHOD_OR_DDIC=BEFORE_DISPATCH, DISPATCH_BATCH
ANCHOR=same pattern as Slices A4/B3, for the FUGR buffer
ACTION=insert
CHANGE=`DATA(lv_prefetch_buffer_fugr) =
  zcl_abapgit_ortec_ser_pref_ext=>extract_for_batch_fugr(
  it_object_keys ).` plus DISPATCH_BATCH signature/call-list insertions,
  per serialization_slice_4_fugr_design.md &sect;9
INVARIANTS=see Slice 0
SQL_OR_API_SHAPE=none directly
MEMORY_BOUNDS=one xstring local per invocation/split level
ERROR_FALLBACK=same structural fallback as Slices A4/B3
TESTS=Slice 0's combined-size test, extended for a FUGR-heavy batch
VALIDATION=ABAP Unit extension
COMMIT_BOUNDARY=bundle with Slice 0 and C5
STOP_IF=any existing dispatch test regresses
```

### Slice C5 — RFC worker + interface

```text
FILE_OR_OBJECT=src/ortec/serial/rfc/zabapgit_ortec_serial.fugr.z_abapgit_ortec_ser_batch.abap,
  src/ortec/serial/rfc/zabapgit_ortec_serial.fugr.xml
METHOD_OR_DDIC=FUNCTION z_abapgit_ortec_ser_batch
ANCHOR=same anchor pattern as Slices A5/B4, for the FUGR buffer/branch
ACTION=insert
CHANGE=new `IV_PREFETCH_BUFFER_FUGR` interface parameter; body gains
  unconditional `clear_fugr_cache( )` then conditional
  `inject_batch_from_buffer_fugr(...)`; CASE gains `WHEN 'FUGR'.` per
  serialization_slice_4_fugr_design.md &sect;7 - this slice's root-cause
  claim was independently CONFIRMED_SOURCE across cycle 1's adversarial
  review AND this orchestrator's own direct source read
INVARIANTS=same as Slices A5/B4's pattern
SQL_OR_API_SHAPE=none (delegates to C2)
MEMORY_BOUNDS=see C2 &sect;8
ERROR_FALLBACK=see C2 &sect;5
TESTS=&sect;10's cross-object/cross-batch isolation case at the worker
  level
VALIDATION=live SAP syntax check; ABAP Unit; final IT8 end-to-end
COMMIT_BOUNDARY=bundle with Slice 0 and C4
STOP_IF=none identified beyond the generic worker-source spot-check
  discipline (independently verified across this package's own review
  cycles)
```

## Cross-package ordering recommendation

```text
1. Slice A1/B1/C1 (DDIC, independent, any order, can be one combined
   transport)
2. Slice A2, B2, C2 (provider core - INDEPENDENT of each other, since
   correctness gate confirmed zero shared-entry-point interference; may
   be implemented in parallel by different implementers/sessions if
   desired, though the senior implementation agent should own the
   overall slice per the mode's routing rules given each touches
   protocol/persistence-adjacent wire-format code)
3. Slice A3, C3 (object-class seams - B has none; these are the highest-
   consequence slices, implement and IT8-validate ONE AT A TIME, never
   both uncommitted simultaneously)
4. Slice 0 + A4/B3/C4 + A5/B4/C5 together, per package (ORCH + RFC
   worker + interface form one coherent, testable unit per package -
   implement whichever package's slices 1-3 are already IT8-validated
   FIRST, to keep the "no partial success" invariant meaningful at each
   checkpoint)
```

## Non-goals restated (binding, not open questions)

```text
TABL DD03P/DD05M/DD08V/DD12V/DD17V/DD35V/DD36M bundle: OUT OF SCOPE, no
  slice above touches it (tabl_ttyp_design.md &sect;12 explicit named
  follow-up, not authorized here)
TTYP: NO SLICE this package (DEFER, tabl_ttyp_design.md &sect;1)
PROG source (RPY_PROGRAM_READ), dynpros/CUA: OUT OF SCOPE, no slice above
FUGR source/includes (Option C), dynpros/CUA: OUT OF SCOPE, no slice above
Longtexts (all three packages): OUT OF SCOPE, no slice above
```
