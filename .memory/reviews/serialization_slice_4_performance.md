# Performance Design Gate — SER-SLICE-4 (Packages A/B/C)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_4_PERFORMANCE_DESIGN_GATE
MODE=DESIGN_GATE
BASELINE_HEAD=e5e10d62137033801739b8a53c69bdff9cfa650e
REVIEW_SCOPE=READ_ONLY_EXCEPT_THIS_ARTIFACT
```

## Context read

- `.memory/logs/serialization_slice_4_tabl_ttyp_design.md` (cycle 3)
- `.memory/logs/serialization_slice_4_prog_design.md` (cycle 3)
- `.memory/logs/serialization_slice_4_fugr_design.md` (cycle 3)
- `.memory/logs/serialization_slice_4_shared_infrastructure.md`
- `.memory/logs/serialization_slice_4_common_discovery.md`
- `.memory/reviews/serialization_slice_4_correctness.md`
- Spot-verified against current productive source:
  `src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap`
  (`before_dispatch`, `dispatch_batch`, RFC `CALL FUNCTION
  'Z_ABAPGIT_ORTEC_SER_BATCH'`, byte-budget constants) and
  `src/ortec/serial/zcl_abapgit_ortec_ser_pref_ext.clas.abap`
  (`extract_for_batch`, `extract_for_object`, `prepare`,
  `prepare_prog_langs`, `prepare_fugr`) plus
  `zcl_abapgit_ortec_ser_pref_oo.clas.abap`/`zcl_abapgit_ortec_ser_pref
  .clas.abap`'s own `extract_for_batch` (CLAS/INTF, MSAG precedent) and
  `zif_abapgit_lang_definitions.intf.abap` (`ty_i18n_tpool` shape).

## Verdict

**REVISE_AND_REVIEW_ONCE**

One BLOCKER (PF-001) — a real algorithmic-complexity defect in Package
A's new cache shape, not present in Packages B/C, which reuse existing,
already-correct O(1) lookup caches. The fix is narrowly scoped (a
secondary key or a cache-shape change to `mt_tabl_text`/
`ty_tabl_text_cache_tt`) and does not require re-opening Packages B or C.

## Findings

### PF-001 — BLOCKER — Package A (TABL): O(N²) scan risk in `mt_tabl_text`

- Path: `ZCL_ABAPGIT_ORTEC_SER_PREF_EXT=>get_tabl_i18n` (§5, new
  single-object accessor) and `=>extract_for_batch_tabl` (§6, new batch
  method), both in `serialization_slice_4_tabl_ttyp_design.md`.
- Observed shape: `ty_tabl_text_cache_tt` is declared `HASHED TABLE ...
  WITH UNIQUE KEY tabname ddlanguage` (composite key, one row per
  table+language) with **no secondary key**. Both consumers read it via
  `LOOP AT mt_tabl_text ... WHERE tabname = iv_tabname` — a **partial-key**
  condition against a hashed table's full composite key. A hashed table
  only gives O(1) access for a full-key `READ TABLE ... WITH TABLE KEY`;
  a `LOOP AT ... WHERE` on a *leading subset* of a hashed key cannot use
  the hash index and falls back to a full linear scan of the table
  (confirmed ABAP runtime behavior, and previously documented as a known
  trap in this exact codebase's own repo memory).
- Why it matters: `PREPARE`/`prepare_tabl` runs **once per whole
  dispatch run** with the run's full `it_tadir` (confirmed at
  `zcl_abapgit_ortec_ser_orch.clas.abap` `serialize`, line ~876 —
  `zcl_abapgit_ortec_ser_pref_ext=>prepare( it_tadir = it_tadir ... )`,
  the full run's object list, not a per-batch slice). So `mt_tabl_text`
  holds text rows for **every** TABL object in the run (call this M
  rows, proportional to N_tabl × avg extra languages). `get_tabl_i18n`
  is called **once per TABL object actually serialized** — sequential
  path or RFC-worker path alike — so the *cumulative* cost of every
  TABL object's i18n lookup across a run of N_tabl objects is
  O(N_tabl × M) ≈ **O(N_tabl²)**, not O(N_tabl). `extract_for_batch_tabl`
  adds the same per-object scan again, once per batch/split-level.
  This directly contradicts the "normal incremental work scales with K
  (and bounded batches), not with all N" requirement, and does not
  match the pattern every sibling cache in this same slice uses:
  `mt_fugr_enlfdir` nests **all** function modules for one area under a
  **single** hashed row keyed by `area` (`READ TABLE ... WITH TABLE KEY
  area = ...`, O(1), no WHERE-scan), and `mt_prog_langs`/`mt_fugr_areat`/
  `mt_fugr_func_meta` are all single full-key reads. TABL's design chose
  a flatter, per-language-row shape instead of following that same
  precedent, which is what introduces the scan.
- Estimated cardinality impact: at the mandated "large" scenario
  (≥40,000 objects), a TABL-heavy subset of even a few thousand tables
  with 2-3 languages each would turn a linear i18n-lookup cost into a
  multi-million-comparison scan total — a real, silent CPU/response-time
  regression with zero SQL involved (it never shows up in an SQL trace,
  only in ABAP runtime/SAT).
- Required fix (either is sufficient, no code executed, design-level
  only):
  1. Add a secondary key to the cache and use it explicitly:
     `TYPES ty_tabl_text_cache_tt TYPE HASHED TABLE OF ty_tabl_text_cache
     WITH UNIQUE KEY tabname ddlanguage WITH NON-UNIQUE SORTED KEY
     by_tabname COMPONENTS tabname.` and `LOOP AT mt_tabl_text USING KEY
     by_tabname WHERE tabname = iv_tabname` in both consumers — turns the
     scan into a bounded binary-search-entry + contiguous-match walk.
  2. OR restructure the cache to mirror `mt_fugr_enlfdir`'s own shape:
     one row per `tabname`, holding a **nested** internal table of
     `(ddlanguage, ddtext)` rows, looked up via a single full-key `READ
     TABLE ... WITH TABLE KEY tabname = iv_tabname` (no WHERE-scan at
     all). This is the already-proven, already-reviewed pattern one
     provider over in the very same slice already uses correctly.
- Regression test: populate `mt_tabl_text` with a synthetic large N
  (e.g. 5,000+ distinct tabnames × 2 languages) and assert
  `get_tabl_i18n`'s call count/cost does not degrade with N once N is
  large relative to a single table's own language count — MEASURE_AT_IT8
  for the actual SAT-measured effect; the algorithmic shape itself is
  verifiable at design/code-review time without live measurement.

### PF-002 — MINOR — Package C (FUGR): missing explicit empty-driver guard on new TFDIR bulk SELECT

- Path: `serialization_slice_4_fugr_design.md` §6, `prepare_fugr`
  extension (`SELECT funcname, rfcscope, rfcvers FROM ('TFDIR') FOR ALL
  ENTRIES IN @lt_funcnames ...`).
- Observed shape: the pseudocode never declares/populates `lt_funcnames`
  nor guards it with `IF lt_funcnames IS INITIAL. RETURN/EXIT. ENDIF.`
  before the `FOR ALL ENTRIES` SELECT. Every sibling `FOR ALL ENTRIES`
  call in this same slice explicitly guards its driver table first:
  `prepare_tabl` (`IF it_names IS INITIAL. RETURN. ENDIF.`, confirmed in
  design and matches the existing `prepare_prog_langs`/`prepare_fugr`
  bodies read directly from current source, both of which guard
  `it_programs`/`it_areas` before their own bulk SELECTs).
- Why it matters: on current ABAP kernels an empty `FOR ALL ENTRIES`
  driver table is a documented no-op (no rows read, no full-table
  fallback), so this is not a correctness/performance hazard on any
  currently supported release — but the omission breaks the
  design's own internal consistency (every other bulk read in this
  slice is explicitly guarded) and removes a cheap, free early-exit for
  the common case of a batch whose function groups all resolved zero
  function modules this run.
- Fix: add `IF lt_funcnames IS INITIAL. RETURN. ENDIF.` (or fold the
  check into the existing `IF it_areas IS INITIAL. RETURN. ENDIF.`
  guard's structure) immediately before the new SELECT, matching the
  sibling convention.

### PF-003 — MINOR — oversized-single-object reliance not stated explicitly

- Path: `serialization_slice_4_fugr_design.md` §8 (and, by the same
  reasoning, TABL's §10/PROG's memory-bounds section).
- Observed shape: §8 says the *existing* `c_max_actual_batch_bytes`
  split-and-recurse admission check "is the actual safety bound" for a
  function group with an extreme function-module count. Direct source
  read of `zcl_abapgit_ortec_ser_orch.clas.abap` `before_dispatch`
  confirms the split condition is `IF lv_actual_bytes >
  c_max_actual_batch_bytes AND lines( it_object_keys ) > 1` — splitting
  is only possible for a *group* of more than one object; a single
  pathologically large FUGR (or, hypothetically, a TABL with an extreme
  language count) that alone exceeds the cap is dispatched as an
  unsplittable singleton batch regardless. The real safety net for that
  case is a **separate**, already-existing, pre-approved mechanism —
  the post-hoc `c_max_object_output_bytes`/`c_oversized_threshold`
  adaptive-shrink counters (confirmed in `zcl_abapgit_ortec_ser_orch
  .clas.abap`'s own constant doc comments) — not
  `c_max_actual_batch_bytes` itself.
- Why it matters: this is not a functional gap (the mechanism exists and
  applies uniformly to every object type, including the three new ones,
  with zero code change needed), but §8's wording could mislead an
  implementer into thinking `c_max_actual_batch_bytes` alone prevents an
  oversized single-object payload, when it only prevents oversized
  *multi-object* batches from being dispatched at all sizes.
- Fix: documentation-only — clarify that a pathological single object
  relies on the pre-existing post-hoc shrink-adaptation, not the
  pre-dispatch admission gate. MEASURE_AT_IT8 for whether a real
  repository ever produces a FUGR/TABL large enough to trigger it.

## Non-findings (checked, no issue)

- **Focus 1 (bulk SQL shape)**: TABL's `DD02T`/`TDDAT` `FOR ALL ENTRIES`
  reads and the guard against an empty `it_names` driver — CLEAN.
  FUGR's `TFDIR` addition — CLEAN except PF-002 (guard omission, MINOR).
  No unbounded/unfiltered SELECT found anywhere in the three designs.
  No N+1 SQL pattern (all bulk reads are single FOR-ALL-ENTRIES calls
  outside any per-object loop) — the one N+1-*shaped* defect found
  (PF-001) is a pure in-memory ABAP scan, not a repeated SQL statement.
- **Focus 2 (cache lifetime/cleanup)**: `clear_tabl_cache`/
  `clear_prog_cache`/`clear_fugr_cache` all exist and each clears
  exactly its own new CLASS-DATA, mirroring the existing
  `clear_dd_cache`/`clear_oo_cache` unconditional-clear-first RFC-worker
  pattern. No unbounded cross-invocation growth identified. One
  pre-existing, already-tracked checklist item (correctness gate
  CG-002) still applies: TABL's own worker-wiring section was never
  independently spot-checked against the real
  `z_abapgit_ortec_ser_batch` source in any TABL review cycle — carried
  forward here as a pre-implementation checklist item, not a new
  performance finding.
- **Focus 3 (byte/row bounds)**: PROG's `tpool_i18n` payload is
  confirmed, via direct read of `zif_abapgit_lang_definitions.intf.abap`
  and `prepare_prog_langs`'s actual body, to only ever populate the
  `language` field per row (the nested `textpool` field of
  `ty_i18n_tpool` is never filled by this cache) — bounded by language
  count, not by text-pool size, exactly as the design claims. FUGR's
  `enlfdir`/`func` payload scales with function-module count per group
  (design's own disclosed MEDIUM risk, §8) — see PF-003 for a
  documentation-only clarification, not a functional gap.
- **Focus 4 (actual-byte admission)**: shared_infrastructure.md §3's
  claim — "all provider buffers ARE already computed and forwarded to
  the RFC call, but only the DD buffer is currently summed into the
  admission check" — **independently re-confirmed true** against
  current source (`before_dispatch` computes `lv_prefetch_buffer_dd`/
  `_oo_batch`/`_msag`, sums only `xstrlen(lv_prefetch_buffer_dd)` into
  `lv_actual_bytes`; `dispatch_batch`'s RFC `CALL FUNCTION` genuinely
  forwards `iv_prefetch_buffer_oo_batch`/`iv_prefetch_buffer_msag` to the
  worker). This matches the correctness gate's own
  `CG-001=REJECTED_WITH_PROOF` disposition — no re-litigation needed.
  The proposed 6-way summation itself is performant: one `xstrlen(...)`
  per buffer, computed once per `before_dispatch` invocation (the
  existing recursive-split design already recomputes all provider
  buffers at every split level for the 3 existing providers — extending
  this to 6 buffers increases the constant factor per split but does not
  change the complexity class, and is consistent with pre-existing,
  already-accepted behavior). Aside from PF-001's cost hiding inside one
  of those six calls, no redundant `xstrlen` recomputation or redundant
  `extract_for_batch_*` call was found.
- **Focus 5 (copy/duplication count)**: all three new providers call
  `extract_for_object( ls_tadir )` once per `P`-state entry purely to
  measure `actual_bytes`, byte-for-byte the same pattern
  `zcl_abapgit_ortec_ser_pref_oo` (CLAS/INTF) and `zcl_abapgit_ortec_ser_
  pref` (MSAG) already use today (confirmed via direct source read of
  both classes' `extract_for_batch`) — no additional copy step beyond
  the accepted baseline shape (main cache → sizing EXPORT →
  combined-payload EXPORT → RFC transport → worker IMPORT → worker
  cache → consumed by object serializer).
- **Focus 6 (TABL's DD03P non-goal)**: confirmed genuinely
  performance-neutral. `DDIF_TABL_GET` (CONFIRMED_SOURCE) already
  returns `DD02V`/`DD09L`/`DD03P`/`DD05M`/`DD08V`/`DD12V`/`DD17V`/
  `DD35V`/`DD36M` in ONE function-module call per object — this call
  count does not change whether or not DD03P is ever batched, so leaving
  it on the standard path is a true no-op relative to today, not a
  deferred optimization opportunity that regresses anything.

## Summary

```text
sql_shape=CLEAN (all bulk reads properly FOR-ALL-ENTRIES, one guard
  omission PF-002)
byte_bounds=CLEAN except one algorithmic-complexity defect (PF-001,
  in-memory scan, not a byte-budget issue)
blockers=1 (PF-001)
majors=0
minors=2 (PF-002, PF-003)
```

## Note (correctness cross-reference, not scored here)

Package A's `extract_for_batch_tabl`/`inject_batch_from_buffer_tabl`
pseudocode references `mv_prepared` (`IF mv_prepared = abap_false.
RETURN.` / `mv_prepared = abap_true.`), but no `mv_prepared` CLASS-DATA
is declared anywhere in Package A's own §4 new-field list, and the
correctness-gate-verified current source of
`ZCL_ABAPGIT_ORTEC_SER_PREF_EXT` has only `mv_language` (Packages B/C
both independently confirmed this and switched to `mv_language IS
INITIAL` for exactly this reason). As written, Package A's guard would
not compile. This is a correctness/compilation defect, out of scope for
this performance gate's verdict, but it is called out here because it
sits directly at the same location as PF-001's required fix and should
be corrected in the same implementation pass.

## Cycle 2

```text
TASK=SER_SLICE_4_PERFORMANCE_DESIGN_GATE_CYCLE_2
BASELINE_HEAD=e5e10d62137033801739b8a53c69bdff9cfa650e
```

### Context read

- `.memory/logs/serialization_slice_4_tabl_ttyp_design.md` (revised, cycle 4)
- `.memory/logs/serialization_slice_4_fugr_design.md` (revised, cycle 4)
- `.memory/logs/serialization_slice_4_prog_design.md` (unchanged, was
  already clean — not re-reviewed in depth)
- `.memory/logs/serialization_slice_4_shared_infrastructure.md`
- This review's own Cycle 1 findings (above)
- `src/ortec/serial/zcl_abapgit_ortec_ser_pref_ext.clas.abap` — spot-check
  of the `mt_fugr_enlfdir` precedent shape (single hashed row per `area`,
  nested function-module list, full-key `READ TABLE ... WITH TABLE KEY`)

### PF-001 — CLOSED

`ty_tabl_text_cache_tt` is now `HASHED TABLE OF ty_tabl_text_cache WITH
UNIQUE KEY tabname` (single-field key), each row holding a nested
`texts TYPE ty_tabl_text_lang_tt` (`ddlanguage`/`ddtext` pairs) — this
now mirrors `mt_fugr_enlfdir`'s shape exactly, as PF-001's required fix
specified.

- `prepare_tabl` (§4 body) populates it correctly: one O(1) full-key
  `READ TABLE mt_tabl_text ASSIGNING FIELD-SYMBOL(<ls_text_cache>) WITH
  TABLE KEY tabname = ls_dd02t-tabname`, `INSERT ... ASSIGNING
  <ls_text_cache>` only on miss, then `APPEND ... TO <ls_text_cache>-
  texts` — a single linear pass over that call's own bulk-SELECT result,
  never a scan of the accumulating global cache.
- `get_tabl_i18n` (§5): contract explicitly documents "ONE full-key
  `READ TABLE ... WITH TABLE KEY tabname = iv_tabname` (O(1), PF-001
  fix)" and builds `et_i18n_langs`/`et_dd02_texts` from the found row's
  nested `texts` table.
- `extract_for_batch_tabl` (§6): body confirmed literally —
  `READ TABLE mt_tabl_text INTO DATA(ls_text_cache) WITH TABLE KEY
  tabname = lv_tabname` followed by `LOOP AT ls_text_cache-texts INTO
  DATA(ls_lang)` — a loop over the found row's own small nested table,
  not the global cache. Guarded by `IF sy-subrc = 0` before use.
- No remaining `LOOP AT mt_tabl_text ... WHERE tabname = ...`
  partial-key scan anywhere in the design (confirmed by full-file
  search — every remaining `mt_tabl_text` access is either the §4
  full-key insert/append or the §5/§6 full-key reads above).

The O(N_tabl²) shape identified in Cycle 1 is eliminated; the cache is
now genuinely O(1) per lookup regardless of how many TABL objects
`PREPARE` accumulated across the whole run.

### PF-002 — CLOSED

`fugr_design.md` §6 now declares `DATA lt_funcnames TYPE STANDARD TABLE
OF rs38l_fnam WITH DEFAULT KEY.`, populates it from `mt_fugr_func_meta`
(`lt_funcnames = VALUE #( FOR ls_meta IN mt_fugr_func_meta ( ls_meta-
funcname ) )`), and guards `IF lt_funcnames IS INITIAL. RETURN. ENDIF.`
immediately before the `FOR ALL ENTRIES IN @lt_funcnames` TFDIR SELECT —
matching the sibling-guard convention this slice already establishes
elsewhere (`prepare_tabl`'s `it_names` guard, `prepare_prog_langs`'s
`it_programs` guard).

### PF-003 — CLOSED

`fugr_design.md` §8 now reads: the `c_max_actual_batch_bytes`
split-and-recurse check "is the safety bound ONLY for a MULTI-object
batch" (citing the real `IF lv_actual_bytes > c_max_actual_batch_bytes
AND lines( it_object_keys ) > 1` condition), and explicitly attributes
the single-oversized-FUGR safety net to "the SEPARATE, already-existing,
pre-approved post-hoc adaptive-shrink mechanism
(`c_max_object_output_bytes`/oversized-result handling in
`ZCL_ABAPGIT_ORTEC_SER_ORCH`, unchanged, applies uniformly ... with zero
code change needed) — NOT `c_max_actual_batch_bytes` itself, which this
section's cycle-1/2/3 wording could be misread as implying." This
correctly resolves the ambiguity flagged in Cycle 1.

### Fresh pass — new defect check

Specifically checked `extract_for_batch_tabl`'s restructured lookup for
a stale-field-symbol/leaked-reference risk across a loop mixing
found/not-found `tabname` values (the nested-cache restructure changes
how rows are read, so this was worth re-verifying independently of
PF-001 itself):

- Both `READ TABLE mt_tabl_text INTO DATA(ls_text_cache) WITH TABLE KEY
  ...` and `READ TABLE mt_tabl_extras INTO DATA(ls_extras) WITH TABLE
  KEY ...` use `INTO` (a fresh data-object copy per outer-loop
  iteration), not `ASSIGNING FIELD-SYMBOL` — there is no field-symbol to
  go stale or dangle across iterations here.
- Both reads are followed by an explicit `IF sy-subrc = 0` (or
  equivalent) check before the copied work area's content is ever used
  or appended anywhere — a not-found `tabname` cannot silently reuse a
  previous iteration's leftover `ls_text_cache`/`ls_extras` content,
  because the `IF sy-subrc <> 0` branch never reads that variable at
  all (it takes the separate `ls_entry-state = 'M'` / no-append path).
- No new performance or correctness defect found in this pass.

### Cycle 2 verdict

**APPROVE**

All three Cycle 1 findings are closed with no regressions and no new
defects introduced by the fixes themselves. No outstanding blockers or
majors remain. PF-001's originally-flagged compilation-defect adjacent
note (`mv_prepared` non-existent field) is also independently confirmed
fixed in the same §6 diff (`IF mv_language IS INITIAL` now used, per
the design doc's own inline fix-comment) — resolved as a side effect of
the same edit, though it was scored as correctness-scope, not
performance-scope, in Cycle 1.

```text
pf001=CLOSED pf002=CLOSED pf003=CLOSED
new_findings=0
```
