# Performance scan — Variant B Package E, checkpoint 1

```text
PACKET=COMPACT_HANDOFF_V1
TASK=E_CHECKPOINT_1_PERFORMANCE_SCAN
BASELINE=b6f019de
SCOPE=E1-TEST, E3-TEST, E4-VERIFY, E-HARDEN (OF-2 constant extraction only)
STATUS=PASS, no production-scale runtime change
```

## Production code (OF-2)

Pure constant substitution, zero runtime-cost difference from the prior
literal-string implementation:

- `zcl_abapgit_ortec_git_switch.clas.abap`: one new `CONSTANTS` declaration
  (compile-time, no runtime cost at all).
- `zcl_abapgit_ortec_porcelain.clas.abap`: 4 raise-site string templates
  and 1 `CS` operand changed from a literal to `|{ class=>constant }
  suffix|` / `class=>constant`. String-template construction cost is
  identical in shape to the prior literal concatenation the surrounding
  code already did elsewhere in the same file (e.g. `|walk_tree: unknown
  chmod { <ls_node>-chmod }|`); no new loop, no new DB access, no new HTTP
  call. These raise sites are exception/error paths only, never on the
  hot/success path of `walk()`/`pull_by_branch()`.
- No change to `walk_tree()`, `pull()`, `materialize_from_manifest()`, or
  any bulk/loop logic — the existing PREWARM/`fetch_blobs_bulk` batching
  behavior (already reviewed and approved in prior Package E/B/C
  checkpoints) is untouched.

## Test code

- `index_chunk_boundary_ok` (E1-T-04): builds 1200 blob+node pairs and
  persists them via a SINGLE `zcl_abapgit_ortec_obj_store=>store_objects`
  bulk call (not a per-row loop), per the project's documented per-row-
  DB-loop performance lesson. No new per-row loop pattern introduced.
- All other new tests operate on small (1-2 commit, 1-2 row) fixtures —
  no scale concern.
- Test code only runs under ABAP Unit, never in a production request
  path — even if it were unoptimized, it would not affect production-scale
  behavior. Included here only for completeness.

## Scope boundary confirmed

- E1-PERF (raising the `rebuild_index` chunk size from the bare literal
  1000 to a named 5000 constant) is explicitly OUT OF SCOPE for this
  checkpoint (design doc marks it `AUTHORIZED_NOW` as a separate slice,
  not requested in this checkpoint's mandate) — the bare `1000` literal
  in `zcl_abapgit_ortec_obj_index`'s `rebuild_index` was left unchanged.
  `index_chunk_boundary_ok` deliberately asserts against `lc_file_count`
  (1200), never hardcoding `1000`, so it remains valid once E1-PERF later
  changes the chunk size.
- No DDIC change, no new table, no new index — no schema-level performance
  surface introduced.

## Pre-import corrective audit (2026-07-29) — re-scanned

Two source-level corrections were made after the original scan; both
re-confirmed as zero production-scale impact:

- `zcl_abapgit_ortec_cache_admin.clas.testclasses.abap` /
  `f4_dedup_prefers_state`: replaced `FILTER #( lt_values WHERE repo_key =
  c_repo )` (a real compile error on the keyless standard table type, not
  a performance issue) with `LOOP AT lt_values TRANSPORTING NO FIELDS
  WHERE repo_key = c_repo` (count) + `READ TABLE ... WITH KEY`. The
  fixture is test-only, at most 2 repo keys — a linear scan is
  performance-irrelevant at this scale, and this is test code that never
  runs in a production request path regardless.
- `zcl_abapgit_ortec_obj_index.clas.testclasses.abap` /
  `build_commit`: added `zcx_abapgit_exception` to the `RAISING` clause.
  A `RAISING` declaration is a compile-time checked-exception contract
  with zero runtime cost — no behavior or performance change.
- 3 placeholder test methods removed, 2 test assertions changed from
  wildcard to exact-equality comparisons (`assert_char_cp` →
  `assert_equals`) — both are O(1) string comparisons on short fixed
  literals; no measurable cost difference either way.

No production file (`zcl_abapgit_ortec_git_switch.clas.abap`,
`zcl_abapgit_ortec_porcelain.clas.abap`) was touched by this corrective
audit — the original scan's PASS verdict for production code stands
unchanged.

## Verdict

```text
PERFORMANCE_SCAN=PASS
BLOCKING_FINDINGS=0
DESIGN_GATE_REQUIRED=NO (no new production-scale loop/DB/HTTP pattern
  introduced by this checkpoint's authorized scope, nor by the 2026-07-29
  corrective audit)
NEXT=none — E1-PERF remains a separate, not-yet-requested slice.
```
