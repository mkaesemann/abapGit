# Performance IMPLEMENTATION_AUDIT — Variant B, Package B checkpoint B1

## Final gate closure

- Original verdict: REVISE_AND_REVIEW_ONCE
- Finding PF-B1-001: RETRACTED
- Resolution basis:
  - unconditional missing-key validation in GET_OBJECTS;
  - pre-existing GET_OBJECTS_MISSING test;
  - owner-executed IT8 VERIFY_CLOSURE_* tests: PASS
- Remaining blocking findings: none
- Final verdict: APPROVE
- Gate status: CLOSED
 
## ORCHESTRATOR REBUTTAL (post-review, overrides Finding 1 below)

The subagent's sole `BLOCKING` finding ("Finding 1") is **factually
incorrect** and is rejected after direct, repeated re-reads of the current
source. `zcl_abapgit_ortec_obj_store=>get_objects` (lines 341-427 of
`src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap` at the time of this
audit) contains an **unconditional** final check, reached after BOTH the
`iv_bulk_fetch = abap_true` and `iv_bulk_fetch = abap_false` branches
(there is no `RETURN` between the branch and this check):

```abap
LOOP AT lt_unique_sha1s ASSIGNING <lv_sha1>.
  READ TABLE lt_found_sha1s WITH TABLE KEY table_line = <lv_sha1> TRANSPORTING NO FIELDS.
  IF sy-subrc <> 0.
    zcx_abapgit_ortec_git=>raise( |Object { <lv_sha1> } not found in store| ).
  ENDIF.
ENDLOOP.
```

This means `get_objects` raises `zcx_abapgit_ortec_git` for ANY requested
SHA1 absent from the store, for BOTH `iv_bulk_fetch` values - the subagent's
claim that "a missing/absent tree below the root silently drops out instead
of raising" and that there is "no exception, no `sy-subrc` check anywhere in
the call chain back up to `verify_tree_closure`" is false; the check is
right there in `get_objects` itself. Corroborating evidence: `iv_bulk_fetch`
defaults to `abap_false`
([declaration](src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap#L77)),
and the pre-existing, previously SAP-validated `get_objects_missing` test in
`zcl_abapgit_ortec_git_tests.clas.testclasses.abap` (`ltcl_obj_store`)
already exercises this exact `abap_false` + missing-SHA1 path today and
asserts a raise - proving this behavior is not new or fragile. Therefore
`verify_tree_closure`'s `WHILE` loop correctly relies on `get_objects`
raising before `LOOP AT lt_tree_objects` is ever reached with an incomplete
result, and the new `verify_closure_missing_tree` test is correct as
written; no code change is required. **B1 verdict after rebuttal:
APPROVE** (0 confirmed blocking, 0 confirmed major - Finding 1 rejected,
all other audit items already confirmed correct by the subagent itself).

No second audit pass was requested for this specific point (re-running the
same static/textual analysis would not resolve a source-comprehension
error); the rebuttal is evidenced by exact, quoted, current source. A real
ABAP Unit run of the three `verify_closure_*` tests during owner-executed
IT8 validation remains the authoritative confirmation, per the existing SAP
validation boundary for this checkpoint.

## Original subagent report (Finding 1 rejected above, other items stand)

- Mode: `IMPLEMENTATION_AUDIT`
- Task: `VB-B-B1-PERF-AUDIT`
- Baseline: `cbe8bc73f7c1e17e3d436b1371fcc65f806dd989` (last SAP-validated HEAD)
- Change scope verified via `git status`/`git diff` (not assumed from the
  handoff): exactly the four stated files, plus unrelated `.memory` log/
  review files. No other productive source touched.
  - New (untracked): `src/ortec/git/zcl_abapgit_ortec_cold_init.clas.abap`,
    `.clas.testclasses.abap`, `.clas.xml`.
  - Modified: `src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap` (+112/-0,
    purely additive — new `verify_tree_closure` declaration + method body,
    no other lines touched).
  - Modified: `src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap`
    (+115/-0, purely additive — 3 new test methods only).

## Evidence used

1. Current-source call-chain read of
   [zcl_abapgit_ortec_cold_init.clas.abap](src/ortec/git/zcl_abapgit_ortec_cold_init.clas.abap)
   (full method), `verify_tree_closure` and `get_objects`/`read_object_rows`
   in
   [zcl_abapgit_ortec_obj_store.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap#L668-L734)
   and
   [zcl_abapgit_ortec_obj_store.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap#L689-L410).
2. Prior evidence taken as given and spot-verified against current source
   (not re-derived): `.memory/reviews/performance_design_variant_b_package_b.md`
   (both DESIGN_GATE passes, `APPROVE_WITH_MINOR_REVISIONS`),
   `.memory/reviews/perf_scan_variant_b_b1.md` (static scan, `PASS`),
   `.memory/logs/variant_b_package_b_design.md` §6/§10/§12.
3. `git diff`/`git status` against baseline for regression scope (§ above).
4. Full read of
   [zcl_abapgit_ortec_cold_init.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_cold_init.clas.testclasses.abap)
   and the 3 new methods in
   [zcl_abapgit_ortec_git_tests.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap#L291-L406).
5. No live SAP execution performed in this audit (matches the static scan's
   own disclosed evidence limit) — see "Unexecuted scenarios" below.

## Audit-focus items 1, 3, 4 — confirmed correct, no findings

**Item 1 — call-chain ordering in `acquire_blobless_graph`**: confirmed
exact sequence `reset_completion_budget()` → `begin_attempt()` →
info/refs GET → `build_request(INITIAL_BRANCH_BLOBLESS)` →
`send_receive_close()` → **memory-risk gate on the raw response XSTRING**
(`xstrlen( lv_response ) > c_max_graph_response_bytes`, 209,715,200) →
`zcl_abapgit_ortec_fastpath=>parse()` → pack-empty guard →
`decode_streaming()` → `verify_tree_closure()` → `mark_graph_complete()` →
one `COMMIT WORK`. The gate runs before `parse`/`decode_streaming` on the
already-materialized response, and the certificate publish
(`mark_graph_complete` + `COMMIT WORK`) is the last two statements, after
`verify_tree_closure` returns without exception. No reordering defect.
INV-B-12 confirmed correctly implemented, matching the static scan.

**Item 3 — `reset_completion_budget()` usage**: called exactly once, at the
top of `acquire_blobless_graph`, before any HTTP call — correct top-level
attempt-boundary usage (the method sets one `CLASS-DATA` counter to 0,
[zcl_abapgit_ortec_pack_stream.clas.abap](src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.abap#L303)).
Grep for `acquire_blobless_graph`/`ZCL_ABAPGIT_ORTEC_COLD_INIT` across
`src/**` confirms zero productive callers exist yet (only the new class's
own implementation and its test class reference it) — no concurrent
top-level caller exists today, so no conflict is possible. Matches the
design's stated "no productive caller in Package B" claim.

**Item 4 — no regression to `GET_REACHABLE_OBJECTS`/`GET_REACHABLE_SHA1S`/
`GET_MISSING_SHA1S`**: confirmed byte-for-byte unchanged. `git diff` against
baseline shows the entire `obj_store` diff is one contiguous insertion (new
declaration block + new `verify_tree_closure` method body); no line inside
any pre-existing method was touched.

## Item 2 — VERIFY_TREE_CLOSURE DB access shape: chunking confirmed, but a genuine completeness gap found (BLOCKING)

**Chunking/dedup — confirmed correct.** Both the root-commit read and every
frontier iteration call `get_objects( ..., iv_bulk_fetch = abap_false )`
([zcl_abapgit_ortec_obj_store.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap#L675)
and
[zcl_abapgit_ortec_obj_store.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap#L698)),
which is genuinely chunked at `c_select_package_size` (1000) — confirmed by
reading `get_objects`'s `abap_false` branch
([zcl_abapgit_ortec_obj_store.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap#L379-L395)),
not merely declared. `populate_cache` is never called. The `WHILE
lt_current_trees IS NOT INITIAL` frontier loop terminates (bounded by the
finite tree structure) and cannot revisit an already-seen tree — dedup is
via `lt_seen_trees TYPE ty_sha1_set`, confirmed `HASHED TABLE ... WITH
UNIQUE KEY`
([zcl_abapgit_ortec_obj_store.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap#L218)).

### Finding 1 — BLOCKING: missing/omitted trees are silently accepted, so `verify_tree_closure` does not actually verify closure completeness beyond the root commit

- **Path/method**: `zcl_abapgit_ortec_obj_store=>verify_tree_closure`, the
  `WHILE lt_current_trees IS NOT INITIAL` loop
  ([zcl_abapgit_ortec_obj_store.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap#L692-L733)).
- **Observed call shape**: `get_objects(..., iv_bulk_fetch = abap_false)` →
  `read_object_rows` issues a plain `SELECT * ... INTO TABLE rt_rows WHERE
  obj_sha1 IN lr_sha1s` with no row-count check
  ([zcl_abapgit_ortec_obj_store.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap#L869-L887)).
  When a requested SHA1 is not present in the store, it is simply absent
  from the result table — no exception, no `sy-subrc` check anywhere in the
  call chain back up to `verify_tree_closure`. The frontier loop only
  iterates over `lt_tree_objects` (what was **found**), never compares that
  count against `lt_current_trees` (what was **requested**). Only the
  single root-commit read is guarded (`READ TABLE ... INDEX 1 IF sy-subrc
  <> 0 ... raise`); every subsequent frontier level has no equivalent
  guard.
- **Concrete trace confirming the defect**: the new
  `verify_closure_missing_tree` test
  ([zcl_abapgit_ortec_git_tests.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap#L334-L358))
  stores a commit whose `tree` field points at SHA1
  `7777777777777777777777777777777777777777`, which is **never stored**,
  and expects `verify_tree_closure` to raise. Tracing the actual code: the
  root-commit read succeeds (the commit itself is stored and type-correct),
  so `ls_commit-tree` is extracted and `lt_current_trees =
  ['7777...']`. The `WHILE` loop's `get_objects` call returns an **empty**
  `lt_tree_objects` (the tree is absent). `LOOP AT lt_tree_objects`
  executes zero times — no raise fires. `lt_next_trees` stays empty
  (cleared, never appended to). `lt_current_trees = lt_next_trees` is now
  empty, the `WHILE` condition is false, and the method **returns
  normally**. The test's `cl_abap_unit_assert=>fail( 'Missing reachable
  tree must raise' )` would be reached — this test will fail on real
  execution. It has clearly never been run against a live system yet (no
  SAP validation evidence exists for B1 in `.memory/state.md`, consistent
  with this).
- **Expected production cardinality / why it matters**: this is not a
  call-count or byte-budget defect — it is a verification-completeness
  defect with the same net effect the DESIGN_GATE's ordering requirement
  exists to prevent. `ACQUIRE_BLOBLESS_GRAPH` calls `verify_tree_closure`
  specifically so that `mark_graph_complete`/`COMMIT WORK` only ever
  publish `GRAPH_COMPLETE` for a genuinely present commit→tree closure
  (binding Package B constraint: "Graph and snapshot certificates must
  only be published after verification"; `git-partial-clone` skill:
  `GRAPH_COMPLETE` = "requested commit and **complete tree closure** are
  present"). As implemented, any tree missing below the root — e.g. from a
  truncated/corrupted decode, a partial persistence failure that
  `decode_streaming` didn't itself catch, or any other reason a child tree
  ends up absent — is silently treated as "no children to walk further"
  rather than "closure incomplete," and the certificate is published
  anyway. Because Package B's entire performance premise is that
  downstream consumers can trust `GRAPH_COMPLETE`/`SNAPSHOT_COMPLETE`
  certificates instead of re-scanning or re-verifying at repository scale,
  a false-positive certificate here defeats that premise for exactly the
  paths this design exists to make cheap.
- **Estimated/measured SQL/HTTP/memory impact**: none directly (no extra
  calls) — the defect is a missing comparison, not an extra loop or
  round-trip.
- **Required fix**: after each `get_objects` call inside the `WHILE` loop,
  build a found-SHA1 hashed set from `lt_tree_objects` and diff it against
  the requested `lt_current_trees`; raise `zcx_abapgit_ortec_git` naming
  the first (or all) missing SHA1(s) before proceeding to decode/walk
  further. This is a narrow, single-method, mechanical fix — it does not
  require touching the chunking, dedup, or batching shape, all of which are
  already correct.
- **Regression test/measurement**: `verify_closure_missing_tree` already
  exists and correctly pins the required behavior — use it, unmodified, to
  confirm the fix once ABAP Unit can be executed against this object on a
  real system. No new test is required.

## Unexecuted scenarios (flagged, not converted to measured results)

- Small (1–20 objects): only the 3 new unit tests exist; not yet run on a
  real system (no SAP validation evidence for B1 in `.memory/state.md`).
- Medium (≥5,000 mixed objects, multiple batches): not exercised. No
  synthetic fixture exists yet for a wide/deep tree frontier at B1.
- Large (≥40,000 objects, cold/warm cache): not exercised.
- Interrupted attempt and retry: not exercised (`acquire_blobless_graph` has
  no productive caller yet, per item 3).
- These gaps are expected at this checkpoint (B1 has no productive caller
  yet, per design §1) and are not, by themselves, a basis for
  `BLOCK_PRODUCTION_SCALE` — they are listed per the mandatory-scenario
  reporting requirement, not treated as failures.

## Verdict

**REVISE_AND_REVIEW_ONCE**

Rationale: items 1, 3, and 4 are fully confirmed correct against current
source with no findings. Item 2's batching/dedup/termination shape is also
confirmed correct. One BLOCKING finding remains: `verify_tree_closure`'s
frontier walk does not detect a missing/absent tree below the root commit,
so it can allow `GRAPH_COMPLETE` to be published for an incomplete graph —
a real regression against the method's stated purpose, caught by the
method's own new `verify_closure_missing_tree` test, which would fail if
run today. This is a narrow, single-method fix (add a found-vs-requested
SHA1 diff inside the existing loop) that does not require redesigning the
walk, its chunking, or its HTTP/memory-gate architecture — one revision
pass addressing Finding 1, followed by an actual ABAP Unit run of the three
new `verify_closure_*` tests, should be sufficient to close this audit.
