# OBJ_PERF_FINAL — Implementation performance audit (OBJ-PERF-AUDIT-1)

Mode: IMPLEMENTATION_AUDIT
Baseline: `4193733de3c8ad75bae61f6ceb95b1c3e0ccb3e4` vs current working tree
(branch `ortec/abapgit_1_133-opt-rework`, HEAD `d0d7f3eb` + one uncommitted
working-tree change to `zcl_abapgit_ortec_obj_index.clas.abap` — the PS-001
fix; see §0).

Scope: full active call chain `get_files_for_filter -> ensure_filtered_coverage
-> (warm-complete | warm-coverage | walk_filtered) -> select_rows_for_filter /
select_partial_rows_for_filter -> build_files_from_rows`, plus
`invalidate_commit_index`, `zcl_abapgit_ortec_obj_cover` (`get_coverage`,
`write_coverage`, `compute_context_hash`), `zcl_abapgit_ortec_cache_admin
=>clear_repo`, `zcl_abapgit_ortec_filter_walk=>get_remote_files_for_stage`,
and the `ZAOG_OBJ_COVER`/`ZAOG_OBJ_PIDX`/`ZAOG_OBJ_INDEX` DDIC definitions.
Evidence: current-source call-chain analysis and direct comparison against
the approved design (`obj_index_partial_design.md`) and its adversarial
closure list (AR-1-xx/AR-2-xx). No live SAP execution, SAT/ST05 trace, or
synthetic large-fixture run was performed this pass — all cardinality
statements below are static-analysis estimates, not measurements.

## 0. PS-001 fix verification

**Verified correct and complete.** The fix (currently an uncommitted
working-tree change — `git status` shows `M
src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap`, not yet in a commit)
replaces the flagged `line_exists( it_filter[ ... ] )` linear membership
test inside `walk_filtered`'s per-node loop with:

- a `HASHED TABLE ... WITH UNIQUE KEY obj_type obj_name` (`lt_filter_set`,
  reusing the existing `ty_match_set` type already used for `lt_matched`),
  built via a single `LOOP AT it_filter ... INSERT ... INTO TABLE
  lt_filter_set` **once**, before `acquire_repo_lock` and before the BFS
  loop begins;
- the per-node check itself now does `READ TABLE lt_filter_set
  TRANSPORTING NO FIELDS WITH TABLE KEY obj_type = ... obj_name = ...` —
  O(1) hashed lookup, not a scan of `it_filter`.

This matches the required fix exactly: O(1) per node, built exactly once
per `walk_filtered` call (not rebuilt per node or per BFS level/chunk).
`PS-001` is closed. **This fix is not yet committed** — flagging so the
orchestrator does not lose it; it must land in the same slice as any
follow-up fix below.

## 1–9. Required-audit walkthrough

1. **Cold init → filter chain, SQL/HTTP counts.** For K=1 and K=100–250
   cold: 1 commit `get_objects` (bulk) + O(L) tree-level `get_objects`
   calls (L = BFS depth, unchanged from `rebuild_index`) + 1 `get_coverage`
   (single chunk, 0/K rows) + ≤1 `write_coverage` (single chunk) + ≤1
   `ZAOG_OBJ_PIDX` write chunk. Matches design. For K near F (≈40,000):
   `get_coverage`/`write_coverage`/`ZAOG_OBJ_PIDX` writes correctly become
   `ceil(K/5000)` bounded chunks (verified in `zcl_abapgit_ortec_obj_cover`
   and in `walk_filtered`'s `lt_pidx_rows` chunk-flush at
   `c_filter_chunk_size`). **Exception found**: when K is large *and* the
   COMPLETE-mode index is already warm (`is_index_ready = true`), the read
   goes through `select_rows_for_filter`, which is **not** chunked — see
   PA-001. Warm repeated request (coverage fully FOUND/NO_FILES/NOT_
   PRESENT_REMOTE): confirmed zero-walk, `get_coverage` + one
   `select_partial_rows_for_filter` chunked read only — matches design.

2. **Small K does not scan/write all F in `ZAOG_OBJ_INDEX`.** Confirmed.
   `ensure_filtered_coverage`'s uncovered-object path calls `walk_filtered`,
   which writes exclusively to `ZAOG_OBJ_PIDX` (never `ZAOG_OBJ_INDEX`,
   never the `$IDX/__READY__` marker). `ZAOG_OBJ_INDEX` is only written by
   `rebuild_index`, reached solely via `ensure_index` (Step 1's warm check)
   or the `get_files_for_filter` corrupt-data catch fallback (full
   COMPLETE rebuild on `CORRUPT_OR_INCOMPLETE`, an existing, unchanged,
   exceptional recovery path — not the normal small-K flow).

3. **No SQL/HTTP per filter/object/tree node.** Confirmed for the walk
   itself: `get_objects` is called once per BFS frontier level (bulk, list
   of tree SHA1s), identical to `rebuild_index`. `ZAOG_OBJ_PIDX` writes are
   chunked at `c_filter_chunk_size` (5000); `ZAOG_OBJ_INDEX` writes
   (COMPLETE mode) remain chunked at `c_index_write_chunk_size` (30000).
   HTTP: the only network path is `build_files_from_rows`'s existing
   best-effort bulk `ensure_available` top-up (all missing blobs in one
   call), unchanged. **Exception**: `select_rows_for_filter`'s single
   unchunked `FOR ALL ENTRIES` (PA-001) is not a per-node/per-object call,
   but it is an unbounded-size single statement contradicting the
   program's own stated invariant that every filter-keyed SQL statement in
   this class chunks at `c_filter_chunk_size`.

4. **Presence checks avoid payloads; `ZAOG_OBJ_STORE` untouched by
   coverage.** Confirmed by direct grep of
   `zcl_abapgit_ortec_obj_cover.clas.abap`: the only two matches for
   `obj_store` are type references
   (`zcl_abapgit_ortec_obj_store=>ty_repo_key`) in method signatures —
   `get_coverage`/`write_coverage`/`compute_context_hash` never SELECT,
   MODIFY, or read `ZAOG_OBJ_STORE`, and never load a blob payload.

5. **Duplicate keys / redundant decode.** `walk_filtered`'s BFS reuses the
   identical `lt_seen_trees` dedup-before-enqueue pattern as
   `rebuild_index` (unchanged), so a shared subtree is fetched/decoded once
   per walk, not once per path reaching it. No new duplicate-decode path
   was introduced by this program.

6. **Negative results never derive from a missing row alone.** Confirmed.
   `RESOLVED_NO_FILES`/`RESOLVED_NOT_PRESENT_REMOTE` are written only
   inside `walk_filtered`, after a real completed BFS walk, based on
   whether the object's key is present in `lt_matched` (built from actual
   tree-node matches found during that walk) — never inferred from the
   mere absence of a `ZAOG_OBJ_PIDX`/`ZAOG_OBJ_COVER` row. The stronger
   `RESOLVED_NOT_PRESENT_REMOTE` additionally requires both
   `is_graph_have_eligible` and `iv_commit = iv_current_remote` (W8/§11.4
   gate), correctly implemented.

7. **Memory bounds.** `walk_filtered`'s `lt_pidx_rows` write buffer flushes
   at `c_filter_chunk_size` = 5000, strictly below COMPLETE-mode's 30000
   (`c_index_write_chunk_size`) peak, as designed. `get_coverage`/
   `write_coverage`/`select_partial_rows_for_filter` all chunk at the same
   5000 constant. **Exception**: `select_rows_for_filter`'s read has no
   row cap — for a large warm-COMPLETE request (K near F) this builds one
   unbounded in-memory result table and issues one unbounded-size SQL
   statement (PA-001).

8. **Output parity (FILTERED vs. COMPLETE reader).** **PROVEN.**
   `select_partial_rows_for_filter`'s manual projection into
   `ty_index_rows_tt`/`zaog_obj_index` shape covers every non-key,
   non-client column `ZAOG_OBJ_INDEX` has (`file_path`, `file_name`,
   `blob_sha1`, `tree_sha1`, `idx_status`, `context_hash`) plus the key
   columns `select_rows_for_filter`'s `SELECT *` also returns
   (`repo_key`, `commit_sha1`, `obj_type`, `obj_name`, `path_hash`) — a
   field-by-field comparison against the DDIC XML for both tables shows no
   dropped/renamed field, and `build_files_from_rows` only ever reads
   `blob_sha1`/`file_path`/`file_name` from either source, so both readers
   feed it identically.

9. **PS-001 fix** — see §0. Verified correct.

## Findings

### PA-001
- Severity: **MAJOR**
- Path and method: [src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap) / `ZCL_ABAPGIT_ORTEC_OBJ_INDEX` / `select_rows_for_filter`
- Observed call shape: a single, unchunked `SELECT * FROM zaog_obj_index ... FOR ALL ENTRIES IN it_filter WHERE repo_key = ... AND commit_sha1 = ... AND idx_status = c_status_ready AND obj_type = it_filter-object AND obj_name = it_filter-obj_name.` — no loop, no `c_filter_chunk_size` boundary, and **no `context_hash` predicate at all** despite the method now taking `iv_context_hash` as a mandatory importing parameter.
- Expected production cardinality: this is the method reached by `ensure_filtered_coverage`'s Step 1 (`is_index_ready = true` warm-COMPLETE fast path) on **every** filtered request once a repository has a fully-built COMPLETE index — the most common steady-state case, including full-Stage requests where `it_filter` can approach F (tens of thousands of TADIR entries).
- Estimated or measured SQL calls: 1 (static estimate) unbounded-size statement instead of `ceil(K/5000)` bounded statements.
- Estimated or measured HTTP calls: 0 (unaffected).
- Estimated or measured memory impact: one in-memory `it_filter`-sized `FOR ALL ENTRIES` operand plus an unbounded `rt_rows` result table held entirely in memory before returning — no chunk-boundary cap, unlike every sibling filter-keyed SQL statement in this program (`get_coverage`, `write_coverage`, `select_partial_rows_for_filter`, `rebuild_index`'s write path).
- Why it matters: this is a confirmed regression against the program's own approved, adversarially-reviewed design. `obj_index_partial_design.md` explicitly logs this as **AR-1-04 (MAJOR)**, states the closure as "a shared `c_filter_chunk_size = 5000` constant bounds `get_coverage`, `write_coverage`, and (now context-aware) `select_rows_for_filter` — every SQL statement in this program keyed by a caller-supplied filter chunks at this one named constant," and the W2 work-order for this exact method reads: *"select_rows_for_filter gains the same new mandatory IMPORTING iv_context_hash. Its body changes from one unchunked FOR ALL ENTRIES to a loop over it_filter in chunks of `c_filter_chunk_size` (AR-1-04), each chunk's SELECT gaining `AND context_hash = iv_context_hash`, results APPENDED across chunks."* Neither the chunking loop nor the `context_hash` predicate was implemented — only the signature parameter was added and left unused. The design-mandated regression test (`select_rows_chunk_boundary`, >5000 filter rows) is correspondingly absent from the testclasses file (confirmed via grep — only `select_partial_rows_chunk_boundary`, for the *new* PIDX reader, exists). This was missed by the earlier static scan (PS-001 was the only finding reported) and by all "no output" subagent-gap-fill verification passes recorded in `obj_index_partial_implementation.md`.
- Required fix: implement the W2 work order literally as designed — chunk `it_filter` at `zcl_abapgit_ortec_obj_cover=>c_filter_chunk_size` inside `select_rows_for_filter` (same `LOOP ... APPEND ... IF lines(...) >= c_filter_chunk_size ... SELECT ... APPENDING TABLE ...` idiom already used in `get_coverage`/`select_partial_rows_for_filter`), and add `AND context_hash = iv_context_hash` to each chunk's WHERE clause.
- Regression test or measurement: add the design-mandated `select_rows_chunk_boundary` test (>5000 `it_filter` rows, asserts all rows returned across multiple chunk statements) and `ready_rejects_different_context`/`select_rows_excludes_other_context`-style coverage confirming the new `context_hash` predicate is live (the existing tests of those names, per the implementation log, currently validate `is_index_ready` and end-to-end purge behavior, not this method's own predicate directly — verify they actually exercise `select_rows_for_filter`'s WHERE clause once the fix lands).

### PA-002 (informational, non-blocking for this performance audit)
- Severity: MINOR (correctness-adjacent, not itself a performance defect)
- Path and method: same as PA-001.
- Observation: the missing `context_hash` predicate is not currently known to be exploitable, because `invalidate_commit_index`/`rebuild_index` purge all contexts' `ZAOG_OBJ_INDEX` rows for a commit before writing a new context's rows (AR-2-01's stated invariant), so only one context's rows can physically coexist per commit today. This finding is noted only because it was directly evidenced while verifying PA-001's identical, un-implemented W2 work order — route it to the design/correctness reviewer alongside the PA-001 fix rather than treating it as closed by that reviewer's own record, since the source no longer matches what AR-1-01/W2 describe as done.
- Required action: fix alongside PA-001 (same method, same W2 work order); no separate performance regression test needed beyond PA-001's.

## Mandatory scale scenarios

- Small (1–20 objects): not executed live; static call-chain trace only — no per-object SQL/HTTP found.
- Medium (≥5,000 mixed objects, multiple batches): not executed live; static trace confirms `get_coverage`/`write_coverage`/`ZAOG_OBJ_PIDX` chunk correctly at the 5000 boundary, but `select_rows_for_filter`'s warm-COMPLETE path would NOT chunk at this cardinality (PA-001).
- Large (≥40,000 objects/paths, cold/warm cache): not executed live. Cold path matches design (O(L) SQL calls, chunked writes). Warm-COMPLETE path is exposed to PA-001 at this cardinality.
- Shared branches (95–98% shared): not executed live; no change in this program's scope affects tree/blob sharing behavior.
- Incremental store (~100 affected objects, ~1,000,000 stored keys): not executed live; all new predicates remain primary-key-exact (repo_key/commit_sha1/context_hash/obj_type/obj_name), no repository-wide scan introduced.
- Interrupted attempt and retry: not executed live; static trace of `walk_filtered`'s `CATCH zcx_abapgit_exception` best-effort `'M'`-row write and `ensure_filtered_coverage`'s backoff short-circuit confirms the design's non-fatal/backoff shape, unchanged by this audit's findings.

No scenario above was measured with a live trace or synthetic fixture this pass — all statements are static-analysis estimates per the evidence order in the operating instructions.

## Verdict

**FAIL_IMPLEMENTATION_PERFORMANCE**

PS-001 is verified fixed and correct. However, direct comparison against
the approved design surfaced PA-001: `select_rows_for_filter` — the method
serving every warm-COMPLETE-mode filtered request, including large-K/full-
Stage requests — never received the chunking (and `context_hash`
predicate) that the design's own adversarial cycle explicitly flagged as
MAJOR (AR-1-04) and recorded as closed. This is a real, currently-reachable
gap in the primary hot path, not a hypothetical one, and it was not caught
by the prior static scan or by any of the recorded subagent gap-fill
verification passes. Fix required before this slice can be approved as
matching its own design.
