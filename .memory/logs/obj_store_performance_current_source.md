# OBJ-PERF-DISC-1 — ZAOG_OBJ_STORE performance current-source reconciliation
Read-only discovery. BASELINE_COMMIT=4193733de3c8ad75bae61f6ceb95b1c3e0ccb3e4,
branch ortec/abapgit_1_133-opt-rework. No source modified.
## Q12 — Are complete payload columns selected for presence-only checks?
No, for the two existence-only helpers:
- `get_present_sha1s` / `get_missing_sha1s`
  ([zcl_abapgit_ortec_obj_store.clas.abap](../../src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap)):
  `SELECT obj_sha1 FROM zaog_obj_store ... WHERE repo_key = ? AND obj_sha1 IN
  lr_sha1s AND status = 'R'` — never selects `obj_data`, chunked at
  `c_select_package_size = 1000` per range-window.
- `verify_ready_blobs`: `SELECT obj_sha1, obj_type FROM zaog_obj_store FOR ALL
  ENTRIES ... WHERE ... status = 'R'` — metadata only (sha1+type), no
  `obj_data`.
- `exists`: `SELECT SINGLE obj_sha1 ... INTO lv_dummy` — single-column,
  no payload.
- `fetch_blobs_bulk`'s metadata pre-pass
  ([zcl_abapgit_ortec_walk_prep.clas.abap](../../src/ortec/git/zcl_abapgit_ortec_walk_prep.clas.abap)):
  `SELECT store~obj_sha1, store~obj_size FROM zaog_obj_store AS store INNER
  JOIN @lt_requested ... INTO TABLE @lt_metadata` — size-only, payload
  deferred to a second, byte-budgeted `get_objects` call.
**Yes, payload IS pulled unnecessarily** in one path: `verify_tree_closure`
and `get_tip_blob_sha1s` (blob-blind/blob-collecting tree walks) call
`get_objects(iv_bulk_fetch = abap_false)` for **commit and tree** objects —
that is correct (tree walk needs tree bytes to decode child nodes) — but
`get_reachable_objects`/`get_reachable_sha1s` (used by `full_tree`/
`walk_tree` consumers, not this scope's filtered path) load full blob
`obj_data` via `get_objects` for the **entire** reachable blob set even when
only existence is eventually asserted for `get_reachable_sha1s` (it uses
`get_present_sha1s`, correctly cheap) — but its full-object sibling
`get_reachable_objects` always fetches every blob's payload, no partial mode.
Not on the current filtered-Stage/Diff hot path (Q7 in the index artifact),
but shared infrastructure other ORTEC callers use.
## Q13 — Are the same SHA/type/repository keys requested more than once in one user action?
Yes, structurally, across independent call layers with **no cross-call
sharing beyond the session cache (`mt_cache`)**:
- A filtered Stage/Diff resolution calls `ensure_index` → (if rebuild needed)
  `rebuild_index`'s own `get_objects` calls for the commit object and every
  tree level, **then**, once index rows are known, `build_files_from_rows`
  calls `get_objects` again for the **blob** SHA1 set. These are disjoint SHA1
  sets by construction (commits/trees vs. blobs), so no duplicate SHA1 is
  requested *within* one `get_files_for_filter` call — but see below for
  cross-call duplication.
- `zcl_abapgit_ortec_missing_obj=>ensure_available` (called from
  `build_files_from_rows` when `iv_url`/`iv_commit` are supplied) itself calls
  `get_missing_sha1s` (existence-only) up to **twice** (once before, once
  after `materialize_missing_batches`) for the identical `it_sha1s` set — by
  design (verify-then-fetch-then-reverify), not a bug, but 2 extra chunked
  SELECTs on the common "nothing missing" case beyond the one needed.
- Cross-call duplication: `get_reachable_objects`/`get_reachable_sha1s`/
  `verify_tree_closure`/`get_tip_blob_sha1s` **each independently re-walk the
  same commit→tree structure from scratch** with their own `get_objects`
  calls when more than one is invoked for the same commit in one broader
  operation (e.g. a graph-closure check followed later by a snapshot
  materialization) — `get_tip_blob_sha1s`'s own doc says it "re-verif[ies]
  closure independently every call - no cross-call in-memory trust" as a
  **deliberate** design choice (Package B design §11 step 1), not an oversight
  — but it does mean the commit + every tree object's bytes are decoded twice
  (once per verification call) if both are invoked in the same request, with
  only the session object-store cache (`mt_cache`, keyed `repo_key+obj_sha1`)
  softening the DB-read cost (decode/tree-parse CPU cost is still repeated).
## Q14 — Deduplication before DB access; read pattern (singleton/FOR ALL ENTRIES/range/cursor/chunked); WHERE predicates and covering indexes
- **Deduplication**: `get_objects`, `get_available_objects`,
  `get_staged_delta_objects`, `has_dangling_delta_base`, `get_present_sha1s`,
  `get_missing_sha1s`, `verify_ready_blobs` all deduplicate their input via a
  `HASHED TABLE ... WITH UNIQUE KEY table_line` insert-loop
  (`lt_unique_sha1s`/`lt_unique`) before touching the DB — consistent pattern
  across the class. `fetch_blobs_bulk` in `walk_prep` also dedups via
  `lt_requested_set`.
- **Read pattern**: range-based (`RANGE OF zaog_obj_store-obj_sha1`, built as
  `sign = 'I' option = 'EQ'`, i.e. an `IN`-list, not literal `FOR ALL
  ENTRIES` in most of `zcl_abapgit_ortec_obj_store`) chunked at
  `c_select_package_size = 1000` — `read_object_rows` (used by `get_objects`,
  `get_available_objects`, `has_dangling_delta_base`), `get_present_sha1s`.
  `get_staged_delta_objects` builds its own inline `IN`-range per chunk
  (does not call `read_object_rows`, since it needs `status IN ('D','R')`
  instead of the hard-coded `'R'`). `fetch_blobs_bulk`
  ([zcl_abapgit_ortec_walk_prep.clas.abap](../../src/ortec/git/zcl_abapgit_ortec_walk_prep.clas.abap))
  uses a genuine `FOR ALL ENTRIES`-style DB-side `INNER JOIN @lt_requested`
  (an internal-table join, chunked at `lc_key_chunk_size = 2000`) — a
  different, newer idiom than the rest of the class.
  `has_dangling_delta_base`'s base-collection step uses classic
  `FOR ALL ENTRIES IN @lt_package` against `zaog_pack_idx` (not
  `zaog_obj_store`).
- **WHERE predicates and covering indexes** (against
  [zaog_obj_store.tabl.xml](../../src/ortec/git/zaog_obj_store.tabl.xml),
  primary key `(MANDT, REPO_KEY, OBJ_SHA1)`; secondary indexes `RPK
  (REPO_KEY, PACK_ID, STATUS, OBJ_SHA1)` and `STA (REPO_KEY, STATUS)`):
  - `read_object_rows`/`get_present_sha1s`/`verify_ready_blobs`/
    `get_staged_delta_objects`: `WHERE repo_key = ? AND obj_sha1 IN (...)
    AND status = ?` — `repo_key + obj_sha1` is the **primary key prefix**
    (optimal, no secondary index needed); `status` is a residual filter
    applied after primary-key lookup, not used for index selection here.
  - `exists`: `WHERE repo_key = ? AND obj_sha1 = ? AND status = 'R'` —
    primary-key exact match, optimal.
  - `get_known_commits`: `WHERE repo_key = ? AND obj_type = 'commit' AND
    status = 'R'` — **no `obj_sha1`**, so this cannot use the primary key
    beyond `repo_key`; it lands on secondary index `STA (repo_key, status)`
    at best (still requires a residual filter on `obj_type` since `STA`
    doesn't include it) — this is the one predicate shape in this class that
    plausibly benefits from `STA`, though `obj_type` is not part of `STA`
    either, so SAP must still scan+filter by `obj_type` across all
    `status='R'` rows of the repo.
  - `populate_cache`/`get_all_objects`: `WHERE repo_key = ? AND status = 'R'
    ORDER BY obj_sha1` — unbounded row/byte selection (loads `obj_data` for
    every ready object of the repo) — matches `STA (repo_key, status)`
    exactly for index selection, but the **payload volume**, not index
    coverage, is the real cost driver (see the `SYSTEM_NO_ROLL` incident
    note inline in `get_reachable_objects`'s comment block, which is why that
    method stopped calling `populate_cache`; `get_all_objects` still does).
  - `fetch_blobs_bulk`'s metadata join: `WHERE store~repo_key = @iv_repo_key
    AND store~status = 'R'` plus the join `ON store~obj_sha1 =
    requested~table_line` — same `STA`-index shape as above, further
    narrowed by the join.
  - `clear_repo`: `DELETE FROM zaog_obj_store WHERE repo_key = ?` — primary
    key prefix.
  - `has_dangling_delta_base`'s `zaog_pack_idx` query: not in this table's
    scope (separate DDIC object, not read for this task).
**`ddic_index_candidates` for `ZAOG_OBJ_STORE`**: 1 plausible candidate —
`get_known_commits`'s `(repo_key, obj_type, status)` predicate does not
fully match either existing index (`RPK` needs `pack_id` too; `STA` lacks
`obj_type`); a `(REPO_KEY, OBJ_TYPE, STATUS)` secondary index would make it
index-only, but `get_known_commits` was not identified as being on the
filtered-Stage/Diff hot path in this scope (no caller found in the read
files) — flagged as a candidate, not a hot-path finding.
## Q15 — Is repository identity part of every key where required? Can object type confusion occur for equal hashes/generic rows?
`repo_key` is included in every `WHERE` clause across every method in this
class — confirmed no cross-repository leak pattern in any predicate read.
`set_active_repo_key`/`mv_cache_repo_key`'s blank-`iv_repo_key` fallback in
`get_object` is the one deliberate exception (documented risk in the method's
own doc: "never rely on an accidental side effect of an unrelated
get_objects/populate_cache call, which can leak a stale, unrelated repo's
key") — a real, already-flagged-in-source risk of cross-repo key confusion
if a caller relies on the implicit fallback instead of passing `iv_repo_key`
explicitly, not observed to be exploited by any file read in this scope.
Object-type confusion for **equal hashes** cannot occur via SHA1 collision
in practice, but a genuine **content-addressed same-SHA1-different-type**
inconsistency (store corruption) is explicitly guarded in
`build_files_from_rows` (raises `CORRUPT_OR_INCOMPLETE` if a `blob_sha1` from
the index resolves to a non-blob type) and in every tree-walk method's
`IF <ls_tree_object>-type <> c_type-tree` / `IF <ls_blob_object>-type <>
c_type-blob` checks — consistently enforced, not skipped anywhere read.
## Q16 — Request-scoped state/cache ownership/cleanup; concurrent cleanup vs. read; repeated integrity verification; unavoidable vs redundant reads
- **Cache ownership**: `mt_cache` (`CLASS-DATA`, session-scoped, keyed
  `repo_key + obj_sha1`) is populated by `get_objects`/`get_available_objects`/
  `get_staged_delta_objects` on every DB hit, and fully cleared by
  `invalidate_cache` — called from `store_object`/`store_objects` (every
  write invalidates the **entire** cache, not just the written keys) and
  explicitly by `clear_repo`. No other owner/cleanup path found in this
  class. `mv_full_cache_repo_key`/`is_cache_valid` gate `populate_cache`'s
  full-repo preload specifically (separate from the per-SHA1 `mt_cache`
  validity, which has no equivalent "valid until write" flag beyond the
  blanket `invalidate_cache` on any write).
- **Concurrent cleanup vs. read**: no explicit lock is taken by any
  `zcl_abapgit_ortec_obj_store` read method (only `rebuild_index` in the
  index class takes `acquire_repo_lock`, see index artifact Q11) — a
  concurrent `clear_repo`/retention delete against `ZAOG_OBJ_STORE` for the
  same `repo_key` while a read is in flight is not defended against by
  anything in this file (relies entirely on DB-level read consistency /
  whatever isolation the caller's LUW provides); not evaluated further
  (no retention/cleanup job source was in `SOURCE_SCOPE`).
- **Repeated integrity verification**: yes, structurally, in two independent
  ways: (a) `zcl_abapgit_ortec_missing_obj=>ensure_available`'s
  verify→fetch→reverify double `get_missing_sha1s` (Q13); (b)
  `get_reachable_sha1s`/`verify_tree_closure`/`get_tip_blob_sha1s`'s
  by-design independent re-walks (Q13) — each is a full tree-decode
  traversal, not just a metadata check, so "verification" here means
  repeating real decode CPU work, not just a cheap re-query.
- **Unavoidable vs redundant reads** (synthesis for the performance map
  below): the two SQL calls in `get_files_for_filter` on a warm index
  (`is_index_ready` + `select_rows_for_filter`) plus the one blob
  `get_objects` in `build_files_from_rows` are the **minimum unavoidable**
  set for a warm, complete-index filtered read. Everything above (double
  missing-check, independent re-walks, `populate_cache`'s full-repo preload
  in `get_all_objects`, the second `file_to_object` pass in
  `apply_object_filter`) is either (a) a deliberate, documented safety
  trade-off (independent re-walks, double missing-check) or (b) genuinely
  redundant work not gated by any completeness/cache signal (the
  `apply_object_filter` re-derivation — see index artifact Q3).
## Performance map (state derived from source shape, not live traces)
Legend: **K**=requested filter/object count, **F**=total files at commit,
**L**=tree levels (directory depth), **B**=batches
(`c_select_package_size=1000` for obj_store chunked reads;
`c_index_write_chunk_size=30000` for index writes;
`c_materialize_batch_max`/adaptive 50-1000 for `cold_init` HTTP batches — not
on the currently-wired filtered path, see index artifact Q6).
| Scenario | SQL stmts | Rows read/written | Trees loaded | Blob payloads loaded | HTTP requests | Peak bytes | Complexity |
|---|---|---|---|---|---|---|---|
| **Index: cold/no index** | `is_index_ready` (1) + `rebuild_index`: 1 commit `get_objects` + ceil(treeCount/1) tree-level `get_objects` calls (one per BFS level, not chunked by count here — each level is its own single `get_objects` call regardless of width) + `MODIFY zaog_obj_index` every 30000 rows + 1 marker `MODIFY` + `select_rows_for_filter` (1) + `build_files_from_rows` blob `get_objects` (chunked ceil(K-blob-count/1000)) | writes: F rows + 1 marker row; reads: 1 commit + all L-level tree objects (all trees at the commit, not just filter-relevant ones) + K's blob set | **all** trees reachable from commit root (not filter-scoped — full O(F) tree walk) | only the K requested objects' blobs | 0 (assumes objects already in `ZAOG_OBJ_STORE`; see index artifact Q7 — remote fetch is a separate caller's responsibility) | full tree-object working set in memory during the walk (all `lt_pending`/`lt_tree_data` for widest level) + F-row `lt_rows` buffer capped at 30000 | O(F) dominates regardless of K; **ensure_index forces a full rebuild for small K today** |
| **Index: partial coverage** (some but not all commit's files indexed — **not a supported state**; index is all-or-nothing) | same as cold — `is_index_ready` only recognizes 0% or 100% (marker gate), so any partial state is治treated identically to cold and fully rebuilt | same as cold | same as cold | same as cold | same as cold | same as cold | identical cost to cold/no-index; no partial-credit path exists |
| **Index: complete (warm, exact K rows requested)** | `is_index_ready` (1) + `select_rows_for_filter` (1, `FOR ALL ENTRIES`) + `build_files_from_rows` blob `get_objects` (chunked ceil(distinct-blob-count/1000)) + `apply_object_filter`'s implicit re-derivation (0 SQL, pure ABAP `file_to_object` per file) | reads: K-object row set (F_k files) + F_k blobs | 0 | F_k (files belonging to the K requested objects) | 0 | F_k blob payload total | O(F_k) + O(K) filter lookup; minimum unavoidable cost for this design |
| **Index: stale (rebuild interrupted, no marker)** | identical to cold — `is_index_ready` false ⇒ full `rebuild_index`, including its own leading `DELETE FROM zaog_obj_index` (redundant since a non-`READY` state already implies no complete data, but still executes) | same as cold, plus 1 extra DELETE (bounded by whatever partial rows exist) | same as cold | same as cold | same as cold | same as cold | identical to cold; the DELETE is the only extra, negligible cost |
| **Index: concurrent build (same repo, two commits or same commit)** | second caller blocks on `acquire_repo_lock` (repo-wide, not per-commit) for the entire duration of the first rebuild's SQL sequence above | none extra (serialized) | none extra | none extra | 0 | none extra | wall-clock latency = sum, not max, of concurrent rebuild costs for the same repo (repo-wide lock, see Q11 in index artifact) |
| **Obj-store: cold branch load** (via `zcl_abapgit_git_porcelain`→`zcl_abapgit_ortec_porcelain=>pull_by_branch`, not this file's own tree-walk methods) | outside `SOURCE_SCOPE` for the fetch/decode pipeline itself; downstream persistence is `store_objects` (1 bulk `MODIFY zaog_obj_store FROM TABLE`, unchunked) + `invalidate_cache` | writes: all F commit/tree/blob objects fetched in this pull | all reachable trees | all reachable blobs (standard/ORTEC pull is not filter-aware) | 1 (the pull's own upload-pack request; ORTEC fastpath/negotiation specifics are out of `SOURCE_SCOPE`) | one `store_objects` in-memory `lt_rows` table sized to the whole fetched object set | O(F); **`cold_init`'s GRAPH_COMPLETE/SNAPSHOT_COMPLETE partial-fetch machinery exists but has no wired caller for this scenario today** |
| **Obj-store: cold filtered Stage by Transport** | = "Index: cold/no index" row above, reached via `zcl_abapgit_ortec_filter_walk=>get_remote_files_for_stage`→`zcl_abapgit_ortec_obj_index=>get_files_for_filter` | see cold/no-index row | see cold/no-index row | see cold/no-index row | 0-1 (`try_filtered_commit_fetch`'s blobless fetch only if branch has no usable cached state — see index artifact Q1's caller chain) | see cold/no-index row | O(F) despite small K — same finding as index cold row |
| **Obj-store: cold single-object Diff** | identical mechanics to cold filtered Stage (via `get_remote_files_for_diff`→`get_remote_files_for_stage`, same code path, K=1) | same shape as cold/no-index but K=1 | same as cold/no-index (full tree walk still required) | 1 object's files only | same as stage row | same as cold/no-index | O(F) for a K=1 request — same full-rebuild finding, most disproportionate case |
| **Obj-store: warm repeated filtered Stage** | = "Index: complete (warm)" row | see warm row | 0 | F_k | 0 | F_k blob total | O(F_k); best case, no redundant full-repo cost |
| **Obj-store: complete ZAOG_OBJ_INDEX rebuild** | = "Index: cold/no index" row (rebuild_index IS the complete-rebuild operation; there is no separate "just the index, not files" API) | writes: F+1 rows | all | 0 (index build never loads blob content, see index artifact Q5) | 0 | tree-object working set only (no blob payload) | O(F) tree-only walk; cheaper than a full file-content pull |
| **Obj-store: selected-tip snapshot materialization** (`cold_init=>materialize_tip_snapshot`, not wired to filtered path — Q6) | `get_tip_blob_sha1s` (own commit+tree walk, `get_objects(iv_bulk_fetch=abap_false)` per level) + `get_missing_sha1s` (chunked existence) + adaptive `materialize_batch` HTTP+persist loop + final `verify_ready_blobs`/re-check | writes: only the missing blob subset | all reachable trees (own independent walk, Q13) | only missing blobs (delta from already-`READY` set) | ceil(missing-blob-count / adaptive-batch-size, 50-1000 rows or byte-budget-limited) | one HTTP response buffer per batch, byte-budgeted (`c_max_batch_response_bytes = 26214400`) | O(L) tree walk + O(missing-blob-count) fetch; **not reachable from any caller in this scope today** |
| **Obj-store: missing-tree/blob recovery** (`build_files_from_rows`'s `zcl_abapgit_ortec_missing_obj=>ensure_available` best-effort top-up) | `get_missing_sha1s` (1) + `materialize_missing_batches` (adaptive HTTP+persist, chunked like above) + `get_missing_sha1s` retry (1) | writes: only the missing blob subset | 0 (blob-only recovery, never trees — index artifact Q1's tree-not-found case has no equivalent automatic recovery, only outer full-remote-read fallback) | only the previously-missing blobs | ceil(missing/adaptive-batch-size) | one HTTP response buffer per batch | O(missing-count); **biggest redundant read pattern found: the verify→fetch→reverify double existence-check always runs even when nothing is ultimately missing** |
| **Obj-store: full repository status** (`get_all_objects`/`populate_cache`) | 1 unbounded `SELECT * ... WHERE repo_key = ? AND status = 'R' ORDER BY obj_sha1` | reads: **every** ready object of the repo, including full blob payloads | all | all (unconditionally, regardless of caller's actual need) | 0 | entire repo's ready-object payload in one internal table (`lt_rows`) — the exact shape that caused the documented `SYSTEM_NO_ROLL` incident when called from `get_reachable_objects` (since fixed there by removing the `populate_cache` call; `get_all_objects` still calls it, unguarded) | O(total repo object count) — **the single most over-broad read pattern found in this scope: no row/byte limit, no filter, and still called from at least one live method (`get_all_objects`)** |
## Summary answers for parent envelope
- **Index key finding**: yes — `ensure_index`/`is_index_ready`'s all-or-nothing
  `$IDX/__READY__` marker means `rebuild_index` performs a full O(F) commit→
  tree→blob-metadata walk even for K=1, with no partial-K or incremental
  rebuild path in current source.
- **Obj-store key finding**: `get_all_objects`/`populate_cache`'s unbounded
  `SELECT * ... status = 'R'` (no row/byte limit, full `obj_data` for every
  ready object in the repo) is the most over-broad pattern — already
  root-caused a live `SYSTEM_NO_ROLL` incident when called from
  `get_reachable_objects` (now fixed there specifically), but `get_all_objects`
  itself still calls `populate_cache` unguarded.