# OBJ-PERF-DISC-1 — ZAOG_OBJ_INDEX partial-index current-source reconciliation
Read-only discovery. BASELINE_COMMIT=4193733de3c8ad75bae61f6ceb95b1c3e0ccb3e4,
branch ortec/abapgit_1_133-opt-rework. No source modified. All claims are
CONFIRMED_CURRENT against the file states read for this task unless marked
otherwise.
## Q1 — What exact calls cause `ensure_index` to trigger `rebuild_index`?
[zcl_abapgit_ortec_obj_index.clas.abap](../../src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap)
`ENSURE_INDEX` (single call site of `REBUILD_INDEX`):
```
IF is_index_ready( iv_repo_key iv_commit ) = abap_true. RETURN. ENDIF.
rebuild_index( ... ).
```
`ENSURE_INDEX` itself is called from exactly two places in `GET_FILES_FOR_FILTER`:
1. unconditionally, before the first `select_rows_for_filter` call;
2. again inside the `CATCH zcx_abapgit_exception` retry block, after an
   explicit `DELETE FROM zaog_obj_index WHERE repo_key = ... AND commit_sha1 = ...`
   (stale-row purge) — this second call is unconditional too, since the
   preceding `DELETE` guarantees `IS_INDEX_READY` will be false.
So: `rebuild_index` fires whenever `is_index_ready` is false for
`(repo_key, commit)` — i.e. on every FIRST filtered call for a commit, and on
every retry after a `build_files_from_rows` failure (corrupt/incomplete blob).
There is no partial/incremental rebuild path — `rebuild_index` always deletes
**every** row for `(repo_key, commit)` and walks the **entire** commit tree
from `/` (full width-first tree walk, no filter awareness at all — the filter
argument used elsewhere is never passed into `rebuild_index`/`ensure_index`).
## Q2 — Does `get_files_for_filter` require a complete index, or only exact requested rows?
It requires a **complete index build** even for a tiny `K` (requested-object
count). `ensure_index`/`is_index_ready` check only the presence of the
`$IDX/__READY__` marker row (`c_marker_obj_type`/`c_marker_obj_name`) written
as the **last** step of `rebuild_index` — there is no notion of "index has
rows for these K objects, that's enough". A single unmatched object filter on
a never-before-indexed commit forces a full tree walk of the commit (all
levels, all blobs referenced by any tree — see Q6) before
`select_rows_for_filter` ever runs its `FOR ALL ENTRIES` `SELECT`.
`RELAXED` mode (`zcl_abapgit_ortec_git_switch=>cs_absent_strictness-mode_relaxed`,
"benchmark-only, never ships as default" per its own doc) trusts the first
row found — still not partial-K-aware, only skips the completeness guarantee.
## Q3 — How are object filters represented? Can one object generate multiple files, and where is that mapping determined?
Filter entries are `zif_abapgit_definitions=>ty_tadir` rows (`object`,
`obj_name`) obtained from `ii_obj_filter->get_filter( )`. One TADIR object
**can** map to multiple files (e.g. FUGR → multiple includes, CLAS → main +
testclasses, etc.) — the mapping is materialized as multiple
`ZAOG_OBJ_INDEX` rows sharing the same `(obj_type, obj_name)` but distinct
`path_hash`/`file_path`/`file_name`, one row per tree leaf discovered during
`rebuild_index`'s walk via
`zcl_abapgit_filename_logic=>file_to_object( iv_filename = <ls_node>-name
iv_path = <ls_work>-path ... )` (obj_index.clas.abap, `rebuild_index`, tree
leaf branch). `select_rows_for_filter`'s `FOR ALL ENTRIES ... WHERE obj_type =
it_filter-object AND obj_name = it_filter-obj_name` then naturally returns all
rows for that object, i.e. all its files, in one shot.
**Redundant re-derivation found**: after `build_files_from_rows` returns
`rt_files`, `GET_FILES_FOR_FILTER` calls
[zcl_abapgit_repo_filter.clas.abap](../../src/repo/filter/zcl_abapgit_repo_filter.clas.abap)
`apply_object_filter`, which re-runs `file_to_object` **again for every
returned file** to re-derive `(obj_type, obj_name)` and re-check membership
against the filter list (`READ TABLE lt_filter ... BINARY SEARCH`) — a second,
functionally redundant object/file mapping pass over a result set that
`select_rows_for_filter`'s SQL predicate already exactly matched. This exists
purely "to keep generated-object handling aligned with standard
apply_object_filter logic" (comment in `GET_FILES_FOR_FILTER`) — no
correctness bug, but F extra `file_to_object` calls (folder/namespace/escaping
logic, see Q4) per filtered call regardless of K.
## Q4 — How do .abapgit config, folder logic, namespace handling, devclass, ignored paths, and filename escaping affect paths?
- `io_dot` (parsed `.abapgit`) and `iv_devclass` are threaded through
  `rebuild_index`'s tree-walk leaf branch into
  `zcl_abapgit_filename_logic=>file_to_object( iv_devclass = iv_devclass
  io_dot = io_dot )` — namespace `#`→`/` unescaping and package/folder
  resolution happen once per leaf, at index-build time, and the resulting
  `obj_type`/`obj_name` are what gets persisted, not re-derived at read time
  (except in the `apply_object_filter` redundant pass — Q3).
- DEVC folder-path scoping is handled **separately** in `GET_FILES_FOR_FILTER`
  itself (not inside the index build): for every filter entry `WHERE object =
  'DEVC'`, `zcl_abapgit_folder_logic=>get_instance( )->package_to_path(...)`
  resolves the package's folder path, collected into `lt_devc_paths`; matched
  `DEVC` rows returned from the index (`obj_type = 'DEVC'`) are then
  additionally filtered by `<ls_row>-file_path` membership in
  `lt_devc_paths` — an ORTEC-specific post-filter with no equivalent
  persisted in the index rows themselves (a DEVC row's own `path_hash` key
  does not encode which requested package it belongs to).
- Filename escaping/ignored-path logic is entirely inside
  `zcl_abapgit_filename_logic=>file_to_object` (standard class, unmodified by
  ORTEC) — `rebuild_index` calls it in a `TRY...CATCH zcx_abapgit_exception.
  CONTINUE.` guard, so any file that standard logic rejects (bad name, wrong
  extension, etc.) is silently skipped from the index, exactly mirroring
  standard behavior for `apply_object_filter`.
## Q5 — Which data come from tree path vs. blob content?
- `file_path`/`file_name`/`path_hash` and `obj_type`/`obj_name` (index key
  columns) all come from the **tree walk** (path string built while
  recursing `chmod = dir` nodes, `sha1_string(path&name)` for `path_hash`) —
  zero blob bytes are touched to build the index.
- `blob_sha1` is captured from the tree leaf's `<ls_node>-sha1` (the blob's
  identity, still tree-level metadata, not content).
- Actual file **content** (`ls_file-data`) is loaded only in
  `build_files_from_rows`, which bulk-`get_objects`'s exactly the distinct
  `blob_sha1` set collected from the matched rows — content is never read
  during `rebuild_index`.
## Q6 — Which tree/blob objects are guaranteed present after GRAPH_COMPLETE and SNAPSHOT_COMPLETE?
Per [zcl_abapgit_ortec_mat_state.clas.abap](../../src/ortec/git/zcl_abapgit_ortec_mat_state.clas.abap)
class doc and
[zcl_abapgit_ortec_obj_store.clas.abap](../../src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap)
`verify_tree_closure`/`get_tip_blob_sha1s` docs:
- `GRAPH_COMPLETE` (`cs_hist_level-graph_complete`): commit + **complete tree
  closure** verified present (`verify_tree_closure` walks commit→tree,
  blob-blind, raises on any missing/undecodable/wrong-type tree) — blobs are
  explicitly **not** required and may be "promised" only.
- `FULL_COMPLETE` (`cs_hist_level-full_complete`): additionally every blob
  reachable from the tip's tree has been verified present
  (`get_tip_blob_sha1s` + `get_missing_sha1s` + `materialize_tip_snapshot`'s
  fetch loop + a final `verify_ready_blobs`/re-check before
  `publish_snapshot_complete`).
- **Not wired to `zcl_abapgit_ortec_obj_index`/`filter_walk` today**: neither
  `rebuild_index` nor `ensure_index`/`is_index_ready` reads or writes
  `ZAOG_COMMIT_HIST`/`mat_state` at all — the index's own completeness
  concept (`$IDX/__READY__` marker, Q1/Q2) is a **separate, independent**
  completeness signal from GRAPH_COMPLETE/SNAPSHOT_COMPLETE. `cold_init`
  (`acquire_blobless_graph`/`materialize_tip_snapshot`) has "No productive
  caller ... wired ... yet (Package C scope)" per its own class doc — so on
  the currently reachable filtered-Stage/Diff path, GRAPH_COMPLETE/
  SNAPSHOT_COMPLETE are never consulted or produced; `rebuild_index` does its
  own from-scratch commit→tree→blob-metadata walk regardless of any
  mat_state certificate that might already exist for that commit.
## Q7 — Does filtered access already know/materialize the requested blobs?
No advance knowledge: `GET_FILES_FOR_FILTER` calls `ensure_index` then
`select_rows_for_filter`, and only after the matching index rows are known
does `build_files_from_rows` compute the distinct `blob_sha1` set and call
`zcl_abapgit_ortec_obj_store=>get_objects` (with `iv_url`+`iv_commit`-gated
best-effort top-up via `zcl_abapgit_ortec_missing_obj=>ensure_available`
first). The index build itself (`rebuild_index`) never checks/loads blob
*content* at all (Q5) — it only ever needs tree bytes to walk structure, so a
first-time filtered call on a cold commit still requires the full tree
closure to already be fetched into `ZAOG_OBJ_STORE` by whatever pulled the
commit (standard/ORTEC pull, or `try_filtered_commit_fetch`'s blobless
fetch) before `rebuild_index` can even start.
## Q8 — Which calls can raise Walk/tree-not-found and what recovery exists?
- `rebuild_index`: raises `Index build: commit { } missing` if
  `get_objects` doesn't return a commit object of type `commit`; raises
  `Index build: commit { } has no tree` if `decode_commit` yields no tree.
  Both come from `zcl_abapgit_ortec_obj_store=>get_objects`, which itself
  raises `Object { } not found in store` (`zcx_abapgit_ortec_git`) whenever a
  wanted SHA1 isn't in `ZAOG_OBJ_STORE` with `status = 'R'` — `rebuild_index`
  wraps that via `CATCH zcx_abapgit_ortec_git ... raise_with_text`.
- Recovery: `rebuild_index`'s only recovery is the repo lock
  release-and-reraise (`CATCH zcx_abapgit_exception`/`CATCH cx_root` both
  release `lv_lock_id` then re-raise) — there is **no** automatic remote
  fetch inside `rebuild_index` itself for a missing commit/tree object; that
  is entirely the caller's responsibility (`filter_walk`'s outer
  `CATCH zcx_abapgit_exception. rt_files = ii_repo_online->get_files_remote(
  ... )` fallback to the full standard remote read).
- `build_files_from_rows` raises its own `CORRUPT_OR_INCOMPLETE` error (a
  SHA1 that exists in the store but is not type `blob`) — `get_files_for_filter`
  catches exactly this (`CATCH zcx_abapgit_exception`), purges the commit's
  index rows, and retries the whole `ensure_index`→`select_rows_for_filter`→
  `build_files_from_rows` sequence **once**; a second failure propagates to
  `filter_walk`'s outer fallback.
## Q9 — What side effects occur after standard or ORTEC pull that an early return must preserve?
[zcl_abapgit_git_porcelain.clas.abap](../../src/git/zcl_abapgit_git_porcelain.clas.abap)
`pull_by_branch`/`pull_by_commit`/`push` all route to
`zcl_abapgit_ortec_porcelain` when `zcl_abapgit_ortec_git_switch=>is_active_for_repo`
is true — that path performs the full decode/persist/certify lifecycle
(object-store writes, `zcl_abapgit_ortec_fastpath=>persist_pull_result`
→`persist_missing_objects`, repo-state/commit-history updates) as a side
effect of a "pull". Any code path in `filter_walk`/`obj_index` that falls
back to `ii_repo_online->get_files_remote(...)` (standard, non-ORTEC file
retrieval) **bypasses** that persistence entirely — it is a pure read that
does not re-seed `ZAOG_OBJ_STORE`/`ZAOG_OBJ_INDEX`. An "early return" inside
`get_files_for_filter`/`get_remote_files_for_stage` (e.g. empty filter, no
repo key, no commit) must therefore not be mistaken for "the persistence
side effects of a real pull already happened" — none of the current early
returns in these two classes perform any object-store write themselves; they
only ever read or (in `try_filtered_commit_fetch`'s success path) perform a
best-effort blobless fetch+persist before continuing.
## Q10 — Exact WHERE predicates against ZAOG_OBJ_INDEX and which DDIC indexes cover them
DDIC: [zaog_obj_index.tabl.xml](../../src/ortec/git/zaog_obj_index.tabl.xml) —
primary key `(MANDT, REPO_KEY, COMMIT_SHA1, OBJ_TYPE, OBJ_NAME, PATH_HASH)`,
**no secondary indexes defined** (only `DD03P_TABLE` fields; no `DD12V`/`DD17V`
blocks present, unlike `ZAOG_OBJ_STORE` below).
Predicates found:
1. `is_index_ready` (both STRICT and RELAXED):
   `SELECT SINGLE path_hash ... WHERE repo_key = ? AND commit_sha1 = ?
   [AND obj_type = '$IDX' AND obj_name = '__READY__'] AND idx_status = 'R'`
   — STRICT variant's `repo_key+commit_sha1+obj_type+obj_name` prefix is a
   **left-anchored, fully-specified primary-key prefix up to obj_name**
   (only `path_hash` unspecified — a known constant, `c_marker_path_hash`,
   in practice), so it is primary-key-covered/optimal. RELAXED's
   `repo_key+commit_sha1` prefix alone is also primary-key-covered but must
   scan (SINGLE, so stops at first hit) — cheap either way.
2. `select_rows_for_filter`: `SELECT * ... FOR ALL ENTRIES IN it_filter
   WHERE repo_key = ? AND commit_sha1 = ? AND idx_status = 'R' AND obj_type =
   it_filter-object AND obj_name = it_filter-obj_name` — this is a full
   primary-key prefix (`repo_key, commit_sha1, obj_type, obj_name`) per
   `FOR ALL ENTRIES` row, missing only `path_hash` (returns all matching
   files for that object) — also primary-key-covered, no secondary index
   needed for this specific access pattern.
3. `rebuild_index`'s own housekeeping: `DELETE FROM zaog_obj_index WHERE
   repo_key = ? AND commit_sha1 = ?` (both in `rebuild_index` itself and in
   `get_files_for_filter`'s stale-row retry) — primary-key prefix, covered.
**No `ddic_index_candidates` found for `ZAOG_OBJ_INDEX`** — every observed
read pattern already uses a full or near-full primary-key prefix; the table
has no query pattern in this scope that would benefit from an additional
secondary index (e.g. no lookup by `blob_sha1`/`tree_sha1` alone was found in
this scope).
## Q11 — READY-marker vs filtered-row reads; stale-row cleanup; concurrency; config-change invalidation; which ops need every row
- **Separate SQL calls**: yes — `is_index_ready` (marker check) and
  `select_rows_for_filter` (filtered rows) are two independent `SELECT`
  statements, always both issued on every `get_files_for_filter` call when
  the index is already ready (no combined single-SELECT fast path).
- **Stale partial rows before rebuild**: yes, explicitly —
  `rebuild_index` does `DELETE FROM zaog_obj_index WHERE repo_key = ...
  AND commit_sha1 = ...` before starting its walk, and `get_files_for_filter`'s
  retry branch does the identical delete again before calling `ensure_index`
  a second time.
- **Concurrent sessions**: `rebuild_index` acquires
  `zcl_abapgit_ortec_pack_raw=>acquire_repo_lock( iv_repo_key )` for the
  **whole repo** (not scoped to the commit) before its
  `is_index_ready`-recheck/delete/walk/write sequence, and releases it in
  every exit path (success, `zcx_abapgit_exception`, `cx_root`). A second
  session for the *same repo* (even a different commit) blocks on this lock
  for the full walk+chunked-write duration — this is a repo-wide serialization
  point, not per-commit. A concurrent reader calling only
  `get_files_for_filter`/`is_index_ready` (no rebuild needed) takes no lock at
  all, so a reader can see a fully-committed prior index while a rebuild for
  a *different* commit of the same repo is blocked behind the lock, or (if it
  arrives after the rebuild's initial `DELETE` but the transaction/LUW hasn't
  committed) could observe a transiently empty result set for the commit
  being rebuilt, depending on DB isolation — no explicit LUW/COMMIT WORK call
  is visible in this class, so persistence timing follows the caller's own
  LUW.
- **Algorithm/config-change invalidation**: none found in this class. There
  is no version/config stamp stored per index row or per commit — a change to
  `file_to_object`/folder-logic/namespace rules would silently produce a
  **stale** index for any commit already marked `READY` before the change,
  since `is_index_ready` only checks the marker's existence, never a content
  or algorithm version. No invalidation hook ties config changes to
  `ZAOG_OBJ_INDEX` rows in this scope.
- **Which operations need every file row**: none, by design — every read
  path here is filter-scoped (`select_rows_for_filter`'s `FOR ALL ENTRIES`).
  The only "every row" operation is `rebuild_index`'s own DELETE+full walk,
  which is unavoidable under the current all-or-nothing marker model (Q1/Q2).
## Cross-reference: filename mapping / folder-path helpers actually invoked
- [zcl_abapgit_filename_logic.clas.abap](../../src/objects/core/zcl_abapgit_filename_logic.clas.abap)
  `file_to_object` — namespace unescape (`#`→`/`), object type/name/ext split,
  `map_filename_to_object` (devclass/`.abapgit` dependent).
- `zcl_abapgit_folder_logic=>get_instance( )->package_to_path` — DEVC-only
  path resolution, called only in `get_files_for_filter`, not in the index
  build.
- [zcl_abapgit_repo_filter.clas.abap](../../src/repo/filter/zcl_abapgit_repo_filter.clas.abap)
  `apply_object_filter` — second, redundant `file_to_object` pass (Q3).
## Test-file coverage note (context only, not re-verified live)
[zcl_abapgit_ortec_obj_index.clas.testclasses.abap](../../src/ortec/git/zcl_abapgit_ortec_obj_index.clas.testclasses.abap)
(`ltcl_obj_index`) exercises: `marker_required_for_ready`,
`index_no_cross_commit_leak`, `ready_rejects_other_commit`/
`ready_accepts_exact_commit`, `index_chunk_boundary_ok` (chunk-size boundary
around `c_index_write_chunk_size = 30000`), `index_bulk_rows_preserved`,
`index_empty_no_match` — confirms the marker-gated all-or-nothing model and
chunked bulk MODIFY are the tested/intended current behavior, not accidental.