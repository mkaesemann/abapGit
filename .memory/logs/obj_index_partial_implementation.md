## Slice 1/1b/1d gap-fill (orchestrator direct fix, post IMPL-A/IMPL-A2)
The `OBJ-PERF-IMPL-A`/`OBJ-PERF-IMPL-A2` senior-implementation subagent calls both returned
"Agent completed with no output" despite having made real, mostly-correct changes to
`zcl_abapgit_ortec_obj_index.clas.abap`/`.testclasses.abap`. Verification (per the new repo
memory note on this failure mode) found two real gaps, both fixed directly by the orchestrator
rather than via a third subagent retry:
1. `select_partial_rows_for_filter` was declared in the `PUBLIC SECTION` (design §3.0b) but had
   **no method implementation at all** - a hard "method not implemented" compile error that local
   `get_errors` did not flag. Implemented it: chunks `it_filter` at
   `zcl_abapgit_ortec_obj_cover=>c_filter_chunk_size` (5000), reads `ZAOG_OBJ_PIDX` with an
   explicit field list (its column order differs from `ZAOG_OBJ_INDEX` - `CONTEXT_HASH` is a real
   key field positioned between `OBJ_NAME` and `PATH_HASH`), binds `repo_key`/`commit_sha1`/
   `context_hash`/`idx_status` plus the filter's `obj_type`/`obj_name`, then projects the result
   into the identical `ty_index_rows_tt`/`zaog_obj_index` shape `build_files_from_rows` already
   consumes (zero changes needed there for either COMPLETE- or FILTERED-mode rows).
2. The 5 design-mandated new tests for Slice 1b/1d were entirely missing:
   `ready_rejects_different_context`, `select_rows_excludes_other_context`,
   `blank_legacy_context_is_never_ready`, `partial_rows_context_disjoint`,
   `select_partial_rows_chunk_boundary`. All 5 written directly, plus `setup`/`teardown` extended
   to also purge `ZAOG_OBJ_PIDX` test rows. `select_rows_excludes_other_context` is implemented as
   an end-to-end proof (via `get_files_for_filter`) that a context-B rebuild purges the whole
   commit's `ZAOG_OBJ_INDEX` rows, so context A's marker/rows can never be silently reused -
   confirming `select_rows_for_filter`'s lack of an explicit `context_hash` predicate is safe only
   because of that purge-before-rewrite invariant (per the design's AR-2-01 closure rationale), not
   because the column is checked at read time.
Verified via `get_errors` (0 errors, all touched/created files) and `grep_search` for each promised
symbol name. No live SAP/ADT tooling was available in this environment for this gap-fill pass (only
`OBJ-PERF-IMPL-A3` used live `SAPDiagnose`/`SAPRead` and found the DDIC objects not yet imported
there, as expected for local-only git work).
## Slice 1c (IMPL-A3, cache-admin lock unification)
TASK_ID=OBJ-PERF-IMPL-A3, BASELINE_COMMIT=4193733de3c8ad75bae61f6ceb95b1c3e0ccb3e4
Applied the exact "Cache admin invalidation" spec from
`.memory/logs/obj_store_performance_design.md` (AR-1-05 cleanup + AR-2-03 lock
unification, cycle 3).
### Files changed
- `src/ortec/git/zcl_abapgit_ortec_cache_admin.clas.abap`
  - `ty_clear_result` gains `obj_cover TYPE i` and `obj_pidx TYPE i`, placed
    directly after `obj_index`.
  - `clear_repo`: the three derived-filter-table deletes
    (`ZAOG_OBJ_INDEX`/`ZAOG_OBJ_COVER`/`ZAOG_OBJ_PIDX`) now run wrapped by
    `zcl_abapgit_ortec_pack_raw=>acquire_repo_lock`/`release_repo_lock`, at
    the very top of the existing `TRY` block. `release_repo_lock` runs
    before the (unchanged, further down) `DELETE FROM zaog_fetch_sess`
    statement, per the design's mandatory ordering invariant. The
    `CATCH cx_root` path is unchanged (it already wraps the mutex
    INSERT/deletes in the same LUW and re-raises as `zcx_abapgit_ortec_git`).
- `src/ortec/git/zcl_abapgit_ortec_cache_admin.clas.testclasses.abap`
  - `cleanup()` extended to also purge `ZAOG_OBJ_COVER`/`ZAOG_OBJ_PIDX` test
    rows for `c_repo`/`c_other_repo`.
  - New `CONSTANTS c_context_hash` and new private helper `seed_filter_rows`
    (seeds one `$IDX/__READY__` marker row in `ZAOG_OBJ_INDEX`, one
    `ZAOG_OBJ_COVER` row, one `ZAOG_OBJ_PIDX` row, all under one context
    hash).
  - New tests: `clear_repo_deletes_derived`, `clear_repo_then_filtered_read_rewalks`,
    `clear_repo_blocks_on_pack_lock` (see below).
### Tests added
- `clear_repo_deletes_derived`: seeds `ZAOG_OBJ_STORE`/`OBJ_INDEX`/`OBJ_COVER`/
  `OBJ_PIDX` rows for a repo, runs `clear_repo`, asserts `OBJ_INDEX`/
  `OBJ_COVER`/`OBJ_PIDX` are empty afterward and that
  `rs_result-obj_index`/`obj_cover`/`obj_pidx` reflect the deleted counts.
- `clear_repo_then_filtered_read_rewalks`: asserts `is_index_ready` is
  `abap_true` before `clear_repo` (fixture has the completion marker under
  the seeded context hash) and `abap_false` afterward, and that
  `ZAOG_OBJ_COVER`/`ZAOG_OBJ_PIDX` are empty post-clear — proving a
  subsequent filtered read cannot trust orphaned coverage. Scoped to the
  `is_index_ready`/coverage-table invariant directly rather than invoking
  the full `get_files_for_filter` (which can fall back to a live remote
  fetch) — see Deviations.
- `clear_repo_blocks_on_pack_lock`: holds
  `zcl_abapgit_ortec_pack_raw=>acquire_repo_lock` for the repo, invokes
  `clear_repo`, asserts it raises `zcx_abapgit_ortec_git` (the lock's own
  bounded ~5.15s retry ceiling expires) rather than proceeding, and that the
  seeded `ZAOG_OBJ_COVER` row was not deleted.
### Verification
- `get_errors` on both changed files: 0 errors.
- Live ADT/SAP tooling (`mcp_arc-12`) is reachable, but a
  `SAPDiagnose(action="syntax", source=...)` dry-run against the connected
  system failed with two errors: `ZAOG_OBJ_COVER`/`ZAOG_OBJ_PIDX` "is either
  not active or does not exist" (confirmed independently via
  `SAPRead(type="TABL", name="ZAOG_OBJ_COVER")` -> 404). These two DDIC
  tables are out of this task's scope (marked "ALREADY DONE" locally in
  the task prompt) but are not yet imported/activated on the connected
  live system, so live activation and `SAPDiagnose(action="unittest")` for
  `ZCL_ABAPGIT_ORTEC_CACHE_ADMIN` could not be executed this pass. Only
  static verification (`get_errors`) was possible for the Slice 1c changes
  themselves; no error was found in the actual Slice 1c logic (both syntax
  errors are solely the missing-table condition, not a defect in the new
  code).
### Deviations from the design
- `clear_repo_then_filtered_read_rewalks` verifies the underlying
  `is_index_ready` + coverage-table-empty invariant directly instead of
  calling `zcl_abapgit_ortec_obj_index=>get_files_for_filter` end-to-end,
  to avoid a unit test depending on that method's live-remote-fetch
  fallback path. This still proves the design's required behavior ("no
  false empty result from orphaned coverage") without a network dependency.