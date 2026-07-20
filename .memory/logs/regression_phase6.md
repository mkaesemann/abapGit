# Regression Phase 6 validation

- Branch: `ortec/abapgit_1_133-opt-rework`
- Date: 2026-07-12
- Scope: Phase 6 implementation slice for large-repo index/schema optimisation.
- Validation basis: direct source review of the touched DDIC XML, the pack decoder change, the new cache-admin class/report, and the new regression test; diff review against baseline commit `46b3f398`; and a source-level where-used search confirming the admin class/report are not wired into the hot Stage/Diff/Patch/fetch path.
- Verdict: PASS_WITH_NOTES

## Scenario matrix

| Scenario | Result | Evidence |
|---|---|---|
| New secondary indexes are additive and structurally valid | PASS | In [src/ortec/git/zaog_obj_store.tabl.xml](src/ortec/git/zaog_obj_store.tabl.xml), the existing primary key remains `CLIENT + REPO_KEY + OBJ_SHA1` and the new index `STA` is an additional secondary index on `REPO_KEY + STATUS` only; no existing index or key was removed or reordered. In [src/ortec/git/zaog_pack_idx.tabl.xml](src/ortec/git/zaog_pack_idx.tabl.xml), the new secondary index `SHA` is added on `REPO_KEY + OBJ_SHA1` and the table previously had no secondary indexes. Both index names are unique within their table and the field order matches the stated implementation intent. |
| Bulk-prefetch change preserves the old WHERE semantics | PASS | In [src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap](src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap), the replacement `SELECT * FROM zaog_obj_store FOR ALL ENTRIES IN @lt_done_idx WHERE repo_key = @iv_repo_key AND pack_id = @iv_pack_id AND obj_sha1 = @lt_done_idx-obj_sha1 AND status = 'P'` matches the old per-row `SELECT SINGLE ... WHERE repo_key = iv_repo_key AND pack_id = iv_pack_id AND obj_sha1 = ls_done_idx-obj_sha1 AND status = 'P'` conditions exactly. |
| The new hashed lookup table is safe because `OBJ_SHA1` is unique per repo | PASS | [src/ortec/git/zaog_obj_store.tabl.xml](src/ortec/git/zaog_obj_store.tabl.xml) shows the table primary key is `CLIENT + REPO_KEY + OBJ_SHA1`. Because `REPO_KEY` is part of the primary key and the code uses `repo_key = @iv_repo_key`, the combination is unique for the lookup scope; a `HASHED TABLE ... WITH UNIQUE KEY obj_sha1` is therefore safe for the local in-memory cache of rows fetched for a single repo/pack resume pass. |
| The empty-driving-table guard prevents an invalid `FOR ALL ENTRIES` select | PASS | The new code in [src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap](src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap) wraps the bulk `SELECT` in `IF lt_done_idx IS NOT INITIAL`, so an empty driving table never triggers the `FOR ALL ENTRIES` path. |
| The cache-admin class is off the hot path and not called by Stage/Diff/Patch/fetch | PASS | A source search across [src/ortec/git](src/ortec/git) shows the only call sites for `zcl_abapgit_ortec_cache_admin` are the new report [src/ortec/git/zabapgit_ortec_cache_admin.prog.abap](src/ortec/git/zabapgit_ortec_cache_admin.prog.abap) and its new unit test in [src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap). No references were found in the hot-path classes named in the request: `zcl_abapgit_ortec_fastpath`, `zcl_abapgit_ortec_filter_walk`, `zcl_abapgit_ortec_git_facade`, `zcl_abapgit_ortec_git_stage`, `zcl_abapgit_ortec_git_patch`, or the standard hook classes. |
| `clear_repo` releases the enqueue lock on every exit path | PASS | In [src/ortec/git/zcl_abapgit_ortec_cache_admin.clas.abap](src/ortec/git/zcl_abapgit_ortec_cache_admin.clas.abap), `acquire_lock` is called before the `TRY` block, the `clear_repo_cache` call is wrapped in a `TRY ... CATCH`, and `release_lock` is called both in the exception handler and after the successful call. For the early-raise path (`acquire_lock` returns false), no lock was acquired and the method exits without a release call, which is correct. |
| The new report overview path is pure read/display | PASS | The report in [src/ortec/git/zabapgit_ortec_cache_admin.prog.abap](src/ortec/git/zabapgit_ortec_cache_admin.prog.abap) only calls `get_overview( )` and displays it via ALV when `p_clear` is false. It does not invoke any delete/update/write logic in that path. |
| The new regression test covers the overview aggregation behavior | PASS | The new test class `ltcl_cache_admin` in [src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap) seeds one row into each of the six relevant source tables and asserts that `get_overview( )` returns matching aggregated counts. |

## Hard-stop checks

- Secondary indexes: PASS. The new indexes are additive and do not change the existing key structure.
- Bulk-prefetch correctness: PASS. The new SELECT uses identical predicates to the old per-row lookup, and the empty-table guard is present.
- Hot-path isolation: PASS. The cache-admin entry points are not referenced from the requested hot-path classes.
- Lock cleanup: PASS. The lock is released on the success path and the exception path; the early-raise path is also correct because no lock was acquired.
- Report read-only path: PASS. The overview path is pure display and has no side effects.

## Failing class/method

- None.

## Notes

- This validation is source-level and static rather than a live execution of the new report or ABAP Unit suite in the current environment.
- The implementation matches the approved Phase 6 intent and does not introduce any obvious hot-path coupling or lock-lifecycle bug.
