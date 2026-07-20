# Regression Phase 1 validation

- Branch: ortec/abapgit_1_133-opt-rework
- Date: 2026-07-11
- Scope: Phase 1 regression fix + walk_tree repo_key correctness fix
- Validation basis: full source review of the five changed ABAP objects, editor diagnostics on the touched files, and the repository abaplint run.
- Verdict: PASS_WITH_NOTES

## Scenario matrix

| Scenario | Result | Evidence |
|---|---|---|
| Read-only filtered stage/diff lookup no longer depends on the fastpath opt-in gate | PASS | [src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap](src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap) keeps the data-validity checks and falls back to standard remote file retrieval if they fail. The standard hooks in [src/repo/stage/zcl_abapgit_stage_logic.clas.abap](src/repo/stage/zcl_abapgit_stage_logic.clas.abap) and [src/ui/pages/diff/zcl_abapgit_gui_page_diff_base.clas.abap](src/ui/pages/diff/zcl_abapgit_gui_page_diff_base.clas.abap) now attempt the filtered walk first without a hard pre-gate. |
| Fallback chain remains intact | PASS | The helper in [src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap](src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap) still catches repository/validation failures and returns [src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap](src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap) standard remote-file resolution. The stage and diff hooks likewise keep TRY/CATCH fallback to the standard remote-file path. |
| Hard-stop: unknown/not-buffered does not become remote-deleted | PASS | The changed read-path logic does not classify unresolved state as deletion. It only falls back to the standard remote-file path when the Ortec fast-path conditions are not met, preserving the previous safe behavior. |
| Hard-stop: branch-switch/tree-not-found remains recoverable via retry/fetch | PASS | The existing repair path in [src/git/zcl_abapgit_git_porcelain.clas.abap](src/git/zcl_abapgit_git_porcelain.clas.abap) remains intact: pull-by-branch still retries after a walk failure by resetting the fetch commit and re-running the fetch/pull path. |
| Hard-stop: fastpath-disabled mode preserves standard behavior | PASS | With the opt-in gate removed from the read-only path, a disabled fastpath still resolves through the same standard fallback chain rather than breaking or changing semantics. |
| walk_tree repo_key fix preserves backward compatibility | PASS | [src/git/zcl_abapgit_git_porcelain.clas.abap](src/git/zcl_abapgit_git_porcelain.clas.abap) now accepts iv_repo_key as OPTIONAL and threads it through the recursive walk. Existing callers that do not pass it remain valid because the parameter is optional. |
| Regression test coverage for the repo_key isolation bug | PASS | The new unit test in [src/git/zcl_abapgit_git_porcelain.clas.testclasses.abap](src/git/zcl_abapgit_git_porcelain.clas.testclasses.abap) explicitly proves that walk_tree uses the explicit repo_key instead of a stale session-level cache from a different repository. |

## Hard-stop checks

- unknown/not-buffered becomes remote deleted: PASS
- branch switch can still trigger tree-not-found after retry/fetch: PASS
- standard abapGit fallback hides a performance regression: PASS
- fastpath disabled no longer preserves standard behavior: PASS

## Notes

- Editor diagnostics reported no issues in the five changed files.
- The repository abaplint run still exits non-zero because of long-standing repo-wide style debt and unrelated baseline issues; no new issues were reported in the touched files.
- The full npm unit run remains blocked by unrelated dependency drift against the fresh open-abap-core/open-abap-gui snapshot and was not treated as a Phase 1 regression.

## Failing class/method

- None.
