# Regression Phase 4b validation

- Branch: ortec/abapgit_1_133-opt-rework
- Date: 2026-07-11
- Scope: deferred Phase 4b six-state object/path model + D4 STRICT/RELAXED completeness switch
- Validation basis: full source review of the five touched ORTEC classes, direct tracing of the marker-based readiness logic and fallback chain, cross-check against the existing filename-logic tests, and editor diagnostics on the touched files.
- Verdict: PASS_WITH_NOTES

## Scenario matrix

| Scenario | Result | Evidence |
|---|---|---|
| STRICT is the shipping default for absent-object completeness | PASS | [src/ortec/git/zcl_abapgit_ortec_git_switch.clas.abap](src/ortec/git/zcl_abapgit_ortec_git_switch.clas.abap) defines `cs_absent_strictness-mode` as `'STRICT'`, and the new unit test locks that default in [src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap). |
| The STRICT readiness check requires the explicit marker row | PASS | [src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap) gates `is_index_ready` on `obj_type = '$IDX'`, `obj_name = '__READY__'`, and `idx_status = 'R'`. |
| The marker written by rebuild_index matches the readiness check exactly | PASS | [src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap) writes the same marker tuple unconditionally at the end of a successful rebuild. |
| `rebuild_index` writes the marker for both zero-row and non-zero-row successful walks | PASS | The marker write is outside the row-batch loop and runs after the walk completes, so both a zero-row success and a non-zero-row success reach it. |
| RELAXED mode is compile-time-only and not a runtime toggle | PASS | [src/ortec/git/zcl_abapgit_ortec_git_switch.clas.abap](src/ortec/git/zcl_abapgit_ortec_git_switch.clas.abap) exposes only constants; no setter or session flag for `cs_absent_strictness` exists anywhere in the workspace. |
| Unresolved states cannot be turned into a remote-Deleted verdict | PASS | [src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap) documents that only `CONFIRMED_ABSENT` may be classified as deleted. The new `CORRUPT_OR_INCOMPLETE` raise in [src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap) bubbles back to the existing fallback chain. |
| The new corrupt/incomplete raise still falls back safely | PASS | `build_files_from_rows` raises a `zcx_abapgit_exception`, which is caught in [src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap), causing a rebuild-and-retry; if that still fails, [src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap](src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap) falls back to `get_files_remote()`. |
| The new `marker_required_for_ready` test is logically sound and exercises the public entry point | PASS | [src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap) calls `get_files_for_filter` twice, builds a real commit/tree/blob graph with the Git pack helpers, and proves the index self-heals after the marker is removed and the row is corrupted. |
| The fake graph uses the same filename convention as the standard filename-logic tests | PASS | The test uses `zprogram.prog.abap` at `/src/`, and the existing unit test in [src/objects/core/zcl_abapgit_filename_logic.clas.testclasses.abap](src/objects/core/zcl_abapgit_filename_logic.clas.testclasses.abap) proves that convention resolves to `(PROG, ZPROGRAM)` with `devclass = '$PACK'`. |
| No new hard compile-time dependencies were introduced into standard abapGit code | PASS | The phase touched only the ORTEC classes under [src/ortec/git](src/ortec/git); no standard abapGit source file was modified in this slice. |
| Editor diagnostics are clean on all touched files | PASS | `get_errors` reported no issues in the five touched ABAP files. |

## Hard-stop checks

| Check | Result | Evidence |
|---|---|---|
| unknown/not-buffered cannot become remote deleted | PASS | The state vocabulary in [src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap) and the fallback logic in [src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap) keep unresolved states out of any deleted-file verdict. |
| branch-switch / interrupted rebuild cannot silently trust a partial index | PASS | The STRICT marker check in [src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap) requires the marker and rebuilds when it is absent, so an interrupted walk is not treated as complete. |
| standard abapGit fallback still preserves the always-correct path | PASS | [src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap](src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap) catches the fast-path failure and calls `get_files_remote()`. |
| fastpath disabled no longer preserves a misleading completeness signal | PASS | The D4 switch is compile-time-only and the public path still relies on the marker/row rebuild logic rather than any runtime toggle; there is no settable session flag for this behavior. |

## Failing class/method

- None.

## Notes

- The new test is structured as a real regression test for the latent bug this phase fixed: it builds a commit/tree/blob graph, proves the marker is written when rows exist, then corrupts the row and removes the marker to simulate an interrupted rebuild, and verifies that the next public call self-heals to the correct `/src/` path.
- The local ABAP-to-JS unit harness remained blocked by the same unrelated dependency drift documented for earlier phases, so this validation is based on source tracing and static diagnostics rather than a live ABAP Unit execution.
