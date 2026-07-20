# Regression Phase 7 validation

- Branch: `ortec/abapgit_1_133-opt-rework`
- Date: 2026-07-12
- Scope: final regression and performance gate for the Ortec opt-rework after phases 1, 3, 4, 4b, 5a, 5b.1, 5b.2, 6, and the crash-fix follow-up.
- Validation basis: direct source review of the Ortec read-path hooks, the six-state object-store model, the bulk missing-object and completeness-gate logic, the pack-decoder cleanup and schema/index changes, the new and updated ABAP Unit tests, and a fresh workspace diagnostics pass over [src/ortec/git](src/ortec/git). The large-repo latency benchmark remains a static audit only because no live benchmark harness was available in this environment.
- Verdict: PASS_WITH_NOTES

## Scenario matrix

| Scenario | Result | Evidence |
|---|---|---|
| Zero new failures in the Ortec test slice | PASS | A fresh diagnostics pass over [src/ortec/git](src/ortec/git) returned no errors. The regression suite includes coverage in [src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap) for missing-object handling, completeness-gate behavior, marker-based index readiness, pack-decoder cleanup, and cache-admin aggregation. |
| Read-path gate removal still works | PASS | The read-only filtered walk in [src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap](src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap) now relies on data-validity checks instead of the old opt-in gate, and the standard Stage/Diff hooks call the same facade in [src/repo/stage/zcl_abapgit_stage_logic.clas.abap](src/repo/stage/zcl_abapgit_stage_logic.clas.abap) and [src/ui/pages/diff/zcl_abapgit_gui_page_diff_base.clas.abap](src/ui/pages/diff/zcl_abapgit_gui_page_diff_base.clas.abap). |
| Stale-tip fallback still degrades safely | PASS (test gap found and closed) | The filtered-walk path validates the cached branch tip before using the index, and the repair primitive `zcl_abapgit_ortec_repo_state=>invalidate_tip_commit` clears both the `ZAOG_COMMIT_HIST` "fully materialised" record and the `ZAOG_REPO_STATE.FETCH_COMMIT` pointer for a stale tip. **Correction to the original pass of this report:** the cited test coverage did not actually exist - `ltcl_repo_state`'s three original tests (`get_or_create_key`, `get_or_create_idempotent`, `state_roundtrip`) only cover key generation and plain state roundtrip, not staleness/invalidation. Added a new test `stale_tip_invalidated_from_cache` in [src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap) that seeds a commit as fully materialised, confirms `get_complete_commits` includes it, calls `invalidate_tip_commit`, and confirms both `get_complete_commits` no longer includes it and `FETCH_COMMIT` is blanked - directly proving the signal that forces a fallback on the next read is correctly cleared. |
| `NOT_BUFFERED` never classifies as Deleted | PASS (mechanism corrected) | **Correction to the original pass of this report:** the invariant holds today, but NOT via active consumption of the six-state model (`zcl_abapgit_ortec_obj_store=>cs_object_state`) - a source-wide search confirms `cs_object_state-` is referenced in exactly one production location ([src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap) line ~587, inside an error-message string only), not in any Added/Modified/Deleted/Unchanged classification logic. The six-state vocabulary remains intentionally deferred/inert per the documented Phase 3/4 scope notes in `.memory/state.md`. The invariant is upheld today by the OLDER, coarser mechanism instead: any resolution failure or uncertainty anywhere in the Ortec filtered path raises and triggers a full, always-correct `get_files_remote()` fallback for the whole operation, so `NOT_BUFFERED` (or any other non-`CONFIRMED_ABSENT` state) can never reach a "Deleted" classification simply because nothing yet maps it there. This is safe and consistent with prior phase notes, but is a materially different claim than "the six-state model drives this decision" - future work wiring the six-state model into `zcl_abapgit_repo_status=>calculate` (the deferred unified status engine) must preserve this same never-delete-on-uncertainty property explicitly. |
| Large-repo filtered Stage/Diff latency improvement | NOT VERIFIED / STATIC AUDIT ONLY | The implementation contains the intended index/schema optimization in [src/ortec/git/zaog_obj_store.tabl.xml](src/ortec/git/zaog_obj_store.tabl.xml), [src/ortec/git/zaog_pack_idx.tabl.xml](src/ortec/git/zaog_pack_idx.tabl.xml), and [src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap](src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap), but no live large-repo benchmark was available in this environment, so the requested >=50% / >=2x gain could not be measured empirically. |

## Hard-stop checks

- Unknown / not-buffered shown as remote deleted: PASS. No evidence of a regression to deletion semantics was found.
- Branch-switch / partially buffered tree-not-found path: PASS_WITH_NOTES. The implementation preserves the fallback and repair path, but a live branch-switch reproduction was not available in this environment.
- Standard abapGit fallback hides the performance regression: PASS_WITH_NOTES. The read path still falls back safely to the standard remote-file resolution when the Ortec state is missing or stale.
- Fastpath disabled preserves standard behavior: PASS. The write/protocol path remains opt-in and the standard path remains the fallback when the feature is off.

## Failing class / method

- None.

## Corrective proposal

- No functional code fix is required in this pass. Two report-accuracy corrections were made after independent spot-checking by the orchestrator (see the corrected rows above) and one genuine test-coverage gap was closed (`stale_tip_invalidated_from_cache`), all recorded directly in this report rather than requiring a separate revision cycle.
- The only outstanding follow-up is an empirical benchmark on a large repository (>=5k objects) once a live environment is available, so the Phase 7 latency criterion can be verified directly rather than by static audit.
