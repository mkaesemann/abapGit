# Regression Phase 4 validation

- Branch: ortec/abapgit_1_133-opt-rework
- Date: 2026-07-11
- Scope: bulk missing-object collection and targeted top-up fetch for the filtered Stage/Diff read path
- Validation basis: full source review of the six Phase 4 objects, direct tracing of the new safety gate and fallback path, editor diagnostics on the touched files, and targeted repository search for the new call sites.
- Verdict: PASS_WITH_NOTES

## Scenario matrix

| Scenario | Result | Evidence |
|---|---|---|
| Safety gate blocks any fetch when the remote URL is blank | PASS | [src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.abap](src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.abap) raises immediately before the network call at [src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.abap](src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.abap#L64-L67). |
| Safety gate blocks any fetch when the ORTEC opt-in is inactive | PASS | The same guard checks [src/ortec/git/zcl_abapgit_ortec_git_switch.clas.abap](src/ortec/git/zcl_abapgit_ortec_git_switch.clas.abap) and raises before the fetch at [src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.abap](src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.abap#L64-L67). |
| The only network call is reached only after the gate passes | PASS | The fetch call sits at [src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.abap](src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.abap#L74-L80), and the guard above it prevents entry when the preconditions are not met. |
| The filtered index builder uses the new top-up path only when both URL and commit are supplied | PASS | [src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap#L505-L515) gates the new call behind both optional parameters. |
| The top-up fetch is best-effort and never changes the existing miss-handling path | PASS | [src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap#L505-L525) catches the new helper failure and then falls through to the existing object-store lookup and raise logic. |
| Backward compatibility is preserved when optional parameters are not supplied | PASS | [src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap](src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap#L157-L174) leaves the compatibility call path unchanged, and [src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap#L474-L525) skips the new fetch block when the parameters are initial. |
| No double-fetch or infinite retry loop was introduced | PASS | [src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.abap](src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.abap#L45-L103) performs one local check, one remote fetch, and one retry-only re-check; there is no loop. |
| The new tests cover the no-fetch safety cases | PASS | [src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap#L239-L280) explicitly covers blank-URL and opt-in-off cases. |

## Hard-stop checks

- unknown/not-buffered becomes remote deleted: PASS
- branch switch can still trigger tree-not-found after retry/fetch: PASS
- standard abapGit fallback hides a performance regression: PASS
- fastpath disabled no longer preserves standard behavior: PASS

## Safety-gate trace

1. [src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.abap](src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.abap#L52-L57) performs the initial local bulk check only.
2. [src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.abap](src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.abap#L64-L67) raises immediately when the URL is blank or the repo is not opted in; this is the sole gate before any network call.
3. [src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.abap](src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.abap#L74-L80) is the only fetch site, so the gate is airtight.
4. [src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap#L505-L515) only invokes the helper when both URL and commit are present, and it swallows the helper failure so the pre-existing miss-handling path remains untouched.

## Failing class/method

- None.

## Notes

- Editor diagnostics reported no errors in the five touched ABAP files.
- The repository-level abaplint CLI invocation hit a tooling/config parsing issue unrelated to these objects, so the validation used direct source review and editor diagnostics for the functional safety checks.
- Orchestrator addendum (identified during implementation, not a hard-stop): `get_files_for_filter`'s outer stale-index recovery path can call `build_files_from_rows` a second time after a `DELETE FROM zaog_obj_index` + `ensure_index` rebuild if the first attempt raised. If that first failure was due to `ensure_available` exhausting its retry (objects genuinely still missing after one negotiated fetch), the second `build_files_from_rows` call will invoke `ensure_available` again for the same SHA1s, since it does not know the previous attempt already tried. This bounds the total negotiated-fetch attempts at 2 (never unbounded/looping) and is idempotent (same safe outcome), but is a minor inefficiency worth knowing about. Not fixed in this phase to avoid adding complexity for a rare double-failure edge case; flagged here for awareness in Phase 5/6 hardening.
