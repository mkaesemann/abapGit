# Regression validation — Variant B Slice 1

Date: 2026-07-20
Scope: additive/inert Slice 1 for durable materialization state (new DDIC fields, isolated class `zcl_abapgit_ortec_mat_state`, new unit tests only).

## Sign-off
- Status: PASS
- Rationale: the slice is additive and inert, the implementation review audit is PASS, and no existing method body was changed. The only limitation is that the new class is not present in the connected SAP system yet, so live ABAP Unit execution could not be performed.

## 1) Live SAP verification
- Attempted live read of class `ZCL_ABAPGIT_ORTEC_MAT_STATE` via SAP read.
- Result: SAP returned `404 Resource CLASS ZCL_ABAPGIT_ORTEC_MAT_STATE does not exist`.
- Conclusion: the object has not been imported/activated into the connected target system yet, so live ABAP Unit execution is blocked for this slice.
- Static fallback: local diagnostics report `0 errors` for the implementation and test class files.

## 2) Static/manual trace of test methods
The following test methods were traced against the implementation logic in the class and should logically pass:

| Test method | Status | Evidence |
|---|---|---|
| `get_state_returns_initial` | PASS | `get_state` returns empty/default state when no row exists; the test asserts initial `hist_level`/`snap_state`. |
| `get_state_legacy_row` | PASS | `get_state` simply returns the row values; legacy rows with space-valued fields are preserved and the helper methods return `abap_false` for `graph/full` eligibility. |
| `begin_attempt_creates_row` | PASS | `begin_attempt` creates a row with `UNKNOWN/NONE`, sets a generated attempt ID, and `get_state` reads it back. |
| `begin_attempt_no_downgrade` | PASS | `begin_attempt` never downgrades existing `hist_level`; after `mark_graph_complete`, a fresh attempt leaves `GRAPH_COMPLETE` intact. |
| `begin_attempt_sets_pending` | PASS | When the existing state is `NONE/INVALID`, `begin_attempt` sets `PENDING`. |
| `begin_attempt_keeps_complete` | PASS | Once the snapshot is complete, a new attempt leaves `snap_state = COMPLETE`. |
| `mark_graph_complete_ok` | PASS | `mark_graph_complete` updates the row to `GRAPH_COMPLETE` and sets the eligibility helper to `abap_true`. |
| `mark_graph_complete_stale` | PASS | The method raises on a mismatched attempt ID; the test expects that exception. |
| `mark_graph_no_attempt` | PASS | When no row exists, the method raises; the test expects that exception. |
| `mark_graph_idempotent` | PASS | The method returns early when `hist_level` is already `GRAPH_COMPLETE` or `FULL_COMPLETE`, so a bogus attempt ID does not trigger a stale-try failure. |
| `publish_snapshot_ok` | PASS | After graph-complete, `publish_snapshot_complete` sets `snap_state = COMPLETE` and updates the repo row’s `fetch_commit`/`snap_state`. |
| `publish_snapshot_stale` | PASS | The method validates the attempt ID and raises on mismatch, matching the test. |
| `publish_from_unknown` | PASS | A snapshot cannot be published before the graph is certified; the method raises, as tested. |
| `mark_full_complete_ok` | PASS | `mark_full_complete` requires graph-complete and then upgrades to `FULL_COMPLETE`; the test checks full eligibility. |
| `mark_full_complete_stale` | PASS | The method checks attempt ID and raises on mismatch, matching the test. |
| `mark_full_requires_graph` | PASS | The method rejects a full-complete transition before graph-complete, matching the test. |
| `invalidate_commit_resets_row` | PASS | `invalidate_commit` resets `hist_level`, `snap_state`, and clears `attempt_id`, matching the test assertions. |
| `invalidate_commit_cascades` | PASS | The update targets repo rows with matching `repo_key` and `fetch_commit`; the test verifies the scoped cascade and the non-matching repo row is untouched. |
| `clean_attempts_clears_stale` | PASS | The bulk cleanup updates old rows by `repo_key`/age and clears `attempt_id`; the test checks that the stale row is cleaned. |
| `clean_attempts_keeps_fresh` | PASS | Recent attempts are left intact; the test checks that cleanup returns `0` and preserves the attempt ID. |

## 3) Repository grep check for existing readers/writers
- Grep across the repository for `zaog_repo_state-fetch_commit` and `zaog_commit_hist` found existing readers/writers in existing classes/tests, but the Slice 1 worktree diff only adds the new DDIC field extensions and the new class/test files.
- There are no edits to existing method bodies in the Slice 1 change scope; this matches the intended inert/additive design and the review resolution note.

## 4) Performance implementation audit verdict
- Reviewed `.memory/reviews/performance_design_variant-b-partial-clone_slice1.md`.
- Verdict: `PASS`.

## 5) ORTEC-disabled / standard abapGit behavior
- No existing method body changed and nothing is wired into the live fetch/branch-switch path in this slice.
- Therefore, ORTEC-disabled / standard abapGit behavior is unaffected by this slice.

## Final verdict
- Slice 1: PASS
- Most important finding: the new class is not yet present in the connected SAP system, so live ABAP Unit execution could not be performed; the static/manual trace and local diagnostics indicate the implementation and tests are internally consistent.
