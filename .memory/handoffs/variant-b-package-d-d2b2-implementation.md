# Package D2b2 — lock + attempt orchestration (orchestrator-verified closeout)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=D2B2-LOCK-ATTEMPT-ORCHESTRATION
STATUS=PASS (verified by orchestrator; subagent's final reply was a stray
             mid-verification fragment, not the requested compact envelope)
```

Note: `ortec-abapgit-implementation-senior` completed the code changes but
its final chat reply was a truncated internal-verification fragment ("AC-3
confirmed... Now let's verify AC-5...") rather than the requested compact
return envelope — no proper self-report was produced. Per repeated past
incidents (user memory), this was NOT trusted at face value. The
orchestrator independently ran `git status`/`git diff --stat`, reviewed the
full diff of every changed productive and test file line-by-line, and ran
`get_errors` on all touched files before accepting this as PASS.

## Verified changes

- `zcl_abapgit_ortec_fastpath.clas.abap`:
  - `pull_by_branch` Phase-1b branch: `acquire_repo_lock` + `begin_attempt`
    called immediately before `resume_decode`, both wrapped in
    `TRY...CATCH zcx_abapgit_exception zcx_abapgit_ortec_git` (exact set
    raised by `acquire_repo_lock`/`begin_attempt` respectively — verified
    via direct read of `zcl_abapgit_ortec_mat_state.clas.abap`, no
    over/under-catch). `resume_decode` called with `iv_lock_held =
    abap_true` + the minted `iv_attempt_id` only when both lock and
    attempt succeeded; if the lock was acquired but attempt-mint failed,
    the lock is released immediately and no resume is attempted. Lock is
    released on every downstream path: the inner success path (before the
    `RETURN`), the inner `CATCH zcx_abapgit_exception` path (before its
    `RETURN`), and a final catch-all release right before "Phase 2" for
    the "resume ran but didn't match" / "resume failed and was silently
    caught" paths. Confirmed the lock span starts at `acquire_repo_lock`
    (not the earlier `branches()` HTTP lookup) and ends before Phase 2
    (AC-1).
  - `persist_pull_result`: gained optional `iv_attempt_id`; forwards
    caller-supplied id as-is to `persist_missing_objects`/
    `certify_fetched_commit`, or mints one internally via `begin_attempt`
    exactly once when not supplied (preserves both pre-existing external
    callers, `zcl_abapgit_git_porcelain.clas.abap:650` and
    `ltcl_ortec_git~persist_creates_state`, unmodified and untouched).
  - `persist_missing_objects`: gained optional `iv_attempt_id`, set on
    every row in its internal `lt_new` before the single `MODIFY ...
    FROM TABLE` (verified: change is inside `persist_missing_objects`,
    lines 1615–1678; a `git diff` hunk header showing `METHOD upload_pack.`
    as context was a git heuristic artifact, not an actual placement bug —
    confirmed via `grep` of all `METHOD` boundaries).
  - `certify_fetched_commit`: `iv_attempt_id` is now MANDATORY; its
    internal `begin_attempt` call was removed entirely (grep-verified: zero
    remaining `begin_attempt` calls in this method) — AC-3 confirmed. Its
    one in-file caller (`persist_pull_result`) always supplies an id
    (either caller-forwarded or freshly minted in the fallback branch).
- `zcl_abapgit_ortec_porcelain.clas.abap`: `pull_by_branch`'s
  `INCREMENTAL_UPDATE` branch gains its own independent
  `acquire_repo_lock`/`begin_attempt` immediately before the existing
  `persist_pull_result` call (guarded on `lv_ortec_repo_key IS NOT
  INITIAL`), same graceful-degrade `CATCH` pattern, lock released
  unconditionally right after the existing `TRY...CATCH
  zcx_abapgit_ortec_git` block (covers both success and the already-caught
  persistence-failure path). Span starts here, not at the preceding
  `upload_pack_by_branch`/`pull(...)` calls (AC-2).
- Confirmed via call-graph reasoning: `upload_pack_by_branch` (fastpath)
  internally calls fastpath's own `pull_by_branch` (Unit #1), which fully
  acquires+releases its own lock before returning, *before* control ever
  reaches porcelain's own Unit #2 lock acquisition later in the same
  request — the two units' lock spans are sequential, never nested, per
  design.

## Tests added (all verified, method names ≤ 30 chars)

`zcl_abapgit_ortec_fastpath.clas.testclasses.abap` (12 new): existing
`certify_fetched_commit` call sites in 4 pre-existing tests updated to
supply an explicit `iv_attempt_id` (now mandatory) — verified these are
mechanical, behavior-preserving updates only.
`stale_attempt_rejected`, `one_attempt_one_id`, `retry_gets_new_attempt`,
`certify_reuses_attempt`, `attempt_id_cross_table`, `porcelain_path_gets_lock`,
`lock_not_held_over_http`, `lock_timeout_falls_back`, `filtered_fetch_lock_ok`,
`missing_objects_has_id`, `resume_new_attempt_when_new`,
`resume_reuses_attempt` (documented `NOT_APPLICABLE` placeholder, per design
evidence that no attempt-id reuse concept exists for `resume_decode`).

`stale_attempt_rejected`'s assumption (that `mark_graph_complete` has a
real staleness guard) was independently re-verified by the orchestrator by
reading `zcl_abapgit_ortec_mat_state=>mark_graph_complete`'s actual body:
`IF ls_row-attempt_id <> iv_attempt_id. zcx_abapgit_ortec_git=>raise(
'... stale attempt ID ...' ). ENDIF.` — confirmed real, test is correct.

`zcl_abapgit_ortec_porcelain.clas.testclasses.abap` (3 new):
`fresh_pull_unit_atomic`, `fresh_pull_fail_no_publish`,
`lock_release_on_failure`. Also added a `cleanup_repo` helper +
`c_repo2` constant and wired `setup`/`teardown` to use the
rollback-then-delete-then-commit idiom (correct given
`zcl_abapgit_ortec_mat_state`'s writes issue their own `COMMIT WORK`).

Several tests are explicitly, honestly documented as adaptations rather
than true end-to-end drives of `pull_by_branch` (which requires a live
`zcl_abapgit_git_transport` HTTP round-trip with no mock seam in either
test class) — each such test instead exercises the exact same public
primitives (`acquire_repo_lock`/`release_repo_lock`/`begin_attempt`/
`certify_fetched_commit`/`persist_missing_objects`) in the same sequence
the productive code now uses, with the adaptation reasoning written inline
in the test's own doc comment. This is a reasonable, transparent choice
given the fixture limitation and was not treated as a blocking gap.

## Verification performed

- `git status --short` / `git diff --stat`: only `SOURCE_SCOPE` files
  changed (plus pre-existing D2a/D2b1 diffs and pre-existing untracked
  handoff files). No forbidden path touched.
- `get_errors` clean on `zcl_abapgit_ortec_fastpath.clas.abap`,
  `.testclasses.abap`, `zcl_abapgit_ortec_porcelain.clas.abap`,
  `.testclasses.abap`.
- Full `git diff` read and reasoned about line-by-line for both productive
  files (exception-set exhaustiveness cross-checked against
  `zcl_abapgit_ortec_mat_state`'s actual `RAISING` clauses; every lock
  acquire matched against every code-path release; call-graph reasoning
  for non-nesting between the two units) and both test files (spot-checked
  `stale_attempt_rejected`'s guarded behavior against real source, verified
  `cs_snap_state-complete` is a real constant).

## Outstanding process gaps

- Subagent did not produce a proper compact return envelope (this closeout
  file was written by the orchestrator after independent verification).
- `lock_not_held_over_http`, `lock_timeout_falls_back`,
  `porcelain_path_gets_lock`, `filtered_fetch_lock_ok`,
  `resume_new_attempt_when_new`, `fresh_pull_unit_atomic`,
  `fresh_pull_fail_no_publish`, `lock_release_on_failure` are all primitive-
  level proxy tests rather than true end-to-end drives of `pull_by_branch`,
  due to a genuine, pre-existing fixture limitation (no HTTP transport mock
  seam in either test class). This is a known test-coverage gap inherited
  from the existing test infrastructure, not introduced by D2b2, and is
  noted here for visibility rather than treated as a blocker.
