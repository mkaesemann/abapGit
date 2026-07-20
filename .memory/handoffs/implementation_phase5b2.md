# Handoff — Implementation Phase 5b.2: capability negotiation + fail-safe cascade + decode-failure cleanup (2026-07-12)

## Prerequisite / context
Phase 5a (`zcl_abapgit_ortec_delta` + OFS decode plumbing) and Phase 5b.1 (completeness
verification gate APIs) were already landed as additive prerequisites. The approved target
for this slice was `target_design_phase5.md` §4/§5/§6: wire the completeness gate into live
capability negotiation, add a conservative fail-safe transport cascade, and close the
remaining temp-row orphan risk on catchable decode/resolve failures.

Implementation followed the established instruct/check/correct delegation pattern: a
`MAI-Code-1-Flash` subagent produced the initial exact-spec patch, then review/correct passes
closed two real defects before sign-off (details below).

## What was implemented (Phase 5b.2)
- `src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap`
  - `decode_and_persist` catch block for `zcx_abapgit_exception` now performs full cleanup
    before re-raising:
    - delete temp `zaog_obj_store` rows (`status = 'P'`) for repo_key+pack_id,
    - delete all `zaog_pack_idx` rows for repo_key+pack_id,
    - delete `zaog_pack_meta` row for repo_key+pack_id,
    - call `zcl_abapgit_ortec_pack_raw=>delete` in its own swallowing TRY/CATCH,
    - call `fail_session` if a session was created,
    - `COMMIT WORK` before `release_repo_lock` and re-raise.
  - Effect: catchable decode/resolve failures now leave the persistent store exactly as if the
    attempt never started (no orphan temp rows for a later resume attempt).
  - Scope guard preserved: crash/timeout failures that never reach this catch block are
    unchanged and continue to use the existing resume-on-crash mechanism.

- `src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap`
  - `upload_pack` gained `iv_allow_thin TYPE abap_bool DEFAULT abap_false`.
  - Have resolution moved before the want/capability-string loop.
  - New thin advertisement gate:
    - `lv_advertise_thin = xsdbool( iv_allow_thin = abap_true AND lt_ortec_haves IS NOT INITIAL )`.
    - `lt_ortec_haves` comes from `get_verified_have_commits` when thin is requested,
      otherwise existing `get_have_commits` logic is used.
    - Capability string includes `thin-pack ofs-delta` only when `lv_advertise_thin = abap_true`;
      otherwise remains unchanged.
  - `upload_pack_by_branch` and `upload_pack_by_commit` now implement the approved 3-tier
    fail-safe cascade:
    - try 1: `upload_pack(... iv_allow_thin = abap_true)`
    - try 2 on failure: retry once with `iv_allow_thin = abap_false` (fresh HTTP client)
    - fail both: raise `zcx_abapgit_ortec_git` including both failure texts so the existing
      transport-level `CATCH zcx_abapgit_ortec_git` falls through to standard fetch.

- `src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap`
  - Added `cleanup_after_decode_failure` in existing `ltcl_pack_decoder`.
  - Test shape:
    - build a real pack via `zcl_abapgit_git_pack=>encode`,
    - corrupt only the final byte of the trailing 20-byte pack SHA1 trailer,
    - call `decode_and_persist` expecting `zcx_abapgit_exception`,
    - assert zero residual rows for the test repo key in `zaog_obj_store`,
      `zaog_pack_idx`, `zaog_pack_meta`, and `zaog_raw_pack`.
  - This intentionally exercises failure after objects were parsed/persisted but before a
    successful trailer-validated completion.

## Key design/review decisions
- Cascade exception scope had to catch BOTH `zcx_abapgit_ortec_git` and
  `zcx_abapgit_exception`.
  - Why: `upload_pack`'s existing internals can swallow `decode_and_persist`
    `zcx_abapgit_exception` and fall back to standard `zcl_abapgit_git_pack=>decode`, which
    cannot decode OFS_DELTA and raises `zcx_abapgit_exception` uncaught at that level once
    thin/ofs is active.
  - Review found this exact gap in the first draft (which caught only
    `zcx_abapgit_ortec_git`) and it was corrected before sign-off; independently confirmed by
    `ortec-abapgit-regression`.

- Final cascade re-raise message includes both thin and non-thin failure texts.
  - This fixed two real `unused_variables` abaplint findings from the first draft and also
    improved post-failure diagnosability without changing control flow.

## Validation performed
- Independent regression validation: PASS_WITH_NOTES.
  - Full report: `.memory/logs/regression_phase5b2.md`.
  - Verified in that review:
    - thin/ofs is never advertised without explicit allow-thin plus a verified-complete have,
    - fail-safe order is thin Ortec -> non-thin Ortec -> standard,
    - catch-path cleanup is scoped to catchable decode/resolve failures and does not conflict
      with crash/timeout resume semantics,
    - `status = 'R'` promotion remains after trailer check + successful delta resolution.
- abaplint before/after reconciliation on touched files:
  - raw count appeared +20 due to line shifts,
  - per-rule/line reconciliation showed only 2 genuinely new findings (`unused_variables`),
    both fixed in-slice.

## Outstanding / not done yet
- This slice is not yet committed.
- Real ABAP Unit execution is still not done in this session due to the same unrelated
  pre-existing local transpile-harness dependency drift.
- ATC follow-up remains outstanding because arc-1 connectivity was unreachable when last
  checked:
  - prior commits `b6b8372a` and `1b0dccc5` still need ATC via arc-1,
  - this Phase 5b.2 slice needs the same ATC check after import into IT8 once arc-1 is
    reachable.

## Next recommended step
Commit this Phase 5b.2 slice (using the established functionality-first commit-message
convention), import into IT8 for syntax check, then run arc-1 ATC for `b6b8372a`, `1b0dccc5`,
and Phase 5b.2 once connectivity is restored.
