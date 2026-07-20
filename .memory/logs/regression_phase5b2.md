# Regression Phase 5b.2 validation

- Branch: `ortec/abapgit_1_133-opt-rework`
- Date: 2026-07-12
- Scope: Phase 5b-2 slice for capability negotiation (thin-pack/ofs-delta), fail-safe 3-try cascade, and decode-failure cleanup.
- Validation basis: direct source review of the three touched production/test classes, diff review against HEAD, editor diagnostics on the touched files, and tracing of the existing transport fallback path.
- Verdict: PASS_WITH_NOTES

## Scenario matrix

| Scenario | Result | Evidence |
|---|---|---|
| Thin-pack/ofs-delta capability is advertised only when thin is explicitly allowed and a verified-complete have exists | PASS | In [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap), `lv_advertise_thin` is computed as `xsdbool( iv_allow_thin = abap_true AND lt_ortec_haves IS NOT INITIAL )`. The only source of `lt_ortec_haves` is `get_verified_have_commits` when thin is requested, otherwise `get_have_commits`. Any exception in the verification step is caught and leaves the table empty, so the capability cannot be advertised on a false-positive path. |
| The 3-try cascade executes thin Ortec → non-thin Ortec → standard fetch in the expected order | PASS | Both [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap) methods `upload_pack_by_branch` and `upload_pack_by_commit` try `upload_pack(... iv_allow_thin = abap_true )` first, then retry with `iv_allow_thin = abap_false`, and finally raise `zcx_abapgit_ortec_git` with both failure texts. The existing catch in [src/git/zcl_abapgit_git_transport.clas.abap](src/git/zcl_abapgit_git_transport.clas.abap) is a plain `CATCH zcx_abapgit_ortec_git`, so the final re-raise falls through to the standard fetch path exactly as intended. |
| Decode cleanup runs only for catchable decode/resolve failures and does not interfere with the crash/timeout resume path | PASS | In [src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap](src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap), the cleanup block is inside the outer `CATCH zcx_abapgit_exception` around `decode_and_persist`. A work-process crash/timeout would bypass that catch entirely and leave the temp rows/session/raw-pack state intact for the existing resume logic. The cleanup block therefore only runs for recoverable decode/resolve failures, not for an uncatchable process death. |
| No resolved rows are promoted to `status = 'R'` before the pack passes the trailer check and delta resolution succeeds | PASS | In [src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap](src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap), the pack trailer SHA1 is checked before the targeted base prefetch and before `zcl_abapgit_ortec_delta=>resolve_all`. The promotion loop that writes `status = 'R'` runs only after `resolve_all` has completed and the final rows have been assembled. A failure before that point leaves only temporary `status = 'P'` rows, which the cleanup path removes. |
| The new regression test exercises the cleanup path faithfully | PASS | The new test in [src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap) builds a real pack with `zcl_abapgit_git_pack=>encode`, corrupts only the last byte of the trailing SHA1 trailer, and then asserts that `decode_and_persist` raises `zcx_abapgit_exception` and that no rows remain in `ZAOG_OBJ_STORE`, `ZAOG_PACK_IDX`, `ZAOG_PACK_META`, or `ZAOG_RAW_PACK`. That is a faithful simulation of the exact failure point after temp rows have already been written and committed. |
| Static editor diagnostics are clean on the touched files | PASS | `get_errors` reported no issues for [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap), [src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap](src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap), and [src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap). |

## Hard-stop checks

- Thin advertisement gate: PASS. `lv_advertise_thin` is false unless both conditions hold, and the failure path leaves `lt_ortec_haves` empty.
- Cascade ordering: PASS. The thin attempt is always first, the non-thin retry is always second, and the final re-raise reaches the existing standard fallback handler in the transport layer.
- Cleanup semantics: PASS. Cleanup is scoped to the catchable failure path and does not run for an uncatchable crash/timeout, preserving the existing crash-resume mechanism.
- Promotion safety: PASS. The promotion to resolved rows happens only after the trailer check and delta resolution finish successfully.

## Failing class/method

- None.

## Notes

- No code changes were made during this validation pass.
- This is a source-level/static validation rather than a live ABAP Unit execution in the current environment; the implementation and the new regression test both line up with the approved Phase 5b-2 design intent.
