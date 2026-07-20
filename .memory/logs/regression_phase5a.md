# Regression Phase 5a validation

- Branch: `ortec/abapgit_1_133-opt-rework`
- Date: 2026-07-11
- Scope: Phase 5a decode-only slice for OBJ_OFS_DELTA support: new unified delta resolver, OFS varint decoding, pack-decoder integration, and new unit tests.
- Validation basis: direct source review of the four Phase 5a files, targeted tracing of the requested invariants (offset-varint math, dependency-ordered resolution, fail-safe error handling, field-symbol re-fetches after recursion, dead-code reachability, unchanged commits-only path, and the new test vectors), plus editor diagnostics on the touched files.
- Verdict: PASS_WITH_NOTES

## Scenario matrix

| Scenario | Result | Evidence |
|---|---|---|
| `get_offset` follows the git pack "offset encoding" with the mandatory `+1` bias | PASS | [src/ortec/git/zcl_abapgit_ortec_delta.clas.abap](src/ortec/git/zcl_abapgit_ortec_delta.clas.abap) implements `rv_offset = ( rv_offset + 1 ) * 128 + lv_int`, and the tests in [src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap) assert `80 7F -> 255`, `81 00 -> 256`, and `80 80 00 -> 16512`. |
| `resolve_one` resolves a delta chain before applying the dependent delta | PASS | The recursive `resolve_one` calls in [src/ortec/git/zcl_abapgit_ortec_delta.clas.abap](src/ortec/git/zcl_abapgit_ortec_delta.clas.abap) happen before the dependent object is applied, and the chain test in [src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap) proves object 3 resolves only after object 2 has already become a real blob. |
| Unresolvable bases raise instead of silently producing wrong data | PASS | The ref-delta and ofs-delta branches in [src/ortec/git/zcl_abapgit_ortec_delta.clas.abap](src/ortec/git/zcl_abapgit_ortec_delta.clas.abap) raise explicit exceptions for missing or still-unresolved bases, including `Delta base not found`, `Delta, base still unresolved`, and the OFS-specific offset/object-index errors. |
| Field-symbols are re-fetched after recursive calls that may reallocate `ct_objects` | PASS | Both recursive call sites in [src/ortec/git/zcl_abapgit_ortec_delta.clas.abap](src/ortec/git/zcl_abapgit_ortec_delta.clas.abap) re-read the base object and the dependent object after recursion, matching the comment about `APPEND`/reallocation risk. |
| The feature remains dead/unreachable in production today | PASS | The new logic is only wired into [src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap](src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap), and the existing Ortec fastpath still sends only the pre-Phase-5b capability string in [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap); no capability-negotiation changes were made in this slice. |
| The sibling commits-only decode path still has no OFS support and is unchanged | PASS | [src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap](src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap) leaves `decode_commits_only` on the standard ref-delta path and still calls `zcl_abapgit_git_delta=>decode_deltas`; it does not now receive OFS entries. |
| The new unit-test delta vectors are logically sound | PASS | The `apply_copy_and_insert` test in [src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap) decodes the exact `0x90`/`0x01` instruction stream to `Hello!`, and the chain test decodes `Hello!!` from the second-hop delta. |
| Static diagnostics are clean on all four Phase 5a files | PASS | `get_errors` reported no issues in [src/ortec/git/zcl_abapgit_ortec_delta.clas.abap](src/ortec/git/zcl_abapgit_ortec_delta.clas.abap), [src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap](src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap), [src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap), and [src/ortec/git/zcl_abapgit_ortec_delta.clas.xml](src/ortec/git/zcl_abapgit_ortec_delta.clas.xml). |

## Hard-stop checks

| Check | Result | Evidence |
|---|---|---|
| Spec-correct OFS varint decode | PASS | The implementation in [src/ortec/git/zcl_abapgit_ortec_delta.clas.abap](src/ortec/git/zcl_abapgit_ortec_delta.clas.abap) matches the official offset-encoding algorithm and the requested test vectors. |
| Dependency-ordered chain resolution | PASS | `resolve_one` resolves the base object before applying the dependent delta, including the chain case in [src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap). |
| Fail-safe handling for missing/unresolvable bases | PASS | Missing bases raise exceptions rather than silently continuing or leaving wrong data behind. |
| No new reachability or capability-negotiation path | PASS | No Phase 5b capability changes were made; the existing fastpath remains gated by the pre-existing opt-in and still does not advertise `thin-pack`/`ofs-delta`. |
| No regressions in the commits-only path | PASS | [src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap](src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap) leaves that method unchanged and still relies on the standard ref-delta path. |
| Static diagnostics clean | PASS | `get_errors` returned no issues for the four Phase 5a files. |

## Failing class/method

- None.

## Notes

- No code changes were made during this validation pass.
- This validation is source-level and static; no live ABAP Unit execution was run in this session.
- The slice is consistent with the accepted design boundary: decode-only, dead code, and explicitly out of scope for capability negotiation.
