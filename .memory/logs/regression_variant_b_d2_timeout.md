# Regression validation: Variant B D2 TIME_OUT fix

## Summary

- Task: read-only regression validation for the Variant B D2 TIME_OUT
  incident fix (ensure_available -> materialize_missing_batches).
- Scope: `zcl_abapgit_ortec_missing_obj`, `zcl_abapgit_ortec_cold_init`
  (extraction only), plus the new/changed test files.
- Validation mode: source inspection, workspace diagnostics, and reasoning
  against unchanged call-site/signature evidence; no live SAP ABAP Unit or
  activation run was available in this environment.
- Result: PASS_WITH_FINDINGS (consistent with the SYSTEM_NO_ROLL fix's own
  prior regression verdict format for this same incident).

## Files reviewed

- [src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.abap](src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.abap)
- [src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.testclasses.abap) (new)
- [src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.xml](src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.xml)
- [src/ortec/git/zcl_abapgit_ortec_cold_init.clas.abap](src/ortec/git/zcl_abapgit_ortec_cold_init.clas.abap)
- [src/ortec/git/zcl_abapgit_ortec_cold_init.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_cold_init.clas.testclasses.abap)
- [src/ortec/git/zcl_abapgit_ortec_fetch_req.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_fetch_req.clas.testclasses.abap)
- [src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap) (unchanged — verified, see below)

## Evidence collected

- Workspace diagnostics (`get_errors`) reported zero errors on every
  touched/created file.
- `git status --short` / `git diff --stat`: only the intended files
  changed (`zcl_abapgit_ortec_missing_obj.clas.abap/.xml`,
  `zcl_abapgit_ortec_cold_init.clas.abap/.testclasses.abap`,
  `zcl_abapgit_ortec_fetch_req.clas.testclasses.abap`, plus the new
  `zcl_abapgit_ortec_missing_obj.clas.testclasses.abap` file and the
  design/review/scan/audit memory artifacts). No file in D1/D2/Package C
  scope, no standard `src/git/**` file, and no file from the already-
  committed SYSTEM_NO_ROLL fix (`zcl_abapgit_ortec_obj_store.clas.abap`/
  `.testclasses.abap`) was touched.
- Independent protocol/persistence review:
  [.memory/reviews/variant_b_d2_timeout_protocol_review.md](.memory/reviews/variant_b_d2_timeout_protocol_review.md)
  — `APPROVE_WITH_MINOR_REVISIONS`, 0 blocking findings, condition applied
  to the design doc.
- Independent performance DESIGN_GATE:
  [.memory/reviews/performance_design_variant_b_d2_timeout.md](.memory/reviews/performance_design_variant_b_d2_timeout.md)
  — `APPROVE_WITH_MINOR_REVISIONS`, 0 blocking findings, condition applied.
- Independent performance IMPLEMENTATION_AUDIT:
  [.memory/logs/performance_audit_variant_b_d2_timeout.md](.memory/logs/performance_audit_variant_b_d2_timeout.md)
  — `PASS`, 0 blocking, 0 major findings.

## Scenario matrix

| Scenario | Status | Evidence |
| --- | --- | --- |
| `ensure_available`'s external signature and 4-step shape (local check -> gate -> fetch -> local re-check) unchanged | PASS | Direct source read of the rewritten method; both real call sites (`zcl_abapgit_ortec_obj_index=>build_files_from_rows`, `zcl_abapgit_ortec_walk_prep=>topup_missing_blobs`) require no change. |
| The 3 existing `ensure_available` gate tests in the legacy aggregate class (`ltcl_missing_objects`: `noop_when_nothing_missing`, `no_fetch_without_url`, `no_fetch_when_opt_in_off`) keep passing unmodified | PASS (by inspection) | These tests exercise Step 1's short-circuit and the URL/opt-in gate, both byte-for-byte unchanged by this fix (only Step 2's fetch mechanism was replaced) — confirmed the legacy file itself was not touched by this diff (`git status` shows no change to `zcl_abapgit_ortec_git_tests.clas.testclasses.abap`). |
| New class-local `ensure_available` tests port the same 3 gate scenarios plus new fetch-narrowing/certification-isolation/mostly-shared-branch coverage | PASS | [zcl_abapgit_ortec_missing_obj.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.testclasses.abap), 9 tests, all HTTP-free by construction (either the short-circuit fires, or an early gate raises before any network call). |
| `materialize_tip_snapshot`'s external contract (begin_attempt -> [adaptive fetch] -> verify_ready_blobs -> may_publish_snapshot -> finalize_snapshot -> COMMIT WORK) unchanged after extracting its loop | PASS | Direct read of the refactored method; statement order, exception propagation, and the single trailing `COMMIT WORK` are all confirmed identical to the pre-extraction version — corrected once during implementation (an initial extraction misplaced the trailing `COMMIT WORK`/`ENDMETHOD` boundary; caught via `get_errors` + direct re-read before finalizing, per this project's own established self-check discipline for this exact risk class). |
| `materialize_missing_batches` never calls `begin_attempt`/`mark_full_complete`/`publish_snapshot_complete`/`prepare_full_snapshot` and issues no `COMMIT WORK` | PASS | Confirmed by direct read (protocol review §6, performance audit §6 both independently verified this). |
| Existing `cold_init` pure-helper test coverage (`take_batch_respects_limit`, `oversize_action_byte_limit`, `oversize_action_repeatable`, `adaptive_*`, `deduplicate_keeps_order`) already covers the algorithm `materialize_missing_batches` now shares — no duplicate tests added | PASS | Confirmed by direct read of the existing test file before adding new tests (per protocol review finding 1's correction). |
| New `capability_intersection` test fills the one previously-untested `MATERIALIZE_BLOBS` capability-fallback combination | PASS | [zcl_abapgit_ortec_fetch_req.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_fetch_req.clas.testclasses.abap), verified no duplicate of `materialize_wants_and_bounds`/`materialize_missing_capa_raise`. |
| SYSTEM_NO_ROLL fix (commit `2111b288`, `zcl_abapgit_ortec_obj_store.clas.abap`'s `get_reachable_objects`) and its own regression test (`reachable_ignores_extra_ready`) remain untouched | PASS | Confirmed via `git diff --stat` — this file does not appear in the current diff at all. |
| D1 bulk external delta-base resolution, D2 staged-visibility/attempt isolation, Package C F/C certification | PASS (untouched) | No file in either package's scope (`zcl_abapgit_ortec_delta`, `zcl_abapgit_ortec_pack_stream`, `zcl_abapgit_ortec_pack_dec`, `zcl_abapgit_ortec_fastpath`, `zcl_abapgit_ortec_mat_state`, `zcl_abapgit_ortec_repo_state`) appears in this diff. |
| ORTEC-disabled standard abapGit behavior | PASS (unaffected) | No file under `src/git/**` or any standard (non-`ortec_`) class was touched. |
| Live SAP activation / ABAP Unit execution | NOT RUN | No connected SAP system or live ABAP Unit runner was available in this environment — this is the owner's next action (IT8 retest, §15 of the design doc). |

## Failure analysis

- Failing class/method: none identified in this static pass.
- One process near-miss (not a shipped defect): during the
  `materialize_tip_snapshot` extraction, the first draft edit mis-split the
  method boundary and left a stray duplicate `ENDMETHOD`/misplaced
  `COMMIT WORK` fragment. This was caught immediately via `get_errors` (a
  syntax error) and a direct re-read before any further work proceeded, and
  is recorded here for transparency rather than silently corrected without
  a trace — the final, committed state has been independently re-verified
  by both the protocol review and the performance implementation audit,
  each of which explicitly confirmed the corrected method boundary.

## Corrective proposal

- None required for this validation pass.
- Next step: owner imports this checkpoint's commit (together with the
  already-pending `2111b288` SYSTEM_NO_ROLL fix, if not yet imported) into
  IT8 and runs the exact reproduction/retest sequence in
  `.memory/logs/variant_b_d2_timeout_fix_design.md` §15.

## Final local regression confirmation

- Regression verdict: PASS_WITH_FINDINGS (the sole "finding" is the
  inherent, pre-existing lack of a live HTTP mock seam preventing full
  end-to-end exercise of the adaptive fetch loop locally — not a defect in
  this diff).
- Blocking findings: 0.
- Protocol/persistence review verdict: APPROVE_WITH_MINOR_REVISIONS (conditions applied).
- Performance DESIGN_GATE verdict: APPROVE_WITH_MINOR_REVISIONS (conditions applied).
- Performance IMPLEMENTATION_AUDIT verdict: PASS.
- SAP validation: NOT RUN in this environment.
