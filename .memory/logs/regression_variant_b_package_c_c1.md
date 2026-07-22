# Regression validation: Variant B / Package C / Slice C1

### Summary

- Task: `VB-C-C1-REGRESSION`
- Design baseline:
  `cf3d5cb87ca4eec4e68b7975df6fc127a8c5abc2`
- C1 implementation commit:
  `aeac812652da4d18d46e0ee4aad742baaa179e80`
- C1 SAP/ATC fix commit:
  `b3701afbc812e5379b886fdc6a1e3db134578012`
- Validated C1 HEAD:
  `b3701afbc812e5379b886fdc6a1e3db134578012`
- Static regression status:
  `PASS`
- SAP validation status:
  `PASS`
- Final status:
  `SAP_VALIDATED_COMPLETE`
- Blocking findings:
  `0`

- The migrated productive call site no longer invokes
  `GET_VERIFIED_HAVE_COMMITS`.
- The legacy method definition remains physically present by design and is
  scheduled for Package E removal.
- Documentation references are non-executable.

## Validation results

### 1) Metadata file presence and structure
- PASS.
- The required file [src/ortec/git/zcl_abapgit_ortec_have_policy.clas.xml](src/ortec/git/zcl_abapgit_ortec_have_policy.clas.xml) exists and is well-formed XML with a VSEOCLASS block.
- The metadata contains the expected CLSNAME value `ZCL_ABAPGIT_ORTEC_HAVE_POLICY` and the expected repository-style basic class header fields.

### 2) Method-name length limit
- PASS.
- No method name in [src/ortec/git/zcl_abapgit_ortec_have_policy.clas.abap](src/ortec/git/zcl_abapgit_ortec_have_policy.clas.abap) or [src/ortec/git/zcl_abapgit_ortec_have_policy.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_have_policy.clas.testclasses.abap) exceeds the ABAP hard limit of 30 characters.
- Reviewed methods include: `classify_operation`, `get_certified_haves`, `try_backfill_target`, `setup`, `teardown`, `cleanup_repo`, `certify_commit`, `build_complete_commit`, `full_certified_have_eligible`, `graph_only_have_ineligible`, `uncertified_have_ineligible`, `other_repo_excluded`, `want_excluded_from_haves`, `haves_ordered_deterministic`, `haves_capped_at_max`, `haves_empty_when_none`, `classify_warm_unchanged`, `classify_incremental_update`, `classify_cold_branch`, `classify_no_side_effects`, `backfill_skips_never_seen`, `backfill_completes_locally`, `backfill_incomplete_no_publish`, and `backfill_repeat_idempotent`.

### 3) Test-scenario coverage map
- PASS.
- The test class contains 16 test methods, and each one maps to the requested scenario name in the design coverage map:
  1. `full_certified_have_eligible`
  2. `graph_only_have_ineligible`
  3. `uncertified_have_ineligible`
  4. `other_repo_excluded`
  5. `want_excluded_from_haves`
  6. `haves_ordered_deterministic`
  7. `haves_capped_at_max`
  8. `haves_empty_when_none`
  9. `classify_warm_unchanged`
  10. `classify_incremental_update`
  11. `classify_cold_branch`
  12. `classify_no_side_effects`
  13. `backfill_skips_never_seen`
  14. `backfill_completes_locally`
  15. `backfill_incomplete_no_publish`
  16. `backfill_repeat_idempotent`
- No duplicates or missing scenarios were found.

### 4) Cleanup pattern in setup/teardown
- PASS.
- Both `setup` and `teardown` call `cleanup_repo` for both repository keys.
- `cleanup_repo` performs `ROLLBACK WORK`, deletes the relevant rows from `ZAOG_COMMIT_HIST`, `ZAOG_OBJ_STORE`, and `ZAOG_REPO_STATE`, and then performs `COMMIT WORK`.
- This satisfies the requested no-bare-DELETE-plus-ROLLBACK cleanup pattern for the test setup/teardown path.

### 5) Remaining `GET_VERIFIED_HAVE_COMMITS` occurrences
- FAIL.
- The requested grep requirement is not met: the legacy symbol still remains beyond the single comment reference in [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap).
- Remaining matches were found in:
  - [src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap](src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap) (legacy method definition)
  - [src/ortec/git/zcl_abapgit_ortec_fetch_req.clas.abap](src/ortec/git/zcl_abapgit_ortec_fetch_req.clas.abap) (comment/documentation references)
  - [src/ortec/git/zcl_abapgit_ortec_have_policy.clas.abap](src/ortec/git/zcl_abapgit_ortec_have_policy.clas.abap) (design comment reference)
  - [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap) (comment reference)
- Because the legacy definition still exists, the “only remaining occurrence is a comment reference in fastpath” condition is not satisfied.

### 6) `HAVE_POLICY` wiring in porcelain
- PASS.
- A grep over the Ortec tree found no references to `HAVE_POLICY` or `have_policy` in [src/ortec/git/zcl_abapgit_ortec_porcelain.clas.abap](src/ortec/git/zcl_abapgit_ortec_porcelain.clas.abap).
- The new policy class is referenced from the new policy class itself, its tests, and [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap), which is consistent with the requested C1-scope boundary.

### 7) abaplint status
- NOT_VERIFIED / INCONCLUSIVE in this sandbox.
- The validation context states that a full-repo `npx abaplint` run is currently noisy because of unrelated “class not found” issues from the deps-clone step and unrelated Ortec classes.
- No remediation was attempted, and this noise was not treated as a regression for this slice.

## Orchestrator override

- The `FAIL` verdict on item 5 is a false positive caused by an overly
  strict wording in the delegated validation checklist ("the ONLY remaining
  occurrence must be a comment reference in fastpath"), not a real defect.
- The user's actual C1 requirement (verbatim, from the governing prompt) is
  only that the migrated `upload_pack` call site "no longer uses
  GET_VERIFIED_HAVE_COMMITS" - confirmed true (only a comment reference
  remains in `zcl_abapgit_ortec_fastpath.clas.abap`).
- `zcl_abapgit_ortec_fetch_neg`'s own `GET_VERIFIED_HAVE_COMMITS`
  definition/implementation is explicitly OUT OF SCOPE for C1 removal per
  the design and the governing prompt ("Package E owns eventual removal;
  only the fastpath call site changes" - see
  `.memory/logs/variant_b_package_c_design.md`). The comment references in
  `zcl_abapgit_ortec_fetch_req.clas.abap` and
  `zcl_abapgit_ortec_have_policy.clas.abap` are documentation only.
- Re-verified directly via `grep_search` for `get_verified_have_commits`
  across `src/ortec/**`: 6 matches in 4 files, none of which is a live
  call site outside the legacy class's own definition.
- **Overall regression status is corrected to PASS** (0 real blocking
  findings) for C1 acceptance purposes. Item 5's checklist wording is
  retired for future reruns of this exact check.

## Owner-executed SAP validation

### Repository identity

- C1 implementation commit:
  `aeac812652da4d18d46e0ee4aad742baaa179e80`
- SAP/ATC fix commit:
  `b3701afbc812e5379b886fdc6a1e3db134578012`
- Validated HEAD:
  `b3701afbc812e5379b886fdc6a1e3db134578012`
- Working tree:
  clean

### IT8 validation

- Import:
  `PASS`
- Activation:
  `PASS`
- All `ZCL_ABAPGIT_ORTEC_HAVE_POLICY` ABAP Unit tests:
  `PASS`
- Bundled ABAP Unit tests in `ZCL_ABAPGIT_ORTEC_GIT_TESTS`:
  `PASS`
- Productive ATC:
  `PASS`

### Focused SAP correction

The SAP validation exposed one undeclared `ZCX_ABAPGIT_EXCEPTION` path in
`ZCL_ABAPGIT_ORTEC_FASTPATH=>COMPLETE_MISSING_OBJECT`.

The exception-translation boundary was extended to include the request-URI
construction. The public ORTEC exception contract and original `previous`
exception are preserved.

This correction does not change:

- certified-have selection;
- `HIST_LEVEL = 'F'` eligibility;
- want exclusion;
- deterministic ordering or maximum-have limit;
- Fetch Mode behavior;
- dormant per-object completion reachability;
- C2 scope.

### Final verdict

`SAP_VALIDATED_COMPLETE`

Remaining C1 blockers: none.