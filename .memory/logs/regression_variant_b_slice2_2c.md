# Regression validation: Variant B Sub-slice 2C (call-site migration)

Scope: `.memory/logs/variant_b_slice2c_migration_map.md`. Static/structural
validation only - no live SAP system access in this pass.

## Verdict: `PASS` (static/structural)

## What changed

- `src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap`:
  - `upload_pack` (private): `iv_allow_thin`/`iv_force_full` replaced with
    `iv_mode TYPE zcl_abapgit_ortec_fetch_req=>ty_fetch_mode` +
    `iv_server_caps TYPE string OPTIONAL`; have resolution gated to
    `INCREMENTAL_THIN`/`INCREMENTAL_SELF_CONTAINED`; buffer construction
    delegated to `zcl_abapgit_ortec_fetch_req=>build_request`.
  - `upload_pack_by_branch`/`upload_pack_by_commit`: tier 1 →
    `INCREMENTAL_THIN`, tier 2 → `INCREMENTAL_SELF_CONTAINED`, tier 3 →
    exactly one `RECOVERY_BRANCH_FULL` attempt (progressive `DO ... TIMES`
    widening loop removed from the live flow); `get_cdata`/
    `parse_capabilities` added per connection;
    `reset_completion_budget()` added a second time immediately before the
    recovery attempt (DR-004).
  - `complete_missing_object`: migrated to `MATERIALIZE_BLOBS` with a
    1-element want list.
  - `try_filtered_commit_fetch`: migrated to `INITIAL_BRANCH_BLOBLESS`;
    inline capability-parse duplicate replaced by
    `zcl_abapgit_ortec_fetch_req=>parse_capabilities`; outer broad catch
    (DR-001 deferral) left unchanged - net `rv_applicable` behavior
    unchanged.
  - `fetch_tip_commits` untouched (Slice 3 boundary).
  - Legacy `build_upload_pack_buffer`, `first_progressive_deepen`,
    `next_progressive_deepen`, `c_progressive_*` constants left physically
    present, now unreachable from any live call site
    (`LEGACY_BUT_UNREACHABLE_AFTER_2C`, `DELETE_LATER_IN_SLICE_9`).
- `src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap`:
  - `is_commit_complete` body replaced with a single delegating call to
    `zcl_abapgit_ortec_mat_state=>is_graph_have_eligible` (O(1) certified
    read, was O(reachable-graph-size) tree walk). Signature and callers
    (`get_verified_have_commits`) unchanged.
- `src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap`:
  - `ltcl_completeness_gate`: `setup`/`teardown` now also clean
    `zaog_commit_hist`; `complete_true_when_ready`/`complete_true_without_index`
    updated to certify the test commit via
    `begin_attempt`/`mark_graph_complete` before asserting eligibility
    (required because certification, not object-store presence, now governs
    `is_commit_complete`); `complete_false_missing_object` comment updated to
    reflect the new semantics (same expected result, different reason);
    added `complete_false_uncertified` (object-store-complete but never
    certified → not eligible) and `complete_true_full_complete`
    (`FULL_COMPLETE` → eligible).
  - No other test class touched; `ltcl_fastpath_protocol` and
    `ltcl_filtered_fetch` remain structurally valid and untouched (they test
    methods/guards this sub-slice did not modify).

## Validation performed

- `get_errors`: clean on all three edited files.
- Repo-wide grep: `fetch_tip_commits` has exactly one live caller
  (`zcl_abapgit_ortec_branch_list.clas.abap`, pre-existing, unrelated to
  this sub-slice) - no new caller added.
- Repo-wide grep: `first_progressive_deepen`/`next_progressive_deepen` only
  referenced by their own declaration/implementation and pre-existing tests
  - no live call site.
- Repo-wide grep: `iv_allow_thin`/`iv_force_full` only remain inside the
  now-dead `build_upload_pack_buffer` and its own pre-existing tests.
- Repo-wide grep: no new `deepen`/`shallow` wire-token emission in any of
  the 5 migrated methods (only in `fetch_tip_commits`/
  `build_upload_pack_buffer`, both out of scope/legacy).
- Public call-site compatibility: every existing caller of
  `upload_pack_by_branch`/`upload_pack_by_commit`/`complete_missing_object`/
  `try_filtered_commit_fetch` confirmed unaffected (unchanged public
  signatures; only the private `upload_pack` helper's signature changed).
- New test method names checked against the 30-character ABAP identifier
  limit (`complete_false_uncertified` = 26, `complete_true_full_complete` =
  27); pre-existing over-30-char names elsewhere in the same test include
  (`buffer_sends_deepen_even_forced` = 31,
  `progressive_deepen_widens_and_caps` = 34) confirmed present at HEAD
  before this sub-slice - not introduced by 2C, flagged separately below.
- Low-cost performance scan (`ortec-abapgit-performance-scan`): `PASS`.
- Performance `IMPLEMENTATION_AUDIT` (`ortec-abapgit-performance-review`):
  `PASS`. Confirmed ≤3 HTTP upload-pack attempts per logical fetch (down
  from an unbounded-in-practice progressive cascade), have resolution
  correctly mode-gated, `reset_completion_budget()` bounded at 2 calls per
  logical fetch, `is_commit_complete` genuinely O(1),
  `collect_ancestor_haves`'s pre-existing unbounded read correctly
  untouched (tracked Slice 3 candidate, not this sub-slice's scope).

## Non-blocking observation carried forward (not introduced by 2C)

`buffer_sends_deepen_even_forced` (31 chars) and
`progressive_deepen_widens_and_caps` (34 chars) in
`ltcl_fastpath_protocol` already exceed the 30-character ABAP method-name
limit at HEAD, before this sub-slice's changes. Neither was touched by
2C. This is a pre-existing condition outside 2C's scope; flagged for
awareness, not fixed here (fixing it would be unrelated refactoring per the
prompt's "do not broaden the implementation" instruction). Recommend a
follow-up rename before these methods are ever re-touched, or before Slice
9's cleanup pass, whichever comes first - if this hasn't already caused a
live activation failure, it may indicate the true SAP limit differs from
30 in this scenario, or the file's real system state already differs from
this local source (verify before relying on either assumption).

## Pending (explicitly not performed in this pass)

- Import into the target SAP system (IT8).
- Activation of all affected objects.
- ABAP Unit execution (including the new/updated `ltcl_completeness_gate`
  tests).
- ATC static check against the live system.
