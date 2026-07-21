# Variant B Slice 2 checkpoint handoff — sub-slices 2A+2B only

Status: checkpoint-ready. Sub-slice 2C (productive call-site migration) is
NOT implemented and NOT part of this checkpoint.

Scope: additive/inert fetch-mode model and pure request serializer only. No
productive call site (`zcl_abapgit_ortec_fastpath`, `zcl_abapgit_ortec_fetch_neg`,
`zcl_abapgit_ortec_filter_walk`) is touched or migrated in this checkpoint.

## Completed artifacts

- New class `ZCL_ABAPGIT_ORTEC_FETCH_REQ` (+ `.xml`):
  - `ty_fetch_mode` enum (`cs_fetch_mode`: `incremental_thin` (T),
    `incremental_self_contained` (S), `initial_branch_blobless` (B),
    `materialize_blobs` (M), `recovery_branch_full` (R)).
  - `build_request` — pure request serializer (zero SQL, zero HTTP).
  - `parse_capabilities` — single implementation of the capability-parsing
    logic currently duplicated inline in `zcl_abapgit_ortec_fastpath`'s
    `fetch_tip_commits`/`try_filtered_commit_fetch` (neither call site is
    migrated to call it yet).
  - `c_materialize_batch_max = 100` hard cap, enforced as a `RAISE`.
  - Private helpers `validate_single_want`, `build_want_lines`,
    `build_have_lines`.
- `ZCX_ABAPGIT_ORTEC_GIT` extension (additive only):
  - New READ-ONLY attributes `mv_unsupported_capability`,
    `mv_missing_capability`.
  - Constructor extended with 2 new DEFAULT-valued optional parameters —
    all existing call sites unchanged.
  - New `raise_unsupported_capability` class-method factory.
- New test class `ltcl_fetch_req` (in
  `zcl_abapgit_ortec_fetch_req.clas.testclasses.abap`), 18 test methods
  covering the wire-shape decision table for all 5 modes plus
  `parse_capabilities` edge cases.

## Review/gate verdicts (this checkpoint)

- Design, correctness review, protocol/persistence review, performance
  `DESIGN_GATE`: all previously `APPROVE`/`APPROVE_WITH_MINOR_REVISIONS`,
  gate `CLOSED`, `AUTHORIZED_FOR_2A_2B` —
  `.memory/reviews/performance_design_variant-b-partial-clone_slice2.md`.
- Low-cost performance scan: PASS, no findings.
- Performance `IMPLEMENTATION_AUDIT`: `PASS`, AC1–AC4 all PASS —
  `.memory/logs/performance_audit_variant-b-partial-clone_slice2.md`.
- Regression validation: `PASS` (static/structural; live SAP
  import/activation/ABAP Unit execution pending — no SAP system connection
  used in this session). Exception API backward compatibility confirmed
  (all 4 original constructor params + `raise`/`raise_corruption`/
  `get_text`/`get_source_position` unchanged); zero diff on
  `zcl_abapgit_ortec_fastpath.clas.abap`, `zcl_abapgit_ortec_fetch_neg.clas.abap`,
  `zcl_abapgit_ortec_filter_walk.clas.abap`,
  `zcl_abapgit_ortec_mat_state.clas.abap`/`.clas.testclasses.abap`.

## Strict Slice 3 boundary — confirmed intact

- No new code calls `fetch_tip_commits` or `build_upload_pack_buffer`.
- No code path in `zcl_abapgit_ortec_fetch_req` can emit the literal
  substring `deepen` (grep-verified; only doc-comment prose mentions it).
- No productive caller has been migrated to the new serializer.
- Deferred Slice 3 preconditions (DR-001/DR-002/DR-004 second half, and the
  `collect_ancestor_haves` unbounded read) remain recorded in
  `.memory/state.md` and untouched by this checkpoint.

## Binding Slice 2C scope (not yet started)

- Reroute `zcl_abapgit_ortec_fastpath`'s `upload_pack`/`upload_pack_by_branch`/
  `upload_pack_by_commit`/`complete_missing_object`/`try_filtered_commit_fetch`
  to `zcl_abapgit_ortec_fetch_req=>build_request` + explicit modes; remove
  the progressive-deepen retry loops from the live flow (constants/methods
  themselves deleted in Slice 9).
- Add the DR-004 decode-local cache reset
  (`zcl_abapgit_ortec_pack_stream=>reset_completion_budget()`) immediately
  before the `RECOVERY_BRANCH_FULL` tier's attempt.
- Swap `zcl_abapgit_ortec_fetch_neg=>is_commit_complete`'s body to delegate
  to `zcl_abapgit_ortec_mat_state=>is_graph_have_eligible` (AC6 unit test).
- Each of the above requires its own performance scan + `IMPLEMENTATION_AUDIT`
  before the whole Slice 2 can close and regression can sign off.

## Checkpoint commit scope

- Include only: `src/ortec/git/zcl_abapgit_ortec_fetch_req.clas.abap`,
  `.clas.xml`, `.clas.testclasses.abap` (new); `zcx_abapgit_ortec_git.clas.abap`
  (additive diff); this handoff and the linked `.memory` logs/reviews.
- Do not include any Slice 2C work (none exists yet).
