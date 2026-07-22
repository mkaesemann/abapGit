# ORTEC abapGit opt-rework — active state

- Repository / branch: abapGit on `ortec/abapgit_1_133-opt-rework`
- Topic: `variant-b-partial-clone`
- Owner-approved goal: Variant B partial-clone fetch orchestration with certified haves, cold blobless graph acquisition, current-tip blob materialization, and no per-object SQL/HTTP repair.
- Baseline / relevant commits: Slice 1 import/activation `10e75f850c3530fdf5de820fb5f4f5f1147359f6`; Slice 2A/2B SAP validation `96536177393e7d6650fdfe4487e8fa7f17ad4044` and ATC follow-up `6638cb6683e9b508b2e9144dfb9c80628ec23c98`.
- Current phase: `Package B B0: APPROVED_WITH_RESOLVED_REVISIONS`.
- Next phase: `Package B B1 - cold blobless graph acquisition (implementation)`.
- Current validated HEAD: `cbe8bc73f7c1e17e3d436b1371fcc65f806dd989`.
- Relevant commit chain: Slice 2C checkpoint `a2c4d155`; SAP syntax fix `246bae779f2504e50822a390e466dd652b7782f2`; post-IT8 resolver and ATC correction `90543197573601481430132e2e0b9677e9687be8`; base-cache hashed-key lookup fix `cbe8bc73f7c1e17e3d436b1371fcc65f806dd989`.

## Completed work
- Slice 0 — status: COMPLETE; SAP: N/A; handoff: `.memory/logs/variant_b_reconciliation.md`; regression: N/A; performance: N/A; blocker: none.
- Slice 1 — status: COMPLETE; implementation: `10e75f850c3530fdf5de820fb5f4f5f1147359f6`; SAP: PASS; handoff: `.memory/logs/variant_b_design.md`; regression: `.memory/logs/regression_variant_b_slice1.md`; performance: `.memory/reviews/performance_design_variant-b-partial-clone_slice1.md`; blocker: none.
- Slice 2A/2B — status: COMPLETE; implementation: `96536177393e7d6650fdfe4487e8fa7f17ad4044` / `6638cb6683e9b508b2e9144dfb9c80628ec23c98`; SAP: PASS; handoff: `.memory/handoffs/variant-b-slice2-2a2b-checkpoint.md`; regression: `.memory/logs/regression_variant_b_slice2_2a2b.md`; performance: `.memory/logs/performance_audit_variant-b-partial-clone_slice2.md`; blocker: none.
- Slice 2C — status: SAP_VALIDATED_COMPLETE; implementation chain: `a2c4d155` → `246bae779f2504e50822a390e466dd652b7782f2` → `90543197573601481430132e2e0b9677e9687be8` → `cbe8bc73f7c1e17e3d436b1371fcc65f806dd989`; SAP: PASS; handoff: `.memory/handoffs/variant-b-slice2c-checkpoint.md`; regression: `.memory/logs/regression_variant_b_slice2_2c.md`; performance: `.memory/logs/performance_audit_variant-b-partial-clone_slice2c.md`; blocker: none.

## Productive validation
- Import and activation: PASS.
- Relevant ABAP Unit: PASS.
- Productive ATC: PASS.
- Base-cache ABAP Unit: PASS.
- Base-cache ATC: PASS.
- Slice 2C blockers: none.

## Deferred non-blocking issue
- Issue: `LTCL_CACHE_ADMIN=>OVERVIEW_AGGREGATES_COUNTS`
- Classification: `DEFERRED_NON_PRODUCTIVE`
- Current evidence: `CX_SY_OPEN_SQL_DB` in the cache-admin aggregation/report path; exact live SQL diagnostic remains unresolved.
- Package B blocker: `NO`
- Release requirement: Resolve or explicitly disposition before final release validation.

## Package B objective
- Package B combines Slices 3+4:
  - cold blobless commit/tree graph acquisition;
  - bounded graph-closure verification;
  - current-tip blob discovery;
  - deduplicated and bounded snapshot materialization.

## Binding Package B constraints
- No `deepen` or `shallow` in Variant B requests.
- No per-object SQL or HTTP.
- No uncertified haves.
- Server capabilities must be intersected before request emission.
- `INITIAL_BRANCH_BLOBLESS` may use `filter blob:none` only when advertised.
- Selected blob materialization must use bounded, deduplicated batches.
- Graph and snapshot certificates must only be published after verification.
- No productive blank-repository-key fallback.
- No per-delta-base remote repair.
- Package D1/Slice 7 remains responsible for collect/deduplicate/bulk external delta-base resolution.
- Package D2/Slice 8 remains responsible for final attempt and transaction isolation.
- Physically obsolete legacy code is removed in Package E/Slice 9 only after replacement paths are validated.

## Active links
- Owner specification: `.github/prompts/variant-b.prompt.md`
- Current design and reviews: `.memory/logs/variant_b_slice2_design.md`, `.memory/reviews/variant_b_slice2_design_review.md`
- Current protocol / persistence review: `.memory/logs/protocol_persistence.md`
- Latest performance and regression artifacts: `.memory/logs/performance_audit_variant-b-partial-clone_slice2c.md`, `.memory/logs/regression_variant_b_slice2_2c.md`
- Latest handoff: `.memory/handoffs/variant-b-slice2c-checkpoint.md`
- Current implementation map: `.memory/logs/variant_b_slice2c_migration_map.md`
- Active diagram: `.memory/diagrams/variant_b_flow.mmd`

## Package B progress
- B0 design: `.memory/logs/variant_b_package_b_design.md` (DRAFT, both
  reviews resolved in-place).
- B0 protocol/persistence review: `.memory/reviews/variant_b_package_b_protocol_review.md`
  (`APPROVE_WITH_MINOR_REVISIONS`, minor UUID-method note fixed in design).
- B0 performance DESIGN_GATE review: `.memory/reviews/performance_design_variant_b_package_b.md`
  (`APPROVE_WITH_MINOR_REVISIONS` after 2 passes; all 3 findings resolved:
  memory-risk gate `c_max_graph_response_bytes`/INV-B-12 added, walk switched
  to `iv_bulk_fetch = abap_false`/INV-B-13, split budget rescoped per-batch/
  INV-B-07b).
- New symbols planned: `ZCL_ABAPGIT_ORTEC_COLD_INIT` (acquire_blobless_graph,
  materialize_tip_snapshot); `zcl_abapgit_ortec_obj_store=>verify_tree_closure`,
  `=>get_tip_blob_sha1s` (new methods). No productive ABAP written yet.
- Checkpoint plan: B1 (graph acquisition, own IT8 gate) then B2+B3 combined
  (blob discovery + materialization — B2 has no independent productive
  caller, combined per explicit checkpoint exception, decision recorded in
  design §9).
- B1 implementation: `ZCL_ABAPGIT_ORTEC_COLD_INIT=>acquire_blobless_graph`
  and `zcl_abapgit_ortec_obj_store=>verify_tree_closure` implemented
  (untracked, not yet committed/imported). Static perf scan: `PASS`
  (`.memory/reviews/perf_scan_variant_b_b1.md`). Perf `IMPLEMENTATION_AUDIT`:
  subagent verdict `REVISE_AND_REVIEW_ONCE` on one claimed BLOCKING finding
  (`.memory/reviews/perf_audit_variant_b_b1.md`) was **rejected by the
  orchestrator after direct source re-verification** — the finding claimed
  `verify_tree_closure`'s frontier walk never detects a missing tree, but
  `zcl_abapgit_ortec_obj_store=>get_objects` has an unconditional
  found-vs-requested check that raises `zcx_abapgit_ortec_git` for ANY
  missing SHA1 regardless of `iv_bulk_fetch`, and the pre-existing
  `get_objects_missing` test already proves this exact path live. Rebuttal
  with quoted source is recorded at the top of
  `.memory/reviews/perf_audit_variant_b_b1.md`. No code change was needed.
  Regression: `PASS` (`.memory/logs/regression_variant_b_b1.md`) — purely
  additive diff, no productive caller wired yet, no method name >30 chars,
  no test-class collisions, existing `ltcl_obj_store` tests intact.
- B1 status: `IMPLEMENTATION_COMPLETE_PENDING_IT8` — ready for a selective
  checkpoint commit, then must STOP for owner-executed IT8 validation
  (import, activation, targeted + regression ABAP Unit, ATC) before B2+B3.

## Next action
After the B1 checkpoint commit lands, STOP productive editing and wait for
owner-executed IT8 validation of B1 (import/activate
`zcl_abapgit_ortec_cold_init` + the `zcl_abapgit_ortec_obj_store`/
`zcl_abapgit_ortec_git_tests` changes; run ABAP Unit incl. the 3 new
`verify_closure_*` tests and the 3 new `ltcl_cold_init` tests; run ATC) per
the explicit owner instruction not to proceed to B2+B3 without that
checkpoint. Once IT8-validated, proceed to B2+B3 (combined): add
`get_tip_blob_sha1s` to `zcl_abapgit_ortec_obj_store` and
`materialize_tip_snapshot` to `zcl_abapgit_ortec_cold_init`, per
`.memory/logs/variant_b_package_b_design.md` §9/§11.
