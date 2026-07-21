# ORTEC abapGit opt-rework — active state

- Repository / branch: abapGit on `ortec/abapgit_1_133-opt-rework`
- Topic: `variant-b-partial-clone`
- Owner-approved goal: Variant B partial-clone fetch orchestration with certified haves, cold blobless graph acquisition, current-tip blob materialization, and no per-object SQL/HTTP repair.
- Baseline / relevant commits: Slice 1 import/activation `10e75f850c3530fdf5de820fb5f4f5f1147359f6`; Slice 2A/2B SAP validation `96536177393e7d6650fdfe4487e8fa7f17ad4044` and ATC follow-up `6638cb6683e9b508b2e9144dfb9c80628ec23c98`.
- Current phase: `Slice 2C committed pending SAP validation`; Package B / Slice 3 not started.
- Current status: `SLICE_2C_COMMITTED_PENDING_SAP`.
- Committed productive paths: `src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap`, `src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap`, `src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.abap`, `src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap`.

## Completed work
- Slice 0 — status: COMPLETE; SAP: N/A; handoff: `.memory/logs/variant_b_reconciliation.md`; regression: N/A; performance: N/A; blocker: none.
- Slice 1 — status: COMPLETE; implementation: `10e75f850c3530fdf5de820fb5f4f5f1147359f6`; SAP: PASS; handoff: `.memory/logs/variant_b_design.md`; regression: `.memory/logs/regression_variant_b_slice1.md`; performance: `.memory/reviews/performance_design_variant-b-partial-clone_slice1.md`; blocker: none.
- Slice 2A/2B — status: COMPLETE; implementation: `96536177393e7d6650fdfe4487e8fa7f17ad4044` / `6638cb6683e9b508b2e9144dfb9c80628ec23c98`; SAP: PASS; handoff: `.memory/handoffs/variant-b-slice2-2a2b-checkpoint.md`; regression: `.memory/logs/regression_variant_b_slice2_2a2b.md`; performance: `.memory/logs/performance_audit_variant-b-partial-clone_slice2.md`; blocker: none.
- Slice 2C — status: COMMITTED; implementation: `a2c4d155` (`ORTEC: Complete explicit fetch-mode call-site migration`); SAP: PENDING_SAP_IMPORT; handoff: `.memory/handoffs/variant-b-slice2c-checkpoint.md`; regression: `.memory/logs/regression_variant_b_slice2_2c.md`; performance: `.memory/logs/performance_audit_variant-b-partial-clone_slice2c.md`; blocker: none.

## Current Slice 2C facts
- Corrected verdict: `READY_FOR_2C_CHECKPOINT`.
- Per-delta-base HTTP completion is disabled.
- Missing external delta bases escalate with `retry_without_haves`.
- All migrated Variant B fetch tiers report `ev_deepen_used = 0`.
- Technical certified-have resolution failures are propagated and are not silently converted into an empty valid have set.
- Slice 2C performance audit: `PASS`.
- Regression: static/structural `PASS`.
- SAP import / activation / ABAP Unit / ATC: `PENDING_SAP_IMPORT`.

## Binding future work
- No progressive deepen.
- No `deepen` or `shallow` in Variant B requests.
- No per-object SQL or HTTP.
- No uncertified haves.
- No branch-owned object payload duplication.
- No productive blank repository-key fallback.
- No same-pack standard-decoder fallback after ORTEC decode failure.
- External delta bases are deferred to Package D1 / Slice 7 for collection, deduplication, and bulk loading.
- Obsolete legacy methods are removed only after replacement paths are validated.

## Combined package roadmap
- Package A: corrected Slice 2C close-out and SAP validation.
- Package B: Slices 3+4 — cold blobless graph acquisition plus current-tip snapshot materialization.
- Package C: Slices 5+6 — branch orchestration plus certified-have policy.
- Package D: shared design for Slices 7+8, followed by D1 bulk external bases and D2 attempt/transaction isolation.
- Package E: Slice 9 obsolete-code cleanup.

## Active links
- Owner specification: `.github/prompts/variant-b.prompt.md`
- Current design and reviews: `.memory/logs/variant_b_slice2_design.md`, `.memory/reviews/variant_b_slice2_design_review.md`
- Current protocol / persistence review: `.memory/logs/protocol_persistence.md`
- Latest performance and regression artifacts: `.memory/logs/performance_audit_variant-b-partial-clone_slice2c.md`, `.memory/logs/regression_variant_b_slice2_2c.md`
- Latest handoff: `.memory/handoffs/variant-b-slice2c-checkpoint.md`
- Current implementation map: `.memory/logs/variant_b_slice2c_migration_map.md`
- Active diagram: `.memory/diagrams/variant_b_flow.mmd`

## Next action
Import and validate the new Slice 2C checkpoint commit in IT8, activate the affected objects, run ABAP Unit and ATC, then update the state before starting Package B.
