# ORTEC abapGit opt-rework — active state

- Repository / branch: abapGit on `ortec/abapgit_1_133-opt-rework`
- Topic: `variant-b-partial-clone`
- Owner-approved goal: Variant B partial-clone fetch orchestration with certified haves, cold blobless graph acquisition, current-tip blob materialization, and no per-object SQL/HTTP repair.

## Validated baseline

- Current validated HEAD: `fc06f7f62218d6ce45e4ae5de1c709b4b39477ab`
- B1 implementation commit: `a94f08dad597c01d130f72b4ef3037a48f023111`
- B1 exception-contract fix: `85533762168fd2509ca469acd79c75b8e8b62279`
- B2+B3 implementation commit: `99e20f8d`
- B2+B3 SAP validation fix commit: `fc06f7f62218d6ce45e4ae5de1c709b4b39477ab` (pushed, on `origin/ortec/abapgit_1_133-opt-rework`)

## Completed work

- Slice 0: COMPLETE
- Slice 1: SAP_VALIDATED_COMPLETE
- Slice 2A/2B: SAP_VALIDATED_COMPLETE
- Slice 2C / Package A: SAP_VALIDATED_COMPLETE
- Package B B0: APPROVED_WITH_RESOLVED_REVISIONS
- Package B B1: SAP_VALIDATED_COMPLETE
- Package B B2+B3: SAP_VALIDATED_COMPLETE

## Package B status

- Implemented: `ZCL_ABAPGIT_ORTEC_COLD_INIT=>ACQUIRE_BLOBLESS_GRAPH`; `ZCL_ABAPGIT_ORTEC_OBJ_STORE=>VERIFY_TREE_CLOSURE`
- IT8 import and activation: PASS
- ABAP Unit: PASS
- Productive ATC: PASS
- Performance scan: PASS
- Performance audit verdict: APPROVE
- Performance gate: CLOSED
- Original missing-tree finding: RETRACTED
- Remaining B1 blockers: none
- No productive caller is wired yet.

## Current phase

- Current phase: Package B B2+B3 — bounded selected-tip blob discovery and selected snapshot materialization
- Status: `SAP_VALIDATED_COMPLETE` — owner-executed IT8 import/ABAP Unit/ATC passed after one fix commit
- Fix commit `fc06f7f62218d6ce45e4ae5de1c709b4b39477ab`: corrected `ltcl_cold_init` test-class friend declaration (added `zcl_abapgit_ortec_cold_init.clas.locals_imp.abap` with `CLASS ltcl_cold_init DEFINITION DEFERRED.` so the main class's `LOCAL FRIENDS` statement resolves); moved `obj_store` unit tests out of the shared `zcl_abapgit_ortec_git_tests` class into a dedicated `zcl_abapgit_ortec_obj_store.clas.testclasses.abap`; changed `zcl_abapgit_ortec_obj_store`'s `mt_cache` internal table from `HASHED` to `SORTED` to avoid a possible sequential read on partial-key access.
- Previous checkpoint: Package B B1 — SAP_VALIDATED_COMPLETE
- Checkpoint artifact: `.memory/handoffs/variant-b-package-b-b2b3-checkpoint.md`

## Current objective

- Iterative bounded selected-tip tree traversal
- Unique selected-blob SHA discovery without payload reads
- Bulk READY-presence checks
- Bounded and deduplicated missing-blob batches
- `MATERIALIZE_BLOBS` request use
- Final selected snapshot verification
- Snapshot-complete publication only after successful verification

## Active links

- Owner specification: `.github/prompts/variant-b.prompt.md`
- Package B design: `.memory/logs/variant_b_package_b_design.md`
- Package B protocol review: `.memory/reviews/variant_b_package_b_protocol_review.md`
- Package B performance design gate: `.memory/reviews/performance_design_variant_b_package_b.md`
- B1 handoff: `.memory/handoffs/variant-b-package-b-b1-checkpoint.md`
- B1 performance audit: `.memory/reviews/perf_audit_variant_b_b1.md`
- B1 regression: `.memory/logs/regression_variant_b_b1.md`
- B2+B3 handoff: `.memory/handoffs/variant-b-package-b-b2b3-checkpoint.md`
- B2+B3 performance audit: `.memory/reviews/perf_audit_variant_b_b2b3.md`
- B2+B3 regression: `.memory/logs/regression_variant_b_b2b3.md`

## Binding constraints

- No `deepen` or `shallow` in Variant B requests.
- No per-object SQL or HTTP.
- No uncertified haves.
- No productive blank repository-key fallback.
- No per-delta-base remote repair.
- Server capabilities must be intersected before request emission.
- `INITIAL_BRANCH_BLOBLESS` uses `filter blob:none` only when advertised.
- B2 tree traversal is iterative and uses bounded bulk frontiers.
- B2 performs no blob-payload reads.
- B3 performs bounded, deduplicated selected-blob materialization.
- Graph and snapshot completeness remain separate states.
- No graph or snapshot certificate is published before verification.
- Standard abapGit behavior remains unchanged when ORTEC is disabled.
- Package C owns productive branch orchestration and final have policy.
- Package D1 owns generalized bulk external delta-base resolution.
- Package D2 owns final attempt and transaction isolation.
- Package E owns validated legacy-code removal.

## Deferred non-blocking issue

- `LTCL_CACHE_ADMIN=>OVERVIEW_AGGREGATES_COUNTS`
- Classification: `DEFERRED_NON_PRODUCTIVE`
- Package B blocker: `NO`
- Release requirement: Resolve or explicitly disposition before final release validation.

## Remaining roadmap

- Package B B2+B3: SAP_VALIDATED_COMPLETE — no further action
- Package C: combined Slices 5+6
- Package D: shared design for Slices 7+8, separate D1 and D2 implementation checkpoints
- Package E: Slice 9 cleanup

## Next action

- Package B (B0+B1+B2+B3) is fully SAP_VALIDATED_COMPLETE. Start Package C
  (combined Slices 5+6: productive branch orchestration and final have
  policy) planning in a new session from validated HEAD
  `fc06f7f62218d6ce45e4ae5de1c709b4b39477ab`.
