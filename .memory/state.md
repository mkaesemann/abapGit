# ORTEC abapGit opt-rework — active state

- Repository / branch: abapGit on `ortec/abapgit_1_133-opt-rework`
- Topic: `variant-b-partial-clone`
- Owner-approved goal: Variant B partial-clone fetch orchestration with certified haves, cold blobless graph acquisition, current-tip blob materialization, and no per-object SQL/HTTP repair.

## Validated baseline

- Validated productive baseline: `fc06f7f62218d6ce45e4ae5de1c709b4b39477ab`
- Current repository HEAD: `cf3d5cb87ca4eec4e68b7975df6fc127a8c5abc2`
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
- Package B final productive validation: PASS
- Package B blockers: none
- Package B final handoff:
  `.memory/handoffs/variant-b-package-b-b2b3-checkpoint.md`

### Current phase

- Current phase: `Package C C1 - IMPLEMENTED_PENDING_SAP_VALIDATION`
- Previous phase: `Package C C0 - APPROVED_WITH_RESOLVED_REVISIONS`
- Previous package: `Package B - SAP_VALIDATED_COMPLETE`
- C1 handoff: `.memory/handoffs/variant-b-package-c-c1-checkpoint.md`
- C1 implementation audit: PASS, 0 blocking — `.memory/reviews/implementation_audit_variant_b_package_c_c1.md`
- C1 performance scan: PASS, 0 blocking — `.memory/reviews/performance_scan_variant_b_package_c_c1.md`
- C1 regression: PASS (after 1 orchestrator-corrected false positive) — `.memory/logs/regression_variant_b_package_c_c1.md`
- Validated productive baseline:
  `fc06f7f62218d6ce45e4ae5de1c709b4b39477ab`
- Current repository HEAD:
  `cf3d5cb87ca4eec4e68b7975df6fc127a8c5abc2`
- C0 design: `.memory/logs/variant_b_package_c_design.md`
- C0 correctness review: APPROVE_WITH_MINOR_REVISIONS (after 1 revision cycle)
  — `.memory/reviews/variant_b_package_c_correctness_review.md`
- C0 protocol review: APPROVE_WITH_MINOR_REVISIONS —
  `.memory/reviews/variant_b_package_c_protocol_review.md`
- C0 performance DESIGN_GATE: APPROVE_WITH_MINOR_REVISIONS —
  `.memory/reviews/performance_design_variant_b_package_c.md`
- Checkpoint plan: `C1_THEN_C2`
- Central defect found and designed-for: `persist_pull_result`'s raw
  `ZAOG_COMMIT_HIST` INSERT never sets `HIST_LEVEL`/`SNAP_STATE`, so
  `get_verified_have_commits` returns empty in production today — every
  incremental fetch after the first is effectively haves-less.

### Current objective

- Productive warm, incremental, cold, and recovery branch orchestration.
- Certificate-based operation classification.
- Bounded deterministic certified-have selection.
- Migration of approved productive branch pull/switch callers.
- Reuse of validated Package B APIs.
- Publication only after graph and snapshot verification.

## Active links

- Owner specification: `.github/prompts/variant-b.prompt.md`
- Package B design: `.memory/logs/variant_b_package_b_design.md`
- B1 handoff: `.memory/handoffs/variant-b-package-b-b1-checkpoint.md`
- B2+B3 final handoff: `.memory/handoffs/variant-b-package-b-b2b3-checkpoint.md`

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

  ### Next action

- Package C C1 is implemented (new `ZCL_ABAPGIT_ORTEC_HAVE_POLICY` class +
  `upload_pack` call-site migration), all static/audit gates PASS. Awaiting
  owner-executed IT8 import/activation/ABAP Unit/ATC. Do NOT start C2
  (porcelain routing + §6 certification lifecycle + cold-branch wiring)
  until C1 is `SAP_VALIDATED_COMPLETE`. See
  `.memory/handoffs/variant-b-package-c-c1-checkpoint.md` for full details.

- Performance scan for Package C C1 (static, new/changed files only): PASS.
  Artifact: `.memory/reviews/perf_scan_variant_b_package_c_c1.md`.
  Verified invariants: no per-object SQL inside loops, one bulk SELECT in
  `get_certified_haves`, no additional per-request SQL introduced in
  `upload_pack`, and no full-table scan on `ZAOG_COMMIT_HIST`.
