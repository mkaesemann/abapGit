# ORTEC abapGit opt-rework — active state

- Repository / branch: abapGit on `ortec/abapgit_1_133-opt-rework`
- Topic: `variant-b-partial-clone`
- Owner-approved goal: Variant B partial-clone fetch orchestration with certified haves, cold blobless graph acquisition, current-tip blob materialization, and no per-object SQL/HTTP repair.

### Validated baseline

- Current repository HEAD:  `b3701afbc812e5379b886fdc6a1e3db134578012`
- Current SAP-validated productive baseline:  `b3701afbc812e5379b886fdc6a1e3db134578012`
- Package C C1 implementation commit:  `aeac812652da4d18d46e0ee4aad742baaa179e80`
- Package C C1 SAP/ATC fix commit:  `b3701afbc812e5379b886fdc6a1e3db134578012`
- Package B final productive baseline:  `fc06f7f62218d6ce45e4ae5de1c709b4b39477ab`

## Completed work

- Slice 0: COMPLETE
- Slice 1: SAP_VALIDATED_COMPLETE
- Slice 2A/2B: SAP_VALIDATED_COMPLETE
- Slice 2C / Package A: SAP_VALIDATED_COMPLETE
- Package B B0: APPROVED_WITH_RESOLVED_REVISIONS
- Package B B1: SAP_VALIDATED_COMPLETE
- Package B B2+B3: SAP_VALIDATED_COMPLETE
- Package C C0: `APPROVED_WITH_RESOLVED_REVISIONS`
- Package C C1: `SAP_VALIDATED_COMPLETE`

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

- Current phase:  `Package C C2 — productive branch orchestration and certification`
- Previous checkpoint:  `Package C C1 — SAP_VALIDATED_COMPLETE`
- Validated C1 HEAD:  `b3701afbc812e5379b886fdc6a1e3db134578012`
- C1 implementation commit:  `aeac812652da4d18d46e0ee4aad742baaa179e80`
- C1 SAP/ATC fix commit:  `b3701afbc812e5379b886fdc6a1e3db134578012`
- C1 handoff:  `.memory/handoffs/variant-b-package-c-c1-checkpoint.md`
- C1 implementation audit:  `PASS`, gate closed
- C1 performance scan:  `PASS`
- C1 regression:  `PASS`
- IT8 import and activation:  `PASS`
- C1 ABAP Unit:  `PASS`
- Relevant bundled regression ABAP Unit:  `PASS`
- Productive C1 ATC:  `PASS`
- Remaining C1 blockers:  none
- Checkpoint plan:  `C1_THEN_C2`

### Current objective

Package C C2:

- migrate the approved productive branch orchestration callers;
- resolve and classify the advertised target as warm, incremental, or cold;
- invoke the explicit opportunistic backfill at most once before accepting a
  cold classification;
- use the SAP-validated C1 certified-have policy;
- route cold branches through the validated Package B graph and snapshot APIs;
- preserve the bounded thin, self-contained, and recovery cascade;
- replace the raw commit-history write with the approved certification
  lifecycle;
- publish branch state only after graph and selected snapshot verification;
- preserve standard abapGit behavior when ORTEC is disabled.

### Active links

- Owner specification:  `.github/prompts/variant-b.prompt.md`
- Package C design:  `.memory/logs/variant_b_package_c_design.md`
- Package C correctness review:  `.memory/reviews/variant_b_package_c_correctness_review.md`
- Package C protocol/persistence review:  `.memory/reviews/variant_b_package_c_protocol_review.md`
- Package C performance design gate:  `.memory/reviews/performance_design_variant_b_package_c.md`
- C1 handoff:  `.memory/handoffs/variant-b-package-c-c1-checkpoint.md`
- C1 implementation audit:  `.memory/reviews/implementation_audit_variant_b_package_c_c1.md`
- C1 performance scan:  `.memory/reviews/performance_scan_variant_b_package_c_c1.md`
- C1 regression:  `.memory/logs/regression_variant_b_package_c_c1.md`
- Package B final handoff:  `.memory/handoffs/variant-b-package-b-b2b3-checkpoint.md`

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

### Remaining roadmap

- Package C C2:  current work — productive orchestration and certification lifecycle
- Package D:  shared design for Slices 7+8, followed by separate D1 and D2 implementation checkpoints
- Package E:  Slice 9 validated legacy-code cleanup

### Next action

Start Package C C2 in a new orchestrator chat from SAP-validated C1 HEAD
`b3701afbc812e5379b886fdc6a1e3db134578012`.

Do not repeat C0 or C1. Do not start Package D.
