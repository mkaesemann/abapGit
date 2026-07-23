# ORTEC abapGit opt-rework — active state

- Repository / branch: abapGit on `ortec/abapgit_1_133-opt-rework`
- Topic: `variant-b-partial-clone`
- Current phase: `Package D — not started`
- Previous phase: `Package C — SAP_VALIDATED_COMPLETE`

## Validated baseline

```text
PACKAGE_C_STATUS=SAP_VALIDATED_COMPLETE
PACKAGE_C_VALIDATED_HEAD=29199f629773c676e0eaa2f3a006f5167d304ae8
SAP_SYSTEM=IT8
SAP_VALIDATION_DATE=2026-07-23
ATC=PASS
ABAP_UNIT=PASS
COLD_BRANCH=PASS
WARM_UNCHANGED=PASS
CERTIFIED_STATE=F/C
CACHE_ADMIN=PASS
LARGE_REPO_FUNCTIONAL=PASS
```

Package C at `29199f629773c676e0eaa2f3a006f5167d304ae8` is the current productive SAP-validated baseline.

## Completed work

- Slice 0: `COMPLETE`
- Slice 1: `SAP_VALIDATED_COMPLETE`
- Slice 2A/2B: `SAP_VALIDATED_COMPLETE`
- Slice 2C / Package A: `SAP_VALIDATED_COMPLETE`
- Package B B0: `APPROVED_WITH_RESOLVED_REVISIONS`
- Package B B1: `SAP_VALIDATED_COMPLETE`
- Package B B2+B3: `SAP_VALIDATED_COMPLETE`
- Package C C0: `APPROVED_WITH_RESOLVED_REVISIONS`
- Package C C1: `SAP_VALIDATED_COMPLETE`
- Package C C2 / Package C final: `SAP_VALIDATED_COMPLETE`

## Package C closeout

Validated in SAP IT8:

- all affected classes import and activate successfully;
- productive ATC checks are clean;
- all affected ABAP Unit tests pass;
- cold-branch reconstruction produces correct files and deltas;
- warm-unchanged reconstruction succeeds;
- snapshot publication produces `HIST_LEVEL = F` and `SNAP_STATE = C` in
  `ZAOG_COMMIT_HIST`;
- the matching `ZAOG_REPO_STATE` row contains `SNAP_STATE = C`;
- repository URL, URL hash, current commit, fetched commit and fetch timestamp
  are persisted for cold snapshot publication;
- cache administration clears all repository-scoped cache, certificate and
  state data directly by `REPO_KEY`;
- cache clearing supports incomplete repository metadata and orphaned cache
  state;
- large unfiltered repositories no longer create one repository-wide SHA1
  range in `FETCH_BLOBS_BULK`;
- bounded active blob-key and payload windows complete functionally for the
  tested large repository.

Package C correctness blockers: `NONE`.

## Current objective

Start Package D from the SAP-validated Package C baseline.

Package D scope:

- shared design for Slices 7 and 8;
- D1: generalized bounded external delta-base resolution;
- D2: final attempt and transaction isolation;
- preserve all Package C certification, reconstruction and cache-management
  invariants;
- do not reopen Package C without concrete regression evidence.

## Binding constraints

- No `deepen` or `shallow` in Variant B requests.
- No per-object SQL or HTTP.
- No uncertified haves.
- No productive blank repository-key fallback.
- No per-delta-base remote repair.
- Server capabilities must be intersected before request emission.
- `INITIAL_BRANCH_BLOBLESS` uses `filter blob:none` only when advertised.
- Tree and blob processing must use bounded bulk windows.
- Presence, metadata and payload access remain separated.
- Graph and snapshot completeness remain separate states.
- Snapshot publication requires `HIST_LEVEL = F`.
- No graph or snapshot certificate is published before verification.
- Standard abapGit behavior remains unchanged when ORTEC is disabled.
- Package D1 owns generalized bounded external delta-base resolution.
- Package D2 owns final attempt and transaction isolation.
- Package E owns validated legacy-code removal.

## Active links

- Owner specification: `.github/prompts/variant-b.prompt.md`
- Package C design and closeout:
  `.memory/logs/variant_b_package_c_design.md`
- Package C final handoff:
  `.memory/handoffs/variant-b-package-c-c2-checkpoint.md`
- Package C correctness review:
  `.memory/reviews/variant_b_package_c_correctness_review.md`
- Package C protocol/persistence review:
  `.memory/reviews/variant_b_package_c_protocol_review.md`
- Package C performance design gate:
  `.memory/reviews/performance_design_variant_b_package_c.md`
- Package B final handoff:
  `.memory/handoffs/variant-b-package-b-b2b3-checkpoint.md`

## Deferred non-blocking performance work

The following work is intentionally deferred to the final performance pass:

- final SAT/ST05 profiling of very large unfiltered repositories;
- tuning the active blob-key window and payload-byte budget;
- reducing simultaneous manifest, SHA1, remote-file and local-status working
  sets if measurements justify it;
- reviewing remaining legacy cache-population paths;
- assessing whether additional database-side joins or package processing
  improve the validated bounded implementation.

These are optimization items, not Package C correctness blockers.

## Remaining roadmap

1. Package D shared design for Slices 7 and 8.
2. Package D1 implementation and checkpoint validation.
3. Package D2 implementation and checkpoint validation.
4. Package E validated legacy-code cleanup.
5. Final cross-package performance profiling and tuning.
6. Final release validation.

## Next action

Start Package D design in a new orchestrator chat from the SAP-validated
Package C baseline.

Read only the current compact state, the final Package C handoff, the owner
specification and the Package-D-relevant design context.

Do not repeat Package C discovery, design review or implementation review
unless Package D exposes a concrete regression.

Do not perform speculative Package C performance work during Package D.
Record new performance evidence for the final performance pass.
