# ORTEC abapGit opt-rework — active state

- Repository / branch: abapGit on `ortec/abapgit_1_133-opt-rework`
- Topic: `variant-b-partial-clone`
- Current phase: `Package D2 — authorized, not started`
- Previous checkpoint: `Package D1 — SAP_VALIDATED_COMPLETE`
- Planned next phase: Package E — Snapshot Consumer Coherence and Adaptive Materialization
- Planned following phase: Package F — Validated Legacy-Code Cleanup
- Package sequence decision: OWNER_DECISION, 2026-07-24
- Renumbering decision: .memory/decisions/variant_b_package_renumbering.md
- Next action: Start Package D2 in a new senior implementation chat from the
  approved Package D design and the SAP-validated D1 baseline.
 
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

## Package D0 status

```text
PACKAGE_D_D0=APPROVED
PACKAGE_D_SOURCE_BASELINE=5e5403546554dbe4f0f8e7a1eb2f07ab434dbe95
CORRECTNESS_REVIEW=APPROVE
PROTOCOL_PERSISTENCE_REVIEW=APPROVE
PERFORMANCE_DESIGN_GATE=APPROVE_WITH_MINOR_REVISIONS
IMPLEMENTATION_AUTHORIZED=YES
BLOCKERS=NONE
```

## Package D1 status

```text
PACKAGE_D_D1=IMPLEMENTED
BASELINE=5a1171f24f0fe0664eeaa3a832fee1bc7076a2e3
CHANGED_PRODUCTIVE=zcl_abapgit_ortec_delta.clas.abap/.xml,
  zcl_abapgit_ortec_pack_dec.clas.abap, zcl_abapgit_ortec_pack_stream.clas.abap
CHANGED_TESTS=zcl_abapgit_ortec_delta.clas.testclasses.abap (13 tests total:
  9 pre-existing + 4 new this gate: duplicate_declared_sha_ok,
  base_later_in_pack_order, exhausted_recovery_raises added here;
  partial_recovery_resumes added to zcl_abapgit_ortec_pack_dec.clas.testclasses.abap),
  zcl_abapgit_ortec_pack_stream.clas.testclasses.abap (+LOCAL FRIENDS,
  +no_thin_fetch_for_ext_base)
TEST_MATRIX=COMPLETE (13/13 D1 acceptance IDs mapped to a distinct method;
  see .memory/logs/regression_variant_b_package_d_d1.md "D1 acceptance
  test-ID matrix" section)
MISSING_TESTS=NONE
INSTRUMENTATION_DISPOSITION=gv_bulk_load_calls/gv_thin_fetch_calls kept as
  approved bounded observability counters, doc comments tightened with
  explicit lifecycle/reset/concurrency/production-semantics statements
  (owner directive item 2, closed)
SQL_SHAPE=one bulk get_objects(iv_bulk_fetch=X) call per pack per resolver
HTTP_SHAPE=unchanged (no new HTTP call sites)
LOCAL_VALIDATION=get_errors PASS on all 5 changed/new files (0 errors);
  abaplint run shows only pre-existing project-wide baseline noise
  (confirmed via zcl_abapgit_ortec_fastpath.clas.abap, an untouched file,
  also showing 38 findings with the same top-level config) - not diff-specific
SAP_VALIDATION=NOT_PERFORMED
ABAP_UNIT_EXECUTION=NOT_PERFORMED (no local ABAP Unit runner available)
PERFORMANCE_SCAN=PASS (see .memory/logs/performance_scan_variant_b_package_d_d1.md)
PERFORMANCE_AUDIT=PASS_WITH_MINOR_FINDINGS (0 blocking, 1 documented minor
  finding - see .memory/logs/performance_audit_variant_b_package_d_d1.md)
REGRESSION=PASS_WITH_FINDINGS (static-only; see
  .memory/logs/regression_variant_b_package_d_d1.md "Final local regression
  confirmation")
D1_STATUS=OWNER_SAP_RETEST_PENDING
D2_STATUS=NOT_STARTED
BLOCKERS=NONE (no D2-owned concern touched: no change to
  zcl_abapgit_ortec_obj_store.clas.abap, staged-visibility status='D' logic,
  attempt/lock/transaction handling, or get_staged_delta_objects)
```

## Package D1 final closeout — SAP_VALIDATED_COMPLETE

```text
PACKAGE_D_D1=SAP_VALIDATED_COMPLETE
PACKAGE_D_D1_HEAD=73cb519a
SAP_SYSTEM=IT8
ACTIVATION=PASS
SYNTAX=PASS
ABAP_UNIT=PASS
ATC=PASS
WARM_BRANCH=PASS
COLD_BRANCH=PASS
SLIN_WARNINGS=NONE
PERFORMANCE_SCAN=PASS
PERFORMANCE_AUDIT=PASS_WITH_MINOR_FINDINGS
D1_BLOCKERS=NONE
PACKAGE_E_FOLLOWUP=FALSE_MODIFIED_CONSUMER_COHERENCE
PACKAGE_D_D2=AUTHORIZED_NOT_STARTED
PUSHED=NO
```

See (not duplicated here): [.memory/logs/variant_b_package_d_design.md](.memory/logs/variant_b_package_d_design.md),
[.memory/handoffs/variant-b-package-d-d1-implementation.md](.memory/handoffs/variant-b-package-d-d1-implementation.md),
[.memory/logs/regression_variant_b_package_d_d1.md](.memory/logs/regression_variant_b_package_d_d1.md),
[.memory/logs/performance_scan_variant_b_package_d_d1.md](.memory/logs/performance_scan_variant_b_package_d_d1.md),
[.memory/logs/performance_audit_variant_b_package_d_d1.md](.memory/logs/performance_audit_variant_b_package_d_d1.md),
[.memory/logs/variant_b_package_d_d1_modified_status_triage.md](.memory/logs/variant_b_package_d_d1_modified_status_triage.md).

Package C at `29199f629773c676e0eaa2f3a006f5167d304ae8` remains the prior productive SAP-validated baseline; Package D1 at `73cb519a` is now also SAP-validated.

Repository HEAD is at `73cb519a` (D1 SAP closeout commit), local only, not pushed.

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
- do not absorb the newly planned Package E scope into Package D.

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
- Package E owns snapshot consumer coherence and adaptive materialization.
- Package E owns the certified-snapshot repair contract for
  CERTIFIED_BUT_MISSING.
- Package E must prevent normal certified current-tip consumers from using
  `ZCL_ABAPGIT_ORTEC_MISSING_OBJ=>ENSURE_AVAILABLE`.
- Package E materialization discovers capabilities once per complete
  materialization operation.
- Package E materialization uses adaptive row- and response-byte-bounded blob
  batches.
- Package E final snapshot verification is metadata-only and never reloads
  blob payloads merely to prove completeness.
- Package F owns validated legacy-code removal.

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
- Package D0 design:
  `.memory/logs/variant_b_package_d_design.md`
- Package D0 delta discovery:
  `.memory/logs/variant_b_package_d_delta_discovery.md`
- Package D0 concurrent-commit reconciliation:
  `.memory/logs/variant_b_package_d_concurrent_commit_impact.md`
- Package D0 correctness review decision:
  `.memory/reviews/variant_b_package_d_correctness_decision.md`
- Package D0 protocol/persistence review decision:
  `.memory/reviews/variant_b_package_d_protocol_decision.md`
- Package D0 performance design gate:
  `.memory/reviews/performance_design_variant_b_package_d.md`
- Package D1 implementation handoff:
  `.memory/handoffs/variant-b-package-d-d1-implementation.md`

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
4. Package E focused discovery:
  branch-switch-to-Stage consumer coherence and current materialization cost.
5. Package E correctness, protocol/persistence and performance design reviews.
6. Package E implementation:
  - capability discovery once;
  - adaptive materialization batches;
  - metadata-only final verification;
  - certified snapshot consumer coherence;
  - one bounded CERTIFIED_BUT_MISSING repair.
7. Package E implementation performance audit and regression validation.
8. Package E live validation on the large repository.
9. Package F validated legacy-code cleanup.
10. Final cross-package performance profiling and tuning.
11. Final release validation.

## Next action

Package D1 implementation is complete (see Package D1 status block above) and
handed off in `.memory/handoffs/variant-b-package-d-d1-implementation.md`.

Remaining before D1 can be marked SAP_VALIDATED_COMPLETE:
- run `ortec-abapgit-regression` against the full existing REF/OFS/mixed/
  external-base/missing-base test surface across
  `zcl_abapgit_ortec_git_tests.clas.testclasses.abap`,
  `zcl_abapgit_ortec_pack_dec.clas.testclasses.abap`, and
  `zcl_abapgit_ortec_pack_stream.clas.testclasses.abap`, plus the new
  `zcl_abapgit_ortec_delta.clas.testclasses.abap`;
- import to IT8 and run live ABAP Unit + ATC;
- performance scan/audit of the changed call paths.

Do not start Package D2 (staged-visibility `status='D'` fix, attempt/lock/
transaction changes, `get_staged_delta_objects`,
`zcl_abapgit_ortec_obj_store.clas.abap`) until D1 is SAP-validated or the
owner explicitly authorizes parallel work.

Read only the current compact state and the Package D0/D1 links above. Do not
repeat Package D0 discovery, design, reconciliation, or review — it is
DESIGN_APPROVED with `PERFORMANCE_DESIGN_GATE=APPROVE_WITH_MINOR_REVISIONS`
and `IMPLEMENTATION_AUTHORIZED=YES`. Do not repeat Package C discovery,
design review or implementation review unless Package D exposes a concrete
regression. Do not perform speculative Package C performance work during
Package D.
