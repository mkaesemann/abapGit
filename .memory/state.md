# ORTEC abapGit opt-rework — active state

- Repository / branch: abapGit on `ortec/abapgit_1_133-opt-rework`
- Topic: `variant-b-partial-clone`
- Current phase: `Package E — authorized, not started`
- Previous checkpoint: `Package D2 — SAP_VALIDATED_COMPLETE`
- Planned next phase: Package E — Snapshot Consumer Coherence and Adaptive Materialization
- Planned following phase: Package F — Validated Legacy-Code Cleanup
- Package sequence decision: OWNER_DECISION, 2026-07-24
- Renumbering decision: .memory/decisions/variant_b_package_renumbering.md
- Next action: Start Package E in a new orchestrator chat, beginning with a
  focused design and performance gate for OBJ_INDEX rebuild batching and the
  existing certified snapshot consumer-coherence findings.
 
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

## Package D2 final closeout — SAP_VALIDATED_COMPLETE

```text
PACKAGE_D_D2=SAP_VALIDATED_COMPLETE
PACKAGE_D_D2_VALIDATED_HEAD=733bb30799886ef8659be7e293b82c3ccfcebbdd
SAP_SYSTEM=IT8
ACTIVATION=PASS
ABAP_UNIT=PASS
ATC=PASS_WITHOUT_SEVERE_FINDINGS
SYSTEM_NO_ROLL_INCIDENT=SAP_VALIDATED_RESOLVED
TIME_OUT_INCIDENT=SAP_VALIDATED_RESOLVED
DBSQL_STMNT_TOO_LARGE_INCIDENT=SAP_VALIDATED_RESOLVED
SAT_PERFORMANCE_CLASSIFICATION=PACKAGE_E_FOLLOWUP
D2_BLOCKERS=NONE
PACKAGE_E=AUTHORIZED_NOT_STARTED
PUSHED=NO
```

`PACKAGE_D_D2_VALIDATED_HEAD` (`733bb307`) is the exact commit chain
verified live-active in IT8 (confirmed by direct `SAPRead` of
`ZCL_ABAPGIT_ORTEC_OBJ_STORE=>GET_OBJECTS`, matching the committed source
byte-for-byte) after the DBSQL_STMNT_TOO_LARGE fix — it supersedes
`2111b288`/`17513ba7` as the tested baseline; those two commits remain its
ancestors (`cdc5caed` → `2111b288` → `17513ba7` → `733bb307`).

Three live IT8 incidents are resolved and SAP-validated as not reproducing
on this head: `SYSTEM_NO_ROLL` (fix: removed the unbounded `populate_cache`
preload from `get_reachable_objects`), `TIME_OUT` (fix: bounded
`ensure_available`'s remote top-up to the caller's missing SHA1 set via
adaptive `MATERIALIZE_BLOBS` batching), and `DBSQL_STMNT_TOO_LARGE` (fix:
chunked `get_objects`' bulk-fetch branch at `c_select_package_size`). A
follow-up SAT trace (`95C45B828A9B11F1B129001DD8B728C2`) completed
successfully with none of the three reproducing; its dominant cost
(`ZCL_ABAPGIT_ORTEC_OBJ_INDEX=>REBUILD_INDEX`, ~9.39s of ~31.4s total) is
classified `PACKAGE_E_FOLLOWUP`, not a D2 defect — see
`.memory/incidents/variant_b_d2_sat_warm_to_cold_o4h8794.md`.

See (not duplicated here):
[.memory/incidents/variant_b_d2_it8_system_no_roll_timeout.md](.memory/incidents/variant_b_d2_it8_system_no_roll_timeout.md),
[.memory/incidents/variant_b_d2_it8_dbsql_stmt_too_large.md](.memory/incidents/variant_b_d2_it8_dbsql_stmt_too_large.md),
[.memory/incidents/variant_b_d2_sat_warm_to_cold_o4h8794.md](.memory/incidents/variant_b_d2_sat_warm_to_cold_o4h8794.md),
[.memory/handoffs/variant-b-d2-timeout-fix.md](.memory/handoffs/variant-b-d2-timeout-fix.md),
[.memory/handoffs/variant-b-package-d-d2-implementation.md](.memory/handoffs/variant-b-package-d-d2-implementation.md).

Package E follow-ups recorded (not started):

```text
E-PERF-OBJINDEX-1: review/benchmark ZAOG_OBJ_INDEX bulk MODIFY package
  sizing (measured: 41 packages at 1000 rows, 82 DB round trips). Candidate
  sizes (5000/10000) require a Package E performance DESIGN_GATE and live
  measurement before adoption.
E-CONSUMER-COHERENCE-1: resolve the existing false Local/Remote MODIFIED
  status for content-identical files (see
  variant_b_package_d_d1_modified_status_triage.md).
E-CACHE-ADMIN-F4-1: review Cache Admin repository F4 help, which derives
  repository choices from ZAOG_REPO_STATE and may omit repositories
  accessed only through partial filtered object materialization -
  usability issue only, not a reason to publish false F/C certification.
```

Repository HEAD is at `733bb307`, local only, not pushed.

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
- Package D1: `SAP_VALIDATED_COMPLETE`
- Package D2: `SAP_VALIDATED_COMPLETE` (SYSTEM_NO_ROLL, TIME_OUT,
  DBSQL_STMNT_TOO_LARGE incidents all SAP_VALIDATED_RESOLVED; SAT
  warm-to-cold classification = PACKAGE_E_FOLLOWUP, non-blocking for D2)

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

Package D2 is `SAP_VALIDATED_COMPLETE` (see the Package D2 final closeout
section above). Start Package E from the SAP-validated Package D2 baseline
(`733bb307`).

Package E scope (authorized, not started):

- E-PERF-OBJINDEX-1, E-CONSUMER-COHERENCE-1, E-CACHE-ADMIN-F4-1 (see the
  Package D2 final closeout section above for exact wording);
- snapshot consumer coherence and adaptive materialization per the binding
  constraints below;
- do not reopen Package C/D1/D2 without concrete regression evidence.

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
- Package D2 implementation handoff:
  `.memory/handoffs/variant-b-package-d-d2-implementation.md`
- Package D2 SYSTEM_NO_ROLL/TIME_OUT incident:
  `.memory/incidents/variant_b_d2_it8_system_no_roll_timeout.md`
- Package D2 DBSQL_STMNT_TOO_LARGE incident:
  `.memory/incidents/variant_b_d2_it8_dbsql_stmt_too_large.md`
- Package D2 SAT warm-to-cold performance classification:
  `.memory/incidents/variant_b_d2_sat_warm_to_cold_o4h8794.md`

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

1. ~~Package D shared design for Slices 7 and 8.~~ DONE.
2. ~~Package D1 implementation and checkpoint validation.~~ DONE (SAP_VALIDATED_COMPLETE).
3. ~~Package D2 implementation and checkpoint validation.~~ DONE (SAP_VALIDATED_COMPLETE; SYSTEM_NO_ROLL, TIME_OUT, DBSQL_STMNT_TOO_LARGE all SAP_VALIDATED_RESOLVED).
4. Package E focused discovery:
  branch-switch-to-Stage consumer coherence and current materialization cost
  (E-PERF-OBJINDEX-1, E-CONSUMER-COHERENCE-1, E-CACHE-ADMIN-F4-1).
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

Package D2 is `SAP_VALIDATED_COMPLETE` (see the Package D2 final closeout
section above) - all three live IT8 incidents (`SYSTEM_NO_ROLL`,
`TIME_OUT`, `DBSQL_STMNT_TOO_LARGE`) are `SAP_VALIDATED_RESOLVED`, and the
follow-up SAT warm-to-cold trace is classified `PACKAGE_E_FOLLOWUP`
(non-blocking for D2).

Start Package E in a new orchestrator chat, beginning with a focused design
and performance gate for OBJ_INDEX rebuild batching
(`E-PERF-OBJINDEX-1`) and the existing certified snapshot
consumer-coherence findings (`E-CONSUMER-COHERENCE-1`,
`E-CACHE-ADMIN-F4-1`).

Read only the current compact state and the Package D2 final closeout
section/linked artifacts above. Do not repeat Package D0-D2 discovery,
design, reconciliation, review, or incident diagnosis - all are closed with
no open blockers. Do not repeat Package C discovery, design review or
implementation review unless Package E exposes a concrete regression.
