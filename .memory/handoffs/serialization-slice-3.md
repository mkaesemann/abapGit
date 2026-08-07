# SER-SLICE-3 — final two-path architecture (repository setting, CLAS/INTF, MSAG, Path 3 removal)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_3_FINAL_TWO_PATH_ARCHITECTURE
STATUS=LOCAL_COMPLETE_AWAITING_CONSOLIDATED_IT8
BASELINE_HEAD=bf436db0632de0098875d97b4a28a8f246eb50a5
```

## Summary (this continuation)

The DOMA/DTEL parity incident recorded below is `SUPERSEDED_FALSE_ORACLE`
- owner IT8 debug evidence established the batch serializer's DOMA/DTEL
output was correct all along; the "113 files" oracle came from the
legacy ORTEC non-batch path (Path 3), independently confirmed to report
false MODIFIED results. See
`.memory/incidents/serialization_slice_3_dtel_doma_parity.md`.

This continuation completed, locally, all 8 authorized phases:

```text
Phase 0/1: reconciled HEAD, corrected state.md/incident memory.
Phase 2: full path/hook inventory, `.memory/logs/
  serialization_final_two_path_audit.md` (Findings F-1/F-2/F-3, binding
  routing design).
Phase 3: repository-scoped "Use ORTEC Adaptive Batch Serialization"
  setting, replacing the global is_serial_batch_active toggle as the
  production routing decision (default OFF, per-repository, persisted).
Phase 4: CLAS/INTF batch prefetch provider (versioned multi-object
  envelope, 21 local tests).
Phase 5: mandatory 10-family assessment (`.memory/logs/
  serialization_mandatory_family_assessment.md`) - every family
  dispositioned, none silently omitted.
Phase 6: MSAG batch prefetch provider (T100/T100A/T100T only, DOKIL
  explicitly out of scope, 15 local tests).
Phase 7: removed the legacy ORTEC non-batch (Path 3) optimization path -
  is_serial_prefetch_active now defaults OFF and is only turned on by
  ZCL_ABAPGIT_ORTEC_SER_ORCH for the duration of its own run;
  is_wapa_active now delegates to the same flag instead of being
  unconditionally TRUE; the classic path's own prepare()/clear() block
  is deleted entirely.
Phase 8: correctness/adversarial/performance reviews (all APPROVE/
  APPROVE_WITH_MINOR_REVISIONS, 0 blockers/majors), this handoff, the
  consolidated IT8 validation plan.
```

## Final architecture

```text
Repository setting OFF -> pure standard abapGit path only (no ORTEC
  PREF/PREF_EXT/PREF_OO prepare/inject/lookup, no ORTEC WAPA
  replacement, no ORTEC batch RFC).
Repository setting ON  -> ORTEC adaptive batch path only (reviewed
  providers: DOMA, DTEL, CLAS, INTF, MSAG; ORTEC WAPA replacement as a
  singleton batch).
No third hybrid path is reachable - Path 3's own code (the classic
  path's prefetch prepare()/clear() block) no longer exists.
```

## What changed (files, this continuation)

```text
MODIFIED:
  src/objects/core/zcl_abapgit_serialize.clas.abap
  src/ortec/git/zcl_abapgit_ortec_git_switch.clas.abap (+.xml)
  src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap
    (+.testclasses.abap)
  src/ortec/serial/rfc/zabapgit_ortec_serial.fugr.z_abapgit_ortec_ser_batch.abap
    (+.xml)
  src/ortec/serial/zcl_abapgit_ortec_ser_pref_oo.clas.abap
  src/ortec/serial/zcl_abapgit_ortec_ser_pref.clas.abap
  src/ortec/zcl_abapgit_persistence_ortec.clas.abap
    (+.testclasses.abap)
  src/repo/zcl_abapgit_repo.clas.abap
  src/ui/pages/sett/zcl_abapgit_gui_page_sett_repo.clas.abap
CREATED:
  src/ortec/git/zcl_abapgit_ortec_git_switch.clas.testclasses.abap
  src/ortec/serial/zcl_abapgit_ortec_ser_pref_oo.clas.testclasses.abap
  src/ortec/serial/zcl_abapgit_ortec_ser_pref.clas.testclasses.abap
  src/ortec/serial/core/zaog_ser_env_bhdr.tabl.xml
  src/ortec/serial/core/zaog_ser_env_bentry.tabl.xml
  src/ortec/serial/core/zaog_ser_env_bentry_tt.ttyp.xml
  .memory/logs/serialization_final_two_path_audit.md
  .memory/logs/serialization_mandatory_family_assessment.md
  .memory/logs/serialization_repository_setting.md
  .memory/logs/serialization_slice_3_msag.md
UPDATED (memory):
  .memory/incidents/serialization_slice_3_dtel_doma_parity.md
  .memory/logs/serialization_slice_3_clas_intf.md (now IMPLEMENTED)
  .memory/logs/serialization_slice_3_object_ranking.md
  .memory/logs/serialization_slice_3_it8_validation_plan.md (fully
    consolidated for the whole scope)
  .memory/reviews/serialization_slice_3_correctness.md
  .memory/reviews/serialization_slice_3_adversarial.md
  .memory/reviews/serialization_slice_3_performance.md
  .memory/state.md
```

## Honest validation boundary

```text
GET_ERRORS=CLEAN on every touched/created file
LIVE_SYNTAX_DRY_RUN=NOT_RUN this session (no live SAP connectivity)
ABAP_UNIT=NOT_RUN_LIVE this session
CORRECTNESS_REVIEW=APPROVE_WITH_MINOR_REVISIONS (0 blocker/0 major - see
  serialization_slice_3_correctness.md)
ADVERSARIAL_REVIEW=APPROVE_WITH_MINOR_REVISIONS (0 blocker/0 major - see
  serialization_slice_3_adversarial.md)
PERFORMANCE_REVIEW=APPROVE (see serialization_slice_3_performance.md,
  Phase 8 section)
DIRECT SERIALIZED FILE-SET PARITY for CLAS/INTF/MSAG against a real live
  object was NOT run (no live connectivity) - this is the primary
  remaining IT8-only validation gap, same class of boundary DOMA/DTEL had
  before its own owner-debug validation.
```

## Global object creation required before IT8

```text
OWNER_ACTION_REQUIRED=CREATE_GLOBAL_OBJECTS (see section 1 of
`.memory/logs/serialization_slice_3_it8_validation_plan.md` for the full
dependency-sorted manifest - this includes both the pre-existing
ZAOG_SER_DD_* objects, still not yet created on IT8, and the new generic
ZAOG_SER_ENV_* objects this continuation introduced).
```

## Next action

Owner creates the global DDIC objects per the manifest, imports/activates
in the stated order, then runs the consolidated IT8 validation plan
(`.memory/logs/serialization_slice_3_it8_validation_plan.md`) sections
1-10 in full. Do not report SER-SLICE-3 as SAP-validated/complete before
that.

---

## Original DOMA/DTEL-scoped handoff (prior pass, retained for history)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_3_BATCH_PREFETCH_PROVIDERS
STATUS=BLOCKED_BY_DTEL_DOMA_PARITY_FIXES_APPLIED_AWAITING_RETEST
BASELINE_HEAD=daef510e9bd50cdef2adcfb26a3f2a01050bb401
SER_SLICE_2_STATUS=SAP_VALIDATED_COMPLETE_WITH_LATE_CALLBACK_TEST_DEFERRED
```

## Parity incident (2026-08-07)

The owner's IT8 import surfaced a real output-parity failure: Feature ON
produced 2 files instead of 113 for a real repository. Full investigation,
root cause, fixes, and review verdicts:
`.memory/incidents/serialization_slice_3_dtel_doma_parity.md`. Six
corrections applied (prepare()/clear() wiring, extract_for_batch's
all-miss guard, a new zero-file-success guard in ON_END_OF_BATCH, an
unconditional worker-side DD-cache clear, and hardened zero-file guards in
ROUTE_TO_SEQUENTIAL_FALLBACK and MERGE_INTO_MT_FILES). Adversarial review
cycle 1 REJECTED the first pass (2 blockers found via active attack);
cycle 2 APPROVED after fixes. Correctness review: APPROVE_WITH_MINOR_
REVISIONS (1 disclosed, non-blocking performance trade-off). Do NOT
report SER-SLICE-3 as SAP-validated/complete until the owner re-runs the
parity retest (IT8 validation plan section 0) against this exact
repository/scope and confirms byte-identical Feature ON/OFF output.

## Summary

Phase 0 (reconciliation): current HEAD `daef510e` classified and confirmed
as the correct SER-SLICE-3 baseline - see
`.memory/handoffs/serialization-slice-2.md`'s "Final IT8 validation"
section for the full owner-commit classification (RPERF_ILLEGAL_STATEMENT
fix, `mv_serial_batch_active` semantic flip to ON, cosmetic reformatting).

Phase 1 (wire format): finalized in
`.memory/logs/serialization_slice_3_provider_contract.md`, superseding the
older draft's generic-interface class split (no second provider family
exists yet to justify it) while keeping its lifecycle/byte-gating/DOMA
table-semantics concepts. One design-review cycle (3 major/3 minor, all
fixed).

Phase 2 (DOMA/DTEL provider): IMPLEMENTED. See
`.memory/logs/serialization_slice_3_doma_dtel.md` for the full change list,
`.memory/reviews/serialization_slice_3_correctness.md` for the
implementation-time correctness review (1 BLOCKER found and fixed - a real
silent-data-loss bug, DR-001), and
`.memory/reviews/serialization_slice_3_performance.md` (PASS_WITH_FINDINGS,
non-blocking).

Phase 3 (CLAS/INTF): DEFERRED with an explicit, evidence-based reason - see
`.memory/logs/serialization_slice_3_clas_intf.md`. Not implemented this
run.

Phase 4 (remaining object families): ranking reused from existing discovery
evidence (CONFIRMED_CURRENT, not re-derived), dispositions recorded in
`.memory/logs/serialization_slice_3_object_ranking.md`. No new
implementation authorized or performed for any of these families.

Phase 5 (additional providers): moot this run - CLAS/INTF (the next-ranked
family) was itself deferred, so no further provider was implemented.

Phase 6 (mixed-provider integration/regression): `batch_round_trip_finds_
data` (ltcl_dd_batch_wire) exercises a genuine mixed DOMA+DTEL batch
envelope round trip. SER-SLICE-2's own existing ORCH test suite (terminal-
outcome accounting, no-partial-success, MERGE_INTO_MT_FILES, WAPA
singleton, etc.) is unmodified in logic and must be re-run live as part of
the consolidated IT8 pass (section 2 of the validation plan) to confirm no
regression - not re-verified independently this session beyond `get_errors`
and manual source review, since none of that logic's own bodies changed.

## What changed (files)

```text
MODIFIED:
  src/ortec/serial/zcl_abapgit_ortec_ser_pref_ext.clas.abap
  src/ortec/serial/zcl_abapgit_ortec_ser_pref_ext.clas.testclasses.abap
  src/objects/zcl_abapgit_object_doma.clas.abap
  src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap
  src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.testclasses.abap
  src/ortec/serial/rfc/zabapgit_ortec_serial.fugr.z_abapgit_ortec_ser_batch.abap
CREATED:
  src/ortec/serial/core/zaog_ser_dd_bhdr.tabl.xml
  src/ortec/serial/core/zaog_ser_dd_bentry.tabl.xml
  src/ortec/serial/core/zaog_ser_dd_bentry_tt.ttyp.xml
  .memory/logs/serialization_slice_3_provider_contract.md
  .memory/logs/serialization_slice_3_doma_dtel.md
  .memory/logs/serialization_slice_3_clas_intf.md
  .memory/logs/serialization_slice_3_object_ranking.md
  .memory/logs/serialization_slice_3_it8_validation_plan.md
  .memory/reviews/serialization_slice_3_correctness.md
  .memory/reviews/serialization_slice_3_performance.md
  .memory/reviews/serialization_slice_3_adversarial.md
  .memory/handoffs/serialization-slice-3.md (this file)
```

## Honest validation boundary

```text
GET_ERRORS=CLEAN on every touched/created file (final re-verification
  after the DR-001 fix and the method-name-length rename)
LIVE_SYNTAX_DRY_RUN=EXPECTED_DDIC_FAILURE only, confirmed via live
  SAPRead that the 3 new DDIC objects do not exist on IT8 yet
ABAP_UNIT=NOT_RUN_LIVE this session (no IT8 DDIC objects to activate
  against)
ATC=NOT_RUN_LIVE this session
IMPLEMENTATION_CORRECTNESS_REVIEW=one cycle, 1 blocker/2 major/2 minor,
  all fixed and self-verified by the orchestrator, NOT independently
  re-reviewed a second time (disclosed explicitly in
  serialization_slice_3_correctness.md)
PERFORMANCE_SCAN=PASS_WITH_FINDINGS, non-blocking
ADVERSARIAL_COVERAGE=covered via the design review (one cycle, fixed) and
  the implementation correctness review's own adversarial-flavored focus
  items; no separate post-implementation adversarial subagent pass was
  run (disclosed in serialization_slice_3_adversarial.md)
```

## Next action

Owner runs the mandatory parity retest FIRST (section 0 of
`.memory/logs/serialization_slice_3_it8_validation_plan.md`) against the
exact same repository/scope as the incident screenshots, confirming
byte-identical Feature ON/OFF output. Only then proceed to the rest of the
consolidated IT8 validation plan (create the 3 new DDIC objects,
import/activate in the stated order, run ABAP Unit + ATC, execute the
functional/performance parity runs, then report back using that file's
decision matrix). Do not report SER-SLICE-3 as SAP-validated/complete
before that.
