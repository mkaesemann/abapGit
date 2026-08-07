# SER-SLICE-3 — batch prefetch providers (DOMA/DTEL implemented, CLAS/INTF deferred)

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
