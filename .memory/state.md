# ORTEC abapGit opt-rework — active state

```text
BRANCH=ortec/abapgit_1_133-opt-rework
CURRENT_HEAD=f54860d18bb9c8e7c3ca6dd04f04d1c9e73b455c ("Syntax und Unit Test
  Fixes" - final IT8 syntax/ABAP Unit fixes, owner-confirmed source for
  SER-SLICE-4 closeout) plus this session's SER-SLICE-5 memory + the
  SLICE5-001 productive fix (see below)
LATEST_SAP_VALIDATED_HEAD=f54860d18bb9c8e7c3ca6dd04f04d1c9e73b455c
  (owner IT8 evidence, SER-SLICE-5 kickoff prompt 2026-08-08:
  ACTIVATION/ATC/ABAP_UNIT/MULTI_REPOSITORY/MULTI_SLICE/OUTPUT_PARITY/
  FULL_REPOSITORY_BATCH_RUN=PASS, INDIVIDUAL_PROVIDER_OFF_ON_BENCHMARKS=
  WAIVED_BY_OWNER, ALL_ENABLED_PROVIDERS_INTEGRATED_RUN=PASS,
  LATE_CALLBACK_TEST=DEFERRED_OWNER_ACCEPTED). SER-SLICE-5 discovery
  found and fixed SLICE5-001 (batch RFC worker never activated the
  provider/WAPA gate) - NOT yet IT8-validated, see
  `.memory/handoffs/serialization-slice-5.md`. The DTEL/DOMA parity
  incident below is SUPERSEDED_FALSE_ORACLE - do not treat it as an open
  blocker.
```

```text
SER_SLICE_4=SAP_VALIDATED_COMPLETE
SER_SLICE_5=SAP_VALIDATED_COMPLETE_WITH_WAPA_RUNTIME_TEST_DEFERRED
  (`.memory/handoffs/serialization-slice-5.md`, "IT8 CLOSEOUT" section) -
  SLICE5-001 confirmed live on IT8 (active source match, 151/151 ABAP Unit,
  ATC clean, gate lifecycle exception-safe by exhaustive static proof).
  Residuals (do not block closeout, see handoff): WAPA replacement not
  runtime-exercised (no fixture), worker-side provider consumption proven
  only statically (no live counter/trace this pass). The "fresh SAT
  retest not yet run" residual is now CLOSED by SER-FINAL (2026-08-10,
  see below): the owner-supplied `FUGR Set - Batch - Worker` true-worker
  traces show the FUGR provider actually consulted and mostly HIT inside
  the RFC worker's own aRFC session. DDLS remains WAIVED_BY_OWNER/
  DEFERRED.
```

```text
SER_FINAL_WAPA_FUGR=EVIDENCE_REVIEW_COMPLETE_NO_IMPLEMENTATION
  (2026-08-10, `.memory/logs/ser_final_wapa_fugr_evidence.md` +
  `ser_final_wapa_design.md` + `ser_final_fugr_design.md` +
  `ser_final_ddls_disposition.md`, reviews under `.memory/reviews/
  ser_final_*`). Owner supplied fresh dedicated WAPA/FUGR SAT traces plus
  mixed FUGR+DDLS+WAPA Main/Worker traces; classified each as MAIN/
  AGGREGATE_PARALLEL/TRUE_WORKER before drawing conclusions. Findings:
  (1) closes the SER-SLICE-5 SAT-retest residual (see above); (2)
  WAPA=D_KEEP_SINGLETON_NO_CHANGE - `ZCL_ABAPGIT_ORTEC_WAPA` already
  implements the only safe optimization, remaining O2PAGCON cluster-import
  cost is architecturally bounded, no batch-worker WAPA evidence exists to
  justify multi-WAPA batching; (3) FUGR=F_NO_FURTHER_OPTIMIZATION_THIS_
  PASS - found a real, evidenced ~1.3s/trace direct-DB cost inside
  `ZIF_ABAPGIT_OBJECT~CHANGED_BY` (own uncached `RS_GET_ALL_INCLUDES` +
  `REPOTEXT`/`REPOSRC`/`EUDB`/`D010INC`/`RSEUINC` reads, 0% covered by the
  existing dispatch-scoped ENLFDIR/func-metadata provider because
  `CHANGED_BY` runs on a different, repository-wide status-calc lifecycle)
  but deliberately deferred as a named follow-up rather than implemented
  without a dedicated design+adversarial pass - see "Resumable backlog
  topics" below (`FUGR-CHANGED-BY-STATUS-SWEEP`); (4)
  DDLS=DEFER_NO_MATERIAL_SAFE_CHANGE, unchanged. No productive ABAP source
  was changed this pass. Full detail:
  `.memory/handoffs/ser-final-wapa-fugr-it8.md`.
```

```text
SERIALIZATION_TARGET_ARCHITECTURE=TWO_PATH_ONLY
PURE_STANDARD_PATH=VERIFIED_CORRECT_KEEP
ADAPTIVE_BATCH_PATH=KEEP_AND_EXTEND
LEGACY_ORTEC_NON_BATCH_PATH=REMOVE_DO_NOT_REPAIR
DOMA_DTEL_PROVIDER=ACCEPTED
ACTIVE_PROVIDER_BLOCK=CLAS_INTF_AND_MANDATORY_FAMILY_ASSESSMENT
REPOSITORY_SETTING_CHECKBOX=OPEN_REQUIRED
FINAL_PATH_ISOLATION=OPEN_REQUIRED
LATE_CALLBACK_TEST=DEFERRED_OWNER_ACCEPTED
```

## Active topic

```text
TOPIC=SER_SLICE_5_IT8_CLOSEOUT
STATUS=SAP_VALIDATED_COMPLETE_WITH_WAPA_RUNTIME_TEST_DEFERRED
SER_SLICE_3=SAP_VALIDATED_COMPLETE_WITH_LATE_CALLBACK_TEST_DEFERRED
SER_SLICE_4=SAP_VALIDATED_COMPLETE
SER_SLICE_5=SAP_VALIDATED_COMPLETE_WITH_WAPA_RUNTIME_TEST_DEFERRED
```

SER-SLICE-5 (2026-08-08): closed SER-SLICE-4 from owner IT8 evidence, verified
the final two-path architecture against two full-repository SAT traces (both
read in full), and ran trace-grounded FUGR/DDLS/WAPA/tail-latency discovery.
Found and fixed SLICE5-001: the ORTEC batch RFC worker
(`Z_ABAPGIT_ORTEC_SER_BATCH`) never activated `is_serial_prefetch_active`/
`is_wapa_active` in its own aRFC session, so every one of the 8 built provider
families and the WAPA batch-path replacement silently fell back to per-object
reads in every real production batch dispatch - a confirmed, source- and
trace-verified regression, fixed with a 2-line precedented change (mirrors the
existing standard RFC's own activation pattern). Correctness was never at
risk. `OWNER_ACTION_REQUIRED`: rerun the SER-SLICE-4 IT8 SAT comparison with
this fix before any further FUGR/DDLS/WAPA work. See
`.memory/handoffs/serialization-slice-5.md` for full detail and
`.memory/reviews/serialization_final_two_path_trace_audit.md` for the finding.

Do not resume Variant B / Package E/F backlog topics (below) without a
new explicit owner instruction naming that topic.

DOMA/DTEL parity incident: SUPERSEDED_FALSE_ORACLE (owner IT8 debug
evidence, 2026-08-07). The batch serializer's DOMA/DTEL data was correct
all along; the previously-trusted "113 files" Feature-OFF oracle came
from the legacy ORTEC non-batch path (Path 3), which is independently
known to report false MODIFIED results and is being removed, not
repaired. Full detail (kept as history, not an open blocker):
`.memory/incidents/serialization_slice_3_dtel_doma_parity.md`.
`DOMA_DTEL_PROVIDER=IMPLEMENTED_AND_OWNER_DEBUG_VALIDATED`.

Current work: complete the two-path architecture (pure standard path OFF
/ adaptive batch path ON, no third hybrid path reachable), a
repository-scoped setting to replace the global `is_serial_batch_active`/
`is_serial_prefetch_active` session switches, a CLAS/INTF batch provider,
the mandatory 10-family assessment, and removal of Path 3. See
`.memory/handoffs/serialization-slice-3.md` for the live handoff.

### SER-SLICE-4 (TABL/PROG/FUGR batch providers - implementation complete, awaiting IT8)

```text
SER_SLICE_3=SAP_VALIDATED_COMPLETE_WITH_LATE_CALLBACK_TEST_DEFERRED
SER_SLICE_4=LOCAL_COMPLETE_AWAITING_CONSOLIDATED_IT8
AUTHORITATIVE_BASELINE=8c9e5df4f9dd4fdaa4e05103cc0ac1e773758a32
```

Owner authorized implementation of Package A (TABL partial provider),
Package B (PROG), Package C (FUGR), and the shared aggregate-byte-
admission prerequisite (TTYP and all full-provider follow-ups remain
`DEFERRED_BY_APPROVED_DESIGN`, unchanged from the design phase). All
three packages plus the prerequisite are implemented, get_errors-clean,
locally unit-tested, and committed: `9a67c8ee` (Phase 2 + Package A),
`bf11d559` (Package B), `f545fc45` (Package C), `070a8775` (IC-002 fix:
aggregate byte-admission I-precision overflow), `f75f1e87` (PS-001 fix:
O(K^2) batch-entry correlation lookups). Post-implementation correctness
review (8 invariants) and performance IMPLEMENTATION_AUDIT both
verdict `APPROVE` after those two fixes. No live SAP syntax check or
ABAP Unit execution performed (no live connectivity this session).
`OWNER_ACTION_REQUIRED=RUN_CONSOLIDATED_IT8_VALIDATION` per
`.memory/logs/serialization_slice_4_it8_validation_plan.md`. Full detail:
`.memory/handoffs/serialization-slice-4.md` ("IMPLEMENTATION UPDATE"
section), `.memory/reviews/serialization_slice_4_implementation_
correctness.md`, `.memory/reviews/serialization_slice_4_implementation_
performance.md`, `.memory/logs/serialization_slice_4_{tabl_ttyp,prog,
fugr}_implementation.md`.

Design-phase history (unchanged, still accurate as the approved design
this implementation followed):

Convergent design-only pass for the remaining serialization providers
(TABL/TTYP, PROG, FUGR) while the owner independently ran SER-SLICE-3's
consolidated IT8 validation. Decisions: `TABL=IMPLEMENT_PARTIAL_PROVIDER`
(per-extra-language DD02T text + TDDAT extras only; TTYP=`DEFER`),
`PROG=IMPLEMENT_METADATA_TEXT_PROVIDER`,
`FUGR=IMPLEMENT_METADATA_AND_DIRECTORY_PROVIDER`. Central finding: PROG's
and FUGR's EXISTING single-object prefetch seams currently provide ZERO
benefit under the RFC/adaptive-batch dispatch path (`before_dispatch`
never populates the old generic `iv_prefetch_buffer_ext`) - both designs
fix this dead optimization path, not a new large win. TABL's DD03P
(fields, SAP-runtime-flattened/include-resolved) is explicitly excluded
as too high-risk for naive bulk reconstruction this slice (named,
NOT-authorized follow-up only). All three designs passed 3 adversarial
review cycles (0 open blockers/majors), a cross-package correctness gate
(`APPROVE_WITH_MINOR_REVISIONS` - one reviewer finding, CG-001, was
independently re-verified against live source by the orchestrator and
`REJECTED_WITH_PROOF`), and a performance design gate (`APPROVE` after
fixing one real BLOCKER, PF-001: an O(N^2)-shape ABAP cache-scan defect
in the TABL design, fixed before this slice closed). No productive code/
DDIC/UI/RFC/test changes were made. Full detail, decisions, and the
17-slice implementation-readiness breakdown:
`.memory/handoffs/serialization-slice-4.md` (primary handoff),
`.memory/logs/serialization_slice_4_common_discovery.md`,
`.memory/logs/serialization_slice_4_tabl_ttyp_design.md`,
`.memory/logs/serialization_slice_4_prog_design.md`,
`.memory/logs/serialization_slice_4_fugr_design.md`,
`.memory/logs/serialization_slice_4_shared_infrastructure.md`,
`.memory/reviews/serialization_slice_4_tabl_ttyp_adversarial.md`,
`.memory/reviews/serialization_slice_4_prog_adversarial.md`,
`.memory/reviews/serialization_slice_4_fugr_adversarial.md`,
`.memory/reviews/serialization_slice_4_correctness.md`,
`.memory/reviews/serialization_slice_4_performance.md`,
`.memory/reviews/serialization_slice_4_readiness.md`.

`OWNER_DECISION_REQUIRED` (superseded - Package A/B/C were subsequently
authorized and implemented, see the "SER-SLICE-4" section above): whether
to authorize implementation of Package A (TABL)/B (PROG)/C (FUGR),
independently or together - do not start implementation on any of them
without an explicit new owner instruction naming the package(s).

SER-SLICE-2 is now SAP_VALIDATED_COMPLETE (owner evidence: ATC=PASS,
ABAP_UNIT=PASS, FEATURE_OFF_TEST=PASS, FEATURE_ON_AFTER_RPERF_FIX=PASS,
NO_DUMP_AFTER_RPERF_FIX=YES, OUTPUT_PARITY_AFTER_RPERF_FIX=PASS).
LATE_CALLBACK_TEST=DEFERRED_OWNER_ACCEPTED (documented residual, not a
blocker). Do not reopen SER-SLICE-2 correctness/RPERF work without new
contradicting evidence. Full detail:
`.memory/handoffs/serialization-slice-2.md` ("Final IT8 validation"
section, 2026-08-06).

SER-SLICE-0 and SER-SLICE-1: SAP_VALIDATED_COMPLETE (established prior
sessions, not revisited this pass).

SER-SLICE-2 (adaptive, cost-aware, bounded multi-object serialization
batching via `ZCL_ABAPGIT_ORTEC_SER_ORCH`, gated behind
`zcl_abapgit_ortec_git_switch=>is_serial_batch_active`, default OFF):

```text
SER_SLICE_2_STATUS=SAP_VALIDATED_COMPLETE_WITH_LATE_CALLBACK_TEST_DEFERRED
```

SAP-validated complete as of 2026-08-06 (owner evidence in the "Final IT8
validation" section of `.memory/handoffs/serialization-slice-2.md`); the
IT8 validation plan below is now historical record of how that result was
reached, not an open gate. Historical local-review narrative retained
below for context:

- Terminal-outcome correction (2026-08-06) is now locally review-clean:
  successful return requires `wait_result = 0`, `terminal_count =
  expected_count`, and `failed_count = 0`; callback-side `drain_queue`
  helper failures now become explicit failed-object outcomes, including
  the formerly vulnerable "selected batch removed from queue before
  dispatch" window; terminal/failure counts are maintained as O(1)
  run-context counters rather than rescanned per object in the `WAIT
  UNTIL` completion predicate.
- Current independent review state for that correction:
  correctness = APPROVE_WITH_MINOR_REVISIONS,
  adversarial = PASS,
  regression = PASS_WITH_FINDINGS,
  performance scan = PASS_WITH_FINDINGS,
  performance audit = PASS_WITH_MINOR_FINDINGS.
- Honest live-validation boundary: local `get_errors` is clean on ORCH
  main + testclasses, but a full SAPDiagnose class-pool dry-run is still
  pending import/activation of the CURRENT local testclasses include on
  IT8. The latest local dry-run got past the repaired main-include syntax
  issues and then failed on the live system's stale testclasses include
  still referencing `C_STATE_ABANDONED`.

- Phase 1/2 contracts + full ORCH state machine + minimal standard hook
  implemented and locally verified (`get_errors` clean; see the live
  syntax boundary note above for why a full clean class-pool dry-run is
  not yet claimed here).
- Minimal-hook restoration done: `ZCL_ABAPGIT_SERIALIZE=>IS_NO_PARALLEL`
  reverted to its exact original PRIVATE instance form (confirmed via
  diff against the pre-SER-SLICE-2 parent `ada103d5` - only the hook
  block itself remains as a residual, intentional diff). ORCH now uses
  its own private local copy `IS_STANDARD_NO_PARALLEL_TYPE` (parity-
  pinned by a unit test) instead of calling the standard method.
- Independent adversarial audit (5 MAJOR findings, 0 BLOCKER) and
  independent performance implementation audit
  (APPROVE_WITH_MINOR_REVISIONS) both completed and all in-scope
  findings fixed:
  - AR-1-001 (i18n per-object MAIN_LANGUAGE_ONLY override lost inside a
    batch) - fixed via forced-sequential routing + ROUTE_TO_SEQUENTIAL_
    FALLBACK recompute.
  - AR-1-002 (circuit breaker tripped but never read) - fixed:
    BEFORE_DISPATCH now gates on MT_BROKEN_RUNS first.
  - AR-1-003 (task-name collision risk from truncated RUN_ID hex) -
    fixed via a new session-wide monotonic NEXT_TASK_NAME helper.
  - AR-1-004 (silent resolved-without-output on merge failure) - fixed:
    MERGE_INTO_MT_FILES returns RV_MERGED, checked by the caller.
  - AR-1-005 (avoidable 5s wait on empty work) - fixed: exit check moved
    before the first WAIT.
  - PS-001 (blind WAIT UP TO 5 SECONDS never returns early for an aRFC
    callback, confirmed via ABAP Keyword Documentation) - fixed via
    WAIT FOR ASYNCHRONOUS TASKS UNTIL <run>-changed = abap_true UP TO
    5 SECONDS.
  - PS-002/PS-003 (dispatch-table scan shape / resolved-table growth) -
    reviewed and accepted as documented, non-blocking trade-offs; no
    code change (see performance implementation audit artifact).
- All ten section-9 limit constants re-verified: 5 ENFORCED, 4
  DECLARED_ONLY (disclosed follow-up scope, not silent gaps), 1
  DEFERRED_BY_APPROVED_SCOPE (c_max_actual_batch_bytes - batch-scoped
  prefetch extraction does not exist yet), 0 BROKEN.
- Owner correction `3a85b286` classification: ORCH `MERGE_INTO_MT_FILES`
  IMPORT TRY/CATCH = SYNTAX_ONLY and semantically safe; ORCH testclasses
  adjustments = TEST_FIX; `mv_serial_batch_active VALUE abap_true` = real
  SEMANTIC_CHANGE and was reverted in that pass's working tree back to
  default OFF. SUPERSEDED 2026-08-06: owner commit `e34c7e06` flipped it
  back to `abap_true` (default ON) after FEATURE_ON_AFTER_RPERF_FIX=PASS
  IT8 evidence - this is now the confirmed, IT8-validated default; do not
  revert it again without new contradicting evidence. Porcelain changes
  remain unrelated to this topic.
- Stage A local closeout changes (current working tree, not yet owner IT8-
  validated): fail-fast WAIT contract (successful return iff the run is
  complete; WAIT result 4/8 while incomplete raises a visible abapGit
  exception and discards the entire partial result), WAPA singleton batch
  admission (batch path YES, mixed batches NO), removed obsolete
  IV_TEST_DELAY_S T-DRAIN seam from the batch RFC interface/body, and
  strengthened MERGE_INTO_MT_FILES tests (path/item/multi-file/empty-list/
  bad-payload/no-partial-append coverage).
- Disclosed, still-open limitations after Stage A: no batch-scoped
  prefetch buffers yet; IV_ABAP_LANGUAGE_VERS always SPACE; PROVIDER_HIT/
  MISS/FALLBACK always 0. The old long-running T-DRAIN gate is now
  superseded by the fail-fast WAIT/error contract and is no longer a live
  open slice.
- Checkpoint commits (not pushed): `6a94b62c` (restore minimal hook),
  `1278b13d` (adversarial + performance fixes), `f6a021ac` (IT8 plan
  expansion, memory-only).
- WAPA current decision: WAPA is no longer blanket-excluded from the batch
  RFC. Current Stage-A source admits it only as singleton batches; no two
  WAPAs and no WAPA+non-WAPA mix. Owner reports long-running productive
  output parity for the replacement WAPA serializer; Stage-A still needs
  final IT8 trace/parity confirmation for the singleton-batch path.
- T-DRAIN current decision: superseded. Stage-A no longer uses the old
  T/X/abandon/drain lifecycle or the IV_TEST_DELAY_S seam; the owner IT8
  gate is now the fail-fast WAIT/error contract in
  `.memory/logs/serialization_slice_2_it8_validation_plan.md` section 4.

Authoritative artifacts (read these, do not re-derive):
`.memory/handoffs/serialization-slice-2.md` (primary status handoff),
`.memory/logs/serialization_adaptive_batch_design.md` (design, sect 1-10),
`.memory/logs/serialization_slice_2_it8_validation_plan.md` (execution
checklist - nothing below may be claimed complete until this passes),
`.memory/reviews/serialization_slice_2_hook_audit_adversarial.md`,
`.memory/reviews/serialization_slice_2_performance_scan.md`,
`.memory/reviews/serialization_slice_2_performance_implementation_audit.md`.

## Next action

SER-SLICE-2 IT8 validation plan executed and PASSED (see "Final IT8
validation" section of `.memory/handoffs/serialization-slice-2.md`);
`is_serial_batch_active` is now IT8-validated default ON. Do not re-run
this gate absent new contradicting evidence.

SER-SLICE-3 implementation is now IN_PROGRESS on top of `daef510e`.
Discovery result remains authoritative for ranking (see
`.memory/logs/serialization_slice_3_discovery.md`):

- no `ZCL_ABAPGIT_ORTEC_SER_PROV_DD` class exists yet in source;
- existing prefetch wire formats (`ser_pref`, `_ext`, `_oo`) are strictly
  single-object EXPORT/IMPORT payloads and cannot be concatenated safely;
- ordered static-evidence ranking: DTEL > DOMA > CLAS/INTF >
  MSAG/TRAN/FUGR/PROG > TABL/TTYP > DDLS/DCLS > WAPA/ENQU/SHLP/VIEW;
- first provider slice recommendation: DOMA/DTEL;
- smallest viable design shape: additive `EXTRACT_FOR_BATCH` methods on
  the existing prefetch classes plus one real versioned batch envelope.

SER-SLICE-3 P2A (SER_SLICE_3_P2A_DD_PROVIDER_CORE, this pass) is
IMPLEMENTED_LOCAL_NOT_IT8_VALIDATED: `ZCL_ABAPGIT_ORTEC_SER_PREF_EXT`
gained `mt_doma`/`prepare_doma`/`get_doma_data`/`get_doma_i18n` (design
&sect;1) and `extract_for_batch`/`inject_batch_from_buffer` (design &sect;2),
plus new DDIC `ZAOG_SER_DD_BHDR` (structure), `ZAOG_SER_DD_BENTRY`
(structure), `ZAOG_SER_DD_BENTRY_TT` (table type) in
`src/ortec/serial/core/`.

SER-SLICE-3 Phase 2 is now FULLY IMPLEMENTED (local, not yet IT8-
validated) - the ORCH `BEFORE_DISPATCH`/`DISPATCH_BATCH` wiring and the
`ZCL_ABAPGIT_OBJECT_DOMA` seam originally scoped out of P2A were completed
in the same pass, plus the RFC worker's `iv_prefetch_buffer_dd` injection.
An independent implementation-time correctness review found and this
orchestrator fixed 1 BLOCKER (DR-001: a domain with no main-language
DD01T/DD07T text row was silently dropped entirely - a real data-loss
bug, unlike the unmodified standard DDIF_DOMA_GET path) plus 2 major/2
minor test-coverage gaps (all fixed). Full detail, review verdicts, and
the exact fix disposition:
`.memory/handoffs/serialization-slice-3.md` (primary handoff),
`.memory/logs/serialization_slice_3_provider_contract.md` (finalized
Phase 1 wire format),
`.memory/logs/serialization_slice_3_doma_dtel.md` (Phase 2 implementation
log incl. owner DDIC creation manifest),
`.memory/logs/serialization_slice_3_clas_intf.md` (Phase 3 - DEFERRED
with exact reason, not implemented this run),
`.memory/logs/serialization_slice_3_object_ranking.md` (Phase 4 - all
other requested object families assessed and dispositioned),
`.memory/reviews/serialization_slice_3_correctness.md`,
`.memory/reviews/serialization_slice_3_performance.md`,
`.memory/reviews/serialization_slice_3_adversarial.md`,
`.memory/logs/serialization_slice_3_it8_validation_plan.md` (consolidated
IT8 checklist - nothing above may be claimed SAP-validated until this
passes).

This paragraph and the P2A/Phase-2 paragraphs above it remain accurate for
what was implemented (DOMA/DTEL provider, wire format, ORCH wiring) -
only the *incident interpretation* below them changed.

The DTEL/DOMA parity incident recorded in
`.memory/incidents/serialization_slice_3_dtel_doma_parity.md` is
`SUPERSEDED_FALSE_ORACLE` (owner IT8 debug evidence, 2026-08-07): the
batch serializer's DOMA/DTEL output was correct; the "113 files" oracle
came from the legacy ORTEC non-batch path (Path 3), independently known
to report false MODIFIED results. `DOMA_DTEL_PROVIDER=IMPLEMENTED_AND_
OWNER_DEBUG_VALIDATED`. Current HEAD `bf436db0` includes this session's
Fix A-F (`5ff237b9`) plus the owner's own follow-up hardening commit.

```text
SER_SLICE_3_STATUS=IN_PROGRESS_TWO_PATH_ARCHITECTURE
```

Authorized and in progress (this session, explicit owner instruction
SER-SLICE-3 continuation): final two-path inventory/audit, a
repository-scoped adaptive-batch setting, CLAS/INTF batch provider, the
mandatory 10-family assessment, further justified providers, and removal
of the legacy ORTEC non-batch (Path 3) optimization path. See
`.memory/handoffs/serialization-slice-3.md` for the live handoff and
`.memory/logs/serialization_final_two_path_audit.md` for the path/hook
inventory.

Do not resume Variant B / Package E/F backlog topics (below) without a
new explicit owner instruction naming that topic.

## Resumable backlog topics (Variant B / Package E, paused)

Package E checkpoint 1 (E1-TEST, E3-TEST, E4-VERIFY, E-HARDEN OF-2) is
SAP_VALIDATED_COMPLETE (head `3c77d898`, 2026-07-29 - see
`.memory/handoffs/variant-b-package-e-checkpoint-1.md`). The items below
are paused, not abandoned; do not start real design/implementation work
on any of them before its entry condition is met.

```text
E1-TREE-REUSE: APPROVED_DESIGN, PARKED_MEASUREMENT_PENDING (owner
  decision 2026-07-20). Entry condition: a focused (non-aggregated) IT8
  SAT trace proves material, repeated REBUILD_INDEX tree-decode/mapping
  cost after the live 30000-row write-batching value, followed by an
  explicit new owner GO. Do not implement DDIC (ZAOG_TREE_MAP/
  ZAOG_TREE_CHILD) or code before that.
E2_CONSUMER_COHERENCE: POSTPONED by owner decision 2026-07-31. Active
  incident OS4 (large-repo false-MODIFIED, candidate root cause PC-1 -
  overview/Full-Stage use a cached remote-files path, single-object Diff
  independently revalidates via the ORTEC filtered-walk facade) has a
  complete static (D0/D1) trace but multiple contradictory, unvalidated
  fix hypotheses across handoff files. Entry condition: owner executes
  the bounded debugger worksheet in
  `.memory/logs/variant_b_package_e_false_modified_os4_d1.md` section 5
  and confirms a live mismatch, THEN resumes after Package E/
  serialization work is fully settled.
E4-OOB-DELETION-RISK: ACCEPTED_NON_BLOCKING_RISK (permanent, not an open
  item) - an out-of-band blob deletion bypassing all ORTEC write APIs is
  not detected by either repair mechanism, but consumers fail loudly
  (never silently wrong) and the existing manual remote re-fetch/
  rebuild_index path recovers. No E4-FIX slice authorized.
E-HARDEN-STANDARD-FILE-COUPLING: whether zcl_abapgit_git_porcelain's
  embedded ORTEC-aware branching/duplicate 'Walk,' retry logic should be
  refactored into one clean hook. Entry condition: explicit owner input
  on touching standard abapGit source for this.
SYSTEM_NO_ROLL-OS4-STAGE-AFTER-OVERVIEW: a SYSTEM_NO_ROLL dump when Full
  Stage runs immediately after a full Overview serialize of a 17321-
  object repo (cache/memory not released between the two). Not
  investigated. Entry condition: a dedicated Package E/F slice is
  scheduled. See
  `.memory/logs/variant_b_package_e_false_modified_os4_d1.md` section 11.
Package F (validated legacy-code cleanup): NOT_STARTED. Entry condition:
  Package E fully settled first.
FUGR-CHANGED-BY-STATUS-SWEEP: NOT_STARTED, design sketch only (2026-08-10,
  SER-FINAL). `ZCL_ABAPGIT_OBJECT_FUGR~ZIF_ABAPGIT_OBJECT~CHANGED_BY`
  independently calls `RS_GET_ALL_INCLUDES` plus `REPOTEXT`/`REPOSRC`/
  `EUDB`/`D010INC`/`RSEUINC` SELECTs with zero provider coverage
  (~1.3s aggregate DB time for 147 FUGR objects in the supplied trace) -
  uncovered because it runs on the repository-wide status-calc sweep, a
  different lifecycle than the existing dispatch-scoped `prepare_fugr`
  prefetch. See `.memory/logs/ser_final_fugr_design.md` Candidate B for
  the exact sketch and `.memory/reviews/ser_final_readiness.md` for what
  is still missing. Entry condition: owner authorizes a dedicated design
  +adversarial-review pass (this touches `CHANGED_BY`, which feeds
  `ZCL_ABAPGIT_CTS_INTEGRATION`, a correctness-sensitive area) AND the
  repository-wide `CHANGED_BY` sweep call site is located/read first.
```

Full narrative history, per-slice validation matrices, and incident
detail for the above remain in
`.memory/handoffs/variant-b-package-e-checkpoint-1.md`,
`.memory/logs/variant_b_package_e_design.md`,
`.memory/logs/variant_b_package_e_discovery.md`, and the incident files
under `.memory/incidents/` - this file intentionally does not duplicate
them.

## Binding invariants (all topics)

- Standard abapGit behavior is unchanged when any ORTEC feature flag is
  disabled (default state for every flag introduced by either the
  Variant B partial-clone work or SER-SLICE-2).
- No `deepen`/`shallow` widening in Variant B requests; no per-object
  SQL/HTTP; no uncertified haves; no productive blank repository-key
  fallback.
- Package D1 owns bounded external delta-base resolution; Package D2
  owns attempt/transaction isolation (both SAP_VALIDATED_COMPLETE - do
  not reopen without new contradicting evidence).
- WAPA is batch-eligible in SER-SLICE-2 only as a singleton batch: never
  mixed with any other WAPA and never mixed with any non-WAPA object
  (separate from the ECTC/ECTD-only no-parallel denylist).
- Any standard-abapGit-class change for an ORTEC hook must stay the
  smallest possible delegation - no unnecessary visibility/staticness
  changes to standard methods (see SER-SLICE-2's IS_NO_PARALLEL revert,
  this pass).
- Every slice, including test-only and doc-only ones, requires real IT8
  activation/syntax check + ABAP Unit PASS + ATC PASS before being
  considered complete - there is no test-only IT8 exemption.
