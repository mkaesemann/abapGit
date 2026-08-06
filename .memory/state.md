# ORTEC abapGit opt-rework — active state

```text
BRANCH=ortec/abapgit_1_133-opt-rework
CURRENT_HEAD=3a85b2863fb4a242504e86fd76919a0a72f837d4 (+ local Stage-A working tree)
LATEST_SAP_VALIDATED_HEAD=3c77d898b62f3b0464e48cf59844e2e30c7b6e89
  (Package E checkpoint 1, 2026-07-29 - see Variant B backlog below)
```

## Active topic

```text
TOPIC=SERIALIZATION_PERFORMANCE
STATUS=IN_PROGRESS
```

SER-SLICE-0 and SER-SLICE-1: SAP_VALIDATED_COMPLETE (established prior
sessions, not revisited this pass).

SER-SLICE-2 (adaptive, cost-aware, bounded multi-object serialization
batching via `ZCL_ABAPGIT_ORTEC_SER_ORCH`, gated behind
`zcl_abapgit_ortec_git_switch=>is_serial_batch_active`, default OFF):

```text
SER_SLICE_2_STATUS=LOCAL_COMPLETE_AWAITING_FINAL_IT8
```

Do NOT report this as complete/enforced/validated until the IT8
validation plan below is executed. Current local state:

- Phase 1/2 contracts + full ORCH state machine + minimal standard hook
  implemented and locally verified (live `SAPDiagnose(action="syntax")`
  dry-runs clean, local `get_errors` clean).
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
  SEMANTIC_CHANGE and therefore reverted in the current Stage-A working
  tree back to default OFF; porcelain changes are unrelated to this topic.
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

Run the SER-SLICE-2 IT8 validation plan in full (import/activate in
dependency order, ABAP Unit, ATC, fail-fast WAIT/error-contract cases,
callback/run isolation, output parity incl. WAPA + i18n-pattern cases,
performance comparison) before enabling `is_serial_batch_active` outside
controlled validation, or before reporting SER-SLICE-2 as SAP-
validated/complete.
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
- WAPA is never batch-eligible in SER-SLICE-2 (structural exclusion,
  separate from the ECTC/ECTD-only no-parallel denylist).
- Any standard-abapGit-class change for an ORTEC hook must stay the
  smallest possible delegation - no unnecessary visibility/staticness
  changes to standard methods (see SER-SLICE-2's IS_NO_PARALLEL revert,
  this pass).
- Every slice, including test-only and doc-only ones, requires real IT8
  activation/syntax check + ABAP Unit PASS + ATC PASS before being
  considered complete - there is no test-only IT8 exemption.
