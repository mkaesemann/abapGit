# ORTEC abapGit opt-rework — active state
```text
BRANCH=ortec/abapgit_1_133-opt-rework
CURRENT_HEAD=03273b568949ab7e9485458ddbb411703804ea9c
LATEST_SAP_VALIDATED_HEAD=03273b568949ab7e9485458ddbb411703804ea9c
SER_SLICE_4=SAP_VALIDATED_COMPLETE
SER_SLICE_5=SAP_VALIDATED_COMPLETE
SER_FINAL_WAPA_FUGR=SAP_VALIDATED_COMPLETE
GENERIC_BATCH_ORCHESTRATION=SAP_VALIDATED_COMPLETE
INTEGRATED_PROVIDER_SET=SAP_VALIDATED_ACCEPTED
WAPA_RAW_PREFETCH=SAP_VALIDATED_COMPLETE
WAPA_MULTI_OBJECT_BATCHING=REJECTED_WITH_LIVE_IT8_EVIDENCE
WAPA_SINGLETON_POLICY=KEPT
FUGR_CHANGED_BY=SAP_VALIDATED_COMPLETE
FUGR_ADDITIONAL_OPTIMIZATION=CLOSED_FOR_CURRENT_SCOPE
DDLS=DEFERRED_BY_OWNER
INDIVIDUAL_PROVIDER_BENCHMARKS=WAIVED_BY_OWNER
LATE_CALLBACK_TEST=DEFERRED_OWNER_ACCEPTED
ATC=PASS
ABAP_UNIT=PASS
OUTPUT_PARITY=PASS
OPEN_BLOCKERS=0
OPEN_MAJORS=0
```
The completed serialization work is now fully documented in the repository memory artifacts and committed locally. The final WAPA/FUGR validation evidence is captured in `.memory/handoffs/ser-final-wapa-fugr-it8.md`; the implementation work itself remains in the validated productive commits listed in the history below.
## Active topic
```text
TOPIC=OBJ_PERF_FINAL
STATUS=DESIGN_APPROVED
START_HEAD=4193733de3c8ad75bae61f6ceb95b1c3e0ccb3e4
OBJ_PERF_FINAL=DESIGN_APPROVED
OBJ_INDEX_SLICE_1=DESIGN_APPROVED
OBJ_STORE_SLICE_1=DESIGN_APPROVED
LAUNCH_PROMPT=obj-store-partial-index-integrated-orchestrator.prompt.md
ADVERSARIAL_CYCLES=3
ADVERSARIAL_VERDICT=APPROVE (0 open blockers/majors, see .memory/reviews/obj_index_partial_adversarial.md)
CORRECTNESS_GATE=APPROVE (.memory/reviews/obj_index_partial_correctness.md)
READINESS_GATE=APPROVE (.memory/reviews/obj_index_partial_readiness.md)
PROTOCOL_PERSISTENCE_GATE=APPROVE_WITH_MINOR_REVISIONS, PP-01/PP-02 closed by doc correction in obj_store_performance_design.md (.memory/reviews/obj_index_partial_protocol_persistence.md)
PERFORMANCE_DESIGN_GATE=APPROVE_WITH_MINOR_REVISIONS, non-blocking (.memory/reviews/obj_index_partial_performance.md)
DESIGN_ARTIFACTS=.memory/logs/obj_index_partial_design.md, .memory/logs/obj_store_performance_design.md
SELECTED_INDEX_DESIGN=Candidate C hybrid: full commit-tree BFS walk unchanged, demand-driven per-object coverage (ZAOG_OBJ_COVER) + context-keyed partial rows (ZAOG_OBJ_PIDX, new) avoid full-F persistence for small K; COMPLETE-mode ZAOG_OBJ_INDEX unchanged (+ non-key CONTEXT_HASH column)
OBJ_STORE_DISPOSITIONS=OS-A/B/C/D/F=IMPLEMENT, OS-E/H=DEFER_OUT_OF_SCOPE, OS-G/I/J=REJECT_WITH_SOURCE_PROOF
DDIC_CHANGES=ZAOG_OBJ_COVER (new), ZAOG_OBJ_PIDX (new), ZAOG_OBJ_INDEX +CONTEXT_HASH (non-key column)
NEXT=senior implementation per obj_index_partial_design.md §9 commit-slice order
SER_SLICE_4=SAP_VALIDATED_COMPLETE
SER_SLICE_5=SAP_VALIDATED_COMPLETE
SER_FINAL_WAPA_FUGR=SAP_VALIDATED_COMPLETE
GENERIC_BATCH_ORCHESTRATION=SAP_VALIDATED_COMPLETE
INTEGRATED_PROVIDER_SET=SAP_VALIDATED_ACCEPTED
WAPA_RAW_PREFETCH=SAP_VALIDATED_COMPLETE
WAPA_MULTI_OBJECT_BATCHING=REJECTED_WITH_LIVE_IT8_EVIDENCE
WAPA_SINGLETON_POLICY=KEPT
FUGR_CHANGED_BY=SAP_VALIDATED_COMPLETE
FUGR_ADDITIONAL_OPTIMIZATION=CLOSED_FOR_CURRENT_SCOPE
DDLS=DEFERRED_BY_OWNER
INDIVIDUAL_PROVIDER_BENCHMARKS=WAIVED_BY_OWNER
LATE_CALLBACK_TEST=DEFERRED_OWNER_ACCEPTED
ATC=PASS
ABAP_UNIT=PASS
OUTPUT_PARITY=PASS
OPEN_BLOCKERS=0
OPEN_MAJORS=0
OBJ_INDEX_SLICE_1=DISCOVERY_ARCHAEOLOGY
OBJ_STORE_SLICE_1=DISCOVERY
```
The serialization closeout has been validated and documented. The active engineering topic is
`OBJ_PERF_FINAL` (integrated demand-driven partial commit index + ZAOG_OBJ_STORE performance),
launched from `obj-store-partial-index-integrated-orchestrator.prompt.md`. Focused evidence and
design artifacts are being written under `.memory/logs/obj_index_partial_*.md`,
`.memory/logs/obj_store_performance_*.md`, `.memory/reviews/obj_index_partial_*.md`, and
`.memory/reviews/obj_store_performance_*.md` as phases complete.
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
FUGR-CHANGED-BY-STATUS-SWEEP: CLOSED_FOR_CURRENT_SCOPE (2026-08-10, see
  SER_FINAL_WAPA_FUGR above). The narrow `functions()`-skip fix, the
  `BINARY SEARCH` improvement, and the regression tests are complete and
  validated. The larger repository-wide `CHANGED_BY_BULK` concept remains
  explicitly out of scope for this closeout and is not an active backlog
  item.
DDLS-OPTIMIZATION: DEFERRED_BY_OWNER (non-blocking, no current change).
LATE-CALLBACK-TEST: DEFERRED_OWNER_ACCEPTED (non-blocking, no current
  change).
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