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
TOPIC=WAPA_PAYLOAD_PERF
STATUS=SAP_VALIDATED_COMPLETE
SYSTEM=IT8 (productive source of truth); shared memory is this repository .memory tree
SSFO_LOCAL_CACHE=SAP_VALIDATED_COMPLETE: persistent client-isolated full-result SSFO cache with active-only dependency signature, SHOW-lock effective language, validated replay, pre/post-publication signature comparison, bounded payload/eviction, and aRFC switch propagation.
SSFO_PRODUCTIVE_OBJECTS=ZAOG_SSFO_CACHE (secondary index 001 LAST_USED_AT,FORMNAME);ZCL_ABAPGIT_ORTEC_SSFO_CACHE;ZCL_ABAPGIT_ORTEC_SER_CACHE;ZCL_ABAPGIT_ORTEC_GIT_SWITCH;ZCL_ABAPGIT_ORTEC_SER_ORCH;Z_ABAPGIT_ORTEC_SER_BATCH;ZCL_ABAPGIT_ORTEC_CACHE_ADMIN;ZABAPGIT_ORTEC_CACHE_ADMIN.
SSFO_EVIDENCE=Owner tested the cache and verified correct cache updates. SSFO cache integration active source is syntax-clean; ORCH ABAP Unit 49/49; cache-admin ABAP Unit 16/16; DDIC table client dependency and active index 001 verified.
SSFO_DESIGN=C:\Projects\abap\abapGit\.memory\logs\ssfo_local_cache_design.md
SSFO_CORRECTNESS_REVIEW=C:\Projects\abap\abapGit\.memory\reviews\ssfo_local_cache_correctness_review.md
SSFO_PERFORMANCE_DESIGN=C:\Projects\abap\abapGit\.memory\reviews\ssfo_local_cache_performance_design.md
SSFO_IMPLEMENTATION=C:\Projects\abap\abapGit\.memory\handoffs\ssfo_local_cache_implementation.md
SSFO_TEST_GAP=No direct SSFO cache ABAP Unit tests; testclasses include remains placeholder due local-friend/test-include tooling limitation. Treat the owner’s cache-update verification as runtime acceptance; retain focused parity/invalidation/SHOW-lock SAT trace as optional follow-up if regression evidence is needed.
SSFO_NEXT=Closed for current scope.
WAPA_PAYLOAD_DISCOVERY=C:\Projects\abap\abapGit\.memory\logs\wapa_payload_discovery.md
WAPA_OPTION1=SAP_VALIDATED_COMPLETE: bounded raw O2PAGCON manifest/payload/verification path; page-local decode with raw-row release before IMPORT; contiguous bounded bisection with terminal reference-range fallback. WAPA remains singleton-batched.
TUNED_CONSTANTS=C_RAW_PREFETCH_INITIAL_PAGES=6000;C_MAX_RAW_PREFETCH_ROWS=30000;C_MAX_RAW_MANIFEST_ROWS=40000;C_MAX_RAW_PAYLOAD_BYTES=104857600;C_MAX_RAW_SPLIT_DEPTH=5;C_MAX_DECODED_PAGE_BYTES=15728640.
WAPA_EVIDENCE=Functional output test successful. SAT worker evidence: C:\Users\MichaelK\Downloads\SAT WAPA Tuned 1000-20000 Worker 1.txt and C:\Users\MichaelK\Downloads\SAT WAPA Tuned 6000-30000 Worker 1.txt. The 6000/30000 tune reduced Companion-worker O2PAGCON DB time from 7.768888s to 6.442467s and worker gross from 53.249011s to 29.110946s; no memory issue was observed. Main traces are paired in the same Downloads folder.
VALIDATION=Active class syntax PASS; WAPA ABAP Unit 23/23 PASS; production static scan closed PS-001/PS-002; implementation audit confirmed page rows are released before IMPORT and malformed manifests reject before payload.
FOLLOW_UP=Closed for current scope. Diagnostics-counter saturation and synthetic 5k/40k active-path coverage remain non-blocking hardening work; do not reopen without explicit owner priority. Future tuning should preserve the 100MiB raw-payload bound unless a separate memory decision is approved.
NEXT=Closed.
FDT0_LOCAL_CACHE=SAP_VALIDATED_COMPLETE: IT-01 confirmed after activation for formula and decision-table-cell changes; cache hit parity and warm-run performance confirmed by owner; all related IT8/local commits synchronized.
DESIGN=C:\Projects\abap\abapGit\.memory\logs\fdt0_local_cache_design.md
ADVERSARIAL_REVIEW=C:\Projects\abap\abapGit\.memory\reviews\fdt0_local_cache_adversarial_review.md
PROTOCOL_GATE=APPROVE_WITH_MINOR_REVISIONS
PERFORMANCE_DESIGN_GATE=APPROVE_WITH_MINOR_REVISIONS
PERFORMANCE_SCAN=PASS
PERFORMANCE_IMPLEMENTATION_AUDIT=PASS_WITH_MINOR_FINDINGS
REGRESSION=PASS_WITH_FINDINGS
PRODUCTIVE_OBJECTS=ZAOG_FDT_CACHE;ZCL_ABAPGIT_ORTEC_FDT0_CACHE;ZCL_ABAPGIT_ORTEC_GIT_SWITCH;ZCL_ABAPGIT_ORTEC_SER_ORCH;Z_ABAPGIT_ORTEC_SER_BATCH
IMPLEMENTATION_FIXES=bounded signature SELECT (cap+1); pre-EXPORT 48MB file-content cap plus post-EXPORT 50MB cap; superseded per-app signature purge; linear signature assembly
VALIDATION=cache server syntax PASS + active/inactive aligned; ORCH ABAP Unit 49/49; GIT_SWITCH ABAP Unit 7/7; worker FM activated
RELEASE_BLOCKERS=CLOSED: IT-01 confirmed by owner after activated formula and decision-table cell changes; cache hit parity and warm-run scale evidence confirmed.
TEST_GAP=No focused cache ABAP Unit tests (test-class friendship could not be safely established through current class-pool/test-include tooling); do not claim cache-hit behavior unit-tested
NEXT=Closed; proceed only with the separate WAPA_PAYLOAD_PERF trace-first slice.
```

## Prior active topic
```text
TOPIC=OBJ_PERF_FINAL
STATUS=LOCAL_COMPLETE_AWAITING_IT8
START_HEAD=4193733de3c8ad75bae61f6ceb95b1c3e0ccb3e4
CURRENT_HEAD=d3f0679d
OBJ_PERF_FINAL=LOCAL_COMPLETE_AWAITING_IT8
OBJ_INDEX_SLICE_1=LOCAL_COMPLETE_AWAITING_IT8
OBJ_STORE_SLICE_1=NO_CHANGE_JUSTIFIED
LAUNCH_PROMPT=obj-store-partial-index-integrated-orchestrator.prompt.md
ADVERSARIAL_CYCLES=3
ADVERSARIAL_VERDICT=APPROVE (0 open blockers/majors)
CORRECTNESS_GATE=APPROVE
READINESS_GATE=APPROVE
PROTOCOL_PERSISTENCE_GATE=APPROVE_WITH_MINOR_REVISIONS (PP-01/PP-02 closed by doc correction)
PERFORMANCE_DESIGN_GATE=APPROVE_WITH_MINOR_REVISIONS (non-blocking)
STATIC_PERFORMANCE_SCAN=PASS (PS-001 fixed)
PERFORMANCE_IMPLEMENTATION_AUDIT=PASS after fix (PA-001/PA-002 fixed)
REGRESSION=PASS_WITH_FINDINGS (0 blocking, 1 non-blocking residual noted)
DDIC_CHANGES=ZAOG_OBJ_COVER (new), ZAOG_OBJ_PIDX (new), ZAOG_OBJ_INDEX +CONTEXT_HASH (non-key)
CHECKPOINT_COMMITS=f7be8296,3e1e804a,9603813e,bdacce79,b239dd2a,80641c3b,d0d7f3eb,09c695dd,d3f0679d
PUSHED=NO
IT8_HANDOFF=.memory/handoffs/obj-store-partial-index-it8.md
GENERAL_HANDOFF=.memory/handoffs/obj-store-partial-index.md
IT8_ABAP_UNIT_VALIDATION=PASS (2026-08-11, live IT8 edits, NOT YET IN
  CHECKPOINT_COMMITS/git - see IT8_LIVE_FIXES below). 83/83 across
  ZCL_ABAPGIT_ORTEC_OBJ_INDEX (26), ZCL_ABAPGIT_ORTEC_CACHE_ADMIN (16),
  ZCL_ABAPGIT_ORTEC_OBJ_COVER (10), ZCL_ABAPGIT_ORTEC_OBJ_STORE (31
  regression guard). ATC DEFAULT variant: 0 errors on both changed
  objects (OBJ_INDEX 17 info; CACHE_ADMIN 8 warn/10 info, all pre-existing
  "no WHERE condition" on get_overview's intentional full-scan admin
  aggregation + missing text-element info, unrelated to this pass).
IT8_LIVE_FIXES=two real production bugs found+fixed live in IT8 during
  this pass, plus one test-code defect and ten stale pre-Slice-3 test
  expectations corrected (CONFIRMED_CURRENT, evidence = live IT8 test
  runs+ATC, this session):
  (1) CLAS ZCL_ABAPGIT_ORTEC_OBJ_INDEX method REBUILD_INDEX: the
      $IDX/__READY__ completion-marker row omitted
      `ls_row-context_hash = iv_context_hash` (every other row in the
      write loop sets it, the marker did not) - is_index_ready could
      never match a freshly-built COMPLETE index under any real context.
      Design doc already mandated this ("stamps it onto every
      ZAOG_OBJ_INDEX row it writes, including the $IDX/__READY__ marker
      row") - pure implementation-vs-design gap, fixed to match the
      documented design, no design change.
  (2) CLAS ZCL_ABAPGIT_ORTEC_OBJ_INDEX method SELECT_PARTIAL_ROWS_FOR_FILTER:
      both SELECT statements used an explicit 11-column list into
      `lt_pidx_rows` typed as the FULL 12-component ZAOG_OBJ_PIDX
      structure (12th component = implicit MANDT/client) - a classic
      ABAP positional-assignment mismatch that silently shifted every
      field by one slot (CONTEXT_HASH received PATH_HASH's value,
      FILE_NAME received BLOB_SHA1's value, IDX_STATUS was left blank).
      Root-caused via a live IT8 diagnostic (temporary inline FOR ALL
      ENTRIES replica + COMMIT WORK + direct SQL inspection), not
      guessed. Fixed to `SELECT *` (matching the already-correct sibling
      SELECT_ROWS_FOR_FILTER pattern). This bug had TWO production
      impacts, not just failing tests: silently-empty file lists for
      warm/covered FILTERED-mode reads that happened to shift a blank
      field into FILE_NAME/BLOB_SHA1's slot, and - for real walked rows -
      an accidental CORRUPT_OR_INCOMPLETE exception that silently forced
      an expensive COMPLETE-mode rebuild_index fallback on every miss,
      defeating OBJ_PERF_FINAL's own core write-volume goal without ever
      surfacing as a visible error.
  (3) Test-only: ZCL_ABAPGIT_ORTEC_CACHE_ADMIN testclasses method name
      `clear_repo_then_filtered_read_rewalks` (37 chars) exceeded ABAP's
      30-char limit, silently blocking the ENTIRE testclasses include
      from compiling ("no test classes found", 0/16 run). Renamed to
      `clear_repo_forces_rewalk`.
  (4) Test-only, 10 methods (INDEX_EMPTY_NO_MATCH, MARKER_REQUIRED_FOR_READY,
      INDEX_NO_CROSS_COMMIT_LEAK, READY_REJECTS_OTHER_COMMIT,
      READY_ACCEPTS_EXACT_COMMIT, INDEX_CHUNK_BOUNDARY_OK,
      INDEX_BULK_ROWS_PRESERVED, COV_REBUILD_WHEN_INCOMPLETE,
      SELECT_ROWS_CHUNK_BOUNDARY, READY_REJECTS_OTHER_CONTEXT): each
      asserted GET_FILES_FOR_FILTER always drives a COMPLETE-mode
      REBUILD_INDEX (pre-Slice-3 assumption). Fixing bug (2) above
      exposed that this assumption is now genuinely false whenever the
      filter is uncovered - ENSURE_FILTERED_COVERAGE correctly prefers
      the cheaper WALK_FILTERED path per the already-documented Slice 3
      design ("FILTERED-mode coverage never becomes COMPLETE-mode
      readiness"), so these tests were only ever passing "by accident"
      via bug (2)'s own exception-triggered fallback. Tests whose actual
      purpose is COMPLETE-mode-specific behavior now call REBUILD_INDEX/
      ENSURE_INDEX directly (LOCAL FRIENDS-accessible); COV_REBUILD_WHEN_
      INCOMPLETE's assertion was corrected to expect IS_INDEX_READY=FALSE,
      matching the documented invariant. No design change - tests brought
      into alignment with the already-approved Slice 3 design.
NEXT=(a) sync IT8_LIVE_FIXES back into the local git working tree/commits
  (git currently only has CHECKPOINT_COMMITS through d3f0679d, which does
  NOT include these 4 live IT8 fixes yet); (b) owner still needs to
  execute the remaining IT8 handoff items this session did not cover -
  functional/output-parity scenarios 1-9 and the K=1/K=100-250 owner
  performance measurements in IT8_HANDOFF; do not mark
  SAP_VALIDATED_COMPLETE before both (a) and (b)
```
The serialization closeout (SER-SLICE-4/5, SER-FINAL-WAPA-FUGR, etc., listed in the header block
above) remains `SAP_VALIDATED_COMPLETE` and is not affected by this topic. Full design/adversarial/
gate/implementation/audit/regression detail for `OBJ_PERF_FINAL` lives in
`.memory/logs/obj_index_partial_*.md`, `.memory/logs/obj_store_performance_*.md`,
`.memory/reviews/obj_index_partial_*.md`, and `.memory/handoffs/obj-store-partial-index*.md` - this
file intentionally does not duplicate them.

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