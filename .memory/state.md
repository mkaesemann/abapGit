# ORTEC abapGit opt-rework — active state

## Topic and phase

```text
TOPIC=variant-b-partial-clone
CURRENT_PHASE=PACKAGE_E_CHECKPOINT_1_SAP_VALIDATED_COMPLETE
PACKAGE_D2_STATUS=SAP_VALIDATED_COMPLETE
PACKAGE_D2_VALIDATED_HEAD=733bb30799886ef8659be7e293b82c3ccfcebbdd
PACKAGE_E_STATUS=CHECKPOINT_1_SAP_VALIDATED_COMPLETE; E1_OBJINDEX_PERFORMANCE
  and E2_CONSUMER_COHERENCE POSTPONED 2026-07-31 (owner decision — working
  hypotheses only, not to be touched right now); E3_CACHE_ADMIN_F4 COMPLETED
  2026-07-31 (owner-confirmed, manually fixed/verified in the productive
  system)
PACKAGE_E_CHECKPOINT_1=SAP_VALIDATED_COMPLETE
PACKAGE_E_CHECKPOINT_1_VALIDATED_HEAD=3c77d898b62f3b0464e48cf59844e2e30c7b6e89
PACKAGE_E_CHECKPOINT_1_SCOPE=E1-TEST, E3-TEST, E4-VERIFY, E-HARDEN OF-2
PACKAGE_E_CHECKPOINT_1_VALIDATION=2026-07-29 (SAP_SYSTEM=IT8; ACTIVATION=PASS,
  SYNTAX=PASS, ABAP_UNIT=PASS, ATC=PASS, SEVERE_ATC_FINDINGS=NONE — owner-
  reported, covers the corrected head including the pre-import audit's
  placeholder removal and the 2 IT8-reported syntax/exception-contract
  fixes; detail in the checkpoint-1 handoff/regression log)
PRODUCTIVE_CHANGES_ALLOWED=NO for E1/E2 (POSTPONED, see below); E3 is
  COMPLETED, no further change; all other Package E slices remain gated per
  their own authorization state below
```

Branch: `ortec/abapgit_1_133-opt-rework`. Package sequence decision:
OWNER_DECISION 2026-07-24 (`.memory/decisions/variant_b_package_renumbering.md`).
Planned following phase: Package F — validated legacy-code cleanup (not
before Package E is live-validated).

## Validated productive baseline

```text
PACKAGE_C_VALIDATED_HEAD=29199f629773c676e0eaa2f3a006f5167d304ae8 (SAP_VALIDATED_COMPLETE)
PACKAGE_C_LAST_VALIDATION=2026-07-24 (SAP_SYSTEM=IT8; see Package C
  design/closeout link below — kept separate from Package D2's date, they
  are different validation runs on different code states)
PACKAGE_D2_VALIDATED_HEAD=733bb30799886ef8659be7e293b82c3ccfcebbdd (SAP_VALIDATED_COMPLETE)
PACKAGE_D2_LAST_VALIDATION=2026-07-23 (SAP_SYSTEM=IT8)
ATC=PASS, ABAP_UNIT=PASS, COLD_BRANCH=PASS, WARM_UNCHANGED=PASS,
CERTIFIED_STATE=F/C, CACHE_ADMIN=PASS, LARGE_REPO_FUNCTIONAL=PASS
  (all against the PACKAGE_D2_VALIDATED_HEAD/PACKAGE_D2_LAST_VALIDATION pair
  above; do not conflate with Package C's separate, earlier validation run)
D2_LIVE_INCIDENTS_RESOLVED=SYSTEM_NO_ROLL, TIME_OUT, DBSQL_STMNT_TOO_LARGE
  (all SAP_VALIDATED_RESOLVED — see incident links below)
MEMORY_HEAD=8eef0b55fb37892c3d6b6428c038886481c73192 (memory-only range vs
  the PACKAGE_D2_VALIDATED_HEAD above, confirmed via `git diff --name-status`;
  this corrective E0 pass's own edits land on top of this same memory-only
  range — no productive file has been touched)
```

## Active Package E scope (corrective, 9-slice model — see design §0)

Corrective design/review complete for all 9 slices
(`.memory/logs/variant_b_package_e_discovery.md`,
`.memory/logs/variant_b_package_e_design.md` §0-§12). Per-slice
implementation authorization (source of truth: design §0 — this is a
convenience mirror, not a second source of truth; if these ever diverge,
the design document wins):

```text
E1_OBJINDEX_CORRECTNESS=CONFIRMED_CURRENT (not a defect). Slice E1-TEST,
  SAP_VALIDATED_COMPLETE (checkpoint 1, head 3c77d898, 2026-07-29).
E1_OBJINDEX_PERFORMANCE=CORRECT_BUT_PERFORMANCE_OPEN (reclassified from the
  prior draft's ACCEPTABLE_AS_IMPLEMENTED — see design §2, CR-04). Slice
  E1-PERF, AUTHORIZED_NOW for candidate E1-A only; contract FIXED this pass
  (bootstrap consistency review): new constant
  `c_index_write_chunk_size TYPE i VALUE 5000` in
  `zcl_abapgit_ortec_obj_index`'s `rebuild_index`, replacing the bare
  literal `1000` — no implementation-time choice remains open. E1-B/D/E
  remain NOT_AUTHORIZED, own design/review cycles required; E1-D is PROVEN
  UNSAFE as a bare tree-SHA1 key (see design §2).
  POSTPONED 2026-07-31 (owner decision): E1-A was separately found
  live-implemented at `VALUE 30000` (not the documented 5000 above —
  undiffed discrepancy, see
  `.memory/logs/variant_b_package_e_false_modified_os4_d1.md`). No further
  E1 work (contract reconciliation or E1-B/D/E) is authorized right now;
  this remains a working hypothesis only. PRODUCTIVE_CHANGES_ALLOWED=NO
  until the owner resumes after Package E is fully complete and the full
  situation is verified in the productive development system.
E2_CONSUMER_COHERENCE=NOT_VERIFIED root cause. Slice E2-DIAG, D0/D1
  AUTHORIZED_NOW (D0=owner reproduction packet, no code; D1=read-only
  single-row comparison tool, no persistence); D2/D3
  BLOCKED_PENDING_D0_D1_INSUFFICIENCY. Slice E2-FIX NOT_AUTHORIZED, gated on
  a live D1/D2-confirmed mismatch. OF-1 (stale-but-present index row) is
  KEPT ACTIVE as the leading candidate root cause with a concrete,
  bounded detection/repair design (see design §3) — not silently dropped.
  2026-07-30: the earlier small-repo (abapGit-testing) D0/D1 reproduction is
  RETRACTED (stale local clone; branches actually differ — see
  `.memory/handoffs/variant-b-package-e-e2-diagnostic.md`,
  STATUS=SUPERSEDED_INVALID_REPRODUCTION). Active incident is now OS4
  (large repo, DTEL /LOT/GC_GEOLAT, overview+Full-Stage show MODIFIED,
  Diff shows no differences) — D0/D1 static trace complete, WAITING_FOR_
  OWNER_DEBUG_CAPTURE; see
  `.memory/handoffs/variant-b-package-e-e2-os4-diagnostic.md`.
  POSTPONED 2026-07-31 (owner decision): the OS4 investigation has since
  produced multiple candidate root causes and partially-implemented fixes
  (parallel-worker stale-cache injection, FUGR get_includes flag, a
  near-empty checksum baseline) with CONTRADICTORY phase status across
  the D0/D1/handoff files above — all working hypotheses, none
  owner-validated end-to-end yet. No further E2 work is authorized right
  now. PRODUCTIVE_CHANGES_ALLOWED=NO until the owner resumes after
  Package E is fully complete and the full situation is verified in the
  productive development system.
E3_CACHE_ADMIN_F4=CONFIRMED_CURRENT (not a defect); F4 already unions
  repo_state + obj_store + commit_hist orphans. Slice E3-TEST,
  SAP_VALIDATED_COMPLETE (checkpoint 1, head 3c77d898, 2026-07-29).
  COMPLETED 2026-07-31 (owner-confirmed, manually fixed/verified in the
  productive system — no further action).
E4_CERTIFIED_REPAIR=E4_NOT_REQUIRED_CURRENTLY_COMPLETE for 7 of 9
  constructed scenarios (reclassified from a blanket NOT_REQUIRED — see
  design §6, CR-05). Scenario 8 (stale-but-wrong index content) is
  OWNED_BY_E2-DIAG, not E4's remit. Scenario 7 (a certified tip's blob
  deleted out-of-band, bypassing all ORTEC write APIs) is a genuine,
  evidence-based residual gap NOT reachable by normal operation — tracked
  below as E4-OOB-DELETION-RISK, requires explicit owner risk-acceptance.
  Slice E4-VERIFY, SAP_VALIDATED_COMPLETE (checkpoint 1, head 3c77d898,
  2026-07-29; E4-D-01/E4-D-02 disposed BLOCKED_BY_MISSING_TEST_SEAM,
  E4-D-04 disposed NOT_APPLICABLE_WITH_EXACT_SOURCE_PROOF — no placeholder
  ABAP Unit methods, see checkpoint-1 regression log). Slice E4-FIX
  NOT_REQUIRED (no code; risk documented, not designed away).
E-HARDEN (new this pass, CR-07)=OF-3 (RELAXED absent-strictness mode)
  ALREADY_ADEQUATELY_MITIGATED, closed, no action. OF-2 ('Walk,' string-
  match duplication) SAP_VALIDATED_COMPLETE (checkpoint 1, head 3c77d898,
  2026-07-29) — shared-constant extraction in the ORTEC-owned file only,
  exact pre-existing text preserved ('Walk, tree not found'/'Walk, blob
  not found', proven via ABAP string-template semantics and pinned by
  exact-equality tests). The cross-file architecture question it surfaced
  (why zcl_abapgit_git_porcelain carries embedded ORTEC branching/
  duplicate logic) is tracked below as E-HARDEN-STANDARD-FILE-COUPLING,
  NOT_AUTHORIZED to resolve unilaterally.
```

## Binding invariants

- No `deepen`/`shallow` in Variant B requests. No per-object SQL/HTTP. No
  uncertified haves. No productive blank repository-key fallback.
- Tree/blob processing uses bounded bulk windows; presence, metadata and
  payload access stay separated; graph and snapshot completeness stay
  separate states; snapshot publication requires `HIST_LEVEL=F`.
- Standard abapGit behavior is unchanged when ORTEC is disabled.
- Package D1 owns bounded external delta-base resolution; Package D2 owns
  attempt/transaction isolation (both SAP_VALIDATED_COMPLETE, do not reopen
  without new contradicting evidence).
- Package E may NOT reintroduce a `deepen`/whole-commit fetch as a repair
  mechanism, may NOT add a new certified-repair state machine where an
  existing one already covers the case (E4), and may NOT authorize a
  corrective E2 fix without a reproduced root cause.
- Every Package E slice, including test-only and doc-only ones, requires
  real IT8 activation/syntax check + ABAP Unit `PASS` + ATC `PASS` before
  being considered complete — there is no test-only IT8 exemption (CR-08
  correction; the prior bootstrap handoff's claim to the contrary was
  false and has been withdrawn).
- Package F owns validated legacy-code removal (not started).

## Active evidence links

- Owner spec: `.github/prompts/variant-b.prompt.md`
- Package E memory audit: `.memory/logs/variant_b_package_e_memory_audit.md`
- Package E discovery: `.memory/logs/variant_b_package_e_discovery.md`
- Package E design (corrective, single active source):
  `.memory/logs/variant_b_package_e_design.md`
- Package E reviews (all rerun against the corrective design):
  `.memory/reviews/variant_b_package_e_correctness_review.md` (APPROVE_WITH_MINOR_REVISIONS, 2 MINOR),
  `.memory/reviews/variant_b_package_e_protocol_review.md` (APPROVE),
  `.memory/reviews/performance_design_variant_b_package_e.md` (APPROVE)
- Package E bootstrap handoff (corrective):
  `.memory/handoffs/variant-b-package-e-bootstrap.md`
- Package D1/D1 triage: `.memory/handoffs/variant-b-package-d-d1-implementation.md`,
  `.memory/logs/variant_b_package_d_d1_modified_status_triage.md`
- Package D2 implementation/incidents:
  `.memory/handoffs/variant-b-package-d-d2-implementation.md`,
  `.memory/incidents/variant_b_d2_it8_system_no_roll_timeout.md`,
  `.memory/incidents/variant_b_d2_it8_dbsql_stmt_too_large.md`,
  `.memory/incidents/variant_b_d2_sat_warm_to_cold_o4h8794.md`
- Package D2 performance audit (source of AUDIT-M-1):
  `.memory/logs/performance_audit_variant_b_package_d2.md`
- Package C design/closeout: `.memory/logs/variant_b_package_c_design.md`,
  `.memory/handoffs/variant-b-package-c-c2-checkpoint.md`

## Deferred topics

```text
AUDIT-M-1: same-repo lock-contention latency under concurrent resumed
  decodes (MAJOR, not blocking). No fixture/scale test yet. Entry condition:
  a dedicated concurrency scale-test slice is scheduled (not yet).
FINAL-SAT-PROFILING: final ST05/SAT profiling of very large unfiltered
  repositories, active-window/payload-byte-budget tuning, remaining legacy
  cache-population path review. Entry condition: after Package F cleanup.
E1-TREE-REUSE: tree-SHA1-keyed or incremental-diff zaog_obj_index row reuse
  across commits (E1-D/E1-E). PROVEN UNSAFE as a bare tree-SHA1 key this
  corrective pass (design §2) — any future attempt needs a composite key of
  at minimum (tree_sha1, hash-of-.abapgit-content, devclass). Entry
  condition: owner approves an additive secondary-index DDIC change and a
  dedicated design + performance DESIGN_GATE for it.
E2-REPRODUCTION: false-MODIFIED root cause. Entry condition: owner supplies
  one concrete reproduction packet (repo, branch, exact file path, both
  SHA1s, believed-current commit — D0 in design §3) or the E2-DIAG D1 tool
  (once implemented) captures one live occurrence. 2026-07-30: OS4 (large
  repo) reproduction packet received, D0/D1 STATIC trace complete (source-
  confirmed candidate PC-1: overview/Full-Stage use the cached
  `mt_remote`/`get_files_remote` path, single-object Diff uses the ORTEC
  filtered-walk facade which independently revalidates the live branch tip
  via `ZAOG_REPO_STATE`/`ZAOG_OBJ_INDEX`, bypassing that cache). Entry
  condition for D2/E2-FIX: owner executes the bounded debugger worksheet in
  `.memory/logs/variant_b_package_e_false_modified_os4_d1.md` §5 and
  confirms a live mismatch.
E4-OOB-DELETION-RISK (new this pass, CR-05; DISPOSITION FIXED this
  consistency pass = ACCEPTED_NON_BLOCKING_RISK, see design §6 and
  bootstrap handoff): a certified/complete snapshot tip whose blob is
  later deleted by an out-of-band administrative action bypassing all
  ORTEC write APIs is not detected by either existing repair mechanism.
  Not reachable by normal operation; no live incident reported. Consumers
  remain safe (the existing `ensure_available` top-up either self-heals or
  raises the existing generic missing-object exception — a loud, safe
  failure, never a silently wrong result); recovery is the existing manual
  remote re-fetch / `rebuild_index` path, no new mechanism required. No
  E4-FIX slice authorized or required. Remains listed here as a permanent,
  accepted residual risk, not an open entry-condition item; owner may
  revisit if this framing is disputed.
E-HARDEN-STANDARD-FILE-COUPLING (new this pass, CR-07): whether
  zcl_abapgit_git_porcelain.clas.abap's embedded ORTEC-aware branching and
  duplicate 'Walk,' retry logic should be refactored into a single clean
  hook. Entry condition: explicit owner input on whether/how to touch
  standard abapGit's own source for this.
SYSTEM_NO_ROLL-OS4-STAGE-AFTER-OVERVIEW (new, discovered during the E2 OS4
  diagnosis, explicitly NOT part of E2): a `SYSTEM_NO_ROLL` runtime dump
  observed in IT8 when Full Stage is triggered immediately after a full
  Overview serialize of the OS4 repo (17321 objects). Owner's working
  theory: cache/memory not released between the two back-to-back
  large-repo computations. Not investigated. Entry condition: a dedicated
  Package E or F backlog slice is scheduled for it; see
  `.memory/logs/variant_b_package_e_false_modified_os4_d1.md` §11.
```

## Next action

Package E checkpoint 1 (E1-TEST, E3-TEST, E4-VERIFY, E-HARDEN OF-2) is
SAP_VALIDATED_COMPLETE as of 2026-07-29, head `3c77d898`
(`.memory/handoffs/variant-b-package-e-checkpoint-1.md`,
`.memory/logs/regression_variant_b_package_e_checkpoint_1.md`,
`.memory/logs/performance_scan_variant_b_package_e_checkpoint_1.md`). A
pre-import audit found and fixed 3 forbidden placeholder ABAP Unit tests
and 2 real IT8-reported compile defects (`ZCL_ABAPGIT_ORTEC_CACHE_ADMIN`
keyless-table `FILTER`; `ZCL_ABAPGIT_ORTEC_OBJ_INDEX` `build_commit`
missing `zcx_abapgit_exception` in `RAISING`) before the owner's IT8 run;
full finding-to-fix matrix is in the regression log. No placeholder ABAP
Unit methods remain anywhere in this checkpoint's scope.

2026-07-31 owner decision: E1_OBJINDEX_PERFORMANCE and
E2_CONSUMER_COHERENCE are POSTPONED — do NOT touch either (no contract
reconciliation, no E1-B/D/E, no E2 fix work) until the owner resumes after
Package E is fully complete and has verified the full situation in the
productive development system. E3_CACHE_ADMIN_F4 is COMPLETED (owner-
confirmed, manually fixed/verified). The remaining open Package E item
needing owner input is E-HARDEN-STANDARD-FILE-COUPLING (see Deferred
topics); E4 remains SAP_VALIDATED_COMPLETE with its one accepted residual
risk (E4-OOB-DELETION-RISK). No Package E slice is currently authorized
for implementation. Do not resume E1/E2 work or start Package F without a
new explicit owner instruction. Re-read this file plus the Package E
design/review artifacts before any code change; do not resume an
unrelated backlog topic without checking this file's own active-topic
status first.
