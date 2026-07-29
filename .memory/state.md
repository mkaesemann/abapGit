# ORTEC abapGit opt-rework — active state

## Topic and phase

```text
TOPIC=variant-b-partial-clone
CURRENT_PHASE=PACKAGE_E_E0_CORRECTIVE_DESIGN_COMPLETE
PACKAGE_D2_STATUS=SAP_VALIDATED_COMPLETE
PACKAGE_D2_VALIDATED_HEAD=733bb30799886ef8659be7e293b82c3ccfcebbdd
PACKAGE_E_STATUS=E0_CORRECTIVE_DESIGN_COMPLETE, IMPLEMENTATION_PARTIAL_AUTHORIZED
PRODUCTIVE_CHANGES_ALLOWED=NO (this phase is design/memory-only; per-slice
  implementation authorization is recorded below and in the design doc §0,
  but no implementation session has started yet)
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
  AUTHORIZED_NOW (test-only).
E1_OBJINDEX_PERFORMANCE=CORRECT_BUT_PERFORMANCE_OPEN (reclassified from the
  prior draft's ACCEPTABLE_AS_IMPLEMENTED — see design §2, CR-04). Slice
  E1-PERF, AUTHORIZED_NOW for candidate E1-A only; contract FIXED this pass
  (bootstrap consistency review): new constant
  `c_index_write_chunk_size TYPE i VALUE 5000` in
  `zcl_abapgit_ortec_obj_index`'s `rebuild_index`, replacing the bare
  literal `1000` — no implementation-time choice remains open. E1-B/D/E
  remain NOT_AUTHORIZED, own design/review cycles required; E1-D is PROVEN
  UNSAFE as a bare tree-SHA1 key (see design §2).
E2_CONSUMER_COHERENCE=NOT_VERIFIED root cause. Slice E2-DIAG, D0/D1
  AUTHORIZED_NOW (D0=owner reproduction packet, no code; D1=read-only
  single-row comparison tool, no persistence); D2/D3
  BLOCKED_PENDING_D0_D1_INSUFFICIENCY. Slice E2-FIX NOT_AUTHORIZED, gated on
  a live D1/D2-confirmed mismatch. OF-1 (stale-but-present index row) is
  KEPT ACTIVE as the leading candidate root cause with a concrete,
  bounded detection/repair design (see design §3) — not silently dropped.
E3_CACHE_ADMIN_F4=CONFIRMED_CURRENT (not a defect); F4 already unions
  repo_state + obj_store + commit_hist orphans. Slice E3-TEST,
  AUTHORIZED_NOW (test-only).
E4_CERTIFIED_REPAIR=E4_NOT_REQUIRED_CURRENTLY_COMPLETE for 7 of 9
  constructed scenarios (reclassified from a blanket NOT_REQUIRED — see
  design §6, CR-05). Scenario 8 (stale-but-wrong index content) is
  OWNED_BY_E2-DIAG, not E4's remit. Scenario 7 (a certified tip's blob
  deleted out-of-band, bypassing all ORTEC write APIs) is a genuine,
  evidence-based residual gap NOT reachable by normal operation — tracked
  below as E4-OOB-DELETION-RISK, requires explicit owner risk-acceptance.
  Slice E4-VERIFY, AUTHORIZED_NOW (test-only). Slice E4-FIX NOT_REQUIRED
  (no code; risk documented, not designed away).
E-HARDEN (new this pass, CR-07)=OF-3 (RELAXED absent-strictness mode)
  ALREADY_ADEQUATELY_MITIGATED, closed, no action. OF-2 ('Walk,' string-
  match duplication) DECIDED — AUTHORIZED_NOW for a shared-constant
  extraction in the ORTEC-owned file only; the cross-file architecture
  question it surfaced (why zcl_abapgit_git_porcelain carries embedded
  ORTEC branching/duplicate logic) is tracked below as
  E-HARDEN-STANDARD-FILE-COUPLING, NOT_AUTHORIZED to resolve unilaterally.
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
  (once implemented) captures one live occurrence.
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
```

## Next action

Bootstrap consistency pass complete (2026-07-29): E1-A's implementation
contract is now fully fixed (design §2), E4-OOB-DELETION-RISK has one
fixed disposition (ACCEPTED_NON_BLOCKING_RISK, design §6), and both
correctness-review MINOR findings are resolved (MINOR-1=ALREADY_APPLIED,
MINOR-2=IMPLEMENTATION_PRECONDITION for E4-VERIFY). No `AUTHORIZED_NOW`
slice depends on an unresolved open question. Audit note: the orchestrator
read `/memories/repo/git-state-notes.md` in the immediately preceding turn
(different task instructions); no design claim depends on its content, and
the two `M`-flagged testclasses files were independently confirmed
content-empty via `git diff` (see bootstrap handoff for full detail). No
implementation has started. Next session: read this file, then the
design's §0 per-slice authorization table, then begin only the
`AUTHORIZED_NOW` slices in the order listed in
`.memory/handoffs/variant-b-package-e-bootstrap.md` — starting with
E1-TEST/E3-TEST/E4-VERIFY/E-HARDEN (mechanical, junior-routable), then
E1-PERF and E2-DIAG D0/D1 (senior-routable). Re-read this file plus the
Package E design/review artifacts before any code change; do not resume an
unrelated backlog topic without checking this file's own active-topic
status first.
