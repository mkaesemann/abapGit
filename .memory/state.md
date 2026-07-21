# ORTEC abapGit opt-rework - active state

> Keep this file concise. Detailed evidence belongs in linked files under
> `.memory/logs`, `.memory/reviews`, `.memory/decisions`, and `.memory/handoffs`.

## Active topic

- Topic ID: `variant-b-partial-clone`
- Status: `IN_PROGRESS`
- Work branch: `ortec/abapgit_1_133-opt-rework`
- Owner-approved specification: `.github/prompts/variant-b.prompt.md`
- Current phase: `Slice 2 design gates complete; senior implementation not yet started`
- Last completed slice: `Slice 1 - durable materialization model` - DDIC
  append to `ZAOG_COMMIT_HIST`/`ZAOG_REPO_STATE` + new class
  `zcl_abapgit_ortec_mat_state` (9 methods, zero `COMMIT WORK`). Fully
  reviewed and imported/activated on IT8 (fixed missing
  `<WITH_UNIT_TESTS>X</WITH_UNIT_TESTS>` in commit
  `10e75f850c3530fdf5de820fb5f4f5f1147359f6`).
- Slice 2 status: `explicit ORTEC fetch modes and one request serializer` -
  focused reconciliation, design, correctness review, protocol/persistence
  review, and performance `DESIGN_GATE` are all complete
  (`APPROVE`/`APPROVE_WITH_MINOR_REVISIONS`, all revisions resolved directly
  in the design doc). Senior implementation has **not** started. See
  `.memory/logs/variant_b_slice2_design.md` for the full design plus all
  three "Review resolution" sections.
- Next action: senior implementation of Slice 2 per
  `.memory/logs/variant_b_slice2_design.md` §8 (new class
  `zcl_abapgit_ortec_fetch_req` + `.xml`; `zcx_abapgit_ortec_git` extension;
  `zcl_abapgit_ortec_fastpath` call-site rewiring incl. the DR-004 decode
  -local cache reset; `zcl_abapgit_ortec_fetch_neg=>is_commit_complete` body
  swap), then performance scan + `IMPLEMENTATION_AUDIT`, then regression.
- Binding preconditions carried forward for Slice 3 (do not start Slice 3
  design without addressing all of these in one coherent change, per
  `.memory/logs/variant_b_slice2_design.md` §5 and its review-resolution
  sections):
  - migrate `zcl_abapgit_ortec_fastpath`'s `pull_by_branch` fast-path
    shortcut and `zcl_abapgit_ortec_filter_walk`'s walk-target reader off
    raw `FETCH_COMMIT` trust onto the new certificate
    (`is_graph_have_eligible`/`SNAP_STATE`);
  - reroute `zcl_abapgit_ortec_repo_state=>update_after_fetch`'s write
    through `publish_snapshot_complete`;
  - decide how `zcl_abapgit_ortec_fastpath=>try_filtered_commit_fetch`'s
    `mv_unsupported_capability` signal reaches `filter_walk` (DR-001, Slice
    2 review);
  - route `fetch_tip_commits` through `build_request` or explicitly
    re-justify its hand-built `deepen 1`/`filter tree:0` buffer (DR-002,
    Slice 2 review);
  - assign fresh attempt/session/pack IDs to the `RECOVERY_BRANCH_FULL` tier
    (DR-004, Slice 2 review);
  - bound or eliminate `collect_ancestor_haves`'s unbounded
    `zaog_obj_store` commit-object read (performance `DESIGN_GATE` finding,
    Slice 2).
- Blocking condition: none currently known.
- Productive changes: Slice 1 is live on IT8. Slice 2 is design-approved but
  not implemented; implementation must follow the exact §8 file list.
- Supersedes as standalone topic: H4 walk delegation.

## Current architecture decision

Variant B is the owner-approved target:

- one physical SHA-addressed object store per repository;
- branch/ref state separate from object payloads;
- cold unknown branch: unbounded blobless commit/tree graph acquisition, no `deepen`;
- bulk materialization of only current-tip blobs;
- repository-wide reuse of commits, trees, and unchanged blobs;
- thin/OFS deltas only with certified local bases;
- no progressive-deepen completeness strategy;
- no one-request-per-object or one-SQL-per-object repair;
- branch-scoped full recovery only as exceptional, memory-gated fallback;
- standard abapGit behavior preserved when ORTEC is disabled.

## Required workflow for repository-scale slices

1. Focused current-source reconciliation.
2. Design delta.
3. Correctness design review.
4. Protocol/persistence review when applicable.
5. Performance review in `DESIGN_GATE` mode.
6. Senior implementation; delegate exact mechanical tasks to junior implementation.
7. Low-cost performance scan.
8. Senior performance review in `IMPLEMENTATION_AUDIT` mode.
9. Regression validation.

Implementation requires correctness and performance approval. Final regression
requires no blocking correctness or production-scale performance verdict.

## Active harness routing

- Orchestration: `ortec-abapgit-orchestrator` - Claude Sonnet 5.
- Discovery/mechanical search: `ortec-abapgit-discovery` - MAI-Code-1-Flash.
- Design: `ortec-abapgit-design` - Claude Sonnet 5.
- Correctness review: `ortec-abapgit-design-review` - Claude Sonnet 5.
- Protocol/persistence: `ortec-abapgit-protocol-persistence` - Claude Sonnet 5.
- Senior implementation: `ortec-abapgit-implementation-senior` - Claude Sonnet 5.
- Junior implementation: `ortec-abapgit-implementation-junior` - MAI-Code-1-Flash.
- Performance scan: `ortec-abapgit-performance-scan` - MAI-Code-1-Flash.
- Performance review: `ortec-abapgit-performance-review` - Claude Sonnet 5.
- Regression: `ortec-abapgit-regression` - MAI-Code-1-Flash.

## Current source of truth

Read only these by default:

1. this file;
2. `.github/prompts/variant-b.prompt.md`;
3. the latest files linked below for the active slice;
4. exact productive source files named by the slice.

Do not read the complete archive or all historical logs unless a current finding
requires specific historical evidence.

### Active links

- Owner specification: `.github/prompts/variant-b.prompt.md`
- Slice 0 reconciliation: `.memory/logs/variant_b_reconciliation.md`
- Slice 1 design + review resolution: `.memory/logs/variant_b_design.md`,
  `.memory/diagrams/variant_b_flow.mmd`
- Slice 1 correctness review: `.memory/reviews/variant_b_design_review.md`
- Slice 1 persistence review: appended to `.memory/logs/protocol_persistence.md`
- Slice 1 performance design gate + audit:
  `.memory/reviews/performance_design_variant-b-partial-clone_slice1.md`
- Slice 1 regression: `.memory/logs/regression_variant_b_slice1.md`
- Slice 2 reconciliation: `.memory/logs/variant_b_slice2_reconciliation.md`
- Slice 2 design + all review resolutions:
  `.memory/logs/variant_b_slice2_design.md`
- Slice 2 correctness review:
  `.memory/reviews/variant_b_slice2_design_review.md`
- Slice 2 persistence review: appended to `.memory/logs/protocol_persistence.md`
- Slice 2 performance design gate:
  `.memory/reviews/performance_design_variant-b-partial-clone_slice2.md`
- External review: `.memory/logs/external_review_2026-07-20.md`
- Historical full state: `.memory/archive/state_pre_variant_b_2026-07-20.md`

### Files to create during Slice 0/design

- `.memory/logs/variant_b_reconciliation.md`
- `.memory/logs/variant_b_design.md`
- `.memory/reviews/variant_b_design_review.md`
- `.memory/reviews/performance_design_variant-b-partial-clone_<slice>.md`
- `.memory/diagrams/variant_b_flow.mmd`

Do not create empty placeholder decision/review results before the respective
agent has performed the work.

## Superseded topics and conclusions

### H4 walk delegation

- Status: `SUPERSEDED_AS_STANDALONE_TOPIC`.
- Replaced by: `variant-b-partial-clone`.
- Existing H4 source, tests, logs, and handoffs remain evidence and may be reused.
- During reconciliation classify components as `REUSE_UNCHANGED`, `ADAPT`,
  `REPLACE`, or `OBSOLETE`.
- Do not resume H4 independently and do not delete working H4 code merely because
  the standalone topic is closed.

### Progressive deepening

- Status: `SUPERSEDED` as a correctness/completeness/recovery architecture.
- Historical implementation and incident evidence remain in the archive and logs.
- Do not restore or enlarge numeric deepen retry sequences.

## Working conventions

- Commit messages describe functionality and technical changes only; do not
  mention local memory files or Michael by name.
- Use the smallest safe model and the narrowest source scope.
- Never pass concatenated full source exports or the complete memory archive to a
  subagent when exact source files are available.
- Keep chat output short; write detailed evidence to one focused memory file.
- Mark load-bearing memory claims as `CONFIRMED_CURRENT`, `OWNER_DECISION`,
  `ASSUMPTION`, `UNVERIFIED`, `SUPERSEDED`, or `CONTRADICTED`.
- Current productive source and reproducible live evidence outrank historical
  conclusions. Current explicit owner decisions outrank earlier recommendations
  unless platform capability makes them impossible.
- SAP syntax/activation and ATC results are separate evidence. Record exactly what
  was and was not executed.

## Last update

- Date: 2026-07-21
- DESIGN_GATE (performance, Slice 2 explicit ORTEC fetch modes and request
  serializer): verdict `APPROVE_WITH_MINOR_REVISIONS`. Evidence: current
  productive source, not design prose alone
  (`zcl_abapgit_ortec_fastpath.clas.abap`, `zcl_abapgit_ortec_fetch_neg.clas.abap`,
  `zcl_abapgit_ortec_mat_state.clas.abap`, `zaog_obj_store.tabl.xml`).
  Confirmed: `build_request`/`parse_capabilities` genuinely 0 SQL/HTTP;
  `is_commit_complete` swap to `is_graph_have_eligible` is a real
  O(tree size)→O(1) improvement (re-confirmed, not re-derived); the 200-have
  cap is already hard-enforced, the new 100-materialize cap is designed as a
  hard raise (AC3); `reset_completion_budget()` is a trivial static-var
  reset, no cost. One non-blocking finding: `get_have_commits`'s
  `collect_ancestor_haves` (pre-existing, untouched by Slice 2) runs an
  unbounded-by-cap `SELECT ... FROM zaog_obj_store WHERE repo_key = ... AND
  obj_type = 'commit' AND status = 'R'` that scales with the repo's stored
  commit-object count — the design's §7 prose overstates the have-resolution
  pipeline as fully N-independent; only per-candidate certification is.
  Recommend correcting that prose and tracking the read as a Slice 3
  candidate. Report:
  `.memory/reviews/performance_design_variant-b-partial-clone_slice2.md`.
  Next action: correct the design doc's §7 prose (documentation-only), then
  Slice 2 implementation may proceed.
- Date: 2026-07-20
- Change: harness consistency review; oversized legacy state archived; Variant B
  established as the single active topic.
- IMPLEMENTATION_AUDIT (performance, Slice 1 durable materialization model):
  verdict `PASS`, no blocking findings. Evidence: current committed source
  (full class + testclasses + DDIC append, line-by-line, not static-scan-only).
  Inspected: `src/ortec/git/zcl_abapgit_ortec_mat_state.clas.abap`,
  `.clas.testclasses.abap`, `zaog_commit_hist.tabl.xml`,
  `zaog_repo_state.tabl.xml`. All 9 public methods confirmed O(1)/O(B) by
  full/leading-PK SQL shape; zero `COMMIT WORK`; cascade/cleanup are each one
  set-based statement. Report appended to
  `.memory/reviews/performance_design_variant-b-partial-clone_slice1.md`. Next
  action: regression validation for Slice 1, then Slice 2 design.
- Import to IT8 SAP system PASSED. Initially without the test classes due to missing `<WITH_UNIT_TESTS>X</WITH_UNIT_TESTS>` declaration in zcl_abapgit_ortec_mat_state.clas.xml. Fixed in commit 10e75f850c3530fdf5de820fb5f4f5f1147359f6