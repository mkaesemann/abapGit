# OBJ_PERF_FINAL — Correctness Gate Review (OBJ-PERF-CORRECTNESS-1)
Task: OBJ-PERF-CORRECTNESS-1 (gate, not adversarial cycle 4)
Baseline: `4193733de3c8ad75bae61f6ceb95b1c3e0ccb3e4`
Scope read: `.memory/state.md`, `obj_index_partial_design.md` (cycle-3 FINAL, full
§1-14), `obj_store_performance_design.md` (cycle-3 FINAL), the complete
3-cycle `obj_index_partial_adversarial.md` ledger, `obj_index_partial_current_source.md`,
plus direct re-reads of `zaog_obj_index.tabl.xml`, `zaog_commit_hist.tabl.xml`,
`zcl_abapgit_ortec_pack_raw.clas.abap` (`acquire_repo_lock`/`release_repo_lock`),
`zcl_abapgit_ortec_cache_admin.clas.abap` (`clear_repo`/`acquire_lock`/`release_lock`),
`zcx_abapgit_ortec_git.clas.abap`, and `zcl_abapgit_ortec_filter_walk.clas.abap`
to verify the design's claims against real current source, not just its own narration.
## Verdict
**CORRECTNESS = APPROVE**
## Confidence
High — every BLOCKER/MAJOR from all 3 adversarial cycles is closed with a
concrete mechanism (not a deferred judgment call), and independent spot-checks
against live source (lock session-id shapes, DDIC field lengths, exception
class hierarchy) confirm the design's factual claims are accurate, not
narrated-only.
## Strengths
- The six-concept functional model (§1) keeps commit-graph completeness,
  index completeness, coverage, positive mappings, and negative facts
  structurally distinct, with an explicit table naming which artifact proves
  which fact — no implicit derivation between them.
- The core invariant this mode's mission treats as absolute — a missing row
  is never remote absence — is enforced by construction, not convention:
  `ZAOG_OBJ_COVER`/`ZAOG_OBJ_PIDX` only ever gain a `FOUND`/`RESOLVED_*` row
  after a walk that actually covered that object's full identity; a raised
  exception (missing commit/tree) writes zero resolution facts (only a
  scheduling-hint `'M'` row, never read as an answer), and `walk_filtered`
  never emits a negative fact from an incomplete walk.
- Partial vs. complete coverage cannot be confused: `is_index_ready`'s
  context-checked `$IDX/__READY__` marker is the only "arbitrary object"
  certificate; `ZAOG_OBJ_COVER`/`ZAOG_OBJ_PIDX` only ever answer for the
  caller's own named objects (`get_coverage`/`select_partial_rows_for_filter`
  both require `it_filter`), and §11 step 3.1 always checks `is_index_ready`
  first and returns before any coverage/partial-row consultation — the two
  paths are mutually exclusive per request, never blended.
- The AR-2-01 fix (context-keyed `ZAOG_OBJ_PIDX`, non-key `CONTEXT_HASH` kept
  only on `ZAOG_OBJ_INDEX` where it is provably safe because `rebuild_index`
  always purges-then-rewrites) is a real, checked correctness argument, not
  an assertion — I re-derived the counterexample from cycle 2 (AR-2-01) and
  confirmed the cycle-3 fix structurally prevents it: two contexts can never
  share one `ZAOG_OBJ_PIDX` primary key, so an upsert-without-purge `MODIFY`
  cannot cross-context-overwrite.
- The AR-2-02 fix removes the object-reference ambiguity cleanly:
  `iv_current_remote` is a plain, optional SHA1 value threaded from the one
  caller (`get_remote_files_for_stage`) that actually holds
  `li_repo_online` in scope; `pull_filtered` structurally omits it, so it
  can never produce the strong `RESOLVED_NOT_PRESENT_REMOTE` state — this is
  provable from the call graph alone, not from an implementer's discipline.
- The AR-2-03 lock unification is deadlock-free by construction (one
  one-directional acquire order, writers never take the enqueue lock) and I
  independently confirmed the exact session-id mechanics
  (`ENQUEUE_EZAOG_REPO_LOCK` keyed on bare `iv_repo_key` vs.
  `acquire_repo_lock`'s `ZAOG_FETCH_SESS` mutex row keyed on
  `LOCK_<repo_key>`) against live source — the two really were non-conflicting
  before this fix, exactly as the adversarial review proved.
- Fallback policy (§6) is an exhaustive, closed list (4 triggers) with no
  residual "every cold request falls back" escape hatch; every trigger maps
  to an existing, unmodified exception path (`zcx_abapgit_exception` from
  `walk_filtered`, same shape `rebuild_index` already raises today).
- Full-index/partial coexistence (§5) is airtight: `rebuild_index`'s
  `invalidate_commit_index` purges all three tables context-blind before a
  COMPLETE rewrite, so a COMPLETE rebuild always supersedes any FILTERED
  leftovers regardless of which context(s) produced them.
## Issues
None open. No BLOCKER, MAJOR, or MINOR correctness finding survives cycle 3;
my independent re-verification against live source found no new
counterexample and no factual claim in the design that contradicts actual
current source (DDIC field lengths in `zaog_obj_index.tabl.xml` match §3.0's
claims exactly; `zcl_abapgit_ortec_pack_raw`'s lock mechanics match §5's/AR-2-03's
claims exactly; `zcx_abapgit_ortec_git` does inherit only from
`cx_static_check`, but the design's own §6 trigger 4 already mandates that
every `write_coverage` raise is swallowed non-fatally at its call site inside
`walk_filtered`, so no unhandled/uncaught checked-exception propagation gap
exists).
One observation, not a correctness defect (see readiness report for its
implementation-completeness framing):
### DR-001
- Type: maintainability
- Severity: minor
- Evidence: §3.2 declares `get_diagnostics` `RETURNING VALUE(rs_diagnostics)
  TYPE ty_cover_diagnostics` and narrates its two fields
  (`failure_count`/`last_error`), but no `TYPES: BEGIN OF ty_cover_diagnostics
  ... END OF` block is given anywhere in the design (unlike `ty_coverage`,
  which is spelled out in full).
- Why it matters: purely a documentation completeness gap — the two fields'
  types are already fully constrained by the paired `CLASS-DATA`
  declarations (`gv_write_coverage_failures TYPE i`,
  `gv_last_write_coverage_error TYPE string`), so no architectural judgment
  is required to fill it in. Does not affect correctness of any invariant.
- Fix: none required before implementation; see readiness report.
## Required revisions
None.
## Optional improvements
- Consider resolving open question 1 (`UNRESOLVED_AMBIGUOUS_MAPPING`
  remaining reserved-but-unwritten) in a future slice once
  `build_files_from_rows`'s `CORRUPT_OR_INCOMPLETE` raise can carry the
  specific failing `(obj_type, obj_name)` — explicitly out of this design's
  scope today and correctly not blocking.
- Open question 2 (bounded periodic cleanup of orphaned old-context rows on
  any of the three tables) remains a reasonable non-blocking follow-up, as
  the design itself states; no change needed for this gate.