# Variant B — Slice 1 design review (durable materialization model)

Reviewer: independent correctness design review (balanced, not adversarial).
Scope: Slice 1 only, per `.github/prompts/variant-b.prompt.md`.
Inputs: `.memory/logs/variant_b_design.md`, `.memory/logs/variant_b_reconciliation.md`,
live grep of `FETCH_COMMIT` usages in `src/ortec/git/*.abap`.

## Verdict

**APPROVE_WITH_MINOR_REVISIONS**

## Confidence

High — Slice 1 is schema-additive plus one new, inert class with no method-body
changes to existing classes, so blast radius is small and independently
verifiable against live source.

## Strengths

- Correctly satisfies "physical objects keyed by repo+SHA, branch rows never
  own duplicate payloads": `ZAOG_REPO_STATE` gains only a 1-byte status field
  (`SNAP_STATE`), no payload column; `ZAOG_OBJ_STORE` is untouched. Verified
  against live schema/reconciliation — holds.
- No-auto-backfill reasoning is sound and structurally enforced: new columns
  are additive (space on activation), every reader compares against named
  constants that space never equals, and AC5 requires a unit test proving a
  legacy-shaped row is not auto-certified.
- `publish_snapshot_complete` correctly issues no internal `COMMIT WORK` and
  documents the caller-owned commit boundary, matching the owner's Slice 1
  and Slice 8 wording. The schema (separate `HIST_LEVEL`/`SNAP_STATE`/
  `ATTEMPT_ID` columns) is additive-friendly, so a later Slice 8 two-phase
  staged/published protocol can still be layered on without another
  incompatible schema change.
- `invalidate_commit`'s branch cascade is correctly bounded by branch count
  (`WHERE repo_key = ... AND fetch_commit = ...`), never by object count —
  matches the required O(B) bound.
- Performance model is complete and correctly shows all Slice 1 API calls as
  O(1)/O(B) with zero `ZAOG_OBJ_STORE` access, a real improvement over today's
  tree-walk-based `is_commit_complete`.

## Issues

### DR-001
- Type: correctness
- Severity: major (blocks Slice 2/3 sign-off, does not block merging Slice 1's
  own additive artifacts)
- Evidence: independent grep of `src/ortec/git/*.abap` confirms `FETCH_COMMIT`
  has live, unverified-value readers the design never enumerates:
  `zcl_abapgit_ortec_fastpath` (`ls_state-fetch_commit` drives the "remote tip
  unchanged → reconstitute locally" fast-path shortcut, [zcl_abapgit_ortec_fastpath.clas.abap](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L679-L683))
  and `zcl_abapgit_ortec_filter_walk` (uses `ls_state-fetch_commit` as the walk
  target when no commit is explicitly selected, [zcl_abapgit_ortec_filter_walk.clas.abap](src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap#L124)).
  Today both trust `FETCH_COMMIT` as "last fetched tip" with **no**
  verification. The design's §2 "Open question" only discusses the *writer*
  transition (`update_after_fetch` → `publish_snapshot_complete`) and misses
  these two live *reader* call sites entirely.
- Why it matters: repurposing `FETCH_COMMIT`'s meaning from "last raw fetch
  pointer" to "certified materialized pointer" is only safe once every
  consumer is migrated in the same step as the writer. Until
  `zcl_abapgit_ortec_fastpath`/`zcl_abapgit_ortec_filter_walk` are updated to
  gate on the new certificate (`is_graph_have_eligible`/`SNAP_STATE`) rather
  than raw field presence, Slice 2/3 wiring risks a window where the fast
  path still trusts an old-semantics value written by the still-live
  `update_after_fetch`, silently defeating the certification the new API
  exists to provide.
- Fix: add an explicit reader-migration checklist to the design (or a Slice
  2/3 precondition) naming these two call sites, so the next slice does not
  have to rediscover them by grep.

### DR-002
- Type: correctness (documentation consistency)
- Severity: minor
- Evidence: §2 states in the same paragraph that the old `update_after_fetch`
  write "must move out of the raw-fetch path once Slice 2/3 lands" and then
  "it does not yet remove the old writer (that removal is Slice 9
  territory)". These two sentences give an implementer contradictory
  guidance on when the dual-writer situation ends.
- Why it matters: an implementer building Slice 2/3 needs one unambiguous
  answer for whether the old `update_after_fetch` write to `FETCH_COMMIT`
  is disabled then, or coexists with the new writer until Slice 9.
- Fix: pick one and state it once — recommend: old writer is disabled/rerouted
  in Slice 2/3 (when the new orchestration first has a call site to replace
  it), and Slice 9 only removes now-dead code, not a still-active write path.

## Required revisions

- Resolve DR-002's internal contradiction before Slice 2/3 design begins.
- Before Slice 2/3 implementation, explicitly address DR-001: either migrate
  `zcl_abapgit_ortec_fastpath`/`zcl_abapgit_ortec_filter_walk` to consult the
  certificate API in the same change that starts writing via
  `publish_snapshot_complete`, or explicitly document an interim compatibility
  rule for the transition window.

## Optional improvements

- Consider recording in the design that `zcl_abapgit_ortec_cache_admin`'s use
  of `FETCH_COMMIT` (diagnostic overview report, off hot path) is unaffected
  and needs no migration, to close out the grep trail completely.
- The `begin_attempt`/`mark_graph_complete` ordering between "idempotent
  no-op if already complete" vs "raise if attempt_id stale" is inferable but
  not explicitly sequenced in the method doc; spelling out check order would
  remove a small judgment call for the implementer.

## Method signature / RAISING review

Signatures are otherwise internally consistent: pure predicates
(`get_state`, `is_graph_have_eligible`, `is_full_have_eligible`) correctly
never raise; state-mutating methods correctly raise `zcx_abapgit_ortec_git`
on stale attempts or premature transitions; `clean_incomplete_attempts` is
correctly the sole method with a bulk/set-based contract distinct from the
per-row O(1) methods. No RAISING/idempotency contradiction found beyond the
minor ordering ambiguity noted above.
