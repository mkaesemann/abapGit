# Variant B Package C (C0) — Correctness Review

Reviewer: balanced design-review agent (ortec-abapgit-design-review)
Baseline: `cf3d5cb87ca4eec4e68b7975df6fc127a8c5abc2` (Package B SAP_VALIDATED_COMPLETE)
Artifact reviewed: `.memory/logs/variant_b_package_c_design.md`
Method: direct source re-read (not trusting the design doc's paraphrase), per task instructions.

## Verdict

**REVISE_AND_REVIEW_ONCE**

The core defect this package fixes (Slice 6's real production bug — see item 1
below) is real, verified, and the proposed `have_policy` + certification
lifecycle fix for it is sound and consistent with `zcl_abapgit_ortec_mat_state`'s
documented preconditions. However, the design's central supporting claim that
`zcl_abapgit_ortec_fastpath=>pull_by_branch` is dead code is **factually
wrong** (verified against live call graph), which invalidates part of §0's
narrative, the migration map (§13), and the "free of branch-pointer-equality
reliance" property claimed for the post-C2 system as a whole. A second,
unaddressed migration-day risk (§DR-003) also needs an explicit decision
before implementation. Neither issue is a data-corruption hazard (both are
guarded by existing raise-on-incompleteness / idempotent-certificate
mechanics), so this is corrective revision, not a redesign.

## Confidence

High — every load-bearing claim below was checked against the actual current
source (not the design doc's paraphrase), including full call-chain tracing
across `zcl_abapgit_ortec_porcelain`, `zcl_abapgit_git_transport`,
`zcl_abapgit_git_porcelain`, and `zcl_abapgit_ortec_fastpath`.

## Strengths

- §0 finding #1 (persist_pull_result never certifies) is real and precisely
  described — confirmed by direct read of
  `zcl_abapgit_ortec_fastpath=>persist_pull_result` (line ~1567): the raw
  `INSERT zaog_commit_hist FROM ls_hist` only populates
  `repo_key`/`commit_sha1`/`branch_name`/`fetched_at`; `hist_level`/
  `snap_state` are left space. This does make
  `get_verified_have_commits`/`is_commit_complete` (which now delegate to
  `mat_state=>is_graph_have_eligible`, requiring `hist_level IN ('G','F')`)
  return `abap_false`/empty for every commit ever produced by the live
  incremental path. This is the correct, most important finding in the
  document and the design's fix for it (§5/§6) is sound.
- `get_certified_haves`'s SQL (`WHERE repo_key = @iv_repo_key AND hist_level
  IN ('G','F')`) is a leading-primary-key-column range scan, not a full-table
  scan — confirmed against `zaog_commit_hist.tabl.xml`'s key
  (MANDT, REPO_KEY, COMMIT_SHA1). One bulk SELECT per call, no per-candidate
  SQL. Compliant with the repo's binding constraints
  (`.memory/state.md` §"Binding constraints": "No per-object SQL or HTTP",
  "No uncertified haves").
- §6's certification lifecycle ordering
  (`begin_attempt` → `verify_tree_closure` → `mark_graph_complete` →
  `get_tip_blob_sha1s`/`get_missing_sha1s` → `mark_full_complete` →
  `publish_snapshot_complete` → one `COMMIT WORK`) exactly matches the
  preconditions enforced by `zcl_abapgit_ortec_mat_state`'s actual
  implementation (verified by reading `mark_graph_complete`,
  `mark_full_complete`, `publish_snapshot_complete`): `mark_full_complete`
  raises unless `hist_level = GRAPH_COMPLETE`; `publish_snapshot_complete`
  raises unless `hist_level IN (G,F)`; both raise on stale `attempt_id`. No
  ordering violation possible.
- `acquire_blobless_graph` and `materialize_tip_snapshot`
  (`zcl_abapgit_ortec_cold_init`) do both self-commit
  (`COMMIT WORK.` at line 332 and line 409 respectively, confirmed by direct
  read) — the design correctly does not add a redundant orchestrator-level
  commit around them.
- COLD_BRANCH routing (§4) correctly fails closed on missing capability (no
  catch-and-degrade to an unfiltered fetch) and does not send haves/deepen/
  shallow — compliant with `.github/skills/git-partial-clone/SKILL.md`'s
  forbidden-substitutes list.
- `zcl_abapgit_ortec_fastpath=>first_progressive_deepen`/
  `next_progressive_deepen`/`c_progressive_*`/`build_upload_pack_buffer` are
  indeed only referenced from `zcl_abapgit_ortec_git_tests` (test-only) in
  current source — the migration map's `LEGACY_UNREACHABLE_AFTER_C2`
  classification for these specific symbols is accurate.

## Issues

### DR-001 — "pull_by_branch is dead code" is factually wrong; it is live and stays live after C2

- Type: correctness
- Severity: major
- Evidence:
  - `zcl_abapgit_git_porcelain=>pull_by_branch` (line ~528): for an
    Ortec-active repo, calls `zcl_abapgit_ortec_porcelain=>pull_by_branch`
    and `RETURN`s — this is the *only* live path for active repos (the
    `zcl_abapgit_ortec_fastpath=>pull_by_branch` call further down in the
    same method is in the `ELSE` branch, reached only when Ortec is
    *inactive* for the repo, and that call is indeed a genuine no-op there
    since fastpath's own `pull_by_branch` immediately `RETURN`s on
    `is_active_for_repo = abap_false`. This inactive-branch call is the
    *only* one that is actually dead — the design conflates it with the
    active-repo call path below).
  - `zcl_abapgit_ortec_porcelain=>pull_by_branch` (confirmed by direct read)
    unconditionally calls `zcl_abapgit_git_transport=>upload_pack_by_branch`.
  - `zcl_abapgit_git_transport=>upload_pack_by_branch` (line ~412): `IF
    zcl_abapgit_ortec_git_switch=>is_active_for_repo(...) = abap_true` →
    calls `zcl_abapgit_ortec_fastpath=>upload_pack_by_branch(...)` and
    `RETURN`s (never falls through to standard decode for active repos).
  - `zcl_abapgit_ortec_fastpath=>upload_pack_by_branch` (line ~809) calls its
    own private `pull_by_branch` **first**, before any HTTP
    thin/self-contained/recovery tier: `ls_pull = pull_by_branch(...). IF
    ls_pull IS NOT INITIAL. et_objects = ls_pull-objects. ... RETURN.
    ENDIF.`
  - Net effect: for **every** Ortec-active `pull_by_branch` call today, and
    — per the design's own migration map row ("add classification + 3-way
    routing **before existing `upload_pack_by_branch` call**") — for every
    future `INCREMENTAL_UPDATE`-classified call after Package C ships,
    `zcl_abapgit_ortec_fastpath=>pull_by_branch`'s Phase 1/1b/2/3 warm-skip
    logic runs and can short-circuit the entire request before
    `have_policy`'s certified-have selection or the thin/self-contained/
    recovery cascade is ever reached.
- Why it matters: this is one of the two "current-source findings" (§0) the
  whole package's rationale is built on, it is a direct answer to review
  task item 2, and it makes the migration map (§13) wrong: labeling this
  method `LEGACY_UNREACHABLE_AFTER_C2` is incorrect — it is reachable *before*
  C2, and remains reachable *after* C2 (the migration map does not touch
  `upload_pack_by_branch`'s call chain at all — it is marked
  `REUSE_NO_CHANGE`).
- Fix: correct §0/§13 to state the method is live (reachable via
  `upload_pack_by_branch`, not via the inactive-repo branch of
  `zcl_abapgit_git_porcelain=>pull_by_branch`), and make an explicit decision
  for it (see DR-002) rather than leaving it silently mischaracterized.

### DR-002 — surviving legacy warm-skip still trusts `ZAOG_REPO_STATE.fetch_commit` branch-pointer equality, uncontrolled by Package C

- Type: correctness
- Severity: major
- Evidence: `zcl_abapgit_ortec_fastpath=>pull_by_branch`'s "Phase 2" check is
  literally `IF lv_remote_sha = ls_state-fetch_commit.` → reconstitutes the
  entire pull result from the local store
  (`zcl_abapgit_ortec_obj_store=>get_reachable_objects`) with **no reference
  to `hist_level`/`snap_state` at all**. `fetch_commit` is written
  unconditionally on every successful fetch by
  `persist_pull_result`→`zcl_abapgit_ortec_repo_state=>update_after_fetch`
  (confirmed: this call is unconditional, independent of whether
  certification in §6 step 2/3 actually succeeded) — i.e. it is exactly the
  "branch-pointer equality as a trust source" review item 4 requires the
  design to be free of, and it is not read-gated by the new certificate at
  all.
  - Mitigating factor (checked directly, not assumed): `get_reachable_objects`
    is not unsafe in the "missing data → assumed complete" sense — it calls
    `get_objects(..., iv_bulk_fetch = abap_true)`, which unconditionally
    raises `zcx_abapgit_ortec_git` for any requested SHA1 not found (verified
    by reading the method body), and the caller's own `CATCH
    zcx_abapgit_ortec_git zcx_abapgit_exception` invalidates the tip and
    falls through to a real fetch. So this is not a silent-corruption bug —
    it is an *architectural* problem: two independent, differently-sourced
    warm-skip authorities (old: raw branch-pointer equality + full local
    tree-walk verification at read time; new: `have_policy`'s O(1)
    certificate read) now coexist in the same call chain, with the old one
    running *first* and able to pre-empt the new classifier's decision, plus
    an extra, unbudgeted `branches()` info/refs GET (fastpath's own tip
    resolution) on top of the one the new orchestrator's §2 already performs.
- Why it matters: undermines the claimed post-C2 invariant that
  classification is "never a branch-pointer comparison... as a trust
  source" (review item 4) at the whole-system level, even though the *new*
  code the design adds is itself clean. Also means §12's SQL/HTTP budget
  under-counts one HTTP GET for every `INCREMENTAL_UPDATE` call.
- Fix: pick one explicitly, document it in the design, and update §12's
  budget accordingly:
  (a) retire/short-circuit fastpath's own Phase 2/3 check so
      `have_policy`'s certificate is the sole gate reached via this call
      chain, or
  (b) keep it as an intentional, documented redundant fast-path
      optimization (it is not unsafe) and account for its extra GET/local
      walk cost in §12, or
  (c) route `INCREMENTAL_UPDATE` through a lower-level entry point that
      bypasses `zcl_abapgit_git_transport=>upload_pack_by_branch`'s Ortec
      detour entirely.

### DR-003 — unaddressed migration-day "cold storm" for every existing repo

- Type: correctness / performance
- Severity: major (self-healing, but a real, undocumented one-time cost)
- Evidence: after C1+C2 ship, every commit ever recorded by today's
  live `persist_pull_result` has `hist_level` = space (finding #1, DR
  n/a — this is the same root cause). `classify_operation`'s rules (§3):
  rule 1 requires `snap_state = COMPLETE` (never true for pre-existing
  rows); rule 2 requires `get_certified_haves` to return ≥1 row with
  `hist_level IN ('G','F')` for the repo (also never true for pre-existing
  rows, since `get_certified_haves` filters on exactly that column). Every
  repo that was previously "warm" under the old fetch_commit-based check
  will therefore classify as `COLD_BRANCH` on its very next pull after
  deployment — triggering a full `INITIAL_BRANCH_BLOBLESS` fetch (§4 step 1)
  plus tip-blob materialization (§4 step 2) for repositories whose objects
  are, in fact, already fully present locally.
- Why it matters: this is exactly the class of expensive operation
  (`filter blob:none` graph fetch + full tip-blob materialization for a
  potentially large repository) Package B/C exist to avoid triggering
  unnecessarily. The design's §9 ("idempotent restart behavior") and §15
  (checkpoint plan) do not mention or budget for this one-time, deployment-
  triggered cost at all — it is a real gap in review item 6's "migration map
  correctness" scope, not covered by any existing section.
- Fix: the design must explicitly address this before implementation —
  either (a) a documented one-time backfill/certification pass for
  already-fully-present repos' current recorded commits (e.g. running
  `verify_tree_closure`/`get_missing_sha1s`+certify once per repo as a
  migration step, not via a full re-fetch), or (b) an explicit, reasoned
  written decision to accept the one-time COLD_BRANCH cost per repo as
  acceptable, with the rationale documented (e.g. "acceptable because X").
  Silence on this is not acceptable for a package whose stated purpose is
  avoiding exactly this class of cost.

### DR-004 — §6 step 2's catch/skip behavior is not made explicit

- Type: maintainability
- Severity: minor
- Evidence: §6 step 2 says "On failure: do not call `mark_graph_complete`;
  skip straight to step 5" but `verify_tree_closure` raises
  `zcx_abapgit_ortec_git` on failure (confirmed) — the design never states
  this must be wrapped in `TRY/CATCH zcx_abapgit_ortec_git` inside
  `persist_pull_result`'s replacement code. Without that being explicit, an
  implementer could let the exception propagate uncaught, aborting before
  step 5 (repo-state bookkeeping) and the final `COMMIT WORK` — losing the
  already-persisted objects' visibility to the caller for that pull even
  though the underlying `ZAOG_OBJ_STORE` rows would still be committed by
  whatever LUW boundary is active.
- Why it matters: ambiguous enough to produce two different, both-plausible
  implementations with different failure behavior.
- Fix: make the `TRY ... CATCH zcx_abapgit_ortec_git` (or equivalent)
  explicit in the design text for step 2.

### DR-005 — stale docstrings on already-superseded methods (informational, not a Package C regression)

- Type: maintainability
- Severity: minor
- Evidence: `zcl_abapgit_ortec_fetch_neg=>get_verified_have_commits`/
  `is_commit_complete`'s docstrings still describe a "STRICT marker" +
  dangling-delta-base check, but the actual method body (confirmed by direct
  read) already delegates purely to
  `zcl_abapgit_ortec_mat_state=>is_graph_have_eligible` (a Slice 2C
  simplification, predating Package C). The design's `have_policy` proposal
  is therefore behaviorally equivalent to what `is_commit_complete` already
  does today, not a narrowing of guarantees — this is **not** a new gap
  introduced by Package C.
- Why it matters: purely a pre-existing documentation-hygiene issue that
  could mislead a future reader into thinking a dangling-delta-base check
  still happens somewhere in the have-selection path. Worth a one-line
  mention in the migration map since these methods are being marked
  `LEGACY_UNREACHABLE_AFTER_C2` anyway.
- Fix: optional — note in the migration map that these docstrings are stale
  and should be corrected or removed together with the physical removal in
  Package E.

## Required revisions

1. Correct §0/§13 to reflect that `zcl_abapgit_ortec_fastpath=>pull_by_branch`
   is live today and remains live after C2 via `upload_pack_by_branch`
   (DR-001).
2. Make an explicit, documented decision for the surviving legacy warm-skip
   and update §12's SQL/HTTP budget accordingly (DR-002).
3. Add an explicit migration-day plan (backfill or a reasoned accept-the-cost
   decision) for pre-existing, uncertified repo history (DR-003).
4. Make the failure-handling of §6 step 2 explicit (`TRY/CATCH`) (DR-004).

## Optional improvements

- Note stale `is_commit_complete`/`get_verified_have_commits` docstrings for
  cleanup in Package E (DR-005).

## Auto-iteration note

Per mode rules: this verdict allows exactly one revision + re-review cycle.
If the corrected design still leaves DR-001/002/003 unresolved after that
iteration, escalate to `.memory/decisions/design_review_impasse.md` and stop.

---

## RE-REVIEW (VB-C-C0-REVISION-REREVIEW) — 2026-07-22

Scope: verify resolution of DR-001..DR-004 (this report) and F1/F4 (protocol
review) against the revised `.memory/logs/variant_b_package_c_design.md`
only; re-read cited source sections as needed; sanity-check for any newly
introduced contradiction. Full from-scratch review not repeated.

### Per-finding verification

- **DR-001 — RESOLVED.** §0's "Correction after C0 correctness review"
  paragraph now correctly states `zcl_abapgit_ortec_fastpath=>pull_by_branch`
  is live (reached as the first statement of `upload_pack_by_branch`), not
  dead code, and §13's migration map now classifies it `REUSE_NO_CHANGE`
  with an explicit correction note instead of the old
  `LEGACY_UNREACHABLE_AFTER_C2` mischaracterization.
- **DR-002 — RESOLVED.** §2 ("Disclosed, not eliminated, redundant GET
  (DR-002 disposition)") explicitly picks option (b) from the original
  fix menu: keeps the shared, unmodified legacy check as a documented,
  safe, redundant fast-path (out of Package C's edit scope because it is
  shared with `zcl_abapgit_git_commit`/`zcl_abapgit_merge`), and §12's HTTP
  budget for `INCREMENTAL_UPDATE`/`COLD_BRANCH` now explicitly line-items
  "up to 1 further GET (shared legacy `fastpath=>pull_by_branch`'s own tip
  re-resolution... disclosed in §2)" instead of omitting it.
- **DR-003 — RESOLVED**, with one new observation (see below). New §3a
  ("Opportunistic local backfill") adds a one-shot, local-only,
  HTTP-free certification attempt (`exists` → `begin_attempt` →
  `verify_tree_closure` → `mark_graph_complete` → tip-blob/missing check →
  `mark_full_complete` + `publish_snapshot_complete` + one `COMMIT WORK`,
  reusing only already-approved Package B B1/B2 bulk APIs) before falling
  through to `COLD_BRANCH`, closing the migration-day cold-storm for
  already-locally-complete legacy repos. Bounded by the one target commit's
  own graph/blob size (K), not repository-wide (N) — no new SQL/HTTP shape.
- **DR-004 / F2 — RESOLVED.** §6 step 2 now explicitly states
  `verify_tree_closure` is "wrapped in `TRY ... CATCH zcx_abapgit_ortec_git`
  (explicit, closes DR-004/protocol F2...)".
- **F1 — RESOLVED.** §5's SQL now filters `hist_level = 'F'` only (not
  `IN ('G','F')`), with an explicit rationale (graph-complete-only commits
  may lack verified blobs — dangling-delta-base hazard). §3 rule 2 text
  confirms the same `get_certified_haves` call (same `'F'`-only filter) is
  reused for the "any candidate" classification check, so the restriction
  is applied consistently in both places as required. §14's test map also
  now lists a "graph-complete-only (`hist_level='G'`, not yet full)
  ineligible" case, consistent with the fix.
- **F4 — RESOLVED.** §5 has an explicit "Disclosed trade-off (protocol
  review F4)" paragraph stating `get_certified_haves` intentionally does not
  fall back to `ZAOG_REPO_STATE.fetch_commit` as an additional have source,
  and reasons this is fail-safe (fewer, always-certified haves; the residual
  gap is closed for already-known repos by §3a's backfill).

### Sanity check — previously-approved properties still hold

- No per-candidate SQL: still true. §12 explicitly re-states "No SQL per
  candidate" and bounds §3a's backfill cost by K (one commit's graph/blob
  size), not N (repository-wide).
- No unfiltered fallback: still true. §4 COLD_BRANCH step 1 unchanged —
  missing `filter` capability still propagates typed, no catch-and-degrade.
- One orchestration owner: still true. §1 unchanged — `zcl_abapgit_ortec_porcelain`
  remains the sole owner; `have_policy` is a new pure/bulk-SQL helper class,
  not a second orchestrator.
- One `COMMIT WORK` per attempt: still true per-attempt, but see new
  observation below for a related documentation gap introduced by §3a.

### New observation (not one of the six required items; minor, non-blocking)

**DR-006 — §1's "pure certificate read, no want-side effects" description of
`classify_operation` is now inconsistent with §3a's actual behavior.**
- Type: maintainability (documentation/API-contract precision)
- Severity: minor
- Evidence: §1 describes `classify_operation` as "pure certificate read, no
  HTTP, no want-side effects." §3a, added to resolve DR-003, has
  `classify_operation` itself call `begin_attempt`, `verify_tree_closure`,
  `mark_graph_complete`, `mark_full_complete`, `publish_snapshot_complete`,
  and issue its own `COMMIT WORK` before returning `WARM_UNCHANGED` for an
  already-locally-complete legacy repo. This is a real write path (and a
  real, if small, extra commit boundary) inside a method §1 still documents
  as read-only/side-effect-free.
- Why it matters: purely a documentation-consistency gap, not a functional
  defect — the write sequence itself reuses the same self-committing
  single-attempt shape already approved for Package B's cold-init methods,
  and it only fires once per legacy repo's first post-deployment pull (not
  a repeating cost). But an implementer reading only §1 could reasonably
  assume `classify_operation` is safe to call speculatively/repeatedly with
  no durability side effects, which is no longer true once §3a fires.
- Fix (optional, non-blocking): update §1's one-line description to note
  that `classify_operation` may, at most once per repo, perform the §3a
  backfill write+commit before returning `WARM_UNCHANGED`/falling through to
  `COLD_BRANCH` — or split backfill into an explicitly-named, separately
  documented step so §1's contract for the pure classification read stays
  accurate.

### Updated verdict

**APPROVE_WITH_MINOR_REVISIONS** — all six required findings (DR-001,
DR-002, DR-003, DR-004, F1, F4) are correctly resolved in the revised
design text; no blocking correctness/performance issue found. DR-006 (new,
minor, documentation-only) may be fixed now or deferred; it does not gate
C1 implementation start.
