# Design Review — Variant B, Slice 2 (Explicit fetch modes and one request serializer)

Reviewed: `.memory/logs/variant_b_slice2_design.md` against
`.memory/logs/variant_b_slice2_reconciliation.md`,
`.memory/logs/variant_b_design.md` (Slice 1 "Review resolution"), and
`.github/prompts/variant-b.prompt.md` (Slice 2 section + non-negotiable
invariants). Spot-checked against live source:
`src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap`,
`zcl_abapgit_ortec_fetch_neg.clas.abap`, `zcl_abapgit_ortec_repo_state.clas.abap`,
`zcl_abapgit_ortec_filter_walk.clas.abap`, `zcl_abapgit_ortec_pack_stream.clas.abap`,
`zcx_abapgit_ortec_git.clas.abap`, `zcl_abapgit_ortec_mat_state.clas.abap`.

## Verdict
APPROVE_WITH_MINOR_REVISIONS

## Confidence
Medium-High

## Strengths

- The mode enum + pure serializer (`ZCL_ABAPGIT_ORTEC_FETCH_REQ`) is a correct
  architectural answer to "replace interacting booleans with explicit modes."
  `build_request` is genuinely zero-SQL/zero-HTTP (verified against the
  reconciliation's own claim and consistent with the `build_upload_pack_buffer`
  precedent it replaces).
- The DR-001/DR-002 deferral-to-Slice-3 reasoning (§5) is sound, not lazy: I
  independently traced `is_commit_complete`'s reroute (§4) and confirmed it does
  **not** touch `FETCH_COMMIT`'s write path, and that `publish_snapshot_complete`
  genuinely cannot be honestly called without a real Slice-3 orchestration
  result. The argument that a *fabricated* graph-complete certificate would be
  strictly worse than the current known-uncertified gap is correct and
  well-argued against the owner's "failed attempts publish nothing" invariant.
- `MATERIALIZE_BLOBS`'s capability names (`allow-reachable-sha1-in-want` /
  `allow-tip-sha1-in-want`) are **not** an unverified assumption — they are
  already referenced verbatim in the existing `complete_missing_object` doc
  comment (fastpath, ~L120-128), i.e. carried over from already-reviewed code,
  not invented for this design.
- AC4 ("no mode's branch can emit `deepen`") is structurally real for the 5
  modes: I confirmed no `deepen` token appears anywhere plausible in the
  designed `build_request` branches.
- The `is_graph_have_eligible` API the design's §4 swap depends on genuinely
  exists (`zcl_abapgit_ortec_mat_state.clas.abap` L163/L410) with the signature
  the design assumes.

## Issues

### DR-001
- Type: correctness
- Severity: major
- Evidence: `try_filtered_commit_fetch` (fastpath, ~L462-548, live source) wraps
  its entire body — including the point where §3 says it now calls
  `build_request( iv_mode = initial_branch_blobless )` — in
  `CATCH zcx_abapgit_exception zcx_abapgit_ortec_git. CLEAR rv_applicable.`.
  `raise_unsupported_capability` (§2.4) raises exactly `zcx_abapgit_ortec_git`.
- Why it matters: the owner's `INITIAL_BRANCH_BLOBLESS` bullet says "if filter
  is unavailable, raise a structured unsupported-capability result; do not
  silently issue a huge unfiltered fetch." `build_request` itself honors this
  (AC5, unit-tested in isolation), but at the **one live call site Slice 2
  itself reroutes this slice**, the existing outer catch immediately swallows
  that exact exception back into a silent `rv_applicable = abap_false`, which
  its caller (`zcl_abapgit_ortec_filter_walk=>get_remote_files_for_stage`,
  untouched this slice) already turns into a fallback to
  `ii_repo_online->get_files_remote(...)` — the standard, non-filtered fetch
  path. Net effect for this call site is unchanged from today (today's code
  already does a bare `RETURN` on missing filter, with the same downstream
  fallback) — so Slice 2 does not make anything *worse*, but it also does not
  deliver the "structured, caller-actionable, no-silent-fallback" guarantee
  the design's own §2.4/§6 claim for this mode; the signal is created and then
  immediately discarded within the same slice's own code, one frame up from
  where AC5 tests it.
- Fix: either (a) have `try_filtered_commit_fetch` special-case
  `mv_unsupported_capability = abap_true` before its generic catch (e.g.
  re-raise, or set a distinct out-parameter the Slice-3 caller can later
  inspect) instead of folding it into the generic "not applicable" path, or
  (b) explicitly document in §3/§5 that end-to-end signal surfacing to
  `filter_walk` is deliberately deferred to Slice 3 alongside DR-001's other
  reader migrations — i.e. give this the same explicit, named deferral
  treatment DR-001/DR-002 already got, rather than leaving it unstated.

### DR-002
- Type: correctness
- Severity: major
- Evidence: `fetch_tip_commits` (fastpath, ~L323-383, live source) still
  builds `want ... deepen 1 ... filter tree:0` by hand and is confirmed to
  literally emit `deepen 1` (not a progressive/widening value, but still the
  `deepen` wire token). The design (§3) acknowledges this and leaves it
  untouched, calling it "a deliberate scope boundary, not an oversight —
  flagged for the reviewer," reasoning that inventing a 6th mode is not this
  slice's call.
- Why it matters: not inventing a 6th named mode is a defensible scope
  decision. But the practical effect is that a live, still-active ORTEC
  request-building code path continues to emit the exact protocol token
  (`deepen`) that Slice 2's whole purpose is to eliminate from the reviewable
  surface, and it does so **entirely outside** the new serializer's AC4
  guarantee, outside the §6 wire-test plan, and with no named follow-up slice
  committed to closing it (unlike DR-001/DR-002 from Slice 1, which got an
  explicit "Slice 3 MUST..." binding consequence). This is a scope hole that
  could silently persist indefinitely if not tracked.
- Fix: add a binding consequence sentence analogous to §5's DR-001/DR-002
  wording — e.g. "Slice 3's cold-branch design MUST either route
  `fetch_tip_commits` through `build_request` (extending the mode table only
  if truly needed) or explicitly re-justify its continued existence as a
  hand-built buffer" — so this doesn't become a second, permanently-untracked
  legacy path the way the progressive-deepen constants almost did.

### DR-003
- Type: correctness (evidence completeness)
- Severity: minor
- Evidence: `zcl_abapgit_ortec_repo_state=>get_complete_commits` (live source,
  L232-267) is a **third** live reader of `FETCH_COMMIT` (via
  `SELECT DISTINCT fetch_commit FROM zaog_repo_state`), feeding candidates
  directly into `get_have_commits` → `get_verified_have_commits` → exactly the
  certification pipeline Slice 2's §4 modifies. Neither the reconciliation
  report nor the design names this reader alongside DR-001's two named readers
  (`pull_by_branch` shortcut, `filter_walk` default lookup).
- Why it matters: this is not a live bug — the method's own comment already
  states "each is independently verified (`is_commit_complete`) before ever
  being trusted," and I confirmed every candidate it produces is filtered
  through `is_commit_complete`/`is_graph_have_eligible` before being usable as
  a `have`, so Slice 2's swap actually closes this reader's trust gap rather
  than leaving it open. But Focus Q1 of this review is precisely "does
  anything read `FETCH_COMMIT` differently, even indirectly" — the design's
  evidence base should have surfaced this third touchpoint explicitly rather
  than the review having to trace it independently, especially since it sits
  directly on the code path §4 rewrites.
- Fix: add one sentence to §4 naming `get_complete_commits` as a third,
  already-gated `FETCH_COMMIT` reader whose safety is preserved (not
  incidentally) by the `is_commit_complete` swap.

### DR-004
- Type: correctness
- Severity: major
- Evidence: the owner's `RECOVERY_BRANCH_FULL` bullet list requires "fresh
  HTTP client, attempt/session/pack IDs, and decode-local cache" (in addition
  to the memory gate). The design (§7) only addresses "fresh HTTP client"
  (and confirms via live-source spot check that **all three** existing tiers,
  not just recovery, already call `find_branch_ortec` fresh per attempt — so
  this part is a no-op preservation, not new Slice 2 work) and explicitly
  defers the memory gate to Slice 5. Neither "attempt/session/pack IDs" nor
  "decode-local cache" is mentioned anywhere in the design, not even as an
  explicit scope exclusion. Live source shows
  `zcl_abapgit_ortec_pack_stream=>reset_completion_budget()` is currently
  called **once** per whole `upload_pack_by_branch`/`by_commit` call, shared
  across all three tiers — meaning today's (and, unchanged, Slice 2's)
  recovery tier runs with a decode-local budget/cache already partially
  consumed by the failed thin/self-contained attempts, not the fresh state
  the owner spec calls for.
- Why it matters: this is a named, mode-specific owner requirement with no
  design coverage at all, not a deferred/justified scope cut — the design
  reads as if "fresh HTTP client" fully satisfies the bullet when the bullet
  lists three additional properties.
- Fix: either scope this explicitly out with the same reasoning discipline
  used for DR-001/DR-002 (e.g. "attempt/session/pack IDs require Slice 1's
  `begin_attempt` identity, not available in a useful form until Slice 3 —
  deferred, tracked"), or add the decode-local cache reset
  (`reset_completion_budget( )` or equivalent) explicitly to the
  `RECOVERY_BRANCH_FULL` call site as part of this slice's fastpath rewiring.

### DR-005
- Type: maintainability / documentation
- Severity: minor
- Evidence: today's `build_upload_pack_buffer` (live source, L1330-1337)
  emits `shallow <sha>` lines for existing haves whenever
  `iv_force_full = abap_false`. The new wire-shape table (§2.3) sets
  `shallow: never` for every mode, including `INCREMENTAL_THIN`/
  `INCREMENTAL_SELF_CONTAINED` where haves are used. This is a real,
  unremarked-upon behavior change bundled implicitly into the "AC4 realizes
  delete-progressive-deepen" narrative, even though `shallow` and `deepen` are
  distinct protocol tokens serving different purposes.
- Why it matters: the change is very likely correct (shallow-clone semantics
  are an artifact of the old deepen-based model; once haves are sourced only
  from mat_state-certified commits, telling the server "my history is
  boundary-truncated here" is no longer the right signal) — but the design
  never says so, so a future reader can't tell if this was a deliberate,
  reasoned removal or an accidental side effect of reusing the "no deepen"
  table cell for a different token.
- Fix: add one explicit sentence to §2.3 or §3 stating that `shallow` line
  emission is intentionally dropped because certified-have negotiation
  supersedes shallow-clone semantics, distinct from (though related to) the
  deepen removal.

## Required revisions

1. Resolve DR-001: decide and document how `mv_unsupported_capability` is
   meant to surface (or be explicitly, trackedly deferred) past
   `try_filtered_commit_fetch`'s existing broad catch.
2. Resolve DR-002: add a named-slice commitment for bringing
   `fetch_tip_commits`'s hand-built `deepen 1` buffer under review, mirroring
   the DR-001/DR-002-from-Slice-1 binding-consequence pattern.
3. Resolve DR-004: either explicitly, reasoned-ly defer "attempt/session/pack
   IDs" and "decode-local cache" for `RECOVERY_BRANCH_FULL`, or add the
   missing decode-local cache reset to this slice's scope.

## Optional improvements

- DR-003: name `get_complete_commits` explicitly in §4 as a third,
  already-gated `FETCH_COMMIT` touchpoint.
- DR-005: one sentence justifying the `shallow` line removal separately from
  `deepen`.
- Add a static `is_unsupported_capability( ix_exception )` reader on
  `zcx_abapgit_ortec_git`, parallel to the existing `is_retry_without_haves`,
  for symmetry/discoverability (functionally optional — direct attribute read
  works today, and I confirmed no current call site is at risk of confusing
  the two flags: `upload_pack_by_branch`/`by_commit`'s tiers never invoke a
  mode capable of raising `mv_unsupported_capability`, and
  `complete_missing_base`'s catch is a blanket, flag-agnostic swallow).
- Add a one-line class-doc rule on `ZCL_ABAPGIT_ORTEC_FETCH_REQ` committing it
  to remain SQL/HTTP-free forever, as a guardrail against it becoming a
  dumping ground once Slices 3-5 depend on it (the current design already
  achieves this boundary; documenting the rule makes future drift a visible
  doc violation, not just a convention).
