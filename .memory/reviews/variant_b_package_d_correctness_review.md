# Design Review (re-review after REVISE_AND_REVIEW_ONCE)

Reviewed: `.memory/logs/variant_b_package_d_design.md` (revised), baseline commit
`29199f629773c676e0eaa2f3a006f5167d304ae8`. This pass re-verifies the 3 findings from the prior review
(DR-001, DR-002, DR-003) against the revised §4/§5.1, §11, and §15 text, cross-checked directly against
current source in `src/ortec/git/*.clas.abap` (read-only; no source modified).

## Verdict
REVISE_AND_REVIEW_ONCE

## Confidence
High — all three prior findings and the one new finding below cite exact source re-reads (methods and
call sites), not the design document's own claims.

## Prior findings — resolution check

### DR-001 (was: two non-interoperable repo-lock primitives; nested-acquisition risk) — RESOLVED, with one new residual gap (see DR-004)

- The revised §11 now names exactly one canonical primitive for D2's attempt-lifetime serialization:
  `zcl_abapgit_ortec_pack_dec=>acquire_repo_lock`/`release_repo_lock` (the SAP enqueue-based lock,
  `ENQUEUE_EZAOG_REPO_LOCK`/`DEQUEUE_EZAOG_REPO_LOCK`, `_scope = '2'`) — confirmed correct against
  [zcl_abapgit_ortec_pack_dec.clas.abap](src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap). The
  competing `pack_raw` DB-row mutex is explicitly left untouched and correctly re-scoped as
  `zcl_abapgit_ortec_obj_index`'s own unrelated concern, flagged as a future rename/consolidation
  follow-up outside Package D — matches source (`pack_raw=>acquire_repo_lock` has no other callers).
- The nested-acquisition/premature-dequeue fix (new `iv_lock_held` parameter on `resume_decode`, and a
  matching parameter threaded through `pull_by_branch`, defaulting to `abap_false` for backward
  compatibility) is a structurally sound fix that sidesteps the open question of whether SAP enqueue
  locks are reference-counted per owner — it never relies on that undocumented behavior at all, per the
  review's own suggested fix option 2. Verified plausible against the real, current `resume_decode`
  body (it currently unconditionally calls `acquire_repo_lock`/`release_repo_lock` around its whole
  body — trivial to guard with `IF iv_lock_held = abap_false`) and the real `pull_by_branch` body
  (currently calls `zcl_abapgit_ortec_pack_dec=>resume_decode( lv_repo_key )` only inside the
  conditional "resume matches branch+deepen" branch — adding a second actual parameter here is a
  mechanical, low-risk change).
- Residual gap found during this pass: see **DR-004** below — the *lock's own scope*, as literally
  described in §15, does not obviously cover "the full attempt lifetime" that §11 requires. This is a
  new, distinct finding from the original DR-001 (which was about which primitive to use and the
  double-acquire hazard, both now fixed); DR-004 is about whether the fix's *placement* actually
  delivers the stated concurrency guarantee.

### DR-002 (was: `cleanup_partial_session` incorrectly cited) — RESOLVED

- §5.3 now contains an explicit correction: `cleanup_partial_session` is called out as filtering
  `status = 'P'` (the legacy `resumable_decode`/`pack_dec` session convention), not `'I'`, and is
  explicitly excluded from the fix. §12 and §15 both consistently repeat this exclusion.
- Re-confirmed directly against source:
  [zcl_abapgit_ortec_pack_raw.clas.abap:356](src/ortec/git/zcl_abapgit_ortec_pack_raw.clas.abap#L356)
  (`cleanup_partial_session`, filters `status = 'P'`) and its one caller,
  `zcl_abapgit_ortec_fastpath=>pull_by_branch`'s context-mismatch cleanup — matches the design's
  corrected attribution exactly. `zcl_abapgit_ortec_pack_stream=>cleanup_incomplete` remains the sole,
  correctly-scoped target for the `status IN ('I','D')` widening (§5.3, §12, §15).

### DR-003 (was: Phase 1.5 merge must sync `ct_tabix_by_index`/`ct_sha_idx`) — RESOLVED

- §4 now contains an explicit, correctly-targeted "Mandatory auxiliary-index synchronization" clause
  requiring Phase 1.5 to insert every merged row into `ct_tabix_by_index` (non-streaming) and
  `ct_sha_idx` (streaming) — naming the exact hashed tables and exact existing on-demand-fetch fallback
  bookkeeping to mirror.
- Re-confirmed against source: `zcl_abapgit_ortec_delta`'s `resolve_all`/`resolve_one` populate and read
  `ct_tabix_by_index` exactly as described (startup loop + on-demand-fetch fallback insert), and
  `zcl_abapgit_ortec_pack_stream`'s `resolve_one_meta`/`resolve_streaming` populate/read `ct_sha_idx` via
  `READ TABLE ct_sha_idx ... WITH TABLE KEY sha1` exactly as described. §16's new test list also adds
  `bulk_base_unique_index`/`no_sql_in_pack_phase`, which would catch a missed index-sync regression
  (zero-fallback-invocation assertion, not just a correct final result) — appropriately strengthens the
  exit criteria referenced in §19.

## New finding from this pass

### DR-004
- Type: correctness
- Severity: major
- Evidence: §9 and the newly-revised §11/§15 both require `begin_attempt` to be called **earlier than
  today, before any pack fetch**, at the top of the fastpath orchestration (`upload_pack_by_branch`), so
  the resulting `attempt_id` can be threaded down into `decode_streaming`/
  `decode_and_persist_streaming`/`create_session`/`update_session_progress` (§9) and so the new
  attempt-lifetime lock can be acquired before `pull_by_branch` runs (§11/§15). However, the **existing,
  unmodified** `zcl_abapgit_ortec_fastpath=>certify_fetched_commit`
  ([zcl_abapgit_ortec_fastpath.clas.abap:1656](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L1656))
  — called later, at the end of the same successful fetch, from `persist_pull_result` — **also calls
  `begin_attempt` itself** to obtain its own local `lv_attempt_id`. Direct read of
  `zcl_abapgit_ortec_mat_state=>begin_attempt` confirms it **always generates a brand-new UUID via
  `cl_system_uuid=>create_uuid_c32_static( )` on every call, and unconditionally overwrites
  `ZAOG_COMMIT_HIST.attempt_id`** with that new value on every invocation — it does not reuse an
  already-in-progress attempt_id for the same `repo_key`/`commit_sha1` row. §15's file-scope list does
  **not** list `certify_fetched_commit`/`persist_pull_result` as being changed to accept and reuse the
  earlier-generated `attempt_id` instead of calling `begin_attempt` again.
  Net effect: for a single real fetch attempt, two different `attempt_id` UUIDs would be generated —
  one early (A, used to tag the new `attempt_id` columns on `ZAOG_OBJ_STORE`/`ZAOG_FETCH_SESS`/
  `ZAOG_PACK_META` per §9) and one later inside `certify_fetched_commit` (B, the value actually
  persisted as `ZAOG_COMMIT_HIST.attempt_id` and used for `mark_graph_complete`/`mark_full_complete`).
  A and B would never match for any real attempt.
- Why it matters: this silently defeats §9's entire stated purpose ("given an `attempt_id`, an operator
  can now join across `ZAOG_COMMIT_HIST`, `ZAOG_OBJ_STORE`, `ZAOG_FETCH_SESS`, and `ZAOG_PACK_META` in
  one WHERE clause") — the join would return zero rows for every attempt, since the object/session/
  pack-meta rows are tagged with A while `ZAOG_COMMIT_HIST` ends up with B. It does not corrupt data,
  break have-eligibility, or violate the missing-vs-deleted invariant (it is a diagnostics/correlation
  feature only), so it is scoped as major rather than blocking.
- Fix: either (a) have the early `begin_attempt` call's result flow into `certify_fetched_commit` as an
  `iv_attempt_id` input that it reuses instead of calling `begin_attempt` again (requires adding
  `certify_fetched_commit`/`persist_pull_result` to §15's file-scope list, threading the same
  `iv_attempt_id` parameter already being added elsewhere), or (b) change `begin_attempt` itself to
  become idempotent per `repo_key`/`commit_sha1` — reuse the existing row's `attempt_id` if one is
  already set and not stale, only generating a fresh UUID when none exists or the existing one is stale
  (mirroring the stale-attempt rejection window already used elsewhere). Option (a) is the smaller,
  more localized change and is consistent with the rest of §9's threading-not-regenerating design intent.

## Sanity-check notes (iv_lock_held threading plausibility)

- `pull_by_branch` (current source) calls `resume_decode` conditionally — only when an active session
  exists whose `branch_name`/`deepen_level` match the request — not unconditionally as the design's
  prose slightly overstates. This does not change the substance of the DR-001 fix: threading
  `iv_lock_held` through `pull_by_branch` into that one call site is still mechanically correct
  regardless of whether the branch is always or sometimes taken.
- `pull_by_branch` itself never calls `acquire_repo_lock`/`release_repo_lock` directly (only
  `resume_decode` does internally) — §11's "those methods skip their own internal acquire/release
  calls" slightly overstates `pull_by_branch`'s role (it only needs to *forward* the parameter, it has
  no internal lock calls of its own to skip). Cosmetic only, not a functional defect.
- §15's phrasing "wrap `pull_by_branch` (called from `upload_pack_by_branch`) with ...
  `acquire_repo_lock`/`release_repo_lock` for the full attempt lifetime" is ambiguous on first read (it
  could be misread as scoping the lock to only the `pull_by_branch` call, which would NOT cover the
  subsequent thin/self-contained/recovery HTTP-fetch cascade that runs later in the same
  `upload_pack_by_branch` method when `pull_by_branch` returns an empty result). §11's own text is
  unambiguous ("the top-level fastpath orchestration has already acquired the enqueue lock for the
  whole attempt"), and §15 explicitly cross-references "(§11)", so this is treated as a wording/optional
  clarity issue, not a second blocking finding — but it should be tightened before implementation so
  `upload_pack_by_branch`'s entire body (not just the `pull_by_branch` call) is understood as the lock's
  span.

## Required revisions

1. DR-004: thread the early-acquired `attempt_id` into `certify_fetched_commit` (via
   `persist_pull_result`) so it reuses that value instead of calling `begin_attempt` a second time, and
   add both methods to §15's file-scope list — or make `begin_attempt` itself idempotent per
   `repo_key`/`commit_sha1`.

## Optional improvements

- Reword §15's lock-wrapping sentence to explicitly say "wrap the entire `upload_pack_by_branch` method
  body" rather than "wrap `pull_by_branch`", to remove the first-read ambiguity noted above.
- (Carried over, still open) Consider whether `ZAOG_PACK_META` actually needs the new `attempt_id`
  column, since no evidence has been found that the streaming path writes `ZAOG_PACK_META` at all.
- (Carried over, still open) Consider consolidating or renaming the two identically-named
  `acquire_repo_lock`/`release_repo_lock` implementations (`pack_raw` vs `pack_dec`) as a follow-up
  hardening item independent of Package D.
