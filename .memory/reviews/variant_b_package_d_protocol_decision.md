# Package D — D0 protocol/persistence review: decision checkpoint

```text
PACKET=COMPACT_HANDOFF_V1
TASK=D-REVIEW-PROTOCOL-DECISION
BASELINE=29199f629773c676e0eaa2f3a006f5167d304ae8
STATUS=STOPPED_FOR_OWNER_DECISION
```

## Why this file exists

Same governance pattern as
[variant_b_package_d_correctness_decision.md](variant_b_package_d_correctness_decision.md):
a `REVISE_AND_REVIEW_ONCE` verdict allows exactly one automatic
design-revision + re-review iteration. That iteration has now been used for
the protocol/persistence review track. The re-review still returned
`REVISE_AND_REVIEW_ONCE` (one new blocking finding, B-3). Per the
gatekeeping rule ("If still disagreement → write decision file → STOP for
Michael"), and per the reviewer's own explicit recommendation ("Escalate to
owner-decision checkpoint... rather than a further automatic re-review"),
this checkpoint is written instead of looping a third time.

## Iteration history (protocol/persistence review)

1. **Review 1**: `REVISE_AND_REVIEW_ONCE`. 2 blocking (B-1: `pull_by_branch`
   has two independent callers, the design's lock/attempt-id plan covered
   only one; B-2: the planned lock methods are `PRIVATE`, cannot be called
   cross-class as scoped), 5 major (M-1..M-5: independent lock consumer via
   `try_filtered_commit_fetch`; lock-hold-duration/timeout regression risk
   from wrapping HTTP round-trips; exception-type mismatch breaking the
   established fallback pattern; `ZAOG_FETCH_SESS`/`ZAOG_PACK_META` never
   written on the default path; `persist_missing_objects` outside the
   attempt-id threading scope), 1 minor. I independently re-verified B-1,
   B-2, and M-1 against live source before applying fixes: narrowed the
   lock+`begin_attempt` scope to exactly the local
   decode/resolve/persist/certify/commit sequence (never across HTTP),
   split into two independent, sequential (never nested) units — "unit #1"
   inside `zcl_abapgit_ortec_fastpath=>pull_by_branch`'s own Phase-1b
   branch, "unit #2" believed (incorrectly, see below) to be inside
   `zcl_abapgit_ortec_fastpath=>upload_pack_by_branch`'s post-cascade
   fetch — changed the lock methods' visibility to `PUBLIC`, added
   exception-conversion/fallback handling, corrected the §9 diagnostic-value
   wording, and threaded `iv_attempt_id` into `persist_missing_objects`.
2. **Review 2 (the one automatic re-review)**: `REVISE_AND_REVIEW_ONCE`
   again. B-1 and B-2 confirmed resolved (B-1's fix is sound; its written
   rationale slightly overstates coverage — non-blocking, noted as m-2).
   M-1 and M-4 fully resolved. **One new blocking finding, B-3**: "unit #2"
   as located in the revised design (inside
   `zcl_abapgit_ortec_fastpath=>upload_pack_by_branch`) **does not exist**
   — that method never calls `persist_pull_result`/`certify_fetched_commit`
   at all. I independently re-verified this directly against source
   (grepping every call site of `persist_pull_result` in the workspace)
   and confirmed it is accurate:
   - The real, live call to `persist_pull_result` for a fresh-HTTP-fetch
     ("pull with new commits") attempt is in
     [zcl_abapgit_ortec_porcelain.clas.abap](src/ortec/git/zcl_abapgit_ortec_porcelain.clas.abap#L370),
     inside `zcl_abapgit_ortec_porcelain=>pull_by_branch`'s
     `INCREMENTAL_UPDATE` branch — a **third class, entirely absent from
     the design's §15 scope**. Its real sequence is: call
     `zcl_abapgit_git_transport=>upload_pack_by_branch` (which delegates to
     `zcl_abapgit_ortec_fastpath=>upload_pack_by_branch` for the HTTP
     cascade + streaming decode — object persistence only, no
     certification, exactly matching design §10's ownership table) → call
     `pull(...)` (standard tree-walk/file-materialization helper) → **then**
     call `persist_pull_result` (where `certify_fetched_commit` and the
     single `COMMIT WORK` actually happen).
   - Two more pre-existing external callers of `persist_pull_result` are
     also outside §15's scope: `zcl_abapgit_git_porcelain.clas.abap:650`
     (a switch-inactive fallback branch, a no-op at runtime today but must
     still compile) and `zcl_abapgit_ortec_git_tests.clas.testclasses.abap:680`
     (`persist_creates_state` unit test).
   - I additionally confirmed `zcl_abapgit_ortec_porcelain=>pull_by_commit`
     (the sibling method) does **not** call `persist_pull_result` at all —
     a separate, pre-existing gap, out of Package D's scope, not something
     this design needs to address.
   - Consequence if implemented as currently scoped: either a
     compile-breaking mandatory-parameter change (if `iv_attempt_id` is
     required) or a silent revival of the DR-004 double-mint/correlation
     defeat for the common "pull with new commits" case (if optional with
     an internal fallback) — and either way, unlocked, since no
     `acquire_repo_lock` call exists anywhere in this real chain today.
   M-2/M-3/M-5's "unit #2" portions are consequently unverifiable until
   B-3 is resolved (their "unit #1" portions remain confirmed resolved).

## Current state of the design and review artifacts

`.memory/logs/variant_b_package_d_design.md` still describes "unit #2" as
living inside `zcl_abapgit_ortec_fastpath=>upload_pack_by_branch` — **this
is now known to be incorrect** and has **not yet been corrected**, pending
this decision. `.memory/reviews/variant_b_package_d_protocol_review.md`
reflects the review-2 findings (B-3 open, blocking).

## Options for Michael

- **A.** Authorize the orchestrator to relocate "unit #2" directly to
  `zcl_abapgit_ortec_porcelain=>pull_by_branch`'s `INCREMENTAL_UPDATE`
  branch (the reviewer's own recommended, evidence-based location),
  add that class/method to design §15's scope, explicitly resolve the two
  other pre-existing `persist_pull_result` call sites (documented
  intentional "no lock, own `begin_attempt`" fallback vs. direct update),
  then run one further protocol/persistence re-review (this would be a
  second manual iteration on this track, mirroring how DR-004 was handled
  on the correctness track) before proceeding to the performance
  DESIGN_GATE.
- **B.** Provide different direction on where "unit #2" should live (e.g.
  push the lock/attempt boundary even further up/down the call graph, or
  reconsider whether a second unit is needed at all — e.g. could
  `zcl_abapgit_ortec_porcelain=>pull_by_branch` become the SOLE place
  `begin_attempt`/lock is acquired for the fresh-fetch case, with
  `zcl_abapgit_ortec_fastpath=>upload_pack_by_branch` never needing its own
  lock cycle at all, only `pull_by_branch`'s two Ortec entry points —
  `zcl_abapgit_ortec_fastpath=>pull_by_branch` for the resume-match case,
  `zcl_abapgit_ortec_porcelain=>pull_by_branch` for the fresh-fetch case —
  each owning exactly one lock/attempt unit).
- **C.** Pause Package D0 here without further design iteration; revisit
  once Michael has reviewed the full call-graph evidence directly.

No productive ABAP has been changed by any of this. `IMPLEMENTATION_AUTHORIZED`
remains `no` — the protocol/persistence review has not reached
`APPROVE`/`APPROVE_WITH_MINOR_REVISIONS`, and the performance DESIGN_GATE
has not yet run at all.

## Awaiting

Michael's choice of A, B, or C before continuing Package D0.

## Owner decision (2026-07-24)

Michael chose **Option A**, with a binding 7-point design decision:

1. The attempt/lock/publication boundary must follow the real production
   persistence call graph, not be anchored where final
   persistence/certification does not occur.
2. For the normal fresh-fetch path,
   `zcl_abapgit_ortec_porcelain=>pull_by_branch` has exactly one local
   persistence/publication unit: `BEGIN_ATTEMPT` → persist verified
   objects → `PERSIST_PULL_RESULT` → `CERTIFY_FETCHED_COMMIT` (same
   `attempt_id`) → final publication → orchestrator-owned `COMMIT`.
3. `attempt_id` is generated exactly once for this unit and threaded
   through to `PERSIST_PULL_RESULT`/`CERTIFY_FETCHED_COMMIT`;
   `CERTIFY_FETCHED_COMMIT` must not call `BEGIN_ATTEMPT` a second time
   within the same unit.
4. The repository lock protects exactly the local
   persistence/promotion/certification phase — exact acquire/release
   points, exception-safe release, no premature release before
   promotion/certification, no retention across unbounded HTTP wait.
5. `zcl_abapgit_ortec_fastpath=>upload_pack_by_branch` is not the owner of
   Publication Unit #2 and must not mint its own `attempt_id` or acquire
   a competing lock for it.
6. The resume-match path must be classified against current source, not
   proactively modeled as a second equivalent unit; no second unit may be
   invented without source evidence.
7. Existing invariants (no READY before verification, no certificate/
   branch pointer from a failed attempt, one `attempt_id` per real
   publication unit, separate IDs for genuine retries, stale-attempt
   rejection, Package C F/C unchanged, ORTEC-disabled behavior unchanged)
   remain binding.

Plus: `zcl_abapgit_ortec_porcelain=>pull_by_branch`,
`zcl_abapgit_ortec_fastpath=>persist_pull_result`/`certify_fetched_commit`,
the real `begin_attempt` caller, the lock methods used, and the affected
persistence/promotion methods must be explicit in D2 scope; 10 named
tests required (5 pre-existing + `fresh_pull_unit_atomic`,
`fresh_pull_fail_no_publish`, `lock_release_on_failure`,
`resume_reuses_attempt`, `resume_new_attempt_when_new`, the last two
finalized only after unambiguous source-based classification). A third
full protocol/persistence review round is not required for B-3 alone,
provided the Performance DESIGN_GATE finds no new protocol/persistence
inconsistency.

**Resolution applied:** the design
([variant_b_package_d_design.md](../logs/variant_b_package_d_design.md)
§9/§11/§15/§16/§19) and the review artifact
([variant_b_package_d_protocol_review.md](variant_b_package_d_protocol_review.md))
have been updated accordingly. Source-based classification of the
resume-match path (§9) found it requires **no second publication unit**:
`resume_decode` has zero interaction with `attempt_id`/`ZAOG_COMMIT_HIST`,
so it neither continues nor needs to reuse an existing attempt — it
remains exactly Unit #1 as already designed, always minting a fresh
`attempt_id`. `resume_reuses_attempt` is retained as an explicit
`NOT_APPLICABLE` placeholder with this evidence recorded per Michael's
"no second unit without source evidence" instruction; the other 9 tests
are added as directly applicable. **B-3 is RESOLVED.** This decision
track is now closed.
