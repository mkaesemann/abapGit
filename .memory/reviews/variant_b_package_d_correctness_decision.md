# Package D — D0 correctness review: decision checkpoint

```text
PACKET=COMPACT_HANDOFF_V1
TASK=D-REVIEW-CORRECTNESS-DECISION
BASELINE=29199f629773c676e0eaa2f3a006f5167d304ae8
STATUS=STOPPED_FOR_OWNER_DECISION
```

## Why this file exists

Per orchestrator gatekeeping: a `REVISE_AND_REVIEW_ONCE` verdict allows exactly
one automatic design-revision + re-review iteration. That one iteration has
now been used. The re-review still returned `REVISE_AND_REVIEW_ONCE` (a new
finding, DR-004). Per rule ("If still disagreement → write decision file →
STOP for Michael"), this checkpoint is written instead of looping a third
automatic re-review.

## Iteration history

1. **Review 1** (`ortec-abapgit-design-review`, Claude Sonnet 5): verdict
   `REVISE_AND_REVIEW_ONCE`. Findings: DR-001 (blocking — two
   non-interoperable `acquire_repo_lock`/`release_repo_lock` primitives,
   nested-acquisition/premature-dequeue risk), DR-002 (major — design
   incorrectly cited `cleanup_partial_session` as filtering `status='I'`;
   it actually filters the unrelated legacy `status='P'`), DR-003 (major —
   Phase 1.5's bulk-merge step must also sync `ct_tabix_by_index`/
   `ct_sha_idx` auxiliary lookup tables or silently fall back to per-object
   SQL). All three were independently re-verified against live source by
   the orchestrator (not just trusted) before being applied. **Fixed** in
   `.memory/logs/variant_b_package_d_design.md` §4/§5.1, §9/§11/§15.
2. **Review 2 (the one automatic re-review)**: verdict `REVISE_AND_REVIEW_ONCE`
   again. DR-001/002/003 confirmed resolved (0 remaining issues on those
   three). One **new** finding, **DR-004** (major, not blocking): the
   design's §9/§11 require a single early `begin_attempt` call (before any
   pack fetch) to obtain one `attempt_id` for `ZAOG_OBJ_STORE`/
   `ZAOG_FETCH_SESS`/`ZAOG_PACK_META` tagging, but the existing, unmodified
   `zcl_abapgit_ortec_fastpath=>certify_fetched_commit` also calls
   `begin_attempt` itself later in the same flow.
   `zcl_abapgit_ortec_mat_state=>begin_attempt` always mints a fresh UUID
   and unconditionally overwrites `ZAOG_COMMIT_HIST.attempt_id` on every
   call — so two different attempt IDs would exist for one real attempt,
   defeating §9's cross-table-join purpose (no data corruption, no
   have-eligibility impact — diagnostics/correlation only, hence "major"
   not "blocking"). **Fixed** directly by the orchestrator (not via a third
   subagent loop): `certify_fetched_commit` now takes `iv_attempt_id` as an
   input and no longer calls `begin_attempt` itself;
   `persist_pull_result` forwards the single early-acquired `attempt_id`
   to it; both are added to §15's file-scope list. The reviewer's one
   cosmetic/optional note (tighten §15's lock-wrapping wording to cover
   `upload_pack_by_branch`'s full body, not just the `pull_by_branch` call,
   so the thin/self-contained/recovery HTTP-fetch cascade stays inside the
   lock's span) was also applied.

## Current state of the design document

`.memory/logs/variant_b_package_d_design.md` now reflects the DR-001
through DR-004 fixes described above. This has **not** been re-submitted
for a third automated correctness review pass, per the one-iteration rule.

## Options for Michael

- **A.** Approve proceeding on the strength of the orchestrator's own
  direct source-verification of DR-004 (the same rigor already applied to
  DR-001/002/003 in review 1) without a third subagent re-review pass, and
  continue to the protocol/persistence review and performance design gate.
- **B.** Request one additional (manual, non-"automatic") correctness
  re-review pass specifically scoped to confirming only the DR-004 fix,
  before proceeding.
- **C.** Provide different direction on the DR-004 fix itself (e.g. prefer
  making `begin_attempt` idempotent per `repo_key`/`commit_sha1` — the
  reviewer's alternative "option (b)" — instead of threading `iv_attempt_id`
  into `certify_fetched_commit`).

No productive ABAP has been changed by any of this. `IMPLEMENTATION_AUTHORIZED`
remains `no` regardless of which option is chosen, since the protocol/
persistence review and performance design gate have not yet run.

## Awaiting

Michael's choice of A, B, or C before continuing Package D0.

## Owner decision (2026-07-24)

**Option A accepted.** Michael confirmed the DR-004 fix and issued a
binding design decision (recorded verbatim in
`.memory/logs/variant_b_package_d_design.md` §9 "Owner-confirmed binding
decision"): one `attempt_id` per real top-level attempt; `begin_attempt`
stays non-idempotent (a genuine retry/recovery attempt gets a new id, but
`begin_attempt` is never called twice within one attempt); the early id is
threaded through `persist_pull_result`/`certify_fetched_commit`; all four
tables (`ZAOG_OBJ_STORE`/`ZAOG_FETCH_SESS`/`ZAOG_PACK_META`/
`ZAOG_COMMIT_HIST`) receive the same id for the same attempt; stale/missing
id rejection at publication is unchanged. Five additional regression tests
were added to §16: `one_attempt_one_id`, `retry_gets_new_attempt`,
`certify_reuses_attempt`, `stale_attempt_rejected` (pre-existing),
`attempt_id_cross_table` — all ≤ 30 characters.

DR-004 is now checked into the protocol/persistence review as an explicit
checkpoint. `IMPLEMENTATION_AUTHORIZED` remains `no` until that review and
the performance DESIGN_GATE both complete with no blocking finding.

