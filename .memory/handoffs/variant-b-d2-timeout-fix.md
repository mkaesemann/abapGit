# Variant B D2 TIME_OUT fix — implementation handoff

```text
PACKET=COMPACT_HANDOFF_V1
TASK=D2-TIMEOUT-FIX-IMPLEMENTATION
BASELINE=2111b2887cc4fbf2ee481f753fd4af2c3e5085c4 (SYSTEM_NO_ROLL fix, IT8 retest pending)
STATUS=LOCAL_CHECKPOINT_COMPLETE_AWAITING_IT8_RETEST
```

## What changed

- `src/ortec/git/zcl_abapgit_ortec_cold_init.clas.abap`: added new
  **PUBLIC** `materialize_missing_batches(iv_url, iv_repo_key, it_sha1s)` —
  a behavior-preserving extraction of `materialize_tip_snapshot`'s adaptive
  row/byte-bounded `MATERIALIZE_BLOBS` batching loop (client init ->
  `take_next_batch` -> `materialize_batch` -> `calculate_next_batch_size`),
  WITHOUT the certification calls (`begin_attempt`/`verify_ready_blobs`/
  `finalize_snapshot`), which remain exclusively in `materialize_tip_
  snapshot`. `materialize_tip_snapshot` now calls the new method for its
  own loop instead of running it inline — zero external behavior change.
- `src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.abap`: rewrote
  `ensure_available`'s Step 2/3 (was `zcl_abapgit_git_transport=>
  upload_pack_by_commit(iv_deepen_level=1)` + `store_objects`) to call
  `materialize_missing_batches` once with the caller's own already-computed
  missing set (`lt_missing`), propagating any raised `zcx_abapgit_ortec_git`
  unchanged (not re-wrapped, so `mv_unsupported_capability` survives).
  External signature, Step 1/4 gates, and raise-on-still-missing contract
  are unchanged. Class/method ABAP Doc updated to narrow `it_sha1s`'
  documented semantics to blob SHA1s and to explain `iv_commit` is retained
  for interface stability only.
- `src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.xml`: added
  `WITH_UNIT_TESTS = X` (new test include).
- NEW `src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.testclasses.abap`:
  9 tests (`topup_narrows_to_blobs`, `no_fetch_without_url`, `no_fetch_
  when_opt_in_off` — ported gate tests; `unexpected_extra_ignored`,
  `attempt_cleanup_preserved`, `mostly_shared_cold_branch` — new; `missing_
  after_topup_raises`, `no_repo_wide_topup`, `retry_is_bounded` —
  documented `NOT_APPLICABLE` placeholders, same pattern as this project's
  own established `resume_reuses_attempt` precedent).
- `src/ortec/git/zcl_abapgit_ortec_cold_init.clas.testclasses.abap`: added
  `materialize_missing_empty` (the one HTTP-free branch of the new public
  method; the non-empty branch's underlying algorithm is already covered
  by pre-existing `take_batch_respects_limit`/`oversize_action_*`/
  `adaptive_*`/`deduplicate_keeps_order` tests).
- `src/ortec/git/zcl_abapgit_ortec_fetch_req.clas.testclasses.abap`: added
  `capability_intersection` (the one previously-untested `MATERIALIZE_
  BLOBS` capability-fallback combination — `allow-tip-sha1-in-want` only).

## Root cause fixed

`ensure_available` (called by Stage-By-Filter's `build_files_from_rows` and
`walk_prep`'s `topup_missing_blobs`) used to fetch a target commit's ENTIRE
reachable object graph via `upload_pack_by_commit(deepen=1)` to resolve a
caller-supplied set of specific missing blob SHA1s — measured live on IT8:
162,919 objects fetched for one incident occurrence, 132,963 of them
unresolved deltas, causing the confirmed TIME_OUT. The fix scopes the fetch
to exactly the caller's own missing SHA1 set via the existing, already-
SAP-validated (Package B) adaptive `MATERIALIZE_BLOBS` batching primitive.

## Gates passed

```text
PROTOCOL_PERSISTENCE_REVIEW=APPROVE_WITH_MINOR_REVISIONS (conditions applied)
PERFORMANCE_DESIGN_GATE=APPROVE_WITH_MINOR_REVISIONS (conditions applied)
PERFORMANCE_IMPLEMENTATION_AUDIT=PASS
REGRESSION=PASS_WITH_FINDINGS (static-only; no live SAP runner available)
```

See:
[.memory/logs/variant_b_d2_timeout_fix_design.md](.memory/logs/variant_b_d2_timeout_fix_design.md),
[.memory/reviews/variant_b_d2_timeout_protocol_review.md](.memory/reviews/variant_b_d2_timeout_protocol_review.md),
[.memory/reviews/performance_design_variant_b_d2_timeout.md](.memory/reviews/performance_design_variant_b_d2_timeout.md),
[.memory/logs/performance_scan_variant_b_d2_timeout.md](.memory/logs/performance_scan_variant_b_d2_timeout.md),
[.memory/logs/performance_audit_variant_b_d2_timeout.md](.memory/logs/performance_audit_variant_b_d2_timeout.md),
[.memory/logs/regression_variant_b_d2_timeout.md](.memory/logs/regression_variant_b_d2_timeout.md).

## What is NOT done yet

- Live IT8 import/retest of this checkpoint (together with the still-
  pending `2111b288` SYSTEM_NO_ROLL fix) — see design §15 for the exact
  reproduction sequence. Do not mark the incident resolved until that
  sequence succeeds with measured evidence.
- `.memory/state.md` intentionally NOT updated (per run-brief instruction:
  do not write state until a gate-clean, IT8-validated checkpoint exists).

## Preserved invariants (verified)

```text
Package C F/C certification invariant           - untouched (no file in scope)
D1 bulk external delta-base resolution           - untouched (no file in scope)
D2 staged visibility and attempt isolation        - untouched (no file in scope)
No uncertified haves                              - MATERIALIZE_BLOBS never negotiates haves
No deepen/shallow correctness strategy            - MATERIALIZE_BLOBS never emits either
No per-object SQL or HTTP                         - confirmed by all 3 independent reviews
K-not-N incremental scaling                       - confirmed (§14 of design, performance audit)
Row and byte bounds                               - inherited unmodified from cold_init
Oversized-object behavior                         - inherited unmodified (solo-object raise)
ORTEC-disabled standard behavior                  - unaffected (no src/git/** file touched)
SYSTEM_NO_ROLL fix (commit 2111b288)              - untouched, confirmed via git diff --stat
```
## SAP validation closeout

```text
STATUS=SAP_VALIDATED_RESOLVED
FIX=bound ENSURE_AVAILABLE remote top-up to caller missing SHA set through adaptive MATERIALIZE_BLOBS batching
LIVE_RESULT=not reproduced after fix
PACKAGE_D_D2_VALIDATED_HEAD=733bb30799886ef8659be7e293b82c3ccfcebbdd
```

Owner IT8 retest confirmed `TIME_OUT_REPRODUCED=NO` together with
`SYSTEM_NO_ROLL_REPRODUCED=NO` on the combined checkpoint. See
[.memory/incidents/variant_b_d2_it8_system_no_roll_timeout.md](.memory/incidents/variant_b_d2_it8_system_no_roll_timeout.md)
for the full incident record and
[.memory/incidents/variant_b_d2_sat_warm_to_cold_o4h8794.md](.memory/incidents/variant_b_d2_sat_warm_to_cold_o4h8794.md)
for the follow-up SAT trace confirming zero measurable cost from this fix's
code path on the warm case.