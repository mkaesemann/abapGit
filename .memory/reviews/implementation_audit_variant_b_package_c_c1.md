# Variant B Package C — C1 Implementation Audit (VB-C-C1-AUDIT)

Mode: `IMPLEMENTATION_AUDIT`
Baseline: `cf3d5cb87ca4eec4e68b7975df6fc127a8c5abc2` (Package B SAP_VALIDATED_COMPLETE)
Approved design: `.memory/logs/variant_b_package_c_design.md` (`APPROVED_WITH_RESOLVED_REVISIONS`, `C1_AUTHORIZED`)
Correctness review: `.memory/reviews/variant_b_package_c_correctness_review.md` (`APPROVE_WITH_MINOR_REVISIONS`)
Protocol review: `.memory/reviews/variant_b_package_c_protocol_review.md` (`APPROVE_WITH_MINOR_REVISIONS`, F1 required-fix)
Performance design gate: `.memory/reviews/performance_design_variant_b_package_c.md` (`APPROVE_WITH_MINOR_REVISIONS`)
Prior static scan: `.memory/reviews/performance_scan_variant_b_package_c_c1.md` (`CLEAN`, 0 blocking)

Evidence basis: direct re-read of current workspace source (not the static
scan's paraphrase); scan findings independently verified against exact
source lines cited below. No live SAT/ST05 trace or synthetic large-scale
fixture was executed this session (see "Unexecuted scenarios").

## Scope compliance check

`git diff <baseline> --stat -- src/` + `git status --porcelain -- src/`
confirm the change set is exactly:

- `src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap` — modified, 20
  insertions / 18 deletions, confined to the `upload_pack` have-resolution
  block (see AC-5).
- `src/ortec/git/zcl_abapgit_ortec_have_policy.clas.abap` — new.
- `src/ortec/git/zcl_abapgit_ortec_have_policy.clas.testclasses.abap` — new.
- `src/ortec/git/zcl_abapgit_ortec_have_policy.clas.xml` — new.

No other `src/**` file is touched. Matches the declared C1 scope exactly
(no orchestrator wiring — `zcl_abapgit_ortec_porcelain` is unmodified, `grep`
of `have_policy` across `src/ortec/` confirms `classify_operation`/
`try_backfill_target` are referenced only from the new class's own
testclasses, never from `zcl_abapgit_ortec_porcelain` or elsewhere).

## Per-AC verdicts

### AC-1 — `classify_operation` performs no write/COMMIT/HTTP — **PASS**

`zcl_abapgit_ortec_have_policy.clas.abap`, `METHOD classify_operation`
(lines ~136–157): body is exactly — initial-parameter guard/RETURN, one
`zcl_abapgit_ortec_mat_state=>get_state` call (documented `SELECT SINGLE`,
O(1) keyed, no write), an `IF`/`RETURN` on `snap_state = complete`, then one
`get_certified_haves` call (bulk `SELECT`, no write) and an `IF`/`RETURN`.
No `INSERT`/`UPDATE`/`MODIFY`/`DELETE`, no `COMMIT WORK`, no HTTP client
usage, no call to `try_backfill_target` or any object-store/HTTP method
anywhere in the method body. Closes design DR-006 as specified.

### AC-2 — `try_backfill_target` is the only writer/committer; never called from `classify_operation` — **PASS**

Grepped `COMMIT WORK` and write-capable calls (`begin_attempt`,
`mark_graph_complete`, `mark_full_complete`, `publish_snapshot_complete`)
across `zcl_abapgit_ortec_have_policy.clas.abap`: all four appear exactly
once, all inside `METHOD try_backfill_target` (lines ~178–224); the single
`COMMIT WORK` (line ~220) is also only in this method.
`classify_operation`'s body (AC-1) contains zero references to
`try_backfill_target`. The two methods are fully decoupled — confirmed by
direct read, not by docstring trust.

### AC-3 — `get_certified_haves` only returns `HIST_LEVEL = FULL_COMPLETE` — **PASS**

`METHOD get_certified_haves`:
```
SELECT commit_sha1, updated_at
  FROM zaog_commit_hist
  INTO TABLE @lt_candidates
  WHERE repo_key   = @iv_repo_key
    AND hist_level = @zcl_abapgit_ortec_mat_state=>cs_hist_level-full_complete.
```
Single predicate on `hist_level`, bound to the `full_complete` constant only
— no `IN ('G','F')`, no `OR`. Matches protocol review finding F1's required
fix. Confirmed by test `graph_only_have_ineligible` (certifies a
`hist_level = GRAPH_COMPLETE`-only commit via `iv_full = abap_false` and
asserts `get_certified_haves` returns empty) and `full_certified_have_eligible`
(FULL_COMPLETE commit is returned). Both tests read as written, not just
present by name.

### AC-4 — `try_backfill_target`'s only caught exception is `zcx_abapgit_ortec_git` from `verify_tree_closure` — **PASS**

`METHOD try_backfill_target` contains exactly one `TRY...ENDTRY` block,
wrapping only the `zcl_abapgit_ortec_obj_store=>verify_tree_closure(...)`
call, `CATCH zcx_abapgit_ortec_git.` → `RETURN.` (matches protocol review F2
and design §6 step 2 exactly). Every other call in the method —
`zcl_abapgit_ortec_obj_store=>exists` (no `RAISING` in its signature, cannot
raise), `mat_state=>begin_attempt`, `mark_graph_complete`,
`get_tip_blob_sha1s`, `get_missing_sha1s` (no `RAISING`), `mark_full_complete`,
`publish_snapshot_complete` — is called unwrapped; all `RAISING`-declared
ones (`begin_attempt`, `mark_graph_complete`, `mark_full_complete`,
`publish_snapshot_complete`, and `verify_tree_closure`/`get_tip_blob_sha1s`
from the obj_store class) are declared `RAISING zcx_abapgit_ortec_git` only
(verified by reading both classes' public-section signatures) — consistent
with `try_backfill_target`'s own `RAISING zcx_abapgit_ortec_git` signature,
so no exception type mismatch and no silent swallow of any other step's
failure.

### AC-5 — `upload_pack`'s migrated have-resolution: same two gating modes, empty (not exception) on unresolved repo key — **PASS**

`zcl_abapgit_ortec_fastpath.clas.abap`, `METHOD upload_pack` (~line 1120):
```
IF iv_mode = zcl_abapgit_ortec_fetch_req=>cs_fetch_mode-incremental_thin
    OR iv_mode = zcl_abapgit_ortec_fetch_req=>cs_fetch_mode-incremental_self_contained.
  lv_have_repo_key = zcl_abapgit_ortec_repo_state=>get_repo_key_for_url( iv_url ).
  IF lv_have_repo_key IS NOT INITIAL.
    lt_ortec_haves = zcl_abapgit_ortec_have_policy=>get_certified_haves(
      iv_repo_key    = lv_have_repo_key
      it_want_hashes = it_hashes ).
  ENDIF.
ENDIF.
```
The outer `IF` condition is byte-for-byte unchanged from the pre-migration
source (`git diff` shows the `IF`/`ENDIF` lines outside the changed hunk) —
same two-mode gate. `lt_ortec_haves` is `DATA`-declared (defaults to
initial/empty table) and is left untouched (empty) whenever
`get_repo_key_for_url` returns space — no exception path exists here.
Verified this is the **same observable behavior** as the pre-migration call:
read `zcl_abapgit_ortec_fetch_neg=>get_verified_have_commits` (the method
this replaces) — it also does `lv_repo_key = get_repo_key_for_url(...). IF
lv_repo_key IS INITIAL. RETURN. ENDIF.`, i.e. it too silently returns an
empty have-set (no exception) for a URL with no resolvable repo state.
`get_repo_key_for_url` itself (`zcl_abapgit_ortec_repo_state.clas.abap`)
only ever `RETURN`s early (on a hash failure) or leaves `rv_key` unset via a
`SELECT SINGLE` `sy-subrc <> 0`, never raises — confirmed for both the old
and new call paths.

### AC-6 — `try_backfill_target` never downgrades an existing certificate; safe to call repeatedly — **PASS**

Relies on already-established, unmodified `zcl_abapgit_ortec_mat_state`
contracts (re-verified by reading the current signatures/docstrings, not
re-trusting Package B's prior audit alone):
- `begin_attempt`: "Never downgrades an existing hist_level"; sets
  `snap_state = PENDING` only when currently `NONE`/`INVALID` — leaves
  `COMPLETE`/`PENDING` untouched.
- `mark_graph_complete`: "No-op-safe (idempotent) if already GRAPH_COMPLETE
  or FULL_COMPLETE."
- `mark_full_complete`: "Raises unless current hist_level is already
  GRAPH_COMPLETE (or FULL_COMPLETE, idempotent)."
- `publish_snapshot_complete`: raises on stale attempt ID / hist_level
  precondition, never silently downgrades `snap_state`.

`try_backfill_target`'s own body issues a fresh `begin_attempt` on every
call (new `attempt_id` each time) but never writes any lower-priority state
than what is already present — a repeat call on an already-`FULL_COMPLETE`/
`snap_state=COMPLETE` commit re-verifies the same K-bounded tree/blob set
and re-affirms (never regresses) the certificate. Confirmed empirically by
test `backfill_repeat_idempotent`: calls `try_backfill_target` twice on the
same commit, asserts both calls return `abap_true` and the final state is
still `hist_level = FULL_COMPLETE` / `snap_state = COMPLETE`. No test
exercises "repeat call on a commit that later became certified via the real
incremental fetch path" but that path is disjoint (Package C's future
`try_backfill_target` orchestrator wiring is explicitly out of C1 scope per
the design's own §1/§3a note that C1 does not wire this into the
orchestrator), so it is not exercisable yet and not required for this
audit's AC-6 scope (single-class idempotency).

## Test suite verification

`zcl_abapgit_ortec_have_policy.clas.testclasses.abap`: counted 16
`FOR TESTING` methods (8 `get_certified_haves`-focused, 4
`classify_operation`-focused, 4 `try_backfill_target`-focused), matching the
task's declared count. Each test was read in full (not just enumerated by
name):
- `full_certified_have_eligible` / `graph_only_have_ineligible` /
  `uncertified_have_ineligible` directly exercise AC-3's `FULL_COMPLETE`-only
  filter across all three `hist_level` states relevant to it.
- `classify_no_side_effects` directly exercises AC-1 by calling
  `classify_operation` twice and asserting `sy-subrc = 4` (no row was ever
  created in `ZAOG_COMMIT_HIST` for that commit) — a genuine negative-write
  assertion, not just a classification-result check.
- `backfill_incomplete_no_publish` / `backfill_repeat_idempotent` directly
  exercise AC-4/AC-6's partial-failure and idempotency behavior.
- `cleanup_repo`'s `setup`/`teardown` correctly avoid the documented
  `DELETE + ROLLBACK WORK` pitfall (repo memory) by explicitly committing
  the cleanup delete, since `try_backfill_target` issues its own
  `COMMIT WORK`.

No test was found to assert something the source does not actually do; no
fabricated evidence detected (per the documented `ortec-abapgit-performance-review`
hallucination risk in user memory — this audit re-read every cited line
directly rather than trusting the design/scan artifacts' paraphrase).

## Performance re-verification (evidence, not re-litigation of DESIGN_GATE)

- `get_certified_haves`: one bulk `SELECT` scoped by `repo_key` (leading
  non-MANDT primary-key column of `ZAOG_COMMIT_HIST` — confirmed against
  the protocol review's DD03P key trace), capped ABAP-side at
  `iv_max_haves` inside the `LOOP`/`EXIT` (no unbounded accumulation). No
  per-candidate SQL.
- `classify_operation`: one `SELECT SINGLE` (`get_state`) + at most one
  bulk `get_certified_haves` call (`iv_max_haves = 1`). O(1) DB round trips,
  independent of repository size N.
- `try_backfill_target`: `exists` (existence-only, no payload), one
  `begin_attempt`/`mark_graph_complete`/`mark_full_complete`/
  `publish_snapshot_complete` (each O(1) keyed writes), `verify_tree_closure`
  + `get_tip_blob_sha1s` (Package B B1/B2 frontier-BFS, bounded by K = this
  commit's own reachable tree/blob set, not N), `get_missing_sha1s`
  (chunked bulk presence check). One `COMMIT WORK` per call. No SQL or HTTP
  loop bounded by repository-wide object count anywhere in this class.
- `upload_pack`'s migrated block: one `get_repo_key_for_url` (single keyed
  SELECT) + one `get_certified_haves` bulk SELECT, replacing a costlier
  legacy path (full ready-commit payload load + in-memory ancestor BFS,
  per the performance design gate's own verification) — a net improvement,
  not a regression.

Matches `.memory/reviews/performance_scan_variant_b_package_c_c1.md`'s
`CLEAN` verdict; independently re-verified against exact source lines
rather than accepted at face value.

## Mandatory scale scenarios

| Scenario | Status |
|---|---|
| Small (1–20 objects) | Exercised via unit tests (16 tests, single/few-commit fixtures) — **executed at unit-test scale only, not run this session** (no test-runner invocation performed; source-level trace confirms correct call shape) |
| Medium (≥5,000 mixed objects, multiple batches) | **Not executed** — no synthetic medium fixture run this session |
| Large (≥40,000 objects/paths, cold+warm cache) | **Not executed** |
| Shared branches (95–98% shared) | Not applicable to this slice (no branch-sharing logic touched by C1) |
| Incremental store (~100 affected objects / ~1,000,000 stored keys) | **Not executed** — static analysis confirms O(H)/O(K) bound (H = this repo's own certified-row count, K = target commit's own graph/blob set), independent of total stored-key count, but this is a static estimate, not a measured result |
| Interrupted attempt + retry | Exercised by `backfill_repeat_idempotent` and `backfill_incomplete_no_publish` at unit-test scope only; no live interrupted-HTTP-attempt scenario run |

No scale scenario was converted from a static estimate into a measured
result this session. This audit's PASS verdict rests on source-level
call-shape verification (SQL/HTTP shape, batching, exception scoping) plus
existing unit-test source review, not on live trace or large-fixture
execution.

## Overall verdict

**PASS**

All six acceptance criteria verified directly against current source with
exact line-level evidence; no blocking or major finding. Scope compliance
confirmed (no out-of-scope `src/**` change, no premature orchestrator
wiring). The only residual gaps are the same class of non-blocking,
already-disclosed items carried from the DESIGN_GATE (M1/M2/M3 — WARM_UNCHANGED
single-object seed step, optional `EXISTS`/`UP TO 1 ROWS` short-circuit,
pre-existing walk-failure retry documentation) and the unexecuted
medium/large/incremental-scale measured scenarios, none of which are C1
implementation defects — they are C2 orchestrator-wiring and
measurement-phase concerns.
