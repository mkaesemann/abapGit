# Variant B Package C — design (Slices 5+6 combined)

Status: `APPROVED_WITH_RESOLVED_REVISIONS`

C0 gate closure:

- correctness: `APPROVE_WITH_MINOR_REVISIONS`
- protocol/persistence: `APPROVE_WITH_MINOR_REVISIONS`
- performance DESIGN_GATE: `APPROVE_WITH_MINOR_REVISIONS`
- required findings: resolved
- remaining findings: non-blocking documentation or optional optimization only
- implementation authorization: `C1_AUTHORIZED`
  
Baseline: HEAD `cf3d5cb87ca4eec4e68b7975df6fc127a8c5abc2` (Package B SAP_VALIDATED_COMPLETE)

## 0. Current-source finding that drives this design (verified by direct read)

`zcl_abapgit_ortec_fastpath=>persist_pull_result` (the only productive writer of
`ZAOG_COMMIT_HIST` today, called from `zcl_abapgit_ortec_porcelain=>pull_by_branch`)
does a raw `INSERT zaog_commit_hist FROM ls_hist` that never sets `HIST_LEVEL`/
`SNAP_STATE` (left space). `zcl_abapgit_ortec_mat_state=>is_graph_have_eligible`
requires `HIST_LEVEL IN ('G','F')`. **Every commit produced by the live
incremental pull path is therefore certificate-ineligible today** —
`zcl_abapgit_ortec_fetch_neg=>get_verified_have_commits` always returns empty in
production, so every fetch after the first is effectively haves-less. Only test
code and the not-yet-wired `zcl_abapgit_ortec_cold_init` (Package B) call
`begin_attempt`/`mark_graph_complete`/`mark_full_complete`/
`publish_snapshot_complete`. This is Slice 6's primary defect to fix, not a new
feature.

Second finding: `zcl_abapgit_git_porcelain=>pull_by_branch`/`pull_by_commit`
route unconditionally to `zcl_abapgit_ortec_porcelain=>pull_by_branch`/
`pull_by_commit` for any Ortec-active repo (no `TRY`, so Ortec failures already
propagate uncaught — no silent standard fallback exists today at this layer,
confirmed intentional per the matching comment in
`zcl_abapgit_git_transport=>upload_pack_by_branch`). There is no cold-branch
routing to `zcl_abapgit_ortec_cold_init` anywhere in the live call chain; a
first pull of a large repo goes through `INCREMENTAL_THIN`/
`INCREMENTAL_SELF_CONTAINED` with an empty have-set, i.e. an unfiltered full
clone with all blobs.

**Correction after C0 correctness review (DR-001/DR-002, resolved):** an
earlier draft of this document wrongly claimed
`zcl_abapgit_ortec_fastpath=>pull_by_branch` was dead code. It is live:
`zcl_abapgit_ortec_porcelain=>pull_by_branch` → (unconditionally)
`zcl_abapgit_git_transport=>upload_pack_by_branch` → (Ortec-active) →
`zcl_abapgit_ortec_fastpath=>upload_pack_by_branch`, whose **first statement**
calls its own private `pull_by_branch`, which still gates its local-store
reconstitution on raw `ls_state-fetch_commit` equality (`ZAOG_REPO_STATE`,
uncertified). This method is **shared infrastructure**: `upload_pack_by_branch`
is also called directly by `src/git/zcl_abapgit_git_commit.clas.abap` and
`src/repo/stage/zcl_abapgit_merge.clas.abap`, both outside Package C's scope
(commit-view and merge-preview flows, not branch pull/switch). Package C
therefore does **not** modify or bypass this shared method — doing so would
change behavior for callers outside this package's mandate. It is confirmed
safe, not silently-corrupting (`get_reachable_objects` raises on genuine
incompleteness via `get_objects( iv_bulk_fetch = abap_true )`, and the caller
invalidates the tip and falls through to a real fetch), so keeping it
unmodified does not introduce a correctness hazard. See §2/§4/§12 for how this
is now explicitly budgeted and disclosed rather than silently mischaracterized,
and §13 for the corrected migration-map classification.

## 1. Orchestration owner

`zcl_abapgit_ortec_porcelain` remains the single productive Package C
orchestration owner (already the entry point invoked by
`zcl_abapgit_git_porcelain=>pull_by_branch`/`pull_by_commit` for every
Ortec-active repo). No new porcelain-level class is introduced.

A new pure/bulk-SQL-only class, `zcl_abapgit_ortec_have_policy` (29 chars),
owns classification and certified-have selection (C1 scope) so it is
independently unit-testable without HTTP:

- `classify_operation( iv_repo_key, iv_target_commit ) RETURNING rv_class` —
  pure certificate-only decision logic: no HTTP, no `COMMIT WORK`, no write of
  any kind, no raw `FETCH_COMMIT` read, no object-graph traversal. Returns one
  of `cs_op_class-warm_unchanged`, `cs_op_class-incremental_update`,
  `cs_op_class-cold_branch`, using only §3's two certificate reads.
- `get_certified_haves( iv_repo_key, it_want_hashes, iv_max_haves DEFAULT 50 )
  RETURNING rt_haves` — single bulk read, exclude/dedupe/order/cap.
- `try_backfill_target( iv_repo_key, iv_target_commit ) RETURNING rv_certified`
  — the §3a opportunistic local backfill, refactored per C0 review DR-006 into
  its own explicitly named operation (closes DR-006: `classify_operation` no
  longer has a hidden write/commit side effect). Local-only, zero HTTP,
  bounded by K (the target commit's own graph/blob set), idempotent, and
  invoked by the caller at most once per orchestration decision — never from
  inside `classify_operation` itself. See §3a for the exact steps and
  `rv_certified` semantics.

`zcl_abapgit_ortec_porcelain=>pull_by_branch`/`pull_by_commit` call
`have_policy=>classify_operation` before doing any HTTP work. If the result is
`cold_branch`, the caller invokes `have_policy=>try_backfill_target` exactly
once; if it returns `abap_true`, the caller re-evaluates
`classify_operation` (which will now observe `snap_state = COMPLETE` and
return `warm_unchanged`) instead of re-implementing the WARM_UNCHANGED
decision inline. The caller then routes to one of three paths (§4).
`zcl_abapgit_ortec_fetch_neg=>get_verified_have_commits`'s only remaining
caller (`zcl_abapgit_ortec_fastpath=>upload_pack`) is switched to
`have_policy=>get_certified_haves` (C1 change, one call-site edit).

## 2. Branch-tip resolution and validation

`pull_by_branch`: resolve the advertised remote tip via
`zcl_abapgit_git_transport=>branches( iv_url )->find_by_name( iv_branch_name
)-sha1` (existing public method, previously used for this exact purpose by the
still-live `zcl_abapgit_ortec_fastpath=>pull_by_branch`, see §0 correction).
This is one lightweight `info/refs` GET, independent of and prior to any
upload-pack POST. On failure to resolve (`zcx_abapgit_exception`), Package C
does not swallow it — it propagates (matches "no silent fallback" precedent
already established at this layer).

**Disclosed, not eliminated, redundant GET (DR-002 disposition):** when
classification yields `INCREMENTAL_UPDATE` or `COLD_BRANCH`, the call still
reaches `zcl_abapgit_git_transport=>upload_pack_by_branch` →
`zcl_abapgit_ortec_fastpath=>upload_pack_by_branch`, whose first statement is
the shared, unmodified `pull_by_branch` (§0) — which performs its **own**
second `branches()` GET and its own local `fetch_commit`-equality check before
falling through to the thin/self-contained/recovery cascade. This is
pre-existing, shared-infrastructure behavior (also used by
`zcl_abapgit_git_commit`/`zcl_abapgit_merge`) that Package C does not modify
(out of scope — modifying it would change behavior for those other callers).
It is safe (raises-and-falls-through on genuine incompleteness, never silently
trusts a wrong result) and is now explicitly counted in §12's budget instead of
being silently absent from it.

`pull_by_commit`: the target commit is already given by the caller
(`iv_commit_hash`); no separate tip resolution is needed — classification reads
that exact SHA1's certificate directly.

If the remote's advertised tip cannot be resolved, or the repo key cannot be
resolved (`zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url`),
Package C treats this as `cold_branch` only when `has_state`/certificate lookup
is genuinely empty; a transient resolution failure raises, it is never silently
mapped to a fetch mode.

## 3. Warm / cold / incremental classification rules

`classify_operation` reads exactly one row via
`zcl_abapgit_ortec_mat_state=>get_state( iv_repo_key, iv_target_commit )`
(O(1) keyed read, no traversal):

1. `snap_state = COMPLETE` → `WARM_UNCHANGED`. By construction (§6, and
   confirmed for Package B's own `materialize_tip_snapshot`), every commit
   that reaches `snap_state = COMPLETE` was also marked `hist_level =
   FULL_COMPLETE` first — every blob reachable from its tree is verified
   present, not just the graph. No branch-pointer comparison, no
   `FETCH_COMMIT` read.
2. Else, if `have_policy=>get_certified_haves( iv_repo_key, it_want_hashes =
   VALUE #( ( iv_target_commit ) ), iv_max_haves = 1 )` returns at least one
   candidate for this repo → `INCREMENTAL_UPDATE`. (Bulk read against
   `ZAOG_COMMIT_HIST`, not a call `is_graph_have_eligible` per candidate — the
   same single query used for have-selection also answers "does this repo have
   any certified ancestor at all".)
3. Else → `COLD_BRANCH`. The caller (not `classify_operation` itself) may
   then invoke `try_backfill_target` (§3a) exactly once and, only if it
   returns `abap_true`, call `classify_operation` again before finally
   committing to `COLD_BRANCH`.

This is a commit-identity certificate check, never a `ZAOG_REPO_STATE`
`fetch_commit`/branch-pointer comparison, and never an object-presence probe.
`classify_operation` itself performs no write, no `COMMIT WORK`, and no HTTP
(closes DR-006 — see "API boundary refinement" below).

## 3a. Opportunistic local backfill — `try_backfill_target` (closes DR-003 — migration-day "cold storm"; API boundary refined per DR-006)

Every commit recorded by today's live (pre-Package-C) `persist_pull_result`
has `hist_level` space — under rules 1/2 above, every pre-existing,
already-fully-present repo would misclassify as `COLD_BRANCH` on its very
first post-deployment pull, triggering an unneeded `INITIAL_BRANCH_BLOBLESS`
fetch for objects already stored locally. When `classify_operation` returns
`COLD_BRANCH`, the caller invokes `try_backfill_target` — a separate, explicitly
named public method, not a hidden branch inside `classify_operation` — to make
one local-only, HTTP-free attempt to certify the target commit in place,
reusing exactly §6's lifecycle and Package B's already-approved bulk APIs
(bounded by K, the commit's own reachable graph/blob size — never N):

1. `zcl_abapgit_ortec_obj_store=>exists( iv_repo_key, iv_target_commit )` — if
   `abap_false` (genuinely never seen before), skip straight to `COLD_BRANCH`.
2. If `abap_true`: `lv_attempt = mat_state=>begin_attempt(...)`,
   `obj_store=>verify_tree_closure(...)`. On failure (caught, per §6's
   explicit `TRY`/`CATCH zcx_abapgit_ortec_git`): fall through to
   `COLD_BRANCH` (the local copy really is incomplete).
3. On success: `mark_graph_complete`, then
   `get_tip_blob_sha1s`/`get_missing_sha1s`; if no blobs are missing:
   `mark_full_complete` + `publish_snapshot_complete` + one `COMMIT WORK`
   (identical shape to §6, applied once as a backfill instead of after a
   fetch), and return `rv_certified = abap_true` — the caller then
   re-evaluates `classify_operation`, which observes `snap_state = COMPLETE`
   and returns `WARM_UNCHANGED`; `try_backfill_target` never returns a
   classification itself. If blobs are missing: no publish (§6 partial-state
   rule), return `rv_certified = abap_false` — the caller falls through to
   `COLD_BRANCH`; the already-present commit/tree data still remains as
   delta-base context for whatever fetch mode is chosen next.

`try_backfill_target` performs zero HTTP calls and reuses only
already-approved, bulk, no-per-object-SQL Package B APIs, so it introduces no
new SQL/HTTP shape (§12) — it only changes which of two already-designed paths
(§6's lifecycle vs. §4 COLD_BRANCH) an already-locally-complete legacy repo
takes on its first post-deployment pull. It is idempotent (a second call for
an already-`WARM_UNCHANGED` or already-partially-certified target either
no-ops via `mat_state`'s own idempotent guards or safely re-verifies the same
K-bounded closure) and is invoked by the orchestrator at most once per
classification decision (no retry loop).

**API boundary refinement (closes DR-006):** the C0 correctness re-review
noted that §1's "pure certificate read, no want-side effects" description of
`classify_operation` was inconsistent with §3a's original design, which
performed the backfill write+commit *inside* `classify_operation`. This is
resolved by extracting the backfill into the standalone `try_backfill_target`
method described above: `classify_operation` is now genuinely pure (no write,
no commit, no HTTP, under all inputs), and the one-time local certification
side effect is owned exclusively by `try_backfill_target`, called explicitly
by the orchestrator. This is a C1 API-boundary refinement only — the approved
behavior (one bounded, local, idempotent backfill attempt before concluding
COLD_BRANCH) is unchanged.

## 4. Mode routing per classification

### WARM_UNCHANGED

No HTTP upload-pack call. Seed `it_objects` with exactly one bounded,
single-key read, `zcl_abapgit_ortec_obj_store=>get_object( iv_repo_key,
iv_target_commit )` (not a repository- or tree-wide read — closes performance
review M1), then reconstruct `rs_result` by walking the local store from
`iv_target_commit` via the existing `zcl_abapgit_ortec_porcelain=>pull`/
`walk`/`walk_tree` machinery (same code already used for the HTTP-fetched
case — no new local-read code; `walk`'s existing object-store fallback resolves
every tree/blob it needs one bounded batch at a time). No write to
`ZAOG_COMMIT_HIST`/`ZAOG_REPO_STATE` (nothing changed, nothing to certify
again). Only `ZAOG_REPO_STATE.curr_commit` bookkeeping may be refreshed via the
existing `update_after_fetch` call for UI display consistency — this is not a
completeness trust write.

### INCREMENTAL_UPDATE

Unchanged wire cascade (already productive, Slice 2C):
`INCREMENTAL_THIN` → on failure `INCREMENTAL_SELF_CONTAINED` → on
`is_retry_without_haves` from either, at most one `RECOVERY_BRANCH_FULL`. Haves
for the first two tiers now come from `have_policy=>get_certified_haves`
instead of `get_verified_have_commits`'s ancestor-BFS (§7). `RECOVERY_BRANCH_FULL`
still sends no haves (unchanged). No progressive deepen, no `deepen`/`shallow`
lines (already true today — `upload_pack`'s migrated modes never emit them).

On success, replace `persist_pull_result`'s raw `ZAOG_COMMIT_HIST` INSERT with
the certification lifecycle (§6) before the single `COMMIT WORK`.

### COLD_BRANCH

1. `zcl_abapgit_ortec_cold_init=>acquire_blobless_graph( iv_url, iv_repo_key,
   iv_target_commit )` — requires advertised `filter`; raises
   `zcx_abapgit_ortec_git` typed on missing capability or any failure. Package C
   does not catch-and-degrade to an unfiltered fetch (no "separate competing
   cold-branch implementation", no unfiltered fallback) — the exception
   propagates to the same uncaught boundary `zcl_abapgit_git_porcelain` already
   uses for every other Ortec failure today.
2. On success (self-committing, per Package B's own contract):
   `zcl_abapgit_ortec_cold_init=>materialize_tip_snapshot( iv_url, iv_repo_key,
   iv_branch_name, iv_target_commit )` — self-committing on success, publishes
   `SNAPSHOT_COMPLETE` via `mat_state=>publish_snapshot_complete` internally.
3. Reconstruct `rs_result` from the now-fully-materialized local store (same
   `pull`/`walk`/`walk_tree` machinery as WARM_UNCHANGED — no new code).

No haves are sent (`INITIAL_BRANCH_BLOBLESS` never negotiates haves, by
Package B contract). No `deepen`/`shallow`.

## 5. Certified-have policy (Slice 6 core)

`get_certified_haves`:

```
SELECT commit_sha1, updated_at
  FROM zaog_commit_hist
  INTO TABLE @DATA(lt_candidates)
  WHERE repo_key   = @iv_repo_key
    AND hist_level = 'F'.
```

**Fix applied after C0 protocol review (F1):** filters `hist_level = 'F'`
(`FULL_COMPLETE`) only, not `IN ('G','F')`. A `GRAPH_COMPLETE`-only commit
(e.g. an interrupted cold-init that finished B1 but never B2+B3) has trees but
not necessarily blobs present — offering it as a wire-protocol "have" would be
a dangling-delta-base hazard for a thin/self-contained response the server
builds against it. `hist_level = 'F'` is a strict superset guarantee: only
commits with every reachable blob verified present are ever offered. This is
the same restriction now applied uniformly to §3 rule 2's "any candidate"
check. (The identical gap already exists, unacknowledged, in today's live
`is_graph_have_eligible`/`get_complete_commits` — not a Package C regression,
but Package C's own new query must not repeat it.)

One bulk SELECT, scoped by `repo_key` (leading primary-key column — an index
range access, not a full table scan of all repositories). Result bounded by
this one repository's own certified-commit count (a small state table: SHA1 +
2 one-char flags + timestamps, never object payloads) — not "all repository
objects N". No per-candidate SQL, no per-candidate graph walk, no object-store
read.

ABAP-side, in order:
1. Exclude every SHA1 present in `it_want_hashes` (current wants).
2. Deduplicate (defensive; `repo_key+commit_sha1` is already the table's
   primary key, so duplicates cannot occur structurally).
3. Sort deterministically: `updated_at` descending (most recently certified
   first — most likely to be close to the new tip), tie-broken by
   `commit_sha1` ascending for full determinism.
4. Cap at `iv_max_haves` (constant `c_max_certified_haves = 50`, same order of
   magnitude as the cap the legacy ancestor-BFS already used, chosen to bound
   pkt-line size — tunable later without contract change).

Returns an empty table (not an exception) when the repo has zero certified
candidates — a legitimate, valid outcome (cold or brand-new repo). A genuine
technical SQL failure is not caught here (matches this codebase's existing
convention: no other `SELECT` in `zcl_abapgit_ortec_mat_state`/
`zcl_abapgit_ortec_repo_state` wraps its `SELECT` in `TRY`/`CATCH`) — it
propagates as a runtime error rather than being silently converted to an empty
"valid" result, satisfying "propagate technical certificate-read failures".

This entirely replaces, for all migrated callers,
`zcl_abapgit_ortec_fetch_neg=>get_have_commits`/`get_verified_have_commits`/
`collect_ancestor_haves`/`is_commit_complete` and
`zcl_abapgit_ortec_repo_state=>get_complete_commits` (§8 — legacy
classification).

**Disclosed trade-off (protocol review F4):** unlike the legacy
`get_complete_commits`, `get_certified_haves` does not fall back to
`ZAOG_REPO_STATE.fetch_commit` rows as an additional have-candidate source.
This is intentionally fail-safe (strictly fewer, but only ever *certified*,
haves) — never a correctness risk, at most a missed delta-base opportunity for
a commit that is genuinely complete but was never certified (closed for
already-known repos by §3a's opportunistic backfill).

## 6. Publication / certification lifecycle for the incremental path

Replaces `persist_pull_result`'s raw `INSERT zaog_commit_hist` (hist_level
never set). After objects for the new tip are persisted into
`ZAOG_OBJ_STORE` (unchanged — `decode_streaming`/`serve_cached_when_nothing_new`
already do this) and before the method's existing single `COMMIT WORK`:

1. `lv_attempt = mat_state=>begin_attempt( iv_repo_key, iv_commit )`.
2. `obj_store=>verify_tree_closure( iv_repo_key, iv_commit )` (Package B B1
   API, reused as-is — commit→tree closure, blobs not required by this call),
   wrapped in `TRY ... CATCH zcx_abapgit_ortec_git` (explicit, closes DR-004/
   protocol F2 — the exception is caught here, not left to propagate and abort
   before step 5/the final `COMMIT WORK`).
   On failure: do not call `mark_graph_complete`; skip straight to step 5
   (persist objects/repo-state bookkeeping still happens, but no certificate
   is published — matches "no graph or snapshot certificate is published
   before verification").
3. On success: `mat_state=>mark_graph_complete( iv_repo_key, iv_commit,
   lv_attempt )`.
4. Full-completeness check reuses Package B B2's bulk helper:
   `lt_tip_blobs = obj_store=>get_tip_blob_sha1s( iv_repo_key, iv_commit )`,
   then `lt_missing = obj_store=>get_missing_sha1s( iv_repo_key, lt_tip_blobs
   )` (already-existing bulk, no-payload-read APIs — no new query shape). If
   `lt_missing` is empty (expected for `INCREMENTAL_THIN`/
   `INCREMENTAL_SELF_CONTAINED`/`RECOVERY_BRANCH_FULL`, which are never
   blobless): `mat_state=>mark_full_complete( iv_repo_key, iv_commit,
   lv_attempt )`, then `mat_state=>publish_snapshot_complete( iv_repo_key,
   iv_branch_name, iv_commit, lv_attempt )`.
5. `zcl_abapgit_ortec_repo_state=>update_after_fetch(...)` (unchanged, kept for
   URL/repo_key bookkeeping and UI display — no longer read as a completeness
   authority by any migrated path).
6. Single `COMMIT WORK` (unchanged position — one commit for the whole
   attempt, identical shape to Package B's already-approved pattern; this is
   not a new transaction/attempt-isolation design, it is the same approved
   pattern applied to a second caller).

If step 4 finds missing blobs (should not normally happen for a non-blobless
mode, but is possible after a partial `RECOVERY_BRANCH_FULL`), the commit stays
at `GRAPH_COMPLETE`/`snap_state` untouched (no publish) — the branch pointer is
not advertised as a snapshot-complete tip, but the pull itself still returns
its already-decoded files to the caller unchanged (no regression to the UI).

`pull_by_commit` is not required to run this lifecycle for its target commit
(out of objective scope — see migration map); it benefits automatically from
the certified-have fix wherever it shares `upload_pack`.

## 7. Retry / recovery limits

Unchanged from the already-productive Slice 2C cascade: at most one
`INCREMENTAL_SELF_CONTAINED` retry, at most one `RECOVERY_BRANCH_FULL` recovery
attempt, gated by `is_retry_without_haves`. Package C does not add a fourth
tier and does not reintroduce `first_progressive_deepen`/
`next_progressive_deepen` (already unreachable from any migrated path, see
migration map).

## 8. ORTEC-disabled behavior

Unchanged. `zcl_abapgit_git_switch=>is_active_for_repo` gates
`zcl_abapgit_git_porcelain`'s routing to `zcl_abapgit_ortec_porcelain` exactly
as today; nothing in this design touches the inactive branch of
`zcl_abapgit_git_porcelain=>pull_by_branch`/`pull_by_commit` or
`zcl_abapgit_git_transport`.

## 9. Idempotent restart behavior

`mat_state=>begin_attempt` never downgrades `hist_level` and is safe to call
repeatedly (already proven by Package B's own tests). A retried
`pull_by_branch` for the same tip after a crash before `COMMIT WORK` simply
redoes the fetch and re-certifies; nothing is left half-published because
`publish_snapshot_complete` is the last statement before the single
`COMMIT WORK` in both the incremental and Package B cold paths.

## 10. Capability / stale-certificate behavior

- Missing `filter` capability for `COLD_BRANCH`: typed `zcx_abapgit_ortec_git`
  propagates (§4 COLD_BRANCH step 1) — no unfiltered fallback.
- Stale certificate (e.g. `invalidate_commit`/`invalidate_all_history` ran
  since the last classification): the next `classify_operation` call simply
  re-reads current state — there is no cross-call caching of classification
  results, so staleness cannot outlive one call.

## 11. Deferred Package D/E boundaries

- Package D1: bulk external delta-base resolution remains untouched;
  `zcl_abapgit_ortec_pack_stream`'s existing per-base repair path is not
  invoked by any new Package C code and is not modified.
- Package D2: no new transaction/attempt-isolation model is introduced —
  Package C reuses Package B's already-approved single-attempt/single-commit
  shape verbatim for a second caller (§6, §9). No concurrent-attempt or
  cross-session isolation problem is solved or introduced here.
- Package E: no legacy code is deleted. Newly-unreachable methods are labeled
  in the migration map for future physical removal only.

## 12. SQL / HTTP multiplicity and memory budget

- Classification: 1 keyed `SELECT SINGLE` (`mat_state=>get_state`) + at most 1
  bulk `SELECT` (`get_certified_haves`, reused for the "any candidate"
  check) per `pull_by_branch`/`pull_by_commit` call. No SQL per candidate.
- Have selection: 1 bulk `SELECT` per fetch attempt (§5), 0 per candidate.
- HTTP: `WARM_UNCHANGED` = 1 GET (`info/refs`, §2) + 0 POST + 0 further GET
  (short-circuits before reaching `upload_pack_by_branch`, so the shared
  legacy `fastpath=>pull_by_branch` is never entered for this case).
  `INCREMENTAL_UPDATE` = 1 GET (§2) + up to 1 further GET (shared legacy
  `fastpath=>pull_by_branch`'s own tip re-resolution, disclosed in §2 —
  pre-existing, unmodified, out of Package C's edit scope) + 1..3 POSTs
  (thin/self-contained/recovery, unchanged existing bound). `COLD_BRANCH` = the
  same 1 (+ up to 1 legacy) GETs, plus exactly the Package B B1+B2+B3 request
  counts (already audited/approved in Package B's own performance review —
  not re-derived here); §3a's opportunistic backfill adds 0 HTTP.
- Memory: no new full-repository or full-object-store read is introduced;
  `get_tip_blob_sha1s`/`get_missing_sha1s` are the already-approved bulk,
  no-payload-read Package B B2 APIs, bounded by the tip's own blob set (K),
  not by total repository objects (N).

## 13. Migration map

| Symbol | Classification | Notes |
|---|---|---|
| `zcl_abapgit_ortec_porcelain=>pull_by_branch` | `MIGRATE_IN_C2` | add classification + 3-way routing before existing `upload_pack_by_branch` call |
| `zcl_abapgit_ortec_porcelain=>pull_by_commit` | `MIGRATE_IN_C2` | add classification + warm/cold routing; certification lifecycle for its target commit is optional/non-blocking, not required by this package |
| `zcl_abapgit_ortec_fastpath=>upload_pack` (have resolution call) | `MIGRATE_IN_C1` | switch `get_verified_have_commits` → `have_policy=>get_certified_haves` |
| `zcl_abapgit_ortec_fastpath=>persist_pull_result` | `MIGRATE_IN_C2` | replace raw `ZAOG_COMMIT_HIST` INSERT with §6 lifecycle |
| `zcl_abapgit_ortec_have_policy` (new) | `MIGRATE_IN_C1` | new class, C1 scope |
| `zcl_abapgit_ortec_fetch_neg=>get_have_commits` / `get_verified_have_commits` / `collect_ancestor_haves` / `is_commit_complete` | `LEGACY_UNREACHABLE_AFTER_C2` | superseded by `have_policy`; kept physically present |
| `zcl_abapgit_ortec_repo_state=>get_complete_commits` | `LEGACY_UNREACHABLE_AFTER_C2` | only caller was `get_have_commits` |
| `zcl_abapgit_ortec_fastpath=>pull_by_branch` | `REUSE_NO_CHANGE` | **corrected (DR-001/DR-002):** live, not dead — first statement of `upload_pack_by_branch`, shared with `zcl_abapgit_git_commit`/`zcl_abapgit_merge`; out of Package C's edit scope; its raw `fetch_commit` check is safe (raises/falls through on genuine incompleteness) and is disclosed, not silently trusted, by the new classifier (§2/§12) |
| `zcl_abapgit_ortec_fastpath=>first_progressive_deepen` / `next_progressive_deepen` / `c_progressive_*` | `LEGACY_UNREACHABLE_AFTER_C2` | already unreachable since Slice 2C; confirmed still true, no change needed |
| `zcl_abapgit_ortec_fastpath=>build_upload_pack_buffer` | `LEGACY_UNREACHABLE_AFTER_C2` | already documented as such since Slice 2C |
| `zcl_abapgit_ortec_cold_init=>acquire_blobless_graph` / `materialize_tip_snapshot` | `REUSE_NO_CHANGE` | invoked from C2, Package B contract unchanged |
| `zcl_abapgit_ortec_mat_state=>*` | `REUSE_NO_CHANGE` | invoked from C1 (`get_state`) and C2 (lifecycle), no signature change |
| `zcl_abapgit_ortec_pack_stream` external-base repair (`complete_missing_base`, `get_base_bytes`) | `DEFER_TO_PACKAGE_D1` | untouched |
| `zcl_abapgit_git_transport` / `zcl_abapgit_git_porcelain` hooks | `REUSE_NO_CHANGE` | already minimal, correct isolation points; no edit needed |
| `zcl_abapgit_ortec_walk_prep` / `zcl_abapgit_ortec_missing_obj` | `REUSE_NO_CHANGE` | used unchanged by `pull`/`walk` reconstruction in all three routed paths |

## 14. Mandatory test coverage map (method-level, ≤30-char names)

C1 (`zcl_abapgit_ortec_have_policy`, pure/bulk-SQL, no HTTP):
full-certified (`hist_level='F'`) eligible; graph-complete-only (`hist_level=
'G'`, not yet full) ineligible (closes protocol review F1); non-certified
ineligible; pending/failed/legacy-only ineligible; other-repo excluded; wants
excluded; duplicates removed (structural, via PK); deterministic order;
max-have cap enforced; empty-candidate valid; technical read failure
propagates (uncaught, proven by not wrapping SELECT); no graph-walk/payload
read (proof: method never calls `obj_store=>get_object`/`get_objects`);
classification matches certificate state (warm/incremental/cold, 3 cases +
boundary at `iv_max_haves=1`).

C2 (`zcl_abapgit_ortec_porcelain`, `zcl_abapgit_ortec_fastpath`):
warm does no HTTP POST (mock/stub the classification result, assert
`upload_pack_by_branch` not reached — extract the routing decision into a
separate pure method for this, per the prompt's "extract pure
classification/policy/retry-decision/publication-decision methods"
requirement); incremental thin-first; self-contained retry ≤1; recovery ≤1; no
migrated request emits deepen/shallow (regression on existing
`ltcl_fastpath_protocol` assertions, unchanged); cold invokes B1 then B2+B3 in
order (spy/stub); missing filter capability fails typed, no full-fetch;
partial graph state (verify_tree_closure fails) prevents publish; partial
snapshot state (missing blobs) prevents publish; successful verified state
publishes; stale/wrong attempt does not publish (reuses existing
`mat_state` stale-attempt tests); ORTEC-disabled unchanged (existing tests
green); no migrated caller invokes `fetch_tip_commits`/legacy request builder
(grep-based regression assertion); Package B/Slice 2/resolver/completeness/
base-cache regressions remain green (existing suites, no changes expected);
§3a backfill: already-locally-complete legacy commit self-certifies to
`WARM_UNCHANGED` with 0 HTTP calls; locally-present-but-incomplete legacy
commit falls through to `COLD_BRANCH`; never-seen commit skips backfill
entirely (no `obj_store=>exists` false positive).

## 15. Checkpoint plan

`C1_THEN_C2`. C1 (`zcl_abapgit_ortec_have_policy` + the one-line
`upload_pack` call-site switch) is independently meaningful: it already fixes
the "always-empty certified haves" production defect for every existing
incremental fetch, without touching branch orchestration/publication/cold
routing. It is import/activate/ABAP-Unit/ATC-validatable on its own.
