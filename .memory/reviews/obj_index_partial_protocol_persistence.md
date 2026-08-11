# OBJ_PERF_FINAL — Protocol/Persistence independent review (OBJ-PERF-PROTOCOL-1)
Task: OBJ-PERF-PROTOCOL-1
Reviewing: cycle-3 FINAL design (`.memory/logs/obj_index_partial_design.md`,
`.memory/logs/obj_store_performance_design.md`), already adversarially
APPROVED (`.memory/reviews/obj_index_partial_adversarial.md`, cycle 3).
Baseline: `4193733de3c8ad75bae61f6ceb95b1c3e0ccb3e4`.
Scope: DDIC identity, LUW/lock/commit boundaries, publication ordering only —
no re-litigation of AR-1-xx/AR-2-xx, which are independently re-verified
below only where they touch this role's domain.
## Evidence
| ID | Source |
|---|---|
| PP-E-DDIC | Direct read of `src/ortec/git/zaog_obj_index.tabl.xml`: key `(CLIENT, REPO_KEY, COMMIT_SHA1, OBJ_TYPE, OBJ_NAME, PATH_HASH)`, no `CONTEXT_HASH` yet — matches design §3.0's "append non-key column" premise exactly |
| PP-E-OBJIDX-SRC | Direct read of `src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap`: `get_files_for_filter` (no `COMMIT WORK`/`ROLLBACK WORK` anywhere in class), `rebuild_index` (lock acquired once at top, `is_index_ready` re-checked after acquire, pre-walk `DELETE FROM zaog_obj_index`, chunked `MODIFY` at `c_index_write_chunk_size`, marker `MODIFY` is the last write before `release_repo_lock`, retry path's standalone `DELETE FROM zaog_obj_index` at the `CORRUPT_OR_INCOMPLETE` catch) |
| PP-E-LOCK-RAW | Direct read of `src/ortec/git/zcl_abapgit_ortec_pack_raw.clas.abap`: `acquire_repo_lock` is a bare `INSERT zaog_fetch_sess` retry loop (session_id `LOCK_<repo_key>`, `status='L'`, 7 attempts, exponential 50ms/jitter, ≤2s/attempt cap, ~5.15s total ceiling) with **no age/timestamp-based staleness check of any kind**; `release_repo_lock` is a bare `DELETE ... WHERE session_id = iv_lock_id AND status = 'L'`. Neither method issues `COMMIT WORK`. |
| PP-E-LOCK-DEC | Direct read of `src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap` (lines ~610-630): a **second, differently-named-but-identically-named-method** `acquire_repo_lock`/`release_repo_lock` pair exists in this class, implemented via SAP enqueue (`ENQUEUE_EZAOG_REPO_LOCK`, `_SCOPE='2'`), with an explicit comment: "automatically released by the enqueue server if the work process ends (crash, timeout, short dump) — no stale lock possible." Confirmed via grep that `zcl_abapgit_ortec_pack_dec` never touches `zaog_obj_index`/`zaog_obj_cover`/`zaog_obj_pidx` — out of this design's table scope, not a new risk for THIS review. |
| PP-E-CACHE-ADMIN | Direct read of `src/ortec/git/zcl_abapgit_ortec_cache_admin.clas.abap`: `clear_repo`'s exact statement order confirmed — `acquire_lock` (ENQUEUE_EZAOG_REPO_LOCK) → TRY → DELETE obj_index → pack_idx → raw_pack → pack_meta → **fetch_sess** → commit_hist → obj_store → repo_state → `invalidate_cache( )` → **one** `COMMIT WORK AND WAIT` → `release_lock`. Confirms the design's stated insertion point (new 3-table block replaces only the first DELETE, ahead of the unchanged `fetch_sess` bulk delete) is accurate. |
## Required-work findings
### 1. Canonical identity (ZAOG_OBJ_COVER, ZAOG_OBJ_PIDX, ZAOG_OBJ_INDEX+CONTEXT_HASH)
No blocker. Confirmed against source and design text:
- `ZAOG_OBJ_INDEX`'s existing key (6 fields, MANDT implicit) is unchanged; `CONTEXT_HASH` is
  specified as a genuine non-key append, safe *only* because every writer
  (`rebuild_index`) purges the full `(repo_key, commit)` scope via
  `invalidate_commit_index` before writing — verified true in source (no
  other `MODIFY`/`INSERT` call site touches this table without that
  preceding purge).
- `ZAOG_OBJ_PIDX`'s key inserts `CONTEXT_HASH` between `OBJ_NAME` and
  `PATH_HASH`, preserving a contiguous primary-key-prefix shape for both
  `select_partial_rows_for_filter`'s read and `invalidate_commit_index`'s
  context-blind delete (repo_key+commit_sha1 prefix only, correctly omits
  context_hash so a commit-scoped purge clears every context, §5). No
  ambiguous identity: no code path ever selects/deletes this table without
  binding at least the `(repo_key, commit_sha1)` prefix.
- `ZAOG_OBJ_COVER`'s key is `(MANDT, REPO_KEY, COMMIT_SHA1, OBJ_TYPE,
  OBJ_NAME, CONTEXT_HASH)` — no bare-SHA-as-identity violation anywhere;
  every predicate in `get_coverage`/`write_coverage` binds all of
  `repo_key`+`commit_sha1`+`obj_type`+`obj_name`+`context_hash`.
- No table in this design is ever addressed by `obj_sha1`/blob SHA1 alone —
  identity is always `(repo, commit, object)` or `(repo, commit)`, consistent
  with OS-INV-01.
### 2. LUW/lock/commit boundaries
Verified correct: `acquire_repo_lock`'s DB-row mutex is a valid cross-session
serialization primitive independent of `COMMIT WORK` timing — a competing
`INSERT` for the same `session_id` blocks/fails against an uncommitted
`INSERT`/`DELETE` on that same key by normal DB row-lock semantics, so
`walk_filtered`↔`rebuild_index`↔`clear_repo` (post-AR-2-03) do correctly
serialize on the same mutex regardless of exactly when `release_repo_lock`'s
own `DELETE` statement runs inside a caller's still-open transaction.
`invalidate_commit_index`'s three-table delete is unchunked (single-commit
scope) and issues no `COMMIT WORK` of its own — correct, matches this
class's no-commit discipline; the caller's LUW is genuinely the only
atomicity boundary for all three tables together.
**New finding — see PP-01 below**: the *effective* concurrent-blocking
window for `clear_repo`'s "narrow" lock acquisition is materially wider than
the design's own text claims, because `release_repo_lock`'s `DELETE` of the
mutex row is itself uncommitted until `clear_repo`'s single trailing
`COMMIT WORK AND WAIT` — not narrowed to "three deletes only" as intended.
### 3. Publication ordering
Verified correct and unchanged: `rebuild_index`'s `$IDX/__READY__` marker
`MODIFY` is confirmed (by direct source read) to be the last write in the
method before `release_repo_lock`, both today and unchanged by this design
(only gains the `CONTEXT_HASH` stamp). FILTERED (`ZAOG_OBJ_PIDX`) and
COMPLETE (`ZAOG_OBJ_INDEX`) reads are structurally mutually exclusive per
request: §11 step 3.1 returns directly from the `is_index_ready=true`
branch before any `ZAOG_OBJ_COVER`/`ZAOG_OBJ_PIDX` consultation, so one
request can never partially decide from both sources. Promotion from
FILTERED to COMPLETE is explicit and provably race-free: `rebuild_index` and
`walk_filtered` hold the *same* mutex for their *entire* duration (confirmed
in source for `rebuild_index`; specified identically for `walk_filtered`),
so a `rebuild_index`'s `invalidate_commit_index` purge of `ZAOG_OBJ_PIDX`
can never interleave with a concurrent `walk_filtered`'s writes to the same
commit — no visible inconsistency window is possible, because neither
writer's intermediate state is visible to any other session before its own
LUW commits (chunking at `c_filter_chunk_size`/`c_index_write_chunk_size`
bounds SQL statement size only, not publication atomicity — the true
publication unit is always the caller's own commit, never a chunk).
### 4. Rollback/retry/crash-recovery
Verified safe for the common case: every write in this design (`ZAOG_OBJ_PIDX`,
`ZAOG_OBJ_COVER`, `invalidate_commit_index`'s deletes) uses `MODIFY`/`DELETE`
with **no** `COMMIT WORK` issued by either class, so a request that aborts
mid-chunk (exception, session timeout, hard kill) leaves **zero** durable
rows for that request — standard ABAP LUW auto-rollback discards all
uncommitted chunks together. A retry is always safe: re-running `walk_filtered`
re-selects `is_index_ready`/`get_coverage` from scratch and re-issues `MODIFY`
(never `INSERT`), so no `ITAB_DUPLICATE_KEY`/SQL duplicate-key error is
possible on retry, and no orphaned fact can result from a partial abort
because nothing partial was ever committed.
**New finding — see PP-02 below**: the store-performance design's specific
claim that an orphaned mutex row is "reclaimed by `acquire_repo_lock`'s
existing age/retry logic" is not supported by the actual method body (no
age/timestamp check exists) — the real (and correct) safety mechanism is
plain LUW auto-rollback of the uncommitted `INSERT` on abnormal termination,
not a reclaim mechanism inside `acquire_repo_lock` itself.
## New findings
### ID=PP-01
SEVERITY=MAJOR
CLAIM=`clear_repo`'s AR-2-03 fix narrows its `acquire_repo_lock` hold to
"exactly its three derived-filter-table deletes," so the lock coupling
"only applies to the already-infrequent admin clear operation, never to the
per-request read/write path" (`obj_store_performance_design.md`, cache-admin
section + cycle-3 cost addition).
COUNTEREXAMPLE=`clear_repo` acquires `acquire_repo_lock`, deletes
`ZAOG_OBJ_INDEX`/`ZAOG_OBJ_COVER`/`ZAOG_OBJ_PIDX`, then calls
`release_repo_lock` (a plain `DELETE FROM zaog_fetch_sess WHERE session_id =
lv_pack_lock AND status = 'L'`) — but this `DELETE` executes inside
`clear_repo`'s own still-open transaction, which does not commit until the
method's single trailing `COMMIT WORK AND WAIT`, reached only after five
more unchunked repo-wide deletes (`pack_idx`, `raw_pack`, `pack_meta`,
`fetch_sess`, `commit_hist`, `obj_store` — potentially hundreds of thousands
of rows per `state.md`'s 1,000,000-object scale — `repo_state`) plus
`invalidate_cache( )`. Under ordinary row-lock semantics, a concurrent
`walk_filtered`/`rebuild_index` on the same repo calling
`zcl_abapgit_ortec_pack_raw=>acquire_repo_lock` attempts an `INSERT` for the
identical `session_id = LOCK_<repo_key>` key that `clear_repo`'s
(uncommitted) `DELETE` still holds exclusively — the competing `INSERT`
blocks (or eventually fails/retries) until `clear_repo`'s transaction
actually ends, i.e. for the *entire remaining duration of `clear_repo`*, not
just the three-delete window the design's prose implies is being minimized.
`acquire_repo_lock`'s own bounded retry (7 attempts, 50ms base, ≤2s/attempt
cap, ≈5.15s total ceiling) can therefore time out and raise
`zcx_abapgit_exception=>raise('Repo lock timeout...')` for a legitimate,
concurrent, per-request `walk_filtered`/`rebuild_index` call whenever
`clear_repo`'s *remaining* work (after its own release) exceeds ~5 seconds
on a large repo — directly contradicting the "never affects the per-request
path" claim. (Not a correctness/data-integrity defect: the resulting
timeout is caught by the existing `zcx_abapgit_exception` fallback to
`get_files_remote`, §6 trigger 2 — this is an availability/cost-model
accuracy finding, not a false-fact or orphaned-row risk.)
EVIDENCE=PP-E-LOCK-RAW (bare `INSERT`/`DELETE`, no intermediate commit, bounded
retry ceiling), PP-E-CACHE-ADMIN (single trailing `COMMIT WORK AND WAIT`,
five further unchunked deletes after the point where `release_repo_lock`
would run), `obj_store_performance_design.md` cache-admin section and its
"Cycle 3 cost additions" paragraph (claims no new cost class / never affects
per-request path).
IMPACT=availability / cost-model overclaim — a rare admin `clear_repo` on a
large repo can cause concurrent legitimate `walk_filtered`/`rebuild_index`
requests to spuriously fall back to a full unfiltered remote fetch, a cost
class the design explicitly says this change does not introduce.
REQUIRED_CHANGE=Either (a) have `clear_repo` issue an explicit interim
`COMMIT WORK` (not `AND WAIT`, to avoid a second synchronous round trip)
immediately after the three-table delete block and its `release_repo_lock`
call, so the mutex-row release becomes durable and visible to waiting
sessions before the remaining (larger, slower) cache-table deletes proceed —
this would make the lock hold genuinely as narrow as the design intends; or
(b) explicitly document and accept that the true contention window is
`clear_repo`'s full remaining duration (not three-deletes-only), correcting
the cost-model claim, on the grounds that `clear_repo` is already rare and
the resulting fallback is graceful and bounded.
RETEST=Concurrency/seam test: start a `clear_repo` on a repo with a
large (e.g. >100k row) `ZAOG_OBJ_STORE`, pause it immediately after its
`release_repo_lock` call (before the `obj_store`/`repo_state` deletes and
final commit), then invoke a concurrent `walk_filtered`/`rebuild_index` on
the same repo and measure whether it blocks/times out for materially longer
than the three-delete window; if fix (a) is applied, the concurrent call
should succeed promptly once the interim commit lands.
### ID=PP-02
SEVERITY=MINOR
CLAIM=`obj_store_performance_design.md`'s cache-admin section states "a
mutex row orphaned by a hard process kill is reclaimed by
`acquire_repo_lock`'s existing age/retry logic (unchanged)."
COUNTEREXAMPLE=Direct read of `zcl_abapgit_ortec_pack_raw=>acquire_repo_lock`
shows no age/timestamp-based staleness check anywhere in the method — it
only retries a plain `INSERT` with exponential backoff and eventually raises
a timeout exception; there is no code that inspects `created_at`/`updated_at`
of an existing `LOCK_<repo_key>` row to decide it is stale and reclaim it.
The actually-correct explanation (matching the more precisely-commented,
structurally-different `acquire_repo_lock` in `zcl_abapgit_ortec_pack_dec`,
which uses SAP enqueue with an explicit "no stale lock possible" crash-safety
comment) is that the DB-row mutex's crash-safety comes from ordinary ABAP
LUW auto-rollback of the still-uncommitted `INSERT` when the holding session
terminates abnormally before any commit — not from a reclaim mechanism
inside `acquire_repo_lock` itself.
EVIDENCE=PP-E-LOCK-RAW (no age check in the method body), PP-E-LOCK-DEC
(the differently-implemented, correctly-commented sibling method in
`zcl_abapgit_ortec_pack_dec`, confirming the actual mechanism this design's
prose conflates).
IMPACT=documentation accuracy only — the underlying behavior is still
correctly crash-safe under normal LUW semantics (an orphan can only persist
if some outer caller committed mid-walk before the crash, a scenario this
design does not introduce), but the stated mechanism is wrong and could
mislead a future maintainer searching for non-existent reclaim code.
REQUIRED_CHANGE=Correct the sentence to attribute crash-safety to LUW
auto-rollback of the uncommitted mutex-row `INSERT`/`DELETE`, not to an
"age/retry" mechanism that does not exist in `acquire_repo_lock`.
RETEST=None required (documentation-only); no behavior change.
## Verdict
**APPROVE_WITH_MINOR_REVISIONS**.
Blocking: 0. No DDIC-identity, LUW/lock-correctness, or publication-ordering
defect was found — every claim re-verified against current source in items
1, 3, and the non-PP-01/PP-02 parts of items 2 and 4 holds exactly as the
cycle-3 design and its adversarial approval state. PP-01 (MAJOR) is a
cost-model/availability overclaim with a graceful, already-existing fallback,
not a correctness or identity violation; PP-02 (MINOR) is documentation-only.
Neither reopens any AR-1-xx/AR-2-xx finding or changes the adversarial
review's APPROVE verdict on correctness grounds — both are additive,
protocol/persistence-specialist findings the prior three cycles did not
examine (contention-window-vs-commit-boundary timing, and crash-recovery
mechanism accuracy).
| ID | Severity | Status | Domain |
|---|---|---|---|
| PP-01 | MAJOR | OPEN | LUW/lock/commit boundary — cache-admin lock scope vs. real commit timing |
| PP-02 | MINOR | OPEN | Crash-recovery documentation accuracy |