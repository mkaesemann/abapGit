# Incident: Variant B / Package D2 — IT8 SYSTEM_NO_ROLL and TIME_OUT on Stage-By-Filter (cold branch)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=D2-INCIDENT-SYSTEM_NO_ROLL-TIMEOUT
BASELINE=de0f11ce7e2146a1f7d47b2170594f4fa77c2658 (owner SAP syntax-fix commit, HEAD)
STATUS=INCIDENT_ROOT_CAUSE_CONFIRMED (memory) / INCIDENT_ROOT_CAUSE_CONFIRMED (timeout)
```

## 1. Verified IT8 connection

The owner updated the `arc-1` MCP definition mid-session. Re-verified before any
evidence was trusted:

- First attempt (before the owner's change) resolved to a **different** system:
  host `HANES6`/`HANES6B`, SID `ES6`, client 200 — confirmed wrong by dump IDs
  (`...B-SAP-HANES6_ES6_00...`) and zero ORTEC-class dumps present.
- After the owner's change, the connection (now exposed as tool prefix
  `arc-12`) resolves to host `HANIT8`, **SID `IT8`**, client **100**, user
  `MICHAELK` — confirmed by dump IDs of the form
  `...B-SAP-HANIT8_IT8_00...MICHAELK...100...` and by a dense history of
  `ZCL_ABAPGIT_ORTEC_*` dumps matching this project's own class names.
- No SAP objects were modified during evidence gathering (dumps/read/query
  only) prior to the one confirmed, minimal productive fix described in §10.

## 2. Exact dump IDs/timestamps

| Dump | ID | Timestamp (UTC) | Error | Program |
| --- | --- | --- | --- | --- |
| SYSTEM_NO_ROLL | `20260728133704T-SAP-HANIT8_IT8_00...MICHAELK...100...8` | 2026-07-28T11:37:04Z | SYSTEM_NO_ROLL | `ZCL_ABAPGIT_ORTEC_OBJ_STORE===CP` |
| TIME_OUT | `20260728135009T-SAP-HANIT8_IT8_00...MICHAELK...100...8` | 2026-07-28T11:50:09Z | TIME_OUT | `ZCL_ABAPGIT_ORTEC_OBJ_STORE===CP` |

Disambiguation: both dumps are same user (`MICHAELK`), same client (100), same
day, ~13 minutes apart, both terminating in `ZCL_ABAPGIT_ORTEC_OBJ_STORE`, and
both reached via the identical Stage-By-Filter → `ENSURE_AVAILABLE` →
`UPLOAD_PACK_BY_COMMIT` call chain — matching the owner's reported workflow
(crash, restart, retry, second crash) exactly. No other SYSTEM_NO_ROLL/TIME_OUT
dump in the surrounding window matches this program/user/workflow combination.

Three earlier `SYNTAX_ERROR` dumps at 10:19–11:02 the same day
(`ZCL_ABAPGIT_ORTEC_OBJ_INDEX`) are the owner's own pre-`de0f11ce` iteration
noise and are unrelated to this incident (both target dumps post-date them and
post-date the `de0f11ce` import).

## 3. Side-by-side dump evidence

### 3.1 SYSTEM_NO_ROLL (11:37:04Z)

```text
Short text:        The memory request for 502096 bytes could not be complied with.
Termination point: ZCL_ABAPGIT_ORTEC_OBJ_STORE===CP, method POPULATE_CACHE,
                    line 12 of include ZCL_ABAPGIT_ORTEC_OBJ_STORE===CM00B
                    (the SELECT * FROM zaog_obj_store statement itself).
```

Source extract at the termination point (`populate_cache`):

```abap
SELECT * FROM zaog_obj_store
  INTO TABLE lt_rows
  WHERE repo_key = iv_repo_key AND status = 'R'
  ORDER BY obj_sha1.
```

Memory section (kap40):

```text
Extended Memory (EM):        1,983,677,632 bytes  (~1.98 GB)
PRIV Memory (Heap):          1,996,620,336 bytes  (~1.99 GB)
Used Memory:                 3,962,447,872 bytes  (~3.96 GB)
Free Memory:                     7,133,856 bytes  (~7.0 MB)
Largest Free Block:                 73,520 bytes
Used Blocks:                       195,353
Free Blocks:                         8,845
```

Selected variables (kap10): `LT_ROWS` = `Table IT_522883[54226x280]` — 54,226
rows already materialized in the failing internal table at the moment of the
dump, each row carrying the full `zaog_obj_store` record shape including the
`obj_data` payload column (blob/tree/commit content), not just presence
metadata. `IV_REPO_KEY` = `288c81fc1cad`.

**Failing-allocation-vs-root-cause analysis (explicit, per Phase 1
requirement):** the failing allocation itself was only 502,096 bytes (~490 KB)
— trivially small. The true cause is that **3.96 GB was already resident**
before this allocation was attempted, and the single `SELECT * ... INTO TABLE`
statement executing at the termination point has **no row limit, no chunking,
and no byte budget** — it is a full, repository-wide read of every `status =
'R'` row (including full payloads) ever stored for repo `288c81fc1cad`. The
502 KB allocation is the *tipping point*, not the root cause; the root cause is
the unbounded `SELECT` itself, which was already 54,226 rows deep and still
growing when the process ran out of headroom.

Call stack (kap11), abbreviated to the ORTEC-owned frames (full 30-frame stack
captured; GUI/dispatcher frames below `ZABAPGIT` omitted here as routine):

```text
ZABAPGIT (START-OF-SELECTION) -> RUN -> OPEN_GUI -> GUI dispatch
  -> ZCL_ABAPGIT_GUI_ROUTER=>GET_PAGE_STAGE
  -> ZCL_ABAPGIT_GUI_PAGE_STAGE=>CREATE / CONSTRUCTOR / INIT_FILES
  -> ZCL_ABAPGIT_STAGE_LOGIC=>ZIF_ABAPGIT_STAGE_LOGIC~GET
  -> ZCL_ABAPGIT_ORTEC_GIT_FACADE=>RESOLVE_FILTERED_REMOTE
  -> ZCL_ABAPGIT_ORTEC_FILTER_WALK=>GET_REMOTE_FILES_FOR_STAGE
  -> ZCL_ABAPGIT_ORTEC_OBJ_INDEX=>GET_FILES_FOR_FILTER
  -> ZCL_ABAPGIT_ORTEC_OBJ_INDEX=>BUILD_FILES_FROM_ROWS
  -> ZCL_ABAPGIT_ORTEC_MISSING_OBJ=>ENSURE_AVAILABLE
  -> ZCL_ABAPGIT_GIT_TRANSPORT=>UPLOAD_PACK_BY_COMMIT
  -> ZCL_ABAPGIT_ORTEC_FASTPATH=>UPLOAD_PACK_BY_COMMIT
  -> ZCL_ABAPGIT_ORTEC_FASTPATH=>UPLOAD_PACK
  -> ZCL_ABAPGIT_ORTEC_FASTPATH=>SERVE_CACHED_WHEN_NOTHING_NEW
  -> ZCL_ABAPGIT_ORTEC_OBJ_STORE=>GET_REACHABLE_OBJECTS
  -> ZCL_ABAPGIT_ORTEC_OBJ_STORE=>POPULATE_CACHE   [CRASH]
```

### 3.2 TIME_OUT (11:50:09Z)

```text
Short text:         Time limit exceeded
Runtime limit:       rdisp/scheduler/prio_high/max_runtime = 660 seconds
Termination point:   ZCL_ABAPGIT_ORTEC_OBJ_STORE===CP, method
                     GET_STAGED_DELTA_OBJECTS, line 76 of include
                     ZCL_ABAPGIT_ORTEC_OBJ_STORE===CM00P (mid-way through the
                     chunked SELECT loop's final leftover-package branch,
                     specifically the "APPEND LINES OF lt_db_rows TO lt_rows"
                     statement immediately after a real DB SELECT).
```

Call stack (kap11), ORTEC-owned frames:

```text
ZABAPGIT -> ... (identical GUI/Stage-By-Filter dispatch chain as §3.1) ...
  -> ZCL_ABAPGIT_ORTEC_GIT_FACADE=>RESOLVE_FILTERED_REMOTE
  -> ZCL_ABAPGIT_ORTEC_FILTER_WALK=>GET_REMOTE_FILES_FOR_STAGE
  -> ZCL_ABAPGIT_ORTEC_OBJ_INDEX=>GET_FILES_FOR_FILTER
  -> ZCL_ABAPGIT_ORTEC_OBJ_INDEX=>BUILD_FILES_FROM_ROWS
  -> ZCL_ABAPGIT_ORTEC_MISSING_OBJ=>ENSURE_AVAILABLE
  -> ZCL_ABAPGIT_GIT_TRANSPORT=>UPLOAD_PACK_BY_COMMIT
  -> ZCL_ABAPGIT_ORTEC_FASTPATH=>UPLOAD_PACK_BY_COMMIT
  -> ZCL_ABAPGIT_ORTEC_FASTPATH=>UPLOAD_PACK
  -> ZCL_ABAPGIT_ORTEC_PACK_STREAM=>DECODE_STREAMING
  -> ZCL_ABAPGIT_ORTEC_PACK_STREAM=>RESOLVE_STREAMING
  -> ZCL_ABAPGIT_ORTEC_PACK_STREAM=>RESOLVE_ONE_META
  -> ZCL_ABAPGIT_ORTEC_OBJ_STORE=>GET_STAGED_DELTA_OBJECTS   [TIME_OUT]
```

**Identical entry sequence, divergent inner branch.** Both dumps share the
EXACT same call chain down to `ZCL_ABAPGIT_ORTEC_FASTPATH=>UPLOAD_PACK` — the
divergence happens at `upload_pack`'s own internal branch: the first attempt
took the "nothing new, serve from cache" shortcut (`serve_cached_when_
nothing_new`); the retry took the "decode a real pack" path
(`decode_streaming`). This is direct dump evidence for the
`HYPOTHESIS_INTERACTION` question (see §7).

## 4. Complete current-source call paths

All methods below were re-read directly from the current workspace source at
`de0f11ce` (identical to the confirmed live IT8 content — the incident dump's
own kap8 source extracts for `populate_cache` and `get_staged_delta_objects`
match the workspace file byte-for-byte).

### 4.1 `zcl_abapgit_ortec_obj_store=>get_reachable_objects` (owner: D-era, pre-dates D1/D2)

```text
File:      src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap (line ~692, pre-fix)
Operation: commit -> tree(s) -> blob(s) walk, ALREADY correctly bounded per
           level via get_objects( iv_bulk_fetch = abap_true ) scoped to that
           level's own SHA1 set (commit: 1 SHA1; each tree level: only that
           level's child SHA1s; blobs: only the blobs actually referenced by
           the walked trees).
Defect:    the method's FIRST statement was
           `populate_cache( iv_repo_key ).` - an unconditional pre-load of
           EVERY status='R' row for the whole repo_key into the session
           cache (mt_cache), regardless of how small the actual reachable
           set turns out to be.
Loop cardinality of the walk itself: O(objects reachable from iv_commit) -
           correctly bounded (K, not N).
Loop cardinality of the removed pre-load: O(ALL 'R' rows ever stored for
           iv_repo_key) - this is N, the entire shared-object-store history
           of every branch ever buffered for this repository, not K.
SQL:       populate_cache issued exactly ONE unbounded, unchunked
           `SELECT * FROM zaog_obj_store WHERE repo_key = ... AND
           status = 'R' ORDER BY obj_sha1` with no row/byte limit.
COMMIT:    none in this method (read-only).
Payload ownership: lt_rows (populate_cache-local) held ALL 'R' rows' full
           obj_data in memory simultaneously; mt_cache (session-global,
           CLASS-DATA) then held a second full copy (MOVE-CORRESPONDING per
           row) for the remainder of the session.
Entry/exit cleanup: none - mt_cache is never bounded or evicted for this
           path; it persists for the life of the ABAP session/work process.
Owner:     pre-dates the Package C/D bounded-window model. This exact
           full-preload anti-pattern is the one Package C's closeout
           explicitly targeted for FETCH_BLOBS_BULK ("large unfiltered
           repositories no longer create one repository-wide SHA1 range")
           - but that fix was never applied to this separate,
           get_reachable_objects-only preload call. Classified D2-adjacent /
           cross-package (see §12): reached via D2's own `ensure_available`
           anti-pattern (§4.2) but the defect itself lives in code that D1/D2
           never touched.
```

The sibling method `get_reachable_sha1s` (same file, ~line 808) performs the
IDENTICAL commit->tree->blob walk shape but has an explicit doc comment
proving the correct pattern was already known: *"Same commit -> tree -> blob
walk as get_reachable_objects, but never calls populate_cache (no full-repo
preload) and never fetches blob DATA."* This confirms `get_reachable_objects`
was the outlier, not the norm, and that removing the `populate_cache` call
does not require inventing new bounding logic — the correctly-bounded
per-level `get_objects` calls that remain are already proven sufficient by
this sibling's own existing test coverage and design intent.

### 4.2 `zcl_abapgit_ortec_missing_obj=>ensure_available` (owner: pre-Package-D "fastpath" utility, Package E-adjacent per state.md)

```text
File:      src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.abap
Operation: Step 1: get_missing_sha1s(it_sha1s)   - ONE bulk set-based lookup,
           correctly scoped to the caller's actual K missing blob SHA1s.
Step 2:    upload_pack_by_commit( iv_hash = iv_commit, iv_deepen_level = 1 )
           - fetches the ENTIRE reachable object graph of iv_commit (every
           commit/tree/blob reachable from that one commit tip), NOT a
           fetch scoped to it_sha1s. Git's upload-pack wire protocol has no
           "fetch only these N blob OIDs" primitive used here; the request
           is want=<commit>, deepen=1 - "this commit's full tree", which for
           a large repository can be N (nearly the whole current-tip working
           set), not K (the caller's actual missing subset).
Step 3:    store_objects - bulk INSERT of whatever the fetch returned.
Step 4:    get_missing_sha1s again - bulk re-check.
Loop cardinality: no explicit loop over it_sha1s (correctly bulk-shaped at
           the ensure_available level) - the disproportion is entirely
           inside Step 2's fetch scope, not a loop-count defect.
SQL:       bulk only (Steps 1 and 4), matching K, not N.
HTTP:      exactly ONE upload_pack_by_commit call - correctly "one call",
           but the call's OWN payload scope is N (whole commit), not K
           (missing blobs).
Entry/exit cleanup: none needed at this level; the disproportion is a
           request-shape problem, not a resource-leak problem, at this
           specific frame.
Owner:     ZCL_ABAPGIT_ORTEC_MISSING_OBJ is the exact class state.md already
           flags: "Package E must prevent normal certified current-tip
           consumers from using ZCL_ABAPGIT_ORTEC_MISSING_OBJ=>
           ENSURE_AVAILABLE." Confirmed live: Stage-By-Filter on a cold
           branch is now the PRIMARY, not exceptional, caller of this method
           - directly contradicting its own doc comment ("best-effort bulk
           top-up... never worse than the prior behavior").
```

### 4.3 `zcl_abapgit_ortec_pack_stream=>resolve_streaming` / `resolve_one_meta` / `preload_delta_rows` (D1/D2-owned, verified NOT the defect)

```text
Confirmed sound: preload_delta_rows performs exactly ONE bulk, chunked
  get_staged_delta_objects call for ALL of a pack's temp keys before the
  fixpoint loop starts; resolve_one_meta's own per-row
  get_staged_delta_objects call (single-element it_sha1s) is a pure
  mt_cache hit (0 SQL) whenever the row was already warmed by
  preload_delta_rows, matching the D1/D2 design exactly
  (variant_b_package_d_design.md Section 5.3.1/PERF-B-1).
This is NOT where the TIME_OUT's excess cost originates. The excess cost
  originates one level up: the PACK ITSELF, as fetched by
  ensure_available's disproportionate Step 2 (see §4.2), was abnormally
  large (see §6 for the exact measured size: 162,919 objects for ONE
  commit's top-up fetch). preload_delta_rows/resolve_one_meta correctly
  processed that oversized pack in bounded SQL-call terms, but the sheer
  object count made the CUMULATIVE wall-clock cost of correctly-bounded
  chunked SELECTs (dozens of SELECT * ... WHERE obj_sha1 IN (...) statements,
  each against a large table, each carrying real payload bytes) plus the
  recursive apply()/SHA1-hash work for 132,963 deltas exceed the 660s
  runtime budget. SM50's "very many individual database accesses" is
  consistent with dozens-to-~130+ distinct chunked SELECT statements firing
  in rapid succession for a pack of this size (132,963 delta rows /
  c_select_package_size candidates per preload_delta_rows chunk, plus
  get_staged_delta_objects' cache-miss fallback branch for anything not
  already warmed) - a genuine THROUGHPUT problem caused by the oversized
  input, not a reintroduction of the old per-object-SQL anti-pattern D2
  already fixed.
```

## 5. Memory ownership/lifetime model

| Structure | Scope | Lifetime | Bound | Eviction |
| --- | --- | --- | --- | --- |
| `populate_cache`'s local `lt_rows` | method-local | one `populate_cache` call | NONE (whole repo's 'R' rows) | freed on method return, but only AFTER the full unbounded SELECT already materialized |
| `mt_cache` (`ZCL_ABAPGIT_ORTEC_OBJ_STORE`, `CLASS-DATA`) | session/work-process | life of the ABAP session (reset only by a session/work-process restart, or explicit `invalidate_cache( )`) | NONE (no LRU, no byte budget, unlike `zcl_abapgit_ortec_base_cache`'s 256 MiB budget) | never evicted; only fully cleared by `populate_cache`'s own unconditional `CLEAR mt_cache` when `mv_full_cache_repo_key` does not match the requested repo |
| `mv_full_cache_repo_key` | session/work-process | same as `mt_cache` | single-slot (one repo at a time) | overwritten, never explicitly invalidated except via `invalidate_cache( )` |
| Pack decode buffers (`resolve_streaming`'s `ct_meta`/`ct_write_batch`) | method-local (D1/D2 owned) | one `resolve_streaming` call | bounded (`c_batch_size` for write batches; `ct_meta` sized to the pack's own object count) | flushed periodically; correctly bounded per D1/D2 design |
| `zcl_abapgit_ortec_base_cache` (LRU) | process-global singleton | life of the ABAP session | 256 MiB byte budget, LRU-evicted | correctly bounded (Package D0/D1 verified) |

**Root of the SYSTEM_NO_ROLL:** the `populate_cache`/`mt_cache` combination is
the one memory structure in this call chain with **no bound at all**, and it
was reached at the START of `get_reachable_objects` before any of the
correctly-bounded structures above ever got a chance to matter.

## 6. SQL multiplicity model

| Call site | Statement shape | Multiplicity vs. K/N |
| --- | --- | --- |
| `populate_cache` | `SELECT * ... WHERE repo_key = X AND status = 'R' ORDER BY obj_sha1` | **O(N)** — every ready row for the WHOLE repo, regardless of the actual K needed. Confirmed 54,226 rows resident for repo `288c81fc1cad` at crash time (and the repo's live `ZAOG_OBJ_STORE` total for that repo_key is 87,486 'R' rows + 132,963 orphaned 'D' rows as of this writing — see §7). |
| `get_reachable_objects`'s own per-level `get_objects(iv_bulk_fetch=True)` calls (commit, then each tree level, then blobs) | one unchunked `read_object_rows` call per level, `WHERE ... obj_sha1 IN (<level's own SHA1 set>)` | **O(K)** per level, correctly bounded — NOT the defect. |
| `ensure_available`'s `get_missing_sha1s` (Steps 1 and 4) | one bulk set-based lookup each | **O(K)** — correctly bounded to the caller's actual missing-SHA1 set. |
| `ensure_available`'s `upload_pack_by_commit` (Step 2) | ONE HTTP call, but the fetched PACK's own object count is driven by "this commit's full reachable graph", not `it_sha1s` | **HTTP call count = O(1)** (correct), but **fetched-object count = O(N)** relative to the caller's narrow K-object need — this is the disproportion. |
| `preload_delta_rows` (inside the resulting oversized pack's decode) | chunked `get_staged_delta_objects`, `c_select_package_size`-bounded | **O(K/chunk_size)** statements — correctly bounded per-statement, but K itself (132,963 delta rows in the one oversized pack) is abnormally large because of the upstream disproportion in Step 2, not because this method reintroduced per-object SQL. |
| `resolve_one_meta`'s per-row `get_staged_delta_objects` | mt_cache lookup, 0 SQL on a warm hit | **O(1) SQL per pack (amortized)** when the preceding bulk preload is exhaustive — confirmed sound by source re-read. |

**K-not-N verdict:** `get_reachable_objects`'s `populate_cache` call is a
direct, literal violation of "no repository-wide read used for incremental
work" (O(N), not O(K)). `ensure_available`'s Step 2 is a related, distinct
violation of the same principle at the HTTP/pack-fetch layer: the *number of
HTTP calls* is O(1) (correct), but the *number of objects fetched per call* is
O(N) relative to the caller's true K-object need for a Stage-By-Filter top-up.

## 7. Post-crash persisted/session-state analysis

Read-only `ZAOG_OBJ_STORE` inspection for the incident repo (`repo_key =
'288c81fc1cad'`, branch `refs/heads/development/6.0.x`, commit
`81157b1448b4183f38403ec63caad1291a2226a4`):

```text
ZAOG_COMMIT_HIST (this repo/branch/commit):
  HIST_LEVEL  = F  (FULL_COMPLETE)
  SNAP_STATE  = C  (COMPLETE)
  ATTEMPT_ID  = 001DD8B728C21FE1A2CF00A1B5615129
  FETCHED_AT  = 2026-07-28 11:30:59
  VERIFIED_AT = 2026-07-28 11:34:35
```

This commit was **already fully certified** (F/C) roughly 2.5 minutes BEFORE
the SYSTEM_NO_ROLL crash (11:37:04) and ~15.5 minutes before the TIME_OUT
crash (11:50:09) — directly explaining why the FIRST attempt took the
`serve_cached_when_nothing_new` "nothing new" shortcut (the server correctly
reported no new commits for an already-certified tip) and crashed inside the
unbounded `populate_cache` reachability-cache pre-load.

`ZAOG_OBJ_STORE` breakdown for the same repo_key, by `pack_id`:

```text
pack_id 288c81fc1cad20260728113102.65265  (11:31:02, part of the FIRST,
    successful, pre-crash fetch/materialization):
  status=R obj_type=tree   26,547 rows
  status=R obj_type=commit    256 rows
  (plus ~40 additional smaller-timestamped pack_ids between 11:32 and 11:34,
  each contributing 200-1,000 status=R blob rows - all part of the same
  successful initial materialization that produced the F/C certificate above)

pack_id 288c81fc1cad20260728113923.77169  (11:39:23 - AFTER the SYSTEM_NO_ROLL
    crash at 11:37:04, BEFORE the TIME_OUT crash at 11:50:09 - this is the
    RETRY's own pack):
  status=D obj_type=ref_d  132,963 rows   <- ORPHANED, still staged/unresolved
  status=R obj_type=blob    24,111 rows
  status=R obj_type=commit   3,555 rows
  status=R obj_type=tree     2,290 rows
  TOTAL for this one pack: 162,919 objects
```

**This is direct, measured confirmation of the TIME_OUT's scale:** the retry's
`ensure_available`-triggered fetch produced ONE pack containing 162,919
objects for a SINGLE commit top-up request, of which 132,963 (81.6%) are
still-unresolved REF_DELTA rows at the moment `resolve_streaming`'s work was
aborted by the runtime limit. This single pack is comparable in size to this
repo's ENTIRE first successful fetch (~87,486 'R' rows across ~45 smaller
packs) — strong, concrete evidence that `ensure_available`'s unfiltered
`upload_pack_by_commit` re-fetched close to the commit's *entire* reachable
object set, not a narrow top-up of the Stage-By-Filter's actual missing blobs.

**Orphan confirmation:** those 132,963 `status='D'` rows are, as of this
writing, still present and unresolved in `ZAOG_OBJ_STORE` — they were never
cleaned up. This is because `cleanup_incomplete`'s `status IN ('I','D')`
DELETE (Package D2's own crash-safety mechanism) is only reached from an
explicit ABAP `CATCH` block inside `decode_streaming`; a hard runtime-limit
TIME_OUT is an uncaught kernel-level termination, not a caught ABAP exception,
so that CATCH block never runs. **This is a genuine, currently-live gap**: D2's
crash-safety design correctly handles caught exceptions but does not (and,
by the nature of a TIME_OUT/SYSTEM_NO_ROLL dump, structurally cannot) run its
own cleanup for a hard runtime abort. These rows are invisible to every normal
read path (`get_object`/`get_objects`/`get_present_sha1s`/`get_missing_sha1s`
all filter `status = 'R'` only — confirmed unaffected, no correctness risk to
current reads) but they are dead, orphaned storage that a future attempt for
the SAME repo will not automatically reclaim (a new attempt mints a new
`pack_id`, so the old `'D'` rows are never matched by any future
`cleanup_incomplete` DELETE, whose predicate is `pack_id`-scoped).

**Session/static-cache state (ABAP internal-session):** `mt_cache`/
`mv_full_cache_repo_key`/`mv_cache_repo_key` are `CLASS-DATA` (session-scoped,
not DB-persistent). A SYSTEM_NO_ROLL dump terminates the current dialog
work-process context; the owner explicitly restarted abapGit before retrying,
which starts a fresh session — so these session-static caches were verifiably
EMPTY at the start of the retry (no stale cache carried over). This is
**CONFIRMED, not inferred**: it directly explains why the retry took the
`decode_streaming` branch instead of `serve_cached_when_nothing_new` again —
NOT because of any left-over "bad" state from the crash, but because a fresh
session naturally re-evaluates from a cold cache and — since the target
commit had never had its OWN pack decoded in this new session — `upload_pack`
routed to the ordinary decode path this time. No SAP table-buffer, enqueue
lock, or HTTP/session-state artifact of the first crash was found to have
influenced the retry; the divergence is fully explained by (a) an intentional
abapGit restart clearing session-local ABAP static memory and (b) the two
requests naturally hitting different internal branches of `upload_pack` for
unrelated structural reasons (the server-side "nothing new" advertisement
depends on request shape/haves, not on any corrupted local state). No SAP
database inconsistency claim is made or needed to explain the sequence.

`ZAOG_FETCH_SESS` / `ZAOG_PACK_META`: not populated for this incident's
decode path — confirmed by source (the live streaming decoder,
`zcl_abapgit_ortec_pack_stream`, deliberately never writes these two tables;
they are legacy-resumable-session-path-only, per the D2 design's own M-4
finding). No orphan rows expected or found there for this repo.

`ZAOG_REPO_STATE`: not queried directly in this pass (no additional risk
signal expected beyond the already-examined `ZAOG_COMMIT_HIST` F/C state,
which is the authoritative gate for branch-pointer publication); not a
blocking evidence gap for this incident's two confirmed root causes.

No stale enqueue lock was observed or is implicated: neither dump's call
stack passes through `acquire_repo_lock`/`release_repo_lock` at all (Stage-
By-Filter's `ensure_available` path does not acquire the D2 repo lock — that
lock is scoped to the two D2 publication units, `pull_by_branch` in
`zcl_abapgit_ortec_fastpath`/`zcl_abapgit_ortec_porcelain`, neither of which
appears on either stack).

## 8. Neutral hypothesis matrix

| ID | Candidate | Verdict | Evidence |
| --- | --- | --- | --- |
| M1 | Unbounded payload/internal-table accumulation | **CONFIRMED** | `populate_cache`'s `SELECT * ... status = 'R'` with no row/byte limit; `LT_ROWS[54226x280]` resident at crash time; §3.1, §4.1 |
| M2 | Duplicate pack/payload/XSTRING residency | **CONFIRMED (contributing)** | The retry's pack (162,919 objects, pack_id `...77169`) substantially re-fetched objects whose content-addressed equivalents were already resident from the first successful fetch (~87,486 'R' rows, same repo) — a large-scale duplicate materialization, not merely a duplicate in-memory copy within one call. See §7. |
| M3 | Process-global or attempt cache not cleared/bounded | **CONFIRMED** | `mt_cache`/`mv_full_cache_repo_key` (`CLASS-DATA`) have no LRU/byte bound, unlike `zcl_abapgit_ortec_base_cache`'s 256 MiB budget; `populate_cache` is the only writer that ever fully clears it, and only on a repo-key mismatch. §5 |
| M4 | Cold-branch materialization batch too large | **CONFIRMED** | `ensure_available`'s single `upload_pack_by_commit(deepen=1)` call fetched a 162,919-object pack for what should have been a narrow, filtered top-up; §4.2, §7 |
| M5 | Repository-wide read on an incremental/filter path | **CONFIRMED (two distinct instances)** | (a) `populate_cache`'s full-table SELECT inside a per-commit reachability lookup; (b) `ensure_available`'s unfiltered whole-commit fetch inside a Stage-By-Filter (inherently narrow, filtered) blob top-up. §4.1, §4.2 |
| M6 | SELECT/GET_OBJECT singleton call inside a large loop | **CONTRADICTED** | Direct source re-read of `preload_delta_rows`/`resolve_one_meta`/`get_staged_delta_objects` confirms the D1/D2 bulk-preload-then-cache-hit design is intact and correctly used; the TIME_OUT's cost is cumulative bounded-chunk SQL across an abnormally large pack (M4), not a reintroduced per-object SQL call. §4.3 |
| M7 | Staged D-row cache misses causing per-delta DB fallback | **NOT_VERIFIED (not the primary cause)** | No source evidence of a cache-warming defect; `get_staged_delta_objects`'s cache-hit check correctly admits `status IN ('D','R')` per the D2 design. Could not be fully excluded as a minor secondary amplifier without a live SQL trace (`ST05`), but is not needed to explain the observed TIME_OUT given M4 alone is sufficient and measured. |
| M8 | Index/cache invalidation after failed attempt causing a cold rebuild | **CONTRADICTED as the cause of the divergence; CONFIRMED as a real but separate orphan-accumulation defect** | The retry's different branch (decode vs. serve-cached) is explained by a legitimate session restart (§7), not by any invalidation bug. However, the 132,963 orphaned `status='D'` rows from the aborted retry (§7) are a real, currently-live artifact of a genuine crash-cleanup gap (hard runtime abort bypasses `cleanup_incomplete`'s CATCH-block trigger). |
| M9 | Missing or unsuitable DDIC secondary index | **NOT_APPLICABLE** | No slow-index symptom observed; the defect is unbounded SELECT SCOPE (no WHERE-clause row limit), not an index-selectivity problem — even a perfect index on `(repo_key, status)` would still return all 87,486+ matching rows for `populate_cache`'s query. |
| M10 | Lock contention or retry/fallback path amplification | **NOT_APPLICABLE** | Neither dump's stack touches `acquire_repo_lock`/`release_repo_lock`; Stage-By-Filter's `ensure_available` path does not participate in D2's lock/attempt orchestration at all. §7 |
| M11 | Package E consumer-coherence/status path triggering unnecessary work | **CONFIRMED (structural precondition, not the mechanism)** | `ZCL_ABAPGIT_ORTEC_MISSING_OBJ=>ENSURE_AVAILABLE` is on both stacks, exactly the method state.md already flags as one "normal certified current-tip consumers" must be prevented from using — Stage-By-Filter on a cold branch is triggering it as a frequent, large-scale path rather than the rare top-up it was designed for. The actual crash mechanisms (M1/M4/M5) live one level below this call, in `get_reachable_objects`/`ensure_available`'s own fetch-shape choices. |
| M12 | Unrelated SAP resource/configuration limit | **CONTRADICTED** | `rdisp/scheduler/prio_high/max_runtime = 660s` is a standard, unmodified dialog runtime limit; the memory limit that tripped SYSTEM_NO_ROLL is the normal MM/heap ceiling, not a mis-configured artificially low value — both are being hit because of genuinely excessive, unbounded work, not because the limits themselves are unusual. |

## 9. Root cause(s) ranked by confidence

1. **(Highest confidence, directly measured) `zcl_abapgit_ortec_obj_store=>
   populate_cache`, called unconditionally from `get_reachable_objects`,
   performs an unbounded, repository-wide `SELECT * ... status = 'R'` with no
   row/byte limit — the direct, sole cause of the SYSTEM_NO_ROLL dump.**
   Confirmed by termination-point source match, `LT_ROWS[54226x280]`, and
   ~3.96 GB resident memory at crash time.
2. **(High confidence, directly measured) `zcl_abapgit_ortec_missing_obj=>
   ensure_available`'s `upload_pack_by_commit(deepen=1)` fetch is unfiltered
   and unbounded relative to the caller's actual K-object need, producing a
   162,919-object pack for a single Stage-By-Filter top-up — the primary
   contributor to the TIME_OUT dump**, compounded by the resulting pack's
   132,963 delta rows needing bounded-but-cumulatively-expensive chunked SQL
   and recursive delta-application work inside the (structurally sound) D1/D2
   resolver.
3. **(Confirmed, secondary/derivative finding) 132,963 `status='D'` rows from
   the aborted retry (`pack_id 288c81fc1cad20260728113923.77169`) are
   permanently orphaned** because a hard runtime-limit TIME_OUT bypasses the
   ABAP `CATCH`-block-triggered `cleanup_incomplete` call. This does not
   cause either crash but is a real, live, currently-uncorrected data-hygiene
   defect this incident exposed.

No evidence supports a database-buffer inconsistency, a lock/enqueue defect,
a DDIC index problem, or a reintroduction of D1/D2's already-fixed per-object
SQL pattern.

## 10. Exact fix scope

### 10.1 Implemented in this session (minimal, mechanical, low-risk — CONFIRMED root cause #1)

```text
File:   src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap
Method: get_reachable_objects
Change: removed the single `populate_cache( iv_repo_key ).` call (and its
        now-inapplicable comment) from the top of the method. No other
        line of this method changed. The method's own pre-existing,
        correctly-bounded per-level get_objects( iv_bulk_fetch = abap_true )
        calls are unchanged and remain the sole source of object data.
Why safe: get_objects already handles a session-cache miss with its own
        bounded, correctly-scoped DB read (read_object_rows, scoped to
        exactly that call's own it_sha1s) - confirmed by direct source
        read (see src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap,
        method get_objects). The sibling method get_reachable_sha1s
        already proves this exact pattern is sufficient without any
        populate_cache pre-load. No change to get_objects/get_object/
        get_available_objects/get_staged_delta_objects/read_object_rows.
Not touched: populate_cache/is_cache_valid/mv_full_cache_repo_key remain
        (still used by the separate get_all_objects method, out of this
        incident's confirmed call-stack scope - not touched to avoid an
        unscoped change; flagged in §12/§13 for a future, separately
        reviewed fix since get_all_objects' own doc comment already
        self-documents the same "small repos only" caveat this incident
        proves is unenforced).
```

### 10.2 Confirmed but NOT implemented in this session — requires senior implementation + protocol/persistence review (root cause #2)

```text
File:      src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.abap
Method:    ensure_available
Required design decision: how to fetch ONLY the caller's actual missing
  blob SHA1s (it_sha1s) instead of the whole commit's reachable graph.
  Candidate directions (NOT decided here - this is a genuine protocol-shape
  question requiring the mandatory correctness + protocol/persistence
  review gate before implementation, per this project's own workflow
  rules):
    (a) request specific blob OIDs directly via `want <blob-sha1>` for each
        missing blob (git's wire protocol permits `want`-ing any object,
        not only ref tips) instead of `want <commit-sha1>` + deepen=1;
    (b) or bound Step 2's fetch to only the tree paths the caller's
        it_sha1s actually correspond to, if a pathspec-narrowed fetch mode
        is available;
    (c) or reject/cap ensure_available's usage pattern itself so Stage-By-
        Filter is not routed through it at all for a large K, deferring to
        Package E's already-planned "prevent normal certified current-tip
        consumers from using ENSURE_AVAILABLE" invariant.
  This is intentionally NOT implemented in this incident-response turn:
  changing what is requested over the git wire protocol is a correctness-
  critical, protocol-shape decision that must go through this project's
  own mandatory gated workflow (correctness review, protocol/persistence
  review, performance DESIGN_GATE) before any code changes, exactly as
  the run brief's own "mandatory live-system orientation"/"fix constraints"
  sections require. Recommended next step: a dedicated senior-implementation
  design pass scoped exactly to this method and its one call site
  (build_files_from_rows), owned as a cross-package/Package-E-adjacent fix
  per §12.
```

### 10.3 Confirmed but NOT implemented — recommended follow-up (finding #3, orphan cleanup)

```text
Recommendation only, not implemented: extend crash-recovery cleanup so a
  FUTURE attempt (or a scheduled maintenance job) can reclaim status='D'
  rows whose pack_id is older than a bounded age threshold, mirroring
  zcl_abapgit_ortec_mat_state=>clean_incomplete_attempts' existing
  age-gated, set-based UPDATE pattern - not scoped or implemented in this
  incident response, since it requires the same age/threshold design
  decision that method already embodies and is not a crash-blocking fix
  (orphaned 'D' rows are invisible to every correctness-relevant read path;
  they are a cleanliness/storage concern, not a live-crash cause).
```

## 11. Tests and live-retest plan

### 11.1 Implemented in this session

```text
File:   src/ortec/git/zcl_abapgit_ortec_obj_store.clas.testclasses.abap
Test:   reachable_ignores_extra_ready (29 chars)
Asserts: get_reachable_objects, when the SAME repo_key already has many
  (50, simulating "several other branches already loaded") unrelated
  status='R' rows present, still returns EXACTLY the commit's own 3-object
  reachable set (commit + tree + blob) - locking in that the per-level
  get_objects walk, not a full-repo preload, is what drives the result.
  This is a correctness/regression anchor for the fix in §10.1; it does not
  (and, given this test class's real-DB-backed harness with no SQL-call-
  counting seam, cannot within this class's existing test infrastructure)
  directly assert "zero rows beyond the reachable set were read from the
  DB" - that stronger claim is proven by the source-level fix itself
  (populate_cache is no longer called at all in this method) and by this
  test failing to fabricate any way for a would-be reintroduction of a
  full-repo preload to silently pass unnoticed if it changed the returned
  object set.
```

### 11.2 Recommended for the deferred fix (§10.2), NOT implemented here

```text
- large loop with DB/HTTP call-count assertion: assert ensure_available's
  Step 2 fetch is bounded by (or proportional to) it_sha1s' cardinality,
  not by the target commit's total reachable object count, for a
  synthetic commit with e.g. 40,000 objects but only ~100 missing blobs
  (matches the run brief's "100 affected objects with 1,000,000 stored
  keys" scale scenario).
- Stage-By-Filter on a cold, mostly-shared branch: end-to-end scenario
  reproducing this incident's exact workflow once the ensure_available fix
  lands, asserting the resulting pack's object count is proportional to
  the filter's actual missing set, not the commit's full tree.
- orphan I/D cleanup: a test proving a bounded, age-gated sweep reclaims
  stale status='D' rows whose owning pack_id is old and was never
  completed (§10.3 follow-up).
```

### 11.3 Live IT8 retest plan (owner-executed)

```text
1. Import this session's one selective commit (§14) into IT8.
2. Re-run the EXACT reported workflow: open several other branches first
   (to reproduce "already loaded"), then Stage-By-Filter on the SAME cold
   branch (refs/heads/development/6.0.x) that produced this incident.
3. Confirm: no SYSTEM_NO_ROLL. Populate_cache's removal directly and fully
   addresses this crash class for get_reachable_objects specifically; a
   distinct, NOT-yet-fixed risk remains for get_all_objects if any live
   caller reaches it for a large repo (see §12/§13 - out of this fix's
   scope, flagged for follow-up, not reproduced by the reported workflow).
4. The TIME_OUT is NOT expected to be resolved by this session's fix alone
   - ensure_available's disproportionate fetch (§10.2) is a separate,
   not-yet-implemented root cause. Do NOT mark D2_STATUS resolved for the
   TIME_OUT symptom until that fix lands and is retested.
5. Manually confirm via read-only query whether the 132,963 orphaned
   status='D' rows for pack_id 288c81fc1cad20260728113923.77169 are still
   present (they will be, until the §10.3 follow-up lands or the rows are
   otherwise reclaimed) - this is expected and non-blocking.
```

## 12. Package ownership classification

```text
Root cause #1 (populate_cache / get_reachable_objects):
  OWNERSHIP=CROSS_PACKAGE
  This method pre-dates Package D entirely (neither D1 nor D2 touched
  get_reachable_objects/populate_cache - confirmed via git history and the
  D1/D2 implementation maps' exact file/method scope lists, neither of
  which lists this method). It is reachable, however, only via a call
  chain D2/Package-E-adjacent code (ensure_available, Stage-By-Filter)
  newly exercises at production scale. Classified cross-package rather
  than D1/D2/Package-C/Package-E specifically, since the defect itself
  lives in pre-existing shared infrastructure, not in any package's own
  newly-written code.

Root cause #2 (ensure_available's unfiltered fetch):
  OWNERSHIP=PACKAGE_E (pulled forward by severity)
  ZCL_ABAPGIT_ORTEC_MISSING_OBJ=>ENSURE_AVAILABLE is explicitly named in
  .memory/state.md as a Package E-owned concern ("Package E must prevent
  normal certified current-tip consumers from using
  ZCL_ABAPGIT_ORTEC_MISSING_OBJ=>ENSURE_AVAILABLE"). This incident proves
  the underlying crash risk is live and production-blocking NOW, not only
  a future Package E design nicety - per the run brief's own instruction,
  "a crash or unusable Stage-By-Filter path is blocking even if the code
  belongs to future Package E scope. Package sequencing must not defer a
  confirmed production crash." This should be pulled forward and
  implemented as a dedicated, gated fix BEFORE full Package E
  implementation begins, not folded into or blocking generic Package E
  scope-creep.

Finding #3 (orphan 'D' rows):
  OWNERSHIP=D2 (documented, non-blocking follow-up)
  D2's own design already anticipated a caught-exception crash window
  (variant_b_package_d_design.md Section "Failure/crash windows") but did
  not anticipate an UNCAUGHT hard runtime-limit abort bypassing the same
  cleanup trigger. This is a D2-owned gap in the crash-safety design,
  not a new defect introduced by this incident's changes.
```

## 13. Evidence limitations

```text
- No live SAT/ST05 SQL trace was captured for the TIME_OUT run; the "many
  individual database accesses" observation is corroborated by (a) the
  owner's direct SM50 observation, (b) the measured 132,963-row delta
  count in the retry's pack (proving the chunked-SELECT count was
  genuinely large, on the order of 130+ statements at
  c_select_package_size granularity), and (c) confirmed-sound source
  re-read of the bulk-preload/cache-hit design - but the EXACT statement-
  by-statement count and per-statement duration were not measured
  end-to-end with a trace tool in this session.
- get_all_objects' own populate_cache call site (line ~1113) was
  identified as a related, NOT-yet-triggered risk (its own doc comment:
  "preload small repos < 55K, handle large separately" - a caveat that is
  not enforced in code) but no caller of get_all_objects appears on
  either incident dump's call stack; it was deliberately left untouched
  to keep this fix scoped to the confirmed, measured crash path.
- ensure_available's disproportionate-fetch root cause (§10.2) is
  confirmed via source-level request-shape analysis and the measured
  162,919-object pack size, but the EXACT git wire-protocol capability
  needed for a correctly-scoped fix (whether arbitrary blob `want`s are
  supported by this project's server-side assumptions, or whether a
  pathspec-narrowed fetch mode already exists elsewhere in this codebase)
  was not fully re-verified against zcl_abapgit_ortec_fetch_neg/
  have_policy in this session, per the effort/scope boundary of an
  incident-response pass versus a full design pass. This is why root
  cause #2's fix is deliberately deferred to a dedicated, gated
  senior-implementation + protocol-review pass rather than implemented
  here.
- ZAOG_REPO_STATE was not directly queried for this repo in this pass
  (not needed to confirm either root cause; the ZAOG_COMMIT_HIST F/C state
  already provides the authoritative branch-pointer/certification
  evidence needed for §7's analysis).
- This incident's fix (§10.1) was validated via get_errors (0 syntax
  errors) and direct line-by-line review only in this session; live IT8
  ABAP Unit/ATC/activation re-verification of the new test and the
  modified method is the owner's next action (see final response NEXT
  field) and has not yet been performed.
```

## 14. SAP validation closeout

### SYSTEM_NO_ROLL

```text
STATUS=SAP_VALIDATED_RESOLVED
FIX=remove unbounded POPULATE_CACHE preload from GET_REACHABLE_OBJECTS
LIVE_RESULT=not reproduced after fix
PACKAGE_D_D2_VALIDATED_HEAD=733bb30799886ef8659be7e293b82c3ccfcebbdd
```

### TIME_OUT

```text
STATUS=SAP_VALIDATED_RESOLVED
FIX=bound ENSURE_AVAILABLE remote top-up to caller missing SHA set through adaptive MATERIALIZE_BLOBS batching
LIVE_RESULT=not reproduced after fix
PACKAGE_D_D2_VALIDATED_HEAD=733bb30799886ef8659be7e293b82c3ccfcebbdd
```

Both incidents were confirmed non-reproducing on the live IT8 system on the
same retest that also confirmed `DBSQL_STMNT_TOO_LARGE` fixed - see
[.memory/incidents/variant_b_d2_it8_dbsql_stmt_too_large.md](.memory/incidents/variant_b_d2_it8_dbsql_stmt_too_large.md)
and the follow-up SAT trace,
[.memory/incidents/variant_b_d2_sat_warm_to_cold_o4h8794.md](.memory/incidents/variant_b_d2_sat_warm_to_cold_o4h8794.md),
which independently confirms neither fix's code path contributes any
measurable cost on the warm/already-materialized case.
