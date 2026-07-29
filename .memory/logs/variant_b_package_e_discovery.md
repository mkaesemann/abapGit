# Variant B Package E — Discovery (E0)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=E0-DISCOVERY
BASELINE=8eef0b55fb37892c3d6b6428c038886481c73192 (memory HEAD)
PRODUCTIVE_BASELINE=733bb30799886ef8659be7e293b82c3ccfcebbdd (SAP_VALIDATED_COMPLETE)
STATUS=DISCOVERY_COMPLETE
```

Method: direct current-source re-read of every cited method (not discovery
prose, not the stale Package E draft) plus the existing SAT/incident
artifacts. No live SAP session was used in this pass; where live
reproduction is required, that is stated explicitly.

## Verified baselines

```text
PACKAGE_C_VALIDATED_HEAD=29199f629773c676e0eaa2f3a006f5167d304ae8
PACKAGE_D2_SAP_VALIDATED_HEAD=733bb30799886ef8659be7e293b82c3ccfcebbdd
PACKAGE_D2_MEMORY_CLOSEOUT_HEAD=8eef0b55fb37892c3d6b6428c038886481c73192 (= current HEAD)
```

`733bb307..8eef0b55` touches only `.memory/**` (verified via
`git diff --name-status`). No productive drift exists between the SAP-tested
system and this design baseline.

---

## E1 — OBJ_INDEX validity, reuse, and persistence performance

### Call chain (verified by direct read)

```text
zcl_abapgit_ortec_filter_walk=>get_remote_files_for_stage
  -> zcl_abapgit_ortec_obj_index=>get_files_for_filter
       -> ensure_index -> is_index_ready (STRICT: SELECT SINGLE ... WHERE
            obj_type = '$IDX' AND obj_name = '__READY__' AND idx_status = 'R')
       -> [not ready] rebuild_index
            -> acquire_repo_lock (repo-scoped, held for the whole rebuild)
            -> re-check is_index_ready under lock (double-checked locking -
               a concurrent rebuild for the SAME repo+commit cannot race)
            -> DELETE FROM zaog_obj_index WHERE repo_key/commit_sha1
            -> get_objects( commit ) -> decode_commit -> BFS over trees:
                 per frontier level: 1 get_objects( bulk, chunked by
                 c_select_package_size=1000 internally ) for that level's
                 tree SHA1s, decode_tree per node, MODIFY zaog_obj_index
                 FROM TABLE every 1000 accumulated file rows
            -> unconditional completion-marker MODIFY ($IDX/__READY__)
            -> release_repo_lock
       -> select_rows_for_filter (FOR ALL ENTRIES, idx_status='R', exact
            commit_sha1) -> build_files_from_rows (blob content read)
```

### Measured evidence (SAT trace `95C45B828A9B11F1B129001DD8B728C2`, re-verified against
[.memory/incidents/variant_b_d2_sat_warm_to_cold_o4h8794.md](../incidents/variant_b_d2_sat_warm_to_cold_o4h8794.md))

```text
TOTAL_ELAPSED_US=31417756
OBJ_INDEX_REBUILD_US=~9390000 (~29.9% of total)
INDEX_ROWS=~42000 (spread ≤12 rows across 4 indexed commits, ≤0.03%)
WRITE_SHAPE=41 bulk MODIFY packages @ 1000 rows + matching per-frontier-level
  get_objects reads (2 DB operation KINDS per cycle: 1 read of that level's
  tree objects, 1 write of accumulated file rows - not a hidden duplicate
  write; this is the BFS's natural read/write alternation, confirmed by the
  code shape above, not measured via a separate DB trace in this pass)
ZAOG_OBJ_STORE_READS=11, K-bounded (tree objects were already locally
  materialized by cold_init/D2 - no HTTP in this method)
HTTP_REQUESTS=2, ~0.99s total (capability discovery + one negotiated fetch,
  both OUTSIDE rebuild_index - confirmed: rebuild_index issues no HTTP)
```

### Why REBUILD_INDEX ran

`is_index_ready` keys strictly on `(repo_key, commit_sha1)`. The cold branch's
tip is a **commit SHA1 never indexed before** in this repository - there is no
row for it under any key, so `ensure_index` unconditionally takes the
`rebuild_index` path. This is correct-by-design, not a bug: the index is a
per-commit cache and this commit has no cache entry yet.

### Could a valid same-tip index have been reused?

**No** for the exact tip (never indexed before - nothing to reuse at the
commit-key level). **Partially, in principle, at the tree level**: `zaog_obj_index`
already stores `TREE_SHA1` per row (`ls_row-tree_sha1 = <ls_work>-tree_sha1`),
and Git trees are content-addressed - an unchanged subtree between
`development/6.0.x` and `bugfix/O4H-8794-...` has the **identical** `tree_sha1`
regardless of which commit references it. The row-count-similarity proxy
(≤0.03% spread across 4 commits) is consistent with the vast majority of
subtrees being unchanged between these two branches. `rebuild_index` does
**not** attempt this reuse today - it always re-fetches and re-decodes every
tree object reachable from the new commit, even when an existing row set for
the same `tree_sha1` (under a different `commit_sha1`) already exists.

**Verified constraint**: `TREE_SHA1` has **no secondary index** in
`zaog_obj_index.tabl.xml` (confirmed: only `CLIENT+REPO_KEY+COMMIT_SHA1+OBJ_TYPE+OBJ_NAME`
form the primary key; no `DD12V`/`DD17V` secondary index entry exists). A
tree-SHA1-keyed reuse lookup would require either a new non-unique secondary
index (additive, not destructive - existing rows/queries are unaffected) or a
full-table scan, which would be worse than the current per-commit walk for
large tables.

### Are all rows rewritten? Are writes bounded to changed rows?

Yes, all rows for the new commit are (re)written by `rebuild_index` -
`MODIFY ... FROM TABLE` in 1000-row packages is a correct, bounded, chunked
bulk write; it is not a per-row loop. No unbounded read, no per-object SQL/HTTP
inside the walk. The "changed rows only" question only becomes meaningful if
tree-SHA1 reuse is implemented (see below); as implemented today, "all rows"
is inherent to a first-time index build for a never-before-seen commit, not an
inefficiency in the write path itself.

### Delete/replace/marker publication atomicity

`rebuild_index` runs under a single repo-scoped ABAP lock
(`acquire_repo_lock`/`release_repo_lock`) spanning DELETE → walk → MODIFY(s) →
marker MODIFY. There is **no explicit `COMMIT WORK`** inside `rebuild_index`
itself (confirmed by direct read - no `COMMIT WORK` statement appears in the
method). This means the DELETE + all MODIFYs + the marker write are part of
the caller's own LUW and become durable only at the caller's next commit
point, consistent with the rest of ORTEC's transaction-ownership model
(Package D2's ownership map). The ABAP lock, not a DB commit boundary, is what
prevents a second concurrent `rebuild_index` for the same repo+commit from
observing a half-written state - a second caller blocked on the lock will,
after acquiring it, re-check `is_index_ready` and either see the completed
marker (if the first caller's LUW already committed) or redo the walk itself
(if the first caller's LUW has not yet committed, since the marker row is not
yet visible outside that LUW). This is correct but means the enqueue lock
alone - not a DB-visible "committed" checkpoint - is the sole race guard.
**No defect found**; documented for the design's transaction-ownership table.

### Package-size / DBSQL statement-size interaction

`c_select_package_size = 1000` (existing constant, reused by `get_objects`)
governs the read side; the same `1000` literal governs the write side
(`IF lines( lt_rows ) >= 1000`). Both numbers are independent of the
`DBSQL_STMNT_TOO_LARGE` incident's root cause (that incident was an **unbounded
RANGE-table IN-list** built from 40,891 individual SHA1 values in one
statement, fixed in `733bb307` by chunking `read_object_rows`) - `rebuild_index`'s
MODIFY is a true array bulk operation (one exec, N rows), not an expanding
IN-list, so it does not carry the same statement-size risk class. Increasing
the package size (e.g., to 5000/10000) would reduce round trips further but
grows peak memory for `lt_rows` and the array-DML batch size sent to the DB
per call; 1000 is the same convention already used elsewhere in this project
(`c_select_package_size`) and was not shown by the SAT trace to be a bottleneck
by itself (41 packages in 9.39s ≈ 229ms/package, dominated by tree
fetch+decode, not the MODIFY itself).

### E1 disposition

```text
ID=E1-OBJINDEX
ROOT_CAUSE_STATUS=CONFIRMED_CURRENT (structural, not a defect)
FINDING_CLASS=E_SLICE_FINDING (optimization opportunity, not a correctness
  or blocking-performance defect)
VERDICT=ACCEPTABLE_AS_IMPLEMENTED; tree-SHA1-based row reuse across commits
  is a real, evidence-backed optimization candidate but requires an additive
  secondary-index DDIC change and its own dedicated design + performance
  gate - NOT authorized in this Package E slice order (see design §E1).
```

---

## E2 — False Local/Remote MODIFIED consumer coherence

### Exact current paths (verified by direct read)

```text
Standard comparison (UNCHANGED by ORTEC, confirmed no override exists -
  grep for "status_calc"/"calculate_status" under src/ortec/** = 0 matches):
  zcl_abapgit_status_calc=>zif_abapgit_status_calc~calculate_status
    -> ensure_state (indexes it_cur_state, standard abapGit's own persisted
         "last synced" per-file checksum table - unrelated to ORTEC)
    -> process_local -> build_existing:
         rs_result-match = boolc( is_local-file-sha1 = is_remote-sha1 ).
         IF match = TRUE: RETURN (no MODIFIED flag at all, regardless of
           it_state's content or freshness).
         ELSE: compare each side against it_state's cached sha1 (or, if no
           it_state row exists for that path, unconditionally flag BOTH
           lstate and rstate MODIFIED - the "first run"/no-cached-checksum
           fallback).

Filtered/Stage-by-filter remote source:
  zcl_abapgit_ortec_git_facade=>resolve_filtered_remote
    -> zcl_abapgit_ortec_filter_walk=>get_remote_files_for_stage
    -> zcl_abapgit_ortec_obj_index=>get_files_for_filter/build_files_from_rows
       (blob_sha1 taken verbatim from zaog_obj_index rows written by
       rebuild_index directly from THIS commit's decoded tree - see E1)

Unfiltered/full Stage remote source (different code path entirely):
  zcl_abapgit_git_porcelain=>pull_by_branch
    -> zcl_abapgit_ortec_porcelain=>pull_by_branch (routes by scenario)
       -> zcl_abapgit_git_transport=>upload_pack_by_branch
            -> zcl_abapgit_ortec_fastpath=>upload_pack(_by_branch)
                 -> serve_cached_when_nothing_new -> get_reachable_objects
                    (or decode_streaming for a real fetch)
    -> zcl_abapgit_ortec_porcelain's own pull() -> STANDARD abapGit tree/blob
       walk (zcl_abapgit_git_pack decode_tree/decode_blob) over the objects
       ORTEC supplied, producing ty_files_tt with sha1 = the tree-walk's own
       computed blob SHA1 (NOT taken from any ORTEC-side index row)
```

### Ruling out the comparison algorithm itself

`zcl_abapgit_status_calc` is 100% standard, unmodified by ORTEC (confirmed by
source-wide grep). Its logic is airtight for genuinely byte-identical content:
`match = boolc(local_sha1 = remote_sha1)` returns immediately with no MODIFIED
flag whenever the two SHA1 values are equal, **regardless of how stale or
missing the third value (`it_state`, standard abapGit's own cached checksum)
is**. Therefore a false MODIFIED for truly identical content **cannot**
originate in this method - it requires that `is_local-file-sha1` and
`is_remote-sha1` genuinely differ as computed, even though the reporter
believes "no actual delta exists."

### Ruling out the filtered index-build algorithm (for a freshly built index)

`rebuild_index` (E1) writes `blob_sha1` directly from the decoded tree node of
the **exact target commit** under a repo-scoped lock with double-checked
locking - a freshly built index for a given commit cannot, by construction,
contain a blob SHA1 that does not belong to that commit's true tree. A
**stale** index row (one written for an earlier state and never invalidated)
remains a possible cause but was not reproduced or found in this pass; the
existing `get_files_for_filter` "stale rows -> rebuild once and retry" branch
(`CATCH zcx_abapgit_exception. DELETE ... rebuild once`) only fires on an
exception (e.g., a blob genuinely missing from the store), not on a silently
wrong-but-present SHA1 - so it would not self-heal a stale-but-present row.

### Two most probable root-cause categories, ranked by evidence

1. **Standard abapGit serialization/regeneration non-determinism** (a
   pre-existing, ORTEC-independent class of "noise": XML/whitespace/attribute
   ordering differences between what a local ABAP object serializes to today
   versus what was captured in the historical remote blob). This would show
   as a genuine, correctly-detected SHA1 difference that is functionally
   harmless - not a bug in comparison or in ORTEC's object supply, but a
   long-standing general abapGit characteristic. **Not disprovable or
   provable without a live side-by-side byte diff of one affected file.**
2. **Stale `zaog_obj_index` row surviving past its commit's true content**
   (e.g., a row from an earlier, partially-cleaned rebuild, or a marker
   written for a commit whose underlying blob data was later evicted/
   overwritten by an unrelated cache-clear that did not also clear the index
   row). Consistent with the D1 triage's classification. **Not reproduced.**

No source-level defect was found in D1/D2-owned code that could cause this
(re-confirmed, consistent with
[variant_b_package_d_d1_modified_status_triage.md](variant_b_package_d_d1_modified_status_triage.md)).

### Required mismatch matrix

| Scenario | Would `zcl_abapgit_status_calc` show MODIFIED? | Verified by |
| --- | --- | --- |
| Same bytes, same path and mode | No (match=true, early return) | Direct source read |
| Same blob SHA1 but reconstructed metadata differs | No (comparison is SHA1-only, no metadata field is compared for MODIFIED classification) | Direct source read |
| Path normalization difference | Yes, as a genuine ADDED+DELETED pair (different `path+filename` key), not MODIFIED - would look like a rename, not a false MODIFIED | Direct source read (`build_new_local`/`build_new_remote`) |
| Line-ending/serialization difference | Yes, correctly, as a genuine SHA1 difference (not "false" from the algorithm's perspective - the bytes really do differ) | Inference from standard SHA1-based design; not reproduced |
| Stale index row | Yes, if the row's `blob_sha1` no longer matches the commit's true tree - NOT reproduced, most probable ORTEC-side cause | Static plausibility only |
| Wrong commit/tip selected | Yes, if `iv_commit` passed to `get_files_for_filter`/`resolve_filtered_remote` differs from what the user believes is current - NOT reproduced | Static plausibility only |
| Partial filtered snapshot | No direct link found - `select_rows_for_filter` always filters by the exact `iv_commit` passed in | Direct source read |
| Stale local status source | Out of ORTEC scope - `it_cur_state`/local file serialization is 100% standard abapGit | Direct source read |
| Consumer compares different object identities | Only plausible via the "wrong commit/tip" row above | Static plausibility only |
| Real local modification | Correctly detected (standard, unmodified logic) | Direct source read |
| Real remote modification | Correctly detected (standard, unmodified logic) | Direct source read |
| Remote deletion | Handled by `build_new_local`/standard delete-detection; unrelated to MODIFIED | Direct source read |
| Not-buffered/unknown object | `ensure_available`/`get_objects` raise on genuine absence - never silently reported as MODIFIED or deleted | Direct source read (missing_obj class doc, explicit design statement) |

### E2 disposition

```text
ID=E2-CONSUMER-COHERENCE
ROOT_CAUSE_STATUS=NOT_VERIFIED (narrowed, not proven - see hard-stop below)
FINDING_CLASS=E_BLOCKER for a definitive code fix; E_SLICE_FINDING for the
  defensive/diagnostic groundwork that CAN proceed without knowing root cause
HARD_STOP_TRIGGERED=YES, for the corrective fix only (see design/§ hard stops)
```

Per the run brief's own hard-stop list ("false MODIFIED cannot be reproduced
or traced to a first incorrect decision"): the comparison algorithm is
definitively ruled out, and the two remaining candidate causes are
plausible-but-unproven. A corrective code change cannot be authorized without
either (a) a live reproduction with a captured affected file's local SHA1,
remote SHA1, and the exact `zaog_obj_index`/`zaog_obj_store` rows involved, or
(b) explicit owner acceptance that E2 will ship only a diagnostic/telemetry
slice pending that reproduction. See design document for the proposed
diagnostic-only slice and the exact reproduction recipe requested of the
owner.

---

## E3 — Cache Admin repository discovery (F4)

### Current implementation (verified by direct read, `zcl_abapgit_ortec_cache_admin=>get_repo_f4_values`)

```text
1. SELECT * FROM zaog_repo_state -> one F4 row per (repo_key, branch),
   normalizing "refs/heads/<name>" to "<name>".
2. SELECT DISTINCT repo_key FROM zaog_obj_store, for any repo_key NOT already
   present from step 1 -> appended as one synthetic row labeled
   branch_name = '<orphaned cache>', remote_url = '<no repository state>'.
3. SELECT DISTINCT repo_key FROM zaog_commit_hist, for any repo_key still not
   present -> same dedup pattern (read further to confirm the row shape
   matches step 2's convention; not fully re-read in this pass, but the
   loop structure through line ~289 is identical to step 2's).
```

This is **already a repository-discovery union across all three
repository-scoped tables**, not a single-source `ZAOG_REPO_STATE`-only lookup
as the stale draft assumed. The core discoverability gap M-STATE-06/the old
draft worried about (a repository reachable only through partial filtered
access, with no `ZAOG_REPO_STATE` row, disappearing from Cache Admin F4) is
**already closed** by step 2's fallback, since `ZAOG_OBJ_STORE` rows are
written by any access path (filtered or not) that persists objects.

### Remaining considerations

- Steps 2/3 are unfiltered `SELECT DISTINCT repo_key` scans of
  `zaog_obj_store`/`zaog_commit_hist`. The class doc explicitly states this
  class is "off the hot path: never called from Stage/Diff/Patch/fetch" -
  confirmed true (no caller of `get_repo_f4_values` exists outside the Cache
  Admin report, verified by the class doc and by this method's own isolated
  purpose). For a very large multi-repository installation this could still
  be a slow admin-report query, but it is bounded to run only when an admin
  opens the F4 help, not on any correctness-critical or per-request path.
- Deduplication is correct (a `HASHED TABLE ... WITH UNIQUE KEY` build-as-you-go
  set, checked before each append).
- No destructive or state-mutating operation occurs in this method - it is a
  pure read.

### E3 disposition

```text
ID=E3-CACHE-ADMIN-F4
ROOT_CAUSE_STATUS=CONFIRMED_CURRENT (repository discovery is already correct)
FINDING_CLASS=NOT_APPLICABLE for a repository-discovery correctness fix (the
  scenario already works); DEFERRED_NON_BLOCKING for the admin-report query
  cost on very large multi-repo installations (no measurement exists; not
  reported as a problem by the owner)
VERDICT=NOT_REQUIRED as a corrective Package E slice. Retained only as a
  regression-coverage item (confirm existing behavior with tests) since no
  class-local test currently exercises the orphaned-cache/commit-hist-only
  fallback branches (verified: no test method name matching this scenario
  exists in zcl_abapgit_ortec_cache_admin.clas.testclasses.abap's visible
  method list from the incident/audit artifacts; to be confirmed in the
  test-gap sub-slice).
```

---

## E4 — Certified-snapshot repair and adaptive materialization

### Current implementation is already two independent, already-approved repair mechanisms

**Filtered/partial-access repair** (`zcl_abapgit_ortec_missing_obj=>ensure_available`,
D2 TIME_OUT fix, already SAP-validated):

```text
1. local bulk resolve (get_missing_sha1s)
2. if none missing: return (no HTTP)
3. if URL blank or ORTEC opt-in inactive: raise (never silently fetch)
4. zcl_abapgit_ortec_cold_init=>materialize_missing_batches( exactly the
   caller's own missing SHA1 set ) - reuses the SAME adaptive 500/50/1000/2x/
   16 MiB/25 MiB batching primitive as cold init's full snapshot
   materialization (constants confirmed identical, same class)
5. retry local bulk resolve once
6. if still missing: raise (no second repair, no deepen-1 fallback)
```

Call sites (confirmed exhaustive by grep): `zcl_abapgit_ortec_obj_index=>build_files_from_rows`
(filtered Stage) and `zcl_abapgit_ortec_walk_prep=>topup_missing_blobs`
(prewarm/walk-prep). Both are **filtered or best-effort top-up paths**, never
the fully-certified unfiltered consumer.

**Full/unfiltered repair** (`zcl_abapgit_ortec_porcelain=>pull_by_branch`,
pre-dates D2, already production code):

```text
1. normal upload_pack_by_branch + pull()
2. IF pull() raises with text containing 'Walk,' (standard abapGit's own
   "tree not found"/"blob not found" walk exception - confirmed identical
   literal strings exist in both src/git/zcl_abapgit_git_porcelain.clas.abap
   and src/ortec/git/zcl_abapgit_ortec_porcelain.clas.abap) AND ORTEC is
   active for this URL AND a repo_key is known:
     invalidate_all_history( repo_key )  " zcl_abapgit_ortec_repo_state
     COMMIT WORK
     retry upload_pack_by_branch + pull() exactly once
     on any further failure: re-raise the ORIGINAL exception (no second
       repair, no partial invalidation ambiguity)
```

`invalidate_all_history` is intentionally whole-repo (not single-commit)
because haves are shared across all branches of a repo and the code cannot
tell which shared ancestor is actually incomplete - documented in the
method's own inline comment as a deliberate, already-reviewed design choice.

### Does a normal fully-certified unchanged-tip consumer ever reach `ensure_available`?

**No** for the standard/unfiltered path (it never calls `ensure_available` at
all - its own repair mechanism is `invalidate_all_history` + full retry, a
structurally different mechanism). For the **filtered** path, `ensure_available`
is reached on every filtered Stage build whenever the local bulk check finds a
gap, **regardless of whether the tip is "certified"** in the `zcl_abapgit_ortec_mat_state`
sense - because `build_files_from_rows` has no concept of `mat_state`'s
`hist_level`/`snap_state` at all; it only knows "is this blob present or not."
This is not a defect: the filtered/partial-access path was never designed to
require full-graph certification (Package C explicitly scoped
partial-filtered access as intentionally omitting branch/snapshot
publication). It matches the "partial or uncertified flows may still require
the approved bounded missing-set top-up" carve-out the run brief itself
anticipates.

### Already-present state-machine primitives relevant to a future "invalidate on corruption" design

`zcl_abapgit_ortec_mat_state` already has `cs_snap_state-invalid = 'I'` and a
public `invalidate_commit` method (cascades to every `zaog_repo_state` row
whose `fetch_commit` matches, bounded by branch count, single bulk UPDATE) -
this generic building block already exists and is already tested
(`invalidate_commit_resets_row`, `invalidate_commit_cascades`). It is,
however, not currently wired to any "certified but a blob turned out missing"
detection point - only to explicit administrative/repair call sites already
covered above.

### E4 disposition

```text
ID=E4-CERTIFIED-REPAIR
ROOT_CAUSE_STATUS=NOT_APPLICABLE (no unresolved defect - both consumer
  classes already have a working, already-validated repair mechanism)
FINDING_CLASS=NOT_APPLICABLE for new repair-contract code;
  E_SLICE_FINDING (minor) for the brittleness of the 'Walk,' string-match
  trigger and for the absence of class-local regression tests that exercise
  either existing repair path end-to-end
VERDICT=E4_NOT_REQUIRED as new production code. The stale draft's INV-E-01..13
  are RESATISFIED by existing code (INV-E-10..13 byte-for-byte; INV-E-02/03/
  04/05/07/08/09 functionally, via the two mechanisms above) or ruled
  NOT_APPLICABLE to the filtered path's intentionally-uncertified design
  (Package C carve-out). No new CERTIFIED_BUT_MISSING state, certificate
  invalidation trigger, or adaptive-materialization code is authorized or
  needed in Package E.
```

---

## Phase 4 — Cross-workstream and overlooked-defect audit

| Question | Finding | Classification |
| --- | --- | --- |
| OBJ_INDEX rebuild causes false MODIFIED | Not found - a freshly built index directly reflects the true tree for its exact commit (E1/E2 analysis) | NOT_VERIFIED (cannot rule out a STALE row scenario, only a freshly-built one) |
| False MODIFIED causes unnecessary rebuild | No causal link found - `rebuild_index` is only triggered by `is_index_ready`'s marker check, never by a status-comparison result | NOT_APPLICABLE |
| Missing branch state causes rebuild | No - `rebuild_index` triggers on missing/absent index marker for the commit, not on `zaog_repo_state` presence | NOT_APPLICABLE |
| Partial filtered access uses a stale tip | Not found - `iv_commit` is always the caller-supplied exact commit; `select_rows_for_filter` filters by it exactly | NOT_VERIFIED (no reproduction attempted for a race between two callers passing different commits concurrently for the same repo - the repo lock only serializes `rebuild_index`, not `get_files_for_filter` reads) |
| Index readiness/marker semantics permit a stale index | STRICT mode (default) requires the marker row written as the LAST statement of a complete walk - a partial walk cannot satisfy it. RELAXED mode (`cs_absent_strictness-mode_relaxed`, explicitly documented "benchmark-only... never ships as default") could accept an incomplete index if ever misconfigured to relaxed in production | DEFERRED_NON_BLOCKING (confirm relaxed mode is not enabled in IT8; not verified live in this pass) |
| Same commit index rows can coexist with a different reconstruction contract | Not found | NOT_APPLICABLE |
| Cache Admin F4 is independent from runtime correctness | Confirmed - pure read-only report, no caller outside the admin report | CONFIRMED_CURRENT |
| Current repair code can publish a certificate after incomplete verification | `mark_full_complete` (mat_state) is gated on `hist_level` sequencing and an attempt ID match, per D2's already-reviewed and SAP-validated design; not reopened here (out of E4's now-NOT_REQUIRED scope) | NOT_REOPENED |
| Current consumer paths contain hidden per-object SQL/HTTP | None found in any method read in this pass (`rebuild_index`, `get_files_for_filter`, `ensure_available`, `get_repo_f4_values` all confirmed set-based/bulk) | CONFIRMED_CURRENT |
| Current index rebuild loads payloads unnecessarily | No - `get_objects` for tree objects returns tree data (needed to decode structure); no blob payload is read during index build, only during `build_files_from_rows`'s later, separate call | CONFIRMED_CURRENT |
| Current E changes would reintroduce a resolved D incident | No E-authorized code changes exist yet in this design (E1/E4 both concluded NOT_REQUIRED/DEFERRED for new code; E2 is diagnostic-only pending reproduction; E3 is test-only) | NOT_APPLICABLE |

### Additional scoped source-pattern search (Phase 4 required list)

Searched `src/ortec/**` for: unbounded ranges/FAE inputs, per-object HTTP,
blank-repo-key fallback, secondary-key `sy-tabix` misuse, low-level
`COMMIT WORK` inside a loop. No new instance was found beyond what D1/D2's own
performance audits already documented and resolved
(`733bb307`'s chunked `read_object_rows`, D2's attempt-ID/lock work). No new
`E_BLOCKER` was produced by this search.

## Cross-workstream relationships

- E1 (index rebuild cost) and E2 (false MODIFIED) are **not causally linked**
  in either direction per the analysis above - they were investigated
  together only because both surfaced from the same SAT trace/owner report
  session, not because one causes the other.
- E3 (Cache Admin F4) is fully independent of E1/E2/E4 - a pure
  administrative read model with no interaction with any hot-path consumer.
- E4 (certified repair) already covers both E1's "index" domain (indirectly -
  a corrupted/incomplete index is NOT what E4's mechanisms repair; they repair
  missing **objects**, not missing **index rows** - a stale/corrupt
  `zaog_obj_index` row is NOT self-healing via either existing repair
  mechanism, since neither one re-triggers `rebuild_index`) and E2's
  "consumer" domain (partially - a genuinely missing blob is repaired; a
  present-but-wrong blob_sha1 in an index row is not detected or repaired by
  either mechanism). **This is the one genuine, evidence-backed gap this
  audit identifies**: there is no repair path for "index row present, but
  wrong" as opposed to "object present, but wrong" or "object missing." See
  design for the proposed narrow, diagnostic-first response.

## Overlooked findings summary

```text
OF-1: zaog_obj_index has no self-healing path for a stale-but-present row
  (only "missing" is repaired anywhere in the system). CLASS=E_SLICE_FINDING.
OF-2: 'Walk,' string-match repair trigger in zcl_abapgit_ortec_porcelain is
  brittle (depends on an unchanged literal exception string in TWO files).
  CLASS=DEFERRED_NON_BLOCKING (pre-existing, not introduced by Package E).
OF-3: RELAXED absent-strictness mode for is_index_ready is a live footgun if
  ever enabled outside benchmarking. CLASS=DEFERRED_NON_BLOCKING (verify not
  enabled in IT8; no evidence it is).
OF-4: No class-local test exercises get_repo_f4_values' orphaned-cache/
  commit-hist-only fallback branches. CLASS=E_SLICE_FINDING (test-only).
```

## Addendum — corrective-pass evidence (E0-CORRECTIVE-DESIGN, CR-01..CR-10)

New direct-source findings made during the corrective design pass, not
present in the original discovery above. These do not replace the original
findings; they refine or resolve open questions the original discovery left
unanswered. Full disposition of each is in
[variant_b_package_e_design.md](variant_b_package_e_design.md) (§2, §6, §7).

```text
NEW-1 (refines OF-1/E1-D): `zcl_abapgit_ortec_obj_index=>rebuild_index`'s
  per-file mapping call `zcl_abapgit_filename_logic=>file_to_object` depends
  on caller-supplied `io_dot` (parsed `.abapgit` content) and `iv_devclass`,
  not a fixed per-repository constant (confirmed via direct read,
  `zcl_abapgit_ortec_obj_index.clas.abap` lines ~399-414). This PROVES a
  bare `(repo_key, tree_sha1)` key is UNSAFE for cross-commit index-row
  reuse (E1-D), correcting the original discovery's more tentative
  "plausible... not yet formally proven" framing.
NEW-2 (refines OF-3): `zcl_abapgit_ortec_git_switch.clas.abap`'s
  `cs_absent_strictness-mode` is confirmed, via direct read of lines ~44-58,
  to be a compile-time CONSTANT hardcoded to `mode_strict`, with an existing
  pinning test (`zcl_abapgit_ortec_git_tests` line ~645). This resolves
  OF-3's "verify not enabled in IT8" action item definitively: RELAXED
  cannot be enabled without a code change and redeploy, full stop — no
  further IT8 verification action is needed.
NEW-3 (refines OF-2): the `'Walk,'` literal exists in TWO files — ORTEC's
  own `zcl_abapgit_ortec_porcelain.clas.abap` (raise + catch, same class)
  and standard `zcl_abapgit_git_porcelain.clas.abap`'s own embedded legacy
  fallback copy (raise + catch, also same class) — but direct read of
  `zcl_abapgit_git_porcelain.clas.abap` lines ~525-560 confirms these two
  copies are DISPATCH-EXCLUSIVE at runtime (a clean `RETURN`-based branch on
  `zcl_abapgit_ortec_git_switch=>is_active_for_repo`), not simultaneously-
  live divergent implementations. The real risk is future maintenance drift
  between the two copies, not a current correctness defect. This upgrades
  OF-2's original "brittle" framing to a decided, actionable fix (shared-
  constant extraction in the ORTEC-owned file only) plus a named, separately
  tracked architecture question about the standard file's embedded
  branching (`E-HARDEN-STANDARD-FILE-COUPLING` in `.memory/state.md`).
NEW-4 (new, supports CR-10 outcome #3): `zcl_abapgit_ortec_obj_store=>
  verify_ready_blobs` (line ~1448) is confirmed, via direct read of its
  SELECT list, to be genuinely metadata-only (`SELECT obj_sha1, obj_type`,
  no payload column), and is confirmed live (not dead code) via a call from
  `zcl_abapgit_ortec_cold_init` line ~498. This directly satisfies the
  original Package E scope's "metadata-only final verification" outcome
  with existing code — no new slice was needed for it.
```
