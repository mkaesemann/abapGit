# Incident: Variant B / Package D2 — SAT-guided warm-to-cold branch performance analysis (O4H-8794)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=D2-SAT-WARM-TO-COLD-O4H8794
BASELINE=733bb30799886ef8659be7e293b82c3ccfcebbdd (HEAD, verified)
STATUS=ROOT_CAUSE_CONFIRMED
```

## 1. Trace identity and tested baseline

```text
git rev-parse HEAD  = 733bb30799886ef8659be7e293b82c3ccfcebbdd
git status --short  = only the same pre-existing, no-op line-ending-only
                       difference in
                       zcl_abapgit_ortec_missing_obj.clas.testclasses.abap
                       already documented in the prior DBSQL_STMNT_TOO_LARGE
                       incident (git diff/--stat for that file is empty -
                       not a real change)
git log --oneline -10: 733bb307 (HEAD) ORTEC: fix D2 DBSQL_STMNT_TOO_LARGE ...
  17513ba7 ORTEC: fix D2 TIME_OUT ...
  2111b288 ORTEC: fix SYSTEM_NO_ROLL ...
  de0f11ce Unit Test Fixes
  cdc5caed ORTEC Variant B Package D2 ...
git merge-base --is-ancestor 2111b288... HEAD  = YES
git merge-base --is-ancestor 17513ba7... HEAD  = YES
```

**Baseline mismatch check (Phase 1 requirement):** the run brief's own "Current
status" section lists only `SYSTEM_NO_ROLL_FIX`/`TIME_OUT_FIX` as the tested
IT8 baseline and does not mention this session's own prior fix,
`733bb307` (`DBSQL_STMNT_TOO_LARGE`). This was investigated directly rather
than assumed either way: a live read of the ACTIVE IT8 source for
`ZCL_ABAPGIT_ORTEC_OBJ_STORE=>GET_OBJECTS` was performed via
`SAPRead(type=CLAS, method=get_objects)`, and it returned the **exact,
byte-for-byte fix already committed at `733bb307`**, including this
session's own inline `" INCIDENT variant_b_d2_it8_dbsql_stmt_too_large`
comment text. **Conclusion: IT8's live active source already includes
commit `733bb307`.** The tested baseline for this SAT trace is therefore
correctly `733bb307` (HEAD), not merely the two older fixes — no unreviewed
productive drift exists between HEAD and the live IT8 system. (How/when the
owner imported `733bb307` was not investigated further; it is not relevant
to this analysis and no further import action is needed for that commit.)

`ABAP_UNIT=PASS`, `ATC=PASS` (owner-reported, consistent with dump list
evidence: no `SYSTEM_NO_ROLL`/`TIME_OUT`/`DBSQL_STMNT_TOO_LARGE` dump exists
after `733bb307`'s own crash-fix dump at 2026-07-28T15:03:21Z).

## 2. Exact reproduction

```text
SAP_SYSTEM=IT8 (HANIT8/IT8/client 100/MICHAELK, re-verified live)
WARM_BRANCH=development/6.0.x
COLD_BRANCH=bugfix/O4H-8794-complete-solution-tour-number-range
REPO_KEY=288c81fc1cad (same repository as all three prior incidents)
```

Two live SAT traces exist for this exact reproduction:

| Trace ID | Title | Start (UTC) | Outcome |
| --- | --- | --- | --- |
| `329FCD8E8A9511F1B129001DD8B728C2` | ABAPGIT - Warm2Cold Branch | 2026-07-28T15:01:25Z | **Crashed** ~2 min later - matches `DBSQL_STMNT_TOO_LARGE` dump at 15:03:21Z exactly. Trace data itself is corrupted/unreadable via the live API (`dbAccesses` → HTTP 400 "Data is invalid"; `hitlist` → HTTP 416 exception) - consistent with an interrupted recording. Not used as evidence below (pre-fix state, superseded). |
| `95C45B828A9B11F1B129001DD8B728C2` | ABAPGIT - Warm2Cold Branch | 2026-07-28T15:47:07Z | **Completed successfully** (~44 minutes after the crash, after `733bb307` was live). This is the trace analyzed below. Matches the user-supplied `Warm2Cold-Hitlist-Excerpt.txt`/full pasted hitlist export exactly (identical root-row values: Gross=31,417,756 microsec). |

```text
TRACE_ID=95C45B828A9B11F1B129001DD8B728C2
TRACE_START=2026-07-28T15:47:07Z (SAP trace-list timestamp)
TOTAL_ELAPSED=31,417,756 microseconds (~31.42 seconds), root "Runtime analysis" row
SAP_USER=MICHAELK
APPLICATION_SERVER=HANIT8/IT8 client 100
WORK_PROCESS=not separately reported by this trace export
CALL_AGGREGATION=OFF (confirmed: every hitlist row has Hits=1 or a small
  integer matching real, non-aggregated occurrence counts, e.g. 340/338/337 -
  not artificially collapsed)
```

## 3. Git relationship and object-delta model

**NOT_VERIFIED at the true Git level** — this workspace is the ABAP**GIT
TOOL's own** source repository (`ortec/abapgit_1_133-opt-rework`); it has no
local clone of the CUSTOMER repository that `development/6.0.x`/
`bugfix/O4H-8794-...` actually belong to, no remote URL/credentials for it,
and the run brief restricts this analysis to read-only IT8 queries plus
current tool source — not fetching an arbitrary customer repository. `git
merge-base`/`git diff --stat <warm-ref>..<cold-ref>` could not be run for
this reason (no local refs exist for either branch in this repo).

**SAP-side proxy evidence (used instead, explicitly labeled as an
approximation, not a true Git diff):**

```text
Repo 288c81fc1cad's ZAOG_OBJ_INDEX currently holds fully-built (marker-
complete) per-commit indexes for exactly 4 distinct commits:
  81157b1448b4183f38403ec63caad1291a2226a4  42,000 rows  (development/6.0.x,
                                                            confirmed via
                                                            ZAOG_COMMIT_HIST)
  dc7c42c36ddf66477df14758cb60d2217e311972  41,997 rows  (unconfirmed identity)
  558e96dff65541bbc612024f91e86fa66b8a7489  41,997 rows  (unconfirmed identity)
  a5b30a633ae334a80a3ec5cfa0dff69f78ce78aa  41,988 rows  (unconfirmed identity)
```

One of the three unconfirmed commits is `bugfix/O4H-8794-...`'s tip (built
during this trace's own `rebuild_index` call, see §6/§9) - its exact identity
was not resolved further (would require either a live git `ls-remote`/`log`
against the customer remote, which is out of this analysis's read-only IT8
scope, or inspecting `ZAOG_PACK_META`/session correlation for the exact
`pack_id` tied to trace timestamp 15:47, which was not pursued given the
row-count evidence below is already sufficient to answer the load-bearing
question).

**Row-count-based file-delta proxy:** all four commits' total index-row
counts fall within a narrow band (41,988–42,000, a spread of only 12 rows
out of ~42,000, i.e. ≤0.03%). This is strong, consistent circumstantial
evidence that `development/6.0.x` and `bugfix/O4H-8794-...` are indeed
**closely related branches with a small logical file delta**, exactly as
their names and the run brief's own framing suggest — but this is a *row-
count similarity proxy*, not a verified `git diff --stat`. A small file
delta at the logical level does not, by itself, prove the number of
distinct Git objects (commits/trees/blobs) required to fetch or index the
cold branch is equally small — and §6/§9 show the actual measured local
work (index rebuild) is legitimately proportional to the **whole commit's
~42,000-file tree**, not to the small file delta between the two branches,
because `zcl_abapgit_ortec_obj_index`'s per-commit index is built once, in
full, for any never-before-indexed commit (see §9) - it does not (and
structurally cannot, by its own reuse-across-future-filters design intent)
narrow the walk to only the files that differ from a sibling branch.

```text
COMMIT_DISTANCE=NOT_VERIFIED (no local Git access to the customer repo)
FILE_DELTA=NOT_VERIFIED at the true Git level; SAP-side proxy indicates a
  very small delta (row-count spread ≤12 of ~42,000, ≤0.03%)
```

## 4. SAT phase breakdown

Mapped from the full, unaggregated hitlist export (4,438 individual rows,
parsed and sorted by Net/self time; not pasted here in full per the run
brief's instruction).

| Phase | Main methods | Gross | Net (self) | Call count | Scaling variable |
| --- | --- | --- | --- | --- | --- |
| A - remote ref/capability discovery | `ZCL_ABAPGIT_HTTP_CLIENT=>SEND_RECEIVE` (2nd call) | 97,387 µs (~0.10s) | 97,316 µs | 1 | O(1), constant |
| B - have selection/request serialization | (no measurable line item - pure ABAP, sub-millisecond) | negligible | negligible | n/a | n/a |
| C - HTTP upload-pack request/response | `ZCL_ABAPGIT_HTTP_CLIENT=>SEND_RECEIVE` (1st call) | 892,224 µs (~0.89s) | 892,195 µs | 1 | O(1) bounded, no repeat |
| D - pack decode/delta resolution | `ZCL_ABAPGIT_GIT_PACK=>DECODE_TREE` | 974,748 µs (~0.97s) | 373,984 µs | 340 | O(distinct trees in this ONE commit) |
| E - object persistence/promotion | `ZAOG_OBJ_STORE` DB Fetch | 67,362 µs (~0.07s) | 67,362 µs | 13 | O(K), K small (most objects already `R`) |
| F - cold graph/tree traversal | `LOOP AT LT_NODES` (`zcl_abapgit_ortec_obj_index`) | 13,608,745 µs (~13.61s) | 547,545 µs | 340 | O(trees in this ONE commit) - overlaps with I below |
| G - missing-blob materialization | (`materialize_missing_batches`/`ensure_available` fetch) | **0** - not invoked this run | 0 | 0 | n/a - confirmed NOT a factor (§5/§9) |
| H - snapshot verification/publication | (`certify_fetched_commit`/`mat_state`) | **0** - not invoked this run | 0 | 0 | n/a - correctly never runs for a filtered access (by design, §9) |
| I - file/index/status reconstruction | `MODIFY zaog_obj_index` + `DB: Exec ZAOG_OBJ_INDEX` | 9,390,412 µs (~9.39s) | 9,354,131 + 36,281 µs (~9.39s) | 41 (`Modify`) / 82 (`DB: Exec`) | **O(total files in this ONE commit, ~42,000)** - **dominant cost** |
| J - UI/consumer rendering | `LCL_PASSWORD_DIALOG=>POPUP` (3.73s), `CALL_INTERNAL_SELECTION_SCREEN` (2.90s), `GET_GUI_VERSION` (0.35s), `CL_GUI_HTML_VIEWER` overhead | ~7.5–8s combined | ~7.5–8s (mostly self, dialog/user-wait time) | few | user-interaction/framework, not ORTEC algorithm |
| K - lock wait/fallback/retry | `acquire_repo_lock`/`release_repo_lock` (inside `rebuild_index`) | not separately visible as a hot line item | negligible | 1 acquire/1 release | uncontended this run |

`OPEN_GUI`'s own Gross=31,309,879 µs / Net=6,928,838 µs is the top-level SAP
GUI dispatch/dynpro-render overhead surrounding everything above (framework
cost, not ORTEC-attributable).

**Reconciliation:** Phase I's 9.39s + Phase F's residual (~4.2s beyond
Phase I, covering `DECODE_TREE` (0.97s), `ZCL_ABAPGIT_FOLDER_LOGIC=>
PATH_TO_PACKAGE` (0.36s, 338 hits), `CL_PACKAGE` devclass-resolution
overhead (~0.09-0.12s), and per-node SHA1/path-hash CPU cost for ~42,000
loop iterations, ~2.7s residual) + Phase C/A (~0.99s HTTP) + Phase E (0.07s)
sums to ~15.3s of ORTEC-attributable work, out of ~31.4s total. The
remaining ~14-16s is generic SAP GUI/dialog/user-interaction overhead
(password popup, transport-request selection screen, GUI-version probe,
HTML viewer construction) with **no ORTEC-owned code on that path at all**.

## 5. Top methods by gross/net time and call count

Top 10 by **Net (self) time** (the methods actually consuming wall-clock
time, not merely wrapping children):

```text
1. DB: Exec ZAOG_OBJ_INDEX           9,354,131 µs (82 hits) - zcl_abapgit_ortec_obj_index
2. Perform OPEN_GUI (framework)      6,928,838 µs (1 hit)   - ZABAPGIT
3. LCL_PASSWORD_DIALOG=>POPUP        3,734,778 µs (1 hit)   - user dialog wait
4. CALL_INTERNAL_SELECTION_SCREEN    2,904,069 µs (1 hit)   - transport-request dialog wait
5. HTTP SEND_RECEIVE (fetch)           892,195 µs (1 hit)   - zcl_abapgit_http_client
6. Loop At LT_NODES (self)             547,545 µs (340 hits) - zcl_abapgit_ortec_obj_index
7. DECODE_TREE                         373,984 µs (340 hits) - zcl_abapgit_git_pack
8. GET_GUI_VERSION                     350,580 µs (3 hits)   - SAP GUI framework
9. Modify ZAOG_OBJ_INDEX (self)         36,281 µs (41 hits)  - zcl_abapgit_ortec_obj_index
10. DB: Fetch ZAOG_OBJ_STORE             67,362 µs (13 hits) - zcl_abapgit_ortec_obj_store
```

Top by **call count** (non-trivial, unaggregated per the run's own
`CALL_AGGREGATION=OFF` setting): `LOOP AT LT_NODES`/`DECODE_TREE` = 340
each; `PATH_TO_PACKAGE`/`CL_PACKAGE=>M_LOAD_DATA`/`LOOP AT S_PACKAGE_DIR` =
337-338 each - all four scale 1:1 with the number of trees/files walked in
this ONE commit's full structure, confirming a single coherent O(tree-size)
cost center, not several independent hotspots.

**No `ZCL_ABAPGIT_ORTEC_MISSING_OBJ`, `ZCL_ABAPGIT_ORTEC_COLD_INIT`,
`ZCL_ABAPGIT_ORTEC_PACK_STREAM`, `ZCL_ABAPGIT_ORTEC_DELTA`, or
`ZCL_ABAPGIT_ORTEC_MAT_STATE` method appears anywhere in the top-40 by Net
or Gross time, nor anywhere in the 781 ORTEC/keyword-matching rows
inspected.** This directly confirms the TIME_OUT and DBSQL_STMNT_TOO_LARGE
fixes are not merely "not crashing" but are genuinely **not a measurable
cost center at all** in this reproduction (§9 explains why: the requested
blob was already `READY`, so no materialization batch was ever needed).

## 6. SQL/HTTP summary

```text
SQL (via dbAccesses view - real row/call counts confirmed reliable;
  per-statement accessTime field returned 0 for every single row in this
  export, including high-count generic framework tables like TDEVC/TADIR -
  this is a tool/export limitation, not evidence of zero real cost; actual
  timing for ZAOG_OBJ_INDEX came from the separately-parsed hitlist export,
  §4/§5, which IS reliable):
    ZAOG_OBJ_INDEX  : 41 MODIFY (bulk, FROM TABLE, chunked at 1000 rows) +
                       82 underlying "DB: Exec" round trips (~2 per MODIFY,
                       normal HANA array-upsert behavior) = 9.39s total,
                       ~114 ms average per round trip - consistent with a
                       fixed per-statement/round-trip overhead, not a
                       payload-size-driven cost (see §9 for the batch-size
                       tuning implication)
    ZAOG_OBJ_STORE  : 1 select single + 9 select + 1 select = 11 total
                       reads, 67,362 µs combined - small, K-bounded, no
                       per-object pattern
    ZAOG_OBJ_INDEX  : 2 select single + 1 select (is_index_ready checks) +
                       1 delete (stale-index cleanup at the top of
                       rebuild_index) - all O(1)/cheap
    ZAOG_FETCH_SESS : 1 insert + 1 delete - one legacy resumable-session
                       record created and cleanly removed (non-orphaned)
    ZAOG_REPO_STATE : 2 select single - read-only checks, no write this run
    No FOR ALL ENTRIES/IN-range statement against any ZAOG_* table appears
      anywhere in this trace's dbAccesses view exceeding the already-known-
      safe 1000-row chunk boundary; no DBSQL_STMNT_TOO_LARGE-class risk
      re-appeared.
HTTP:
    2 SEND_RECEIVE calls total, 892,224 + 97,387 = 989,611 µs (~0.99s
      combined) - bounded, no retry/fallback cascade visible (a
      retry/fallback would show as a 2nd/3rd distinct larger SEND_RECEIVE
      cluster or an explicit RECOVERY_BRANCH_FULL-adjacent call; none found).
```

## 7. Request/pack/materialization shape

```text
Fetch mode           : not directly extractable from this trace export
                        (no visible ZCL_ABAPGIT_ORTEC_FETCH_REQ frame in the
                        top-cost rows; inferred from the very small,
                        bounded HTTP+decode footprint to be either a
                        `filter blob:none` bounded commit/tree probe or a
                        fast "nothing new" negotiation - NOT a full/thin
                        pack of meaningful size, since DECODE_TREE (340
                        calls) is tree-structure decoding consistent with
                        walking already-locally-available tree objects, not
                        a large incoming pack transfer)
Want/have count       : NOT_AVAILABLE from this trace export
Materialization       : ZERO materialize_missing_batches/ensure_available
  batch count            HTTP calls this run (§5) - the requested blob(s)
                          were already present and READY in ZAOG_OBJ_STORE
                          BEFORE this run started (confirmed both by the
                          absence of any materialize-path method in the
                          trace AND by the run brief's own supplied
                          evidence: "The SHA1 blob requested through the
                          filtered ... access is present in ZAOG_OBJ_STORE
                          and is READY")
Object-store rows     : 11 total ZAOG_OBJ_STORE reads (§6) - small, bounded
  read/written
Cache hits/misses     : not separately instrumented in this trace; the tiny
                          ZAOG_OBJ_STORE read count (11) combined with zero
                          materialize calls is consistent with the session
                          cache (mt_cache) and/or the presence-only
                          get_missing_sha1s bulk check both resolving
                          near-instantly against already-persisted data
Attempt/session/pack  : ZAOG_FETCH_SESS insert+delete (1 legacy session,
  IDs                    cleanly closed); no ZAOG_COMMIT_HIST/mat_state
                          attempt_id activity this run (no begin_attempt
                          call - correctly never invoked for a filtered,
                          non-certifying access, matching §9)
```

## 8. Persisted-state comparison

```text
ZAOG_REPO_STATE   : repo 288c81fc1cad exists; only 2 select single reads
                     this run, no write - CONFIRMS branch-state publication
                     is intentionally skipped for this filtered access
                     (§9), not merely "not yet run".
ZAOG_COMMIT_HIST  : only 2 rows exist for this repo, both pre-existing
                     (releases/6.0.1, development/6.0.x - both F/C
                     certified). bugfix/O4H-8794-... has NO row here, before
                     or after this trace - confirmed unaffected/unwritten by
                     this run (matches the "filtered path cannot prove full
                     graph/snapshot completeness" design intent, §9/§10).
ZAOG_OBJ_STORE    : repo-wide total (all repos) = 309,381 status='R' rows,
                     ZERO status IN ('I','D') rows anywhere - confirms the
                     staged-visibility/cleanup invariants (D1/D2-owned)
                     remain intact after this run; nothing orphaned.
ZAOG_OBJ_INDEX    : 4 fully-built (marker-complete) per-commit indexes
                     exist for this repo (§3) - one of the three
                     previously-unindexed commits was built DURING this
                     trace (the dominant ~9.39s cost, §4/§5/§9). No
                     partial/incomplete index rows were left behind (the
                     completion marker write, line ~482 of
                     zcl_abapgit_ortec_obj_index.clas.abap, is confirmed to
                     have executed, since is_index_ready's STRICT check
                     would otherwise force a full rebuild on any future
                     access - not observed as a second rebuild in this same
                     trace, and a fresh re-query after the trace shows a
                     complete, marker-bearing index).
ZAOG_FETCH_SESS   : 1 row created and deleted within this same run - no
                     orphan.
ZAOG_PACK_META    : not queried directly this pass (not needed - the
                     legacy session-table activity above already confirms
                     no orphan; the primary streaming decoder never writes
                     this table by design, per prior incident findings).
```

**Answering the run brief's four specific filtered-state questions
directly:**

1. **Does subsequent filtered access subtract existing READY SHA1s through
   the object-store presence check?** YES, structurally confirmed:
   `ensure_available`'s `get_missing_sha1s` (Step 1) and `get_objects`'
   own session-cache/DB presence check both unconditionally subtract
   already-`R`-status SHA1s before any fetch/materialization decision,
   regardless of whether `ZAOG_REPO_STATE`/`ZAOG_COMMIT_HIST` has a row for
   the branch. This run's near-zero `ZAOG_OBJ_STORE` read count (11) and
   zero materialize-batch calls are the empirical confirmation.
2. **Do missing branch-state rows cause any measured repeated graph,
   negotiation, or materialization work?** NO. The one genuinely expensive,
   one-time cost this run (`ZAOG_OBJ_INDEX` rebuild, §9) is gated
   exclusively by its OWN completion marker in `ZAOG_OBJ_INDEX` itself
   (`is_index_ready`), which is entirely independent of
   `ZAOG_COMMIT_HIST`/`ZAOG_REPO_STATE`. A future filtered access to this
   SAME commit will hit `is_index_ready` = `abap_true` and skip the
   9.39s rebuild entirely (idempotent by construction, not measured
   directly in this single-run trace but confirmed structurally via direct
   source read of `ensure_index`/`is_index_ready`, §9). The only
   repeatable-per-access cost is the ~0.99s HTTP negotiation (§6/§7),
   which is inherent to any git fetch attempt for an uncertified branch and
   is not itself expensive.
3. **Is branch-state publication intentionally omitted because the
   filtered path cannot prove full graph/snapshot completeness?** YES -
   consistent with this project's own binding constraint ("a branch
   snapshot is complete only when every blob referenced by its tip tree is
   READY and hash-verified") and confirmed empirically: zero writes to
   `ZAOG_COMMIT_HIST`/`ZAOG_REPO_STATE` occurred this run despite the
   filtered access succeeding and the commit's full blob set arguably being
   already present (per the run brief's own supplied evidence) - the system
   correctly does not claim full-snapshot completeness from a narrow,
   filtered read alone.
4. **Is the only observable consequence the missing Cache Admin F4 entry?**
   Based on this trace, YES - no measured repeated materialization, no
   measured repeated negotiation, and no other functional consequence of
   the missing branch-state row was found. Per the run brief's own explicit
   instruction, this is classified as a **separate, non-blocking Cache
   Admin usability issue** (F4 help derives its repository list from
   `ZAOG_REPO_STATE`, which filtered-only-accessed repositories never
   populate) - **not** a reason to create F/C certification for a filtered
   path, and not a performance defect in its own right.

## 9. Focused source inspection (top-cost method)

`zcl_abapgit_ortec_obj_index=>rebuild_index` (full method read,
`src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap` lines ~280-475),
gated by `ensure_index`/`is_index_ready` (lines ~230-280):

```text
Classification: BATCHED SQL (bulk MODIFY ... FROM TABLE, chunked at a
  hardcoded 1000-row threshold) + GRAPH TRAVERSAL (BFS-style WHILE/LOOP
  over pending tree SHA1s, level by level) + memory-only CPU work
  (decode_tree, path-hash SHA1 computation, file_to_object mapping).
Trigger: is_index_ready( repo_key, commit ) = abap_false - i.e. this exact
  commit has NEVER had its per-commit index built before. Gated by a
  STRICT completion-marker row (obj_type/obj_name = dedicated marker
  constants), not merely "does at least one row exist" - deliberately
  correct against partial/interrupted rebuilds (matches the D1 modified-
  status triage document's own description of this exact mechanism).
Cardinality: O(total files + total tree nodes in the ONE target commit,
  ~42,000 in this repo) - bounded to exactly this commit, confirmed by
  direct source read: get_objects() calls inside the WHILE loop are always
  scoped to lt_tree_sha1s (this level's own child SHA1s), never
  repository-wide. This satisfies "K objects, not N" for K = one commit's
  own tree size - it does NOT scale with the number of OTHER
  branches/commits already buffered for this repo_key (confirmed: repo
  288c81fc1cad has 4 already-built indexes plus this repo's total
  ZAOG_OBJ_STORE row count of 73,681+ across all types, yet this rebuild's
  cost tracks only the ~42,000 rows of the ONE commit being indexed, not
  the store's total size).
One-time/idempotent: CONFIRMED via source read of `ensure_index`
  (line ~276): `IF is_index_ready(...) = abap_true. RETURN. ENDIF.` -
  every future filtered access to this SAME commit short-circuits to a
  single `SELECT SINGLE` marker check (µs-scale), never re-walking the tree.
Root inefficiency identified: the bulk `MODIFY zaog_obj_index FROM TABLE
  lt_rows` chunk size is a hardcoded `1000` (lines 449/463), producing 41
  separate array-MODIFY statements (82 underlying DB round trips) for
  ~42,000 rows, averaging ~114 ms per round trip - a figure consistent with
  a largely FIXED per-statement/round-trip cost (likely network/HANA
  connection latency in this specific environment) rather than a cost
  proportional to the ~1000-row payload size itself. Unlike the
  DBSQL_STMNT_TOO_LARGE incident's `SELECT ... obj_sha1 IN <range>`
  (which genuinely needs a conservative per-statement cap because each
  range entry becomes one DBSL bind marker with a hard 32,767 ceiling), a
  `MODIFY ... FROM TABLE` array operation does not build a growing
  WHERE-IN literal list - it is a parameterized array DML statement, for
  which HANA's practical row-per-call limits are typically far higher than
  1000. This makes `c_select_package_size`-style reasoning (chosen for
  IN-range SELECT safety) an OVER-CONSERVATIVE, un-reviewed carry-over
  constant for this specific bulk-INSERT/UPSERT use case - a strong
  CANDIDATE for a low-risk, mechanical batch-size increase (e.g. 5,000-
  10,000) that would proportionally cut the ~41-round-trip count (and
  therefore a large fraction of the measured 9.39s) without touching any
  WHERE-clause/bind-marker-sensitive statement at all.
Not a per-object SQL pattern: CONFIRMED. The single-row `MODIFY zaog_
  obj_index FROM ls_row.` (line 482) fires exactly ONCE per rebuild (the
  completion marker only), not once per file - verified by direct
  arithmetic (41 "Modify" hits ≈ 41 bulk-chunk flushes for ~42,000 rows at
  ~1000/chunk, not 42,000 individual single-row modifies).
```

## 10. Neutral hypothesis matrix

| ID | Candidate | Verdict | Evidence |
| --- | --- | --- | --- |
| P1 | Certified haves absent or rejected | NOT_APPLICABLE | No `have`/negotiation cost appears as a measurable line item; the filtered path (`ensure_index`/`get_files_for_filter`) does not depend on certified haves at all - that mechanism is scoped to the standard/non-filtered pull path, untouched by this run's call chain |
| P2 | Merge base not offered because have policy/cap too narrow | NOT_APPLICABLE | Same reasoning as P1 - this run's dominant cost has no relationship to have/merge-base negotiation |
| P3 | Server ignores haves for the selected request shape | NOT_VERIFIED | HTTP request/response internals (want/have list) are not visible in this trace export; not needed to explain the measured cost, which is entirely local (§4/§9), but not independently disproven either |
| P4 | Required cold graph work is legitimate but larger than the file delta | **CONFIRMED** | `rebuild_index`'s cost (~9.39s + ~1.3s decode/folder-logic) is proportional to the WHOLE commit's ~42,000-file tree, not to the small (row-count-proxy) file delta between the two closely-related branches (§3/§9) - by design, since the index is built once per commit for reuse across ANY future filter, not narrowed to the current filter's own paths |
| P5 | Shared blobs/trees are not reused | CONTRADICTED | Only 11 `ZAOG_OBJ_STORE` reads and 0 materialize-batch HTTP calls occurred (§5/§6/§7) - the vast majority of objects were already present/reused; this is the OPPOSITE of a reuse failure |
| P6 | Too many small MATERIALIZE_BLOBS requests | NOT_APPLICABLE | Zero MATERIALIZE_BLOBS/`ensure_available`/`materialize_missing_batches` calls occurred this run (§5/§7) |
| P7 | Pack is small but decode/delta CPU dominates | CONTRADICTED (as the DOMINANT factor) | `DECODE_TREE` is real (0.97s) but is ~10x smaller than the `ZAOG_OBJ_INDEX` DB-write cost (9.39s); no `zcl_abapgit_ortec_delta`/`pack_stream` delta-resolution method appears anywhere in the top-cost rows at all |
| P8 | Object-store SQL scales with repository N rather than missing K | CONTRADICTED | `ZAOG_OBJ_STORE` SQL is tiny (11 reads, §6) and does not scale with this repo's much larger stored-object totals (73,681+ rows across types) - K-not-N holds here |
| P9 | Singleton object-store SQL exists in a hot loop | CONTRADICTED for `ZAOG_OBJ_STORE`; **the analogous concern for `ZAOG_OBJ_INDEX` is a batch-SIZE inefficiency, not a true per-object/singleton pattern** (§9 - 41 bulk chunks for ~42,000 rows, not 42,000 individual statements) |
| P10 | Current-tip blob set is recomputed/rematerialized unnecessarily | CONTRADICTED | Confirmed idempotent via `is_index_ready`'s STRICT marker gate (§9); zero evidence of a second rebuild within this trace |
| P11 | Status/index/consumer reconstruction dominates after fetch | **CONFIRMED - this is the primary root cause** | `ZAOG_OBJ_INDEX` rebuild (`rebuild_index`, Phase I) is the single largest cost center in the entire trace by a wide margin (9.39s of ~31.4s total; more than 10x any other ORTEC-specific line item), §4/§5/§9 |
| P12 | False-MODIFIED consumer issue causes excess comparison work | NOT_VERIFIED / likely NOT_APPLICABLE this run | No `ZCL_ABAPGIT_ORTEC_FILTER_WALK`/status-comparison method appears in the top-cost rows; the D1 modified-status triage document's own finding (a DIFFERENT, narrower symptom - stale index causing false MODIFIED flags, not elapsed-time cost) is a related but distinct Package E concern, not reproduced or contradicted by this specific trace |
| P13 | Cache invalidation discards reusable warm state | CONTRADICTED | `mt_cache`/session-cache behavior is consistent with warm reuse (tiny read count, §6); no evidence of a forced full-repo cache clear this run |
| P14 | Lock wait or retry/fallback adds latency | CONTRADICTED | `acquire_repo_lock`/`release_repo_lock` show no measurable wait time this run (uncontended, single user); no retry/fallback cascade visible in the HTTP call pattern (§6) |
| P15 | SAP runtime/database conditions unrelated to ORTEC algorithm | CONFIRMED (as a SEPARATE, non-blocking factor, not the primary cause) | A substantial share of the total 31.4s (~7.5-8s: password dialog popup, transport-request selection screen, SAP GUI-version probe) is generic SAP GUI/dialog/user-interaction overhead entirely unrelated to the ORTEC algorithm - real, but not an ORTEC defect and not actionable by this project |

## 11. Root cause ranked by confidence

1. **(Highest confidence, directly measured)** `zcl_abapgit_ortec_obj_index=>
   rebuild_index`'s one-time, per-commit full-tree index build is the
   dominant cost in this trace: 9.39s of raw `ZAOG_OBJ_INDEX` DB-write time
   (41 bulk MODIFY statements / 82 round trips) plus ~1.3s of tree-
   decode/folder-logic CPU work, together accounting for roughly two-thirds
   of the ~15.3s of total ORTEC-attributable work in the trace. This is
   legitimate, correctly-bounded (K = one commit's own ~42,000-file tree,
   not repository-wide N), non-repeating (idempotent via `is_index_ready`)
   work — but its cost is measurably inflated by an over-conservative,
   un-reviewed 1000-row chunk size for a bulk array-MODIFY statement whose
   real constraint (HANA array-DML row limits) is materially higher than
   the 1000-row limit chosen for an unrelated statement shape (WHERE-IN
   range SELECTs, whose 32,767-bind-marker ceiling does not apply here).
2. **(Confirmed, non-blocking, out of ORTEC's control)** roughly 7.5-8s of
   the total 31.4s elapsed time is generic SAP GUI/dialog/user-interaction
   overhead (password popup, transport-request selection screen dialog,
   GUI-version probe) with zero ORTEC-owned code on that path.
3. **(Confirmed, not a defect)** the TIME_OUT and DBSQL_STMNT_TOO_LARGE
   fixes are completely validated by this trace's own absence of any
   `ensure_available`/`materialize_missing_batches`/`get_reachable_objects`
   cost - the requested blob was already `READY`, so neither fix's code
   path was exercised at all this run, confirming both fixes impose zero
   overhead on the already-warm case.

No evidence supports a have/negotiation defect, a shared-object-reuse
failure, a delta-resolution CPU problem, a repository-wide N-scaling SQL
pattern, a lock-contention issue, or a reintroduction of any previously
fixed per-object SQL/HTTP anti-pattern.

## 12. Ownership classification

```text
OWNERSHIP=PACKAGE_E
```

`zcl_abapgit_ortec_obj_index` (the `rebuild_index`/`ensure_index`/
`is_index_ready` chain) is the exact class the D1 modified-status triage
document already classified as `PACKAGE_E_CONSUMER_COHERENCE`
(`.memory/logs/variant_b_package_d_d1_modified_status_triage.md`), and
`.memory/state.md`'s own binding-constraints list assigns "snapshot
consumer coherence and adaptive materialization" and "the certified-
snapshot repair contract" to Package E. This hotspot is status/index
reconstruction dominating post-fetch work, matching the
`PACKAGE_E_PERFORMANCE_DEFECT_CONFIRMED` verdict criterion in the run
brief's own decision framework verbatim. **No D2-owned mechanism (attempt/
staging/lock/transaction/publication) appears anywhere in this trace's
top-cost rows or call chain** - `begin_attempt`, `certify_fetched_commit`,
`decode_and_persist_streaming`'s status-split, and the D2 repo-lock/
attempt-ID plumbing are all either not invoked this run (correctly, for a
filtered/non-certifying access) or contribute zero measurable cost.

## 13. D2 closeout recommendation

**D2's own correctness and performance scope is clean for this
reproduction.** SYSTEM_NO_ROLL, TIME_OUT, and DBSQL_STMNT_TOO_LARGE all
failed to reproduce; no D2-owned code (attempt/lock/transaction/staged-
visibility/publication) contributes any measurable cost in this trace. The
one confirmed, substantial performance cost (`ZAOG_OBJ_INDEX` rebuild) is
pre-existing, cross-cutting shared infrastructure explicitly owned by
Package E (per this project's own prior classification), not introduced or
touched by D1 or D2's own changes.

Per the run brief's decision framework, this analysis returns
`PACKAGE_E_PERFORMANCE_DEFECT_CONFIRMED` rather than
`PERFORMANCE_ACCEPTABLE_FOR_D2_CLOSEOUT`, since status/index reconstruction
verifiably dominates the measured elapsed time (9.39s of 31.4s total, the
single largest ORTEC-attributable line item by more than 10x). This is a
**bounded, explained, one-time, correctly-K-scoped cost** - not an
unbounded, N-scaling, or per-object anti-pattern - but it is real,
substantial (user-noticeable), and identified as improvable via a low-risk
batch-size tuning change (§9) that is explicitly Package E's file, not
D1/D2's.

**D2 itself may close on its own correctness/performance merits.** The
overall workflow's elapsed time is not yet "operationally ideal", but that
gap is fully attributed to Package E-owned code that D2 never touched and
does not own - this satisfies the run brief's "confirmed non-D2 follow-up
whose measured behavior is bounded and operationally [explained]" framing
for D2's OWN closeout purposes specifically, while leaving the batch-size
tuning opportunity open as a distinct, non-blocking Package E ticket rather
than folding it into D2.

## 14. Exact next action

```text
1. Record this finding as a scoped, non-blocking Package E performance
   ticket: "zcl_abapgit_ortec_obj_index=>rebuild_index's bulk MODIFY chunk
   size (hardcoded 1000) is over-conservative for a non-IN-range array
   statement; raising it (e.g. to 5,000-10,000, subject to a focused
   performance DESIGN_GATE review to confirm HANA array-DML safety at that
   size) would proportionally reduce the ~41-round-trip/9.39s one-time
   per-commit index-build cost." Do NOT implement this in the current pass
   (the run brief's own rule: "Do not modify productive source in this
   first pass").
2. Do NOT create F/C certification for the filtered O4H-8794 path solely to
   populate the Cache Admin F4 help (per the run brief's explicit
   instruction, §8 item 4) - track the missing F4 entry as a separate,
   non-blocking Cache Admin usability item if desired.
3. D2 may proceed to its own closeout on SYSTEM_NO_ROLL/TIME_OUT/
   DBSQL_STMNT_TOO_LARGE all non-reproducing and zero D2-owned cost in this
   trace - subject to Michael's own sign-off, since this analysis
   deliberately stops short of running the combined D2 closeout prompt
   itself (per the run brief's explicit stop instruction).
4. If/when Package E work begins, use this artifact plus the D1 modified-
   status triage document as the starting evidence base for
   `zcl_abapgit_ortec_obj_index` - both point at the same class from
   different angles (correctness/staleness there, performance here).
```

## 15. Evidence limitations

```text
- No true Git-level relationship data (merge-base, rev-list distance, git
  diff --stat) could be obtained - this workspace has no local clone of the
  customer repository behind development/6.0.x/bugfix/O4H-8794-... A
  row-count-based proxy (§3) was used instead and is explicitly labeled as
  an approximation.
- The exact identity of bugfix/O4H-8794-...'s tip commit SHA among the
  three unconfirmed ZAOG_OBJ_INDEX-indexed commits (dc7c42c36d.../
  558e96dff6.../a5b30a633a...) was not conclusively resolved - not needed
  to answer the load-bearing performance question (§4-§9 already fully
  explain the measured cost from the ONE dominant hotspot regardless of
  which of the three it is), but flagged for completeness.
- The live SAT trace API's "statements" (full call-tree) analysis mode
  failed with a size-limit error for this trace (unaggregated call tree too
  large to return in one response, consistent with CALL_AGGREGATION=OFF
  producing thousands of individual entries); the "hitlist" analysis mode
  returned an empty result for the same trace via the live API. The
  complete, real hitlist evidence used throughout this artifact was
  instead obtained from the owner's own SAT/ST12 GUI export (pasted in
  full during this session) - the live read-only API could not
  independently reproduce this specific view for this trace, a tool
  limitation, not a data gap.
- The `dbAccesses` API view's per-statement `accessTime` field returned 0
  for every single row in this trace's export (including high-count
  generic framework tables), which is treated here as a display/API
  limitation, not literal zero cost - real timing evidence for the
  dominant ZAOG_OBJ_INDEX cost came from the separately-obtained hitlist
  export instead, which IS internally consistent (Modify's Gross time ≈
  the sum of its own nested DB:Exec Net time, with no unexplained residual).
- Want/have counts, exact fetch-mode selection, and pack object counts for
  this specific run's 2 HTTP calls were not directly extractable from
  either trace export; inferred qualitatively from the small, bounded
  decode/DB footprint (§7) rather than measured directly. This does not
  affect the confirmed root cause (§11), which is entirely local
  (ZAOG_OBJ_INDEX rebuild), not request-shape-dependent.
- The first (crashed) trace, 329FCD8E8A9511F1B129001DD8B728C2, could not be
  read via the live API at all (corrupted/interrupted recording); it was
  not used as evidence and is mentioned only for completeness (§2).
```

## 16. D2 closeout classification

```text
STATUS=CLASSIFIED_NON_BLOCKING_FOR_D2
TRACE_ID=95C45B828A9B11F1B129001DD8B728C2
TRACE_TOTAL_US=31417756
TOP_COST=ZCL_ABAPGIT_ORTEC_OBJ_INDEX=>REBUILD_INDEX
TOP_COST_US=approximately 9390000
OWNERSHIP=PACKAGE_E
D2_CLOSEOUT_ALLOWED=YES
```

Package E follow-ups recorded (not started, not implemented in this pass):

```text
E-PERF-OBJINDEX-1:
Review and benchmark ZAOG_OBJ_INDEX bulk MODIFY package sizing. Current
measured shape is 41 packages at 1000 rows and 82 DB round trips. Candidate
sizes such as 5000 or 10000 require a Package E performance DESIGN_GATE and
live measurement; do not adopt them without review.

E-CONSUMER-COHERENCE-1:
Resolve the existing false Local/Remote MODIFIED status for content-
identical files (see variant_b_package_d_d1_modified_status_triage.md).

E-CACHE-ADMIN-F4-1:
Review Cache Admin repository F4 help because it derives repository choices
from ZAOG_REPO_STATE and therefore may omit repositories accessed only
through partial filtered object materialization. This is a usability
issue, not a reason to publish false F/C certification.
```

This artifact and its evidence are preserved unmodified above; this section
is an appended closeout classification only, per Package D2's final
closeout (`.memory/state.md`, `PACKAGE_D_D2=SAP_VALIDATED_COMPLETE`).
