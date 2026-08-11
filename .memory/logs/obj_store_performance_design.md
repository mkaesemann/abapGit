# OBJ_PERF_FINAL — `ZAOG_OBJ_STORE` Performance Design (OBJ-PERF-DESIGN-1)
Design only. No source modified by this task. Grounded exclusively in
`.memory/state.md`, `.memory/logs/obj_index_partial_history_archaeology.md`,
`.memory/logs/obj_store_performance_current_source.md`, and direct re-reads of
`zcl_abapgit_ortec_obj_store.clas.abap`, `zaog_obj_store.tabl.xml`,
`zcl_abapgit_ortec_missing_obj.clas.abap`, `zcl_abapgit_ortec_walk_prep.clas.abap`,
`zcl_abapgit_ortec_cold_init.clas.abap` (all under `SOURCE_SCOPE`, BASELINE_COMMIT
`4193733d`). Cross-referenced from `obj_index_partial_design.md` §11-12 for the
integrated small-K path; do not duplicate the index-side content here.
## Revision log (cycle 3 — FINAL, cross-reference)
Full cycle-3 revision log lives at the top of `obj_index_partial_design.md`
per the parent task's `OUTPUT_ARTIFACTS` instruction. This file's cycle-3
changes close the store side of two cycle-2 blockers:
- **AR-2-03** (BLOCKER, lock unification): the "Cache admin invalidation"
  section below is rewritten so `clear_repo` now acquires the ONE canonical
  `zcl_abapgit_ortec_pack_raw=>acquire_repo_lock` around exactly its three
  derived-filter-table deletes (`ZAOG_OBJ_INDEX`/`ZAOG_OBJ_COVER`/
  `ZAOG_OBJ_PIDX`), removing the cycle-2 uncoordinated two-lock model.
  OS-INV-11 updated accordingly.
- **AR-2-01** (BLOCKER, context identity): filtered positive rows now live in
  the new context-keyed `ZAOG_OBJ_PIDX` table (`obj_index_partial_design.md`
  §3.0b). `clear_repo` deletes it too; OS-INV-01/05 updated to name it.
Cycle-2 changes retained: **AR-1-05** (cache-admin `ZAOG_OBJ_COVER` cleanup,
folded into the rewritten section below) and **AR-1-06** (OS-D/OS-INV-05/
OS-INV-12 re-scoped, static call-graph proof requirement). No cycle-1
finding is reopened.
Note: this file has no numbered `§5`/`§9 Slice 1c` of its own — the cycle-3
log's AR-2-03 references to "§5 (new bullet)" and "§9 Slice 1c (updated)"
point at those sections in `obj_index_partial_design.md`; the store-side
spec they cross-reference is the rewritten "Cache admin invalidation"
section below.
## 9. Candidate dispositions (OS-A..OS-J)
Evidence base: Q12–Q16 and the performance map in
`obj_store_performance_current_source.md`, plus the archaeology's
`REJECTED_PATTERN`/`SAME_FAILURE_CLASS` findings (`733bb307`, `2111b288`,
`9c3d297a`). No new problem is asserted here beyond what those artifacts
already name.
| Candidate | Disposition | Evidence / justification |
|---|---|---|
| **OS-A** Dedup before SQL/HTTP | **IMPLEMENT** | Already present and consistent across `get_objects`, `get_available_objects`, `get_staged_delta_objects`, `has_dangling_delta_base`, `get_present_sha1s`, `get_missing_sha1s`, `verify_ready_blobs` (`HASHED TABLE ... WITH UNIQUE KEY`, Q14) and `fetch_blobs_bulk` (`lt_requested_set`). Disposition = **preserve, extend to any new method this program adds** (§11 of the index design's `walk_filtered`/coverage calls introduce no new SHA1 set that bypasses this pattern — they never touch `ZAOG_OBJ_STORE` directly). |
| **OS-B** Request-scoped cache | **IMPLEMENT** | The existing session-static `mt_cache` (`repo_key + obj_sha1` keyed, Q16) already serves this role for the lifetime of one request/session — a second call for the same SHA1 within one Stage/Diff resolution already hits it. **No new cache is introduced.** A broader interpretation of "request-scoped cache" (a decode-result cache spanning `verify_tree_closure`/`get_tip_blob_sha1s`/`rebuild_index`'s independent tree-decode passes) is the same mechanism as **OS-E** below and is deliberately not built here — see OS-E's disposition. |
| **OS-C** Metadata-only presence | **IMPLEMENT** | Already correctly implemented: `get_present_sha1s`/`get_missing_sha1s`/`verify_ready_blobs`/`exists` never select `obj_data` (Q12). Disposition = **preserve; mandate its exclusive use for any new presence check** — the index design's `ZAOG_OBJ_COVER` lookups (`obj_index_partial_design.md` §3.2) never touch `ZAOG_OBJ_STORE` at all, so they cannot regress this. |
| **OS-D** Bounded bulk payload retrieval | **IMPLEMENT (scope re-stated, AR-1-06)** | `c_select_package_size = 1000` chunking (Q14) and `fetch_blobs_bulk`'s DB-side `INNER JOIN` window (`lc_key_chunk_size = 2000`, byte-budgeted at `lc_byte_budget = 67108864`) are both already correct and validated (`733bb307`/`29199f62`). Disposition = **preserve, scoped explicitly to the integrated small-K call graph reachable from `get_files_for_filter`**: `get_all_objects`/`populate_cache`'s unbounded `SELECT * ... status = 'R'` (Q14, the one still-unbounded caller after `2111b288`'s fix to `get_reachable_objects`) is confirmed, by the static call-graph proof this program now requires (§10 OS-INV-05/OS-INV-12), to be **unreachable from any code this program adds or changes** — not merely "not on the path" as an informal claim. It remains a known, separate, live risk for a future dedicated slice mirroring `2111b288`'s fix, explicitly out of `OBJ_PERF_FINAL`'s scope since no code this program adds calls it, and the invariant table below no longer overclaims it as globally satisfied. |
| **OS-E** Cross-phase tree/blob reuse | **DEFER_OUT_OF_SCOPE** | `state.md`'s `E1-TREE-REUSE` is explicitly `PARKED_MEASUREMENT_PENDING`, with a named entry condition (a focused, non-aggregated IT8 SAT trace proving material residual tree-decode cost *after* the 30000-row write-batching value, plus explicit new owner GO) that has not been met. `get_tip_blob_sha1s`'s own doc additionally states "re-verif[ies] closure independently every call — no cross-call in-memory trust" as a **deliberate** correctness choice (Package B design §11 step 1, Q13), not an oversight to fix. This program does not implement any cache that would reuse a tree decode across `verify_tree_closure`/`get_tip_blob_sha1s`/`rebuild_index`/`walk_filtered`. |
| **OS-F** Streaming/bounded decode | **IMPLEMENT** | Already implemented and validated (`bcc91801`: decode-and-free, atomic promote-`'R'`-or-purge-`'I'` on the pack decoder). Disposition = **preserve as-is, no change**. |
| **OS-G** DDIC indexes from real predicates | **REJECT_WITH_SOURCE_PROOF** | The only concrete candidate found (Q14): `get_known_commits`'s `(repo_key, obj_type, status)` predicate matches neither `RPK` (needs `pack_id`) nor `STA` (lacks `obj_type`) exactly. However `get_known_commits` "was not identified as being on the filtered-Stage/Diff hot path... no caller found in the read files" (Q14) and is not called anywhere in `obj_index_partial_design.md`'s integrated path. Adding a third secondary index to `ZAOG_OBJ_STORE` has real write-amplification cost at the mission's 1,000,000-object scale (every `store_object`/`store_objects` INSERT would maintain 3 indexes instead of 2) with no proven hot-path benefit — reject for this program; revisit only if `get_known_commits` is later proven hot with its own IT8 evidence. |
| **OS-H** Cross-request cache | **DEFER_OUT_OF_SCOPE** | `state.md`'s `E2_CONSUMER_COHERENCE` is explicitly `POSTPONED by owner decision 2026-07-31`, with an **open, unresolved incident (OS4)** whose candidate root cause is exactly this class of mechanism (overview/Full-Stage using a cached remote-files path that a single-object Diff independently revalidates). Introducing any **new** cross-request (beyond-one-LUW) cache while that incident is open and its entry condition (owner-executed debugger worksheet confirming a live mismatch) is unmet would compound an already-flagged coherence risk. This program's `ZAOG_OBJ_COVER` (`obj_index_partial_design.md` §3) is **not** a cross-request cache in the sense OS-H means — it is a durable, per-commit **fact table** with an explicit `CONTEXT_HASH` identity key (never silently reused across a config change, §5 of that design), not an in-memory or ambient session cache; it is therefore not blocked by this deferral, but no additional ambient cross-request caching layer is introduced beyond it. |
| **OS-I** Storage-format change | **REJECT_WITH_SOURCE_PROOF** | No discovery evidence in either artifact identifies a `ZAOG_OBJ_STORE` column-layout or storage-format problem. Every confirmed hot predicate (Q14) is already covered by the primary key or the `RPK`/`STA` secondary indexes; the only real, evidence-backed cost driver found is **payload volume** on one specific unbounded caller (`get_all_objects`, OS-D), not the storage format itself. |
| **OS-J** No-change | **REJECTED overall** (real, named gaps exist — `get_all_objects`'s unbounded preload, OS-D), but individual sub-parts are correctly "no change" where marked **preserve** above (OS-A, OS-C, OS-D's chunking, OS-F). No single blanket "no change" disposition applies to the whole class. |
## 10. OS-INV-01..15 and how the selected candidates satisfy each
| # | Invariant | Satisfied by |
|---|---|---|
| OS-INV-01 | Identity exactness — every read/write keys strictly on `(repo_key, sha1/commit/obj identity)`; no fuzzy/partial-key matches | Every `ZAOG_OBJ_STORE` predicate in `SOURCE_SCOPE` includes `repo_key` (Q15, confirmed no cross-repo leak in any predicate read); `ZAOG_OBJ_COVER`'s key additionally includes `commit_sha1` + `context_hash` (`obj_index_partial_design.md` §3); `ZAOG_OBJ_PIDX`'s key additionally includes `commit_sha1` + `context_hash` as a genuine KEY field (cycle 3, AR-2-01, `obj_index_partial_design.md` §3.0b), so two contexts' positive rows for the same object/path are physically distinct rows, never an overwrite |
| OS-INV-02 | No payload for presence | OS-C (preserved): `get_present_sha1s`/`get_missing_sha1s`/`verify_ready_blobs`/`exists` never select `obj_data`; `ZAOG_OBJ_COVER` carries no payload column at all |
| OS-INV-03 | Dedup before SQL/HTTP | OS-A (preserved, all listed methods + `fetch_blobs_bulk`) |
| OS-INV-04 | No per-key SQL/HTTP in the small-K path | `get_coverage`/`write_coverage` are each one set-based statement per call regardless of K (`obj_index_partial_design.md` §3.2); `get_objects`/`read_object_rows` chunk at `c_select_package_size`, never loop per SHA1 (Q14, `733bb307` fix preserved) |
| OS-INV-05 | Bounded rows/bytes, **scoped to the integrated small-K call graph reachable from `get_files_for_filter`** (AR-1-06: not a general `ZAOG_OBJ_STORE`-wide claim — `get_all_objects`/`populate_cache` remains unbounded and out of this program's scope, §9 OS-D) | `c_select_package_size=1000`, `c_index_write_chunk_size=30000` (unchanged), `c_filter_chunk_size=5000` (new, cycle 2, renamed from `c_cover_write_chunk_size`, now also covering `select_rows_for_filter`/`get_coverage` reads and, cycle 3, `walk_filtered`'s `ZAOG_OBJ_PIDX` writes plus `select_partial_rows_for_filter`'s reads, `obj_index_partial_design.md` §3.0b), `lc_key_chunk_size=2000`/`lc_byte_budget=67108864` (`fetch_blobs_bulk`, unchanged) — no unbounded `SELECT *` is introduced by this program on the paths it adds or changes |
| OS-INV-06 | No type confusion | Every tree-walk method's `IF <ls_obj>-type <> c_type-...` check is unchanged (Q15); `walk_filtered` (`obj_index_partial_design.md` §11.4) reuses the identical checks `rebuild_index` already performs |
| OS-INV-07 | Unchanged integrity checks | OS-F (preserved): streaming decoder's atomic promote-`'R'`-or-purge-`'I'` (`bcc91801`) is not touched by this program |
| OS-INV-08 | No cross-context cache leakage | No new `CLASS-DATA`/session cache is introduced anywhere in this program (OS-B, OS-H dispositions); `set_active_repo_key`'s documented blank-`iv_repo_key` risk (Q15) is pre-existing and not exercised by any new code path here |
| OS-INV-09 | Missing-object recovery always reachable | `walk_filtered`'s blob resolution reuses the unchanged `build_files_from_rows` → `zcl_abapgit_ortec_missing_obj=>ensure_available` → `materialize_missing_batches` chain (`17513ba7`, still bounded to the caller's own missing set, never a whole-commit-graph fetch) |
| OS-INV-10 | No partial/mixed publication | `write_coverage` is only called after `walk_filtered` completes without exception (`obj_index_partial_design.md` §5); `$IDX/__READY__` semantics are completely unchanged |
| OS-INV-11 | No read/cleanup races | `walk_filtered` acquires the same `zcl_abapgit_ortec_pack_raw=>acquire_repo_lock` as `rebuild_index` for its own writes (`obj_index_partial_design.md` §5); **cycle 3 (AR-2-03)**: `clear_repo` now ALSO acquires that same canonical mutex around exactly its three derived-filter-table deletes (`ZAOG_OBJ_INDEX`/`ZAOG_OBJ_COVER`/`ZAOG_OBJ_PIDX`, "Cache admin invalidation" section below), so a clear can no longer interleave a live filtered walk and orphan `ZAOG_OBJ_COVER FOUND` from its backing positive rows; concurrent reads see either pre- or post-walk state, never a torn write, matching today's `MODIFY`-chunk atomicity |
| OS-INV-12 | Bounded large-payload memory, **scoped to the integrated small-K call graph reachable from `get_files_for_filter`** (AR-1-06: same explicit narrowing as OS-INV-05 — `get_all_objects`'s unbounded preload is a separate, out-of-scope risk, not covered by this claim) | FILTERED-mode peak buffer is `MIN(F_k, 5000)` rows, strictly ≤ today's COMPLETE-mode 30000-row peak (`obj_index_partial_design.md` §7); no new unbounded in-memory table is introduced by this program |
| OS-INV-13 | Standard behavior preserved when ORTEC disabled | All new code is reached only through the existing `zcl_abapgit_ortec_git_switch=>is_active_for_repo`-gated `filter_walk` call chain (unchanged routing); a repository with ORTEC inactive never calls `ensure_filtered_coverage`/`walk_filtered`/`ZCL_ABAPGIT_ORTEC_OBJ_COVER` at all |
| OS-INV-14 | No silent fallback masking real failure | `walk_filtered` raises exactly like `rebuild_index` on a genuine miss and propagates to the same outer `get_files_remote` fallback — no new decoder/fallback cascade is introduced (mirrors the `8e03a191` lesson: never mask a real failure by silently retrying against a structurally different path) |
| OS-INV-15 | Explicit bounded fetch scope for any remote request | The only HTTP path this program touches (`build_files_from_rows`'s best-effort top-up) is the unchanged, already-hardened `ensure_available`/`materialize_missing_batches` chain, which places exactly the caller's own missing SHA1 set on the wire with adaptive row/byte batching — never an implicit "send everything" deepen/haves default (mirrors `58a90001`/`a51e743b`; no new fetch primitive is added by this program) |
## Weak-model change list — Store-A (verification-only)
```text
FILE_OR_OBJECT=zcl_abapgit_ortec_obj_store.clas.abap (verification only, no
  functional change in this program)
METHOD_OR_DDIC=get_objects, get_present_sha1s, get_missing_sha1s,
  verify_ready_blobs, fetch_blobs_bulk (zcl_abapgit_ortec_walk_prep)
ANCHOR=existing method bodies as read at BASELINE_COMMIT 4193733d
ACTION=none (verification checklist only — run before/alongside
  obj_index_partial_design.md Slice 1)
CHANGE=confirm, for each method above, that: (a) it deduplicates its input via
  a hashed set before any SQL (OS-A); (b) any presence-only branch never
  selects obj_data (OS-C); (c) every SELECT/JOIN is chunked at a named
  constant, never unbounded (OS-D/OS-INV-05). This is a read-only regression
  guard confirming Slice 1-4 of the index design introduce zero new callers
  of get_all_objects/populate_cache (OS-D's known out-of-scope risk).
INVARIANTS=OS-INV-01..05
SQL_SHAPE=NONE (no new SQL; verification only)
ERROR_ROLLBACK_FALLBACK=NONE
TESTS=re-run existing bulk_fetch_uses_pkg_size, bulk_fetch_no_per_key_sql,
  reachable_ignores_extra_ready, zero_byte_blob_is_a_hit (all pre-existing per
  the archaeology) — must still PASS unmodified after obj_index_partial_design.md
  Slice 1-4 land, proving no regression was introduced in this class
VALIDATION=SAPDiagnose(action="unittest", type="CLAS",
  name="ZCL_ABAPGIT_ORTEC_OBJ_STORE") PASS, no new failures
STOP_IF=any of the named existing tests fail after Slice 1-4 — indicates an
  unintended coupling was introduced between the new index-side code and this
  class, which this program's own design does not call for (§9/§11 of
  obj_index_partial_design.md never modify zcl_abapgit_ortec_obj_store)
```
Recommended position: **Store-A runs in parallel with Slice 1** of
`obj_index_partial_design.md` (no source dependency between them — Store-A is
read-only verification, Slice 1 is additive-only DDIC+class work) and is
re-run as a gate before Slice 3 lands (the slice that actually changes write
volume).
## AR-1-06 static call-graph proof requirement
OS-INV-05/OS-INV-12's re-scoping above ("the integrated small-K call graph
reachable from `get_files_for_filter`") is not accepted as satisfied by
narrated text alone — it is a mandatory Slice 1 gate, run alongside Store-A:
```text
FILE_OR_OBJECT=zcl_abapgit_ortec_obj_index.clas.abap,
  zcl_abapgit_ortec_obj_cover.clas.abap (verification only, no functional
  change)
METHOD_OR_DDIC=get_files_for_filter, ensure_filtered_coverage,
  walk_filtered, get_coverage, write_coverage, select_rows_for_filter,
  is_index_ready, rebuild_index, invalidate_commit_index
ANCHOR=every method this program adds or changes in
  obj_index_partial_design.md Slices 1/1b/2/3/4
ACTION=none (static verification checklist — a where-used/call-graph search,
  e.g. SAPNavigate(action="references") on get_all_objects and
  populate_cache, or an equivalent grep across the changed methods' bodies)
CHANGE=confirm that none of the methods listed under METHOD_OR_DDIC contains
  a call, direct or transitive, to
  zcl_abapgit_ortec_obj_store=>get_all_objects or its private
  populate_cache helper. This is a textual/structural proof (a where-used
  result showing zero matches from this method set), not a runtime trace.
INVARIANTS=OS-INV-05, OS-INV-12
SQL_SHAPE=NONE
ERROR_ROLLBACK_FALLBACK=NONE
TESTS=no automated test replaces this — it is a one-time static gate re-run
  whenever any method in the ANCHOR list is modified in a later slice
VALIDATION=SAPNavigate(action="references", name="GET_ALL_OBJECTS",
  type="CLAS") (and the same for POPULATE_CACHE) reviewed manually to confirm
  no caller in the ANCHOR list appears
STOP_IF=any method in the ANCHOR list is found to call get_all_objects/
  populate_cache, directly or transitively — OS-INV-05/OS-INV-12 would then
  be violated and this program's Slice 3 (the write-volume-changing slice)
  must not proceed until resolved
```
## Cache admin invalidation (AR-1-05 cleanup + AR-2-03 lock unification, cycle 3)
The cycle-1 review proved that `zcl_abapgit_ortec_cache_admin=>clear_repo`
deletes `ZAOG_OBJ_INDEX` (and every other repo cache table) but has no
matching `ZAOG_OBJ_COVER` cleanup — a manual repair/clear leaves coverage
rows behind, and a later warm filtered read sees coverage, skips the walk,
selects zero index rows (deliberately cleared), and returns a false empty
result. Cycle 2 fixed that *missing-cleanup* half; cycle 3's **AR-2-03** fixes
the *lock* half.
Direct source read of `clear_repo` (BASELINE_COMMIT `4193733d`) confirms its
exact shape: it validates that the repo key has cached data, then calls the
PRIVATE `acquire_lock( iv_repo_key )` (a `CALL FUNCTION
'ENQUEUE_EZAOG_REPO_LOCK'` enqueue keyed on `iv_repo_key`), then inside one
`TRY … COMMIT WORK AND WAIT` block deletes, in "dependent/derived before
parent-like" order, `zaog_obj_index → zaog_pack_idx → zaog_raw_pack →
zaog_pack_meta → zaog_fetch_sess → zaog_commit_hist → zaog_obj_store →
zaog_repo_state`, each into its own `rs_result-<table>` counter, then
`zcl_abapgit_ortec_obj_store=>invalidate_cache( )`, then `COMMIT WORK AND
WAIT`; on `cx_root` it does `ROLLBACK WORK` + `invalidate_cache( )` +
`release_lock( iv_repo_key )` + re-raise as `zcx_abapgit_ortec_git`; a final
`release_lock( iv_repo_key )` runs on the success path.
**AR-2-03 problem (cycle-2 state).** `clear_repo`'s enqueue
(`ENQUEUE_EZAOG_REPO_LOCK`, session_id = `iv_repo_key`) does **not** conflict
with the `ZAOG_FETCH_SESS`-row mutex
`zcl_abapgit_ortec_pack_raw=>acquire_repo_lock` (session_id =
`LOCK_<repo_key>`) that `rebuild_index`/`walk_filtered` hold while writing
`ZAOG_OBJ_INDEX`/`ZAOG_OBJ_COVER`/`ZAOG_OBJ_PIDX`. So a `clear_repo` could
delete those three derived-filter tables mid-walk and leave `ZAOG_OBJ_COVER
FOUND` with no backing positive rows (`obj_index_partial_design.md` §5,
AR-2-03 counterexample).
**Fix (single canonical lock).**
`zcl_abapgit_ortec_pack_raw=>acquire_repo_lock`/`release_repo_lock` is the ONE
canonical lock gating every writer **and** deleter of the three derived-filter
tables `ZAOG_OBJ_INDEX`, `ZAOG_OBJ_COVER`, `ZAOG_OBJ_PIDX`. `clear_repo` keeps
its existing `ENQUEUE_EZAOG_REPO_LOCK` (it still serializes whole-repo admin
clears across every OTHER cache table it deletes — `zaog_pack_idx`/`raw_pack`/
`pack_meta`/`fetch_sess`/`commit_hist`/`obj_store`/`repo_state`, an unrelated
concern this fix does not touch) but now ALSO acquires `acquire_repo_lock`,
narrowly, immediately before and released immediately after **exactly** the
three derived-filter-table deletes. Fixed, one-directional acquire order —
enqueue (whole method, outer) then mutex (three deletes only, inner) — is
deadlock-free by construction: `walk_filtered`/`rebuild_index` never take the
enqueue lock, so no acquisition cycle between the two lock kinds can form.
```text
FILE_OR_OBJECT=zcl_abapgit_ortec_cache_admin.clas.abap
METHOD_OR_DDIC=clear_repo, ty_clear_result
ANCHOR=(1) the ty_clear_result TYPES block, at its `obj_index TYPE i,` line;
  (2) the existing `DELETE FROM zaog_obj_index WHERE repo_key = iv_repo_key.
      rs_result-obj_index = sy-dbcnt.` pair, which is the FIRST statement
      inside clear_repo's TRY block, per BASELINE_COMMIT 4193733d source read
ACTION=insert + replace
CHANGE=
  (a) ty_clear_result gains two new counters directly after obj_index, keeping
      the three derived-filter-table counters grouped:
        obj_cover TYPE i,
        obj_pidx  TYPE i,
  (b) Wrap ONLY the three derived-filter-table deletes in the canonical mutex.
      Replace the existing first three lines inside the TRY block —
        DELETE FROM zaog_obj_index WHERE repo_key = iv_repo_key.
        rs_result-obj_index = sy-dbcnt.
      — with:
        DATA(lv_pack_lock) = zcl_abapgit_ortec_pack_raw=>acquire_repo_lock(
                               iv_repo_key ).
        DELETE FROM zaog_obj_index WHERE repo_key = iv_repo_key.
        rs_result-obj_index = sy-dbcnt.
        DELETE FROM zaog_obj_cover WHERE repo_key = iv_repo_key.
        rs_result-obj_cover = sy-dbcnt.
        DELETE FROM zaog_obj_pidx WHERE repo_key = iv_repo_key.
        rs_result-obj_pidx = sy-dbcnt.
        zcl_abapgit_ortec_pack_raw=>release_repo_lock( lv_pack_lock ).
  All remaining deletes (zaog_pack_idx … zaog_repo_state), invalidate_cache( ),
  and COMMIT WORK AND WAIT are UNCHANGED and follow exactly as today, in the
  same TRY block / same LUW.
  Ordering invariant (mandatory): release_repo_lock MUST run BEFORE the
  existing `DELETE FROM zaog_fetch_sess WHERE repo_key = iv_repo_key`.
  acquire_repo_lock INSERTs its mutex row (session_id = LOCK_<repo_key>,
  repo_key = iv_repo_key) INTO zaog_fetch_sess, so releasing first makes the
  later blanket fetch_sess delete a clean no-op with respect to the lock row
  and prevents that bulk delete from removing the mutex row out from under
  release_repo_lock. (The three new-table deletes sit at the very top of the
  TRY, the fetch_sess delete stays at its existing position further down, so
  this ordering holds automatically once the block above is inserted.)
  (c) CATCH cx_root path is UNCHANGED. The mutex-row INSERT and all three
  deletes are inside the same TRY/LUW, so the existing ROLLBACK WORK discards
  the mutex INSERT and all three deletes together atomically even if
  release_repo_lock was not reached; the enqueue release_lock( ) in the CATCH
  is unchanged. `acquire_repo_lock` has no age/timestamp-based staleness
  check (confirmed by source read, PP-02) — a mutex row orphaned by a hard
  process kill is instead discarded by plain LUW auto-rollback of the
  uncommitted `INSERT`, the same mechanism that already protects every other
  caller of this lock; no new recovery path is introduced or required.
  (d) Lock-hold-window accuracy (PP-01, ACCEPTED as documented behavior,
  fix option (b) — no interim COMMIT WORK added): because `release_repo_lock`'s
  `DELETE` runs inside `clear_repo`'s own still-open transaction, the mutex
  row does not become durably released until `clear_repo`'s single trailing
  `COMMIT WORK AND WAIT`, which is reached only after the remaining unchunked
  deletes (`pack_idx`, `raw_pack`, `pack_meta`, `fetch_sess`, `commit_hist`,
  `obj_store`, `repo_state`) and `invalidate_cache( )` complete. The true
  concurrent-blocking window for a competing `walk_filtered`/`rebuild_index`
  `acquire_repo_lock` call is therefore `clear_repo`'s **entire remaining
  duration** on a large repo, not "three deletes only". This is accepted as
  correct, not fixed with an interim commit, because: `clear_repo` is an
  already-rare admin operation; the resulting behavior for a blocked caller
  is `acquire_repo_lock`'s existing bounded ~5.15s retry ceiling followed by
  a caught `zcx_abapgit_exception` and a graceful fallback to
  `get_files_remote` (§6 trigger 2) — never a false fact, orphaned row, or
  data-integrity defect, only a rare extra remote-fetch cost on an
  already-slow admin path. Do not read the phrase "never affects the
  per-request path" elsewhere in this document as "never adds latency" —
  it means "never produces an incorrect result on the per-request path".
INVARIANTS=OS-INV-10, OS-INV-11 (and obj_index_partial_design.md §5 lock
  section, INV-01)
SQL_SHAPE=three primary-key-prefix DELETEs (WHERE repo_key = iv_repo_key), one
  per derived-filter table, same shape as the existing obj_index delete — no
  chunking (repo-scoped, not caller-filter-sized); plus one acquire_repo_lock
  INSERT and one release_repo_lock DELETE, both the existing pack_raw mutex SQL
ERROR_ROLLBACK_FALLBACK=unchanged clear_repo error path: any exception in the
  TRY (including the two new deletes, neither of which can raise under normal
  DB operation, or acquire_repo_lock's own timeout raise before the TRY-body
  writes) triggers the existing ROLLBACK WORK + invalidate_cache +
  release_lock + re-raise; the two new deletes and the mutex INSERT participate
  in the same all-or-nothing LUW, never a partial clear. If acquire_repo_lock
  raises (repo genuinely locked by a live walk), clear_repo fails cleanly with
  that exception rather than clearing under a concurrent writer — the intended
  AR-2-03 behavior
TESTS=
  clear_repo_deletes_derived (seed ZAOG_OBJ_STORE/INDEX/COVER/PIDX rows for a
    repo, run clear_repo, assert INDEX+COVER+PIDX are empty and
    rs_result-obj_index/obj_cover/obj_pidx reflect the deleted counts),
  clear_repo_then_filtered_read_rewalks (after clear_repo a subsequent
    get_files_for_filter for that repo/commit must re-walk or return no
    coverage trust, never a false empty result from orphaned coverage),
  clear_repo_blocks_on_pack_lock (seam/concurrency: hold acquire_repo_lock for
    the repo, invoke clear_repo, assert it cannot delete the three tables while
    the lock is held — it either waits or fails via acquire_repo_lock's
    timeout raise — so an end state of COVER FOUND with no matching PIDX rows
    is unreachable; the direct AR-2-03 retest)
VALIDATION=SAPDiagnose(action="unittest", type="CLAS",
  name="ZCL_ABAPGIT_ORTEC_CACHE_ADMIN") PASS
STOP_IF=ZAOG_OBJ_COVER or ZAOG_OBJ_PIDX does not yet exist at implementation
  time — this slice (Slice 1c/Store-B per obj_index_partial_design.md §9) must
  land after or together with BOTH tables' DDIC creation (Slice 1 for
  ZAOG_OBJ_COVER, Slice 1d for ZAOG_OBJ_PIDX), never before
```
Commit-slice placement: this is **Slice 1c (Store-B)** per
`obj_index_partial_design.md` §9 — depends on `ZAOG_OBJ_COVER`'s DDIC
(Slice 1) AND `ZAOG_OBJ_PIDX`'s DDIC (Slice 1d); independent of Slice 1b/2/3/4
otherwise, and may land in parallel once both dependency tables exist.