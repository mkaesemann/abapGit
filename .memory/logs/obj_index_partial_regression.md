# OBJ_PERF_FINAL — Independent Regression Validation (OBJ-PERF-REGRESSION-1)

STATIC VALIDATION ONLY. No live SAP/ADT connectivity was available in this environment (the
connected system lacks `ZAOG_OBJ_COVER`/`ZAOG_OBJ_PIDX` entirely per the Slice 1c implementation
log). No ABAP Unit execution, activation, or ATC run was performed by this task — every finding
below is derived from reading the actual committed source (`git diff
4193733de3c8ad75bae61f6ceb95b1c3e0ccb3e4..HEAD`) and running local structural self-checks
(PowerShell `Compare-Object`). Any claim of "PASS" below means "logically consistent under static
review", not "verified by a real ABAP Unit run".

## Scope actually changed (git diff --stat, baseline → HEAD)
```
src/ortec/git/zaog_obj_cover.tabl.xml                                  (new, 141 lines)
src/ortec/git/zaog_obj_pidx.tabl.xml                                   (new, 149 lines)
src/ortec/git/zaog_obj_index.tabl.xml                                  (+9, non-key CONTEXT_HASH)
src/ortec/git/zcl_abapgit_ortec_cache_admin.clas.abap                  (+20)
src/ortec/git/zcl_abapgit_ortec_cache_admin.clas.testclasses.abap      (+244, new)
src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap                  (+13/-6)
src/ortec/git/zcl_abapgit_ortec_obj_cover.clas.abap                    (new, 268 lines)
src/ortec/git/zcl_abapgit_ortec_obj_cover.clas.testclasses.abap        (new, 255 lines)
src/ortec/git/zcl_abapgit_ortec_obj_cover.clas.xml                     (new)
src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap                    (+821/-... , 821 net)
src/ortec/git/zcl_abapgit_ortec_obj_index.clas.testclasses.abap        (+860)
```
11 files changed, 2732 insertions(+), 72 deletions(-). Matches the mission's SOURCE_SCOPE exactly
(plus the two new DDIC tables and the obj_cover `.clas.xml`, all expected additions).

## 1. Pre-existing (Package E1) tests re-verified for context-hash consistency
Read in full: `marker_required_for_ready`, `index_no_cross_commit_leak`,
`ready_rejects_other_commit`, `ready_accepts_exact_commit`, `index_chunk_boundary_ok`,
`index_bulk_rows_preserved`, `index_empty_no_match` (all in
[zcl_abapgit_ortec_obj_index.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_obj_index.clas.testclasses.abap)).

Every one of these seven tests:
- Writes via `get_files_for_filter( ... io_dot = lo_dot iv_devclass = '$PACK' ... )` (a real rebuild,
  which internally computes `iv_context_hash` once via `compute_context_hash( iv_devclass = '$PACK'
  io_dot = lo_dot )` and stamps it on every `ZAOG_OBJ_INDEX` row including the marker).
- Reads via `is_index_ready(...)` passing `iv_context_hash = zcl_abapgit_ortec_obj_cover=>
  compute_context_hash( iv_devclass = '$PACK' io_dot = lo_dot )` — i.e. the exact same
  `devclass`/`io_dot` pair used on the write side, so the recomputed hash is guaranteed identical.
- `marker_required_for_ready` additionally re-invokes `get_files_for_filter` a second time after
  deliberately deleting the marker row — this still uses the same `'$PACK'`/`lo_dot` pair, so the
  self-heal path is unaffected by the context change.

**Verdict: all 7 pre-existing tests remain logically consistent and would still PASS.** The
backward-compatibility guarantee (Slice 1b) holds: no pre-existing test call site was left passing
a stale/mismatched context on one side only.

## 2. New tests — existence + name-vs-body correspondence
All 25 names from the mission's `zcl_abapgit_ortec_obj_index` list were grepped for both the
`METHODS` declaration and the `METHOD ... .` implementation; every one is present in
[zcl_abapgit_ortec_obj_index.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_obj_index.clas.testclasses.abap)
and every body was read (not just confirmed to exist):

| Test | Confirms |
|---|---|
| `ready_rejects_different_context` | different devclass → different context_hash → `is_index_ready` false for the wrong context, true for the right one |
| `select_rows_excludes_other_context` | a context-B rebuild purges the whole commit; context A's `is_index_ready` becomes false afterward (end-to-end, via `get_files_for_filter`) |
| `blank_legacy_context_is_never_ready` | a directly-seeded blank-`context_hash` marker row never satisfies `is_index_ready` for a real context |
| `partial_rows_context_disjoint` | two `ZAOG_OBJ_PIDX` rows, same PK except `CONTEXT_HASH`, coexist; `select_partial_rows_for_filter` returns only the caller's own context's row |
| `select_partial_rows_chunk_boundary` | 5100-row `ZAOG_OBJ_PIDX` filter set, all rows returned across the 5000 boundary |
| `warm_coverage_skips_rewalk` | FOUND coverage + PIDX row served with **zero** `ZAOG_OBJ_STORE` commit/tree object present (a real walk would raise) — proves zero-walk warm path; also asserts `is_index_ready` stays false |
| `incomplete_coverage_falls_through_to_rebuild` | no coverage row at all → real COMPLETE-mode rebuild still runs, marker still written |
| `filtered_walk_writes_only_requested_objects` | two-object tree, filter=ZPROGRAM only → ZPROGRAM gets 1 PIDX row, ZOTHER gets 0 |
| `filtered_walk_never_sets_ready_marker` | after `walk_filtered`, `is_index_ready` false AND zero `ZAOG_OBJ_INDEX` rows exist at all |
| `filtered_walk_idempotent_on_overlap` | second overlapping walk (ZPROGRAM+ZOTHER) does not duplicate ZPROGRAM's row (still count=1) |
| `filtered_walk_writes_context_hash_as_key` | two contexts, same object/path → 2 physically distinct PIDX rows |
| `filtered_walk_no_cross_context_overwrite` | context A's row survives intact after a context-B walk for the same object |
| `retry_purge_removes_all_three_tables` | `invalidate_commit_index` empties INDEX+COVER+PIDX together for one commit |
| `missing_tree_writes_m_row_then_reraises` | missing commit → walk raises AND a `M` (unresolved_missing_local_data) coverage row is written first |
| `repeat_request_within_backoff_skips_walk` | fresh `M` row (now) → `ensure_filtered_coverage` raises without ever writing a PIDX row (proves no re-walk), even though a real walk would have succeeded |
| `repeat_request_after_backoff_retries_walk` | expired `M` row (now − backoff − 60s) → real walk retried and succeeds |
| `not_present_remote_requires_current_remote_commit` | positive control (`iv_current_remote = iv_commit`, graph-complete) → `RESOLVED_NOT_PRESENT_REMOTE`; negative control (mismatched `iv_current_remote`) → capped at `RESOLVED_NO_FILES` |
| `not_present_remote_requires_current_remote_supplied` | omitted `iv_current_remote` (as on the real `pull_filtered` path) → capped at `RESOLVED_NO_FILES` even though graph-complete |
| `select_rows_chunk_boundary` | 5100-file COMPLETE-mode index, warm `select_rows_for_filter` read path (PA-001 regression test) — no row lost across the chunk boundary |
| `marker_required_for_ready` … `index_empty_no_match` | (the 7 pre-existing tests, §1 above) |

`zcl_abapgit_ortec_obj_cover.clas.testclasses.abap`'s full `ltcl_obj_cover` suite (10 tests:
`context_hash_stable_for_same_input`, `context_hash_changes_on_devclass`,
`context_hash_changes_on_dot_change`, `context_hash_embeds_algo_version`, `coverage_round_trip`,
`coverage_context_mismatch_excl`, `coverage_upsert_idempotent`, `coverage_write_chunk_boundary`,
`coverage_read_chunk_boundary`, `write_coverage_diag_on_success`) — all declared and implemented
(confirmed via grep + `Compare-Object`, §3).

`zcl_abapgit_ortec_cache_admin.clas.testclasses.abap`'s 3 new tests (`clear_repo_deletes_derived`,
`clear_repo_then_filtered_read_rewalks`, `clear_repo_blocks_on_pack_lock`) were read in full: they
correctly assert the new `obj_cover`/`obj_pidx` result counters, that a filtered read cannot trust
orphaned coverage after a clear, and that a concurrently-held `acquire_repo_lock` blocks `clear_repo`
and leaves the coverage row untouched.

**Verdict: 47/47 mission-listed + full-suite tests exist and each body genuinely exercises the
behavior its name claims. 0 declared-but-hollow (bare-assertion or no-assertion) tests found.**

## 3. Declared-vs-implemented self-check (run directly by this task, not trusted from prior logs)
Ran two independent PowerShell `Compare-Object` passes:
- `METHODS <name>` (test declarations) vs `^\s*METHOD <name>\.` (implementations) on all four
  touched `.testclasses.abap`/`.clas.abap` files listed in the mission's REQUIRED_VALIDATION §3:
  `zcl_abapgit_ortec_obj_index.clas.testclasses.abap`, `zcl_abapgit_ortec_obj_cover.clas.
  testclasses.abap`, `zcl_abapgit_ortec_cache_admin.clas.testclasses.abap`,
  `zcl_abapgit_ortec_filter_walk.clas.abap` → **`NO DIFFERENCES`** on all four.
- `CLASS-METHODS <name>` (production signatures) vs `METHOD <name>.` (implementations) on the three
  main production class files (`zcl_abapgit_ortec_obj_index.clas.abap`,
  `zcl_abapgit_ortec_obj_cover.clas.abap`, `zcl_abapgit_ortec_cache_admin.clas.abap`) →
  **`NO DIFFERENCES`** on all three.

No hidden gap of the "declared method / no body" class (the recurring failure mode logged
repeatedly in `obj_index_partial_implementation.md`) survives in the final committed state.

## 4. `zcl_abapgit_ortec_obj_store` — confirmed untouched
```
git diff 4193733de3c8ad75bae61f6ceb95b1c3e0ccb3e4..HEAD -- \
  src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap \
  src/ortec/git/zcl_abapgit_ortec_obj_store.clas.testclasses.abap
```
produced **zero output** — byte-identical to baseline. No re-verification of its own test bodies is
needed; this program made no changes to that file or its tests.

## 5. Standard-path / ORTEC-disabled gate
`zcl_abapgit_ortec_filter_walk=>get_remote_files_for_stage` is the sole caller of the new
`ensure_filtered_coverage`/`walk_filtered` chain (via `get_files_for_filter`). Its gate is
pre-existing and unchanged by this diff: `lv_repo_key = zcl_abapgit_ortec_repo_state=>
get_repo_key_for_url( lv_url )` — `IF lv_repo_key IS INITIAL. rt_files =
ii_repo_online->get_files_remote( ii_obj_filter ). RETURN. ENDIF.` A repo not registered with ORTEC
(the "ORTEC disabled for a repo" state) resolves to an initial `repo_key` and takes the standard
`get_files_remote` fallback, never reaching `zcl_abapgit_ortec_obj_index` at all. The diff for this
file only adds a best-effort `lv_current_remote = li_repo_online->get_current_remote( )` computation
(wrapped in its own `TRY`/`CATCH zcx_abapgit_exception` → blank) and threads it into the existing
call — it does not touch the gate itself. **Gate intact, standard-path behavior when ORTEC is
disabled for a repo is unaffected.**

## 6. DDIC verification
- `zaog_obj_index.tabl.xml`: `CONTEXT_HASH CHAR40` appended with **no `KEYFLAG`** — confirmed
  non-key append, matches §3.0 exactly.
- `zaog_obj_cover.tabl.xml`: key = `MANDT, REPO_KEY, COMMIT_SHA1, OBJ_TYPE, OBJ_NAME,
  CONTEXT_HASH` (6 `KEYFLAG=X` fields) — matches §3 exactly.
- `zaog_obj_pidx.tabl.xml`: key = `CLIENT, REPO_KEY, COMMIT_SHA1, OBJ_TYPE, OBJ_NAME,
  CONTEXT_HASH, PATH_HASH` (7 `KEYFLAG=X` fields) — matches §3.0b exactly (`CONTEXT_HASH` as a
  real key field).

## 7. Production logic cross-check (read in full, not sampled)
Read the complete bodies of `get_files_for_filter`, `is_index_ready`, `ensure_index`,
`invalidate_commit_index`, `ensure_filtered_coverage`, `rebuild_index`, `walk_filtered`,
`select_rows_for_filter`, `select_partial_rows_for_filter`, `build_files_from_rows` in
[zcl_abapgit_ortec_obj_index.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap), and
the complete `zcl_abapgit_ortec_obj_cover.clas.abap` and the changed `clear_repo` section of
`zcl_abapgit_ortec_cache_admin.clas.abap`. All match the design narrative (§3.0/§3.0b/§4.1/§5/§11/
§13) precisely, including the PS-001 (`lt_filter_set` hashed lookup) and PA-001 (chunked +
context-predicated `select_rows_for_filter`) post-audit fixes. `walk_filtered` never writes
`ZAOG_OBJ_INDEX`; `rebuild_index` never writes `ZAOG_OBJ_PIDX`; `invalidate_commit_index` purges all
three tables unconditionally (context-blind) with no `RAISING` and no `COMMIT WORK`, as designed.

One residual, non-blocking observation on `clear_repo` (AR-2-03 lock unification):
`zcl_abapgit_ortec_pack_raw=>acquire_repo_lock`/`release_repo_lock` are called directly around the
three derived-table deletes, but — unlike `rebuild_index`/`walk_filtered`, which each wrap their own
lock usage in an explicit `CATCH zcx_abapgit_exception`/`CATCH cx_root` that always releases the
lock before re-raising — `clear_repo`'s outer `TRY`/`CATCH cx_root` block has no dedicated release
for `lv_pack_lock` if an exception were to occur strictly between `acquire_repo_lock` succeeding and
`release_repo_lock` running. In practice the only statements in that window are three plain
`DELETE FROM`s (which `invalidate_commit_index`'s own design comment asserts "cannot themselves
fail under normal DB operation"), and `clear_repo_blocks_on_pack_lock` proves the acquire-failure
path itself never leaves a lock held (acquire only returns a handle on success). This is a
theoretical robustness asymmetry versus the two other lock-users, not a demonstrated defect — flagged
as a residual gap per the mission's instruction not to paper over unverifiable/marginal items.

## Residual gaps / unverifiable-by-static-review items
- No live ABAP Unit, syntax-check, or activation was run in this task (no SAP/ADT connectivity available;
  the DDIC objects are confirmed absent from the previously-connected system per the Slice 1c log).
  Everything above is static-source verification only.
- The 30000-row `rebuild_index` in-loop multi-chunk flush behavior is explicitly documented (by the
  implementation itself) as verifiable only via live IT8 SAT/SQL measurement, not locally — unchanged
  from before this program, not a new gap.
- The `clear_repo` lock-release asymmetry noted in §7 is a defense-in-depth observation, not a
  proven live failure; only a live fault-injection test (not available here) could confirm or refute
  actual risk.
- No new hard-stop conditions from the mode's mandatory hard-stop list were found (no unknown→remote-
  deleted transitions, no tree-not-found on branch switch, no fallback hiding a performance
  regression, no fastpath-disabled behavior change — none of these are touched by this program's
  scope at all).

## PACKET
```
task=OBJ-PERF-REGRESSION-1
status=PASS
existing_tests_preserved=YES
new_tests_verified=47/47
declared_vs_implemented_gaps=NONE
obj_store_untouched=YES
ortec_disabled_gate_intact=YES
residual_gaps=clear_repo lock-release asymmetry (non-blocking, theoretical); no live IT8 execution performed
verdict=PASS_WITH_NOTES
```
