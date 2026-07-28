# Package D2 — implementation map (pre-implementation, orchestrator-owned)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=D2-IMPLEMENTATION-MAP
D1_VALIDATED_HEAD=73cb519a (SAP_VALIDATED_COMPLETE)
D2_IMPLEMENTATION_BASELINE=6a42ada594760ef822eac8fac260559c51586689
STATUS=MAP_COMPLETE_AWAITING_IMPLEMENTATION
```

Baseline verification: `git rev-parse HEAD` = `6a42ada5...`; `git status --short`
clean; `git show --stat 6a42ada5` confirmed the one commit after `73cb519a`
touches only `.memory/handoffs/variant-b-package-d-d1-implementation.md`,
`.memory/logs/regression_variant_b_package_d_d1.md`, `.memory/state.md` (D1
closeout memory only, no productive ABAP). Used as the D2 implementation
baseline per the run brief's baseline-reconciliation rule.

Design authority: `.memory/logs/variant_b_package_d_design.md` §1.3, §2, §5.3,
§5.3.1, §7–§14, §15, §16, §17–§19, §20 (all APPROVE/APPROVE_WITH_MINOR_REVISIONS,
DR-001..DR-004 and B-1/B-2/B-3/M-1..M-5 resolved per
`variant_b_package_d_correctness_decision.md`/`variant_b_package_d_protocol_decision.md`;
performance DESIGN_GATE iteration 2 = APPROVE_WITH_MINOR_REVISIONS per
`performance_design_variant_b_package_d.md`).

## Verified current source vs. design (reconciliation)

All D2-relevant methods below were re-read directly from current source
(`D2_IMPLEMENTATION_BASELINE`), not from design prose:

- `zcl_abapgit_ortec_pack_stream=>decode_and_persist_streaming` (line 994):
  confirmed current blanket `UPDATE zaog_obj_store SET status = 'R' WHERE
  ... status = c_status_incomplete` (single statement, no OBJ_TYPE
  discrimination) — matches design §5.3's described defect exactly.
- `zcl_abapgit_ortec_pack_stream=>cleanup_incomplete` (line 336): confirmed
  `DELETE ... WHERE ... status = c_status_incomplete` only — matches §5.3's
  "widen to `IN ('I','D')`" requirement.
- `zcl_abapgit_ortec_pack_stream=>resolve_one_meta` (line ~666,
  `get_object(iv_sha1 = <ls_row>-temp_key)`) and `preload_delta_rows`
  (line 443, `get_objects(iv_bulk_fetch = abap_false)` over temp keys):
  both confirmed to route through `read_object_rows`'s hard-coded
  `status = 'R'` filter — matches §5.3.1's PERF-B-1 fix scope exactly (both
  call sites, not just one).
- `zcl_abapgit_ortec_obj_store`: `read_object_rows` (private, line 1093,
  hard-coded `status = 'R'`), `get_objects`/`get_available_objects` cache-hit
  checks (`ls_cache_entry-status = 'R'`, lines ~536/~500) — confirmed
  untouched by this design; no `get_staged_delta_objects` method exists yet.
- `zcl_abapgit_ortec_mat_state=>begin_attempt` (line 224): confirmed
  unconditional fresh-UUID mint + unconditional `ZAOG_COMMIT_HIST.attempt_id`
  overwrite, no reuse path — matches DR-004 finding exactly.
- `zcl_abapgit_ortec_fastpath=>certify_fetched_commit` (line 1655, PRIVATE):
  confirmed current signature has **no** `iv_attempt_id` parameter and calls
  `begin_attempt` internally — DR-004 fix (add param, remove internal call)
  applies as designed.
- `zcl_abapgit_ortec_fastpath=>persist_pull_result` (line 1605, PUBLIC):
  confirmed calls `persist_missing_objects` then `certify_fetched_commit`
  then `update_after_fetch` then one `COMMIT WORK` — matches §10's
  transaction-ownership map exactly. No `iv_attempt_id` parameter yet.
- `zcl_abapgit_ortec_fastpath=>persist_missing_objects` (line 1543, PUBLIC):
  confirmed direct `MODIFY zaog_obj_store FROM TABLE lt_new` with no
  `attempt_id` field set — M-5 fix applies as designed.
- `zcl_abapgit_ortec_fastpath=>pull_by_branch` (line 617, PUBLIC): confirmed
  Phase-1b branch structure exactly as design §11/§15 describes — calls
  `zcl_abapgit_ortec_pack_dec=>resume_decode`, then (inside a nested `TRY`)
  `persist_pull_result`, with no lock/`begin_attempt` call anywhere in this
  method today.
- `zcl_abapgit_ortec_fastpath=>upload_pack_by_branch` (line 814, PUBLIC):
  confirmed calls `pull_by_branch` first, then `upload_pack` on a cache-miss
  — **never** calls `persist_pull_result`/`certify_fetched_commit` anywhere
  in its body. Confirms Owner Decision A / B-3: this method mints no
  `attempt_id` and acquires no lock in the target design either.
- `zcl_abapgit_ortec_porcelain=>pull_by_branch` (line 165, PUBLIC): confirmed
  the `INCREMENTAL_UPDATE` fallthrough (no `WHEN` matched in the `CASE
  lv_op_class` block) ends with `zcl_abapgit_git_transport=>upload_pack_by_branch`
  → `pull(...)` → a `TRY ... CATCH zcx_abapgit_ortec_git` block wrapping
  exactly one `zcl_abapgit_ortec_fastpath=>persist_pull_result` call (comment:
  "ORTEC: persistence failure is non-critical, continue normally") — this is
  the exact, sole call site for Publication Unit #2 per Owner Decision A.
- `zcl_abapgit_ortec_pack_dec`: `resume_decode` (line 497) is already
  **PUBLIC** (before the `PROTECTED SECTION` at line 114 — actually declared
  in `PUBLIC SECTION`, confirmed by its position before `PROTECTED SECTION.`
  at line 114). `acquire_repo_lock`/`release_repo_lock` (lines 566/615) and
  `create_session`/`update_session_progress` (lines 717/730) are confirmed
  **PRIVATE** (after `PRIVATE SECTION.` at line 168) — B-2 visibility fix
  (lock methods only) applies as designed; `create_session`/
  `update_session_progress` stay `PRIVATE` (only `resumable_decode`'s own
  call chain uses them; no cross-class caller needed).
- `zcl_abapgit_ortec_pack_dec=>persist_objects` (line 631, PROTECTED... " no,
  actually PRIVATE — confirmed by position after line 168): direct `MODIFY
  zaog_obj_store FROM TABLE lt_rows` with no `attempt_id` field — needs the
  same additive `iv_attempt_id` threading as `persist_missing_objects`.
- `zcl_abapgit_ortec_pack_raw=>create_session`/`update_session_progress`
  (lines 218/251): confirmed direct `INSERT`/`UPDATE zaog_fetch_sess` with
  no `attempt_id` field — matches §15's "accept and persist `iv_attempt_id`"
  scope.
- DDIC: `zaog_obj_store.tabl.xml`, `zaog_fetch_sess.tabl.xml`,
  `zaog_pack_meta.tabl.xml` confirmed to have **no** `ATTEMPT_ID` field today
  (full field lists re-read) — additive `c length 32` field required in all
  three, matching `zcl_abapgit_ortec_mat_state=>ty_attempt_id` exactly
  (already `TYPE c LENGTH 32`, confirmed at line 19 of that class).
- No test file exists yet for `zcl_abapgit_ortec_pack_raw`
  (`zcl_abapgit_ortec_pack_raw.clas.testclasses.abap` — confirmed absent);
  its `.clas.xml` has no `WITH_UNIT_TESTS` flag yet (confirmed by direct
  read) — both must be added if any D2 test is hosted there.
- `zcl_abapgit_ortec_fastpath.clas.testclasses.abap` (`ltcl_fastpath`) and
  `zcl_abapgit_ortec_porcelain.clas.testclasses.abap` (`ltcl_porcelain`)
  already exist with `LOCAL FRIENDS`/`setup`/`teardown` scaffolding —
  reused as the natural host for Unit #1 / Unit #2 end-to-end tests
  respectively (deviating from §16's literal "pack_stream +
  pack_raw only" placement list, since those two units' call chains live in
  fastpath/porcelain, not pack_stream/pack_raw — placement follows the
  established "most natural test host" convention from D1's own note, not
  a new rule).

No contradiction with a binding owner decision was found. Implementation is
authorized to proceed exactly as designed.

## Exact files and methods (D2 scope, verified against source)

| File | Method(s) | Change |
| --- | --- | --- |
| `zcl_abapgit_ortec_obj_store.clas.abap` | NEW `get_staged_delta_objects` (PUBLIC CLASS-METHODS) | New status-`IN ('D','R')` chunked bulk read, own cache-hit check `IN ('D','R')`, raise-on-missing (mirrors `get_objects` contract). No change to `get_object`/`get_objects`/`get_available_objects`/`read_object_rows`. |
| `zcl_abapgit_ortec_pack_stream.clas.abap` | `decode_and_persist_streaming` | Split blanket `UPDATE` into two: `WHERE ... status='I' AND obj_type IN ('ref_d','ofs_d')` → `status='D'`; complementary predicate → `status='R'`. Add optional `iv_attempt_id` param; set on every row in `lt_batch` before `flush_batch` (or via a final `UPDATE ... SET attempt_id` piggybacked on the same promotion statements — either is acceptable if bounded/set-based and O(1) extra statements). |
| | `cleanup_incomplete` | Widen `status = c_status_incomplete` to `status IN (c_status_incomplete, c_status_decoded)` (new constant, see below). |
| | NEW `CONSTANTS c_status_decoded TYPE c LENGTH 1 VALUE 'D'` | Mirrors `c_status_incomplete`'s existing doc-comment convention. |
| | `resolve_one_meta` | Switch its `get_object(iv_sha1 = <ls_row>-temp_key)` call (this pack's own delta raw-bytes fetch) to `get_staged_delta_objects` with a one-element `it_sha1s`. |
| | `preload_delta_rows` | Switch `get_objects(iv_bulk_fetch = abap_false)` over `lt_temp_keys` to `get_staged_delta_objects`. |
| | `resolve_streaming` | Add optional `iv_attempt_id` param, threaded to `flush_resolve_batch`/`store_objects` calls is NOT needed (resolved rows are already `status='R'` real content, no attempt tagging required there per §9 — only staged/incomplete rows need attempt correlation for cleanup diagnostics). Confirm no attempt_id write needed here (resolved 'R' rows are final content, not staged) — **no change to `resolve_streaming`'s own signature required** unless the senior implementer finds `flush_resolve_batch`'s writes also need attempt_id for the `attempt_id_cross_table` test; if so, add it there, additively, and document in the closeout. |
| `zaog_obj_store.tabl.xml` | DDIC | Add non-key field `ATTEMPT_ID`, `DATATYPE CHAR LENG 000032` (mirror `PACK_ID`'s field block shape but length 32, matching `zcl_abapgit_ortec_mat_state=>ty_attempt_id`). No key change, no index change unless the senior implementer finds a query needs it (none identified — attempt_id is diagnostics-only, never a WHERE-filter on a hot path). |
| `zaog_fetch_sess.tabl.xml` | DDIC | Same additive `ATTEMPT_ID` field. |
| `zaog_pack_meta.tabl.xml` | DDIC | Same additive `ATTEMPT_ID` field. |
| `zcl_abapgit_ortec_pack_dec.clas.abap` | `acquire_repo_lock`, `release_repo_lock` | Move from `PRIVATE SECTION` to `PUBLIC SECTION` (mechanical, B-2 fix). No body change. |
| | `resume_decode` | Add `iv_lock_held TYPE abap_bool DEFAULT abap_false` and `iv_attempt_id TYPE zcl_abapgit_ortec_mat_state=>ty_attempt_id OPTIONAL`. When `iv_lock_held = abap_true`, skip its own internal `acquire_repo_lock`/`release_repo_lock` calls (both call sites in the method body). Thread `iv_attempt_id` into `resumable_decode`. |
| | `resumable_decode` (PROTECTED) | Add `iv_attempt_id OPTIONAL` param, thread into `persist_objects`. |
| | `persist_objects` (PRIVATE) | Add `iv_attempt_id OPTIONAL` param; set `ls_row-attempt_id` before each `MODIFY zaog_obj_store FROM TABLE lt_rows`. |
| | `create_session` (PRIVATE, delegates to `pack_raw`) | Add `iv_attempt_id OPTIONAL` param, forward to `zcl_abapgit_ortec_pack_raw=>create_session`. |
| `zcl_abapgit_ortec_pack_raw.clas.abap` | `create_session`, `update_session_progress` | Add `iv_attempt_id OPTIONAL` param; persist into new `ZAOG_FETCH_SESS.ATTEMPT_ID` column. `cleanup_partial_session` — **NOT modified** (filters legacy `status='P'`, unrelated per §5.3 correction, DR-002). |
| `zcl_abapgit_ortec_fastpath.clas.abap` | `pull_by_branch` (Phase-1b branch only) | Immediately before invoking `resume_decode`: `acquire_repo_lock`; `begin_attempt`; call `resume_decode` with `iv_lock_held = abap_true` and the new `iv_attempt_id`; call `persist_pull_result` with the same `iv_attempt_id`; `release_repo_lock` unconditionally after (exception-safe — wrap in `TRY...CATCH`/cleanup so a `resume_decode`/`persist_pull_result` failure still releases). Catch `acquire_repo_lock`'s `zcx_abapgit_exception` locally, fall back exactly like today's "not applicable" path (never surface as hard failure). |
| | `upload_pack_by_branch` | **No change** (confirmed neither `attempt_id` owner nor lock owner). |
| | `persist_pull_result` | Add `iv_attempt_id TYPE zcl_abapgit_ortec_mat_state=>ty_attempt_id OPTIONAL`. When supplied, forward as-is to `certify_fetched_commit` (no internal `begin_attempt`). When not supplied (both pre-existing external callers — `zcl_abapgit_git_porcelain.clas.abap:650`, `zcl_abapgit_ortec_git_tests.clas.testclasses.abap` `persist_creates_state`), call `begin_attempt` internally exactly once and use that id for `persist_missing_objects`/`certify_fetched_commit`. Forward the same id to `persist_missing_objects`. |
| | `persist_missing_objects` | Add `iv_attempt_id OPTIONAL`; set on every `ZAOG_OBJ_STORE` row built in `lt_new` (M-5 fix). |
| | `certify_fetched_commit` (PRIVATE) | Add mandatory `iv_attempt_id TYPE zcl_abapgit_ortec_mat_state=>ty_attempt_id`; remove its internal `begin_attempt` call; use the passed-in id for `mark_graph_complete`/`mark_full_complete`/`publish_snapshot_complete` (DR-004 fix). |
| `zcl_abapgit_ortec_porcelain.clas.abap` | `pull_by_branch` (`INCREMENTAL_UPDATE` fallthrough only) | Immediately before the existing `persist_pull_result` call (inside/around the existing `TRY...CATCH zcx_abapgit_ortec_git` block): `zcl_abapgit_ortec_pack_dec=>acquire_repo_lock`; `zcl_abapgit_ortec_mat_state=>begin_attempt`; pass the resulting id into `persist_pull_result`; `release_repo_lock` unconditionally after the existing `TRY...CATCH` (covers both success and caught-exception paths). Catch `acquire_repo_lock`'s `zcx_abapgit_exception` locally as a non-critical skip (same graceful-degrade pattern already used one level up). |
| `zcl_abapgit_git_porcelain.clas.abap` | *(no change)* | Confirmed: unreachable switch-inactive fallback call to `persist_pull_result` at line ~650 keeps compiling unchanged (optional param). |
| `zcl_abapgit_ortec_git_tests.clas.testclasses.abap` | *(no change)* | Confirmed: `persist_creates_state`'s existing call site keeps compiling unchanged (optional param). |

Files NOT to touch: `zcl_abapgit_ortec_mat_state.clas.abap` (verification
only — its existing `begin_attempt`/`mark_graph_complete`/`mark_full_complete`/
`publish_snapshot_complete`/`clean_incomplete_attempts` contracts are already
correct per §7/§12, no code change needed), `zcl_abapgit_ortec_delta.clas.abap`
(D1-owned, SAP-validated, do not touch), `zcl_abapgit_ortec_fetch_req`,
`zcl_abapgit_ortec_have_policy`, any wire-protocol file (§20 exclusion), any
standard (non-`ortec_`) abapGit class other than the two confirmed-unchanged
call sites above.

## Attempt-ID call graph

```text
Unit #1 (fastpath pull_by_branch, Phase-1b):
  acquire_repo_lock(repo_key) -> lock_id
  begin_attempt(repo_key, remote_commit) -> attempt_id_1
  resume_decode(repo_key, iv_lock_held=X, iv_attempt_id=attempt_id_1)
    -> resumable_decode(..., iv_attempt_id=attempt_id_1)
      -> persist_objects(..., iv_attempt_id=attempt_id_1)   [ZAOG_OBJ_STORE.attempt_id]
      -> create_session(..., iv_attempt_id=attempt_id_1)     [ZAOG_FETCH_SESS.attempt_id]
  persist_pull_result(..., iv_attempt_id=attempt_id_1)
    -> persist_missing_objects(..., iv_attempt_id=attempt_id_1)  [ZAOG_OBJ_STORE.attempt_id]
    -> certify_fetched_commit(..., iv_attempt_id=attempt_id_1)   [ZAOG_COMMIT_HIST.attempt_id, via begin_attempt's own write]
    -> COMMIT WORK
  release_repo_lock(lock_id)   [unconditional / exception-safe]

Unit #2 (porcelain pull_by_branch, INCREMENTAL_UPDATE):
  upload_pack_by_branch(...)   [no lock, no attempt_id - object persistence only]
  pull(...)                     [tree-walk, no lock, no attempt_id]
  acquire_repo_lock(repo_key) -> lock_id_2
  begin_attempt(repo_key, commit) -> attempt_id_2
  persist_pull_result(..., iv_attempt_id=attempt_id_2)
    -> persist_missing_objects(..., iv_attempt_id=attempt_id_2)
    -> certify_fetched_commit(..., iv_attempt_id=attempt_id_2)
    -> COMMIT WORK
  release_repo_lock(lock_id_2)  [unconditional / exception-safe]

Fallback callers (no attempt_id supplied - unchanged compile, internal mint):
  zcl_abapgit_git_porcelain=>pull_by_branch:650  -> persist_pull_result()  [mints its own attempt_id internally]
  ltcl_ortec_git~persist_creates_state test      -> persist_pull_result()  [mints its own attempt_id internally]
```

## Canonical repository-lock owner

`zcl_abapgit_ortec_pack_dec=>acquire_repo_lock`/`release_repo_lock`
(SAP enqueue `ENQUEUE_EZAOG_REPO_LOCK`, `_scope = '2'`) is canonical for
both units. The unrelated `zcl_abapgit_ortec_pack_raw` DB-row session mutex
(used only by `zcl_abapgit_ortec_obj_index` via `try_filtered_commit_fetch`
→ `decode_and_persist`) is untouched and independent.

## COMMIT/ROLLBACK ownership map (verified against source, extends design §10)

| Site | Statement | Change in D2 |
| --- | --- | --- |
| `decode_and_persist_streaming` | `COMMIT WORK` (success) | None — now commits `'R'` + `'D'` rows (post-split) instead of `'R'` only. |
| `decode_and_persist_streaming` (catch) | `COMMIT WORK` after `cleanup_incomplete` | None — `cleanup_incomplete`'s predicate widens to also remove `'D'` rows. |
| `resolve_streaming` | `COMMIT WORK` (success only) | None. |
| `decode_streaming` (catch) | `ROLLBACK WORK` | None. |
| `resumable_decode`/`persist_objects` | periodic `COMMIT WORK` (commit-interval batching) | None — attempt_id piggybacks existing rows, no new commit. |
| `resume_decode` | (delegates; no own COMMIT) | New: lock acquire/release wraps this call only when NOT `iv_lock_held` (today's behavior preserved for any other caller); when `iv_lock_held = abap_true`, the caller (`pull_by_branch`) owns the lock span, `resume_decode` still owns none of the actual commits inside `resumable_decode`/`persist_objects`/`complete_pack`/`complete_session`. |
| `zcl_abapgit_ortec_fastpath=>pull_by_branch` (Phase-1b) | *(new)* lock acquire/release wraps `resume_decode` + `persist_pull_result` | New — never spans the earlier `zcl_abapgit_git_transport=>branches(iv_url)` HTTP call. |
| `persist_pull_result` | `COMMIT WORK` (single, end of method) | None — still the sole commit for both units' certification writes. |
| `zcl_abapgit_ortec_porcelain=>pull_by_branch` (`INCREMENTAL_UPDATE`) | *(new)* lock acquire/release wraps only the `persist_pull_result` call | New — never spans the preceding `upload_pack_by_branch`/`pull(...)` calls. |
| `zcl_abapgit_ortec_mat_state=>*` | none | Unchanged — never commits. |
| `zcl_abapgit_ortec_cache_admin=>clear_repo` | `COMMIT WORK AND WAIT` | Unchanged, out of scope. |

No low-level object/delta/cache/tree method gains a new `COMMIT WORK`/
`ROLLBACK WORK`. Only the two publication-unit call sites gain a lock
acquire/release; no new commit statement is introduced anywhere.

## Staged/READY visibility matrix

| Table | Marker | Visible to generic reads? | New narrow exception |
| --- | --- | --- | --- |
| `ZAOG_OBJ_STORE` | `status IN ('I','D')` | No (`get_object`/`get_objects`/`get_present_sha1s`/`get_missing_sha1s`/`get_available_objects` all stay `status='R'`-only, unmodified) | `get_staged_delta_objects` additionally admits `status='D'`, callable only by `zcl_abapgit_ortec_pack_stream` for a pack's own temp-key resolution |

## Cleanup map

- `cleanup_incomplete`: one `DELETE ... WHERE repo_key=... AND pack_id=... AND status IN ('I','D')` (was `= 'I'`) — pack-scoped, set-based, unchanged statement count.
- `cleanup_partial_session`: **unchanged** (legacy `status='P'`, DR-002 correction — do not touch).
- `clean_incomplete_attempts`: **unchanged** (already correct, §12 verification only).

## Failure/crash windows

- Crash between `decode_and_persist_streaming`'s commit and `resolve_streaming`'s commit: only `'D'`-status rows remain, now removable by `cleanup_incomplete`'s widened predicate on the next attempt (closes the §1.3 orphan-leak gap).
- Crash/exception inside Unit #1 or Unit #2's locked window (after `begin_attempt`, before `persist_pull_result`'s `COMMIT WORK`): no certificate, no branch pointer (both gated by `certify_fetched_commit`'s existing tree-closure/missing-blob checks feeding `mark_full_complete`/`publish_snapshot_complete`); lock is released via exception-safe cleanup in both units; a retry mints a genuinely new `attempt_id` via a fresh `begin_attempt` call.
- Lock-acquisition timeout (`acquire_repo_lock` raises `zcx_abapgit_exception`) at either unit's call site: caught locally, treated as "ORTEC fastpath not applicable this round", never surfaced as a hard failure.

## SQL shape

- `get_staged_delta_objects`: same chunked-bulk shape as `get_objects` (bounded by `c_select_package_size`), same raise-on-any-missing contract. Net new SQL calls per pack: 0 extra (replaces, not adds to, the existing `get_object`/`get_objects` call sites in `resolve_one_meta`/`preload_delta_rows`).
- `decode_and_persist_streaming`'s status split: 2 set-based `UPDATE`s instead of 1, still O(1) per pack (not per object).
- `cleanup_incomplete`: 1 `DELETE`, unchanged statement count, widened `IN` list.
- Attempt-ID columns: 0 extra SQL calls — piggyback existing `MODIFY`/`INSERT`/`UPDATE` statements in `persist_objects`, `create_session` (pack_raw), `persist_missing_objects`.
- Lock acquire/release: `ENQUEUE_EZAOG_REPO_LOCK`/`DEQUEUE_EZAOG_REPO_LOCK`, O(1) per publication unit, already the existing primitive (no new RFC/DB call shape).

## Row/byte bounds and payload model

No new unbounded structure. `get_staged_delta_objects` is bounded exactly like `get_objects` (pack-sized input set, chunked). No new XSTRING/payload copy pattern — the method returns the same `ty_objects_tt` shape already used everywhere else in this class.

## Cache behavior

`get_staged_delta_objects`'s cache-hit check must test `ls_cache_entry-status IN ('D','R')` (PERF-M-2, so `preload_delta_rows`'s warm entries are hit by `resolve_one_meta`'s follow-up call). `get_object`/`get_objects`/`get_available_objects` keep their existing `= 'R'`-only check, unmodified.

## Same-repository / cross-repository concurrency

- Same-repo: both units use the same canonical `acquire_repo_lock(repo_key)` — two concurrent attempts on the same repo serialize at the enqueue lock; never nested (each unit acquires and fully releases before any subsequent unit call in the same request).
- Cross-repo: unaffected — every mat_state/obj_store/repo_state operation is keyed by explicit `repo_key`; two different repos never share a lock or a row (already true, no change).

## Tests (method names, all ≤ 30 chars, verified by count)

Class-local, `LOCAL FRIENDS` pattern, **not** added to
`zcl_abapgit_ortec_git_tests.clas.testclasses.abap`:

| Test | Host file | Chars |
| --- | --- | --- |
| `delta_temp_row_status_d` | `zcl_abapgit_ortec_pack_stream.clas.testclasses.abap` | 24 |
| `temp_row_hidden_from_get` | same | 24 |
| `cleanup_removes_d_status` | same | 24 |
| `resolve_reads_own_d_row` | same | 23 |
| `staged_cache_hit_no_sql` | same | 23 |
| `crash_before_resolve_ok` | same | 23 |
| `attempt_id_on_obj_store` | same | 23 |
| `attempt_id_on_fetch_sess` | `zcl_abapgit_ortec_pack_raw.clas.testclasses.abap` (NEW file + `WITH_UNIT_TESTS=X`) | 24 |
| `stale_attempt_rejected` | `zcl_abapgit_ortec_fastpath.clas.testclasses.abap` | 22 |
| `one_attempt_one_id` | same | 18 |
| `retry_gets_new_attempt` | same | 22 |
| `certify_reuses_attempt` | same | 22 |
| `attempt_id_cross_table` | same | 22 |
| `same_repo_lock_serializes` | `zcl_abapgit_ortec_pack_raw.clas.testclasses.abap` (lock is `pack_dec`-owned, but exercised via `pack_raw`'s session fixtures is also acceptable — senior implementer's choice; `zcl_abapgit_ortec_fastpath.clas.testclasses.abap` is equally acceptable) | 25 |
| `cross_repo_no_lock_share` | same host as above | 24 |
| `porcelain_path_gets_lock` | `zcl_abapgit_ortec_fastpath.clas.testclasses.abap` | 24 |
| `lock_not_held_over_http` | `zcl_abapgit_ortec_fastpath.clas.testclasses.abap` | 23 |
| `lock_timeout_falls_back` | `zcl_abapgit_ortec_fastpath.clas.testclasses.abap` | 23 |
| `filtered_fetch_lock_ok` | `zcl_abapgit_ortec_fastpath.clas.testclasses.abap` | 22 |
| `missing_objects_has_id` | `zcl_abapgit_ortec_fastpath.clas.testclasses.abap` | 22 |
| `fresh_pull_unit_atomic` | `zcl_abapgit_ortec_porcelain.clas.testclasses.abap` | 22 |
| `fresh_pull_fail_no_publish` | same | 26 |
| `lock_release_on_failure` | same | 23 |
| `resume_new_attempt_when_new` | `zcl_abapgit_ortec_fastpath.clas.testclasses.abap` | 27 |
| `resume_reuses_attempt` | same, **NOT_APPLICABLE placeholder** (documented, not a real assertion, per §9/§16 evidence: `resume_decode` has zero interaction with `attempt_id`/`ZAOG_COMMIT_HIST`) | 21 |

Test-placement deviates from §16's literal "pack_stream + pack_raw only"
list for the fastpath/porcelain-level attempt/lock end-to-end tests, since
those units' call chains physically live in `zcl_abapgit_ortec_fastpath`/
`zcl_abapgit_ortec_porcelain` — this follows the same "most natural test
host" convention D1 already used (streaming-adapter tests placed in
`pack_stream`'s own file, not forced into `zcl_abapgit_ortec_delta`).

## Rollback boundary for this checkpoint

If any hard-stop condition in the run brief is hit, or the productive diff
review finds a scope violation, the orchestrator reverts only the
unauthorized/incorrect files via `git checkout HEAD -- <file>` (after
confirming via `git diff`/`git status` that the target file had no other
legitimate uncommitted change), and re-delegates with a narrower, corrected
scope rather than attempting to salvage a partially-wrong diff.
