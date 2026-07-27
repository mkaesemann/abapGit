# Variant B Package D — transaction and persistence discovery

Status: read-only discovery from current ORTEC source and DDIC definitions.
Baseline: Package C SAP-validated baseline at 29199f629773c676e0eaa2f3a006f5167d304ae8.
Scope: transaction isolation, staged visibility, attempt/session/pack correlation, commit/rollback ownership, cleanup, and repo-scoped invalidation for the ORTEC persistence layer.

## Executive summary

- The current design separates staged/incomplete rows from ready rows by explicit status semantics and by the materialization-state publication gate.
- `ZAOG_OBJ_STORE` is the only table that carries the durable object payloads; normal reads intentionally exclude status `I` rows.
- `ZAOG_COMMIT_HIST` and `ZAOG_REPO_STATE` are the authoritative state layer for attempt lifecycle and snapshot publication. The current code does not let the materialization-state layer issue its own `COMMIT WORK`; it relies on the caller/orchestrator to commit once the related writes succeed.
- The streaming pack decoder owns the temporary row lifecycle for a pack: it writes incomplete rows, promotes them to ready rows, and removes them on failure. The legacy pack decoder and raw-pack/session tables still exist but are not the current Package D transaction-isolation anchor.
- Repo-scoped cleanup and cache invalidation are explicit and commit-owned in the cache-admin layer.

## Acceptance ID D2-1 — final attempt / session isolation / transaction isolation

### Current behavior

- `zcl_abapgit_ortec_pack_stream=>decode_and_persist_streaming` generates a pack ID from `repo_key + timestamp`, writes decoded objects to `ZAOG_OBJ_STORE` with `pack_id`, `status = 'I'` (incomplete) and then promotes them in one set-based `UPDATE ... SET status = 'R'` before committing.
- `zcl_abapgit_ortec_pack_stream=>resolve_streaming` resolves deltas in bulk, writes resolved objects to `ZAOG_OBJ_STORE` with `status = 'R'`, deletes superseded temp-key rows, and issues a single `COMMIT WORK`.
- `zcl_abapgit_ortec_pack_stream=>decode_streaming` wraps the decode+resolve chain and issues `ROLLBACK WORK` if resolve fails so the current call does not leave partially-resolved state behind.
- `zcl_abapgit_ortec_pack_stream=>cleanup_incomplete` removes the current pack’s incomplete rows by `repo_key + pack_id + status = 'I'`.
- `zcl_abapgit_ortec_pack_raw=>create_session` / `update_session_progress` / `fail_session` / `complete_session` persist a fetch session in `ZAOG_FETCH_SESS` using `session_id`, `repo_key`, `branch_name`, `pack_id`, `obj_done`, `obj_total`, `status`, and `deepen_level`.
- `zcl_abapgit_ortec_pack_raw=>cleanup_partial_session` deletes `ZAOG_PACK_IDX`, `ZAOG_OBJ_STORE` temp rows, `ZAOG_RAW_PACK`, and updates `ZAOG_FETCH_SESS` to failed state.
- `zcl_abapgit_ortec_mat_state=>begin_attempt` creates or updates a materialization attempt row keyed by `repo_key + commit_sha1`, stores a generated `attempt_id`, and marks `snap_state = pending` if the row was missing or invalid.
- `zcl_abapgit_ortec_mat_state=>mark_graph_complete` / `mark_full_complete` / `publish_snapshot_complete` all validate the current `attempt_id` before changing the row, so stale attempts are rejected.

### Evidence IDs

- `zcl_abapgit_ortec_pack_stream=>decode_and_persist_streaming`
- `zcl_abapgit_ortec_pack_stream=>resolve_streaming`
- `zcl_abapgit_ortec_pack_stream=>decode_streaming`
- `zcl_abapgit_ortec_pack_stream=>cleanup_incomplete`
- `zcl_abapgit_ortec_pack_raw=>create_session`
- `zcl_abapgit_ortec_pack_raw=>update_session_progress`
- `zcl_abapgit_ortec_pack_raw=>fail_session`
- `zcl_abapgit_ortec_pack_raw=>complete_session`
- `zcl_abapgit_ortec_pack_raw=>cleanup_partial_session`
- `zcl_abapgit_ortec_mat_state=>begin_attempt`
- `zcl_abapgit_ortec_mat_state=>mark_graph_complete`
- `zcl_abapgit_ortec_mat_state=>mark_full_complete`
- `zcl_abapgit_ortec_mat_state=>publish_snapshot_complete`

## Acceptance ID D2-2 — branch switch / re-entry safety

### Current behavior

- `zcl_abapgit_ortec_repo_state=>prepare_full_snapshot` writes or updates a branch row keyed by `repo_key + branch_name` and persists `remote_url`, `url_hash`, `curr_commit`, `fetch_commit`, `fetch_ts`, `is_shallow`, and `deepen_lvl`.
- `zcl_abapgit_ortec_repo_state=>update_after_fetch` writes the latest branch pointer and marks the branch as shallow/deepen-aware.
- `zcl_abapgit_ortec_repo_state=>reset_fetch_commit`/`invalidate_tip_commit`/`invalidate_all_history` clear fetch pointers or whole-history state for the repository key and branch scope.
- `zcl_abapgit_ortec_obj_store=>get_object` and `get_objects` accept an optional `repo_key`; if it is blank, they fall back to the active cache-repo key set by `set_active_repo_key`. This is a local read-side convenience, but the current ORTEC fastpath and stream code pass `repo_key` explicitly, so the persistence boundary is still repository-scoped rather than session-global.
- `zcl_abapgit_ortec_cache_admin=>clear_repo` removes repo-scoped rows by `repo_key` and then issues `COMMIT WORK AND WAIT`.

### Evidence IDs

- `zcl_abapgit_ortec_repo_state=>prepare_full_snapshot`
- `zcl_abapgit_ortec_repo_state=>update_after_fetch`
- `zcl_abapgit_ortec_repo_state=>reset_fetch_commit`
- `zcl_abapgit_ortec_repo_state=>invalidate_tip_commit`
- `zcl_abapgit_ortec_repo_state=>invalidate_all_history`
- `zcl_abapgit_ortec_obj_store=>set_active_repo_key`
- `zcl_abapgit_ortec_obj_store=>get_object`
- `zcl_abapgit_ortec_obj_store=>get_objects`
- `zcl_abapgit_ortec_cache_admin=>clear_repo`

## Acceptance ID D2-3 — F/C publication invariant

### Current behavior

- The current enforcement point is `zcl_abapgit_ortec_mat_state=>publish_snapshot_complete`.
- It raises unless the existing row already has `hist_level = full_complete`; this is the live gate that prevents a snapshot publication from occurring before the full-complete state has been certified.
- On success it updates `ZAOG_COMMIT_HIST` `snap_state = complete` and writes the same commit into `ZAOG_REPO_STATE` as the branch’s `fetch_commit` and `snap_state = complete`.
- This is the current implementation of the Package C invariant “publication requires `HIST_LEVEL = F` before `SNAP_STATE = C`.”

### Evidence IDs

- `zcl_abapgit_ortec_mat_state=>publish_snapshot_complete`
- `zcl_abapgit_ortec_fastpath=>certify_fetched_commit`

## Acceptance ID D2-4 — promised-vs-current-tip missing blob after retry

### Current behavior

- `zcl_abapgit_ortec_pack_stream=>get_base_bytes` looks in the base-cache and then in `ZAOG_OBJ_STORE`; if the base is missing, it invokes `complete_missing_base` (currently disabled) and otherwise raises a retry-capable exception with `iv_retry_without_haves = abap_true`.
- `zcl_abapgit_ortec_pack_stream=>resolve_one_meta` uses the same pattern for REF_DELTA bases and includes diagnostic data: declaring object index, pack offset, total pack size, and unresolved-count context.
- `zcl_abapgit_ortec_pack_stream=>decode_and_persist_streaming` writes temp rows for delta objects under a temporary key and only promotes them to real identity after resolution.
- On any failure, the decode path cleans up the current pack’s incomplete rows and commits the cleanup; on resolve failure, the higher-level decode method rolls back the current resolve transaction.

### Evidence IDs

- `zcl_abapgit_ortec_pack_stream=>get_base_bytes`
- `zcl_abapgit_ortec_pack_stream=>resolve_one_meta`
- `zcl_abapgit_ortec_pack_stream=>decode_and_persist_streaming`
- `zcl_abapgit_ortec_pack_stream=>decode_streaming`

## Acceptance ID D2-5 — bounded external-delta-base recovery / no silent fallback

### Current behavior

- `zcl_abapgit_ortec_pack_stream=>resolve_streaming` runs two passes: an in-pack pass first, then a second pass that allows an external lookup from the object store.
- If a base is genuinely missing after that, `resolve_one_meta` raises and the caller must treat the whole resolve as failed; there is no silent fallback to a partial or lossy publish.
- `zcl_abapgit_ortec_pack_stream=>complete_missing_base` is intentionally disabled and documented as a no-op in the current source; the current implementation does not attempt one-object-at-a-time repair.
- `zcl_abapgit_ortec_fastpath=>complete_missing_object` still exists as a higher-level entry point for a targeted fetch, but the current streaming decoder no longer relies on it for ordinary delta-base completion.

### Evidence IDs

- `zcl_abapgit_ortec_pack_stream=>resolve_streaming`
- `zcl_abapgit_ortec_pack_stream=>resolve_one_meta`
- `zcl_abapgit_ortec_pack_stream=>complete_missing_base`
- `zcl_abapgit_ortec_fastpath=>complete_missing_object`

## Table inventory

### `ZAOG_OBJ_STORE`

- Writers: `zcl_abapgit_ortec_obj_store=>store_object`, `zcl_abapgit_ortec_obj_store=>store_objects`, `zcl_abapgit_ortec_pack_stream=>flush_batch`, `zcl_abapgit_ortec_pack_stream=>flush_resolve_batch`.
- Readers: `zcl_abapgit_ortec_obj_store=>get_object`, `get_objects`, `get_present_sha1s`, `get_missing_sha1s`, `exists`, `get_known_commits`, `get_all_objects`, `get_commit_parents`, `get_reachable_objects`, `get_reachable_sha1s`, `verify_tree_closure`, `get_tip_blob_sha1s`.
- Visibility rule: normal reads filter on `status = 'R'` and therefore do not see the streaming decoder’s temporary `status = 'I'` rows.
- Correlation fields: `repo_key`, `obj_sha1`, `pack_id`, `status`, `created_at`, `obj_type`, `obj_data`, `obj_size`.

### `ZAOG_FETCH_SESS`

- Writers: `zcl_abapgit_ortec_pack_raw=>create_session`, `update_session_progress`, `fail_session`, `complete_session`, `zcl_abapgit_ortec_pack_raw=>acquire_repo_lock`, `release_repo_lock`.
- Readers: `zcl_abapgit_ortec_pack_raw=>find_active_session`, `get_session`.
- Correlation fields: `session_id`, `repo_key`, `branch_name`, `pack_id`, `phase`, `obj_done`, `obj_total`, `status`, `deepen_level`, `error_text`.

### `ZAOG_PACK_META`

- Writers: `zcl_abapgit_ortec_pack_dec=>complete_pack` (legacy decoder path) and `zcl_abapgit_ortec_pack_dec=>create_session` / `update_session_progress` / `fail_session` / `complete_session` delegate to raw/session helpers rather than writing the metadata table directly.
- Readers: the legacy decoder’s session/pack helpers and the current decoder’s pack metadata reads are routed through the raw/session layer rather than through the streaming path.
- Correlation fields: `repo_key`, `pack_id`, `status`, `obj_decoded`.

### `ZAOG_PACK_IDX`

- Writers: `zcl_abapgit_ortec_pack_index=>store_entries` (legacy path).
- Readers: `zcl_abapgit_ortec_pack_dec=>resumable_decode` rehydrates existing pack-index rows when resuming a decode.
- Correlation fields: `repo_key`, `pack_id`, `obj_index`, `obj_sha1`, `dec_status`, `pack_offset`, `adler32`, `delta_base`.

### `ZAOG_RAW_PACK`

- Writers: `zcl_abapgit_ortec_pack_raw=>store`.
- Readers: `zcl_abapgit_ortec_pack_raw=>load`.
- Cleanup: `zcl_abapgit_ortec_pack_raw=>delete`, `delete_repo`, and `cleanup_partial_session`.
- Correlation fields: `repo_key`, `pack_id`, `raw_data`.

### `ZAOG_COMMIT_HIST`

- Writers: `zcl_abapgit_ortec_mat_state=>begin_attempt`, `mark_graph_complete`, `mark_full_complete`, `publish_snapshot_complete`, `invalidate_commit`, `clean_incomplete_attempts`.
- Readers: `zcl_abapgit_ortec_mat_state=>get_state`, `is_graph_have_eligible`, `is_full_have_eligible`.
- Correlation fields: `repo_key`, `commit_sha1`, `hist_level`, `snap_state`, `attempt_id`, `verified_at`, `updated_at`, `fetched_at`, `branch_name`.

### `ZAOG_REPO_STATE`

- Writers: `zcl_abapgit_ortec_repo_state=>prepare_full_snapshot`, `update_after_fetch`, `reset_fetch_commit`, `clear_state`, `invalidate_tip_commit`, `invalidate_all_history`, `zcl_abapgit_ortec_mat_state=>publish_snapshot_complete`.
- Readers: `zcl_abapgit_ortec_repo_state=>get_state`, `has_state`, `get_repo_key_for_url`, `get_complete_commits`, `zcl_abapgit_ortec_mat_state=>invalidate_commit`.
- Correlation fields: `repo_key`, `branch_name`, `remote_url`, `url_hash`, `curr_commit`, `fetch_commit`, `fetch_ts`, `is_shallow`, `deepen_lvl`, `snap_state`, `changed_at`.

## Commit and rollback ownership

- No `COMMIT WORK` or `ROLLBACK WORK` exists in `zcl_abapgit_ortec_mat_state`.
- `zcl_abapgit_ortec_pack_stream=>resolve_streaming` commits its own bulk resolve work.
- `zcl_abapgit_ortec_pack_stream=>decode_streaming` rolls back on resolve failure.
- `zcl_abapgit_ortec_cache_admin=>clear_repo` commits or rolls back the full repo clear and then releases the lock.

## Cleanup and stale-attempt handling

- Incomplete pack rows are removed by `zcl_abapgit_ortec_pack_stream=>cleanup_incomplete` and by `zcl_abapgit_ortec_pack_raw=>cleanup_partial_session`.
- Stale materialization attempts are cleared by `zcl_abapgit_ortec_mat_state=>clean_incomplete_attempts` via a set-based `UPDATE` that blanks `attempt_id` for rows older than the cutoff.
- The current code does not implement cross-repository concurrent-attempt coordination beyond repository-key scoping and `attempt_id` validation; repo-level lock helpers exist, but the core materialization state machine itself is still repository-scoped and attempt-validated rather than globally serialized.
