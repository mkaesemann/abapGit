# Archaeology log — historical fast path vs. current slow path

## 2026-07-10 — Repository/commit topology (ground truth, not an assumption)

- `git rev-parse HEAD` on the checked-out branch `ortec/abapgit_1_133-opt-rework` ==
  `b4f41e38372a0fe9f67483f71e968b1885b594c1` == the stated "historical fast-but-wrong
  baseline". `ortec/abapgit_1_133-optimized` and `origin/ortec/abapgit_1_133-optimized`
  point to the **same commit**.
- `git status --porcelain=v1 -uall` shows **no uncommitted ABAP source changes** (only
  untracked new `.github/agents/*`, `.memory/*` orchestration files and one modified
  `abaplint.json`). Conclusion: **there is no commit-level or working-tree diff between
  "historical" and "current"** for productive ABAP code. Any regression must be found by
  tracing *conditional branches inside the current source* (fast path vs. fallback path
  that coexist in the same commit), not by diffing two states.
- One stash exists: `stash@{0}` "On ortec/abapgit_1_133-optimized: Prefetch in Parallel
  Processing", based on parent `8d3de207` (2026-05-31), touching
  `zcl_abapgit_serialize`, `zcl_abapgit_oo_base`, `zcl_abapgit_object_dtel/msag`,
  `zcl_abapgit_ortec_ser_pref(_ext)`, `zabapgit_parallel` fugr. This is **serializer
  prefetch/parallelization work, unrelated to the Stage/Diff/Patch remote-retrieval
  regression** investigated below. Left untouched (not applied, not dropped) — flagged
  as a separate, out-of-scope finding for a future session (see "Unrelated" below).

## Evidence chain for the actual regression

1. Baseline commit `b4f41e38` ("Page Rendering Optimizations for Ortec Stage and
   Patch", 2026-06-23 17:04 UTC) touches only
   [src/ortec/git/zcl_abapgit_ortec_git_patch.clas.abap](../../src/ortec/git/zcl_abapgit_ortec_git_patch.clas.abap)
   and [src/ortec/git/zcl_abapgit_ortec_git_stage.clas.abap](../../src/ortec/git/zcl_abapgit_ortec_git_stage.clas.abap).
   Full diff vs. parent `dfd71649` confirms this is **purely client-side JS/DOM
   rendering work** for the Patch page: hunks/subblocks per file are moved out of one
   big inline JSON blob into per-file `<script type="application/json"
   id="ortec-patch-details-N">` tags that are lazily parsed only when a file row is
   expanded (`ensureHunksRendered`), the sidebar file list is now built incrementally in
   batches of 80 rows (`fileRenderBatch`, `appendMoreFileRows`, `resetFileRows`), and
   checkbox-sync DOM queries are cached (`lineCacheByFile`/`lineCacheByHunk`). Server
   side, `build_nav_data` still computes hunks for **every** file in `mt_diff_files`
   eagerly (no change there) — this commit reduces DOM-construction/JS work in the
   browser, it does not reduce ABAP-side computation or network payload. **This commit
   is not the cause of the "gains disappeared" regression** — it is a genuine,
   self-contained, still-active optimization.
2. `mt_diff_files` (consumed by `render_nav_data_script`) is populated by
   [zcl_abapgit_gui_page_diff_base.clas.abap](../../src/ui/pages/diff/zcl_abapgit_gui_page_diff_base.clas.abap)
   `calculate_diff` → `get_files_and_status`, which is the real fork point between a
   **filtered/sparse** remote-file lookup and a **full-repo** remote-file lookup.
   `zcl_abapgit_stage_logic.clas.abap` `zif_abapgit_stage_logic~get` has the identical
   fork for the Stage page.
3. `git log -S"is_active_for_repo"` / targeted `git show` on the ORTEC classes
   identifies the pivotal commit:
   **`e61fcb11e90b039fe1cd26e1b8e1a86ef37c787f` — "Attach Filtered Tree Walk
   Optimization to Fastpath Switch" — 2026-06-23 08:31:40 UTC**, author Michael
   Kaesemann. Confirmed via `git merge-base --is-ancestor e61fcb11 b4f41e38` (exit 0)
   that this commit **is already an ancestor of, and included in, the current HEAD /
   "historical" baseline** — there are 5 commits between them
   (`d15d14ee`, `dfd71649`, `64d4e9e1`, `fe1e94a8`, `131320ed`), none of which reverts
   this change.
4. Diff of `e61fcb11` (full text captured during this session; not persisted as a
   separate file — reproducible via `git show e61fcb11`):
   - `zcl_abapgit_ortec_filter_walk.clas.abap`: adds
     `IF zcl_abapgit_ortec_git_switch=>is_active_for_repo( lv_url ) = abap_false. rt_files = ii_repo_online->get_files_remote( ii_obj_filter ). RETURN. ENDIF.`
     as the **first** check inside `get_remote_files_for_stage`, before any repo-state /
     cache-validity check.
   - `zcl_abapgit_stage_logic.clas.abap` `zif_abapgit_stage_logic~get`: previously
     called `CALL METHOD ('ZCL_ABAPGIT_ORTEC_FILTER_WALK')=>('GET_REMOTE_FILES_FOR_STAGE')`
     **unconditionally** whenever `ii_obj_filter` was bound, falling back to
     `get_files_remote` only in a `CATCH cx_root` (i.e., on a hard failure). After this
     commit, it first resolves
     `lv_use_ortec = zcl_abapgit_ortec_git_switch=>is_active_for_repo( url )` and only
     calls the filtered walk `IF lv_use_ortec = abap_true`; otherwise it takes the full
     `get_files_remote` path unconditionally.
   - `zcl_abapgit_gui_page_diff_base.clas.abap` `get_files_and_status` already carries
     the equivalent `lv_use_ortec` gate in the current source (added by the related
     commit `64d4e9e1` "Add Filtered Walk in Base Diff Calculation", 2026-06-23 11:07
     UTC, same day, same pattern copied to the Diff/Patch flow).
5. `zcl_abapgit_ortec_git_switch=>is_active_for_repo` (read in full from current
   source) resolves to
   `zcl_abapgit_persistence_ortec=>get_instance( )->get_repo_use_cache( iv_url )`,
   which is `SWITCH #( read_repo_config( iv_url )-use_cache WHEN abap_true THEN abap_true ELSE abap_false )`.
   `read_repo_config` does `READ TABLE ms_user-repo_config INTO rs_repo_config WITH KEY url = lv_url`
   — for any URL without a persisted row, `rs_repo_config` stays at its **initial**
   value, so `use_cache` is space → `abap_false`. `get_use_repo_cache` additionally
   wraps the whole read in `CATCH cx_root. rv_enabled = abap_false.` The only place that
   flips this flag to true is the manual "Use Persistent Object Cache" checkbox in
   Repository Settings (`zcl_abapgit_gui_page_sett_repo.clas.abap` line ~402, calling
   `zcl_abapgit_ortec_git_switch=>set_use_repo_cache`). **Default is OFF for every
   repository/user that has not explicitly opted in**, and silently OFF on any
   persistence-read failure.
6. Contrast with the **write-side** persistent-cache gate in
   `zcl_abapgit_ortec_fastpath.clas.abap` (lines 252, 649, 728): `git log -S"is_active_for_repo"`
   on that file shows the guard was introduced in `3e964850` "Enable Delta Handling and
   Persistant Object Caching" (2026-05-13) and refined in `81345469` "Simplify Fastpath
   Calling and Handle Empty Packs" (2026-05-28) — **more than a month before**
   `e61fcb11`. That gate is a deliberate, long-standing safety choice: the fastpath pull
   **writes** to `ZAOG_OBJ_STORE` / `ZAOG_OBJ_INDEX` / `ZAOG_PACK_*` /
   `ZAOG_REPO_STATE`, so opt-in-by-default is the correct, conservative default for a
   mutating persistent cache.
7. `zcl_abapgit_ortec_filter_walk=>get_remote_files_for_stage` (read in full) shows the
   **read path's own, independent, data-driven fallback chain** that already existed
   before `e61fcb11` and is untouched by it: resolve `repo_key` via a pure SHA1 hash of
   the URL against `ZAOG_REPO_STATE` (`zcl_abapgit_ortec_repo_state=>get_repo_key_for_url`,
   confirmed to have **no dependency on `is_active_for_repo`** — it is a plain
   `SELECT SINGLE ... WHERE url_hash = ...`), then read `fetch_commit`, then verify the
   branch tip still matches the remote HEAD via a lightweight `branches()` call, and
   only then perform the sparse `ZCL_ABAPGIT_ORTEC_OBJ_INDEX=>get_files_for_filter`
   lookup. Every one of these steps already fails safe to
   `ii_repo_online->get_files_remote(...)` if the cache is missing, stale, or the branch
   tip moved.

## Direct answer to the required question

**What changed between the historical fast version and the current version that caused
performance gains to disappear?**

Nothing changed in the sense of a code diff between "historical" and "current" — both
branches sit on the identical commit `b4f41e38`. What changed, a few commits *earlier
that same day* (still inside this one baseline's ancestry, commit `e61fcb11` "Attach
Filtered Tree Walk Optimization to Fastpath Switch", 2026-06-23 08:31 UTC, and its
sibling `64d4e9e1` for the Diff/Patch flow, 2026-06-23 11:07 UTC), is that the
**read-only, non-mutating, self-validating sparse/cached remote-file lookup**
(`ZCL_ABAPGIT_ORTEC_FILTER_WALK`, used by Stage, Diff and Patch whenever an object
filter is present) — which previously ran unconditionally and fell back to full
retrieval only when the cache was genuinely missing/stale — was made conditional on
`ZCL_ABAPGIT_ORTEC_GIT_SWITCH=>IS_ACTIVE_FOR_REPO`, the **same** per-user/per-repository
opt-in flag that gates the unrelated, *mutating*, write-side pull/persist cache
(`ZCL_ABAPGIT_ORTEC_FASTPATH`). That flag defaults to `abap_false` for any
repository/user that has not explicitly ticked "Use Persistent Object Cache" in
Repository Settings, and silently resolves to `abap_false` on any persistence-read
exception. As a result, for the overwhelming majority of real usage (any repo/user that
never explicitly opted in, or whose opt-in state cannot be read), Stage/Diff/Patch now
take the **exact same full `get_files_remote()` path that existed before any Ortec
performance work was done** — with no error, no log entry, and no visible signal that
the fast path was skipped. The optimization code is fully present and functionally
correct; it is simply gated behind a switch that is off by default and was not off
before `e61fcb11`/`64d4e9e1`. That is the mechanism behind "performance gains
disappeared" even though the fast-path implementation itself was never removed or
broken.

## Classification of the relevant changes

| # | Change (commit) | Classification | Evidence |
|---|---|---|---|
| 1 | `zcl_abapgit_ortec_fastpath` opt-in gate on `is_active_for_repo` for the **write/persist** pull cache (`3e964850`, `81345469`) | **Required for diff correctness and must be preserved** | Mutates `ZAOG_OBJ_STORE`/`ZAOG_OBJ_INDEX`/`ZAOG_PACK_*`/`ZAOG_REPO_STATE`; opt-in-by-default is the safe posture for a persistent write cache. Predates the regression by >1 month. |
| 2 | `zcl_abapgit_ortec_filter_walk`'s own data-validity fallback chain (repo_key lookup → fetch_commit → branch-tip match → sparse index lookup, else `get_files_remote`) | **Required for diff correctness and must be preserved** | Confirmed in current source; this is what actually protects against stale/incorrect data (`Walk, tree not found`-class bugs referenced in `.memory/state.md`). Independent of the `is_active_for_repo` switch (`get_repo_key_for_url` is a plain hash lookup). |
| 3 | `e61fcb11` — adding `is_active_for_repo` guard **inside** `zcl_abapgit_ortec_filter_walk=>get_remote_files_for_stage` (read-only path) | **Cached/sparse retrieval replaced by standard full retrieval** + **Accidental performance regression** | Diff of `e61fcb11`: new `IF ... = abap_false. rt_files = get_files_remote(...). RETURN. ENDIF.` placed before any cache-validity check. The read path has no mutating side effects and does not need this policy gate to be correct — its own data-driven fallback already guarantees correctness. |
| 4 | `e61fcb11` / `64d4e9e1` — adding the identical `lv_use_ortec = is_active_for_repo(...)` gate around the `CALL METHOD ('ZCL_ABAPGIT_ORTEC_FILTER_WALK')` in `zcl_abapgit_stage_logic~get` and `zcl_abapgit_gui_page_diff_base~get_files_and_status` | **Cache invalidation/bypass regression** | Before: filtered walk attempted unconditionally, fallback only on `CATCH cx_root` (genuine failure). After: fallback is taken **by default**, for every repo/user without an explicit, persisted opt-in — functionally identical to bypassing a working, still-valid cache. |
| 5 | Conflation of the read-only rendering optimization with the write-side persistent-cache opt-in switch (same flag, `use_repo_obj_cache` / "Use Persistent Object Cache", used for both) | **Workaround needing narrower fix** | The two concerns are independent (one mutates ZAOG_* tables and needs a conservative default-off; the other only reads and self-validates). A narrower fix is a **separate flag** (or no flag at all, relying solely on the existing data-validity fallback) for the read-only filtered walk, decoupled from the write-side opt-in. |
| 6 | `fe1e94a8` "Filterberücksichtigung- und Filter Tree Walk Verbesserungen und Fixes" — DEVC path filtering in `zcl_abapgit_ortec_obj_index`, removal of `get_all_sub_packages` expansion in `zcl_abapgit_object_filter_tran`, lightweight `get_current_remote` branch lookup in `zcl_abapgit_repo_online`, `lcl_multi_filter` for Stage-selection-based Patch/Diff filtering | **Required for diff correctness and must be preserved** | Fixes package-path scoping so DEVC filter rows only match objects under the correct package path; narrows the transport-object filter to avoid pulling unrelated sub-package objects; adds a real (independent, harmless) fast path for resolving the current remote SHA1 before falling back to a full `fetch_remote()`. No evidence this narrows the fast-path eligibility further than item 3/4 above. |
| 7 | `b4f41e38` "Page Rendering Optimizations for Ortec Stage and Patch" (the named baseline commit itself) — lazy per-file hunk JSON, batched sidebar row rendering, DOM-query caching in `zcl_abapgit_ortec_git_patch` | **Unrelated** (to the retrieval-performance regression) | Confirmed by full diff vs. parent `dfd71649`: pure client-side JS/DOM change, no interaction with `is_active_for_repo`, `ZAOG_*`, or `zcl_abapgit_ortec_filter_walk`. Still an active, working optimization for large diffs. |
| 8 | Stashed `stash@{0}` "Prefetch in Parallel Processing" (serializer prefetch for `zcl_abapgit_serialize`/`zcl_abapgit_oo_base`/object handlers, based on `8d3de207`) | **Unrelated** (different subsystem: local object serialization, not remote Stage/Diff/Patch retrieval) | `git stash show --stat`; never applied to any commit in this branch's history. Flagged for a separate future investigation, not touched here. |

## Files changed by this archaeology pass

- [.memory/diagrams/historical_fast_path.mmd](../diagrams/historical_fast_path.mmd) — rewritten to reflect the actual pre-`e61fcb11` unconditional-attempt/data-validity-fallback flow.
- [.memory/diagrams/current_slow_path.mmd](../diagrams/current_slow_path.mmd) — refined to show the `is_active_for_repo` policy gate as the fork point, with the default-OFF path highlighted.
- [.memory/logs/archaeology.md](archaeology.md) — this file.
- [.memory/state.md](../state.md) — archaeology findings appended (see "Archaeology phase findings" section).
- No productive ABAP source was modified. No transports created.

## Residual uncertainty / explicit assumptions

- **Assumption**: because branches `ortec/abapgit_1_133-opt-rework` and
  `ortec/abapgit_1_133-optimized` point to the identical commit and the working tree has
  no ABAP diffs, the "current version" the user perceives as slow **is this same
  checked-out source**, and the regression is a *runtime/configuration* one (switch
  defaulting off) rather than a code-drift one. This is the most direct reading of the
  task's own hint ("still perform classification using source evidence and fallback
  paths documented in discovery") and is consistent with everything found in source, but
  it has not been confirmed by a live system trace (`SAPRead`/ADT connectivity was
  unavailable in this session — network error on first probe) or by asking Michael
  directly whether the "Use Persistent Object Cache" checkbox is actually ticked on the
  affected repositories.
- **Assumption**: no other, larger reflog/branch history in this repository holds a
  later commit that reverted or superseded `e61fcb11`/`64d4e9e1` — `git log
  e61fcb11..b4f41e38` was walked in full (5 commits) and none of them re-introduces
  unconditional filtered-walk usage.
- **Not verified**: whether any repository in the affected environment currently *has* a
  persisted `use_cache = 'X'` row (in which case that specific repo/user combination
  would still see the fast path) — this needs either a live system check
  (`ZAOG`/ORTEC persistence table read) or a question to Michael.
- **Out of scope, not investigated further**: the stashed "Prefetch in Parallel
  Processing" work (serializer prefetch) is unrelated to this regression and was left
  untouched in the stash.
