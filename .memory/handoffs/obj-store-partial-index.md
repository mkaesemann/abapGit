# OBJ_PERF_FINAL — Program handoff (general)

Consolidated resume-point summary for the integrated demand-driven partial `ZAOG_OBJ_INDEX` +
`ZAOG_OBJ_STORE` performance program. For the detailed IT8 owner validation plan, see
`.memory/handoffs/obj-store-partial-index-it8.md`. For narrative implementation detail, see
`.memory/logs/obj_index_partial_implementation.md`.

## Status

```text
OBJ_PERF_FINAL=LOCAL_COMPLETE_AWAITING_IT8
OBJ_INDEX_SLICE_1=LOCAL_COMPLETE_AWAITING_IT8
OBJ_STORE_SLICE_1=NO_CHANGE_JUSTIFIED
PUSHED=NO
```

## What was built

A demand-driven, context-scoped partial commit index (`ZAOG_OBJ_PIDX` + `ZAOG_OBJ_COVER`) that lets
a small filtered request (K objects) avoid the full O(F) `ZAOG_OBJ_INDEX` rebuild that previously
ran on every cold filtered Stage/Diff for a never-before-indexed commit. The full commit→tree BFS
walk itself is unchanged in shape (no safe path-prefix-pruning oracle exists — candidates A/B were
evaluated and rejected with source proof), but persistence is now bounded to the caller's own K
objects instead of the commit's full F files, and a warm repeat request for the same filter/context
serves entirely from persisted coverage facts with zero tree walk.

- **New DDIC**: `ZAOG_OBJ_COVER` (per-object resolution facts: FOUND / RESOLVED_NO_FILES /
  RESOLVED_NOT_PRESENT_REMOTE / UNRESOLVED_MISSING_LOCAL_DATA, keyed including `CONTEXT_HASH`),
  `ZAOG_OBJ_PIDX` (FILTERED-mode positive file rows, `CONTEXT_HASH` as a real key field so two
  contexts' rows for the same object/path physically coexist). `ZAOG_OBJ_INDEX` gains a non-key
  `CONTEXT_HASH` column (safe because `rebuild_index` always purges-then-rewrites the whole commit).
- **New class**: `ZCL_ABAPGIT_ORTEC_OBJ_COVER` (context-hash computation, chunked coverage
  read/write, non-fatal write-failure diagnostics).
- **Changed class**: `ZCL_ABAPGIT_ORTEC_OBJ_INDEX` — `is_index_ready`/`rebuild_index`/
  `select_rows_for_filter` are now context-aware; new `ensure_filtered_coverage` (warm-complete →
  warm-coverage → backoff-short-circuit → `walk_filtered` fallback, in that order); new
  `walk_filtered` (bounded FILTERED-mode walk, never touches `ZAOG_OBJ_INDEX`); new
  `invalidate_commit_index` (atomic 3-table purge, replacing two previously-separate standalone
  deletes); new `select_partial_rows_for_filter`.
- **Changed class**: `ZCL_ABAPGIT_ORTEC_CACHE_ADMIN` — `clear_repo` now also purges
  `ZAOG_OBJ_COVER`/`ZAOG_OBJ_PIDX` under the same canonical repo lock `walk_filtered`/`rebuild_index`
  use (previously a different, non-conflicting lock — a real race was closed here, AR-2-03).
- **Changed class**: `ZCL_ABAPGIT_ORTEC_FILTER_WALK` — `get_remote_files_for_stage` computes a
  best-effort `iv_current_remote` and threads it down, enabling the strong
  `RESOLVED_NOT_PRESENT_REMOTE` fact only when it is safe (graph-complete AND the requested commit
  actually is the known current remote tip).
- **`ZAOG_OBJ_STORE` / `ZCL_ABAPGIT_ORTEC_OBJ_STORE`**: zero code changes. Verified
  (`.memory/logs/obj_store_performance_scan.md`) that every OS-A/B/C/D/F candidate is already
  correctly implemented and that no new code in this program reaches the one known unbounded
  caller (`get_all_objects`/`populate_cache`).

## Process notes for anyone resuming this topic

- Design went through 3 full adversarial review cycles (`.memory/reviews/obj_index_partial_adversarial.md`)
  before reaching `APPROVE` with 0 open blockers/majors. Two BLOCKER findings were rejected and
  reopened once each (context-blind `ZAOG_OBJ_INDEX` overwrite risk, missing current-remote
  parameter) before the final `ZAOG_OBJ_PIDX`/`iv_current_remote` design closed them for good — read
  that file's "Cycle 2"/"Cycle 3" sections before assuming any single-context-column design for a
  filtered/partial table is safe without re-deriving why it wasn't here.
- The `ortec-abapgit-implementation-senior` subagent reliably produced correct, sometimes
  genuinely sophisticated PRODUCTION code (e.g. `walk_filtered`'s full backoff/coverage logic) but
  returned "Agent completed with no output" on every one of its 4 calls in this program, and each
  time silently left some or all of the requested TEST code unwritten (once, an entire declared
  method body). This was NOT caught by `get_errors`. Always run a `Compare-Object` of declared
  (`METHODS`/`CLASS-METHODS`) vs. implemented (`METHOD ... .`) names on every touched file after
  any implementation subagent call - see `/memories/repo/git-state-notes.md` for the exact
  one-liner and its "shared declaration line" false-positive caveat.
- A post-implementation performance audit (mandatory per the mission's own gate sequence) caught a
  real, previously-undetected regression: `select_rows_for_filter` had its `iv_context_hash`
  parameter added in Slice 1b but never actually used in the method body, and was never chunked as
  the design's own AR-1-04 closure required - it survived Slices 1b/2/3 and every prior gap-fill
  verification pass undetected, because no existing test exercised that method above the chunk
  boundary or under a mismatched context. Fixed in the dedicated PS-001/PA-001 commit. This is the
  strongest evidence in this program for never skipping the mandatory performance scan +
  IMPLEMENTATION_AUDIT sequence even when every individual slice's own checks passed clean.

## Next step

Owner executes `.memory/handoffs/obj-store-partial-index-it8.md`'s activation order and validation
plan on the real IT8 system. Do not mark `SAP_VALIDATED_COMPLETE` before that.
