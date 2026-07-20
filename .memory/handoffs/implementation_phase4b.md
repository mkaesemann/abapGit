# Handoff — Implementation Phase 4b: deferred six-state model + D4 switch (2026-07-11)

## Prerequisite confirmed
Michael re-imported the Phase 4 rename-fix commit (`cd2b602f`) into IT8 and confirmed the
syntax error is gone. Per Michael's explicit choice when asked what to do next
("Complete deferred Phase 4 scope"), this session implements the six-state object/path model
and the D4 STRICT/RELAXED completeness switch that Phase 4 deliberately deferred.

## Scope decision (read this before assuming this is the full six-state model everywhere)
The target design's six states (`LOADED`, `INDEXED_NEEDS_LOAD`, `NOT_BUFFERED`,
`UNKNOWN_NEEDS_FETCH`, `CONFIRMED_ABSENT`, `CORRUPT_OR_INCOMPLETE`) are defined at the
*object* level. Investigation this session found they split cleanly across two existing
layers:
- `zcl_abapgit_ortec_obj_store` / `zcl_abapgit_ortec_missing_obj` (SHA1-keyed, no path
  context): only `LOADED` / `INDEXED_NEEDS_LOAD` / `NOT_BUFFERED` / `CORRUPT_OR_INCOMPLETE`
  are meaningful here - a raw blob/tree SHA1 has no "deleted" concept independent of a path.
- `zcl_abapgit_ortec_obj_index` (path-keyed, per-commit): this is where `UNKNOWN_NEEDS_FETCH`
  (index not yet resolved) and `CONFIRMED_ABSENT` (index resolved, filtered object genuinely
  absent from that tree) belong.

Rather than force a sweeping rewrite of the already-shipped, already-regression-validated
Phase 3/4 pipeline (`facade -> filter_walk -> obj_index -> repo_status.calculate`) to
literally thread a `ty_object_state` value through every call, this phase:
1. Adds the six-state vocabulary as real, documented `CONSTANTS` (not free-floating design
   prose) with exactly the semantics from `target_design.md` §2 - including the
   non-negotiable rule that only `CONFIRMED_ABSENT` may ever map to a remote-Deleted verdict.
2. Adds the D4 `cs_absent_strictness` STRICT/RELAXED compile-time constant to
   `zcl_abapgit_ortec_git_switch`, exactly as Michael's D4 decision specified ("implement
   both modes in code... controlled via a constant... so speed can be benchmarked
   side-by-side").
3. Finds and fixes ONE concrete, real correctness gap this analysis surfaced (see below),
   gated on that switch, so the six-state model has genuine consuming logic rather than being
   empty scaffolding - mirroring exactly how Phase 4 itself was scoped.
4. Ties the vocabulary into the one place a genuine `CORRUPT_OR_INCOMPLETE` condition already
   existed (a silent generic error message) for clearer diagnostics, with zero behavior
   change to the safe fallback chain.

No standard abapGit code was touched in this phase (even more minimal-touch than Phase 1/3/4);
everything is contained inside `zcl_abapgit_ortec_obj_store`, `zcl_abapgit_ortec_git_switch`,
`zcl_abapgit_ortec_obj_index`, and `zcl_abapgit_ortec_missing_obj` (doc-comment only).

## The concrete bug found and fixed
`zcl_abapgit_ortec_obj_index=>is_index_ready` decided a per-commit filtered index was
"complete" if **any** row existed for `(repo_key, commit_sha1)` with `idx_status = 'R'`. Every
data row is marked `'R'` as soon as it is inserted during the tree walk in `rebuild_index`
(batched every 1000 rows) - **before** the walk finishes. The `$IDX/__READY__` completion
marker row was only written when the walk completed **and** found zero filter-relevant
objects (`lv_row_count = 0`), never for the (overwhelmingly common) case of a walk that
completed and found real rows.

Consequence: if `rebuild_index` were ever interrupted partway through (a raised exception from
a corrupt tree, a missing object, or a decode failure - all of which propagate out of the walk
today), the data rows already `MODIFY`'d in earlier 1000-row batches remain in the database
(no `COMMIT WORK`/rollback boundary inside the walk itself protects against this once the
surrounding request eventually commits), all already marked `'R'`. The **next** call to
`ensure_index`/`is_index_ready` for that exact commit would then see those partial rows and
wrongly conclude the index was fully built - silently serving an **incomplete** filtered file
set. A file that exists in the real tree but wasn't reached before the interruption would then
be missing from `rt_files`, which the caller could misread as "this file doesn't exist
remotely" - exactly the non-negotiable invariant this whole design exists to prevent
("missing local cache data is not remote deletion").

This was a **latent** bug (not yet observed/reported in production) surfaced by deliberately
reasoning through the six-state model's completeness requirements; it was not previously
covered by any test.

### Fix
- `rebuild_index` now **always** writes the `$IDX/__READY__` completion marker as the last
  step of a fully successful walk, regardless of how many rows were found (previously only
  for the zero-rows case).
- `is_index_ready` is now gated by `zcl_abapgit_ortec_git_switch=>cs_absent_strictness-mode`:
  - **STRICT (default, ships)**: requires the explicit marker row. A partial/interrupted
    rebuild is correctly detected as not-ready and is rebuilt again (self-healing; the DELETE
    at the top of `rebuild_index` already wipes any partial data before re-walking).
  - **RELAXED (benchmark-only, never ships as default)**: reintroduces the previous "any row"
    check, explicitly documented as not distinguishing a complete index from a partial one -
    kept only so the marker check's cost can be measured on very large repositories.
- `build_files_from_rows`'s remaining generic "blob missing" raise (which can only trigger
  when a SHA1 exists in the store as the *wrong type* - `get_objects` already guarantees
  existence for every requested SHA1 or raises first) now reports this explicitly as
  `CORRUPT_OR_INCOMPLETE` with the affected path, and the comment states plainly this must
  never be read as a deleted-file signal.

## What was implemented (full list)
- `zcl_abapgit_ortec_obj_store`: new `ty_object_state` + `cs_object_state` constants (six
  states, fully documented, including the non-negotiable Deleted-only-on-CONFIRMED_ABSENT
  rule).
- `zcl_abapgit_ortec_git_switch`: new `cs_absent_strictness` constants (`mode_strict`,
  `mode_relaxed`, `mode` defaulting to `STRICT`), documented as a compile-time,
  redeploy-to-change benchmarking switch (matching D6's existing style in this class).
- `zcl_abapgit_ortec_obj_index`: the `rebuild_index`/`is_index_ready` marker fix above, and
  the improved `CORRUPT_OR_INCOMPLETE` diagnostic in `build_files_from_rows`. Removed the
  now-dead `lv_row_count` local variable.
- `zcl_abapgit_ortec_missing_obj`: doc-comment-only update explicitly mapping its existing
  behavior onto the new vocabulary (`NOT_BUFFERED` -> `LOADED`; never resolves
  `CONFIRMED_ABSENT` itself). No functional change.
- Tests (`zcl_abapgit_ortec_git_tests.clas.testclasses.abap`):
  - `ltcl_obj_store=>object_state_constants` - locks the six string values.
  - `ltcl_switch=>absent_strictness_default` - locks the shipping default to `STRICT`.
  - New `ltcl_obj_index` class, `marker_required_for_ready` test: builds a real
    commit -> `/src/` tree -> `zprogram.prog.abap` blob graph (same filename/path convention
    already proven in `zcl_abapgit_filename_logic`'s own tests), calls
    `get_files_for_filter` once to prove the marker is now written even when rows exist, then
    directly corrupts the row's `file_path` and deletes the marker (simulating a rebuild
    interrupted after the row but before the marker) and calls `get_files_for_filter` again to
    prove STRICT mode detects the incompleteness and rebuilds from the real stored objects
    rather than trusting the stale corrupted row.

## Validation performed
- `get_errors` clean on all 5 touched files.
- `npx abaplint` run before/after via `git stash` on just the touched files, with a per-rule,
  per-file occurrence-count diff (not a raw line diff, which is meaningless across line-number
  shifts) to isolate genuinely new findings from pre-existing style debt:
  - `zcl_abapgit_ortec_obj_index.clas.abap`: +1 `select_single_full_key`, +2
    `sql_escape_host_variables`, +1 `strict_sql` - all from the STRICT-branch duplicate of the
    exact same `SELECT SINGLE` pattern the original code already used (same style already
    flagged on the RELAXED branch and on hundreds of other pre-existing statements in this
    codebase); no new style pattern introduced.
  - `zcl_abapgit_ortec_git_tests.clas.testclasses.abap`: two genuinely fixable findings
    (`unused_variables`, `use_new`) were found and fixed (consumed the fetched marker value
    instead of leaving it unused; changed `CREATE OBJECT` to `NEW #( )`). The remaining deltas
    (`check_subrc`, `select_single_full_key`, `sql_escape_host_variables`, `strict_sql`) match
    the same pre-existing SQL-style convention already used dozens of times elsewhere in this
    same test file.
- Could not run the new `marker_required_for_ready` test end-to-end (the local
  ABAP-to-JS transpile+execute harness remains blocked by the same pre-existing, unrelated
  dependency drift documented since Phase 1+3). Syntax-verified only, same caveat as every
  other test added this session pending real ABAP Unit execution on a system Michael controls.

## Deferred by design (not a gap)
- The full unified `zcl_abapgit_ortec_status_engine` (threading explicit per-file states
  through `zcl_abapgit_repo_status=>calculate`'s Added/Modified/Deleted/Unchanged
  comparison) is still not built. The existing comparison already satisfies the
  non-negotiable invariant today via a coarser mechanism (any resolution failure anywhere in
  the Ortec filtered path raises and triggers a full, always-correct `get_files_remote()`
  fallback for the whole operation) - it is safe, just not maximally fast for the rare
  single-missing-object case. Building the full status engine to make that one case faster
  is a larger, standard-code-adjacent lift explicitly flagged in the original design as going
  together with Phase 3's facade; introducing it now without a concrete second consumer would
  repeat the exact premature-abstraction risk already avoided in Phase 3.
- D6 (`cs_tip_validation-mode` PER_OP/TTL) was not touched - out of scope for the D4-only
  request this session; the existing per-op `branches(url)` tip check in
  `zcl_abapgit_ortec_filter_walk` is unchanged.

## Next recommended step
- This has NOT been imported/compiled on a real SAP system yet. Recommend a syntax-check-only
  IT8 import first (same caveat pattern as Phase 4), then real functional validation
  alongside the still-outstanding Phase 4 functional validation.
- Remaining design-approved larger slices: Phase 5 (delta-base completeness + protocol
  hardening, D3) or building the full status engine. Recommend explicit go-ahead before
  starting either, consistent with the same reasoning that led to this session's scope choice.
