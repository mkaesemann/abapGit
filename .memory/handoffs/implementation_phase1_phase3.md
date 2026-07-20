# Handoff — Implementation Phase 1 + Phase 3 (2026-07-11)

## Summary
Continued from the approved design (all D1–D7 resolved). Implemented and independently
regression-validated two phases in this session:

### Phase 1 — Regression fix (read-path gate removal) + walk_tree repo_key bug fix
- Removed the `is_active_for_repo` hard gate from the read-only filtered walk
  (`zcl_abapgit_ortec_filter_walk=>get_remote_files_for_stage`) and from both standard
  hooks (`zcl_abapgit_stage_logic`, `zcl_abapgit_gui_page_diff_base`). The data-validity
  fallback chain to `get_files_remote` is preserved everywhere a gate was removed.
- Fixed `zcl_abapgit_git_porcelain=>walk_tree` to thread an explicit `iv_repo_key`
  through its signature, its recursive call, and into
  `zcl_abapgit_ortec_obj_store=>get_object`, eliminating a real wrong-store-read risk
  in multi-repo sessions (previously silently fell back to a stale session-cached
  repo_key in the object store).
- Added ABAP Unit regression test `walk_tree_repo_key_isolation` in
  `zcl_abapgit_git_porcelain.clas.testclasses.abap` (existing `ltcl_git_porcelain` local
  friend class) that proves the fix: stores a tree in repo A, poisons the object store's
  session cache with repo B, then calls `walk_tree` with an explicit `iv_repo_key = A`
  and asserts the correct file is returned.

### Phase 3 — Facade + it_remote seam + ping-pong removal
- New class `zcl_abapgit_ortec_git_facade` (single static method
  `resolve_filtered_remote`) is now the one Ortec entry point both standard hooks call
  (via the same dynamic `CALL METHOD ('...')=>('...')` indirection pattern used before,
  preserving zero hard compile dependency from standard code onto `ZCL_ABAPGIT_ORTEC_*`).
  It delegates to `zcl_abapgit_ortec_filter_walk=>get_remote_files_for_stage`, adding no
  duplicate logic.
- Added an optional `it_remote` parameter to `zcl_abapgit_repo_status=>calculate`
  (checked via `IS SUPPLIED`). When supplied, the pre-resolved remote set is reused
  instead of calling `get_files_remote` a second time. Fully backward compatible: `ii_repo`
  remains the sole mandatory parameter, so all 8 pre-existing call sites (including the
  single-unnamed-argument shorthand calls) are unaffected.
- Removed the `set_files_remote` → `calculate` (re-fetch) → `refresh` ping-pong from both
  `zcl_abapgit_stage_logic~get` and `zcl_abapgit_gui_page_diff_base~get_files_and_status`.
  Neither hook mutates the repository's cached remote-file baseline anymore for filtered
  operations. The diff hook also now passes `it_local` into `calculate`, removing a
  previously-redundant second `get_files_local_filtered` call.

## Tooling fix (unrelated, pre-existing)
- `abaplint.json` had 45 occurrences of an invalid `(?i)` inline regex flag (not
  supported by this Node.js regex engine) in naming-convention rules, blocking ALL
  `npm run abaplint` output with a crash. Fixed by stripping the invalid markers
  (verified via Node that removing `(?i)` produces valid, equivalent-intent patterns).
  This predates this session's work (already present as an uncommitted working-tree
  change) and is unrelated to the Ortec Git rework, but was blocking regression tooling
  so it was fixed to unblock validation.

## Validation performed
- `npx abaplint` run to completion after the fix; 0 new issues in any of the 9
  changed/new ABAP files (only pre-existing repo-wide style debt elsewhere, confirmed by
  diffing against the stashed/original baseline).
- Independent regression subagent (`ortec-abapgit-regression`) validated both phases
  separately: PASS_WITH_NOTES for each, no hard-stop violations. See
  `.memory/logs/regression_phase1.md` and `.memory/logs/regression_phase3.md`.
- `npm run unit` (full ABAP-to-JS transpile + ABAP Unit execution) is blocked by
  PRE-EXISTING, UNRELATED dependency drift against freshly cloned
  open-abap-core/open-abap-gui companion libraries (syntax errors in
  `zcl_abapgit_hash.clas.abap`, `zcl_abapgit_cts_integration.clas.abap`, and the
  `zif_abapgit_cts_api`/`prefetch_descriptions` test injection). Confirmed unrelated to
  any of the 9 files touched in this session. Not fixed — out of scope for the Ortec Git
  rework. This means the new `walk_tree_repo_key_isolation` test is syntax-verified but
  has not yet been executed end-to-end; real execution needs either a working local
  transpile environment (open-abap-core version alignment) or a real SAP/ADT system.

## Deferred by design (not a gap)
- `zcl_abapgit_ortec_status_engine` (mentioned in the target design) was deliberately
  NOT created in Phase 3. Its real value (six-state model, `CONFIRMED_ABSENT` enforcement)
  only exists once Phase 4 introduces the explicit object/path states. Creating it now
  would be an empty wrapper with no behavior difference from calling
  `zcl_abapgit_repo_status=>calculate` directly — deferred to avoid premature abstraction.

## Next recommended step
- Phase 4: explicit six-state model (`LOADED`, `INDEXED_NEEDS_LOAD`, `NOT_BUFFERED`,
  `UNKNOWN_NEEDS_FETCH`, `CONFIRMED_ABSENT`, `CORRUPT_OR_INCOMPLETE`) + bulk
  missing-object collection/retry (`zcl_abapgit_ortec_missing_obj`) + the D4
  strict/relaxed switch constant. This is a larger lift (new persistence-facing class,
  changes to `walk`/`walk_tree` raise behavior) — recommend a fresh design-detail pass
  before coding, or explicit go-ahead to proceed directly.
- Alternatively, first secure a working ABAP Unit execution environment (fix the
  open-abap-core drift, or validate on a real system) before continuing further phases,
  to get real (not just static) regression confidence on Phases 1 and 3.
