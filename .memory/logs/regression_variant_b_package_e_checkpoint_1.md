# Regression validation — Variant B Package E, checkpoint 1

```text
PACKET=COMPACT_HANDOFF_V1
TASK=E_CHECKPOINT_1_REGRESSION
BASELINE=b6f019de
SCOPE=E1-TEST, E3-TEST, E4-VERIFY, E-HARDEN (OF-2 constant extraction only)
STATUS=PASS (local static gates only — no live ABAP Unit run performed by
  the agent; per established division of responsibility the owner runs
  ABAP Unit + ATC in IT8 after import)
```

## Changed files (5, confirmed via `git status --short`)

Productive (2):
- `src/ortec/git/zcl_abapgit_ortec_git_switch.clas.abap` — added
  `c_walk_error_prefix` constant only, no behavior change to any existing
  method.
- `src/ortec/git/zcl_abapgit_ortec_porcelain.clas.abap` — 4 raise-site
  literals + 1 `CS` check literal in `walk()`/`pull_by_branch()` replaced
  by references to the new shared constant. Byte-for-byte equivalent
  strings (verified: `c_walk_error_prefix` = `'Walk,'`, template
  `|{ c_walk_error_prefix } tree not found|` = `'Walk, tree not found'`,
  matching the original literal exactly). `walk_tree()`'s own separate,
  out-of-scope literals (`'walk_tree, tree not found'`, `'tree not
  found'`) were confirmed left untouched (full-file read, this session).

Test-only (3):
- `src/ortec/git/zcl_abapgit_ortec_obj_index.clas.testclasses.abap` (E1-TEST)
- `src/ortec/git/zcl_abapgit_ortec_cache_admin.clas.testclasses.abap` (E3-TEST)
- `src/ortec/git/zcl_abapgit_ortec_porcelain.clas.testclasses.abap`
  (E-HARDEN-01/02, E4-VERIFY / MINOR-2)

No DDIC, `.github`, `.memory/state.md`, or diagram changes in this
checkpoint (confirmed via `git status --short` scope check — exactly the
5 files above are modified, nothing else).

## Test matrix (design doc `variant_b_package_e_design.md` §1/§7/§9)

| ID | Method | Outcome |
| --- | --- | --- |
| E1-T-01 | `index_no_cross_commit_leak` | New — no leak between two commits sharing repo_key |
| E1-T-02 | `ready_rejects_other_commit` | New — `is_index_ready` false for an unindexed commit |
| E1-T-03 | `ready_accepts_exact_commit` | New — `is_index_ready` true independently for each of 2 built commits |
| E1-T-04 | `index_chunk_boundary_ok` | New — 1200-row fixture crosses today's chunk boundary without hardcoding it |
| E3-T-01 | `f4_repo_state_only` | New — repo_state-only row surfaces real branch/remote_url |
| E3-T-02 | (existing `overview_large_sizes`/obj_store-only coverage) | CONFIRMED_CURRENT, not duplicated |
| E3-T-03 | `f4_commit_hist_only` | New — commit_hist-only row surfaces via 3rd dedup tier, distinct orphan label |
| E3-T-04 | `f4_dedup_prefers_state` | New — all 3 tiers populated, repo_state wins dedup |
| E4-D-01 | `walk_retry_not_applicable` | NOT_APPLICABLE placeholder — no HTTP transport mock seam (confirmed absent, matches `fresh_pull_unit_atomic` precedent) |
| E4-D-02 | `walk_reraise_not_applicable` | NOT_APPLICABLE placeholder, same reason |
| E4-D-03 | (existing `ensure_available` blob-missing-after-retry coverage) | CONFIRMED_CURRENT via grep, not duplicated |
| E4-D-04 | `dispatch_excl_not_appl` | NOT_APPLICABLE placeholder, rewritten to prose citing `zcl_abapgit_git_porcelain.clas.abap` lines ~525-534 |
| MINOR-2 (§9 row 9) | `status_after_cold_switch` | New, REAL working test (not a placeholder) — proves `zif_abapgit_status_calc~calculate_status` reports `rstate = modified`/`lstate` initial after a branch switch, using real `zcl_abapgit_status_calc=>get_instance` + `materialize_from_manifest` |
| E-HARDEN-01 | `walk_uses_shared_prefix` | New — `walk()`'s raised text matches `c_walk_error_prefix` exactly |
| E-HARDEN-02 | `pull_retry_matches_walk` | New — `pull_by_branch`'s `CS` check matches the same shared constant |
| E-HARDEN-03 | (existing `cs_absent_strictness-mode_strict` pinning test) | CONFIRMED_CURRENT, unrelated to OF-2, not touched |

Three placeholders (E4-D-01/02/04) are honest NOT_APPLICABLE stubs, not
full end-to-end coverage — this is a known, design-accepted gap (design
§6 already classifies the underlying scenarios as low-residual-risk, not
newly discovered here).

## Static/local validation performed

- `get_errors` (ADT language server): clean on all 5 files, checked
  multiple times across edits (final pass after all edits: 0 errors).
- Method-name length scan (all new methods ≤ 30 chars): zero violations,
  confirmed by direct enumeration of every new `METHODS`/`METHOD` name
  added this checkpoint.
- `abaplint` CLI: PROVEN UNRELIABLE in this sandbox this session (see
  handoff for the git-stash-comparison evidence) — not used as a gate.
  Two genuinely-actionable single-file findings it did surface
  (`commented_code`, `line_break_multiple_parameters`) were fixed anyway.
- Supplementary live-system syntax dry-run (`mcp_arc-12_SAPDiagnose
  action=syntax`): **NOTE per `/memories/repo/git-state-notes.md`, the
  connected `mcp_arc-12_*` system is CONFIRMED NOT this repo's IT8 target
  (unrelated environment, stale/mismatched ORTEC objects observed
  previously)** — its result is treated ONLY as a generic real-ABAP-kernel
  sanity check (useful for catching real compiler-only errors invisible to
  `get_errors`, per several documented past incidents), NOT as IT8
  validation. Results: `zcl_abapgit_ortec_git_switch.clas.abap` — 0 errors
  (2 pre-existing, unrelated doc-comment-position warnings only).
  `zcl_abapgit_ortec_porcelain.clas.abap` — 5 errors, ALL of the form
  "Field C_WALK_ERROR_PREFIX is unknown", exactly the 5 sites that
  reference the new constant — this is the EXPECTED artifact of dry-run
  checking this file's source in isolation against that connected system's
  own (older) copy of `zcl_abapgit_ortec_git_switch`, which does not yet
  have the new constant; it is not evidence of a defect in the submitted
  source (every other statement in the ~660-line file, all pre-existing
  logic, compiled clean). Testclasses includes could NOT be dry-run this
  way — submitting testclasses source under the class's main-include name
  is rejected with "You may not define the global class X in class X"
  (the tool has no include-selector for the `syntax` action); relied on
  `get_errors` (which IS include-aware) for the 3 test files instead.

## Targeted regression review (no live ABAP Unit run)

- No new method name collides with any existing method name in the same
  class (verified via the length-scan enumeration, which also lists all
  names — no duplicates found).
- No existing test method's signature or body was modified — all changes
  are pure additions (new `METHODS`/`METHOD` blocks appended after the
  last pre-existing one in each file), confirmed via `git diff` hunk
  review during editing.
- `zcl_abapgit_ortec_cache_admin.clas.testclasses.abap`'s new tests reuse
  the pre-existing `cleanup()` method (called from both `setup`/
  `teardown`), which already deletes `c_repo`/`c_other_repo` rows from
  every relevant table with `COMMIT WORK AND WAIT` — no new cleanup gap
  introduced for the new direct-`MODIFY` tests.
- `zcl_abapgit_ortec_obj_index.clas.testclasses.abap`'s `setup`/`teardown`
  already scope-delete `zaog_obj_store`/`zaog_obj_index` by `mc_repo` —
  new tests reuse the same constant, no new repo-key collision risk.
- Live ABAP Unit / ATC execution is explicitly NOT performed by the agent
  (per established division of responsibility); owner runs this in IT8
  after import.

## Verdict

```text
REGRESSION=PASS (local/static gates only)
BLOCKING_FINDINGS=0
NEXT=Owner imports checkpoint into IT8 and runs activation, ABAP Unit,
  and ATC.
```
