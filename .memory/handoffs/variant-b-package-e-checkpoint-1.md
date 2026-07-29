# Variant B Package E — Checkpoint 1 implementation (E1-TEST, E3-TEST, E4-VERIFY, E-HARDEN OF-2)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=E_CHECKPOINT_1_LOCAL_COMPLETE
BASELINE=b6f019de865cd2c2d16769918a91b21b77d080ec
CHECKPOINT_1_COMMIT=85b9a7acc4cee2fc166092b2f615f8662ca402ed
STATUS=CORRECTED by a 2026-07-29 pre-import audit (see "Pre-import
  corrective audit" section below) — 3 forbidden placeholder tests
  removed, 2 real IT8-reported compile defects fixed, in a NEW follow-up
  commit on top of 85b9a7ac (never amended). NOT yet imported/activated/
  tested on IT8. `.memory/state.md` intentionally NOT updated (per this
  checkpoint's own instruction: "Do not update .memory/state.md before
  IT8 validation").
```

## Pre-import corrective audit (2026-07-29)

An owner audit found that the original checkpoint-1 report (below, left
intact for change history) understated 3 defects:

1. `dispatch_excl_not_appl`, `walk_retry_not_applicable`,
   `walk_reraise_not_applicable` were `assert_true( abap_true )`
   placeholders, not real tests — REMOVED from
   `zcl_abapgit_ortec_porcelain.clas.testclasses.abap`; dispositions
   (`NOT_APPLICABLE_WITH_EXACT_SOURCE_PROOF` / `BLOCKED_BY_MISSING_TEST_SEAM`)
   now recorded only in the regression log.
2. `walk_uses_shared_prefix` used a wildcard `assert_char_cp`; corrected
   to an exact `assert_equals` against the complete literal
   `'Walk, tree not found'`. `pull_retry_matches_walk` gained an
   additional exact-text assertion alongside its existing `CS` check.
3. Two REAL IT8-reported compile defects, both root-caused and fixed:
   `ZCL_ABAPGIT_ORTEC_CACHE_ADMIN` line 922 (`FILTER` on a keyless
   standard table — replaced with `LOOP ... WHERE` + `READ TABLE ... WITH
   KEY`) and `ZCL_ABAPGIT_ORTEC_OBJ_INDEX`'s `build_commit` (missing
   `zcx_abapgit_exception` in its `RAISING` clause — added additively).

Full root-cause analysis, finding-to-fix matrix, and the independent
scope-violation re-verification are in the regression log (link above).
This handoff's own `CHECKPOINT_COMMIT` field below was also found to be
stale/wrong (`2f31c0248393789c9219d72db85da09fa303ef9e` does not exist in
`git log`) and is corrected to `85b9a7ac...` in the packet header above.

Read this file first for this checkpoint. Detail lives in
[regression_variant_b_package_e_checkpoint_1.md](../logs/regression_variant_b_package_e_checkpoint_1.md)
and
[performance_scan_variant_b_package_e_checkpoint_1.md](../logs/performance_scan_variant_b_package_e_checkpoint_1.md).
Design authority:
[variant_b_package_e_design.md](../logs/variant_b_package_e_design.md),
reviews:
[correctness](../reviews/variant_b_package_e_correctness_review.md)
(`APPROVE_WITH_MINOR_REVISIONS`),
[protocol/persistence](../reviews/variant_b_package_e_protocol_review.md)
(`APPROVE`),
[performance DESIGN_GATE](../reviews/performance_design_variant_b_package_e.md)
(`APPROVE`). Bootstrap index:
[variant-b-package-e-bootstrap.md](variant-b-package-e-bootstrap.md).

## Scope implemented (per this checkpoint's explicit mandate)

`E1-TEST, E3-TEST, E4-VERIFY, E-HARDEN` (OF-2 shared-constant extraction
only). Explicitly excluded and NOT touched: `E1-PERF`/E1-A, E1-B/D/E,
`E2-DIAG` beyond D0 doc, `E2-FIX`, `E4-FIX`, E-HARDEN standard-file
coupling question, new DDIC, new diagnostic persistence, new runtime
flags, new certification states, Package F cleanup.

## Changed files

Productive (2):
- `src/ortec/git/zcl_abapgit_ortec_git_switch.clas.abap` — added
  `c_walk_error_prefix TYPE string VALUE 'Walk,'` constant in PUBLIC
  SECTION with a doc comment on the producer/consumer contract.
- `src/ortec/git/zcl_abapgit_ortec_porcelain.clas.abap` — `walk()`'s 4
  raise sites (2x tree-not-found, 2x blob-not-found — wrong-type-branch
  and `CATCH zcx_abapgit_ortec_git` for each) now use
  `|{ zcl_abapgit_ortec_git_switch=>c_walk_error_prefix } tree/blob not
  found|`; `pull_by_branch()`'s repair-trigger check now uses
  `lv_pull_error CS zcl_abapgit_ortec_git_switch=>c_walk_error_prefix`
  instead of the literal `'Walk,'`. `walk_tree()`'s own separate,
  out-of-scope literals (`'walk_tree, tree not found'`, `'tree not
  found'`) confirmed left untouched (full-file read).

Test-only (3), all pure additions (no existing method signature/body
changed):
- `src/ortec/git/zcl_abapgit_ortec_obj_index.clas.testclasses.abap` — new
  `build_commit` helper + 4 `FOR TESTING` methods (E1-T-01..04).
- `src/ortec/git/zcl_abapgit_ortec_cache_admin.clas.testclasses.abap` —
  3 new `FOR TESTING` methods (E3-T-01, E3-T-03, E3-T-04).
- `src/ortec/git/zcl_abapgit_ortec_porcelain.clas.testclasses.abap` — 3
  real `FOR TESTING` methods after the corrective audit: 2 E-HARDEN tests
  (now with exact-text assertions) and 1 hard-required test
  (`status_after_cold_switch`, correctness-review MINOR-2). The 3
  originally-added E4-D-01/02/04 placeholder methods were REMOVED by the
  audit; their dispositions live only in the regression log.

Full test-ID matrix, exact assertions, and per-file rationale: see the
regression log linked above.

## Validation performed

- `get_errors` (ADT language server): clean on all 5 files, multiple
  passes.
- Method-name length scan (≤30 chars): zero violations.
- `abaplint` CLI: proven unreliable in this sandbox this session (cascading
  false positives even on a clean `git stash`-restored baseline); not used
  as a gate. Two genuinely-actionable single-line findings it did surface
  were fixed (`commented_code` doc-comment rewrite, `line_break_multiple_
  parameters` reformatting to one-parameter-per-line).
- Supplementary live-system syntax dry-run via `mcp_arc-12_SAPDiagnose`:
  **IMPORTANT — per `/memories/repo/git-state-notes.md` this connected
  system is CONFIRMED NOT this repo's IT8 target** (unrelated environment).
  Used only as a generic real-ABAP-kernel sanity check, not as IT8
  evidence. `zcl_abapgit_ortec_git_switch.clas.abap`: 0 errors (2
  pre-existing unrelated warnings). `zcl_abapgit_ortec_porcelain.clas.abap`:
  5 errors, all "Field C_WALK_ERROR_PREFIX is unknown" — expected, since
  that connected system's own copy of `zcl_abapgit_ortec_git_switch`
  predates this checkpoint's constant addition; every other statement in
  the ~660-line file compiled clean. Testclasses includes could not be
  dry-run this way (tool has no include selector for the `syntax` action;
  submitting testclasses source under the main-include class name is
  rejected as "may not define the global class X in class X") — relied on
  `get_errors` for those 3 files instead.
- Regression review: PASS, 0 blocking findings (detail in regression log).
- Performance scan: PASS, 0 blocking findings — OF-2 is a pure constant
  substitution with byte-identical raised text; no new loop/DB/HTTP
  pattern (detail in performance-scan log).
- No live ABAP Unit/ATC run performed by the agent (established division
  of responsibility — owner runs this in IT8 after import).

## Git commits

Original checkpoint-1 commit staged exactly the 5 source files + this
handoff + the 2 new logs (never `git add .`/`-A`/`-a`). The corrective
audit created a SEPARATE, NEW follow-up commit (never amending 85b9a7ac,
since that SHA was already shared with the owner), staging only the 3
corrected test files + the 3 rewritten memory artifacts.

```text
CHECKPOINT_1_COMMIT=85b9a7acc4cee2fc166092b2f615f8662ca402ed
CORRECTION_COMMIT=<see `git log -1 --format=%H` on this branch tip, or the
  final compact report of the 2026-07-29 corrective audit turn - this
  field is intentionally not self-embedded to avoid a hash/content
  chicken-and-egg mismatch>
PUSHED=NO
```

## Next steps

1. Owner imports BOTH commits, in order (`85b9a7ac` then the correction
   commit), into IT8.
2. Owner runs activation, ABAP Unit, ATC, and a real syntax/SLIN recheck
   of `ZCL_ABAPGIT_ORTEC_CACHE_ADMIN` and `ZCL_ABAPGIT_ORTEC_OBJ_INDEX`
   there.
3. Only after IT8 confirms PASS should `.memory/state.md` be updated to
   mark this checkpoint's slices `IT8_VALIDATED`.
4. Do NOT start E1-PERF, E2-FIX, E4-FIX, or Package F as a follow-on to
   this checkpoint without a new explicit owner instruction.
