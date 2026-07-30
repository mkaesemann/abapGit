# Implementation handoff — Variant B Package E, slice E1-PERF-A

```text
PACKET=COMPACT_HANDOFF_V1
TASK=E1_PERF_A_IMPLEMENTATION
BASELINE=1437a87509e1d0392b57a965ad7d6885abb0a3e4
CHECKPOINT_1_VALIDATED_HEAD=3c77d898b62f3b0464e48cf59844e2e30c7b6e89
BASE_COMMIT=77b66464 (superseded 5000-row local candidate, not imported)
STATUS=LOCAL_COMPLETE, PENDING_IT8 (revised candidate)
```

## Revision history

- `77b66464` — first local candidate, batch size `1000 -> 5000`. Never
  imported into IT8. Superseded before owner review completed, based on
  the owner's assessment that 5000 rows is too conservative relative to
  measured DB round-trip/array-DML setup cost.
- This checkpoint (follow-up commit on top of `77b66464`) — revises the
  candidate batch size `5000 -> 30000`, per explicit owner direction
  (measured baseline: 42000 rows, ~9.39s rebuild, 41 write packages at
  the OLD 1000-row size). This is the candidate to import into IT8.

## Contract implemented (E1-A, revised)

- Target: `ZCL_ABAPGIT_ORTEC_OBJ_INDEX=>REBUILD_INDEX`'s bulk index-row
  write batching.
- Original batch size: `1000` (bare literal).
- Superseded candidate: `5000` (never imported).
- Revised/current candidate batch size: `30000` (named `CONSTANTS
  c_index_write_chunk_size`).
- Byte bound: `zaog_obj_index` = 11 fixed-width `CHAR` DDIC fields = 730
  bytes/row (re-verified against the live table XML); `30000 * 730 ≈
  21.9 MB` (~21.37 MiB) row-payload buffer. This is the row-payload lower
  bound, not total ABAP/DB-interface runtime allocation — see the
  performance audit for the full risk assessment.
- Deliberately conservative, still explicitly bounded (not unbounded, not
  derived from total row count): a possible future 50,000-row/1-package
  candidate is `NOT_AUTHORIZED_PENDING_30000_MEASUREMENT`.
- No change to: index row contents, `is_index_ready` readiness semantics,
  the completion marker mechanism, repo-scoped lock acquire/release,
  transaction/commit ownership, filtering, DDIC, or secondary indexes.
  Verified by isolated single-constant diff in the production file (see
  below).

## Performance model (candidate matrix)

| Batch size | Row payload | Packages @ 42000 rows | Status |
| ---: | --- | ---: | --- |
| 1,000 | 0.73 MB (~0.70 MiB) | ~42 (41 measured) | Original (live-measured baseline) |
| 5,000 | 3.65 MB (~3.48 MiB) | 9 | Superseded candidate, never imported (`77b66464`) |
| 10,000 | 7.30 MB (~6.96 MiB) | 5 | Not selected |
| 20,000 | 14.60 MB (~13.92 MiB) | 3 | Not selected |
| 30,000 | 21.90 MB (~21.37 MiB) | 2 | **Current candidate (this checkpoint)** |
| 50,000 | 36.50 MB (~34.81 MiB) | 1 | `NOT_AUTHORIZED_PENDING_30000_MEASUREMENT` |

Row-payload figures are the fixed-width-row lower bound only (`rows *
730 bytes`), not a claim of total runtime peak memory — actual ABAP
work-area/DB-interface overhead adds to this; see the performance audit
for the qualitative risk discussion.

## Files changed (this follow-up, on top of `77b66464`)

- `src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap` (production):
  - Changed `CONSTANTS c_index_write_chunk_size TYPE i VALUE 5000.` to
    `VALUE 30000.` and updated its ABAP doc comment. This is the ONLY
    behavioral change in the file — same statement, same private
    constant, same single use site inside `rebuild_index`.
- `src/ortec/git/zcl_abapgit_ortec_obj_index.clas.testclasses.abap`
  (test): replaced the 3 old 5000-boundary-specific tests
  (`index_chunk_below_boundary`, `index_chunk_at_boundary`,
  `index_chunk_above_boundary`) with 1 consolidated test
  (`index_bulk_rows_preserved`, 5000-row fixture) proving bulk
  multi-row correctness on the single final-flush path below the new
  30000-row boundary. `index_empty_no_match` (zero-row path) and all 5
  Checkpoint-1 tests (`marker_required_for_ready`, `build_commit`,
  `index_no_cross_commit_leak`, `ready_rejects_other_commit`,
  `ready_accepts_exact_commit`, `index_chunk_boundary_ok`) are
  behaviorally unchanged (only `index_chunk_boundary_ok`'s comment was
  corrected to stop claiming it still crosses the active boundary).
  `build_bulk_commit` fixture helper unchanged.

## Known, documented local test-coverage limitation

Building 30,000+ complete Git objects (blob + tree-node + hash) in an
ABAP Unit `DURATION SHORT` test is not appropriate — excessive runtime
cost for no correctness benefit over the already-proven chunking
algorithm (which is unchanged; only the threshold constant changed). The
constant is `PRIVATE` with no `LOCAL FRIENDS` declared, and adding one
would exceed this task's explicit "change only the named constant" scope.
Consequently:

- The exact-boundary/multi-package in-loop flush behavior AT 30000 rows
  is NOT exercised locally in this checkpoint.
- Local tests instead prove: bulk multi-row correctness on the (now
  exclusively reachable) single final-flush path, unchanged zero-row
  behavior, and unchanged marker/no-cross-commit-leak invariants via the
  untouched Checkpoint-1 tests.
- Actual package count and multi-flush correctness at 30000 rows must be
  confirmed via the owner's live IT8 SAT/SQL measurement
  (`ACTUAL_MODIFY_PACKAGE_COUNT`, expected `2` at ~42000 rows).

## Validation performed this session (all local)

- Verified `git` HEAD was exactly `77b66464` and its diff contained only
  the expected 6 files before editing (no unrelated productive drift).
- Re-read the complete `rebuild_index` method and confirmed all 8
  required correctness invariants hold: array `MODIFY ... FROM TABLE`
  (not FAE), fixed-width row type, `CLEAR lt_rows` immediately after
  every flush, no second full in-memory copy of the package before the
  DB call, no `COMMIT WORK` per package, the repo lock already spans the
  complete rebuild (unchanged), the completion marker is written after
  all data packages, and failure before the marker cannot leave a false
  `is_index_ready` result in STRICT mode (unchanged `CATCH` structure).
- `get_errors` (ADT language server): clean on both changed files.
- Method-name length scan (`Select-String` for `>30` chars): 0 violations
  across the whole test file.
- Structural balance: production `METHOD`/`ENDMETHOD` = 6/6 (unchanged
  count); test file `METHOD`/`ENDMETHOD` = 11/11 (13 - 3 + 1); test file
  `ENDCLASS` = 2 (definition + implementation, correct).
- `grep` for `COMMIT WORK` in the production file: 0 matches (unchanged).
- `grep` for `c_index_write_chunk_size`: exactly 2 matches (1 declaration,
  1 use site) — no hidden second consumer.
- `git diff --check` (working tree, pre-stage): 0 whitespace errors.
- Full correctness regression review: see
  [regression_variant_b_package_e_e1_perf_a.md](../logs/regression_variant_b_package_e_e1_perf_a.md)
  — PASS, 0 blocking findings.
- Focused performance scan: see
  [performance_scan_variant_b_package_e_e1_perf_a.md](../logs/performance_scan_variant_b_package_e_e1_perf_a.md)
  — PASS, 0 findings.
- Senior implementation performance audit: see
  [performance_audit_variant_b_package_e_e1_perf_a.md](../logs/performance_audit_variant_b_package_e_e1_perf_a.md)
  — PASS, `BOUNDED_SAFETY=PASS`, `BLOCK_PRODUCTION_SCALE=NO`.

## Explicitly deferred (per run brief, not an omission)

- Package/flush-count-matches-`ceil(rows/batch_size)` verification has no
  safe production observability seam (no counter authorized by the
  approved design). Per the run brief's own fallback instruction, this is
  deferred to the owner's post-import IT8 SAT/SQL measurement rather than
  added as new production instrumentation.
- No live ABAP Unit/ATC execution occurred this session. The connected
  live SAP diagnostic tools in this workspace are confirmed (per
  `/memories/repo/git-state-notes.md`) to point at an unrelated, mismatched
  system — not this repo's IT8 target — so they were not used for
  validation, consistent with the run brief's "local tooling is not
  sufficient for SAP validation" guidance.

## Not touched (explicit scope boundary)

- `.memory/state.md` was NOT modified this session (per explicit
  instruction: "Do not update state.md before IT8 validation").
- No other Package E slice, no Package F, no Package D work was reopened
  or touched.
- Commit `77b66464` was NOT amended; this is a new follow-up commit on
  top of it.

## Next step

Owner imports the chain through the follow-up commit into IT8 and runs
activation/syntax check, ABAP Unit, ATC, and a comparable ~42,000-row
rebuild with SAT/SQL measurement, reporting at minimum: `INDEX_ROWS`,
`ACTUAL_MODIFY_PACKAGE_COUNT` (expected `2`), `REBUILD_TIME`,
`TOTAL_ELAPSED`, `PEAK_MEMORY` (if available), `LOCK_WAIT` (if
available), `SYSTEM_NO_ROLL`, `TIME_OUT`, `DBSQL_STMNT_TOO_LARGE`,
`READY_MARKER`, `FUNCTIONAL_RESULT`. The 30,000-row candidate is not
declared superior to the 1,000-row baseline (or the never-imported
5,000-row candidate) until measured; `.memory/state.md` is updated only
after that measurement.
