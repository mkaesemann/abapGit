# Regression validation — Variant B Package E, slice E1-PERF-A

```text
PACKET=COMPACT_HANDOFF_V1
TASK=E1_PERF_A_REGRESSION
BASELINE=1437a87509e1d0392b57a965ad7d6885abb0a3e4
CHECKPOINT_1_VALIDATED_HEAD=3c77d898b62f3b0464e48cf59844e2e30c7b6e89
BASE_COMMIT=77b66464 (superseded 5000-row candidate, not imported)
SCOPE=E1-PERF-A revision only (rebuild_index chunk-size constant raised
  5000 -> 30000, plus corresponding test-fixture correction). No other
  slice touched.
STATUS=LOCAL_REGRESSION_PASS, PENDING_IT8 (revised candidate)
```

## Change under review (this follow-up, on top of `77b66464`)

`src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap`:
1. `CONSTANTS c_index_write_chunk_size TYPE i VALUE 5000.` changed to
   `VALUE 30000.` and its ABAP doc comment updated. This is the ONLY
   line changed. The comparison site inside `rebuild_index`
   (`IF lines( lt_rows ) >= c_index_write_chunk_size.`) is untouched —
   it already referenced the named constant from the prior checkpoint,
   so no second edit was needed there.

`src/ortec/git/zcl_abapgit_ortec_obj_index.clas.testclasses.abap`: the 3
old 5000-boundary-specific tests (`index_chunk_below_boundary`,
`index_chunk_at_boundary`, `index_chunk_above_boundary`) were replaced
with 1 consolidated test (`index_bulk_rows_preserved`), since none of the
old boundary values (4999/5000/5001) are anywhere near the new 30000
threshold and keeping 3 near-identical, now-non-boundary tests would be
redundant, expensive test cost with no differentiated correctness value.
`index_empty_no_match` is unchanged. `index_chunk_boundary_ok`'s comment
was corrected (see below); its assertions/body are unchanged.

## Invariant-by-invariant re-verification (run-brief mandatory list)

| Invariant | Verified how | Result |
| --- | --- | --- |
| Array DML, not expanding IN/FAE | Re-read `rebuild_index`: still `MODIFY zaog_obj_index FROM TABLE lt_rows.` | CONFIRMED |
| Row type fixed/bounded at documented size | `zaog_obj_index` re-confirmed as 11 fixed-width `CHAR` DDIC fields = 730 bytes/row via the live table XML | CONFIRMED |
| Accumulator cleared immediately after every flush | `CLEAR lt_rows.` unchanged, immediately follows the in-loop `MODIFY` | CONFIRMED |
| No second full copy of the package before the DB call | `lt_rows` is passed directly to `MODIFY ... FROM TABLE`; no intermediate copy/APPEND-to-another-itab introduced | CONFIRMED |
| No `COMMIT WORK` per package | `grep -i "COMMIT WORK"` against the file: 0 matches | CONFIRMED |
| Repo lock already spans the complete rebuild | `acquire_repo_lock`/`release_repo_lock` call sites untouched; unchanged span | UNCHANGED |
| Completion marker written after all data packages | Marker-write block position unchanged, after the final `IF lt_rows IS NOT INITIAL` flush | UNCHANGED |
| Failure before marker cannot produce false-ready in STRICT mode | `CATCH` handlers untouched, unchanged position relative to the marker write | UNCHANGED |
| Exact repo_key + commit_sha1 isolation | `DELETE`/row-key assignments unchanged | UNCHANGED |
| STRICT readiness behavior | `is_index_ready` untouched | UNCHANGED |
| No effect on ORTEC-disabled behavior | Dispatch/caller chain untouched | UNCHANGED |
| No unbounded statement/input shape | `c_index_write_chunk_size = 30000` remains a fixed compile-time constant, not adaptive/derived from row count | CONFIRMED |

## Checkpoint-1 regression tests (retained, not modified)

`marker_required_for_ready`, `index_no_cross_commit_leak`,
`ready_rejects_other_commit`, `ready_accepts_exact_commit` — all 4
byte-for-byte unchanged. `index_chunk_boundary_ok`'s test BODY/assertions
are unchanged (still builds 1200 rows, still asserts all rows indexed +
marker present); only its header COMMENT was corrected because it
previously (accurately, at the time) claimed to cross the active chunk
boundary — at 30000 rows this is no longer true, so the comment was
updated to say so explicitly and reframe the test as generic multi-row/
marker regression coverage rather than boundary coverage. This is a
documentation-only correction, not a behavior change.

## Revised E1-A-specific coverage (design/run-brief test matrix, this checkpoint)

| Run-brief requirement | Test(s) | Notes |
| --- | --- | --- |
| Named constant equals 30000, if legally visible | N/A | Constant is `PRIVATE`, no `LOCAL FRIENDS` declared; adding one would exceed this task's "change only the named constant" scope. Documented as a known limitation, not tested directly. |
| Multi-package correctness with a fixture crossing the new boundary, if practical | NOT ADDED | Building 30000+ Git objects is not appropriate for `DURATION SHORT`. Per the run brief's explicit fallback, deferred to the owner's live IT8 SAT/SQL measurement (`ACTUAL_MODIFY_PACKAGE_COUNT`, expected `2` at ~42000 rows). |
| Completion marker after the final package | `index_bulk_rows_preserved` asserts `is_index_ready = abap_true` after a 5000-row rebuild (single final-flush path, the only path locally reachable below 30000) | Covered for the reachable path; exact behavior at the true boundary deferred to IT8 |
| All rows preserved | `index_bulk_rows_preserved` asserts `lines( lt_files ) = 5000` AND `SELECT COUNT(*) ... = 5000` (no duplicate/dropped rows) | Covered |
| No cross-commit leakage | Unchanged, covered by the retained `index_no_cross_commit_leak` (batch-size-independent) | Covered, unaffected by this revision |
| Empty/no-row behavior unchanged | `index_empty_no_match`, unchanged | Covered |

## Method-name length check

`Select-String -Pattern '^\s*METHODS?\s+(\w+)' ... | Where-Object { $_.Length -gt 30 }`
against the test file: 0 matches. The new consolidated test method name
`index_bulk_rows_preserved` is 25 characters.

## Structural checks

- `METHOD`/`ENDMETHOD` counts in the test file: 11/11 (13 - 3 removed + 1
  consolidated).
- `METHOD`/`ENDMETHOD` counts in the production file: 6/6 (unchanged —
  only the constant's VALUE literal and its comment changed).
- `ENDCLASS` count in the test file: 2 (definition + implementation).
- `get_errors` (ADT language server): clean on both changed files.
- `git diff --check` (working tree, pre-stage): 0 whitespace errors.
- Full diff review against `77b66464`: production diff is a single
  4-line-comment + 1-value change; test diff shows only the intended
  declaration/method replacement, no unintended adjacent corruption
  (re-read the full surrounding region after editing to confirm — an
  initial edit did briefly drop the `METHOD index_empty_no_match.`
  header line during a boundary-spanning replace; caught immediately by
  re-reading the file and corrected before proceeding).

## No placeholder tests

`index_bulk_rows_preserved` performs real assertions (row count, DB row
count, readiness) against real rebuild/query results; not a stub.

## Verdict

```text
REGRESSION=PASS
BLOCKING=0
MAJOR=0
MINOR=0
NOTE=Local-only. Real ABAP Unit/ATC execution has not occurred yet (no
  reliable live-IT8 tool connection this session, per
  /memories/repo/git-state-notes.md's confirmed-mismatched-system finding);
  owner IT8 run of the follow-up commit is the acceptance gate per the
  run brief. The 30000-row candidate is not declared superior to 1000 (or
  the never-imported 5000 candidate) until measured.
```
