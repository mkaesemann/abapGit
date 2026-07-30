# Regression validation — Variant B Package E, slice E1-PERF-A

```text
PACKET=COMPACT_HANDOFF_V1
TASK=E1_PERF_A_REGRESSION
BASELINE=1437a87509e1d0392b57a965ad7d6885abb0a3e4
CHECKPOINT_1_VALIDATED_HEAD=3c77d898b62f3b0464e48cf59844e2e30c7b6e89
SCOPE=E1-PERF-A only (rebuild_index chunk-size constant + E1-A-specific
  boundary/empty-row test coverage). No other slice touched.
STATUS=LOCAL_REGRESSION_PASS, PENDING_IT8
```

## Change under review

`src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap`:
1. New PRIVATE `CONSTANTS c_index_write_chunk_size TYPE i VALUE 5000.`
   added alongside the existing sibling marker constants.
2. `rebuild_index`'s single chunk-check site changed from the bare literal
   `IF lines( lt_rows ) >= 1000.` to `IF lines( lt_rows ) >=
   c_index_write_chunk_size.`. This is the ONLY behavioral line changed in
   the entire method. No other statement in `rebuild_index` was touched.

`src/ortec/git/zcl_abapgit_ortec_obj_index.clas.testclasses.abap`: 4 new
`FOR TESTING` methods + 1 new private fixture helper (`build_bulk_commit`),
all pure additions. No existing method signature or body changed.

## Invariant-by-invariant verification (run-brief mandatory list)

| Invariant | Verified how | Result |
| --- | --- | --- |
| Exact repo_key + commit_sha1 isolation | `DELETE FROM zaog_obj_index WHERE repo_key = iv_repo_key AND commit_sha1 = iv_commit` and every `ls_row-repo_key`/`ls_row-commit_sha1` assignment are unchanged (line-for-line diff outside the 2 edited lines) | UNCHANGED |
| All file/index rows persisted | New tests `index_chunk_below_boundary` (4999), `index_chunk_at_boundary` (5000), `index_chunk_above_boundary` (5001) each assert `lines( lt_files ) = iv_file_count` | PASS (design) |
| Completion marker written last | Marker-write block (`CLEAR ls_row. ... MODIFY zaog_obj_index FROM ls_row.`) is physically positioned after the `WHILE lt_pending IS NOT INITIAL` loop and the final `IF lt_rows IS NOT INITIAL` flush, exactly as before — this code was not touched at all | UNCHANGED |
| STRICT readiness behavior | `is_index_ready` method body untouched (0 lines changed); all 4 new tests call it unmodified | UNCHANGED |
| Repo-scoped lock coverage | `acquire_repo_lock`/`release_repo_lock` call sites untouched; lock still spans the whole method including all chunk MODIFYs | UNCHANGED |
| No `COMMIT WORK` inside `rebuild_index` | `grep -i "COMMIT WORK"` against the file: 0 matches (same as baseline) | CONFIRMED (0 matches) |
| Failure leaves no false-ready marker | The 3 `CATCH` handlers (`zcx_abapgit_exception`, `cx_root`) are physically positioned before the marker-write line in the `TRY` block's structure and are untouched; a failure at any chunk boundary still releases the lock and re-raises before reaching the marker write | UNCHANGED |
| No per-row SQL | The changed statement is still `MODIFY zaog_obj_index FROM TABLE lt_rows.` — a true array bulk DML, one exec per chunk, not a loop | CONFIRMED |
| No unbounded statement/input shape | `c_index_write_chunk_size = 5000` is a fixed constant (not adaptive, not a range); `lt_rows` is a bounded internal table cleared after every flush | CONFIRMED |
| No effect on ORTEC-disabled behavior | `rebuild_index` is only reachable via `zcl_abapgit_ortec_obj_index=>get_files_for_filter`, itself only called from ORTEC-active filtered-Stage dispatch (Package D2 ownership map, unchanged); no dispatch condition touched | UNCHANGED |

## Checkpoint-1 regression tests (retained, not modified)

`marker_required_for_ready`, `index_no_cross_commit_leak`,
`ready_rejects_other_commit`, `ready_accepts_exact_commit`,
`index_chunk_boundary_ok` — all 5 pre-existing test method bodies are
byte-for-byte unchanged (only new methods were appended after the last
one). `index_chunk_boundary_ok`'s own fixture (1200 rows) crosses the OLD
1000-row boundary but not the NEW 5000-row boundary; it remains a valid,
still-meaningful regression test (single-flush-at-the-end case, a subset
of what `index_chunk_below_boundary` now also covers at a larger scale) —
per the run brief's explicit instruction to retain, not replace, it.

## New E1-A-specific coverage (design/run-brief test matrix)

| Run-brief requirement | Test(s) | Notes |
| --- | --- | --- |
| All rows survive more than one new-size package | `index_chunk_above_boundary` (5001 rows = 2 flushes) | Covered |
| One row below, exactly at, one row above the boundary | `index_chunk_below_boundary` (4999), `index_chunk_at_boundary` (5000), `index_chunk_above_boundary` (5001) | 3 distinct tests, exact boundary values tied to the fixed `5000` contract (intentionally hardcoded — these tests exist specifically to pin THIS contract's exact boundary, unlike the chunk-agnostic `index_chunk_boundary_ok`) |
| Completion marker present only after successful completion | All 4 new tests assert `is_index_ready(...) = abap_true` after a full, uninterrupted rebuild; no new failure-injection test was added (no safe seam exists to force a mid-walk exception without a new test-only hook, which is out of scope for E1-A) — this property is otherwise already covered by the untouched `CATCH`/lock-release structure (see invariant table above) | Covered by construction + existing CATCH-block invariant, not by a new failure-path test |
| No duplicate or cross-commit rows | `index_chunk_at_boundary` and `index_chunk_above_boundary` add an explicit `SELECT COUNT(*) ... WHERE obj_type = 'PROG'` check against the exact expected row count, specifically at the two flush-boundary cases most likely to reveal a duplicate/lost-row defect. Cross-commit isolation itself is unchanged and already covered by the retained `index_no_cross_commit_leak` | Covered |
| Package/flush count matches `ceil(rows / batch_size)` | NOT tested via a unit-test observability seam — none exists, and the design/run-brief explicitly forbid adding a production counter solely for this. Deferred to the IT8_MEASUREMENT plan (SAT trace comparison, design §2) | DEFERRED_TO_IT8 (by design, not an omission) |
| Empty/no-row case remains correct | `index_empty_no_match` (a "readme" file with no `.` segment maps to an empty `obj_type` via `file_to_object`, which `rebuild_index`'s own `obj_type IS INITIAL ... CONTINUE` guard skips — a real, reachable zero-relevant-rows path, not a synthetic filter mismatch, since `rebuild_index` has no filter parameter of its own) | Covered |

## Method-name length check

`Select-String -Pattern '^\s*METHODS?\s+(\w+)' ... | Where-Object { $_.Length -gt 30 }`
against the test file: 0 matches. All 4 new test methods and the new
helper are ≤ 27 characters (`index_chunk_below_boundary` / 26 chars is the
longest).

## Structural checks

- `METHOD`/`ENDMETHOD` counts in the test file: 13/13 (balanced after
  adding 5 new method bodies: 4 tests + 1 helper).
- `METHOD`/`ENDMETHOD` counts in the production file: 6/6 (unchanged count
  — only 2 lines inside an existing method were edited, no method
  added/removed).
- `get_errors` (ADT language server): clean on both changed files.

## No placeholder tests

All 4 new test methods perform real assertions against real rebuild/query
results; none is a `assert_true( abap_true )` stub or similar.

## Verdict

```text
REGRESSION=PASS
BLOCKING=0
MAJOR=0
MINOR=0
NOTE=Local-only. Real ABAP Unit/ATC execution has not occurred yet (no
  reliable live-IT8 tool connection this session, per
  /memories/repo/git-state-notes.md's confirmed-mismatched-system finding);
  owner IT8 run is the acceptance gate per the run brief.
```
