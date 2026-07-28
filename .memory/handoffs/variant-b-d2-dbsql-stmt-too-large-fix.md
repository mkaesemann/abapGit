# Variant B D2 DBSQL_STMNT_TOO_LARGE fix — implementation handoff

```text
PACKET=COMPACT_HANDOFF_V1
TASK=D2-DBSQL-STMT-TOO-LARGE-FIX
BASELINE=17513ba741322d11078b8c6ff5fe41b1c6978d4c (TIME_OUT fix, IT8 retest pending)
STATUS=LOCAL_CHECKPOINT_COMPLETE_AWAITING_IT8_RETEST
```

## What changed

- `src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap`:
  - `get_objects`: unified the `iv_bulk_fetch = abap_true`/`abap_false`
    branches into one chunked code path (chunk `lt_package` at
    `c_select_package_size` = 1000 before every `read_object_rows` call,
    regardless of `iv_bulk_fetch`). `iv_bulk_fetch` is retained in the
    signature for call-site compatibility only and has no remaining effect.
  - `read_object_rows`: added a new `gv_read_object_rows_calls` increment
    (test-observability only, no behavior change).
  - Added a new `gv_read_object_rows_calls` `CLASS-DATA` counter (PRIVATE,
    `LOCAL FRIENDS`-only test access), following the exact disposition of
    `zcl_abapgit_ortec_delta=>gv_bulk_load_calls`/`gv_thin_fetch_calls`.
  - Corrected two stale ABAP Doc comments (`verify_tree_closure`,
    `get_tip_blob_sha1s`) that claimed `iv_bulk_fetch = abap_true` never
    chunks - no longer true after this fix; their own choice of
    `iv_bulk_fetch = abap_false` remains correct and unaffected.
  - Added a doc comment on `get_objects` itself documenting the unified
    chunking contract.
- `src/ortec/git/zcl_abapgit_ortec_obj_store.clas.testclasses.abap`: added
  `LOCAL FRIENDS` declaration plus 6 new tests: `bulk_fetch_dedups_input`,
  `bulk_fetch_uses_pkg_size`, `bulk_fetch_empty_no_sql`,
  `bulk_fetch_preserves_where`, `bulk_fetch_large_n_small_k`,
  `bulk_fetch_no_per_key_sql`.

## Root cause fixed

`get_objects`' `iv_bulk_fetch = abap_true` branch built the caller's ENTIRE
cache-miss SHA1 set into one unchunked `read_object_rows` call - unlike its
own `abap_false` branch and every other `read_object_rows` caller in the
class. `get_reachable_objects`'s blob-level frontier for a real cold branch
(`bugfix/O4H-8794-complete-solution-tour-number-range`) reached 40,891
distinct, previously-uncached blob SHA1s, producing one
`SELECT ... obj_sha1 IN <range>` statement with 40,893 bind markers -
exceeding HANA/DBSL's 32,767-marker ceiling and crashing with
`DBSQL_STMNT_TOO_LARGE`. This defect was previously latent because
`get_reachable_objects`'s own `populate_cache` full-repo preload (removed
by the unrelated, already-committed SYSTEM_NO_ROLL fix, `2111b288`) had
been incidentally pre-warming the session cache for any previously-touched
repository, masking the gap. The fix makes `get_objects` chunk
unconditionally, matching the already-reviewed pattern used by 4 sibling
call sites in the same class.

## Gates passed

```text
DESIGN_REVIEW=NOT_REQUIRED (mechanical fix inside an already-reviewed
  set-based object-store batching contract - documented justification in
  .memory/incidents/variant_b_d2_it8_dbsql_stmt_too_large.md §10; no
  request-semantics, materialization-semantics, persistence-visibility, or
  graph-completeness change)
FOCUSED_PERFORMANCE_CORRECTNESS_AUDIT=PASS (0 blocking, 1 non-blocking
  informational note)
REGRESSION=PASS_WITH_FINDINGS (static-only; no live SAP runner available)
```

See:
[.memory/incidents/variant_b_d2_it8_dbsql_stmt_too_large.md](.memory/incidents/variant_b_d2_it8_dbsql_stmt_too_large.md),
[.memory/logs/performance_scan_variant_b_d2_dbsql_stmt_too_large.md](.memory/logs/performance_scan_variant_b_d2_dbsql_stmt_too_large.md),
[.memory/logs/performance_audit_variant_b_d2_dbsql_stmt_too_large.md](.memory/logs/performance_audit_variant_b_d2_dbsql_stmt_too_large.md),
[.memory/logs/regression_variant_b_d2_dbsql_stmt_too_large.md](.memory/logs/regression_variant_b_d2_dbsql_stmt_too_large.md).

## What is NOT done yet

- Live IT8 import/retest of this checkpoint - see incident artifact §13 for
  the exact reproduction sequence (import, ABAP Unit, ATC, warm
  `development/6.0.x`, SAT trace, switch to
  `bugfix/O4H-8794-complete-solution-tour-number-range` cold, confirm no
  `DBSQL_STMNT_TOO_LARGE`/`SYSTEM_NO_ROLL`/`TIME_OUT`, export SAT).
- The SAT-guided warm-to-cold branch-switch performance analysis itself
  remains outstanding regardless of this fix's success - do not close
  Package D2 on this fix alone.
- `.memory/state.md` intentionally NOT updated (per run-brief instruction:
  no state writes during this incident's analysis/local implementation).

## Preserved invariants (verified)

```text
SYSTEM_NO_ROLL fix (commit 2111b288)              - untouched, confirmed via grep + git diff --stat
TIME_OUT fix (commit 17513ba7)                     - untouched, confirmed via grep + git diff --stat
Package C F/C certification invariant              - untouched (no file in scope)
D1 bulk external delta-base resolution              - untouched (no file in scope; caller unaffected)
D2 staged visibility and attempt isolation          - untouched (no file in scope)
No uncertified haves / no deepen/shallow            - unaffected (this fix is local-DB-side only)
No per-object SQL or HTTP                           - confirmed by focused audit
K-not-N incremental scaling                         - confirmed (bulk_fetch_large_n_small_k)
Row/byte bounds                                     - c_select_package_size (1000) applied uniformly
ORTEC-disabled standard behavior                    - unaffected (no src/git/** file touched)
```
