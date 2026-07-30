# Performance scan — Variant B Package E, slice E1-PERF-A

```text
PACKET=COMPACT_HANDOFF_V1
TASK=E1_PERF_A_PERFORMANCE_SCAN
MODE=STATIC_SCAN (post-implementation, pre-audit)
SCOPE=src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap (production diff
  only); testclasses.abap scanned for anti-pattern reuse only, not scored.
STATUS=PASS
```

## What changed (scan target)

1 new `CONSTANTS c_index_write_chunk_size TYPE i VALUE 5000.` (compile-time
literal, zero runtime cost). 1 comparison operand swapped from a bare
literal (`1000`) to a named constant (`c_index_write_chunk_size`, value
`5000`) inside an existing `IF` guard. No new statement, loop, branch,
DB call, or HTTP call was introduced.

## SQL shape

- Statement before: `MODIFY zaog_obj_index FROM TABLE lt_rows.` (array
  bulk DML), reached when `lines( lt_rows ) >= 1000`.
- Statement after: identical statement, identical table target, identical
  `FROM TABLE` array form, reached when `lines( lt_rows ) >= 5000`.
- No new `SELECT`/`MODIFY`/`INSERT`/`DELETE` statement added anywhere in
  the diff.
- No `FOR ALL ENTRIES`, no per-row loop DML, no dynamic SQL introduced.

## Loop shape

- The single `WHILE lt_pending IS NOT INITIAL` BFS tree walk and its
  inner `LOOP AT <ls_work>-nodes` are structurally unchanged — no new
  nesting level, no new iteration source, no new per-iteration DB/HTTP
  call added.
- The only change is the numeric RHS of one existing `IF` comparison
  inside that loop — an O(1) integer compare, same cost class as before
  regardless of the constant's value.

## Memory / batching shape

- `lt_rows` remains a single bounded internal table, `CLEAR`-ed
  immediately after every flush — no accumulation across chunks.
- Design-verified byte bound: `zaog_obj_index` is 11 fixed-width `CHAR`
  DDIC fields summing to 730 bytes/row (re-confirmed this session by
  reading `zaog_obj_index.tabl.xml` directly: CLIENT 3 + REPO_KEY 12 +
  COMMIT_SHA1 40 + OBJ_TYPE 4 + OBJ_NAME 40 + PATH_HASH 40 + FILE_PATH 255
  + FILE_NAME 255 + BLOB_SHA1 40 + TREE_SHA1 40 + IDX_STATUS 1 = 730).
  `5000 * 730 bytes ≈ 3.65 MB` peak buffer — bounded, matches the approved
  design contract exactly (no XSTRING/blob payload ever held in this
  table).
- Expected package count at a representative 42,000-row repository:
  `CEIL(42000 / 5000) = 9` packages (vs. `CEIL(42000 / 1000) = 42`
  packages under the old constant) — an ~4.7x reduction in the number of
  bulk MODIFY round-trips for that scale, with no increase in per-package
  peak memory beyond the approved ~3.65 MB bound.

## Lock duration

- `acquire_repo_lock`/`release_repo_lock` call sites are untouched and
  still wrap the identical span of work (walk + all chunk flushes +
  marker write). Fewer, larger MODIFY statements for the same total row
  count is expected to REDUCE total lock hold time (less DB round-trip
  overhead), not increase it — consistent with the E1-A design intent.

## Test-fixture scan (informational, not scored against production gates)

`build_bulk_commit` (new test helper) persists all generated blob/tree/
commit objects via a single `zcl_abapgit_ortec_obj_store=>store_objects`
bulk call, not a per-object loop — consistent with the project's
documented "never loop DB writes per-row" lesson. This is test-fixture
code, not part of the scored production diff, but is noted here since it
generates up to 5001 rows per test run.

## Verdict

```text
PERFORMANCE_SCAN=PASS
FINDINGS=NONE
BLOCKING=0
```
