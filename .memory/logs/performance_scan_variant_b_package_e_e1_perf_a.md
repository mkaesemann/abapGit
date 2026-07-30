# Performance scan — Variant B Package E, slice E1-PERF-A

```text
PACKET=COMPACT_HANDOFF_V1
TASK=E1_PERF_A_PERFORMANCE_SCAN
MODE=STATIC_SCAN (post-implementation, pre-audit; revised candidate)
SCOPE=src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap (production diff
  only, on top of superseded commit 77b66464); testclasses.abap scanned
  for anti-pattern reuse only, not scored.
STATUS=PASS
```

## What changed (scan target, this revision)

`CONSTANTS c_index_write_chunk_size TYPE i VALUE 5000.` changed to
`VALUE 30000.` (compile-time literal, zero runtime cost) plus its ABAP
doc comment. No comparison site changed this time — `rebuild_index`
already referenced the named constant from the superseded `77b66464`
candidate. No new statement, loop, branch, DB call, or HTTP call was
introduced.

## SQL shape

- Statement before (superseded candidate): `MODIFY zaog_obj_index FROM
  TABLE lt_rows.`, reached when `lines( lt_rows ) >= 5000`.
- Statement after (this candidate): identical statement, identical table
  target, identical `FROM TABLE` array form, reached when
  `lines( lt_rows ) >= 30000`.
- No new `SELECT`/`MODIFY`/`INSERT`/`DELETE` statement added anywhere in
  the diff. No `FOR ALL ENTRIES`, no per-row loop DML, no dynamic SQL.

## Loop shape

- The `WHILE lt_pending IS NOT INITIAL` BFS tree walk and its inner
  `LOOP AT <ls_work>-nodes` are structurally unchanged from the prior
  checkpoint — no new nesting level, no new iteration source, no new
  per-iteration DB/HTTP call added. Only the numeric RHS of the existing
  `IF` comparison's constant value changed — an O(1) integer compare,
  same cost class regardless of the constant's value.

## Memory / batching shape (revised candidate matrix)

`zaog_obj_index` byte bound re-confirmed unchanged: 11 fixed-width `CHAR`
DDIC fields summing to 730 bytes/row (CLIENT 3 + REPO_KEY 12 +
COMMIT_SHA1 40 + OBJ_TYPE 4 + OBJ_NAME 40 + PATH_HASH 40 + FILE_PATH 255 +
FILE_NAME 255 + BLOB_SHA1 40 + TREE_SHA1 40 + IDX_STATUS 1 = 730). No
XSTRING/blob payload is ever held in this table — this is a pure,
fixed-width array-DML row buffer, not comparable to prior pack/object
blob-memory incidents (see performance audit for the explicit
distinction).

| Batch size | Row payload (rows × 730B) | Packages @ 42,000 rows |
| ---: | --- | ---: |
| 1,000 (original) | 0.73 MB (~0.70 MiB) | ~42 (41 measured live) |
| 5,000 (superseded, never imported) | 3.65 MB (~3.48 MiB) | 9 |
| 10,000 | 7.30 MB (~6.96 MiB) | 5 |
| 20,000 | 14.60 MB (~13.92 MiB) | 3 |
| 30,000 (this candidate) | 21.90 MB (~21.37 MiB) | 2 |
| 50,000 | 36.50 MB (~34.81 MiB) | 1 — `NOT_AUTHORIZED_PENDING_30000_MEASUREMENT` |

Row-payload figures are a lower-bound estimate of the fixed-width row
data only, not a claim of total ABAP/DB-interface peak runtime
allocation (work areas, internal table overhead, and the DB client
buffer add to this on top). At 30,000 rows the total is still a small,
single-digit-MB-class allocation relative to typical SAP work-process
memory budgets, but this remains a static estimate — the owner's live
SAT/memory measurement is the actual acceptance evidence, not this scan.

- `lt_rows` remains a single bounded internal table, `CLEAR`-ed
  immediately after every flush — no accumulation across chunks, no
  second full in-memory copy constructed before the `MODIFY` call.
- Expected package count at the owner's measured ~42,000-row repository:
  `CEIL(42000 / 30000) = 2` packages (vs. 41 measured live packages at
  the original 1,000-row size, and vs. 9 packages under the superseded,
  never-imported 5,000-row candidate) — a further, larger reduction in
  bulk MODIFY round-trips for that scale.

## Lock duration

- `acquire_repo_lock`/`release_repo_lock` call sites are untouched and
  still wrap the identical span of work. Fewer, larger MODIFY statements
  for the same total row count is expected to further REDUCE total lock
  hold time relative to both the original 1,000-row size and the
  superseded 5,000-row candidate, not increase it.

## Test-fixture scan (informational, not scored against production gates)

`build_bulk_commit` (test helper, unchanged this revision) persists all
generated blob/tree/commit objects via a single
`zcl_abapgit_ortec_obj_store=>store_objects` bulk call, not a per-object
loop. The consolidated `index_bulk_rows_preserved` test now invokes it
with a 5,000-file fixture (down from 3 separate tests totaling
4999+5000+5001 ≈ 15,000 generated objects in the superseded candidate) —
a reduced local test-fixture cost, consistent with the run brief's
"without introducing an excessive or misleading test cost" guidance,
since none of those old fixture sizes exercise the new 30,000-row
boundary anyway.

## Verdict

```text
PERFORMANCE_SCAN=PASS
FINDINGS=NONE
BLOCKING=0
```
