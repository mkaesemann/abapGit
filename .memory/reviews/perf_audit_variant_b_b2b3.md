# Performance implementation audit — Variant B Package B, checkpoints B2+B3

Verdict: **APPROVE**

## Scope

`ZCL_ABAPGIT_ORTEC_OBJ_STORE=>GET_TIP_BLOB_SHA1S` (B2) and
`ZCL_ABAPGIT_ORTEC_COLD_INIT=>MATERIALIZE_TIP_SNAPSHOT` plus its private
helpers `MATERIALIZE_BATCH`, `CHUNK_MISSING_SHA1S`, `DECIDE_OVERSIZE_ACTION`,
`SPLIT_BATCH_IN_HALF`, `VERIFY_BATCH_OBJECTS`, `MAY_PUBLISH_SNAPSHOT` (B3).

## Complexity check (K/F/B/R discipline)

| Operation | Bound | Evidence |
|---|---|---|
| Tree frontier walk | O(F), chunked at 1000 (private `c_select_package_size`) per iteration via `get_objects(iv_bulk_fetch=abap_false)` | Reuses `verify_tree_closure`'s proven shape; no new SQL. |
| Blob discovery | O(K), in-memory hashed dedup (`lt_seen_blobs`) | Never reads blob payload/presence. |
| Presence subtraction | O(K), bulk `get_missing_sha1s`, chunked at 1000 | Reused unchanged. |
| Batch chunking | O(K), one pass + hashed dedup | `chunk_missing_sha1s`, pure. |
| Per-batch HTTP + decode | O(B), B ≤ 100 | One `MATERIALIZE_BLOBS` request per batch. |
| Per-batch verification | O(B), B ≤ 100, via `get_objects` | Bounded to the batch just received — same order of magnitude as its own HTTP response, never O(K) or O(N). |
| Final completeness re-check | O(K), presence-only `get_missing_sha1s` | Matches design INV-B-09 literally. |
| Oversize split budget | ≤ `c_max_oversize_splits` (7) per top-level batch, threaded through recursion, never shared across batches | Matches design-review Finding 3 fix. |

No operation scales with total repository object count N. No per-object SQL or
HTTP. No per-object `COMMIT WORK` (one commit per `MATERIALIZE_TIP_SNAPSHOT`
call plus `decode_streaming`'s existing one-per-batch internal commit,
unchanged pre-existing behavior).

## Documented deviation from literal design text

`VERIFY_BATCH_OBJECTS` uses `get_objects` (reads object data) rather than a
second `get_missing_sha1s` call, to additionally satisfy the "wrong-type
returned object raises" acceptance criterion that a presence-only check
cannot detect. This is bounded to the batch size (≤100) already resident from
the just-received HTTP response, not a new N-scaling shape, and does not
require a new DESIGN_GATE pass. The final cross-batch completeness gate
(`may_publish_snapshot`, fed by a `get_missing_sha1s` call) still uses the
presence-only shape mandated by INV-B-09.

## Findings

None blocking. None major.
