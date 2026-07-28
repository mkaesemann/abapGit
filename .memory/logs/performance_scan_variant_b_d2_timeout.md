# Performance Scan — Variant B D2 TIME_OUT fix

## Scope
- Topic: variant-b-partial-clone / D2 incident follow-up
- Slice: TIME_OUT root-cause fix (ensure_available -> materialize_missing_batches)
- Entry methods: zcl_abapgit_ortec_missing_obj=>ensure_available,
  zcl_abapgit_ortec_cold_init=>materialize_missing_batches,
  zcl_abapgit_ortec_cold_init=>materialize_tip_snapshot (refactored caller)
- Files inspected (full diff read): src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.abap,
  src/ortec/git/zcl_abapgit_ortec_cold_init.clas.abap,
  src/ortec/git/zcl_abapgit_ortec_cold_init.clas.testclasses.abap,
  src/ortec/git/zcl_abapgit_ortec_fetch_req.clas.testclasses.abap,
  src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.testclasses.abap (new),
  src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.xml
- Expected cardinality: one Stage-By-Filter top-up per action, SHA1 lists
  bounded to the caller's own missing set (K), never repository-wide (N)

## Summary
- Verdict: CLEAN
- SQL shape: `ensure_available`'s Step 1/3 (`get_missing_sha1s`) are
  unchanged, bulk/chunked, presence-only (no `obj_data` load). The new
  `materialize_missing_batches` introduces zero new SQL of its own — its
  only SQL exposure is via `materialize_batch`'s existing
  `decode_streaming` persistence call, unchanged from every other
  MATERIALIZE_BLOBS caller.
- HTTP shape: one HTTP client per `ensure_available` invocation that
  actually needs a fetch (unchanged count vs. today), issuing
  `ceil(K / adaptive_batch_rows)` POSTs on that one client instead of
  today's single "whole commit graph" POST — this is the fix itself, not a
  new cost class.
- Memory risk: bounded per batch (25 MiB hard ceiling, adaptive controller
  targets 16 MiB, oversize batches recursively halved up to 10 times, a
  genuinely oversized solo object raises a structured failure instead of
  being accepted unbounded). No `SELECT *`/full-payload read was
  introduced anywhere in this diff.

## Findings
- No findings in the inspected diff. The change is a behavior-preserving
  extraction (`materialize_tip_snapshot`'s loop moved into
  `materialize_missing_batches`, unchanged statement order/count) plus a
  replacement of `ensure_available`'s Step 2/3 fetch mechanism with a call
  to that extracted method — no new loop, no new per-object SQL/HTTP, no
  new commit boundary.
- One incidental correction made during extraction: the original
  `materialize_tip_snapshot` had a trailing `COMMIT WORK.` after
  `finalize_snapshot(...)` that had to be carefully re-attached to the
  refactored method (the extraction initially mis-split it) — verified via
  `get_errors` (0 errors) and a direct re-read of the full method boundary
  after the fix; not a performance finding, noted for the record since it
  is exactly the class of self-inflicted corruption this project's own
  memory notes warn about.

## Unverified paths
- The adaptive batching loop's actual live HTTP round-trip behavior (as
  opposed to its pure decision helpers, which are unit-tested) was not
  exercised in this static scan — this is unavoidable without a live IT8
  round-trip (no HTTP mock seam exists in this project's test
  infrastructure, a pre-existing, documented limitation).

## Evidence limits
- No runtime execution, SQL trace, or network trace was performed;
  conclusions are based on the inspected source and diff only, consistent
  with every other performance scan in this project's D1/D2/incident
  history.
