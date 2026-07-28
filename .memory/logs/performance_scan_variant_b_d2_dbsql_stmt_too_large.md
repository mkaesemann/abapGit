# Performance Scan — Variant B D2 DBSQL_STMNT_TOO_LARGE fix

## Scope
- Topic: variant-b-partial-clone / D2 incident follow-up
- Slice: DBSQL_STMNT_TOO_LARGE root-cause fix (get_objects bulk-fetch chunking)
- Entry method: zcl_abapgit_ortec_obj_store=>get_objects
- Files inspected (full diff read): src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap,
  src/ortec/git/zcl_abapgit_ortec_obj_store.clas.testclasses.abap
- Expected cardinality: one get_objects call issues ceil(K / 1000) bounded
  SELECT statements for K cache-miss SHA1s, regardless of iv_bulk_fetch

## Summary
- Verdict: CLEAN
- SQL shape: both iv_bulk_fetch branches now build lt_package and call
  read_object_rows identically, chunked at c_select_package_size = 1000.
  No new SELECT statement shape was introduced; read_object_rows itself is
  unchanged (still a single unchunked SELECT per call - correctness of that
  primitive was never in question, only its caller's failure to bound the
  input set size).
- HTTP shape: unaffected - this fix is entirely local-DB-side; it does not
  touch any wire-protocol/request-serialization code.
- Memory risk: reduced. Previously, iv_bulk_fetch=abap_true could build one
  IN-range with an unbounded number of entries (confirmed live at 40,891,
  exceeding HANA/DBSL's 32,767 marker ceiling). Now every chunk is capped at
  1000 entries, matching every other read_object_rows caller in this class
  and leaving comfortable headroom (1000 << 32,767) even if a future object
  type's frontier is wider still.
- Call-count impact: for K <= 1000 (the overwhelming majority of existing
  get_objects(iv_bulk_fetch=abap_true) callers - get_reachable_objects's
  commit level (K=1), most tree levels, zcl_abapgit_ortec_delta's external-
  base bulk load (per Package D1 design, typically << 1000 distinct bases
  per pack), zcl_abapgit_ortec_obj_index/zcl_abapgit_ortec_walk_prep's own
  bulk calls) this fix is a NO-OP in SQL call count (still exactly 1 SELECT).
  Only K > 1000 call sites (previously crash-prone) now correctly issue
  ceil(K/1000) SELECTs instead of one oversized, failing statement.

## Findings
- No findings in the inspected diff. The change unifies two previously-
  divergent branches of one already-reviewed method into a single,
  consistently-chunked code path, following the exact pattern already used
  by 4 other read_object_rows call sites in the same class
  (get_available_objects, has_dangling_delta_base, get_present_sha1s,
  get_staged_delta_objects). No new loop nesting, no new per-object SQL, no
  new commit boundary, no predicate change.
- One incidental instrumentation addition: a new
  gv_read_object_rows_calls CLASS-DATA counter (PRIVATE, LOCAL FRIENDS
  test-only access) was added, following the exact disposition already
  established for zcl_abapgit_ortec_delta=>gv_bulk_load_calls/
  gv_thin_fetch_calls (plain integer increment, never persisted, never
  logged, test-reset only) - not a performance concern (single integer
  increment per read_object_rows call, already dwarfed by the SELECT
  itself).

## Callers affected (all benefit transparently, none require a code change)
- zcl_abapgit_ortec_obj_store=>get_reachable_objects (commit/tree/blob
  levels) - the confirmed crash site for this incident
- zcl_abapgit_ortec_delta (external-base bulk load, line ~572)
- zcl_abapgit_ortec_obj_index (lines ~333, ~365, +1 more)
- zcl_abapgit_ortec_walk_prep (line ~260)

None of these call sites required any change - the fix is entirely
contained within get_objects' own internal chunking logic.

## Unverified paths
- The exact live HANA DBSL marker ceiling (32,767, confirmed via this
  incident's own dump text) was not independently re-verified against a
  live IT8 syntax/statement probe in this pass - the dump's own kap3 text
  is treated as authoritative (see incident artifact §4).
- Live re-execution of the fixed warm-to-cold branch switch under SAT was
  not performed in this pass (owner retest, SQLSIZE-8/9/10, remains open).

## Evidence limits
- No runtime execution, SQL trace, or network trace was performed in this
  scan; conclusions are based on the inspected source, diff, and the live
  incident dump's own measured cardinality (§4/§6 of the incident artifact).
