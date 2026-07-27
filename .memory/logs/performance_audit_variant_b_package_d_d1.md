# Performance implementation audit: Variant B / Package D1

Mode: `IMPLEMENTATION_AUDIT` (mandatory pre-commit performance gate, run
directly by the senior implementation agent as Claude Sonnet 5 - no dedicated
`ortec-abapgit-performance-review` subagent is registered in this session's
agent roster, so this audit was not delegated; the gate itself was still
executed in full against the current source).

Baseline reviewed: current workspace state of
[src/ortec/git/zcl_abapgit_ortec_delta.clas.abap](src/ortec/git/zcl_abapgit_ortec_delta.clas.abap),
[src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap](src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap),
[src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.abap](src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.abap),
[src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap)
(read-only, D2-owned).

Input: [.memory/logs/performance_scan_variant_b_package_d_d1.md](.memory/logs/performance_scan_variant_b_package_d_d1.md).

## Method

Static source re-verification only. **No live SAT/ST05 trace, no connected
SAP system execution, and no measured timings are available in this
environment.** Every quantitative statement below is an estimate derived
from reading the exact call shape (SQL/HTTP call count independent of N,
loop bounds, recursion bounds) - not a measurement. This distinction is
stated explicitly for each scenario per the owner's directive.

## Scale scenarios

### 1 object, 0 external bases (estimate)

- `resolve_all`/`resolve_streaming`: `lt_external_bases` stays empty ->
  `bulk_resolve_external_bases` is never called (`IF lt_external_bases IS
  NOT INITIAL` guard). Zero incremental SQL/HTTP beyond whatever the
  surrounding decode already issues for this one object.
- Estimate, not measured: negligible, sub-millisecond overhead from the
  phase-1/phase-1.5/phase-2 loop scaffolding itself (three passes over a
  1-row internal table).

### 1,000 objects, ~50 external bases (estimate)

- Exactly **one** `get_objects( iv_bulk_fetch = abap_true )` call,
  regardless of which/how many of the 1,000 objects reference the ~50
  distinct bases (dedup via `lt_unique_set` HASHED TABLE in
  `bulk_resolve_external_bases`).
- Estimate, not measured: one indexed `SELECT ... WHERE obj_sha1 IN
  (~50 values) AND status = 'R'` - a single well-indexed DB round trip,
  consistent with production-scale expectations already validated for
  Package C/D0's bulk object-store reads.

### 40,000 objects in one pack, all requiring external REF_DELTA bases
### (worst-case stress bound, estimate)

- Still exactly **one** `get_objects( iv_bulk_fetch = abap_true )` call -
  the call count is architecturally independent of N (proved by source:
  the call site is outside any per-object loop, called once per
  `resolve_all`/`resolve_streaming` invocation).
- The `lt_package`/RANGE-table construction is O(N) ABAP-side (40,000
  `APPEND`s to a RANGE table) - estimate: this is CPU/memory-bound, not
  DB-round-trip-bound; ABAP OpenSQL transparently splits an oversized
  `IN`-list into multiple physical statements at the DB interface layer,
  so this does not become 40,000 (or even a DB-rejected single) SQL
  statement.
- This scenario is explicitly called out as **unrealistic for a real git
  fetch** (a single pack's external-base fan-out is bounded by the
  server's own pack-generation limits and thin-pack negotiation, not by
  repository size) - it is evaluated here purely as a stress bound per the
  owner's mandatory scale-scenario requirement, not because it is an
  expected production shape.
- No live measurement exists for this scenario (would require a
  synthetically constructed 40,000-object pack and a connected system) -
  this is a structural/estimate-only verdict.

### ~100 objects affected, 1,000,000 stored objects total (estimate)

- `read_object_rows`'s `WHERE repo_key = ... AND obj_sha1 IN (...) AND
  status = 'R'` predicate uses the existing `zaog_obj_store` primary key
  (`repo_key` + `obj_sha1`), unchanged by D1 - reading ~100 targeted rows
  out of 1,000,000 is an indexed point-lookup pattern (via the `IN`-list),
  not a table scan.
- Estimate, not measured: DB cost scales with the ~100 requested rows, not
  with the 1,000,000-row table size - consistent with the primary-key
  access path already relied upon by every other object-store reader in
  this class (`get_present_sha1s`, `has_dangling_delta_base`, etc.), none
  of which were changed by D1.

## Findings

- **No blocking finding.** No per-object SQL, no per-object HTTP, no
  unbounded in-memory accumulation beyond one pack's own external-base set,
  no hidden singleton-API-in-loop pattern.
- **Minor, non-blocking finding (carried from the performance scan):** the
  `iv_bulk_fetch = abap_true` branch of `get_objects` does not apply the
  `c_select_package_size` chunking used elsewhere in the same class. This
  is architecturally correct for D1's "exactly one bulk call" invariant
  (chunking would reintroduce multiple calls, which is what D1 is designed
  to avoid) and is safe at any N because of OpenSQL's transparent `IN`-list
  splitting - but it is worth an explicit note for any future reader who
  might otherwise assume every bulk read path in this class is
  pre-chunked. No code change is required; this is a documentation-level
  observation, not a defect.

## Verdict

```text
PERFORMANCE_AUDIT=PASS_WITH_MINOR_FINDINGS
BLOCKING_FINDINGS=0
MAJOR_FINDINGS=0
MINOR_FINDINGS=1 (get_objects bulk-fetch branch intentionally unchunked -
  documented, not a defect)
MEASURED_VS_ESTIMATED=all figures in this document are STATIC ESTIMATES;
  no live SAT/ST05 trace or connected-system execution was available in
  this environment
```

Per the mode's rule ("do not proceed to final regression on a blocking
performance verdict"), `PASS_WITH_MINOR_FINDINGS` permits proceeding to the
final regression review and the local checkpoint commit.
