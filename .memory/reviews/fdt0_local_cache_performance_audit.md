# FDT0 Local Runtime Cache -- Performance Implementation Re-Audit

```text
TASK_ID=FDT0_LOCAL_CACHE_PERFORMANCE_AUDIT_20260813_FIX2
REVIEW_MODE=IMPLEMENTATION_AUDIT
BASELINE_COMMIT=b4f41e38372a0fe9f67483f71e968b1885b594c1
TOPIC=FDT0 local runtime cache
VERDICT=PASS_WITH_MINOR_FINDINGS
```

## Evidence and inspected call chain

Active source verified on IT8: `ZAOG_FDT_CACHE`,
`ZCL_ABAPGIT_ORTEC_FDT0_CACHE`, `ZCL_ABAPGIT_ORTEC_GIT_SWITCH`,
`ZCL_ABAPGIT_ORTEC_SER_ORCH=>serialize/dispatch_batch/route_to_sequential_fallback`,
and RFC function `Z_ABAPGIT_ORTEC_SER_BATCH`.

The flag is default-off and is explicitly forced to `abap_false` by
`SER_ORCH=>serialize` pending IT-01. `dispatch_batch` transmits
`iv_fdt0_cache_active`; the RFC worker sets its local switch before its
`LOOP AT it_tadir`, invokes the wrapper once per object, and clears the flag on
exit. The sequential fallback uses the same wrapper. Thus flag propagation is
complete but production cache execution is intentionally disabled.

`ZAOG_FDT_CACHE` has the specified `#L` delivery class, `#RESTRICTED` data
maintenance, and key `(client, application_id, signature)`. The productive
source omits `client = sy-mandt` predicates; Open SQL client handling supplies
the client predicate for this client-dependent table. `store` uses one upsert
then a set-based superseded-signature delete, closing PERF-DESIGN-001.

## Findings

### FDT0-PERF-01 -- CLOSED

```text
Severity: CLOSED
Path and method: ZCL_ABAPGIT_ORTEC_FDT0_CACHE=>COMPUTE_SIGNATURE
Observed call shape: SELECT ... UP TO c_max_signature_rows + 1 ROWS INTO
  TABLE; a result above c_max_signature_rows bypasses the cache.
Expected production cardinality: up to 5,179 rows measured for one application;
  source selects no more than 200,001 rows.
Estimated SQL calls: 1 per enabled FDT0 object; no HTTP.
Estimated memory impact: at most 200,001 metadata rows are materialized.
Why it matters: the former full graph materialization before the ceiling check
  is removed.
Required fix: none for this finding.
Regression test or measurement: add a ceiling-plus-one test and verify the
  bounded SELECT with ST05 during IT-01/IT-02.
```

### FDT0-PERF-02 -- CLOSED

```text
Severity: CLOSED
Path and method: ZCL_ABAPGIT_ORTEC_FDT0_CACHE=>STORE and
  =>GET_SERIALIZATION_BYTES
Observed call shape: file-data bytes are summed before EXPORT. Content above
  48 MB returns before a second XSTRING is allocated; the exported payload is
  independently capped at 50 MB before MODIFY.
Expected production cardinality: one FDT0 serialization at a time per worker;
  a cacheable file payload is at most 48 MB.
Estimated SQL calls: 1 MODIFY plus 1 set-based DELETE on a cacheable miss; 0
  HTTP calls.
Estimated memory impact: the pre-check creates no XSTRING. A cacheable result
  additionally holds one exported payload, capped at 50 MB for persistence.
Why it matters: oversized serialized file content no longer creates a cache
  EXPORT buffer.
Required fix: none for this finding.
Regression test or measurement: add a >48 MB synthetic file-data case and a
  near-50 MB persisted-payload case; capture SAT memory evidence.
```

### FDT0-PERF-03 -- NON-WAIVABLE RELEASE GATE

```text
Severity: VALIDATION_GATE
Path and method: complete FDT0 cache call chain and test/trace readiness
Observed call shape: no FDT0 cache ABAP Unit class exists (0 tests and 0% class
  coverage); no counters, ST05/SAT trace, or executed medium/large scenario.
Expected production cardinality: K FDT0 applications, including the measured
  5,179-row application; repository N is not the governing axis.
Estimated SQL calls: 3 point/application-scoped reads on hit; miss adds one
  MODIFY and one purge DELETE. HTTP calls: 0.
Estimated memory impact: unmeasured by runtime trace.
Why it matters: IT-01 is explicitly non-waivable and the cache remains forced
  off. Static inspection and syntax cannot demonstrate signature invalidation,
  payload lifetime, warm-hit performance, or aRFC execution.
Required fix: add focused unit coverage and execute IT-01 plus cold/warm IT8
  traces before enabling the switch. This is not elevated into a code blocker
  before regression sign-off.
Regression test or measurement: execute small 1-20, medium 5,000 mixed-object,
  large 40,000 path cold/warm, shared-branch, N~1,000,000/K~100 incremental,
  and interrupted-retry scenarios. Record unapplicable FDT0 cardinalities with
  a justified synthetic equivalent rather than inferring results from syntax.
```

### FDT0-PERF-04 -- CLOSED

```text
Severity: CLOSED
Path and method: ZCL_ABAPGIT_ORTEC_FDT0_CACHE=>COMPUTE_SIGNATURE
Observed call shape: one SELECT bounded to c_max_signature_rows + 1 fills
  lt_rows. Each row is rendered once into lt_parts; one CONCATENATE LINES
  operation creates the sole digest input after the loop.
Expected production cardinality: 5,179 rows measured; 200,000 rows remain
  accepted by the defensive ceiling.
Estimated SQL calls: unchanged, one bounded SELECT; HTTP calls: zero.
Estimated memory impact: metadata rows, rendered line table, and one final
  digest string are all bounded by the 200,000-row defensive ceiling. The
  previous repeated growth-copy of the entire prefix is absent.
Why it matters: the malformed-graph defensive path no longer has quadratic
  repeated STRING-concatenation behavior. ORDER BY id, fixed field order,
  semicolon field separators, and newline row separators make the digest input
  deterministic for an unchanged application graph.
Required fix: none.
Regression test or measurement: source recheck on active IT8 source confirms
  lt_parts plus CONCATENATE LINES; active syntax check is clean. Retain a
  synthetic multi-thousand-row and ceiling-adjacent SAT memory/CPU measurement
  in FDT0-PERF-03 release-gate evidence.
```

## SQL/HTTP/memory summary

Static shape is otherwise sound: an enabled hit has 4 statements (application
lookup, bounded signature SELECT, keyed payload SELECT, last-used UPDATE); a
miss has 6 (plus MODIFY and set-based purge). No SQL occurs for non-FDT0 wrapper
calls and no HTTP occurs anywhere in this cache path. No commit is issued below
the orchestrator. The wrapper does not load a payload for a mere existence
check. COMPUTE_SIGNATURE now has deterministic `ORDER BY id` canonicalization
with field and row separators, and avoids repeated growing-STRING concatenation.
ATC still reports the two `FDT_ADMN_0000S` accesses bypass its single-row buffer;
retain this as trace evidence to collect, not a speculative rewrite.

Measured: active/inactive source state aligned; cache syntax clean; ORCH unit
suite passed. Estimated/static only: SQL counts, hit rate, memory behavior.
Unexecuted: all mandatory scale scenarios, cache class unit tests, IT-01,
ST05/SAT, and cache-hit/miss counters.

## Required handoff

FDT0-PERF-01, FDT0-PERF-02, and FDT0-PERF-04 are closed. Route FDT0-PERF-03
to the regression owner. Do not enable the cache for a production or
live-repository run until the non-waivable validation gate passes.