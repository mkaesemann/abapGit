# SSFO local cache performance design gate

Task: `SSFO_LOCAL_CACHE_PERFORMANCE_DESIGN_GATE_20260814`  
Mode: `DESIGN_GATE`  
Baseline: IT8 active source, 2026-08-14  
Verdict: `APPROVE_WITH_MINOR_REVISIONS`

## Scope and evidence

Reviewed design: `.memory/logs/ssfo_local_cache_design.md`. Current active source checked:

- `ZCL_ABAPGIT_ORTEC_SER_ORCH=>SERIALIZE`, `=>DISPATCH_BATCH`, and `=>ROUTE_TO_SEQUENTIAL_FALLBACK`
- `Z_ABAPGIT_ORTEC_SER_BATCH`
- `ZCL_ABAPGIT_ORTEC_FDT0_CACHE`
- `ZCL_ABAPGIT_ORTEC_GIT_SWITCH`
- `ZCL_ABAPGIT_OBJECTS=>SERIALIZE`
- `ZCL_ABAPGIT_OBJECT_SSFO=>ZIF_ABAPGIT_OBJECT~SERIALIZE`
- `CL_SSF_FB_SMART_FORM=>LOAD` and `=>ENQUEUE`

No scoped performance-scan output was available in the allowed context. This is a design assessment, not a measured production-performance claim.

The current source verifies the proposed interception point: the ORTEC fallback and aRFC worker both currently call the FDT0 wrapper, and `ZCL_ABAPGIT_OBJECTS=>SERIALIZE` owns final i18n, metadata, item state, duplicate checking, and file SHA calculation. The worker is a separate aRFC session and already receives the FDT0 decision as an explicit parameter. `CL_SSF_FB_SMART_FORM=>LOAD` confirms that its effective language is obtained from the same `SHOW` enqueue boundary used by the design.

## Design assessment

The normal SSFO hot path is form-local and bounded. It reads only the form's active dependency rows, reads one cache payload by the client/form primary key plus identity predicates, and updates recency. It introduces no HTTP call. It does not read all cache keys or payloads, and eviction selects only form name and payload-size metadata.

The proposed `5,000` signature-row, `16 MiB` signature-input, `12 MiB` serialization-content, `16 MiB` exported-payload, `5,000` cache-row, `5 GiB` total-byte, `500` eviction-row, and `10` eviction-batch limits define a safe oversize bypass and bounded maintenance path. The one-row-per-client/form key prevents context or language cardinality from multiplying stored rows. The `LAST_USED_AT, FORMNAME` index supports deterministic oldest-first metadata eviction without reading `PAYLOAD`.

The signature algorithm has fixed statement shape: one header read, one saved-source presence read, and five active dependency reads. It is $O(R + P)$ for $R \leq 5{,}000$ selected signature rows and $P \leq 16\,MiB$ signature bytes, not $O(N)$ over all cached forms. The cache lookup is a primary-key lookup with identity predicates. The design correctly makes saved/inactive source a bypass and validates the candidate under the same Smart Form `SHOW` lock boundary that determines effective language.

## Scale model

| Cardinality | Normal read behavior | Store/maintenance behavior |
|---|---|---|
| 1 form | 9 cache SQL statements, 0 HTTP; bounded signature and one payload | 17 cache SQL statements without eviction; 0 HTTP |
| 1,000 forms | Still form-local primary-key read; no cache-wide payload scan | Aggregate only on store; no eviction while below target |
| 40,000 forms | Per-read work remains independent of stored cache cardinality | Retains at most 5,000 rows/5 GiB; each store repairs at most 10 x 500 metadata rows, then removes/skips the new row |
| 1,000,000 stored rows | Read remains $O(R + P)$, not $O(N)$ | At most 10 bounded select/delete eviction batches; no million-row internal table or payload scan |

For $N$ stored cache rows, $K$ forms serialized by the run, and $B$ eviction batches, normal cache work is $O(K(R + P))$ and maintenance is $O(B \cdot 500)$ metadata keys, where $B \leq 10$. There is no graph traversal and no HTTP request. A cold store with maximum maintenance is bounded at 37 cache SQL statements plus unchanged standard serialization; the design's stated hot hit is 9 cache SQL statements and 0 HTTP.

## Findings

### PERF-SSFO-003

Severity: Minor  
Path and method: Proposed `ZCL_ABAPGIT_ORTEC_SSFO_CACHE=>TRY_READ`, `=>STORE`, and `=>COMPUTE_ACTIVE_SIGNATURE`  
Observed call shape: The design bounds bytes but does not state a strict maximum simultaneous XSTRING-copy count by phase. It lists buffers and a 96 MiB target but leaves the overlapping payload/export/import copies implicit.  
Expected production cardinality: One SSFO call, up to 16 MiB cache payload and 12 MiB serialized content.  
Estimated SQL calls: Unchanged: 9 on a hit, 17-37 cache SQL on a cold store.  
Estimated HTTP calls: 0.  
Estimated memory impact: Bounded, but the peak proof is incomplete until it explicitly counts the signature export, selected DB payload, imported serialization, outgoing export, and any retained standard result by phase.  
Why it matters: The performance gate requires a stated maximum number of simultaneous payload/XSTRING copies; byte caps alone do not prove that the 96 MiB delta is enforceable.  
Required fix: Add a phase-by-phase lifetime table that states the maximum simultaneous XSTRING buffers and their byte caps for hot read, cold store, oversize bypass, corrupt payload, and eviction. Clear signature/export buffers before payload import and require an explicit no-overlap assertion or SAT proof.  
Regression test or measurement: SAT peak-memory comparison for a 12 MiB-content/16 MiB-export fixture, reporting measured peak delta and confirming it stays within the documented bound.

### PERF-SSFO-004

Severity: Minor  
Path and method: Proposed `ZCL_ABAPGIT_ORTEC_SSFO_CACHE=>SERIALIZE` / `=>RESOLVE_EFFECTIVE_LANGUAGE`  
Observed call shape: A cache hit retains the Smart Form `SHOW` global-lock/permission boundary while it computes the full signature, imports and validates the payload, hashes file content, and updates `LAST_USED_AT`. The design specifies whole-hit time but not a direct lock-hold measurement or threshold.  
Expected production cardinality: Hot SSFO calls, including concurrent aRFC workers and users editing the same form.  
Estimated SQL calls: 9 per hit.  
Estimated HTTP calls: 0.  
Estimated memory impact: No additional unbounded allocation; lock duration is dominated by at most 16 MiB signature work plus 16 MiB payload import/validation.  
Why it matters: The lock is intentionally required for correctness, so its duration must be measured and bounded as an observable concurrency cost rather than inferred from total elapsed time.  
Required fix: Add an acceptance metric around the exact enqueue/dequeue interval: five warmed hot runs, median and worst lock duration, no editor/aRFC deadlock, and a stated threshold no greater than the accepted 1.0 s hot-run ceiling. Record the per-worker concurrency used for this test.  
Regression test or measurement: Focused SAT/enqueue trace with one editor contention scenario and concurrent aRFC cache hits; verify one `SHOW` enqueue/dequeue pair per candidate and no lock leak on every cache-error fallback.

## Gate decision

No blocking performance architecture defect was found. The design avoids all hard failures for this slice: no SQL/HTTP per source file or DOM node, no recursive SQL traversal, no full cache-key/payload scan for normal reads, no unbounded payload accumulation, no cache-owned commit, and no per-row invalidation. aRFC activation is explicit, optional, and reset in the worker design, matching the active FDT0 pattern.

Implementation may proceed after the two minor design revisions above are incorporated. Production-scale approval remains conditional on the documented medium, large, million-row-resilience, hot-SAT, and concurrency acceptance scenarios; none has been executed by this review.

## Required next action

Update the SSFO design with `PERF-SSFO-003` and `PERF-SSFO-004`, then route the implementation slice to `ortec-abapgit-implementation-senior`. After implementation, run an `IMPLEMENTATION_AUDIT` using the configured SQL/HTTP counters and the specified SAT and scale scenarios.