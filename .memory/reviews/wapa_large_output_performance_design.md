# WAPA large-output performance design gate

```text
TASK_ID=WAPA_LARGE_OUTPUT_OPTION1_PERF_GATE_20260813
MODE=DESIGN_GATE
SCOPE=ZCL_ABAPGIT_ORTEC_WAPA direct raw-prefetch helpers only
DESIGN_ARTIFACT=.memory/logs/wapa_large_output_design.md
PRIOR_REVIEW=.memory/reviews/wapa_large_output_performance_design.md
VERDICT=APPROVE_WITH_MINOR_REVISIONS
```

## Final post-owner-decision gate

This review supersedes the previous `BLOCK_PERFORMANCE_ARCHITECTURE` conclusion for the
revision-2 design. It assesses revision 3 and the owner's Option 1 decision only. Productive
source is intentionally unchanged; the active direct-helper source could not be reopened through
the supplied virtual-filesystem file surface, so current-source confirmation remains an
implementation checkpoint rather than claimed evidence here.

### Prior finding closure

| Prior finding | Status | Closure evidence |
|---|---|---|
| WAPA-PD-04 | CLOSED | `C_MAX_RAW_SPLIT_DEPTH = 5` is terminal. A 1,000-page root has at most 127 raw-helper SELECTs: 63 rejected manifests plus 32 admitted leaf payload and 32 verification selects. A depth-5 rejection takes exactly one contiguous reference range, with zero further raw-helper SELECTs. |
| WAPA-PD-05 | CLOSED_AT_DESIGN_GATE | The design states physical raw input <=100 MiB, one decoded page <=15 MiB, at most four application page-scale owners, and one kernel IMPORT transient. It frees raw/manifest/page locals before suffix/reference fallback and makes paired SAT a publication gate. |

### Scale and call-shape assessment

| Scale | Healthy raw-helper SQL | Maximum raw-helper SQL | Reference fallback | New HTTP | Raw/decode live state |
|---:|---:|---:|---|---:|---|
| 1 page | 3 | 3 | defensive only | 0 | <=100 MiB charged raw plus one <=15 MiB decoded page |
| 1,000 pages | 3 | 127 | one or more terminal contiguous ranges | 0 | same bounded attempt state |
| 40,000 pages | 120 | 5,080 | defensive only | 0 | 40 bounded top-level groups |
| 1,000,000 pages | 3,000 | 127,000 | analytical only | 0 | bounded per group; total output remains existing linear owner |

The 127 bound is correct: at depths 0 through 4 a rejected node contributes one
metadata SELECT and two child attempts; depth 5 cannot split. The reference range retains the
unchanged `READ_PAGE` shape of one mandatory and up to two optional cluster imports per page,
but it is no longer a normal raw-admission correctness path. `/O4H/COMPANION` acceptance requires
zero range fallbacks, zero reference pages for raw-eligible pages, and zero depth-ceiling hits.

### Lifecycle and architecture assessment

- Metadata admission has a 40,001-row probe and charges 2,886 bytes per row before the payload
  SELECT. Admitted payloads are capped at 36,333 rows and 104,857,038 charged bytes.
- Decode processes one `BY_PAGE` slice, deletes raw rows as fragments are consumed, frees each
  assembly buffer after IMPORT, and publishes maps only after a complete page succeeds.
- A decode failure or post-decode 15 MiB breach frees the admitted remainder before the reference
  suffix begins. No raw map, raw rows, or raw admission remains beneath `SERIALIZE_REFERENCE_RANGE`.
- No new HTTP/aRFC, persistence, cache, invalidation, or transaction boundary is introduced;
  WAPA stays singleton-batched and the outer worker remains publication owner.

### Minor finding: WAPA-PD-05-M1

```text
Severity=MINOR
Path and method=ZCL_ABAPGIT_ORTEC_WAPA=>DECODE_RAW_PAGE / RAW_PREFETCH_AND_READ
Observed call shape=The design bounds application-owned raw and decoded lifetimes but the kernel IMPORT allocation is only measurable at runtime.
Expected production cardinality=Largest page <=15 MiB; /O4H/COMPANION acceptance, then 40,000-page synthetic acceptance.
Estimated or measured SQL calls=No additional SQL.
Estimated or measured HTTP calls=0 new.
Estimated or measured memory impact=<=100 MiB charged raw + one <=15 MiB decoded page + four application page-scale owners + one unmeasured kernel transient.
Why it matters=The physical kernel transient and process peak are not established by design alone.
Required fix=Before publication, execute the specified paired SAT and reject publication on an unclassified transient, more than four page-scale owners, page >15 MiB, nonzero Companion range fallback, or reference-peak regression.
Regression test or measurement=Run all section-8 40,000-page fixtures and the paired /O4H/COMPANION SAT acceptance; retain SQL/row/byte/counter evidence.
```

## Required implementation and publication checks

1. Confirm current direct-helper source has no accidental raw recursion below
   `SERIALIZE_REFERENCE_RANGE` and no SELECT inside its page loop.
2. Run the prescribed 40,000-page healthy, rejected, depth-5-admitted, pair-over-budget,
   payload-drift, and decode-failure fixtures; record <=5,080 raw-helper SELECTs.
3. Run paired `/O4H/COMPANION` SAT/parity acceptance with zero fallbacks and the declared
   decoded/copy/process-peak limits before activating production publication.

```text
BLOCKERS=0
MAJORS=0
MINORS=1 (WAPA-PD-05-M1, publication measurement)
EVIDENCE=Design and prior-review analysis; scale counts are estimates, not measured performance.
IMPLEMENTATION_ALLOWED=YES_AFTER_OWNER_REVIEW
NEXT_HANDOFF=Correctness review, then implementation senior; return to performance implementation audit after checkpoints A-F and measured acceptance.
```# WAPA large-output performance design gate

```text
TASK_ID=WAPA_LARGE_OUTPUT_PERF_DESIGN_REGATE_20260813
MODE=DESIGN_GATE
SCOPE=ZCL_ABAPGIT_ORTEC_WAPA and direct raw-prefetch helpers; ORCH/BATCH read-only
DESIGN_ARTIFACT=.memory/logs/wapa_large_output_design.md
VERDICT=BLOCK_PERFORMANCE_ARCHITECTURE
```

## Cycle 2 re-gate

This section supersedes the earlier gate findings and recommendation below. It assesses design
revision 2 only; prior findings remain as historical evidence.

### Prior finding closure

| Prior finding | Cycle 2 status | Evidence |
|---|---|---|
| WAPA-PD-01 | CLOSED | Metadata admission projects no CLUSTD, applies 40,001-row and conservative 100 MiB capacity checks, and rejects before the payload SELECT. Admitted payload fetches cap at 36,333 rows / 104,857,038 charged bytes. |
| WAPA-PD-02 | PARTIALLY_CLOSED | The design correctly withdraws the false decoded-byte preflight claim and limits raw input plus decoded lifetime to one page. It still leaves Bpage/Dpage/Timport/Opage and their simultaneous-copy count unquantified. |
| WAPA-PD-03 | CLOSED_AT_DESIGN_GATE | The 40,000-page instrumented acceptance, multi-group/split/fallback coverage, and explicit unexecuted 1,000,000-page model are specified. Execution remains required before production approval. |

### Cycle 2 findings

#### WAPA-PD-04

```text
Severity=BLOCKER
Path and method=ZCL_ABAPGIT_ORTEC_WAPA=>RAW_PREFETCH_AND_READ recursive manifest-split path
Observed call shape=A rejected n-page node issues one metadata SELECT and splits. Every admitted leaf issues admission + payload + verification SELECTs. A fully splitting 1,000-page top-level group costs 4n-1 = 3,999 SQL statements.
Expected production cardinality=40,000 pages and 1,000,000-page analytical capacity.
Estimated or measured SQL calls=Healthy: 120 at 40,000 pages and 3,000 at 1,000,000 pages. Fully split: 159,960 at 40,000 pages and 3,999,000 at 1,000,000 pages.
Estimated or measured HTTP calls=0 new.
Estimated or measured memory impact=Each individual attempt remains metadata-bounded or <=100 MiB charged raw payload, but the retry count is unbounded across all top-level groups.
Why it matters=The recursive correctness fallback becomes effectively SQL-per-page at production scale. The sentence that a trend toward the upper bound is "rejection, not permission" does not stop the path or select a bounded alternative.
Required fix=Define and enforce a per-top-level-group split-attempt/depth budget. Once exhausted, clear state and take one explicitly bounded group-level fallback that does not issue one metadata/payload/verification sequence per page; specify its SQL shape, byte policy, and parity behavior. Add a counter and a hard acceptance threshold that prevents the 159,960/3,999,000 shapes.
Regression test or measurement=40,000-page adversarial manifest fixture that forces repeated rejects. Assert the configured split ceiling, no payload fetch for rejected parents, bounded SQL counter, empty context at fallback boundaries, and reference parity.
```

#### WAPA-PD-05

```text
Severity=BLOCKER
Path and method=ZCL_ABAPGIT_ORTEC_WAPA=>DECODE_RAW_PAGE / RAW_PREFETCH_AND_READ
Observed call shape=One admitted raw batch is retained while a page buffer, IMPORT targets, decoded maps, final page output, and potentially FILES_XSTRING coexist.
Expected production cardinality=Largest productive logical page is unknown; final observed /O4H/COMPANION output is 791,297,854 bytes.
Estimated or measured SQL calls=No additional SQL.
Estimated or measured HTTP calls=0 new.
Estimated or measured memory impact=Raw input is bounded to 104,857,600 charged bytes, but Bpage + Dpage + Timport_page + Opage_final are explicitly unknown and the design does not state a maximum simultaneous XSTRING-copy count.
Why it matters=One-page lifetime prevents chunk-wide decoded retention but is not a memory envelope. The mandatory design input requires a stated copy bound; a compression-expanding page can add unquantified transient allocations on top of the retained raw batch and final-output copies.
Required fix=Measure the largest logical page and kernel IMPORT transient against the paired IT8 trace before implementation approval, then document an enforceable copy-count/lifetime budget. If it exceeds the process envelope, add an owner-approved safe route that releases the raw batch before decode or bypasses prefetch for that page; do not claim a decoded cap without a preflight capability.
Regression test or measurement=Compression-expanding fixture plus focused SAT records Bpage, Dpage, IMPORT transient, Opage_final, maximum simultaneous copies, and process peak against reference. Assert context and page locals are freed at every page boundary.
```

### Cycle 2 scale and lifecycle summary

| Scale | Healthy SQL | Current fully-split SQL | Raw input per admitted attempt | Decoded/copy status |
|---:|---:|---:|---:|---|
| 1 page | 3 | 3 plus reference on failure | <=36,333 rows / 100 MiB charged | One page, unquantified peak |
| 1,000 pages | 3 | 3,999 | <=36,333 rows / 100 MiB charged | One page, unquantified peak |
| 40,000 pages | 120 | 159,960 | <=36,333 rows / 100 MiB charged | Required acceptance, unexecuted |
| 1,000,000 pages | 3,000 | 3,999,000 | <=36,333 rows / 100 MiB charged | Analytical only, unacceptable fallback count |

The designed one-page lifecycle clears maps before each retry, after successful consumption, and
around reference fallback; this closes DR-001 provided the specified helper is implemented
exactly. Cache scope is request/method only, publication remains worker-owned, and no new HTTP,
transaction, or cache invalidation path is introduced. Evidence is design/static estimation only;
no current-source or trace measurement was provided in the allowed context for this re-gate.

### Cycle 2 verdict

```text
VERDICT=BLOCK_PERFORMANCE_ARCHITECTURE
BLOCKERS=WAPA-PD-04,WAPA-PD-05
MAJORS=0
MINORS=0
IMPLEMENTATION_ALLOWED=NO
NEXT_HANDOFF=Return to WAPA design owner for bounded split-fallback architecture and measured per-page copy envelope; then rerun DESIGN_GATE.
```

## Evidence

- Current productive source was read for `SERIALIZE`, `TRY_RAW_PREFETCH`, `READ_RAW_ROWS`, `ASSEMBLE_AND_DECODE`, `ADD_PAGE_CONTENT_FILE`, `ADD_FULL_PAGE_DETAILS`, and `BUILD_REQUESTED_KEYS`.
- The scoped design and payload-discovery artifacts were read. No scoped performance-scan output was supplied in `ALLOWED_CONTEXT`.
- The design's recursion and fallback call chain is valid: `RAW_PREFETCH_AND_READ` can leave `RAW_PREFETCH_ACTIVE` false, and both page consumers then use the existing per-page `IMPORT` path. Existing maps are not consumed while inactive.
- Current `READ_RAW_ROWS` is one set-based join but has no `UP TO` limit. Current `ASSEMBLE_AND_DECODE` increments `LV_TOTAL_BYTES` from `CLSTR` before `IMPORT ... FROM DATA BUFFER`; the decoded maps are populated only afterwards.

## Scale Model

| Pages in one WAPA | Healthy proposed raw SELECT attempts | Fully split proposed raw SELECT attempts | HTTP change |
|---:|---:|---:|---:|
| 1 | 1 | 1 | 0 new |
| 1,000 | 1 | 1,999 | 0 new |
| 40,000 | 40 | 79,960 | 0 new |
| 1,000,000 | 1,000 | 1,999,000 | 0 new |

The healthy shape is `ceil(P / 1000)` raw attempts plus unchanged context SQL. For a group of `n <= 1000`, recursive failure has the correctly documented `2n - 1` attempt bound. The unchanged singleton worker still exports/imports `FILES_XSTRING`; no HTTP/RFC call is added by this design.

The stated row bound would be at most 40,000 rows per attempt after `UP TO @iv_max_rows ROWS` is implemented. It is not a byte bound: each selected row includes an unrestricted `CLUSTD XSTRING`. The design's stated serializer memory expression, `F_so_far + R40K + D100 + Tdecode + metadata`, remains unbounded because `R40K` and `Tdecode` have no enforceable maxima. `F_so_far` already reaches 791,297,854 bytes in the supplied evidence, and the unchanged `FILES_XSTRING` representation can coexist with it.

## Findings

### WAPA-PD-01

```text
Severity=BLOCKER
Path and method=ZCL_ABAPGIT_ORTEC_WAPA=>READ_RAW_ROWS / ASSEMBLE_AND_DECODE
Observed call shape=One payload SELECT materializes up to 40000 CLUSTD XSTRING rows; only then does the decoder concatenate/import them.
Expected production cardinality=1000-page chunks; 11307 pages / 73861 O2PAGCON rows / 791MB observed; design models 40000 and 1000000 pages.
Estimated SQL calls=Healthy: ceil(P/1000); fully split: 2n-1 per <=1000-page group.
Estimated HTTP calls=0 new; unchanged singleton FILES_XSTRING export/import.
Estimated memory impact=R40K is byte-unbounded because CLUSTD is fetched before a cumulative byte check. The proposed 100MiB cap cannot constrain this input allocation.
Why it matters=UP TO limits row count only. A small number of very large CLUSTD rows can exceed the process memory envelope before bisection detects a limit. This violates the required row-and-byte batch policy and cap detection before payload materialization.
Required fix=Add an enforceable WAPA-local raw-payload byte budget before selecting CLUSTD payloads. The design must specify the metadata-first/set-based selection and bounded payload-fetch shape, including the exact raw-byte field, conservative cap semantics, SQL-call count, and an oversized single-page/reference-path rule. Do not treat 40000 rows as a byte budget.
Regression test or measurement=Fixture with few rows whose aggregate CLUSTD/CLUSTR bytes exceed the raw budget must split before any payload-bearing SELECT for the rejected parent chunk. SAT must report selected raw bytes and maximum payload-bearing SELECT bytes.
```

### WAPA-PD-02

```text
Severity=BLOCKER
Path and method=ZCL_ABAPGIT_ORTEC_WAPA=>ASSEMBLE_AND_DECODE
Observed call shape=LV_TOTAL_BYTES adds CLUSTR before IMPORT FROM DATA BUFFER; output is subsequently decoded into content tables, XML XSTRINGs, event-handler tables, and type source.
Expected production cardinality=Up to 1000 pages per initial group, then recursive halves; 791MB final WAPA output observed.
Estimated SQL calls=No additional SQL in this method.
Estimated HTTP calls=0 new.
Estimated memory impact=The proposed D100 is not a decoded-byte cap. CLUSTR counts compressed/input fragment length, while decoded content and transient import/concatenation copies can be larger and are currently unknown.
Why it matters=The design labels the 100MiB budget as decoded content and uses it to justify bounded XSTRING memory, but the current accumulator cannot measure or stop decoded-map growth. Post-hoc SAT cannot repair an unbounded productive allocation.
Required fix=Separate raw-input and decoded-output budgets. Define an enforceable limit for each decoded map and the aggregate maps, with a pre-publication cleanup path and explicit maximum simultaneous copies. Where decoded size cannot be known before IMPORT, define a conservative per-logical-key admission bound or an alternate bounded decode mechanism; a post-decode counter alone is insufficient for the stated peak-memory guarantee.
Regression test or measurement=Use a compression-expanding fixture to prove that raw-byte and decoded-byte accounting diverge; assert the rejected attempt publishes no active maps, releases attempt-local buffers, bisects, and uses reference processing only at a singleton leaf. Paired SAT must record largest logical-key decode, peak transient copies, and process peak.
```

### WAPA-PD-03

```text
Severity=MAJOR
Path and method=Design section 7 / RAW_PREFETCH_AND_READ
Observed call shape=The only live acceptance trace is /O4H/COMPANION at 11307 pages. The 40000- and 1000000-page cases are static calculations.
Expected production cardinality=40000 and 1000000 pages are explicitly required scale checkpoints.
Estimated SQL calls=40 healthy / 79960 fully split at 40000; 1000 healthy / 1999000 fully split at 1000000.
Estimated HTTP calls=0 new.
Estimated memory impact=Not measured at either required larger scale; final output and unchanged FILES_XSTRING remain linear in total output.
Why it matters=The 2n-1 retry shape is correctly recognized as unacceptable, but no acceptance scenario verifies counters, split-rate threshold, or cleanup through multiple top-level groups at 40000 pages.
Required fix=Add a medium/large non-production acceptance scenario: at least a 40000-page synthetic/instrumented model that exercises multiple 1000-page groups, both successful and splitting chunks, aggregate counters, and bounded live attempt state. Keep 1000000 as an analytical capacity model unless a memory-safe harness is available; state it as unexecuted rather than evidence.
Regression test or measurement=Record per-attempt rows/raw bytes/decoded bytes, recursion depth, terminal references, SQL count, and peak memory for the 40000-page scenario and the paired 11307-page IT8 trace.
```

## Required Correction

Revise the design before implementation to make both raw and decoded byte limits physical and enforceable before an over-budget parent payload is materialized. Specify the resulting two-phase SQL shape, byte budgets, oversized-page handling, maximum XSTRING copies, and the 40000-page acceptance scenario. Preserve the already-valid singleton fallback and order-preserving recursion.

## Status

Measured evidence is limited to the supplied 11,307-page IT8 case and current-source inspection. SQL counts for 40,000 and 1,000,000 pages are estimates from the proposed recurrence, not measurements. No productive source, state, design, or diagram artifact was modified.

## Next Handoff

Route the required architecture revision to the WAPA design owner. After revision, rerun this DESIGN_GATE before any implementation work is authorized.