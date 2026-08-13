# FDT0 Local Runtime Cache -- Performance Design Gate

```text
TASK_ID=FDT0_LOCAL_CACHE_PERFORMANCE_DESIGN_GATE_20260813
REVIEW_MODE=DESIGN_GATE
BASELINE_COMMIT=b4f41e38372a0fe9f67483f71e968b1885b594c1
DESIGN_ARTIFACT=.memory/logs/fdt0_local_cache_design.md
  (TASK_ID=FDT0_LOCAL_CACHE_DESIGN_REVISION_20260812, Cycle 2 body,
  Cycle-3-approved per .memory/reviews/fdt0_local_cache_adversarial_review.md)
CORRECTNESS_GATE=APPROVE_WITH_MINOR_REVISIONS (adversarial review, Cycle 3 FINAL,
  0 open BLOCKER/MAJOR -- see that artifact for AR-1-001/002/003, AR-2-001,
  AR-1-004, AR-1-001-RESIDUAL, AR-3-001)
STATIC_PERFORMANCE_SCAN=NOT AVAILABLE this pass (no scoped scan artifact supplied
  in ALLOWED_CONTEXT / handoff) -- evidence base is the design text plus the
  adversarial review's own live-source citations (same baseline, same session
  family); no independent re-read of SOURCE_SCOPE objects performed beyond what
  is already cited and cross-checked in the design/adversarial-review artifacts.
VERDICT=APPROVE_WITH_MINOR_REVISIONS
OPEN_BLOCKER=0
OPEN_MAJOR=1 (PERF-DESIGN-001)
OPEN_MINOR=1 (PERF-DESIGN-002)
```

## Scope

Performance-only recheck of the FDT0 local runtime cache design, on top of the
already-completed correctness adversarial review (3 cycles, APPROVE_WITH_MINOR_
REVISIONS). This review does not re-litigate signature-soundness (AC-03/AR-1-002/
AR-2-001) or activation-wiring reachability (AC-02/AR-1-001) -- both are correctness
findings already closed with live source evidence in the adversarial review and are
treated here as given. This review's own focus is strictly: SQL/HTTP call shape,
read/write volume, payload/XSTRING memory lifetime, limits/eviction, lock
contention under parallel workers, cache hit/miss estimates, and measurement
acceptance -- per REVIEW_FOCUS.

## Required design inputs -- coverage check

| Input | Present? | Where |
|---|---|---|
| Expected production cardinality | YES | Design section 2 (K = distinct BRF+ applications, system ceiling 307 on IT8, not N = repo object count) |
| Entry methods / complete hot path | YES | Sections 4-5 (two interception anchors, both traced end to end) |
| SQL statement shape | YES | Sections 4, 6 (resolve_application_id, compute_signature, try_read, store -- each a single, fully-scoped statement) |
| HTTP request shape | YES (N/A, correctly argued) | Section 2 -- BRF+ export is in-process, no HTTP anywhere in this path |
| Row batch limit | YES | c_max_signature_rows = 200000 defensive ceiling on compute_signature's SELECT |
| Byte batch limit | YES | c_max_cache_payload_bytes = 52428800 (50 MB) on store() |
| Oversized single-object behavior | YES | Section 2/4 -- oversized payload skips the cache write only; real result still returned unchanged |
| Internal-table lookup structures/complexity | YES (trivial) | No internal-table lookup structure needed -- both reads are single-row-keyed DB SELECTs, not itab scans |
| Cache scope and invalidation | PARTIAL -- see PERF-DESIGN-001 | Section 2 ("per SAP client, keyed by (application_id, signature)"), section 8 ("Eviction / size limits") -- scope is correct, but the invalidation/eviction story rests on an incorrect cardinality premise |
| Transaction owner and publication boundary | YES | Section 8 -- single-row MODIFY, natural aRFC/dialog commit boundary, matches the already-proven ZCL_ABAPGIT_ORTEC_OBJ_STORE idiom |
| Maximum simultaneous payload/XSTRING copies | YES | Section 2 "Peak memory model" -- one transient EXPORT-buffer copy, released immediately after MODIFY or discard |
| Medium and large acceptance scenarios | YES | Section 2 (two-application cold/warm collapse scenario matching EVIDENCE; one-application-modified scenario) |

All twelve mandatory design-input categories are present in the design text; one
(cache scope and invalidation) is present but materially incomplete, which is the
basis for PERF-DESIGN-001 below. This is not a missing-input rejection
(REVISE_AND_REVIEW_ONCE / BLOCK_PERFORMANCE_ARCHITECTURE) -- the category exists
and is addressed, just with an incorrect assumption that a minor revision closes.

## Design analysis -- cardinality axes

**N axis (total repository objects) -- correctly decoupled.** Unlike
OBJ_PERF_FINAL, this cache's cost/benefit is not a function of repository object
count at all. A 1,000,000-object repository containing zero FDT0 TADIR rows incurs
zero cost and zero benefit from this design; a small repository containing every
system-wide FDT0 application still only drives, at most, 307 independent
signature computations. The design's own section 2 makes this argument explicitly
and correctly -- verified against ZCL_ABAPGIT_ORTEC_SER_COST's FDT0 costing
(c_default_ms_brf = 150000, cited and cross-checked by the adversarial review,
Cycle 1/2) and against the measured system-wide ceiling (307 non-deleted AP rows,
measured live on IT8, design section 1).

**K axis (distinct BRF+ applications actually serialized in one run) -- the real
governing cardinality:**

| K (FDT0 objects in one repo/run) | SQL calls | HTTP calls | Rows read (worst case, largest known app = 5179) | Rows written (miss) | Max simultaneous payload bytes | Cache invalidation events |
|---|---|---|---|---|---|---|
| 1 | 3 (miss) / 3 (hit) | 0 | up to 5179 | 1 | <= 50 MB (1 buffer) | 0 explicit (content-addressed, no invalidation call) |
| 2 (EVIDENCE case) | 6 (miss) / 6 (hit) | 0 | up to 2 x 5179 | up to 2 | <= 50 MB per object, never concurrent across objects in one worker | 0 |
| 10 | 30 | 0 | up to 10 x 5179 | up to 10 | <= 50 MB, one at a time | 0 |
| 307 (system-wide ceiling, single run) | up to 921 | 0 | up to 307 x 5179 (bounded by real graph sizes, not a scan) | up to 307 | <= 50 MB, one at a time | 0 |

No scenario multiplies SQL/HTTP calls by repository size (N); every number above
scales with K only, and K is hard-capped by a real, measured, system-wide ceiling
that the design cannot itself cause to grow (BRF+ application count is owned by
functional users, not by this cache). This satisfies the "normal incremental work
scales with K, not N" requirement.

**store() write-volume per FDT0 object, both paths:**

- Miss: `resolve_application_id` (1 SELECT SINGLE) + `compute_signature` (1 SELECT,
  bounded by that application's own real graph size) + `try_read` (1 SELECT
  SINGLE, miss) + unmodified real `serialize()` (unchanged cost, ~150-200 s per
  EVIDENCE) + `store` (1 MODIFY). Net *new* overhead vs. today: 3 lightweight
  statements + 1 upsert.
- Hit: identical 3 lightweight statements, then a cached-buffer `IMPORT` in place
  of the ~150-200 s export chain. This is the entire mechanism of the claimed
  speedup and is architecturally sound -- no batching, HTTP, or additional SQL is
  introduced on the hit path beyond what miss already pays for the "is this still
  valid" check.

No SQL-per-object-in-a-loop pattern exists anywhere in this design: every SQL
statement is scoped to exactly one already-known key (an `application_id`, or an
`(application_id, signature)` pair), never iterated inside a loop over an
unbounded or repository-scale collection. This matches the mandatory bulk-access
pattern for the one case where iteration does occur (fetching one application's
own admin-row graph is a single unbounded-cardinality-but-single-application
SELECT, not a per-row SELECT).

## Findings

### PERF-DESIGN-001 -- MAJOR

```text
ID=PERF-DESIGN-001
SEVERITY=MAJOR
CLAIM=Design section 8 ("Eviction / size limits"): "Row count is not expected to
  need active eviction given the measured system-wide ceiling of 307 applications"
  -- i.e., the design treats "307 applications" as an effective row-count ceiling
  for ZAOG_FDT_CACHE and defers any cleanup mechanism to an unscheduled "later
  slice."
COUNTEREXAMPLE=The table's primary key is (client, application_id, signature) --
  not (client, application_id). Every time a BRF+ application's owned admin-row
  graph changes in any way (per the AC-03 signature-soundness proof this same
  design relies on for correctness), compute_signature produces a NEW, different
  signature value, and store() writes a NEW row under that new key -- it never
  overwrites or removes the row(s) left behind under the application's PREVIOUS
  signature(s), because MODIFY's key does not match. Section 3 ("No status
  column... eviction is a hard DELETE") and section 7 ("last MODIFY wins
  harmlessly") both correctly describe same-key idempotency, but neither describes
  -- and the design never triggers -- any DELETE of superseded, no-longer-
  reachable rows for the SAME application_id under its OLD signature(s). "307
  applications" bounds the number of distinct application_id VALUES, not the
  number of distinct (application_id, signature) rows the table will ever
  accumulate. Over the system's real operational life, every content change to
  every actively-maintained BRF+ application permanently adds one more orphaned
  row (up to 50 MB of payload each, per c_max_cache_payload_bytes) that no code
  path in this design will ever read or delete again -- this is unbounded growth
  of a persistent DB table with zero garbage collection built into this slice,
  not the bounded, self-limiting cache the section 8 text asserts. The admin
  cleanup action mentioned as the mitigation is explicitly "not built in this
  slice" (section 11, non-goals) and has no scheduled follow-up slice or owner
  commitment recorded anywhere in the design or in .memory/state.md.
IMPACT=performance/lifecycle -- not an immediate production-unusability risk (the
  growth rate is bounded by how often BRF+ applications are actually edited, and
  each individual serialize() call's own cost is unaffected by total table size
  since every lookup is a fully-keyed SELECT SINGLE / point SELECT, not a scan) --
  but it directly contradicts a design input this mode requires (a documented,
  correct eviction/invalidation story) and leaves an unbounded-growth table with
  no owner-visible size ceiling, no monitoring hook, and no cleanup path shipped
  in this slice. A years-long-lived, actively-maintained set of BRF+ applications
  with frequent content edits could accumulate materially large numbers of
  50 MB-capped orphaned rows with no mechanism to reclaim them short of a manual
  DBA table truncate/reorg -- an operational cost this design does not disclose
  as a real, non-optional consequence of shipping this slice without eviction.
REQUIRED_CHANGE=Either (a) close the gap in this slice with a trivial, already-
  available mechanism: since only the CURRENT signature for a given application_id
  is ever useful (an older signature can never be looked up again once the
  application has changed), have store() delete any other existing rows for the
  same (client, application_id) before or immediately after writing the new row
  (a single additional set-based DELETE keyed by client+application_id, excluding
  the just-written signature) -- this permanently bounds ZAOG_FDT_CACHE to at
  most 307 rows (the real, correctly-scoped ceiling) for the entire life of the
  system, at negligible extra cost (one more fully-keyed statement on the
  already-taken miss-path write, never on the hit path); or (b) if eviction is
  deliberately deferred, section 8/11 must say so accurately -- replace "not
  expected to need active eviction" with an explicit, owner-acknowledged statement
  that ZAOG_FDT_CACHE will grow without bound in proportion to cumulative BRF+
  content-edit history until a follow-up admin-cleanup slice ships, and record a
  concrete follow-up commitment (not an open-ended "later slice") in
  .memory/state.md once implementation begins. Option (a) is strongly preferred:
  it is cheaper to build than to document and monitor as an accepted risk, and it
  requires no new class, table, or admin-tooling scope (stays inside
  ZCL_ABAPGIT_ORTEC_FDT0_CACHE, no FDT0-INV-04 exposure).
ESTIMATED_OR_MEASURED=Estimated (no live measurement of long-run table growth is
  possible for a not-yet-implemented slice); the row-count math above
  (application-count x historical-edit-count) is a direct, non-speculative
  consequence of the table's own literal key design and the store() pseudocode as
  currently written, not a hypothetical edge case.
RETEST=Re-review confirms either (a) store()'s pseudocode includes a superseded-
  row purge scoped to (client, application_id) excluding the current signature,
  with a corresponding UT (store() for a changed application leaves exactly one
  row for that application_id), or (b) sections 8/11 are corrected to state the
  unbounded-growth consequence accurately and a concrete follow-up owner
  commitment is recorded.
```

### PERF-DESIGN-002 -- MINOR (contingent on PERF-DESIGN-001 option (a))

```text
ID=PERF-DESIGN-002
SEVERITY=MINOR
CLAIM=If PERF-DESIGN-001 is closed via option (a) (purge-on-store), no new race
  condition is introduced.
COUNTEREXAMPLE=Design section 6's own "torn/concurrent read" analysis already
  acknowledges that two parallel workers can legitimately compute two different
  transient signatures for the SAME application_id if that application is mid-edit
  during a git export (rare, already accepted as harmless for correctness since
  neither signature can ever be looked up as the other). Adding a superseded-row
  purge keyed only by (client, application_id) means that if this rare interleaving
  occurs, one worker's purge (scoped to "not my signature") could delete the
  OTHER worker's just-written row before that worker's own caller ever gets a
  chance to look it up on a later warm run -- a wasted cache write, self-healing
  on the next real miss, never a false hit or data loss (the source of truth in
  both cases is the real serialize() result already returned to each worker's own
  caller, unaffected by the cache row's fate).
IMPACT=performance only (a rare, self-healing wasted write under an already-rare
  interleaving), not correctness -- no false hit, no data loss, no propagated
  error.
REQUIRED_CHANGE=Non-blocking. If PERF-DESIGN-001 option (a) is adopted, note this
  interaction explicitly in section 6 or 8 alongside the existing torn-read
  discussion, so a future reader does not mistake an occasional "cache miss right
  after a hit should have been possible" symptom for a bug.
RETEST=Not required for approval.
```

## SQL/HTTP/memory summary

- **SQL calls per FDT0 object per serialize() call**: 3 (miss) or 3 (hit), all
  single-row/single-application-scoped, none looped over a repository-scale
  collection. No `SELECT`/`INSERT`/`MODIFY`/`DELETE` inside a loop over TADIR
  rows, pack entries, or any other repository-scale collection -- compliant with
  abap-performance-patterns skill section 2/3.
- **HTTP calls**: 0, correctly argued (in-process BRF+ export, no network I/O in
  this path at all).
- **Row batch limit**: `c_max_signature_rows` = 200000 (defensive; real observed
  max is 5179 on IT8) -- compliant with skill section 4 (row limit present).
- **Byte batch limit**: `c_max_cache_payload_bytes` = 50 MB per row -- compliant
  with skill section 4 (byte limit present) and section 5 (oversized-object path
  does not degrade the real result, only skips the cache write).
- **Maximum simultaneous payload copies**: 1 transient EXPORT buffer, released
  immediately after MODIFY or discard -- compliant with skill section 12 (no
  duplicate persistent-row-buffer accumulation).
- **Lock contention under parallel workers**: none introduced -- content-addressed
  key makes concurrent same-key writes idempotent and safe without ENQUEUE,
  matching skill section 9's guidance against unnecessary locking; different
  FDT0 objects dispatched to different aRFC workers never share a key. The one
  new race surfaced by this review (PERF-DESIGN-002) is self-healing and
  performance-only, contingent on adopting the PERF-DESIGN-001 fix.
- **Cache hit-rate estimate**: for the EVIDENCE scenario (2 unchanged
  applications across two runs), a warm second run is expected to hit on both,
  collapsing the measured 276078 ms / 18.6% contribution to the low hundreds of
  ms (3 point-SELECTs + one in-memory IMPORT per object) -- consistent with the
  design's own acceptance scenario (section 2) and IT-04 test plan.
- **Table growth over time (new axis this review)**: unbounded without
  PERF-DESIGN-001's fix; bounded to <=307 rows for the life of the system with it.

## Measured vs. estimated evidence

All of the above is estimated from the design text and from the adversarial
review's own live-source citations (SER_COST costing, FDT_ADMN_0000S cardinality
measurement, GIT_SWITCH/SER_ORCH/SER_BATCH anchor shapes) -- no new live SAP reads
were performed this pass (no implementation exists yet to measure; this is a
design-only gate). The EVIDENCE figures (276078 ms / 193640 ms max / 18.6% of
1483415 ms) and the 307-application / 5179-row cardinality figures are themselves
already-measured, live-IT8 facts carried over from the design's own section 1 and
independently re-confirmed by the adversarial review across three cycles -- this
review treats them as measured, not estimated.

## Verdict

**APPROVE_WITH_MINOR_REVISIONS.** Zero blocking performance findings: SQL/HTTP
shape, batching, XSTRING/memory bounds, hit-path speedup mechanism, and lock-
avoidance under parallel workers are all sound and correctly scoped to the real
governing cardinality (K = distinct BRF+ applications, hard-capped at 307 on
IT8), not to repository object count. One MAJOR (PERF-DESIGN-001) requires a
concrete fix -- preferably a one-statement superseded-row purge in `store()` --
before this design's own "cache scope and invalidation" input can be considered
accurate and complete; this is implementable within the existing class/method
boundaries already approved by the correctness gate, with no new FDT0-INV-04
exposure, and does not require a further design review cycle. One MINOR
(PERF-DESIGN-002) is a documentation note contingent on that fix. This verdict is
independent of, and additive to, the correctness gate's own
APPROVE_WITH_MINOR_REVISIONS verdict and its non-waivable IT-01 production gate.
