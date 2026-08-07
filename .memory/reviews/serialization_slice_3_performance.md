# SER-SLICE-3 Phase 2 — performance scan (DOMA/DTEL provider)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_3_PHASE_2_PERFORMANCE_SCAN
VERDICT=PASS_WITH_FINDINGS
```

## Raw findings (verbatim from the compact envelope)

```text
PS-001|MINOR|Bulk DOMA prefetch uses wide SELECT * and repeated per-domain
  scans over the bulk DD01T/DD07L/DD07T tables; bounded by the current
  dispatch cap but still worth field-limiting if batch sizes grow.
  |src/ortec/serial/zcl_abapgit_ortec_ser_pref_ext.clas.abap

BULK_SELECT_SHAPE=CONCERN-4 bulk FOR ALL ENTRIES reads are single-pass and
  shape-correct, but the driver table can become large in a full-repo
  serialize and the code uses SELECT * instead of field-limited selects
EXTRACT_FOR_BATCH_REPEATED_COST=BOUNDED-linear work per dispatch with no
  extra DB work, and the recursive split path is depth-limited so the
  compounding is bounded
NESTED_LOOP_COMPLEXITY=ACCEPTABLE_AT_BATCH_CAP-the per-domain scans are
  in-memory and the SER-SLICE-2 batch cap of 25 keeps the worst-case
  growth manageable
XSTRING_HANDLING=OK-export uses COMPRESSION ON and no repeated
  append/copy pattern is visible
NEW_PER_OBJECT_DB_CALLS=NONE
RECOMMENDATIONS=Consider field-limited SELECTs and a small language-map/
  hash helper if DOMA batches grow beyond the current cap
```

## Disposition

No blocking finding. PS-001 accepted as a documented, non-blocking
trade-off at the current `c_max_batch_rows = 25` cap - consistent with this
project's existing precedent (SER-SLICE-2's own PS-002/PS-003 were
similarly accepted without code change). Revisit only if a future slice
raises the batch-row cap materially or DOMA/DTEL prevalence in a single
repository proves unusually high in a real IT8 trace.

## Addendum: parity-incident fix performance impact (2026-08-07)

The parity-incident Fix A (`.memory/incidents/serialization_slice_3_dtel_
doma_parity.md`) makes `ZCL_ABAPGIT_ORTEC_SER_ORCH=>SERIALIZE` call
`prepare()` on all three existing prefetch classes (MSAG/EXT/OO) - a REAL,
new bulk-SELECT cost on the adaptive batch path that did not exist before
(previously `prepare()` was never called there at all). This EXACTLY
matches the cost the classic/OFF path already pays unconditionally
whenever `is_serial_prefetch_active()` is on, so it is not a NEW class of
cost, only newly paid on a path that skipped it. Per the correctness
review's DR-001 (accepted, disclosed), the MSAG/OO families' bulk reads
currently benefit only the forced_seq/WAPA/in-process-fallback subset of
objects on this path, not the RFC-dispatched majority - a known,
non-blocking inefficiency, not a correctness issue.

## Phase 8 — final performance sanity review (repository setting, CLAS/
INTF provider, MSAG provider, Path 3 removal)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_3_PHASE_8_PERFORMANCE_REVIEW
REVIEWER_VERDICT=APPROVE
STATUS=STATIC_REVIEW_COMPLETED_NO_LIVE_TRACE
```

Written directly by the orchestrator (lightweight sanity pass, not a full
DESIGN_GATE/IMPLEMENTATION_AUDIT cycle - none of this session's changes
introduce a new O(n^2) shape or per-object DB access in a loop).

```text
Repository setting (Phase 3): one additional TRY/CATCH-wrapped read of a
  small in-memory user-settings table per zcl_abapgit_serialize
  construction - same proven-cheap pattern as the existing object-cache
  setting. Called once per serialize() invocation, not per object.

CLAS/INTF and MSAG extract_for_batch (Phase 4/6): each called EXACTLY
  ONCE per BEFORE_DISPATCH invocation (once per batch, not per object),
  doing an in-memory LOOP over it_object_keys against already-prepared
  HASHED TABLE caches - zero DB access inside extract_for_batch itself
  (the DB access already happened once in PREPARE(), unchanged from
  before this session). Byte-sizing reuses extract_for_object per HIT
  object (one small additional EXPORT, not a DB call, bounded by batch
  size) - same order of magnitude as the DD provider's existing pattern.
  Disclosed, accepted, non-blocking: neither new buffer is folded into
  the existing c_max_actual_batch_bytes admission/split check (that gate
  remains DD-buffer-scoped only) - the same class of limitation the DD
  provider itself shipped with, not a new risk introduced this session.

Path 3 removal (Phase 7): net REDUCES per-run work in the classic path -
  it no longer performs 3 PREPARE() calls' worth of FOR ALL ENTRIES
  SELECTs (DD01L/DD01T/DD07L/DD07T/DD04L/DD04T/SEOCLASSTX/SEOCOMPOTX/
  SEOSUBCOTX/T100/T100T) when the repository setting is OFF - these were
  PREVIOUSLY ALWAYS executed (Finding F-1's actual bug: default-on) and
  are now correctly executed ONLY when the batch path is active. Net
  performance IMPROVEMENT for Path A, not a regression.

Duplicate PREPARE() removal (Phase 4 cleanup, Finding F-3): removes one
  redundant full set of DD01L/DD01T/DD07L/DD07T/DD04L/DD04T SELECTs per
  batch run that was executed TWICE for no reason - a real, measurable
  improvement.
```

APPROVE. No new per-object-in-a-loop DB access was introduced anywhere in
this session's changes. The one disclosed limitation (OO/MSAG buffers not
yet folded into the byte-cap admission check) does not block this run's
authorized scope. A live SAT/ST05 trace on a large real repository
remains part of the consolidated IT8 validation plan's optional metrics,
not a blocking gate.
