# SER-SLICE-5 Phase 5 — WAPA singleton-brake audit

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_5_WAPA_BATCH_POLICY
STATUS=AUDIT_COMPLETE
```

## Current state (source-confirmed, current HEAD f54860d1)

```text
src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap:
  PARTITION_OBJECTS (line ~1045): "ELSEIF ls_tadir-object = 'WAPA'. APPEND
    ls_tadir TO rs_partition-wapa." - WAPA objects are unconditionally routed
    to their own bucket, BEFORE any is_standard_no_parallel_type check applies
    to them.
  BUILD_WAPA_SINGLETON_BATCHES (line ~1064): "LOOP AT it_wapa INTO ls_wapa.
    APPEND VALUE #( items = VALUE #( ( tadir = ls_wapa ) ) ) TO rt_batches.
    ENDLOOP." - literally one WAPA object per batch, unconditionally, no
    byte/count-based grouping logic exists at all for WAPA.
  No max-WAPA-per-batch constant, no byte estimate, no EWMA-based grouping is
    applied to the WAPA bucket - only the non-WAPA "eligible" bucket goes
    through ZCL_ABAPGIT_ORTEC_SER_PLANNER's adaptive sizing.
```

```text
WAPA_CURRENT_POLICY=WAPA_SINGLETON_ENFORCED
```

## Why it was introduced (from prior-session evidence, `serialization_wapa_
review.md`, SER-5) and whether the risk remains

```text
large/unpredictable serialized output   Still true today - WAPA's own comment
                                         ("do not add an artificial WAPA page
                                         size cap") is unchanged; no page-count
                                         cap exists.
lack of output-size estimate            Still true - ZCL_ABAPGIT_ORTEC_SER_COST's
                                         EWMA estimate is object-type-generic,
                                         not proven for WAPA's own highly
                                         variable per-page content shape.
one-object RFC contract (legacy)        Superseded - WAPA now runs inside the
                                         SAME adaptive batch RFC as every other
                                         type (SER-SLICE-2 Stage A), just as a
                                         one-item batch.
known parity limitation                 Zero ABAP Unit tests existed for
                                         ZCL_ABAPGIT_ORTEC_WAPA as of the SER-5
                                         review (not re-verified this slice -
                                         out of this session's SOURCE_SCOPE).
historical caution only                 Partially - the "never mixed with a
                                         non-WAPA object or another WAPA" rule
                                         was a DELIBERATE, reviewed policy
                                         choice (SER-SLICE-2 OD-14 audit), not
                                         an accidental gap.
```

## New, session-specific consideration: WAPA's batch-path gate was ALSO part
of finding SLICE5-001

`IS_WAPA_ACTIVE()` delegates to the exact same `mv_serial_prefetch_active` flag
that SLICE5-001 found was never set to TRUE inside the real ORTEC batch RFC
worker. This means: **every WAPA object that has ever been dispatched through
a real production batch run so far was silently served by the STANDARD
`ZCL_ABAPGIT_OBJECT_WAPA`/legacy `cl_o2_api_*` path, not the
`ZCL_ABAPGIT_ORTEC_WAPA` replacement**, despite the planner already isolating
it into its own singleton batch. Correctness was never at risk (the standard
path is always correct), but this materially changes the evidence base for any
WAPA relaxation decision: the replacement serializer's real behavior inside a
live batch dispatch has literally never been exercised in production, only in
local unit tests. Only 4 WAPA-related `CL_O2_API_APPLICATION` constructor
calls appear in either supplied trace - too small a sample to draw a
performance conclusion either way, and now known to be running the STANDARD
path in both traces regardless.

## WAPA decision

```text
WAPA_DECISION=KEEP_SINGLETON_WITH_EVIDENCE
```

Do not relax the singleton this slice. Two independent blockers exist for
relaxation right now, either of which alone would be sufficient:

```text
1. Insufficient trace evidence: this repository's two supplied traces contain
   only ~4 WAPA-related pages/applications - far too small a sample to derive
   safe byte/count thresholds "from existing constants/evidence" as required.
   The owner's own stated motivating case (thousands of WAPA/BSP artifacts) is
   not represented in this trace pair.
2. The replacement serializer has NEVER actually run inside a real batch
   dispatch in production (SLICE5-001) - relaxing to multi-object WAPA batches
   before even the EXISTING singleton-batch path is proven live and correct
   at IT8 would stack two unvalidated changes at once, violating the mandatory
   implementation flow's own ordering.
```

Required before any future relaxation design is authorized:

```text
1. IT8 retest of the SLICE5-001 fix, specifically including at least one real
   WAPA object in a batch run, confirming ZCL_ABAPGIT_ORTEC_WAPA's replacement
   now actually activates for that singleton batch and produces output
   byte-identical to the standard path.
2. A focused SAT/ST05 trace on a WAPA-heavy repository (owner's own motivating
   case) with the fix in place, to derive real per-page byte/count evidence
   for any future bounded-multi-WAPA design - this cannot be fabricated from
   the current trace pair.
```

No test matrix (one/multiple/large/mixed WAPA, output parity, ordering, state
leakage, oversized singleton, batch split, failure/partial-success, OFF-path)
is authored this slice, since no relaxation design exists to test against.
