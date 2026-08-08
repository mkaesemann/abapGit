# SER-SLICE-5 Phase 4 — DDLS batch-potential discovery

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_5_DDLS_DISCOVERY
STATUS=DISCOVERY_COMPLETE
```

## Trace evidence (933 DDLS objects in both traces)

```text
Normal: CL_DD_DDL_HANDLER=>GET_ALL gross=8,838,960 net=24,556 (933 hits)
Batch:  CL_DD_DDL_HANDLER=>GET_ALL gross=19,505,245 net=35,531 (933 hits)
Normal: CL_DD_DDL_HANDLER=>GET      gross=4,793,930 net=48,065 (933 hits)
Batch:  CL_DD_DDL_HANDLER=>GET      gross=6,250,211 net=70,214 (933 hits)
Normal: ZCL_ABAPGIT_OBJECT_DDLS->IF_DD_DDL_HANDLER~READ gross=8,218,416 net=30,380 (877 hits)
Batch:  ZCL_ABAPGIT_OBJECT_DDLS->IF_DD_DDL_HANDLER~READ gross=18,852,688 net=47,633 (877 hits)
Normal: RADDDLDF "DB: Fetch DDDDLSRC12B" net=1,028,282 (933 hits)
Batch:  RADDDLDF "DB: Fetch DDDDLSRC12B" net=9,342,109 (933 hits)
```

Hit counts are IDENTICAL (933/877) between Normal and Batch - confirmed via
source grep, NO `ZCL_ABAPGIT_ORTEC_SER_PREF*` code path exists for DDLS at all
today (zero `is_serial_prefetch_active`/`get_ddls_*` references anywhere in
`zcl_abapgit_object_ddls.clas.abap`). This is unbatched, un-optimized, per-
object work in BOTH runs - the adaptive-batch architecture reduces RFC round
trips (Phase 0/1 closeout) but does nothing for DDLS's own per-object DB cost
today, exactly as the mandatory family assessment (SER-SLICE-3) already
recorded (DDLS was never one of the 10 mandatory families).

**NET times are consistently 1.4x-9x HIGHER in the Batch trace for the exact
same statement/hit-count** (e.g. `DB: Fetch DDDDLSRC12B`: 1.03M net Normal vs
9.34M net Batch). This is NOT explained by anything in the ORTEC source (no
DDLS provider exists to blame) - the most plausible explanation is DB buffer/
lock contention from 705 concurrent-ish batch RFC worker sessions each hitting
the same shared `DDDDLSRC*`/`CL_DD_DDL_HANDLER`-cached tables inside a
comparatively tight dispatch window, versus the Normal run's 17,321 RFC starts
spread more thinly over more round trips. This is a HYPOTHESIS, not a
confirmed root cause - flagged for the focused measurement plan below rather
than asserted.

## Required decomposition

```text
active source and active-version semantics   CL_DD_DDL_HANDLER=>READ/GET/
                                              GET_ALL/GET_INDX/GET_TS - SAP's
                                              own handler, already release-
                                              normalized; NOT proposed to be
                                              bypassed (no direct-table-read
                                              equivalence proof exists)
texts and language handling                  DDDDLSRCT/DDDDLSRC12BT/DDDDLSRC09B
                                              families visible per-object;
                                              smallest, most isolated candidate
                                              scope if a provider is ever built
dependencies                                 DDLDEPENDENCY (437 hits, separate
                                              from the 933 DDLS objects - this
                                              is CL_DD_DDL_UTILITIES/
                                              ZCL_ABAPGIT_OBJECT_VIEW's own
                                              usage, NOT exclusively DDLS)
generated files/ordering                     Not investigated this slice - no
                                              output-shape risk assessment done
static/shared SAP caches                     CL_DD_DDL_HANDLER's GET_ALL/GET_TS/
                                              GET_INDX/FILL_BASEINFO_TAB calls
                                              show large GROSS but small NET,
                                              consistent with SAP's own internal
                                              buffering already absorbing most
                                              repeat-read cost - this weakens
                                              the case for a NEW ORTEC-side
                                              cache duplicating what SAP already
                                              buffers
```

## Candidate scopes

```text
D1 TEXT/LANGUAGE PROVIDER ONLY     Smallest, safest candidate if pursued -
                                   mirrors the DTEL/DOMA/MSAG i18n-text pattern
                                   exactly. NOT authorized this slice (no
                                   measured isolated benefit).
D2 DEPENDENCY/METADATA PROVIDER    DDLDEPENDENCY volume (437) is smaller than
                                   DDLS object count (933) and shared with VIEW
                                   - low expected benefit, not pursued.
D3 ACTIVE DDL SOURCE PROVIDER      Highest structural risk (must prove field-
                                   for-field equivalence with CL_DD_DDL_HANDLER
                                   across releases) - explicitly NOT authorized
                                   without a real trace proving CL_DD_DDL_
                                   HANDLER itself (not just the DB tables under
                                   it) is the bottleneck.
D4 COMBINED PROVIDER               Requires D1-D3 to each be justified first -
                                   not reached.
D5 GENERIC BATCH ONLY (SAP caches
   already dominate)               Partially supported by the GET_ALL/GET_TS/
                                   GET_INDX gross-vs-net gap above, but the
                                   NET-time inflation between Normal and Batch
                                   contradicts "already fully absorbed" -
                                   inconclusive.
D6 OPTIMIZE WORKER-LOCAL CALL
   REUSE (no source transport)     Plausible low-risk option (e.g. ensure one
                                   CL_DD_DDL_HANDLER instance/session is reused
                                   across the ~24.57 objects in one batch
                                   rather than one per object) - NOT verified
                                   against current worker source this slice.
```

## DDLS decision

```text
DDLS_DECISION=MEASURE_FIRST
```

Rationale: no DDLS provider exists today (confirmed absent from source); the
933-object cost is identical in shape between Normal and Batch (proving
batching alone does not help it); the NET-time inflation under batch load is
real but unexplained and could equally indicate a scheduling/contention issue
(Phase 6) as a provider opportunity (Phase 4). Given SLICE5-001's fix has not
yet been IT8-validated for the EIGHT families that already have a built
provider, opening a NINTH (DDLS) family before that result is known would
violate the mandatory implementation flow's own ordering (design -> review ->
implement only after prior slices are proven). The smallest focused
measurement: re-capture a SAT trace pair AFTER the SLICE5-001 fix, and check
whether `DB: Fetch DDDDLSRC12B` NET time normalizes closer to the Normal run's
value once true worker-side contention (if that is the cause) is unaffected by
the fix - this isolates "provider-shaped opportunity" from "scheduling/
contention" without any new DDIC/ABAP change.
