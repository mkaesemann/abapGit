# SER-SLICE-5 Phase 6 — tail latency, Bulk Exists, object store

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_5_TAIL_LATENCY
STATUS=ASSESSMENT_COMPLETE
```

## A. Batch tail latency / load balancing

```text
WAIT ASYNC             705 hits, gross=net=254,370,273 microsec, 66.26% of the
                        entire batch run's net time - the single largest line
                        item in the whole trace.
WAIT_FOR_RUN_COMPLETION 1 hit, gross=303,721,078 (79.12%), net=109,505 - this
                        is the WRAPPING call; its own net cost is tiny, all the
                        time is the nested WAIT ASYNC plus RFC dispatch/
                        collection below it. Not a separate additive cost.
Objects per batch       24.57 average (17,321 / 705) - no batch-size
                        distribution or per-batch elapsed-time breakdown is
                        available from the Hit List format (it aggregates
                        across all 705 batches, not per-batch) - a finer-
                        grained conclusion (specific slow batches, straggler
                        objects) is NOT derivable from these two files.
RFC dispatch itself     Rfc Z_ABAPGIT_ORTEC_SER_BATCH: 705 hits, net=19,709,700
                        (5.13%) + 5,662,655 (1.48%) across two rows (start vs.
                        completion accounting) - genuinely small relative to
                        the WAIT itself.
```

Given (a) 66% of the run is spent waiting for async workers to actually
finish real serialization work, (b) SLICE5-001 means every one of those
workers was doing MORE per-object DB work than intended (provider misses),
and (c) no per-batch/per-object elapsed breakdown exists in this trace format
to distinguish "real work" from "scheduling inefficiency" - the correct,
evidence-respecting disposition is:

```text
TAIL_LATENCY_DECISION=NO_ACTION_WAIT_REFLECTS_REAL_WORK (provisional)
```

This should be re-assessed after the SLICE5-001 IT8 retest: if the WAIT ASYNC
share drops materially once providers actually activate (less per-object DB
work inside each worker), that confirms real work was the dominant factor and
closes this item. If it does NOT drop, that would point at genuine scheduling/
worker-availability inefficiency (`SPBT_FIND_FREE_SERVER`/`ADMIN_LOAD_CHECK`/
`CHECK_SRV_STILL_ACTIVE` overhead is visible but small: <1% each) worth a
dedicated `ZCL_ABAPGIT_ORTEC_SER_PLANNER`/`ZCL_ABAPGIT_ORTEC_SER_COST` review.
Do not optimize the WAIT statement itself, per the hard boundary - only the
re-measurement above can justify touching the planner/cost estimator.

```text
MEASURE_FIRST (rebalancing/batch-size changes) - do not implement this slice.
```

## B. Bulk Exists

```text
Normal: ZCL_ABAPGIT_ORTEC_BULK_EXISTS=>EXISTS_STANDARD 3,113 hits,
        gross=17,747,263 (3.64%), net=11,504 (<0.01%)
Batch:  ZCL_ABAPGIT_ORTEC_BULK_EXISTS=>EXISTS_STANDARD 3,113 hits,
        gross=31,550,097 (8.22%), net=17,657 (<0.01%)
```

Identical hit count in both traces (3,113 - this is an independent feature,
`is_bulk_exists_active`, unrelated to the serialization-batch setting, exactly
as SER-SLICE-3's two-path audit already recorded). NET cost is negligible in
both cases; the large GROSS values are child-call aggregation (the loop over
`IT_TADIR` calling `ZCL_ABAPGIT_OBJECTS=>EXISTS` for every object type not
covered by a dedicated bulk existence check) - real elapsed work happening
inside those child calls (individually small, e.g. `DB: Open DD02L`/`DD40L`/
`DD01L`/`DD04L`, a handful of hits each), not overhead specific to Bulk
Exists itself. This is outside provider-implementation scope per the task's
own instruction; no focused design is proposed this slice.

```text
BULK_EXISTS_DECISION=NO_ACTION_THIS_SLICE (real per-object child-call work,
  not a Bulk Exists defect; unrelated feature area, do not mix with
  serializer commits)
```

## C. Object store

`ZAOG_OBJ_STORE` activity is visible and comparable in both traces (`DB: Fetch
ZAOG_OBJ_STORE`: 2,607 hits/10.1M net in Normal vs 2,611 hits/10.97M net in
Batch - consistent volume, not serializer-provider-related). Per the hard
boundary, this is treated strictly as a separate performance area and not
analyzed further or mixed into any serializer commit this slice.

## D. DDLS priority vs. FUGR/WAPA

```text
FUGR   REPAIR_EXISTING_PROVIDER_COVERAGE - fix already applied (shared with
       all 8 families), zero new implementation risk, highest immediate value
       once IT8-confirmed (358 objects/run, real native-SQL reduction
       expected).
WAPA   KEEP_SINGLETON_WITH_EVIDENCE - no implementation this slice; blocked on
       IT8 proof that the (now-fixed) batch-path replacement even activates
       correctly for a real WAPA object first.
DDLS   MEASURE_FIRST - no existing provider, 933 objects/run, real but
       unexplained NET-time inflation under batch load; lowest implementation
       readiness of the three (would be an entirely new 9th family, highest
       risk given CL_DD_DDL_HANDLER's own SAP-side buffering muddies the
       expected-benefit case).
```
