# Performance Scan

## Scope
- Topic: Serialization Slice 2 adaptive batch orchestration
- Slice: SER_SLICE_2_PERFORMANCE_SCAN
- Entry methods: ZCL_ABAPGIT_ORTEC_SER_ORCH=>SERIALIZE, ON_END_OF_BATCH, CHECK_TIMEOUTS, BEFORE_DISPATCH, DISPATCH_BATCH, MERGE_INTO_MT_FILES, ROUTE_TO_SEQUENTIAL_FALLBACK
- Files inspected:
  - src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap
  - src/ortec/serial/core/zcl_abapgit_ortec_ser_cost.clas.abap
  - src/ortec/serial/core/zcl_abapgit_ortec_ser_planner.clas.abap
  - src/ortec/serial/rfc/zabapgit_ortec_serial.fugr.z_abapgit_ortec_ser_batch.abap
- Expected cardinality: large repository / many objects and many RFC batches; the inspected path is intended to scale across hundreds or thousands of objects in one serialize call.

## Summary
- Verdict: FINDINGS
- Estimated SQL shape: none observed in the inspected scope; the hot path is orchestrator/RFC work rather than DB SQL.
- Estimated HTTP shape: none; the inspected path uses RFC task dispatch rather than HTTP.
- Estimated memory risk: moderate/high for large runs because per-run outcome/resolution structures can grow with object count and are only purged at run completion.

## Findings

### PS-001
- Severity: MAJOR
- File/class/method: src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap :: ZCL_ABAPGIT_ORTEC_SER_ORCH=>SERIALIZE
- Evidence: The main poll loop uses WAIT UP TO 5 SECONDS before re-checking the queue and timeout state; task completion is only observed after that fixed sleep interval.
- Hidden call chain: SERIALIZE -> WAIT UP TO 5 SECONDS -> CHECK_TIMEOUTS -> REFILL/BEFORE_DISPATCH.
- Multiplicity: once per polling cycle, which can become frequent for a large run with many dispatches.
- Scaling variable: BATCHES
- Why it matters: A batch that finishes in a few hundred milliseconds still waits until the next 5-second boundary before the orchestrator can refill capacity or notice progress, adding avoidable tail latency.
- Required review: Consider a shorter wake-up interval or an event-driven refill path tied to callback completion rather than a fixed sleep.
- Status: NEW static finding

### PS-002
- Severity: MAJOR
- File/class/method: src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap :: ZCL_ABAPGIT_ORTEC_SER_ORCH=>CHECK_TIMEOUTS
- Evidence: CHECK_TIMEOUTS scans mt_dispatch with WHERE run_id = iv_run_id AND state = ... even though mt_dispatch is a HASHED TABLE keyed by task_name, not by run_id.
- Hidden call chain: SERIALIZE -> CHECK_TIMEOUTS -> BEFORE_DISPATCH / ROUTE_TO_SEQUENTIAL_FALLBACK.
- Multiplicity: once per poll iteration and once for the timed-out state sweep.
- Scaling variable: N / BATCHES
- Why it matters: The scan is linear over the full dispatch table for each poll, so cost grows with the number of retained dispatch rows rather than with the current run's active subset.
- Required review: Maintain a run-scoped awaiting/timed-out index or queue instead of scanning the whole hashed dispatch table on each poll.
- Status: NEW static finding

### PS-003
- Severity: MAJOR
- File/class/method: src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap :: ZCL_ABAPGIT_ORTEC_SER_ORCH=>ON_END_OF_BATCH and ROUTE_TO_SEQUENTIAL_FALLBACK
- Evidence: mt_resolved grows by one row per resolved object in a run and is only purged at run completion; there is no per-run memory cap or early trim beyond the run lifecycle.
- Hidden call chain: ON_END_OF_BATCH -> INSERT INTO TABLE mt_resolved; ROUTE_TO_SEQUENTIAL_FALLBACK -> INSERT INTO TABLE mt_resolved.
- Multiplicity: one row per object processed/merged/fallback-resolved.
- Scaling variable: N
- Why it matters: For a very large repository, this retains one row per object for the life of the run, which can become material even though the design preserves correctness.
- Required review: Confirm whether this is an intentional run-sized trade-off or whether a more compact structure (for example, a per-batch dedup or earlier cleanup once a run reaches a terminal state) is needed.
- Status: NEW static finding

## Unverified paths
- The current working tree does not activate the actual-bytes admission gate in BEFORE_DISPATCH because the pre-dispatch byte extraction is still effectively zero in this slice; the scan therefore focuses on structural orchestration cost rather than the runtime byte-split logic.
- The scan is static only; the RFC worker and callback path were not executed against a live system.

## Evidence limits
- No DB SQL hot spots were found in the inspected scope; the main concerns are orchestration loop granularity, run-scoped table scans, and per-run memory growth.
- The planner and cost-estimator classes are stateless and do not show obvious algorithmic hotspots in the current working tree.
