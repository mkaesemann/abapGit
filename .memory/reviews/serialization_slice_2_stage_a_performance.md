# SER-SLICE-2 Stage A Performance Implementation Audit

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_2_STAGE_A_PERFORMANCE_REVIEW
MODE=IMPLEMENTATION_AUDIT
READ_ONLY=yes
SOURCE_SCOPE=src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap,
  src/ortec/serial/core/zcl_abapgit_ortec_ser_planner.clas.abap,
  src/ortec/serial/core/zcl_abapgit_ortec_ser_cost.clas.abap,
  src/ortec/serial/rfc/zabapgit_ortec_serial.fugr.z_abapgit_ortec_ser_batch.abap,
  src/ortec/git/zcl_abapgit_ortec_git_switch.clas.abap
EVIDENCE=current-source call-chain analysis (no live trace this pass);
  cross-checked against .memory/reviews/serialization_slice_2_performance_scan.md
  and .memory/reviews/serialization_slice_2_stage_a_adversarial.md
```

## Scale scenarios

- small (1-20 objects): not executed this pass; structurally unaffected (single-batch, single WAIT).
- medium (~5,000 mixed): not executed; estimated B≈200 batches, is_run_complete cost negligible.
- large (~40,000): not executed; estimated B≈1,600 batches → up to ~2.5M row-compares total from the
  finding below (F1) — cheap relative to RFC dispatch overhead, not a blocker.
- shared/incremental/interrupted-retry: out of scope for this source set (no persistence layer here).
Static estimates only — no ABAP Unit/ST05/SAT run performed this pass; do not read as measured.

## Findings

### F1 — MINOR
- Path: `zcl_abapgit_ortec_ser_orch=>is_run_complete`, called as the `UNTIL` predicate of
  `WAIT FOR ASYNCHRONOUS TASKS UNTIL is_run_complete(...) UP TO c_batch_rfc_timeout_s SECONDS`
  (`wait_for_run_completion`).
- Observed shape: `line_exists( mt_dispatch[ run_id = iv_run_id state = c_state_awaiting ] )` against
  `mt_dispatch`, a `HASHED TABLE ... UNIQUE KEY task_name` with no secondary key on `run_id`/`state` —
  each call is a full linear scan of the table.
- The runtime re-evaluates the `UNTIL` predicate once per task completion, so cost is O(this run's own
  dispatch/batch count) per completion → O(B²) per run, where B ≈ K / 25.
- Scaling variable: B (bounded by the current run's own object count K, not global N) — `purge_run_state`/
  `discard_run_state` fully clear a run's rows at completion, so this never grows with total repository size.
- Why it matters: negligible at K≈40,000 (B≈1,600 → ~2.5M compares, sub-second), but avoidable and would
  become material if a single `serialize()` call is ever given a very large K (the design's own
  1,000,000-object scale point, if ever passed to one call rather than split beforehand).
- Fix: add a secondary sorted/hashed key on `run_id`+`state`, or track a live `awaiting` counter on
  `ty_run_context` (mirroring `in_flight`) instead of scanning `mt_dispatch`.
- Regression test: unit test asserting `is_run_complete` call count times table size stays sub-quadratic,
  or a synthetic 1,000+ batch run with an instrumented counter.

### F2 — INFORMATIONAL (stale prior scan)
- The prior static scan's PS-001 (fixed 5-second poll) and PS-002 (`CHECK_TIMEOUTS` scanning
  `mt_dispatch` by `run_id`) do not match current Stage A source: no `CHECK_TIMEOUTS` method,
  `c_state_timed_out`/`c_state_abandoned`, `c_max_drain_wait_s`, or `c_max_retries` exist. Stage A replaced
  polling with a single event-driven `WAIT FOR ASYNCHRONOUS TASKS UNTIL <predicate>` — confirmed by grep,
  matching E3 in the Stage A adversarial review. PS-003 (`mt_resolved` grows per object, purged at run end)
  remains accurate and is an intentional, K-scoped trade-off, not a regression.
- Action: do not reuse PS-001/PS-002 as current evidence in future passes; the scan needs to be re-run
  against Stage A source.

## Specific questions answered

- **Does the fail-fast wait preserve the measured CLAS batching gain?** Yes. The wait-model change only
  alters what happens on incomplete/timeout (discard-and-raise instead of a T/X drain state) — it does not
  touch `build_initial_batches`, `c_max_batch_rows` (25), or `drain_queue` refill-on-callback. RFC dispatch
  count is still driven by batch count, not object count, so the batching win is structurally intact.
- **Does WAPA singleton batching introduce a regression beyond the intentional per-object cost?** No.
  `build_wapa_singleton_batches` produces one one-object batch per WAPA object, but those batches flow
  through the same `worker_count`-bounded dispatch/`drain_queue` path as any other batch — same RFC-per-object
  cost as today's standard parallel path for WAPA only (no worse), with no batching win applied (by design,
  since prefetch buffers aren't proven WAPA-safe). No unbounded memory or extra RFC round trips beyond
  one-per-object were found.
- **Default-off switch**: confirmed in source — `mv_serial_batch_active TYPE abap_bool VALUE abap_false`,
  the only setter is `set_serial_batch_active`.

## Related, already-tracked (not re-opened here)

AR-1-002 (Stage A adversarial review): the standard hook has no TRY/CATCH around the ORTEC call, so a
fail-fast timeout/missing-result raises visibly instead of falling back — at scale this means one timed-out
run must be retried in full rather than degrading gracefully. This is a correctness/availability finding
already owned by that review; noted here only because it changes the practical blast radius of F1/timeout
behavior at scale.

## Verdict

```text
PASS_WITH_MINOR_FINDINGS
```
