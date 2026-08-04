"! <p class="shorttext synchronized">ORTEC serialization: LPT-first batch planner</p>
"! Groups a run's work items into batches for the adaptive serialization
"! orchestrator (ZCL_ABAPGIT_ORTEC_SER_ORCH), and computes how large the
"! NEXT refill batch should be once earlier batches complete.
"!
"! RESPONSIBILITY: pure scheduling algorithm only - longest-processing-
"! time-first (LPT) initial batch assignment, and guided-self-scheduling
"! batch-size shrinkage for later refills (so the LAST few batches of a
"! run are small, reducing tail/straggler latency). WAPA object types are
"! explicitly EXCLUDED from batch eligibility, both here and in the caller
"! (see the OD-14 static-state audit, serialization_slice_2_od14_audit.md)
"! - WAPA must always be routed to the standard, unchanged, single-object
"! path.
"!
"! LIFECYCLE AND OWNERSHIP: STATELESS, PURE utility - holds no CLASS-DATA.
"! Every method receives all the data it needs as parameters and returns
"! a result; nothing is remembered between calls. Run-scoped bookkeeping
"! (which items remain, which batches are in flight) is owned entirely by
"! ZCL_ABAPGIT_ORTEC_SER_ORCH's own run-scoped static state.
"!
"! DETERMINISM: for a fixed input list and fixed cost estimates,
"! BUILD_INITIAL_BATCHES always produces the IDENTICAL batch assignment -
"! ties (equal EST_MS) are broken by the item's original TADIR input
"! order, never by hash order or any other non-reproducible criterion.
"! This determinism is a required, tested property (equal-weight ordering
"! test), not an implementation detail.
"!
"! HARD LIMITS ALWAYS WIN: ROW/BYTE limits passed into these methods are
"! never exceeded by the returned batch composition, regardless of what
"! the cost estimates say - an inaccurate estimate can change which items
"! end up in the same batch, but can never produce an over-limit batch.
CLASS zcl_abapgit_ortec_ser_planner DEFINITION
  PUBLIC
  FINAL
  CREATE PRIVATE.

  PUBLIC SECTION.

    "! Classic guided-self-scheduling shrink factor for refill batch
    "! sizing (serialization_adaptive_batch_design.md &sect;4) - larger
    "! values shrink later batches faster/more aggressively.
    CONSTANTS c_shrink_factor TYPE i VALUE 2.

    "! One object still to be dispatched, with its current cost estimate.
    TYPES: BEGIN OF ty_work_item,
             "! The object's TADIR identity (object type + name + package).
             tadir      TYPE zif_abapgit_definitions=>ty_tadir,
             "! Estimated serialization time, in milliseconds (see
             "! ZCL_ABAPGIT_ORTEC_SER_COST).
             est_ms     TYPE i,
             "! Estimated serialized output size, in bytes.
             est_bytes  TYPE i,
             "! Estimate provenance - see
             "! ZCL_ABAPGIT_ORTEC_SER_COST=>C_SOURCE_EXACT/C_SOURCE_FAMILY.
             est_source TYPE c LENGTH 1,
           END OF ty_work_item.
    TYPES tt_work_item TYPE STANDARD TABLE OF ty_work_item WITH EMPTY KEY.

    "! One planned batch: a set of work items intended for one RFC
    "! dispatch. Does not itself carry a task name or run ID - those are
    "! assigned by the orchestrator at actual dispatch time (a planned
    "! batch may still be re-split by the actual-bytes admission check,
    "! serialization_adaptive_batch_design.md &sect;5.9, before it becomes
    "! a real dispatch).
    TYPES: BEGIN OF ty_batch,
             "! Work items assigned to this planned batch, in dispatch
             "! order.
             items            TYPE tt_work_item,
             "! Sum of ITEMS' EST_MS - the planner's own load-balancing
             "! measure for this batch, not a hard limit.
             total_est_ms     TYPE i,
             "! Sum of ITEMS' EST_BYTES - an ADVISORY planning figure
             "! only; the actual-bytes admission check
             "! (serialization_adaptive_batch_design.md &sect;5.9) is the
             "! authoritative gate, not this estimate.
             total_est_bytes  TYPE i,
           END OF ty_batch.
    TYPES tt_batch TYPE STANDARD TABLE OF ty_batch WITH EMPTY KEY.

    "! Builds the initial set of batches for a run using longest-
    "! processing-time-first (LPT) assignment: items are walked in
    "! descending EST_MS order and each is added to whichever batch
    "! currently has the smallest accumulated TOTAL_EST_MS, so long as
    "! doing so would not exceed IV_ROW_LIMIT or IV_BYTE_LIMIT for that
    "! batch - otherwise that batch is closed and a new one is opened.
    "! WAPA work items must never be passed to this method - the caller
    "! is responsible for excluding them before calling (see class-level
    "! documentation).
    "! @parameter it_work_items | All work items for this run, any input
    "!   order (this method sorts internally - callers must not rely on
    "!   pre-sorting - by EST_MS descending, then by original TADIR order
    "!   for ties, to guarantee determinism regardless of caller order)
    "! @parameter iv_worker_count | Number of batches to target building
    "!   immediately (roughly this many are produced ready-to-dispatch;
    "!   remaining items form additional queued batches if the item count
    "!   or size forces more than IV_WORKER_COUNT batches)
    "! @parameter iv_row_limit | Maximum work items per batch (hard limit)
    "! @parameter iv_byte_limit | Maximum summed EST_BYTES per batch (hard,
    "!   but ADVISORY-ONLY limit - see TOTAL_EST_BYTES documentation above;
    "!   the real safety gate is the actual-bytes check, not this figure)
    "! @parameter rt_batches | The ordered batch queue - the first
    "!   IV_WORKER_COUNT entries (or fewer, if there is less work than
    "!   that) are intended for immediate dispatch; any remainder queues
    "!   for later REFILL
    CLASS-METHODS build_initial_batches
      IMPORTING
        !it_work_items   TYPE tt_work_item
        !iv_worker_count TYPE i
        !iv_row_limit    TYPE i
        !iv_byte_limit   TYPE i
      RETURNING
        VALUE(rt_batches) TYPE tt_batch.

    "! Computes how many work items the NEXT refill batch should contain,
    "! per the guided-self-scheduling shrink rule: later batches get
    "! progressively smaller as the remaining work shrinks, so the tail of
    "! a run is made of small batches rather than one large straggler.
    "! @parameter iv_remaining_items | Count of work items not yet
    "!   dispatched in any batch (queued or not-yet-planned)
    "! @parameter iv_worker_count | Number of workers/parallel dispatch
    "!   slots this run is using
    "! @parameter iv_row_limit | Hard per-batch row limit - the result
    "!   never exceeds this
    "! @parameter rv_batch_size | Target row count for the next refill
    "!   batch; always &gt;= 1 when IV_REMAINING_ITEMS &gt; 0, and never
    "!   greater than IV_ROW_LIMIT
    CLASS-METHODS compute_refill_size
      IMPORTING
        !iv_remaining_items TYPE i
        !iv_worker_count    TYPE i
        !iv_row_limit       TYPE i
      RETURNING
        VALUE(rv_batch_size) TYPE i.

  PROTECTED SECTION.
  PRIVATE SECTION.
ENDCLASS.



CLASS zcl_abapgit_ortec_ser_planner IMPLEMENTATION.

  METHOD build_initial_batches.
    " SER-SLICE-2 Phase 2: implement LPT-first assignment per
    " serialization_adaptive_batch_design.md &sect;4, with stable-sort
    " tie-breaking on original TADIR order for determinism.
  ENDMETHOD.

  METHOD compute_refill_size.
    " SER-SLICE-2 Phase 2: implement the shrink-factor formula per
    " serialization_adaptive_batch_design.md &sect;4:
    " MAX( 1, CEIL( iv_remaining_items / ( iv_worker_count *
    " c_shrink_factor ) ) ), capped by iv_row_limit.
  ENDMETHOD.

ENDCLASS.