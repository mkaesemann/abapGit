"! <p class="shorttext synchronized">ORTEC serialization: run-local adaptive cost estimator</p>
"! Estimates per-object serialization cost (time and output size) for the
"! adaptive batch planner (ZCL_ABAPGIT_ORTEC_SER_PLANNER).
"!
"! RESPONSIBILITY: turn an object type into a cost estimate the planner
"! can use to balance batches (longest-processing-time-first ordering,
"! batch sizing). Estimates are advisory for SCHEDULING ONLY - they never
"! decide whether an object is processed, never change its output, and
"! never gate whether an actual serialized payload is dispatched (that is
"! the separate, hard, actual-bytes admission check owned by the
"! orchestrator, see serialization_adaptive_batch_design.md &sect;5.9).
"! A wrong estimate can only make scheduling less balanced; it can never
"! cause an object to be skipped, duplicated, or corrupted.
"!
"! LIFECYCLE AND OWNERSHIP: this class is a STATELESS, PURE utility - it
"! holds no CLASS-DATA of its own. The EWMA (exponentially weighted
"! moving average) sample table is owned and kept alive by the CALLER
"! (ZCL_ABAPGIT_ORTEC_SER_ORCH's own run-scoped static state, keyed by
"! RUN_ID) and passed into every call by reference. This class never
"! persists anything (SER-6 persistent statistics are explicitly deferred
"! and out of scope) and is safe to call from any session without any
"! cross-run or cross-session contamination risk, because it never
"! remembers anything between calls itself.
"!
"! NON-RESPONSIBILITIES: does not read TADIR, does not call any DDIC/RFC
"! API, does not decide batch membership (that is
"! ZCL_ABAPGIT_ORTEC_SER_PLANNER's job), does not enforce any hard limit
"! (row/byte/in-flight limits live in the orchestrator, see
"! serialization_adaptive_batch_design.md &sect;9).
CLASS zcl_abapgit_ortec_ser_cost DEFINITION
  PUBLIC
  FINAL
  CREATE PRIVATE.

  PUBLIC SECTION.

    "! Marks an estimate as a live, run-local EWMA sample for this exact
    "! object type (this run has already serialized at least one object of
    "! this type, and the estimate reflects observed, not assumed, cost).
    CONSTANTS c_source_exact TYPE c LENGTH 1 VALUE 'E'.
    "! Marks an estimate as a static, owner-adjustable per-type-family
    "! default (no object of this type has completed in this run yet).
    CONSTANTS c_source_family TYPE c LENGTH 1 VALUE 'F'.

    "! EWMA smoothing factor: weight given to the newest observation
    "! versus the existing running average. Fixed, not learned, so
    "! scheduling stays deterministic and reproducible for a given
    "! sequence of observations. 0.3 means each new sample moves the
    "! estimate 30% of the way toward the latest observed value.
    CONSTANTS c_ewma_alpha TYPE p LENGTH 4 DECIMALS 2 VALUE '0.30'.

    "! Static default cost for CLAS/INTF objects, in milliseconds, used
    "! until this run has observed at least one real sample of this type.
    "! Seeded from ser0_audit's traced average (~55ms/object); see
    "! serialization_adaptive_batch_design.md &sect;3.
    CONSTANTS c_default_ms_oo TYPE i VALUE 55.
    "! Static default output size for CLAS/INTF objects, in bytes.
    CONSTANTS c_default_bytes_oo TYPE i VALUE 15000.
    "! Static default cost for DTEL/DOMA objects, in milliseconds. No
    "! trace evidence yet exists for this family; conservative low
    "! default, precision flagged UNKNOWN pending SER-4 remeasurement.
    CONSTANTS c_default_ms_ddic TYPE i VALUE 15.
    "! Static default output size for DTEL/DOMA objects, in bytes.
    CONSTANTS c_default_bytes_ddic TYPE i VALUE 3000.
    "! Static default cost for any other (generic/no-prefetch) object
    "! type, in milliseconds. No trace evidence yet exists; mid-range
    "! default.
    CONSTANTS c_default_ms_generic TYPE i VALUE 30.
    "! Static default output size for any other object type, in bytes.
    CONSTANTS c_default_bytes_generic TYPE i VALUE 8000.

    "! One run-local cost estimate for one object type.
    TYPES: BEGIN OF ty_estimate,
             "! Estimated wall-clock serialization time, in milliseconds.
             est_ms     TYPE i,
             "! Estimated serialized output size, in bytes.
             est_bytes  TYPE i,
             "! How this estimate was derived - C_SOURCE_EXACT (a real
             "! run-local sample exists) or C_SOURCE_FAMILY (static
             "! per-type-family default; no sample yet this run).
             est_source TYPE c LENGTH 1,
           END OF ty_estimate.

    "! One run-local EWMA sample, keyed by object type.
    TYPES: BEGIN OF ty_ewma,
             "! abapGit object type this sample applies to (e.g. CLAS).
             obj_type  TYPE trobjtype,
             est_ms    TYPE i,
             est_bytes TYPE i,
           END OF ty_ewma.
    "! Run-local EWMA sample table. OWNED BY THE CALLER (the orchestrator's
    "! own run-scoped static state, one such table per active RUN_ID) -
    "! this class never stores or retains this table itself.
    TYPES ty_ewma_tt TYPE HASHED TABLE OF ty_ewma WITH UNIQUE KEY obj_type.

    "! Returns the current best cost estimate for one object type.
    "! @parameter iv_obj_type | abapGit object type to estimate (e.g. CLAS)
    "! @parameter it_ewma | This run's own EWMA sample table (caller-owned;
    "!   pass the same table every call within one run so exact samples
    "!   accumulate; pass a fresh/empty table for a new, isolated run)
    "! @parameter rs_estimate | The resulting estimate; EST_SOURCE tells
    "!   the caller whether this came from a real sample or a static
    "!   default - purely informational, both are equally usable for
    "!   scheduling
    CLASS-METHODS get_estimate
      IMPORTING
        !iv_obj_type       TYPE trobjtype
        !it_ewma           TYPE ty_ewma_tt
      RETURNING
        VALUE(rs_estimate) TYPE ty_estimate.

    "! Folds one completed object's ACTUAL observed cost into this run's
    "! EWMA sample table for its object type, creating the sample if this
    "! is the first completed object of that type this run.
    "! @parameter iv_obj_type | abapGit object type that just completed
    "! @parameter iv_actual_ms | Real elapsed serialization time observed
    "!   for the just-completed object, in milliseconds
    "! @parameter iv_actual_bytes | Real serialized output size observed
    "!   for the just-completed object, in bytes
    "! @parameter ct_ewma | This run's own EWMA sample table (caller-owned)
    "!   - updated in place; the caller must persist the CHANGED table
    "!   itself (this method does not retain anything)
    CLASS-METHODS update_estimate
      IMPORTING
        !iv_obj_type     TYPE trobjtype
        !iv_actual_ms    TYPE i
        !iv_actual_bytes TYPE i
      CHANGING
        !ct_ewma         TYPE ty_ewma_tt.

  PROTECTED SECTION.
  PRIVATE SECTION.
ENDCLASS.



CLASS zcl_abapgit_ortec_ser_cost IMPLEMENTATION.

  METHOD get_estimate.
    " SER-SLICE-2 Phase 2: implement the lookup-or-default hierarchy
    " described in serialization_adaptive_batch_design.md &sect;3.
  ENDMETHOD.

  METHOD update_estimate.
    " SER-SLICE-2 Phase 2: implement the EWMA fold-in described in
    " serialization_adaptive_batch_design.md &sect;3 (new = alpha*actual +
    " (1-alpha)*old, or a fresh sample if none exists yet for this type).
  ENDMETHOD.

ENDCLASS.
