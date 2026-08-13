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
    "! Static default cost for CDS/RAP source artifacts (DDLS/DCLS/DDLX/
    "! SRVD/SRVB/BDEF), in milliseconds. These drive the DDL handler and
    "! dependency metadata reads and are among the heaviest per-object
    "! types in trace evidence; seeded high so the LPT planner front-loads
    "! them across workers in the first dispatch wave. Owner-adjustable.
    CONSTANTS c_default_ms_cds TYPE i VALUE 120.
    "! Static default output size for CDS/RAP source artifacts, in bytes.
    CONSTANTS c_default_bytes_cds TYPE i VALUE 20000.
    "! Static default cost for enhancement artifacts (ENHS/ENHO/ENHC), in
    "! milliseconds - the enhancement framework (CL_ENH_FACTORY and the
    "! ENHSPOT* persistence) is a consistent straggler in trace evidence.
    CONSTANTS c_default_ms_enh TYPE i VALUE 120.
    "! Static default output size for enhancement artifacts, in bytes.
    CONSTANTS c_default_bytes_enh TYPE i VALUE 15000.
    "! Static default cost for aggregate DDIC objects that read the DDIC
    "! framework (TABL/VIEW/TTYP/SHLP/ENQU/NROB), in milliseconds.
    CONSTANTS c_default_ms_ddic_agg TYPE i VALUE 90.
    "! Static default output size for aggregate DDIC objects, in bytes.
    CONSTANTS c_default_bytes_ddic_agg TYPE i VALUE 10000.
    "! Static default cost for known-heavy framework serializers
    "! (BRF+/FDT0, Web Dynpro WDYN/WDYA), in milliseconds.
    CONSTANTS c_default_ms_heavy TYPE i VALUE 150.
    "! Static default output size for known-heavy framework serializers.
    CONSTANTS c_default_bytes_heavy TYPE i VALUE 20000.
    "! Static default cost for BRF+ (FDT0) serialization, in milliseconds.
    "! Measured on IT8 at ~150000 ms/object (two BRF+ objects alone were
    "! 18.6% of a 17321-object run; max single object 202 s). EWMA is
    "! run-local, so this seed governs the FIRST BRF+ object's placement on
    "! every run - it must dominate so the LPT planner dispatches BRF+ first
    "! and in its own batch, since the run can never finish faster than its
    "! slowest single object.
    CONSTANTS c_default_ms_brf TYPE i VALUE 150000.
    "! Static default output size for BRF+ (FDT0) objects, in bytes.
    CONSTANTS c_default_bytes_brf TYPE i VALUE 4000000.
    "! Static default cost for function groups (FUGR), in milliseconds -
    "! measured ~480 ms/object with individual outliers up to ~9 s. High
    "! object count means run-local EWMA self-corrects quickly after the
    "! first samples; the seed only needs to front-load FUGR ahead of the
    "! light types on a cold run.
    CONSTANTS c_default_ms_fugr TYPE i VALUE 500.
    "! Static default output size for function groups, in bytes.
    CONSTANTS c_default_bytes_fugr TYPE i VALUE 30000.

    "! One run-local cost estimate for one object type.
    TYPES BEGIN OF ty_estimate.
      "! Estimated wall-clock serialization time, in milliseconds.
      TYPES est_ms     TYPE i.
      "! Estimated serialized output size, in bytes.
      TYPES est_bytes  TYPE i.
      "! How this estimate was derived - C_SOURCE_EXACT (a real
      "! run-local sample exists) or C_SOURCE_FAMILY (static
      "! per-type-family default; no sample yet this run).
      TYPES est_source TYPE c LENGTH 1.
    TYPES END OF ty_estimate.

    "! One run-local EWMA sample, keyed by object type.
    TYPES BEGIN OF ty_ewma.
      "! abapGit object type this sample applies to (e.g. CLAS).
      TYPES obj_type  TYPE trobjtype.
      TYPES est_ms    TYPE i.
      TYPES est_bytes TYPE i.
    TYPES END OF ty_ewma.
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
    READ TABLE it_ewma WITH TABLE KEY obj_type = iv_obj_type INTO DATA(ls_ewma).
    IF sy-subrc = 0.
      rs_estimate-est_ms     = ls_ewma-est_ms.
      rs_estimate-est_bytes  = ls_ewma-est_bytes.
      rs_estimate-est_source = c_source_exact.
      RETURN.
    ENDIF.

    CASE iv_obj_type.
      WHEN 'CLAS' OR 'INTF'.
        rs_estimate-est_ms    = c_default_ms_oo.
        rs_estimate-est_bytes = c_default_bytes_oo.
      WHEN 'DTEL' OR 'DOMA'.
        rs_estimate-est_ms    = c_default_ms_ddic.
        rs_estimate-est_bytes = c_default_bytes_ddic.
      WHEN 'DDLS' OR 'DCLS' OR 'DDLX' OR 'SRVD' OR 'SRVB' OR 'BDEF'.
        rs_estimate-est_ms    = c_default_ms_cds.
        rs_estimate-est_bytes = c_default_bytes_cds.
      WHEN 'ENHS' OR 'ENHO' OR 'ENHC'.
        rs_estimate-est_ms    = c_default_ms_enh.
        rs_estimate-est_bytes = c_default_bytes_enh.
      WHEN 'TABL' OR 'VIEW' OR 'TTYP' OR 'SHLP' OR 'ENQU' OR 'NROB'.
        rs_estimate-est_ms    = c_default_ms_ddic_agg.
        rs_estimate-est_bytes = c_default_bytes_ddic_agg.
      WHEN 'FDT0'.
        rs_estimate-est_ms    = c_default_ms_brf.
        rs_estimate-est_bytes = c_default_bytes_brf.
      WHEN 'WDYN' OR 'WDYA'.
        rs_estimate-est_ms    = c_default_ms_heavy.
        rs_estimate-est_bytes = c_default_bytes_heavy.
      WHEN 'FUGR'.
        rs_estimate-est_ms    = c_default_ms_fugr.
        rs_estimate-est_bytes = c_default_bytes_fugr.
      WHEN OTHERS.
        rs_estimate-est_ms    = c_default_ms_generic.
        rs_estimate-est_bytes = c_default_bytes_generic.
    ENDCASE.
    rs_estimate-est_source = c_source_family.
  ENDMETHOD.

  METHOD update_estimate.
    DATA lv_new_ms    TYPE p LENGTH 8 DECIMALS 4.
    DATA lv_new_bytes TYPE p LENGTH 8 DECIMALS 4.

    READ TABLE ct_ewma WITH TABLE KEY obj_type = iv_obj_type ASSIGNING FIELD-SYMBOL(<ls_ewma>).
    IF sy-subrc = 0.
      lv_new_ms    = c_ewma_alpha * iv_actual_ms    + ( 1 - c_ewma_alpha ) * <ls_ewma>-est_ms.
      lv_new_bytes = c_ewma_alpha * iv_actual_bytes + ( 1 - c_ewma_alpha ) * <ls_ewma>-est_bytes.
      <ls_ewma>-est_ms    = round( val = lv_new_ms    dec = 0 ).
      <ls_ewma>-est_bytes = round( val = lv_new_bytes dec = 0 ).
    ELSE.
      " First observation of this type this run - the sample starts at
      " the observed value itself (standard EWMA cold-start).
      INSERT VALUE #( obj_type  = iv_obj_type
                       est_ms    = iv_actual_ms
                       est_bytes = iv_actual_bytes ) INTO TABLE ct_ewma.
    ENDIF.
  ENDMETHOD.

ENDCLASS.

