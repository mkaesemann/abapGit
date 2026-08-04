"! <p class="shorttext synchronized">ORTEC serialization: adaptive batch orchestrator and run registry</p>
"! Entry point and static run registry for ORTEC's adaptive, cost-aware,
"! bounded multi-object serialization batching (SER-SLICE-2). Partitions
"! a run's objects into batches via ZCL_ABAPGIT_ORTEC_SER_PLANNER/
"! ZCL_ABAPGIT_ORTEC_SER_COST, dispatches them to
"! Z_ABAPGIT_ORTEC_SER_BATCH, and owns the entire callback/retry/
"! bisection/circuit-breaker/cleanup lifecycle.
"!
"! WHY STATIC OWNERSHIP IS REQUIRED (OD-13, see
"! serialization_adaptive_batch_design.md &sect;5.1a for the full
"! documented evidence trail): an aRFC callback
"! ("CALLING ... ON END OF TASK") can arrive after the ABAP statement that
"! issued the dispatch has long since returned, and possibly after the
"! object/method that issued it would otherwise have gone out of scope.
"! SAP's own documentation proves callback delivery only requires that
"! "the calling program still exists in its internal session" - CLASS-
"! DATA/CLASS-METHODS satisfy exactly this, by basic ABAP language
"! semantics, with no garbage-collection ambiguity of any kind (an earlier
"! instance-lifetime/GC-retention design was PROVEN WRONG and replaced by
"! this static design - do not reintroduce instance-lifetime assumptions).
"!
"! RUN IDENTITY AND CROSS-RUN ISOLATION: every call to SERIALIZE()
"! generates its own immutable GUID RUN_ID
"! (CL_SYSTEM_UUID=>CREATE_UUID_X16_STATIC). Because the static tables
"! below are shared across the WHOLE internal session (potentially many
"! sequential SERIALIZE() calls, e.g. multiple repositories pulled in one
"! session), EVERY row in EVERY table is keyed by RUN_ID - lookups,
"! inserts, and the sliding-window circuit breaker are all filtered by
"! RUN_ID, so one run's data or one run's outage can never affect a
"! different run's outcome. This is enforced by construction (every
"! reader/writer here must be RUN_ID-scoped), not by convention.
"!
"! WHAT MAY BE RETAINED VERSUS WHAT MUST NOT: only small, bounded, TADIR-
"! key-shaped metadata is ever retained per dispatch (task/run/batch
"! identity, object keys, state, a timestamp) - see TY_DISPATCH. Full
"! serialized payloads (the actual FILES_XSTRING content) are NEVER
"! retained here merely to wait for a late callback; a late callback for
"! an already-purged or already-resolved dispatch is drained (RECEIVEd and
"! discarded) rather than acted on. Retention is bounded by
"! MAX_ABANDONED_TASKS_PER_RUN/_PER_INTERNAL_SESSION (see
"! serialization_adaptive_batch_design.md &sect;5.1a) - once exceeded, the
"! OLDEST abandoned rows are purged first.
"!
"! LOGICAL ABANDONMENT IS NOT RFC CANCELLATION: a dispatch that exceeds
"! its wait budget becomes LOGICALLY_ABANDONED from THIS run's point of
"! view (routed to sequential fallback so the run can complete) - this
"! does NOT cancel the remote work process, does NOT reclaim its RFC
"! resource, and does NOT guarantee its callback will never arrive. A late
"! callback for a logically-abandoned dispatch is a NORMAL, EXPECTED,
"! SAFE event, handled by draining it, never by treating it as an error.
"!
"! PURGE CONDITIONS: a run's own rows are purged
"! (PURGE_RUN_STATE) once every dispatch belonging to that RUN_ID has
"! reached a terminal state (RECEIVED, RECEIVED_FAILURE, or DRAINED) -
"! any dispatch still LOGICALLY_ABANDONED at that point is deliberately
"! LEFT behind (bounded, see above) so its eventual late callback still
"! has a row to resolve against or safely miss.
"!
"! WAPA IS NEVER ELIGIBLE: no WAPA object is ever included in any batch
"! this class builds or dispatches - WAPA remains on its existing,
"! unchanged, single-object serialization path (see the OD-14 audit).
"!
"! FALLBACK: any object this class cannot safely batch (unsupported type,
"! feature disabled, ORTEC initialization failure, circuit breaker open
"! for this run) is routed to the existing standard sequential/parallel
"! path - this class never invents a new fallback mechanism.
CLASS zcl_abapgit_ortec_ser_orch DEFINITION
  PUBLIC FINAL
  CREATE PRIVATE.

  PUBLIC SECTION.
    "! Dispatch is awaiting its RFC callback.
    CONSTANTS c_state_awaiting         TYPE c LENGTH 1 VALUE 'A'.
    "! Callback received; result successfully merged.
    CONSTANTS c_state_received         TYPE c LENGTH 1 VALUE 'R'.
    "! Callback received, but the batch's own confirmed RFC-level failure
    "! (not an individual object's failure) - always leads to deterministic
    "! bisection or single-object fallback, never a retry of the identical
    "! batch.
    CONSTANTS c_state_received_failure TYPE c LENGTH 1 VALUE 'F'.
    "! Wait budget exceeded without a callback - LOGICAL ABANDONMENT, not
    "! cancellation (see class-level documentation). The work is always
    "! resubmitted/falls back; this state never blocks run completion.
    CONSTANTS c_state_timed_out        TYPE c LENGTH 1 VALUE 'T'.
    "! This run's own rows for a terminal dispatch have been purged
    "! (retained only until PURGE_RUN_STATE runs for this RUN_ID).
    CONSTANTS c_state_drained          TYPE c LENGTH 1 VALUE 'D'.

    "! One outstanding or historical RFC dispatch. Retained metadata is
    "! deliberately small and TADIR-key-shaped only - see class-level
    "! "WHAT MAY BE RETAINED" documentation. Never carries a serialized
    "! payload.
    TYPES: BEGIN OF ty_dispatch,
             "! Globally unique (for the whole internal session) task name
             "! used in "STARTING NEW TASK" / "RECEIVE RESULTS FROM
             "! FUNCTION" - formatted |SER-{run_id}-{dispatch_seq}|, always
             "! within the SAP task-ID length limit. Never reused, even
             "! for a retry of the same logical work (a retry gets a NEW
             "! task name and a NEW row).
             task_name   TYPE char40,
             "! Immutable run identity - see class-level "RUN IDENTITY"
             "! documentation. MUST be set on every insert; every reader
             "! MUST filter by it.
             run_id      TYPE sysuuid_x16,
             "! Logical grouping id, stable across an original dispatch
             "! and its retries/bisections (telemetry/correlation only -
             "! callback resolution always uses TASK_NAME, never this).
             batch_id    TYPE char32,
             "! 1 for a group's first dispatch, +1 per retry/bisection.
             attempt     TYPE i,
             "! Exact TADIR rows sent in this one dispatch.
             object_keys TYPE zif_abapgit_definitions=>ty_tadir_tt,
             "! Current lifecycle state - one of the C_STATE_* constants.
             state       TYPE c LENGTH 1,
             "! Timestamp this dispatch was issued, used to detect wait-
             "! budget expiry (logical abandonment).
             dispatch_ts TYPE timestampl,
           END OF ty_dispatch.
    "! All dispatches, current run and any not-yet-purged abandoned
    "! dispatches from earlier runs in the same internal session. Keyed
    "! for O(1) callback resolution by TASK_NAME.
    TYPES ty_dispatch_tt TYPE HASHED TABLE OF ty_dispatch WITH UNIQUE KEY task_name.

    "! Records that one object's result has already been merged or
    "! terminally logged as failed, for exactly one run - the belt-and-
    "! suspenders guard against ever merging the same object's result
    "! twice (duplicate/late callback safety).
    TYPES: BEGIN OF ty_resolved,
             run_id   TYPE sysuuid_x16,
             obj_type TYPE trobjtype,
             obj_name TYPE sobj_name,
           END OF ty_resolved.
    "! RUN_ID is part of the key so two DIFFERENT runs resolving the SAME
    "! OBJ_TYPE/OBJ_NAME (e.g. the same CLAS in two repositories in one
    "! session) can never collide.
    TYPES ty_resolved_tt TYPE HASHED TABLE OF ty_resolved WITH UNIQUE KEY run_id obj_type obj_name.

    "! One confirmed task outcome, for the per-run sliding-window circuit
    "! breaker.
    TYPES: BEGIN OF ty_outcome,
             run_id  TYPE sysuuid_x16,
             "! Monotonic sequence number, scoped to this RUN_ID only (no
             "! cross-run ordering is implied or needed).
             seq     TYPE i,
             success TYPE abap_bool,
           END OF ty_outcome.
    "! Windowed (see serialization_adaptive_batch_design.md &sect;5.8) PER
    "! RUN_ID - a systemic outage in one run can never trip or influence
    "! another run's breaker.
    TYPES ty_outcome_tt     TYPE STANDARD TABLE OF ty_outcome WITH EMPTY KEY.

    "! The set of RUN_IDs whose circuit breaker has tripped and not yet
    "! been purged. Replaces a single shared broken/not-broken flag so
    "! one run's systemic outage can never degrade a different run.
    TYPES ty_broken_runs_tt TYPE HASHED TABLE OF sysuuid_x16 WITH UNIQUE KEY table_line.

    "! Sliding-window size (confirmed task outcomes) for the per-run
    "! circuit breaker.
    CONSTANTS c_breaker_window_size         TYPE i                     VALUE 10.
    "! Minimum confirmed outcomes before the breaker ratio is evaluated at
    "! all (avoids tripping on a tiny, non-representative early sample).
    CONSTANTS c_breaker_min_sample          TYPE i                     VALUE 5.
    "! Failure ratio (of the last C_BREAKER_WINDOW_SIZE outcomes) that
    "! trips the breaker for a run.
    CONSTANTS c_breaker_failure_ratio       TYPE p LENGTH 4 DECIMALS 2 VALUE '0.70'.
    "! Per-run cap on undrained LOGICALLY_ABANDONED dispatches before the
    "! oldest are force-purged (bounded retained-metadata guarantee).
    CONSTANTS c_max_abandoned_tasks_per_run TYPE i                     VALUE 50.
    "! Session-wide cap across ALL runs' undrained abandoned dispatches.
    CONSTANTS c_max_abandoned_tasks_sess    TYPE i                     VALUE 200.
    "! Session-wide cap on distinct runs each holding >=1 undrained
    "! abandoned dispatch before the OLDEST such run is force-purged.
    CONSTANTS c_max_abandoned_runs_sess     TYPE i                     VALUE 20.

    "! Serializes a set of objects using the adaptive batch path when
    "! eligible, falling back to the existing standard sequential/parallel
    "! path for anything unsafe or unsupported. This is the ONLY entry
    "! point the standard-abapGit hook (ZCL_ABAPGIT_SERIALIZE~SERIALIZE)
    "! calls - all ORTEC planning/registry/dispatch/callback/retry/
    "! telemetry logic stays behind this one call.
    "! @parameter it_tadir              | Objects to serialize, already filtered by the
    "!   caller's own unsupported/ignored-object logic (same list shape
    "!   the standard sequential/parallel path already receives)
    "! @parameter iv_max_processes      | Parallel worker budget for this run,
    "!   as already determined by the caller's own
    "!   DETERMINE_MAX_PROCESSES (mirrored, not recomputed, here)
    "! @parameter iv_group              | RFC server group for
    "!   "DESTINATION IN GROUP", as already resolved by the caller
    "!   (mirrors the standard path's own MV_GROUP)
    "! @parameter is_i18n_params        | The caller's own resolved i18n
    "!   parameters (main language, translation languages, LXE flag,
    "!   PO-comment suppression) - unchanged, reused as-is
    "! @parameter ii_log                | The caller's own log sink for per-object
    "!   warnings/errors, reused as-is
    "! @parameter rt_files              | Serialized files for every object in
    "!   IT_TADIR, in the SAME shape the standard path returns - output
    "!   parity with the standard path is a hard design requirement, not
    "!   an implementation detail
    "! @raising   zcx_abapgit_exception | Only for a batch-level failure
    "!   that could not be resolved even by falling back to the standard
    "!   path (expected to be rare to never in practice, since the whole
    "!   design point is to fall back rather than fail)
    CLASS-METHODS serialize
      IMPORTING it_tadir         TYPE zif_abapgit_definitions=>ty_tadir_tt
                iv_max_processes TYPE i
                iv_group         TYPE rzlli_apcl             OPTIONAL
                is_i18n_params   TYPE zif_abapgit_definitions=>ty_i18n_params
                ii_log           TYPE REF TO zif_abapgit_log OPTIONAL
      RETURNING VALUE(rt_files)  TYPE zif_abapgit_definitions=>ty_files_item_tt
      RAISING   zcx_abapgit_exception.

    "! aRFC callback target for "CALLING on_end_of_batch ON END OF TASK".
    "! MUST be PUBLIC (the ABAP runtime invokes it directly) but is NOT
    "! part of this class's application-level API - no other caller should
    "! ever invoke this directly.
    "!
    "! IDEMPOTENCY AND CORRELATION: P_TASK is looked up in MT_DISPATCH by
    "! TASK_NAME. Unknown task names (already purged, or - defensively -
    "! never recognized at all) are drained via a plain, exception-safe
    "! RECEIVE and discarded, never treated as an error. A callback for a
    "! dispatch already in state C_STATE_RECEIVED/DRAINED (a duplicate or
    "! very-late callback) is likewise drained and discarded - a result is
    "! merged into RT_FILES at most once per object per run, enforced via
    "! TY_RESOLVED. RECEIVE ownership: this method issues exactly one
    "! "RECEIVE RESULTS FROM FUNCTION" per invocation and is the ONLY place
    "! in this class that does so - callers must never RECEIVE a task this
    "! class dispatched.
    "! @parameter p_task | Task name supplied by the ABAP runtime; matched
    "!   against TY_DISPATCH-TASK_NAME
    CLASS-METHODS on_end_of_batch
      IMPORTING p_task TYPE clike.

  PRIVATE SECTION.
    "! All in-flight and not-yet-purged dispatches, this session-wide.
    "! See TY_DISPATCH_TT and the class-level "WHAT MAY BE RETAINED"
    "! documentation.
    CLASS-DATA mt_dispatch      TYPE ty_dispatch_tt.
    "! Objects already resolved (merged or terminally failed), keyed by
    "! run, this session-wide. See TY_RESOLVED_TT.
    CLASS-DATA mt_resolved      TYPE ty_resolved_tt.
    "! Sliding-window confirmed task outcomes, keyed by run, this
    "! session-wide. See TY_OUTCOME_TT.
    CLASS-DATA mt_task_outcomes TYPE ty_outcome_tt.
    "! Runs whose circuit breaker has tripped and not yet been purged.
    CLASS-DATA mt_broken_runs   TYPE ty_broken_runs_tt.

    "! Dispatches one bounded batch via
    "! "CALL FUNCTION 'Z_ABAPGIT_ORTEC_SER_BATCH' STARTING NEW TASK", after
    "! the actual-bytes admission check
    "! (serialization_adaptive_batch_design.md &sect;5.9) has passed, and
    "! records the dispatch in MT_DISPATCH (state C_STATE_AWAITING) BEFORE
    "! the CALL FUNCTION statement, so a callback can never arrive before
    "! its own row exists.
    "! @parameter iv_run_id      | Owning run - stamped into the new
    "!   MT_DISPATCH row; every downstream read of that row relies on this
    "!   being correct at insert time
    "! @parameter it_object_keys | Exact TADIR rows for this one dispatch
    "! @parameter iv_attempt     | 1 for a group's first dispatch, +1 per
    "!   retry/bisection
    "! @parameter iv_batch_id    | Logical grouping id, stable across retries
    "! @raising zcx_abapgit_exception |
    CLASS-METHODS dispatch_batch
      IMPORTING iv_run_id      TYPE sysuuid_x16
                it_object_keys TYPE zif_abapgit_definitions=>ty_tadir_tt
                iv_attempt     TYPE i
                iv_batch_id    TYPE char32
      RAISING   zcx_abapgit_exception.

    "! Scans MT_DISPATCH for this run's C_STATE_AWAITING rows whose wait
    "! budget has expired and marks them C_STATE_TIMED_OUT (logical
    "! abandonment - see class-level documentation; this NEVER cancels the
    "! remote work process or reclaims its RFC resource, it only lets THIS
    "! run stop waiting and fall back).
    "! @parameter iv_run_id | Run to check; other runs' dispatches are
    "!   never touched by this call
    CLASS-METHODS check_timeouts
      IMPORTING iv_run_id TYPE sysuuid_x16.

    "! Handles a confirmed RFC-level RECEIVE failure (communication/
    "! system/resource failure - the whole call did not complete, as
    "! opposed to an individual object's own serialization failure) for
    "! one dispatch: always deterministically bisects down toward single
    "! objects before ever falling back to in-process sequential handling
    "! for an object that still fails alone.
    "! @parameter iv_run_id   | Owning run
    "! @parameter is_dispatch | The failed dispatch row
    "! @raising zcx_abapgit_exception |
    CLASS-METHODS handle_receive_failure
      IMPORTING iv_run_id   TYPE sysuuid_x16
                is_dispatch TYPE ty_dispatch
      RAISING   zcx_abapgit_exception.

    "! Serializes the given objects in-process, sequentially, exactly like
    "! the existing standard fallback path - the last-resort, always-safe
    "! path for any object this class cannot successfully batch for this
    "! run (bisected down to a single object, breaker open, or unsafe
    "! type). Marks each object resolved (MT_RESOLVED) as it completes.
    "! @parameter iv_run_id      | Owning run
    "! @parameter it_object_keys | Objects to serialize sequentially
    "! @raising zcx_abapgit_exception |
    CLASS-METHODS route_to_sequential_fallback
      IMPORTING iv_run_id      TYPE sysuuid_x16
                it_object_keys TYPE zif_abapgit_definitions=>ty_tadir_tt
      RAISING   zcx_abapgit_exception.

    "! Folds one CONFIRMED task outcome (a real success or a real,
    "! confirmed failure - never a mere timeout by itself) into this run's
    "! sliding window and trips MT_BROKEN_RUNS for IV_RUN_ID if the
    "! failure ratio threshold is met. See
    "! serialization_adaptive_batch_design.md &sect;5.8.
    "! @parameter iv_run_id  | Run this outcome belongs to
    "! @parameter iv_success | Whether the confirmed outcome was a success
    CLASS-METHODS record_task_outcome
      IMPORTING iv_run_id  TYPE sysuuid_x16
                iv_success TYPE abap_bool.

    "! Removes IV_RUN_ID's own rows from MT_DISPATCH/MT_RESOLVED/
    "! MT_TASK_OUTCOMES/MT_BROKEN_RUNS once every dispatch for that run has
    "! reached a terminal state - see class-level "PURGE CONDITIONS"
    "! documentation. A dispatch still C_STATE_TIMED_OUT is deliberately
    "! left behind (bounded) so a late callback still has a row to
    "! resolve against.
    "! @parameter iv_run_id | Run to purge
    CLASS-METHODS purge_run_state
      IMPORTING iv_run_id TYPE sysuuid_x16.

ENDCLASS.


CLASS zcl_abapgit_ortec_ser_orch IMPLEMENTATION.
  METHOD serialize.
    " SER-SLICE-2 Phase 2: implement the full run flow per
    " serialization_adaptive_batch_design.md &sect;5.0 (9-step flow) -
    " generate run_id, partition via the planner, dispatch/poll/purge.
  ENDMETHOD.

  METHOD on_end_of_batch.
    " SER-SLICE-2 Phase 2: implement callback resolution per
    " serialization_adaptive_batch_design.md &sect;5.5.
  ENDMETHOD.

  METHOD dispatch_batch.
    " SER-SLICE-2 Phase 2: implement per
    " serialization_adaptive_batch_design.md &sect;5.2 and &sect;5.9
    " (actual-bytes admission check before every dispatch).
  ENDMETHOD.

  METHOD check_timeouts.
    " SER-SLICE-2 Phase 2: implement per
    " serialization_adaptive_batch_design.md &sect;5.4.
  ENDMETHOD.

  METHOD handle_receive_failure.
    " SER-SLICE-2 Phase 2: implement per
    " serialization_adaptive_batch_design.md &sect;5.6.
  ENDMETHOD.

  METHOD route_to_sequential_fallback.
    " SER-SLICE-2 Phase 2: implement per
    " serialization_adaptive_batch_design.md &sect;5.7.
  ENDMETHOD.

  METHOD record_task_outcome.
    " SER-SLICE-2 Phase 2: implement per
    " serialization_adaptive_batch_design.md &sect;5.8 (run_id-scoped
    " sliding window and breaker).
  ENDMETHOD.

  METHOD purge_run_state.
    " SER-SLICE-2 Phase 2: implement per
    " serialization_adaptive_batch_design.md &sect;5.0 step 8.
  ENDMETHOD.
ENDCLASS.
