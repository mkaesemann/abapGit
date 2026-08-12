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
"! an already-purged or already-discarded dispatch is RECEIVEd and
"! discarded rather than acted on. Under the Stage A fail-fast contract,
"! run bookkeeping is retained only while the run is still active or
"! until an explicit DISCARD_RUN_STATE happens after a failed wait.
"!
"! FAIL-FAST WAIT CONTRACT (SER-SLICE-2 Stage A): SERIALIZE returns
"! successfully iff every planned dispatch reached exactly one accepted
"! terminal outcome and the complete result is valid. WAIT_FOR_RUN_
"! COMPLETION performs one bounded WAIT FOR ASYNCHRONOUS TASKS against
"! the ACTUAL completion condition. A WAIT result of 4 while incomplete
"! is treated as an internal missing-result inconsistency; a WAIT result
"! of 8 is treated as a timeout. In both cases the ENTIRE partial run
"! result is discarded (DISCARD_RUN_STATE) and SERIALIZE raises a visible
"! ZCX_ABAPGIT_EXCEPTION - never a silent partial success.
"!
"! LATE CALLBACKS AFTER A DISCARDED RUN: DISCARD_RUN_STATE removes this
"! run's MT_DISPATCH rows unconditionally, even ones still AWAITING. This
"! does NOT cancel the remote work process - a real, late ON_END_OF_BATCH
"! callback for a discarded dispatch is expected and safe: its task name
"! no longer resolves in MT_DISPATCH, so it falls into the existing
"! unknown-task RECEIVE-and-discard branch (see ON_END_OF_BATCH).
"!
"! WAPA IS BATCH-ELIGIBLE, SINGLETON ONLY: WAPA objects use the same
"! adaptive batch architecture as any other type, but are always planned
"! as their OWN one-object batch, never mixed with non-WAPA objects or
"! with each other (see SERIALIZE's partition logic).
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

    "! One outstanding or historical RFC dispatch. Retained metadata is
    "! deliberately small and TADIR-key-shaped only - see class-level
    "! "WHAT MAY BE RETAINED" documentation. Never carries a serialized
    "! payload.
    TYPES BEGIN OF ty_dispatch.
    "! Globally unique (for the whole internal session) task name
    "! used in "STARTING NEW TASK" / "RECEIVE RESULTS FROM
    "! FUNCTION" - built by NEXT_TASK_NAME from a session-wide
    "! monotonic counter (MV_NEXT_TASK_SEQ), NOT from any part of
    "! RUN_ID (a truncated-RUN_ID scheme was found, via independent
    "! adversarial audit AR-1-003, to be collision-prone: two
    "! different runs could share the same first-8-hex-chars prefix
    "! and dispatch_seq, corrupting MT_DISPATCH's UNIQUE KEY). Always
    "! within the SAP task-ID length limit. Never reused, even for a
    "! retry of the same logical work (a retry gets a NEW task name
    "! and a NEW row).
    TYPES task_name   TYPE char40.
    "! Immutable run identity - see class-level "RUN IDENTITY"
    "! documentation. MUST be set on every insert; every reader
    "! MUST filter by it.
    TYPES run_id      TYPE sysuuid_x16.
    "! Logical grouping id, stable across an original dispatch
    "! and its retries/bisections (telemetry/correlation only -
    "! callback resolution always uses TASK_NAME, never this).
    TYPES batch_id    TYPE char32.
    "! 1 for a group's first dispatch, +1 per retry/bisection.
    TYPES attempt     TYPE i.
    "! Exact TADIR rows sent in this one dispatch.
    TYPES object_keys TYPE zif_abapgit_definitions=>ty_tadir_tt.
    "! Current lifecycle state - one of the C_STATE_* constants.
    TYPES state       TYPE c LENGTH 1.
    "! Timestamp this dispatch was issued, used to measure this run's
    "! current fail-fast wait budget.
    TYPES dispatch_ts TYPE timestampl.
    TYPES END OF ty_dispatch.
    "! All dispatches currently owned by active runs, keyed for O(1)
    "! callback resolution by TASK_NAME.
    TYPES ty_dispatch_tt TYPE HASHED TABLE OF ty_dispatch WITH UNIQUE KEY task_name
            WITH NON-UNIQUE SORTED KEY run COMPONENTS run_id state.

    "! Records that one object's result has already been merged or
    "! terminally logged as failed, for exactly one run - the belt-and-
    "! suspenders guard against ever merging the same object's result
    "! twice (duplicate/late callback safety).
    TYPES BEGIN OF ty_resolved.
    TYPES run_id   TYPE sysuuid_x16.
    TYPES obj_type TYPE trobjtype.
    TYPES obj_name TYPE sobj_name.
    TYPES END OF ty_resolved.
    "! RUN_ID is part of the key so two DIFFERENT runs resolving the SAME
    "! OBJ_TYPE/OBJ_NAME (e.g. the same CLAS in two repositories in one
    "! session) can never collide.
    TYPES ty_resolved_tt TYPE HASHED TABLE OF ty_resolved WITH UNIQUE KEY run_id obj_type obj_name.

    "! One confirmed task outcome, for the per-run sliding-window circuit
    "! breaker.
    TYPES BEGIN OF ty_outcome.
    TYPES run_id  TYPE sysuuid_x16.
    "! Monotonic sequence number, scoped to this RUN_ID only (no
    "! cross-run ordering is implied or needed).
    TYPES seq     TYPE i.
    TYPES success TYPE abap_bool.
    TYPES END OF ty_outcome.
    "! Windowed (see serialization_adaptive_batch_design.md &sect;5.8) PER
    "! RUN_ID - a systemic outage in one run can never trip or influence
    "! another run's breaker.
    TYPES ty_outcome_tt     TYPE STANDARD TABLE OF ty_outcome WITH EMPTY KEY.

    "! The set of RUN_IDs whose circuit breaker has tripped and not yet
    "! been purged. Replaces a single shared broken/not-broken flag so
    "! one run's systemic outage can never degrade a different run.
    TYPES ty_broken_runs_tt TYPE HASHED TABLE OF sysuuid_x16 WITH UNIQUE KEY table_line.

    "! Per-run context shared by every method reachable from the async
    "! ON_END_OF_BATCH callback path (ON_END_OF_BATCH itself,
    "! HANDLE_RECEIVE_FAILURE, ROUTE_TO_SEQUENTIAL_
    "! FALLBACK, DISPATCH_BATCH/BEFORE_DISPATCH for retries). ON_END_OF_
    "! BATCH is invoked directly by the ABAP runtime and receives ONLY
    "! P_TASK - it has no access to SERIALIZE()'s own local variables
    "! (ABAP has no closures) - so everything those methods need beyond
    "! P_TASK/TASK-derived identity must be looked up here by RUN_ID.
    "! RUN_ID-keyed for the SAME cross-run isolation reason as every other
    "! table in this class. Removed entirely by PURGE_RUN_STATE.
    "!
    "! FILES carries this run's own accumulated output
    "! (serialization_adaptive_batch_design.md &sect;5.0 step 9, &sect;5.5's
    "! MERGE_INTO_MT_FILES) - the one deliberate, narrow exception to the
    "! class-level "WHAT MAY BE RETAINED" rule, since this IS the run's
    "! own real, actively-accumulating output, not speculative retention
    "! for a late callback.
    TYPES BEGIN OF ty_run_context.
    TYPES run_id         TYPE sysuuid_x16.
    TYPES files          TYPE zif_abapgit_definitions=>ty_files_item_tt.
    TYPES ii_log         TYPE REF TO zif_abapgit_log.
    TYPES iv_group       TYPE rzlli_apcl.
    TYPES is_i18n_params TYPE zif_abapgit_definitions=>ty_i18n_params.
    "! Same "objects without translation" path-pattern list as the
    "! standard path's MT_WO_TRANSLATION_PATTERNS (see AR-1-001,
    "! independent adversarial audit) - the run-level IS_I18N_PARAMS-
    "! MAIN_LANGUAGE_ONLY flag alone is NOT enough for output parity:
    "! the standard path recomputes MAIN_LANGUAGE_ONLY per object via
    "! ZCL_ABAPGIT_I18N_PARAMS=>MATCH_OBJ_PATTERNS whenever the run-
    "! level flag is FALSE and this list is non-empty. ROUTE_TO_
    "! SEQUENTIAL_FALLBACK reproduces that exact per-object check.
    TYPES wo_translation_patterns TYPE string_table.
    "! Per-run monotonically increasing dispatch counter
    "! (serialization_adaptive_batch_design.md &sect;5.1) - incremented
    "! on EVERY dispatch including retries/bisections/refills, never
    "! reused. Must be readable/writable from ON_END_OF_BATCH's retry
    "! path, hence run-scoped rather than a SERIALIZE()-local variable.
    TYPES dispatch_seq   TYPE i.
    "! This run's own EWMA sample table
    "! (ZCL_ABAPGIT_ORTEC_SER_COST=>TY_EWMA_TT) - per that class's own
    "! documentation, "kept alive by the caller... keyed by RUN_ID".
    "! Updated after every resolved object (&sect;5.5) and read by the
    "! planner when sizing refills.
    TYPES ewma           TYPE zcl_abapgit_ortec_ser_cost=>ty_ewma_tt.
    "! Parallel worker budget for this run (mirrors the caller's own
    "! DETERMINE_MAX_PROCESSES, unchanged) - bounds how many dispatches
    "! may be simultaneously AWAITING (state C_STATE_AWAITING) at once.
    TYPES worker_count   TYPE i.
    "! Count of this run's dispatches currently AWAITING a callback.
    "! Incremented by DISPATCH_BATCH, decremented by
    "! RELEASE_IN_FLIGHT_BUDGET once a dispatch reaches any terminal
    "! state - bounds concurrency at WORKER_COUNT, mirroring the
    "! standard path's own MV_FREE semantics.
    TYPES in_flight      TYPE i.
    "! Planner-produced batches not yet dispatched (queued because they
    "! exceeded WORKER_COUNT's immediately-ready slots, or produced by a
    "! later REFILL) - drained by the poll loop as IN_FLIGHT capacity
    "! frees up.
    TYPES queue          TYPE zcl_abapgit_ortec_ser_planner=>tt_batch.
    "! Monotonically increasing counter dedicated to BATCH_ID naming
    "! ("B1", "B2", ...) - distinct from DISPATCH_SEQ (which counts
    "! actual CALL FUNCTION dispatches, including retries/bisections of
    "! the SAME logical batch). Incremented once per planned batch, by
    "! both SERIALIZE's initial dispatch loop and DRAIN_QUEUE.
    TYPES batch_seq      TYPE i.
    "! O(1) terminal-outcome counters for this run. Updated exactly at
    "! the object mark sites so the wait-completion predicate never has
    "! to rescan MT_RESOLVED/MT_FAILED on every callback wake-up.
    TYPES terminal_count TYPE i.
    TYPES failed_count   TYPE i.
    "! Expected canonical object count for this run. Duplicates are
    "! intentionally collapsed by OBJECT+OBJ_NAME at run start, matching
    "! the existing duplicate-suppression model used everywhere else in
    "! this class - successful return requires the full terminal count
    "! to reach exactly this value.
    TYPES expected_count TYPE i.
    "! First failed object identity, if any terminal failure was
    "! recorded. Used only for the final visible failure summary.
    TYPES first_fail_type TYPE trobjtype.
    TYPES first_fail_name TYPE sobj_name.
    "! Progress indicator for this run - shown after each terminal object outcome.
    TYPES ii_progress TYPE REF TO zif_abapgit_progress. "saved
    TYPES END OF ty_run_context.
    TYPES ty_run_context_tt TYPE HASHED TABLE OF ty_run_context WITH UNIQUE KEY run_id.

    "! Sliding-window size (confirmed task outcomes) for the per-run
    "! circuit breaker.
    CONSTANTS c_breaker_window_size         TYPE i                     VALUE 10.
    "! Minimum confirmed outcomes before the breaker ratio is evaluated at
    "! all (avoids tripping on a tiny, non-representative early sample).
    CONSTANTS c_breaker_min_sample          TYPE i                     VALUE 5.
    "! Failure ratio (of the last C_BREAKER_WINDOW_SIZE outcomes) that
    "! trips the breaker for a run.
    CONSTANTS c_breaker_failure_ratio       TYPE p LENGTH 4 DECIMALS 2 VALUE '0.70'.

    "! Upper bound on work items per planned/dispatched batch
    "! (serialization_adaptive_batch_design.md &sect;9). Unit: TADIR rows.
    "! HARD SAFETY BOUND, never exceeded regardless of cost estimates -
    "! the planner's own per-type-adjusted effective row limit (&sect;9)
    "! can only be SMALLER than this, never larger. SER-SLICE-2 scope: the
    "! per-type-adjustment formula itself is not implemented this slice
    "! (no batch-scoped prefetch buffers exist yet, see BEFORE_DISPATCH
    "! documentation) - this constant is used directly as the row limit.
    CONSTANTS c_max_batch_rows              TYPE i VALUE 25.
    "! Estimated-size planning gate (serialization_adaptive_batch_design.
    "! md &sect;4). Unit: bytes. ADVISORY/TUNING value only - influences
    "! planning balance, never dispatch admission (see
    "! C_MAX_ACTUAL_BATCH_BYTES for the hard gate, &sect;5.9).
    CONSTANTS c_max_batch_input_bytes_est   TYPE i VALUE 8388608.
    "! HARD SAFETY BOUND on the REAL, actually-extracted provider-buffer
    "! size for one dispatch (serialization_adaptive_batch_design.md
    "! &sect;5.9), evaluated by BEFORE_DISPATCH immediately before every
    "! single CALL FUNCTION - never depends on estimate accuracy. Unit:
    "! bytes. Exceeding it (for a group of more than one object) triggers
    "! an unconditional recursive split, continuing to n=1 with no depth
    "! cap (recursion on strictly decreasing size always terminates).
    CONSTANTS c_max_actual_batch_bytes      TYPE i VALUE 12582912.
    "! HARD SAFETY BOUND (SER-SLICE-3, serialization_slice_3_provider_
    "! contract.md &sect;4) on BEFORE_DISPATCH's own recursion depth,
    "! independent of and in addition to C_MAX_ACTUAL_BATCH_BYTES itself -
    "! covers a batch of up to 4096 objects splitting all the way to
    "! singletons; a group that still has more than one object left after
    "! this many splits is routed to ROUTE_TO_SEQUENTIAL_FALLBACK instead
    "! of recursing further (see BEFORE_DISPATCH's IV_SPLIT_DEPTH
    "! parameter and SPLIT_DEPTH_AT_CAP). Previously declared but never
    "! enforced (disclosed SER-SLICE-2 DECLARED_ONLY scope) - now the real
    "! gate.
    CONSTANTS c_max_pre_dispatch_splits     TYPE i VALUE 12.
    "! Session-wide cap on total bytes across all currently in-flight
    "! dispatches (serialization_adaptive_batch_design.md &sect;9). Unit:
    "! bytes. TUNING value protecting overall RFC/memory pressure across
    "! concurrently active batches, independent of any single batch's own
    "! C_MAX_ACTUAL_BATCH_BYTES gate.
    CONSTANTS c_max_in_flight_bytes         TYPE i VALUE 104857600.
    "! Post-hoc oversized-SINGLE-OBJECT-output signal
    "! (serialization_adaptive_batch_design.md &sect;6). Unit: bytes. NOT
    "! a pre-dispatch admission gate (that is C_MAX_ACTUAL_BATCH_BYTES,
    "! &sect;5.9) - this is evaluated AFTER a result returns, purely to
    "! flag that one object's own serialized output was unusually large,
    "! for the C_OVERSIZED_THRESHOLD counter below.
    CONSTANTS c_max_object_output_bytes     TYPE i VALUE 20971520.
    "! Number of C_MAX_OBJECT_OUTPUT_BYTES-exceeding occurrences of the
    "! SAME object type, THIS run, before that type's future batches start
    "! shrinking (serialization_adaptive_batch_design.md &sect;6, AR-1-006
    "! fix). Unit: occurrences (count, not bytes). TUNING value - purely a
    "! scheduling adaptation, never a correctness gate. SER-SLICE-2 scope:
    "! declared per the approved limits table; the shrink-on-oversized
    "! adaptation itself is a planner-side refinement not implemented this
    "! slice (BUILD_INITIAL_BATCHES/COMPUTE_REFILL_SIZE do not yet consult
    "! it) - reserved for a future slice, not silently dropped.
    CONSTANTS c_oversized_threshold         TYPE i VALUE 3.
    "! Fail-fast wait budget (SER-SLICE-2 Stage A): one bounded
    "! WAIT FOR ASYNCHRONOUS TASKS call blocks until the run is ACTUALLY
    "! complete, no callback-enabled tasks remain, or this limit elapses.
    "! On timeout the entire partial run is discarded and a visible
    "! ZCX_ABAPGIT_EXCEPTION is raised - there is no retry-then-fallback
    "! on timeout. Unit: seconds.
    CONSTANTS c_batch_rfc_timeout_s         TYPE i VALUE 300.
    "! Poll cap for the pipeline wait loop. The WAIT wakes on every RFC
    "! callback anyway; this only bounds how often the stall budget is
    "! re-checked while no callback arrives. Unit: seconds.
    CONSTANTS c_poll_interval_s             TYPE i VALUE 5.

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
    "! @parameter it_wo_translation_patterns | The caller's own MT_WO_
    "!   TRANSLATION_PATTERNS (objects-without-translation path patterns,
    "!   see AR-1-001 independent adversarial audit) - objects matching a
    "!   pattern are routed to forced-sequential so their per-object
    "!   MAIN_LANGUAGE_ONLY override (computed exactly like the standard
    "!   path's own MATCH_OBJ_PATTERNS check) is never lost to a batch's
    "!   single, uniform i18n treatment
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
      IMPORTING it_tadir                   TYPE zif_abapgit_definitions=>ty_tadir_tt
                iv_max_processes           TYPE i
                iv_group                   TYPE rzlli_apcl             OPTIONAL
                is_i18n_params             TYPE zif_abapgit_definitions=>ty_i18n_params
                it_wo_translation_patterns TYPE string_table            OPTIONAL
                ii_log                     TYPE REF TO zif_abapgit_log OPTIONAL
      RETURNING VALUE(rt_files)            TYPE zif_abapgit_definitions=>ty_files_item_tt
      RAISING   zcx_abapgit_exception.

    "! aRFC callback target for "CALLING on_end_of_batch ON END OF TASK".
    "! MUST be PUBLIC (the ABAP runtime invokes it directly) but is NOT
    "! part of this class's application-level API - no other caller should
    "! ever invoke this directly.
    "!
    "! IDEMPOTENCY AND CORRELATION: P_TASK is looked up in MT_DISPATCH by
    "! TASK_NAME. Unknown task names (already purged/discarded, or -
    "! defensively - never recognized at all) are drained via a plain,
    "! exception-safe RECEIVE and discarded, never treated as an error. A
    "! callback for a dispatch already in state C_STATE_RECEIVED (a
    "! duplicate or very-late callback) is likewise a no-op - a result is
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
    "! Objects whose serialization result was ACCEPTED and whose output is
    "! valid and merged exactly once, keyed by run. This is the SUCCESS
    "! set only - failures never enter this table.
    CLASS-DATA mt_resolved      TYPE ty_resolved_tt.
    "! Objects whose processing ended in a terminal FAILURE, keyed by run.
    "! Failures are terminal, but never count as success and never allow a
    "! successful overall return.
    CLASS-DATA mt_failed        TYPE ty_resolved_tt.
    "! Sliding-window confirmed task outcomes, keyed by run, this
    "! session-wide. See TY_OUTCOME_TT.
    CLASS-DATA mt_task_outcomes TYPE ty_outcome_tt.
    "! Runs whose circuit breaker has tripped and not yet been purged.
    CLASS-DATA mt_broken_runs   TYPE ty_broken_runs_tt.
    "! Each active run's shared context (output accumulator, log sink,
    "! RFC group, i18n params) - see TY_RUN_CONTEXT_TT.
    CLASS-DATA mt_run_context   TYPE ty_run_context_tt.
    "! Session-wide monotonic counter dedicated to TASK_NAME generation
    "! (see AR-1-003, independent adversarial audit) - guarantees every
    "! task name is globally unique regardless of RUN_ID content,
    "! replacing an earlier truncated-RUN_ID-hex scheme that had a real
    "! (if low-probability) collision risk. Never reset, never reused.
    CLASS-DATA mv_next_task_seq TYPE i.
    "! ABAP Unit seam only: force DRAIN_QUEUE to raise after selecting a
    "! queued batch but before dispatching/deleting it, so callback-side
    "! queued-failure accounting can be tested deterministically.
    CLASS-DATA mv_test_raise_drain TYPE abap_bool.

    "! Session-scoped table types and static state above; helper methods
    "! below.

    "! Result of PARTITION_OBJECTS - see that method's own documentation.
    TYPES BEGIN OF ty_partition.
    TYPES forced_seq TYPE zif_abapgit_definitions=>ty_tadir_tt.
    TYPES eligible   TYPE zif_abapgit_definitions=>ty_tadir_tt.
    TYPES wapa       TYPE zif_abapgit_definitions=>ty_tadir_tt.
    TYPES END OF ty_partition.

    "! Splits IT_TADIR into the three buckets SERIALIZE needs - extracted
    "! as a pure, deterministic helper (no run context, no dispatch, no
    "! RFC) so the partitioning DECISION itself (in particular, that WAPA
    "! lands in its own bucket rather than forced-sequential or the
    "! general eligible pool) can be unit tested without a live aRFC
    "! dispatch.
    "! @parameter it_tadir                   | Objects to partition
    "! @parameter iv_max_processes           | Same meaning as SERIALIZE's
    "!   own parameter - 1 forces everything to FORCED_SEQ
    "! @parameter is_i18n_params             | Same meaning as SERIALIZE's
    "!   own parameter
    "! @parameter it_wo_translation_patterns | Same meaning as SERIALIZE's
    "!   own parameter
    "! @parameter rs_partition-forced_seq | Objects routed to
    "!   ROUTE_TO_SEQUENTIAL_FALLBACK (parallel disabled, a standard
    "!   never-parallel type, or a per-object i18n override that a
    "!   batch's single shared i18n flag cannot represent)
    "! @parameter rs_partition-eligible   | Non-WAPA objects for the
    "!   general adaptive planner (ZCL_ABAPGIT_ORTEC_SER_PLANNER)
    "! @parameter rs_partition-wapa       | WAPA objects - always
    "!   singleton-batched (see BUILD_WAPA_SINGLETON_BATCHES), never
    "!   forced-sequential-only and never mixed into RS_PARTITION-ELIGIBLE
    CLASS-METHODS partition_objects
      IMPORTING it_tadir                   TYPE zif_abapgit_definitions=>ty_tadir_tt
                iv_max_processes           TYPE i
                is_i18n_params             TYPE zif_abapgit_definitions=>ty_i18n_params
                it_wo_translation_patterns TYPE string_table OPTIONAL
      RETURNING VALUE(rs_partition)        TYPE ty_partition
      RAISING   zcx_abapgit_exception.

    "! Turns each WAPA object into its OWN one-object planned batch -
    "! WAPA objects use the same adaptive batch architecture as any other
    "! type, but only ever as singleton batches, never mixed with
    "! non-WAPA objects or with each other. Never passed to
    "! ZCL_ABAPGIT_ORTEC_SER_PLANNER (see that class's own "WAPA work
    "! items must never be passed to this method" documentation).
    "! @parameter it_wapa      | WAPA objects (typically
    "!   PARTITION_OBJECTS-WAPA)
    "! @parameter rt_batches   | One single-item batch per object in
    "!   IT_WAPA, same order
    CLASS-METHODS build_wapa_singleton_batches
      IMPORTING it_wapa           TYPE zif_abapgit_definitions=>ty_tadir_tt
      RETURNING VALUE(rt_batches) TYPE zcl_abapgit_ortec_ser_planner=>tt_batch.

    "! Count canonical objects in the input by OBJ_TYPE+OBJ_NAME. This is
    "! the run's expected object count contract - duplicate rows collapse
    "! deterministically, matching the existing per-object identity model.
    CLASS-METHODS count_expected_objects
      IMPORTING it_tadir        TYPE zif_abapgit_definitions=>ty_tadir_tt
      RETURNING VALUE(rv_count) TYPE i.

    "! Record one accepted successful terminal outcome exactly once.
    CLASS-METHODS mark_object_success
      IMPORTING iv_run_id TYPE sysuuid_x16
                is_tadir  TYPE zif_abapgit_definitions=>ty_tadir.

    "! Record one or more terminal failures exactly once. Objects already
    "! marked successful are never downgraded.
    CLASS-METHODS mark_object_failures
      IMPORTING iv_run_id      TYPE sysuuid_x16
                it_object_keys TYPE zif_abapgit_definitions=>ty_tadir_tt.

    "! Total terminal-object count for this run: SUCCESS + FAILED.
    CLASS-METHODS count_terminal_objects
      IMPORTING iv_run_id       TYPE sysuuid_x16
      RETURNING VALUE(rv_count) TYPE i.

    "! FAILED-object count for this run.
    CLASS-METHODS count_failed_objects
      IMPORTING iv_run_id       TYPE sysuuid_x16
      RETURNING VALUE(rv_count) TYPE i.

    "! Mark every currently queued, not-yet-dispatched object for this
    "! run as terminally failed, then clear the queue. Used when queue
    "! draining itself fails inside the callback path.
    CLASS-METHODS mark_queued_failures
      IMPORTING iv_run_id TYPE sysuuid_x16.

    "! Raise if the run did not end in all-successful terminal outcomes.
    CLASS-METHODS assert_successful_run
      IMPORTING iv_run_id      TYPE sysuuid_x16
                iv_wait_result TYPE i
      RAISING   zcx_abapgit_exception.

    "! Merges one resolved object's serialized files into this run's own
    "! MT_RUN_CONTEXT-FILES accumulator (serialization_adaptive_batch_
    "! design.md &sect;5.5's MERGE_INTO_MT_FILES) - assigns each file's
    "! PATH from its own TADIR entry (mirrors the standard path's own
    "! ADD_TO_RETURN, since a batch can contain objects from different
    "! paths, unlike the single-object worker).
    "! RV_MERGED (see AR-1-004, independent adversarial audit): the
    "! caller (ON_END_OF_BATCH) MUST check this and treat ABAP_FALSE as a
    "! failed merge (missing run context, or IS_RESULT-FILES_XSTRING
    "! could not be IMPORTed) - marking the object MT_RESOLVED without
    "! checking this would silently record a "successful" object with NO
    "! actual output.
    "! @parameter iv_run_id | Owning run
    "! @parameter is_tadir  | The object's own TADIR row (for PATH)
    "! @parameter is_result | One ET_RESULT row with RC = 0
    "! @parameter rv_merged | ABAP_TRUE only if the run context was found
    "!   AND the IMPORT of IS_RESULT-FILES_XSTRING succeeded
    CLASS-METHODS merge_into_mt_files
      IMPORTING iv_run_id        TYPE sysuuid_x16
                is_tadir         TYPE zif_abapgit_definitions=>ty_tadir
                is_result        TYPE zaog_ser_batch_result
      RETURNING VALUE(rv_merged) TYPE abap_bool.

    "! Set-equality check between an ET_RESULT row set and a dispatch's own
    "! OBJECT_KEYS (same OBJ_TYPE/OBJ_NAME pairs, same count) -
    "! serialization_adaptive_batch_design.md &sect;5.5's result-set
    "! integrity check (fixes the cycle-2 AR-1-002 rejection). Requires no
    "! RFC signature change (ET_RESULT already carries OBJ_TYPE/OBJ_NAME).
    "! @parameter it_result      | The callback's own ET_RESULT rows
    "! @parameter it_object_keys | The dispatch's own requested objects
    "! @parameter rv_equal       | ABAP_TRUE only if both sides contain
    "!   EXACTLY the same OBJ_TYPE/OBJ_NAME pairs
    CLASS-METHODS object_key_sets_equal
      IMPORTING it_result       TYPE zaog_ser_batch_result_tt
                it_object_keys  TYPE zif_abapgit_definitions=>ty_tadir_tt
      RETURNING VALUE(rv_equal) TYPE abap_bool.

    "! Decrements this run's MT_RUN_CONTEXT-IN_FLIGHT counter, freeing one
    "! concurrency slot (serialization_adaptive_batch_design.md &sect;5.4/
    "! &sect;5.5/&sect;5.6 all call this once a dispatch leaves state
    "! C_STATE_AWAITING for any reason - resolved or confirmed failed).
    "! Never lets the counter go below zero.
    "! @parameter iv_run_id | Owning run
    CLASS-METHODS release_in_flight_budget
      IMPORTING iv_run_id TYPE sysuuid_x16.

    "! Actual-bytes admission check and recursive splitting before every
    "! dispatch (serialization_adaptive_batch_design.md &sect;5.9) -
    "! initial batches, refills, AND every retry/bisection from
    "! HANDLE_RECEIVE_FAILURE all funnel through here, never
    "! call DISPATCH_BATCH directly. Never depends on the planner's
    "! estimates - extracts the REAL provider buffers for IT_OBJECT_KEYS
    "! and compares their actual combined size against
    "! C_MAX_ACTUAL_BATCH_BYTES; if exceeded AND more than one object
    "! remains, splits into two halves and recurses on each (no depth cap
    "! - recursion on strictly decreasing size always terminates at n=1).
    "! A true singleton whose own buffer still exceeds the limit is
    "! dispatched anyway WITH NO provider buffers attached (falls back to
    "! the worker's own standard per-object read - a safe prefetch MISS,
    "! never an oversized-payload dispatch).
    "!
    "! SER-SLICE-2 SCOPE BOUNDARY: ZCL_ABAPGIT_ORTEC_SER_PREF/_EXT/_OO
    "! only expose EXTRACT_FOR_OBJECT (one object at a time); their
    "! EXPORT/IMPORT wire format cannot be safely combined for multiple
    "! objects by simple concatenation (a later IMPORT would silently
    "! recover only the first object's data). Building a real batch-scoped
    "! extraction method on those three EXISTING classes is a genuine new
    "! capability, not a value to transcribe from the design doc, so it is
    "! NOT implemented in this slice - this method always passes INITIAL
    "! (empty) prefetch buffers to DISPATCH_BATCH, so LV_ACTUAL_BYTES is
    "! legitimately 0 and the split branch structurally exists but never
    "! triggers yet. This is SAFE (every object still gets correctly
    "! serialized via its own standard per-object read, exactly like a
    "! normal prefetch miss) but means the PERFORMANCE benefit of batch-
    "! scoped prefetching does not yet apply - a disclosed limitation, not
    "! a silent gap, flagged as a candidate follow-up slice.
    "!
    "! BREAKER GATE (see AR-1-002, independent adversarial audit): if
    "! IV_RUN_ID's circuit breaker has tripped (MT_BROKEN_RUNS), this
    "! method routes IT_OBJECT_KEYS straight to ROUTE_TO_SEQUENTIAL_
    "! FALLBACK instead of splitting/dispatching - this is the SINGLE
    "! choke point for every dispatch source (initial batches, queue
    "! drain, receive-failure bisection all funnel through here), so a
    "! tripped breaker reliably stops ALL further RFC dispatches for that
    "! run, not just new ones.
    "! @parameter iv_run_id      | Owning run
    "! @parameter it_object_keys | Candidate objects for one dispatch (may
    "!   be split into smaller dispatches by this method)
    "! @parameter iv_attempt     | Passed through unchanged to DISPATCH_BATCH
    "! @parameter iv_batch_id    | Passed through unchanged to DISPATCH_BATCH
    "! @parameter iv_split_depth | Recursion depth of THIS call, relative to
    "!   the original (non-split) dispatch attempt - 0 for the first call,
    "!   +1 per recursive split. Compared against C_MAX_PRE_DISPATCH_SPLITS
    "!   (SER-SLICE-3) via SPLIT_DEPTH_AT_CAP: once reached, an otherwise-
    "!   splittable oversized group is routed to ROUTE_TO_SEQUENTIAL_
    "!   FALLBACK instead of recursing further. External callers never set
    "!   this explicitly (default 0).
    "! @raising zcx_abapgit_exception | Propagated from DISPATCH_BATCH or
    "!   ROUTE_TO_SEQUENTIAL_FALLBACK
    CLASS-METHODS before_dispatch
      IMPORTING iv_run_id      TYPE sysuuid_x16
                it_object_keys TYPE zif_abapgit_definitions=>ty_tadir_tt
                iv_attempt     TYPE i
                iv_batch_id    TYPE char32
                iv_split_depth TYPE i DEFAULT 0
      RAISING   zcx_abapgit_exception.

    "! SER-SLICE-3 (serialization_slice_3_provider_contract.md &sect;4):
    "! pure boundary check for BEFORE_DISPATCH's recursion-depth cap,
    "! extracted as its own method purely so the exact boundary value can
    "! be unit tested deterministically without needing to manufacture a
    "! real oversized DOMA/DTEL buffer.
    "! @parameter iv_split_depth | Recursion depth to check
    "! @parameter rv_yes | ABAP_TRUE iff IV_SPLIT_DEPTH has reached or
    "!   exceeded C_MAX_PRE_DISPATCH_SPLITS
    CLASS-METHODS split_depth_at_cap
      IMPORTING iv_split_depth TYPE i
      RETURNING VALUE(rv_yes)  TYPE abap_bool.

    "! SER-SLICE-4 shared prerequisite (serialization_slice_4_shared_
    "! infrastructure.md &sect;3): overflow-safe aggregate actual-byte sum
    "! across every active provider buffer for one dispatch - extracted as
    "! its own pure method for the SAME reason SPLIT_DEPTH_AT_CAP was: the
    "! exact threshold/overflow arithmetic can be unit tested
    "! deterministically with synthetic xstrings, without needing to
    "! manufacture real oversized DOMA/DTEL/CLAS/INTF/MSAG/TABL/PROG/FUGR
    "! provider data. An INITIAL (0-byte) buffer contributes exactly 0 -
    "! never treated as payload. Uses TYPE int8 throughout so six
    "! provider buffers, each individually bounded well under 2 GB, can
    "! never wrap a TYPE i accumulator even in a pathological scenario.
    "! @parameter iv_buffer_dd | DOMA/DTEL batch buffer
    "! @parameter iv_buffer_oo_batch | CLAS/INTF batch buffer
    "! @parameter iv_buffer_msag | MSAG batch buffer
    "! @parameter iv_buffer_tabl | TABL batch buffer (SER-SLICE-4 Package A)
    "! @parameter iv_buffer_prog | PROG batch buffer (SER-SLICE-4 Package B)
    "! @parameter iv_buffer_fugr | FUGR batch buffer (SER-SLICE-4 Package C)
    "! @parameter rv_bytes | Combined byte length across every buffer
    CLASS-METHODS sum_provider_buffer_bytes
      IMPORTING iv_buffer_dd       TYPE xstring OPTIONAL
                iv_buffer_oo_batch TYPE xstring OPTIONAL
                iv_buffer_msag     TYPE xstring OPTIONAL
                iv_buffer_tabl     TYPE xstring OPTIONAL
                iv_buffer_prog     TYPE xstring OPTIONAL
                iv_buffer_fugr     TYPE xstring OPTIONAL
      RETURNING VALUE(rv_bytes)    TYPE int8.

    "! Dispatches one bounded batch via
    "! "CALL FUNCTION 'Z_ABAPGIT_ORTEC_SER_BATCH' STARTING NEW TASK", after
    "! the actual-bytes admission check
    "! (serialization_adaptive_batch_design.md &sect;5.9) has passed, and
    "! records the dispatch in MT_DISPATCH (state C_STATE_AWAITING) BEFORE
    "! the CALL FUNCTION statement, so a callback can never arrive before
    "! its own row exists. Called ONLY by BEFORE_DISPATCH - never call this
    "! directly (the admission check must always run first).
    "! @parameter iv_run_id      | Owning run - stamped into the new
    "!   MT_DISPATCH row; every downstream read of that row relies on this
    "!   being correct at insert time
    "! @parameter it_object_keys | Exact TADIR rows for this one dispatch
    "! @parameter iv_attempt     | 1 for a group's first dispatch, +1 per
    "!   retry/bisection
    "! @parameter iv_batch_id    | Logical grouping id, stable across retries
    "! @parameter iv_prefetch_buffer | ZCL_ABAPGIT_ORTEC_SER_PREF buffer for
    "!   this dispatch's objects - see BEFORE_DISPATCH's SER-SLICE-2 scope
    "!   note (currently always initial)
    "! @parameter iv_prefetch_buffer_ext | ZCL_ABAPGIT_ORTEC_SER_PREF_EXT
    "!   buffer - same scope note as IV_PREFETCH_BUFFER
    "! @parameter iv_prefetch_buffer_oo | ZCL_ABAPGIT_ORTEC_SER_PREF_OO
    "!   buffer - same scope note as IV_PREFETCH_BUFFER
    "! @parameter iv_prefetch_buffer_dd | ZCL_ABAPGIT_ORTEC_SER_PREF_EXT's
    "!   DOMA/DTEL batch envelope (SER-SLICE-3, serialization_slice_3_
    "!   provider_contract.md &sect;4) - computed once by BEFORE_DISPATCH
    "!   via EXTRACT_FOR_BATCH and threaded through unchanged.
    "! @parameter iv_prefetch_buffer_oo_batch | ZCL_ABAPGIT_ORTEC_SER_PREF_OO's
    "!   CLAS/INTF batch envelope (SER-SLICE-3 Phase 4,
    "!   serialization_slice_3_clas_intf.md) - computed once by
    "!   BEFORE_DISPATCH via EXTRACT_FOR_BATCH and threaded through
    "!   unchanged, mirroring IV_PREFETCH_BUFFER_DD exactly.
    "! @parameter iv_prefetch_buffer_msag | ZCL_ABAPGIT_ORTEC_SER_PREF's
    "!   MSAG batch envelope (SER-SLICE-3 Phase 6,
    "!   serialization_slice_3_msag.md) - computed once by BEFORE_DISPATCH
    "!   via EXTRACT_FOR_BATCH and threaded through unchanged, mirroring
    "!   IV_PREFETCH_BUFFER_DD/IV_PREFETCH_BUFFER_OO_BATCH exactly.
    "! @parameter iv_prefetch_buffer_tabl | ZCL_ABAPGIT_ORTEC_SER_PREF_EXT's
    "!   TABL batch envelope (SER-SLICE-4 Package A,
    "!   serialization_slice_4_tabl_ttyp_design.md) - computed once by
    "!   BEFORE_DISPATCH via EXTRACT_FOR_BATCH_TABL and threaded through
    "!   unchanged, mirroring IV_PREFETCH_BUFFER_DD/_OO_BATCH/_MSAG exactly.
    "! @parameter iv_prefetch_buffer_prog | ZCL_ABAPGIT_ORTEC_SER_PREF_EXT's
    "!   PROG batch envelope (SER-SLICE-4 Package B,
    "!   serialization_slice_4_prog_design.md) - computed once by
    "!   BEFORE_DISPATCH via EXTRACT_FOR_BATCH_PROG and threaded through
    "!   unchanged, mirroring IV_PREFETCH_BUFFER_DD/_OO_BATCH/_MSAG/_TABL
    "!   exactly.
    "! @parameter iv_prefetch_buffer_fugr | ZCL_ABAPGIT_ORTEC_SER_PREF_EXT's
    "!   FUGR batch envelope (SER-SLICE-4 Package C,
    "!   serialization_slice_4_fugr_design.md) - computed once by
    "!   BEFORE_DISPATCH via EXTRACT_FOR_BATCH_FUGR and threaded through
    "!   unchanged, mirroring IV_PREFETCH_BUFFER_DD/_OO_BATCH/_MSAG/_TABL/
    "!   _PROG exactly.
    "! @raising zcx_abapgit_exception | Batch-level dispatch failure (e.g.
    "!   STARTING NEW TASK could not be issued at all)
    CLASS-METHODS dispatch_batch
      IMPORTING iv_run_id                   TYPE sysuuid_x16
                it_object_keys              TYPE zif_abapgit_definitions=>ty_tadir_tt
                iv_attempt                  TYPE i
                iv_batch_id                 TYPE char32
                iv_prefetch_buffer          TYPE xstring OPTIONAL
                iv_prefetch_buffer_ext      TYPE xstring OPTIONAL
                iv_prefetch_buffer_oo       TYPE xstring OPTIONAL
                iv_prefetch_buffer_dd       TYPE xstring OPTIONAL
                iv_prefetch_buffer_oo_batch TYPE xstring OPTIONAL
                iv_prefetch_buffer_msag     TYPE xstring OPTIONAL
                iv_prefetch_buffer_tabl     TYPE xstring OPTIONAL
                iv_prefetch_buffer_prog     TYPE xstring OPTIONAL
                iv_prefetch_buffer_fugr     TYPE xstring OPTIONAL
      RAISING   zcx_abapgit_exception.

    "! Builds a globally unique TASK_NAME by incrementing MV_NEXT_TASK_
    "! SEQ (see AR-1-003, independent adversarial audit) - a pure,
    "! side-effect-free-except-for-the-counter helper, deliberately kept
    "! separate from DISPATCH_BATCH's own CALL FUNCTION so it can be unit
    "! tested (repeated calls must never return the same value) without
    "! any RFC dependency.
    "! @parameter rv_task_name | A new, never-before-returned task name,
    "!   well within the SAP task-ID length limit
    CLASS-METHODS next_task_name
      RETURNING VALUE(rv_task_name) TYPE char40.

    "! Dispatches queued planned batches (TY_RUN_CONTEXT-QUEUE) into free
    "! IN_FLIGHT capacity - extracted so both SERIALIZE's own initial
    "! dispatch wave and WAIT_FOR_RUN_COMPLETION's per-iteration drain
    "! share one implementation. A no-op if IV_RUN_ID has no context, no
    "! queued batches, or no free capacity.
    "! @parameter iv_run_id | Owning run
    CLASS-METHODS drain_queue
      IMPORTING iv_run_id TYPE sysuuid_x16
      RAISING   zcx_abapgit_exception.

    "! ABAP_TRUE iff IV_RUN_ID has nothing left to do: no queued planned
    "! batches AND no MT_DISPATCH row still C_STATE_AWAITING. This is the
    "! ONLY definition of "done" under the fail-fast contract - there is
    "! no more separate timed-out/abandoned interim state.
    "! @parameter iv_run_id    | Run to check
    "! @parameter rv_complete  | ABAP_TRUE if the run has fully resolved
    CLASS-METHODS is_run_complete
      IMPORTING iv_run_id          TYPE sysuuid_x16
      RETURNING VALUE(rv_complete) TYPE abap_bool.

    "! Pure, deterministic mapping from one "WAIT FOR ASYNCHRONOUS TASKS"
    "! outcome plus the run's own completion state to this class's
    "! fail-fast result code - isolated from the real WAIT statement so
    "! every case (0+complete, 0+incomplete, 4, 8/other) can be unit
    "! tested without needing the kernel to actually produce each code.
    "! IV_RUN_COMPLETE always wins: if the run is genuinely complete, the
    "! result is always 0, regardless of IV_WAIT_SUBRC. A WAIT result 0
    "! with IV_RUN_COMPLETE = ABAP_FALSE is treated as an internal
    "! consistency failure, not "keep waiting".
    "! @parameter iv_wait_subrc   | SY-SUBRC exactly as the preceding
    "!   WAIT FOR ASYNCHRONOUS TASKS ... UP TO ... SECONDS statement set it
    "! @parameter iv_run_complete | IS_RUN_COMPLETE( ) for the same run,
    "!   evaluated immediately after that WAIT statement returned
    "! @parameter rv_result | 0 if actually complete; 4 if not complete
    "!   and no callback-enabled tasks remain OR the WAIT returned 0 even
    "!   though the completion condition is still false; 8 for any other
    "!   non-complete outcome (wait budget elapsed)
    CLASS-METHODS interpret_wait_result
      IMPORTING iv_wait_subrc    TYPE sy-subrc
                iv_run_complete  TYPE abap_bool
      RETURNING VALUE(rv_result) TYPE i.

    "! Fail-fast completion wait (SER-SLICE-2 Stage A) - the ONLY place
    "! that issues "WAIT FOR ASYNCHRONOUS TASKS" for this run. Queue
    "! refills happen from ON_END_OF_BATCH itself; this wait therefore
    "! blocks only on the true completion condition, never a generic
    "! "progress happened" flag. The caller (SERIALIZE) discards all of
    "! this run's state on any non-zero result.
    "! @parameter iv_run_id  | Run to wait for
    "! @parameter rv_result  | 0 = complete; 4 = no callbacks remain but
    "!   incomplete; 8 = wait budget elapsed without completion/progress
    CLASS-METHODS wait_for_run_completion
      IMPORTING iv_run_id        TYPE sysuuid_x16
      RETURNING VALUE(rv_result) TYPE i
      RAISING   zcx_abapgit_exception.

    "! Handles a confirmed RFC-level RECEIVE failure (communication/
    "! system/resource failure - the whole call did not complete, as
    "! opposed to an individual object's own serialization failure) for
    "! one dispatch: always deterministically bisects down toward single
    "! objects before ever falling back to in-process sequential handling
    "! for an object that still fails alone.
    "! @parameter iv_run_id   | Owning run
    "! @parameter is_dispatch | The failed dispatch row
    "! @raising zcx_abapgit_exception | Propagated from the bisected
    "!   re-dispatch or sequential-fallback path when that also fails
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
    "! @raising zcx_abapgit_exception | Propagated unchanged from the
    "!   underlying standard per-object serialize() call
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
    "! MT_TASK_OUTCOMES/MT_BROKEN_RUNS/MT_RUN_CONTEXT once every dispatch
    "! for that run has reached a terminal state - called only from
    "! SERIALIZE's success path (WAIT_FOR_RUN_COMPLETION returned 0), at
    "! which point no C_STATE_AWAITING row can exist for this run. The
    "! guard below is a defensive no-op, not a real code path.
    "! @parameter iv_run_id | Run to purge
    CLASS-METHODS purge_run_state
      IMPORTING iv_run_id TYPE sysuuid_x16.

    "! Unconditionally discards ALL of IV_RUN_ID's bookkeeping - dispatch
    "! rows (even ones still C_STATE_AWAITING), resolved markers,
    "! task-outcome history, the broken-run marker, and the run context -
    "! used ONLY on SERIALIZE's fail-fast failure path (WAIT_FOR_RUN_
    "! COMPLETION returned non-zero), where the entire partial run result
    "! is deliberately discarded rather than returned. Unlike
    "! PURGE_RUN_STATE, this never checks for outstanding work first - a
    "! real, late ON_END_OF_BATCH callback for a dispatch discarded here
    "! is expected and safe: its task name no longer resolves in
    "! MT_DISPATCH, so it falls into the existing unknown-task
    "! RECEIVE-and-discard branch.
    "! @parameter iv_run_id | Run to discard
    CLASS-METHODS discard_run_state
      IMPORTING iv_run_id TYPE sysuuid_x16.

    "! Deliberate LOCAL COPY of ZCL_ABAPGIT_SERIALIZE's PRIVATE instance
    "! method IS_NO_PARALLEL's exact denylist logic (currently ECTC/ECTD
    "! only, see #7148). This class does NOT call the standard method -
    "! the standard hook (ZCL_ABAPGIT_SERIALIZE~SERIALIZE) must remain the
    "! smallest possible delegation, and widening a standard PRIVATE
    "! method's visibility to CLASS-PUBLIC purely so this class could
    "! reuse it was rejected as an unnecessary standard-class API change.
    "! WAPA's own exclusion from batching is unrelated and handled
    "! separately in SERIALIZE (see the OD-14 audit) - do not fold it into
    "! this method.
    "! MAINTENANCE: if ZCL_ABAPGIT_SERIALIZE=>IS_NO_PARALLEL's own denylist
    "! ever changes upstream, this copy must be reviewed and updated to
    "! match (see the parity-pinning unit test in this class's testclasses
    "! include, which fails if the two diverge for any of today's known
    "! object types).
    "! @parameter iv_object_type | TADIR object type to check
    "! @parameter rv_result      | ABAP_TRUE if the standard path would
    "!   also treat this object type as never-parallel-eligible
    CLASS-METHODS is_standard_no_parallel_type
      IMPORTING iv_object_type   TYPE tadir-object
      RETURNING VALUE(rv_result) TYPE abap_bool.

    CLASS-METHODS has_no_pending_callbacks
      IMPORTING !iv_run_id       TYPE sysuuid_x16
      RETURNING VALUE(rv_result) TYPE abap_bool.

    "! ABAP_TRUE iff this run has queued work AND a free worker slot, i.e. a
    "! dispatch can happen right now. Pipeline wake condition for
    "! WAIT_FOR_RUN_COMPLETION so a freed slot is refilled immediately.
    CLASS-METHODS has_dispatchable_capacity
      IMPORTING !iv_run_id       TYPE sysuuid_x16
      RETURNING VALUE(rv_result) TYPE abap_bool.

    "! Main-thread-only progress update. Must NEVER be called from the
    "! ON_END_OF_BATCH aRFC callback context (SAPGUI_PROGRESS_INDICATOR is
    "! a synchronous front-end RFC and is unsafe there).
    CLASS-METHODS report_progress
      IMPORTING !iv_run_id TYPE sysuuid_x16.

    "! SER-SLICE-3 parity incident fix (serialization_slice_3_dtel_doma_
    "! parity.md, H5): pure, deterministic guard extracted so the exact
    "! boundary can be unit tested without a live aRFC round trip. An
    "! ordinary requested object can never legitimately serialize to ZERO
    "! output files while the worker also reports RC = 0 - that
    "! combination is treated as suspicious and must never be silently
    "! accepted as success.
    "! @parameter is_row       | One worker result row
    "! @parameter iv_key_found | Whether IS_ROW's identity matches a
    "!   requested object key for this dispatch
    "! @parameter rv_yes       | ABAP_TRUE iff this row must be routed to
    "!   the single-object fallback instead of being trusted as-is
    CLASS-METHODS is_zero_file_success_bad
      IMPORTING is_row        TYPE zaog_ser_batch_result
                iv_key_found  TYPE abap_bool
      RETURNING VALUE(rv_yes) TYPE abap_bool.

ENDCLASS.



CLASS zcl_abapgit_ortec_ser_orch IMPLEMENTATION.


  METHOD serialize.
    DATA lv_run_id          TYPE sysuuid_x16.
    DATA ls_partition       TYPE ty_partition.
    DATA lt_work_items      TYPE zcl_abapgit_ortec_ser_planner=>tt_work_item.
    DATA lt_batches         TYPE zcl_abapgit_ortec_ser_planner=>tt_batch.
    DATA lv_ready           TYPE i.
    DATA lv_wait_result     TYPE i.
    DATA lv_use_ortec_prefetch TYPE abap_bool.
    DATA lv_expected_count  TYPE i.

    " SER-SLICE-3 parity incident fix (serialization_slice_3_dtel_doma_
    " parity.md): this entry point never called PREPARE on any of the
    " three existing prefetch classes, so EXTRACT_FOR_BATCH/EXTRACT_FOR_
    " OBJECT always operated on a permanently-empty cache - every DOMA/
    " DTEL/CLAS/INTF/MSAG/etc. object on the adaptive batch path was an
    " unconditional MISS, exactly mirroring what the standard sequential/
    " parallel path already does before its own per-object loop.
    " ORCH is the production owner of the prefetch window (see
    " ZCL_ABAPGIT_ORTEC_GIT_SWITCH=>IS_SERIAL_PREFETCH_ACTIVE doc): turn it
    " ON for the duration of this run so PREPARE()/EXTRACT_FOR_BATCH actually
    " populate the worker buffers. It was never set anywhere in the main
    " process, so it stayed FALSE, PREPARE() was skipped, the batch buffers
    " were empty, and every worker object fell back to per-object DDIC reads.
    zcl_abapgit_ortec_git_switch=>set_serial_prefetch_active( abap_true ).
    lv_use_ortec_prefetch = abap_true.
    IF lv_use_ortec_prefetch = abap_true.
      zcl_abapgit_ortec_ser_pref=>prepare(
        it_tadir    = it_tadir
        iv_language = is_i18n_params-main_language ).
      zcl_abapgit_ortec_ser_pref_ext=>prepare(
        it_tadir    = it_tadir
        iv_language = is_i18n_params-main_language ).
      zcl_abapgit_ortec_ser_pref_oo=>prepare(
        it_tadir    = it_tadir
        iv_language = is_i18n_params-main_language ).
    ENDIF.

    TRY.
        lv_run_id = cl_system_uuid=>create_uuid_x16_static( ).
      CATCH cx_uuid_error.
        " Correctness review DR-002 (serialization_slice_3_dtel_doma_
        " parity.md): no run context exists yet on this path, so there is
        " nothing for DISCARD_RUN_STATE to clean up, but the providers
        " were already PREPARE()'d above - CLEAR them here too, not only
        " on the run-established failure path below.
        IF lv_use_ortec_prefetch = abap_true.
          zcl_abapgit_ortec_ser_pref=>clear( ).
          zcl_abapgit_ortec_ser_pref_ext=>clear( ).
          zcl_abapgit_ortec_ser_pref_oo=>clear( ).
        ENDIF.
        zcl_abapgit_ortec_git_switch=>set_serial_prefetch_active( abap_false ).
        zcx_abapgit_exception=>raise( 'ORTEC batch: could not generate a run id' ).
    ENDTRY.

    lv_expected_count = count_expected_objects( it_tadir ).
    INSERT VALUE #( run_id                 = lv_run_id
                     ii_log                 = ii_log
                     iv_group               = iv_group
                     is_i18n_params         = is_i18n_params
                     wo_translation_patterns = it_wo_translation_patterns
                     worker_count           = iv_max_processes
                     ii_progress            = zcl_abapgit_progress=>get_instance( lv_expected_count )
                     expected_count         = lv_expected_count ) INTO TABLE mt_run_context.
    ASSIGN mt_run_context[ run_id = lv_run_id ] TO FIELD-SYMBOL(<ls_ctx>).

    TRY.
        ls_partition = partition_objects(
          it_tadir                   = it_tadir
          iv_max_processes           = iv_max_processes
          is_i18n_params             = is_i18n_params
          it_wo_translation_patterns = it_wo_translation_patterns ).

        route_to_sequential_fallback( iv_run_id = lv_run_id it_object_keys = ls_partition-forced_seq ).
        report_progress( lv_run_id ).

        IF <ls_ctx> IS ASSIGNED.
          LOOP AT ls_partition-eligible INTO DATA(ls_key).
            DATA(ls_estimate) = zcl_abapgit_ortec_ser_cost=>get_estimate(
              iv_obj_type = ls_key-object
              it_ewma     = <ls_ctx>-ewma ).
            APPEND VALUE #( tadir      = ls_key
                             est_ms     = ls_estimate-est_ms
                             est_bytes  = ls_estimate-est_bytes
                             est_source = ls_estimate-est_source ) TO lt_work_items.
          ENDLOOP.
        ENDIF.

        lt_batches = zcl_abapgit_ortec_ser_planner=>build_initial_batches(
          it_work_items   = lt_work_items
          iv_worker_count = iv_max_processes
          iv_row_limit    = c_max_batch_rows
          iv_byte_limit   = c_max_batch_input_bytes_est ).

        APPEND LINES OF build_wapa_singleton_batches( ls_partition-wapa ) TO lt_batches.

        LOOP AT lt_batches INTO DATA(ls_batch).
          DATA(lt_keys) = VALUE zif_abapgit_definitions=>ty_tadir_tt( FOR ls_wi IN ls_batch-items ( ls_wi-tadir ) ).
          DATA lv_batch_id TYPE char32.
          lv_batch_id = |B{ sy-tabix }|.
          IF lv_ready < iv_max_processes.
            lv_ready = lv_ready + 1.
            before_dispatch( iv_run_id      = lv_run_id
                              it_object_keys = lt_keys
                              iv_attempt     = 1
                              iv_batch_id    = lv_batch_id ).
          ELSEIF <ls_ctx> IS ASSIGNED.
            APPEND ls_batch TO <ls_ctx>-queue.
          ENDIF.
        ENDLOOP.

        IF <ls_ctx> IS ASSIGNED.
          <ls_ctx>-batch_seq = lines( lt_batches ).
        ENDIF.

        lv_wait_result = wait_for_run_completion( lv_run_id ).
        assert_successful_run( iv_run_id = lv_run_id iv_wait_result = lv_wait_result ).

        IF <ls_ctx> IS ASSIGNED.
          rt_files = <ls_ctx>-files.
          IF <ls_ctx>-ii_progress IS BOUND.
            TRY.
                <ls_ctx>-ii_progress->off( ).
              CATCH zcx_abapgit_exception ##NO_HANDLER.
            ENDTRY.
          ENDIF.
        ENDIF.
        purge_run_state( lv_run_id ).
        IF lv_use_ortec_prefetch = abap_true.
          zcl_abapgit_ortec_ser_pref=>clear( ).
          zcl_abapgit_ortec_ser_pref_ext=>clear( ).
          zcl_abapgit_ortec_ser_pref_oo=>clear( ).
        ENDIF.
        zcl_abapgit_ortec_git_switch=>set_serial_prefetch_active( abap_false ).
      CATCH zcx_abapgit_exception INTO DATA(lx_run_failure).
        ASSIGN mt_run_context[ run_id = lv_run_id ] TO <ls_ctx>.
        IF <ls_ctx> IS ASSIGNED AND <ls_ctx>-ii_progress IS BOUND.
          TRY.
              <ls_ctx>-ii_progress->off( ).
            CATCH zcx_abapgit_exception ##NO_HANDLER.
          ENDTRY.
        ENDIF.
        discard_run_state( lv_run_id ).
        IF lv_use_ortec_prefetch = abap_true.
          zcl_abapgit_ortec_ser_pref=>clear( ).
          zcl_abapgit_ortec_ser_pref_ext=>clear( ).
          zcl_abapgit_ortec_ser_pref_oo=>clear( ).
        ENDIF.
        zcl_abapgit_ortec_git_switch=>set_serial_prefetch_active( abap_false ).
        CLEAR rt_files.
        RAISE EXCEPTION lx_run_failure.
    ENDTRY.
  ENDMETHOD.


  METHOD partition_objects.
    LOOP AT it_tadir INTO DATA(ls_tadir).
      IF iv_max_processes = 1
         OR is_standard_no_parallel_type( ls_tadir-object ) = abap_true
         OR ( is_i18n_params-main_language_only = abap_false
              AND it_wo_translation_patterns IS NOT INITIAL
              AND zcl_abapgit_i18n_params=>match_obj_patterns(
                    is_tadir                   = ls_tadir
                    it_wo_translation_patterns = it_wo_translation_patterns ) = abap_true ).
        APPEND ls_tadir TO rs_partition-forced_seq.
      ELSEIF ls_tadir-object = 'WAPA'.
        APPEND ls_tadir TO rs_partition-wapa.
      ELSE.
        APPEND ls_tadir TO rs_partition-eligible.
      ENDIF.
    ENDLOOP.
  ENDMETHOD.


  METHOD build_wapa_singleton_batches.
    LOOP AT it_wapa INTO DATA(ls_wapa).
      APPEND VALUE #( items = VALUE #( ( tadir = ls_wapa ) ) ) TO rt_batches.
    ENDLOOP.
  ENDMETHOD.


  METHOD count_expected_objects.
    TYPES ty_tadir_keys TYPE HASHED TABLE OF zif_abapgit_definitions=>ty_tadir WITH UNIQUE KEY object obj_name.
    DATA lt_keys TYPE ty_tadir_keys.

    LOOP AT it_tadir INTO DATA(ls_tadir).
      INSERT ls_tadir INTO TABLE lt_keys.
    ENDLOOP.

    rv_count = lines( lt_keys ).
  ENDMETHOD.


  METHOD mark_object_success.
    ASSIGN mt_run_context[ run_id = iv_run_id ] TO FIELD-SYMBOL(<ls_ctx>).

    IF line_exists( mt_resolved[ run_id = iv_run_id obj_type = is_tadir-object obj_name = is_tadir-obj_name ] ).
      RETURN.
    ENDIF.

    IF line_exists( mt_failed[ run_id = iv_run_id obj_type = is_tadir-object obj_name = is_tadir-obj_name ] ).
      DELETE mt_failed WHERE run_id = iv_run_id
        AND obj_type = is_tadir-object
        AND obj_name = is_tadir-obj_name.
      IF <ls_ctx> IS ASSIGNED AND <ls_ctx>-failed_count > 0.
        <ls_ctx>-failed_count = <ls_ctx>-failed_count - 1.
      ENDIF.
    ELSEIF <ls_ctx> IS ASSIGNED.
      <ls_ctx>-terminal_count = <ls_ctx>-terminal_count + 1.
    ENDIF.

    INSERT VALUE #( run_id = iv_run_id obj_type = is_tadir-object obj_name = is_tadir-obj_name )
      INTO TABLE mt_resolved.
  ENDMETHOD.


  METHOD mark_object_failures.
    ASSIGN mt_run_context[ run_id = iv_run_id ] TO FIELD-SYMBOL(<ls_ctx>).

    LOOP AT it_object_keys INTO DATA(ls_key).
      IF line_exists( mt_resolved[ run_id = iv_run_id obj_type = ls_key-object obj_name = ls_key-obj_name ] )
         OR line_exists( mt_failed[ run_id = iv_run_id obj_type = ls_key-object obj_name = ls_key-obj_name ] ).
        CONTINUE.
      ENDIF.

      INSERT VALUE #( run_id = iv_run_id obj_type = ls_key-object obj_name = ls_key-obj_name )
        INTO TABLE mt_failed.

      IF <ls_ctx> IS ASSIGNED.
        <ls_ctx>-terminal_count = <ls_ctx>-terminal_count + 1.
        <ls_ctx>-failed_count = <ls_ctx>-failed_count + 1.
      ENDIF.

      IF <ls_ctx> IS ASSIGNED AND <ls_ctx>-first_fail_type IS INITIAL.
        <ls_ctx>-first_fail_type = ls_key-object.
        <ls_ctx>-first_fail_name = ls_key-obj_name.
      ENDIF.
    ENDLOOP.
  ENDMETHOD.


  METHOD count_terminal_objects.
    ASSIGN mt_run_context[ run_id = iv_run_id ] TO FIELD-SYMBOL(<ls_ctx>).
    IF <ls_ctx> IS ASSIGNED.
      rv_count = <ls_ctx>-terminal_count.
    ENDIF.
  ENDMETHOD.


  METHOD count_failed_objects.
    ASSIGN mt_run_context[ run_id = iv_run_id ] TO FIELD-SYMBOL(<ls_ctx>).
    IF <ls_ctx> IS ASSIGNED.
      rv_count = <ls_ctx>-failed_count.
    ENDIF.
  ENDMETHOD.


  METHOD mark_queued_failures.
    ASSIGN mt_run_context[ run_id = iv_run_id ] TO FIELD-SYMBOL(<ls_ctx>).
    IF NOT <ls_ctx> IS ASSIGNED.
      RETURN.
    ENDIF.

    LOOP AT <ls_ctx>-queue INTO DATA(ls_batch).
      mark_object_failures(
        iv_run_id      = iv_run_id
        it_object_keys = VALUE #( FOR ls_wi IN ls_batch-items ( ls_wi-tadir ) ) ).
    ENDLOOP.

    CLEAR <ls_ctx>-queue.
  ENDMETHOD.


  METHOD assert_successful_run.
    ASSIGN mt_run_context[ run_id = iv_run_id ] TO FIELD-SYMBOL(<ls_ctx>).
    IF NOT <ls_ctx> IS ASSIGNED.
      zcx_abapgit_exception=>raise(
        'ORTEC adaptive batch serialization did not complete: the result was incomplete and was discarded (run context missing). Retry the operation, or reduce the scope; no partial result was accepted.' ).
    ENDIF.

    DATA(lv_terminal_count) = count_terminal_objects( iv_run_id ).
    DATA(lv_failed_count) = count_failed_objects( iv_run_id ).

    IF iv_wait_result = 8.
      zcx_abapgit_exception=>raise(
        'ORTEC adaptive batch serialization did not complete: the result was incomplete and was discarded (wait limit exceeded). Retry the operation, or reduce the scope; no partial result was accepted.' ).
    ELSEIF iv_wait_result = 4 OR lv_terminal_count <> <ls_ctx>-expected_count.
      zcx_abapgit_exception=>raise(
        'ORTEC adaptive batch serialization did not complete: the result was incomplete and was discarded (missing batch result condition). Retry the operation, or reduce the scope; no partial result was accepted.' ).
    ELSEIF lv_failed_count > 0.
      zcx_abapgit_exception=>raise(
        |ORTEC adaptive batch serialization failed: { lv_failed_count } object(s) failed| &&
        |{ COND string( WHEN <ls_ctx>-first_fail_type IS NOT INITIAL
                         THEN |; first failed object { <ls_ctx>-first_fail_type } { <ls_ctx>-first_fail_name }|
                         ELSE '' ) }. The result was incomplete and discarded. Retry the operation, or reduce the scope; no partial result was accepted.| ).
    ENDIF.
  ENDMETHOD.


  METHOD on_end_of_batch.
    DATA lv_msg TYPE c LENGTH 100.
    DATA lt_discard        TYPE zaog_ser_batch_result_tt.
    DATA lt_result         TYPE zaog_ser_batch_result_tt.
    DATA lv_out_rows       TYPE i.

    ASSIGN mt_dispatch[ task_name = p_task ] TO FIELD-SYMBOL(<ls_d>).
    IF sy-subrc <> 0.
      " Defensive: unknown/already-discarded task name - still RECEIVE to
      " free the RFC resource, then discard (sect 5.5).
      RECEIVE RESULTS FROM FUNCTION 'Z_ABAPGIT_ORTEC_SER_BATCH'
        IMPORTING et_result = lt_discard
        EXCEPTIONS system_failure = 1 communication_failure = 2 OTHERS = 3.
      RETURN.
    ENDIF.

    CASE <ls_d>-state.
      WHEN c_state_awaiting.
        RECEIVE RESULTS FROM FUNCTION 'Z_ABAPGIT_ORTEC_SER_BATCH'
          IMPORTING et_result = lt_result ev_output_row_count = lv_out_rows
          EXCEPTIONS system_failure = 1 communication_failure = 2 OTHERS = 3.
        IF sy-subrc <> 0.
          <ls_d>-state = c_state_received_failure.
          release_in_flight_budget( <ls_d>-run_id ).
          record_task_outcome( iv_run_id = <ls_d>-run_id iv_success = abap_false ).
          TRY.
              handle_receive_failure( iv_run_id = <ls_d>-run_id is_dispatch = <ls_d> ).
            CATCH zcx_abapgit_exception INTO DATA(lx_receive_fail_error).
              " never let an exception escape the aRFC callback - the
              " design's own "fall back rather than fail" philosophy
              " means this should never happen; log if a sink exists.
              ASSIGN mt_run_context[ run_id = <ls_d>-run_id ] TO FIELD-SYMBOL(<ls_rf_ctx>).
              IF <ls_rf_ctx> IS ASSIGNED AND <ls_rf_ctx>-ii_log IS BOUND.
                <ls_rf_ctx>-ii_log->add_exception( lx_receive_fail_error ).
              ENDIF.
              mark_object_failures( iv_run_id = <ls_d>-run_id it_object_keys = <ls_d>-object_keys ).
          ENDTRY.
          RETURN.
        ENDIF.

        IF lv_out_rows <> lines( <ls_d>-object_keys )
           OR object_key_sets_equal( it_result = lt_result it_object_keys = <ls_d>-object_keys ) = abap_false.
          <ls_d>-state = c_state_received_failure.
          release_in_flight_budget( <ls_d>-run_id ).
          record_task_outcome( iv_run_id = <ls_d>-run_id iv_success = abap_false ).
          ASSIGN mt_run_context[ run_id = <ls_d>-run_id ] TO FIELD-SYMBOL(<ls_mismatch_ctx>).
          IF <ls_mismatch_ctx> IS ASSIGNED AND <ls_mismatch_ctx>-ii_log IS BOUND.
            <ls_mismatch_ctx>-ii_log->add_warning(
              |ORTEC batch { p_task } returned a result set that does not match | &&
              |its requested object list - treating as failed, routing to fallback| ).
          ENDIF.
          TRY.
              route_to_sequential_fallback( iv_run_id = <ls_d>-run_id it_object_keys = <ls_d>-object_keys ).
            CATCH zcx_abapgit_exception INTO DATA(lx_mismatch_error).
              IF <ls_mismatch_ctx> IS ASSIGNED AND <ls_mismatch_ctx>-ii_log IS BOUND.
                <ls_mismatch_ctx>-ii_log->add_exception( lx_mismatch_error ).
              ENDIF.
              mark_object_failures( iv_run_id = <ls_d>-run_id it_object_keys = <ls_d>-object_keys ).
          ENDTRY.
          RETURN.
        ENDIF.

        <ls_d>-state = c_state_received.
        release_in_flight_budget( <ls_d>-run_id ).
        record_task_outcome( iv_run_id = <ls_d>-run_id iv_success = abap_true ).

        LOOP AT lt_result INTO DATA(ls_row).
          IF line_exists( mt_resolved[ run_id = <ls_d>-run_id obj_type = ls_row-obj_type obj_name = ls_row-obj_name ] )
             OR line_exists( mt_failed[ run_id = <ls_d>-run_id obj_type = ls_row-obj_type obj_name = ls_row-obj_name ] ).
            CONTINUE.
          ENDIF.

          READ TABLE <ls_d>-object_keys INTO DATA(ls_tadir_row)
            WITH KEY object = ls_row-obj_type obj_name = ls_row-obj_name.
          DATA(lv_key_found) = xsdbool( sy-subrc = 0 ).

          IF is_zero_file_success_bad( is_row = ls_row iv_key_found = lv_key_found ) = abap_true.
            " SER-SLICE-3 parity incident fix (serialization_slice_3_
            " dtel_doma_parity.md, H5): an ordinary object can never
            " legitimately serialize to ZERO files while still reporting
            " RC = 0 - a worker-side per-object RETURN-with-no-exception
            " (e.g. a standard object class's own defensive "not found"
            " early exit) must never be silently accepted as success here.
            " Force the always-correct, single-object standard path to
            " re-confirm the result instead of trusting a suspicious
            " empty-but-successful row.
            ASSIGN mt_run_context[ run_id = <ls_d>-run_id ] TO FIELD-SYMBOL(<ls_zero_file_ctx>).
            IF <ls_zero_file_ctx> IS ASSIGNED AND <ls_zero_file_ctx>-ii_log IS BOUND.
              <ls_zero_file_ctx>-ii_log->add_warning(
                |ORTEC batch { p_task }: { ls_row-obj_type } { ls_row-obj_name } reported success with | &&
                |zero output files - falling back to per-object serialization| ).
            ENDIF.
            TRY.
                route_to_sequential_fallback( iv_run_id = <ls_d>-run_id it_object_keys = VALUE #( ( ls_tadir_row ) ) ).
              CATCH zcx_abapgit_exception INTO DATA(lx_zero_file_error).
                IF <ls_zero_file_ctx> IS ASSIGNED AND <ls_zero_file_ctx>-ii_log IS BOUND.
                  <ls_zero_file_ctx>-ii_log->add_exception( lx_zero_file_error ).
                ENDIF.
                mark_object_failures( iv_run_id = <ls_d>-run_id it_object_keys = VALUE #( ( ls_tadir_row ) ) ).
            ENDTRY.
            CONTINUE.
          ENDIF.

          IF ls_row-rc = 0 AND lv_key_found = abap_true.
            IF merge_into_mt_files( iv_run_id = <ls_d>-run_id is_tadir = ls_tadir_row is_result = ls_row ) = abap_false.
              " AR-1-004 (independent adversarial audit): the batch
              " itself reported success (RC = 0), but this run's own
              " local merge failed (missing run context, or a corrupted/
              " incompatible FILES_XSTRING payload) - do NOT mark this
              " object MT_RESOLVED with no actual output. Recover via the
              " always-correct, single-object standard path instead
              " (bypasses the batch RFC's EXPORT/IMPORT wire format
              " entirely). ROUTE_TO_SEQUENTIAL_FALLBACK inserts
              " MT_RESOLVED itself, so skip this row's own trailing
              " insert/EWMA update below.
              ASSIGN mt_run_context[ run_id = <ls_d>-run_id ] TO FIELD-SYMBOL(<ls_merge_fail_ctx>).
              IF <ls_merge_fail_ctx> IS ASSIGNED AND <ls_merge_fail_ctx>-ii_log IS BOUND.
                <ls_merge_fail_ctx>-ii_log->add_warning(
                  |ORTEC batch { p_task }: { ls_row-obj_type } { ls_row-obj_name } reported success but | &&
                  |its result could not be merged locally - falling back to per-object serialization| ).
              ENDIF.
              TRY.
                  route_to_sequential_fallback( iv_run_id = <ls_d>-run_id it_object_keys = VALUE #( ( ls_tadir_row ) ) ).
                CATCH zcx_abapgit_exception INTO DATA(lx_merge_fallback_error).
                  IF <ls_merge_fail_ctx> IS ASSIGNED AND <ls_merge_fail_ctx>-ii_log IS BOUND.
                    <ls_merge_fail_ctx>-ii_log->add_exception( lx_merge_fallback_error ).
                  ENDIF.
                  mark_object_failures( iv_run_id = <ls_d>-run_id it_object_keys = VALUE #( ( ls_tadir_row ) ) ).
              ENDTRY.
              CONTINUE.
            ENDIF.
            mark_object_success( iv_run_id = <ls_d>-run_id is_tadir = ls_tadir_row ).
          ELSE.
            ASSIGN mt_run_context[ run_id = <ls_d>-run_id ] TO FIELD-SYMBOL(<ls_fail_ctx>).
            IF <ls_fail_ctx> IS ASSIGNED AND <ls_fail_ctx>-ii_log IS BOUND.
              <ls_fail_ctx>-ii_log->add_error(
                |ORTEC batch: { ls_row-obj_type } { ls_row-obj_name } failed ({ ls_row-msgid } { ls_row-msgno })| ).
            ENDIF.
            IF lv_key_found = abap_true.
              mark_object_failures( iv_run_id = <ls_d>-run_id it_object_keys = VALUE #( ( ls_tadir_row ) ) ).
            ENDIF.
          ENDIF.

          IF ls_row-rc = 0.
            ASSIGN mt_run_context[ run_id = <ls_d>-run_id ] TO FIELD-SYMBOL(<ls_ewma_ctx>).
            IF <ls_ewma_ctx> IS ASSIGNED.
              zcl_abapgit_ortec_ser_cost=>update_estimate(
                EXPORTING iv_obj_type     = ls_row-obj_type
                          iv_actual_ms    = ls_row-elapsed_ms
                          iv_actual_bytes = ls_row-output_bytes
                CHANGING  ct_ewma         = <ls_ewma_ctx>-ewma ).
            ENDIF.
          ENDIF.
        ENDLOOP.
    ENDCASE.
  ENDMETHOD.


  METHOD merge_into_mt_files.
    DATA ls_serialization TYPE zif_abapgit_objects=>ty_serialization.

    ASSIGN mt_run_context[ run_id = iv_run_id ] TO FIELD-SYMBOL(<ls_ctx>).
    IF sy-subrc <> 0.
      RETURN.
    ENDIF.

    TRY.
        IMPORT data = ls_serialization FROM DATA BUFFER is_result-files_xstring.
      CATCH cx_sy_import_format_error
            cx_sy_import_mismatch_error
            cx_sy_compression_error
            cx_sy_conversion_codepage.
        RETURN.
    ENDTRY.
    IF sy-subrc <> 0.
      RETURN.
    ENDIF.

    " SER-SLICE-3 parity incident fix (serialization_slice_3_dtel_doma_
    " parity.md, AR-3-003): guard against a genuine metadata/payload
    " mismatch - IS_RESULT-OUTPUT_FILE_COUNT already passed IS_ZERO_FILE_
    " SUCCESS_BAD's check by the time this is called, but the ACTUAL
    " imported file list could still disagree (a corrupted/truncated
    " FILES_XSTRING). Never accept an empty imported file list as a
    " successful merge - the caller's own RV_MERGED = ABAP_FALSE recovery
    " (route via ROUTE_TO_SEQUENTIAL_FALLBACK) already exists for exactly
    " this shape of failure.
    IF ls_serialization-files IS INITIAL.
      RETURN.
    ENDIF.

    LOOP AT ls_serialization-files INTO DATA(ls_file).
      APPEND INITIAL LINE TO <ls_ctx>-files ASSIGNING FIELD-SYMBOL(<ls_return>).
      <ls_return>-file = ls_file.
      <ls_return>-file-path = is_tadir-path.
      <ls_return>-item = ls_serialization-item.
    ENDLOOP.

    " SER-SLICE-3 (H5/H11): a batch worker reporting RC=0 with ZERO files is not a
    " valid merge - treat it like a failed merge so the caller reroutes through
    " ROUTE_TO_SEQUENTIAL_FALLBACK instead of silently accepting an empty result as
    " success (see serialization_slice_3_dtel_doma_parity.md).
    rv_merged = boolc( lines( ls_serialization-files ) > 0 ).
  ENDMETHOD.


  METHOD object_key_sets_equal.
    IF lines( it_result ) <> lines( it_object_keys ).
      rv_equal = abap_false.
      RETURN.
    ENDIF.

    rv_equal = abap_true.
    LOOP AT it_object_keys INTO DATA(ls_key).
      IF NOT line_exists( it_result[ obj_type = ls_key-object obj_name = ls_key-obj_name ] ).
        rv_equal = abap_false.
        RETURN.
      ENDIF.
    ENDLOOP.
  ENDMETHOD.


  METHOD release_in_flight_budget.
    ASSIGN mt_run_context[ run_id = iv_run_id ] TO FIELD-SYMBOL(<ls_ctx>).
    IF sy-subrc <> 0.
      RETURN.
    ENDIF.
    IF <ls_ctx>-in_flight > 0.
      <ls_ctx>-in_flight = <ls_ctx>-in_flight - 1.
    ENDIF.
  ENDMETHOD.


  METHOD before_dispatch.
    DATA lt_half_1       TYPE zif_abapgit_definitions=>ty_tadir_tt.
    DATA lt_half_2       TYPE zif_abapgit_definitions=>ty_tadir_tt.
    DATA lv_actual_bytes TYPE int8.
    DATA lv_split_at     TYPE i.

    " AR-1-002 (independent adversarial audit): a tripped circuit breaker
    " must actually stop future dispatches for this run - this is the
    " single choke point every dispatch source funnels through (initial
    " batches, queue drain, timeout retries, receive-failure bisection),
    " so checking here covers all of them.
    IF line_exists( mt_broken_runs[ table_line = iv_run_id ] ).
      route_to_sequential_fallback( iv_run_id = iv_run_id it_object_keys = it_object_keys ).
      RETURN.
    ENDIF.

    " SER-SLICE-3 (serialization_slice_3_provider_contract.md &sect;4): the
    " real, actually-extracted DOMA/DTEL batch buffer for this dispatch's
    " objects - computed once and reused for the final DISPATCH_BATCH call
    " below (DR-003). Batches with no DOMA/DTEL objects legitimately
    " extract to an INITIAL buffer (0 bytes) with no DB access, exactly
    " like before this slice.
    DATA(lv_prefetch_buffer_dd) = zcl_abapgit_ortec_ser_pref_ext=>extract_for_batch( it_object_keys ).

    " SER-SLICE-4 shared prerequisite (serialization_slice_4_shared_
    " infrastructure.md &sect;3): restores CLAS/INTF and MSAG batch-buffer
    " computation/forwarding, which a prior owner commit silently dropped
    " from this method (DISPATCH_BATCH's signature still declared both
    " OPTIONAL parameters, but neither was ever assigned here nor forwarded
    " into the real RFC CALL FUNCTION - see serialization_slice_3_owner_
    " test_rework.md's "Separate, undocumented drift" section). Restoring
    " these two lines is a precondition for the aggregate byte-admission
    " sum below to be meaningful for CLAS/INTF/MSAG, not new scope.
    DATA(lv_prefetch_buffer_oo_batch) = zcl_abapgit_ortec_ser_pref_oo=>extract_for_batch( it_object_keys ).
    DATA(lv_prefetch_buffer_msag) = zcl_abapgit_ortec_ser_pref=>extract_for_batch( it_object_keys ).

    " SER-SLICE-4 Package A (serialization_slice_4_tabl_ttyp_design.md
    " &sect;6): the TABL batch prefetch envelope (per-extra-language DD02T
    " text + TDDAT) for this dispatch's objects - computed once and
    " reused for the final DISPATCH_BATCH call below, mirroring
    " LV_PREFETCH_BUFFER_DD/_OO_BATCH/_MSAG exactly.
    DATA(lv_prefetch_buffer_tabl) = zcl_abapgit_ortec_ser_pref_ext=>extract_for_batch_tabl( it_object_keys ).

    " SER-SLICE-4 Package B (serialization_slice_4_prog_design.md &sect;6):
    " the PROG batch prefetch envelope (per-extra-language D010TINF
    " text-pool language list) for this dispatch's objects - computed
    " once and reused for the final DISPATCH_BATCH call below, mirroring
    " LV_PREFETCH_BUFFER_DD/_OO_BATCH/_MSAG/_TABL exactly.
    DATA(lv_prefetch_buffer_prog) = zcl_abapgit_ortec_ser_pref_ext=>extract_for_batch_prog( it_object_keys ).

    " SER-SLICE-4 Package C (serialization_slice_4_fugr_design.md &sect;9):
    " the FUGR batch prefetch envelope (per-area TLIBT text, per-area
    " ENLFDIR directory, per-function TFDIR RFCSCOPE/RFCVERS) for this
    " dispatch's objects - computed once and reused for the final
    " DISPATCH_BATCH call below, mirroring LV_PREFETCH_BUFFER_DD/
    " _OO_BATCH/_MSAG/_TABL/_PROG exactly.
    DATA(lv_prefetch_buffer_fugr) = zcl_abapgit_ortec_ser_pref_ext=>extract_for_batch_fugr( it_object_keys ).

    " Overflow-safe aggregate sum (TYPE int8, not the c_max_actual_batch_
    " bytes constant's own TYPE i) - never counts an empty buffer as
    " payload (XSTRLEN of an INITIAL xstring is exactly 0, contributing
    " nothing to the sum), and never counts anything beyond ORTEC's own
    " provider envelopes (no other buffer/envelope exists on this call
    " path). SER-SLICE-4 Packages A/B/C each extend this same sum with
    " their own xstrlen(...) term as they land (TABL/PROG/FUGR) - see
    " each package's own implementation log for its exact addition.
    lv_actual_bytes = sum_provider_buffer_bytes(
      iv_buffer_dd       = lv_prefetch_buffer_dd
      iv_buffer_oo_batch = lv_prefetch_buffer_oo_batch
      iv_buffer_msag     = lv_prefetch_buffer_msag
      iv_buffer_tabl     = lv_prefetch_buffer_tabl
      iv_buffer_prog     = lv_prefetch_buffer_prog
      iv_buffer_fugr     = lv_prefetch_buffer_fugr ).

    IF lv_actual_bytes > c_max_actual_batch_bytes AND lines( it_object_keys ) > 1.
      IF split_depth_at_cap( iv_split_depth ) = abap_true.
        route_to_sequential_fallback( iv_run_id = iv_run_id it_object_keys = it_object_keys ).
        RETURN.
      ENDIF.

      lv_split_at = lines( it_object_keys ) DIV 2.
      LOOP AT it_object_keys INTO DATA(ls_key).
        IF sy-tabix <= lv_split_at.
          APPEND ls_key TO lt_half_1.
        ELSE.
          APPEND ls_key TO lt_half_2.
        ENDIF.
      ENDLOOP.
      before_dispatch( iv_run_id      = iv_run_id
                        it_object_keys = lt_half_1
                        iv_attempt     = iv_attempt
                        iv_batch_id    = iv_batch_id
                        iv_split_depth = iv_split_depth + 1 ).
      before_dispatch( iv_run_id      = iv_run_id
                        it_object_keys = lt_half_2
                        iv_attempt     = iv_attempt
                        iv_batch_id    = iv_batch_id
                        iv_split_depth = iv_split_depth + 1 ).
      RETURN.
    ENDIF.

    dispatch_batch( iv_run_id                   = iv_run_id
                     it_object_keys              = it_object_keys
                     iv_attempt                  = iv_attempt
                     iv_batch_id                 = iv_batch_id
                     iv_prefetch_buffer_dd       = lv_prefetch_buffer_dd
                     iv_prefetch_buffer_oo_batch = lv_prefetch_buffer_oo_batch
                     iv_prefetch_buffer_msag     = lv_prefetch_buffer_msag
                     iv_prefetch_buffer_tabl     = lv_prefetch_buffer_tabl
                     iv_prefetch_buffer_prog     = lv_prefetch_buffer_prog
                     iv_prefetch_buffer_fugr     = lv_prefetch_buffer_fugr ).
  ENDMETHOD.


  METHOD split_depth_at_cap.
    rv_yes = boolc( iv_split_depth >= c_max_pre_dispatch_splits ).
  ENDMETHOD.


  METHOD sum_provider_buffer_bytes.
    " Every XSTRLEN( ) result is TYPE i - ABAP's classic arithmetic type
    " inference computes a "+" chain's intermediate results from the
    " OPERAND types, never from the target variable's type, so a bare
    " `xstrlen(a) + xstrlen(b) + ...` chain would accumulate in 32-bit I
    " precision (max 2,147,483,647) and can overflow BEFORE the final
    " conversion to RV_BYTES (TYPE int8) ever happens - defeating the
    " entire purpose of an int8 accumulator. CONV int8( ... ) on every
    " term forces int8 + int8 = int8 promotion at every step, so the
    " running sum never re-enters I precision.
    rv_bytes = CONV int8( xstrlen( iv_buffer_dd ) )
             + CONV int8( xstrlen( iv_buffer_oo_batch ) )
             + CONV int8( xstrlen( iv_buffer_msag ) )
             + CONV int8( xstrlen( iv_buffer_tabl ) )
             + CONV int8( xstrlen( iv_buffer_prog ) )
             + CONV int8( xstrlen( iv_buffer_fugr ) ).
  ENDMETHOD.


  METHOD is_zero_file_success_bad.
    rv_yes = boolc( is_row-rc = 0 AND iv_key_found = abap_true AND is_row-output_file_count = 0 ).
  ENDMETHOD.


  METHOD dispatch_batch.
    DATA lv_task_name TYPE char40.
    DATA lv_now       TYPE timestampl.
    DATA lv_msg       TYPE c LENGTH 100.
    DATA lt_rfc_tadir TYPE zaog_ser_tadir_tt.
    DATA lv_retries   TYPE i.
    DATA lv_subrc     TYPE sy-subrc.

    ASSIGN mt_run_context[ run_id = iv_run_id ] TO FIELD-SYMBOL(<ls_ctx>).
    IF sy-subrc <> 0.
      RETURN.
    ENDIF.

    LOOP AT it_object_keys INTO DATA(ls_key).
      APPEND VALUE #( pgmid      = ls_key-pgmid
                       object     = ls_key-object
                       obj_name   = ls_key-obj_name
                       devclass   = ls_key-devclass
                       korrnum    = ls_key-korrnum
                       delflag    = ls_key-delflag
                       genflag    = ls_key-genflag
                       path       = ls_key-path
                       srcsystem  = ls_key-srcsystem
                       masterlang = ls_key-masterlang ) TO lt_rfc_tadir.
    ENDLOOP.

    <ls_ctx>-dispatch_seq = <ls_ctx>-dispatch_seq + 1.
    lv_task_name = next_task_name( ).

    GET TIME STAMP FIELD lv_now.

    INSERT VALUE #( task_name   = lv_task_name
                     run_id      = iv_run_id
                     batch_id    = iv_batch_id
                     attempt     = iv_attempt
                     object_keys = it_object_keys
                     state       = c_state_awaiting
                     dispatch_ts = lv_now ) INTO TABLE mt_dispatch.
    <ls_ctx>-in_flight = <ls_ctx>-in_flight + 1.

    DO.
      CALL FUNCTION 'Z_ABAPGIT_ORTEC_SER_BATCH'
        STARTING NEW TASK lv_task_name
        DESTINATION IN GROUP <ls_ctx>-iv_group
        CALLING zcl_abapgit_ortec_ser_orch=>on_end_of_batch ON END OF TASK
        EXPORTING
          iv_batch_id             = iv_batch_id
          iv_attempt              = iv_attempt
          iv_abap_language_vers   = space
          iv_language             = <ls_ctx>-is_i18n_params-main_language
          iv_path                 = ''
          iv_main_language_only   = <ls_ctx>-is_i18n_params-main_language_only
          iv_suppress_po_comments = <ls_ctx>-is_i18n_params-suppress_po_comments
          iv_use_lxe              = <ls_ctx>-is_i18n_params-use_lxe
          it_translation_langs    = <ls_ctx>-is_i18n_params-translation_languages
          it_tadir                = lt_rfc_tadir
          iv_prefetch_buffer      = iv_prefetch_buffer
          iv_prefetch_buffer_ext  = iv_prefetch_buffer_ext
          iv_prefetch_buffer_oo   = iv_prefetch_buffer_oo
          iv_prefetch_buffer_dd   = iv_prefetch_buffer_dd
          iv_prefetch_buffer_oo_batch = iv_prefetch_buffer_oo_batch
          iv_prefetch_buffer_msag = iv_prefetch_buffer_msag
          iv_prefetch_buffer_tabl = iv_prefetch_buffer_tabl
          iv_prefetch_buffer_prog = iv_prefetch_buffer_prog
          iv_prefetch_buffer_fugr = iv_prefetch_buffer_fugr
          iv_input_row_count      = lines( it_object_keys )
          iv_input_version        = 1
        EXCEPTIONS
          system_failure          = 1 MESSAGE lv_msg
          communication_failure   = 2 MESSAGE lv_msg
          resource_failure        = 3
          OTHERS                  = 4.
      lv_subrc = sy-subrc.
      IF lv_subrc = 3.
        lv_retries = lv_retries + 1.
        IF lv_retries > 5.
          EXIT.
        ENDIF.
        WAIT UP TO 1 SECONDS.
        CONTINUE.
      ENDIF.
      EXIT.
    ENDDO.

    IF lv_subrc <> 0.
      " Could not even start the task - no callback will ever arrive for
      " it. Undo the pending row and fall back rather than fail the run.
      DELETE mt_dispatch WHERE task_name = lv_task_name.
      release_in_flight_budget( iv_run_id ).
      route_to_sequential_fallback( iv_run_id = iv_run_id it_object_keys = it_object_keys ).
    ENDIF.
  ENDMETHOD.


  METHOD next_task_name.
    mv_next_task_seq = mv_next_task_seq + 1.
    rv_task_name = |SER-{ mv_next_task_seq }|.
  ENDMETHOD.


  METHOD drain_queue.
    ASSIGN mt_run_context[ run_id = iv_run_id ] TO FIELD-SYMBOL(<ls_ctx>).
    IF NOT <ls_ctx> IS ASSIGNED.
      RETURN.
    ENDIF.

    WHILE lines( <ls_ctx>-queue ) > 0 AND <ls_ctx>-in_flight < <ls_ctx>-worker_count.
      READ TABLE <ls_ctx>-queue INDEX 1 INTO DATA(ls_next_batch).
      <ls_ctx>-batch_seq = <ls_ctx>-batch_seq + 1.
      DATA(lt_next_keys) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
        FOR ls_wi IN ls_next_batch-items ( ls_wi-tadir ) ).
      IF mv_test_raise_drain = abap_true.
        CLEAR mv_test_raise_drain.
        zcx_abapgit_exception=>raise( 'ORTEC test seam: drain_queue failed before dispatch' ).
      ENDIF.
      before_dispatch( iv_run_id      = iv_run_id
                        it_object_keys = lt_next_keys
                        iv_attempt     = 1
                        iv_batch_id    = |B{ <ls_ctx>-batch_seq }| ).
      DELETE <ls_ctx>-queue INDEX 1.
    ENDWHILE.
  ENDMETHOD.


  METHOD is_run_complete.
    rv_complete = abap_true.

    ASSIGN mt_run_context[ run_id = iv_run_id ] TO FIELD-SYMBOL(<ls_ctx>).
    IF NOT <ls_ctx> IS ASSIGNED.
      rv_complete = abap_false.
      RETURN.
    ENDIF.

    IF lines( <ls_ctx>-queue ) > 0.
      rv_complete = abap_false.
      RETURN.
    ENDIF.

    IF line_exists( mt_dispatch[ KEY run run_id = iv_run_id state = c_state_awaiting ] ).
      rv_complete = abap_false.
      RETURN.
    ENDIF.

    IF count_terminal_objects( iv_run_id ) <> <ls_ctx>-expected_count.
      rv_complete = abap_false.
    ENDIF.
  ENDMETHOD.


  METHOD interpret_wait_result.
    IF iv_run_complete = abap_true.
      rv_result = 0.
      RETURN.
    ENDIF.

    CASE iv_wait_subrc.
      WHEN 0 OR 4.
        rv_result = 4.
      WHEN OTHERS.
        rv_result = 8.
    ENDCASE.
  ENDMETHOD.


  METHOD wait_for_run_completion.
    DATA lv_wait_subrc     TYPE sy-subrc.
    DATA lv_last_progress  TYPE i.
    DATA lv_now_progress   TYPE i.
    DATA lv_stall_deadline TYPE timestampl.
    DATA lv_now            TYPE timestampl.

    drain_queue( iv_run_id ).

    " True pipeline: wake as soon as a callback frees a worker slot with
    " queued work AND refill it immediately, instead of waiting for the whole
    " in-flight wave to finish first (which left fast workers idle behind one
    " slow batch). Dispatch stays on the main path - STARTING NEW TASK is
    " illegal inside the ON_END_OF_BATCH callback.
    lv_last_progress = count_terminal_objects( iv_run_id ).
    GET TIME STAMP FIELD lv_stall_deadline.
    lv_stall_deadline = cl_abap_tstmp=>add( tstmp = lv_stall_deadline secs = c_batch_rfc_timeout_s ).

    WHILE is_run_complete( iv_run_id ) = abap_false.
      WAIT UNTIL is_run_complete( iv_run_id ) = abap_true
              OR has_dispatchable_capacity( iv_run_id ) = abap_true
              UP TO c_poll_interval_s SECONDS.
      drain_queue( iv_run_id ).
      report_progress( iv_run_id ).

      " Stall (not total-time) budget: reset on any newly completed object, so
      " a legitimately long-but-progressing run is never aborted, while a truly
      " hung batch (no progress for the whole budget) still fails fast.
      lv_now_progress = count_terminal_objects( iv_run_id ).
      IF lv_now_progress > lv_last_progress.
        lv_last_progress = lv_now_progress.
        GET TIME STAMP FIELD lv_stall_deadline.
        lv_stall_deadline = cl_abap_tstmp=>add( tstmp = lv_stall_deadline secs = c_batch_rfc_timeout_s ).
      ELSE.
        GET TIME STAMP FIELD lv_now.
        IF cl_abap_tstmp=>compare( tstmp1 = lv_now tstmp2 = lv_stall_deadline ) >= 0.
          lv_wait_subrc = 8.
          EXIT.
        ENDIF.
      ENDIF.
    ENDWHILE.

    rv_result = interpret_wait_result(
      iv_wait_subrc   = lv_wait_subrc
      iv_run_complete = is_run_complete( iv_run_id ) ).
  ENDMETHOD.


  METHOD handle_receive_failure.
    DATA lt_half_1   TYPE zif_abapgit_definitions=>ty_tadir_tt.
    DATA lt_half_2   TYPE zif_abapgit_definitions=>ty_tadir_tt.
    DATA lv_split_at TYPE i.

    ASSIGN mt_run_context[ run_id = iv_run_id ] TO FIELD-SYMBOL(<ls_ctx>).

    IF lines( is_dispatch-object_keys ) > 1.
      lv_split_at = lines( is_dispatch-object_keys ) DIV 2.
      LOOP AT is_dispatch-object_keys INTO DATA(ls_key).
        IF sy-tabix <= lv_split_at.
          APPEND ls_key TO lt_half_1.
        ELSE.
          APPEND ls_key TO lt_half_2.
        ENDIF.
      ENDLOOP.
      IF <ls_ctx> IS ASSIGNED.
        APPEND VALUE #( items = VALUE #( FOR ls_half_key_1 IN lt_half_1 ( tadir = ls_half_key_1 ) ) ) TO <ls_ctx>-queue.
        APPEND VALUE #( items = VALUE #( FOR ls_half_key_2 IN lt_half_2 ( tadir = ls_half_key_2 ) ) ) TO <ls_ctx>-queue.
      ELSE.
        route_to_sequential_fallback( iv_run_id = iv_run_id it_object_keys = is_dispatch-object_keys ).
      ENDIF.
    ELSE.
      route_to_sequential_fallback( iv_run_id = iv_run_id it_object_keys = is_dispatch-object_keys ).
    ENDIF.
  ENDMETHOD.


  METHOD route_to_sequential_fallback.
    DATA ls_item       TYPE zif_abapgit_definitions=>ty_item.
    DATA ls_i18n_params TYPE zif_abapgit_definitions=>ty_i18n_params.

    ASSIGN mt_run_context[ run_id = iv_run_id ] TO FIELD-SYMBOL(<ls_ctx>).
    IF sy-subrc <> 0.
      RETURN.
    ENDIF.

    LOOP AT it_object_keys INTO DATA(ls_key).
      IF line_exists( mt_resolved[ run_id = iv_run_id obj_type = ls_key-object obj_name = ls_key-obj_name ] )
         OR line_exists( mt_failed[ run_id = iv_run_id obj_type = ls_key-object obj_name = ls_key-obj_name ] ).
        CONTINUE.
      ENDIF.

      CLEAR ls_item.
      ls_item-obj_type  = ls_key-object.
      ls_item-obj_name  = ls_key-obj_name.
      ls_item-devclass  = ls_key-devclass.
      ls_item-srcsystem = ls_key-srcsystem.
      ls_item-origlang  = ls_key-masterlang.

      " AR-1-001 (independent adversarial audit): mirror the standard
      " path's own per-object MAIN_LANGUAGE_ONLY override exactly (see
      " ZCL_ABAPGIT_SERIALIZE~RUN_SEQUENTIAL) - the run-level flag alone
      " is not sufficient when WO_TRANSLATION_PATTERNS is non-empty.
      ls_i18n_params = <ls_ctx>-is_i18n_params.
      IF ls_i18n_params-main_language_only = abap_false AND <ls_ctx>-wo_translation_patterns IS NOT INITIAL.
        ls_i18n_params-main_language_only = zcl_abapgit_i18n_params=>match_obj_patterns(
          is_tadir                   = ls_key
          it_wo_translation_patterns = <ls_ctx>-wo_translation_patterns ).
      ENDIF.

      TRY.
          DATA(ls_serialization) = zcl_abapgit_objects=>serialize(
            is_item        = ls_item
            io_i18n_params = zcl_abapgit_i18n_params=>new( is_params = ls_i18n_params ) ).

          IF ls_serialization-files IS INITIAL.
            " SER-SLICE-3 parity incident fix (serialization_slice_3_
            " dtel_doma_parity.md, AR-3-002): this method is the LAST-
            " RESORT recovery path (called directly for forced-sequential
            " objects, and as the recovery mechanism for a suspicious
            " batch/merge result) - it must never itself silently accept
            " a zero-file "success", or the whole fail-fast contract's
            " "no successful partial output" guarantee has a hole. Treat
            " exactly like an exception: no file added, marked failed.
            IF <ls_ctx>-ii_log IS BOUND.
              <ls_ctx>-ii_log->add_warning(
                |ORTEC fallback: { ls_key-object } { ls_key-obj_name } serialized to zero files - treating as failed| ).
            ENDIF.
            mark_object_failures( iv_run_id = iv_run_id it_object_keys = VALUE #( ( ls_key ) ) ).
            CONTINUE.
          ENDIF.

          LOOP AT ls_serialization-files INTO DATA(ls_file).
            APPEND INITIAL LINE TO <ls_ctx>-files ASSIGNING FIELD-SYMBOL(<ls_return>).
            <ls_return>-file = ls_file.
            <ls_return>-file-path = ls_key-path.
            <ls_return>-item = ls_serialization-item.
          ENDLOOP.
          mark_object_success( iv_run_id = iv_run_id is_tadir = ls_key ).
        CATCH zcx_abapgit_exception INTO DATA(lx_error).
          IF <ls_ctx>-ii_log IS BOUND.
            <ls_ctx>-ii_log->add_exception( lx_error ).
          ENDIF.
          mark_object_failures( iv_run_id = iv_run_id it_object_keys = VALUE #( ( ls_key ) ) ).
      ENDTRY.
    ENDLOOP.
  ENDMETHOD.


  METHOD record_task_outcome.
    DATA lv_next_seq TYPE i.
    DATA lv_max_seq  TYPE i.
    DATA lv_total     TYPE i.
    DATA lv_failures  TYPE i.
    DATA lv_ratio     TYPE p LENGTH 8 DECIMALS 4.

    LOOP AT mt_task_outcomes INTO DATA(ls_o) WHERE run_id = iv_run_id.
      IF ls_o-seq > lv_max_seq.
        lv_max_seq = ls_o-seq.
      ENDIF.
    ENDLOOP.
    lv_next_seq = lv_max_seq + 1.

    APPEND VALUE #( run_id = iv_run_id seq = lv_next_seq success = iv_success ) TO mt_task_outcomes.

    DELETE mt_task_outcomes WHERE run_id = iv_run_id AND seq <= ( lv_next_seq - c_breaker_window_size ).

    LOOP AT mt_task_outcomes INTO DATA(ls_this) WHERE run_id = iv_run_id.
      lv_total = lv_total + 1.
      IF ls_this-success = abap_false.
        lv_failures = lv_failures + 1.
      ENDIF.
    ENDLOOP.

    IF lv_total >= c_breaker_min_sample.
      lv_ratio = lv_failures / lv_total.
      IF lv_ratio >= c_breaker_failure_ratio.
        INSERT iv_run_id INTO TABLE mt_broken_runs.
      ENDIF.
    ENDIF.
  ENDMETHOD.


  METHOD purge_run_state.
    IF line_exists( mt_dispatch[ KEY run run_id = iv_run_id state = c_state_awaiting ] ).
      RETURN.
    ENDIF.

    DELETE mt_dispatch USING KEY run
      WHERE run_id = iv_run_id
        AND ( state = c_state_received OR state = c_state_received_failure ).

    DELETE mt_resolved WHERE run_id = iv_run_id. "#EC CI_HASHSEQ
    DELETE mt_failed WHERE run_id = iv_run_id.   "#EC CI_HASHSEQ
    DELETE mt_task_outcomes WHERE run_id = iv_run_id.
    DELETE mt_broken_runs WHERE table_line = iv_run_id.
    DELETE mt_run_context WHERE run_id = iv_run_id.
  ENDMETHOD.


  METHOD discard_run_state.
    DELETE mt_dispatch USING KEY run
      WHERE run_id = iv_run_id.
    DELETE mt_resolved WHERE run_id = iv_run_id.  "#EC CI_HASHSEQ
    DELETE mt_failed WHERE run_id = iv_run_id.    "#EC CI_HASHSEQ
    DELETE mt_task_outcomes WHERE run_id = iv_run_id.
    DELETE mt_broken_runs WHERE table_line = iv_run_id.
    DELETE mt_run_context WHERE run_id = iv_run_id.
  ENDMETHOD.


  METHOD is_standard_no_parallel_type.
    " Local copy of ZCL_ABAPGIT_SERIALIZE=>IS_NO_PARALLEL's exact logic -
    " see this method's own declaration doc for why it is not reused
    " directly. Keep in sync with #7148's denylist.
    IF iv_object_type = 'ECTC' OR iv_object_type = 'ECTD'.
      rv_result = abap_true.
    ENDIF.
  ENDMETHOD.


  METHOD has_no_pending_callbacks.
    ASSIGN mt_run_context[ run_id = iv_run_id ] TO FIELD-SYMBOL(<ls_ctx>).
    IF NOT <ls_ctx> IS ASSIGNED.
      rv_result = abap_true.
      RETURN.
    ENDIF.
    rv_result = boolc( <ls_ctx>-in_flight = 0 ).
  ENDMETHOD.


  METHOD has_dispatchable_capacity.
    ASSIGN mt_run_context[ run_id = iv_run_id ] TO FIELD-SYMBOL(<ls_ctx>).
    IF NOT <ls_ctx> IS ASSIGNED.
      RETURN.
    ENDIF.
    rv_result = boolc( lines( <ls_ctx>-queue ) > 0 AND <ls_ctx>-in_flight < <ls_ctx>-worker_count ).
  ENDMETHOD.


  METHOD report_progress.
    ASSIGN mt_run_context[ run_id = iv_run_id ] TO FIELD-SYMBOL(<ls_ctx>).
    IF NOT <ls_ctx> IS ASSIGNED OR NOT <ls_ctx>-ii_progress IS BOUND.
      RETURN.
    ENDIF.
    TRY.
        <ls_ctx>-ii_progress->show(
          iv_current = <ls_ctx>-terminal_count
          iv_text    = |Serialize: { <ls_ctx>-terminal_count } of { <ls_ctx>-expected_count } objects| ).
      CATCH zcx_abapgit_exception ##NO_HANDLER.
    ENDTRY.
  ENDMETHOD.
ENDCLASS.

