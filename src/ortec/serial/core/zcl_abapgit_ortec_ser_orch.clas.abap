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
    "! ABANDONED (serialization_adaptive_batch_design.md &sect;5.4).
    "! Reached from C_STATE_TIMED_OUT once C_MAX_DRAIN_WAIT_S has ALSO
    "! elapsed without a callback. Distinct from C_STATE_TIMED_OUT purely
    "! for the poll loop's exit condition (&sect;5.3): a 'T' row still
    "! blocks SERIALIZE() from returning (its RFC task may still be
    "! genuinely running and could still deliver a late callback); an 'X'
    "! row does NOT block return, but is otherwise handled identically to
    "! 'T' if a callback ever does arrive (ON_END_OF_BATCH still drains
    "! and discards it, &sect;5.5) - this state governs ONLY how long
    "! THIS run waits, never whether the remote task is cancelled or its
    "! resource reclaimed (see class-level "LOGICAL ABANDONMENT"
    "! documentation, which applies equally to 'T' and 'X').
    CONSTANTS c_state_abandoned        TYPE c LENGTH 1 VALUE 'X'.

    "! One outstanding or historical RFC dispatch. Retained metadata is
    "! deliberately small and TADIR-key-shaped only - see class-level
    "! "WHAT MAY BE RETAINED" documentation. Never carries a serialized
    "! payload.
    TYPES BEGIN OF ty_dispatch.
      "! Globally unique (for the whole internal session) task name
      "! used in "STARTING NEW TASK" / "RECEIVE RESULTS FROM
      "! FUNCTION" - formatted |SER-{run_id}-{dispatch_seq}|, always
      "! within the SAP task-ID length limit. Never reused, even
      "! for a retry of the same logical work (a retry gets a NEW
      "! task name and a NEW row).
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
      "! Timestamp this dispatch was issued, used to detect wait-
      "! budget expiry (logical abandonment).
      TYPES dispatch_ts TYPE timestampl.
    TYPES END OF ty_dispatch.
    "! All dispatches, current run and any not-yet-purged abandoned
    "! dispatches from earlier runs in the same internal session. Keyed
    "! for O(1) callback resolution by TASK_NAME.
    TYPES ty_dispatch_tt TYPE HASHED TABLE OF ty_dispatch WITH UNIQUE KEY task_name.

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
    "! CHECK_TIMEOUTS, HANDLE_RECEIVE_FAILURE, ROUTE_TO_SEQUENTIAL_
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
      "! RELEASE_IN_FLIGHT_BUDGET once a dispatch reaches any terminal or
      "! logically-abandoned state - bounds concurrency at WORKER_COUNT,
      "! mirroring the standard path's own MV_FREE semantics.
      TYPES in_flight      TYPE i.
      "! Planner-produced batches not yet dispatched (queued because they
      "! exceeded WORKER_COUNT's immediately-ready slots, or produced by a
      "! later REFILL) - drained by the poll loop as IN_FLIGHT capacity
      "! frees up.
      TYPES queue          TYPE zcl_abapgit_ortec_ser_planner=>tt_batch.
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
    "! Per-run cap on undrained LOGICALLY_ABANDONED dispatches before the
    "! oldest are force-purged (bounded retained-metadata guarantee).
    CONSTANTS c_max_abandoned_tasks_per_run TYPE i                     VALUE 50.
    "! Session-wide cap across ALL runs' undrained abandoned dispatches.
    CONSTANTS c_max_abandoned_tasks_sess    TYPE i                     VALUE 200.
    "! Session-wide cap on distinct runs each holding >=1 undrained
    "! abandoned dispatch before the OLDEST such run is force-purged.
    CONSTANTS c_max_abandoned_runs_sess     TYPE i                     VALUE 20.

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
    "! TELEMETRY/WARNING COUNTER ONLY (serialization_adaptive_batch_design.
    "! md &sect;5.9, cycle-3 revision) - splitting in BEFORE_DISPATCH is
    "! NEVER capped by this value; it exists purely so an excessively
    "! fragmented batch can be logged as a warning signal for later
    "! investigation, never as a reason to dispatch an over-limit group.
    CONSTANTS c_max_pre_dispatch_splits     TYPE i VALUE 3.
    "! Additional bounded wait, ON TOP OF C_BATCH_RFC_TIMEOUT_S
    "! (serialization_adaptive_batch_design.md &sect;5.3/&sect;5.4), before
    "! a C_STATE_TIMED_OUT dispatch is reclassified C_STATE_ABANDONED and
    "! stops blocking the poll loop's exit. Unit: seconds. HARD BOUND on
    "! SERIALIZE()'s own worst-case return time for one straggler dispatch
    "! (approx. C_BATCH_RFC_TIMEOUT_S + C_MAX_DRAIN_WAIT_S = 600s total by
    "! default) - does not affect correctness (the straggler's objects are
    "! already resolved via resubmit/fallback before this elapses), only
    "! how long this run keeps waiting for its harmless, already-
    "! superseded callback.
    CONSTANTS c_max_drain_wait_s            TYPE i VALUE 300.
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
    "! Wait budget for one dispatch's FIRST callback attempt before it is
    "! marked C_STATE_TIMED_OUT (serialization_adaptive_batch_design.md
    "! &sect;5.4) - matches the existing standard path's own avoid_timeout
    "! window. Unit: seconds. HARD BOUND governing when CHECK_TIMEOUTS
    "! (&sect;5.4) transitions an 'A' row to 'T'; does not by itself lose
    "! any object (the object is immediately resubmitted or falls back).
    CONSTANTS c_batch_rfc_timeout_s         TYPE i VALUE 300.
    "! Maximum RFC-level retry attempts PER LOGICAL OBJECT GROUP before
    "! that group is routed to sequential fallback instead of retried again
    "! (serialization_adaptive_batch_design.md &sect;5.6). Unit: attempts.
    "! HARD BOUND - each bisection half gets its OWN fresh budget (never
    "! inherited from its parent group), so this never blocks eventual
    "! completion, only how many RFC round trips are spent before falling
    "! back to the always-safe in-process path.
    CONSTANTS c_max_retries                 TYPE i VALUE 2.

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
    "! Each active run's shared context (output accumulator, log sink,
    "! RFC group, i18n params) - see TY_RUN_CONTEXT_TT.
    CLASS-DATA mt_run_context   TYPE ty_run_context_tt.

    "! Session-scoped table types and static state above; helper methods
    "! below.

    "! Merges one resolved object's serialized files into this run's own
    "! MT_RUN_CONTEXT-FILES accumulator (serialization_adaptive_batch_
    "! design.md &sect;5.5's MERGE_INTO_MT_FILES) - assigns each file's
    "! PATH from its own TADIR entry (mirrors the standard path's own
    "! ADD_TO_RETURN, since a batch can contain objects from different
    "! paths, unlike the single-object worker).
    "! @parameter iv_run_id | Owning run
    "! @parameter is_tadir  | The object's own TADIR row (for PATH)
    "! @parameter is_result | One ET_RESULT row with RC = 0
    CLASS-METHODS merge_into_mt_files
      IMPORTING iv_run_id  TYPE sysuuid_x16
                is_tadir   TYPE zif_abapgit_definitions=>ty_tadir
                is_result  TYPE zaog_ser_batch_result.

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
      IMPORTING it_result         TYPE zaog_ser_batch_result_tt
                it_object_keys    TYPE zif_abapgit_definitions=>ty_tadir_tt
      RETURNING VALUE(rv_equal)   TYPE abap_bool.

    "! Decrements this run's MT_RUN_CONTEXT-IN_FLIGHT counter, freeing one
    "! concurrency slot (serialization_adaptive_batch_design.md &sect;5.4/
    "! &sect;5.5/&sect;5.6 all call this once a dispatch leaves state
    "! C_STATE_AWAITING for any reason - resolved, confirmed failed, or
    "! logically abandoned). Never lets the counter go below zero.
    "! @parameter iv_run_id | Owning run
    CLASS-METHODS release_in_flight_budget
      IMPORTING iv_run_id TYPE sysuuid_x16.

    "! Actual-bytes admission check and recursive splitting before every
    "! dispatch (serialization_adaptive_batch_design.md &sect;5.9) -
    "! initial batches, refills, AND every retry/bisection from
    "! CHECK_TIMEOUTS/HANDLE_RECEIVE_FAILURE all funnel through here, never
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
    "! @parameter iv_run_id      | Owning run
    "! @parameter it_object_keys | Candidate objects for one dispatch (may
    "!   be split into smaller dispatches by this method)
    "! @parameter iv_attempt     | Passed through unchanged to DISPATCH_BATCH
    "! @parameter iv_batch_id    | Passed through unchanged to DISPATCH_BATCH
    "! @raising zcx_abapgit_exception | Propagated from DISPATCH_BATCH
    CLASS-METHODS before_dispatch
      IMPORTING iv_run_id      TYPE sysuuid_x16
                it_object_keys TYPE zif_abapgit_definitions=>ty_tadir_tt
                iv_attempt     TYPE i
                iv_batch_id    TYPE char32
      RAISING   zcx_abapgit_exception.

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
    "! @raising zcx_abapgit_exception | Batch-level dispatch failure (e.g.
    "!   STARTING NEW TASK could not be issued at all)
    CLASS-METHODS dispatch_batch
      IMPORTING iv_run_id              TYPE sysuuid_x16
                it_object_keys         TYPE zif_abapgit_definitions=>ty_tadir_tt
                iv_attempt             TYPE i
                iv_batch_id            TYPE char32
                iv_prefetch_buffer     TYPE xstring OPTIONAL
                iv_prefetch_buffer_ext TYPE xstring OPTIONAL
                iv_prefetch_buffer_oo  TYPE xstring OPTIONAL
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
    DATA lv_run_id     TYPE sysuuid_x16.
    DATA lt_forced_seq TYPE zif_abapgit_definitions=>ty_tadir_tt.
    DATA lt_eligible   TYPE zif_abapgit_definitions=>ty_tadir_tt.
    DATA lt_work_items TYPE zcl_abapgit_ortec_ser_planner=>tt_work_item.
    DATA lt_batches    TYPE zcl_abapgit_ortec_ser_planner=>tt_batch.
    DATA lv_ready      TYPE i.
    DATA lv_batch_ctr  TYPE i.

    TRY.
        lv_run_id = cl_system_uuid=>create_uuid_x16_static( ).
      CATCH cx_uuid_error.
        zcx_abapgit_exception=>raise( 'ORTEC batch: could not generate a run id' ).
    ENDTRY.

    INSERT VALUE #( run_id         = lv_run_id
                     ii_log         = ii_log
                     iv_group       = iv_group
                     is_i18n_params = is_i18n_params
                     worker_count   = iv_max_processes ) INTO TABLE mt_run_context.
    ASSIGN mt_run_context[ run_id = lv_run_id ] TO FIELD-SYMBOL(<ls_ctx>).

    LOOP AT it_tadir INTO DATA(ls_tadir).
      " WAPA is never batch-eligible (OD-14 audit) - IS_NO_PARALLEL alone
      " does not cover it (that denylist is only ECTC/ECTD). This routes
      " WAPA through ROUTE_TO_SEQUENTIAL_FALLBACK, which calls the SAME
      " generic zcl_abapgit_objects=>serialize() dispatch as the standard
      " RUN_SEQUENTIAL - structurally identical, not a regression.
      IF iv_max_processes = 1
         OR ls_tadir-object = 'WAPA'
         OR zcl_abapgit_serialize=>is_no_parallel( ls_tadir-object ) = abap_true.
        APPEND ls_tadir TO lt_forced_seq.
      ELSE.
        APPEND ls_tadir TO lt_eligible.
      ENDIF.
    ENDLOOP.

    route_to_sequential_fallback( iv_run_id = lv_run_id it_object_keys = lt_forced_seq ).

    IF <ls_ctx> IS ASSIGNED.
      LOOP AT lt_eligible INTO DATA(ls_key).
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

    LOOP AT lt_batches INTO DATA(ls_batch).
      lv_batch_ctr = lv_batch_ctr + 1.
      DATA(lt_keys) = VALUE zif_abapgit_definitions=>ty_tadir_tt( FOR ls_wi IN ls_batch-items ( ls_wi-tadir ) ).
      IF lv_ready < iv_max_processes.
        lv_ready = lv_ready + 1.
        before_dispatch( iv_run_id      = lv_run_id
                          it_object_keys = lt_keys
                          iv_attempt     = 1
                          iv_batch_id    = |B{ lv_batch_ctr }| ).
      ELSEIF <ls_ctx> IS ASSIGNED.
        APPEND ls_batch TO <ls_ctx>-queue.
      ENDIF.
    ENDLOOP.

    DO.
      WAIT UP TO 5 SECONDS.
      check_timeouts( lv_run_id ).

      IF <ls_ctx> IS ASSIGNED.
        WHILE lines( <ls_ctx>-queue ) > 0 AND <ls_ctx>-in_flight < <ls_ctx>-worker_count.
          READ TABLE <ls_ctx>-queue INDEX 1 INTO DATA(ls_next_batch).
          DELETE <ls_ctx>-queue INDEX 1.
          lv_batch_ctr = lv_batch_ctr + 1.
          DATA(lt_next_keys) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
            FOR ls_wi2 IN ls_next_batch-items ( ls_wi2-tadir ) ).
          before_dispatch( iv_run_id      = lv_run_id
                            it_object_keys = lt_next_keys
                            iv_attempt     = 1
                            iv_batch_id    = |B{ lv_batch_ctr }| ).
        ENDWHILE.
      ENDIF.

      IF NOT ( <ls_ctx> IS ASSIGNED AND lines( <ls_ctx>-queue ) > 0 )
         AND NOT line_exists( mt_dispatch[ run_id = lv_run_id state = c_state_awaiting ] )
         AND NOT line_exists( mt_dispatch[ run_id = lv_run_id state = c_state_timed_out ] ).
        EXIT.
      ENDIF.
    ENDDO.

    IF <ls_ctx> IS ASSIGNED.
      rt_files = <ls_ctx>-files.
    ENDIF.

    purge_run_state( lv_run_id ).
  ENDMETHOD.

  METHOD on_end_of_batch.
    DATA lv_msg TYPE c LENGTH 100.
    DATA lt_purged_discard TYPE zaog_ser_batch_result_tt.
    DATA lt_discard        TYPE zaog_ser_batch_result_tt.
    DATA lt_result         TYPE zaog_ser_batch_result_tt.
    DATA lv_out_rows       TYPE i.

    ASSIGN mt_dispatch[ task_name = p_task ] TO FIELD-SYMBOL(<ls_d>).
    IF sy-subrc <> 0.
      " Defensive: unknown/already-purged task name - still RECEIVE to
      " free the RFC resource, then discard (sect 5.5).
      RECEIVE RESULTS FROM FUNCTION 'Z_ABAPGIT_ORTEC_SER_BATCH'
        IMPORTING et_result = lt_purged_discard
        EXCEPTIONS system_failure = 1 communication_failure = 2 OTHERS = 3.
      RETURN.
    ENDIF.

    CASE <ls_d>-state.
      WHEN c_state_timed_out OR c_state_abandoned.
        RECEIVE RESULTS FROM FUNCTION 'Z_ABAPGIT_ORTEC_SER_BATCH'
          IMPORTING et_result = lt_discard
          EXCEPTIONS system_failure = 1 communication_failure = 2 OTHERS = 3.
        <ls_d>-state = c_state_drained.
        RETURN.

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
          ENDTRY.
          RETURN.
        ENDIF.

        <ls_d>-state = c_state_received.
        release_in_flight_budget( <ls_d>-run_id ).
        record_task_outcome( iv_run_id = <ls_d>-run_id iv_success = abap_true ).

        LOOP AT lt_result INTO DATA(ls_row).
          IF line_exists( mt_resolved[ run_id = <ls_d>-run_id obj_type = ls_row-obj_type obj_name = ls_row-obj_name ] ).
            CONTINUE.
          ENDIF.

          READ TABLE <ls_d>-object_keys INTO DATA(ls_tadir_row)
            WITH KEY object = ls_row-obj_type obj_name = ls_row-obj_name.

          IF ls_row-rc = 0 AND sy-subrc = 0.
            merge_into_mt_files( iv_run_id = <ls_d>-run_id is_tadir = ls_tadir_row is_result = ls_row ).
          ELSE.
            ASSIGN mt_run_context[ run_id = <ls_d>-run_id ] TO FIELD-SYMBOL(<ls_fail_ctx>).
            IF <ls_fail_ctx> IS ASSIGNED AND <ls_fail_ctx>-ii_log IS BOUND.
              <ls_fail_ctx>-ii_log->add_error(
                |ORTEC batch: { ls_row-obj_type } { ls_row-obj_name } failed ({ ls_row-msgid } { ls_row-msgno })| ).
            ENDIF.
          ENDIF.

          " EWMA is updated for EVERY resolved object regardless of
          " success/failure (sect 5.5) - re-assigned fresh every
          " iteration, never conditional on the (possibly stale) field
          " symbol from the branch above.
          ASSIGN mt_run_context[ run_id = <ls_d>-run_id ] TO FIELD-SYMBOL(<ls_ewma_ctx>).
          IF <ls_ewma_ctx> IS ASSIGNED.
            zcl_abapgit_ortec_ser_cost=>update_estimate(
              EXPORTING iv_obj_type     = ls_row-obj_type
                        iv_actual_ms    = ls_row-elapsed_ms
                        iv_actual_bytes = ls_row-output_bytes
              CHANGING  ct_ewma         = <ls_ewma_ctx>-ewma ).
          ENDIF.

          INSERT VALUE #( run_id = <ls_d>-run_id obj_type = ls_row-obj_type obj_name = ls_row-obj_name )
            INTO TABLE mt_resolved.
        ENDLOOP.
    ENDCASE.
  ENDMETHOD.

  METHOD merge_into_mt_files.
    DATA ls_serialization TYPE zif_abapgit_objects=>ty_serialization.

    ASSIGN mt_run_context[ run_id = iv_run_id ] TO FIELD-SYMBOL(<ls_ctx>).
    IF sy-subrc <> 0.
      RETURN.
    ENDIF.

    IMPORT data = ls_serialization FROM DATA BUFFER is_result-files_xstring.
    IF sy-subrc <> 0.
      RETURN.
    ENDIF.

    LOOP AT ls_serialization-files INTO DATA(ls_file).
      APPEND INITIAL LINE TO <ls_ctx>-files ASSIGNING FIELD-SYMBOL(<ls_return>).
      <ls_return>-file = ls_file.
      <ls_return>-file-path = is_tadir-path.
      <ls_return>-item = ls_serialization-item.
    ENDLOOP.
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
    DATA lv_actual_bytes TYPE i.
    DATA lv_split_at     TYPE i.

    " SER-SLICE-2 scope boundary (see class-level documentation on this
    " method): no batch-scoped extraction exists yet on PREF/PREF_EXT/
    " PREF_OO, so LV_ACTUAL_BYTES is legitimately 0 here - the gate below
    " is structurally correct but does not trigger yet.
    lv_actual_bytes = 0.

    IF lv_actual_bytes > c_max_actual_batch_bytes AND lines( it_object_keys ) > 1.
      lv_split_at = lines( it_object_keys ) DIV 2.
      LOOP AT it_object_keys INTO DATA(ls_key).
        IF sy-tabix <= lv_split_at.
          APPEND ls_key TO lt_half_1.
        ELSE.
          APPEND ls_key TO lt_half_2.
        ENDIF.
      ENDLOOP.
      before_dispatch( iv_run_id = iv_run_id it_object_keys = lt_half_1 iv_attempt = iv_attempt iv_batch_id = iv_batch_id ).
      before_dispatch( iv_run_id = iv_run_id it_object_keys = lt_half_2 iv_attempt = iv_attempt iv_batch_id = iv_batch_id ).
      RETURN.
    ENDIF.

    dispatch_batch( iv_run_id = iv_run_id it_object_keys = it_object_keys iv_attempt = iv_attempt iv_batch_id = iv_batch_id ).
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
    DATA(lv_run_hex) = |{ iv_run_id }|.
    lv_task_name = |SER-{ lv_run_hex(8) }-{ <ls_ctx>-dispatch_seq }|.

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

  METHOD check_timeouts.
    DATA lv_now     TYPE timestampl.
    DATA lv_elapsed TYPE i.
    DATA lt_abandoned TYPE STANDARD TABLE OF ty_dispatch WITH EMPTY KEY.
    DATA lt_seen_runs TYPE ty_broken_runs_tt.

    GET TIME STAMP FIELD lv_now.

    LOOP AT mt_dispatch ASSIGNING FIELD-SYMBOL(<ls_d>)
         WHERE run_id = iv_run_id AND state = c_state_awaiting.
      lv_elapsed = cl_abap_tstmp=>subtract( tstmp1 = lv_now tstmp2 = <ls_d>-dispatch_ts ).
      IF lv_elapsed >= c_batch_rfc_timeout_s.
        <ls_d>-state = c_state_timed_out.
        release_in_flight_budget( iv_run_id ).
        TRY.
            IF <ls_d>-attempt < c_max_retries.
              before_dispatch( iv_run_id      = iv_run_id
                                it_object_keys = <ls_d>-object_keys
                                iv_attempt     = <ls_d>-attempt + 1
                                iv_batch_id    = <ls_d>-batch_id ).
            ELSE.
              route_to_sequential_fallback( iv_run_id = iv_run_id it_object_keys = <ls_d>-object_keys ).
            ENDIF.
          CATCH zcx_abapgit_exception INTO DATA(lx_timeout_error).
            " never let an exception escape - see ON_END_OF_BATCH's same
            " defensive pattern; log if a sink exists for this run.
            ASSIGN mt_run_context[ run_id = iv_run_id ] TO FIELD-SYMBOL(<ls_to_ctx>).
            IF <ls_to_ctx> IS ASSIGNED AND <ls_to_ctx>-ii_log IS BOUND.
              <ls_to_ctx>-ii_log->add_exception( lx_timeout_error ).
            ENDIF.
        ENDTRY.
      ENDIF.
    ENDLOOP.

    LOOP AT mt_dispatch ASSIGNING FIELD-SYMBOL(<ls_t>)
         WHERE run_id = iv_run_id AND state = c_state_timed_out.
      lv_elapsed = cl_abap_tstmp=>subtract( tstmp1 = lv_now tstmp2 = <ls_t>-dispatch_ts ).
      IF lv_elapsed >= ( c_batch_rfc_timeout_s + c_max_drain_wait_s ).
        <ls_t>-state = c_state_abandoned.
      ENDIF.
    ENDLOOP.

    " Bounded retention (sect 5.1a) - memory/observability bounds only,
    " never a correctness requirement (a purged run's late callback
    " always safely hits the existing unknown-task_name path, sect 5.5).
    LOOP AT mt_dispatch INTO DATA(ls_ab) WHERE state = c_state_abandoned.
      APPEND ls_ab TO lt_abandoned.
    ENDLOOP.

    IF lines( lt_abandoned ) > c_max_abandoned_tasks_sess.
      SORT lt_abandoned BY dispatch_ts ASCENDING.
      DATA(lv_excess) = lines( lt_abandoned ) - c_max_abandoned_tasks_sess.
      LOOP AT lt_abandoned INTO DATA(ls_old).
        IF sy-tabix > lv_excess.
          EXIT.
        ENDIF.
        DELETE mt_dispatch WHERE task_name = ls_old-task_name.
      ENDLOOP.
    ENDIF.

    LOOP AT lt_abandoned INTO DATA(ls_a2).
      INSERT ls_a2-run_id INTO TABLE lt_seen_runs.
    ENDLOOP.

    IF lines( lt_seen_runs ) > c_max_abandoned_runs_sess.
      SORT lt_abandoned BY dispatch_ts ASCENDING.
      READ TABLE lt_abandoned INTO DATA(ls_global_oldest) INDEX 1.
      IF sy-subrc = 0.
        DELETE mt_dispatch WHERE run_id = ls_global_oldest-run_id.
        DELETE mt_resolved WHERE run_id = ls_global_oldest-run_id.
        DELETE mt_task_outcomes WHERE run_id = ls_global_oldest-run_id.
        DELETE mt_broken_runs WHERE table_line = ls_global_oldest-run_id.
        DELETE mt_run_context WHERE run_id = ls_global_oldest-run_id.
      ENDIF.
    ENDIF.
  ENDMETHOD.

  METHOD handle_receive_failure.
    DATA lt_half_1   TYPE zif_abapgit_definitions=>ty_tadir_tt.
    DATA lt_half_2   TYPE zif_abapgit_definitions=>ty_tadir_tt.
    DATA lv_split_at TYPE i.

    IF lines( is_dispatch-object_keys ) > 1.
      lv_split_at = lines( is_dispatch-object_keys ) DIV 2.
      LOOP AT is_dispatch-object_keys INTO DATA(ls_key).
        IF sy-tabix <= lv_split_at.
          APPEND ls_key TO lt_half_1.
        ELSE.
          APPEND ls_key TO lt_half_2.
        ENDIF.
      ENDLOOP.
      before_dispatch( iv_run_id = iv_run_id it_object_keys = lt_half_1
                        iv_attempt = is_dispatch-attempt + 1 iv_batch_id = is_dispatch-batch_id ).
      before_dispatch( iv_run_id = iv_run_id it_object_keys = lt_half_2
                        iv_attempt = is_dispatch-attempt + 1 iv_batch_id = is_dispatch-batch_id ).
    ELSE.
      route_to_sequential_fallback( iv_run_id = iv_run_id it_object_keys = is_dispatch-object_keys ).
    ENDIF.
  ENDMETHOD.

  METHOD route_to_sequential_fallback.
    DATA ls_item TYPE zif_abapgit_definitions=>ty_item.

    ASSIGN mt_run_context[ run_id = iv_run_id ] TO FIELD-SYMBOL(<ls_ctx>).
    IF sy-subrc <> 0.
      RETURN.
    ENDIF.

    LOOP AT it_object_keys INTO DATA(ls_key).
      IF line_exists( mt_resolved[ run_id = iv_run_id obj_type = ls_key-object obj_name = ls_key-obj_name ] ).
        CONTINUE.
      ENDIF.

      CLEAR ls_item.
      ls_item-obj_type  = ls_key-object.
      ls_item-obj_name  = ls_key-obj_name.
      ls_item-devclass  = ls_key-devclass.
      ls_item-srcsystem = ls_key-srcsystem.
      ls_item-origlang  = ls_key-masterlang.

      TRY.
          DATA(ls_serialization) = zcl_abapgit_objects=>serialize(
            is_item        = ls_item
            io_i18n_params = zcl_abapgit_i18n_params=>new( is_params = <ls_ctx>-is_i18n_params ) ).

          LOOP AT ls_serialization-files INTO DATA(ls_file).
            APPEND INITIAL LINE TO <ls_ctx>-files ASSIGNING FIELD-SYMBOL(<ls_return>).
            <ls_return>-file = ls_file.
            <ls_return>-file-path = ls_key-path.
            <ls_return>-item = ls_serialization-item.
          ENDLOOP.
        CATCH zcx_abapgit_exception INTO DATA(lx_error).
          IF <ls_ctx>-ii_log IS BOUND.
            <ls_ctx>-ii_log->add_exception( lx_error ).
          ENDIF.
      ENDTRY.

      INSERT VALUE #( run_id = iv_run_id obj_type = ls_key-object obj_name = ls_key-obj_name ) INTO TABLE mt_resolved.
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
    IF line_exists( mt_dispatch[ run_id = iv_run_id state = c_state_awaiting ] )
       OR line_exists( mt_dispatch[ run_id = iv_run_id state = c_state_timed_out ] ).
      RETURN.
    ENDIF.

    " Only R/F/D are removed - 'X' (ABANDONED) rows are deliberately left
    " behind so a late callback still has a row to drain against (bounded
    " by MAX_ABANDONED_*, enforced in CHECK_TIMEOUTS).
    DELETE mt_dispatch WHERE run_id = iv_run_id
      AND ( state = c_state_received OR state = c_state_received_failure OR state = c_state_drained ).

    DELETE mt_resolved WHERE run_id = iv_run_id.
    DELETE mt_task_outcomes WHERE run_id = iv_run_id.
    DELETE mt_broken_runs WHERE table_line = iv_run_id.
    DELETE mt_run_context WHERE run_id = iv_run_id.
  ENDMETHOD.

ENDCLASS.

