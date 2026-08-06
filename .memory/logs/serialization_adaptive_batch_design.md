# SER-2 — Adaptive Batch Orchestration Design

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SERIALIZATION_SER2_ADAPTIVE_BATCH_DESIGN
STATUS=DRAFT_FOR_REVIEW
DEPENDS_ON=serialization_ser0_audit.md §1,§6,§7; serialization_performance_design.md §2-3
```

## Current-source supersedure (2026-08-06)

```text
STATUS_OF_THIS_DOCUMENT=HISTORICAL_DESIGN_RATIONALE
CURRENT_HEAD_SUPERSEDURE=The current Stage-A source descended from
  cdfcbe8b/3a85b286 intentionally SUPERSEDES the older timeout lifecycle
  specified in this document's §5.1a/§5.3/§5.4 and the associated
  T-DRAIN closure gate: there is no longer any T/X/abandoned post-return
  drain model in the active source. The active contract is now fail-fast
  WAIT semantics only: successful return iff all planned objects are
  complete; WAIT result 4 while incomplete => visible abapGit exception,
  partial result discarded; WAIT result 8 => visible timeout exception,
  partial result discarded. Late callbacks after DISCARD_RUN_STATE are
  still safe via the unknown-task RECEIVE-and-discard path, but no
  successful-return drain window remains.
WAPA_SUPERSEDURE=This design's older "WAPA remains excluded from the
  batch worker" wording is also superseded for Stage A. Current source
  admits WAPA only as singleton batches, never mixed with any other
  object.
AUTHORITATIVE_SOURCE=Current source outranks stale prose. Use this
  document as rationale/history unless a section matches the active code.
```

## 1. Why (quantitative justification, not assumed)

`SAT-WarmToColdBranchWithMinChanges.txt` (large-repo trace, 17,148
`RUN_PARALLEL`/async-RFC dispatches) shows the RFC task-dispatch machinery
itself — `SPBT_PARALLEL_PROCESSING` (15.40% gross), `Loop At PRFC_SESSION-
RESOURCE_TBL` (12.94%), `SPBT_GET_CURR_RESOURCE_INFO` (12.92%),
`CHECK_SRV_STILL_ACTIVE` (12.79%), `GET_SERVER_PBT_RESOURCES` (12.49%),
`TH_ARFC_REQUESTS` (12.33%), `Call C ThSysInfo` (11.81%) — costs 12-15%
of GROSS elapsed time EACH, largely independent of the actual ABAP
serialization work. This scales with the NUMBER of RFC task starts, not
with total object count directly: fewer, larger batches reduce this
overhead roughly in proportion to the reduction in task-start count. The
CLAS-only sample (`classes-parallel-main.txt`) separately confirms
parallel dispatch is already far better than sequential (~6.6x) — SER-2
must ADD to that win, not trade it away for a simpler-but-slower design.

## 2. RFC contract

```text
OWNER_DECISION OD-2  New function module Z_ABAPGIT_ORTEC_SER_BATCH in a new
                      function group under $ABAPGIT_ORTEC_SERIAL_RFC, NOT a
                      versioned extension of the standard
                      Z_ABAPGIT_SERIALIZE_PARALLEL.
RECOMMENDATION        NEW FM (default-safe).
ALTERNATIVE_REJECTED  Add OPTIONAL batch parameters to the existing standard
                      FM. Rejected because: (a) it is a standard abapGit
                      object, not ORTEC-owned — touching its signature risks
                      every non-ORTEC fork/consumer and violates "prefer few,
                      stable hooks in standard, keep orchestration in the
                      ORTEC extension"; (b) a dual-mode FM (single-object vs
                      batch, selected by which optional parameters are
                      filled) is harder to test, review, and reason about
                      than two separate, single-purpose FMs; (c) a NEW FM
                      gives 100% blast-radius isolation — a batch-worker bug
                      cannot affect the existing single-object path at all,
                      which stays completely unused once the ORTEC batch
                      switch is on, but remains the guaranteed fallback when
                      it is off.
```

### Signature

```text
FUNCTION Z_ABAPGIT_ORTEC_SER_BATCH.
*"----------------------------------------------------------------------
*"  IMPORTING
*"     VALUE(IV_BATCH_ID)              TYPE  CHAR32
*"     VALUE(IV_ATTEMPT)               TYPE  I DEFAULT 1
*"     VALUE(IV_ABAP_LANGUAGE_VERS)    TYPE  ZIF_ABAPGIT_DEFINITIONS=>TY_LANGUAGE_VERSION  " same type as today's FM
*"     VALUE(IV_LANGUAGE)              TYPE  SPRAS
*"     VALUE(IV_PATH)                  TYPE  STRING
*"     VALUE(IV_MAIN_LANGUAGE_ONLY)    TYPE  ABAP_BOOL
*"     VALUE(IV_SUPPRESS_PO_COMMENTS)  TYPE  ABAP_BOOL
*"     VALUE(IV_USE_LXE)               TYPE  ABAP_BOOL
*"     VALUE(IT_TRANSLATION_LANGS)     TYPE  ZIF_ABAPGIT_DEFINITIONS=>TY_LANGUAGES_TT
*"     VALUE(IT_TADIR)                 TYPE  ZIF_ABAPGIT_DEFINITIONS=>TY_TADIR_TT
*"     VALUE(IV_PREFETCH_BUFFER)       TYPE  XSTRING OPTIONAL
*"     VALUE(IV_PREFETCH_BUFFER_EXT)   TYPE  XSTRING OPTIONAL
*"     VALUE(IV_PREFETCH_BUFFER_OO)    TYPE  XSTRING OPTIONAL
*"     VALUE(IV_PREFETCH_BUFFER_DD)    TYPE  XSTRING OPTIONAL   " new DOMA/DTEL provider, SER-3
*"     VALUE(IV_INPUT_ROW_COUNT)       TYPE  I
*"     VALUE(IV_INPUT_VERSION)         TYPE  I VALUE 1
*"  EXPORTING
*"     VALUE(ET_RESULT)                TYPE  ZAOG_SER_BATCH_RESULT_TT
*"     VALUE(EV_OUTPUT_ROW_COUNT)      TYPE  I
*"  EXCEPTIONS
*"     ERROR                                 " batch-level catastrophic
*"                                            " failure only (e.g. cannot
*"                                            " even start) - object-level
*"                                            " failures are ET_RESULT rows
*"----------------------------------------------------------------------
```

New DDIC (minimal — everything else reuses existing types):

```text
ZAOG_SER_BATCH_RESULT (structure)
  OBJ_TYPE          TROBJTYPE
  OBJ_NAME          SOBJ_NAME
  RC                I                " 0 = success, <>0 = failure
  FILES_XSTRING     XSTRING          " EXPORT of one ty_file_item, same
                                      "  shape/format as today's per-object
                                      "  EV_RESULT
  MSGID             SY-MSGID
  MSGNO             SY-MSGNO
  MSGV1..MSGV4      SY-MSGV1..4
  ELAPSED_MS        I
  OUTPUT_BYTES      I
  OUTPUT_FILE_COUNT I
  PROVIDER_HIT      I
  PROVIDER_MISS     I
  PROVIDER_FALLBACK I
ZAOG_SER_BATCH_RESULT_TT  STANDARD TABLE OF ZAOG_SER_BATCH_RESULT
                          WITH EMPTY KEY
```

`ET_RESULT` NEVER omits a row for a requested object: every object in
`IT_TADIR` produces exactly one `ET_RESULT` row, success or failure — this
is what lets the caller correlate results without any ambiguity and
directly satisfies "one failed object must not silently invalidate or
discard successful sibling results."

### Worker body contract (decision-free pseudocode)

```text
FUNCTION z_abapgit_ortec_ser_batch.
  IF iv_prefetch_buffer     IS NOT INITIAL. zcl_abapgit_ortec_ser_pref=>inject_from_buffer( iv_prefetch_buffer ).         ENDIF.
  IF iv_prefetch_buffer_ext IS NOT INITIAL. zcl_abapgit_ortec_ser_pref_ext=>inject_from_buffer( iv_prefetch_buffer_ext ). ENDIF.
  IF iv_prefetch_buffer_oo  IS NOT INITIAL. zcl_abapgit_ortec_ser_pref_oo=>inject_from_buffer( iv_prefetch_buffer_oo ).   ENDIF.
  IF iv_prefetch_buffer_dd  IS NOT INITIAL. zcl_abapgit_ortec_ser_prov_dd=>inject_from_buffer( iv_prefetch_buffer_dd ).   ENDIF.

  LOOP AT it_tadir INTO DATA(ls_tadir).
    GET RUN TIME FIELD DATA(lv_t0).
    CLEAR ls_result.
    ls_result-obj_type = ls_tadir-object.
    ls_result-obj_name = ls_tadir-obj_name.
    TRY.
        DATA(ls_item) = build_item( ls_tadir ).   " same shape run_sequential/run_parallel build today
        DATA(lt_files) = zcl_abapgit_objects=>serialize( is_item = ls_item ... ).  " UNCHANGED call
        " oversized-object isolation (§6): if the combined EXPORT of lt_files
        " exceeds c_max_object_output_bytes, still return it (never drop data)
        " but flag via a dedicated MSGNO so the orchestrator learns to solo-batch
        " this object on any FUTURE occurrence in the SAME run.
        EXPORT files = lt_files TO DATA BUFFER ls_result-files_xstring.
        ls_result-rc = 0.
      CATCH zcx_abapgit_exception INTO DATA(lx_error).
        ls_result-rc    = 4.
        ls_result-msgid = lx_error->if_t100_message~t100key-msgid.
        ls_result-msgno = lx_error->if_t100_message~t100key-msgno.
    ENDTRY.
    GET RUN TIME FIELD DATA(lv_t1).
    ls_result-elapsed_ms = ( lv_t1 - lv_t0 ) / 1000.
    APPEND ls_result TO et_result.
  ENDLOOP.
  ev_output_row_count = lines( et_result ).
ENDFUNCTION.
```

One object's exception is caught INSIDE the loop, never aborting the
batch — this is the structural guarantee for partial-success preservation.

## 3. Work item and cost model

```abap
TYPES: BEGIN OF ty_work_item,
         tadir            TYPE zif_abapgit_definitions=>ty_tadir,
         est_ms           TYPE i,
         est_bytes        TYPE i,
         est_source       TYPE c LENGTH 1,  " 'E'=exact-run-local,
                                              " 'B'=type/size bucket,
                                              " 'F'=type family,
                                              " 'S'=static default
       END OF ty_work_item,
       tt_work_item TYPE STANDARD TABLE OF ty_work_item WITH EMPTY KEY.
```

`ZCL_ABAPGIT_ORTEC_SER_COST` (run-local only, no persistence — SER-6 is
deferred):

```text
get_estimate( iv_obj_type ) -> ( est_ms, est_bytes, est_source )
  1. IF a run-local EWMA sample exists for iv_obj_type (this run only,
     updated after every completed object in every batch): return it,
     est_source = 'E'.  (Note: keyed by TYPE, not TYPE+NAME - repeats of
     the exact same object within one run are rare/non-existent for a
     single serialize() call, so exact-object memoization would add
     complexity for no real benefit; keyed by type only.)
  2. ELSE: static per-type-family default table (owner-adjustable
     constants, seeded from THIS design's trace evidence):
       CLAS/INTF      -> est_ms = 55, est_bytes = 15_000   (from ~25s/458
                          objects average in classes-serial-main.txt,
                          rounded; INTF typically much lighter than CLAS
                          but no INTF-only trace exists yet - same bucket
                          until SER-4 measures them separately)
       DTEL/DOMA      -> est_ms = 15, est_bytes = 3_000    (DDIC-only,
                          no trace evidence yet - conservative low default,
                          UNKNOWN precision, flagged for SER-4 remeasurement)
       WAPA/generic   -> est_ms = 30, est_bytes = 8_000    (no trace
                          evidence, mid-range static default)
     est_source = 'F'.
  UPDATE(iv_obj_type, iv_actual_ms, iv_actual_bytes) after each batch
     result row: new_ewma = alpha * actual + (1-alpha) * old_ewma,
     alpha = 0.3 (fixed constant, deterministic, no learned parameter).
```

CORRECTED per AR-1-005: estimates alone are advisory for scheduling
ORDER/BATCH SIZE only — they are explicitly NOT the mechanism that admits
a batch to RFC dispatch. The actual-bytes admission check (§5.9) runs on
the REAL serialized provider-buffer size immediately before every dispatch,
independent of how accurate the estimate was. With that check in place,
the accuracy claim is scoped correctly to: "a wrong estimate can change
scheduling ORDER/BALANCE and, transiently, which objects are grouped
together, but never changes whether an object is processed, what its
output is, or whether an oversized payload is ever actually dispatched" —
this is the single most important safety property of the whole design and
is repeated in the adversarial-review answers, §6.

## 4. Planner

```text
ZCL_ABAPGIT_ORTEC_SER_PLANNER

build_initial_batches( it_work_items SORTED BY est_ms DESCENDING,
                        iv_worker_count, iv_row_limit, iv_byte_limit )
  -> LPT (longest-processing-time-first): walk the sorted list once,
     always adding the next item to the batch with the CURRENTLY SMALLEST
     accumulated estimated time among the iv_worker_count batches being
     built, subject to iv_row_limit/iv_byte_limit; if adding would exceed
     either limit, close that batch and open a new one instead (batch
     count can exceed worker count if items are large/numerous - excess
     batches simply queue for refill).
  -> produces an ordered QUEUE of batches, roughly worker-count many ready
     immediately, remainder queued.

refill( )  " called every time a batch's ET_RESULT callback is fully processed
  -> IF queue empty: no-op.
  -> ELSE: compute lv_remaining = count of un-dispatched work items.
           lv_target_batch_size = MAX( 1,
             CEIL( lv_remaining / ( iv_worker_count * c_shrink_factor ) ) )
           capped by iv_row_limit and iv_byte_limit as always.
           c_shrink_factor = 2 (owner-adjustable constant) - this is the
           classic guided self-scheduling shrink, ensuring the LAST few
           batches are small (reduces straggler/tail latency, satisfies
           "shrinking batches near the end").
  -> dispatch the new batch immediately via CALL FUNCTION
     'Z_ABAPGIT_ORTEC_SER_BATCH' STARTING NEW TASK, subject to
     c_max_in_flight_batches and c_max_in_flight_bytes (if either would be
     exceeded, DEFER refill until the next completing callback frees
     capacity - never silently drop work, never exceed the hard limit).
```

Determinism for equal weights (test seam): when two work items have
identical `est_ms`/`est_source`, the planner breaks ties by the item's
original TADIR order (stable sort) — this makes `build_initial_batches`
fully deterministic for a fixed input and fixed cost table, which is the
unit-test seam required by the owner brief ("deterministic scheduling with
equal weights").

## 5. Orchestrator loop and task identity/lifecycle

```text
REVISION_NOTE  Cycle-1 adversarial review (AR-1-001/002/003/004,
  .memory/reviews/serialization_adversarial_review.md) found the original
  §5 conflated "timeout" with "confirmed failure" and did not define how
  an async RFC callback is mapped back to logical batch/attempt identity,
  creating a real duplicate-merge / lost-update race. Cycles 4-5 then
  proposed an instance-lifetime/GC-retention mechanism to close this,
  which was ACCEPTED by adversarial review but was never verified against
  authoritative documentation, per bootstrap OD-13's own stated
  precondition. A dedicated OD-13 verification pass
  (.memory/reviews/serialization_adversarial_review.md, "OD-13 Correction"
  section) FOUND, from actual ABAP Keyword Documentation, that the
  proposed GC-retention behavior is documented ONLY for classic
  SET HANDLER event handlers (ABENGARBAGE_COLLECTOR_GLOSRY: "...for which
  no method is registered as an event handler"), NOT for aRFC
  `CALLING meth ON END OF TASK` callbacks — no equivalent statement exists
  in ABAPCALL_FUNCTION_STARTING. AR-1-001 is therefore REOPENED and this
  section is corrected again, this time to a mechanism that requires NO
  such proof at all: static (CLASS-DATA/CLASS-METHODS) ownership, which
  is basic, uncontroversial ABAP language semantics (class data and
  static methods exist for the entire internal session, independent of
  any object instance or garbage collection question). AR-1-001's finding
  history is preserved below, not erased.
```

### 5.0 Top-level flow

```text
zcl_abapgit_ortec_ser_orch=>serialize(...)   " STATIC entry point —
                                              " CORRECTED (OD-13 redesign):
                                              " no instance is required for
                                              " correctness; see §5.1a for
                                              " why static ownership is
                                              " the PROVEN-sufficient
                                              " mechanism, not a stepping
                                              " stone to a fancier one
  1. partition it_tadir into forced_sequential / batch_eligible using the
     REAL `zcl_abapgit_serialize=>is_no_parallel` predicate (per AR-1-008,
     see performance design §2 OD-6 — this design no longer recommends
     duplicating the check; it calls the real one via a minimal, safe
     visibility change) plus `lv_max = 1`.
  2. route forced_sequential through `route_to_sequential_fallback` (§5.7)
     immediately — identical call shape to today's `run_sequential`.
  3. build work items for batch_eligible via the cost estimator (§3).
  4. planner->build_initial_batches(...) (§4).
  5. dispatch_batch(...) (§5.2) for each ready initial batch, subject to
     `c_max_in_flight_batches`/`c_max_in_flight_bytes`/the actual-bytes
     admission check (§5.9).
  6. run the bounded poll loop (§5.3) until every work item THIS RUN
     dispatched is resolved (`mt_resolved` covers 100% of
     `batch_eligible` ∪ `forced_sequential` for `lv_run_id`) or the
     circuit breaker (§5.8) has routed everything remaining to fallback.
  7. Once per poll iteration (§5.3), if a cancellation predicate is
     available (`should_cancel( )`, see AR-1-010 — default implementation
     returns `abap_false` always, since SER-0 §7 G-6 found no existing
     standard cancellation flag; this is a decision-free, always-safe
     extension point, not an invented mechanism) and it returns
     `abap_true`: stop producing NEW dispatches, let in-flight dispatches
     drain normally via the SAME poll loop, do not force-kill any task.
  8. purge_run_state( lv_run_id ) — remove THIS run's OWN rows from the
     static tables (§5.1a) once every dispatch of `lv_run_id` has reached
     a terminal state (`'R'`/`'F'`/`'D'`); any dispatch still
     `'T'`/LOGICALLY_ABANDONED at this point is deliberately LEFT in the
     static table (bounded per §5.1a's cleanup policy) so a late callback
     can still be safely drained. This step ALSO deletes THIS run's own
     rows from `mt_task_outcomes` (§5.8) and removes `lv_run_id` from
     `mt_broken_runs` (§5.8) if present — the circuit breaker's state is
     run-scoped, so it is purged alongside the rest of the run's state and
     can never leak into or affect a later, unrelated run.
  9. RETURN `mt_files` — identical shape to today's `serialize()` return.
```

### 5.1a Callback-target lifetime: documented proof, redesign, and the
explicit abandonment/resource state model (SUPERSEDES the cycle-4/5
instance-lifetime mechanism; RESOLVES AR-1-001 for real this time)

```text
DOC_PROVES_CALLBACK_TARGET_RETENTION=NO
DOC_PROVES_CALLBACK_AFTER_CALLER_RETURN=YES (conditional — see citation
  below)
DOC_PROVES_WORKER_CANCELLATION_ON_RETURN=NO
DOC_PROVES_RFC_RESOURCE_RECLAMATION_ON_RETURN=NO
```

**Documentation evidence (ABAP Keyword Documentation, fetched and cited
directly, not from memory):**

- `ABENGARBAGE_COLLECTOR_GLOSRY`: "Deletes objects that are no longer
  referenced by heap references or field symbols **and for which no
  method is registered as an event handler**." — this GC-exemption is
  documented explicitly and ONLY for the classic OO **event handler**
  mechanism (`SET HANDLER`/`FOR EVENT ... OF`), a DIFFERENT ABAP language
  construct from asynchronous RFC's `CALLING meth ON END OF TASK`. No
  analogous statement exists anywhere in `ABAPCALL_FUNCTION_STARTING`,
  `ABAPRECEIVE`, `ABAPWAIT_ARFC`, or `ABENNEWS-40-RFC` for aRFC callback
  methods. **The cycle-4/5 "GC-retention" mechanism was therefore built on
  an unproven analogy between two distinct language features, which the
  documentation does not support — exactly the failure mode the owner's
  OD-13 correction task warned against.**
- `ABAPCALL_FUNCTION_STARTING`: "A prerequisite for the execution of a
  registered callback routine is that **the calling program still exists
  in its internal session** when the remote function is terminated. It is
  then executed here at the next change of the work process in a
  roll-in. If the program was terminated or is located on the stack as
  part of a call sequence, the callback routine is not executed." — this
  is the ACTUAL documented guarantee: callback delivery is scoped to
  **internal-session and call-sequence persistence**, not to any specific
  object reference surviving. This is what `DOC_PROVES_CALLBACK_AFTER_
  CALLER_RETURN=YES` is based on: a plain ABAP OO method returning
  (`serialize()` returning to its caller) does NOT terminate the internal
  session or pop it off the call sequence — the session continues running
  the rest of the abapGit UI flow — so a callback dispatched from inside
  `serialize()` remains DELIVERABLE per this documented rule, PROVIDED
  something can still resolve `meth` when the runtime tries to invoke it.
- `ABAPWAIT_ARFC`: "If the addition `UP TO sec SECONDS` is used to cancel
  the wait time, it does not mean that any outstanding callback routines
  are no longer executed at all. A later change of the work process in
  the same program can result in the callback routines of the
  asynchronous functions executed until now being executed. **Only the
  callback routines of those asynchronous functions not ended at the end
  of the program are not executed.**" — confirms: giving up a `WAIT`
  early does not lose a LATER-arriving callback within the same session,
  and callbacks for tasks that never finish before the program/session
  itself ends are simply never executed (no dangling execution, no crash
  — just silently skipped, matching `LOGICALLY_ABANDONED` below).
- `ABAPRECEIVE` / `ABAPCALL_FUNCTION_STARTING`: "If no `RECEIVE` statement
  is executed in the callback routine..., the connection is preserved and
  implicitly behaves like `RECEIVE ... KEEPING TASK`." — this describes
  the case where a callback DID run but forgot to call `RECEIVE`; it says
  nothing about a task whose callback never runs at all because the
  session ended first, hence `DOC_PROVES_RFC_RESOURCE_RECLAMATION_ON_
  RETURN=NO` (not addressed either way).
- Nothing in any fetched document states that dispatching
  `STARTING NEW TASK` and later abandoning the wait cancels or aborts the
  remotely running function module — the remote function keeps running to
  completion regardless of what the caller does, hence
  `DOC_PROVES_WORKER_CANCELLATION_ON_RETURN=NO`.

**Redesign (Option A, static/class-level ownership — the SMALLEST proven
option, not a more complex one, per the owner's own instruction "do not
choose a more complex option when a smaller proven option is sufficient"):**

```text
CLASS zcl_abapgit_ortec_ser_orch DEFINITION.
  PUBLIC SECTION.
    CLASS-METHODS serialize IMPORTING ... RETURNING VALUE(rt_files) TYPE ... .
    CLASS-METHODS on_end_of_batch IMPORTING p_task TYPE clike.  " PUBLIC,
                                                                 " STATIC —
                                                                 " matches
                                                                 " ABAPCALL_
                                                                 " FUNCTION_
                                                                 " STARTING's
                                                                 " own
                                                                 " statement
                                                                 " that meth
                                                                 " "must be
                                                                 " public"
                                                                 " and can
                                                                 " use
                                                                 " general
                                                                 " method-
                                                                 " call
                                                                 " syntax,
                                                                 " which
                                                                 " explicitly
                                                                 " includes
                                                                 " static
                                                                 " (class=>
                                                                 " method)
                                                                 " calls
  PRIVATE SECTION.
    CLASS-DATA mt_dispatch      TYPE ty_dispatch_tt.   " keyed by task_name
                                                        " (globally unique
                                                        " across the whole
                                                        " internal session,
                                                        " §5.1)
    CLASS-DATA mt_resolved      TYPE ty_resolved_tt.   " keyed by
                                                        " run_id + obj_type
                                                        " + obj_name
    CLASS-DATA mt_task_outcomes TYPE ty_outcome_tt.    " keyed by run_id +
                                                        " seq, windowed per
                                                        " run_id (§5.8)
ENDCLASS.
```

Dispatch is registered as `CALLING zcl_abapgit_ortec_ser_orch=>
on_end_of_batch ON END OF TASK` (a STATIC method reference, not `me->`).
`CLASS-DATA` and `CLASS-METHODS` are guaranteed by basic ABAP language
semantics (not requiring the citation above at all — this is definitional)
to exist for the entire life of the program's internal session, which is
EXACTLY the scope the documentation proves is required for callback
delivery (`ABAPCALL_FUNCTION_STARTING`'s "calling program still exists in
its internal session" clause) — no more, no less, and with zero object-
instance/garbage-collection ambiguity of any kind. This is why it counts
as the SMALLEST proven option: it does not introduce a new class, a new
DDIC persistence layer, or a long-lived explicit owner object (Option B)
— it uses a feature ABAP already guarantees for exactly this lifetime.

**Cross-run isolation with shared static storage:** `task_name` is
globally unique for the WHOLE internal session (`|SER-{run_guid}-{seq}|`,
§5.1), so `on_end_of_batch`'s `READ TABLE mt_dispatch ... WITH TABLE KEY
task_name = p_task` can NEVER resolve a callback against a different run's
dispatch, regardless of how many runs' rows coexist in the shared static
table. `mt_resolved` is additionally keyed by `run_id` so two runs'
identical `obj_type`/`obj_name` pairs (e.g. both serializing `CLAS
ZCL_FOO`) never collide. **The same rule applies to the circuit breaker
(§5.8, fixed per DR-006/AR-OD13-001): `mt_task_outcomes` is keyed by
`run_id` and windowed PER `run_id`, and `mt_broken_runs` records tripped
breakers PER `run_id` — a systemic RFC outage during one run can never
cause a later, unrelated run sharing the same static storage to start out
already broken.**

**Explicit abandonment/resource state model (per owner instruction — do
NOT equate logical abandonment with cancellation or resource release):**

```text
LOGICALLY_ABANDONED       This run's poll loop (§5.3) stopped waiting for
                          this specific dispatch (state 'T'/'X'). Means
                          ONLY that: WE stopped waiting. Says NOTHING
                          about whether the worker is still running,
                          finished, or whether any resource was freed.
CALLBACK_PENDING          Dispatched, no callback delivered yet (covers
                          both 'A' AWAITING and LOGICALLY_ABANDONED 'T'/
                          'X' — abandonment does not change this state
                          from the RUNTIME's perspective, only from ours).
CALLBACK_RECEIVED         RECEIVE executed with sy-subrc = 0 (state 'R',
                          or the drain path's 'D' for an abandoned
                          dispatch whose callback arrived after all).
RECEIVE_FAILED            RECEIVE executed with sy-subrc <> 0 (state 'F').
WORKER_TERMINATED         NOT OBSERVABLE by the calling program via any
                          documented ABAP-language signal for an abandoned
                          task. Can only be inferred indirectly (a later
                          callback firing implies it). Absence of a
                          callback proves nothing on its own.
RFC_RESOURCE_RECLAIMED    NOT OBSERVABLE by the calling program. Owned by
                          the SAP kernel/gateway/work-process lifecycle,
                          independent of application code (confirmed by
                          the documentation excerpts above, which describe
                          RFC-session-close behavior only in terms of
                          RECEIVE/KEEPING TASK during an EXECUTED
                          callback, never for a callback that never runs).
RESOURCE_STATE_UNKNOWN    The DEFAULT, HONEST state for any
                          LOGICALLY_ABANDONED dispatch until it reaches
                          CALLBACK_RECEIVED or RECEIVE_FAILED (or is
                          administratively purged, below). This design
                          NEVER reports or logs a `LOGICALLY_ABANDONED`
                          task as "cancelled" or "resource freed" — those
                          claims are never made.
```

**Bounded limits (all owner-adjustable constants, safe defaults):**

```text
max_abandoned_tasks_per_run                  50   observability threshold
                                                   only (abandonment itself
                                                   is driven by real
                                                   timeouts already bounded
                                                   by c_max_retries/§5.6 —
                                                   this constant governs
                                                   when to LOG a
                                                   degraded-run warning,
                                                   not a hard stop)
max_abandoned_runs_per_internal_session      20   once this many DISTINCT
                                                   run_ids each have >=1
                                                   still-undrained
                                                   abandoned dispatch, the
                                                   OLDEST such run's rows
                                                   are forcibly purged from
                                                   the static tables (its
                                                   own future late
                                                   callback, if any, then
                                                   hits the existing
                                                   "unknown task_name"
                                                   defensive path — RECEIVE
                                                   + discard, §5.5,
                                                   already safe)
max_abandoned_tasks_per_internal_session     200  hard cap on TOTAL
                                                   undrained abandoned rows
                                                   across ALL runs
                                                   combined; once hit,
                                                   purge the OLDEST
                                                   abandoned rows
                                                   (any run) until back
                                                   under the cap
retained_metadata_byte_limit                 2 MB each retained row is
                                                   metadata-only (task_name
                                                   char40, run_id char32,
                                                   a handful of small
                                                   fields, and a bounded
                                                   <=25-row TADIR-key list
                                                   per §9's
                                                   c_max_batch_rows) — NO
                                                   xstring/serialized
                                                   payload is ever retained
                                                   in mt_dispatch; 200 rows
                                                   at this shape is
                                                   trivially under 2 MB,
                                                   stated explicitly as a
                                                   hard ceiling, not just
                                                   an expectation
breaker_behavior_after_abandonment                unchanged from §5.8 —
                                                   abandonment (timeout)
                                                   already counts as a
                                                   failed outcome in the
                                                   sliding window (§5.4
                                                   resubmits or falls back
                                                   on exhausted retries;
                                                   either path records a
                                                   task_outcome)
new_tasks_after_threshold                    NO — once
                                                   max_abandoned_tasks_per_
                                                   internal_session is hit,
                                                   this run (and any other
                                                   run active at that
                                                   moment) does not stop
                                                   dispatching because of
                                                   THIS limit specifically
                                                   (that would conflate
                                                   session-wide bookkeeping
                                                   pressure with THIS run's
                                                   own health) — only the
                                                   PURGE described above
                                                   happens; the SEPARATE,
                                                   already-existing
                                                   per-run circuit breaker
                                                   (§5.8) is what actually
                                                   stops a degraded run's
                                                   own new dispatches
mt_task_outcomes_bound                            PER-RUN, not global:
                                                   capped at
                                                   c_breaker_window_size
                                                   (default 10) rows PER
                                                   ACTIVE run_id (§5.8),
                                                   purged entirely at
                                                   purge_run_state (§5.0
                                                   step 8); total size
                                                   across the session is
                                                   proportional to the
                                                   number of CURRENTLY-
                                                   ACTIVE runs only, never
                                                   to session history
mt_broken_runs_bound                              at most one row per
                                                   DISTINCT run_id that has
                                                   ever tripped the breaker
                                                   AND not yet purged;
                                                   removed at
                                                   purge_run_state (§5.0
                                                   step 8) same as above -
                                                   bounded by concurrently-
                                                   active runs, not session
                                                   history
```

**What remains an explicit, owner-visible boundary (not solved, not
hidden):** if the entire internal session ends (user closes the session,
or it is killed) before a truly-hung worker's task completes, that task's
result is permanently lost — `ABAPWAIT_ARFC`'s own documentation confirms
this is simply how aRFC behaves ("only the callback routines of those
asynchronous functions not ended at the end of the program are not
executed"), identical for TODAY's existing standard `run_parallel`
mechanism. This remains a SAP Basis/gateway/work-process-timeout
operational concern, not something this or any application-level design
resolves.

### 5.1b Minimal IT8 empirical verification test (SER-SLICE-2 requirement)

Even though §5.1a's mechanism is now documentation-proven for the
scope it actually claims (internal-session-scoped callback delivery via
static ownership), the owner's OD-13 correction task requires a concrete
empirical test for SER-SLICE-2, independent of the documentation
argument. This test uses a dedicated test seam (a test-only RFC path with
an artificial, controllable delay), never a permanent production delay:

```text
T-DRAIN-1  Add a test-only function module (or a test-only branch inside
           Z_ABAPGIT_ORTEC_SER_BATCH gated by a parameter never set in
           production, e.g. IV_TEST_DELAY_S) that sleeps for a controlled
           duration before returning, to simulate a slow/hung worker.
T-DRAIN-2  From an ABAP Unit test (or a small test report, since real
           aRFC dispatch needs a live work process — this is an IT8-only
           test, not a unit test that can run in a mocked environment),
           dispatch ONE such delayed batch via the normal
           zcl_abapgit_ortec_ser_orch=>serialize() entry point, with
           c_batch_rfc_timeout_s/c_max_drain_wait_s reduced (via the same
           test seam) to a few seconds so the test does not need to
           actually wait minutes.
T-DRAIN-3  Force the poll loop to give up waiting (let the reduced timeout
           elapse) so the dispatch transitions 'A' -> 'T' -> (after
           c_max_drain_wait_s) -> 'X', and confirm serialize() RETURNS
           within the expected bound (assert elapsed time < timeout +
           drain window + a small margin).
T-DRAIN-4  Let the test's artificially-delayed worker finish AFTER
           serialize() has already returned. Confirm (via a test hook
           reading the static mt_dispatch table, or via a log entry
           on_end_of_batch is required to write when it drains a 'T'/'X'
           row) that the late callback DID execute and DID resolve
           against the correct run_id/task_name (proves run_id and
           task_name correlation survives past the method return, per
           T-DRAIN-item 7's requirement).
T-DRAIN-5  Confirm mt_files/mt_resolved were NOT double-updated by the
           late callback (the object was already routed to sequential
           fallback per §5.4/§5.6 when abandoned — the late callback must
           be DRAINED and DISCARDED, not merged) — this is the core
           correctness property, not just a liveness one.
T-DRAIN-6  Immediately after T-DRAIN-4/5, start a SECOND, completely
           unrelated serialize() call (different repository/run_id) in
           the SAME session, and confirm its own results are unaffected
           by the first run's drained late callback — proves no cross-run
           contamination (task_name uniqueness via GUID, §5.1, makes this
           true by construction, but this test proves it empirically too).
T-DRAIN-7  Repeat T-DRAIN-1..6 enough times (e.g. 25 abandoned dispatches
           across several repeated test runs) to exceed
           max_abandoned_tasks_per_run (50 is the default; use a lower
           test-seam value to make this practical) and confirm the
           abandoned-dispatch-ledger purge policy (§5.1a) actually fires:
           the OLDEST abandoned run's rows are purged once
           max_abandoned_runs_per_internal_session is exceeded, and a
           late callback for a PURGED run safely hits the existing
           "unknown task_name" defensive path (RECEIVE + discard, §5.5) —
           confirms no crash and no unbounded growth (inspect the static
           table's row count before/after via a test hook and assert it
           stays under the documented byte/row limits).
T-DRAIN-8  Confirm, via ST22 (no dumps) and SM50/SM66 (no orphaned work
           processes attributable to this test beyond the SAP kernel's own
           normal timeout-based reclamation) after the test run completes,
           that no resource accumulation is observable from the
           application side — this is the RFC_RESOURCE_RECLAIMED /
           RESOURCE_STATE_UNKNOWN boundary from §5.1a, checked empirically
           rather than just documented as "not our concern."
REQUIRED_BEFORE  SLICE 2 is considered DONE, not before SLICE 2 code is
           written (this is a validation test alongside the implementation,
           per the standard implementation-readiness flow, not a
           precondition that blocks starting the slice).
```

### 5.1 Identity

Every dispatch (an original batch OR any retry/bisection of it) is a
distinct, NEVER-REUSED **attempt** with its own RFC task name:

```text
lv_run_id     generated ONCE per `serialize()` call (a REAL GUID via
              `cl_system_uuid=>create_uuid_x16_static( )` — REQUIRED,
              not optional; see §5.1a — with static/CLASS-DATA ownership,
              a GUID is what makes cross-run isolation in shared storage
              actually work, not merely a task_name-collision nicety).
task_name     = |SER-{lv_run_id}-{lv_dispatch_seq}|
              lv_dispatch_seq is a per-run monotonically increasing
              counter, incremented on EVERY dispatch including retries —
              a retry of batch_id=7 gets a NEW task_name, never re-sends
              the old one.
batch_id      logical grouping id, STABLE across an original dispatch and
              its retries/bisections (used only for telemetry/log
              correlation, NEVER for callback lookup).
attempt       1 for the first dispatch of a given object group, +1 per
              retry/bisection of that group.
```

A run-local table `mt_dispatch` (HASHED, key `task_name`) is the single
source of truth:

```abap
TYPES: BEGIN OF ty_dispatch,
         task_name   TYPE char40,     " unique key, never reused this session
         run_id      TYPE sysuuid_x16, " CORRECTED per DR-005/AR-OD13-001:
                                        " explicit field, not just implied by
                                        " parsing task_name - every lookup/
                                        " filter below uses this directly
         batch_id    TYPE char32,
         attempt     TYPE i,
         object_keys TYPE zif_abapgit_definitions=>ty_tadir_tt, " exact objects in THIS dispatch
         state       TYPE c LENGTH 1, " 'A'=AWAITING 'R'=RECEIVED
                                       " 'T'=TIMED_OUT 'D'=DRAINED
                                       " 'F'=RECEIVED_FAILURE
         dispatch_ts TYPE timestampl,
       END OF ty_dispatch.
```

A second table `mt_resolved` (HASHED, key `run_id` + `obj_type` +
`obj_name` — **CORRECTED per DR-005/AR-OD13-001: `run_id` is now part of
the key, not optional or implied.** With `mt_dispatch`/`mt_resolved` now
CLASS-DATA shared across the whole internal session (§5.1a), two
DIFFERENT runs serializing the SAME object (e.g. the same `CLAS` in two
different repositories, or two pulls of the same repository in one
session) would otherwise silently skip a real, distinct object for the
SECOND run merely because the FIRST run already resolved an object with
the same `obj_type`/`obj_name` — a real, cross-run data-loss bug the
original keying missed) records, EXACTLY ONCE per object PER RUN, that an
object's result has been merged into `mt_files` or terminally logged as
failed — this is the belt-and-suspenders guard the retest in AR-1-001 asked for:
**no ET_RESULT row is ever merged, from any RECEIVE, without first
checking `mt_resolved` and skipping if already present.**

### 5.2 Dispatch

```text
dispatch_batch( it_object_keys, iv_attempt, iv_batch_id )
  lv_dispatch_seq += 1.
  lv_task_name = |SER-{lv_run_id}-{lv_dispatch_seq}|.
  INSERT VALUE ty_dispatch( task_name = lv_task_name run_id = lv_run_id
    batch_id = iv_batch_id attempt = iv_attempt
    object_keys = it_object_keys state = 'A' dispatch_ts = <now> )
    INTO TABLE mt_dispatch.   " CLASS-DATA, §5.1a - CORRECTED per
                              " AR-OD13-001 closure re-check: run_id MUST
                              " be stamped at INSERT time, not left
                              " initial - every downstream read (§5.4-5.7)
                              " relies on `<ls_d>-run_id` already being
                              " correct
  CALL FUNCTION 'Z_ABAPGIT_ORTEC_SER_BATCH' STARTING NEW TASK lv_task_name
    DESTINATION IN GROUP iv_group
    CALLING zcl_abapgit_ortec_ser_orch=>on_end_of_batch ON END OF TASK   " STATIC
                                                                          " callback, §5.1a
    EXPORTING iv_batch_id = iv_batch_id iv_attempt = iv_attempt
      it_tadir = it_object_keys <+ provider buffers, see §5.5>
      iv_input_row_count = lines( it_object_keys ) iv_input_version = 1.
```

### 5.3 Poll loop (replaces the single blanket WAIT; fixes AR-1-001,
cycle 2 rejection — the loop must not exit while any dispatch could still
produce an observable callback effect)

```text
DO.
  WAIT UNTIL <SPBT_INSTANCE or lv_free_slot indicator changes>
    UP TO 5 SECONDS.   " bounded poll, not a single 120s wait
  check_timeouts( ).   " see 5.4 — may transition 'T' -> 'X' (ABANDONED)
  IF planner queue empty AND NOT EXISTS mt_dispatch WHERE state = 'A'
     AND NOT EXISTS mt_dispatch WHERE state = 'T'.
    EXIT.   " only 'R'/'F'/'D'/'X' rows may remain — none of these can
            " still produce a merge or an unaccounted side effect
  ENDIF.
ENDDO.
```

The loop therefore does NOT exit merely because there are no more `'A'`
(awaiting) rows — a `'T'` (TIMED_OUT) row also blocks exit, because its
originating RFC task may still be genuinely running and could still
deliver a late callback. This is what actually closes the cycle-2
counterexample (retry B2 succeeds while original B1 is still `'T'` — the
loop keeps polling until B1 is resolved one way or the other).

### 5.4 Timeout handling (fixes AR-1-001/003: timeout is NEVER treated as
a confirmed failure, it creates a superseded attempt; adds a bounded
ABANDONED terminal state so the poll loop in §5.3 is guaranteed to
terminate even if a straggler RFC task never calls back at all)

```text
check_timeouts( )
  LOOP AT mt_dispatch ASSIGNING <ls_d> WHERE state = 'A'
       AND dispatch_ts < ( now - c_batch_rfc_timeout_s ).
    <ls_d>-state = 'T'.                       " TIMED_OUT, not "failed"
    release_in_flight_budget( <ls_d> ).       " frees capacity for refill
    IF <ls_d>-attempt < c_max_retries.
      resubmit( <ls_d>-object_keys, <ls_d>-attempt + 1, <ls_d>-batch_id ).
    ELSE.
      route_to_sequential_fallback( <ls_d>-run_id, <ls_d>-object_keys ).  " §5.7
    ENDIF.
  ENDLOOP.

  " NEW: bound how long §5.3 will wait for a 'T' row's late callback.
  LOOP AT mt_dispatch ASSIGNING <ls_d> WHERE state = 'T'
       AND dispatch_ts < ( now - c_max_drain_wait_s ).
    <ls_d>-state = 'X'.   " ABANDONED — a terminal state for loop-exit
                          " purposes ONLY; behaves identically to 'T' in
                          " on_end_of_batch (§5.5) if a callback for it
                          " EVER still arrives (still safely drained, still
                          " never merged) — the sole difference is that
                          " 'X' no longer blocks §5.3's exit condition.
  ENDLOOP.
```

`c_max_drain_wait_s` (new constant, default = `c_batch_rfc_timeout_s`, i.e.
total worst case ≈ 2 × 300s = 600s for one straggler before the run stops
waiting for it) bounds `serialize()`'s own return time to a fixed ceiling
regardless of how many objects a stray task's late callback would have
affected — its object_keys were ALREADY resolved via the resubmitted
retry or the final-attempt fallback in `check_timeouts()` above, so
abandoning the wait loses no data, only the (already-superseded) original
RFC session's eventual, harmless, drained callback.

A `'T'`/`'X'` dispatch's callback is **not deregistered** — it stays in
`mt_dispatch` so a LATE arrival can still be recognized and safely drained
(§5.5), rather than looking up a missing key.

### 5.5 Callback handling (`on_end_of_batch`, ABAP binds the real task
name via the standard `p_task` parameter — fixes AR-1-002: lookup is by
the ACTUAL RFC task name, never by an invented/guessed identifier; ALSO
fixes the cycle-2 rejection by validating result-set membership against
the dispatch's own object_keys before any merge, not just checking
`mt_resolved` for duplicates)

```text
on_end_of_batch( p_task ).
  READ TABLE mt_dispatch ASSIGNING <ls_d> WITH TABLE KEY task_name = p_task.
  IF sy-subrc <> 0.
    " defensive: unknown/already-purged task name (e.g. the abandoned-
    " dispatch-ledger purge, §5.1a, already removed this row) - must still
    " RECEIVE to free the RFC resource, then discard. CORRECTED per
    " AR-OD13-002: use the SAME EXCEPTIONS handling as every other RECEIVE
    " in this method, never a bare/unchecked call, so a late failed RFC
    " callback for an already-purged run cannot raise an uncaught runtime
    " error here.
    RECEIVE RESULTS FROM FUNCTION 'Z_ABAPGIT_ORTEC_SER_BATCH'
      IMPORTING et_result = DATA(lt_purged_discard)
      EXCEPTIONS system_failure = 1 communication_failure = 2 OTHERS = 3.
    " sy-subrc is intentionally ignored beyond this point - there is no
    " dispatch row left to update and nothing further to do; the RECEIVE
    " call's only job here is to free the RFC resource cleanly.
    RETURN.
  ENDIF.

  CASE <ls_d>-state.
    WHEN 'T' OR 'X'.  " late callback for an already-superseded/abandoned attempt
      RECEIVE RESULTS FROM FUNCTION 'Z_ABAPGIT_ORTEC_SER_BATCH'
        IMPORTING et_result = DATA(lt_discard)
        EXCEPTIONS system_failure = 1 communication_failure = 2 OTHERS = 3.
      <ls_d>-state = 'D'.                     " DRAINED
      RETURN.                                 " NEVER merge, NEVER update
                                               " cost EWMA from a drained
                                               " (superseded) attempt

    WHEN 'A'.  " normal, first-and-only callback for this attempt
      RECEIVE RESULTS FROM FUNCTION 'Z_ABAPGIT_ORTEC_SER_BATCH'
        IMPORTING et_result = DATA(lt_result) ev_output_row_count = DATA(lv_out_rows)
        EXCEPTIONS system_failure = 1 communication_failure = 2 OTHERS = 3.
      IF sy-subrc <> 0.
        <ls_d>-state = 'F'.                   " RECEIVED_FAILURE (confirmed,
                                               " not a guess)
        release_in_flight_budget( <ls_d> ).
        record_task_outcome( <ls_d>-run_id, abap_false ).  " §5.8, per-RUN
                                                            " sliding window
        handle_receive_failure( <ls_d> ).      " §5.6, ALWAYS bisects
        RETURN.
      ENDIF.

      " NEW result-set integrity check (fixes cycle-2 AR-1-002 rejection):
      " reject the WHOLE result if it does not exactly match what was
      " requested, rather than trusting mt_resolved alone as the only
      " defense.
      IF lv_out_rows <> lines( <ls_d>-object_keys )
         OR NOT ( object_key_sets_equal( lt_result, <ls_d>-object_keys ) ).
        <ls_d>-state = 'F'.
        release_in_flight_budget( <ls_d> ).
        record_task_outcome( <ls_d>-run_id, abap_false ).
        ii_log->add_warning( |ORTEC batch { p_task } returned a result set | &&
                              |that does not match its requested object list - | &&
                              |treating as failed, routing to fallback| ).
        route_to_sequential_fallback( <ls_d>-run_id, <ls_d>-object_keys ).   " never trust
                                                               " a mismatched
                                                               " result
        RETURN.
      ENDIF.

      <ls_d>-state = 'R'.
      release_in_flight_budget( <ls_d> ).
      record_task_outcome( <ls_d>-run_id, abap_true ).       " §5.8
      LOOP AT lt_result INTO DATA(ls_row).
        CHECK NOT line_exists( mt_resolved[ run_id = <ls_d>-run_id
                                             obj_type = ls_row-obj_type
                                             obj_name = ls_row-obj_name ] ).
        " ^ still kept as a second, independent duplicate guard, NOW
        " correctly scoped by run_id (fixes DR-005/AR-OD13-001)
        IF ls_row-rc = 0.
          merge_into_mt_files( ls_row ).
        ELSE.
          log_object_failure( ls_row ).      " same message shape as today
        ENDIF.
        cost_estimator->update( ls_row-obj_type, ls_row-elapsed_ms, ls_row-output_bytes ).
        INSERT VALUE #( run_id = <ls_d>-run_id obj_type = ls_row-obj_type
          obj_name = ls_row-obj_name ) INTO TABLE mt_resolved.
      ENDLOOP.
      planner->refill( ).

    WHEN 'R' OR 'D' OR 'F'.
      RETURN.  " duplicate callback for an already-terminal state, ignore
  ENDCASE.
```

`object_key_sets_equal` is a simple set-equality check (same
obj_type+obj_name pairs, same count) between `ET_RESULT`'s rows and the
dispatch's own `object_keys` — it does not require any NEW export
parameter on `Z_ABAPGIT_ORTEC_SER_BATCH` (§2's `ET_RESULT` already carries
`OBJ_TYPE`/`OBJ_NAME` per row, and `EV_OUTPUT_ROW_COUNT` already exists),
so this fix requires no RFC signature change.

### 5.6 RECEIVE failure and oversized/transfer-limit handling (fixes
AR-1-004: never rely on `OUTPUT_BYTES` — that field is only known on
success; a RECEIVE failure ALWAYS bisects deterministically)

```text
handle_receive_failure( ls_d )
  IF lines( ls_d-object_keys ) > 1.
    split ls_d-object_keys into two halves.
    resubmit( half_1, ls_d-attempt + 1, ls_d-batch_id ).
    resubmit( half_2, ls_d-attempt + 1, ls_d-batch_id ).
    " bisection continues on any further RECEIVE failure of a half,
    " independent of c_max_retries counting per half (each half gets its
    " own fresh attempt budget; the sliding-window circuit breaker, §5.8,
    " now correctly scoped per run_id (fixes DR-006/AR-OD13-001), counts
    " EVERY one of these confirmed failures individually, so a systemic
    " outage trips THIS run's breaker quickly regardless of how many
    " bisection halves are in flight — fixes AR-2-001)
  ELSE.
    " already a single object and RFC transfer/communication still failed
    route_to_sequential_fallback( ls_d-run_id, ls_d-object_keys ).  " never
                                                        " retried via RFC
                                                        " again for this
                                                        " object
  ENDIF.
```

### 5.7 Sequential fallback routing (never loses an object)

```text
route_to_sequential_fallback( iv_run_id, it_object_keys )
  LOOP AT it_object_keys INTO DATA(ls_key).
    CHECK NOT line_exists( mt_resolved[ run_id = iv_run_id
                                         obj_type = ls_key-object obj_name = ls_key-obj_name ] ).
    TRY.
        DATA(lt_files) = zcl_abapgit_objects=>serialize( is_item = build_item( ls_key ) ... ).
        merge_into_mt_files( lt_files ).
      CATCH zcx_abapgit_exception INTO DATA(lx_error).
        ii_log->add_exception( lx_error ).      " identical shape to today's
                                                 " run_sequential error path
    ENDTRY.
    INSERT VALUE #( run_id = iv_run_id obj_type = ls_key-object obj_name = ls_key-obj_name )
      INTO TABLE mt_resolved.
  ENDLOOP.
```

### 5.8 Global circuit breaker (added — see performance review F4;
REVISED per AR-2-001/cycle-2 performance finding: per-TASK, sliding-window
accounting, not per-logical-group consecutive-reset; REVISED AGAIN per
DR-006/AR-OD13-001: now explicitly RUN-scoped `CLASS-DATA`, since a
session-wide flat buffer/flag would let one run's outage permanently
degrade every later, unrelated run in the same session)

```text
REVISION_NOTE  Cycle-2 review found the original "consecutive dispatch
  failures, reset on any success" design (a) under-counted systemic
  failure by deduplicating bisection retries of the SAME logical group,
  allowing O(batch size) failing RFC round trips before the breaker
  tripped, and (b) could be indefinitely reset by one lucky success amid
  an otherwise-flapping outage. The OD-13 correctness review then found
  that making mt_dispatch/mt_resolved CLASS-DATA (§5.1a) without also
  re-scoping the breaker's own state by run_id would let one run's outage
  incorrectly "leak" into and permanently break every subsequent,
  unrelated run for the rest of the session. This revision fixes all
  three issues with one run_id-keyed mechanism.
```

```abap
TYPES: BEGIN OF ty_outcome,
         run_id  TYPE sysuuid_x16,
         seq     TYPE i,           " monotonic per run_id, oldest-first
         success TYPE abap_bool,
       END OF ty_outcome.
CLASS-DATA mt_task_outcomes TYPE STANDARD TABLE OF ty_outcome.  " CLASS-DATA,
                                                                 " windowed
                                                                 " PER
                                                                 " run_id
CLASS-DATA mt_broken_runs   TYPE HASHED TABLE OF sysuuid_x16
                            WITH UNIQUE KEY table_line.  " the set of
                                                          " run_ids whose
                                                          " breaker has
                                                          " tripped -
                                                          " REPLACES a
                                                          " single shared
                                                          " mv_ortec_batch_
                                                          " broken flag

record_task_outcome( iv_run_id, iv_success )
  DATA(lt_this_run_before) = FILTER #( mt_task_outcomes WHERE run_id = iv_run_id ).
  DATA(lv_next_seq) = COND i( WHEN lt_this_run_before IS INITIAL THEN 1
    ELSE REDUCE i( INIT max_seq = 0 FOR ls_o IN lt_this_run_before
                   NEXT max_seq = nmax( val1 = max_seq val2 = ls_o-seq ) ) + 1 ).
                                          " seq is scoped to THIS run_id
                                          " only - no cross-run ordering
                                          " is implied or needed
  APPEND VALUE #( run_id = iv_run_id seq = lv_next_seq success = iv_success )
    TO mt_task_outcomes.
  " keep only the last c_breaker_window_size entries FOR THIS run_id
  " (other runs' entries are untouched - filtering, not a shared FIFO)
  DATA(lt_this_run) = FILTER #( mt_task_outcomes WHERE run_id = iv_run_id ).
  IF lines( lt_this_run ) > c_breaker_window_size.
    DELETE mt_task_outcomes WHERE run_id = iv_run_id
      AND seq <= ( lv_next_seq - c_breaker_window_size ).
  ENDIF.
  IF lines( lt_this_run ) >= c_breaker_min_sample
     AND count_false( lt_this_run ) / lines( lt_this_run )
         >= c_breaker_failure_ratio.
    INSERT iv_run_id INTO TABLE mt_broken_runs.
  ENDIF.
```

Every CONFIRMED RFC task outcome counts — a distinct dispatch, EVERY
bisection half, EVERY retry — nothing is deduplicated by `batch_id` (fixes
AR-2-001: a systemic outage now trips the breaker within
`c_breaker_min_sample` confirmed task failures, not after O(batch size)
of them). A ratio-based sliding window (fixes the cycle-2 performance
finding: one success amid a majority-failing window does not reset
anything, unlike a simple consecutive-counter) requires
`c_breaker_failure_ratio` (default 0.7, i.e. 70%) of the last
`c_breaker_window_size` (default 10) confirmed outcomes **for THAT SPECIFIC
run_id** to be failures, with a minimum fill of `c_breaker_min_sample`
(default 5) before the ratio is even evaluated (avoids tripping on a tiny,
non-representative early sample). `mt_task_outcomes` rows for a `run_id`
are deleted once that run purges its state (§5.0 step 8), keeping this
table's steady-state size proportional to the number of CURRENTLY-ACTIVE
runs' windows only, not all history.

When `line_exists( mt_broken_runs[ table_line = lv_run_id ] )` for THIS
run: the planner stops producing NEW batch dispatches (including bisection
halves not yet started, per AR-2-001's retest) for the remainder of THIS
run ONLY, and every remaining un-resolved work item belonging to THIS run
is routed directly to `route_to_sequential_fallback( lv_run_id, ... )` —
this mirrors the EXISTING standard `mv_parallel_broken`
degrade-to-sequential-for-the-rest-of-the-run semantics (SER-0 §1) that
the design must not regress below, now correctly scoped so it cannot
degrade any OTHER, unrelated run sharing the same static storage. In-flight dispatches already awaiting a
callback are left to resolve normally (§5.4/§5.5 unchanged, still subject
to the §5.3 drain-aware exit) — the breaker only stops NEW dispatches, it
does not abort dispatches already in flight.

### 5.9 Actual-bytes admission check before dispatch (fixes AR-1-005/009 —
estimates are advisory for SCHEDULING, never for ADMISSION)

```text
REVISION_NOTE  Cycle-1 adversarial review found that `c_max_batch_input_
  bytes_est` only gates the ESTIMATED size (§3's coarse, type-keyed
  numbers), while the real serialized provider-buffer size (SER-3 §4's
  extract_for_batch output, e.g. wide DOKIL prefix-range hits) can be much
  larger. An underestimate could previously reach CALL FUNCTION with an
  oversized actual payload. This subsection adds a hard, ACTUAL-bytes gate
  evaluated AFTER extract_for_batch but BEFORE CALL FUNCTION, closing that
  gap without depending on estimate accuracy at all.
```

```text
before_dispatch( it_object_keys )
  lv_buf1 = provider_oo->extract_for_batch( it_object_keys ).
  lv_buf2 = provider_dd->extract_for_batch( it_object_keys ).
  lv_buf3 = provider_msg->extract_for_batch( it_object_keys ).
  lv_actual_bytes = xstrlen( lv_buf1 ) + xstrlen( lv_buf2 ) + xstrlen( lv_buf3 ).
  IF lv_actual_bytes > c_max_actual_batch_bytes AND lines( it_object_keys ) > 1.
    split it_object_keys into two halves.
    before_dispatch( half_1 ). before_dispatch( half_2 ).
    RETURN.
  ENDIF.
  " REVISED per cycle-2 AR-1-005 rejection: the previous "give up after
  " c_max_pre_dispatch_splits and dispatch the largest remaining half
  " anyway" path is REMOVED. Splitting now continues, with no depth cap,
  " until EITHER the group is under c_max_actual_batch_bytes OR it is
  " exactly one object (the only true, unavoidable base case — recursion
  " on a set of decreasing size always terminates at n=1, so no artificial
  " depth bound is needed; c_max_pre_dispatch_splits is retained ONLY as a
  " telemetry/warning counter, never as a reason to dispatch an over-limit
  " multi-object group).
  dispatch_batch( it_object_keys, lv_buf1, lv_buf2, lv_buf3, ... ).  " §5.2
```

A single object whose OWN provider-buffer contribution still exceeds
`c_max_actual_batch_bytes` is the only case where the actual-bytes gate
cannot reduce the group further — it is dispatched via RFC anyway with NO
provider buffers attached at all (`before_dispatch` for a true singleton
skips the provider EXPORT/attach step entirely and lets the worker fall
back to its own standard per-object SELECT, per the existing, already-
proven miss-path semantics) rather than sending an oversized buffer. This
removes the over-limit-dispatch case entirely: a lone object is either
sent WITH a compliant buffer, or sent with NO buffer (safe, just a
prefetch miss), never with an over-limit one.

This runs BEFORE every dispatch (initial batches, refills, AND retries/
bisections from §5.4/§5.6) — it is not a one-time planner-side estimate
check, it is the actual gate immediately preceding every single `CALL
FUNCTION 'Z_ABAPGIT_ORTEC_SER_BATCH' STARTING NEW TASK`.

## 6. Failure/edge-case answers (mirrors the mandatory adversarial
challenge list; full adversarial pass is a separate review artifact, this
is the design's own self-check)

```text
One heavy object dominating the final batch
  -> LPT-first front-loads heavy items; the shrinking end-of-queue batches
     mean a late-discovered heavy object lands in a SMALL batch (few or
     one companion objects), bounding its blast radius on the tail.

Inaccurate initial weights
  -> Never a correctness issue (§3); EWMA self-corrects within the SAME
     run after the first few batches of a type complete.

Bimodal CLAS costs (worker traces show real per-object variance)
  -> CORRECTED per performance-review F1: LPT-first with a type-keyed
     estimate has ZERO signal to distinguish a heavy CLAS from a light one
     WITHIN the same type - this design does NOT claim to solve intra-type
     tail latency for the prototype. It only claims to reduce INTER-type
     imbalance and, independently, to cut RFC dispatch-count overhead
     (§1), which is unaffected by intra-type variance. SER-4's measured-
     expansion process is the designed (not yet built) mechanism to add
     a finer per-type signal once evidence justifies it (e.g. "CLAS with
     testclasses" vs "CLAS without") - not invented speculatively here.
     The effective-row-limit throttle (§9) partially bounds the BLAST
     RADIUS of an unlucky same-type cluster (fewer objects per batch for
     types with a large est_bytes), even without solving the ordering
     problem itself.

One object producing unexpectedly huge output
  -> REFRAMED per AR-1-006 (best-effort telemetry, not a bounded
     guarantee): a completed object whose OUTPUT_BYTES exceeds
     c_max_object_output_bytes increments a run-local per-TYPE counter
     `mv_oversized_count( type )`. Only once that counter reaches
     c_oversized_threshold (default 3) does the cost estimator start
     forcing smaller (eventually solo) batches for THAT type for the
     REMAINDER of this run. Below the threshold, the estimator only raises
     that type's EWMA (already happens automatically, §3) without forcing
     solo batching - this avoids over-reacting to one outlier while still
     adapting to a genuinely bimodal/heavy-tailed type. The FIRST
     oversized object of a run is NEVER protected by this mechanism (it is
     discovered only after it already completed) - this is an accepted,
     explicitly stated limitation, not a claimed guarantee. What DOES
     protect the first oversized object, and every object regardless of
     estimate accuracy, is the actual-bytes pre-dispatch admission check
     in §5.9 (added per AR-1-005/009) and the per-object timeout/bisection
     path in §5.6, both of which act on REAL measured bytes, not estimates.

RFC communication/system failure after partial batch completion
  -> Impossible by construction: Z_ABAPGIT_ORTEC_SER_BATCH does not
     return partial ET_RESULT and then fail - either RECEIVE succeeds and
     ET_RESULT is complete for every input row, or RECEIVE fails and the
     WHOLE batch is requeued/retried/fallback-routed as one unit (§5 step
     7). There is no partial-batch-success state to reconcile.

Duplicate callback or late callback
  -> CORRECTED per AR-1-001/002: lookup is by the REAL RFC task name
     (`p_task`, §5.5), never a batch_id guess, and every task_name is
     unique for the whole run (§5.1, never reused by a retry). A late
     callback for a `TIMED_OUT` (state 'T') dispatch is still RECEIVEd
     (to free the RFC resource) but transitions only to 'D' (DRAINED) and
     is never merged (§5.5, `WHEN 'T'`). The `mt_resolved` guard (§5.1)
     is a second, independent line of defense against merging the same
     object twice from any two callbacks whatsoever.

Missing callback/task timeout
  -> CORRECTED per AR-1-001/003: a timeout NEVER produces a "confirmed
     failure" - it produces a superseded (`TIMED_OUT`) attempt whose
     object set is immediately resubmitted as a NEW attempt (§5.4). If the
     original RFC task eventually completes, its late callback is safely
     drained (§5.5, `WHEN 'T'`), never merged. This is what actually
     bounds a truly hung RFC task without risking a double-merge, versus
     the original design's flawed "timeout = failure" equivalence.

Worker result exceeding transfer limits
  -> CORRECTED per AR-1-004: a transfer-limit failure surfaces as a
     RECEIVE communication/system failure (sy-subrc <> 0), which ALWAYS
     deterministically bisects the object set (§5.6) regardless of
     whether OUTPUT_BYTES was ever known (it is not, in this path - the
     original design's reliance on OUTPUT_BYTES for this case was wrong
     and has been removed). A single object that still fails RFC transfer
     alone is routed straight to in-process sequential fallback (§5.6),
     never retried via RFC again.

Provider hit with incomplete data
  -> Providers (SER-3) never return a "partial" success signal from
     extract_for_batch; a miss for a specific object inside a hit batch
     degrades that ONE object to the standard per-object read
     transparently inside the worker (identical to today's ser_pref* miss
     behavior, SOURCE_CONFIRMED already correct) - "incomplete" as a
     batch-level concept does not exist.

Provider miss/fallback parity
  -> Structural parity guarantee (performance design §4): provider hit or
     miss never changes WHAT gets serialized, only where the data for one
     read comes from. This is the SER-3 parity test's entire purpose.

Stale static buffers from a prior repository/run
  -> Already solved and SOURCE_CONFIRMED for ser_pref* (clear-before-insert
     on inject, per SER-0 §2) - the new IV_PREFETCH_BUFFER_DD (SER-3)
     follows the exact same pattern, no new risk introduced.

Exception before cleanup
  -> CORRECTED per AR-1-007, then CORRECTED AGAIN twice more (adversarial
     cycle 4's instance-lifetime mechanism, then the OD-13 documentation
     verification that reopened AR-1-001 and replaced it with static/
     CLASS-DATA ownership, §5.1a): the orchestrator's `serialize()`
     method's own local cleanup releases ONLY the planner's dispatch
     QUEUE and in-flight budget counters (fully rebuildable, meaningless
     after return) - it does NOT clear the STATIC `mt_dispatch`/
     `mt_resolved`/`mt_task_outcomes` tables. Those are CLASS-DATA,
     scoped to the whole internal session by basic ABAP language
     semantics, not to any one call or object instance - THIS run's own
     rows are explicitly purged via `purge_run_state( lv_run_id )` (§5.0
     step 8) once every dispatch of `lv_run_id` reaches a terminal state,
     and a still-abandoned row is deliberately LEFT behind (bounded by
     §5.1a's explicit abandoned-task limits) so a late callback can still
     be safely drained via the existing "unknown/superseded task_name"
     path. It also NEVER calls ser_pref*/ser_pref_ext/ser_pref_oo/DD
     provider clear() itself. Provider clear() ownership stays exclusively
     with the OUTER `zcl_abapgit_serialize=>serialize` method, exactly as
     it is today, invoked exactly once regardless of whether the
     orchestrator succeeded or the caller fell back to the standard path
     (performance design §2, §3). This removes the double-clear/
     early-clear risk for PROVIDER state, and §5.1a's explicit, bounded
     purge policy removes the premature-clear risk AND the unbounded-
     growth risk for dispatch-tracking state.

Sequential fallback while other batches remain in flight
  -> Explicitly allowed and safe: forced_sequential objects (step 2) run
     BEFORE batch dispatch even starts in the recommended implementation
     order, but even if interleaved, mt_files accumulation is append-only
     and batch-id-keyed bookkeeping is independent of the sequential
     path - no shared mutable state between the two.

Cancellation
  -> UNKNOWN whether zcl_abapgit_serialize exposes a cancellation flag
     today (SER-0 §7, G-6) - NOT invented here. If/when one exists, the
     orchestrator's bounded poll loop (§5 step 6) is the natural place to
     check it once per poll interval and stop dispatching new batches
     (in-flight batches still drain normally, no forced task kill). Flagged
     as OWNER_DECISION-adjacent verification item, not a designed
     mechanism.

No free RFC resources
  -> determine_max_processes' existing logic (unchanged) already caps
     worker count safely; a dispatch attempt that finds zero free slots
     simply defers via the existing in-flight-batch limit (never a new
     failure mode - it is the SAME throttle mechanism as today's mv_free,
     just batch-granular instead of object-granular).

Mixed parallel-safe and unsafe object types
  -> Step 1's partition is exactly this: unsafe types NEVER enter a batch,
     full stop, mirroring today's is_no_parallel check exactly.

Languages and translation modes
  -> it_translation_langs/iv_main_language_only/iv_use_lxe are batch-header
     constants (hoisted, G-4) identical for every object in a run - no
     per-object language variance is possible today, so none is
     introduced.

Active/inactive object changes during the run
  -> No new risk: each worker calls the SAME zcl_abapgit_objects=>serialize
     for its own object at its own dispatch time, exactly as today's
     per-object RFC call does - a mid-run activation change affects
     batching identically to how it already affects the existing parallel
     path (out of scope to change here).

Direct-table bulk reads diverging from SAP API semantics
  -> Applies to the NEW DOMA provider only (SER-3 §2), not to SER-2 itself;
     SER-2 introduces no new direct-table reads.

Authorization differences between main and RFC users/sessions
  -> Unchanged from today - the same RFC destination/server-group
     mechanism and worker identity apply to batch dispatch as to
     single-object dispatch; no new authorization surface introduced.

Lock/enqueue amplification for CLAS/INTF
  -> Batching REDUCES enqueue/dequeue churn versus today's one-lock-cycle-
     per-object-per-worker-session pattern only if a batch's objects share
     a worker session across multiple ENQUEUE_ESEO_CS_INCLUDE/DEQUEUE
     cycles sequentially within that one RFC call - which is exactly what
     happens (the worker LOOP in §2 processes objects one at a time within
     the SAME session), so per-object lock cycles are unchanged in COUNT,
     only the RFC-dispatch overhead AROUND them is reduced. No NEW lock
     amplification risk.

DB overload from too many parallel workers despite lower wall time
  -> Worker COUNT is unchanged (same lv_max cap) - only the objects PER
     TASK changes. DB load per unit time should DECREASE (fewer, larger
     RFC sessions means fewer redundant session-setup SELECTs per object,
     per the DB:Open/DB:Close pattern visible in every attached trace).

Task count reduction failing to improve wall time
  -> This is exactly what the performance ACCEPTANCE gates (SER-0/
     performance design, and the mandatory performance-review DESIGN_GATE)
     must verify empirically before this is considered done - the design
     does not assume success, it is built to be MEASURED (§8 telemetry).

Memory regression despite runtime improvement
  -> Bounded by the memory model (performance design §3) - hard row/byte
     limits on batch size and in-flight count are enforced independently
     of any timing outcome.

Output ordering and duplicate/missing files
  -> ET_RESULT always has exactly one row per input object (§2); mt_files
     merge is append-only and keyed by object identity, matching today's
     mt_files population pattern - no duplicate/missing rows possible by
     construction, PROVIDED the duplicate-callback guard (§5 step 7) is
     correctly implemented (this is the SINGLE most safety-critical
     invariant in this design and the primary adversarial-review target).

Retry causing duplicate successful object results
  -> CORRECTED per AR-1-003: a retry is dispatched as soon as a timeout is
     DETECTED (superseded, not confirmed-failed), so the ORIGINAL attempt
     may indeed still be running concurrently with its retry - this is
     accepted BY DESIGN and made safe, not assumed away: the retry and the
     original race to complete, but only the FIRST to reach `on_end_of_batch`
     with `state = 'A'` actually merges (transitioning to 'R'); every
     subsequent callback for either attempt of the same object set finds
     `mt_resolved` already populated (or the dispatch already in a
     terminal state) and is discarded (§5.5, §5.1). Both attempts
     completing successfully is therefore harmless, not merely unlikely.

Feature disabled or ORTEC initialization failing
  -> CORRECTED per AR-1-007 (fallback boundary): the single hook
     (performance design §2) TRY/CATCH only permits an UNCONDITIONAL fall-
     through to the completely unchanged standard path when ZERO objects
     have been resolved yet (`mt_resolved` empty at the moment of the
     exception). If the orchestrator has already resolved one or more
     objects (via a completed batch or forced_sequential) before an
     exception occurs, it does NOT re-throw to trigger a full fallback
     (which would reprocess/duplicate already-resolved objects); instead
     it catches the exception INTERNALLY, routes every remaining
     UN-resolved object to `route_to_sequential_fallback` (§5.7), and
     returns normally with whatever it has - the outer hook's TRY/CATCH is
     therefore only ever exercised for a failure that happens before any
     work has been done (planner/provider/RFC-group initialization), which
     is exactly the scenario where "nothing to clean up in the caller" is
     actually true.
```

## 7. Compatibility with existing parallelization settings and RFC server
groups

The orchestrator reuses `mv_group` (existing RFC server group resolution,
`determine_rfc_server_group`, unchanged) and `lv_max` (existing
`determine_max_processes`, unchanged) exactly as computed by
`zcl_abapgit_serialize=>serialize` today, passed into the orchestrator as
parameters (performance design §2 hook signature) — no new server-group or
worker-count logic is introduced; SER-2 only changes how many OBJECTS ride
on each already-existing worker slot.

## 8. Telemetry (per-run only, no persistence, feeds SER-4)

Orchestrator accumulates, per object type, for the CURRENT run only (freed
at `serialize()` return, never written to any table):
`object_count`, `sum_elapsed_ms`, `sum_output_bytes`, `batch_count`,
`retry_count`, `provider_hit/miss/fallback_count`, `oversized_count`.

**CORRECTED per AR-1-011:** telemetry is not only emitted at the very end
of `serialize()` (which would be lost if the run fails or falls back
mid-way, exactly when it is most needed). Compact, single-line-per-type
entries are written to the existing log object (a) incrementally, once per
circuit-breaker trip (§5.8) and once per completed
batch's terminal state transition (`R`/`F`/`D`, §5.5/§5.9), and (b) once
more in the orchestrator's own CLEANUP block with whatever partial
aggregates exist at that point — both reuse the SAME log mechanism already
passed into `serialize()` today; no new persistence, no new table.

## 9. Limits table (all owner-adjustable constants, safe defaults;
DESIGN_GATE reviewer to confirm before implementation)

```text
c_max_batch_rows                  25       (upper bound; see
                                             effective_row_limit below for
                                             the per-type-adjusted value
                                             actually used by the planner)
c_max_batch_input_bytes_est         8 MB   (estimated-size planning gate,
                                             §4; advisory only, see §5.9)
c_max_actual_batch_bytes           12 MB   (NEW, per AR-1-005/009 fix,
                                             §5.9 — hard gate on the REAL
                                             extract_for_batch() output,
                                             evaluated immediately before
                                             every CALL FUNCTION)
c_max_pre_dispatch_splits           3      (TELEMETRY/WARNING ONLY as of
                                             cycle 3 — no longer a stopping
                                             condition; logs a warning if
                                             exceeded but splitting always
                                             continues to n=1, §5.9)
c_max_drain_wait_s                300      (NEW, cycle 3 — §5.4/§5.3 —
                                             additional bounded wait, on top
                                             of c_batch_rfc_timeout_s,
                                             before a 'T' dispatch is
                                             reclassified 'X' ABANDONED and
                                             stops blocking loop exit; total
                                             worst-case wait for one
                                             straggler ≈ 600s)
c_max_in_flight_batches            = lv_max (same as today's worker cap)
c_max_in_flight_bytes             100 MB
c_max_object_output_bytes          20 MB   (post-hoc oversized-object
                                             signal, §6; NOT a pre-dispatch
                                             admission gate, see §5.9 for
                                             that)
c_oversized_threshold               3      (NEW, per AR-1-006 fix — number
                                             of oversized occurrences of the
                                             SAME type before that type's
                                             batches start shrinking)
c_batch_rfc_timeout_s              300     (matches existing avoid_timeout
                                             window)
c_max_retries                       2      (per logical object group;
                                             bisection halves each get
                                             their own fresh budget, §5.6)
c_breaker_window_size               10     (NEW, cycle 3, §5.8 — sliding
                                             window of confirmed per-TASK
                                             outcomes, replaces the cycle-2
                                             consecutive-failure counter)
c_breaker_min_sample                 5     (NEW, cycle 3, §5.8 — minimum
                                             window fill before the failure
                                             ratio is evaluated)
c_breaker_failure_ratio             0.7    (NEW, cycle 3, §5.8 — 70% of the
                                             window's confirmed outcomes
                                             failing trips the breaker)
c_shrink_factor                     2
c_ewma_alpha                        0.3
```

**Per-type-adjusted row limit (per performance-review F2):** the planner
does not always use `c_max_batch_rows` at face value — it computes, per
batch it is about to build for a given dominant type,
`effective_row_limit = MIN( c_max_batch_rows, MAX( 1, c_max_batch_input_
bytes_est / MAX( est_bytes_for_type, 1 ) ) )`. For a type whose `est_bytes`
is already large (e.g. a CLAS-heavy run where the estimate creeps up via
EWMA after a few big objects), this proactively reduces how many objects
of that type are grouped together — a complement to, not a replacement
for, the reactive actual-bytes gate in §5.9.

## 10. Generic (non-specialized-provider) object types

`OD-4`: generic batch RFC dispatch (no `IV_PREFETCH_BUFFER_*`, all four
optional) is available to ANY parallel-safe object type from SLICE 1
onward — it only requires the type NOT be in the `is_no_parallel` denylist
(per the REAL predicate, see performance design §2 OD-6, not a duplicated
copy). This means WAPA, FUGR, PROG, and any other already-parallel-safe
type immediately gets the RFC-task-count reduction benefit (SER-0 §6's
dominant finding) even before a specialized provider exists for it, which
is the recommended, default-safe answer to OD-4.
