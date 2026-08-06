# Serialization Performance Design — Master Architecture (SER-0 synthesis, cross-cutting decisions)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SERIALIZATION_PERFORMANCE_DESIGN
STATUS=DRAFT_FOR_REVIEW
DEPENDS_ON=.memory/logs/serialization_ser0_audit.md
CHILD_DESIGNS=serialization_bulk_exists_design.md (SER-1),
  serialization_adaptive_batch_design.md (SER-2),
  serialization_provider_design.md (SER-3),
  serialization_wapa_review.md (SER-5)
```

## Current-source supersedure (2026-08-06)

```text
STATUS_OF_THIS_DOCUMENT=HISTORICAL_MASTER_ARCHITECTURE_RATIONALE
HOOK_SUPERSEDURE=The older §2 hook contract in this document explicitly
  wrapped the ORTEC call in TRY/CATCH and fell through to the unchanged
  standard path on ZCX_ABAPGIT_EXCEPTION. Current Stage-A source
  intentionally does NOT do that for feature-ON incomplete-batch cases:
  fail-fast WAIT result 4/8 now raises a visible abapGit exception and
  discards the partial result, per the owner brief's explicit "no
  partial result accepted" requirement. This is a deliberate contract
  change, not an omission.
FEATURE_OFF_STILL_TRUE=Feature OFF remains unchanged because
  MV_SERIAL_BATCH_ACTIVE is restored to ABAP_FALSE by default and the
  minimal hook itself stays one direct delegation block only.
WAPA_SUPERSEDURE=The older "WAPA is not part of the first batch
  prototype" language is superseded by current Stage-A source: WAPA is
  batch-eligible only as singleton batches.
AUTHORITATIVE_SOURCE=Current source outranks stale prose. Keep using this
  document for package/scope rationale, not as the active Stage-A wait/
  fallback contract.
```

## 0. Scope discipline (non-goals, restated from owner brief)

- No Persistent Local Serialization Index / persistent serialized-object
  snapshot in this package (SER-6 optional, evidence-gated, not designed
  here beyond entry criteria).
- No unbounded internal tables, RFC buffers, task counts, SQL lists, or
  full-repository payload accumulation anywhere in the new code.
- No replacement of a SAP API with direct table access without proven
  semantic parity, authorization parity, release compatibility, and
  fallback (applies to the new DOMA provider, see SER-3 §2).
- WAPA batch-provider integration is a LATER slice, not part of the first
  prototype (SER-5 disposition).

## 1. Package hierarchy (recommendation; final names fixed in the creation
manifest, `.memory/handoffs/serialization-design-bootstrap.md`)

```text
$ABAPGIT_ORTEC_SERIAL                (root, parent = existing ORTEC root
                                       package, sibling of $ABAPGIT_ORTEC_GIT)
  $ABAPGIT_ORTEC_SERIAL_CORE          planner, cost model, orchestrator,
                                       batch header/result types
  $ABAPGIT_ORTEC_SERIAL_EXIST         bulk-exists extension (handler
                                       interface + registry; SER-1 keeps the
                                       existing class in its current package,
                                       see SER-1 design — this sub-package is
                                       reserved for the NEW extension
                                       mechanism only, not a move)
  $ABAPGIT_ORTEC_SERIAL_PROVIDER      batch-scoped provider contract + DDIC
                                       provider (DTEL/DOMA) + OO provider
                                       facade
  $ABAPGIT_ORTEC_SERIAL_RFC           new function group, batch RFC-enabled
                                       function module
```

Existing `zcl_abapgit_ortec_bulk_exists`, `zcl_abapgit_ortec_ser_pref*`, and
`zcl_abapgit_ortec_wapa` STAY in their current package (`$ABAPGIT_ORTEC_GIT`
or wherever they live today per file path `src/ortec/`) — SER-1/SER-3 only
ADD methods to them; they are not moved, to keep the diff minimal and avoid
an unnecessary transport-object churn.

## 2. Minimal standard-code hook (exact anchor, verified against current
source)

```text
FILE     src/objects/core/zcl_abapgit_serialize.clas.abap
METHOD   serialize (starts at line 749 in the audited HEAD)
ANCHOR   Immediately after:
           lv_use_ortec_prefetch = zcl_abapgit_ortec_git_switch=>is_serial_prefetch_active( ).
           IF lv_use_ortec_prefetch = abap_true.
             zcl_abapgit_ortec_ser_pref=>prepare( ... ).
             zcl_abapgit_ortec_ser_pref_ext=>prepare( ... ).
             zcl_abapgit_ortec_ser_pref_oo=>prepare( ... ).
           ENDIF.
         and BEFORE the existing `TRY. lv_count = lines( lt_tadir ). ...` block.
```

Insert exactly:

```abap
IF zcl_abapgit_ortec_git_switch=>is_ser_batch_active( ) = abap_true.
  TRY.
      rt_files = zcl_abapgit_ortec_ser_orch=>serialize(   " STATIC call —
        " CORRECTED (OD-13 redesign): no instance is required or created;
        " see adaptive batch design §5.1a for the documented reason static/
        " CLASS-DATA ownership is the proven-sufficient mechanism for safe
        " async-RFC callback handling, replacing the earlier (unproven)
        " instance-lifetime/GC-retention mechanism.
                    it_tadir           = lt_tadir
                    iv_max_processes   = lv_max
                    iv_group           = mv_group
                    is_i18n_params     = ms_i18n_params
                    ii_log             = ii_log
                    iv_path            = iv_path
                    iv_main_language_only    = iv_main_language_only
                    iv_suppress_po_comments  = iv_suppress_po_comments
                    it_translation_langs     = it_translation_langs
                    iv_use_lxe               = iv_use_lxe
                    iv_abap_language_vers    = iv_abap_language_vers ).
      RETURN.
    CATCH zcx_abapgit_exception INTO DATA(lx_ortec_batch_error).
      IF ii_log IS BOUND.
        ii_log->add_warning( |ORTEC batch serialization unavailable ({ lx_ortec_batch_error->get_text( ) }), | &&
                              |falling back to standard sequential/parallel path| ).
      ENDIF.
      " no RETURN: fall through into the completely unchanged existing body below
  ENDTRY.
ENDIF.
```

**CORRECTED per AR-1-007 (fallback boundary):** the existing `ser_pref*=>
clear()` calls that already exist a few lines further down in the
UNCHANGED CLEANUP block are what perform cleanup on EVERY path (success,
fallback, or exception) — this hook does **not** call `clear()` itself on
the success path either, precisely to avoid a double-clear or an
out-of-order clear relative to the existing CLEANUP block. The
orthogonal, more important correction is the orchestrator's OWN internal
contract (adaptive batch design §5.0/§6 "Feature disabled or ORTEC
initialization failing"): `zcl_abapgit_ortec_ser_orch=>serialize`
only ever lets an exception propagate OUT of itself (to be caught by this
hook) when **zero objects have been resolved yet** — if it has already
resolved one or more objects (a completed batch or a forced-sequential
object) before hitting an internal error, it catches that error itself,
routes every remaining unresolved object to its own in-process sequential
fallback (adaptive batch design §5.7), and returns normally. This makes
"fall through to the completely unchanged standard path" TRUE in every
case this hook's `CATCH` can actually be reached — there is never a
partial-progress state for the caller to reconcile, because partial
progress is exactly the case the orchestrator resolves internally instead
of re-throwing.

Everything below this block (the existing `TRY...ENDTRY` loop, `run_sequential`,
`run_parallel`, `on_end_of_task`, `is_no_parallel`, the CLEANUP block) is
**UNCHANGED**. `FEATURE_OFF_BEHAVIOR`: switch off -> the new `IF` body never
executes -> byte-identical to current behavior. `ROLLBACK`: delete the one
`IF...ENDIF` block; no other edit required in this file.

`zcl_abapgit_ortec_ser_orch=>serialize` is a NEW ORTEC-owned STATIC
method (see adaptive batch design §5.1a for why static/CLASS-DATA
ownership, not an instance, is the documentation-proven-sufficient
mechanism for safe async-RFC callback handling) (full contract in SER-2)
that:
1. partitions objects into `FORCE_SEQUENTIAL` vs `BATCH_ELIGIBLE` by
   calling the REAL `zcl_abapgit_serialize=>is_no_parallel` predicate
   (see OD-6 below — CORRECTED per AR-1-008: this is no longer a
   duplicated/mirrored check);
2. serializes `FORCE_SEQUENTIAL` objects by calling
   `zcl_abapgit_objects=>serialize(...)` directly, in-process, exactly as
   `run_sequential` does today (same call, one line, not a behavior fork);
3. runs the SER-2 planner/scheduler over `BATCH_ELIGIBLE` objects;
4. returns the exact same `zif_abapgit_definitions=>ty_file_item_tt` shape
   as today's `serialize()`, so the caller (`zcl_abapgit_serialize=>serialize`)
   sees no difference in return type.

**OD-6, REVISED (per AR-1-008):** the original recommendation to duplicate
the `is_no_parallel`/`lv_max=1` check inside the orchestrator is
**withdrawn**. The adversarial review correctly identified this as a real
staleness/correctness risk (if the standard denylist changes later — e.g.
a newly-unsafe object type is added — a duplicated copy silently continues
batching it). The revised, default-safe recommendation is a single, tiny,
behavior-free visibility change to standard code:

```text
FILE      src/objects/core/zcl_abapgit_serialize.clas.abap
CHANGE    METHODS is_no_parallel ... (class definition, PRIVATE SECTION)
          -> move the method declaration from PRIVATE SECTION to
             PUBLIC SECTION (or PROTECTED SECTION with the orchestrator
             class named in a CLASS ... DEFINITION ... FRIENDS clause —
             either is acceptable; PUBLIC is simpler and lower-risk since
             is_no_parallel is a pure, side-effect-free predicate over a
             single TROBJTYPE-shaped input).
FEATURE_OFF_BEHAVIOR  A wider visibility keyword has zero runtime
          behavior difference whether the ORTEC switch is on or off — the
          method's logic, signature, and every existing internal call site
          are completely unchanged.
ROLLBACK  Revert the visibility keyword; no other edit required.
```

The orchestrator then calls `zcl_abapgit_serialize=>is_no_parallel(
<obj_type> )` directly — the SAME method, always in sync by construction,
never a stale mirror.

## 3. Memory model (peak, not just retained)

```text
STRUCTURE                          OWNER            BOUND
Planner work-item list             orchestrator     rows = len(lt_tadir),
                                                      no payload (metadata only)
Per-type cost table (EWMA)         cost estimator   rows <= distinct object
                                                      types actually seen this
                                                      run (bounded, <100
                                                      realistically)
In-flight batch set                planner          <= iv_max_in_flight_batches
                                                      (default = lv_max, i.e.
                                                      same worker cap as today)
Per-batch input (IT_TADIR slice)   RFC dispatch     <= c_max_batch_rows (25)
                                                      rows, metadata only
Per-batch prefetch export buffer   ser_pref*        <= sum of per-object
                                                      slices for one batch
                                                      (bounded by row limit,
                                                      not run-wide)
Per-batch RFC result (ET_RESULT)   RFC dispatch     <= c_max_batch_rows rows,
                                                      each row's FILES_XSTRING
                                                      capped by
                                                      c_max_object_output_bytes
                                                      (oversized-object
                                                      isolation, SER-2 §6)
Accumulated mt_files               orchestrator     released to caller
                                                      immediately per batch
                                                      (append + drop local
                                                      copy), never doubles the
                                                      full result set in memory
```

Hard limits (all owner-adjustable constants, safe defaults recommended,
see SER-2 §9 for the full table): `c_max_batch_rows = 25`,
`c_max_batch_input_bytes_est = 8 MB`, `c_max_actual_batch_bytes = 12 MB`
(hard, ACTUAL-bytes gate evaluated immediately before every dispatch, see
SER-2 §5.9), `c_max_in_flight_batches = lv_max` (same as today's worker
cap, never exceeds it), `c_max_in_flight_bytes = 100 MB`,
`c_max_object_output_bytes = 20 MB` (post-hoc oversized-object signal,
not a pre-dispatch gate), `c_batch_rfc_timeout_s = 300` (matches the
existing `avoid_timeout` 300-second redispatch window already used
elsewhere in this method, for consistency).

**NEW structure, per the OD-13 correction (adaptive batch design §5.1a):
a session-wide, bounded, STATIC "abandoned dispatch ledger".** Because
`mt_dispatch`/`mt_resolved`/`mt_task_outcomes` are now `CLASS-DATA`
(scoped to the whole internal session, not to one `serialize()` call —
see below), a session that runs MANY serializations over hours could, in
the worst case, accumulate bookkeeping rows for abandoned dispatches
across multiple runs. This is explicitly bounded: `max_abandoned_tasks_
per_run = 50` (observability threshold), `max_abandoned_runs_per_
internal_session = 20` (triggers forced purge of the oldest abandoned
run), `max_abandoned_tasks_per_internal_session = 200` (hard cap, oldest
purged first), `retained_metadata_byte_limit = 2 MB` (each row is
metadata-only — no serialized payload is ever retained here). Full detail
and rationale in adaptive batch design §5.1a.

**RESIDUAL RISK, explicitly surfaced per the performance DESIGN_GATE
review's finding F3 (not resolved by SER-2/SER-3, not a NEW regression
either):** the table above bounds every structure SER-2/SER-3 introduce,
but it does **not** bound the pre-existing, RUN-scoped `ser_pref`/
`ser_pref_ext`/`ser_pref_oo` SOURCE caches themselves (the 10+ CLASS-DATA
tables these classes already hold today, unchanged by this design,
SER-3 §3). Peak memory for a full, non-filtered 40,000-object
serialization remains **UNKNOWN** (carried over from SER-0 §9's own
"MUST GATHER" item and the prior Git-side backlog's M-3, neither resolved
by this pass). `OD-11` (NEW): obtain this measurement (or an owner-
accepted risk acceptance to defer it) BEFORE implementation sign-off for
any full-repository (non-filtered) serialization scenario — this is a
pre-existing gap this design inherits, not one it introduces, but it must
not be silently presented as already bounded.

Cleanup guarantee, **REDESIGNED per the OD-13 documentation-verification
correction (`.memory/reviews/serialization_adversarial_review.md`, "OD-13
Correction" section — the cycle-4/5 instance-lifetime/GC-retention
mechanism was found to rely on an ABAP behavior documented ONLY for
`SET HANDLER` event handlers, not for aRFC `CALLING ... ON END OF TASK`
callbacks, and is therefore REPLACED, not merely re-argued):** the
orchestrator's public entry point is a STATIC method
(`zcl_abapgit_ortec_ser_orch=>serialize`, §2 above), and its three
dispatch-tracking tables — `mt_dispatch`, `mt_resolved`,
`mt_task_outcomes` — are `CLASS-DATA`, not instance data, and the
callback (`on_end_of_batch`) is a `CLASS-METHODS` (static) method. Static
program data and static methods are guaranteed, by basic ABAP language
semantics (no special citation needed — this is definitional, not
contested), to exist for the entire life of the program's internal
session, which is EXACTLY the scope `ABAPCALL_FUNCTION_STARTING`'s own
documentation proves is required for callback delivery ("a prerequisite
... is that the calling program still exists in its internal session
when the remote function is terminated"). This closes the same gap the
instance-lifetime mechanism attempted to close, without relying on any
unproven GC-retention behavior. Each `serialize()` call still ends by
calling `purge_run_state( lv_run_id )` (adaptive batch design §5.0 step 8)
to remove its OWN fully-terminal rows from the shared static tables
before returning — this keeps steady-state footprint near zero between
calls, while a still-`LOGICALLY_ABANDONED` row is deliberately left
behind (bounded by the abandoned-dispatch-ledger limits above) so a late
callback can still be safely drained.

The orchestrator never calls `ser_pref*=>clear()`/DD-provider `clear()`
itself — provider cleanup ownership stays EXCLUSIVELY with the outer,
unchanged `zcl_abapgit_serialize=>serialize` CLEANUP block, invoked
exactly once regardless of which path (ORTEC batch success, or fallback)
actually ran (CORRECTED per AR-1-007 — see §2 above for the full
rationale). This is unrelated to, and unaffected by, the dispatch-tracking
redesign above — provider state and dispatch-tracking state have always
had, and keep, separate ownership and separate lifetimes.

**Resource vs. object distinction (still holds under the static
redesign, restated precisely):** static ownership establishes that
`mt_dispatch` etc. remain valid for a late callback to resolve against —
it says nothing about, and is not needed to say anything about, whether
the underlying RFC/gateway CONNECTION for a truly-hung worker is ever
reclaimed. That is a separate, pre-existing SAP kernel/gateway/
work-process-timeout concern, identical for EVERY asynchronous RFC call
in ABAP (including today's baseline `run_parallel`), and is explicitly
NOT documented either way by the fetched ABAP Keyword Documentation
(`DOC_PROVES_RFC_RESOURCE_RECLAMATION_ON_RETURN=NO`, adaptive batch
design §5.1a) — recorded as an honest, open boundary, not claimed as
solved.

## 3a. Transaction/LUW check (owner-requested verification)

```text
CALLER_LUW_EFFECT=Documented: dispatching CALL FUNCTION ... STARTING NEW
  TASK, and any WAIT (UNTIL / FOR ASYNCHRONOUS TASKS) or RECEIVE RESULTS
  FROM FUNCTION statement that interrupts the program to change work
  process, triggers an IMPLICIT DATABASE COMMIT in the calling program's
  LUW — EXCEPT during V1 update-task processing. Citations (ABAP Keyword
  Documentation, fetched directly): ABAPCALL_FUNCTION_STARTING
  ("Asynchronous RFC triggers a database commit in the calling program
  with the following exception: No database commit is triggered by an
  aRFC during update processing."); ABAPWAIT_ARFC ("If the statement WAIT
  interrupts the program, the work process is changed, and a database
  commit is executed, except in updates."); ABAPRECEIVE ("Before the
  statement RECEIVE is executed in a callback routine, the current work
  process is interrupted... This results in a database commit except
  during the update.").
WORKER_LUW_EFFECT=Each dispatched function module executes in its own
  separate work process/RFC session with its own independent LUW; not
  documented to interact with or extend the caller's LUW.
DESIGN_IMPACT=NONE requiring correction, one nuance acknowledged (per
  correctness review DR-007). This is IDENTICAL, pre-existing behavior for
  TODAY's standard single-object `run_parallel`/`WAIT UNTIL` mechanism
  (SER-0 §1) — it dispatches the SAME two ABAP statements
  (`CALL FUNCTION ... STARTING NEW TASK`, `WAIT`), so the DIRECTION of the
  effect (implicit commits happen around aRFC dispatch/wait points) is
  unchanged. The CADENCE does change, and is stated explicitly rather than
  glossed over: batching means FEWER, LARGER dispatch/wait cycles (roughly
  batch-count instead of object-count), so implicit commits happen LESS
  OFTEN per `serialize()` call than today, not more. This is very likely
  benign (fewer, not more, commit points; no design in this codebase is
  known to depend on a commit happening at a specific PER-OBJECT
  granularity during serialization), but it is recorded as a real,
  directional behavior change rather than asserted as literally identical
  timing. Recorded here because it was previously UNVERIFIED in this
  design's own artifacts; now explicitly confirmed and captured as a
  standing constraint that already, silently, applied to every prior
  abapGit parallel serialization run: a caller of
  `zcl_abapgit_serialize=>serialize` must not rely on its OWN other
  pending, uncommitted database changes surviving past that call, with or
  without the ORTEC batch path enabled, AND must not rely on a specific
  NUMBER of implicit commit points occurring during the call. No code
  change is required as a result of this finding — it is a documentation/
  verification deliverable only, per the owner's explicit request not to
  broaden this into a general transaction redesign.
```

## 4. Output and semantic parity plan

Parity is enforced structurally, not just tested:
- The orchestrator returns files in the SAME per-object shape
  (`zif_abapgit_definitions=>ty_file_item`) produced by the SAME
  `zcl_abapgit_objects=>serialize(...)` call used by `run_sequential`/
  `run_parallel` today — batching changes WHEN and HOW MANY objects are
  grouped per RFC dispatch, never WHAT serializes an object. No new
  serialization logic is introduced; CLAS/INTF/DTEL/DOMA object handlers
  are untouched.
- Ordering: the existing contract has no cross-object ordering guarantee
  visible to callers (`mt_files`/`rt_files` is an unordered table keyed by
  object identity per `zcl_abapgit_objects_files`); batching preserves this
  (no new ordering contract introduced, none removed).
- Filenames/paths/XML/ABAP source content: byte-identical, since the actual
  per-object serialize call is unchanged.
- Logging/error format: `ET_RESULT` rows carry the same message
  id/number/variables shape the existing single-object `EV_RESULT`/
  exception path already produces, so `ii_log->add_exception`-equivalent
  handling in the orchestrator matches `on_end_of_task`'s existing format.
- Fallback and forced-provider-miss parity: because provider prefetch is
  strictly an internal performance optimization (miss -> standard SELECT,
  proven already true for ser_pref*, verified in SER-0 §2), enabling/
  disabling ANY provider must produce IDENTICAL output — this is the
  primary parity test axis for SER-3.
- **Multi-object-per-session parity (per correctness review DR-002, NOT
  fully closed by this design alone):** today's contract processes exactly
  ONE object per RFC worker dispatch; the new batch worker (adaptive batch
  design §2) processes SEVERAL different objects, sequentially, inside
  ONE RFC session. The three KNOWN ortec `ser_pref*` caches are proven
  safe for this (clear-before-insert, SER-0 §2), but this design does NOT
  independently audit whether any OTHER static/`CLASS-DATA` state exists
  anywhere in the `zcl_abapgit_objects`/OO-framework/per-type-handler call
  chain (e.g. `CL_OO_*` buffered class-pool state) that today's
  one-object-per-session contract structurally prevented from ever being
  observed across two different objects, and that batching would newly
  expose. `OD-14` (NEW): before SLICE 2 implementation is authorized, audit
  `zcl_abapgit_objects`/`zcl_abapgit_oo_base`/`zcl_abapgit_objects_super`/
  per-type handler classes for CLASS-DATA outside the three known ortec
  caches, AND add the specific test the correctness review requires: serialize
  2+ DIFFERENT objects of the SAME type through one simulated batch-worker
  LOOP and diff each object's output against serializing it alone. This is a
  cheap, mechanical audit + test, not a design change, and is added to the
  SLICE 1/2 implementation-readiness checklist (see the bootstrap handoff).

## 5. Owner decisions to surface (index; full detail + evidence in each
child design)

```text
OD-1  Exact package subdivision under $ABAPGIT_ORTEC_SERIAL — recommendation
      given in §1 above (default-safe: as proposed).
OD-2  New RFC function module vs. versioned extension of
      Z_ABAPGIT_SERIALIZE_PARALLEL — see SER-2 §2 (recommendation: NEW FM,
      default-safe).
OD-3  Initial worker count / row / byte budgets — see SER-2 §9
      (recommendation: reuse existing lv_max cap for workers; new
      owner-adjustable constants for row/byte limits, defaults proposed).
OD-4  Whether generic (non-CLAS/INTF/DTEL/DOMA) safe object types use batch
      RFC before specialized providers exist — see SER-2 §10
      (recommendation: YES, generic no-prefetch batches are safe and
      valuable on their own, especially for WAPA-heavy repositories per the
      SPBT-overhead evidence in SER-0 §6).
OD-5  Acceptable fallback policy after partial batch failure — see SER-2
      §5.4-§5.8 (REVISED per AR-1-001/003/004: a timeout is a superseded
      attempt, not a confirmed failure, and is always resubmitted rather
      than "retried after confirmation"; a confirmed RECEIVE failure
      always deterministically bisects down to single objects before
      falling back in-process; recommendation: as designed, plus a global
      circuit breaker, OD-12, mirroring today's `mv_parallel_broken`).
OD-6  Reuse of the real `zcl_abapgit_serialize=>is_no_parallel` predicate
      vs. a duplicated routing check in the orchestrator — see §2 above.
      REVISED per AR-1-008 (original "duplicate" recommendation
      withdrawn — a stale mirror is a real correctness risk, not just a
      style concern): recommendation is now a single, behavior-free
      visibility change (PRIVATE -> PUBLIC) on `is_no_parallel`, called
      directly by the orchestrator.
OD-7  Whether direct-table reads are acceptable for the new DOMA provider —
      see SER-3 §2 (recommendation: YES, with explicit parity proof against
      the existing ZCL_ABAPGIT_OBJECT_DOMA SAP-API-based path, same pattern
      already accepted for the 13 existing bulk-exists types).
OD-8  Whether existing SER_PREF* classes are migrated incrementally (additive
      methods, as designed here) or replaced in one controlled slice — see
      SER-3 §5 (recommendation: additive/incremental, default-safe, avoids
      touching any existing method body).
OD-9  Whether SER-6 persistent statistics are economically justified — see
      §7 below (recommendation: NOT YET, no evidence of run-local
      insufficiency).
OD-10 Final WAPA disposition — see SER-5 (recommendation:
      KEEP_WITH_CORRECTIONS now, REFACTOR_INTO_PROVIDER as a later,
      separately reviewed slice, gated on tests existing first).
OD-11 (NEW, per performance DESIGN_GATE finding F3) Whether a measured or
      estimated 40,000-object peak-memory figure for the pre-existing,
      unchanged run-scoped `ser_pref*` source caches is obtained before
      implementation sign-off, or the owner explicitly accepts this as a
      pre-existing, unresolved risk it inherits rather than introduces —
      see §3 above (recommendation: obtain the measurement; it is cheap —
      one SAT/memory snapshot on an existing large repository — relative
      to the risk of an un-quantified full-repository memory ceiling).
OD-12 (NEW, per performance DESIGN_GATE finding F4) Whether to add the
      global circuit-breaker (now run_id-scoped `mt_broken_runs`, per
      DR-006/AR-OD13-001 correction; adaptive batch design §5.8) mirroring
      today's `mv_parallel_broken` degrade-on-systemic-failure behavior —
      see SER-2 §5.8 (recommendation: YES, default-safe, required to
      avoid regressing below the documented "must not regress below"
      baseline, SER-0 §1, under a full RFC/server-group outage).
OD-13 (adversarial review cycles 1-3 BLOCKER; cycles 4-5 accepted an
      unverified mechanism; the dedicated OD-13 documentation-verification
      task REOPENED it and REPLACED the mechanism — see adaptive batch
      design §5.1a for the full evidence and redesign; kept here as an
      owner-visible decision record, not a silently-dropped item) Whether
      abandoning a straggler RFC task after a bounded wait is an
      acceptable, provably-safe design, and by what exact mechanism.
      DOCUMENTATION_FINDING (authoritative, fetched from the ABAP Keyword
      Documentation, not from memory)
        DOC_PROVES_CALLBACK_TARGET_RETENTION=NO — the "pending handler
        keeps the object alive" behavior is documented ONLY for classic
        `SET HANDLER` event handlers (`ABENGARBAGE_COLLECTOR_GLOSRY`:
        "...for which no method is registered as an event handler"), a
        DIFFERENT ABAP language construct from aRFC's `CALLING meth ON
        END OF TASK`. No equivalent statement exists in
        `ABAPCALL_FUNCTION_STARTING`. The cycle-4/5 mechanism was built on
        an unproven analogy between the two and is WITHDRAWN.
        DOC_PROVES_CALLBACK_AFTER_CALLER_RETURN=YES, conditionally —
        `ABAPCALL_FUNCTION_STARTING`: "A prerequisite for the execution of
        a registered callback routine is that the calling program still
        exists in its internal session when the remote function is
        terminated... If the program was terminated or is located on the
        stack as part of a call sequence, the callback routine is not
        executed." Callback delivery is scoped to INTERNAL-SESSION/
        call-sequence persistence, not to any specific object reference.
        DOC_PROVES_WORKER_CANCELLATION_ON_RETURN=NO and
        DOC_PROVES_RFC_RESOURCE_RECLAMATION_ON_RETURN=NO — neither is
        addressed by any fetched document either way.
      RESOLUTION_MECHANISM (final — static/CLASS-DATA ownership, adaptive
      batch design §5.1a)  Since only INTERNAL-SESSION persistence is
      documented as required (not object-instance survival), the
      orchestrator's public entry point is a STATIC method and its three
      dispatch-tracking tables (`mt_dispatch`/`mt_resolved`/
      `mt_task_outcomes`) are `CLASS-DATA`, with `on_end_of_batch` as a
      STATIC callback (`CALLING zcl_abapgit_ortec_ser_orch=>
      on_end_of_batch ON END OF TASK`). `CLASS-DATA`/`CLASS-METHODS`
      existing for the whole internal session is basic, uncontroversial
      ABAP semantics requiring no special proof — it is EXACTLY the scope
      the documentation confirms is necessary and sufficient. This is the
      SMALLEST proven option (Option A, in static form), not a more
      complex fallback: it needed no new class, no persistent DDIC layer,
      and no long-lived explicit owner object. Cross-run isolation in this
      shared static storage is guaranteed by construction: `task_name` is
      globally unique per internal session (`|SER-{run_guid}-{seq}|`,
      §5.1, `run_guid` a REQUIRED real GUID), so a callback can never
      resolve against a different run's dispatch regardless of how many
      runs' rows coexist.
      BOUNDED GROWTH  Because the tables are now session-wide (not
      call-scoped), each run purges its OWN fully-terminal rows before
      returning (§5.0 step 8), and an explicit, bounded abandoned-dispatch
      ledger policy (§5.1a: `max_abandoned_tasks_per_run=50`,
      `max_abandoned_runs_per_internal_session=20`,
      `max_abandoned_tasks_per_internal_session=200`,
      `retained_metadata_byte_limit=2 MB`) prevents unbounded growth
      across many `serialize()` calls in one long-lived session.
      EXPLICIT STATE MODEL  `LOGICALLY_ABANDONED` is never equated with
      "cancelled" or "resource freed" — see adaptive batch design §5.1a's
      full `LOGICALLY_ABANDONED`/`CALLBACK_PENDING`/`CALLBACK_RECEIVED`/
      `RECEIVE_FAILED`/`WORKER_TERMINATED`/`RFC_RESOURCE_RECLAIMED`/
      `RESOURCE_STATE_UNKNOWN` model.
      REMAINING_BOUNDARY (explicit, not hidden)  If the ENTIRE internal
      session ends before a truly-hung worker's task completes, that
      result is permanently lost — `ABAPWAIT_ARFC`'s own documentation
      confirms this is simply how aRFC behaves ("only the callback
      routines of those asynchronous functions not ended at the end of
      the program are not executed"), identical for TODAY's existing
      standard `run_parallel` mechanism. This is a SAP Basis/gateway/
      work-process-timeout operational concern, not something this or any
      application-level design resolves.
      VERIFICATION_STEPS  The documentation-reading step that OD-13
      previously made a precondition has now been PERFORMED as part of
      this correction (citations above); no further ABAP Keyword
      Documentation reading is required before SLICE 2. The previously-
      recommended IT8 empirical step (deliberately induce a hung batch
      worker behind a test-only feature flag; confirm `serialize()`
      returns within the documented bound, no dump occurs, and no other
      repository's later `serialize()` call is affected — now ALSO
      confirming the abandoned-dispatch-ledger purge policy behaves as
      designed under repeated abandonment) remains required during
      SLICE 2, as a belt-and-suspenders empirical check, not as a
      precondition for an unresolved architectural question.
      DECISION  ACCEPT this resolution. SLICE 2 is authorized to proceed
      to implementation planning; the one remaining action is the SLICE-2
      empirical IT8 test above, which is a normal test-writing task, not
      an open design question.
```


## 6. SER-4 — Measured expansion process (lightweight, no new architecture)

A repeatable, owner-executable prioritization procedure for adding new
object types to (a) bulk-exists and/or (b) a dedicated batch-scoped
provider, beyond the CLAS/INTF/DTEL/DOMA prototype:

```text
STEP 1  From a real repository's SAT trace (or the existing per-run
        telemetry counters the orchestrator now emits per object type,
        SER-2 §8), compute: object_count(type) x p50/p95 elapsed_ms(type)
        = total wall-time contribution(type).
STEP 2  Rank types by total wall-time contribution, descending.
STEP 3  For the top-ranked type NOT yet covered by a specialized provider:
        - confirm it is parallel-safe (no known enqueue/generation
          side effects incompatible with concurrent RFC workers — cross-
          check against is_no_parallel's existing denylist rationale);
        - confirm DB/API access pattern is bulk-able (FOR ALL ENTRIES or
          equivalent exists for its underlying tables);
        - if both hold: it is eligible for a NEW provider (SER-3 contract)
          as its own small design+implementation slice, following the
          SAME lifecycle/parity/test pattern as the DDIC or OO provider.
        - if either fails: it stays on the GENERIC batch path (still gets
          the RFC-count reduction benefit of SER-2, just no specialized
          prefetch) until proven otherwise.
STEP 4  A type is NEVER added to bulk-exists or a provider without (a) a
        measured cost-share justification from STEP 1-2 and (b) a parity
        test proving identical output with the provider forced to MISS.
```

This process requires no new persistent infrastructure — it runs against
whatever telemetry SER-2 already emits per run (see SER-2 §8) plus ad hoc
SAT traces, exactly as this design's own SER-0 evidence was gathered.

## 7. SER-6 — Optional persistent cost model: decision

```text
DECISION        NOT DESIGNED THIS PASS (evidence-gated, matches owner brief
                instruction to design only if run-local adaptation proves
                insufficient)
EVIDENCE        SER-0 found no existing persistent statistics mechanism and
                no measured case where a cold run-local EWMA (falling back
                to type-family static defaults for the first objects of a
                never-before-seen-this-run type) produced a materially bad
                schedule. The LPT-first initial ordering (SER-2 §3) already
                absorbs most of the risk of a bad FIRST guess, because
                heavy-looking objects are front-loaded regardless of
                whether the estimate is exact.
ENTRY CONDITION A future measurement shows repeated, materially bad initial
                batch shapes (e.g. one worker idle while another processes
                a misjudged heavy batch) specifically caused by cold-run
                estimation, on more than one real repository, AFTER SER-2
                is implemented and measured. Until then this stays
                unimplemented; no DDIC, no design.
```
