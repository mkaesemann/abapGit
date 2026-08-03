# SER-3 — Batch-Scoped Provider Lifecycle Design

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SERIALIZATION_SER3_PROVIDER_DESIGN
STATUS=DRAFT_FOR_REVIEW
DEPENDS_ON=serialization_ser0_audit.md §2,§5,§7(G-1,G-2);
  serialization_ser0_audit_ortec.md §2-4
```

## 1. Provider contract (facade over existing classes, not a rewrite)

```abap
INTERFACE zif_abapgit_ortec_ser_prov.
  METHODS supports          IMPORTING iv_obj_type       TYPE trobjtype
                             RETURNING VALUE(rv_yes)     TYPE abap_bool.
  METHODS prepare           IMPORTING it_tadir          TYPE zif_abapgit_definitions=>ty_tadir_tt
                                       iv_language       TYPE spras
                             RAISING   zcx_abapgit_exception.
  METHODS extract_for_batch IMPORTING it_tadir          TYPE zif_abapgit_definitions=>ty_tadir_tt
                             RETURNING VALUE(rv_buffer)  TYPE xstring.
  METHODS inject_from_buffer IMPORTING iv_buffer        TYPE xstring.
  METHODS clear.
  METHODS get_version       RETURNING VALUE(rv_version) TYPE i.
ENDINTERFACE.
```

This is a THIN wrapper contract: `prepare`/`clear` map 1:1 onto the
EXISTING `ser_pref*` `prepare()`/`clear()` methods (unchanged bodies).
`extract_for_batch` and `inject_from_buffer` are the only genuinely NEW
methods, added to the EXISTING classes (not new classes) for the OO and
message/documentation families, per the "reduce existing object-level
modifications through a provider facade" evaluation below. Interface
implementation on an EXISTING static-method class requires each existing
class to also expose an INSTANCE that implements the interface (a thin
singleton wrapper, since `ser_pref*` are pure static/CLASS-DATA classes
today) — see §5 for the exact migration shape.

## 2. Family assignment (as specified in the owner brief)

```text
OO / CLAS / INTF        -> wraps ZCL_ABAPGIT_ORTEC_SER_PREF_OO (facade)
DDIC / DTEL / DOMA      -> NEW ZCL_ABAPGIT_ORTEC_SER_PROV_DD (wraps existing
                           mt_dtel logic from SER_PREF_EXT for DTEL + NEW
                           DOMA bulk read)
Message and documentation/i18n
                        -> wraps ZCL_ABAPGIT_ORTEC_SER_PREF (facade)
WAPA/BSP                -> NOT wrapped in the prototype (SER-5 disposition:
                           later slice, tests-first prerequisite)
Enhancement objects      -> wraps ZCL_ABAPGIT_ORTEC_SER_PREF_EXT's mt_enhs
                           (facade, optional for prototype - ENHS is not in
                           the SLICE 1-2 scope, listed for completeness)
Generic/no-prefetch      -> a trivial ZCL_ABAPGIT_ORTEC_SER_PROV_GEN whose
                           `supports()` always returns TRUE (last-resort
                           match), `prepare`/`extract_for_batch`/
                           `inject_from_buffer` are all no-ops, `clear` is
                           a no-op - this exists purely so the orchestrator
                           has ONE provider-dispatch loop shape (find first
                           supporting provider, generic always matches
                           last) rather than a special-cased "no provider"
                           branch.
```

### New DOMA coverage (the one genuinely new prefetch surface)

```text
TABLES READ   dd01l (domain header), dd01v (version-specific texts/
              attributes), dd07l/dd07t (fixed values + their texts, FOR
              ALL ENTRIES keyed by domname).
VERSION_SEMANTICS  CORRECTED per correctness review DR-003: the original
              text incorrectly borrowed DTEL's EXISTENCE-check version
              filter ('0'/'1', ser0_audit_ortec.md §1) as if it were an
              established DOMA precedent — it is not; no DOMA-specific
              version convention is documented anywhere in the SER-0
              audits, and ZCL_ABAPGIT_OBJECT_DOMA's own serialize()-path
              version semantics were never traced by this design. This is
              an OPEN, NOT-YET-RESOLVED implementation precondition, not a
              design decision made here: before writing the new DOMA
              provider, read ZCL_ABAPGIT_OBJECT_DOMA's existing serialize()
              method and state explicitly which DD01L/DD01V/DD07L/DD07T
              version value(s) it reads TODAY, then cite that (not any
              DTEL rule) as the new provider's target semantics. This is
              added to the SLICE 3 implementation-readiness checklist
              (see the bootstrap handoff) as a mandatory first step, before
              any DDIC/provider code is written.
PARITY PROOF  Required before this is authorized for implementation: a
              side-by-side comparison of ZCL_ABAPGIT_OBJECT_DOMA's existing
              SAP-API-based serialize() output (unchanged) against the SAME
              object's output when its documentation/fixed-value READ calls
              are answered from this new provider instead of a live SELECT
              - byte-identical XML required. This is a parity TEST
              obligation (see test matrix), not a design assumption; the
              provider must not change ZCL_ABAPGIT_OBJECT_DOMA's own logic,
              only supply the SAME data it would otherwise SELECT itself
              (mirrors the message/DTEL/CLAS pattern exactly - miss ->
              standard SELECT, hit -> identical data from cache).
AUTHORIZATION_PARITY  The new bulk SELECTs run in the SAME session/user
              context as the existing standard read they replace (this is
              a prepare-time bulk SELECT executed by the SAME calling code
              that would otherwise trigger the per-object SAP API/SELECT -
              no new RFC destination, no new user context, so authorization
              exposure is identical in kind to the already-accepted
              bulk_exists DOMA/DTEL/CLAS/INTF pattern).
```

## 3. Lifecycle and ownership

```text
prepare()    called ONCE per serialize() call (RUN-scoped, unchanged from
             today - see performance design §3 rationale: this is already
             correctly sized to the caller's lt_tadir, K or N depending on
             the caller, and changing it to per-batch would only shrink an
             already-acceptable footprint at the cost of re-running the
             SAME bulk SELECT once per batch instead of once per run -
             strictly worse, not adopted).
extract_for_batch()  called ONCE per DISPATCHED BATCH (new granularity),
             replacing today's once-per-OBJECT extract_for_object() calls
             for objects going through the batch path. extract_for_object()
             remains for any object still routed through the single-object
             standard path (forced_sequential objects never call this at
             all, since they run in-process and read the cache directly via
             the existing get_*() accessor methods with zero export/import
             round trip).
inject_from_buffer() called ONCE per BATCH WORKER dispatch (was: once per
             single-object worker dispatch) - the SAME clear-before-insert
             defensive pattern applies unchanged, now populating from a
             buffer that may contain MULTIPLE objects' worth of rows in one
             IMPORT.
clear()      called EXCLUSIVELY by the OUTER `zcl_abapgit_serialize=>
             serialize` method's own existing CLEANUP block, exactly once
             per serialize() call, identical timing to today (CORRECTED
             per AR-1-007: the new orchestrator never calls provider
             clear() itself, on any exit path — see performance design §2,
             §3).
```

## 4. `extract_for_batch` exact shape (decision-free pseudocode, per
existing class)

```abap
METHOD extract_for_batch.
  DATA: lt_msag TYPE ty_msag_tt,
        lt_dokil TYPE ty_dokil_tt.
  LOOP AT it_tadir INTO DATA(ls_tadir).
    " reuse the EXISTING per-object extraction logic unchanged, just
    " accumulate into the batch-wide tables instead of exporting per row
    IF ls_tadir-object = 'MSAG'.
      READ TABLE mt_msag ... " same lookup as today's extract_for_object
      IF sy-subrc = 0. APPEND ... TO lt_msag. ENDIF.
    ENDIF.
    " dokil range-read logic (unchanged), appended into lt_dokil
  ENDLOOP.
  IF lt_msag IS NOT INITIAL OR lt_dokil IS NOT INITIAL.
    EXPORT msag = lt_msag dokil = lt_dokil language = mv_language
      TO DATA BUFFER rv_buffer COMPRESSION ON.
  ENDIF.
ENDMETHOD.
```

No existing method body changes; `extract_for_batch` is purely additive and
literally reuses the same per-object lookup logic already proven correct in
`extract_for_object`, just looped over the batch's rows and exported once
instead of N times. This is the lowest-risk possible way to get the "fewer,
larger EXPORT calls" benefit.

**Session-isolation clarification (adversarial review PASS-2 required
clarification):** each parallel RFC worker (`Z_ABAPGIT_ORTEC_SER_BATCH`)
executes in its OWN separate ABAP session/work process — CLASS-DATA is
NOT shared across workers, or between a worker and the main session, by
the ABAP runtime itself. The safety mechanism for a REUSED (pooled) worker
session serving multiple unrelated dispatches over its lifetime is
EXCLUSIVELY the existing clear-before-insert pattern inside
`inject_from_buffer` (SOURCE_CONFIRMED already correct, SER-0 §2) — it is
not, and was never intended to be, instance isolation via the facade
classes in §5 below. Facade instances exist ONLY in the callER's (main
process's) own session, where they intentionally share the SAME run-scoped
static cache with each other by design (there is exactly one
serialization run in that session at a time).

## 5. Migrating existing classes to implement the facade interface

`ser_pref*` classes are pure static-method/`CLASS-DATA` classes today (no
instance, no interface). To implement `zif_abapgit_ortec_ser_prov` (an
instance interface) without rewriting them, use ONE parameterized facade
class (NOT three separate per-class facades — `ZCL_ABAPGIT_ORTEC_SER_PREF_
FACADE`/`_EXT_FACADE`/`_OO_FACADE` all exceed ABAP's 30-character global
object-name limit, 33/37/36 chars respectively; the correctness review
caught this, see .memory/reviews/serialization_correctness_review.md
DR-001):

```abap
CLASS zcl_abapgit_ortec_ser_prov_fcd DEFINITION.  " 30 chars exactly
  PUBLIC SECTION.
    INTERFACES zif_abapgit_ortec_ser_prov.
    METHODS constructor IMPORTING iv_family TYPE c LENGTH 4. " 'MSG '|'EXT '|'OO  '
  PRIVATE SECTION.
    DATA mv_family TYPE c LENGTH 4.
ENDCLASS.
CLASS zcl_abapgit_ortec_ser_prov_fcd IMPLEMENTATION.
  METHOD zif_abapgit_ortec_ser_prov~supports.
    CASE mv_family.
      WHEN 'MSG '. rv_yes = boolc( iv_obj_type = 'MSAG' ).
      WHEN 'EXT '. rv_yes = boolc( iv_obj_type CA 'DTEL,ENHS,FUGR,PROG,SMIM,TOBJ,TRAN' ). " illustrative only
      WHEN 'OO  '. rv_yes = boolc( iv_obj_type = 'CLAS' OR iv_obj_type = 'INTF' ).
    ENDCASE.
  ENDMETHOD.
  METHOD zif_abapgit_ortec_ser_prov~prepare.
    CASE mv_family.
      WHEN 'MSG '. zcl_abapgit_ortec_ser_pref=>prepare( it_tadir = it_tadir iv_language = iv_language ).
      WHEN 'EXT '. zcl_abapgit_ortec_ser_pref_ext=>prepare( it_tadir = it_tadir iv_language = iv_language ).
      WHEN 'OO  '. zcl_abapgit_ortec_ser_pref_oo=>prepare( it_tadir = it_tadir iv_language = iv_language ).
    ENDCASE.
  ENDMETHOD.
  " extract_for_batch / inject_from_buffer / clear / get_version: same
  " CASE-on-mv_family delegation pattern
ENDCLASS.
```

One class, three instances created once by the orchestrator (`NEW
zcl_abapgit_ortec_ser_prov_fcd( 'MSG ' )`, `( 'EXT ' )`, `( 'OO  ' )`),
registered into the SAME provider-dispatch list the DDIC and generic
providers use (§2) — this satisfies OD-8's "additive, not touching any
existing method body" answer while staying within the 30-character limit
with a single new global class instead of three.

## 6. Row/byte/in-flight budgets

**CORRECTED per adversarial finding AR-1-005/009:** the original claim that
row/estimated-byte limits alone implicitly bound provider output was
incomplete — a single row can still produce a disproportionately large
`extract_for_batch` buffer (e.g. a wide DOKIL prefix-range hit). Providers
still do not enforce their OWN separate limit system (avoiding a second,
potentially conflicting set of constants), but their combined
`extract_for_batch` output is now measured with `xstrlen(...)` and checked
against a HARD, ACTUAL-bytes ceiling (`c_max_actual_batch_bytes`)
immediately before every dispatch — see adaptive batch design §5.9 for the
exact gate. This is the authoritative, evidence-based bound; the
estimated-byte planning limit (SER-2 §4/§9) remains a separate, advisory,
SCHEDULING-only heuristic.

## 7. Cleanup matrix

```text
NORMAL COMPLETION      clear() in the OUTER `zcl_abapgit_serialize=>
                        serialize`'s existing CLEANUP block (performance
                        design §3), unchanged timing from today — the new
                        orchestrator never calls provider clear() itself
                        (CORRECTED per AR-1-007).
EXCEPTION MID-RUN       same outer CLEANUP block fires regardless of
                        whether the orchestrator succeeded, internally
                        self-resolved a partial failure (adaptive batch
                        design §5.0/§6 "Feature disabled or ORTEC
                        initialization failing"), or propagated an
                        exception caught by the performance-design §2 hook.
FEATURE OFF             providers are never prepare()'d at all (the
                        orchestrator is never entered, performance design
                        §2) - zero footprint, identical to today's
                        behavior when is_serial_prefetch_active() is off.
RFC WORKER SESSION      inject_from_buffer's existing clear-before-insert
REUSE                   pattern (SOURCE_CONFIRMED already correct) is
                        unchanged and remains the sole defense against
                        stale pooled-session data; no new risk from
                        batching (a batch worker's buffer is simply larger
                        rows-wise, same clear-then-insert shape).
```

## 8. Provider hit/miss/fallback observability

Each provider's `extract_for_batch`/accessor methods increment run-local
counters (SER-2 §8 telemetry) — `PROVIDER_HIT`/`PROVIDER_MISS`/
`PROVIDER_FALLBACK` per `ZAOG_SER_BATCH_RESULT` row, aggregated by the
orchestrator per object type at run end. No new persistence; reuses the
existing per-run log object.

## 9. Strategy for migrating/retiring current SER_PREF* class-data caches

```text
DECISION           NONE of the three existing classes are retired. All
                    three gain two new methods each (extract_for_batch,
                    and a thin facade class per §5) and otherwise remain
                    exactly as they are. ZCL_ABAPGIT_ORTEC_WAPA is
                    explicitly NOT wrapped in this slice (SER-5).
RATIONALE           Every existing consumer call site (SER-0 §5, "Call
                    Site 4" family, 8 files) continues to call the
                    EXISTING get_*()/extract_for_object() methods
                    completely unchanged - this design adds a parallel,
                    additive batch-extraction path used ONLY by the new
                    orchestrator, so there is zero regression surface on
                    any existing consumer.
```
