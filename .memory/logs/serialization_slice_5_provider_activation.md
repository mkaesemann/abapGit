# SER-SLICE-5 — proof that provider buffers are consumed in the worker

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_5_PROVIDER_ACTIVATION
STATUS=PARTIAL_STATICALLY_PROVEN
```

## Evidence tiers used (per the task's own required distinction)

```text
STATICALLY_PROVEN = proven by exhaustive source/control-flow reading, live
  against the actual IT8 active source (not a local guess).
OBSERVED = seen via a live debugger/trace/breakpoint session this turn.
MEASURED = a real counted number captured from a live run this turn (SAT,
  aggregate counter, or added-then-removed diagnostic seam).
```

No `OBSERVED`/`MEASURED` evidence was collected this session - no breakpoint/
debugger tool and no fresh SAT trace were available or supplied this turn (the
owner's message this session did not include a new trace pair, unlike the
prior SER-SLICE-5 discovery session). Adding a new temporary diagnostic seam
and then removing it again was judged out of proportion for this task given
the already-strong static proof below and the owner's own informal
confirmation the fix "still works" - it remains the single concrete
recommended next step if formal measurement is wanted (see &sect;"Recommended
next step").

## STATICALLY_PROVEN chain (live IT8 source, read this session)

```text
1. zabapgit_ortec_serial.fugr.z_abapgit_ortec_ser_batch.abap (live IT8):
   for each of the 6 provider families, `clear_*_cache( )` then
   `IF iv_prefetch_buffer_* IS NOT INITIAL. TRY. inject_batch_from_buffer_*(
   ... ). CATCH ... ENDTRY. ENDIF.` - buffers ARE injected into the worker
   session's own class-static cache tables (mt_msag/mt_dtel/mt_doma/
   mt_fugr_*/mt_classtx/mt_compotx/mt_subcotx/mt_prog_langs/mt_tabl_*)
   whenever the caller (ORCH's BEFORE_DISPATCH) sent a non-empty buffer.
2. Immediately after (line 170), `set_serial_prefetch_active( abap_true )` is
   now called - CONFIRMED live on IT8 (SLICE5-001 fix), unlike before this
   session's earlier fix.
3. `zcl_abapgit_objects=>serialize( ... )` is called per object inside the
   loop, which calls `li_obj->serialize( li_xml )` on the concrete object
   handler (e.g. `zcl_abapgit_object_fugr`, `zcl_abapgit_object_doma`,
   `zcl_abapgit_oo_base` for CLAS/INTF, etc.).
4. Each of those handlers' own serialize path contains
   `IF zcl_abapgit_ortec_git_switch=>is_serial_prefetch_active( ) = abap_true.
   <read from the SAME cache populated in step 1> ELSE <native SELECT>
   ENDIF.` (confirmed by direct source read for FUGR's `functions()`/
   `serialize_xml()`/`serialize_texts()` in the prior SER-SLICE-5 discovery
   session, and by the class-level `IF is_serial_prefetch_active(...)` gate
   pattern already documented for DOMA/DTEL/MSAG/PROG/TABL/CLAS/INTF in
   `serialization_final_two_path_trace_audit.md`).
5. Since step 2 now makes `is_serial_prefetch_active()` return TRUE for the
   entire duration of the loop, step 4's IF branch is now REACHABLE and will
   be taken whenever the specific object's key is present in the cache
   populated in step 1 - this is the exact chain that was PROVEN BROKEN
   before this session's fix (step 2 was previously absent).
```

This chain is proven end-to-end at the source level, on the LIVE IT8 active
version, not a stale local copy - it is as strong as static analysis can be,
but it is still static: no live batch dispatch was observed producing an
actual HIT this session.

## Per-family observability

```text
DOMA/DTEL/CLAS/INTF/MSAG    the RFC's own per-object CASE statement (lines
                             ~205-262) independently calls get_dtel_data/
                             get_doma_data/get_descriptions_class/_compo/
                             _subco/get_msag_data to set ls_result-provider_
                             hit/miss - THIS telemetry is genuinely switch-
                             independent (always reflects real cache content),
                             but ls_result itself is NOT persisted or
                             aggregated anywhere the orchestrator or this
                             session could query afterward (confirmed:
                             grep for provider_hit/provider_miss/provider_
                             fallback in ZCL_ABAPGIT_ORTEC_SER_ORCH returns
                             ZERO matches - the caller does not currently
                             read or accumulate this telemetry at all).
TABL/PROG/FUGR               same CASE-statement telemetry pattern, same
                             non-persistence gap.
WAPA                         no telemetry field exists for WAPA replacement
                             selection at all (is_wapa_active() has no
                             counterpart CASE branch in this RFC) - see the
                             separate WAPA IT8 log.
```

`entries injected`/`hits`/`misses`/`fallbacks`/`bytes` per family: NOT
recorded this session (no counter exists to read, no new run was observed).

```text
WORKER_PROVIDER_CONSUMPTION=PARTIAL
PROVIDER_EVIDENCE=STATIC_ONLY
```

## Recommended next step (not performed this session)

The RFC already COMPUTES `provider_hit`/`provider_miss`/`provider_fallback`
per object (line ~205-262) but the caller discards it. The lowest-risk way to
get real `MEASURED` evidence without any per-object production logging: have
`ZCL_ABAPGIT_ORTEC_SER_ORCH`'s existing `ON_END_OF_BATCH`/merge step
accumulate `ls_result-provider_hit`/`provider_miss`/`provider_fallback` into
ALREADY-EXISTING run-level counters (the run context table already tracks
per-run aggregates for other purposes) and surface the total once per run
(not per object) - e.g. via the existing log interface (`ii_log->add_
success`) at the end of `SERIALIZE`. This is a genuinely new, bounded,
reviewable change (not authorized or implemented this session, since the
prompt's own review-gate rules require a focused design + adversarial review
for anything touching ORCH's cross-batch aggregation state) - named here as
the concrete follow-up if the owner wants durable, queryable per-provider
telemetry instead of the current transient per-RFC-call result table.
