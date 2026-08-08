# SER-SLICE-5 — final two-path trace-purity audit (mandatory Phase 2)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_5_TRACE_PURITY_AUDIT
STATUS=PASS
SCOPE=SOURCE_CROSS_REFERENCE + LIVE TRACE CORRELATION (both supplied SAT files
  read in full: `Full Repo - Normal Serialize.txt`, `Full Repo - Batch Serialize
  1.txt`)
```

## Question asked

Does the Normal Serialize trace's presence of
`ZCL_ABAPGIT_ORTEC_SER_PREF=>EXTRACT_FOR_OBJECT`,
`ZCL_ABAPGIT_ORTEC_SER_PREF_EXT=>EXTRACT_FOR_OBJECT`,
`ZCL_ABAPGIT_ORTEC_SER_PREF_OO=>EXTRACT_FOR_OBJECT` indicate a live ORTEC
optimization on the repository-setting-OFF path (a regression against the
Phase 7 "Path 3 removed" design)?

## Evidence

```text
Normal trace: 25,937 hits each for SER_PREF/SER_PREF_EXT/SER_PREF_OO
  =>EXTRACT_FOR_OBJECT, calling program ZCL_ABAPGIT_SERIALIZE (i.e. inside
  RUN_PARALLEL, not ZCL_ABAPGIT_ORTEC_SER_ORCH - no SER_ORCH symbol appears
  anywhere in the Normal trace). 25,937 = 17,321 objects x ~1.497 retry factor
  (matches SPBT_FIND_FREE_SERVER's own 25,937 hits - the DO/CONTINUE retry loop
  in RUN_PARALLEL on SY-SUBRC=3 "no free work process").
Net cost per call ~41 microsec (1,074,506 net / 25,937 hits for the _EXT
  variant) - no DB/SQL statement appears anywhere under these three specific
  hit-list rows. This is the signature of a pure in-memory READ TABLE against
  an EMPTY hashed cache, not a real optimization.
```

Source cross-reference (current HEAD `f54860d1`):

```text
1. src/objects/core/zcl_abapgit_serialize.clas.abap:662-664 (RUN_PARALLEL) still
   calls EXTRACT_FOR_OBJECT on all three prefetch classes unconditionally, then
   passes the (always-empty, see below) buffers into the STANDARD
   Z_ABAPGIT_SERIALIZE_PARALLEL RFC's IV_PREFETCH_BUFFER*/params. This is a
   DIFFERENT method from the removed Path-3 block (which was
   RUN_SEQUENTIAL/RUN_PARALLEL's own now-deleted PREPARE()/CLEANUP wrapper) -
   confirmed by re-reading RUN_SEQUENTIAL (line 712), which has ZERO ORTEC
   prefetch calls of any kind.
2. `PREPARE()`/`PREPARE_*` on all three prefetch classes has exactly 4
   production call sites, all inside
   src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap's own SERIALIZE
   method (entry, uuid-failure, success-exit, run-failure-exit) - confirmed via
   grep across all of src/. RUN_PARALLEL/RUN_SEQUENTIAL never call PREPARE, so
   MT_MSAG/MT_DTEL/MT_DOMA/MT_FUGR_*/MT_CLASSTX etc. are always empty when
   EXTRACT_FOR_OBJECT runs from the classic path - every one of the 3x25,937
   calls is a guaranteed MISS returning an initial `xstring` (each
   EXTRACT_FOR_OBJECT body: `IF <all relevant lt_* tables> IS INITIAL. RETURN.
   ENDIF.` before the EXPORT).
3. src/objects/core/zabapgit_parallel.fugr.z_abapgit_serialize_parallel.abap:29
   guards BOTH `inject_from_buffer(...)` and
   `set_serial_prefetch_active( abap_true )` behind
   `IF iv_prefetch_buffer IS NOT INITIAL OR ... OR iv_prefetch_buffer_oo IS NOT
   INITIAL.` - since the buffers built in step 1 are always empty on this path,
   this guard never fires. `is_serial_prefetch_active()`/`is_wapa_active()`
   therefore never become TRUE anywhere in the classic path's own RFC worker
   session either.
4. `is_wapa_active()` (src/ortec/git/zcl_abapgit_ortec_git_switch.clas.abap:286)
   delegates to the exact same `mv_serial_prefetch_active` flag (Phase 7 fix,
   `serialization-slice-3.md`) - confirmed via Normal-trace absence of any
   `ZCL_ABAPGIT_ORTEC_WAPA` symbol; the 4 real WAPA objects in this trace are
   served entirely by the STANDARD `ZCL_ABAPGIT_OBJECT_WAPA`/`CL_O2_API_*`
   classes (`DB Buffer & SQL Engine: Fetch TABL FETCH(Q) ZCL_ABAPGIT_OBJECT_WAPA`,
   4 hits, no ORTEC_WAPA symbol anywhere in the file).
```

## Classification

```text
TRACE_PRE_DATES_FINAL_CLEANUP        NO - trace is dated 08.08.2026, taken
                                      against current HEAD f54860d1 (owner
                                      confirms "final IT8 syntax and ABAP Unit
                                      fixes" are the current source)
INERT_CALL_NO_OPTIMIZATION           YES - this is the binding classification
CONFIRMED_OFF_PATH_REGRESSION        NO
INSUFFICIENT_EVIDENCE_OWNER_TRACE_NEEDED  NO - both source and live trace agree
```

`TWO_PATH_TRACE_AUDIT=PASS`. The three `EXTRACT_FOR_OBJECT` calls visible on the
OFF path are a real, structurally-present artifact of `RUN_PARALLEL`'s own
buffer-building code (mirroring the STANDARD RFC's existing parameter shape),
but they perform zero DB/RFC work, populate nothing, and are proven (both by
static call-graph and by the live trace's own timing/absence-of-child-calls) to
never influence what gets serialized on the OFF path. This exact disposition
was already recorded as a deliberate, reviewed decision in
`.memory/handoffs/serialization-slice-3.md` ("Phase 7 implementation record" -
"self-neutralize ... zero DB access") - this audit independently re-verifies it
against the NEW full-repository trace rather than assuming it still holds.

```text
OFF_PATH_ORTEC_OPTIMIZATION_CALLS=INERT
  (ZCL_ABAPGIT_ORTEC_SER_PREF/_EXT/_OO=>EXTRACT_FOR_OBJECT via RUN_PARALLEL)
```

## Required OFF-path invariant - final check

```text
no provider PREPARE/EXTRACT/INJECT/LOOKUP   PARTIAL - EXTRACT_FOR_OBJECT calls
                                             occur but are proven inert (no
                                             PREPARE/INJECT/real LOOKUP hit is
                                             possible on this path)
no ORTEC WAPA replacement                   PASS (confirmed, no ORTEC_WAPA
                                             symbol in the OFF-path trace)
no ORTEC batch RFC                          PASS (zero Z_ABAPGIT_ORTEC_SER_BATCH
                                             calls in the Normal trace)
no legacy non-batch ORTEC serialization
  optimization (Path 3)                     PASS (Path 3 deleted in Phase 7,
                                             confirmed by source; RUN_SEQUENTIAL
                                             has zero ORTEC calls; the classic
                                             PREPARE()/CLEANUP block does not
                                             exist in current HEAD)
```

## Critical finding (out of this audit's literal scope, discovered while
cross-referencing the Batch trace for Phase 3)

```text
FINDING_ID=SLICE5-001
SEVERITY=MAJOR (performance, not correctness)
STATUS=FIXED_THIS_SESSION_AWAITING_IT8
```

While comparing FUGR's per-object DB cost between the two traces (Phase 3), the
Batch trace showed IDENTICAL `DB: Fetch/Open ENLFDIR|REPOTEXT|REPOSRC|EUDB|
D010INC|TCDRP|RSEUINC` hit counts (358, one per FUGR object) and near-identical
net times to the Normal (non-batch) trace - i.e. the FUGR metadata/directory
provider appeared to have a 0% real hit rate even in the batch run, despite
`PREPARE`/`inject_batch_from_buffer_fugr` being called correctly.

Root cause (confirmed via source, `src/ortec/serial/rfc/
zabapgit_ortec_serial.fugr.z_abapgit_ortec_ser_batch.abap`): the ORTEC batch RFC
worker function runs in a SEPARATE aRFC session from
`ZCL_ABAPGIT_ORTEC_SER_ORCH` - `CLASS-DATA` (including
`mv_serial_prefetch_active`) does not cross that RFC boundary. The function
correctly calls `clear_*_cache( )`/`inject_batch_from_buffer_*( )` for every one
of the 8 provider families, but it NEVER called
`zcl_abapgit_ortec_git_switch=>set_serial_prefetch_active( abap_true )` inside
its own session - the ONE existing production call site for that setter is in
the UNRELATED standard `Z_ABAPGIT_SERIALIZE_PARALLEL` RFC (itself dead per this
audit's OFF-path finding above). Consequence: every object serializer's own
`IF is_serial_prefetch_active( ) = abap_true` gate (DOMA, DTEL, CLAS/INTF, MSAG,
TABL, PROG, FUGR, ENHS, SMIM, TOBJ, TRAN) and `IS_WAPA_ACTIVE` returned FALSE
inside the real batch worker for the entire history of SER-SLICE-3/4's
production use - every object fell back to its native per-object read, and the
WAPA replacement serializer never actually ran inside a real batch dispatch
either, DESPITE the injected caches being correctly populated and DESPITE the
worker's own `provider_hit`/`provider_miss` telemetry (a SEPARATE, switch-
independent pre-check) reporting hits.

This exactly explains why `PER_PROVIDER_INCREMENTAL_BENEFIT=NOT_ISOLATED` (SER-
SLICE-4 closeout): there was no incremental benefit to isolate, because the
mechanism delivering it was never live. It does NOT explain away
`OUTPUT_PARITY=PASS`/`ACTIVATION=PASS`/`ABAP_UNIT=PASS` - the fallback path is,
and always was, fully correct; this is a pure missed-optimization defect, not a
data-correctness defect.

**Fix applied this session** (minimal, mechanical, precedented - mirrors the
existing `Z_ABAPGIT_SERIALIZE_PARALLEL` pattern exactly): added
`zcl_abapgit_ortec_git_switch=>set_serial_prefetch_active( abap_true )`
immediately after the six `inject_batch_from_buffer_*`/`inject_from_buffer`
blocks (before the per-object `LOOP AT it_tadir`), and
`set_serial_prefetch_active( abap_false )` immediately after `ENDLOOP` (before
`ev_output_row_count` is set). Per-object exceptions are already caught INSIDE
the loop (existing `TRY...CATCH zcx_abapgit_exception`) and never escape the
function, so a single set/reset pair around the loop is sufficient - no
additional exception-safety wrapper is needed. `get_errors` clean on the
changed file. See
`.memory/reviews/serialization_slice_5_correctness.md` for the full disposition
and `.memory/logs/serialization_slice_4_regression.md` for how this was
originally staged and reviewed.

This fix is authorized under "confirmed regression in the already-approved
two-path architecture" (the adaptive-batch path, `KEEP_AND_EXTEND`, per binding
invariants) - it does not implement any new provider, design, or optimization;
it activates code paths that already existed, were already reviewed, and were
already unit-tested (via a test-only seam that bypassed the real RFC), matching
the design's own original intent exactly.

**This fix has NOT been validated on a live system.** No new performance number
in this slice should be read as proof the fix helps in production - only that
it is the confirmed, source-verified cause of the previously-unexplainable
"integrated but not isolatable" provider result. `OWNER_ACTION_REQUIRED`: rerun
the SER-SLICE-4 IT8 plan's &sect;9 SAT comparison (or a fresh full-repo trace pair)
with this fix in place to measure the REAL incremental provider benefit and
confirm WAPA's batch-path replacement now actually activates and produces
parity output for at least one real WAPA-in-a-batch run.
