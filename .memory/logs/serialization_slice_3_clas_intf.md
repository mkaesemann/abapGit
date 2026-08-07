# SER-SLICE-3 Phase 4 — CLAS/INTF batch provider

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_3_PHASE_4_CLAS_INTF_PROVIDER
STATUS=IMPLEMENTED_LOCAL_GET_ERRORS_CLEAN
```

(Written by the orchestrator - the delegated implementation subagent made
the correct source/test changes per spec but returned no final report;
this file reconstructs the record from the actual diff, which was
reviewed line-by-line. Supersedes the prior DEFERRED disposition
previously recorded in this file - kept below for history is not useful
once implemented, so this file now records only the implementation.)

CLAS/INTF now has a real, versioned, multi-object batch envelope,
mirroring the DOMA/DTEL pattern exactly, reusing the new generic
`ZAOG_SER_ENV_BHDR`/`ZAOG_SER_ENV_BENTRY`/`ZAOG_SER_ENV_BENTRY_TT` DDIC
objects (not yet created on IT8 - see the consolidated global-object
manifest in the final handoff) instead of a third DD-specific set.

## What changed

```text
src/ortec/serial/zcl_abapgit_ortec_ser_pref_oo.clas.abap
  + extract_for_batch(it_object_keys) - CLAS/INTF only, per-object state
    P (any of classtx/compotx/subcotx present) / M (miss), actual_bytes
    measured via the existing extract_for_object single-object wire size.
    Returns INITIAL when not prepared, zero CLAS/INTF objects, or every
    entry would be M (mirrors DD's identical "nothing useful to send"
    guard from the parity-incident fix).
  + inject_batch_from_buffer(iv_buffer) - validates wire_format_version=1,
    object_count vs entries, rejects duplicate obj_type+obj_name entries,
    unconditionally clears mt_classtx/mt_compotx/mt_subcotx before
    repopulating (AR-3-001 pattern), raises zcx_abapgit_exception on any
    corruption (caller treats as a full MISS, never propagates).
  + clear_oo_cache() - narrow clear (3 cache tables only), mirrors
    ser_pref_ext's clear_dd_cache; used by the RFC worker's unconditional
    clear-first-on-every-invocation pattern.
src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap
  dispatch_batch gained iv_prefetch_buffer_oo_batch (parallel to
  iv_prefetch_buffer_dd, threaded through to the RFC call).
  BEFORE_DISPATCH now also computes
  zcl_abapgit_ortec_ser_pref_oo=>extract_for_batch(it_object_keys) once,
  reused across split/dispatch exactly like the DD buffer (disclosed:
  NOT yet folded into the c_max_actual_batch_bytes admission check -
  that gate remains DD-buffer-scoped only, unchanged from before this
  slice, not a silent gap).
  Removed the duplicate, redundant zcl_abapgit_ortec_ser_pref_ext=>
  prepare() call added by a later owner commit (bf436db0) - PREPARE()
  is idempotent (calls CLEAR() internally), so this was a provably safe,
  wasteful duplicate (doubled the DD01L/DD01T/DD07L/DD07T/DD04L/DD04T
  SELECTs on every batch run). The first, comprehensive prepare() call
  covering pref/pref_ext/pref_oo together (this session's Fix A,
  5ff237b9) remains, unchanged.
src/ortec/serial/rfc/zabapgit_ortec_serial.fugr.z_abapgit_ortec_ser_batch.abap
  + IV_PREFETCH_BUFFER_OO_BATCH parameter (NEW - the existing
  IV_PREFETCH_BUFFER_OO parameter is left untouched, still reserved for
  the OLD single-object shape used only by the unrelated standard
  Z_ABAPGIT_SERIALIZE_PARALLEL worker).
  Unconditional clear_oo_cache() at the top of every invocation, then
  conditional inject_batch_from_buffer with a swallowed
  zcx_abapgit_exception (treated as full MISS, mirrors the DD block).
  CASE ls_tadir-object gained WHEN 'CLAS' OR 'INTF': HIT iff any of
  get_descriptions_class/compo/subco found data, one hit/miss per
  object (a class legitimately missing all three text data is a normal
  MISS, not a defect).
src/ortec/serial/rfc/zabapgit_ortec_serial.fugr.xml
  RSIMP/RSFDO rows added for the new parameter.
src/ortec/serial/zcl_abapgit_ortec_ser_pref_oo.clas.testclasses.abap (NEW)
  ltcl_oo_batch_wire, 21 test methods: small class, large class (10
  compo+10 subco rows), interface object type, compo-only/subco-only/
  both-present all count as HIT, missing-optional-still-hit (classtx-only
  class), multiple languages round trip, namespaced name (/NS/ZCL_FOO)
  round trip, mixed CLAS+INTF batch, all-miss batch returns INITIAL,
  neither-present-is-MISS (paired with one real HIT sibling, since an
  envelope needs at least one HIT to be built at all), partial data / no
  cross-contamination, reject unknown wire version, reject duplicate
  entries, reject object_count mismatch, reject corrupt IMPORT, cross-
  batch isolation (two sequential worker-style injects, no pooled-worker
  leakage), full round-trip byte-identical check, extract returns
  INITIAL when no CLAS/INTF objects present, extract returns INITIAL
  when not prepared.
```

## Deviation from spec

None material. `extract_for_batch`'s actual-byte measurement reuses the
existing single-object `extract_for_object` wire shape purely for sizing
(does a second, small EXPORT per HIT object beyond the compound batch
EXPORT) rather than inventing a parallel per-object size-only format -
documented in the method's own comment as the "cheapest correct" choice
available without duplicating export logic.

## Local validation

```text
GET_ERRORS=CLEAN on all 5 touched/created files (ser_pref_oo main +
  testclasses, ser_orch main + testclasses, RFC worker)
METHOD_NAME_LENGTH=CLEAN (longest new name: multiple_languages_round_trip
  = 27 chars, partial_data_no_cross_contam = 27 chars, all <= 30)
LIVE_SYNTAX_DRY_RUN=NOT_RUN this pass (no live SAP connectivity this
  session; ZAOG_SER_ENV_* DDIC objects do not exist on IT8 yet)
ABAP_UNIT=NOT_RUN_LIVE this session
"Direct serialized file-set parity" against a real live CLAS/INTF object
  was explicitly out of scope for this pass (no live SAP connectivity) -
  the full_round_trip_byte_ident test proves the extract/inject round
  trip is lossless for the provider's OWN cached data, but live
  serialized-FILE parity (the actual .clas.abap/.intf.abap output content
  through ZCL_ABAPGIT_OBJECT_CLAS/INTF) remains an IT8-only validation
  step, same disclosed boundary DOMA/DTEL had before its own IT8 pass -
  see the consolidated IT8 validation plan.
```

## Global object dependency

This provider requires `ZAOG_SER_ENV_BHDR` / `ZAOG_SER_ENV_BENTRY` /
`ZAOG_SER_ENV_BENTRY_TT` (already created as local abapGit source files
this session, package same as `ZAOG_SER_BATCH_RESULT`) to exist on IT8
before activation - included in the consolidated global-object manifest
in the final SER-SLICE-3 handoff, not a separate stop.
