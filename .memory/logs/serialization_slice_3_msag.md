# SER-SLICE-3 Phase 6 — MSAG batch provider

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_3_PHASE_6_MSAG_PROVIDER
STATUS=IMPLEMENTED_LOCAL_GET_ERRORS_CLEAN
```

MSAG (message classes) now has a real, versioned, multi-object batch
envelope, reusing the existing generic `ZAOG_SER_ENV_BHDR`/
`ZAOG_SER_ENV_BENTRY`/`ZAOG_SER_ENV_BENTRY_TT` DDIC objects (same ones
CLAS/INTF already uses) - mirroring `ZCL_ABAPGIT_ORTEC_SER_PREF_OO`'s
`extract_for_batch`/`inject_batch_from_buffer`/`clear_oo_cache` shape.

## Scope boundary (deliberate, disclosed)

This batch envelope covers ONLY `mt_msag` (T100/T100A/T100T). `mt_dokil`
(long-text documentation, global, non-per-object-keyed, complex prefix
matching) is explicitly OUT OF SCOPE - it was never part of any batch RFC
path before this task either. `inject_batch_from_buffer`/
`clear_msag_cache` only ever touch `mt_msag`; `mt_dokil`/
`mv_dokil_prepared` are left completely untouched, proven by
`inject_does_not_touch_dokil` (seeds both directly via friend access,
asserts both are unchanged after `inject_batch_from_buffer`). MSAG
long-text documentation remains a MISS on the batch path exactly as it
is today - not a regression.

## What changed

```text
src/ortec/serial/zcl_abapgit_ortec_ser_pref.clas.abap
  + extract_for_batch(it_object_keys) - MSAG only, per-object state P
    (mt_msag has a row for that msg_id) / M (miss), actual_bytes measured
    via the existing extract_for_object single-object wire size (same
    "cheapest correct" sizing reuse as OO/DD; note extract_for_object
    also folds in DOKIL bytes when mv_dokil_prepared is true, which only
    affects the ACTUAL_BYTES sizing metric, never the batch payload
    itself). Returns INITIAL when zero MSAG objects present or every
    entry would be MISS (this also naturally covers "PREPARE() never
    called", since MT_MSAG is then empty and every lookup misses - no
    separate MV_PREPARED flag exists on this class, unlike PREF_OO/
    PREF_EXT).
  + inject_batch_from_buffer(iv_buffer) - validates wire_format_version=1,
    object_count vs entries, rejects duplicate obj_type+obj_name entries,
    unconditionally clears MT_MSAG ONLY before repopulating (MT_DOKIL/
    MV_DOKIL_PREPARED survive untouched), sets MV_LANGUAGE if provided,
    raises zcx_abapgit_exception on any corruption.
  + clear_msag_cache() - narrow clear (MT_MSAG only), mirrors
    CLEAR_OO_CACHE/CLEAR_DD_CACHE; used by the RFC worker's unconditional
    clear-first-on-every-invocation pattern.
src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap
  dispatch_batch gained iv_prefetch_buffer_msag (parallel to
  iv_prefetch_buffer_oo_batch/iv_prefetch_buffer_dd, threaded through to
  the RFC call).
  before_dispatch now also computes
  zcl_abapgit_ortec_ser_pref=>extract_for_batch(it_object_keys) once,
  reused across split/dispatch (disclosed: NOT folded into the
  c_max_actual_batch_bytes admission check - same scope as
  iv_prefetch_buffer_oo_batch, not a silent gap).
src/ortec/serial/rfc/zabapgit_ortec_serial.fugr.z_abapgit_ortec_ser_batch.abap
  + IV_PREFETCH_BUFFER_MSAG parameter.
  Unconditional clear_msag_cache() at the top of every invocation
  (alongside clear_dd_cache/clear_oo_cache), then conditional
  inject_batch_from_buffer with a swallowed zcx_abapgit_exception
  (treated as full MISS, mirrors the DD/OO blocks).
  CASE ls_tadir-object gained WHEN 'MSAG': HIT iff get_msag_data(iv_msg_id,
  iv_language) finds data - single check, this provider only has one
  lookup unlike OO's three.
src/ortec/serial/rfc/zabapgit_ortec_serial.fugr.xml
  RSIMP/RSFDO rows added for the new parameter.
src/ortec/serial/zcl_abapgit_ortec_ser_pref.clas.testclasses.abap (NEW)
  ltcl_msag_batch_wire, 15 test methods: small message class (one T100/
  T100T row), multiple message numbers (5 rows), multiple languages (3
  langu, i18n round trip), namespaced message-class name round trip,
  mixed MSAG batch (one present, one legitimately missing, no cross-
  contamination), all-miss batch returns INITIAL, reject unknown wire
  version, reject duplicate entries, reject object_count mismatch,
  reject corrupt IMPORT, cross-batch isolation (two sequential worker-
  style injects), full round-trip byte-identical check (2 message
  classes, 3 languages, 2 message numbers), extract returns INITIAL when
  no MSAG objects present, extract returns INITIAL when not prepared,
  and inject_does_not_touch_dokil (the scope-boundary regression -
  MT_DOKIL/MV_DOKIL_PREPARED seeded directly via friend access, asserted
  unchanged after inject_batch_from_buffer).
```

## Deviation from spec

`provider_id = 'SER_MSAG'` (8 chars), not the task text's literal
`'SER_MSAG1'` (9 chars) - `ZAOG_SER_ENV_BHDR-PROVIDER_ID` is a CHAR8
field (confirmed by reading the existing DDIC XML; `SER_OO01`/`SER_DD01`
are also exactly 8 chars). `'SER_MSAG1'` would not fit and PROVIDER_ID is
a correlation/telemetry field only, never used for dispatch logic, so
this is a pure cosmetic substitution with no functional effect.

No other material deviations. `extract_for_batch`'s actual-byte
measurement reuses `extract_for_object`'s existing wire shape purely for
sizing (same "cheapest correct" choice OO/DD made), which for MSAG can
also include DOKIL bytes when `mv_dokil_prepared` is true - documented
in the method's own doc comment; this never leaks DOKIL data into the
batch envelope's actual EXPORT payload (only `msag`/`hdr`/`entries`/
`language` are exported).

## Local validation

```text
GET_ERRORS=CLEAN on all 6 touched/created files (pref main + testclasses,
  orch main + testclasses, RFC worker main + xml)
METHOD_NAME_LENGTH=CLEAN (verified via PowerShell Select-String scan of
  the new testclasses file; longest new name 29 chars, all <= 30)
LIVE_SYNTAX_DRY_RUN=NOT_RUN this pass (no live SAP connectivity this
  session; DDIC objects reused, not newly created, so no new IT8
  dependency beyond what CLAS/INTF Phase 4 already required)
ABAP_UNIT=NOT_RUN_LIVE this session
```

## Global object dependency

None new - this provider reuses `ZAOG_SER_ENV_BHDR`/`ZAOG_SER_ENV_BENTRY`/
`ZAOG_SER_ENV_BENTRY_TT`, already required on IT8 by the CLAS/INTF Phase 4
provider (see the consolidated global-object manifest in the final
SER-SLICE-3 handoff).
