# SER-SLICE-3 — DOMA/DTEL batch provider implementation log

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_3_PHASE_2_DOMA_DTEL_PROVIDER
STATUS=LOCAL_COMPLETE_NOT_IT8_VALIDATED
BASELINE_HEAD=daef510e9bd50cdef2adcfb26a3f2a01050bb401
DESIGN=.memory/logs/serialization_slice_3_provider_contract.md
```

## What was implemented

Additive-only extension of `ZCL_ABAPGIT_ORTEC_SER_PREF_EXT` (no new provider
class/interface - see the design doc's SUPERSEDES_DRAFT note):

- `mt_doma` cache (`ty_doma_cache`: `domname`, `dd01v` main-language merged
  structure, `dd01v_i18n`, `dd07v_tab` main-language merged fixed values,
  `dd07v_tab_i18n`) - mirrors the existing `mt_dtel` shape exactly.
- `prepare_doma` - 4 bulk `FOR ALL ENTRIES` SELECTs (DD01L/DD01T/DD07L/DD07T,
  `WHERE as4local = 'A' AND as4vers = '0000'`, never DD01V/DD07V), called
  from the existing `prepare()` entry point alongside `prepare_dtel`.
- `get_doma_data`/`get_doma_i18n` - same found/not-found accessor contract
  as `get_dtel_data`/`get_dtel_i18n`.
- `extract_for_batch`/`inject_batch_from_buffer` - the new versioned batch
  envelope (`ZAOG_SER_DD_BHDR`/`ZAOG_SER_DD_BENTRY`/`ZAOG_SER_DD_BENTRY_TT`,
  new DDIC, `src/ortec/serial/core/`) covering BOTH DOMA and DTEL rows in
  one `EXPORT ... TO DATA BUFFER COMPRESSION ON` call.
- Seam in `zcl_abapgit_object_doma.clas.abap` (`zif_abapgit_object~serialize`
  and `serialize_texts`) mirroring the existing, already-IT8-validated DTEL
  seam exactly: try `get_doma_data`/`get_doma_i18n` when
  `is_serial_prefetch_active()`, fall back to the unchanged
  `CALL FUNCTION 'DDIF_DOMA_GET'`/raw-SELECT path on miss.
- `ZCL_ABAPGIT_ORTEC_SER_ORCH`: `BEFORE_DISPATCH` now computes the real
  DOMA/DTEL batch buffer via `extract_for_batch` once per dispatch and
  reuses it for `lv_actual_bytes` AND the final `DISPATCH_BATCH` call's new
  `iv_prefetch_buffer_dd` parameter (previously always 0/empty,
  DECLARED_ONLY SER-SLICE-2 scope). `c_max_pre_dispatch_splits` (previously
  DECLARED_ONLY, telemetry-only, value 3) is now an ENFORCED hard recursion
  cap, value 12, via the new `split_depth_at_cap` helper.
- `Z_ABAPGIT_ORTEC_SER_BATCH` (RFC worker): injects `iv_prefetch_buffer_dd`
  via `inject_batch_from_buffer`, wrapped in `TRY/CATCH zcx_abapgit_exception`
  so an unknown-version/corrupt buffer degrades to a full MISS for that
  buffer only - the batch itself always still completes.

## Correctness review findings and disposition

Independent review (`.memory/reviews/serialization_slice_3_correctness.md`)
returned `REVISE_AND_REVIEW_ONCE` (1 blocker, 2 major, 2 minor). All 5
required fixes applied and self-verified by re-reading the corrected source
(not independently re-reviewed a second time by the same reviewer - see
that file's STATUS field for the exact honesty disclosure):

```text
DR-001 (BLOCKER) - prepare_doma's language-discovery loop only included
  languages that had an OWN DD01T/DD07T text row, so a domain with no
  main-language text row got a fully INITIAL main-language DD01V and was
  silently dropped by the seam's "ls_dd01v IS INITIAL -> RETURN" guard
  (real, silent data-loss risk, unlike DDIF_DOMA_GET which always returns
  the DD01L-derived header regardless of text presence).
  FIX: lt_langs is now seeded with IV_MAIN_LANGUAGE unconditionally before
  the DD01T/DD07T language-discovery loops.
DR-002 (MAJOR) - no feature-ON/provider-HIT parity test existed for DOMA.
  FIX: PROVIDER_HIT_MATCHES_BASELINE and PROVIDER_HIT_NO_FIXED_VALUES added
  to ltcl_doma_parity, reusing the XFELD/CHAR30 fixtures, asserting
  byte-identical XML between a MISS (empty cache) and a HIT (PREPARE()'d
  cache) call for the SAME domain.
DR-003 (MAJOR) - no test exercised INJECT_BATCH_FROM_BUFFER's actual
  CATCH cx_root branch (only semantic-validation checks were tested).
  FIX: REJECT_CORRUPT_IMPORT added, truncating a well-formed buffer to 3
  bytes and asserting a caught zcx_abapgit_exception.
DR-004 (MINOR) - no dedicated test for "unexpected entry ignored" or
  "valid empty payload is a HIT, not a MISS".
  FIX: UNEXPECTED_ENTRY_IGNORED and EMPTY_PAYLOAD_IS_HIT added.
DR-005 (MINOR, NOT FIXED, documented latent risk) - if a future dispatch
  ever populates BOTH iv_prefetch_buffer_ext and iv_prefetch_buffer_dd for
  the SAME dispatch, inject_batch_from_buffer's unconditional
  "CLEAR mt_dtel" would silently discard whatever inject_from_buffer had
  just populated there. NOT currently reachable - BEFORE_DISPATCH never
  sets iv_prefetch_buffer_ext today, only iv_prefetch_buffer_dd. Recorded
  as a real constraint for whoever wires a future EXT-family batch buffer:
  that work must NOT independently clear mt_dtel, or must coordinate
  clearing order/ownership with this method.
```

Also found and fixed independently (before the correctness review, during
manual orchestrator re-derivation of the reconciliation): the reused
`c_max_pre_dispatch_splits` constant's repurposing from
TELEMETRY/WARNING-only (value 3) to a HARD SAFETY BOUND (value 12) was
confirmed safe via grep (no other code read that constant under the old
semantics - it was genuinely DECLARED_ONLY per SER-SLICE-2's own disclosed
limits table).

Also found and fixed by the orchestrator (not the reviewer): a method-name
length violation (`before_dispatch_dd_buffer_empty`, 31 characters, exceeds
ABAP's 30-character hard limit) introduced in the ORCH testclasses include
while writing the recursion-cap/buffer-threading tests - renamed to
`before_dispatch_dd_buf_empty` (28 chars). A full workspace re-scan of every
touched file confirmed no other violation.

## Performance scan result

`.memory/reviews/serialization_slice_3_performance.md`:
`PASS_WITH_FINDINGS` - one MINOR (PS-001, wide `SELECT *` + repeated
in-memory per-domain scans in `prepare_doma`, acceptable at the existing
`c_max_batch_rows = 25` cap, worth field-limiting only if that cap ever
grows materially). No blocking finding, no new per-object DB call, XSTRING
handling OK (COMPRESSION ON, no repeated append/copy).

## Local validation state

```text
GET_ERRORS=CLEAN on every touched/created file (re-verified after DR-001
  fix and the method-name rename)
LIVE_SYNTAX_DRY_RUN=EXPECTED_DDIC_FAILURE only (ZAOG_SER_DD_BHDR/
  ZAOG_SER_DD_BENTRY/ZAOG_SER_DD_BENTRY_TT do not exist on IT8 yet - this
  is the owner's manual DDIC-creation step, not a real code defect)
UNIT_TESTS=13 new/extended test methods across ltcl_doma_parity (2 new) and
  ltcl_dd_batch_wire (9 total, 4 new: reject_corrupt_import,
  unexpected_entry_ignored, empty_payload_is_hit, plus the earlier 6 from
  the initial implementation pass) plus 4 new ORCH tests
  (split_depth_below_cap_false/at_cap_true/above_cap_true,
  before_dispatch_dd_buf_empty) - NONE executed live yet (no IT8 DDIC
  objects, no ABAP Unit run this session)
METHOD_NAME_LENGTH=CLEAN (full re-scan of all 5 touched files)
```

## Owner manual object creation required before any IT8 validation

```text
OBJECT_TYPE=TABL (structure)
NAME=ZAOG_SER_DD_BHDR
PACKAGE=same as ZAOG_SER_BATCH_RESULT (src/ortec/serial/core/)
DESCRIPTION=ORTEC serialization DOMA/DTEL batch prefetch buffer header
FIELDS=WIRE_FORMAT_VERSION TYPE I, PROVIDER_ID TYPE C LENGTH 8,
  BATCH_ID TYPE CHAR32, OBJECT_COUNT TYPE I

OBJECT_TYPE=TABL (structure)
NAME=ZAOG_SER_DD_BENTRY
PACKAGE=same as above
DESCRIPTION=ORTEC serialization DOMA/DTEL batch prefetch entry (one object)
FIELDS=OBJ_TYPE TYPE TROBJTYPE, OBJ_NAME TYPE SOBJ_NAME,
  PRESENT TYPE ABAP_BOOL

OBJECT_TYPE=TTYP (table type)
NAME=ZAOG_SER_DD_BENTRY_TT
PACKAGE=same as above
DESCRIPTION=ORTEC serialization DOMA/DTEL batch prefetch entry table
ROW_TYPE=STANDARD TABLE OF ZAOG_SER_DD_BENTRY

ACTIVATION_ORDER=ZAOG_SER_DD_BENTRY -> ZAOG_SER_DD_BENTRY_TT ->
  ZAOG_SER_DD_BHDR (no cross-dependency, order is for clarity only) ->
  then activate ZCL_ABAPGIT_ORTEC_SER_PREF_EXT (main + testclasses),
  ZCL_ABAPGIT_OBJECT_DOMA, ZCL_ABAPGIT_ORTEC_SER_ORCH (main + testclasses),
  Z_ABAPGIT_ORTEC_SER_BATCH (function group include).
```

XML source for these 3 DDIC objects already exists in
`src/ortec/serial/core/` (`zaog_ser_dd_bhdr.tabl.xml`,
`zaog_ser_dd_bentry.tabl.xml`, `zaog_ser_dd_bentry_tt.ttyp.xml`) - the owner
can import them via abapGit directly rather than hand-creating empty
objects first, IF this repo's own abapGit-on-abapGit import path is used
for this branch. If a manual empty-object pre-creation step is preferred
instead (matching this project's established "owner creates empty active
objects" convention for other slices), the fields above are the exact spec.
