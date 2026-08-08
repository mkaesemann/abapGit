# SER-SLICE-4 — Shared infrastructure decision

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_4_SHARED_INFRASTRUCTURE
BASELINE_HEAD=e5e10d62137033801739b8a53c69bdff9cfa650e
STATUS=FINALIZED_FOR_ADVERSARIAL_REVIEW
```

## 1. Generic envelope: REUSE, do not reinvent

`ZAOG_SER_ENV_BHDR` / `ZAOG_SER_ENV_BENTRY` / `ZAOG_SER_ENV_BENTRY_TT`
(CONFIRMED_SOURCE, introduced for CLAS/INTF Phase 4, reused unchanged by
MSAG Phase 6 - `.memory/logs/serialization_slice_3_clas_intf.md`,
`.memory/logs/serialization_slice_3_msag.md`) already provide exactly the
typed, versioned, generic header/entry shape every Common Provider
Constraint in this slice's mission requires:

```text
ZAOG_SER_ENV_BHDR:   wire_format_version (I) / provider_id (CHAR8) /
                      batch_id (CHAR32) / object_count (I)
ZAOG_SER_ENV_BENTRY:  obj_type (TROBJTYPE) / obj_name (SOBJ_NAME) /
                      state (CHAR1: P/M/F) / actual_bytes (I)
```

All three new providers in this slice (Package A TABL, Package B PROG,
Package C FUGR) reuse this SAME envelope, each with its OWN
`provider_id` (`'SER_TABL'`, `'SER_PROG'`, `'SER_FUGR'`) and OWN small,
narrowly-typed payload table(s) exported alongside `hdr`/`entries` in the
SAME `EXPORT ... TO DATA BUFFER` call - **no new generic header/entry
DDIC objects are created by this slice.** The older, DOMA/DTEL-specific
`ZAOG_SER_DD_BHDR`/`ZAOG_SER_DD_BENTRY`/`ZAOG_SER_DD_BENTRY_TT` (pre-dates
`ZAOG_SER_ENV_*`) is left exactly as-is - migrating DOMA/DTEL onto the
generic envelope retroactively is explicitly OUT OF SCOPE for this slice
(it is IT8-validated, working code; touching it here would violate the
"no productive-code changes without owner authorization" boundary and
gains nothing beyond cosmetic consistency).

## 2. New typed payload structures (one set per family, never merged)

```text
Package A (TABL):  ZAOG_SER_TABL_TX_BROW / _TT (i18n text)
                    ZAOG_SER_TABL_EX_BROW / _TT (TDDAT extras)
Package B (PROG):  ZAOG_SER_PROG_BROW / _TT (tpool i18n)
Package C (FUGR):  ZAOG_SER_FUGR_AT_BROW / _TT (area text)
                    ZAOG_SER_FUGR_ED_BROW / _TT (ENLFDIR directory)
                    ZAOG_SER_FUGR_FN_BROW / _TT (function metadata +
                      RFCSCOPE/RFCVERS)
```

Each structure mirrors an EXISTING, already-typed PRIVATE cache row
one-to-one (no new field invented beyond &sect;6 of the FUGR design's
RFCSCOPE/RFCVERS/rfc_fields_valid addition, which itself mirrors an
existing DB table's own fields). This satisfies "do not design a
universal untyped mega-envelope": every payload table is independently
typed and independently optional in the combined EXPORT, exactly like
`hdr`/`doma`/`dtel` already coexist independently in the DD envelope
today.

## 3. Provider-buffer summation before dispatch (MANDATORY correction to
today's code, applies to ALL of Packages A/B/C together)

**Current gap (CONFIRMED_SOURCE)**: `before_dispatch`'s actual-byte
admission check currently sums ONLY `xstrlen( lv_prefetch_buffer_dd )`
into `lv_actual_bytes` (`serialization_slice_3_provider_contract.md`
&sect;4) - the CLAS/INTF (`_oo_batch`) and MSAG (`_msag`) buffers were
added AFTER that check was written and are explicitly disclosed as "NOT
yet folded into the c_max_actual_batch_bytes admission check... a
disclosed limitation" (`serialization_slice_3_clas_intf.md`).

**This slice's requirement**: `before_dispatch` MUST compute
`lv_actual_bytes` as the SUM of every provider buffer's `xstrlen(...)` -
`lv_prefetch_buffer_dd` + `lv_prefetch_buffer_oo_batch` +
`lv_prefetch_buffer_msag` + (this slice's new) `lv_prefetch_buffer_tabl`
+ `lv_prefetch_buffer_prog` + `lv_prefetch_buffer_fugr` - BEFORE the
existing `c_max_actual_batch_bytes` split-and-recurse check. Rationale:
the mission's Common Provider Constraints explicitly require "bounded
rows/bytes" and "actual-byte admission" as properties of EVERY provider,
collectively, not per-provider in isolation - a batch could individually
keep each of six provider buffers under the limit while their COMBINED
size (all sent to the SAME RFC worker call) exceeds it, which the
CURRENT DD-only sum would never detect. This is flagged here as a
MANDATORY correction bundled into this slice's implementation scope
(Packages A/B/C's own ORCH-wiring sections each reference this
requirement rather than re-deriving it), not a new, separately-optional
feature - it is required for the "no partial success"/"bounded rows/
bytes" invariant to actually hold once six provider buffers exist
side-by-side instead of one.

**Not required by this correction**: retroactively fixing the OLDER
disclosed gap for CLAS/INTF/MSAG's own buffers not being summed is
already covered by the same code change (the fix sums ALL SIX, including
the pre-existing three) - no separate slice needed for that half.

## 4. Provider dispatch/telemetry pattern (unchanged, reused as-is)

Every new provider follows the SAME three-part pattern already
established by DD/OO_BATCH/MSAG, with NO new pattern invented:

```text
1. before_dispatch: extract_for_batch_<x>( it_object_keys ) once,
   reused across split/dispatch (never recomputed per split half except
   as an unavoidable side effect of the two halves needing independently-
   sized buffers, exactly as documented for DD in provider_contract.md
   &sect;4).
2. dispatch_batch -> RFC CALL FUNCTION: new iv_prefetch_buffer_<x>
   OPTIONAL xstring parameter, threaded through unchanged.
3. RFC worker (z_abapgit_ortec_ser_batch): unconditional clear_<x>_cache()
   FIRST on every invocation, then conditional
   inject_batch_from_buffer_<x>(...) wrapped in a swallowed
   TRY/CATCH zcx_abapgit_exception (never propagated - a corrupt/unknown-
   version buffer for provider X becomes a full MISS for provider X
   only, the batch itself always completes). CASE ls_tadir-object gains
   one WHEN branch per new object type, reporting provider_hit/
   provider_miss via the SAME two existing ZAOG_SER_BATCH_RESULT fields
   every other provider already uses - no new telemetry field needed.
```

## 5. Class ownership (no new provider classes)

```text
ZCL_ABAPGIT_ORTEC_SER_PREF_EXT gains: TABL/TTYP (Package A), PROG
  (Package B), FUGR (Package C) - alongside its existing DOMA/DTEL/ENHS/
  SMIM/TOBJ/TRAN/enhs single-object caches. This is now a fairly wide
  class; SPLITTING it into per-family classes is explicitly considered
  and REJECTED for this slice - every existing provider class in this
  codebase (ser_pref/ser_pref_ext/ser_pref_oo) already groups MULTIPLE
  unrelated object families for pragmatic reasons (shared PREPARE/CLEAR
  entry points, shared collect_keys), and splitting now would be a
  refactor unrelated to this slice's actual goal, with real regression
  risk to IT8-validated DOMA/DTEL/CLAS/INTF/MSAG code paths for zero
  functional benefit - REJECTED per the mission's own "do not modify
  productive code" boundary applying to code this slice does not
  otherwise need to touch.
ZCL_ABAPGIT_ORTEC_SER_ORCH gains three new xstring OPTIONAL parameters on
  DISPATCH_BATCH (iv_prefetch_buffer_tabl/_prog/_fugr) and three new
  extract_for_batch_<x> call sites in BEFORE_DISPATCH, plus the &sect;3
  summation correction - additive only, no existing parameter renamed or
  removed.
CG-003 CORRECTION (correctness gate finding, confirmed):
  ZCL_ABAPGIT_ORTEC_SER_PREF_EXT's shared PREPARE/CLEAR/collect_keys
  entry points are extended by Package A (TABL) ONLY this slice - Package
  B (PROG) and Package C (FUGR) add NO new lines to PREPARE/CLEAR/
  collect_keys, since mt_prog_langs/mt_fugr_areat/mt_fugr_enlfdir/
  mt_fugr_func_meta are PRE-EXISTING caches already wired into all three
  shared entry points from prior slices (SER-SLICE-3). Packages B/C only
  add NEW batch-envelope methods (extract_for_batch_*/inject_batch_from_
  buffer_*/clear_*_cache) layered on top of already-populated caches -
  this does not change &sect;4's composition safety analysis (which never
  depended on B/C touching these entry points), it only corrects the
  earlier, less precise description of "each package adds its own
  wiring".
```

## 6. Byte accounting: what is NOT shared

Each package's `extract_for_batch_<x>` independently measures its own
per-object `actual_bytes` (via the existing `extract_for_object`-reuse
trick already proven for CLAS/INTF/MSAG, per-package designs &sect;3) -
there is no single shared "byte accountant" object across providers,
because each provider's cache/lookup shape is different enough that a
shared accounting abstraction would need to be generic over arbitrary
row shapes, which is exactly the "untyped mega-envelope" anti-pattern the
mission warns against. The ONLY shared accounting step is &sect;3's
SUMMATION of the six already-independently-measured totals - a single
line of arithmetic in `before_dispatch`, not a class or interface.

## 7. Large-payload / documentation / replacement-serializer non-goals
(binding across all three packages)

```text
Longtexts (zcl_abapgit_longtexts.clas.abap): NO prefetch hook exists,
  NONE added by this slice, for any of TABL/TTYP/PROG/FUGR - a uniform,
  explicit non-goal recorded once here rather than re-justified per
  package.
Function-module/program SOURCE and FUGR includes: kernel-level generated-
  source reads with no DB-bulk equivalent - explicitly excluded from
  every package's batch envelope (Package A's DD03P/DD43V exclusion,
  Package B's RPY_PROGRAM_READ exclusion, Package C's Option-C source/
  include exclusion) - none of these are "replacement serializers"
  needing adversarial approval, because none of them are attempted at
  all this slice.
Replacement-serializer approval: NOT triggered by anything in this
  slice - every provider in Packages A/B/C is a pure ORTEC-only lookup
  seam feeding UNCHANGED existing serializer code, exactly the pattern
  the mission prefers ("feeding data to unchanged serializers via
  ORTEC-only lookup seams... a replacement serializer requires explicit
  adversarial approval").
```

## 8. Reusable typed sections: verdict

```text
DDIC rows:          YES, reused (ZAOG_SER_ENV_BHDR/BENTRY, &sect;1) -
                     no new generic header needed.
Language texts:      NO new shared type - each package's i18n-shaped
                     payload (TABL text, PROG tpool, none for FUGR this
                     slice) is small and family-specific enough that a
                     shared "generic i18n row" type would need a
                     variant/union shape, reintroducing the untyped-
                     mega-envelope risk for zero real gain (three
                     providers, three small distinct row shapes, three
                     lines of EXPORT/IMPORT each - not a maintenance
                     burden that justifies an abstraction).
Documentation:       NO shared type - out of scope entirely (&sect;7).
Large-payload
  references:        NOT NEEDED this slice - no provider in Packages
                     A/B/C carries a payload large enough to need
                     reference/chunking semantics (FUGR's ENLFDIR/func
                     metadata is the largest, still bounded per FUGR
                     design &sect;8/&sect;10 by the existing
                     c_max_actual_batch_bytes split-and-recurse
                     mechanism, unchanged).
Provider composition: YES, reused (&sect;4's three-part pattern, already
                     proven for three prior providers, now extended to
                     six without modification).
Byte accounting:     PARTIALLY shared (&sect;3's summation correction
                     only) - no shared class/interface, see &sect;6.
```
