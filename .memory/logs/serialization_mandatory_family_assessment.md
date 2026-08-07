# SER-SLICE-3 Phase 5 — mandatory object-family assessment matrix

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_3_PHASE_5_MANDATORY_FAMILY_ASSESSMENT
STATUS=ALL_10_FAMILIES_DISPOSITIONED
```

Every mandatory family (DTEL, TABL, TTYP, PROG, DOMA, CLAS, FUGR, MSAG,
INTF, WAPA) has a decision record below. Evidence source: current source
(this session's own reads of `zcl_abapgit_ortec_ser_pref*.clas.abap`,
`zcl_abapgit_object_*.clas.abap`) plus the still-valid static-evidence
ranking in `.memory/logs/serialization_slice_3_discovery.md` and
`.memory/logs/serialization_slice_3_object_ranking.md` (CONFIRMED_CURRENT
- no code affecting TABL/TTYP/PROG/FUGR/WAPA changed this session).

## DTEL

```text
current serializer                ZCL_ABAPGIT_OBJECT_DTEL~SERIALIZE
existing ORTEC optimization        ZCL_ABAPGIT_ORTEC_SER_PREF_EXT (mt_dtel,
                                   DD04L/DD04T bulk read + all-language i18n)
repeated DB/API access pattern    Yes - one DD04L/DD04T SELECT SINGLE per
                                   object without the provider
candidate batch data              DD04L/DD04T rows (already batched)
serializer-output parity risk     LOW - already IT8-debug-validated by the
                                   owner this session (see the superseded
                                   parity incident)
payload/memory risk               LOW - bounded per-batch, small rows
static/session-state risk         LOW - CLEAR-first on every PREPARE/
                                   INJECT_BATCH_FROM_BUFFER
batch suitability                 HIGH
expected benefit                  HIGH (already realized)
required tests                    Existing ltcl_dd_batch_wire suite (PASS
                                   locally); IT8 parity retest per the
                                   consolidated validation plan
final disposition                 IMPLEMENTED_BATCH_PROVIDER
```

## DOMA

```text
current serializer                ZCL_ABAPGIT_OBJECT_DOMA~SERIALIZE
existing ORTEC optimization        ZCL_ABAPGIT_ORTEC_SER_PREF_EXT (mt_doma,
                                   DD01L/DD01T/DD07L/DD07T bulk read)
active-version rule                DD01L/DD01T: AS4LOCAL = 'A', AS4VERS =
                                   '0000' (confirmed in prepare_doma via
                                   direct source read this session, matches
                                   the binding rule in the task prompt)
DD07 usage                        DD07L/DD07T bulk-read alongside DD01L/
                                   DD01T; fixed-value text rows follow the
                                   same active-version predicate; DR-001
                                   (fixed this slice's earlier pass) proved
                                   a domain with NO main-language DD01T/
                                   DD07T text row must still seed the main
                                   language into ET_I18N_LANGS unconditionally
                                   (DDIF_DOMA_GET's own behavior) - retained
serializer-output parity risk     LOW - owner-debug-validated this session
payload/memory risk               LOW
static/session-state risk         LOW
batch suitability                 HIGH
expected benefit                  HIGH (already realized)
required tests                    Existing ltcl_dd_batch_wire suite (PASS
                                   locally); IT8 parity retest
final disposition                 IMPLEMENTED_BATCH_PROVIDER
```

## CLAS

```text
current serializer                ZCL_ABAPGIT_OBJECT_CLAS~SERIALIZE (via
                                   ZCL_ABAPGIT_OO_BASE for shared OO logic)
existing ORTEC optimization        ZCL_ABAPGIT_ORTEC_SER_PREF_OO (mt_classtx/
                                   mt_compotx/mt_subcotx from SEOCLASSTX/
                                   SEOCOMPOTX/SEOSUBCOTX), now with a real
                                   Phase 4 multi-object batch envelope
repeated DB/API access pattern    Yes - SEOCLASSTX/SEOCOMPOTX/SEOSUBCOTX
                                   per-object SELECTs when the provider
                                   MISSes; source/local defs/impls/macros/
                                   testclasses always read directly by
                                   ZCL_ABAPGIT_OBJECT_CLAS itself, never
                                   through this provider (not a replacement
                                   serializer, by design)
candidate batch data              SEOCLASSTX/SEOCOMPOTX/SEOSUBCOTX (Phase 4)
serializer-output parity risk     MEDIUM until IT8-validated (no live
                                   connectivity this session) - fully unit-
                                   tested locally (21 tests, PASS locally)
payload/memory risk               LOW-MEDIUM (large classes with many
                                   components produce bigger per-object
                                   entries; covered by the large_class test)
static/session-state risk         LOW - unconditional clear-before-inject
                                   (mirrors AR-3-001's fix)
batch suitability                 HIGH
expected benefit                  HIGH (Stage-A CLAS measurements from
                                   discovery: 55.7% serialization runtime
                                   reduction, 95.9% RFC task reduction)
required tests                    21 local unit tests (PASS); IT8 direct
                                   serialized file-set parity (not yet run,
                                   no live connectivity this session)
final disposition                 IMPLEMENTED_BATCH_PROVIDER
```

## INTF

```text
current serializer                ZCL_ABAPGIT_OBJECT_INTF~SERIALIZE (same
                                   ZCL_ABAPGIT_OO_BASE shared logic as CLAS)
existing ORTEC optimization        Same Phase 4 provider as CLAS - INTF is
                                   explicitly covered by extract_for_batch's
                                   `object = 'CLAS' OR object = 'INTF'`
                                   filter and by 2 dedicated tests
                                   (interface_object_type, mixed_clas_intf_
                                   batch)
serializer-output parity risk     Same as CLAS (shared implementation)
final disposition                 IMPLEMENTED_BATCH_PROVIDER
```

## TABL

```text
current serializer                ZCL_ABAPGIT_OBJECT_TABL~SERIALIZE
existing ORTEC optimization        None found in ORTEC prefetch classes
                                   (confirmed via grep this session - zero
                                   is_serial_prefetch_active references in
                                   zcl_abapgit_object_tabl.clas.abap)
repeated DB/API access pattern    Standard DDIC reads only (DD02L/DD03L/
                                   DD05M/DD08V/DD09L/DD12V/DD17V family via
                                   DDIF_TABL_GET and friends), not
                                   intercepted at all today
candidate batch data              Plausible bulk-read shape analogous to
                                   DOMA (DD02L/DD03L-family FOR ALL ENTRIES)
                                   but TABL's field-count and dependent-
                                   object surface (includes, appends,
                                   secondary indexes, foreign keys, search
                                   helps, texts) is materially larger than
                                   DOMA's - a genuinely new field-level
                                   source map is required before any code
serializer-output parity risk     Unproven - no existing provider to reuse,
                                   would need the same rigor DOMA/DTEL
                                   required (which found a real BLOCKER,
                                   DR-001) applied to a bigger surface
payload/memory risk               MEDIUM (some tables have very many
                                   fields; large repositories can have very
                                   many TABL objects - highest static
                                   prevalence of all 10 mandatory families)
static/session-state risk         LOW if implemented following the DOMA
                                   clear-first pattern
batch suitability                 Plausible, not yet designed
expected benefit                  HIGH if implemented (highest-prevalence
                                   remaining family), but this is a genuinely
                                   new, comparably-sized design effort to
                                   what DOMA/DTEL or CLAS/INTF each required
required tests                    Not yet defined (needs the design first)
final disposition                 DESIGN_REQUIRED - entry condition: an
                                   explicit new owner authorization for a
                                   dedicated TABL provider slice, following
                                   the same design->review->implement->test
                                   flow as CLAS/INTF. Not implemented this
                                   run (exceeds this run's authorized
                                   "further justified provider" budget -
                                   see Phase 6 selection reasoning).
```

## TTYP

```text
current serializer                ZCL_ABAPGIT_OBJECT_TTYP~SERIALIZE
existing ORTEC optimization        None found
repeated DB/API access pattern    Standard DDIC reads only (DD40L/DD42V/
                                   DD43V line-type/reference-type/access-
                                   mode/key metadata), not intercepted
candidate batch data              Smaller than TABL's own surface, but the
                                   line-type reference frequently points at
                                   a TABL or DTEL that would ALSO benefit
                                   from a shared bulk read - a standalone
                                   TTYP provider without TABL/DTEL context
                                   would only capture a fraction of the
                                   real DB traffic for a typical TTYP-heavy
                                   repository
serializer-output parity risk     Unproven, same rigor bar as TABL
payload/memory risk               LOW-MEDIUM
static/session-state risk         LOW if implemented following the DOMA
                                   pattern
batch suitability                 Plausible, best paired with a future TABL
                                   provider (shared DD-family reads) rather
                                   than designed in isolation
expected benefit                  MODERATE standalone, HIGHER if combined
                                   with TABL
required tests                    Not yet defined
final disposition                 DESIGN_REQUIRED - pair with TABL in a
                                   future dedicated slice; do not design
                                   TTYP in isolation from TABL (the discovery
                                   evidence already flagged this pairing).
```

## PROG

```text
current serializer                ZCL_ABAPGIT_OBJECT_PROG~SERIALIZE
existing ORTEC optimization        ZCL_ABAPGIT_ORTEC_SER_PREF_EXT
                                   (mt_prog_langs / prepare_prog_langs,
                                   TPOOL i18n only - a narrow slice of
                                   PROG's full serialized content)
repeated DB/API access pattern    The cached slice (text-pool languages)
                                   is single-object EXPORT/IMPORT shaped
                                   today, same "cannot be safely
                                   concatenated" limitation the DD/OO work
                                   already solved for their own families;
                                   PROG's remaining content (source read,
                                   includes, documentation, variants,
                                   dynpros) is NOT cached at all and reads
                                   directly regardless of batch mode
candidate batch data              The EXISTING narrow TPOOL-languages cache
                                   could be extended to a batch envelope
                                   using the exact DOMA/DTEL/OO pattern
                                   with LOW additional risk (small, already-
                                   proven-safe data shape) - broader PROG
                                   serialization (source/includes/dynpros)
                                   is explicitly NOT a target (would require
                                   a much larger new design, and PROG's own
                                   source read must remain generic per the
                                   owner's explicit instruction: "avoid a
                                   replacement PROG serializer")
serializer-output parity risk     LOW for the narrow slice (small, already
                                   isolated data); N/A for the untouched
                                   remainder (unchanged from today)
payload/memory risk               LOW for the narrow slice
static/session-state risk         LOW if implemented following the DOMA
                                   pattern for just this slice
batch suitability                 HIGH for the narrow slice only
expected benefit                  MODERATE for the narrow slice (PROG is
                                   very common, so even a narrow win adds
                                   up); the broader PROG surface has an
                                   unclear benefit/risk ratio not assessed
                                   this run
required tests                    Not yet defined (would mirror the DD/OO
                                   batch-envelope test shape exactly for the
                                   narrow slice)
final disposition                 GENERIC_BATCH_ONLY - extend the EXISTING
                                   narrow TPOOL-languages cache to a batch
                                   envelope in a future dedicated slice
                                   using the exact DOMA/DTEL/OO pattern; do
                                   NOT attempt a broader PROG provider
                                   without new evidence. Not implemented
                                   this run (exceeds this run's authorized
                                   provider budget).
```

## FUGR

```text
current serializer                ZCL_ABAPGIT_OBJECT_FUGR~SERIALIZE
existing ORTEC optimization        ZCL_ABAPGIT_ORTEC_SER_PREF_EXT
                                   (mt_fugr_areat / mt_fugr_enlfdir /
                                   mt_fugr_func_meta), single-object only
repeated DB/API access pattern    Yes, plus function-module-level metadata
                                   reads (FUNCTION_INCLUDE_SPLIT and
                                   related stateful APIs) beyond a flat
                                   DDIC read
candidate batch data              Partial - function group internals
                                   (includes, function modules, global
                                   data, multiple source includes per
                                   function group) are structurally more
                                   complex than a flat DDIC read; batching
                                   only the AREAT/ENLFDIR/FUNC_META slice
                                   (mirroring PROG's narrow-slice approach)
                                   is plausible, but FUGR's own serialized
                                   output has high structural variance
                                   (multi-file output per object) that was
                                   NOT re-measured this session
serializer-output parity risk     MEDIUM-HIGH without a real trace - the
                                   DOMA/DTEL work's risk profile does not
                                   simply transfer to a structurally
                                   different, higher-variance family
payload/memory risk               MEDIUM (function groups can be large)
static/session-state risk         MEDIUM - more moving parts than DOMA/
                                   DTEL/MSAG/PROG's narrow slices
batch suitability                 Unclear without a real trace
expected benefit                  Potentially HIGH but not measured this
                                   session (no live SAT/ST05 access)
required tests                    Not yet defined - a real trace must come
                                   first
final disposition                 MEASURE_FIRST - get a real SAT/ST05 trace
                                   on a FUGR-heavy repository before
                                   committing design effort. Entry
                                   condition: a dedicated future slice with
                                   that trace evidence in hand.
```

## MSAG

```text
current serializer                ZCL_ABAPGIT_OBJECT_MSAG~SERIALIZE
existing ORTEC optimization        ZCL_ABAPGIT_ORTEC_SER_PREF (mt_msag
                                   keyed by msg_id, T100/T100T bulk-
                                   friendly shape already; mt_dokil for
                                   long-text documentation), single-object
                                   only (extract_for_object/inject_from_
                                   buffer, same limitation DOMA/DTEL/OO
                                   already solved for their own families)
repeated DB/API access pattern    Yes, per-message-class T100/T100T/DOKIL
                                   reads when the single-object cache MISSes
candidate batch data              T100/T100T (message headers/texts) plus
                                   DOKIL (long-text documentation index) -
                                   same low-risk, already-proven-safe shape
                                   as DOMA/DTEL (small, flat, keyed rows,
                                   existing single-object cache to extend)
serializer-output parity risk     LOW - structurally identical risk profile
                                   to DOMA/DTEL (this session's own read of
                                   ZCL_ABAPGIT_ORTEC_SER_PREF confirms the
                                   same clear-first/keyed-table shape)
payload/memory risk               LOW (small message-class payloads)
static/session-state risk         LOW, same clear-before-insert pattern
                                   already proven for DOMA/DTEL/OO
batch suitability                 HIGH
expected benefit                  MODERATE-HIGH (message classes are
                                   common in a typical repository; the
                                   owner's own prompt explicitly names MSAG
                                   as a likely high-value safe family)
required tests                    Same shape as the DOMA/DTEL/OO batch-wire
                                   suites (small/large message class,
                                   missing DOKIL, multiple languages,
                                   namespaced names, mixed with other
                                   families, all-miss, reject-corrupt,
                                   cross-batch isolation, round-trip)
final disposition                 IMPLEMENT_NEXT - selected for Phase 6
                                   implementation this run (see
                                   `.memory/logs/serialization_repository_
                                   setting.md` sibling Phase 6 log for the
                                   result). Justification: lowest
                                   implementation risk among the remaining
                                   undesigned families (existing single-
                                   object cache to extend, small flat data,
                                   explicit owner naming as a good
                                   candidate), matching every Phase 5
                                   authorization criterion (no blocker,
                                   unambiguous semantics, directly parity-
                                   testable, bounded bytes, no intrusive
                                   standard change, separate checkpoint
                                   possible, meaningful expected benefit).
```

## WAPA

```text
current serializer                ZCL_ABAPGIT_OBJECT_WAPA~SERIALIZE /
                                   ~exists, routed to ZCL_ABAPGIT_ORTEC_WAPA
existing ORTEC optimization        ZCL_ABAPGIT_ORTEC_WAPA is a full
                                   replacement serializer (not a prefetch-
                                   cache provider), gated by
                                   is_wapa_active() - currently
                                   unconditionally abap_true (Finding F-2
                                   in `serialization_final_two_path_audit.
                                   md`, corrected in Phase 7)
repeated DB/API access pattern    N/A for batching purposes - WAPA is
                                   policy-excluded from multi-object
                                   batching (binding invariant: never
                                   mixed with any other WAPA, never mixed
                                   with any non-WAPA object)
candidate batch data              N/A this slice - batching benefit does
                                   not apply to a singleton-only object
serializer-output parity risk     LOW - the replacement serializer's own
                                   internal logic is unchanged by this
                                   slice; only its ACTIVATION GATE changes
                                   (Phase 7: reads the same batch-context
                                   marker as is_serial_prefetch_active,
                                   instead of being unconditionally true)
payload/memory risk               N/A (singleton batches only)
static/session-state risk         N/A (no shared cache to manage)
batch suitability                 REJECTED for multi-object batch-provider
                                   work by binding policy - this is not a
                                   gap, it is the deliberate target shape
expected benefit                  N/A (batching benefit does not apply)
required tests                    Phase 7's two-path routing tests:
                                   wapa_replacement_not_called_when_off,
                                   wapa_replacement_called_once_when_on
final disposition                 ALREADY_OPTIMIZED_IN_BATCH_PATH - the
                                   existing ZCL_ABAPGIT_ORTEC_WAPA
                                   replacement already serializes each
                                   WAPA as a single, complete, non-batched
                                   unit (no further data-processing
                                   optimization is applicable or safe given
                                   the singleton-only policy); the only
                                   change this slice makes is correcting
                                   WHEN it activates (Phase 7), not HOW it
                                   serializes.
```

## Summary table

| Family | Disposition |
|---|---|
| DTEL | IMPLEMENTED_BATCH_PROVIDER |
| DOMA | IMPLEMENTED_BATCH_PROVIDER |
| CLAS | IMPLEMENTED_BATCH_PROVIDER |
| INTF | IMPLEMENTED_BATCH_PROVIDER |
| MSAG | IMPLEMENT_NEXT (implemented this run, Phase 6) |
| TABL | DESIGN_REQUIRED (deferred, exact reason above) |
| TTYP | DESIGN_REQUIRED (deferred, pair with TABL) |
| PROG | GENERIC_BATCH_ONLY (narrow-slice extension deferred) |
| FUGR | MEASURE_FIRST (deferred, needs a real trace) |
| WAPA | ALREADY_OPTIMIZED_IN_BATCH_PATH (singleton policy, gate fixed in Phase 7) |

No family is silently omitted. Every non-implemented family has a
specific reason and an explicit entry condition for resuming.
