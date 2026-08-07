# SER-SLICE-3 Phase 4 — remaining object-family ranking and disposition

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_3_PHASE_4_OBJECT_RANKING
STATUS=RANKING_CONFIRMED_CURRENT_REUSED_FROM_DISCOVERY
SOURCE=.memory/logs/serialization_slice_3_discovery.md (static-evidence
  ranking, CONFIRMED_CURRENT - no code affecting this ranking changed this
  session outside DOMA/DTEL/CLAS's own entries)
```

This phase intentionally reuses existing, still-valid discovery evidence
rather than re-deriving it, per the orchestrator's own rule against
repeating discovery already covered by existing evidence. DOMA/DTEL are
now IMPLEMENTED (Phase 2); CLAS/INTF are DEFERRED with an exact reason
(Phase 3, `serialization_slice_3_clas_intf.md`). The table below extends
the discovery ranking with a disposition for every remaining requested
family.

| Object type | Existing ORTEC prefetch coverage | Repeated DB/API calls | Bulk-read opportunity | Payload/memory risk | Session/static-state risk | Parity-test feasibility | Repository prevalence (static evidence) | Expected benefit | Disposition |
|---|---|---|---|---|---|---|---|---|---|
| MSAG | `ZCL_ABAPGIT_ORTEC_SER_PREF` (T100/T100T/DOKIL), single-object only | Yes, per-message-class T100/DOKIL reads | Yes - same shape as DOMA/DTEL, cache already exists | Low (small message-class payloads) | Low, same clear-before-insert pattern already proven | Feasible - reuse the same byte-identical parity technique proven for DOMA | Present but typically modest per repository | Moderate | DESIGN_REQUIRED (needs its own batch envelope; same low-risk shape as DOMA/DTEL, good next candidate after CLAS/INTF or in parallel) |
| TRAN | `ZCL_ABAPGIT_ORTEC_SER_PREF_EXT` (TSTCT/TSTCP/TSTCA), single-object only | Yes | Yes | Low | Low | Feasible | Present, typically small counts | Moderate | DESIGN_REQUIRED |
| FUGR | `ZCL_ABAPGIT_ORTEC_SER_PREF_EXT` (FUGR_AREAT/FUGR_ENLFDIR), single-object only | Yes, plus function-module-level metadata reads | Partial - function group internals (includes, function modules) are more structurally complex than a flat DDIC read | Medium (function groups can be large) | Medium - more moving parts than DOMA/DTEL/MSAG/TRAN | Harder - FUGR serialization has more state to compare byte-for-byte | Present, often large objects | Moderate-High but higher implementation risk | MEASURE_FIRST (get a real SAT/ST05 trace before committing design effort - the DOMA/DTEL work's risk profile does not simply transfer) |
| PROG | `ZCL_ABAPGIT_ORTEC_SER_PREF_EXT` (PROG_LANGS), single-object only | Yes, but narrow (language versions only) | Yes for the narrow slice already cached; broader PROG serialization has more moving parts (includes, text pools, dynpros) not currently prefetched at all | Medium | Medium | Partial - only the already-cached slice is easily parity-testable | High (PROG is very common) | Moderate for the narrow slice, unclear for the rest | GENERIC_BATCH_ONLY (extend the EXISTING narrow PROG_LANGS cache to a batch envelope using the exact DOMA/DTEL pattern; do NOT attempt a broader PROG provider without new evidence) |
| TABL | None found in ORTEC prefetch classes | Standard DDIC reads only, not currently intercepted at all | Plausible (similar shape to DOMA: DD02L/DD03L-family bulk reads) | Medium (some tables have very many fields) | Low if implemented following the DOMA pattern | Feasible with a dedicated byte-identical harness, not yet built | Very high (TABL is one of the most common object types) | High if implemented, but genuinely new design work | DESIGN_REQUIRED (highest-prevalence remaining candidate; needs its own field-level source map, same rigor DOMA required) |
| TTYP | None found | Standard DDIC reads only | Plausible, smaller than TABL | Low-Medium | Low | Feasible | High | Moderate | DESIGN_REQUIRED (pair with TABL - similar DD-family shape) |
| DDLS | None found | CDS-specific compiler/metadata reads, structurally different from classic DDIC | Unclear - CDS activation-state reads are not a simple bulk-SELECT shape | Unclear | Unclear | Harder - CDS output is more structurally complex to diff | Growing but currently lower than TABL/PROG in this codebase's own evidence | Unclear | DEFER (no static evidence yet; requires its own discovery pass before any design work) |
| DCLS | None found | Same family as DDLS | Unclear | Unclear | Unclear | Harder | Lower prevalence than DDLS | Unclear | DEFER (same reasoning as DDLS) |
| WAPA | `ZCL_ABAPGIT_ORTEC_WAPA` exists but is NOT a batch-scoped prefetch cache | N/A - WAPA is explicitly singleton-batch-only per binding policy | N/A this slice | N/A | N/A | N/A | Present but policy-excluded from multi-object batching | N/A (batching benefit does not apply - singleton only) | REJECT for batch-provider work this slice (binding policy: WAPA is batch-eligible only as a singleton; no provider acceleration investigated this pass, consistent with "analyze provider acceleration if useful, but do not combine multiple WAPAs" - not investigated this run, no new evidence gathered) |
| ENQU | None found | Standard DDIC reads only, not currently intercepted | Plausible, small (lock object headers) | Low | Low | Feasible | Low prevalence | Low | DEFER (low expected benefit given low prevalence; revisit only if a real repository shows high ENQU counts) |
| SHLP | None found | Standard DDIC reads only, not currently intercepted | Plausible | Low | Low | Feasible | Low prevalence | Low | DEFER |
| VIEW | None found | Standard DDIC reads only, not currently intercepted | Plausible but VIEW's own active/inactive semantics need the same care DOMA's DD01V-vs-DD01L distinction required | Low-Medium | Low | Feasible with care | Moderate prevalence | Moderate | DESIGN_REQUIRED (needs its own version-semantics resolution pass, analogous to SER-SLICE-1's DOMA work, before any bulk-read design) |

## Updated ordered ranking (Phase 5, SER-SLICE-3 continuation - supersedes
the SER-SLICE-3-Phase-4 ranking below only in that CLAS/INTF are now
IMPLEMENTED and MSAG is IMPLEMENT_NEXT/implemented in Phase 6)

```text
1. DOMA/DTEL - IMPLEMENTED (owner IT8-debug-validated)
2. CLAS/INTF - IMPLEMENTED this run (Phase 4)
3. MSAG - IMPLEMENTED this run (Phase 6) - see
   .memory/logs/serialization_mandatory_family_assessment.md
4. TABL/TTYP - DESIGN_REQUIRED, highest-prevalence remaining untouched pair
5. TRAN - DESIGN_REQUIRED, same low-risk shape as MSAG, not selected this
   run (MSAG was prioritized per the owner's own explicit naming)
6. PROG (narrow PROG_LANGS slice only) - GENERIC_BATCH_ONLY
7. FUGR - MEASURE_FIRST (real trace needed before design)
8. VIEW - DESIGN_REQUIRED (needs its own version-semantics pass first)
9. WAPA - ALREADY_OPTIMIZED_IN_BATCH_PATH (singleton-only policy; gate
   corrected in Phase 7, not a batch-provider candidate)
10. ENQU/SHLP - DEFER (low prevalence)
11. DDLS/DCLS - DEFER (needs dedicated discovery, structurally different)
```

Full field-by-field decision records for all 10 mandatory families
(DTEL/TABL/TTYP/PROG/DOMA/CLAS/FUGR/MSAG/INTF/WAPA) are in
`.memory/logs/serialization_mandatory_family_assessment.md` - this file
retains the original discovery-time ranking table above as historical
evidence (CONFIRMED_CURRENT for every row not superseded above).

## Original SER-SLICE-3-Phase-4 ranking (superseded only where noted above)

```text
1. DOMA/DTEL - IMPLEMENTED this run (Phase 2)
2. CLAS/INTF - DEFERRED this run, ranked highest remaining (Phase 3 file)
3. TABL/TTYP - DESIGN_REQUIRED, highest-prevalence remaining untouched pair
4. MSAG/TRAN - DESIGN_REQUIRED, same low-risk DOMA/DTEL-like shape
5. PROG (narrow PROG_LANGS slice only) - GENERIC_BATCH_ONLY
6. FUGR - MEASURE_FIRST (real trace needed before design)
7. VIEW - DESIGN_REQUIRED (needs its own version-semantics pass first)
8. WAPA - REJECT for batch-provider work (singleton-only policy)
9. ENQU/SHLP - DEFER (low prevalence)
10. DDLS/DCLS - DEFER (needs dedicated discovery, structurally different)
```

No implementation was authorized or performed for any family in this list
beyond DOMA/DTEL/CLAS/INTF/MSAG across the full SER-SLICE-3 effort.
