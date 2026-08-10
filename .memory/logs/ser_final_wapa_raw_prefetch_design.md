# SER-FINAL — WAPA Candidate 2 (raw O2PAGCON prefetch) implementation design (2026-08-10)

## Authoritative live IT8 evidence applied

```text
WAPA_EXP_1_MASS_READ_API=FAIL          -> Candidate 3 REJECT_WITH_LIVE_IT8_PROOF (kept closed)
WAPA_EXP_2_LAYOUT_BULK_READABLE=PASS   -> O2PAGCON is TRANSPARENT, RELID/APPLNAME/PAGEKEY/OBJTYPE/
                                           VERSION+SRTF2+CLUSTR+CLUSTD, bulk-readable
WAPA_EXP_3_DECODE_PARITY=PASS          -> 169/169 exact matches, byte+hash parity, full serializer
                                           cross-check for AXT_UI_COMP -> Candidate 2 IMPLEMENT
WAPA_EXP_4_MULTI_BATCH_BENEFIT=FAIL    -> Candidate 5 REJECT_WITH_LIVE_IT8_PROOF (kept closed,
                                           singleton policy KEPT, not re-measured this pass)
```

This design implements Candidate 2 only. Candidates 3 and 5 remain closed
per the owner's deterministic disposition; no code for either was written.

## Target flow (implemented exactly as specified by the mission)

```
one WAPA
-> derive exact PAGE / EVHNDL / TYPES logical keys      (BUILD_REQUESTED_KEYS)
-> execute one bounded bulk SELECT from O2PAGCON        (READ_RAW_ROWS)
-> order by complete logical key plus SRTF2             (ASSEMBLE_AND_DECODE, defensive re-SORT)
-> append exactly CLUSTR bytes from every CLUSTD chunk  (ASSEMBLE_AND_DECODE)
-> reconstruct one complete xstring per logical key     (ASSEMBLE_AND_DECODE, per-group buffer)
-> decode using IMPORT FROM DATA BUFFER                 (ASSEMBLE_AND_DECODE)
-> feed decoded values into the existing serializer flow (ADD_PAGE_CONTENT_FILE/ADD_FULL_PAGE_DETAILS)
-> current per-key IMPORT FROM DATABASE as safe fallback (same two methods, unchanged else-branch)
```

## The 15 required design decisions

1. **Canonical typed logical key**: `(pagekey, objtype)` — `applname`/`version` are constant for
   the duration of one `serialize()` call (one WAPA, always the active version) and are therefore
   carried as method parameters (`iv_name`) or literals (`c_active`), not repeated per key/row.
   `ty_raw_key` (`pagekey` + `need_evhndl`/`need_types` flags) is the *requested*-key shape;
   `ty_raw_row` (`pagekey`, `objtype`, `srtf2`, `clustr`, `clustd`) is the *physical row* shape.
2. **Requested-key generation for PAGE/EVHNDL/TYPES**: `build_requested_keys` — PAGE is requested
   for every non-controller page (mirrors `read_page`'s own `pagetype <> so2_controller` guard);
   EVHNDL and TYPES are requested only for full-type pages (mirrors `add_full_page_details`'s own
   `pagetype = so2_full_page` guard), and EVHNDL additionally only when
   `is_context-event_handlers` already has an active-version row for that page (mirrors that
   method's own pre-check before attempting the EVHNDL import). The requested-key set can never
   diverge from what the reference path would itself look for, because it reuses the exact same
   `is_context` tables the reference path reads.
3. **SQL shape and bounded row/byte batching**: one `SELECT pagekey, objtype, srtf2, clustr,
   clustd FROM o2pagcon ... FOR ALL ENTRIES IN <requested sub-keys> WHERE relid = 'TR' AND
   applname = iv_name AND pagekey = ... AND objtype = ... AND version = 'A' ORDER BY pagekey,
   objtype, srtf2 UP TO c_max_raw_prefetch_rows ROWS`. Row cap: `c_max_raw_prefetch_rows = 20000`
   (derived, not copied from the IT8 experiment's 5000-row probe cap — see the constant's own doc
   comment for the derivation from the largest real observed payload, 1126 rows). Byte cap:
   `zcl_abapgit_ortec_ser_orch=>c_max_object_output_bytes` (20 MB) — reused, not duplicated, from
   the existing, already-reviewed per-object output-size gate in the ORTEC batch orchestrator, so
   the two subsystems share one tuning knob instead of drifting independently over time.
4. **Physical-row grouping and SRTF2 ordering**: `assemble_and_decode` groups by `(pagekey,
   objtype)` using a `SORTED TABLE ... UNIQUE KEY pagekey objtype` buffer map with a per-group
   `next_srtf2` expectation counter, and defensively re-`SORT`s the input rows by `(pagekey,
   objtype, srtf2)` before processing — so correctness never depends on the caller (real SQL
   `ORDER BY`, or a hand-built unit test table) delivering rows in any particular order.
5. **CLUSTR truncation rule**: `lv_chunk = <row>-clustd(<row>-clustd_length_from_clustr).` — exactly
   `CLUSTR` bytes are read from the start of `CLUSTD`; anything beyond `CLUSTR` in `CLUSTD` (padding)
   is never touched. `CLUSTR < 0` or `CLUSTR > xstrlen(CLUSTD)` is a malformed-row anomaly.
6. **Absent optional EVHNDL/TYPES records**: a *requested* EVHNDL/TYPES sub-key with zero physical
   rows is normal (matches `IMPORT`'s own graceful missing-key behaviour) — an empty map entry is
   still inserted so the consumer can distinguish "legitimately empty" from "raw prefetch inactive"
   (see decision 11). A *requested* PAGE key with zero physical rows is a hard anomaly (mirrors the
   reference path's own `RAISE` for missing content) and aborts the whole-WAPA attempt.
7. **Eager vs. lazy decode**: **eager** — `try_raw_prefetch` fully validates and decodes *every*
   requested key for the WAPA in one pass, before any page is processed, so the all-or-nothing
   commit decision is made once, with zero risk of a decode failure surfacing after some pages have
   already consumed candidate data (see invariant W7/attack "decode failure after partial
   reconstruction" in the adversarial review).
8. **Total per-WAPA byte cap**: `zcl_abapgit_ortec_ser_orch=>c_max_object_output_bytes` (20 MB) —
   see decision 3.
9. **Oversized behaviour**: exceeding either cap raises `zcx_abapgit_exception` *internally* inside
   `assemble_and_decode`/is signalled via `ev_row_cap_hit` from `read_raw_rows`; `try_raw_prefetch`
   catches this and returns with `raw_prefetch_active = abap_false` — the WAPA is then served
   entirely by the original per-key `IMPORT` path, with no size limit of its own (matching today's
   unbounded reference behaviour for an oversized WAPA — this design only bounds the *fast path*,
   never blocks correctness).
10. **Sequence-gap/malformed-row/decode-error handling**: each is an explicit, independently raised
    `zcx_abapgit_exception` inside `assemble_and_decode` (see the method's own inline checks) —
    all funnel into the same `TRY_RAW_PREFETCH` `CATCH` and the same whole-WAPA fallback.
11. **Reference fallback semantics**: per-key, inside `ADD_PAGE_CONTENT_FILE`/
    `ADD_FULL_PAGE_DETAILS`: `IF is_context-raw_prefetch_active = abap_true. READ TABLE
    is_context-raw_* ... IF sy-subrc = 0. <use it> ENDIF. ENDIF. IF <not obtained>. <original
    IMPORT, byte-for-byte>. ENDIF.` — the original `IMPORT` statements are **untouched**, just
    conditionally skipped; there is no new "merged" code path to keep in sync with the reference
    behaviour as SAP's O2 APIs evolve.
12. **Memory-copy limits and cleanup**: bounded by the same byte cap (decision 8); the raw physical
    rows (`lt_rows`) and per-group assembly buffers (`lt_buffers`) are local to `try_raw_prefetch`'s
    call stack and go out of scope (freed) once it returns; only the already-bounded decoded maps
    (`raw_content`/`raw_evhandler`/`raw_typesource`) persist in `ty_context` for the lifetime of one
    `serialize()` call, exactly mirroring the existing `page_dirs`/`page_texts`/etc. bulk-loaded
    tables already held there today - no new memory *class* is introduced, just three more bounded
    tables of the same kind already accepted for this object.
13. **Observability counters**: `gv_raw_prefetch_hits`/`gv_raw_prefetch_fallbacks` (session-lifetime,
    in-memory, never persisted/cross-request), exposed via `get_raw_prefetch_counters`/
    `reset_raw_prefetch_counters`.
14. **Feature-OFF behaviour**: unaffected — `ZCL_ABAPGIT_ORTEC_WAPA` is only ever invoked from
    `ZCL_ABAPGIT_OBJECT_WAPA~SERIALIZE` when `is_wapa_active() = abap_true`; no new switch was
    added (deliberately — see "Design choices not made" below), since the whole class is already
    behind that gate and the raw-prefetch attempt's own fallback is unconditionally safe.
15. **Proof that WAPA remains singleton**: `try_raw_prefetch`/`build_requested_keys`/
    `read_raw_rows`/`assemble_and_decode` all operate on exactly the `it_pages`/`is_context` of
    the *single* WAPA passed into `serialize()` - there is no new parameter, table, or global state
    that could span more than one WAPA object, and no change was made to
    `ZCL_ABAPGIT_ORTEC_SER_ORCH`'s admission/planner logic at all (confirmed via `git diff` scope -
    only `zcl_abapgit_ortec_wapa.*` files changed).

## Design choices not made (explicitly out of scope, per the mission)

- **No new feature switch** for the raw-prefetch specifically: the mission asked for "minimal
  directly necessary plumbing only" and the built-in whole-WAPA fallback already makes any future
  issue self-healing at runtime (falls back automatically, never needs a manual kill-switch to
  recover). Adding one would be scope creep beyond what was authorized.
- **No multi-WAPA planner change**: Candidate 5 is closed (`REJECT_WITH_LIVE_IT8_PROOF`); no file
  under `zcl_abapgit_ortec_ser_orch*` was touched.
- **No DDLS/object-store/partial-index changes**: none touched.

## Disclosed assumption requiring IT8 confirmation if this design is ever revisited

`O2PAGCON`'s exact physical field names (`RELID`/`APPLNAME`/`PAGEKEY`/`OBJTYPE`/`VERSION`/`SRTF2`/
`CLUSTR`/`CLUSTD`) are taken verbatim from the IT8 experiment's own reported evidence (this
workspace has no live DDIC access to independently re-verify them). If IT8 activation of
`READ_RAW_ROWS`'s `SELECT` fails on a field-name mismatch, that is the first and only place to
check. `O2PAGDIR-PAGETYPE` (used identically here and in the pre-existing, already-shipped
`READ_PAGE` method) is not a new assumption - it is reused from working code.
