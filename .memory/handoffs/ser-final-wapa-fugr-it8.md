# SER-FINAL — WAPA/FUGR IT8 handoff (2026-08-10, updated by SER-FINAL apply-IT8-results pass)

## Scope of this pass

SER-FINAL (1st pass): evidence review only, no code changed.
SER-FINAL-CONTINUOUS (2nd pass): one productive FUGR change (the
`iv_extra IS NOT INITIAL` guard around `functions()`).
SER-FINAL-CORRECTION (3rd pass): FUGR regression tests + `BINARY SEARCH`
fix; WAPA candidates re-derived with exact source proofs (1, 4 closed) or
a precise IT8 experiment plan (2, 3, 5).
**SER-FINAL apply-IT8-results (this update, 4th pass)**: the owner
executed the IT8 experiment and returned authoritative, deterministic
results. This pass applied them:

```text
WAPA_EXP_1_MASS_READ_API=FAIL          -> Candidate 3 CLOSED (REJECT_WITH_LIVE_IT8_PROOF)
WAPA_EXP_2_LAYOUT_BULK_READABLE=PASS   -> layout approved for Candidate 2
WAPA_EXP_3_DECODE_PARITY=PASS          -> Candidate 2 IMPLEMENT (169/169 exact matches)
WAPA_EXP_4_MULTI_BATCH_BENEFIT=FAIL    -> Candidate 5 CLOSED (REJECT_WITH_LIVE_IT8_PROOF)
```

**Productive ABAP changes this pass** (all locally committed, no push):

- `src/ortec/serial/zcl_abapgit_ortec_wapa.clas.abap`: implemented
  Candidate 2 - a bounded, whole-WAPA, all-or-nothing raw `O2PAGCON`
  prefetch (`try_raw_prefetch`/`build_requested_keys`/`read_raw_rows`/
  `assemble_and_decode` + 2 observability counters), wired into
  `serialize()`/`add_page_content_file`/`add_full_page_details` with the
  original per-key `IMPORT ... FROM DATABASE` preserved verbatim as the
  fallback. See `.memory/logs/ser_final_wapa_raw_prefetch_design.md` for
  the full 15-point implementation-ready design.
- `src/ortec/serial/zcl_abapgit_ortec_wapa.clas.testclasses.abap`: 21 new
  ABAP Unit test methods (see "WAPA validation" below), all using test
  doubles/hand-built data - none depend on real, repository-specific O2
  data.

Carried over, unchanged from the 3rd pass: the FUGR `CHANGED_BY` fix +
tests + `BINARY SEARCH` fix (see prior sections below).

## Activation / test scope for IT8

### WAPA

1. Activate `ZCL_ABAPGIT_ORTEC_WAPA` (single class + its testclasses
   include, `get_errors` clean locally).
2. Run ABAP Unit for `ZCL_ABAPGIT_ORTEC_WAPA` - all 24 methods (3
   pre-existing `EXISTS()` tests + 21 new raw-prefetch tests) should PASS
   with no dependency on real O2 data (`O2APPL`/`O2PAGCON` test doubles
   + hand-built `ty_context`/`ty_raw_row` tables only).
3. Run existing ATC scope as usual.
4. **Manual IT8 parity check** using the IT8 experiment's own proven
   fixtures (no new fixtures needed - Experiment 3 already established
   these as representative):
   - `/O4H/COMPANION_CUS` (12 pages, small)
   - `/O4H/TPL_GEO` (57 pages, medium)
   - `/O4H/TPL_LIB_MAP` (865 pages, large, multi-row-spanning content)
   - `/O4H/TPL_RTM` (35 pages)
   - `/O4H/TPL_RSM` (38 pages)
   - `AXT_UI_COMP` (has event handlers - exercises the EVHNDL path)

   For each: serialize once with the fix active, compare the resulting
   file set (names, order, byte lengths, SHA-1 hashes) against a
   known-good baseline (the IT8 experiment's own Experiment 3 comparison
   already did this once - this step reconfirms it through the real
   `ZCL_ABAPGIT_OBJECT_WAPA~SERIALIZE`/`is_wapa_active()` entry point,
   which the experiment's own ad-hoc report/debugger approach did not
   necessarily exercise end-to-end).
5. **Fallback/error injection** (optional, higher confidence): temporarily
   corrupt or delete one physical `O2PAGCON` row for a test WAPA (in a
   throwaway/dev object, never a real repository's WAPA) and confirm
   serialization still succeeds via the automatic fallback, with
   `get_raw_prefetch_counters` showing an incremented fallback count.
6. **O2PAGCON call-count/timing check**: capture a focused SAT/ST12 trace
   before/after for one of the larger fixtures (`/O4H/TPL_LIB_MAP` or
   `/O4H/TPL_RTM`) and record: number of `O2PAGCON` `SELECT`/`IMPORT`
   statements, total rows, total bytes, elapsed time, and
   `get_raw_prefetch_counters` hit/fallback counts. No new DDLS
   measurement or individual provider OFF/ON benchmark is required (per
   owner waiver).
7. **Confirm WAPA remains singleton**: no change was made to
   `zcl_abapgit_ortec_ser_orch*` (confirmed via `git diff --name-only`
   in this pass, see `ser_final_wapa_adversarial.md`) - a live check
   that a multi-WAPA batch is still never dispatched is a re-confirmation
   of existing, unchanged behaviour, not a new test.

### FUGR (unchanged from the 3rd pass, re-listed for completeness)

1. Activate `ZCL_ABAPGIT_OBJECT_FUGR` (single class + its testclasses
   include, `get_errors` clean locally for all three changed/added
   files).
2. Run ABAP Unit for `ZCL_ABAPGIT_OBJECT_FUGR` specifically - the
   `ltcl_changed_by` test class (7 methods, `RISK LEVEL HARMLESS`,
   `DURATION SHORT`) should PASS with no database/customizing
   dependencies.
3. Run existing ATC scope as usual.
4. **Manual IT8 spot check**: open the repository content/overview list
   (or the diff view) for a repository containing at least one FUGR with
   (a) an `iv_extra`-empty whole-object lookup path (content list) and
   (b) a per-file/per-include lookup (diff view for a specific function
   module's file). Confirm the reported "changed by" user/date is
   **identical before and after** this change in both cases.
5. Optional focused SAT re-trace: re-run the repository content-list/
   overview render for a repo with multiple changed FUGR objects and
   confirm `RS_FUNCTION_POOL_CONTENTS`/direct `ENLFDIR` hit counts drop
   for the `CHANGED_BY`-only call shape (no change expected for the
   diff-view/per-file call shape).

No new multi-WAPA experiment is required - Candidate 5 is closed. No new
DDLS measurement is required - DDLS remains deferred.

## What this pass established (owner-facing)

1. **SER-SLICE-5's open `OWNER_ACTION_REQUIRED` remains satisfied**
   (unchanged from the 1st pass).
2. **WAPA Candidate 2 (raw O2PAGCON prefetch)**: IMPLEMENTED, adversarially
   reviewed (0 BLOCKER/0 MAJOR), correctness-proven, performance-audited,
   and covered by 21 new local ABAP Unit tests. WAPA remains a singleton
   (no planner change).
3. **WAPA Candidates 3 and 5**: CLOSED per live IT8 proof
   (`REJECT_WITH_LIVE_IT8_PROOF`) - not re-designed or re-measured.
4. **FUGR serializer/provider and CHANGED_BY**: unchanged from the 3rd
   pass (see below).
5. **DDLS**: unchanged, `DEFER_NO_MATERIAL_SAFE_CHANGE`.

## Owner action required

1. Run the WAPA + FUGR ABAP Unit suites on IT8 and confirm PASS (both
   expected to pass with zero real-data dependency).
2. Execute the WAPA manual parity check (activation section, step 4)
   using the already-proven IT8 fixtures.
3. Optional: the fallback/error-injection and SAT call-count checks
   (steps 5-6) for higher confidence before wider rollout.
4. The FUGR manual spot check (routine verification, not a new open
   design question - unchanged from the 3rd pass).

## Historical (3rd-pass) owner action items — now RESOLVED

The 3rd pass's open item ("schedule and execute the WAPA IT8 experiment
in `.memory/logs/ser_final_wapa_it8_experiment.md` to close candidates
2/3/5") is now CLOSED - the owner executed it and this pass applied the
results (see above). That experiment log remains as historical evidence
of the exact methodology used; it is not an open action item anymore.

## Focused SAT comparison (before/after call counts, this pass's own findings)

| Metric | Before (legacy/standard path, `*Normal*` traces) | After (ORTEC batch path, `*Batch*` traces) |
|---|---|---|
| WAPA per-page API round trips (`CL_O2_API_PAGES=>LOAD`+`GET_ATTRS`+`GET_EVENT_HANDLERS`+`GET_PARAMETERS`+`GET_TYPE_SOURCE`) | ~5 calls × page count (159/61 in supplied traces) | 0 (replaced by 5 bulk `SELECT`s total per app) — **not newly measured this pass**, carried over from prior SER-SLICE-5 evidence; no new WAPA batch trace supplied |
| FUGR `ENLFDIR` direct SELECT (SERIALIZE path) | 1 per function group (no provider in the standard path) | 2 direct fallbacks observed against ~19-40 objects per worker (provider HIT for the rest) |
| FUGR `RS_GET_ALL_INCLUDES` (`CHANGED_BY` path) | 1 per FUGR object (same as today) | 1 per FUGR object (**unchanged** - `RS_GET_ALL_INCLUDES` itself has no safe bulk substitute; this is expected, not a regression) |
| FUGR `RS_FUNCTION_POOL_CONTENTS` + `ENLFDIR` fallback (`CHANGED_BY`, whole-object/`iv_extra`-empty shape only) | 1 each per `CHANGED_BY` call, unconditionally | **0** for the whole-object call shape (content-list/overview render) — provably eliminated; **unchanged (1 each)** for the per-file/diff-view call shape |
| FUGR `functions()` ENLFDIR-existence check complexity | O(F·E) linear scan per call | O(F·log E) via `BINARY SEARCH` (same F, E; fewer comparisons per call) |
| WAPA `O2PAGCON` access per page (PAGE/EVHNDL/TYPES) | 1-3 `IMPORT ... FROM DATABASE` statements per page (up to 865 for `/O4H/TPL_LIB_MAP`) | 1 bulk `SELECT` per WAPA (IT8 Experiment 2/3 evidence: 169 real keys across 6 fixtures, up to 1126 physical rows for one key, reconstructed/decoded in one bulk pass) — **not independently re-measured through the live `serialize()` entry point this pass**; the IT8 handoff's step 6 (O2PAGCON call-count/timing check) requests exactly this confirmation |
