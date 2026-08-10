# SER-FINAL — WAPA/FUGR IT8 handoff (2026-08-10, updated by SER-FINAL-CORRECTION)

## Scope of this pass

SER-FINAL (first pass): evidence review only, no code changed.
SER-FINAL-CONTINUOUS (second pass): one productive change (the
`iv_extra IS NOT INITIAL` guard around `functions()`).
SER-FINAL-CORRECTION (this update, third pass): owner rejected the prior
pass's closeout as premature and required (a) real regression tests for
the FUGR fix, (b) an implementation-or-exact-rejection-or-IT8-blocker
disposition for every named WAPA candidate instead of a repeated no-change
summary, and (c) continued FUGR optimization analysis. Result:

**Productive ABAP changes this pass** (all in
`src/objects/zcl_abapgit_object_fugr.clas.abap` plus its `.xml`/
`.testclasses.abap`, all locally committed, no push):

1. `needs_function_lookup( iv_extra )` and `most_recent_user( it_stamps )`
   extracted as small, pure, private `CLASS-METHODS` (pure refactor, see
   `ser_final_correctness.md` for the line-by-line equivalence proof) -
   done specifically to enable local ABAP Unit coverage.
2. New `ltcl_changed_by` test class (`LOCAL FRIENDS`, mirroring the exact
   existing precedent in `zcl_abapgit_object_ecatt_super.clas.
   testclasses.abap`) - 7 test methods covering the guard predicate
   (empty/non-empty/namespaced `iv_extra`) and the stamp tie-break logic
   (no stamps, single stamp, latest-date, same-date-latest-time,
   unsorted input).
3. `functions()`'s `ENLFDIR` existence check upgraded to `BINARY SEARCH`
   (the table is unconditionally sorted by the same key immediately
   before the read on every code path) - O(F·E)→O(F·log E), provably
   behavior-preserving.

**No WAPA productive code was changed.** Every WAPA candidate now carries
either an exhaustive source proof (candidates 1 and 4) or a precise,
ready-to-execute IT8 experiment with exact objects/breakpoints/pass-fail
criteria (candidates 2, 3, 5) - see
`.memory/logs/ser_final_wapa_it8_experiment.md`. This is not a repeated
"architectural floor" summary; every candidate's disposition is now
individually justified and none were rejected on "parity not provable"
alone.

## Activation / test scope for IT8

1. Activate `ZCL_ABAPGIT_OBJECT_FUGR` (single class + its testclasses
   include, `get_errors` clean locally for all three changed/added
   files).
2. Run ABAP Unit for `ZCL_ABAPGIT_OBJECT_FUGR` specifically - the new
   `ltcl_changed_by` test class (7 methods, `RISK LEVEL HARMLESS`,
   `DURATION SHORT`) should PASS with no database/customizing
   dependencies (it tests two pure, extracted helper methods directly via
   `LOCAL FRIENDS`, no live function group needed).
3. Run existing ATC scope as usual.
4. **Manual IT8 spot check** (still needed for the parts local unit tests
   cannot reach - see prior pass's rationale, unchanged): open
   the repository content/overview list (or the diff view) for a
   repository containing at least one FUGR with (a) an `iv_extra`-empty
   whole-object lookup path (content list) and (b) a per-file/per-include
   lookup (diff view for a specific function module's file). Confirm the
   reported "changed by" user/date is **identical before and after** this
   change in both cases. Since the fix is provably a no-op for case (a)
   and untouched for case (b), any observed difference would indicate an
   unexpected caller shape not covered by this analysis and should be
   reported back before this change is treated as final.
5. Optional focused SAT re-trace: re-run the repository content-list/
   overview render for a repo with multiple changed FUGR objects and
   confirm `RS_FUNCTION_POOL_CONTENTS`/direct `ENLFDIR` hit counts drop
   for the `CHANGED_BY`-only call shape (no change expected for the diff-
   view/per-file call shape).
6. **WAPA IT8 experiment** (separate from the FUGR checks above, needed
   to close candidates 2/3/5 - not mandatory to close this FUGR-only
   handoff, but should be scheduled): execute
   `.memory/logs/ser_final_wapa_it8_experiment.md` "Required IT8
   experiment" (4 ordered steps, ~75 min total, ADT + debugger + a focused
   SAT trace). Each step has an explicit pass/fail threshold that
   determines whether the corresponding WAPA candidate becomes
   `IMPLEMENT` or a confirmed `REJECT_WITH_SOURCE_PROOF`.

## What this pass established (owner-facing)

1. **SER-SLICE-5's open `OWNER_ACTION_REQUIRED` remains satisfied** (see
   the original SER-FINAL section below - unchanged by this update).
2. **WAPA**: re-derived per-candidate this pass with exact source line
   citations. Candidates "request/worker-local page-content cache" and
   "dedup of repeated READ_PAGE/GET_PAGE_CONTENT calls" are CLOSED
   (`REJECT_WITH_SOURCE_PROOF` - exhaustively proven no duplicate read
   exists anywhere in `ZCL_ABAPGIT_ORTEC_WAPA`, will not change with new
   evidence). Candidates "one-time raw preload with existing decode
   semantics", "supported SAP mass-read API", and "bounded multi-WAPA
   batching" are `BLOCKED_BY_REQUIRED_IT8_EXPERIMENT` - see the new IT8
   experiment plan (step 6 above). No WAPA code changed.
3. **FUGR serializer/provider**: re-reviewed every existing
   `is_serial_prefetch_active()`-guarded seam; no additional dead-work or
   HIT-still-does-direct-work defect found beyond the one now fixed in
   `CHANGED_BY` (tracked separately, see above). No further change.
4. **FUGR CHANGED_BY**: gate passed, implemented, and now regression-
   tested (7 new ABAP Unit methods) plus one additional complexity fix
   (`BINARY SEARCH`). Two further candidates (a small-table scan
   conversion, and a cross-object bulk `CHANGED_BY_BULK` FUGR branch)
   were evaluated and rejected with exact source proof. See
   `.memory/logs/fugr_changed_by_current_source.md`,
   `.memory/logs/fugr_changed_by_design.md`,
   `.memory/reviews/fugr_changed_by_adversarial.md`,
   `.memory/reviews/ser_final_correctness.md`,
   `.memory/reviews/ser_final_regression.md`.
5. **DDLS**: unchanged, `DEFER_NO_MATERIAL_SAFE_CHANGE` (no new
   measurements taken, per owner waiver).

## Owner action required

1. The manual IT8 spot check in step 4 above (routine verification of a
   locally-reviewed, provably-safe change - not a new open design
   question).
2. Run the new `ltcl_changed_by` ABAP Unit test class on IT8 and confirm
   PASS (expected - no DB/customizing dependency, see step 2 above).
3. Schedule and execute the WAPA IT8 experiment
   (`.memory/logs/ser_final_wapa_it8_experiment.md`) to close candidates
   2/3/5 - this is the one remaining genuinely open item from this
   mission and requires live system access this workspace does not have.

## Focused SAT comparison (before/after call counts, this pass's own findings)

| Metric | Before (legacy/standard path, `*Normal*` traces) | After (ORTEC batch path, `*Batch*` traces) |
|---|---|---|
| WAPA per-page API round trips (`CL_O2_API_PAGES=>LOAD`+`GET_ATTRS`+`GET_EVENT_HANDLERS`+`GET_PARAMETERS`+`GET_TYPE_SOURCE`) | ~5 calls × page count (159/61 in supplied traces) | 0 (replaced by 5 bulk `SELECT`s total per app) — **not newly measured this pass**, carried over from prior SER-SLICE-5 evidence; no new WAPA batch trace supplied |
| FUGR `ENLFDIR` direct SELECT (SERIALIZE path) | 1 per function group (no provider in the standard path) | 2 direct fallbacks observed against ~19-40 objects per worker (provider HIT for the rest) |
| FUGR `RS_GET_ALL_INCLUDES` (`CHANGED_BY` path) | 1 per FUGR object (same as today) | 1 per FUGR object (**unchanged** - `RS_GET_ALL_INCLUDES` itself has no safe bulk substitute; this is expected, not a regression) |
| FUGR `RS_FUNCTION_POOL_CONTENTS` + `ENLFDIR` fallback (`CHANGED_BY`, whole-object/`iv_extra`-empty shape only) | 1 each per `CHANGED_BY` call, unconditionally | **0** for the whole-object call shape (content-list/overview render) — provably eliminated; **unchanged (1 each)** for the per-file/diff-view call shape |
| FUGR `functions()` ENLFDIR-existence check complexity | O(F·E) linear scan per call | O(F·log E) via `BINARY SEARCH` (same F, E; fewer comparisons per call) |
