# SER-FINAL — WAPA/FUGR IT8 handoff (2026-08-10, updated by SER-FINAL-CONTINUOUS)

## Scope of this pass

SER-FINAL (first pass): evidence review only, no code changed.
SER-FINAL-CONTINUOUS (this update): owner authorized re-evaluation and
conditional implementation. One productive ABAP change was made and
locally committed (no push):

- `src/objects/zcl_abapgit_object_fugr.clas.abap`,
  `ZIF_ABAPGIT_OBJECT~CHANGED_BY`: skip the `functions()` call (and its
  backing `RS_FUNCTION_POOL_CONTENTS`/`ENLFDIR` work) when `iv_extra` is
  initial - provably a no-op removal for that case, unconditionally
  correct, not gated behind any ORTEC switch (it benefits feature-OFF and
  feature-ON callers identically since output is unchanged in every
  case). WAPA and the rest of the FUGR serializer/provider surface were
  re-reviewed with more scrutiny and reconfirmed as `NO_CHANGE_JUSTIFIED`.

## Activation / test scope for IT8

1. Activate `ZCL_ABAPGIT_OBJECT_FUGR` (single class, syntax-only change,
   `get_errors` clean locally).
2. Run existing ABAP Unit/ATC scope as usual - this class has no
   dedicated `changed_by` unit test in this repository (consistent with
   most `src/objects/*` handlers, which rely on IT8 live verification);
   no new local test artifact was added.
3. **Manual IT8 spot check** (replaces a missing local unit test): open
   the repository content/overview list (or the diff view) for a
   repository containing at least one FUGR with (a) an `iv_extra`-empty
   whole-object lookup path (content list) and (b) a per-file/per-include
   lookup (diff view for a specific function module's file). Confirm the
   reported "changed by" user/date is **identical before and after** this
   change in both cases. Since the fix is provably a no-op for case (a)
   and untouched for case (b), any observed difference would indicate an
   unexpected caller shape not covered by this analysis and should be
   reported back before this change is treated as final.
4. Optional focused SAT re-trace: re-run the repository content-list/
   overview render for a repo with multiple changed FUGR objects and
   confirm `RS_FUNCTION_POOL_CONTENTS`/direct `ENLFDIR` hit counts drop
   for the `CHANGED_BY`-only call shape (no change expected for the diff-
   view/per-file call shape).

## What this pass established (owner-facing)

1. **SER-SLICE-5's open `OWNER_ACTION_REQUIRED` remains satisfied** (see
   the original SER-FINAL section below - unchanged by this update).
2. **WAPA**: re-evaluated with explicit attention to the owner's
   "don't reject solely because of the cluster mechanism" guidance;
   confirmed via a full re-read of `ZCL_ABAPGIT_ORTEC_WAPA=>serialize`
   that no duplicate/redundant read exists to eliminate, and that the one
   theoretically bulkable avenue (direct cluster-table reconstruction)
   remains explicitly out of bounds without provable parity. No change.
3. **FUGR serializer/provider**: re-reviewed every existing
   `is_serial_prefetch_active()`-guarded seam; no additional dead-work or
   HIT-still-does-direct-work defect found beyond the one now fixed in
   `CHANGED_BY` (tracked separately, see above). No further change.
4. **FUGR CHANGED_BY**: gate passed, implemented. See
   `.memory/logs/fugr_changed_by_current_source.md`,
   `.memory/logs/fugr_changed_by_design.md`,
   `.memory/reviews/fugr_changed_by_adversarial.md`.
5. **DDLS**: unchanged, `DEFER_NO_MATERIAL_SAFE_CHANGE` (no new
   measurements taken, per owner waiver).

## Owner action required

None mandatory beyond the manual IT8 spot check in step 3 above (routine
verification of a locally-reviewed, provably-safe change - not a new
open design question).

## Focused SAT comparison (before/after call counts, this pass's own findings)

| Metric | Before (legacy/standard path, `*Normal*` traces) | After (ORTEC batch path, `*Batch*` traces) |
|---|---|---|
| WAPA per-page API round trips (`CL_O2_API_PAGES=>LOAD`+`GET_ATTRS`+`GET_EVENT_HANDLERS`+`GET_PARAMETERS`+`GET_TYPE_SOURCE`) | ~5 calls × page count (159/61 in supplied traces) | 0 (replaced by 5 bulk `SELECT`s total per app) — **not newly measured this pass**, carried over from prior SER-SLICE-5 evidence; no new WAPA batch trace supplied |
| FUGR `ENLFDIR` direct SELECT (SERIALIZE path) | 1 per function group (no provider in the standard path) | 2 direct fallbacks observed against ~19-40 objects per worker (provider HIT for the rest) |
| FUGR `RS_GET_ALL_INCLUDES` (`CHANGED_BY` path) | 1 per FUGR object (same as today) | 1 per FUGR object (**unchanged** - `RS_GET_ALL_INCLUDES` itself has no safe bulk substitute; this is expected, not a regression) |
| FUGR `RS_FUNCTION_POOL_CONTENTS` + `ENLFDIR` fallback (`CHANGED_BY`, whole-object/`iv_extra`-empty shape only) | 1 each per `CHANGED_BY` call, unconditionally | **0** for the whole-object call shape (content-list/overview render) — provably eliminated; **unchanged (1 each)** for the per-file/diff-view call shape |
