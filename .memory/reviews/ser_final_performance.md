# SER-FINAL — performance gate (2026-08-10)

No productive change was implemented this pass, so there is no
IMPLEMENTATION_AUDIT to run. The DESIGN_GATE question is instead "was a
real, safe, materially-sized performance opportunity missed by choosing
not to implement?" — answered per area:

- **WAPA**: no evidenced opportunity beyond what `ZCL_ABAPGIT_ORTEC_WAPA`
  already implements (bulk directory reads). The remaining `O2PAGCON`
  cluster-import cost is architecturally bounded (see design log).
  `PERFORMANCE_GATE(WAPA)=APPROVE`.
- **FUGR**: one real, evidenced opportunity was found (`CHANGED_BY`'s
  ~1.3s/trace direct-DB cost, uncovered by any provider) and deliberately
  deferred as a named, well-specified follow-up rather than implemented
  under this pass's time/rigor budget, because it requires a design
  decision (bulk include resolution across a differently-scoped sweep)
  this pass could not safely close. `PERFORMANCE_GATE(FUGR)=APPROVE_
  WITH_DOCUMENTED_FOLLOWUP` (the follow-up is not a blocker to closing
  this pass, but should not be silently forgotten — tracked in
  `.memory/state.md` resumable backlog).
- **DDLS**: materiality gate not met (no duplicate-read pattern found).
  `PERFORMANCE_GATE(DDLS)=APPROVE` (defer is correct, not a missed gain).

`PERFORMANCE_GATE=APPROVE` overall, with the FUGR follow-up carried
forward explicitly.

## SER-FINAL-CONTINUOUS update (2026-08-10)

`FUGR_CHANGED_BY` gate re-evaluated per the new mission's explicit
automatic-implementation criteria (THEORETICAL_GAIN/BOUNDED_COMPLEXITY/
CORRECTNESS_MODEL=COMPLETE/NO_CROSS_REQUEST_STATE/EXPECTED_SAVING>
OVERHEAD) - all YES, implemented (see `fugr_changed_by_design.md`). This
supersedes the prior pass's `APPROVE_WITH_DOCUMENTED_FOLLOWUP` for the
specific sub-case that was implementable within a complete correctness
model; the larger cross-object bulk `CHANGED_BY_BULK` FUGR branch
(`FUGR-CHANGED-BY-STATUS-SWEEP`) remains a named, NOT-implemented
follow-up because it would require an incomplete/approximate correctness
model to bulk in its current envisioned shape - correctly not auto-
authorized by the gate. `PERFORMANCE_GATE=APPROVE`.

## SER-FINAL-CORRECTION update (2026-08-10, this pass)

Additional real complexity-class fix implemented: `functions()`'s
`ENLFDIR` existence check upgraded from a linear scan to `BINARY SEARCH`
against a table already unconditionally sorted by the same key
immediately beforehand - O(F·E)→O(F·log E), zero risk, matching this
mission's own named win pattern. The `mt_includes_all` single find-first
scan and the cross-object `CHANGED_BY_BULK` FUGR branch were both
re-evaluated and correctly NOT implemented (small-N/non-repeated lookup
and new-architecture-required, respectively - see
`fugr_changed_by_design.md` for the exact source-backed reasoning).

WAPA: re-derived per-candidate with exact source proofs (candidates 1/4)
and a concrete, ordered, pass/fail-defined IT8 experiment (candidates
2/3/5) in `ser_final_wapa_it8_experiment.md` - no speculative
implementation was made without `EXPECTED_GAIN=YES` evidence, consistent
with this mission's own gate table. `PERFORMANCE_GATE=APPROVE`.

## SER-FINAL apply-IT8-results update (2026-08-10) — WAPA raw prefetch performance audit

Required proofs, verified against the actual implementation:

- **Bounded bulk SQL, not one import per logical key on the hit path**:
  `try_raw_prefetch` issues exactly one `SELECT ... FOR ALL ENTRIES` per
  WAPA (`read_raw_rows`) - PASS.
- **No SQL per physical row**: rows are all returned by the single bulk
  `SELECT`; no further `SELECT`/`IMPORT` is issued while iterating
  `lt_rows` in `assemble_and_decode` - PASS.
- **No O(N·K) correlation**: `lt_buffers` (assembly) and
  `raw_content`/`raw_evhandler`/`raw_typesource` (final maps) are all
  `SORTED TABLE ... UNIQUE KEY` structures, so every group lookup
  (`READ TABLE ... WITH TABLE KEY`) is O(log n), not a linear scan -
  PASS.
- **Suitable HASHED/SORTED key access**: see above; `SORTED` was chosen
  over `HASHED` because the final maps are also iterated in key order in
  a few places (e.g. the completeness-check loop), which `SORTED` serves
  natively without an extra sort - PASS.
- **Bounded raw/reconstructed/decoded buffer copies**: byte cap (20 MB,
  shared with `ZCL_ABAPGIT_ORTEC_SER_ORCH=>C_MAX_OBJECT_OUTPUT_BYTES`)
  and row cap (20000) both enforced inside `assemble_and_decode`/
  `read_raw_rows` before any unbounded accumulation could occur - PASS.
- **Explicit prefetch hit/fallback counters**: `gv_raw_prefetch_hits`/
  `gv_raw_prefetch_fallbacks`, exposed via `get_raw_prefetch_counters`/
  `reset_raw_prefetch_counters` - PASS.
- **No multi-WAPA change**: confirmed via `git diff --name-only` - only
  `zcl_abapgit_ortec_wapa.*` changed - PASS.
- **No regression in other serializer providers**: no file under
  `zcl_abapgit_ortec_ser_pref*`/`zcl_abapgit_ortec_ser_orch*` was touched
  - PASS.

`PERFORMANCE_AUDIT=APPROVE`. Absolute wall-clock improvement was not
independently re-measured in this pass (no live IT8 access this
session) - IT8 Experiment 3's own real-system evidence (successful
reconstruction of up to 1126 rows/~3 MB per key, byte/hash-identical
output) is the load-bearing performance evidence for this design; the
IT8 handoff requests a focused before/after O2PAGCON call-count check to
confirm the SQL-count reduction in production.
