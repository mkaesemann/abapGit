# SER-FINAL — FUGR design (2026-08-10)

## Candidates evaluated

- **A. Repair missing consumer seams/provider coverage only** — partially
  already done (SER-SLICE-4 Package C: ENLFDIR, func metadata, AREAT,
  PROG tpool languages). The one concretely-evidenced remaining gap
  (`CHANGED_BY`'s independent `RS_GET_ALL_INCLUDES` + `REPOTEXT`/
  `REPOSRC`/`EUDB`/`D010INC`/`RSEUINC` reads, ~1.3s aggregate DB time for
  147 objects in the supplied trace) is a **different lifecycle scope**
  than the existing dispatch-scoped prefetch (`prepare_fugr` is populated
  per-dispatch-batch `it_areas`; `CHANGED_BY` is invoked across the whole
  repository's status-calc sweep, not just the currently-dispatched
  batch). Extending coverage here requires a **new**, separately-scoped
  prefetch trigger, not a small patch to the existing one — see Candidate
  B below, NOT implemented this pass.
- **B. Worker-local reuse of include/function-directory results** — this
  is the concrete, evidenced, high-value candidate, but implementing it
  safely requires resolving a design question not answerable from source
  alone this pass: `CHANGED_BY`'s `REPOTEXT`/`REPOSRC`/`EUDB` selects
  compute "latest change stamp across the FUGR's main program **and every
  one of its includes**", with an `iv_extra`-driven override (a specific
  include or function module can be requested directly). A safe bulk
  prefetch would need: (1) the full include list per FUGR **before**
  `CHANGED_BY` runs (itself requiring `RS_GET_ALL_INCLUDES`, which has no
  standard bulk/multi-program variant — reimplementing its TRDIR/D010INC
  resolution logic directly is the same class of risk the DDLS gate
  rejects for `CL_DD_DDL_HANDLER`), and (2) exact preservation of the
  existing `ORDER BY ... / SORT ... DESCENDING / take first row` selection
  semantics across 3 different source tables. This is implementable, but
  is a genuine "material architecture decision not resolvable from source
  and evidence alone" per this mission's own stop condition — NOT
  implemented this pass. Concrete design sketch recorded below as a named
  follow-up.
- **C. Bounded include-directory metadata provider** — subsumed by B; no
  independent value without solving the same include-resolution question.
- **D. Worker-local Dynpro result cache where identical screen reads are
  proven** — REJECTED. No evidence this pass (or in SER-SLICE-4's prior
  discovery) shows the *same* Dynpro being read more than once; each
  Dynpro belongs to exactly one program, so there is nothing to cache
  across objects, and Dynpro cost is already the largest single FUGR/PROG
  cost family precisely because it is irreducibly per-screen work.
- **E. Full source/include or direct Dynpro-table provider** — REJECTED
  per the mission's explicit default rejection: no proof of release-
  independent semantics, no proof of byte-identical output across active/
  inactive/version/release variants was attempted or is safe to attempt
  this pass.
- **F. No further optimization** — **SELECTED for this pass.**

## Concrete follow-up sketch (Candidate B, NOT authorized/implemented)

- New provider method, same file/pattern as `get_fugr_enlfdir`/
  `get_fugr_func_metadata`: `get_fugr_changed_by_stamp( iv_program )` →
  looks up a worker-local cache keyed by resolved program name.
- New preparation entry point, parallel to `prepare_fugr`, but triggered
  from wherever the repository-wide `CHANGED_BY` sweep is orchestrated
  (needs discovery of that call site — likely `ZCL_ABAPGIT_STATUS_CALC`
  or `ZCL_ABAPGIT_OBJECTS`, not yet read this pass), not from `before_
  dispatch`. Must resolve includes for every FUGR in the sweep **once**,
  either by accepting one `RS_GET_ALL_INCLUDES` call per FUGR (already
  the current per-object cost — no regression) purely to build a bulk
  `FOR ALL ENTRIES` driver table for `REPOTEXT`/`REPOSRC`/`EUDB`, or by
  finding a genuinely bulk-safe include-resolution source (needs its own
  research pass).
- Must preserve exact tie-break semantics: `SORT lt_stamps BY date
  DESCENDING time DESCENDING` then take the first row, including the
  `iv_extra`-specific single-program override path.
- Mandatory test: call-count parity (`RS_GET_ALL_INCLUDES` count must not
  increase versus today), output parity (`changed_by` returns byte-
  identical user for a fixture with known multi-include history),
  MISS/fallback parity, feature-OFF purity.
- Entry condition to resume: owner authorization to spend a dedicated
  design+adversarial-review pass on this (it is a correctness-sensitive
  area — `CHANGED_BY` feeds `ZCL_ABAPGIT_CTS_INTEGRATION=>FIND_CHANGED_BY`
  /`CHANGED_BY_BULK`, confirmed live in the supplied traces).

## Decision (SER-FINAL, first pass)

`FUGR_SELECTED_DESIGN=F_NO_FURTHER_OPTIMIZATION_THIS_PASS`. No
implementation. Candidate B recorded as a named, resumable backlog item
in `.memory/state.md`.

## SER-FINAL-CONTINUOUS re-evaluation (2026-08-10, second pass)

Re-read every `is_serial_prefetch_active()`-guarded consumer seam in
`ZCL_ABAPGIT_OBJECT_FUGR` (`functions()`, `serialize_texts` i18n block,
`serialize_xml` AREAT block) looking specifically for an "F-B: eliminate
direct read after successful provider HIT" defect (a HIT that still does
direct work) or a call executed even though its result is unused. Found:

- `serialize_texts`/`serialize_xml`: both correctly guard with
  `IF <flag> = abap_false. SELECT ... ENDIF.` and both consume their
  result unconditionally afterward (no dead-work pattern).
- `functions()` itself (called from `includes()`, `changed_by()`, and the
  main content-serialize path): correctly skips its own direct `SELECT *
  FROM enlfdir` when the provider HIT (`lv_prefetched = abap_true`) - no
  defect.
- The one real dead-work defect found is inside `changed_by()`'s own
  unconditional call **to** `functions()` - not a provider-seam defect at
  all (it doesn't matter whether `functions()`'s own internal provider
  HITs or MISSes - the outer call itself was provably unnecessary for the
  dominant caller). This is handled as its own package - see
  `fugr_changed_by_design.md` - because it is a `CHANGED_BY`-specific
  fix, not a serializer/provider-coverage fix in the Phase 2 sense.

No additional Phase-2-scoped (serializer/provider coverage) defect was
found this pass.

## Decision (SER-FINAL-CONTINUOUS, final)

`FUGR_SERIALIZER_SELECTED_DESIGN=F-G_NO_FURTHER_CHANGE` (re-confirmed,
more thoroughly). The FUGR `CHANGED_BY` fix is tracked and implemented
separately - see `fugr_changed_by_design.md` /
`fugr_changed_by_current_source.md` / `.memory/reviews/
fugr_changed_by_adversarial.md`. Continuing directly to DDLS disposition
(unchanged, `ser_final_ddls_disposition.md`) and then to implementation/
regression/handoff - per the mission's operating rules, this no-change
conclusion does not end the mission.
