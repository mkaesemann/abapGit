# SER-SLICE-4 — Remaining provider design convergence (handoff)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_4_REMAINING_PROVIDER_DESIGN
START_HEAD=e5e10d62137033801739b8a53c69bdff9cfa650e
CURRENT_HEAD=e5e10d62137033801739b8a53c69bdff9cfa650e (memory-only work,
  productive source untouched this slice)
SER_SLICE_3_STATUS=LOCAL_COMPLETE_AWAITING_CONSOLIDATED_IT8 (unaffected,
  not interfered with)
STATUS=DESIGN_APPROVED_AWAITING_OWNER_IMPLEMENTATION_DECISION
```

## Mission recap

Run a design-only convergent design process (design -> adversarial
review -> revision -> re-review -> correctness gate -> performance
design gate -> implementation-readiness audit) for the three remaining
serialization provider candidates (TABL/TTYP, PROG, FUGR) while the
owner independently validates SER-SLICE-3 in IT8. No productive ABAP/
DDIC/UI/RFC/test changes; documentation-only.

## Decisions

```text
TABL_TTYP_DECISION=IMPLEMENT_PARTIAL_PROVIDER (TABL: per-extra-language
  DD02T text + TDDAT extras only; TTYP: DEFER, no safe low-risk win
  identified this slice)
PROG_DECISION=IMPLEMENT_METADATA_TEXT_PROVIDER (extend the EXISTING
  mt_prog_langs cache into a batch envelope - fixes a currently-ZERO-
  benefit architecture gap, no new SQL)
FUGR_DECISION=IMPLEMENT_METADATA_AND_DIRECTORY_PROVIDER (Option B -
  extend the THREE existing mt_fugr_areat/mt_fugr_enlfdir/
  mt_fugr_func_meta caches plus a new small TFDIR-RFCSCOPE/RFCVERS
  addition; Option C full source/include provider explicitly deferred
  as MEASURE_FIRST)
```

## The central architectural finding (applies to Packages B and C)

`ZCL_ABAPGIT_ORTEC_SER_ORCH=>before_dispatch` never populates the OLD
generic `iv_prefetch_buffer_ext` parameter (only the newer `_dd`/
`_oo_batch`/`_msag` batch-specific buffers are computed) - the RFC
worker only injects `ZCL_ABAPGIT_ORTEC_SER_PREF_EXT`'s cache `IF
iv_prefetch_buffer_ext IS NOT INITIAL`, which is always false in the
current call chain. Consequence: PROG's and FUGR's EXISTING single-
object prefetch seams (`get_prog_tpool_languages`, `get_fugr_areat`,
`get_fugr_enlfdir`, `get_fugr_func_metadata`) currently provide **ZERO
benefit** whenever the adaptive RFC batch path is active (the current
IT8-validated default) - every PROG/FUGR object is an unconditional MISS
in an RFC worker today, silently falling back to the pre-existing
per-object SELECT/FM calls. Both packages' designs fix this by adding a
NEW `extract_for_batch_*`/`inject_batch_from_buffer_*` pair (mirroring
the CLAS/INTF/MSAG precedent, reusing the generic
`ZAOG_SER_ENV_BHDR`/`ZAOG_SER_ENV_BENTRY`/`ZAOG_SER_ENV_BENTRY_TT`
envelope), NOT by inventing new SQL - this is fundamentally a "fix a
dead optimization path" package for both, not a new large win.

## TABL's central risk-avoidance finding

`DDIF_TABL_GET` already returns EVERYTHING (header/fields/tech-settings/
FKs/indexes/search-helps) in ONE call per object - there is no per-sub-
table N+1 problem to fix for the main-language path, unlike DOMA/DTEL/
MSAG/PROG/FUGR's original problems. The ONE sub-table genuinely valuable
to batch at scale (DD03P, table fields) is a SAP-runtime-flattened,
include/append-resolved structure - reproducing it via naive bulk reads
would require reimplementing recursive structure-generation logic,
assessed as unacceptably high correctness risk without a dedicated,
separately-reviewed follow-up (explicitly NOT authorized this slice).
The only genuinely safe, low-risk win identified is the per-extra-
language TABL text loop (structurally identical to DOMA/DTEL/PROG/
MSAG's i18n pattern) plus the trivially-bulkable `TDDAT` extras read.

## Convergence record (all cycles, per package)

```text
Package A (TABL/TTYP): 3 adversarial cycles (APPROVE at cycle 3) - 5
  findings (TT-001..TT-005), all CLOSED. Performance gate: 2 cycles
  (APPROVE at cycle 2) - PF-001 BLOCKER (O(N^2) cache-shape defect,
  fixed via nested-by-tabname restructure mirroring mt_fugr_enlfdir).
Package B (PROG): 3 adversarial cycles (APPROVE at cycle 3) - 5 findings
  (PR-001..PR-005), all CLOSED. Performance gate: clean on cycle 1 (no
  package-specific findings).
Package C (FUGR): 3 adversarial cycles (APPROVE at cycle 3) - 4 findings
  (FG-001..FG-004), all CLOSED. Performance gate: 2 cycles (APPROVE at
  cycle 2) - PF-002/PF-003 MINOR, both fixed.
Cross-package correctness gate: APPROVE_WITH_MINOR_REVISIONS - CG-001
  (claimed shared_infrastructure.md &sect;3 was "unimplementable") was
  independently RE-VERIFIED AGAINST LIVE SOURCE by this orchestrator and
  REJECTED_WITH_PROOF (both lv_prefetch_buffer_oo_batch/_msag ARE
  already computed in before_dispatch AND ARE already forwarded through
  dispatch_batch's real RFC CALL FUNCTION - direct source quotes in
  serialization_slice_4_correctness.md's "Orchestrator disposition"
  section). CG-002 (TABL worker-wiring never independently source-
  verified in any TABL review cycle) and CG-003 (PREPARE/CLEAR/
  collect_keys wiring precision) were genuine MINOR findings, both
  actioned (CG-003 fixed directly; CG-002 recorded as a mandatory pre-
  implementation checklist item in readiness Slice A5).
```

## Required artifacts (all created/updated this slice)

```text
.memory/logs/serialization_slice_4_common_discovery.md
.memory/logs/serialization_slice_4_tabl_ttyp_design.md (cycle 3 + PF fixes)
.memory/logs/serialization_slice_4_prog_design.md (cycle 3)
.memory/logs/serialization_slice_4_fugr_design.md (cycle 3 + PF fixes)
.memory/logs/serialization_slice_4_shared_infrastructure.md (+ CG-003 fix)
.memory/reviews/serialization_slice_4_tabl_ttyp_adversarial.md (3 cycles)
.memory/reviews/serialization_slice_4_prog_adversarial.md (3 cycles)
.memory/reviews/serialization_slice_4_fugr_adversarial.md (3 cycles)
.memory/reviews/serialization_slice_4_correctness.md (+ orchestrator
  disposition overriding CG-001)
.memory/reviews/serialization_slice_4_performance.md (2 cycles)
.memory/reviews/serialization_slice_4_readiness.md (17 slices across 3
  packages + 1 shared prerequisite slice)
.memory/handoffs/serialization-slice-4.md (this file)
.memory/state.md (updated)
```

## Owner decision required before any implementation

```text
1. Authorize implementation of Package A (TABL partial provider, TTYP
   deferred), Package B (PROG), and/or Package C (FUGR) - independently
   or together, per the readiness audit's cross-package ordering
   recommendation (readiness.md, final section).
2. Confirm the TABL DD03P/DD43V full-field-provider follow-up
   (explicitly NOT authorized this slice, tabl_ttyp_design.md &sect;12)
   remains deferred, or explicitly request a dedicated new design pass
   for it (would require its own discovery + adversarial cycles given
   the structure-flattening risk).
3. Confirm the FUGR Option C (full source/include provider) remains
   MEASURE_FIRST/deferred, or authorize a real SAT/ST05 trace on a
   FUGR-heavy repository before any further design work there.
```

## Next step

No further design work is queued. Resume SER-SLICE-3's own consolidated
IT8 validation (unaffected, unchanged by this slice) per the owner's own
parallel track; implementation of any Package A/B/C slice above requires
an explicit new owner instruction naming which package(s) to implement,
per the mandatory implementation flow (design review already done here;
next would be senior implementation + junior mechanical subtasks +
performance static scan + performance implementation audit + regression
validation).

---

## IMPLEMENTATION UPDATE (post-design, this session)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_4_IMPLEMENTATION
AUTHORITATIVE_BASELINE=8c9e5df4f9dd4fdaa4e05103cc0ac1e773758a32
STATUS=LOCAL_COMPLETE_AWAITING_CONSOLIDATED_IT8
```

The owner authorized implementation of Package A (TABL partial), Package
B (PROG), Package C (FUGR), and the shared aggregate-byte-admission
prerequisite (TTYP and all full-provider follow-ups above remain
DEFERRED_BY_APPROVED_DESIGN, unchanged). All three packages plus the
prerequisite are now implemented, get_errors-clean, unit-tested locally,
and committed:

```text
CHECKPOINT_COMMITS (in order):
370ebbdb - design phase (memory-only)
9a67c8ee - Phase 2 (aggregate byte admission + drift restoration) +
  Package A (TABL)
bf11d559 - Package B (PROG)
f545fc45 - Package C (FUGR)
070a8775 - IC-002 fix: aggregate byte-admission I-precision overflow
f75f1e87 - PS-001 fix: O(K^2) batch-entry correlation lookups
```

Two real defects were found and fixed during post-implementation review
(NOT present in the approved design, both introduced during coding):

1. **IC-002** (correctness, BLOCKING, fixed): `sum_provider_buffer_bytes`
   summed six `xstrlen()` (`TYPE i`) results with a bare `+` chain into a
   `TYPE int8` target - ABAP computes the intermediate sum in 32-bit `I`
   precision regardless of the target type, so the running total could
   overflow before ever reaching int8, defeating the whole point of the
   int8 accumulator. Fixed by wrapping every term in `CONV int8( ... )`.
   See `.memory/reviews/serialization_slice_4_implementation_correctness.md`.
2. **PS-001** (performance, MAJOR, fixed): `inject_batch_from_buffer_
   tabl/_prog/_fugr` each did an O(K) linear `READ TABLE ... WITH KEY`
   inside a loop over payload rows for the entry-correlation check,
   giving O(K^2) per inject call. Fixed with an O(1) HASHED secondary
   lookup table. See `.memory/logs/performance_scan_serialization_slice_4.md`
   and `.memory/reviews/serialization_slice_4_implementation_performance.md`.

Post-implementation correctness review (8 invariants, IC-001..IC-008)
and performance IMPLEMENTATION_AUDIT both verdict **APPROVE** after the
two fixes above. Full detail in:

```text
.memory/reviews/serialization_slice_4_implementation_correctness.md
.memory/reviews/serialization_slice_4_implementation_performance.md
.memory/logs/serialization_slice_4_tabl_ttyp_implementation.md
.memory/logs/serialization_slice_4_prog_implementation.md
.memory/logs/serialization_slice_4_fugr_implementation.md
.memory/logs/serialization_slice_4_it8_validation_plan.md (NEW -
  consolidated executable IT8 plan for this slice's TABL/PROG/FUGR
  scope, distinct from serialization_slice_3_it8_validation_plan.md)
```

No live SAP syntax check or ABAP Unit execution has been performed
(no live connectivity this session) - this is a disclosed residual, to
be closed by the owner running
`.memory/logs/serialization_slice_4_it8_validation_plan.md` at IT8.
Nothing was pushed.

### Next step (updated)

`OWNER_ACTION_REQUIRED=RUN_CONSOLIDATED_IT8_VALIDATION` per
`.memory/logs/serialization_slice_4_it8_validation_plan.md`. No further
design or implementation work is queued for SER-SLICE-4 pending that
validation's outcome (see &sect;10's per-provider decision matrix for
what happens next depending on results).
