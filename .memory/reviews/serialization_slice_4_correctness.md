# Design Review — SER-SLICE-4 Cross-Package Correctness Gate

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_4_CORRECTNESS_GATE
BASELINE_HEAD=e5e10d62137033801739b8a53c69bdff9cfa650e
REVIEW_SCOPE=READ_ONLY_EXCEPT_THIS_ARTIFACT
```

## Scope control

Read only the seven allowed design/review logs plus, for spot-verification,
`src/ortec/serial/zcl_abapgit_ortec_ser_pref_ext.clas.abap` (full method
listing + `clear`/`clear_dd_cache`/`prepare`/`collect_keys`/
`get_prog_tpool_languages`/`prepare_prog_langs` bodies) and
`src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap`
(`before_dispatch`/`dispatch_batch` bodies and the `dispatch_batch`
signature). No productive ABAP/DDIC/UI/RFC/test file, diagram, state file,
archive file, or editor-memory file was modified. This artifact is the only
write.

## Verdict

REVISE_AND_REVIEW_ONCE

## Confidence

High

## Strengths

- **Method-name audit is genuinely clean.** Exhaustive re-count of every new
  method Packages A/B/C add to `ZCL_ABAPGIT_ORTEC_SER_PREF_EXT`
  (`extract_for_batch_tabl`=22, `inject_batch_from_buffer_tabl`=29,
  `clear_tabl_cache`=16, `get_tabl_i18n`=13, `get_tabl_extras`=15,
  `prepare_tabl`=12, `extract_for_batch_prog`=22,
  `inject_batch_from_buffer_prog`=29, `clear_prog_cache`=16,
  `extract_for_batch_fugr`=22, `inject_batch_from_buffer_fugr`=29,
  `clear_fugr_cache`=16) confirms all ≤30 chars and zero collisions with
  each other or with the existing bare `extract_for_batch`/
  `inject_batch_from_buffer`/`clear_dd_cache` (DOMA/DTEL). CLAS/INTF and
  MSAG equivalents live on separate classes (`ZCL_ABAPGIT_ORTEC_SER_PREF_OO`/
  `ZCL_ABAPGIT_ORTEC_SER_PREF`), so no cross-class collision is possible
  regardless of name reuse there.
- **Package C's PROG-independent dependency claim is verified true against
  live source, not just design prose.** Read `ZCL_ABAPGIT_ORTEC_SER_PREF_EXT`
  directly: `get_prog_tpool_languages` (public, lines 597-608) and
  `mt_prog_langs` (CLASS-DATA, line 347) already exist today, populated by
  the existing `prepare_prog_langs` (lines 925-953), which is already
  called unconditionally from the existing `prepare` (lines 651-695) —
  none of this is new in SER-SLICE-4. Package C's FUGR §6a seam therefore
  works (main-process/sequential benefit) with zero dependency on whether
  Package B's own batch-envelope methods are ever implemented, exactly as
  claimed.
- **DD03P/DD43V-style flattening-risk classification is applied
  consistently.** TABL's DD03P and TTYP's DD42V/DD43V are correctly
  identified as SAP-runtime-flattened/include-resolved structures and
  excluded. FUGR's new TFDIR-RFCSCOPE/RFCVERS addition and its existing
  TLIBT/ENLFDIR/func-metadata seams are all flat, single-table,
  non-recursive lookups — genuinely a different risk class, not a
  double standard.
- The three packages' `PREPARE`/`CLEAR`/`collect_keys` additions do not
  actually collide in practice: verified against live source that `clear`
  already clears `mt_prog_langs`/`mt_fugr_areat`/`mt_fugr_enlfdir`/
  `mt_fugr_func_meta`, `prepare` already calls `prepare_prog_langs`/
  `prepare_fugr` unconditionally, and `collect_keys` already has `WHEN
  'PROG'`/`WHEN 'FUGR'` branches — none of this is new. Only Package A adds
  genuinely new lines to these three shared entry points (a new `WHEN
  'TABL'` branch, a new `prepare_tabl` call, two new `CLEAR` lines for
  `mt_tabl_text`/`mt_tabl_extras`). See CG-003 below — the task's framing
  that "each package adds its own wiring" is not accurate for B/C, but this
  is a documentation nuance, not a real interference risk.

## Issues

### CG-001

- Type: correctness
- Severity: blocker
- Evidence: Direct read of `zcl_abapgit_ortec_ser_orch.clas.abap`
  `before_dispatch` (current body): the ONLY provider buffer computed is
  `DATA(lv_prefetch_buffer_dd) = zcl_abapgit_ortec_ser_pref_ext=>extract_for_batch(...)`,
  and the only buffer forwarded to `dispatch_batch` is
  `iv_prefetch_buffer_dd = lv_prefetch_buffer_dd`. There is no
  `lv_prefetch_buffer_oo_batch` or `lv_prefetch_buffer_msag` local variable
  anywhere in the method. Separately, `dispatch_batch`'s own RFC
  `CALL FUNCTION 'Z_ABAPGIT_ORTEC_SER_BATCH'` `EXPORTING` list forwards only
  `iv_prefetch_buffer`/`iv_prefetch_buffer_ext`/`iv_prefetch_buffer_oo`/
  `iv_prefetch_buffer_dd` — `iv_prefetch_buffer_oo_batch` and
  `iv_prefetch_buffer_msag`, though declared as `OPTIONAL` parameters on
  `dispatch_batch`'s signature, are never referenced inside the method body
  at all, i.e. never forwarded to the worker. `shared_infrastructure.md`
  §3 states the "current gap" is that `before_dispatch` "currently sums
  ONLY `xstrlen(lv_prefetch_buffer_dd)`" while implying `oo_batch`/`msag`
  buffers already exist as computed locals that just need folding into the
  sum ("the CLAS/INTF... and MSAG... buffers were added AFTER that check
  was written... NOT yet folded into the... admission check"). Package B's
  own cycle-1 adversarial review made a similarly incorrect claim
  ("`before_dispatch` currently passes only the DD, OO batch, and MSAG
  buffers") that was never revisited in cycle 2/3 (those cycles moved on to
  the RFC-worker inject side).
- Why it matters: §3's "MANDATORY correction... applies to ALL of Packages
  A/B/C together" instructs implementers to write
  `lv_actual_bytes` as the sum of `lv_prefetch_buffer_dd +
  lv_prefetch_buffer_oo_batch + lv_prefetch_buffer_msag +
  lv_prefetch_buffer_tabl + lv_prefetch_buffer_prog + lv_prefetch_buffer_fugr`,
  and calls this "a single line of arithmetic in `before_dispatch`" (§6).
  That literal instruction does not compile today —
  `lv_prefetch_buffer_oo_batch`/`lv_prefetch_buffer_msag` do not exist as
  locals in `before_dispatch`. An implementer following §3 verbatim must
  either (a) silently improvise — e.g. add the missing
  `extract_for_batch` calls to `ZCL_ABAPGIT_ORTEC_SER_PREF_OO`/
  `ZCL_ABAPGIT_ORTEC_SER_PREF` inside `before_dispatch` and fix
  `dispatch_batch`'s RFC call to actually forward those two buffers —
  which is real, unscoped, un-reviewed ORCH-infrastructure work smuggled in
  under a mis-sized "one line" description, or (b) quietly sum only the 4
  buffers that really exist (dd+tabl+prog+fugr) while the design's own text
  still claims 6-way coverage, leaving the "MANDATORY correction" only
  partially done without anyone noticing the shortfall was ever there. This
  also surfaces, as a byproduct, that CLAS/INTF and MSAG batch providers
  currently deliver **zero** benefit under the RFC/adaptive-batch dispatch
  path today (their buffers are not even transmitted to the worker) — the
  same "zero benefit under RFC batch" architecture gap Packages B/C
  explicitly disclosed for PROG/FUGR's pre-existing seams, but never
  disclosed for OO_BATCH/MSAG. (Note: this does not create a byte-budget
  *safety* hole — since OO_BATCH/MSAG truly contribute 0 bytes to the real
  RFC call today, omitting them from the sum happens to still be a correct
  bound — but it does mean the stated requirement is either unimplementable
  as literally written or is quietly downgraded without the design saying
  so.)
- Fix: Revise `shared_infrastructure.md` §3 before implementation to state
  the true current condition (`before_dispatch` computes/passes only
  `lv_prefetch_buffer_dd`; `dispatch_batch` does not forward
  `iv_prefetch_buffer_oo_batch`/`iv_prefetch_buffer_msag` to the RFC call
  at all) and pick one explicit, bounded scope: either (A) narrow this
  slice's summation requirement to the 4 buffers that will actually exist
  and be transmitted after this slice (`dd + tabl + prog + fugr`), and
  explicitly record OO_BATCH/MSAG's zero-current-benefit-under-RFC-batch as
  a disclosed pre-existing limitation out of scope here (mirroring how
  Packages B/C disclosed the same gap for their own pre-existing seams), or
  (B) explicitly authorize and design the additional `before_dispatch`
  computation + `dispatch_batch` RFC-forwarding fix for OO_BATCH/MSAG as
  part of this slice's bundled ORCH change, sized and reviewed as real
  work, not "a single line."

### CG-002

- Type: correctness
- Severity: minor
- Evidence: Package A's (TABL) adversarial review scope, across all 3
  cycles, never included the RFC worker source
  (`zabapgit_ortec_serial.fugr.z_abapgit_ortec_ser_batch.abap`) — only
  `zcl_abapgit_object_tabl.clas.abap`/`zcl_abapgit_object_ttyp.clas.abap`/
  `zcl_abapgit_ortec_ser_pref_ext.clas.abap`/
  `zcl_abapgit_ortec_ser_orch.clas.abap` were read. By contrast, Package
  B's cycle 2 and Package C's cycle 1 reviews both explicitly read the RFC
  worker source and verified the real
  `IF iv_prefetch_buffer_ext IS NOT INITIAL` / clear-first-then-inject
  pattern there.
- Why it matters: TABL's own §9 worker-wiring text ("unconditional
  `clear_tabl_cache( )` then conditional
  `inject_batch_from_buffer_tabl(...)`, `CASE ls_tadir-object` telemetry
  gains a `WHEN 'TABL'` branch") was never checked against the actual
  worker source in any TABL review cycle. It is a straight structural
  mirror of the already-verified PROG/FUGR pattern, so risk is low, but it
  is the one piece of Package A's design that was never independently
  source-verified anywhere in this slice's review chain.
- Fix: Before/during implementation, do one worker-source spot-check of the
  TABL `WHEN 'TABL'` branch and `clear_tabl_cache`/
  `inject_batch_from_buffer_tabl` call sites against the real
  `z_abapgit_ortec_ser_batch` body, the same way it was already done for
  PROG and FUGR.

### CG-003

- Type: maintainability
- Severity: minor
- Evidence: The task framing assumed "Packages A/B/C's respective
  additions to these THREE existing shared entry points (each package adds
  its own `prepare_x` call inside `PREPARE`, its own `CLEAR mt_x_*` inside
  `CLEAR`, its own `WHEN` branch inside `collect_keys`)". Direct source
  read shows this is true only for Package A. `mt_prog_langs`/
  `mt_fugr_areat`/`mt_fugr_enlfdir`/`mt_fugr_func_meta` are pre-existing
  caches already wired into `clear`, `prepare`, and `collect_keys` from
  prior slices; Packages B and C add no new lines to any of the three
  shared entry points — only new batch-envelope methods layered on top of
  already-populated caches.
- Why it matters: Not a defect — the actual composition is safe (Package A
  is purely additive; B/C touch none of the same lines) — but the
  discrepancy is worth recording so a future reader does not go looking for
  `prepare_prog`/`prepare_fugr`-call additions or new `collect_keys`
  branches that this slice never makes.
- Fix: None required functionally; optionally note in the shared
  infrastructure doc that only Package A extends `PREPARE`/`CLEAR`/
  `collect_keys` this slice, to keep the record accurate.

## Required revisions

- CG-001 must be resolved (shared_infrastructure.md §3 corrected to match
  actual `before_dispatch`/`dispatch_batch` source, with an explicit scope
  decision between options A/B above) before implementation begins on the
  byte-summation piece.

## Orchestrator disposition (post-review independent re-verification)

```text
CG-001=REJECTED_WITH_PROOF (not ACCEPTED_AND_FIXED)
```

Per this session's "verify load-bearing claims against current productive
source" rule, the orchestrator re-read
`src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap` directly,
fresh, after this finding was filed. CG-001's TWO factual claims are both
contradicted by the current file:

1. "There is no `lv_prefetch_buffer_oo_batch` or `lv_prefetch_buffer_msag`
   local variable anywhere in the method [`before_dispatch`]" - FALSE.
   Direct quote from the current method body:
   `DATA(lv_prefetch_buffer_oo_batch) = zcl_abapgit_ortec_ser_pref_oo=>
   extract_for_batch( it_object_keys ).` and
   `DATA(lv_prefetch_buffer_msag) = zcl_abapgit_ortec_ser_pref=>
   extract_for_batch( it_object_keys ).` both exist, and both are passed
   to the `dispatch_batch(...)` call at the end of the method
   (`iv_prefetch_buffer_oo_batch = lv_prefetch_buffer_oo_batch`,
   `iv_prefetch_buffer_msag = lv_prefetch_buffer_msag`).
2. "`dispatch_batch`'s own RFC `CALL FUNCTION 'Z_ABAPGIT_ORTEC_SER_BATCH'`
   ... forwards only `iv_prefetch_buffer`/`iv_prefetch_buffer_ext`/
   `iv_prefetch_buffer_oo`/`iv_prefetch_buffer_dd`" - FALSE. Direct quote
   from `dispatch_batch`'s actual `EXPORTING` list:
   `iv_prefetch_buffer_oo_batch = iv_prefetch_buffer_oo_batch` and
   `iv_prefetch_buffer_msag = iv_prefetch_buffer_msag` are BOTH present,
   immediately after `iv_prefetch_buffer_dd`.

Both buffers ARE computed, ARE forwarded through `dispatch_batch`, AND ARE
forwarded into the real RFC `CALL FUNCTION`. `shared_infrastructure.md`
§3's original description (the admission-check SUM currently covers only
the DD buffer, while all three - dd/oo_batch/msag - are already computed
and transmitted) was and remains ACCURATE; no revision to §3 was needed
or made for CG-001. CG-002 (worker-source spot-check reminder for TABL)
and CG-003 (PREPARE/CLEAR/collect_keys wiring precision) were genuine,
low-risk, MINOR observations and are FIXED (CG-003 directly, in
`shared_infrastructure.md` &sect;5; CG-002 recorded as a pre-
implementation checklist item in the SER-SLICE-4 handoff, not a design
defect).

```text
REVISED_VERDICT=APPROVE_WITH_MINOR_REVISIONS
BLOCKERS_AFTER_DISPOSITION=0
MAJORS_AFTER_DISPOSITION=0
MINORS_AFTER_DISPOSITION=2 (CG-002 actioned as checklist item, CG-003
  fixed in shared_infrastructure.md)
```

## Optional improvements

- CG-002: add a worker-source spot-check for TABL's telemetry/clear/inject
  wiring during implementation.
- CG-003: record in shared_infrastructure.md that only Package A extends
  `PREPARE`/`CLEAR`/`collect_keys`.

## Non-findings (checked, no issue)

- Method-name collision/length audit: CLEAN, zero collisions, all ≤30
  chars (Focus 1).
- Cross-package `PREPARE`/`CLEAR`/`collect_keys` composition: safe: no
  ordering/interference issue, only Package A touches these entry points
  (Focus 2, see CG-003 for the documentation nuance).
- Package C's PROG-accessor dependency: verified independent of Package B
  against live source (Focus 5).
- DD03P/DD43V-style derived-value risk classification: applied
  consistently across A/B/C — no double standard found (Focus 6).
