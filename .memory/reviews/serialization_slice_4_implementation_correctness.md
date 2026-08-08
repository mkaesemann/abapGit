# SER-SLICE-4 — Post-implementation correctness review (A+B+C integrated)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_4_POST_IMPLEMENTATION_CORRECTNESS_REGRESSION
BASELINE_HEAD=commit f545fc45 "ORTEC: Add FUGR metadata and function-directory
  batch provider" (pre-fix), fixed in a follow-up uncommitted edit to
  src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap
STATUS=PASS_WITH_ONE_BLOCKING_FINDING_FOUND_AND_FIXED
```

## Provenance

A first delegation to `ortec-abapgit-regression` produced a shallow,
low-confidence pass (wrong output path
`.memory/logs/serialization_slice_4_regression.md`, no invariant IDs, no
depth on byte-overflow arithmetic or RFC-interface parameter parity).
That artifact is kept as supplementary evidence only - it is NOT
authoritative. The orchestrator independently re-verified the
highest-risk invariants directly against source, per this project's
"never trust a subagent's compact summary alone" convention.

## Invariant matrix

| ID | Invariant | Result | Evidence |
|---|---|---|---|
| IC-001 | Feature-OFF purity: TABL/PROG/FUGR object serializers take the identical pre-SER-SLICE-4 path when the prefetch seam is inactive | PASS | All three seams (`zcl_abapgit_object_tabl.clas.abap`, `zcl_abapgit_object_prog.clas.abap`, `zcl_abapgit_object_fugr.clas.abap`) are gated by `is_serial_prefetch_active()`/prefetch-hit checks with the original per-object read as the unconditional fallback branch - confirmed via diff review during each package's own implementation (see `.memory/logs/serialization_slice_4_{tabl_ttyp,prog,fugr}_implementation.md`). |
| IC-002 | Aggregate byte admission (`sum_provider_buffer_bytes`) never overflows regardless of individual buffer sizes | **FAIL, then FIXED** | `sum_provider_buffer_bytes` summed six `xstrlen()` (`TYPE i`) results with a bare `+` chain assigned to a `TYPE int8` `rv_bytes`. ABAP's classic arithmetic-expression type inference computes the calculation type from the OPERAND types at each step, not the target - so the intermediate accumulation ran in 32-bit `I` precision (max 2,147,483,647) and could overflow BEFORE the final int8 assignment, exactly the failure mode the design intended to prevent. The pre-existing test `byte_sum_overflow_boundary` (two ~1GB buffers, partial sum 2,147,485,646 > `I` max) exercises precisely this path. **Fixed** by wrapping every `xstrlen()` term in `CONV int8( ... )` so every pairwise addition is `int8 + int8 = int8` from the first term onward - see the updated method and its inline rationale comment. |
| IC-003 | Cross-batch isolation: `inject_batch_from_buffer_X` fully replaces (never merges with) a prior batch's cached rows | PASS | Every RFC-worker code path (`zabapgit_ortec_serial.fugr.z_abapgit_ortec_ser_batch.abap`) calls the corresponding `clear_X_cache( )` UNCONDITIONALLY before the conditional `inject_batch_from_buffer_X(...)`, for DD/OO/MSAG/TABL/PROG/FUGR alike - confirmed by direct read of the full worker body. |
| IC-004 | RFC interface parameter parity: caller (`dispatch_batch`'s `CALL FUNCTION`) and callee (`.abap` interface + `.xml`) have identical `IV_PREFETCH_BUFFER_*` parameter sets | PASS | Grep of `zcl_abapgit_ortec_ser_orch.clas.abap` confirms all 6 buffers (`_dd/_oo_batch/_msag/_tabl/_prog/_fugr`) appear in the `sum_provider_buffer_bytes` call, the `dispatch_batch` signature, and the RFC `CALL FUNCTION` EXPORTING list; the worker's interface comment block and `.xml` RSIMP/RSFDO rows carry the same 6 parameters (verified during Package C completion). |
| IC-005 | PRESENT/MISS/FALLBACK semantics mutually exclusive/exhaustive per routed object type | PASS | Each provider's `CASE ls_tadir-object` branch in the RFC worker sets exactly one of `provider_hit`/`provider_miss` via an `IF ... = abap_true. ... ELSE. ... ENDIF.` shape; `WHEN OTHERS` sets `provider_fallback`. No branch can fall through without setting a flag. |
| IC-006 | HIT-rule correlation validated BEFORE any cache mutation in `inject_batch_from_buffer_X` | PASS | TABL/PROG/FUGR `inject_batch_from_buffer_X` all perform every corruption/correlation check (provider_id, wire_format_version, object_count, duplicate entries, language-not-initial, payload/entry correlation) strictly before the `CLEAR mt_*`/`INSERT ... INTO TABLE mt_*` block - confirmed via full read of `inject_batch_from_buffer_fugr` (most complex case, areat/enlfdir "either" rule) and consistent with the TABL/PROG implementation logs' own stated ordering. |
| IC-007 | Test-class `LOCAL FRIENDS` list matches actual test classes present | PASS | `zcl_abapgit_ortec_ser_pref_ext.clas.testclasses.abap`'s `LOCAL FRIENDS` line lists exactly `ltcl_dd_batch_wire ltcl_tabl_batch_wire ltcl_prog_batch_wire ltcl_fugr_batch_wire`, matching the four `LOCAL FRIENDS`-requiring test classes defined in the file (verified: no class was added without an updated friend grant). |
| IC-008 | No regression to DOMA/DTEL/CLAS/INTF/MSAG providers' existing behavior | PASS | All three packages' diffs are strictly additive (new methods, new `CASE`/`WHEN` branches, new interface parameters) - no existing `WHEN 'DTEL'`/`'DOMA'`/`'CLAS' OR 'INTF'`/`'MSAG'` branch, nor any pre-existing method body outside the new TABL/PROG/FUGR-specific additions, was touched by any of the three commits (9a67c8ee, bf11d559, f545fc45), confirmed via each package's own `git diff`/`get_errors` review performed during implementation. |

## Findings

- **BLOCKING (fixed): IC-002** - the aggregate byte-admission accumulator computed its running sum in 32-bit `I` precision instead of `int8`, defeating the overflow-safety design intent. Fixed in `sum_provider_buffer_bytes` (see diff). No test changes were needed - the pre-existing `byte_sum_overflow_boundary` test already exercises the exact scenario and now exercises it correctly against the fixed implementation.
- No other blocking or major findings.

## Residual scope

No live SAP syntax check or ABAP Unit execution has been performed this
session (no live connectivity). The IC-002 fix in particular should be
prioritized for real execution at the consolidated IT8 pass, since it is
the one finding in this review that could only be dispositively proven
by either a live overflow dump (pre-fix) or a clean pass (post-fix) -
static review alone cannot fully rule out a kernel-specific arithmetic
nuance.
