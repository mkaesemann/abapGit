# SER-SLICE-4 — Pre-implementation MINOR disposition

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_4_MINOR_DISPOSITION
BASELINE_HEAD=8c9e5df4f9dd4fdaa4e05103cc0ac1e773758a32
STATUS=GATE_PASSED
```

## Scope

Every review artifact in scope was read in full (all cycles, not just
final verdicts):

```text
.memory/reviews/serialization_slice_4_tabl_ttyp_adversarial.md (3 cycles)
.memory/reviews/serialization_slice_4_prog_adversarial.md (3 cycles)
.memory/reviews/serialization_slice_4_fugr_adversarial.md (3 cycles)
.memory/reviews/serialization_slice_4_correctness.md
.memory/reviews/serialization_slice_4_performance.md (2 cycles)
.memory/reviews/serialization_slice_4_readiness.md
```

Exhaustive `SEVERITY=MINOR`/`OPEN_MINOR` grep across all three
adversarial review files confirms: TABL had 0 minors at every cycle
(only BLOCKER/MAJOR findings, all closed by cycle 3). PROG had exactly
one MINOR (PR-004, test-coverage gap), closed at cycle 2. FUGR had 0
minors at every cycle. The correctness gate had 2 minors (CG-002,
CG-003) after CG-001 was independently disproven. The performance gate
had 0 minors (PF-001/002/003 were all BLOCKER/MINOR-but-fixed, see
below - PF-002/PF-003 were scored MINOR and both closed at cycle 2).
Readiness.md is a specification document, not an adversarial review - it
carries no independent finding ledger.

## Disposition table

```text
ID=PR-004
PACKAGE=B (PROG)
ORIGINAL_SEVERITY=MINOR
CLASSIFICATION=STALE_OR_ALREADY_RESOLVED
RATIONALE=Test-coverage gap (missing provider_id/entry-state/duplicate-
  payload/lifecycle negative tests in the design's own §10). Fully
  closed at cycle 2 - the design's current §10 already names every
  required test. No further action needed; this is not a residual
  finding, it is closed design content.
IMPLEMENTATION_SLICE=Slice B2 (serialization_slice_4_readiness.md) -
  the TESTS field there already lists the full §10 matrix.
CLOSEOUT_CONDITION=Already closed; implementation must simply produce
  the tests §10 names (routine test-authoring, not a disposition risk).
```

```text
ID=PF-002
PACKAGE=C (FUGR)
ORIGINAL_SEVERITY=MINOR
CLASSIFICATION=STALE_OR_ALREADY_RESOLVED
RATIONALE=Missing explicit empty-driver guard on the new TFDIR bulk
  SELECT. Fully closed at performance-gate cycle 2 - the design's §6 now
  declares/populates `lt_funcnames` and guards
  `IF lt_funcnames IS INITIAL. RETURN. ENDIF.` before the SELECT,
  independently re-verified by the performance reviewer against the
  sibling-guard convention.
IMPLEMENTATION_SLICE=Slice C2 (readiness.md) - CHANGE field already
  specifies this exact guard.
CLOSEOUT_CONDITION=Already closed; implementer follows §6 verbatim.
```

```text
ID=PF-003
PACKAGE=C (FUGR)
ORIGINAL_SEVERITY=MINOR
CLASSIFICATION=DOCUMENTATION_ONLY
RATIONALE=Wording ambiguity only (which mechanism bounds a single
  oversized-singleton FUGR) - no code behavior was ever wrong, only the
  design doc's own §8 prose. Fully closed at performance-gate cycle 2 by
  clarifying that `c_max_actual_batch_bytes` bounds multi-object batches
  only and the pre-existing post-hoc adaptive-shrink mechanism handles a
  pathological singleton, with zero code change needed for that
  fallback (it already applies uniformly to every object type).
IMPLEMENTATION_SLICE=none - no code changes result from this finding at
  all, by the finding's own "zero code change needed" conclusion.
CLOSEOUT_CONDITION=Already closed; no action.
```

```text
ID=CG-002
PACKAGE=A (TABL)
ORIGINAL_SEVERITY=MINOR
CLASSIFICATION=VERIFY_DURING_IMPLEMENTATION_REVIEW
RATIONALE=Package A's (TABL) adversarial review, across all 3 cycles,
  never included the RFC worker source
  (`zabapgit_ortec_serial.fugr.z_abapgit_ortec_ser_batch.abap`) in its
  ALLOWED_CONTEXT/SOURCE_SCOPE - only the object class, provider class,
  and ORCH class were read. Package B's cycle 2 and Package C's cycle 1
  reviews both DID independently read the RFC worker source and verify
  the real `IF iv_prefetch_buffer_ext IS NOT INITIAL` / clear-first-
  then-inject pattern there. TABL's own design §8 worker-wiring text is
  a straight structural mirror of that ALREADY-VERIFIED PROG/FUGR
  pattern (unconditional `clear_tabl_cache( )` then conditional
  `inject_batch_from_buffer_tabl(...)`, a new `WHEN 'TABL'` CASE
  branch) - LOW risk (not a novel mechanism), but genuinely never
  independently source-checked anywhere in this slice's review chain.
  This does NOT affect architecture, serialized-output parity, active/
  inactive semantics, language completeness, wire format/key
  correlation, memory bounds, cleanup, fallback, two-path isolation, or
  public/DDIC signatures - it is a "has anyone actually looked at the
  real file" gap, not a design defect, so it does not require reopening
  Package A's design.
IMPLEMENTATION_SLICE=Slice A5 (RFC worker + interface,
  serialization_slice_4_readiness.md) - its own STOP_IF clause already
  requires this exact spot-check before Slice A5 is implemented
  ("the CG-002 pre-implementation spot-check reveals the worker body's
  real structure differs... revise this slice's ANCHOR/CHANGE before
  proceeding").
CLOSEOUT_CONDITION=Before writing Slice A5's code, read the current
  `z_abapgit_ortec_ser_batch` body directly and confirm: (a) the
  existing DD/OO_BATCH/MSAG `clear_*_cache()`-then-conditional-inject
  pattern is structurally identical to what TABL's design assumes; (b)
  the `CASE ls_tadir-object` block has room for a `WHEN 'TABL'` branch
  in the same style as the existing `WHEN 'DOMA'`/`WHEN 'CLAS' OR
  'INTF'` branches. If both hold (expected, since B/C already confirmed
  this pattern on the same file), proceed with Slice A5 as specified. If
  either does not hold, stop and report the exact divergence before
  writing TABL's worker-wiring code.
```

```text
ID=CG-003
PACKAGE=cross-package (shared infrastructure documentation)
ORIGINAL_SEVERITY=MINOR
CLASSIFICATION=STALE_OR_ALREADY_RESOLVED
RATIONALE=Documentation-precision-only finding: the task framing assumed
  all three packages extend `PREPARE`/`CLEAR`/`collect_keys`, but only
  Package A does (Packages B/C's caches were already wired into those
  three shared entry points by prior slices). Not a defect - the actual
  composition is safe either way. Already fixed directly in
  `serialization_slice_4_shared_infrastructure.md` &sect;5 during the
  design session (a "CG-003 CORRECTION" paragraph was added there).
IMPLEMENTATION_SLICE=none - documentation-only, no code slice affected.
CLOSEOUT_CONDITION=Already closed; no action.
```

## Required gate

```text
OPEN_BLOCKERS=0
OPEN_MAJORS=0
MINORS_REQUIRING_DESIGN_CHANGE=0
MINORS_REQUIRING_PREIMPLEMENTATION_SOURCE_CHANGE=0
IMPLEMENTATION_AUTHORIZATION=GO
```

No finding in any reviewed artifact is classified
`MUST_FIX_BEFORE_IMPLEMENTATION`. The single `VERIFY_DURING_
IMPLEMENTATION_REVIEW` item (CG-002) is a source-verification step
already built into Slice A5's own `STOP_IF` clause, not a design or
architecture reopening. No package design requires reopening under any
of the reopening triggers (architecture, serialized-output parity,
active/inactive version semantics, language completeness, wire format or
key correlation, memory bounds or byte admission, cleanup or cross-batch
isolation, fallback or terminal outcomes, final two-path isolation,
public/DDIC signatures) - every one of those categories was already
exercised and closed across the 3 adversarial cycles per package plus
the cross-package correctness and performance gates.
