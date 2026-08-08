# SER-SLICE-5 — integrated provider activation and mixed output parity

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_5_INTEGRATED_PARITY
STATUS=OWNER_OBSERVED_PASS_NOT_INDEPENDENTLY_MEASURED
```

## Evidence available this session

```text
OWNER_STATEMENT="I imported the change to IT8 and validated that it still
  works" (2026-08-08) - a functional confirmation from the person who
  performed the IT8 import, but not accompanied by a formal byte-diff parity
  artifact (file key set / path / content comparison) supplied to this
  session.
```

No live serialize-and-diff tool is available in this session to independently
trigger a full-repo (or scoped mixed-object) serialize twice - once via the
principal switch OFF (normal serializer) and once ON (adaptive batch with all
providers active) - and byte-compare the resulting file sets. This is a real
capability gap for THIS session, not a defect in the implementation.

## What IS independently confirmed this session (see
`serialization_slice_5_gate_lifecycle.md`)

```text
- Live IT8 active source matches the committed SLICE5-001 fix exactly.
- 151/151 ABAP Unit tests pass live on IT8, including the DOMA/DTEL/CLAS/
  INTF/MSAG/TABL/PROG/FUGR batch-wire round-trip suites
  (LTCL_*_BATCH_WIRE/LTCL_DOMA_PARITY) - these already assert byte-identical
  round-trip payloads and HIT/MISS correctness for EACH provider family in
  isolation, at the unit level.
- ATC clean (0 P1, 0 new P2) on every touched/consuming object.
```

Unit-level parity (each provider's own extract/inject round-trip) was already
proven before this session (SER-SLICE-3/4). What was NOT proven before this
session, and is NOT independently proven by this session either, is: does the
INTEGRATED, MIXED, real-batch-worker execution (the actual RFC call with all
six buffers non-empty at once, real objects, the fixed gate flag) produce
output identical to the normal serializer for a real repository. The owner's
"still works" statement is the only evidence for that specific claim right
now.

## Required assertions - status

```text
same complete file key set        NOT INDEPENDENTLY VERIFIED (owner-observed
                                   only)
same paths                        NOT INDEPENDENTLY VERIFIED
byte-identical file contents      NOT INDEPENDENTLY VERIFIED
no duplicate files                NOT INDEPENDENTLY VERIFIED
no missing files                  NOT INDEPENDENTLY VERIFIED
no partial success                NOT INDEPENDENTLY VERIFIED (the fail-fast
                                   WAIT/error contract from SER-SLICE-2 is
                                   unchanged by SLICE5-001 and remains
                                   independently ABAP-Unit-tested - see
                                   INCOMPLETE_BLOCKS_RETURN/FAILURE_BLOCKS_
                                   RETURN/QUEUED_FAILURES_BLOCK_RETURN in the
                                   151-test run above)
zero-file objects handled correctly  Covered at the unit level
                                   (ZERO_FILE_SUCCESS_FLAGGED/ZERO_FILE_
                                   UNMATCHED_ROW_OK/ZERO_FILE_BUT_FAILED_ROW_OK
                                   in ORCH's own suite) - not independently
                                   re-verified against a live integrated run
stable ordering where contractually relevant  Unit-tested (NO_PARALLEL_
                                   PARITY, TIE_BREAK_USES_ORIGINAL_ORDER) -
                                   not independently re-verified live
```

## Disposition

```text
INTEGRATED_OUTPUT_PARITY=PASS (owner-observed evidence tier; not
  independently MEASURED this session)
```

This is recorded honestly as a residual, not hidden or inflated to a stronger
evidence tier than what was actually obtained. Recommended minimal owner
recipe if a formal, independently-reproducible parity artifact is wanted
later: serialize the SAME repository twice back-to-back (principal switch
OFF, then ON) via the standard abapGit "Full Stage"/serialize action, and diff
the two resulting local working-copy file trees (path set + SHA1) - no new
tooling is required beyond what abapGit itself already does for status
calculation.
