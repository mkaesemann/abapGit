# SER-SLICE-3 — adversarial-risk coverage note (DOMA/DTEL provider)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_3_PHASE_2_ADVERSARIAL_COVERAGE
STATUS=COVERED_VIA_DESIGN_AND_CORRECTNESS_REVIEWS_NO_SEPARATE_ADVERSARIAL_PASS
```

Honesty disclosure: a dedicated `ortec-abapgit-adversarial-design-review`
pass was run once, at DESIGN time only
(`.memory/logs/serialization_slice_3_provider_contract.md` review section,
verdict `REVISE_AND_REVIEW_ONCE`, 3 major/3 minor, all fixed). No separate
adversarial pass was run against the FINISHED IMPLEMENTATION - the
implementation-time correctness review
(`.memory/reviews/serialization_slice_3_correctness.md`) was explicitly
scoped to include the adversarial-flavored risk categories this slice's
"Convergent adversarial design protocol" would otherwise target
(corruption, duplication, cross-batch leakage, silent data loss), and its
own BLOCKER finding (DR-001) was exactly this kind of silent-wrong-data
risk. This is a scope/cost trade-off, not an oversight - documented here so
the owner can require a dedicated post-implementation adversarial pass
before IT8 if they judge the risk profile warrants it.

## Adversarial-relevant risks explicitly checked (via the correctness
review's REVIEW FOCUS items 3, 5, 6, 7) and their outcome

```text
Corrupt/truncated buffer causing silent wrong data -> WIRE_VALIDATION_ORDER
  CORRECT (all 3 semantic checks + the IMPORT TRY/CATCH precede any
  CLEAR/INSERT into the shared cache); REJECT_CORRUPT_IMPORT test added
  (DR-003 fix) drives the real CATCH cx_root branch, not just the
  semantic-validation branches.
Duplicate entries silently colliding in a HASHED TABLE INSERT -> explicitly
  guarded by a pre-INSERT sorted-copy adjacent-duplicate check that rejects
  the WHOLE buffer, confirmed by REJECT_DUPLICATE_ENTRIES.
Cross-batch/cross-worker-session state leakage -> CLEAR mt_doma/mt_dtel
  unconditionally BEFORE any INSERT in a rejected-buffer-safe order;
  NO_CROSS_BATCH_LEAKAGE test confirms batch A's data cannot survive batch
  B's inject.
Silent data loss from an incomplete language-discovery join (DR-001) -
  the actual BLOCKER this review cycle found: fixed by seeding the main
  language unconditionally; see the correctness review file for detail.
Repurposing an existing constant's semantics without checking for other
  readers (c_max_pre_dispatch_splits telemetry->hard-bound) -
  SPLIT_CAP_REPURPOSE_SAFE=YES, confirmed via grep before the change was
  accepted.
```

## Not exercised this pass (recorded, not silently dropped)

```text
- No live IT8 fuzz/negative-path run against the real RFC boundary
  (Z_ABAPGIT_ORTEC_SER_BATCH) with a deliberately corrupted
  iv_prefetch_buffer_dd payload sent over an actual aRFC call - only the
  in-process unit-test-level IMPORT/EXPORT round trip was exercised.
  Recorded as an IT8 validation plan item.
- DR-005 (latent iv_prefetch_buffer_ext + iv_prefetch_buffer_dd co-clear
  conflict) is not exploitable today and was not further adversarially
  probed beyond confirming it is unreachable in the current call graph.
```

## Addendum: parity-incident adversarial review, 2 cycles (2026-08-07)

See `.memory/incidents/serialization_slice_3_dtel_doma_parity.md` for the
full incident. A dedicated adversarial review actively attempted to
reproduce the reported 113-vs-2 file loss (and equivalent silent-loss
shapes) against the parity-incident fixes:

```text
CYCLE 1 VERDICT=REJECT (2 blocker, 1 major)
AR-3-001 (BLOCKER) - RFC worker only cleared mt_doma/mt_dtel as a side
  effect of INJECT_BATCH_FROM_BUFFER running (only when the buffer was
  non-initial) - a pooled/reused worker session could carry a PRIOR
  dispatch's real DOMA/DTEL cache into a LATER dispatch whose own buffer
  was legitimately empty. FIXED via new CLEAR_DD_CACHE, called
  unconditionally at the top of every Z_ABAPGIT_ORTEC_SER_BATCH
  invocation.
AR-3-002 (BLOCKER) - ROUTE_TO_SEQUENTIAL_FALLBACK (the last-resort
  recovery path, used both directly and as the recovery mechanism for
  every other guard in this class) unconditionally called
  mark_object_success after a successful serialize() call even with zero
  files - the "safety net" itself had the same hole as the original
  incident. FIXED via an explicit zero-file check routing to
  mark_object_failures instead.
AR-3-003 (MAJOR) - MERGE_INTO_MT_FILES returned success without checking
  the imported file list was non-empty. FIXED via an explicit empty-list
  check returning rv_merged = ABAP_FALSE (routes to the existing
  ROUTE_TO_SEQUENTIAL_FALLBACK recovery, which is itself now also
  guarded).

CYCLE 2 VERDICT=APPROVE (0 blocker, 0 major, 0 minor)
All three findings independently re-verified CLOSED. End-to-end chain
re-traced for the exact original incident shape (a DTEL object silently
producing zero files with RC=0 inside the RFC worker): now terminates
EITHER in real non-empty output OR a definite FAILURE that trips
ASSERT_SUCCESSFUL_RUN and discards the whole run with a visible
exception - never a silent "success, but this object contributed
nothing." No new regression risk identified from fixes D/E/F themselves
(no known legitimate zero-file abapGit object type found in source).
Truncation/collision/missing-entry/partial-hit/stale-cache/merge-loss/
terminal-miscount all re-attempted against the post-fix source and ruled
out.
```
