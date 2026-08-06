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
