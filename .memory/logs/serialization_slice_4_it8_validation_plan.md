# SER-SLICE-4 — consolidated IT8 validation plan (TABL/PROG/FUGR providers)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_4_IT8_VALIDATION_PLAN
SCOPE=TABL i18n+extras provider (Package A), PROG metadata/text provider
  (Package B), FUGR metadata+function-directory provider (Package C),
  shared aggregate provider-byte-admission prerequisite. TTYP, TABL full
  DD03P/DD43V provider, PROG full source provider, FUGR full source/
  include provider, and legacy Path 3 restoration are OUT OF SCOPE
  (DEFERRED_BY_APPROVED_DESIGN - see serialization_slice_4_tabl_ttyp_
  design.md &sect;9 for TTYP's deferral rationale).
PRECONDITION=SER-SLICE-3's adaptive-batch architecture and DOMA/DTEL/
  CLAS/INTF/MSAG providers are already SAP-validated per
  serialization_slice_3_it8_validation_plan.md - this plan does NOT
  re-run that validation, only the NEW TABL/PROG/FUGR-specific scope on
  top of it.
STATUS=NOT_YET_EXECUTED - owner action required.
```

## 1. Object activation order (owner action required)

No new global DDIC objects were created this slice (design correction:
all three providers reuse existing PRIVATE ABAP TYPES as wire payload,
not new DDIC structures - see each package's own implementation log).
Only existing objects were CHANGED:

```text
OWNER_ACTION_REQUIRED=ACTIVATE_CHANGED_OBJECTS_IN_ORDER

1. CLAS ZCL_ABAPGIT_ORTEC_SER_PREF_EXT (main + testclasses) - gained
   TABL/PROG/FUGR extract_for_batch_*/inject_batch_from_buffer_*/
   clear_*_cache methods, ty_fugr_func_meta gained rfcscope/rfcvers/
   rfc_fields_valid, get_fugr_func_metadata gained additive OPTIONAL
   EXPORTING params. Activate first - no new dependencies on any other
   changed object.
2. CLAS ZCL_ABAPGIT_OBJECT_TABL, ZCL_ABAPGIT_OBJECT_PROG,
   ZCL_ABAPGIT_OBJECT_FUGR - prefetch-gated seams calling into step 1's
   new methods - activate after step 1.
3. CLAS ZCL_ABAPGIT_ORTEC_SER_ORCH (main + testclasses) -
   sum_provider_buffer_bytes gained iv_buffer_tabl/_prog/_fugr params
   (and the CONV int8 overflow fix), before_dispatch/dispatch_batch
   gained the three new buffer computations/forwards - activate after
   step 1 (calls extract_for_batch_tabl/_prog/_fugr).
4. FUGR ZABAPGIT_ORTEC_SERIAL, FUNCTION Z_ABAPGIT_ORTEC_SER_BATCH -
   gained IV_PREFETCH_BUFFER_TABL/_PROG/_FUGR import parameters, the
   matching clear+inject blocks, and WHEN 'TABL'/'PROG'/'FUGR'
   telemetry branches - activate after step 1 (calls the same new
   methods) and step 3 (caller-side signature must match).
```

Activate in this order; re-run `get_errors`-equivalent (real ADT syntax
check) after each step before proceeding to the next.

## 2. ABAP Unit / ATC scope

```text
RUN=ABAP_UNIT
OBJECTS=ZCL_ABAPGIT_ORTEC_SER_PREF_EXT, ZCL_ABAPGIT_ORTEC_SER_ORCH
EXPECTED_NEW_TEST_METHODS=
  ltcl_tabl_batch_wire (11), ltcl_prog_batch_wire (8),
  ltcl_fugr_batch_wire (8), plus before_dispatch_msag_buf_empty/
  before_dispatch_tabl_buf_empty and 7 byte_sum_* methods in
  ZCL_ABAPGIT_ORTEC_SER_ORCH's own testclasses (including
  byte_sum_overflow_boundary, which specifically re-validates the
  IC-002 int8-overflow fix)
GATE=ALL_GREEN, ZERO_SKIPPED, ZERO_DUMPS

RUN=ATC
OBJECTS=all 6 files listed in &sect;1 plus the two files' testclasses
  includes
GATE=NO_NEW_PRIORITY_1_OR_2_FINDING introduced by this slice
```

## 3. Path A/B regression (existing behavior unaffected)

```text
SCENARIO=FEATURE_OFF_TABL_PROG_FUGR
STEPS=with the repository-level adaptive-batch setting OFF (or the
  TABL/PROG/FUGR-specific prefetch seam otherwise inactive), serialize a
  repository containing TABL, PROG, and FUGR objects via BOTH the
  legacy non-batch path (Path A) and the adaptive-batch path with the
  new providers left dormant (Path B pre-SER-SLICE-4 behavior).
EXPECTED=byte-for-byte identical serialized output between Path A and
  Path B for every TABL/PROG/FUGR object in both runs - proves IC-001
  feature-OFF purity holds on a real system, not just via static review.
```

## 4. TABL/PROG/FUGR provider OFF-vs-ON comparison (adaptive batching ON in both cases)

```text
SCENARIO=PROVIDER_OFF_VS_ON
STEPS=with adaptive batching ON throughout, run the SAME repository
  scope twice: once with the TABL/PROG/FUGR seams forced inactive
  (provider OFF, batch dispatch still active for other object types),
  once with them active (provider ON).
EXPECTED=byte-for-byte identical serialized output for every TABL/PROG/
  FUGR object between the two runs. Any diff is a BLOCKING finding -
  the provider must never change what gets serialized, only where the
  data comes from.
REPEAT=once per provider (TABL only, PROG only, FUGR only) AND once with
  all three ON together (&sect;5).
```

## 5. Mixed-provider run

```text
SCENARIO=ALL_SIX_PROVIDERS_MIXED
STEPS=serialize a repository containing a realistic mix of DOMA, DTEL,
  CLAS, INTF, MSAG, TABL, PROG, and FUGR objects in the SAME adaptive
  batch run (objects of different types sharing batches per the
  existing adaptive-batch sizing logic).
EXPECTED=every object type's HIT/MISS telemetry and serialized output
  are correct simultaneously - proves no provider's CLEAR/inject logic
  clobbers another provider's cache (cross-provider isolation, distinct
  from cross-BATCH isolation already covered by &sect;6).
```

## 6. HIT / MISS / FALLBACK and cross-batch isolation

```text
SCENARIO=HIT_MISS_FALLBACK
STEPS=include at least one TABL/PROG/FUGR object that DOES have
  prefetchable data (expect HIT), one that does NOT (e.g. a table with
  no i18n texts and no TDDAT extras row, expect MISS with correct
  fallback to the per-object read), and at least one object type NOT
  covered by any provider (expect FALLBACK) in the same run.
EXPECTED=provider_hit/provider_miss/provider_fallback telemetry counts
  match the expected scenario for every object; MISS objects still
  produce fully correct output via the untouched per-object fallback
  path.

SCENARIO=CROSS_BATCH_ISOLATION
STEPS=force at least two separate batches (e.g. via a small
  c_max_actual_batch_bytes-style cap, or simply enough objects to
  exceed one adaptive batch), where batch 1 and batch 2 contain
  DIFFERENT TABL/PROG/FUGR objects.
EXPECTED=no object from batch 1 is ever visible to batch 2's worker
  session or vice versa (a pooled/reused RFC worker must not leak a
  prior dispatch's cache) - this exercises the real RFC worker process
  pooling behavior that static review (IC-003) cannot fully prove.
```

## 7. Corrupt / version-mismatch buffer handling

```text
SCENARIO=CORRUPT_BUFFER_GRACEFUL_DEGRADATION
STEPS=this is best validated by re-running the LOCAL unit tests
  (reject_unknown_provider_id, reject_p_entry_without_payload/
  reject_prog_without_p_entry/reject_extras_or_text_..., reject_
  initial_language across all three ltcl_*_batch_wire classes) as part
  of &sect;2's ABAP Unit gate - these already exercise every corrupt/
  mismatched buffer shape at the unit level. No additional live-system
  scenario is needed beyond confirming these tests pass for real
  (not just get_errors-clean).
EXPECTED=ALL_GREEN per &sect;2.
```

## 8. Actual-byte admission / oversized singleton

```text
SCENARIO=AGGREGATE_BYTE_ADMISSION
STEPS=construct (or find) a repository scope where the combined TABL+
  PROG+FUGR+DD+OO_BATCH+MSAG prefetch buffers for one batch approach or
  exceed c_max_actual_batch_bytes; separately, include at least one
  single TABL/PROG/FUGR object whose OWN prefetch payload alone exceeds
  the cap (an "oversized singleton").
EXPECTED=the oversized singleton is still correctly serialized (falls
  back to MISS/per-object read, never dropped or corrupted); the
  aggregate admission check correctly caps/redistributes remaining
  batch membership without ever silently truncating a buffer.
  byte_sum_overflow_boundary (IC-002 fix) and byte_sum_above_limit/
  byte_sum_exactly_at_limit (unit tests) provide the arithmetic-level
  proof; this live scenario proves the real dispatch behaves correctly
  around the cap, not just the sum function in isolation.
```

## 9. Performance comparison

```text
SCENARIO=PERFORMANCE_COMPARISON
STEPS=capture a SAT trace for a real repository with a meaningful
  TABL/PROG/FUGR object count, once with the new providers OFF and once
  ON (adaptive batching ON in both cases, matching &sect;4's shape).
EXPECTED=measurable reduction in per-object DB round trips (TFDIR/
  TLIBT/ENLFDIR/TDDAT/D010TINF SELECT counts) for HIT objects, with NO
  regression in total wall-clock time vs. the SER-SLICE-3 baseline SAT
  numbers already recorded (non-batch 29.008580s -> batch 11.789157s
  total, serialization 23.162255s -> 5.786946s, RFC starts 1669->68).
  Those SER-SLICE-3 numbers prove the adaptive-batch architecture
  generally - they do NOT by themselves prove this slice's incremental
  TABL/PROG/FUGR provider benefit, which is what this scenario measures.
  Also confirm no NEW O(K^2)-shaped cost is visible for large batches
  (indirectly re-validates the PS-001 fix at real scale, beyond static
  complexity analysis).
```

## 10. Per-provider decision matrix

For EACH of TABL, PROG, FUGR independently, after running &sect;3-&sect;9:

```text
DECISION=ACCEPT
  IF: Path A/B regression clean, OFF-vs-ON byte-identical, HIT/MISS/
  FALLBACK correct, cross-batch/cross-provider isolation clean, no
  corrupt-buffer dump, byte admission correct, performance neutral-or-
  better.

DECISION=FIX_AND_RETEST
  IF: a single, well-understood, narrow defect is found (e.g. one
  object-type edge case) that does not implicate the shared envelope/
  admission/orchestrator code paths shared with DOMA/DTEL/CLAS/INTF/
  MSAG.

DECISION=DISABLE_PROVIDER_KEEP_GENERIC_BATCH
  IF: the provider itself is unsafe/incorrect but the shared adaptive-
  batch architecture (already SER-SLICE-3-validated) is unaffected -
  gate the specific provider off (its seam already degrades gracefully
  to the pre-SER-SLICE-4 per-object path when inactive) without
  reverting the whole slice.

DECISION=REMOVE_OR_DEFER
  IF: a fundamental design flaw is found that FIX_AND_RETEST cannot
  reasonably resolve within this slice's scope.
```

## 11. Historical evidence carried forward (not re-derived, do not re-measure as SER-SLICE-4 proof)

```text
SOURCE=owner-provided real SAT comparison, SER-SLICE-3 scope
non_batch_total_seconds=29.008580
batch_total_seconds=11.789157
total_reduction_percent=59.36
total_speedup_x=2.46
non_batch_serialization_seconds=23.162255
batch_serialization_seconds=5.786946
serialization_reduction_percent=75.02
serialization_speedup_x=4.00
rfc_starts_non_batch=1669
rfc_starts_batch=68
rfc_starts_reduction_percent=95.93
avg_objects_per_batch=24.54
```
