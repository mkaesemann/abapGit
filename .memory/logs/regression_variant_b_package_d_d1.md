# Regression validation: Variant B / Package D1

## Summary

- Task: Package D1 regression validation for generalized bounded external delta-base resolution.
- Scope: read-only validation of the D1 implementation handoff and the affected productive/test classes in the Ortec delta resolver and pack decoder/streaming paths.
- Validation mode: static source review plus workspace diagnostics; no live SAP ABAP Unit execution was available in this environment.
- Result: PASS_WITH_FINDINGS (static evidence is clean; runtime confirmation is still pending).

## Changed files reviewed

- [src/ortec/git/zcl_abapgit_ortec_delta.clas.abap](src/ortec/git/zcl_abapgit_ortec_delta.clas.abap)
- [src/ortec/git/zcl_abapgit_ortec_delta.clas.xml](src/ortec/git/zcl_abapgit_ortec_delta.clas.xml)
- [src/ortec/git/zcl_abapgit_ortec_delta.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_delta.clas.testclasses.abap)
- [src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap](src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap)
- [src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.abap](src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.abap)
- [src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.testclasses.abap)

## Evidence collected

- Workspace diagnostics reported “No errors found” for all six reviewed files.
- The new/changed method names in the D1 scope all remain within the ABAP 30-character limit.
- The relevant implementation map and correctness decision files were reviewed:
  - [.memory/handoffs/variant-b-package-d-d1-implementation.md](.memory/handoffs/variant-b-package-d-d1-implementation.md)
  - [.memory/reviews/variant_b_package_d_correctness_decision.md](.memory/reviews/variant_b_package_d_correctness_decision.md)
- The existing regression baseline for Package D was reviewed:
  - [.memory/logs/regression_variant_b_package_d_baseline.md](.memory/logs/regression_variant_b_package_d_baseline.md)

## Scenario matrix

| Scenario | Status | Evidence |
| --- | --- | --- |
| Bulk external-base resolution uses one shared bulk call | PASS | Verified in [src/ortec/git/zcl_abapgit_ortec_delta.clas.abap](src/ortec/git/zcl_abapgit_ortec_delta.clas.abap) and covered by tests in [src/ortec/git/zcl_abapgit_ortec_delta.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_delta.clas.testclasses.abap). |
| Missing external base still raises | PASS | Covered by `bulk_base_missing_raises` in [src/ortec/git/zcl_abapgit_ortec_delta.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_delta.clas.testclasses.abap). |
| Wrong-type base is rejected | PASS | Covered by `bulk_base_wrong_type_raises` in [src/ortec/git/zcl_abapgit_ortec_delta.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_delta.clas.testclasses.abap). |
| Corrupt-hash base is rejected | PASS | Covered by `bulk_base_corrupt_hash_raises` in [src/ortec/git/zcl_abapgit_ortec_delta.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_delta.clas.testclasses.abap). |
| Shared external base resolves for multiple deltas | PASS | Covered by `shared_base_two_deltas` and `mixed_ref_ofs_chain_ok` in [src/ortec/git/zcl_abapgit_ortec_delta.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_delta.clas.testclasses.abap). |
| Streaming path avoids the on-demand thin-fetch fallback for external bases | PASS | Covered by `no_thin_fetch_for_ext_base` in [src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.testclasses.abap). |
| Non-streaming decoder skips merged external-base rows correctly during promotion | PASS | Verified in [src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap](src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap) by the original-count threshold guard. |
| Live ABAP Unit execution | NOT RUN | No live SAP/ABAP Unit runner or connected system execution was available in this environment. |

## Pre-existing test inventory relevant to the touched logic

The D1 handoff and the existing baseline point to the following pre-existing tests that exercise the surrounding delta-resolution behavior:

- [src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap): `ref_chain_resolves`, `ofs_chain_resolves`, `external_thin_base_resolves`, `two_thin_bases_do_not_collide`, `missing_base_raises`, `missing_base_no_http_retry`, `base_after_dependent`, `resolve_after_prior_in_pass`, `chain_onto_later_unresolved`.
- [src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.testclasses.abap): `decode_populates_all`, `decode_from_pack`, `resume_after_partial`, `resume_no_session`, `cleanup_after_decode_failure`, `prefetch_bases_do_not_collide`, `peek_object_count_cases`.
- [src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.testclasses.abap): `delta_free_pack_decodes`, `ref_delta_stays_unresolved`, `corrupt_trailer_no_rows`, `decode_streaming_is_sparse`, `ext_base_resolve_after_preload`, `no_thin_fetch_for_ext_base`.

## Failure analysis

- Failing class/method: none.
- No new correctness defect was identified from the static review of the D1 implementation path.

## Corrective proposal

- None required for this validation pass.
- If live SAP validation becomes available, the next step is to run the relevant ABAP Unit suite in the connected system and confirm the runtime behavior end to end.

## Final local regression confirmation (pre-commit gate, post test-matrix and performance gates)

- D1 acceptance matrix: **COMPLETE** - all 13 named acceptance IDs map to a
  distinct, class-local test method (see "D1 acceptance test-ID matrix"
  above). `MISSING_TESTS=NONE`.
- Prior test preservation: no pre-existing test method was modified or
  removed in this pass; only new methods were appended (`duplicate_declared_sha_ok`,
  `base_later_in_pack_order`, `exhausted_recovery_raises` in
  `zcl_abapgit_ortec_delta.clas.testclasses.abap`; `partial_recovery_resumes`
  in `zcl_abapgit_ortec_pack_dec.clas.testclasses.abap`). The legacy
  `zcl_abapgit_ortec_git_tests.clas.testclasses.abap` suite was not touched.
- Package C (`29199f629773c676e0eaa2f3a006f5167d304ae8`, `SAP_VALIDATED_COMPLETE`)
  is unaffected: no file outside the D1 scope (delta resolver, pack decoder,
  pack streamer, and their test includes) was changed in this pass.
- Package D2 is unaffected: `zcl_abapgit_ortec_obj_store.clas.abap` remains
  read-only in this pass (verified again by direct diff review - no edits
  were made to it); staged-visibility `status='D'` logic, attempt/lock/
  transaction handling, and `get_staged_delta_objects` were not touched.
- ORTEC-disabled behavior: unchanged - all D1 code lives inside the ORTEC
  delta/pack-decoder/pack-streamer classes and is only reached from the
  existing ORTEC-only call paths already gated by Package B/C's
  enablement switch; no new unconditional call site was introduced.
- Performance gates: `PERFORMANCE_SCAN` produced no blocking/major finding
  (see [.memory/logs/performance_scan_variant_b_package_d_d1.md](.memory/logs/performance_scan_variant_b_package_d_d1.md));
  `PERFORMANCE_AUDIT` verdict is `PASS_WITH_MINOR_FINDINGS`, zero blocking
  findings (see [.memory/logs/performance_audit_variant_b_package_d_d1.md](.memory/logs/performance_audit_variant_b_package_d_d1.md)).
- Static instrumentation review (owner directive item 2): closed - both
  test-only counters retained as approved bounded observability metrics
  with explicit lifecycle/reset/concurrency/production-semantics doc
  comments (see "Test-only instrumentation disposition" above).
- **This local regression pass is static source review and workspace
  diagnostics only. It does NOT constitute executed SAP Unit testing, ATC,
  or functional validation on a connected system.** `SAP_VALIDATION=NOT_RUN`
  remains the accurate status until the owner imports the resulting
  checkpoint commit into SAP and reports real syntax/ABAP Unit/ATC/
  functional results.
- No blocking finding of any kind was identified in this pass.

```text
FINAL_LOCAL_REGRESSION=PASS_WITH_FINDINGS
BLOCKING_FINDINGS=0
TEST_MATRIX=COMPLETE
MISSING_TESTS=NONE
PACKAGE_C_AFFECTED=NO
PACKAGE_D2_AFFECTED=NO
ORTEC_DISABLED_BEHAVIOR=UNCHANGED
SAP_VALIDATION=NOT_RUN
```

## D1 acceptance test-ID matrix (added post-implementation, pre-commit gate)

All 13 named D1 acceptance IDs from
[.memory/logs/variant_b_package_d_design.md](.memory/logs/variant_b_package_d_design.md)
§16 are mapped to an exact, class-local test method. Method-name length was
verified programmatically (all ≤ 30 chars, max observed 29). None were added
to the legacy `zcl_abapgit_ortec_git_tests.clas.testclasses.abap` suite, per
the owner's explicit constraint.

| Acceptance ID | Test method | File | Status | Path |
| --- | --- | --- | --- | --- |
| `bulk_base_dedups_sha1s` | `bulk_base_dedups_sha1s` | `zcl_abapgit_ortec_delta.clas.testclasses.abap` | Pre-existing | Direct unit call to `bulk_resolve_external_bases` |
| `bulk_base_one_call_only` | `bulk_base_one_call_only` | `zcl_abapgit_ortec_delta.clas.testclasses.abap` | Pre-existing | Direct unit call, asserts `gv_bulk_load_calls = 1` |
| `bulk_base_unique_index` | `bulk_base_unique_index` | `zcl_abapgit_ortec_delta.clas.testclasses.abap` | Pre-existing | Direct unit call |
| `bulk_base_missing_raises` | `bulk_base_missing_raises` | `zcl_abapgit_ortec_delta.clas.testclasses.abap` | Pre-existing | Direct unit call, asserts raise |
| `bulk_base_wrong_type_raises` | `bulk_base_wrong_type_raises` | `zcl_abapgit_ortec_delta.clas.testclasses.abap` | Pre-existing | Direct unit call, asserts raise |
| `bulk_base_corrupt_hash_raises` | `bulk_base_corrupt_hash_raises` | `zcl_abapgit_ortec_delta.clas.testclasses.abap` | Pre-existing | Direct unit call, asserts raise |
| `shared_base_two_deltas` | `shared_base_two_deltas` | `zcl_abapgit_ortec_delta.clas.testclasses.abap` | Pre-existing | End-to-end `resolve_all` |
| `mixed_ref_ofs_chain_ok` | `mixed_ref_ofs_chain_ok` | `zcl_abapgit_ortec_delta.clas.testclasses.abap` | Pre-existing | End-to-end `resolve_all`, REF+OFS combo |
| `no_sql_in_pack_phase` | `no_sql_in_pack_phase` | `zcl_abapgit_ortec_delta.clas.testclasses.abap` | Pre-existing | End-to-end, asserts `gv_thin_fetch_calls = 0` |
| `duplicate_declared_sha_ok` | `duplicate_declared_sha_ok` | `zcl_abapgit_ortec_delta.clas.testclasses.abap` | **New (this gate)** | End-to-end `resolve_all`; two REF_DELTA rows declare the identical IN-PACK base SHA1 (phase 1 only, `gv_bulk_load_calls = 0` asserted) - distinct from `shared_base_two_deltas`'s EXTERNAL shared base |
| `base_later_in_pack_order` | `base_later_in_pack_order` | `zcl_abapgit_ortec_delta.clas.testclasses.abap` | **New (this gate)** | End-to-end `resolve_all`; extends legacy `chain_onto_later_unresolved` (3-link A->B->C chain) to phase 1.5 - terminal base C is now genuinely external, links 2-3 are OFS_DELTA (physically later in pack), proving the OFS branch's recursive `resolve_one` terminates correctly through a phase-1.5-merged external result |
| `partial_recovery_resumes` | `partial_recovery_resumes` | `zcl_abapgit_ortec_pack_dec.clas.testclasses.abap` | **New (this gate)** | End-to-end via `resume_decode` (not `decode_and_persist`) against a hand-built raw pack containing a REF_DELTA on a genuinely external base, with a pre-inserted crash-state session (`obj_done = 0`, `status = 'A'`) - proves phase 1.5 fires correctly on the RESUME entry point, not only the normal decode entry point |
| `exhausted_recovery_raises` | `exhausted_recovery_raises` | `zcl_abapgit_ortec_delta.clas.testclasses.abap` | **New (this gate)** | End-to-end `resolve_all` (not a direct call to `bulk_resolve_external_bases`); a REF_DELTA whose declared base is missing everywhere raises through the full sweep -> phase 1.5 path - distinct from `bulk_base_missing_raises`'s direct unit-level call |

Missing-acceptance-coverage gaps closed by this pass: `duplicate_declared_sha_ok`,
`base_later_in_pack_order`, `partial_recovery_resumes`, `exhausted_recovery_raises`
(4 of 13 IDs were previously unmapped to a distinct test; all 4 now have one).
`MISSING_TESTS=NONE` as of this update.

Workspace diagnostics (`get_errors`) re-run after adding the 4 new methods:
0 errors in `zcl_abapgit_ortec_delta.clas.abap`,
`zcl_abapgit_ortec_delta.clas.testclasses.abap`,
`zcl_abapgit_ortec_pack_stream.clas.abap`,
`zcl_abapgit_ortec_pack_dec.clas.testclasses.abap`. Live ABAP Unit execution
of these 4 new methods has NOT been run (no connected SAP system in this
environment) - this remains owner-side validation, tracked separately from
this static gate.

## Test-only instrumentation disposition (owner directive item 2)

`gv_bulk_load_calls` (`zcl_abapgit_ortec_delta`) and `gv_thin_fetch_calls`
(`zcl_abapgit_ortec_delta` and `zcl_abapgit_ortec_pack_stream`) were kept as
approved bounded observability counters rather than replaced with a
counter-free test seam. Rationale, recorded directly in each declaration's
doc comment: private `CLASS-DATA`, incremented at exactly one call site each,
reset only by the LOCAL FRIEND test class's own `setup`, never read or
branched on by productive code, and no counter-free alternative was
practical (`get_objects`/`get_object` are static object-store methods with
no injection seam, and the object store's own session cache defeats a
DB-deletion-based behavioral trap). This satisfies the directive's second
branch: "keep a productive counter only when it is an approved bounded
observability metric with defined lifecycle, reset, concurrency and
production semantics" - all four are now stated explicitly in the doc
comments.

## D1 SAP closeout (owner validation of `8bfca36beffd27f405034426d917556fc7959564`)

- Owner-executed live SAP validation results: Activation/syntax **PASS** except 3 SLIN
  warnings; targeted ABAP Unit **PASS**; ATC **PASS** except the same 3 SLIN warnings;
  warm-branch functional test **PASS**; cold-branch functional test **PASS**.
- The 3 SLIN warnings ("ZCX_ABAPGIT_EXCEPTION is not caught or declared") were on:
  - `zcl_abapgit_ortec_pack_dec=>peek_object_count` (line 234): the method's only
    non-trivial operation, `zcl_abapgit_convert=>xstring_to_int(...)`, is declared
    `RAISING zcx_abapgit_exception` by the standard helper even though it can never
    actually throw for a fixed 4-byte input (a plain X->I MOVE). Fix: wrapped in a
    local `TRY...CATCH zcx_abapgit_exception`, returning the existing `-1` sentinel
    on catch - preserves the method's long-standing no-exception, sentinel-result
    contract relied on by all 3 productive callers (`zcl_abapgit_ortec_fastpath`,
    `zcl_abapgit_ortec_cold_init` x2), which use it as a plain inline `= 0` check
    with no TRY/CATCH.
  - `zcl_abapgit_ortec_delta=>skip_size_header` (lines 302, 312): this PRIVATE
    method genuinely calls `zcx_abapgit_exception=>raise(...)` itself (truncated/
    malformed delta size-header bytes) but its own signature declared no `RAISING`
    clause at all. Fix: added `RAISING zcx_abapgit_exception` to the method
    signature; its only caller, `apply`, already declares `RAISING
    zcx_abapgit_exception`, so no further caller signature changes were needed.
  - Neither fix changes observable runtime behavior (ABAP checked exceptions
    propagate at runtime regardless of caller declaration; both fixes are pure
    exception-contract/SLIN hygiene). Per the owner directive, no new/adjusted
    ABAP Unit tests were added for these two fixes since no observable behavior
    changed; existing coverage (`peek_object_count_cases` and the pre-existing
    `apply`-based delta tests) remains valid and untouched.
  - `get_errors` re-run after both edits: 0 errors in
    `zcl_abapgit_ortec_pack_dec.clas.abap`, `zcl_abapgit_ortec_pack_dec.clas.testclasses.abap`,
    `zcl_abapgit_ortec_delta.clas.abap`, `zcl_abapgit_ortec_delta.clas.testclasses.abap`.
- False-MODIFIED-status anomaly: classified `PACKAGE_E_CONSUMER_COHERENCE` (not a D1
  regression) - full causal-chain evidence in
  [.memory/logs/variant_b_package_d_d1_modified_status_triage.md](.memory/logs/variant_b_package_d_d1_modified_status_triage.md).
  No D1 productive fix was required or made for this anomaly.
- `SAP_VALIDATION` updated from `NOT_RUN` to reflect the owner's real evidence:

```text
SAP_VALIDATION=ABAP_UNIT_PASS,ATC_PASS_EXCEPT_3_SLIN_NOW_FIXED,WARM_PASS,COLD_PASS
SLIN_FIX=IMPLEMENTED
SLIN_FILES=src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap,src/ortec/git/zcl_abapgit_ortec_delta.clas.abap
MODIFIED_STATUS_CLASSIFICATION=PACKAGE_E_CONSUMER_COHERENCE
D1_REGRESSION_FIX=NOT_REQUIRED
```

## Final owner SAP retest (commit `73cb519a`, system IT8)

```text
OWNER_SAP_RETEST=PASS
VALIDATED_HEAD=73cb519a
SLIN_WARNINGS_CLEARED=YES
ABAP_UNIT=PASS
ATC=PASS
WARM_BRANCH=PASS
COLD_BRANCH=PASS
FALSE_MODIFIED_STATUS=PACKAGE_E_CONSUMER_COHERENCE
D1_VERDICT=SAP_VALIDATED_COMPLETE
```

This is the final Package D1 regression evidence. No further D1 regression
work is open; the false-MODIFIED-status anomaly remains tracked, unchanged,
as a Package E follow-up (see
[.memory/logs/variant_b_package_d_d1_modified_status_triage.md](.memory/logs/variant_b_package_d_d1_modified_status_triage.md)).
