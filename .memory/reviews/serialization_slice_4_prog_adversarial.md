# SER-SLICE-4 Package B PROG adversarial review

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_4_PACKAGE_B_ADVERSARIAL_REVIEW_CYCLE_1
BASELINE_HEAD=e5e10d62137033801739b8a53c69bdff9cfa650e
CYCLE=1
VERDICT=REVISE_AND_REVIEW_ONCE
```

## Scope and Verification Boundary

Read-only review was limited to the requested artifacts and source files:

- `.memory/logs/serialization_slice_4_prog_design.md`
- `.memory/logs/serialization_slice_4_shared_infrastructure.md`
- `.memory/logs/serialization_slice_4_common_discovery.md`
- `.memory/logs/serialization_slice_3_clas_intf.md`
- `src/objects/zcl_abapgit_object_prog.clas.abap`
- `src/ortec/serial/zcl_abapgit_ortec_ser_pref_ext.clas.abap`
- `src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap`

No productive code, diagrams, state, archive files, editor-memory files, or test files were modified.

The RFC worker function source was not in the permitted `SOURCE_SCOPE`, so the worker-side claim that `ser_pref_ext=>inject_from_buffer` is called only when `iv_prefetch_buffer_ext IS NOT INITIAL` could not be independently source-verified in this review. The orchestrator half of the central root-cause claim was verified against current source: `before_dispatch` computes only `lv_prefetch_buffer_dd`, `lv_prefetch_buffer_oo_batch`, and `lv_prefetch_buffer_msag`, and its `dispatch_batch` call does not pass `iv_prefetch_buffer_ext`; `dispatch_batch` merely forwards that optional parameter if supplied.

## Root-Cause Claim

Status: PARTIAL.

Verified from `zcl_abapgit_ortec_ser_orch.clas.abap`:

- `dispatch_batch` has optional `iv_prefetch_buffer_ext` in its signature and forwards it to `Z_ABAPGIT_ORTEC_SER_BATCH`.
- `before_dispatch` does not assign or pass `iv_prefetch_buffer_ext`.
- `before_dispatch` currently passes only the DD, OO batch, and MSAG buffers.

Not verified from current source because it was outside the allowed source scope:

- The RFC worker's conditional `IF iv_prefetch_buffer_ext IS NOT INITIAL` around `ser_pref_ext=>inject_from_buffer`.

Impact on review: the design's "currently ZERO benefit under RFC batch" framing is consistent with the verified orchestrator path, but the worker subclaim remains source-unclosed in this cycle.

## Findings

```text
ID=PR-001
SEVERITY=MAJOR
CLAIM=Section 9 fully resolves the method-name collision risk for PROG versus existing DOMA/DTEL batch methods on ZCL_ABAPGIT_ORTEC_SER_PREF_EXT.
COUNTEREXAMPLE=The same design still specifies `METHOD extract_for_batch` in section 3, calls `zcl_abapgit_ortec_ser_pref_ext=>extract_for_batch( it_object_keys )` in section 6, titles section 4 as `inject_batch_from_batch_buffer`, and calls `inject_batch_from_buffer( iv_prefetch_buffer_prog )` in worker pseudocode. Current source already declares `extract_for_batch` and `inject_batch_from_buffer` for DOMA/DTEL, so implementing the earlier pseudocode literally either fails to compile or targets the wrong provider method.
EVIDENCE=serialization_slice_4_prog_design.md lines 163-213, 221, 320-350, 413-420; zcl_abapgit_ortec_ser_pref_ext.clas.abap lines 199, 209, 1311, 1367
IMPACT=compiler/implementation-ambiguity
REQUIRED_CHANGE=Rename every PROG-specific declaration, pseudocode method header, orchestrator call, RFC worker call, and test reference to exactly `extract_for_batch_prog`, `inject_batch_from_buffer_prog`, and `clear_prog_cache`; remove the contradictory `inject_batch_from_batch_buffer` name.
RETEST=Static grep over the design must show no bare PROG use of `extract_for_batch(` or `inject_batch_from_buffer(` on `ZCL_ABAPGIT_ORTEC_SER_PREF_EXT`, except when explicitly describing the existing DOMA/DTEL methods.
```

```text
ID=PR-002
SEVERITY=MAJOR
CLAIM=The proposed batch envelope satisfies strict duplicate/unexpected-key handling and canonical object-key correlation.
COUNTEREXAMPLE=The injection pseudocode validates only duplicate rows in `entries`, then clears `mt_prog_langs` and inserts every row from `lt_prog`. It never validates `provider_id`, never validates entry state values, never rejects duplicate payload rows, and never proves that each payload `program` has exactly one matching `PROG` entry. A corrupt buffer can carry `entries` for program A and a `prog` payload for program B; if B is in the actual dispatch, `get_prog_tpool_languages` will return a HIT for B despite there being no valid envelope entry for B.
EVIDENCE=serialization_slice_4_prog_design.md lines 246-279, 298-315; shared_infrastructure.md sections 1, 3, 4; zcl_abapgit_ortec_ser_pref_ext.clas.abap lines 597-607
IMPACT=correctness/cross-object-contamination/stale-or-corrupt-prefetch-acceptance
REQUIRED_CHANGE=During inject, reject the whole buffer unless `provider_id = 'SER_PROG'`, every entry has `obj_type = 'PROG'` and a known state, every `P` entry has exactly one payload row with `program = obj_name`, every payload row has exactly one matching `P` entry, every `M` entry has no payload row, and duplicate payload programs are rejected before insertion. If `F` is allowed by the generic envelope, specify its exact PROG semantics; otherwise reject it.
RETEST=Add negative tests for provider_id mismatch, non-PROG entry type, invalid state, payload row without entry, P entry without payload, M entry with payload, and duplicate payload program; each must leave the worker cache empty after the clear-first/catch path.
```

```text
ID=PR-003
SEVERITY=MAJOR
CLAIM=The `extract_for_batch_prog` pseudocode is decision-free and mirrors existing provider state.
COUNTEREXAMPLE=Section 3 gates extraction with `IF mv_prepared = abap_false`, and section 4 sets `mv_prepared = abap_true`, but current `ZCL_ABAPGIT_ORTEC_SER_PREF_EXT` has no `mv_prepared` class-data field. Existing `prepare`/`clear` lifecycle is represented by cache content plus `mv_language`, not a prepared flag. Literal implementation will not compile; adding the flag is a new lifecycle decision that needs explicit reset and failure-path semantics.
EVIDENCE=serialization_slice_4_prog_design.md lines 171, 285; zcl_abapgit_ortec_ser_pref_ext.clas.abap grep showed no `mv_prepared`, with `clear` clearing caches and `mv_language` only
IMPACT=compiler/lifecycle-state-divergence
REQUIRED_CHANGE=Either remove `mv_prepared` entirely and rely on the same empty-cache/no-hit behavior as existing `extract_for_batch`, or explicitly add `mv_prepared` to the class design with `prepare`, `clear`, failed-prepare, successful-inject, and clear-cache semantics. The lower-risk amendment is to remove the flag.
RETEST=Static grep must show no `mv_prepared` in Package B design unless accompanied by a complete lifecycle section and tests; compile-oriented review must prove the method can be added to the current class without undeclared state.
```

```text
ID=PR-004
SEVERITY=MINOR
CLAIM=Section 10 covers the required negative and invariant tests.
COUNTEREXAMPLE=The test list covers unknown version, duplicate entries, object_count mismatch, and corrupt IMPORT, but not the additional envelope invariants needed for this provider: provider_id mismatch, invalid state, payload-entry mismatch, duplicate payload rows, non-PROG entry type, failed inject after clear-first, and wrong-language fallback behavior.
EVIDENCE=serialization_slice_4_prog_design.md lines 434-471; PR-002; zcl_abapgit_ortec_ser_pref_ext.clas.abap lines 597-607
IMPACT=test-coverage/regression-detection
REQUIRED_CHANGE=Extend section 10 with one test per missing invariant and explicitly require worker-style `clear_prog_cache` before failed injection so a corrupt buffer cannot leave stale cache data.
RETEST=Updated design test matrix names the missing cases and ties each to the closure proof for PR-002.
```

## Non-Findings

- `get_prog_tpool_languages` preserves the existing HIT semantics for present-but-empty language lists if the batch path injects a cache row for such programs: current source clears `et_tpool_i18n`, checks `iv_language = mv_language`, reads `mt_prog_langs` by `program`, and returns `rv_found = abap_true` whenever the row exists, even if the table is empty.
- The main-source `RPY_PROGRAM_READ` batching rejection is adequately justified for this slice. There is no existing seam comparable to `mt_prog_langs`, and replacing a kernel/source serializer path would be a materially different risk class from transporting already-prepared language-discovery metadata.
- The performance model's "currently ZERO benefit" framing is internally consistent with the verified orchestrator path, provided the worker conditional-injection subclaim is verified in a future review with the RFC worker source in scope.

## Closure Ledger

```text
OPEN_MAJOR=PR-001,PR-002,PR-003
OPEN_MINOR=PR-004
CLOSED=none
ROOT_CAUSE_CLAIM_VERIFIED=PARTIAL
REVIEW_RESULT=REVISE_AND_REVIEW_ONCE
```

## Cycle 2

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_4_PACKAGE_B_ADVERSARIAL_REVIEW_CYCLE_2
BASELINE_HEAD=e5e10d62137033801739b8a53c69bdff9cfa650e
CYCLE=2
VERDICT=REVISE_AND_REVIEW_ONCE
```

## Cycle 2 Scope and Verification Boundary

Read-only review was limited to the requested artifacts and source files:

- `.memory/logs/serialization_slice_4_prog_design.md`
- `.memory/reviews/serialization_slice_4_prog_adversarial.md`
- `.memory/logs/serialization_slice_4_shared_infrastructure.md`
- `.memory/logs/serialization_slice_3_clas_intf.md`
- `src/objects/zcl_abapgit_object_prog.clas.abap`
- `src/ortec/serial/zcl_abapgit_ortec_ser_pref_ext.clas.abap`
- `src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap`
- `src/ortec/serial/rfc/zabapgit_ortec_serial.fugr.z_abapgit_ortec_ser_batch.abap`

No productive ABAP/DDIC/UI/RFC/test file, diagram, state file, archive file, or editor-memory file was modified. This Cycle 2 section is the only write.

## Cycle 2 Root-Cause Claim

Status: VERIFIED.

Verified from current `zcl_abapgit_ortec_ser_orch.clas.abap`:

- `before_dispatch` computes only `lv_prefetch_buffer_dd`, `lv_prefetch_buffer_oo_batch`, and `lv_prefetch_buffer_msag`.
- `before_dispatch` calls `dispatch_batch` with only those three provider buffers.
- `dispatch_batch` still has optional `iv_prefetch_buffer_ext` and forwards it to `Z_ABAPGIT_ORTEC_SER_BATCH`, but no current `before_dispatch` call supplies it.

Verified from current `zabapgit_ortec_serial.fugr.z_abapgit_ortec_ser_batch.abap`:

- The worker calls `zcl_abapgit_ortec_ser_pref_ext=>inject_from_buffer( iv_prefetch_buffer_ext )` only under `IF iv_prefetch_buffer_ext IS NOT INITIAL`.
- Therefore the generic EXT buffer path that can carry `prog_langs` is not populated for adaptive RFC batches today.

## Cycle 2 Closure Ledger

```text
ID=PR-001
SEVERITY=MAJOR
STATUS=CLOSED
CLAIM=Cycle 2 consistently distinguishes PROG batch methods from the existing DOMA/DTEL extract_for_batch/inject_batch_from_buffer methods.
COUNTEREXAMPLE=Cycle 1's contradictory bare PROG method references are no longer present in sections 3/4/6. The remaining bare `extract_for_batch`/`inject_batch_from_buffer` mentions are explicitly about existing DOMA/DTEL or prior-cycle collision context, not the PROG implementation surface.
EVIDENCE=serialization_slice_4_prog_design.md sections 3, 4, 6, 9; grep for extract_for_batch/inject_batch_from_buffer/mv_prepared; current ZCL_ABAPGIT_ORTEC_SER_PREF_EXT existing DOMA/DTEL methods
IMPACT=closure/compiler-ambiguity-removed
REQUIRED_CHANGE=None for PR-001.
RETEST=Static design grep shows PROG declarations/calls/tests use `extract_for_batch_prog`, `inject_batch_from_buffer_prog`, and `clear_prog_cache`.
```

```text
ID=PR-002
SEVERITY=MAJOR
STATUS=CLOSED
CLAIM=Cycle 2 inject_batch_from_buffer_prog validates the provider envelope strongly enough to reject corrupt or cross-object payload acceptance.
COUNTEREXAMPLE=The revised pseudocode now rejects wrong `provider_id`, non-PROG entries, invalid states, duplicate ENTRIES keys, duplicate PROG payload rows, payload rows without a matching P entry, and P-entry/payload cardinality mismatches before clearing/repopulating `mt_prog_langs`.
EVIDENCE=serialization_slice_4_prog_design.md section 4 validation sequence; shared_infrastructure.md sections 1/4; current get_prog_tpool_languages lookup-by-program source
IMPACT=closure/corrupt-prefetch-acceptance-fixed
REQUIRED_CHANGE=None for PR-002.
RETEST=Cycle 2 section 10 names the provider_id, entry-type/state, duplicate-payload, payload-without-P, and P-without-payload negative tests.
```

```text
ID=PR-003
SEVERITY=MAJOR
STATUS=CLOSED
CLAIM=Cycle 2 removed the invented mv_prepared lifecycle field and replaced it with a source-backed prepared-state signal.
COUNTEREXAMPLE=Current `ZCL_ABAPGIT_ORTEC_SER_PREF_EXT` has no `mv_prepared`; its `prepare` method calls `clear( )`, then sets `mv_language = iv_language`, and `get_prog_tpool_languages` gates lookup by `iv_language <> mv_language`. The revised design uses `mv_language IS INITIAL` as the extract guard and sets `mv_language` from the imported buffer language after successful injection.
EVIDENCE=serialization_slice_4_prog_design.md sections 3/4; zcl_abapgit_ortec_ser_pref_ext.clas.abap `clear`, `prepare`, `get_prog_tpool_languages`, `prepare_prog_langs`
IMPACT=closure/compiler-state-divergence-fixed
REQUIRED_CHANGE=None for PR-003 itself. See PR-005 for a new malformed-buffer lifecycle edge introduced by the cycle-2 wording.
RETEST=Static design grep shows no operational `mv_prepared` dependency; current source confirms `mv_language` is the real existing language/prepared guard.
```

```text
ID=PR-004
SEVERITY=MINOR
STATUS=CLOSED
CLAIM=Cycle 2 section 10 covers the missing negative and lifecycle tests.
COUNTEREXAMPLE=The revised test matrix now includes provider_id mismatch, non-PROG/invalid-state entries, duplicate PROG payload rows, payload row without matching P entry, P entry without matching payload row, failed-inject leaves cache clean, and wrong-language fallback behavior.
EVIDENCE=serialization_slice_4_prog_design.md section 10; PR-002 closure evidence
IMPACT=closure/test-matrix-gap-fixed
REQUIRED_CHANGE=None for PR-004.
RETEST=Static review confirms every missing Cycle 1 invariant is named in the updated test matrix.
```

## Cycle 2 New Findings

```text
ID=PR-005
SEVERITY=MAJOR
CLAIM=Using the real `mv_language` field as the prepared-state signal is now fully lifecycle-safe for injected PROG buffers, including negative/lifecycle cases.
COUNTEREXAMPLE=A pooled worker can already have `mv_language = 'E'` from a prior successful provider injection. Cycle 2's worker pseudocode calls `clear_prog_cache( )`, which clears only `mt_prog_langs`, then `inject_batch_from_buffer_prog` accepts a buffer whose imported `language` is initial: it validates hdr/entries/prog, clears `mt_prog_langs`, inserts the PROG payload rows, and skips `mv_language = lv_language` because of `IF lv_language IS NOT INITIAL`. If the current batch language is also `E`, `get_prog_tpool_languages( iv_language = 'E' )` now returns HITs from a malformed buffer even though the new buffer did not establish its own language/prepared state. This contradicts Cycle 2's own reliance on `mv_language` as the prepared-state signal and makes the new `wrong-language fallback behavior` test assert an unsafe stale-session behavior.
EVIDENCE=serialization_slice_4_prog_design.md sections 3, 4, 8, 10; zcl_abapgit_ortec_ser_pref_ext.clas.abap `clear` clears `mv_language` but `clear_dd_cache` clears only DD caches, current DD pattern shows narrow clears do not reset `mv_language`; current `get_prog_tpool_languages` gates only by `iv_language <> mv_language`; RFC worker source shows clear-first/inject patterns run in potentially reused worker sessions
IMPACT=correctness/stale-session-contamination/corrupt-prefetch-acceptance
REQUIRED_CHANGE=Make `inject_batch_from_buffer_prog` reject the whole buffer when imported `language` is initial, before inserting payload rows; or make `clear_prog_cache` also clear `mv_language` and require successful inject to set it before any payload can be served. The lower-risk design amendment is to reject initial `language` as a corrupt PROG batch buffer because `extract_for_batch_prog` can only produce a non-initial language after `mv_language IS INITIAL` has passed.
RETEST=Add a worker-lifecycle test: first inject a valid language `E` buffer, then worker-style `clear_prog_cache` plus inject a buffer with initial `language` and valid-looking payload; the second inject must be rejected and subsequent `get_prog_tpool_languages( iv_language = 'E' )` must return MISS with an empty cache.
```

## Cycle 2 Final Ledger

```text
PR-001=CLOSED
PR-002=CLOSED
PR-003=CLOSED
PR-004=CLOSED
OPEN_MAJOR=PR-005
OPEN_MINOR=none
CLOSED=PR-001,PR-002,PR-003,PR-004
ROOT_CAUSE_CLAIM_VERIFIED=YES
REVIEW_RESULT=REVISE_AND_REVIEW_ONCE
```

## Cycle 3

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_4_PACKAGE_B_ADVERSARIAL_REVIEW_CYCLE_3
BASELINE_HEAD=e5e10d62137033801739b8a53c69bdff9cfa650e
CYCLE=3
VERDICT=APPROVE
```

## Cycle 3 Scope and Verification Boundary

Read-only review was limited to the requested artifacts and source files:

- `.memory/logs/serialization_slice_4_prog_design.md`
- `.memory/reviews/serialization_slice_4_prog_adversarial.md`
- `.memory/logs/serialization_slice_4_shared_infrastructure.md`
- `src/objects/zcl_abapgit_object_prog.clas.abap`
- `src/ortec/serial/zcl_abapgit_ortec_ser_pref_ext.clas.abap`

No productive ABAP/DDIC/UI/RFC/test file, diagram, state file, archive file,
or editor-memory file was modified. This Cycle 3 section is the only write.

## Cycle 3 PR-005 Verification

```text
ID=PR-005
SEVERITY=MAJOR
STATUS=CLOSED
CLAIM=Cycle 3 closes the stale mv_language malformed-buffer path by rejecting initial imported language before cache mutation and by making the successful mv_language assignment unconditional.
COUNTEREXAMPLE=No surviving counterexample in the revised design: section 4 imports `language = lv_language`, validates wire format/provider/object_count, then rejects `lv_language IS INITIAL` before the entry/payload validation, before `CLEAR mt_prog_langs`, and before the payload insertion loop. The only assignment after a successful import/validation path is the unconditional `mv_language = lv_language`; grep found no remaining operational `IF lv_language IS NOT INITIAL` branch in the PROG batch design. Current source confirms why this ordering matters: `get_prog_tpool_languages` gates HITs only by `iv_language <> mv_language` and reads `mt_prog_langs` by program, while `clear_prog_cache` is designed as a narrow cache clear and does not reset `mv_language`.
EVIDENCE=serialization_slice_4_prog_design.md packet header CYCLE_3_FIXES, section 4 validation sequence, section 10 `reject initial language` lifecycle test; zcl_abapgit_ortec_ser_pref_ext.clas.abap `clear`, `get_prog_tpool_languages`, current DOMA/DTEL narrow clear precedent; shared_infrastructure.md section 4 clear-first/inject pattern
IMPACT=closure/stale-session-contamination-fixed/corrupt-prefetch-acceptance-fixed
REQUIRED_CHANGE=None for PR-005.
RETEST=Static review confirms the missing-language guard precedes every cache mutation and the assignment is unconditional; section 10 now requires the exact stale-worker lifecycle regression test from PR-005.
```

## Cycle 3 New Findings

None.

## Cycle 3 Final Ledger

```text
PR-001=CLOSED
PR-002=CLOSED
PR-003=CLOSED
PR-004=CLOSED
PR-005=CLOSED
OPEN_BLOCKER=none
OPEN_MAJOR=none
OPEN_MINOR=none
CLOSED=PR-001,PR-002,PR-003,PR-004,PR-005
ROOT_CAUSE_CLAIM_VERIFIED=YES
REVIEW_RESULT=APPROVE
```