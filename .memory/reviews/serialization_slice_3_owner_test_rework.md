# SER-SLICE-3 — owner LTCL_SER_ORCH test rework review

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_3_OWNER_TEST_REWORK_REVIEW
BASELINE_HEAD=8c9e5df4f9dd4fdaa4e05103cc0ac1e773758a32
STATUS=REVIEWED_BOTH_TESTS_PASS
```

## Scope

Reviewed the owner's commit `fd6c8786` ("Fix Unit Tests"), specifically
the two renamed/reworked tests in
`src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.testclasses.abap`,
against the CURRENT production method bodies in
`src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap` and
`src/objects/zcl_abapgit_objects.clas.abap` (the standard abapGit
serialize wrapper). Not presumed correct merely because they pass -
independently traced against real production code.

## `fallback_missing_doma_resolves` (was `fallback_zero_files_fails`)

**Verdict: PASS - the rename/rework is CORRECT, confirmed against real
production source, not just against the test's own assertions.**

The new test asserts a nonexistent DOMA (`ZZZZ_SLICE3_NOT_A_REAL_
DOMAIN`) routed through `ROUTE_TO_SEQUENTIAL_FALLBACK` ends up in
`mt_resolved` (not `mt_failed`) with `ls_ctx-files` non-empty. The OLD
test asserted the OPPOSITE (failed, zero files).

**Independent verification of the new premise** ("a missing DOMA
returns early in `ZCL_ABAPGIT_OBJECT_DOMA`, but the wrapper
`ZCL_ABAPGIT_OBJECTS=>SERIALIZE` still contributes metadata XML"):

Direct read of `zcl_abapgit_objects.clas.abap` `METHOD serialize`
(CONFIRMED_SOURCE) shows the standard wrapper's flow is:

```abap
TRY.
    li_obj->serialize( li_xml ).          " DOMA's own serialize() -
                                            " returns early, adds
                                            " NOTHING to li_xml, for a
                                            " missing/inactive domain
  CATCH zcx_abapgit_exception INTO lx_error.
    ...
    RAISE EXCEPTION lx_error.
ENDTRY.

...
IF lo_files->is_json_metadata( ) = abap_false.
  lo_files->add_xml(                      " UNCONDITIONAL - runs
    ii_xml      = li_xml                  " regardless of whether
    is_metadata = li_obj->get_metadata( ) " li_obj->serialize() added
  ).                                      " anything to li_xml at all
ENDIF.

rs_files_and_item-files = lo_files->get_files( ).
```

`li_obj->serialize( li_xml )` does NOT raise an exception for a missing
DOMA (it returns normally, having added nothing) - so the `CATCH` never
fires, and execution falls through to the UNCONDITIONAL `add_xml(...)`
call, which produces the object's `.doma.xml` metadata file regardless
of whether the object's own body contributed any content. This
CONFIRMS: a missing DOMA genuinely produces at least one non-empty file
(the metadata XML wrapper) through the standard, unmodified abapGit
path - it is NOT a "zero files" case at all. The ORIGINAL test's
premise ("a missing/nonexistent object produces literally zero files")
was factually wrong; ROUTE_TO_SEQUENTIAL_FALLBACK correctly resolves
this case because the file list genuinely is non-empty, not because of
any ORTEC-side leniency.

**Checklist verification:**

```text
fixture really represents a missing/nonexistent DOMA: YES (a name that
  cannot exist, ZZZZ_SLICE3_NOT_A_REAL_DOMAIN)
sequential fallback behavior matches standard abapGit semantics: YES -
  confirmed via direct read of the real ZCL_ABAPGIT_OBJECTS=>SERIALIZE
  wrapper, not assumed
valid zero-file outcome for a missing object resolved only when the
  standard serializer contract allows it: N/A - this is NOT actually a
  zero-file case (see above); the genuinely-zero-file case remains
  covered by the UNCHANGED zero_file_success_flagged test (H5 parity-
  incident guard, still present immediately after this test in the
  class, comment/logic untouched)
a present ordinary object cannot silently resolve with missing files:
  covered by separate, unchanged tests (nonzero_file_not_flagged,
  zero_file_but_failed_row_ok, zero_file_unmatched_row_ok,
  zero_file_success_flagged) - this test does not touch that guard
failed fallback cannot be marked successful: N/A to this specific test
  (this test is about a genuinely-successful, non-empty-output case)
terminal success/failure counters remain correct: YES - mt_resolved
  gains the row, mt_failed does not, matching the real outcome
no partial output is returned as success: the output is the complete,
  legitimate (if small) metadata file, not a truncated/partial result
test name and assertions describe the exact contract: YES, the new name
  and inline comment accurately state the real mechanism
```

**Conclusion: preserve as-is. No production or test change required.**
This is a genuine bug fix to a test that never matched real production
behavior - not a weakening of terminal-outcome safety. The DISTINCT,
ACTUALLY-zero-file scenario (a present object whose worker reports
RC=0 with truly zero files) remains fully guarded by the unchanged
`zero_file_success_flagged`/`is_zero_file_success_bad` mechanism.

## `merge_empty_file_list_fails` (was `merge_empty_file_list_ok`)

**Verdict: PASS - the rework is CORRECT and requires no production
change; the production method's behavior was NEVER actually changed,
only the test's (previously wrong) assertion was corrected.**

Direct read of `zcl_abapgit_ortec_ser_orch.clas.abap` `METHOD
merge_into_mt_files` (CONFIRMED_SOURCE, unchanged by the owner's commit
- present in this exact form at BOTH the SER-SLICE-4 design baseline
`e5e10d62` and the current HEAD `8c9e5df4`):

```abap
TRY.
    IMPORT data = ls_serialization FROM DATA BUFFER is_result-files_xstring.
  CATCH cx_sy_import_format_error cx_sy_import_mismatch_error
        cx_sy_compression_error cx_sy_conversion_codepage.
    RETURN.
ENDTRY.
IF sy-subrc <> 0.
  RETURN.
ENDIF.

" SER-SLICE-3 parity incident fix (AR-3-003): never accept an empty
" imported file list as a successful merge.
IF ls_serialization-files IS INITIAL.
  RETURN.                                 " rv_merged stays at its
                                            " declared-default INITIAL
                                            " value = abap_false - no
                                            " explicit assignment needed
ENDIF.

LOOP AT ls_serialization-files INTO DATA(ls_file).
  APPEND INITIAL LINE TO <ls_ctx>-files ASSIGNING FIELD-SYMBOL(<ls_return>).
  ...
ENDLOOP.

rv_merged = boolc( lines( ls_serialization-files ) > 0 ).
```

This `IF ls_serialization-files IS INITIAL. RETURN. ENDIF.` guard was
ALREADY ADDED during SER-SLICE-3's own parity-incident fix (AR-3-003,
documented in the method's own comment) - it RETURNS EARLY, before the
`LOOP` that appends files, and before any explicit `rv_merged`
assignment, so `rv_merged` is guaranteed `abap_false` (its type's
default initial value) for an empty imported file list. The OLD test
(`merge_empty_file_list_ok`, `assert_true( lv_merged )`) NEVER matched
this real behavior - it was a latent, incorrect test, most likely never
updated when AR-3-003's guard was added in a prior SER-SLICE-3 pass. The
owner's rename/assertion-flip (`assert_false( lv_merged )`) simply makes
the test agree with reality.

**Checklist verification:**

```text
empty files table returns merge failure: YES, confirmed via the early-
  RETURN/initial-rv_merged mechanism above
no partial files appended: YES - the early RETURN happens strictly
  BEFORE the LOOP AT ls_serialization-files that appends to <ls_ctx>-
  files, so zero files can ever be appended on this path
item/path metadata are not fabricated: YES - the LOOP that sets
  <ls_return>-file-path/-item never executes on the empty-list path
ordinary valid serialized files still merge correctly: YES, unchanged,
  exercised by the separate, untouched merge_succeeds_with_payload test
a legitimate object-level zero-file contract is handled before merge,
  not converted into a false merge success: YES - this is exactly
  AR-3-003's own stated purpose, now correctly tested
the test exercises the productive merge method: YES, calls
  merge_into_mt_files directly with a real (invalid/empty) exported
  buffer
```

Complementary test coverage already present and unaffected by this
change (all in the same test class, confirmed still declared):
`merge_fails_without_context`, `merge_fails_on_bad_payload`,
`merge_succeeds_with_payload` - covering missing run context, corrupt
payload, and valid multi-file merge respectively. No new complementary
test is required beyond what already exists; the empty-file-list case is
now the ONLY one whose assertion needed correcting.

**Conclusion: preserve as-is. No production or test change required.**

## Separate, undocumented drift found during this review (NOT one of the
two named tests, but discovered while reconciling source anchors -
recorded here since it directly affects Phase 2's shared prerequisite)

`git diff e5e10d62...8c9e5df4 -- src/ortec/serial/core/
zcl_abapgit_ortec_ser_orch.clas.abap` shows `BEFORE_DISPATCH` and
`DISPATCH_BATCH` no longer compute/forward the CLAS/INTF
(`iv_prefetch_buffer_oo_batch`) and MSAG (`iv_prefetch_buffer_msag`)
batch buffers to the RFC call - `dispatch_batch`'s method SIGNATURE
still declares both `OPTIONAL` parameters (with their original ABAP Doc
comments), but the method BODY's real RFC `CALL FUNCTION
'Z_ABAPGIT_ORTEC_SER_BATCH'` `EXPORTING` list no longer references
either one, and `before_dispatch` no longer computes either local
variable at all. No comment or commit-message rationale documents this
removal - it is not connected to either of the two named test reworks
above (neither test touches DD/OO_BATCH/MSAG buffer wiring), and no
test would catch it (a missing OPTIONAL xstring parameter silently
defaults to empty, which is indistinguishable from a normal prefetch
MISS). This is a genuine, silent regression of SER-SLICE-3's own
CLAS/INTF and MSAG batch-provider wiring - both providers currently
transmit ZERO bytes to their RFC workers today, at the authoritative
`8c9e5df4` baseline, regardless of this slice's own work.

**Disposition**: since the current SER-SLICE-4 mission explicitly
requires "`BEFORE_DISPATCH` must sum the actual bytes of every active
provider buffer, including the existing and new buffers: DOMA/DTEL,
CLAS/INTF, MSAG, TABL, PROG, FUGR", Phase 2 (shared prerequisite)
RESTORES the dropped `lv_prefetch_buffer_oo_batch`/
`lv_prefetch_buffer_msag` computation and RFC-forwarding as an explicit,
disclosed part of the aggregate-byte-admission implementation - not as
unrelated, unauthorized scope creep, but as a direct, load-bearing
precondition for the literal instruction to sum "every active provider
buffer including CLAS/INTF and MSAG." See
`.memory/logs/serialization_slice_4_it8_validation_plan.md` for the
required regression coverage proving CLAS/INTF/MSAG batch behavior is
restored to its pre-regression, previously-IT8-approved shape.
