# Regression validation: Variant B Slice 2A/2B

## Scope

- explicit fetch-mode model;
- pure request serializer;
- capability parser;
- unsupported-capability exception extensions;
- class-local ABAP Unit tests.

## Repository commits

- implementation/checkpoint commit: `6638cb6683e9b508b2e9144dfb9c80628ec23c98`;
- SAP correction/fix commit (test method names): `96536177393e7d6650fdfe4487e8fa7f17ad4044`;
- local fix (uncommitted, pending IT8 re-import): undeclared/unhandled
  `ZCX_ABAPGIT_EXCEPTION` ATC findings in `BUILD_REQUEST`, `BUILD_WANT_LINES`,
  `BUILD_HAVE_LINES` - see "ATC finding fix" section below.

## IT8 validation

- import: PASS;
- productive-class activation: PASS;
- exception-class activation: PASS;
- testclasses include activation: PASS;
- ABAP Unit execution: PASS;
- test method names <= 30 characters: PASS after fix commit;
- ATC: NOT_EXECUTED;
- productive live fetch behavior: NOT_TESTED_NOT_APPLICABLE because no productive caller is migrated in 2A/2B.

## Regression assertions

- five explicit fetch modes exist: PASS (current source contains the five mode constants in `zcl_abapgit_ortec_fetch_req`).
- request serializer performs no SQL: PASS (current source contains no SQL statements in the serializer implementation).
- request serializer performs no HTTP: PASS (current source contains no HTTP-client call or HTTP dependency in the serializer implementation).
- no productive ORTEC caller uses the serializer yet: PASS (strict 2A/2B scope; productive callers remain unchanged).
- serializer does not call `fetch_tip_commits`: PASS (current source has no such call).
- serializer does not call `build_upload_pack_buffer`: PASS (current source has no such call).
- no Variant B mode emits `deepen`: PASS (current source and tests assert no `deepen` token is emitted).
- required and forbidden wire tokens are covered by ABAP Unit tests: PASS (tests cover `want`, `have`, `filter`, `done`, and the absence of `deepen`/`shallow`).
- unsupported-capability exception additions remain backward compatible: PASS (constructor additions are defaulted and existing call sites remain unchanged).
- strict Slice 3 boundary remains intact: PASS (no productive caller migration in 2A/2B).

## ATC finding fix (local, pending IT8 re-run)

IT8 ATC reported undeclared/unhandled `ZCX_ABAPGIT_EXCEPTION` in
`ZCL_ABAPGIT_ORTEC_FETCH_REQ=>BUILD_REQUEST` (~line 145, the direct
`ZCL_ABAPGIT_GIT_UTILS=>PKT_STRING` call for the `filter blob:none` line),
`=>BUILD_WANT_LINES` (~line 286), and `=>BUILD_HAVE_LINES` (~line 297).

- Root cause: `ZCL_ABAPGIT_GIT_UTILS=>PKT_STRING` declares `RAISING
  zcx_abapgit_exception` (a `CX_STATIC_CHECK` subclass, checked exception).
  `zcx_abapgit_ortec_git` is a sibling class, also directly extending
  `CX_STATIC_CHECK`, not a superclass of `zcx_abapgit_exception` - declaring
  it in `BUILD_REQUEST`'s `RAISING` clause does not satisfy the checked-
  exception requirement for calls to `PKT_STRING`.
- Fix: `BUILD_WANT_LINES`/`BUILD_HAVE_LINES` (private) now declare `RAISING
  zcx_abapgit_exception` and propagate it unchanged. `BUILD_REQUEST`'s
  existing mode `CASE` body is wrapped in `TRY...CATCH zcx_abapgit_exception`
  and re-raises `zcx_abapgit_ortec_git` with the caught exception preserved
  as `previous` - `BUILD_REQUEST`'s public `RAISING zcx_abapgit_ortec_git`
  contract (Slice 2 design §2.2) is unchanged. Existing
  `zcx_abapgit_ortec_git` raises (`validate_single_want`,
  `raise_unsupported_capability`, `WHEN OTHERS`) are unaffected - the new
  `CATCH` only intercepts `zcx_abapgit_exception`, a sibling, non-subclass
  type.
- Scope: `src/ortec/git/zcl_abapgit_ortec_fetch_req.clas.abap` only. No
  productive caller exists yet (grep-confirmed: only
  `zcl_abapgit_ortec_fetch_req.clas.testclasses.abap` calls `BUILD_REQUEST`);
  no wire-format or validation behavior changed.
- Tests: none added. `PKT_STRING`'s `>= 255`-character guard is unreachable
  via `BUILD_REQUEST`'s public API given the fixed `ty_sha1 TYPE c LENGTH 40`
  type and the short, fixed capability tokens used in every mode - every
  built line stays far under 255 characters by construction. Triggering it
  would require testing the private helpers directly or violating the
  `ty_sha1` type contract, both out of scope per the fix instructions. The
  existing 18 `ltcl_fetch_req` tests re-exercise the (now `TRY`-wrapped)
  `BUILD_REQUEST` body for all 5 modes unchanged.
- Static check: `get_errors` (ADT-based) on both the main class and its
  testclasses include: 0 errors. Local `npx abaplint` (root `v702` profile)
  shows pre-existing, untouched findings in the testclasses file (inline
  `DATA(...)` declarations, not permitted under the classic-abapGit `v702`
  portability profile) - confirmed unrelated to this fix (that file was not
  edited) and out of scope.
- IT8 status: fixed locally; NOT yet re-imported/re-activated/re-ATC'd on
  IT8. Re-import, re-run ATC (expect the three findings resolved), and
  re-run `ltcl_fetch_req` ABAP Unit (expect all 18 tests still PASS) are
  required before this fix can be marked SAP-validated.

## Verdict

PASS (structural/static; ATC-finding fix is local-only and requires IT8
re-import, re-ATC, and re-run of ABAP Unit before SAP validation is
complete for this fix).
