# SER-SLICE-0 — Regression Pinning Tests (Local Completion Log)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SERIALIZATION_SER_SLICE_0
STATUS=LOCAL_COMPLETE (bulk-exists), PARTIAL/BLOCKED (WAPA)
DEPENDS_ON=.memory/handoffs/serialization-design-bootstrap.md (SLICE 0),
  .memory/logs/serialization_bulk_exists_design.md (T-1/T-2/T-3),
  .memory/logs/serialization_wapa_review.md (T-WAPA-1..5)
PRODUCTIVE_CODE_CHANGED=NO
STATE_MD_CHANGED=NO
```

## Scope actually implemented

```text
IMPLEMENTED_TEST_IDS
  T-1  -> t1_exists_doma, t1_exists_dtel, t1_exists_clas, t1_exists_intf,
          t1_order_preserved
  T-2  -> t2_absent_doma, t2_absent_dtel, t2_absent_clas, t2_absent_intf
  T-3  -> t3_clas_sadl_generated_excl, t3_intf_proxy_generated_excl
  T-WAPA-1 -> t_wapa_1_active_only, t_wapa_1_inactive_only, t_wapa_1_neither

NOT_IMPLEMENTED (BLOCKED_MISSING_PRODUCTION_SEAM, reported not silently
  skipped)
  T-WAPA-2, T-WAPA-3, T-WAPA-4, T-WAPA-5
```

## Deliberate deviation from the literal design text (T-1/T-2/T-3)

`serialization_bulk_exists_design.md`'s literal wording routes T-1/T-2
through `zcl_abapgit_tadir~read( iv_check_exists = abap_true )` against a
real package. Two hard blockers make that impractical for a deterministic,
repository-independent unit test:

1. `zcl_abapgit_tadir` is `CREATE PRIVATE` with `GLOBAL FRIENDS
   zcl_abapgit_factory` — no direct instantiation from a local test class.
2. `zif_abapgit_tadir~read` unconditionally calls `build()`, which scans a
   REAL package's TADIR content from the database (`ASSERT iv_package IS
   NOT INITIAL`) — this is exactly "repository-specific unstable data",
   explicitly forbidden by this slice's own instructions.

**Substitution used instead:** all three tests call
`ZCL_ABAPGIT_ORTEC_BULK_EXISTS=>filter_existing` DIRECTLY — this is the
actual method whose behavior T-1/T-2/T-3 are pinning, a plain `PUBLIC
CLASS-METHOD` taking `it_tadir` as an ordinary importing parameter (no
package scan involved). Synthetic, deterministic TADIR fixtures are built
in-test, and the underlying DDIC tables the method itself queries
(`DD01L`, `DD04L`, `SEOCLASSDF`, `VSEOEXTEND`, `SPROXHDR`) are doubled via
`CL_OSQL_TEST_ENVIRONMENT` (the proven pattern already used elsewhere in
this workspace, e.g. `zcl_abapgit_ortec_mat_state.clas.testclasses.abap`).

This achieves the exact documented semantics (bulk classification agrees
with existence, deleted/never-existed exclusion, CHDO/SADL/proxy-generated
exclusion) with fully deterministic data, and is a MORE precise unit-level
pin than driving the same assertion through the much heavier
`zcl_abapgit_tadir` package-scan stack. Independently reviewed and
confirmed faithful/non-scope-broadening (see Regression review below).

**UNKNOWN/`ev_success = abap_false` fallback branch:** every `WHEN` branch
in `filter_existing`'s `CASE ls_tadir-object` statement follows the
pattern `IF lv_x_success = abap_false. IF exists_standard(...) = abap_true.
APPEND... ENDIF. ELSEIF line_exists(...). APPEND... ENDIF.` — i.e. on a
bulk-buffer read failure, the decision defers ENTIRELY to the standard
per-object check (`exists_standard` -> `zcl_abapgit_objects=>exists`),
never independently concluding NOT_EXISTS. This satisfies "the optimized
path does not incorrectly classify an object as absent when the handler
cannot provide a complete answer" by direct source inspection (verified,
re-confirmed by regression review), but is NOT exercised by an automated
test in this slice: `CL_OSQL_TEST_ENVIRONMENT` doubles a table
successfully by design and has no supported mechanism to deterministically
force a genuine `cx_root`-raising Open SQL failure inside the `TRY...CATCH`
that guards each `build_*_buffer` method. Forcing this branch would need
either live DB-level fault injection (non-deterministic, unavailable) or a
production hook (forbidden in this slice). Disclosed here rather than
silently omitted or faked with a tautological test.

## WAPA — T-WAPA-1 implemented, T-WAPA-2..5 BLOCKED

`ZCL_ABAPGIT_ORTEC_WAPA=>exists()` is a single, self-contained `SELECT
SINGLE` against the transparent table `O2APPL`, with NO call into
`CL_O2_API_APPLICATION`/`CL_O2_API_PAGES` — fully testable, implemented as
T-WAPA-1 (active-only / inactive-only / neither, matching the legacy
"active OR inactive" `load()` contract).

T-WAPA-2..5 all require calling `serialize()`, which:

1. Unconditionally calls `CL_O2_API_APPLICATION=>LOAD` and
   `CL_O2_API_PAGES=>GET_ALL_PAGES`/`GET_MASTER_LANGUAGE` — hardcoded
   static calls to concrete SAP standard classes with NO interface or
   injection seam in `ZCL_ABAPGIT_ORTEC_WAPA` to substitute a test double.
   ABAP has no mechanism to intercept a statically-addressed class method
   call from within the calling code without either a production-side
   injection point (forbidden: "do not change productive WAPA behavior in
   this slice") or real, live BSP application data for the API to load.
2. Page content (`add_page_content_file`) reads via `IMPORT ... FROM
   DATABASE o2pagcon(tr) ID ls_pagecon_key` — the productive source's own
   comment confirms `O2PAGCON is a cluster/pool table`. `CL_OSQL_TEST_
   ENVIRONMENT` only intercepts Open SQL (`SELECT`/`INSERT`/`UPDATE`/
   `DELETE`/`MODIFY`), never the `IMPORT`/`EXPORT ... FROM/TO DATABASE`
   cluster access path — there is no supported way to double this table
   with the pattern already established and proven elsewhere in this
   workspace.
3. The only alternative — running these tests against real, existing BSP
   application/page content in the target system — would make the tests
   depend on repository-specific, unstable data, explicitly forbidden by
   this slice's own instructions.

**Conclusion: BLOCKED_MISSING_PRODUCTION_SEAM for T-WAPA-2..5**, not a
silently-skipped gap. No production architecture change was made or
proposed to work around this, per the explicit boundary for this slice.
Independently re-verified against the productive source by regression
review (see below) — confirmed technically sound, not an excuse to skip
achievable work.

## Local validation performed

```text
git diff --check                 PASS (only CRLF-normalization notices,
                                  no real whitespace/conflict-marker
                                  errors)
changed-file inspection          PASS (git status shows exactly 4 files:
                                  2 new testclasses includes, 2 modified
                                  .clas.xml files with WITH_UNIT_TESTS
                                  added; both productive .clas.abap files
                                  untouched)
ABAP method-name length check    PASS (longest new method name is 28
                                  chars: t3_intf_proxy_generated_excl;
                                  full list checked via PowerShell
                                  Select-String, all <=30)
local syntax/error diagnostics   PASS (get_errors: no errors on either new
                                  testclasses file or either modified XML)
```

No IT8 activation, ABAP Unit run, or ATC check was performed or is claimed
here — none of these are available in this workspace (per repo memory:
the connected live SAP diagnostic tools do not point at this repo's target
IT8 system). **IT8 validation is still required** before this slice is
considered DONE per the design's own SLICE 0 `SAP_VALIDATION` requirement.

## Regression review (delegated, independent)

```text
REVIEWER=ortec-abapgit-regression
TESTS_MEANINGFUL=YES
DEVIATION_ACCEPTABLE=YES
WAPA_BLOCKED_CLAIM_SOUND=YES
PRODUCTIVE_UNCHANGED=YES
NO_UNSTABLE_DATA=YES
METHOD_NAMES_OK=YES
NO_FORBIDDEN_OBJECTS=YES
CONVENTION_MATCH=YES
ISSUES_FOUND=0
VERDICT=PASS
```

## Changed files

```text
src/ortec/zcl_abapgit_ortec_bulk_exists.clas.testclasses.abap  (NEW)
src/ortec/zcl_abapgit_ortec_bulk_exists.clas.xml                (MODIFIED: + <WITH_UNIT_TESTS>X</WITH_UNIT_TESTS>)
src/ortec/zcl_abapgit_ortec_wapa.clas.testclasses.abap          (NEW)
src/ortec/zcl_abapgit_ortec_wapa.clas.xml                       (MODIFIED: + <WITH_UNIT_TESTS>X</WITH_UNIT_TESTS>)
```

No productive `.clas.abap` file was changed. No new package, class,
interface, DDIC object, function group, or function module was created.

## Next action for the owner

1. Import this commit into IT8; run ABAP Unit for
   `ZCL_ABAPGIT_ORTEC_BULK_EXISTS` and `ZCL_ABAPGIT_ORTEC_WAPA` and confirm
   all listed test methods PASS (they are pinning tests: a failure would
   mean this design's own understanding of current behavior is wrong
   somewhere and must be re-verified before any later SER slice proceeds,
   per SLICE 0's own STOP_CONDITIONS).
2. Run ATC on both classes.
3. Decide whether T-WAPA-2..5 remain permanently out of scope for
   automated pinning (accepting the documented residual risk) or whether a
   future, separately-authorized slice should add a production seam
   (e.g. an injectable factory for the BSP API dependency) purely to make
   these testable — NOT decided or designed here, per this slice's
   explicit boundary against WAPA architecture changes.
