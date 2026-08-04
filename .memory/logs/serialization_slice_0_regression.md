# SER-SLICE-0 — Regression Pinning Tests (Local Completion Log)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SERIALIZATION_SER_SLICE_0
STATUS=LOCAL_COMPLETE (bulk-exists, IT8-INCIDENT-FIXED), PARTIAL/BLOCKED (WAPA)
DEPENDS_ON=.memory/handoffs/serialization-design-bootstrap.md (SLICE 0),
  .memory/logs/serialization_bulk_exists_design.md (T-1/T-2/T-3),
  .memory/logs/serialization_wapa_review.md (T-WAPA-1..5)
PRODUCTIVE_CODE_CHANGED=NO
STATE_MD_CHANGED=NO
```

## IT8 incident and fix (2026-08-04, after initial commit 78f48fc7)

A real IT8 ABAP Unit run short-dumped reproducibly (confirmed on 2
separate runs, not transient) with `RAISE_EXCEPTION`/`NOT_FOUND` on
`t1_exists_clas`, call stack:

```text
LTCL_BULK_EXISTS=>T1_EXISTS_CLAS
 -> CL_OSQL_TEST_ENVIRONMENT=>INSERT_TEST_DATA
  -> LCL_DATASOURCE_STUB=>IF_OSQL_STUB~INSERT
   -> CL_ABAP_STRUCTDESCR=>GET_DDIC_FIELD_LIST
    -> CALL FUNCTION 'DDIF_FIELDINFO_GET' TABNAME = REL_NAME ... sy-subrc <> 0 -> RAISE NOT_FOUND
```

**ROOT CAUSE (owner-supplied hypothesis, confirmed correct):** the original
test doubled `DD01L` (domains), `DD04L` (data elements), and `SEOCLASSDF`
(class/interface definitions) via `CL_OSQL_TEST_ENVIRONMENT`. These are
DDIC CATALOG tables the ABAP runtime itself depends on to resolve types
for the WHOLE session — including to load and describe the test class
itself, `CL_ABAP_UNIT_ASSERT`, and every other type touched during test
execution. Doubling them breaks the runtime's own type-resolution chain,
not just the productive method under test. `DD02L`/`DD03L` (the TABL
catalog) would carry the identical risk, though this slice never doubled
those. Because the FIRST test method to touch the doubled `SEOCLASSDF`
(alphabetically, `t1_exists_clas`) crashes with an uncaught runtime error
(not a normal exception), ABAP Unit does not continue running the REST of
that test class's methods — the original completion report's assumption
that "only one test failed, the others presumably passed" was WRONG and
has been retracted; no test in this class actually ran to completion on
either IT8 attempt.

**FIX:** the test file was rewritten to never double any DDIC catalog
table. See the corrected "Deliberate deviation" section below for the
full before/after design. Independently reviewed and confirmed sound (see
updated Regression review section).

## Scope actually implemented

```text
IMPLEMENTED_TEST_IDS
  T-1  -> t1_exists_doma, t1_exists_dtel, t1_exists_clas, t1_exists_intf,
          t1_order_preserved
  T-2  -> t2_absent_doma, t2_absent_dtel, t2_absent_clas, t2_absent_intf
  T-3  -> t3_clas_sadl_generated_excl, t3_clas_sadl_control_incl,
          t3_intf_proxy_generated_excl, t3_intf_proxy_control_incl
          (2 control tests ADDED during the IT8-incident fix, proving the
          exclusion is genuinely conditional on the fabricated join row)
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

**Substitution used instead:** all tests call
`ZCL_ABAPGIT_ORTEC_BULK_EXISTS=>filter_existing` DIRECTLY — this is the
actual method whose behavior T-1/T-2/T-3 are pinning, a plain `PUBLIC
CLASS-METHOD` taking `it_tadir` as an ordinary importing parameter (no
package scan involved).

**REVISED after the IT8 incident above — no DDIC catalog table is ever
doubled:**
- T-1 "exists" and T-2 "absent" checks for DOMA/DTEL/CLAS/INTF use REAL,
  stable objects instead of fabricated `DD01L`/`DD04L`/`SEOCLASSDF` rows:
  `MANDT` (the universal SAP Basis client-field domain AND data element —
  confirmed present via a live `SELECT` against `DD01L`/`DD04L` on IT8),
  and `ZCL_ABAPGIT_ORTEC_BULK_EXISTS`/`ZIF_ABAPGIT_DEFINITIONS` (existing
  abapGit classes/interfaces — confirmed present via a live `SELECT`
  against `SEOCLASSDF WHERE version = '1'` on IT8). T-2 absent cases use
  obviously-fake names (e.g. `ZZZZ_BEX_NOT_A_REAL_DOM`) with NO doubling
  at all — a real absence check against real system tables.
- T-3 (SADL/proxy exclusion) uses a REAL existing class/interface as the
  base object (`ZCL_ABAPGIT_ORTEC_WAPA`, `ZIF_ABAPGIT_TADIR` — also
  confirmed present via live `SEOCLASSDF` query) so the "exists" side is
  genuine, and fabricates ONLY the join row that TRIGGERS the exclusion —
  `VSEOEXTEND`/`SPROXHDR` — which are plain application content tables
  (class-extension relationships / proxy header registry), NOT part of
  the DDIC catalog/RTTI bootstrap chain, and therefore safe to double.
  Two new control tests (`t3_clas_sadl_control_incl`,
  `t3_intf_proxy_control_incl`) prove the same real object is normally
  INCLUDED when that join row is absent, so the exclusion tests are
  provably conditional, not vacuous.

Using `MANDT`/existing abapGit `CLAS`/`INTF` objects as fixtures is a
dependency on foundational, permanent SAP Basis / abapGit-shipped content
— not "repository-specific unstable data" in the sense the original
design forbade (that concern was about a customer's own arbitrary
package/repo content, not universal Basis fields or abapGit's own shipped
objects).

This achieves the exact documented semantics (bulk classification agrees
with existence, deleted/never-existed exclusion, CHDO/SADL/proxy-generated
exclusion) with fully deterministic data and zero DDIC-catalog doubling,
and is a MORE precise unit-level pin than driving the same assertion
through the much heavier `zcl_abapgit_tadir` package-scan stack.
Independently reviewed and confirmed faithful/non-scope-broadening both
before and after the IT8-incident fix (see Regression review below).

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
as executed BY THIS AGENT in this session prior to the fix — the owner
ran ABAP Unit on IT8 independently and reported the `t1_exists_clas`
failure analyzed above. ATC was reported clean by the owner. **IT8
re-validation of the FIXED test file is still required** before this
slice is considered DONE per the design's own SLICE 0 `SAP_VALIDATION`
requirement — the fix has only been reviewed statically and against live
read-only `SAPQuery` checks confirming the new fixtures
(`MANDT`/`ZCL_ABAPGIT_ORTEC_BULK_EXISTS`/`ZIF_ABAPGIT_DEFINITIONS`/
`ZCL_ABAPGIT_ORTEC_WAPA`/`ZIF_ABAPGIT_TADIR`) genuinely exist on IT8; the
rewritten tests themselves have NOT yet been executed on IT8.

## Regression review (delegated, independent)

First pass (pre-IT8, against the original DD01L/DD04L/SEOCLASSDF-doubling design):

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

**This PASS verdict did not catch the DDIC-catalog-doubling defect** — it
confirmed the tests were well-formed and followed this workspace's
established `CL_OSQL_TEST_ENVIRONMENT` convention, but neither the
implementing agent nor this review considered that `DD01L`/`DD04L`/
`SEOCLASSDF` specifically are unsafe to double (a real-system-only
failure mode, invisible to local static review). Recorded here so a
future reviewer knows this class of defect was previously missed.

Second pass (post-fix, against the MANDT/real-object rewrite):

```text
REVIEWER=ortec-abapgit-regression
NO_DDIC_CATALOG_DOUBLED=YES
JOIN_TABLES_SAFE=YES
TESTS_MEANINGFUL=YES
CONTROL_TESTS_VALUABLE=YES
MANDT_DUAL_USE_SAFE=YES
PRODUCTIVE_UNCHANGED=YES
METHOD_NAMES_OK=YES
FIXTURES_STABLE=YES
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

1. Import this fix commit into IT8; re-run ABAP Unit for
   `ZCL_ABAPGIT_ORTEC_BULK_EXISTS` (now 13 methods, including 2 new
   control tests) and `ZCL_ABAPGIT_ORTEC_WAPA` (unchanged, 3 methods) and
   confirm ALL listed test methods PASS this time, including
   `t1_exists_clas` and everything after it that never got a chance to run
   on the previous 2 attempts.
2. Run ATC on both classes again (was already clean before; should remain
   so, no productive/ATC-relevant code changed).
3. Decide whether T-WAPA-2..5 remain permanently out of scope for
   automated pinning (accepting the documented residual risk) or whether a
   future, separately-authorized slice should add a production seam
   (e.g. an injectable factory for the BSP API dependency) purely to make
   these testable — NOT decided or designed here, per this slice's
   explicit boundary against WAPA architecture changes.
