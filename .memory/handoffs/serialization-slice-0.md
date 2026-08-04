# SER-SLICE-0 — Pinning Tests Handoff

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SERIALIZATION_SER_SLICE_0
STATUS=IT8_VALIDATED_COMPLETE (bulk-exists fixed after a real IT8
  short-dump incident, then confirmed clean on IT8; WAPA partially
  pinned, T-WAPA-2..5 BLOCKED_MISSING_PRODUCTION_SEAM, reported not
  silently skipped)
DETAIL=.memory/logs/serialization_slice_0_regression.md
PRODUCTIVE_CODE_CHANGED=NO
STATE_MD_CHANGED=NO
COMMITS_CREATED=see below
PUSHED=NO
```

## IT8 incident and fix (2026-08-04)

The FIRST version of the bulk-exists tests doubled `DD01L`/`DD04L`/
`SEOCLASSDF` via `CL_OSQL_TEST_ENVIRONMENT`. On IT8 this reproducibly
short-dumped (`RAISE_EXCEPTION`/`NOT_FOUND` inside
`CL_ABAP_STRUCTDESCR=>GET_DDIC_FIELD_LIST`) on every run, because these
are DDIC CATALOG tables the ABAP runtime needs to resolve types for the
WHOLE session — they must never be doubled. Since the crashing test
method aborts the rest of that test class's run, the previous "only one
test failed" assumption was wrong and is retracted. Fixed by rewriting
the tests to use REAL, stable objects (`MANDT`, existing abapGit
CLAS/INTF) instead of fabricated catalog rows, and doubling only
`VSEOEXTEND`/`SPROXHDR` (plain content tables) for the SADL/proxy
exclusion pins. Full root cause and before/after detail:
`.memory/logs/serialization_slice_0_regression.md`.

## Implemented test IDs

```text
T-1        t1_exists_doma, t1_exists_dtel, t1_exists_clas, t1_exists_intf,
           t1_order_preserved
T-2        t2_absent_doma, t2_absent_dtel, t2_absent_clas, t2_absent_intf
T-3        t3_clas_sadl_generated_excl, t3_clas_sadl_control_incl,
           t3_intf_proxy_generated_excl, t3_intf_proxy_control_incl
T-WAPA-1   t_wapa_1_active_only, t_wapa_1_inactive_only, t_wapa_1_neither
```

## Target class/include

```text
ZCL_ABAPGIT_ORTEC_BULK_EXISTS  src/ortec/zcl_abapgit_ortec_bulk_exists.clas.testclasses.abap (NEW)
                                src/ortec/zcl_abapgit_ortec_bulk_exists.clas.xml (WITH_UNIT_TESTS added)
ZCL_ABAPGIT_ORTEC_WAPA         src/ortec/zcl_abapgit_ortec_wapa.clas.testclasses.abap (NEW)
                                src/ortec/zcl_abapgit_ortec_wapa.clas.xml (WITH_UNIT_TESTS added)
```

## Meaningful assertion summary

```text
t1_exists_*             A REAL, stable existing object per prototype type
                        is classified as existing by filter_existing:
                        MANDT (DOMA/DTEL, SAP Basis), ZCL_ABAPGIT_ORTEC_
                        BULK_EXISTS (CLAS), ZIF_ABAPGIT_DEFINITIONS
                        (INTF) - no DDIC catalog table is doubled.
t1_order_preserved      The same 4 real objects, mixed CLAS+DOMA+INTF+
                        DTEL input, returned in the EXACT original input
                        order.
t2_absent_*             An obviously-fake name is excluded for each of
                        the four prototype types (deleted/never-existed
                        exclusion) - real absence check, no doubling.
t3_clas_sadl_*          A REAL existing CLAS (ZCL_ABAPGIT_ORTEC_WAPA) is
                        excluded when a fabricated VSEOEXTEND join row
                        marks it SADL-generated; a control test proves
                        the same class is normally included without that
                        join row.
t3_intf_proxy_*         A REAL existing INTF (ZIF_ABAPGIT_TADIR) is
                        excluded when a fabricated SPROXHDR join row
                        marks it proxy-generated; a control test proves
                        the same interface is normally included without
                        that join row.
t_wapa_1_active_only    exists() = TRUE for an O2APPL row with only an
                        active ('A') version.
t_wapa_1_inactive_only  exists() = TRUE for an O2APPL row with only an
                        inactive ('I') version.
t_wapa_1_neither        exists() = FALSE when no O2APPL row exists at all.
```

Full rationale for the T-1/T-2/T-3 direct-class-call substitution and the
T-WAPA-2..5 blocker evidence: see
`.memory/logs/serialization_slice_0_regression.md`.

## Local validation result

```text
git diff --check                 PASS
changed-file inspection          PASS (only the bulk-exists testclasses
                                  file changed in the fix commit)
ABAP method-name length check    PASS (max 28 chars)
local syntax/error diagnostics   PASS (get_errors clean)
regression review (delegated)    PASS twice: once against the original
                                  (later found unsafe) design, once against
                                  the fixed design (0 issues both times -
                                  the DDIC-catalog-doubling defect was a
                                  real-system-only failure mode neither
                                  local review caught the first time)
IT8 (owner-executed)             FAIL, FAIL (reproducible) on the ORIGINAL
                                  design (t1_exists_clas short-dump) ->
                                  fix committed -> PASS, all 16 test
                                  methods clean, ATC clean (owner-confirmed
                                  2026-08-04)
```

## Changed files

```text
src/ortec/zcl_abapgit_ortec_bulk_exists.clas.testclasses.abap  (NEW, then FIXED after IT8 incident)
src/ortec/zcl_abapgit_ortec_bulk_exists.clas.xml                (MODIFIED)
src/ortec/zcl_abapgit_ortec_wapa.clas.testclasses.abap          (NEW, unchanged by the fix)
src/ortec/zcl_abapgit_ortec_wapa.clas.xml                       (MODIFIED)
.memory/logs/serialization_slice_0_regression.md                (NEW, then UPDATED with incident/fix)
.memory/handoffs/serialization-slice-0.md                       (NEW, then UPDATED with incident/fix, this file)
```

## IT8 validation

```text
COMPLETE — owner-confirmed 2026-08-04: "All unit test now run and are
clean." Covers all 13 ZCL_ABAPGIT_ORTEC_BULK_EXISTS methods (fixed test
file) and all 3 ZCL_ABAPGIT_ORTEC_WAPA methods (T-WAPA-1, unchanged by
the fix) = 16 total. ATC clean (no productive/ATC-relevant code changed
since the earlier clean ATC report). SER-SLICE-0 is DONE for its
actually-implemented scope; T-WAPA-2..5 remain a disclosed, reviewed
residual scope gap (BLOCKED_MISSING_PRODUCTION_SEAM), not an open defect
or a pending validation item.
```
