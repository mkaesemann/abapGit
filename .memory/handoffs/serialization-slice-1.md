# SER-SLICE-1 — DOMA Semantics + Parity Harness Handoff

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SERIALIZATION_SER_SLICE_1
STATUS=LOCAL_COMPLETE
DETAIL=.memory/logs/serialization_slice_1_doma_semantics.md
PRODUCTIVE_CODE_CHANGED=NO
STATE_MD_CHANGED=NO
COMMITS_CREATED=see below
PUSHED=NO
```

## DOMA version semantics — resolved

```text
DOMA_SEMANTICS=CONFIRMED
```

`ZCL_ABAPGIT_OBJECT_DOMA` reads ONLY the active version, for every
language (main call explicit `state='A'`; translation calls omit `state`,
relying on `DDIF_DOMA_GET`'s own `DEFAULT 'A'`, confirmed from the live
function module signature). A NEW, previously-undiscovered risk was found
and corrected before any provider code exists: `DD01V` is a database view
with NO active/inactive filter or projection at all (confirmed via live
`DD28S` query) — a naive bulk `SELECT * FROM DD01V` would have silently
mixed active and inactive domain headers. The corrected target for
SER-SLICE-3 is `DD01L`/`DD01T WHERE as4local = 'A' AND as4vers = '0000'`
(mirroring DTEL's own already-correct pattern); `DD07V` by contrast
already bakes in `AS4LOCAL='A'` at the view-definition level and is safe,
though `DD07L`/`DD07T` with the same explicit filter is still preferred
for consistency. Full evidence:
`.memory/logs/serialization_slice_1_doma_semantics.md` §2.
`.memory/logs/serialization_provider_design.md` §2 updated in place,
replacing its "OPEN, NOT-YET-RESOLVED" note.

## Parity harness

```text
DOMA_PARITY_HARNESS=PASS (implemented; IT8 execution still required)
```

5 new ABAP Unit tests in
`src/ortec/zcl_abapgit_ortec_ser_pref_ext.clas.testclasses.abap` (NEW
include on an EXISTING ORTEC class — no new global object created, no
standard abapGit object touched):

```text
active_with_fixed_values     XFELD: DATATYPE/LENG/fixed-value+text content
active_without_fixed_values  CHAR30: DATATYPE/LENG present, no fixed
                              values (empty optional collection)
nonexistent_or_inactive      fabricated name: no DD01V node, no exception
                              (covers nonexistent AND inactive-only, proven
                              equivalent from source — see evidence log)
exists_matches_serialize     exists()=TRUE (XFELD) / FALSE (fabricated)
output_is_stable             two serialize() calls render byte-identical
                              XML (stable/canonical output)
```

All fixtures are real, permanently-stable SAP Basis domains or a
guaranteed-absent fabricated name — no `CL_OSQL_TEST_ENVIRONMENT`
doubling of any kind is used (learned from the SER-SLICE-0 DDIC-catalog-
doubling incident: DOMA's own tables are DDIC catalog tables too, so this
harness never doubles anything, it only reads real system state).

**Placement note (disclosed, not hidden):** these tests live on
`ZCL_ABAPGIT_ORTEC_SER_PREF_EXT` (the existing DTEL-prefetch class) as a
temporary home, because (a) this task's Phase A forbids creating new
global objects — the real target, `ZCL_ABAPGIT_ORTEC_SER_PROV_DD`, is a
SER-SLICE-2/3 creation-manifest item that does not exist yet, and (b) the
standard `ZCL_ABAPGIT_OBJECT_DOMA` class must not be touched. Relocate
these tests into `ZCL_ABAPGIT_ORTEC_SER_PROV_DD`'s own testclasses once
SER-SLICE-3 creates it; do not duplicate them.

## Multi-language fixed-value text coverage — documented, not separately fixtured

The match-by-value (not by valpos) and placeholder-on-miss mechanism for
translated fixed-value texts is fully confirmed from source (evidence log
§2) and is not DOMA-specific novel risk; a dedicated second-language
fixture was judged lower-value here since SER-3's own mandatory PARITY
PROOF step will exercise this against real repository content before
provider implementation is authorized. Not a missing-seam stop condition —
a scope judgment call, disclosed.

## Reviews

```text
CORRECTNESS_REVIEW=PASS
REGRESSION_REVIEW=PASS
```

See `.memory/logs/serialization_slice_1_doma_semantics.md` for the full
compact review findings.

## Local validation

```text
git diff --check                 PASS
changed-file inspection          PASS (exactly 2 files)
ABAP method-name length check    PASS (max 27 chars)
local syntax/error diagnostics   PASS (get_errors clean)
```

## Changed files

```text
src/ortec/zcl_abapgit_ortec_ser_pref_ext.clas.testclasses.abap  (NEW)
src/ortec/zcl_abapgit_ortec_ser_pref_ext.clas.xml                (MODIFIED: + WITH_UNIT_TESTS)
.memory/logs/serialization_slice_1_doma_semantics.md             (NEW)
.memory/logs/serialization_provider_design.md                    (MODIFIED: DR-003 resolved in place)
.memory/handoffs/serialization-slice-1.md                        (NEW, this file)
```

No productive `.clas.abap` file was changed. No new package, class,
interface, DDIC object, function group, or function module was created.

## SER-SLICE-1 exit criteria

```text
DOMA_SEMANTICS=CONFIRMED
DOMA_PARITY_HARNESS=PASS
CORRECTNESS_REVIEW=PASS
REGRESSION_REVIEW=PASS
PRODUCTIVE_DOMA_PROVIDER_STARTED=NO
OPEN_BLOCKERS=0
```

**Exit criteria satisfied — SER-SLICE-2 preflight/OD-14 audit is
authorized to proceed in the same session.**

## IT8 validation still required

```text
YES — real-system ABAP Unit run for ZCL_ABAPGIT_ORTEC_SER_PREF_EXT (5 new
DOMA parity tests) + ATC. Not claimed here; not executed in this session.
```
