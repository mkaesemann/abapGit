# SER-SLICE-2 — Preflight, OD-14 Audit, Owner Object-Creation Checklist

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SERIALIZATION_SER_SLICE_2
STATUS=NOT_STARTED
REASON=Every SER-SLICE-2 global object is missing (confirmed via
  file_search — zero results for ZCL_ABAPGIT_ORTEC_SER_*, ZABAPGIT_ORTEC_
  SERIAL*, Z_ABAPGIT_ORTEC_SER*, ZAOG_SER*, ZIF_ABAPGIT_ORTEC_SER*). Per
  this task's Phase A, these must be created manually by the owner — this
  agent does not create global repository objects automatically.
PRODUCTIVE_CODE_CHANGED=NO
STATE_MD_CHANGED=NO
COMMITS_CREATED=NONE (nothing to commit for Slice 2 itself this pass)
PUSHED=NO
```

## Completed this pass (safe, achievable without the missing objects)

```text
OD14_STATIC_STATE_AUDIT=PASS — see
  .memory/logs/serialization_slice_2_od14_audit.md for full detail.
  Summary: no unmitigated unsafe cross-object state found for the CLAS/
  INTF/generic-prototype path. Two disclosed residuals (customer BAdI
  exit instance-state assumption; pre-existing CL_OO_* kernel buffering,
  DR-002, already on record) — neither blocks implementation. One
  confirmed-but-not-yet-implementable requirement: the future planner
  MUST explicitly exclude object_type='WAPA' from batch eligibility.
LUW/aRFC_CONTRACT=RE-CONFIRMED, no change from the already-approved
  design (performance_design.md §3a, adaptive_batch_design.md OD-13).
```

## OWNER_ACTION_REQUIRED — exact creation checklist

```text
OWNER_ACTION_REQUIRED=CREATE_GLOBAL_OBJECTS
```

All names verified `<=30` characters (per the bootstrap's own creation
manifest, re-confirmed here). Exact signatures/field lists are in the
cited design sections — the owner creates the OBJECT SHELLS (correct
type, package, and DDIC field lists where applicable); this agent will
then fill in all method/function bodies via normal file edits once the
objects exist.

| STEP | PACKAGE | PARENT_PACKAGE | OBJECT_TYPE | OBJECT_NAME | DESCRIPTION | DEPENDENCIES | CREATE_BEFORE_STEP |
|---|---|---|---|---|---|---|---|
| 1 | `$ABAPGIT_ORTEC_SERIAL` | `$ABAPGIT_ORTEC` (confirmed live parent of the existing `$ABAPGIT_ORTEC_GIT` sibling) | DEVC | n/a | Root package for new serialization-performance work | none | — |
| 2 | `$ABAPGIT_ORTEC_SERIAL_CORE` | `$ABAPGIT_ORTEC_SERIAL` | DEVC | n/a | Planner, cost model, orchestrator, batch DDIC | step 1 | 1 |
| 3 | `$ABAPGIT_ORTEC_SERIAL_RFC` | `$ABAPGIT_ORTEC_SERIAL` | DEVC | n/a | Batch RFC function group | step 1 | 1 |
| 4 | `$ABAPGIT_ORTEC_SERIAL_CORE` | — | TABL (structure) | `ZAOG_SER_BATCH_RESULT` | One row per batch object result — fields: `OBJ_TYPE TROBJTYPE`, `OBJ_NAME SOBJ_NAME`, `RC I`, `FILES_XSTRING XSTRING`, `MSGID SY-MSGID`, `MSGNO SY-MSGNO`, `MSGV1..MSGV4 SY-MSGV1..4`, `ELAPSED_MS I`, `OUTPUT_BYTES I`, `OUTPUT_FILE_COUNT I`, `PROVIDER_HIT I`, `PROVIDER_MISS I`, `PROVIDER_FALLBACK I` (see adaptive_batch_design.md §2) | step 2 | 2 |
| 5 | `$ABAPGIT_ORTEC_SERIAL_CORE` | — | TTYP (table type) | `ZAOG_SER_BATCH_RESULT_TT` | `STANDARD TABLE OF ZAOG_SER_BATCH_RESULT WITH EMPTY KEY` | step 4 | 4 |
| 6 | `$ABAPGIT_ORTEC_SERIAL_CORE` | — | CLAS | `ZCL_ABAPGIT_ORTEC_SER_COST` | Run-local EWMA cost estimator, type-family static defaults (adaptive_batch_design.md §6) | none | 2 |
| 7 | `$ABAPGIT_ORTEC_SERIAL_CORE` | — | CLAS | `ZCL_ABAPGIT_ORTEC_SER_PLANNER` | LPT-first batch builder + guided-self-scheduling refill; MUST exclude object_type='WAPA' from eligibility (OD-14 audit) | step 6 | 6 |
| 8 | `$ABAPGIT_ORTEC_SERIAL_CORE` | — | CLAS | `ZCL_ABAPGIT_ORTEC_SER_PROV_GEN` | Trivial no-op generic provider (last-resort match) | none | 2 |
| 9 | `$ABAPGIT_ORTEC_SERIAL_CORE` | — | CLAS | `ZCL_ABAPGIT_ORTEC_SER_ORCH` | Orchestrator: static run registry (OD-13), partitions objects, drives planner/dispatch/callback lifecycle | steps 4,5,6,7,8 (and step 11, for the FM it dispatches to) | 8 |
| 10 | `$ABAPGIT_ORTEC_SERIAL_RFC` | — | FUGR (function group) | `ZABAPGIT_ORTEC_SERIAL` | Hosts the new batch RFC function module | step 3 | 3 |
| 11 | `$ABAPGIT_ORTEC_SERIAL_RFC` | — | FUNC (RFC-enabled, in FUGR from step 10) | `Z_ABAPGIT_ORTEC_SER_BATCH` | Multi-object batch serialization worker — signature in adaptive_batch_design.md §2 (`IT_TADIR`, `IV_BATCH_ID`, `IV_ATTEMPT`, prefetch buffers, `IV_ABAP_LANGUAGE_VERS`, `IV_LANGUAGE`, `IV_PATH`, i18n flags → `ET_RESULT TYPE ZAOG_SER_BATCH_RESULT_TT`, `EV_OUTPUT_ROW_COUNT`, `EXCEPTIONS ERROR`) | steps 4,5,10 | 10 |

No new interface, no new exception class, no new statistics table for
SER-SLICE-2 (confirmed against the bootstrap's own creation manifest —
`ZIF_ABAPGIT_ORTEC_SER_PROV` and `ZCL_ABAPGIT_ORTEC_SER_PROV_DD` are
SER-SLICE-3 items, not created or needed now).

## What happens once the owner confirms creation

Resume directly at Phase C2 (minimal standard-abapGit hook) through C11
(checkpoint commits), per the already-approved design
(`serialization_adaptive_batch_design.md`, `serialization_performance_
design.md` §2-3) — no further design decisions are required; SER-SLICE-1
already made SER-3's DOMA precondition decision-free, and OD-14 already
cleared the static-state gate. Do not re-run discovery/archaeology/design
for SER-SLICE-2 — proceed straight to implementation once the objects
exist.

## SER-SLICE-2 status summary

```text
SLICE_2_STATUS=NOT_STARTED
OD14_STATIC_STATE_AUDIT=PASS
RUN_REGISTRY=NOT_STARTED
BATCH_RFC=NOT_STARTED
ADAPTIVE_PLANNER=NOT_STARTED
WAPA_PATH=UNCHANGED_EXCLUDED (structurally already true today; planner-
  level explicit exclusion is a confirmed C6 requirement for when the
  planner is implemented)
```
