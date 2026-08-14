# WAPA Option 1 Performance Repair Handoff

```text
TASK_ID=WAPA_OPTION1_PERFORMANCE_REPAIR_20260813
STATUS=BLOCKED_OWNER_REVIEW_REQUIRED
BASELINE=IT8 active CLAS ZCL_ABAPGIT_ORTEC_WAPA after Option 1 activation 2026-08-13T20:48
SOURCE_INSPECTED=active main and testclasses
PRODUCTIVE_EDITS=NONE
TEST_EDITS=NONE

BLOCKER=The only allowed normative design states DESIGN_GATE=READY_FOR_CORRECTNESS_AND_PERFORMANCE_RE_REVIEW and IMPLEMENTATION_ALLOWED=NO_UNTIL_OWNER_REVIEW. Its header likewise reports STATUS=DESIGN_COMPLETE_AWAITING_CORRECTNESS_AND_PERFORMANCE_RE_REVIEW. This does not satisfy the implementation-agent requirement for an APPROVE or APPROVE_WITH_MINOR_REVISIONS verdict.

CURRENT_SOURCE_EVIDENCE=
- PS-001/PS-002: RAW_PREFETCH_AND_READ -> DECODE_RAW_PAGE -> ASSEMBLE_AND_DECODE copies all page rows then copies/sorts all supplied rows; ASSEMBLE_AND_DECODE grows logical-key buffers with repeated CONCATENATE in the physical-row loop.
- PS-003: DECODE_RAW_PAGE appends current-page rows before deleting them from CT_ROWS.
- PS-004: READ_RAW_MANIFEST inserts directly into a unique key and validates only negative SRTF2 / CLUSTR bounds; it lacks mandatory PAGE and sequence/duplicate anomaly rejection.
- PS-005: SERIALIZE classifies every 1,000-page group independently, so a multi-group request can increment both hits and fallbacks; counters are non-saturating.

PROPOSED_APPROVED_SLICE_AFTER_OWNER_VERDICT=
- main: TY_RAW_MANIFEST/TT and TY_RAW_ROW_TT; counter saturation; READ_RAW_MANIFEST; READ_RAW_ROWS; DECODE_RAW_PAGE; RAW_PREFETCH_AND_READ; SERIALIZE_REFERENCE_RANGE; SERIALIZE; minimal raw-map reads in ADD_PAGE_CONTENT_FILE and ADD_FULL_PAGE_DETAILS.
- testclasses: focused manifest-anomaly, consuming-decoder, depth-ceiling/reference, exact request-counter, and saturation tests.
- no changes: ORCH, BATCH, RFC, DDIC, standard abapGit, state, diagrams.

PERFORMANCE_GATE=
SQL: healthy admitted range=3 raw SELECTs; rejected node=1 metadata SELECT; terminal reference=0 raw-helper SELECTs; maximum=127 raw-helper SELECTs per initial 1,000-page group.
HTTP: zero new HTTP/aRFC calls.
LIMITS: metadata probe<=40001 rows; admitted payload<=36333 rows and <=104857600 charged bytes; fragment<=2886 bytes; decoded page<=15728640 bytes; split depth<=5.
LIFETIME: one admitted raw range plus one decoded page, one logical-key buffer, and one IMPORT transient; no full raw-table copy.
TRANSACTION=outer worker remains owner; no COMMIT/ROLLBACK.

VALIDATION_EXECUTED=none; no productive source changed.
NEXT_ACTION=Obtain an explicit owner/design-review verdict of APPROVE or APPROVE_WITH_MINOR_REVISIONS, then implement only this documented WAPA main/testclasses slice.
```