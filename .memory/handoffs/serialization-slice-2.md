# SER-SLICE-2 — Preflight, OD-14 Audit, Owner Object-Creation Checklist

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SERIALIZATION_SER_SLICE_2
STATUS=IMPLEMENTATION_AUDITED_AWAITING_IT8_FEATURE_ON_VALIDATION
SEE_ALSO=Minimal-hook restoration + independent audits section below
  (most current); this legacy STATUS block is Phase-2-local-only history.
REASON=Phase 2 (ORCH state machine, minimal standard-abapGit hook,
  partial T-DRAIN seam, unit tests for testable-without-RFC logic) is
  now fully implemented locally, on top of the already-owner-corrected
  and ATC-clean Phase 1 contracts. Before implementing ORCH, this agent
  re-verified sections 5.0-5.9 and the section 9 limits table against
  the ALREADY-committed Phase 1 ORCH contract and found 5 real
  implementation-contradiction gaps (reported and owner-authorized
  before any code was written, per the mandatory stop-and-report
  discipline): (1) missing C_STATE_ABANDONED ('X') lifecycle state
  required by the sect 5.3/5.4 poll-loop-termination design; (2) ~10
  missing sect 9 limit/retry constants; (3) no method for the sect 5.9
  actual-bytes admission/recursive-split check; (4) DISPATCH_BATCH could
  not receive/forward the 3 prefetch buffers; (5) [discovered while
  implementing, not pre-reported] no CLASS-DATA existed at all for a
  run's own accumulated output/log-sink/i18n-params/dispatch-sequence/
  EWMA-table/queue, which ON_END_OF_BATCH (a static aRFC callback with
  NO access to SERIALIZE()'s own local variables) absolutely requires -
  added MT_RUN_CONTEXT (TY_RUN_CONTEXT_TT), keyed by RUN_ID like every
  other table in this class, as the mechanically-necessary completion.
  All 5 gaps were additive only (no existing activated signature was
  broken) and are fully documented with unit/hard-bound/SER-SLICE-2-vs-
  deferred-scope ABAP Doc on every new declaration.
DISCOVERED_AND_FIXED_DURING_IMPLEMENTATION (not pre-known gaps, caught by
  live syntax dry-runs and a self-driven regression check):
  - RAISE EXCEPTION TYPE zcx_abapgit_exception EXPORTING iv_text = ... is
    NOT valid (that parameter does not exist on the constructor) - fixed
    to zcx_abapgit_exception=>raise( 'text' ).
  - Inline DATA(...) declarations are NOT allowed inside a
    RECEIVE RESULTS FROM FUNCTION IMPORTING clause - fixed to explicit
    DATA declarations before every RECEIVE.
  - ZCX_ABAPGIT_EXCEPTION raised deep inside ON_END_OF_BATCH/
    CHECK_TIMEOUTS (both unable to declare RAISING, since one is an aRFC
    callback with a runtime-fixed signature and the other's contract was
    already approved without RAISING) would have gone UNCAUGHT and
    dumped inside an aRFC callback - wrapped every reachable RAISING call
    in TRY/CATCH, logging via the run's own ii_log if bound, never
    propagating.
  - REAL REGRESSION CAUGHT BEFORE COMMIT: IS_NO_PARALLEL only denylists
    ECTC/ECTD - it does NOT cover WAPA. The initial partition logic would
    have silently routed WAPA into the batch-eligible pool, violating the
    OD-14 audit's explicit "WAPA is never batch-eligible" requirement.
    Fixed by adding an explicit `ls_tadir-object = 'WAPA'` check
    alongside IS_NO_PARALLEL. Verified WAPA's actual fallback path
    (ROUTE_TO_SEQUENTIAL_FALLBACK) is structurally identical to the
    standard RUN_SEQUENTIAL's own zcl_abapgit_objects=>serialize() call
    (read the real source to confirm), so routing WAPA there is not a
    regression.
DISCLOSED, NOT-YET-CLOSED LIMITATIONS (documented in code, not silent):
  - BEFORE_DISPATCH always passes INITIAL prefetch buffers - PREF/
    PREF_EXT/PREF_OO only expose EXTRACT_FOR_OBJECT (one object at a
    time); a real batch-scoped extraction method is a genuine new
    capability on those 3 EXISTING classes, out of this slice's
    authorized scope. Safe (falls back to per-object read, a normal
    prefetch miss) but the prefetch PERFORMANCE benefit does not yet
    apply to batches - candidate follow-up slice.
  - T-DRAIN seam is PARTIAL: Z_ABAPGIT_ORTEC_SER_BATCH got a new
    IV_TEST_DELAY_S (default 0, test-only) parameter and sleep, but
    C_BATCH_RFC_TIMEOUT_S/C_MAX_DRAIN_WAIT_S are hardcoded CONSTANTS with
    no test-time override mechanism - the design's own T-DRAIN-1..8 test
    sequence (sect 5.1b) needs this to avoid a 600s+ real wait per test
    case. NOT implemented this slice (would need yet another contract
    change); T-DRAIN-1..8 have NOT been executed. Per the design's own
    text this is validated "alongside the implementation, not before
    SLICE 2 code is written" - so this does not block Phase 2 code
    completion, but SLICE 2 cannot be called DONE until it is closed.
  - IV_ABAP_LANGUAGE_VERS is always passed as SPACE/initial to the batch
    worker (ORCH's SERIALIZE() has no parameter carrying a repo's custom
    ABAP language version, unlike the standard path's
    MO_ABAP_LANGUAGE_VERSION) - correct for the common case (no custom
    language version set) but a real, disclosed simplification.
  - PROVIDER_HIT/MISS/FALLBACK in ET_RESULT remain always 0 (already
    disclosed in the RFC worker's own Phase-2-slice-1 commit).
PRODUCTIVE_CODE_CHANGED=YES (ORCH contract completion + full state-
  machine bodies; minimal hook + IS_NO_PARALLEL visibility change in
  zcl_abapgit_serialize.clas.abap; new IS_SERIAL_BATCH_ACTIVE feature
  flag, default OFF, in zcl_abapgit_ortec_git_switch; T-DRAIN seam
  parameter on the RFC FM; new ORCH testclasses include)
STATE_MD_CHANGED=NO
PUSHED=NO
```

## Minimal hook restoration + independent audits (most current)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_2_MINIMAL_HOOK_AND_AUDIT
BASELINE_HEAD=d030412ac3410df7ccd9e1cc03ee2a72ece3eb0c
PHASE_0=Confirmed HEAD == BASELINE_HEAD (owner's own "Fixes for Unit
  Tests" commit already at HEAD, own repo push/pull cycle involved a
  rebase - commit MESSAGES match my prior session's report but SHAs
  differ, e.g. 9c9de24c now vs 25c815fe reported earlier - same content).
  Owner's d030412a fixed: (1) LOCAL FRIENDS needs a matching
  `CLASS ltcl_ser_orch DEFINITION DEFERRED.` in a NEW
  zcl_abapgit_ortec_ser_orch.clas.locals_imp.abap include (a real ABAP
  structural requirement, not previously known); (2) several ORCH test
  assertions rewritten from `line_exists( itab[ ... ] )` to
  `READ TABLE ... WITH TABLE KEY ... TRANSPORTING NO FIELDS` +
  `assert_subrc` - table-expression syntax on a FRIEND class's private
  static data appears unreliable/unsupported in that context (kept, not
  reverted - production code's OWN internal use of line_exists() on its
  own data is unaffected and untouched); (3) an UNRELATED standard-class
  test-expectation change in zcl_abapgit_serialize.clas.testclasses.abap
  (determine_max_processes expected values 1->9, 10->32) - classified
  UNRELATED_CHANGE, not touched by this task; (4) apostrophe XML-escaping
  fix (&apos;) and TXTLINES correction (47->38) in the FUGR DOKU, and
  deletion of a redundant SUSH short-text sub-object - all
  OWNER_SYNTAX_CORRECTIONs from real IT8 activation, not touched.
  Diffed zcl_abapgit_serialize.clas.abap against ada103d5 (the true
  pre-SER-SLICE-2 parent, confirmed via `git log --follow`): exactly two
  changes existed - IS_NO_PARALLEL visibility/staticness
  (UNNECESSARY_VISIBILITY_CHANGE) and the minimal hook block
  (REQUIRED_MINIMAL_HOOK). No unrelated changes in that file.
PHASE_1=DONE. Reverted IS_NO_PARALLEL to its exact original PRIVATE
  instance form (signature/doc/call-site unchanged, verified byte-for-
  byte equivalent to ada103d5 via `git diff ada103d5 -- <file>` showing
  ONLY the hook block as a residual diff). Added a new PRIVATE
  CLASS-METHODS IS_STANDARD_NO_PARALLEL_TYPE (28 chars) on
  ZCL_ABAPGIT_ORTEC_SER_ORCH - a deliberate local copy of the exact same
  ECTC/ECTD denylist, with ABAP Doc explaining why it is a copy (standard
  method stays private) and a MAINTENANCE note pointing at the new
  parity-pinning test. Updated ORCH's SERIALIZE() partition logic to call
  the new local helper instead of ZCL_ABAPGIT_SERIALIZE=>IS_NO_PARALLEL.
  Added NO_PARALLEL_PARITY test in the ORCH testclasses include -
  hardcoded-expectation pin (ECTC/ECTD=true; CLAS/INTF/DDLS/WAPA=false)
  since the friend relationship does not extend across classes and the
  standard method must stay private (cannot be called from the test
  either) - documented in the test's own comment that upstream denylist
  changes require a manual review of both sides.
SIDE_EFFECT_LEDGER (standard ZCL_ABAPGIT_SERIALIZE~SERIALIZE, comparing
  the per-object loop's side effects against what the ORTEC hook path
  skips when it fires):
  - SKIPPED: IS_SERIAL_PREFETCH_ACTIVE prepare/clear on PREF/PREF_EXT/
    PREF_OO - a REAL, pre-existing, SEPARATE ORTEC feature is silently a
    no-op whenever IS_SERIAL_BATCH_ACTIVE is also on, since the hook
    RETURNs before that code is ever reached. Functionally safe (ORCH's
    own fallback/RFC-worker paths still serialize correctly without it,
    just without the prefetch speed benefit) but NOT previously called
    out as an explicit feature-interaction; flagged for the IT8
    validation plan and the independent audits below.
  - SKIPPED: progress bar (ZIF_ABAPGIT_PROGRESS) and
    ZCL_ABAPGIT_TIMER - UX/telemetry only, no correctness impact.
  - SKIPPED: the standard AVOID_TIMEOUT redispatch-every-300s call - ORCH
    has its own, functionally analogous but NOT identical,
    C_BATCH_RFC_TIMEOUT_S=300 mechanism; not a gap, a different
    mechanism achieving the same purpose.
  - UNCHANGED: MV_FREE/GV_MAX_PROCESSES/MV_PARALLEL_BROKEN/MI_LOG - all
    read-only or already set before the hook check; the hook's RETURN
    happens before the standard TRY/CLEANUP block, so no prefetch CLEANUP
    is skipped inconsistently (prefetch was never started on this path).
  - NOTED, NOT A DEFECT: the hook's own
    `AND mv_parallel_broken = abap_false` guard is always true at that
    point (MV_PARALLEL_BROKEN is reset to abap_false at the very top of
    SERIALIZE() and nothing between there and the hook sets it) - a
    harmless, defensive-but-currently-redundant condition, left in place
    since a future insertion between those two points could set it and
    this guard is exactly the safety net that would matter then.
STANDARD_PUBLIC_API_CHANGE=NONE (confirmed via ada103d5 diff)
LIVE_VERIFICATION=mcp_arc-12 SAPDiagnose(action="syntax") dry-run, full
  replacement source, on BOTH ZCL_ABAPGIT_SERIALIZE (clean, 0 messages)
  and ZCL_ABAPGIT_ORTEC_SER_ORCH (clean, 1 PRE-EXISTING unrelated
  shorttext-length warning only, not introduced this pass). Local
  get_errors clean on all 3 touched files.
PRODUCTIVE_CODE_CHANGED=YES (zcl_abapgit_serialize.clas.abap:
  IS_NO_PARALLEL reverted to private instance; zcl_abapgit_ortec_ser_
  orch.clas.abap: new IS_STANDARD_NO_PARALLEL_TYPE + call-site swap;
  zcl_abapgit_ortec_ser_orch.clas.testclasses.abap: new
  NO_PARALLEL_PARITY test)
NEXT=Phase 2 (independent adversarial + performance implementation
  audits via dedicated subagents), Phase 3 (T-DRAIN owner-decision
  recommendation + expanded IT8 plan), Phase 4 (compact .memory/state.md)
```

## Prior Phase 1 reconciliation (2026-08-04, kept for history)

```text
  DDIC_ACTIVATION=PASS (owner-confirmed)
  CLASS_CONTRACT_ACTIVATION=PASS (owner-confirmed)
  FUNCTION_GROUP_ACTIVATION=PASS (owner-confirmed)
  RFC_FUNCTION_MODULE_ACTIVATION=PASS (owner-confirmed)
  RFC_INTERFACE_TYPE_REFERENCE=PASS (owner-confirmed, via the new
    ZAOG_SER_TADIR_TT DDIC type, not the original interface-type syntax)
  ATC=PASS - the 6 findings found via a live SAPDiagnose(action="atc")
    run were fixed in f9d0a070; owner imported/activated it and this
    agent independently re-ran ATC live on ZCL_ABAPGIT_ORTEC_SER_ORCH,
    ZCL_ABAPGIT_ORTEC_SER_COST, and ZCL_ABAPGIT_ORTEC_SER_PLANNER -
    all three return zero findings (2026-08-04).
  FOCUSED_ABAP_UNIT=NOT_APPLICABLE (no test classes exist yet for these
    Phase-1 contract-only objects; bodies are stubs except PROV_GEN)
PHASE_1_IT8_GATE=PASS (all gate criteria met; Phase 2 may begin)
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

## OWNER_ACTION_COMPLETED (2026-08-04) — original creation checklist, now historical

```text
OWNER_ACTION_REQUIRED=CREATE_GLOBAL_OBJECTS  -- COMPLETED by owner 2026-08-04
```

All 11 objects below were created and verified live against IT8 in
`.memory/logs/serialization_slice_2_object_verification.md`. Table kept
here for historical/dependency-order reference only — see the
verification log for actual current state and the one open correction
(FUGR/FM package placement).

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

## Object verification — RESOLVED (2026-08-04)

```text
OBJECT=ZABAPGIT_ORTEC_SERIAL (FUGR) + Z_ABAPGIT_ORTEC_SER_BATCH (FUNC, member)
WAS=$ABAPGIT_ORTEC_SERIAL_CORE
NOW=$ABAPGIT_ORTEC_SERIAL_RFC (owner moved it; re-verified live via TADIR)
STATUS=RESOLVED — all 11 manifest objects now match exactly.
```

## Phase 1 (contract/DDIC/ABAP-Doc definitions) authorized

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
SLICE_2_STATUS=PHASE_1_IT8_RECONCILED_PHASE_2_READY
MANUAL_OBJECT_VERIFICATION=PASS (all 11 objects match manifest exactly)
OD14_STATIC_STATE_AUDIT=PASS
PHASE_1_CONTRACT_REVIEW=PASS (ortec-abapgit-design-review, round 2, both
  findings from round 1 fixed and re-confirmed: TTYP KEYDEF/KEYKIND, RFC
  per-parameter documentation)
PHASE_1_COMMITS=93b814dc, 46f77304, c13943be (owner IT8 fixes), f9d0a070
  (ATC-finding fixes on the corrected contracts)
IT8_ACTIVATION=PASS (DDIC, class contracts, function group, RFC function
  module, RFC interface-type reference via ZAOG_SER_TADIR_TT - all
  owner-confirmed on IT8, 2026-08-04)
ATC=PASS (6 real priority-3 findings found via live SAPDiagnose
  (action="atc"), fixed in f9d0a070, owner imported/activated it, and
  this agent independently re-confirmed a clean live ATC pass on all
  three affected classes, 2026-08-04)
FOCUSED_ABAP_UNIT=NOT_APPLICABLE (no test classes yet for Phase-1
  contract-only objects)
RUN_REGISTRY=CONTRACT_ONLY (types/constants/static-DATA declared on
  ZCL_ABAPGIT_ORTEC_SER_ORCH; state-machine method bodies NOT implemented)
BATCH_RFC=CONTRACT_ONLY (Z_ABAPGIT_ORTEC_SER_BATCH signature + docs
  complete, now using ZAOG_SER_TADIR_TT at the RFC boundary; worker body
  NOT implemented)
ADAPTIVE_PLANNER=CONTRACT_ONLY (ZCL_ABAPGIT_ORTEC_SER_PLANNER signature +
  docs complete; LPT/refill-sizing bodies NOT implemented)
COST_MODEL=CONTRACT_ONLY (ZCL_ABAPGIT_ORTEC_SER_COST signature + docs
  complete; EWMA bodies NOT implemented)
PROV_GEN=FULLY_IMPLEMENTED (permanent no-op provider, final not a stub)
WAPA_PATH=UNCHANGED_EXCLUDED (structurally already true today; planner-
  level explicit exclusion is a confirmed C6 requirement for when the
  planner is implemented)
NEXT=Phase 1 gate fully PASSED - begin Phase 2 behavior implementation
  (standard-abapGit hook insertion into zcl_abapgit_serialize.clas.abap,
  ORCH/PLANNER/COST bodies, FM worker body - converting to/from
  ZAOG_SER_TADIR_TT at the RFC boundary, T-DRAIN test seam, all required
  unit tests, and the 4 required Phase-2 reviews: correctness,
  regression, adversarial, performance).
```
