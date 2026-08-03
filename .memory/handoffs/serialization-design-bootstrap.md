# Serialization Design — Bootstrap Handoff, Creation Manifest, Implementation Slices

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SERIALIZATION_PERFORMANCE_DESIGN_BOOTSTRAP
STATUS=DESIGN_COMPLETE, ALL_REVIEWS_APPROVED
DESIGN_ARTIFACTS=.memory/logs/serialization_ser0_audit.md,
  .memory/logs/serialization_ser0_audit_standard.md,
  .memory/logs/serialization_ser0_audit_ortec.md,
  .memory/logs/serialization_performance_design.md,
  .memory/logs/serialization_bulk_exists_design.md,
  .memory/logs/serialization_adaptive_batch_design.md,
  .memory/logs/serialization_provider_design.md,
  .memory/logs/serialization_wapa_review.md
REVIEW_ARTIFACTS=.memory/reviews/serialization_adversarial_review.md
  (5 cycles total: cycle 1 BLOCKER 2/MAJOR 6/MINOR 3; cycles 2-3 closed all
  but AR-1-001; cycles 4-5 (owner-authorized extra cycles) closed AR-1-001
  via an UNVERIFIED instance-lifetime/GC-retention claim -> later REOPENED
  by the dedicated OD-13 documentation-verification task, which proved the
  GC-retention claim FALSE (documented only for SET HANDLER, not aRFC
  callbacks) and REPLACED it with a static CLASS-DATA/CLASS-METHODS design;
  a focused "OD-13 Correction" adversarial cycle then found 2 new MAJOR
  cross-run-isolation gaps (AR-OD13-001/002), both fixed and independently
  re-verified in the "OD-13 Correction closure" section -> FINAL
  VERDICT=APPROVE, 0/0/0),
  .memory/reviews/serialization_correctness_review.md (2 cycles APPROVE,
  then a focused "Cycle 3 (OD-13 Correction)" pass found 1 BLOCKER
  (DR-005)/1 MAJOR (DR-006)/1 MINOR (DR-007), all fixed and independently
  re-verified in "Cycle 4 (OD-13 Correction closure)" -> FINAL
  VERDICT=APPROVE, 0/0/0),
  .memory/reviews/serialization_performance_review.md (3 cycles:
  APPROVE_WITH_MINOR_REVISIONS; "OD-13 Correction addendum" confirms the
  run_id-scoping correctness fix introduced no new performance concern)
STATE_MD_CHANGED=NO
PRODUCTIVE_CODE_CHANGED=NO
```

## Gate status (do not skip this section before starting any implementation)

```text
CORRECTNESS_REVIEW        APPROVE (0 blocker/major/minor — includes the
                          OD-13 Correction closure: DR-005/006/007 all
                          RESOLVED and independently re-verified)
PERFORMANCE_DESIGN_GATE   APPROVE_WITH_MINOR_REVISIONS (0 blocking/major,
                          1 non-gating minor: a single poison object's
                          bisection chain can inflate the PER-RUN circuit-
                          breaker sliding window; self-limiting, not fixed
                          in this pass, safe to defer; OD-13 Correction
                          addendum confirms the run_id-scoping fix changed
                          nothing about this finding)
ADVERSARIAL_REVIEW        APPROVE (0 blocker/major/minor — includes the
                          OD-13 Correction closure: AR-OD13-001/002 both
                          RESOLVED and independently re-verified, including
                          a genuine follow-up gap the first closure pass
                          itself caught, fixed, and re-confirmed)
```

**All three gates are now clear with NO open preconditions on OD-13.** The
prior "read the ABAP Keyword Documentation and confirm in writing"
precondition has been COMPLETED: real documentation was fetched (not
recalled from memory) and PROVED the earlier GC-retention claim FALSE for
aRFC callbacks (documented only for `SET HANDLER` event handlers). The
design was REDESIGNED around the mechanism the documentation actually DOES
prove — that a `CLASS-DATA`/`CLASS-METHODS`-owned callback target persists
for the whole internal session by basic ABAP language semantics, matching
exactly the scope `ABAPCALL_FUNCTION_STARTING` requires ("the calling
program still exists in its internal session"). See adaptive batch design
§5.1a for the full evidence trail and adversarial/correctness review
ledgers for the closure re-verification.

**Implementation of SLICE 2 (the adaptive batch orchestrator) is fully
AUTHORIZED.** The only remaining SLICE-2-time (not design-time) obligation
is unchanged from before: still run the previously-recommended IT8
empirical verification test (adaptive batch design §5.1b, T-DRAIN-1..8) as
a real-system confirmation of the abandonment/drain state model, before
SLICE 2 is considered DONE — this is empirical validation of an already-
proven design, not a remaining design gap.

## Creation manifest

All names verified `<=30` characters (re-confirmed by correctness review
cycle 2). Nothing in this manifest is created by this task — it is a
precise list for the owner to create manually, per package/slice.

### New packages

| PACKAGE | PARENT_PACKAGE | PURPOSE | CREATE_IN_SLICE |
|---|---|---|---|
| `$ABAPGIT_ORTEC_SERIAL` | existing ORTEC root (sibling of `$ABAPGIT_ORTEC_GIT`) | root of new serialization-performance work | SLICE 2 |
| `$ABAPGIT_ORTEC_SERIAL_CORE` | `$ABAPGIT_ORTEC_SERIAL` | planner, cost model, orchestrator, batch DDIC | SLICE 2 |
| `$ABAPGIT_ORTEC_SERIAL_PROVIDER` | `$ABAPGIT_ORTEC_SERIAL` | provider contract, DDIC provider, facade | SLICE 3 |
| `$ABAPGIT_ORTEC_SERIAL_RFC` | `$ABAPGIT_ORTEC_SERIAL` | batch RFC function group | SLICE 2 |

### New classes

| OBJECT_TYPE | OBJECT_NAME | CHARS | DESCRIPTION | CREATE_IN_SLICE | DEPENDENCIES | STANDARD_OR_ORTEC | NEW_OR_EXISTING |
|---|---|---|---|---|---|---|---|
| CLAS | `ZCL_ABAPGIT_ORTEC_SER_ORCH` | 26 | Orchestrator: partitions objects, drives planner/dispatch/callback lifecycle | SLICE 2 | Planner, Cost, Batch DDIC, RFC FM | ORTEC | NEW |
| CLAS | `ZCL_ABAPGIT_ORTEC_SER_PLANNER` | 29 | LPT-first batch builder + guided-self-scheduling refill | SLICE 2 | Cost estimator | ORTEC | NEW |
| CLAS | `ZCL_ABAPGIT_ORTEC_SER_COST` | 26 | Run-local EWMA cost estimator, type-family static defaults | SLICE 2 | none | ORTEC | NEW |
| CLAS | `ZCL_ABAPGIT_ORTEC_SER_PROV_DD` | 29 | New DDIC provider: DOMA (new) + DTEL (wraps existing `ser_pref_ext` cache) | SLICE 3 | `ZCL_ABAPGIT_OBJECT_DOMA` (read-only, for version-semantics parity) | ORTEC | NEW |
| CLAS | `ZCL_ABAPGIT_ORTEC_SER_PROV_GEN` | 30 | Trivial no-op generic provider (last-resort match) | SLICE 2 | none | ORTEC | NEW |
| CLAS | `ZCL_ABAPGIT_ORTEC_SER_PROV_FCD` | 30 | Single parameterized facade wrapping `ser_pref`/`_ext`/`_oo` behind the provider contract | SLICE 3 | `ZCL_ABAPGIT_ORTEC_SER_PREF*` (unchanged) | ORTEC | NEW |

### New interfaces

| OBJECT_TYPE | OBJECT_NAME | CHARS | DESCRIPTION | CREATE_IN_SLICE |
|---|---|---|---|---|
| INTF | `ZIF_ABAPGIT_ORTEC_SER_PROV` | 26 | Batch-scoped provider contract (`supports`/`prepare`/`extract_for_batch`/`inject_from_buffer`/`clear`/`get_version`) | SLICE 3 |
| INTF | `ZIF_ABAPGIT_ORTEC_SER_EXIST_H` | 29 | Bulk-exists handler contract — **FUTURE, SER-4-triggered only, NOT part of SLICE 1-3** | SER-4 (not authorized now) |

### New function modules / function group

| OBJECT_TYPE | OBJECT_NAME | CHARS | DESCRIPTION | CREATE_IN_SLICE |
|---|---|---|---|---|
| FUGR | `ZABAPGIT_ORTEC_SERIAL` (function group) | n/a | Hosts the new batch RFC function module | SLICE 2 |
| FUNC (RFC-enabled) | `Z_ABAPGIT_ORTEC_SER_BATCH` | 26 | Multi-object batch serialization worker (see adaptive batch design §2 for full signature) | SLICE 2 |

### New DDIC objects

| OBJECT_TYPE | OBJECT_NAME | CHARS | DESCRIPTION | CREATE_IN_SLICE |
|---|---|---|---|---|
| TABL (structure) | `ZAOG_SER_BATCH_RESULT` | 21 | One row per batch object result (see adaptive batch design §2) | SLICE 2 |
| TTYP (table type) | `ZAOG_SER_BATCH_RESULT_TT` | 24 | `STANDARD TABLE OF ZAOG_SER_BATCH_RESULT WITH EMPTY KEY` | SLICE 2 |
| TABL (structure) | `ZAOG_SER_EXIST_RESULT` | 21 | Bulk-exists classification row — **FUTURE, SER-4-triggered only** | SER-4 (not authorized now) |
| TTYP (table type) | `ZAOG_SER_EXIST_RESULT_TT` | 24 | table type of the above — **FUTURE** | SER-4 (not authorized now) |

### No new exception classes required

Every failure path reuses `ZCX_ABAPGIT_EXCEPTION` (existing) — no new
exception hierarchy is needed; batch object-level failures are data rows
(`ZAOG_SER_BATCH_RESULT-RC`/`MSGID`/`MSGNO`), not new exception types.

### No new statistics table (SER-6 not approved)

### Test classes

All test classes are colocated `.clas.testclasses.abap` includes on the
classes above (matching repo convention) — no separate global test-double
objects are required for SLICE 0-3.

## Existing objects to edit

| OBJECT_NAME | EXACT_METHOD_OR_SECTION | WHY_CHANGE_IS_REQUIRED | MINIMAL_HOOK_CONTRACT | FEATURE_OFF_BEHAVIOR | ROLLBACK |
|---|---|---|---|---|---|
| `zcl_abapgit_serialize` (CLAS, standard) | `serialize()` method, class-definition visibility of `is_no_parallel` | (a) insert the single delegation `IF...TRY...ENDTRY` block (performance design §2); (b) change `is_no_parallel` from PRIVATE to PUBLIC | See performance design §2 for the exact inserted code; visibility change is a keyword-only edit, zero logic change | Switch off (`is_ser_batch_active( ) = abap_false`): the new `IF` body never executes, byte-identical to today | Delete the one `IF...ENDIF` block; revert the visibility keyword. No other edit. |

**No other standard abapGit file requires any change.** Every other hook
(prefetch prepare/clear, bulk-exists check_exists call) already exists in
current source and is reused unmodified.

## Objects to retain/refactor/retire

```text
ZCL_ABAPGIT_ORTEC_BULK_EXISTS   KEEP (verify-only for the prototype; see
                                 SER-1 design — no code change authorized
                                 now)
ZCL_ABAPGIT_ORTEC_SER_PREF      KEEP_AND_REFACTOR (add extract_for_batch,
                                 SLICE 3)
ZCL_ABAPGIT_ORTEC_SER_PREF_EXT  KEEP_AND_REFACTOR (add extract_for_batch,
                                 SLICE 3; DOES NOT gain a DOMA cache — that
                                 lives in the new sibling ZCL_ABAPGIT_ORTEC_
                                 SER_PROV_DD instead)
ZCL_ABAPGIT_ORTEC_SER_PREF_OO   KEEP_AND_REFACTOR (add extract_for_batch,
                                 SLICE 3)
ZCL_ABAPGIT_ORTEC_WAPA          KEEP_WITH_CORRECTIONS (write tests first,
                                 SLICE 0.5; REFACTOR_INTO_PROVIDER is a
                                 LATER, separately reviewed slice, not
                                 authorized now)
```

Evidence and migration timing for each: see SER-0 audit §8 and the
respective SER-1/SER-3/SER-5 design documents; not restated here.

## Implementation slices

### SLICE 0 — Pinning tests (no new code, no OD-13 dependency, NOT blocked)

```text
SLICE_ID       SER-SLICE-0
GOAL           Close the two independently-flagged, zero-architecture-risk
               gaps found during discovery/review: WAPA has no tests
               (SER-5), and the bulk-exists CLAS/INTF/DTEL/DOMA exclusion
               rules have no regression pin (SER-1 T-1..T-3).
PRECONDITIONS  None beyond normal ABAP Unit/CL_OSQL_TEST_ENVIRONMENT access.
NEW_OBJECTS_REQUIRED  None (test includes only, on EXISTING classes).
EXISTING_FILES_OR_OBJECTS  zcl_abapgit_ortec_wapa (add .clas.testclasses.abap),
               zcl_abapgit_ortec_bulk_exists (add/extend .clas.testclasses.abap),
               zcl_abapgit_tadir (read-only, for T-1/T-2 assertions).
EXACT_METHODS  T-WAPA-1..5 (serialization_wapa_review.md), T-1..T-3
               (serialization_bulk_exists_design.md).
INSERT/REPLACE_ANCHORS  New test methods only; no productive method body
               touched.
BEHAVIORAL_CONTRACT  Tests must PASS against CURRENT, unmodified behavior
               (these are pinning tests, not tests of new functionality).
DATA/RFC_CONTRACT  N/A.
SQL_SHAPE          N/A (tests use CL_OSQL_TEST_ENVIRONMENT per this
               workspace's proven pattern, user memory notes).
ROW/BYTE/TASK_LIMITS  N/A.
ERROR/FALLBACK/CLEANUP  N/A.
TESTS          The tests themselves are the deliverable.
SAP_VALIDATION  Real-system ABAP Unit run confirming all new tests PASS.
PERFORMANCE_VALIDATION  None required (no behavior change).
COMMIT_BOUNDARY  One commit: WAPA tests + bulk-exists pinning tests.
STOP_CONDITIONS  Any new test FAILS against current behavior — this means
               the design's own understanding of current behavior (SER-0/
               SER-1/SER-5) is wrong somewhere and must be re-verified
               against source before proceeding to any other slice.
ROLLBACK       Delete the new test includes; zero risk to production code.
```

### SLICE 1 — DOMA version-semantics investigation + parity harness (no
new productive code; NOT blocked by OD-13)

```text
SLICE_ID       SER-SLICE-1
GOAL           Resolve DR-003: read ZCL_ABAPGIT_OBJECT_DOMA's own
               serialize() method and state explicitly which DD01L/DD01V/
               DD07L/DD07T version value(s) it reads today, before any
               new DOMA provider code is written (SER-3 §2 precondition).
PRECONDITIONS  None.
NEW_OBJECTS_REQUIRED  None yet (this slice is discovery + a written parity
               target, not implementation).
EXISTING_FILES_OR_OBJECTS  zcl_abapgit_object_doma (read-only).
EXACT_METHODS  Whatever method(s) implement zif_abapgit_object~serialize
               for DOMA - trace to source.
OUTPUT         A short addendum to serialization_provider_design.md §2
               stating the confirmed version semantics, replacing the
               "OPEN, NOT-YET-RESOLVED" note.
STOP_CONDITIONS  If DOMA's version semantics turn out to be
               data-dependent/complex in a way that cannot be captured by
               a simple bulk SELECT, escalate to the owner before
               proceeding to SLICE 3 - do not silently approximate.
COMMIT_BOUNDARY  Memory-only update, no productive code, no separate
               commit needed (or a trivial memory-file commit if the
               repo's convention tracks .memory/ in git).
```

### SLICE 2 — Adaptive batch orchestration core (GATED ON OD-13)

```text
SLICE_ID       SER-SLICE-2
GOAL           Implement ZCL_ABAPGIT_ORTEC_SER_ORCH, _PLANNER, _COST,
               _PROV_GEN, the ZAOG_SER_BATCH_RESULT(_TT) DDIC, the
               Z_ABAPGIT_ORTEC_SER_BATCH function module, and the single
               standard-code hook + is_no_parallel visibility change.
PRECONDITIONS  SLICE 0 complete (pinning tests exist and pass). The OD-13
               documentation verification is COMPLETE and CLOSED: real
               ABAP Keyword Documentation was fetched and PROVED the
               earlier GC-retention claim false for aRFC callbacks; the
               design was REDESIGNED around static CLASS-DATA/CLASS-
               METHODS ownership (adaptive batch design §5.1a), and the
               follow-up "OD-13 Correction" adversarial/correctness review
               cycles (AR-OD13-001/002, DR-005/006/007) are all RESOLVED
               and independently re-verified — no remaining OD-13 gate
               condition blocks SLICE 2. OD-14's static-state audit
               (performance design §3/§4) has been performed and found no
               new cross-object contamination risk beyond the three known
               ortec caches, OR any found risk has been mitigated.
NEW_OBJECTS_REQUIRED  See creation manifest, "SLICE 2" rows.
EXISTING_FILES_OR_OBJECTS  zcl_abapgit_serialize.clas.abap (one hook +
               one visibility change, performance design §2).
EXACT_METHODS  serialize() (hook insertion point precisely cited,
               performance design §2); is_no_parallel (visibility only).
INSERT/REPLACE_ANCHORS  Exact anchor text quoted in performance design §2.
BEHAVIORAL_CONTRACT  See adaptive batch design in full (§1-§10); output
               parity per performance design §4.
DATA/RFC_CONTRACT  Z_ABAPGIT_ORTEC_SER_BATCH exact signature, adaptive
               batch design §2.
SQL_SHAPE      No new SQL in this slice (planner/cost/orchestrator are
               pure ABAP logic; the worker's own object serialization
               calls are unchanged existing code).
ROW/BYTE/TASK_LIMITS  Full table, adaptive batch design §9.
ERROR/FALLBACK/CLEANUP  Task-identity/lifecycle state machine, adaptive
               batch design §5; circuit breaker §5.8; cleanup ownership
               performance design §3.
TESTS          Full unit test matrix per adaptive batch design (LPT
               ordering, batch limits, duplicate/late-callback drain,
               result-set fingerprint mismatch, bisection-to-singleton,
               circuit-breaker trip/no-trip, deterministic equal-weight
               scheduling).
SAP_VALIDATION  Activation, syntax, ABAP Unit, ATC on IT8; PLUS the OD-13
               abandonment/drain empirical verification test (adaptive
               batch design §5.1b, T-DRAIN-1..8) — this specific step is a
               hard requirement before SLICE 2 is considered DONE, not
               optional (the earlier "deliberate-hang" GC-retention test
               is superseded: that was designed to probe the now-DISPROVEN
               GC-exemption claim; §5.1b's T-DRAIN tests instead verify
               the static/CLASS-DATA abandonment-state-model actually
               implemented).
PERFORMANCE_VALIDATION  Before/after wall time, RFC task count, DB
               statement count on the SAME CLAS-only fixture used by the
               attached traces, switch ON vs OFF, per the owner brief's
               acceptance model.
COMMIT_BOUNDARY  One commit per class/DDIC object is acceptable; the
               standard-file hook edit should be its own final commit
               once everything else is proven, per this repo's
               checkpoint-commit convention.
STOP_CONDITIONS  §5.1b's T-DRAIN empirical test finds a real drain/
               abandonment behavior contradicting the state model; OD-14
               audit finds unmitigated cross-object state risk; any
               SLICE 0 pinning test starts failing after the hook is added
               (would indicate the hook's "byte-identical when off" claim
               is false).
ROLLBACK       Delete the one hook block + revert the visibility keyword
               in zcl_abapgit_serialize; the new classes/DDIC/FM can
               remain dormant (unreferenced) or be deleted, at the
               owner's discretion — they have zero effect while the
               switch is off or absent.
```

### SLICE 3 — Batch-scoped providers (DOMA/DTEL, facade) — depends on
SLICE 1 and SLICE 2

```text
SLICE_ID       SER-SLICE-3
GOAL           Implement ZCL_ABAPGIT_ORTEC_SER_PROV_DD (new DOMA reads +
               wraps existing DTEL cache) and ZCL_ABAPGIT_ORTEC_SER_PROV_FCD
               (facade for ser_pref/_ext/_oo), plus extract_for_batch on
               the three existing ser_pref* classes.
PRECONDITIONS  SLICE 1 (DOMA version semantics confirmed), SLICE 2 (batch
               orchestrator exists and is validated).
NEW_OBJECTS_REQUIRED  See creation manifest, "SLICE 3" rows.
EXISTING_FILES_OR_OBJECTS  zcl_abapgit_ortec_ser_pref/_ext/_oo (ADD
               extract_for_batch method only — no existing method body
               changes).
TESTS          Provider hit/miss/fallback parity (byte-identical output
               forced-hit vs forced-miss), the OD-14 multi-object-per-
               session parity test, DOMA provider vs
               ZCL_ABAPGIT_OBJECT_DOMA byte-identical parity test.
SAP_VALIDATION  Activation, syntax, ABAP Unit, ATC.
PERFORMANCE_VALIDATION  Confirm extract_for_batch reduces EXPORT-call
               count roughly in proportion to batch size on the CLAS-only
               fixture (per performance review Q3 finding: no
               superlinear-cost risk expected, but should be confirmed).
COMMIT_BOUNDARY  One commit for the DOMA provider + DDIC, one for the
               facade + extract_for_batch additions.
STOP_CONDITIONS  Any parity test fails — never ship a provider whose
               forced-miss and forced-hit outputs differ.
ROLLBACK       Delete the new provider classes; ser_pref* classes'
               extract_for_batch methods are additive and can be left in
               place unused with zero effect if the provider layer is
               rolled back.
```

## Owner decisions requiring explicit resolution (index; full detail and
evidence in `serialization_performance_design.md` §5)

```text
OD-1..OD-12   Recommendations given, default-safe, do not block SLICE 0/1.
OD-13         FULLY RESOLVED. History: adversarial cycle 5 first "closed"
              this via an instance-lifetime/GC-retention claim that was
              NEVER verified against documentation; a dedicated OD-13
              documentation-verification task then fetched real ABAP
              Keyword Documentation, PROVED that claim FALSE for aRFC
              callbacks (GC-exemption is documented only for `SET HANDLER`
              event handlers), and REDESIGNED the mechanism around static
              `CLASS-DATA`/`CLASS-METHODS` ownership (adaptive batch
              design §5.1a) — a proven, not assumed, lifetime guarantee.
              A follow-up focused "OD-13 Correction" review round then
              found and closed 2 adversarial findings (AR-OD13-001/002)
              and 3 correctness findings (DR-005/006/007), all arising
              from run_id-scoping the new shared static state; all 5 are
              RESOLVED and independently re-verified (see both review
              ledgers' "closure" sections). No open design question or
              gate condition remains. The only remaining OD-13-related
              obligation is EMPIRICAL (not architectural): run the §5.1b
              T-DRAIN IT8 test suite during SLICE 2 to confirm the
              abandonment/drain state model on the real system.
OD-14         Gates SLICE 2 authorization (static-state audit) — cheap,
              mechanical, independent of OD-13 (which is now fully closed).
```
