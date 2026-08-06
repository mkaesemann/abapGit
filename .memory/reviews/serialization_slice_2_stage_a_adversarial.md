# SER-SLICE-2 Stage A Adversarial Rereview

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_2_STAGE_A_ADVERSARIAL_REREVIEW
CYCLE=2
DATE=2026-08-06
VERDICT=APPROVE
SOURCE_SCOPE=src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap,src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.testclasses.abap,src/ortec/git/zcl_abapgit_ortec_git_switch.clas.abap,src/ortec/serial/rfc/zabapgit_ortec_serial.fugr.z_abapgit_ortec_ser_batch.abap,src/ortec/serial/rfc/zabapgit_ortec_serial.fugr.xml,src/objects/core/zcl_abapgit_serialize.clas.abap,src/ortec/serial/zcl_abapgit_ortec_wapa.clas.abap
ALLOWED_CONTEXT=.memory/handoffs/serialization-slice-2.md,.memory/logs/serialization_adaptive_batch_design.md,.memory/logs/serialization_performance_design.md,.memory/logs/serialization_slice_2_it8_validation_plan.md
READ_ONLY=yes
STATE_MD_CHANGED=NO
DIAGRAM_CHANGED=NO
```

## Evidence Matrix

```text
E1=.memory/handoffs/serialization-slice-2.md Stage A closeout states WAIT_MODEL=FAIL_FAST, explicitly superseding T/X/abandon/drain, with result 4/8 raising visible ZCX_ABAPGIT_EXCEPTION and discarding partial run state.
E2=.memory/logs/serialization_adaptive_batch_design.md Current-source supersedure marks the old timeout lifecycle/T-DRAIN gate historical and declares the active contract: success iff complete; WAIT 4 incomplete => visible exception; WAIT 8 => visible timeout exception; late callbacks after discard are RECEIVE-and-discard.
E3=.memory/logs/serialization_performance_design.md Current-source supersedure withdraws the old hook TRY/CATCH fallback for Stage A and states fail-fast WAIT 4/8 intentionally raises instead of falling through.
E4=.memory/logs/serialization_slice_2_it8_validation_plan.md replaces the old T-DRAIN gate with required Stage A fail-fast IT8 cases: complete success, induced missing result, induced timeout, late callback after discard, and feature-off regression.
E5=src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap class header documents fail-fast wait, no partial success, discard-on-failure, and late unknown-task receive/discard.
E6=src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap has no C_STATE_TIMED_OUT, no C_STATE_ABANDONED, no C_MAX_DRAIN_WAIT_S; WAIT_FOR_RUN_COMPLETION performs one WAIT FOR ASYNCHRONOUS TASKS and maps incomplete 0/4 to 4 and other incomplete results to 8.
E7=src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap SERIALIZE returns files only on wait result 0; otherwise DISCARD_RUN_STATE, clears RT_FILES, and raises ZCX_ABAPGIT_EXCEPTION.
E8=src/objects/core/zcl_abapgit_serialize.clas.abap minimal hook delegates to ZCL_ABAPGIT_ORTEC_SER_ORCH=>SERIALIZE and RETURNs; it has no CATCH, matching E3's visible-error Stage A contract.
E9=src/ortec/git/zcl_abapgit_ortec_git_switch.clas.abap declares MV_SERIAL_BATCH_ACTIVE VALUE ABAP_FALSE; IS_SERIAL_BATCH_ACTIVE returns it; SET_SERIAL_BATCH_ACTIVE is the only setter.
E10=src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap PARTITION_OBJECTS routes WAPA to a WAPA bucket only after forced-sequential exclusions; BUILD_WAPA_SINGLETON_BATCHES emits one single-item batch per WAPA object.
E11=src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.testclasses.abap covers WAPA partition separation, WAPA singleton batches, wait result 0/4/8 interpretation, no-parallel parity, and merge bad-payload/no-context/success/empty-list cases.
E12=src/ortec/serial/rfc/zabapgit_ortec_serial.fugr.z_abapgit_ortec_ser_batch.abap emits one ET_RESULT row per IT_TADIR row, catches object-level ZCX_ABAPGIT_EXCEPTION inside the loop, and uses ZCL_ABAPGIT_OBJECTS=>SERIALIZE for the real object serializer.
E13=src/ortec/serial/zcl_abapgit_ortec_wapa.clas.abap exposes stateless CLASS-METHODS with no CLASS-DATA singleton/cache state found; batching singleton policy is owned by ORCH, not WAPA's serializer implementation.
E14=src/objects/core/zcl_abapgit_serialize.clas.abap standard RUN_SEQUENTIAL catches per-object ZCX_ABAPGIT_EXCEPTION, logs it, and continues; ORCH fallback's per-object log-and-resolve behavior is parity, not a new fail-fast contradiction.
```

## Closed Prior Findings

```text
ID=AR-1-001
SEVERITY=MAJOR
CLAIM=SER-SLICE-2 Stage A is implementation-ready against the allowed design packet and can be treated as decision-free.
COUNTEREXAMPLE=Cycle 1 found that the then-authoritative packet still required T/X/abandon/drain and T-DRAIN closure while source implemented fail-fast only.
EVIDENCE=E1,E2,E4,E5,E6,E7
IMPACT=false READY; weak-model implementation ambiguity; stale design could reintroduce abandoned-state logic or claim validation that Stage A cannot satisfy.
REQUIRED_CHANGE=Authoritative Stage A packet must supersede old T/X/T-DRAIN text and state exact fail-fast semantics and IT8 closure proof.
RETEST=ACCEPTED_AND_FIXED. The handoff, adaptive design, and IT8 validation plan now explicitly supersede T/X/T-DRAIN for Stage A and define fail-fast 0/4/8 semantics, discard behavior, late-callback handling, and required IT8 proof. Source matches that contract.
```

```text
ID=AR-1-002
SEVERITY=MAJOR
CLAIM=The minimal standard hook preserves default-safe fallback behavior when ORTEC batch serialization fails.
COUNTEREXAMPLE=Cycle 1 found source raised through the hook on ORCH WAIT 4/8 while the master architecture still promised CATCH-and-fall-through standard fallback.
EVIDENCE=E1,E3,E7,E8,E9,E14
IMPACT=availability semantics; false fallback claim; user-facing failure where older design promised standard-path recovery.
REQUIRED_CHANGE=Either restore the TRY/CATCH fallback or update the authoritative Stage A design/handoff to state that feature-on incomplete batch results intentionally raise instead of falling back.
RETEST=ACCEPTED_AND_FIXED. The performance design's current-source supersedure explicitly states the older hook TRY/CATCH fallback is not the Stage A contract. Source raises visibly on incomplete feature-on batch results, and default-off behavior keeps the standard path unchanged until the switch is explicitly enabled.
```

## Hostile-Case Ledger

```text
active fail-fast implementation=PASS. ORCH returns only after IS_RUN_COMPLETE; incomplete WAIT 0/4 maps to 4, timeout/other maps to 8, and SERIALIZE discards all run state before raising. No T/X/drain artifacts remain in source.
late callback after discard=PASS. DISCARD_RUN_STATE deletes MT_DISPATCH rows; ON_END_OF_BATCH unknown-task branch still RECEIVE RESULTS and discards payload.
standard-hook fallback semantics=PASS_WITH_SUPERSEDURE. Feature-on incomplete batches intentionally raise through the minimal hook; feature-off remains unchanged because the switch defaults ABAP_FALSE.
WAPA singleton policy=PASS. WAPA is never sent to the general planner and is converted to one object per planned batch; tests pin separation and singleton shape. WAPA serializer class itself adds no singleton state.
merge tests=PASS. Tests cover missing run context, bad payload preserving existing accumulator rows, multi-file success metadata/path preservation, and empty file-list success.
default-off switch=PASS. MV_SERIAL_BATCH_ACTIVE VALUE ABAP_FALSE, getter returns only that field, and setter is explicit.
new blocker/major scan=PASS. No new BLOCKER/MAJOR found in the constrained source scope for fail-fast, WAPA singleton handling, merge behavior, or default-off switch.
residual validation boundary=MINOR. IT8 plan is still a plan, not executed evidence; do not mark SER-SLICE-2 fully DONE until the listed import/ABAP Unit/ATC/parity/fail-fast/feature-off/WAPA trace cases pass on IT8.
```

## Verdict

```text
OPEN_BLOCKER=0
OPEN_MAJOR=0
OPEN_MINOR=1/residual IT8 execution boundary only
CLOSED=AR-1-001,AR-1-002
VERDICT=APPROVE
NEXT=Run the IT8 validation plan against the imported active source, starting with feature-off baseline and fail-fast incomplete/timeout cases.
```