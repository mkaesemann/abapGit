# Serialization Adversarial Review - Cycle 1

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SERIALIZATION_ADVERSARIAL_REVIEW_CYCLE1
CYCLE=1
PRIMARY_TARGET=.memory/logs/serialization_adaptive_batch_design.md
SECONDARY_TARGET=.memory/logs/serialization_provider_design.md
STATUS=PASS_WITH_FINDINGS
VERDICT=REVISE_AND_REVIEW_ONCE
BLOCKER_COUNT=2
MAJOR_COUNT=6
MINOR_COUNT=3
```

## Scope And Evidence

Read-only review performed against the allowed packet, in the requested order:

- E-SER0: `.memory/logs/serialization_ser0_audit.md`
- E-STD: `.memory/logs/serialization_ser0_audit_standard.md`
- E-ORT: `.memory/logs/serialization_ser0_audit_ortec.md`
- E-PERF: `.memory/logs/serialization_performance_design.md`
- E-BULK: `.memory/logs/serialization_bulk_exists_design.md`
- E-SER2: `.memory/logs/serialization_adaptive_batch_design.md`
- E-SER3: `.memory/logs/serialization_provider_design.md`
- E-WAPA: `.memory/logs/serialization_wapa_review.md`

No productive ABAP, state file, archive file, diagram, or other memory file was modified.

## Open Findings

```text
ID=AR-1-001
SEVERITY=BLOCKER
CLAIM=SER-2 §5 step 6 plus §6 "Missing callback/task timeout" claim that a timed-out batch can be treated exactly like communication failure "WITHOUT waiting for RECEIVE to ever return", then requeued safely.
COUNTEREXAMPLE=Batch B1 is dispatched and genuinely still running in an RFC worker. The poll loop marks B1 overdue at 300s, removes or abandons it to free in-flight budget, and dispatches retry B2 containing the same objects. B2 succeeds and merges its ET_RESULT rows into mt_files. Later, B1's ON END OF TASK callback is delivered. If B1 is no longer in the awaiting set, the design says it is ignored before RECEIVE; if it is still recognized, it can RECEIVE and merge a second successful result set for the same objects. The design does not specify a terminal state that both drains the old RFC result and prevents merge.
EVIDENCE=E-SER2 §5 steps 5-7; E-SER2 §6 "Missing callback/task timeout"; E-SER2 §6 "Retry causing duplicate successful object results"; E-SER0 §7 G-5 says no hidden per-batch timeout/late-callback guard exists today.
IMPACT=correctness/concurrency/idempotency
REQUIRED_CHANGE=Replace the timeout-as-communication-failure assertion with a precise task lifecycle: every dispatched task has immutable task_name, batch_id, generation/attempt, object fingerprint, and state AWAITING|TIMED_OUT|RECEIVED|DRAINED|CLOSED. A late callback for TIMED_OUT must still execute RECEIVE into a throwaway buffer, verify batch_id+attempt, record DRAINED, and never merge or update cost/provider telemetry. Retried objects must carry a new generation and the merge path must reject any result whose generation is not the current owner for those objects.
RETEST=Unit seam simulating: dispatch B1, mark timeout, dispatch/merge B2, then invoke B1 callback; assert mt_files has exactly one row per object, old in-flight budget is released once, late result is drained and ignored, no cost EWMA update from B1.
```

```text
ID=AR-1-002
SEVERITY=BLOCKER
CLAIM=SER-2 §5 step 7 "on_end_of_batch(batch_id)" and §6 "Duplicate callback or late callback" are sufficient identity guards.
COUNTEREXAMPLE=ABAP async RFC callbacks are keyed by the task name supplied to STARTING NEW TASK. The design invents a callback parameter named batch_id but does not define how the callback maps the actual RFC task name to batch_id/attempt, nor require task names to be globally unique for the whole serialize() call and never reused after timeout. A late callback for task name pattern `SER-<n>` can be resolved against a newer batch if the pattern is reused after the first entry is removed, allowing RECEIVE and merge under the wrong batch metadata.
EVIDENCE=E-STD §1.2 documents current task naming and callback contract; E-SER2 §5 step 5 says `lv_task_name` but does not define uniqueness/canonical mapping; E-SER2 §5 step 7 uses `on_end_of_batch(batch_id)`; E-SER2 §6 asks duplicate/late callbacks to rely on a batch_id membership guard.
IMPACT=identity canonicalization/context divergence/correctness
REQUIRED_CHANGE=Specify the exact callback signature and mapping table keyed by RFC task name. Task name must include run UUID + monotonically increasing dispatch sequence + attempt, not just batch position. Callback must look up by task name, then validate returned IV_BATCH_ID/IV_ATTEMPT/IV_INPUT_VERSION/IV_INPUT_ROW_COUNT/object fingerprint before any RECEIVE result is merged. Reuse of a task name within one serialize() call must be forbidden.
RETEST=Callback identity test with two dispatch records using the same logical object set but different attempts; deliver callbacks out of order; assert old task cannot resolve to newer dispatch and cannot merge.
```

```text
ID=AR-1-003
SEVERITY=MAJOR
CLAIM=SER-2 §6 "Retry causing duplicate successful object results" says the original is retried only after it is "confirmed failed (RECEIVE failure or timeout)" and therefore original and retried results cannot both merge.
COUNTEREXAMPLE=A timeout is not confirmation of RFC task failure; it is only confirmation that the caller stopped waiting. The original worker can later complete successfully. This is the same failure family as AR-1-001, but this attacked claim is separately wrong because it equates timeout with confirmed failure in prose and in the retry policy.
EVIDENCE=E-SER2 §6 "Retry causing duplicate successful object results"; E-SER2 §6 "Missing callback/task timeout".
IMPACT=correctness/idempotency/retry semantics
REQUIRED_CHANGE=Change wording and implementation contract: timeout creates a superseded attempt, not a failed attempt. Only RECEIVE sy-subrc <> 0 is a received failure. Superseded attempts must be drained/ignored when their callback arrives.
RETEST=Design closure proof must explicitly distinguish RECEIVED_FAILURE from TIMEOUT_SUPERSEDED and show both in state-transition tests.
```

```text
ID=AR-1-004
SEVERITY=MAJOR
CLAIM=SER-2 §6 "Worker result exceeding transfer limits" is covered by the same oversized-object signal used after successful completion.
COUNTEREXAMPLE=The oversized signal is based on OUTPUT_BYTES in ET_RESULT. If the result exceeds an RFC transfer/data-buffer limit, RECEIVE can fail and ET_RESULT is never available, so OUTPUT_BYTES is never learned and no solo hint is set. Retrying by halving row_limit may still put the same first huge object with companions until retries are exhausted, and the design never says the communication-failure path forces immediate single-object retry before sequential fallback.
EVIDENCE=E-SER2 §2 result row includes OUTPUT_BYTES only after worker success; E-SER2 §6 "One object producing unexpectedly huge output"; E-SER2 §6 "Worker result exceeding transfer limits"; E-SER2 §5 step 7 communication failure retry policy.
IMPACT=availability/performance/fallback correctness
REQUIRED_CHANGE=On RECEIVE communication/system failure after a batch was dispatched, retry policy must split deterministically: if batch size > 1, bisect down to single-object tasks independent of OUTPUT_BYTES; if a single-object RFC still fails transfer, route that object to in-process sequential serialization and log the object identity. Do not rely on OUTPUT_BYTES for failures where no result was received.
RETEST=Fake RECEIVE failure for a multi-object batch, assert retry is split until solo; fake solo transfer failure, assert sequential fallback processes exactly that object once.
```

```text
ID=AR-1-005
SEVERITY=MAJOR
CLAIM=SER-2 §3 says "No correctness ever depends on estimate accuracy" because estimates only affect scheduling order and batch size.
COUNTEREXAMPLE=The estimate is also used to decide whether adding an object exceeds `c_max_batch_input_bytes_est` and to enforce `c_max_in_flight_bytes`. Actual import payload size includes per-batch provider buffers (DOKIL prefix ranges, OO descriptions, DDIC fixed values) that may be much larger than the type-family estimate. Underestimation can dispatch an import payload that fails before the worker can serialize anything, which changes the failure path and can trigger the timeout/late-callback hazards above. This is not merely imbalance.
EVIDENCE=E-SER2 §3 cost model; E-SER2 §4 planner byte limit; E-SER2 §9 limits table; E-SER3 §6 says providers have no separate budgets and are only implicitly bounded by planner estimates; E-ORT §2 DOKIL uses wildcard/prefix ranges that can return variable row counts.
IMPACT=memory/RFC payload bounds/correctness via failure path
REQUIRED_CHANGE=Separate estimated output scheduling from actual dispatch payload admission. Measure actual serialized sizes of provider buffers before CALL FUNCTION; enforce a hard `xstrlen(iv_prefetch_buffer*)` aggregate limit; if actual buffer exceeds the limit, split the batch before dispatch. Revise the safety claim to "wrong estimates cannot change serialized content after a task is successfully admitted and received."
RETEST=Provider-buffer stress test where one object has many DOKIL/fixed-value rows; assert planner splits before RFC dispatch based on actual xstring length, not only estimate.
```

```text
ID=AR-1-006
SEVERITY=MAJOR
CLAIM=SER-2 §6 "One object producing unexpectedly huge output" says the solo hint protects remaining unprocessed objects by flagging the TYPE after an oversized object completes.
COUNTEREXAMPLE=The first huge object is already batched and dispatched before its true output size is known. If a type is bimodal, flagging the whole TYPE after one oversized object can either arrive too late to prevent the first transfer failure, or over-correct by solo-batching many normal objects of the same type. The mechanism is useful as telemetry, but the design overclaims it as bounded protection.
EVIDENCE=E-SER2 §3 says estimates are type-keyed only; E-SER2 §6 "Bimodal CLAS costs"; E-SER2 §6 "One object producing unexpectedly huge output"; E-SER0 §6 says object TYPE alone is not a sufficient weight predictor.
IMPACT=performance/memory bounds/tail latency
REQUIRED_CHANGE=Reframe solo hint as best-effort only. Add an object-level remaining-work rule where possible: once a specific object name produces or fails as oversized, only that object name is solo if it recurs; type-level shrinking should reduce max companions but not force all remaining type members to solo unless repeated evidence crosses a threshold.
RETEST=Cost-model test with one oversized CLAS and many normal CLAS; assert subsequent CLAS batches shrink but do not all become singletons unless the threshold is met.
```

```text
ID=AR-1-007
SEVERITY=MAJOR
CLAIM=Performance design §2 "fall through to the completely unchanged standard path" after catching any orchestrator exception is safe and has no partial-orchestrator-state to clean up in the caller.
COUNTEREXAMPLE=The hook is inserted after the existing ORTEC prefetch prepare calls. SER-2/SER-3 also say the orchestrator has its own CLEANUP that clears provider state on any exit. If the orchestrator throws after clearing provider/static prefetch state, the caller falls through into the existing standard loop without re-running prepare. Output likely remains correct due to miss -> standard SELECT, but the design's claim of unchanged standard behavior is false: fallback now runs with emptied ORTEC prefetch buffers and can silently lose the intended prefetch performance for the whole fallback path. If the orchestrator serialized/logged forced_sequential objects before throwing, the design also does not specify whether those side effects are discarded or duplicated by fallback.
EVIDENCE=E-PERF §2 anchor places hook after `ser_pref*=>prepare`; E-PERF §2 catch falls through; E-PERF §3 says orchestrator CLEANUP clears providers; E-SER3 §7 says exception mid-run clear fires; E-ORT §6 call site shows standard run_parallel extracts from ser_pref* buffers prepared before the loop.
IMPACT=fallback semantics/performance regression/side effects
REQUIRED_CHANGE=Define fallback boundary. Either orchestrator exceptions before first object only may fall through, or the catch must re-run the three existing prepare calls before entering the standard loop, and orchestrator must not externally log/merge partial object results before success. If partial progress exists, do not fall through blindly; use an explicit object-level retry/fallback list.
RETEST=Injected exception after provider cleanup but before first batch; assert standard fallback still has prefetch hit path or explicitly documents/accepts miss. Inject exception after forced_sequential object; assert no duplicate log/result effects.
```

```text
ID=AR-1-008
SEVERITY=MAJOR
CLAIM=Performance design §2 says duplicating `is_no_parallel`/`lv_max=1` routing is a small, stable, safe mirror.
COUNTEREXAMPLE=The exact unsafe set is owned by the standard serializer. If the standard method changes later because a type becomes unsafe (enqueue, generation, dialog, or shared global state), the ORTEC orchestrator can continue batching it because the mirror is stale. That is a correctness risk, not just maintainability, because generic batching is allowed for any type not in the denylist.
EVIDENCE=E-PERF §2 says orchestrator mirrors routing instead of calling `is_no_parallel`; E-SER2 §10 allows generic batch RFC for any parallel-safe object type not in the denylist; E-SER0 §1 identifies `is_no_parallel` as the current routing control.
IMPACT=compatibility/concurrency/stale mirror
REQUIRED_CHANGE=Make the standard denylist the single source of truth. Preferred: expose a protected/public static predicate or FRIENDS seam with a narrow wrapper and a unit test pinning parity. If duplication is retained, add a mandatory test that compares ORTEC routing against standard `is_no_parallel` for every object type in the registry and fails on divergence.
RETEST=Add a synthetic object type to the standard denylist in a test seam; assert ORTEC partition routes it forced_sequential.
```

```text
ID=AR-1-009
SEVERITY=MINOR
CLAIM=SER-3 §6 says provider output size needs no separate cap because planner row/input estimate limits implicitly bound it.
COUNTEREXAMPLE=A batch with few rows can still export a very large provider buffer if one row has many DOKIL/fixed-value/description records. Row count and estimated bytes are not a hard bound on actual provider xstring size.
EVIDENCE=E-SER3 §6; E-ORT §2 DOKIL prefix range behavior; E-SER2 §9 byte limit is estimated.
IMPACT=memory/RFC payload observability
REQUIRED_CHANGE=Add a provider aggregate xstring length counter and log/split threshold. This can share AR-1-005's actual-dispatch-admission fix.
RETEST=Provider extraction unit test with one row producing a large buffer; assert limit is observed before dispatch.
```

```text
ID=AR-1-010
SEVERITY=MINOR
CLAIM=SER-2 §6 "Cancellation" says no mechanism is designed because the standard cancellation flag is unknown.
COUNTEREXAMPLE=The design still introduces a bounded poll loop that is the only practical place to stop dispatching. Leaving the cancellation contract undecided means implementation can accidentally keep queueing new batches after a user abort/cancel signal becomes available later.
EVIDENCE=E-SER0 §7 G-6; E-SER2 §6 "Cancellation"; E-SER2 §5 step 6 poll loop.
IMPACT=operability/user cancellation
REQUIRED_CHANGE=Add an explicit extension point now: `should_cancel( )` default false, checked once per poll and before every refill dispatch. If no standard cancellation source exists, implementation wires false, but the behavior is decision-free.
RETEST=Test seam returns true after one callback; assert no new batches dispatch and in-flight tasks drain.
```

```text
ID=AR-1-011
SEVERITY=MINOR
CLAIM=SER-2 §8 telemetry via `ii_log->add_success`/debug at end is enough for SER-4 measurement.
COUNTEREXAMPLE=If a run fails or falls back mid-run, end-of-run telemetry may never be written, exactly when retry/timeout/provider data is most needed. Since no persistence is intended, losing failure telemetry weakens the measured expansion process and incident diagnosis.
EVIDENCE=E-SER2 §8; E-PERF §6 SER-4 depends on telemetry; E-SER2 §5 step 7 has multiple failure/fallback branches.
IMPACT=observability/post-failure diagnosis
REQUIRED_CHANGE=Emit compact telemetry incrementally on batch close/failure and once in CLEANUP for partial aggregates, guarded by log level to avoid noise.
RETEST=Injected exception mid-run; assert log contains partial retry/timeout/provider counters.
```

## Explicit Passes / Non-Findings

```text
ID=PASS-1
SECTION=SER-1 headline finding and decision
CLAIM=SER-2 does not need its own existence check for CLAS/INTF/DTEL/DOMA in the prototype.
RESULT=PASS
EVIDENCE=E-BULK states `zcl_abapgit_tadir=>check_exists` resolves existing rows before serialization and existing ORTEC bulk-exists already covers the prototype types; E-SER0 §2 confirms the current bulk-exists fallback shape.
```

```text
ID=PASS-2
SECTION=SER-3 §5 facade over static CLASS-DATA classes; SER-3 §7 RFC worker session reuse
CLAIM=An instance facade over static `ser_pref*` classes is intrinsically unsafe for parallel RFC workers.
RESULT=PASS_WITH_REQUIRED_CLARIFICATION
EVIDENCE=E-ORT §2-4 confirms worker injection clears CLASS-DATA before insert; E-SER3 §7 preserves this clear-before-insert pattern. ABAP parallel RFC workers execute in separate ABAP sessions, so static CLASS-DATA is not shared across workers. Within the main session, facade instances intentionally share the existing run-scoped cache.
REQUIRED_CLARIFICATION=State this ABAP session boundary explicitly, because the current text says singleton/facade but does not spell out that instance isolation is not the safety mechanism; clear-before-insert plus per-worker ABAP session isolation is.
```

```text
ID=PASS-3
SECTION=SER-5 disposition
CLAIM=WAPA should not be provider-wrapped in the first prototype.
RESULT=PASS
EVIDENCE=E-WAPA says WAPA has zero direct tests and requires T-WAPA-1..5 before provider wrapping; E-SER3 §2 explicitly excludes WAPA/BSP from the prototype provider set.
```

```text
ID=PASS-4
SECTION=SER-2 §2 worker body contract
CLAIM=One object exception inside a batch should not discard successful sibling rows.
RESULT=PASS
EVIDENCE=E-SER2 §2 catches `zcx_abapgit_exception` inside the LOOP and appends one ET_RESULT row per input object; this is structurally better than batch-level exception for object-level failures.
```

## Verdict

```text
VERDICT=REVISE_AND_REVIEW_ONCE
OPEN_BLOCKER=2 (AR-1-001, AR-1-002)
OPEN_MAJOR=6 (AR-1-003, AR-1-004, AR-1-005, AR-1-006, AR-1-007, AR-1-008)
OPEN_MINOR=3 (AR-1-009, AR-1-010, AR-1-011)
RATIONALE=The design has a plausible performance direction and several sound scope reductions, but the async RFC task identity/timeout/retry contract is not yet decision-free or race-safe. Fixing AR-1-001 and AR-1-002 should be followed by one focused review pass over the revised lifecycle/state machine.
```

# Cycle 2

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SERIALIZATION_ADVERSARIAL_REVIEW_CYCLE2
CYCLE=2
STATUS=PASS_WITH_FINDINGS
VERDICT=REVISE_AND_REVIEW_ONCE
BLOCKER_COUNT=2
MAJOR_COUNT=2
MINOR_COUNT=0
```

## Scope And Evidence

Read-only review performed against the requested Cycle 2 packet:

- E-C1: `.memory/reviews/serialization_adversarial_review.md` Cycle 1 findings AR-1-001 through AR-1-011
- E-SER0: `.memory/logs/serialization_ser0_audit.md`
- E-PERF2: `.memory/logs/serialization_performance_design.md` revised sections 2, 3, 5
- E-SER2R: `.memory/logs/serialization_adaptive_batch_design.md` revised sections 3, 5, 6, 8, 9
- E-SER3R: `.memory/logs/serialization_provider_design.md` revised sections 3, 6, 7
- E-WAPA: `.memory/logs/serialization_wapa_review.md`

No productive ABAP, state file, archive file, diagram, or other memory file was modified except this required append to the output artifact.

## Cycle-1 BLOCKER/MAJOR Dispositions

```text
ID=AR-1-001
DISPOSITION=REJECTED_WITH_PROOF
CLAIM=Revised SER-2 §5.4/§5.5 now makes a timed-out original attempt safe by keeping it in mt_dispatch as state 'T' and draining its late callback.
COUNTEREXAMPLE=Dispatch B1, mark it TIMED_OUT, release its in-flight budget, and dispatch retry B2. B2 succeeds and inserts every object into mt_resolved. At that point SER-2 §5.3's concrete exit condition (`planner queue empty AND mt_dispatch has no 'A' rows`) is true even though B1 remains in state 'T' and has not been drained. serialize() can return and CLEANUP can release mt_dispatch before the genuinely-still-running original B1 callback arrives. The revised text therefore still does not guarantee the late original is recognized and drained; it only guarantees that if the callback arrives before the loop exits.
EVIDENCE=E-SER2R §5.3 poll-loop exit condition; E-SER2R §5.4 says TIMED_OUT dispatch stays in mt_dispatch; E-SER2R §5.5 drains only while mt_dispatch still exists and p_task can be read.
IMPACT=concurrency/RFC-lifecycle/resource-drain/late-callback semantics
REQUIRED_CHANGE=Make loop termination account for unresolved timed-out dispatches explicitly. Either keep a bounded drain phase for 'T' attempts after all objects are resolved, or state and prove an ABAP aRFC contract that callbacks for still-running superseded tasks cannot execute after serialize() returns and do not require RECEIVE. If the design chooses bounded drain, terminal exit must be based on all work resolved AND every non-abandoned dispatch state being R/F/D, with a documented abandonment state that has no callback side effects.
RETEST=Simulate B1 timeout, B2 success, then delay B1 callback until after the retry has resolved all objects; assert the orchestrator does not return before the B1 lifecycle reaches DRAINED or a separately specified ABANDONED terminal state with no observable callback effect.
```

```text
ID=AR-1-002
DISPOSITION=REJECTED_WITH_PROOF
CLAIM=Revised SER-2 §5.1/§5.5 task_name identity closes the callback identity/correlation gap.
COUNTEREXAMPLE=The revised callback lookup is now keyed by real `p_task`, which fixes stale task-name reuse, but it still does not validate the returned result set against the dispatch fingerprint before merge. The FM signature imports IV_BATCH_ID/IV_ATTEMPT/IV_INPUT_ROW_COUNT into the worker but does not export them back, and §5.5 does not compare EV_OUTPUT_ROW_COUNT or every ET_RESULT object key against `<ls_d>-object_keys`. A legitimate callback for task T can therefore return a malformed result set with one missing requested object and one extra/stale object row; §5.5 will merge the extra row if not already in mt_resolved, never mark the missing requested object, and §5.3 can exit once there are no A rows.
EVIDENCE=E-SER2R §2 signature exports only ET_RESULT and EV_OUTPUT_ROW_COUNT; E-SER2R §5.1 stores object_keys but no fingerprint/checksum; E-SER2R §5.5 only checks mt_resolved duplicate presence, not membership/completeness of lt_result against object_keys.
IMPACT=identity canonicalization/result-correlation/missing-or-wrong-output
REQUIRED_CHANGE=Add a returned correlation envelope or callback-side validation before any merge: verify batch_id, attempt, input_version, input_row_count, output_row_count, and exact object-key set equality against `<ls_d>-object_keys`; reject and handle as RECEIVE failure/fallback if any mismatch exists. mt_resolved remains a duplicate guard, not the primary result-integrity check.
RETEST=Worker seam returns ET_RESULT with one requested object omitted and one non-requested object included; assert no merge occurs, the original requested object is routed to fallback, and the non-requested object is never added to mt_files or mt_resolved.
```

```text
ID=AR-1-003
DISPOSITION=ACCEPTED_AND_FIXED
EVIDENCE=E-SER2R §5.4 states `<ls_d>-state = 'T'. " TIMED_OUT, not "failed"`; E-SER2R §6 says `a timeout NEVER produces a "confirmed failure" - it produces a superseded (TIMED_OUT) attempt`; E-SER2R §5.5 `WHEN 'T'` drains and returns without merge or cost update.
```

```text
ID=AR-1-004
DISPOSITION=ACCEPTED_AND_FIXED
EVIDENCE=E-SER2R §5.6 says RECEIVE failure `ALWAYS bisects deterministically` and `never rely on OUTPUT_BYTES`; for a single object it routes to `route_to_sequential_fallback`; E-SER2R §6 repeats that a transfer-limit failure surfaces as RECEIVE failure and is bisected regardless of whether OUTPUT_BYTES was known.
```

```text
ID=AR-1-005
DISPOSITION=REJECTED_WITH_PROOF
CLAIM=Revised SER-2 §5.9 and SER-3 §6 provide a hard actual-bytes admission gate before RFC dispatch.
COUNTEREXAMPLE=The revised §5.9 calls the gate hard, but its own pseudocode says recursive bisection is bounded by `c_max_pre_dispatch_splits` and then gives up by dispatching the largest remaining half anyway; it also says a one-object batch is always dispatched regardless of actual provider-buffer size. A batch or single object with provider buffers above c_max_actual_batch_bytes can therefore still be sent over aRFC, exactly contradicting the hard-admission claim and reintroducing the oversized transfer failure before the worker can return ET_RESULT.
EVIDENCE=E-SER2R §5.9 `bounded to c_max_pre_dispatch_splits ... before giving up and dispatching the largest remaining half anyway`; E-SER2R §5.9 `a batch of 1 object is ALWAYS dispatched regardless of its actual provider-buffer size`; E-SER3R §6 calls c_max_actual_batch_bytes the authoritative hard bound.
IMPACT=memory/RFC-payload-bound/availability
REQUIRED_CHANGE=Remove the give-up dispatch path. Since c_max_batch_rows is 25, split-to-single is naturally bounded; continue bisection until either every sub-batch is under the hard byte limit or it is a single object. If a single object's provider buffer still exceeds c_max_actual_batch_bytes, do not dispatch it via RFC; route it directly to in-process sequential fallback with provider export disabled/unneeded and log the object identity.
RETEST=Provider seam returns an oversized buffer for a 25-object batch after three splits; assert no over-limit RFC dispatch occurs. Provider seam returns an oversized buffer for one object; assert the object is serialized sequentially exactly once and no batch RFC call is made.
```

```text
ID=AR-1-006
DISPOSITION=ACCEPTED_AND_FIXED
EVIDENCE=E-SER2R §6 explicitly reframes oversized output as `best-effort telemetry, not a bounded guarantee`, states the first oversized object is never protected by this mechanism, and only shrinks a type after `c_oversized_threshold` occurrences.
```

```text
ID=AR-1-007
DISPOSITION=ACCEPTED_AND_FIXED
EVIDENCE=E-PERF2 §2 says the hook catch can only be reached when zero objects have been resolved; partial progress is handled internally by routing unresolved objects to sequential fallback and returning normally. E-PERF2 §3 and E-SER3R §3/§7 state provider cleanup remains exclusively owned by the outer existing CLEANUP block and the orchestrator never calls provider clear().
```

```text
ID=AR-1-008
DISPOSITION=ACCEPTED_AND_FIXED
EVIDENCE=E-PERF2 §2 OD-6 withdraws the duplicated mirror and requires the orchestrator to call the real `zcl_abapgit_serialize=>is_no_parallel` predicate after a behavior-free visibility change.
```

## New Cycle-2 Findings

```text
ID=AR-2-001
SEVERITY=MAJOR
CLAIM=The new circuit breaker in SER-2 §5.8 mirrors today's mv_parallel_broken behavior and terminates systemic RFC/server-group outage cheaply.
COUNTEREXAMPLE=§5.8 increments the breaker once per DISTINCT dispatch but explicitly not per retry of the SAME logical group. §5.6 says a RECEIVE failure bisects a failed group into halves and each half gets a fresh attempt budget, while relying on the global circuit breaker for systemic outage termination. Under a full server-group outage, one 25-object original group can produce a failed root, failed halves, failed quarters, and failed singles before fallback, but if all are classified as retries/bisections of the same logical group they do not advance the breaker beyond one. The design then regresses from today's immediate `mv_parallel_broken`-style degrade and can burn O(batch size) failing aRFC dispatches per original group before sequential fallback.
EVIDENCE=E-SER2R §5.6 `each half gets its own fresh attempt budget` and systemic outage terminates via global circuit breaker; E-SER2R §5.8 increments once per DISTINCT dispatch `not per retry of the SAME logical group`; E-SER0 §1 says existing baseline degrades to sequential for the rest of the run on RFC communication/system failure.
IMPACT=availability/performance/failure-amplification
REQUIRED_CHANGE=Define breaker accounting by actual RFC task failure, not ambiguous logical group, or add a separate systemic-failure counter that increments on every confirmed RECEIVE failure task_name. After threshold, stop all new batch dispatches and route all unresolved work to sequential fallback, including queued bisection halves not yet started.
RETEST=Simulate communication_failure for every RFC RECEIVE in a 25-object initial batch; assert no more than c_circuit_breaker_threshold failing RFC task callbacks occur before all remaining unresolved objects are routed sequentially.
```

## Verdict

```text
VERDICT=REVISE_AND_REVIEW_ONCE
OPEN_BLOCKER=2 (AR-1-001, AR-1-002)
OPEN_MAJOR=2 (AR-1-005, AR-2-001)
OPEN_MINOR=0
RATIONALE=The revision closes the original timeout-as-failure wording, transfer-failure bisection, oversized-output overclaim, fallback-boundary, and stale-denylist findings. It still does not fully close the race-safe lifecycle because timed-out tasks can remain undrained at loop exit, result-set identity is not validated before merge, and the actual-bytes gate is explicitly allowed to dispatch over-limit payloads. The new circuit breaker also needs unambiguous per-task accounting to avoid failure amplification under systemic outage.
```

# Cycle 3

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SERIALIZATION_ADVERSARIAL_REVIEW_CYCLE3
CYCLE=3
STATUS=BLOCKED
VERDICT=OWNER_DECISION_REQUIRED
BLOCKER_COUNT=1
MAJOR_COUNT=0
MINOR_COUNT=0
```

## Scope And Evidence

Read-only review performed against the requested Cycle 3 packet:

- E-C1C2: `.memory/reviews/serialization_adversarial_review.md` Cycle 1 and Cycle 2 findings
- E-SER0: `.memory/logs/serialization_ser0_audit.md`
- E-SER2C3: `.memory/logs/serialization_adaptive_batch_design.md` revised sections 5.3, 5.4, 5.5, 5.8, 5.9
- E-PERF3: `.memory/logs/serialization_performance_design.md`
- E-SER3C3: `.memory/logs/serialization_provider_design.md`
- E-WAPA: `.memory/logs/serialization_wapa_review.md`

No productive ABAP, state file, archive file, diagram, or other memory file was modified except this required append to the output artifact.

## Required Open-Finding Dispositions

```text
ID=AR-1-001
DISPOSITION=REJECTED_WITH_PROOF
CLAIM=Cycle-3 SER-2 §5.3/§5.4 closes the timeout lifecycle by blocking loop exit on 'T' and then using bounded 'X' ABANDONED to guarantee eventual return without observable callback side effects.
COUNTEREXAMPLE=Dispatch B1 and let its RFC worker hang or run beyond the caller's patience. At c_batch_rfc_timeout_s, check_timeouts changes B1 from 'A' to 'T', releases the in-flight budget, and resubmits or falls back its object set. Because the new second timeout loop compares the same original dispatch_ts against c_max_drain_wait_s, with the documented defaults equal at 300s, B1 can become 'X' in the same poll pass that first marks it 'T' rather than after the documented total ≈600s wait. §5.3 may then exit and serialize() may run CLEANUP, releasing mt_dispatch. If B1 never calls back, no RECEIVE ever occurs, so the design has not proven the aRFC resource is drained. If B1 calls back only after serialize() has returned, §5.4's claim that the 'T'/'X' dispatch stays in mt_dispatch for recognition is no longer guaranteed by the design because the orchestrator cleanup has released that run-local table. The revision therefore guarantees caller return by abandoning the task, but still does not prove the old RFC result is drained or that post-return callback delivery has no resource or lifecycle side effect.
EVIDENCE=E-SER2C3 §5.3 exits when no 'A' and no 'T' rows remain; E-SER2C3 §5.4 marks 'T' rows 'X' using dispatch_ts and says 'X' no longer blocks loop exit; E-SER2C3 §5.4 also says the dispatch stays in mt_dispatch so late arrival can be recognized; E-PERF3 §3 says the orchestrator CLEANUP releases mt_dispatch; E-SER0 §7 G-5 says no hidden timeout/late-callback guard exists today.
IMPACT=concurrency/RFC-lifecycle/resource-drain/post-return-callback semantics
REQUIRED_CHANGE=Owner must choose and document one executable contract before implementation: either prove from the ABAP aRFC runtime that abandoning a task after c_max_drain_wait_s without RECEIVE cannot leak resources and cannot invoke a callback after the callback context is gone, or keep serialize() from returning until every started task has reached a RECEIVEd terminal state, or route this design through an SAP-supported cancellation/abort mechanism with a proven cleanup callback. If the bounded-abandon path remains, record the ABAP runtime proof and make the timestamp logic use a separate timed_out_ts/abandoned_ts so the documented drain window is actually applied after timeout, not from original dispatch.
RETEST=Seam or integration proof for three cases: (1) B1 times out, B2 resolves, B1 callback arrives before return and is RECEIVEd/discarded; (2) B1 never calls back and serialize() returns only under an owner-approved no-leak abandon contract; (3) B1 callback arrives after serialize() return and is either impossible by proved runtime contract or still safely RECEIVEd without access to freed run-local state.
```

```text
ID=AR-1-002
DISPOSITION=ACCEPTED_AND_FIXED
EVIDENCE=E-SER2C3 §5.5 now validates `lv_out_rows = lines( <ls_d>-object_keys )` and `object_key_sets_equal( lt_result, <ls_d>-object_keys )` before any merge; mismatches transition the dispatch to 'F', release budget, record a failed task outcome, log a warning, and route exactly the requested object_keys to sequential fallback. The `mt_resolved` check remains only a secondary duplicate guard.
```

```text
ID=AR-1-005
DISPOSITION=ACCEPTED_AND_FIXED
EVIDENCE=E-SER2C3 §5.9 removes the c_max_pre_dispatch_splits give-up path, continues splitting multi-object groups until under c_max_actual_batch_bytes or n=1, and sends an oversized singleton with no provider buffers attached so the worker falls back to standard per-object reads instead of receiving an over-limit provider payload. E-SER3C3 §6 points to this actual xstrlen gate as the authoritative provider-buffer bound.
```

```text
ID=AR-2-001
DISPOSITION=ACCEPTED_AND_FIXED
EVIDENCE=E-SER2C3 §5.8 records one outcome per confirmed RFC task, including every bisection half and retry, with no batch_id/logical-group deduplication; when the breaker trips it stops all new batch dispatches including queued bisection halves and routes all unresolved work to sequential fallback. E-SER2C3 §5.6 explicitly states systemic outage trips within c_breaker_min_sample confirmed task failures, not O(batch size).
```

## Specific Cycle-3 Stress Tests

```text
ID=DRAIN-STRESS-3
RESULT=FAILS_RESOURCE_DRAIN_PROOF
CLAIM=The new 'X' ABANDONED state plus c_max_drain_wait_s guarantees serialize() eventually returns even if an RFC task never calls back at all.
EVIDENCE=E-SER2C3 §5.3/§5.4
DETAIL=It does guarantee caller return in the pseudocode, because 'X' is terminal for loop exit. It does not guarantee that the abandoned aRFC task has been RECEIVEd, cancelled, or otherwise released. Therefore it closes the infinite wait symptom but not the resource-lifecycle proof required for a true stuck worker.
```

```text
ID=BREAKER-RATIO-3
RESULT=NO_MAJOR_ISSUE
CLAIM=The per-task sliding-window breaker might suffer an integer-division or off-by-one flaw in ABAP.
EVIDENCE=E-SER2C3 §5.8; E-SER2C3 §9
DETAIL=ABAP integer division is `DIV`; `/` is not the integer-division operator, so the written ratio expression is not inherently an integer-truncation bug. The min-sample/window behavior is also internally consistent: with defaults, 4/5 failures trips because 80% >= 70%, and a full outage trips at five confirmed task failures. Implementation should still type `c_breaker_failure_ratio` explicitly as a decimal/decfloat value or use an integer comparison such as failures * 10 >= lines * 7, but this is not a BLOCKER/MAJOR design defect.
```

## Final Verdict

```text
VERDICT=OWNER_DECISION_REQUIRED
OPEN_BLOCKER=1 (AR-1-001)
OPEN_MAJOR=0
OPEN_MINOR=0
CLOSED=AR-1-002, AR-1-005, AR-2-001
RATIONALE=Cycle 3 closes the result-set identity check, actual provider-byte admission gate, and per-task circuit-breaker accounting. The remaining unresolved issue is the final-cycle RFC lifecycle contract: the design now bounds caller wait by marking 'X', but it does not prove safe resource cleanup or post-return callback behavior for a task that never reaches RECEIVE. Under the orchestrator's three-cycle policy, this requires owner decision before implementation.
```

# Cycle 4

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SERIALIZATION_ADVERSARIAL_REVIEW_CYCLE4
CYCLE=4
STATUS=BLOCKED
VERDICT=OWNER_DECISION_REQUIRED
BLOCKER_COUNT=1
MAJOR_COUNT=1
MINOR_COUNT=0
```

## Scope And Evidence

Read-only review performed against the owner-authorized Cycle 4 packet:

- E-C3: `.memory/reviews/serialization_adversarial_review.md` Cycle 3 AR-1-001 rejection text/counterexample
- E-SER0: `.memory/logs/serialization_ser0_audit.md`
- E-SER2C4: `.memory/logs/serialization_adaptive_batch_design.md` revised §5.1a plus surrounding §5.1-§5.5 and cleanup/telemetry text
- E-PERF4: `.memory/logs/serialization_performance_design.md` revised hook/OD-13 plus memory cleanup text

No productive ABAP, state file, archive file, diagram, or other memory file was modified except this required append to the output artifact.

## Required Open-Finding Disposition

```text
ID=AR-1-001
DISPOSITION=REJECTED_WITH_PROOF
CLAIM=NEW SER-2 §5.1a closes AR-1-001 because a fresh per-serialize orchestrator instance registered via CALLING instance_method ON END OF TASK is retained by the ABAP runtime, so late callbacks after serialize() returns always find the same still-populated mt_dispatch and can RECEIVE/drain the abandoned task.
COUNTEREXAMPLE=Dispatch B1, let it time out to 'T', retry/fallback its objects, then let c_max_drain_wait_s mark B1 'X' so the poll loop exits and zcl_abapgit_ortec_ser_orch->serialize returns. Even if ABAP's callback registration keeps the orchestrator object itself alive, the allowed packet still specifies that the orchestrator's own CLEANUP block releases its own structures, explicitly including mt_dispatch and mt_resolved, on any exit path. A later B1 callback can therefore enter a retained object whose dispatch table was intentionally released by the method cleanup; the callback cannot perform the promised READ TABLE mt_dispatch ... find 'T'/'X' row ... RECEIVE/discard path. The new §5.1a proves, at most, object identity/lifetime; it is contradicted by the design's own cleanup contract for the state that must survive.
EVIDENCE=E-SER2C4 §5.1a says the retained object has valid, still-populated mt_dispatch after serialize() returns; E-SER2C4 §5.5 requires mt_dispatch lookup to identify and drain 'T'/'X' callbacks; E-SER2C4 "Exception before cleanup" says the orchestrator CLEANUP releases mt_dispatch and mt_resolved; E-PERF4 §3 says the orchestrator TRY...CLEANUP releases planner queues, mt_dispatch, mt_resolved, and in-flight budget counters on any exit path; E-C3 required proof that post-return callback either is impossible or still safely RECEIVEd without access to freed run-local state.
IMPACT=concurrency/RFC-lifecycle/resource-drain/post-return-callback semantics
REQUIRED_CHANGE=Amend the executable contract so abandoned in-flight dispatch records needed for late RECEIVE are not cleared while their callback can still be triggered. Either split cleanup into immediate per-run result cleanup plus retained minimal callback registry, with explicit post-DRAINED self-pruning, or remove the bounded-abandon return path and wait until every started task reaches a RECEIVEd terminal state. The design must explicitly state which instance data survives method return, which data is cleared, and when retained late-callback state is finally released.
RETEST=Induced-hang IT8/runtime seam: B1 times out and becomes X, serialize() returns, orchestrator method cleanup has executed, then B1 completes and its callback fires; assert RECEIVE executes exactly once, the old result is discarded, no dump occurs, no mt_files/mt_resolved duplicate occurs, and the retained callback registry is freed after DRAINED.
```

## Additional Cycle-4 Stress Results

```text
ID=AR-4-001
SEVERITY=MAJOR
CLAIM=SER-2 §5.1 says `a simple GUID or sy-uzeit+monotonic-counter concatenation is sufficient` for lv_run_id, while §5.1a says different orchestrator instances cannot collide or contaminate one another even in principle.
COUNTEREXAMPLE=If the implementer chooses the permitted `sy-uzeit+monotonic-counter` form and the monotonic counter is per fresh orchestrator instance, two sequential serialize() calls in the same ABAP session and same second can produce the same lv_run_id and the same initial lv_dispatch_seq values while the first call still has an abandoned outstanding RFC task. This does not necessarily merge into the wrong instance if the callback object binding works, but it can collide at the aRFC task-name/resource layer or make p_task non-unique in diagnostics. The text overclaims impossibility while leaving a non-unique identity option open.
EVIDENCE=E-SER2C4 §5.1 task_name = SER-{lv_run_id}-{lv_dispatch_seq}; E-SER2C4 §5.1 permits sy-uzeit+monotonic-counter; E-SER2C4 §5.1a relies on per-instance isolation and no cross-run contamination even in principle.
IMPACT=identity canonicalization/aRFC task identity/diagnostics
REQUIRED_CHANGE=Require lv_run_id to be generated by a collision-resistant SAP GUID (for example CL_SYSTEM_UUID) or by a session-global/class-level monotonic source proven unique across all live orchestrator instances in the internal session; remove the ambiguous sy-uzeit+per-instance-counter option.
RETEST=Create two orchestrator instances in the same second with an abandoned outstanding task in the first; assert their generated task_name sets are disjoint before any dispatch is attempted.
```

```text
ID=VERIFICATION_NEEDED-4-001
SCOPE=ABAP runtime premise, not a design-level rejection by itself
QUESTION=Confirm the exact ABAP keyword-documentation/runtime contract for CALL FUNCTION ... STARTING NEW TASK ... CALLING instance_method ON END OF TASK: the registered object is retained until the handler can no longer be triggered, and a callback after the dispatching method returns can still run at a later synchronization point in the same session.
PROOF=Small SAP Basis/ABAP-runtime test: create a local orchestrator instance with only a weak/no remaining application reference after method return, dispatch an aRFC task with CALLING me->handler ON END OF TASK, force garbage collection if possible, then complete the worker and process the next synchronization point; assert the instance method fires and can execute RECEIVE.
```

```text
ID=VERIFICATION_NEEDED-4-002
SCOPE=Resource-lifecycle confirmation after the cleanup contradiction is fixed
QUESTION=Confirm that a post-return late callback can still execute RECEIVE and free the underlying aRFC/gateway resource after the caller has abandoned waiting, provided the retained callback registry is not cleared.
PROOF=IT8 induced-hang test behind a test-only feature flag: timeout to X, return from serialize(), complete the worker later in the same session, observe one RECEIVE in the callback, no dump, no leaked task in SM58/SMQS-equivalent monitoring, and no contamination of a later serialize() call.
```

## Final Verdict

```text
VERDICT=OWNER_DECISION_REQUIRED
OPEN_BLOCKER=1 (AR-1-001)
OPEN_MAJOR=1 (AR-4-001)
OPEN_MINOR=0
CLOSED=none
RATIONALE=The new §5.1a is a plausible direction for object-lifetime safety if the ABAP runtime premise is empirically confirmed, but it does not close AR-1-001 as written because the same allowed packet still clears mt_dispatch/mt_resolved in the orchestrator cleanup. Object retention without retained dispatch state is insufficient for the promised late RECEIVE/drain path, and the task-name uniqueness text also leaves a cross-instance collision option open.
```

# Cycle 5

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SERIALIZATION_ADVERSARIAL_REVIEW_CYCLE5
CYCLE=5
STATUS=PASS
VERDICT=APPROVE
BLOCKER_COUNT=0
MAJOR_COUNT=0
MINOR_COUNT=0
```

## Scope And Evidence

Read-only review performed against the owner-authorized Cycle 5 packet:

- E-C4: `.memory/reviews/serialization_adversarial_review.md` Cycle 4 AR-1-001 rejection, AR-4-001, VERIFICATION_NEEDED-4-001, and VERIFICATION_NEEDED-4-002
- E-PERF5: `.memory/logs/serialization_performance_design.md` revised §3 Cleanup guarantee, Resource vs. object distinction, and OD-13 verification gate
- E-SER2C5: `.memory/logs/serialization_adaptive_batch_design.md` revised §5.1a, §5.1 identity, and §6 Exception before cleanup

No productive ABAP, state file, archive file, diagram, or other memory file was modified except this required append to the output artifact.

## Required Open-Finding Dispositions

```text
ID=AR-1-001
DISPOSITION=ACCEPTED_AND_FIXED
CLAIM=Cycle-5 revisions close the Cycle-4 rejection by retaining the instance data needed for late callbacks after serialize() returns, while limiting immediate cleanup to rebuildable planner state.
EVIDENCE=E-PERF5 §3 says the orchestrator CLEANUP releases ONLY the planner dispatch queue and in-flight budget counters, explicitly does NOT clear mt_dispatch, mt_resolved, or mt_task_outcomes, and releases those instance DATA only by normal ABAP garbage collection after the object's async lifecycle is fully drained; E-SER2C5 §6 repeats the same correction for the exception-before-cleanup edge case; E-SER2C5 §5.1a states late callbacks read the still-alive instance mt_dispatch and RECEIVE/discard 'T'/'X' attempts.
REASON=The exact contradiction cited in Cycle 4 is gone: object lifetime and dispatch-state lifetime now align, so a retained callback object still has the callback registry required for the promised late RECEIVE/drain path.
RETEST=Before SLICE 2 implementation begins, perform OD-13 verification step 1 against the target ABAP Keyword Documentation and record the documented GC-exemption behavior; during SLICE 2, execute the induced-hang late-callback test to confirm one RECEIVE/discard path and no cross-run contamination.
```

```text
ID=AR-4-001
DISPOSITION=ACCEPTED_AND_FIXED
EVIDENCE=E-SER2C5 §5.1 requires lv_run_id to be generated once per orchestrator instance via cl_system_uuid=>create_uuid_x16_static(), explicitly removing the sy-uzeit+counter option; E-PERF5 OD-13 records the same GUID requirement and states it removes theoretical task_name collision between live orchestrator instances.
```

```text
ID=VERIFICATION_NEEDED-4-001
DISPOSITION=ACCEPTED_AS_DESIGN_PRECONDITION
EVIDENCE=E-PERF5 OD-13 makes the ABAP Keyword Documentation check the first named verification step and makes the design decision contingent on performing and recording that check before SLICE 2 implementation begins.
REASON=For a design document, an explicit implementation gate against authoritative ABAP documentation is adequate; it is no longer an unstated premise. The empirical IT8 test remains correctly placed as a SLICE 2 confirmation, not as the only possible design closure.
```

```text
ID=VERIFICATION_NEEDED-4-002
DISPOSITION=ACCEPTED_AS_SCOPE_BOUNDARY
EVIDENCE=E-PERF5 §3 Resource vs. object distinction separates two cases: if the worker finishes and the callback is delivered, retained object state lets on_end_of_batch execute RECEIVE; if the worker is truly hung forever, RFC/gateway/work-process reclamation is explicitly a pre-existing SAP Basis/kernel timeout concern identical to the current baseline.
REASON=This is a legitimate non-evasive boundary for the design's claim. The design now claims safe late-drain only for delivered callbacks and does not pretend to solve system-level cleanup of a non-completing worker.
```

## Stale-Contradiction Search

```text
RESULT=PASS
SEARCH_SCOPE=.memory/logs/serialization_performance_design.md and .memory/logs/serialization_adaptive_batch_design.md
TERMS=mt_dispatch, mt_resolved, mt_task_outcomes, CLEANUP/cleanup, clear/release/released, task_name, lv_run_id, sy-uzeit, cl_system_uuid/create_uuid
DETAIL=No remaining current statement says mt_dispatch, mt_resolved, or mt_task_outcomes are cleared or released on orchestrator normal or exceptional return. The remaining hits either state the corrected NOT-clear contract, describe provider cleanup, mention historical Cycle-4 correction text, or discuss unrelated telemetry aggregates.
```

## Final Verdict

```text
VERDICT=APPROVE
OPEN_BLOCKER=0
OPEN_MAJOR=0
OPEN_MINOR=0
CLOSED=AR-1-001, AR-4-001, VERIFICATION_NEEDED-4-001, VERIFICATION_NEEDED-4-002
RATIONALE=Cycle 5 fixes the only Cycle-4 blocking contradiction by preserving the late-callback registry for the orchestrator object's lifetime, turns the ABAP runtime premise into a named pre-implementation documentation gate plus empirical SLICE 2 confirmation, scopes true hung-worker resource reclamation to the existing SAP Basis/kernel baseline, and replaces the weak task-name prefix option with a required real GUID. No new BLOCKER/MAJOR/MINOR finding is opened.
```

# OD-13 Correction

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SERIALIZATION_OD13_CORRECTION_ADVERSARIAL_REVIEW
CYCLE=OD-13 Correction
STATUS=PASS_WITH_FINDINGS
VERDICT=REVISE_AND_REVIEW_ONCE
BLOCKER_COUNT=0
MAJOR_COUNT=2
MINOR_COUNT=0
```

## Scope And Evidence

Read-only review performed against the requested corrected OD-13 packet:

- E-C1C5: `.memory/reviews/serialization_adversarial_review.md` Cycles 1-5, including Cycle 4/5's now-withdrawn instance-lifetime/GC-retention mechanism.
- E-SER2OD13: `.memory/logs/serialization_adaptive_batch_design.md` §5 in full, especially §5.0, §5.1a, §5.1, §5.2, §5.5, §5.7, §5.8, and §5.1b.
- E-PERFOD13: `.memory/logs/serialization_performance_design.md` §2, §3, §3a, and OD-13.
- E-DOC-ARFC: ABAP Keyword Documentation `ABAPCALL_FUNCTION_STARTING`, `ABAPWAIT_ARFC`, `ABAPRECEIVE`, and `ABENMETHOD_CALLS_STATIC` fetched during this review.

No productive ABAP, state file, archive file, or diagram was modified. This section was appended to the requested output artifact only.

## Prior Finding Disposition

```text
ID=AR-1-001
DISPOSITION=ACCEPTED_AND_FIXED
CLAIM=The corrected static CLASS-DATA/CLASS-METHODS ownership closes the original late-callback target/state-lifetime gap without relying on the withdrawn instance-GC premise.
EVIDENCE=E-SER2OD13 §5.1a sets DOC_PROVES_CALLBACK_TARGET_RETENTION=NO, withdraws the cycle-4/5 SET-HANDLER analogy, and replaces it with `CLASS-DATA mt_dispatch/mt_resolved/mt_task_outcomes` plus PUBLIC STATIC `on_end_of_batch`; E-SER2OD13 §5.2 registers `CALLING zcl_abapgit_ortec_ser_orch=>on_end_of_batch ON END OF TASK`; E-DOC-ARFC confirms aRFC callback delivery is scoped to the calling program still existing in its internal session, and `WAIT ... UP TO` does not cancel later callback execution; static data/method lifetime is therefore aligned with the documented internal-session callback scope and has no object-GC ambiguity.
REASON=The original AR-1-001 objection was that the design could return and clear or lose the state needed to RECEIVE/drain a late task. Static CLASS-DATA removes the instance-lifetime premise entirely, and the callback target is a public class method. No fetched aRFC text requires an instance method or rules out static method-call syntax; the design's public class-method declaration satisfies the documented public callback-method requirement.
RETEST=Implement the §5.1b IT8 delayed-worker test, including late callback after `serialize()` return and a second serialize call in the same internal session.
```

## New OD-13 Findings

```text
ID=AR-OD13-001
SEVERITY=MAJOR
CLAIM=Shared static storage is safe because `task_name` is globally unique and `mt_resolved`/`mt_task_outcomes` are scoped by `run_id`.
COUNTEREXAMPLE=Run R1 serializes `CLAS ZCL_FOO`, routes it through sequential fallback after timeout, and leaves at least one abandoned static dispatch row behind. Before R1's late callback arrives, run R2 in the same internal session serializes the same object. §5.1a says `mt_resolved` is keyed by `run_id + obj_type + obj_name`, but §5.1 still defines `mt_resolved` as a run-local table keyed only by `obj_type + obj_name`, and §5.7 checks `line_exists( mt_resolved[ obj_type = ... obj_name = ... ] )` with no `run_id`. A weak implementation following the later pseudocode can skip R2's object because R1 already marked the same object name resolved. The same contradiction exists for the circuit breaker: §5.1a says `mt_task_outcomes` is keyed by `run_id + seq`, but §5.8 defines it as a session-wide `STANDARD TABLE OF abap_bool` ring buffer and `mv_ortec_batch_broken` without any run_id. A failing/abandoned R1 can therefore poison R2's breaker window.
EVIDENCE=E-SER2OD13 §5.1a says cross-run isolation relies on `run_id` and `mt_task_outcomes` keyed by `run_id + seq`; E-SER2OD13 §5.1 defines `mt_resolved` key as only `obj_type + obj_name`; E-SER2OD13 §5.7 uses run_id-less `mt_resolved` lookups/inserts; E-SER2OD13 §5.8 defines `mt_task_outcomes TYPE STANDARD TABLE OF abap_bool` and `mv_ortec_batch_broken` as unscoped run state despite the tables now being CLASS-DATA; E-PERFOD13 §3 says the dispatch-tracking tables are session-wide CLASS-DATA.
IMPACT=context divergence/cross-run false positives/correctness/performance fallback
REQUIRED_CHANGE=Make every static table, flag, and pseudocode access explicitly run-scoped: `mt_resolved` key and every `line_exists`/`INSERT` must include `run_id`; `mt_task_outcomes` must be partitioned by `run_id` or stored in a run-state structure keyed by `run_id`; `mv_ortec_batch_broken` must become run-local or keyed by `run_id`; `purge_run_state( lv_run_id )` must remove exactly that run's resolved/outcome/breaker rows without affecting other runs.
RETEST=In one internal session, run R1 with abandoned/failing dispatches and resolved `CLAS ZCL_FOO`, then start R2 for the same object before R1's late callback; assert R2 does not skip the object, does not inherit R1's breaker failures, and R1's late callback still drains by task_name without touching R2 state.
```

```text
ID=AR-OD13-002
SEVERITY=MAJOR
CLAIM=Forcibly purging the oldest abandoned run is safe because its eventual late callback falls into the unknown-task defensive path, which does `RECEIVE + discard`.
COUNTEREXAMPLE=Run R1's abandoned dispatch is purged after the abandoned-ledger cap is exceeded. Its RFC worker later terminates with `system_failure` or `communication_failure` rather than a normal result. The unknown-task branch in §5.5 executes `RECEIVE RESULTS FROM FUNCTION 'Z_ABAPGIT_ORTEC_SER_BATCH' ##NEEDED` with no `EXCEPTIONS` handling, unlike the normal and late-known branches. A defensive path that can be reached specifically after forced purge must be able to absorb both successful and failed receives; otherwise the bounded purge policy can turn a late failed callback into a callback-time dump or unhandled RFC error.
EVIDENCE=E-SER2OD13 §5.1a says oldest abandoned run rows are forcibly purged and future callbacks hit the unknown `task_name` defensive path; E-SER2OD13 §5.5 unknown branch has a bare `RECEIVE` and returns; E-SER2OD13 §5.5 known `T`/`X` and `A` branches use `EXCEPTIONS system_failure = 1 communication_failure = 2 OTHERS = 3`; E-DOC-ARFC `ABAPRECEIVE` says RECEIVE is the callback-side statement that handles output parameters and exceptions.
IMPACT=late-callback robustness/purge safety/availability
REQUIRED_CHANGE=Make the unknown-task branch use the same exception-safe RECEIVE shape as the known late-drain branch: `RECEIVE ... EXCEPTIONS system_failure = 1 communication_failure = 2 OTHERS = 3`, discard any result, swallow or compactly log `sy-subrc`, and never raise from the callback. Add this exact behavior to the purge-policy text so forced purge is safe for both successful and failed late workers.
RETEST=With a test-seam value forcing abandoned-run purge, deliver two late callbacks for purged task_names: one returning normally and one raising RFC system/communication failure. Assert both execute RECEIVE once, no dump/propagated exception occurs, no result merges, and static row counts remain under the documented cap.
```

## Stress Results / Non-Findings

```text
ID=PASS-OD13-001
SECTION=Static callback method admissibility
RESULT=PASS
EVIDENCE=E-SER2OD13 §5.1a declares `on_end_of_batch` as a PUBLIC STATIC `CLASS-METHODS`; E-SER2OD13 §5.2 registers it with `CALLING zcl_abapgit_ortec_ser_orch=>on_end_of_batch ON END OF TASK`; E-DOC-ARFC does not state that `meth` must be an instance method, and static method-call syntax is valid ABAP method-call syntax. No new finding.
```

```text
ID=PASS-OD13-002
SECTION=Purge-vs-callback race
RESULT=PASS_WITH_CONDITION
EVIDENCE=E-DOC-ARFC says callbacks execute at a later work-process change/roll-in, not concurrently in the middle of arbitrary ABAP statements. Within one internal session, purge and callback execution are cooperative, not truly parallel. Therefore the oldest-run purge can be treated as atomic with respect to a callback: the callback sees either the row before purge or unknown-task after purge. This remains safe only after AR-OD13-002 hardens the unknown-task RECEIVE failure path.
```

```text
ID=PASS-OD13-003
SECTION=True parallel serialize calls in one internal session
RESULT=PASS
EVIDENCE=E-DOC-ARFC callback execution occurs at work-process changes in the same internal session; no evidence supports two ordinary `serialize()` calls executing simultaneously on separate ABAP threads inside one internal session. The real concern is not CPU-thread parallelism but re-entrant callback delivery for older runs during later WAIT/roll-in points, which is covered by task_name/run_id scoping and AR-OD13-001's required run-scoped table/flag cleanup.
```

## Verdict

```text
VERDICT=REVISE_AND_REVIEW_ONCE
OPEN_BLOCKER=0
OPEN_MAJOR=2 (AR-OD13-001, AR-OD13-002)
OPEN_MINOR=0
CLOSED=AR-1-001
RATIONALE=The corrected static CLASS-DATA/CLASS-METHODS mechanism closes the original AR-1-001 target/state-lifetime problem without GC ambiguity, and the fetched ABAP docs support late callback delivery within the same internal session after bounded WAIT. However, the redesign leaves run-id-less pseudocode and session-wide breaker state that contradict the stated cross-run isolation guarantee, and the forced-purge unknown-task path is not exception-safe. Both are local design amendments, but they must be corrected before this OD-13 packet is implementation-ready.
```

## OD-13 Correction closure (focused re-verification)

Fixes for AR-OD13-001 and AR-OD13-002 were applied directly to
`serialization_adaptive_batch_design.md`, then independently re-checked by
a fresh focused adversarial pass (not a full re-review).

```text
ID=AR-OD13-001
STATUS=PARTIALLY_RESOLVED (first pass)
EVIDENCE=mt_resolved/mt_task_outcomes/mt_broken_runs correctly run_id-
  scoped and purged per run (§5.1/§5.5/§5.7/§5.8/§5.0 step 8), BUT §5.2's
  `mt_dispatch` INSERT itself omitted `run_id = lv_run_id` while every
  downstream reader (§5.4-§5.7) already expected `<ls_d>-run_id` to be
  correctly populated - a genuine gap the first closure pass caught.

ID=AR-OD13-001 (closure-2, single-point re-check after the §5.2 fix)
STATUS=RESOLVED
EVIDENCE=§5.2's single INSERT INTO mt_dispatch now includes
  `run_id = lv_run_id`, and `lv_run_id` is confirmed as the same
  per-serialize()-call GUID established in §5.0/§5.1
  (`cl_system_uuid=>create_uuid_x16_static( )`). Independently
  re-confirmed by a second, single-point-scoped subagent pass.

ID=AR-OD13-002
STATUS=RESOLVED
EVIDENCE=The unknown-task/purged-run branch in `on_end_of_batch` (§5.5)
  now uses an exception-safe RECEIVE with an explicit EXCEPTIONS clause
  (system_failure/communication_failure/OTHERS) rather than an unguarded
  RECEIVE; matches the exception shape already used on the normal drain
  path. No new attack surface found.
```

## Final Verdict (OD-13 Correction closure)

```text
VERDICT=APPROVE
OPEN_BLOCKER=0
OPEN_MAJOR=0
OPEN_MINOR=0
CLOSED=AR-1-001, AR-OD13-001, AR-OD13-002
RATIONALE=All findings from the OD-13 Correction cycle are now resolved
  and independently re-verified against the current design text,
  including a genuine follow-up gap (missing run_id stamp at the single
  mt_dispatch INSERT site) that the first closure pass itself caught and
  that has since been fixed and re-confirmed. No new contradictions or
  attack surface were introduced by any of the fixes.
```
