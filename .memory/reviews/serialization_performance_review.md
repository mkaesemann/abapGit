# SER Performance Review — DESIGN_GATE Cycle 1

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SERIALIZATION_PERFORMANCE_DESIGN_GATE_CYCLE1
MODE=DESIGN_GATE
REVIEWER=ortec-abapgit-performance-review
VERDICT=FAIL_DESIGN_GATE_MAJOR
ALLOWED_CONTEXT_READ=serialization_ser0_audit.md, serialization_performance_design.md,
  serialization_bulk_exists_design.md, serialization_adaptive_batch_design.md,
  serialization_provider_design.md, serialization_wapa_review.md
NOTE=This is a DESIGN_GATE review of shape/soundness only. No code exists yet;
  a separate adversarial/line-level correctness review is commissioned
  elsewhere and is not duplicated here.
```

## Q1 — LPT-first + shrinking-batch tail latency vs. measured per-object variance

`serialization_ser0_audit.md` §6 measures a real intra-type variance for
CLAS objects alone (`classes-parallel-worker2.txt` ~567ms/object trace
overhead vs `classes-parallel-worker3.txt` ~238ms/object, ~2.4x). The
adaptive batch design's cost model (`serialization_adaptive_batch_design.md`
§3) is keyed **by object TYPE only, not TYPE+NAME** — every CLAS object in
a run is assigned the SAME `est_ms` (the type's EWMA/static average). LPT-
first (§4) sorts by `est_ms` descending and ties are broken by stable TADIR
order — so within one type, the planner has **no signal at all** to
distinguish a heavy CLAS from a light one; it is scheduling on noise for
same-type objects. A batch of 25 same-type objects can therefore, by chance,
cluster several ~567ms-class heavy objects together while a sibling batch
gets all ~238ms-class light ones — exactly the tail-latency scenario LPT-
first is meant to prevent, and the shrink factor (`c_shrink_factor=2`, §9)
only shrinks batches near the END of the queue, so an unlucky early/mid-run
batch is not protected by it.

The design is self-aware of this (§6 "Bimodal CLAS costs" self-check,
`serialization_adaptive_batch_design.md`) and explicitly defers finer-
grained buckets to SER-4's measured-expansion process
(`serialization_performance_design.md` §6). That is an acceptable posture
for a prototype, but the claim that LPT-first "reduces tail latency" is only
true ACROSS types, not WITHIN the one type (CLAS) that the evidence base
actually measured variance for — the design should say this explicitly
rather than imply LPT-first meaningfully addresses the CLAS worker2/3 case.

**Finding F1 (MAJOR, not blocking):** cost model has zero intra-type signal;
LPT-first cannot mitigate the one variance case actually measured in the
evidence (ser0_audit §6). Recommend the design explicitly scope its
tail-latency claim to inter-type effects only, and consider one cheap,
available-now proxy (e.g. object size/lines-of-code from TADIR-adjacent
metadata, or CLAS-with-testclasses flag) ahead of waiting for SER-4's
run-over-run measurement cycle, since the intra-type variance is already
measured today.

## Q2 — Row/byte limit table internal consistency and safety

Table (`serialization_adaptive_batch_design.md` §9): `c_max_batch_rows=25`,
`c_max_batch_input_bytes_est=8MB`, `c_max_in_flight_batches=lv_max`,
`c_max_in_flight_bytes=100MB`, `c_max_object_output_bytes=20MB`.

Positive: reducing 17,148 `RUN_PARALLEL` dispatches (ser0_audit §6) toward
`17148/25 ≈ 686` batches at the row cap alone is a real, large cut to
RFC task-start count, and the design's own causal claim (§1: dispatch
overhead scales with task-start count, not object count) is well-supported
by the 12-15%-per-statement SPBT evidence cited. This part is sound.

Inconsistency: `c_max_batch_input_bytes_est` is enforced against
**estimated** bytes (`est_bytes`), and the static per-type defaults seeding
that estimate are small and coarse (CLAS/INTF = 15,000 bytes flat, §3) —
this is the SAME type-level averaging problem as F1, applied to bytes
instead of ms. There is no batch-level cap on **actual** output bytes: the
only actual-bytes safeguard, `c_max_object_output_bytes=20MB`, is a
**per-object, post-hoc** trigger that only changes scheduling for objects of
that TYPE for the *remainder of the run* (§6 "One object producing
unexpectedly huge output") — it does not constrain the batch that is
*already in flight* when the oversized object is discovered, and it does
not stop 25 objects each individually just under the 20MB per-object
threshold from summing to ~475MB of real output in a single RFC round trip,
which alone exceeds `c_max_in_flight_bytes=100MB` (a run-wide aggregate
cap) from one batch. Given the design's own admission that static/EWMA
per-type estimates do not track real per-object variance (F1), the
8MB *estimated*-byte gate cannot be relied on to prevent this.

**Finding F2 (MAJOR):** no actual-output-bytes cap exists at the batch
level; the estimated-byte gate is only as accurate as the type-level cost
model, which is demonstrably coarse (F1). Recommend either (a) a
conservative batch-level assumed-worst-case output cap (e.g. treat every
object in a batch as if it could hit `c_max_object_output_bytes` and cap
`c_max_batch_rows` accordingly for byte-safety, not just row-count safety),
or (b) tightening `c_max_batch_input_bytes_est` enforcement to use a
per-type P95 rather than mean-based EWMA once SER-4 data exists.

## Q3 — Provider extraction once-per-batch vs. once-per-object

`serialization_provider_design.md` §4's `extract_for_batch` pseudocode
accumulates all objects' rows into `lt_msag`/`lt_dokil` and issues **one**
`EXPORT ... TO DATA BUFFER` per batch, replacing N per-object EXPORTs with
one — this is a genuine O(1)-calls-per-batch improvement over today's
O(objects)-calls, and ABAP's `EXPORT TO DATA BUFFER` cost is linear (not
superlinear) in the serialized data volume for flat/standard-table content,
which is what these caches hold (§2 family assignment: MSAG/DOKIL/DTEL/DOMA/
description-text tables — no nested/deep structures called out). No
superlinear risk is evidenced or plausible here, and the buffer size is
implicitly bounded by the same `c_max_batch_rows`/`c_max_batch_input_bytes_est`
slice the planner already enforces (provider design §6). **No finding.**

## Q4 — Run-scoped prepare()/clear() vs. full 40,000-object peak memory

`serialization_performance_design.md` §3's memory model table lists:
planner work-item list (metadata-only, O(N)), per-type cost table (bounded,
<100 rows), in-flight batch set (bounded by `c_max_in_flight_batches`),
per-batch input slice (≤25 rows), per-batch prefetch **export buffer**
(bounded by batch row/byte limits), and per-batch RFC result (bounded by
row limit x per-object output cap). It does **not** list the underlying
RUN-scoped `ser_pref`/`ser_pref_ext`/`ser_pref_oo` **source caches**
themselves (10+ CLASS-DATA tables: DTEL, ENHS, FUGR, PROG langs, SMIM,
TOBJ, TRAN, plus CLAS/INTF `classtx`/`compotx`/`subcotx` description
caches) that `extract_for_batch` reads FROM. `serialization_provider_design.md`
§3 explicitly decides to keep `prepare()` run-scoped ("sized to the
caller's lt_tadir, K or N depending on the caller") but gives no byte
estimate for N=40,000, and `serialization_ser0_audit.md` §9 itself lists
peak memory for a full non-filtered 40,000-object serialization as
**"still UNKNOWN"** (carried over, unresolved, from the prior Git-side
backlog). The performance design's memory-model table (§3) reads as if it
fully accounts for peak memory, but it silently omits the one structure
that actually scales unbounded (no stated cap) with full-repository N —
this predates this design (already true today per G-1), but this design
does not resolve or even surface it as a residual risk in its own memory
section, despite the DESIGN_GATE requirement to state a memory model.

**Finding F3 (MAJOR):** the stated memory model is incomplete — it omits
the N-scaling, run-scoped provider caches. Since SER-2/3 do not change
this pre-existing behavior, this is not a NEW regression, but the design
must not present §3 as a complete peak-memory bound while ser0_audit §9's
own "MUST GATHER" item (40k-object peak memory) remains open. Recommend
requiring a measured or estimated worst-case cache footprint for
CLAS/INTF-heavy 40,000-object repositories before implementation sign-off,
or an explicit owner-accepted risk note if measurement is deferred.

## Q5 — EWMA convergence within a single run

`serialization_adaptive_batch_design.md` §3 uses `alpha=0.3`; the weight of
the initial (static-default) estimate decays as `0.7^n` — roughly 17% after
5 samples, 3% after 10. For a CLAS-heavy run at the scale actually measured
(458 objects, ser0_audit §6) or the 17k-task large-repo trace, a type with
hundreds+ of instances converges within the first few dozen objects and has
most of the run left to benefit — plausible and useful at that scale. For a
smaller or more type-diverse run (many types each with only single- or
low-double-digit-digit instance counts), EWMA will not have converged
before the run ends, meaning the static per-type-family defaults (§3) are
what actually determines scheduling for those types, not EWMA.

**Finding (calibration note, not a blocker, per instructions):** the design
should say this explicitly rather than only asserting "EWMA self-corrects
within the same run" (§6) — that claim is only reliably true for
high-count types. The static defaults' accuracy (currently coarse,
single-bucket-per-family, §3) is therefore the primary lever for most
real-world runs, reinforcing that SER-4's measured-expansion process
(performance design §6), not EWMA/SER-6, is the mechanism that will
actually matter in practice.

## Q6 — Other DB/RFC/memory shape concerns

No per-object SQL/HTTP is introduced; worker count is unchanged (provider
design and adaptive batch design both explicitly reuse `lv_max`/`mv_group`
unchanged, adaptive batch design §7); transaction/commit scope is
unaffected (no COMMIT WORK appears anywhere in this design); cache
invalidation (`clear()`) remains once-per-run, not per-row. One further
concern found:

`serialization_ser0_audit.md` §1 states today's baseline explicitly: on RFC
communication/system failure, the existing code sets a `mv_parallel_broken`
-style flag that **degrades the ENTIRE REMAINDER of the run to sequential
once**, and states "no object is silently dropped on RFC failure today...
which is the existing fallback baseline any new design must not regress
below." `serialization_adaptive_batch_design.md` §5 step 7 and §6 instead
retry **each batch independently** (`c_max_retries=2`, each retry bounded by
`c_batch_rfc_timeout_s=300`s) before routing that batch's objects to
per-object sequential fallback — there is no global circuit breaker
equivalent to `mv_parallel_broken` for a systemic RFC/server-group outage.
For a 40,000-object run split into ~1,600 batches (25/batch) with
`c_max_in_flight_batches≈lv_max` concurrent, a systemic outage would need
roughly `1600/lv_max` dispatch waves, each paying up to `2 x 300s` before
falling back — a multi-hour cascading delay in the worst case, materially
worse than today's single-flip-to-sequential baseline.

**Finding F4 (MAJOR):** missing global circuit breaker for systemic RFC
failure risks regressing below the explicitly documented "must not regress
below" baseline (ser0_audit §1) under a full outage. Recommend adding an
aggregate failure counter (e.g. N consecutive batch-level failures, or a
fraction of concurrently in-flight batches failing) that stops dispatching
NEW batches via RFC and degrades the remaining queue directly to in-process
sequential serialization, mirroring `mv_parallel_broken`, rather than
retrying every subsequent batch independently at full timeout cost.

## Verdict rationale

The overall architecture (LPT-first adaptive batching, batch-scoped
provider extraction, additive/non-invasive hook, structural output parity
guarantee, bounded worker/row/in-flight caps) is directionally sound and
well-evidenced against the SPBT dispatch-overhead data (ser0_audit §6).
None of F1-F4 are "predictably unusable at production scale" (no per-object
SQL/HTTP, no unbounded-without-any-limit structure, no recursive DB/RFC
walk) — they are internal-consistency and completeness gaps in the stated
limits/memory model and a missing failure-mode safeguard, all fixable
within the current architecture without a redesign. This is
**FAIL_DESIGN_GATE_MAJOR**, not blocking: revise the byte-budget model
(F2), state or measure the omitted run-scoped cache footprint (F3), add a
systemic-failure circuit breaker (F4), and scope the tail-latency claim
correctly (F1), then re-review.

# Cycle 2

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SERIALIZATION_PERFORMANCE_DESIGN_GATE_CYCLE2
MODE=DESIGN_GATE
REVIEWER=ortec-abapgit-performance-review
VERDICT=APPROVE_WITH_MINOR_REVISIONS
ALLOWED_CONTEXT_READ=serialization_ser0_audit.md,
  serialization_performance_design.md (revised §3), serialization_adaptive_
  batch_design.md (revised §3/§5.8/§5.9/§6/§9), serialization_provider_
  design.md, serialization_wapa_review.md
```

## F1 — tail-latency claim scoping

`serialization_adaptive_batch_design.md` §6 "Bimodal CLAS costs" now reads:
"this design does NOT claim to solve intra-type tail latency for the
prototype. It only claims to reduce INTER-type imbalance and,
independently, to cut RFC dispatch-count overhead (§1), which is unaffected
by intra-type variance." This is exactly the scoping Cycle 1 asked for, and
it correctly cites the effective-row-limit throttle (§9) as a partial,
not-claimed-as-complete, blast-radius mitigant.

**F1: RESOLVED.** Honest scoping + a deferred, evidence-gated SER-4 plan
(performance design §6, which requires a measured cost-share before adding
a finer signal) is an acceptable DESIGN_GATE outcome. An intra-type signal
is a genuine future improvement, not a gate condition — the design no
longer overclaims, and the underlying dispatch-count-reduction benefit
(the primary justification, ser0_audit §6's SPBT evidence) does not depend
on solving intra-type ordering at all.

## F2 — batch-level actual-bytes blowup

`serialization_adaptive_batch_design.md` §5.9 adds a hard,
`c_max_actual_batch_bytes` (12 MB) gate evaluated on the REAL
`extract_for_batch()` output immediately before every `CALL FUNCTION`
(initial dispatch, refill, AND retry/bisection), independent of estimate
accuracy. §9's new `effective_row_limit = MIN( c_max_batch_rows, MAX( 1,
c_max_batch_input_bytes_est / MAX( est_bytes_for_type, 1 ) ) )` proactively
shrinks batch composition for heavy-`est_bytes` types before the reactive
gate is even needed. Together these close the original gap: many
individually-under-threshold objects summing to a large batch payload can
no longer reach dispatch — §5.9 recursively bisects (bounded by
`c_max_pre_dispatch_splits=3`) until under budget.

Edge case verified: §5.9 explicitly states "a batch of 1 object is ALWAYS
dispatched regardless of its actual provider-buffer size... its own worst
case is already bounded by `c_max_object_output_bytes`' PURPOSE, even
though that constant is measured post-hoc" — so a lone unsplittable object
is dispatched rather than deadlocking. This single-object case is a
disclosed, inherent residual (cannot be split further; the
`c_max_actual_batch_bytes` gate cannot apply to it), and it still respects
the run-wide `c_max_in_flight_bytes` aggregate via deferral (§5.0 step 5),
so it cannot compound into unbounded concurrent memory even though its own
size is not gated. This residual is materially smaller and honestly
disclosed, not the same batch-summation gap Cycle 1 found.

**F2: RESOLVED.** The batch-level real-output-bytes blowup this finding
was about is closed; the disclosed single-object worst case is an
acceptable, unavoidable residual for a DESIGN_GATE.

## F3 — 40,000-object peak memory (run-scoped provider caches)

`serialization_performance_design.md` §3 now contains an explicit
"RESIDUAL RISK" paragraph naming the omitted `ser_pref`/`ser_pref_ext`/
`ser_pref_oo` source caches, stating peak memory for a full 40,000-object
run "remains UNKNOWN," and introduces `OD-11`: "obtain this measurement (or
an owner-accepted risk acceptance to defer it) BEFORE implementation
sign-off for any full-repository (non-filtered) serialization scenario."

**F3: RESOLVED.** Cycle 1's own recommendation explicitly allowed "an
explicit owner-accepted risk note if measurement is deferred" as a valid
alternative to an actual number. OD-11 satisfies that: it is honestly
surfaced (no longer silently presented as bounded) and gated as a
pre-implementation condition, not a permanently deferred unknown. A
DESIGN_GATE does not require the number itself when the gap predates this
design (ser0_audit §9 M-3) and a concrete, cheap, pre-signoff measurement
step is named.

## F4 — global circuit breaker

`serialization_adaptive_batch_design.md` §5.8 adds
`mv_consecutive_dispatch_failures`, threshold `c_circuit_breaker_threshold
= 3`, incremented "once per DISTINCT dispatch... that ends in state='F'
... or exhausts its retries via timeout; reset to 0 on any dispatch that
reaches state='R'."

For a clean, TOTAL outage this bounds worst-case delay reasonably: a group
must exhaust `c_max_retries=2` timeouts (~2 x 300s ≈ 600s) before counting
as one failure toward the breaker; with `c_max_in_flight_batches` dispatched
concurrently and all failing together, 3 groups exhaust retries in the same
window, tripping at roughly 600s — a bounded, one-time cost broadly
consistent with "roughly the `mv_parallel_broken` baseline shape."

However, the reset rule — "reset to 0 on any dispatch that reaches
state='R'" — is defined without regard to which CONCURRENT dispatch a
success belongs to. Under a **partial/intermittent** degradation (e.g. one
server in the RFC group is healthy while others are down, or the outage is
flapping) rather than a clean total outage, a single stray success among
many concurrent in-flight batches resets the counter to 0 every time,
regardless of how many other batches are failing in parallel. Across a
40,000-object run with hundreds of concurrent-dispatch waves, this can
indefinitely prevent the breaker from ever tripping while the large
majority of groups still pay the full ~600s exhaust-then-fallback cost
repeatedly, wave after wave — a cumulative delay that can be MANY multiples
of `c_batch_rfc_timeout_s`, worse than the single-flip
`mv_parallel_broken` baseline, which degrades permanently on the first
confirmed systemic-failure signal and never "un-degrades" mid-run. The
design specifies no minimum-window/ratio-based alternative to the pure
consecutive-reset rule, and does not define completion-order semantics for
resets under concurrency.

**F4: PARTIALLY_RESOLVED.** The full-outage case is now bounded and a real
improvement over Cycle 1 (no breaker existed at all). The reset-on-any-
success semantics leave a genuine, non-hallucinated gap for partial/
flapping degradation specifically because of how concurrent in-flight
dispatches interact with a pure "consecutive" counter — this is a MAJOR,
not blocking, finding: recommend a bounded sliding-window or
failure-ratio variant (e.g. N failures within the last M completions,
or "no reset unless K consecutive successes," K>1) before implementation
sign-off, rather than a single stray success clearing the counter entirely.

## Verdict rationale

3 of 4 Cycle 1 findings (F1, F2, F3) are fully resolved against the revised
designs, each verified against exact cited text rather than assumed. F4 is
a genuine improvement (previously nonexistent) but has one remaining,
concretely identified gap (concurrent-dispatch reset semantics under
partial/flapping outages) that is not blocking — it does not make the
architecture predictably unusable at scale, and is fixable with a small,
scoped change to the breaker's counting rule, not a redesign.

**VERDICT: APPROVE_WITH_MINOR_REVISIONS**

```text
BLOCKING_COUNT=0
MAJOR_COUNT=1 (F4 partial residual)
RESOLVED_COUNT=3 (F1, F2, F3)
```

# Cycle 3

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SERIALIZATION_PERFORMANCE_DESIGN_GATE_CYCLE3
MODE=DESIGN_GATE
REVIEWER=ortec-abapgit-performance-review
ALLOWED_CONTEXT_READ=serialization_adaptive_batch_design.md §5.8 only
  (per-TASK sliding window, c_breaker_window_size=10,
  c_breaker_failure_ratio=0.7, c_breaker_min_sample=5)
```

## F4 re-check

The revised `record_task_outcome` (§5.8) replaces the cycle-2 consecutive-
counter-with-reset entirely with a FIFO sliding window (`mt_task_outcomes`,
capped at `c_breaker_window_size=10`) and a ratio test
(`count_false / lines >= c_breaker_failure_ratio`, gated by
`c_breaker_min_sample=5`). There is no reset-on-success concept left — a
lucky success only removes one `FALSE` from the ratio's denominator/
numerator pair as it ages out of the window; it cannot zero out an
otherwise-majority-failing window the way the old consecutive counter
could. This directly closes the cycle-2 gap: a flapping/partial outage
sustaining ≥70% failures across any 5-10 confirmed task outcomes still
trips, regardless of interleaved successes.

Every confirmed task outcome — initial dispatch, every bisection half,
every retry — is counted individually with no dedup (carried over,
unchanged, from the cycle-2 AR-2-001 fix), so a total outage still trips
within `c_breaker_min_sample=5` confirmed failures, independent of how
many bisection halves are in flight — bounded and, if anything, slightly
tighter than cycle 2's threshold of 3 consecutive (comparable order of
magnitude, not a regression).

**F4: RESOLVED** against the specific cycle-2 gap (reset-on-any-success
masking a flapping outage).

**New MINOR residual (not part of the original F4 ask, disclosed for
completeness):** because every bisection half is counted individually into
one SHARED global window, a single non-systemic "poison" object that
triggers a genuine RFC-level `system_failure`/`communication_failure` (not
a per-object `rc<>0`, which does not touch this counter) on every
bisection level containing it can contribute up to
`ceil(log2(c_max_batch_rows))≈5` failure entries by itself before §5.6
isolates it to `n=1` and routes it to sequential fallback. At
`c_breaker_min_sample=5`, that alone can reach the minimum sample with
ratio 1.0 and trip the global breaker — degrading the ENTIRE remaining
40,000-object queue to sequential fallback because of one bad object,
not a systemic outage. This is self-limiting (the poison object is isolated
after one occurrence, not recurring) and does not reopen F4's concurrent-
reset gap, so it is MINOR, not blocking. Recommend (non-gating for this
cycle): exclude/weight down failures whose bisection chain shares the same
originating `batch_id` lineage when they collapse to a shared single
sub-object, or simply document this as an accepted, rare, self-limiting
trade-off of the per-task counting model chosen to fix AR-2-001.

**VERDICT: APPROVE_WITH_MINOR_REVISIONS**

```text
BLOCKING_COUNT=0
MAJOR_COUNT=0
MINOR_COUNT=1 (poison-object shared-window inflation, new, non-gating)
F4_STATUS=RESOLVED
```

# OD-13 Correction addendum (performance-relevance check only)

The OD-13 documentation-verification correction changed §5.8's
`mt_task_outcomes`/breaker mechanism from a single session-wide
flag/buffer to a `run_id`-keyed structure (`mt_task_outcomes` now carries
`run_id`+`seq`; `mv_ortec_batch_broken` replaced by a `mt_broken_runs`
hashed table), purged per-run at `purge_run_state` (§5.0 step 8). This is
a CORRECTNESS fix (cross-run isolation), not a performance-shape change:

- Sliding-window size per run_id is unchanged (`c_breaker_window_size=10`,
  same as Cycle 3's approved design) — only the SCOPE of the window
  changed (per-run instead of session-wide), which does not add any new
  loop, DB access, or unbounded growth. Steady-state size is now
  proportional to the number of CURRENTLY-ACTIVE runs (typically 1) times
  10 rows, versus a single global 10-row buffer before — a negligible,
  still-trivial memory delta.
- `mt_broken_runs` adds at most one row per distinct broken run_id not yet
  purged — bounded by concurrently-active runs, not session history.
- Cycle 3's F4 finding (poison-object shared-window inflation) is
  UNCHANGED by this fix: it was already scoped to "one run's own window,"
  and remains a per-run, self-limiting, non-gating residual under the new
  run_id-scoped design exactly as it was under the session-wide one.

No re-review of F1-F4 is required; none of their conclusions depend on
whether the breaker's storage is session-wide or run-scoped. This addendum
is recorded per the owner's instruction to update this artifact only if
warranted — here, only to confirm explicitly that the correctness fix
introduced no new performance concern, not because any finding changed.

```text
VERDICT=APPROVE (no change to Cycle 1-3 findings)
NEW_PERFORMANCE_FINDINGS=0
```
