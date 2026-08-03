# Serialization Correctness Review — Cycle 1

```text
PACKET=COMPACT_HANDOFF_V1
TASK_ID=SERIALIZATION_CORRECTNESS_REVIEW_CYCLE1
SCOPE=Correctness/parity only (concurrency/RFC lifecycle owned by
      .memory/reviews/serialization_adversarial_review.md, OD-13/AR-1-001
      not re-litigated)
CONTEXT_READ=serialization_ser0_audit.md, serialization_ser0_audit_standard.md,
      serialization_ser0_audit_ortec.md, serialization_performance_design.md,
      serialization_bulk_exists_design.md, serialization_adaptive_batch_design.md,
      serialization_provider_design.md, serialization_wapa_review.md
```

## Verdict
REVISE_AND_REVIEW_ONCE

## Confidence
High for DR-001 (arithmetic fact, independently re-counted twice); Medium for
DR-002/DR-003 (reasoning gaps confirmed against supplied evidence, but the
underlying source classes were out of ALLOWED_CONTEXT so cannot be read
directly this cycle); High for DR-004 (same class of defect as DR-001,
lower impact since deferred/unauthorized).

## Strengths
- SER-1's "UNKNOWN resolves to standard check before the serializer ever
  sees the list" claim is fully supported by the cited evidence chain
  (ser0_audit.md §1 flow diagram + ser0_audit_ortec.md §1's "WHEN OTHERS →
  always falls back to exists_standard()") — no gap found (Q2).
- SER-3's facade-migration claim that all 8 existing "Call Site 4" consumer
  files are untouched is explicit and structurally correct: `extract_for_batch`
  is new/additive, existing `get_*()`/`extract_for_object()` bodies are
  unchanged, and facades are new wrapper classes used only by the new
  orchestrator (provider design §1, §5, §9) (Q3).
- Hook-placement ordering (filter_unsupported_objects/filter_ignored_objects
  before the prepare()/anchor block) is consistently cited across
  ser0_audit.md §1 and the ortec Call Site 1 table; performance design §2's
  anchor is placed consistent with those citations (Q5) — no missed
  filtering step found within the evidence available.
- The design is unusually disciplined about NOT changing WHAT serializes an
  object (performance design §4) and gates the one genuinely new read
  surface (DOMA) behind an explicit byte-identical parity test obligation
  rather than asserting correctness by construction (provider design §2).

## Issues

### DR-001
- Type: correctness
- Severity: blocking
- Section: performance design §2 (hook code); adaptive batch design §5.0;
  provider design §5
- Evidence: `ZCL_ABAPGIT_ORTEC_SER_ORCHESTRATOR` = 34 characters (counted
  twice); `ZCL_ABAPGIT_ORTEC_SER_PREF_FACADE` = 33; `..._PREF_EXT_FACADE` =
  37; `..._PREF_OO_FACADE` = 36. All exceed ABAP's hard 30-character global
  object-name limit. The `ZCL_ABAPGIT_ORTEC_SER_` prefix convention this
  design establishes already consumes 22 of the 30 available characters,
  leaving an 8-character suffix budget — `ORCHESTRATOR` (12),
  `PREF_FACADE` (11), `PREF_EXT_FACADE` (15) and `PREF_OO_FACADE` (14) all
  blow through it.
- Why it matters: this repository has hit this exact class of error before
  with real production impact — an over-length name is not always rejected
  cleanly at creation; it can be silently truncated on import/activation,
  causing every reference in the source to break under a name the code
  never actually used. The orchestrator name appears in the literal
  "insert this code" hook snippet meant for the standard file, and in the
  adaptive-batch design's own top-level flow — this is not a cosmetic
  naming placeholder, it is unimplementable as specified.
- Fix: rename before implementation authorization, e.g.
  `ZCL_ABAPGIT_ORTEC_SER_ORCH` (26) for the orchestrator, and use the
  design's OWN already-compliant fallback (`ZCL_ABAPGIT_ORTEC_SER_PROV_FCD`,
  30 chars exactly, single parameterized facade) instead of three
  over-length per-class facades. Add a length check to the implementation
  checklist for every NEW global name this package introduces.

### DR-002
- Type: correctness
- Severity: major
- Section: performance design §4 (output parity plan); adaptive batch
  design §2 (worker body), §4 (session-isolation clarification)
- Evidence: the worker body LOOPs over multiple TADIR rows and calls
  `zcl_abapgit_objects=>serialize(...)` for each, sequentially, inside ONE
  RFC session per batch (adaptive batch design §2). The only
  session-reuse-safety argument given (provider design §4,
  "Session-isolation clarification") covers exactly the three KNOWN ortec
  `CLASS-DATA` caches (`ser_pref`/`ser_pref_ext`/`ser_pref_oo`, each
  clear-before-insert on inject) — it does not address whether any OTHER
  static/`CLASS-DATA` state exists anywhere in the
  `zcl_abapgit_objects`/OO-framework/object-type-handler call chain
  (e.g. `mi_object_oriented_object_fct`, `CL_OO_*` buffered class-pool
  state, enqueue/dequeue bookkeeping) that today is only ever asked to
  serve ONE object per session-lifetime-slice between pool reuses, but
  under batching is now GUARANTEED, every single multi-object batch, to
  see 2+ DIFFERENT objects served back-to-back with no
  inject/clear cycle between them. None of the SER-0 audits inventoried
  non-ortec statics in this call chain, so this cannot be ruled out from
  the supplied evidence.
- Why it matters: this is precisely the byte-identical-output guarantee
  the design claims (performance design §4) resting on an unverified
  assumption. If such state exists, batching (not the orchestrator logic)
  would be the first thing in this codebase to make it observable, since
  today's one-object-per-dispatch contract structurally prevents two
  different objects sharing one session's post-init state within a single
  LOOP body.
- Fix: before authorizing implementation, audit
  `zcl_abapgit_objects`/`zcl_abapgit_oo_base`/`zcl_abapgit_objects_super`/
  per-type handler classes for any `CLASS-DATA` or static buffered state
  outside the three known ortec caches. Add one explicit test to the SER-2
  test matrix: serialize 2+ DIFFERENT objects of the SAME type (e.g. two
  CLAS) through one simulated batch-worker LOOP invocation and diff each
  object's output against serializing it alone — this specific axis is
  currently absent from performance design §4's parity plan, which only
  tests provider hit/miss parity, not multi-object-per-session parity.

### DR-003
- Type: correctness
- Severity: major
- Section: provider design §2 (DOMA provider); ser0_audit_ortec.md §1
- Evidence: ser0_audit_ortec.md §1 documents a `versions '0'/'1'` filter
  only for DTEL's EXISTENCE check (bulk-exists); the DOMA bulk-exists entry
  in the same section has no version filter documented at all. Provider
  design §2 states the new DOMA provider will use "versions '0'/'1'
  exactly like the existing dtel handling in bulk_exists/ser_pref_ext, for
  consistency" — this borrows a convention from a DIFFERENT object type's
  EXISTENCE-check code path, not from `ZCL_ABAPGIT_OBJECT_DOMA`'s own
  current serialize()-path version semantics, which none of the eight
  supplied documents actually trace (no DOMA serialize() call chain is
  audited anywhere in ser0_audit_standard.md or ser0_audit_ortec.md, unlike
  the CLAS/INTF chain in ser0_audit_standard.md §3).
- Why it matters: Q4's premise is itself imprecise (there is no
  "existing DOMA bulk-exists version-handling precedent" to be consistent
  with — that precedent belongs to DTEL), and the design inherits that
  imprecision rather than resolving it against the actual parity target
  (`ZCL_ABAPGIT_OBJECT_DOMA`'s own serialize()). The design's mitigation
  (a mandatory byte-identical parity test, provider design §2) is a sound
  safety net but does not substitute for stating the correct target
  semantics up front — a failing parity test would tell you THAT it broke,
  not necessarily WHY, if the wrong version field/value is read.
- Fix: before implementation, read `ZCL_ABAPGIT_OBJECT_DOMA`'s existing
  serialize() method to state explicitly which DD01L/DD01V/DD07L/DD07T
  version value(s) it reads today, and cite that (not DTEL's bulk-exists
  rule) as the new provider's target semantics.

### DR-004
- Type: correctness
- Severity: minor
- Section: bulk_exists_design.md (SER-1, "future, not-yet-authorized
  refactor")
- Evidence: `ZCL_ABAPGIT_ORTEC_SER_EXIST_REG` is stated as "29 chars" but
  is actually 31 characters (over the 30-char limit by 1). Separately, the
  document declares `INTERFACE zif_abapgit_ortec_ser_exist_h` but later
  references it as `zif_abapgit_ser_exist_h` (missing "ortec") when typing
  the registry's handler table.
- Why it matters: low severity only because SER-1 explicitly defers this
  entire mechanism to a future, not-yet-authorized SER-4 trigger — it does
  not affect SLICE 1/2 implementation. Recorded so it is not silently
  carried forward and implemented exactly as miscounted/misnamed.
- Fix: correct the character count and the interface name reference if/
  when SER-4 actually triggers this mechanism.

## Required revisions
1. Rename `ZCL_ABAPGIT_ORTEC_SER_ORCHESTRATOR` and the three per-class SER-3
   facade names to fit the 30-character limit (DR-001) before any code is
   written; verify every OTHER new global name introduced by this package
   against the same 30-char rule.
2. Audit the object-serialization call chain for non-ortec static/
   `CLASS-DATA` state and add a same-session multi-object parity test
   before authorizing batching of >1 object per RFC worker dispatch
   (DR-002).
3. State `ZCL_ABAPGIT_OBJECT_DOMA`'s actual current version-read semantics
   as the DOMA provider's explicit target, rather than reasoning by analogy
   to DTEL's bulk-exists rule (DR-003).

## Optional improvements
- Fix the character count and interface-name typo in the deferred SER-1
  registry mechanism (DR-004) whenever it is actually triggered.

---
VERDICT: REVISE_AND_REVIEW_ONCE
COUNTS: BLOCKER=1, MAJOR=2, MINOR=1

# Cycle 2

```text
PACKET=COMPACT_HANDOFF_V1
TASK_ID=SERIALIZATION_CORRECTNESS_REVIEW_CYCLE2
CONTEXT_READ=serialization_performance_design.md, serialization_adaptive_batch_design.md,
      serialization_provider_design.md, serialization_bulk_exists_design.md
      (all re-read in full for this cycle; prior review re-read first)
```

## Independent name-length audit (DR-001)

Every new global ABAP object name across all 4 revised documents was
re-enumerated and counted independently (not trusting the documents' own
stated counts):

```text
zcl_abapgit_ortec_ser_orch          26  (orchestrator, renamed)          OK
zcl_abapgit_ortec_ser_cost          26  (cost estimator, new)            OK
zcl_abapgit_ortec_ser_prov_fcd      30  (unified facade, renamed)        OK (exact limit)
zcl_abapgit_ortec_ser_prov_dd       29  (new DDIC/DOMA+DTEL provider)    OK
zcl_abapgit_ortec_ser_prov_gen      30  (new generic no-op provider)     OK (exact limit)
zcl_abapgit_ortec_ser_ex_reg        28  (SER-1 future registry, deferred) OK
zif_abapgit_ortec_ser_prov          26  (interface)                     OK
zif_abapgit_ortec_ser_exist_h       29  (interface, SER-1 deferred)      OK
z_abapgit_ortec_ser_batch           25  (function module)                OK
zaog_ser_batch_result / _tt         21 / 24 (DDIC structure/table type)  OK
zaog_ser_exist_result / _tt         21 / 24 (DDIC structure/table type)  OK
```

No occurrence of the old over-length names (`..._ORCHESTRATOR`,
`..._PREF_FACADE`, `..._PREF_EXT_FACADE`, `..._PREF_OO_FACADE`) remains
anywhere in the 4 documents. The pre-existing, unchanged classes referenced
alongside these (`zcl_abapgit_ortec_ser_pref`, `_pref_ext`, `_pref_oo` — 22,
26, 29 chars respectively) are also within limit, as expected since they
already exist in the system today. The SER-1 interface typo flagged in
DR-004 (`zif_abapgit_ser_exist_h` missing "ortec") does **not** recur
anywhere in the revised `serialization_bulk_exists_design.md` — every
reference now consistently uses `zif_abapgit_ortec_ser_exist_h`.

One observation, not a defect: `zcl_abapgit_ortec_ser_prov_gen` and
`zcl_abapgit_ortec_ser_prov_fcd` both land at **exactly** 30 characters —
compliant, but with zero margin. Any future suffix growth on either
(e.g. renaming `_gen`/`_fcd` to something more descriptive) will
immediately exceed the limit. Recommend a one-line note in the
implementation-readiness checklist: re-verify length before renaming
either class post-implementation.

**DR-001: CLOSED.** No over-length name found anywhere in scope.

## DR-002 disposition (OD-14 gate)

Performance design §4/§5 OD-14 now states, verbatim, a named,
mandatory, pre-implementation gate: audit
`zcl_abapgit_objects`/`zcl_abapgit_oo_base`/`zcl_abapgit_objects_super`/
per-type handlers for non-ortec `CLASS-DATA`, AND add the specific
multi-object-per-session parity test (2+ different objects of the same
type through one simulated batch-worker loop, diffed against
serialize-alone output), both required **before SLICE 2 implementation is
authorized** — not before design sign-off, and not deferred indefinitely.
This is exactly the disposition the review asked for: an explicit, named,
scoped, testable gate rather than either (a) silently proceeding on the
unverified assumption, or (b) demanding this design cycle itself read
`zcl_abapgit_objects` source (which was legitimately out of this review's
ALLOWED_CONTEXT both cycles). The gate is concrete enough to be checked
mechanically at implementation-readiness review (audit performed: yes/no;
test exists and passes: yes/no) rather than being a vague aspiration.

**DR-002: ACCEPTABLE DISPOSITION.** Downgraded from an open MAJOR
correctness gap to a tracked, named implementation precondition (OD-14).
Not re-opened as blocking this design review; re-verification belongs to
the SLICE 2 implementation-readiness gate, not this cycle.

## DR-003 disposition (DOMA version semantics)

Provider design §2 "New DOMA coverage / VERSION_SEMANTICS" now explicitly
labels the DTEL-analogy reasoning as withdrawn ("it is not [an established
precedent]... an OPEN, NOT-YET-RESOLVED implementation precondition, not a
design decision made here") and states the correct obligation: read
`ZCL_ABAPGIT_OBJECT_DOMA`'s own `serialize()` method and cite its actual
DD01L/DD01V/DD07L/DD07T version semantics as the provider's target,
*before* any DDIC/provider code is written, added to the SLICE 3
implementation-readiness checklist. This is a genuine correction, not a
restatement of the same guess with more words — the design no longer
asserts a specific version convention it hasn't verified; it defers the
factual claim to a named, mandatory source-read step and keeps the
byte-identical parity test as a second, independent safety net.

**DR-003: ACCEPTABLE DISPOSITION.** Adequately defers to source
verification before implementation; does not silently guess.

## DR-004 disposition (SER-1 registry count/typo)

`ZCL_ABAPGIT_ORTEC_SER_EX_REG` is now stated as "28 chars, verified <=30"
— independently re-counted at 28, confirmed correct (previous doc
mis-stated 31-char actual as "29 chars"; both the count and the label are
now right). The interface-name mismatch (`zif_abapgit_ser_exist_h` vs.
`zif_abapgit_ortec_ser_exist_h`) does not recur anywhere in the revised
document (verified above, DR-001 section) — every reference is now the
full, consistent `zif_abapgit_ortec_ser_exist_h`.

**DR-004: FIX CONFIRMED.** Both the count and the typo are corrected.

## New issues found this cycle

None. No new over-length name, no new unresolved analogy-reasoning gap,
and no new scope regression were found in any of the 4 revised documents.

## Verdict

All four Cycle 1 findings are closed or carry an acceptable, named,
testable disposition. No new blocking or major issues were introduced by
the revisions.

---
VERDICT: APPROVE
COUNTS: BLOCKER=0, MAJOR=0, MINOR=0 (one non-blocking observation: two new
class names sit at exactly the 30-char limit with no rename margin —
recorded above, not a defect)

# Cycle 3 (OD-13 Correction)

```text
PACKET=COMPACT_HANDOFF_V1
TASK_ID=SERIALIZATION_OD13_CORRECTION_CORRECTNESS_REVIEW
SCOPE=Cross-run isolation and late-callback correctness ONLY, for the
      OD-13 static/CLASS-DATA redesign (§5 of the adaptive batch design).
      DR-001..DR-004 (Cycle 1-2) are NOT re-litigated.
CONTEXT_READ=serialization_adaptive_batch_design.md §5 in full (§5.0,
      5.1, 5.1a, 5.1b, 5.2-5.9); serialization_performance_design.md §2,
      §3, §3a
```

## Verdict
REVISE_AND_REVIEW_ONCE

## Confidence
High — both blocking/major findings are direct textual contradictions
between §5.1a's stated table-key claims and the concrete pseudocode given
for the only code paths that read/write those tables (§5.5, §5.7, §5.8),
not inferred behavior.

## Strengths
- `mt_dispatch`'s key (`task_name`) is provably collision-free across runs
  without needing a separate `run_id` column, because `task_name` already
  embeds `lv_run_id` (`|SER-{run_id}-{seq}|`, §5.1) — `on_end_of_batch`'s
  `READ TABLE ... WITH TABLE KEY task_name = p_task` can never resolve a
  callback against a different run's dispatch. This part of item 1 is
  correctly designed.
- The late-callback walk-through for cases (a) before purge and (b) after
  the abandoned-ledger purge is correct and matches T-DRAIN-7: a 'T'/'X'
  row drains via the `CASE 'T' OR 'X'` branch (RECEIVE + discard, no
  merge, no `mt_files`/`mt_resolved` write) regardless of when it arrives;
  once forcibly purged, the same callback hits the "unknown task_name"
  defensive path (RECEIVE + discard). Neither path ever merges into a
  live run's `mt_files`.
- §3a's underlying documentation citations (implicit commit on
  `STARTING NEW TASK`/`WAIT`/`RECEIVE`) are accurate and the core
  conclusion (no NEW hazard vs. today's `run_parallel`) is directionally
  correct.

## Issues

### DR-005
- Type: correctness
- Severity: blocking
- Evidence: §5.1a's private-section declaration states
  `CLASS-DATA mt_resolved TYPE ty_resolved_tt. " keyed by run_id + obj_type
  + obj_name` and explicitly claims "`mt_resolved` is additionally keyed
  by `run_id` so two runs' identical `obj_type`/`obj_name` pairs... never
  collide." But the ONLY two code paths shown that read or write
  `mt_resolved` (§5.5's callback-merge loop and §5.7's
  `route_to_sequential_fallback`) never populate or filter on `run_id` at
  all: `CHECK NOT line_exists( mt_resolved[ obj_type = ... obj_name = ... ] )`
  and `INSERT VALUE #( obj_type = ... obj_name = ... ) INTO TABLE
  mt_resolved.` — both omit `run_id` in every instance, in both call
  sites, consistently (not a one-off typo).
- Why it matters: this is exactly the scenario item 1 asks to verify.
  Since `mt_resolved` is `CLASS-DATA` (shared across runs in the same
  internal session) and is never actually scoped by `run_id` in the given
  mechanism, if Run A resolves `CLAS ZCL_FOO` (e.g. repo 1) and its row
  is still present when Run B later resolves the SAME `obj_type`+
  `obj_name` (e.g. repo 2, or a second pull of the same repo), Run B's
  legitimate successful callback for `ZCL_FOO` hits `CHECK NOT
  line_exists(...)` = false and the `CHECK` statement silently skips the
  rest of that loop iteration — `merge_into_mt_files` is never called.
  Run B silently returns an incomplete file list, missing an object it
  DID successfully serialize. This also defeats `purge_run_state(
  lv_run_id )`'s ability to scope its own cleanup to "this run's own
  rows" (§5.0 step 8) for `mt_resolved`, since no row actually carries a
  `run_id` value to select on.
- Fix: add `run_id = lv_run_id` to both the `line_exists` filter and the
  `INSERT VALUE #( ... )` constructor in §5.5 and §5.7, and to
  `ty_resolved_tt`'s key definition/example if not already implied.

### DR-006
- Type: correctness
- Severity: major
- Evidence: §5.1a's declaration states `CLASS-DATA mt_task_outcomes TYPE
  ty_outcome_tt. " keyed by run_id + seq, windowed per run_id (§5.8)" but
  §5.8's actual concrete type is `mt_task_outcomes TYPE STANDARD TABLE OF
  abap_bool` — a flat, unkeyed FIFO ring buffer with no `run_id` field at
  all, and `record_task_outcome`/the breaker-ratio check
  (`count_false( mt_task_outcomes ) / lines( mt_task_outcomes )`) operate
  over the ENTIRE shared buffer with no run scoping. No reset of
  `mv_ortec_batch_broken` (or the buffer itself) at the start of a new
  `serialize()` call is described anywhere in §5.
- Why it matters: this is the item-1 cross-run-isolation question applied
  to the circuit breaker instead of `mt_resolved`. Because ABAP is
  single-threaded per session, two runs' NORMAL processing can't overlap,
  but the breaker's state IS carried forward across sequential runs in
  the same session with no reset: (a) once `mv_ortec_batch_broken` trips
  during Run A (e.g. a transient RFC hiccup), it stays true forever for
  every subsequent run in the session — Run B, C, D... are permanently
  and silently forced into full sequential fallback even after the outage
  clears, with no diagnostic trail explaining why batching stopped; (b) a
  fresh Run B's first few outcomes share the SAME sliding window as Run
  A's leftover entries, so Run B can trip the breaker based on Run A's
  failures before Run B's own health would justify it. This does not
  corrupt `mt_files` content (fallback still serializes correctly), but it
  is a genuine, undocumented cross-run behavioral contamination that
  contradicts the design's own "windowed per run_id" claim.
- Fix: either give `mt_task_outcomes` a real `run_id` field and filter
  every read/write by `lv_run_id` (matching the §5.1a prose), or, if a
  session-wide breaker is actually intended, state that explicitly and
  drop the "windowed per run_id" language — and either way, state whether/
  how `mv_ortec_batch_broken` is reset at the start of each `serialize()`
  call (currently unspecified).

### DR-007
- Type: correctness (documentation precision)
- Severity: minor
- Evidence: performance design §3a's `DESIGN_IMPACT=NONE` reasoning is
  "it dispatches the SAME two ABAP statements..., just fewer, larger
  times under this design" — true, but this glosses over the fact that
  batching genuinely changes the FREQUENCY/timing of the implicit commit:
  fewer, larger dispatches mean fewer, more widely-spaced implicit-commit
  points per run than today's one-per-object mechanism.
- Why it matters: the risk direction is likely favorable (a wider gap
  between forced commits reduces the chance of prematurely committing
  something a caller wanted grouped, rather than increasing it), so the
  "NONE" bottom-line conclusion is probably still fine — but the
  supporting reasoning currently asserts no change at all, when what
  actually changed is commit CADENCE, not just call count. A future
  reader could reasonably rely on "identical behavior" too literally.
- Fix: rephrase to state explicitly that commit *frequency* is reduced
  (fewer, larger windows between forced commits) while the commit
  *mechanism/guarantee* is unchanged, and that this direction of change
  is assessed as neutral-to-favorable, not literally identical.

## Required revisions
1. Add `run_id` to every `mt_resolved` read/write site (§5.5, §5.7) so the
   table's actual behavior matches its declared key (DR-005) — blocking.
2. Either scope `mt_task_outcomes`/the circuit breaker by `run_id` to
   match §5.1a's claim, or explicitly redefine it as session-wide and
   state the reset policy (DR-006).

## Optional improvements
- Sharpen §3a's `DESIGN_IMPACT=NONE` reasoning to describe the commit-
  frequency change explicitly rather than implying no change at all
  (DR-007).

---
VERDICT: REVISE_AND_REVIEW_ONCE
COUNTS: BLOCKER=1, MAJOR=1, MINOR=1

## Cycle 4 (OD-13 Correction closure re-check)

Focused re-verification only (per owner instruction — not a general
re-review). All fixes were applied directly to
`serialization_adaptive_batch_design.md` and
`serialization_performance_design.md`, then independently re-checked by a
fresh subagent pass scoped ONLY to these 3 findings.

- DR-005: RESOLVED. `ty_dispatch`/`mt_resolved` both carry `run_id`; every
  read/write site (§5.2 INSERT, §5.4 `check_timeouts`, §5.5
  `on_end_of_batch`, §5.6 `handle_receive_failure`, §5.7
  `route_to_sequential_fallback`) now consistently populates/filters by
  `run_id`. (A follow-up adversarial pass additionally caught that §5.2's
  `mt_dispatch` INSERT itself had been left without `run_id = lv_run_id`
  despite downstream readers expecting it populated — this has also been
  fixed and independently re-confirmed; see the adversarial ledger's
  "OD-13 Correction closure" and "closure-2" entries.)
- DR-006: RESOLVED. `mt_task_outcomes` now carries an explicit `run_id`
  field, `record_task_outcome` windows/filters strictly per `run_id`, and
  the single `mv_ortec_batch_broken` flag is replaced by a run_id-keyed
  `mt_broken_runs` table. Both structures are purged for a given run_id at
  `purge_run_state` (§5.0 step 8), so no state can leak across runs.
- DR-007: RESOLVED. Performance design §3a now explicitly states that
  batching changes implicit-commit CADENCE (fewer, larger windows) while
  the commit mechanism/guarantee itself is unchanged, rather than implying
  literally identical timing.

No new contradictions were found during closure re-verification.

---
VERDICT: APPROVE
COUNTS: BLOCKER=0, MAJOR=0, MINOR=0
