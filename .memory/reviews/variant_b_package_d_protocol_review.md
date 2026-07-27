# Package D — Protocol & Persistence Review (re-review, iteration 2)

```
PACKET=COMPACT_HANDOFF_V1
TASK=D-REVIEW-PROTOCOL-RERUN
BASELINE=29199f629773c676e0eaa2f3a006f5167d304ae8
REVIEW_FOCUS=Re-verification of B-1, B-2, M-1..M-5, m-1 fixes in the revised design (§9/§11/§15/§16/§19)
DESIGN_UNDER_REVIEW=.memory/logs/variant_b_package_d_design.md (revised)
REVIEWER_ROLE=ortec-abapgit-protocol-persistence (independent, design-only, no code changes)
```

## Owner Decision A / B-3 resolution (2026-07-24, post-dates this review's iteration 2 verdict)

The `REVISE_AND_REVIEW_ONCE` verdict below (B-3 blocking; M-2/M-3/M-5
"unit #2" portions open) triggered the escalation-to-owner-decision
governance rule (this review track's one automatic re-review iteration
had already been used). See
[variant_b_package_d_protocol_decision.md](variant_b_package_d_protocol_decision.md)
for the full escalation write-up. Michael chose **Option A**: relocate
Publication Unit #2 to `zcl_abapgit_ortec_porcelain=>pull_by_branch`'s
`INCREMENTAL_UPDATE` branch, with a 7-point binding design decision. This
has been applied directly to `.memory/logs/variant_b_package_d_design.md`
§9 (dedicated "Owner Decision A — B-3 resolution" subsection), §11 (Unit
#2 relocated, m-2 rationale corrected), §15 (file/symbol scope updated,
`zcl_abapgit_ortec_porcelain.clas.abap` added, the two other pre-existing
`persist_pull_result` callers explicitly resolved via an optional
`iv_attempt_id` fallback-mint), §16 (5 new tests added:
`fresh_pull_unit_atomic`, `fresh_pull_fail_no_publish`,
`lock_release_on_failure`, `resume_new_attempt_when_new`,
`resume_reuses_attempt` recorded `NOT_APPLICABLE` with evidence), and §19
(exit criteria updated). Per Michael's explicit instruction, a third full
protocol/persistence review round is **not** required for B-3 itself,
provided the Performance DESIGN_GATE review does not surface a new
protocol/persistence inconsistency — B-3 is considered **RESOLVED** as of
this update. B-1/B-2/M-1/M-4/m-1 remain resolved as already documented
below; M-2/M-3/M-5's "unit #2" portions are now resolved against the
relocated call site per the same design update.

---

All claims below were re-verified directly against current productive source (local workspace files
under `src/ortec/git/*`, `src/git/zcl_abapgit_git_porcelain.clas.abap`,
`src/git/zcl_abapgit_git_transport.clas.abap`), including one class (`zcl_abapgit_ortec_porcelain`)
outside the review's original `SOURCE_SCOPE`/the design's own `§15` file list — reading it was
unavoidable to answer the task's own reachability questions (items 1 and 6) and surfaced the one new
blocking finding below (B-3). Nothing here relies on the design document's own paraphrase or the prior
iteration's paraphrase without independent confirmation.

---

## Verdict

**REVISE_AND_REVIEW_ONCE** — 1 new BLOCKING finding (B-3), 3 MAJOR findings whose closure is
contingent on B-3 (M-2/M-3/M-5, "unit #2" portion only). B-1 and B-2 as scoped are themselves
correctly resolved (B-1 has one non-blocking documentation/reasoning inaccuracy, noted as new minor
m-2). M-1 and M-4 are fully resolved, no new issue. m-1 carried over as resolved (not re-examined this
pass; nothing found contradicts it).

---

## 1. B-1 — `pull_by_branch`'s two callers (re-check)

**Mechanism confirmed sound, but the design's own justification is factually imprecise — call this out,
non-blocking.**

Re-traced every real caller of `zcl_abapgit_ortec_fastpath=>pull_by_branch` via grep across `src/**`
(exactly 2 textual call sites) and read both call contexts directly:

- `src/git/zcl_abapgit_git_porcelain.clas.abap:542` (inside `zcl_abapgit_git_porcelain=>pull_by_branch`,
  the design's "Chain A / porcelain-direct"): this call is reached **only** inside the
  `IF zcl_abapgit_ortec_git_switch=>is_active_for_repo( iv_url ) = abap_false` branch (the `= abap_true`
  branch returns immediately after delegating entirely to `zcl_abapgit_ortec_porcelain=>pull_by_branch`,
  a different class — see §2 below). But `zcl_abapgit_ortec_fastpath=>pull_by_branch`'s own very first
  statement is `IF is_active_for_repo( iv_url ) = abap_false. RETURN. ENDIF.` — i.e. it is *guaranteed*
  to hit that guard and return empty immediately whenever reached from this call site, before Phase 1,
  1b, 2, or 2b can ever execute. **This call site can never reach Phase-1b in practice — it is
  structurally dead code with respect to the lock/attempt-id concern B-1 is about.**
- `src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap:841` (inside `upload_pack_by_branch`, "Chain B"):
  confirmed live and reachable whenever the switch is active for the repo — this is the only call site
  that can actually execute Phase-1b today.

**Assessment:** the design's chosen fix — acquire the lock and call `begin_attempt` *inside*
`pull_by_branch`'s own Phase-1b branch, rather than around whichever caller invokes it — is a
caller-agnostic, structurally correct solution regardless of this nuance: wrapping the shared method's
own branch covers every caller that could ever reach it, live or dead, by construction. There is no
correctness gap in the fix itself. However, §11's stated rationale ("this single method is reached by
**both** of `pull_by_branch`'s callers... so scoping the lock/attempt here... covers both call chains
with one change") overstates the case: only one of the two callers can ever actually reach the branch
being wrapped. This is a **new, non-blocking finding (m-2)** — correct the rationale text so a future
reader does not conclude the porcelain-direct call site is a live, exercised path for this concern (it
is not, today) — but it does not require any change to the fix itself.

## 2. NEW BLOCKING B-3 — the design's "unit #2" (inside `upload_pack_by_branch`) does not exist where described; the real second persist/certify unit lives one level up, in an unscoped class

Task item 6 asked directly: *"Does `upload_pack_by_branch`'s real structure support a clean second,
sequential, non-nested lock unit after `pull_by_branch` returns?"* Tracing the actual call graph shows
**no** — not as literally described in §11/§15.

**What §11/§15 claim:** after `pull_by_branch` returns empty and the thin/self-contained/recovery HTTP
cascade produces a pack, `upload_pack_by_branch` itself "acquires its own, separate lock + calls
`begin_attempt`... immediately before its own decode/resolve/persist/certify/commit sequence, then
releases," and `persist_pull_result`/`certify_fetched_commit` gain the threaded `iv_attempt_id` for this
unit.

**What the real source does**, confirmed by grepping every call site of `certify_fetched_commit` and
`persist_pull_result` in `zcl_abapgit_ortec_fastpath.clas.abap` (exactly one call to each, both
self-referential: `certify_fetched_commit` is called only from inside `persist_pull_result`;
`persist_pull_result` is called only from inside `pull_by_branch`'s own Phase-1b branch, line 713) and
then grepping every external caller of `persist_pull_result` across `src/**`:

- `zcl_abapgit_ortec_fastpath=>upload_pack_by_branch` (lines 814-1000) and its private helper
  `upload_pack` (lines 1109-1285, called three times for the thin/self-contained/recovery tiers) **never
  call `persist_pull_result` or `certify_fetched_commit` at all.** `upload_pack` only calls
  `zcl_abapgit_ortec_pack_stream=>decode_streaming` (object persistence only — no certification, matching
  §10's ownership table). There is no "decode/resolve/persist/certify/commit sequence" inside
  `upload_pack_by_branch` for the design's lock to wrap.
- The actual, live call to `persist_pull_result` for a fresh-HTTP-fetch ("new commits") attempt is in
  [zcl_abapgit_ortec_porcelain.clas.abap](src/ortec/git/zcl_abapgit_ortec_porcelain.clas.abap#L370),
  inside `zcl_abapgit_ortec_porcelain=>pull_by_branch`'s own `INCREMENTAL_UPDATE` branch — **a
  completely different class, not mentioned anywhere in the design's §15 file-scope list, and outside
  this review's `SOURCE_SCOPE`.** That method's real sequence is: call
  `zcl_abapgit_git_transport=>upload_pack_by_branch` (which, since the switch is active, delegates to
  `zcl_abapgit_ortec_fastpath=>upload_pack_by_branch` — this is where the HTTP cascade + `decode_streaming`
  actually run and where `upload_pack_by_branch` already returns and, per the design, would already have
  released its lock) → then call `pull(...)` (the standard tree-walk helper, in yet another class,
  `zcl_abapgit_git_porcelain`) to build files → **then** call `persist_pull_result` (which is where
  `certify_fetched_commit`/`mark_graph_complete`/`mark_full_complete`/`publish_snapshot_complete` and the
  single `COMMIT WORK` actually happen).
- Two more external call sites of `persist_pull_result` exist and are equally unaddressed by §15:
  `zcl_abapgit_git_porcelain.clas.abap:650` (the switch-inactive fallback branch — a no-op at runtime
  today per `persist_pull_result`'s own `is_active_for_repo` guard, but it must still **compile**) and
  `zcl_abapgit_ortec_git_tests.clas.testclasses.abap:680` (`persist_creates_state` unit test).

**Consequences if implemented literally as scoped today:**

1. If `persist_pull_result` gains a new **mandatory** `IMPORTING iv_attempt_id` parameter (as §15
   implies for the unit it does describe), the three real, unaddressed call sites above fail to
   **compile** — this is not a runtime edge case, it blocks activation of the whole class pool.
2. If `iv_attempt_id` is instead added as **optional** (default-initial, silently falling back to
   calling `begin_attempt` internally when blank), then the actual common-case fresh-fetch path
   (`zcl_abapgit_ortec_porcelain=>pull_by_branch`'s `INCREMENTAL_UPDATE` branch — the everyday "pull with
   new commits" scenario, arguably more common than the Phase-1b resume-match branch) reintroduces
   exactly the DR-004 double-mint/correlation-defeat defect the whole `attempt_id` design exists to
   close, and does so unlocked (no `acquire_repo_lock`/`release_repo_lock` call exists anywhere in this
   call chain today, and the design never proposes adding one to `zcl_abapgit_ortec_porcelain`).
3. Either way, the design's own stated exit criterion ("`attempt_id` threaded end-to-end... through
   both self-contained persist units (`pull_by_branch`'s Phase-1b branch and `upload_pack_by_branch`'s
   post-cascade fetch)", §19 D2 checkpoint) is unattainable as written, because the second "unit" it
   names does not contain the code the checkpoint expects it to cover.

**Required fix:** the design must re-locate "unit #2" to where the real sequence lives —
`zcl_abapgit_ortec_porcelain=>pull_by_branch`'s `INCREMENTAL_UPDATE` branch (covering its own
`upload_pack_by_branch` HTTP-cascade call, the subsequent `pull()` tree-walk, and its own
`persist_pull_result` call) — and must add that class/method to `SOURCE_SCOPE`/§15's file list, together
with an explicit visibility/lock-acquisition plan for it (it is a third class beyond
`zcl_abapgit_ortec_fastpath`/`zcl_abapgit_ortec_pack_dec`, so `acquire_repo_lock`/`release_repo_lock`'s
planned `PUBLIC SECTION` move must also be usable from here). The design must also explicitly decide and
state the fate of the two other pre-existing `persist_pull_result` call sites
(`zcl_abapgit_git_porcelain.clas.abap:650`, effectively dead at runtime, and the existing unit test) —
e.g. keep `iv_attempt_id` optional with an explicit, intentional (not incidental) "no lock, own
`begin_attempt`" fallback documented for genuinely non-locked callers, or update both call sites
directly. Until this is resolved, M-2 and M-3's "unit #2" claims (below) cannot be verified because the
method they describe does not contain the sequence they describe.

## 3. M-1 / M-4 — re-checked, no new issue

- **M-1** (`decode_and_persist` independent consumer via `try_filtered_commit_fetch`): confirmed
  unaffected. §15 still correctly states `decode_and_persist` is **not modified**; its existing brief,
  independent `acquire_repo_lock`/`release_repo_lock` usage is untouched by anything in this design.
  Resolved, no residual concern beyond what B-3 already documents for the *other* lock consumers.
- **M-4** (`ZAOG_FETCH_SESS`/`ZAOG_PACK_META` never written by the default streaming path): §9's wording
  is now correctly scoped ("the 4-table join is a superset view available when the legacy path is
  exercised, and a 2-table... join is always available regardless of which decode path served the
  attempt"). No contradiction found this pass. Resolved.

## 4. M-2 / M-3 — re-checked; sound for unit #1, unverifiable for unit #2 pending B-3

- **Unit #1 (`pull_by_branch`'s own Phase-1b branch):** the narrowed lock scope (acquire immediately
  before `resume_decode`/`persist_pull_result`/`certify_fetched_commit`, release immediately after,
  never spanning the earlier `branches()` HTTP call or the `resume_decode` session lookup that precedes
  it in the method) is confirmed sound against the real method body — this whole branch is already a
  tight, local, in-memory-plus-one-persist sequence with no HTTP round-trip inside it. The
  exception-handling fix (catch `acquire_repo_lock`'s `zcx_abapgit_exception` locally, fall back like the
  existing "ORTEC fastpath not applicable" pattern) is straightforward to add at this one call site and
  consistent with this same method's existing `CATCH zcx_abapgit_exception INTO lx_pull` pattern used a
  few lines below. **M-2 and M-3 are resolved for unit #1.**
- **Unit #2:** as established in §2 (B-3), the method the design names for unit #2
  (`upload_pack_by_branch`) does not contain a persist/certify/commit sequence to wrap, so neither the
  "narrowed lock scope" claim nor the "exception caught at this new call site" claim can be verified
  against real source — they describe a call site that does not exist. **M-2 and M-3 remain open for
  unit #2 until B-3's re-location fix is designed**, at which point they must be re-verified against
  whatever method actually ends up hosting the second lock/attempt unit (most likely
  `zcl_abapgit_ortec_porcelain=>pull_by_branch`'s `INCREMENTAL_UPDATE` branch, per §2).

## 5. M-5 — re-checked; mechanically correct, effectiveness contingent on B-3

`persist_missing_objects`'s real body (`zcl_abapgit_ortec_fastpath.clas.abap:1543`) builds each new
`zaog_obj_store` row (`ls_row-repo_key`, `-obj_sha1`, `-obj_type`, `-obj_data`, `-obj_size`,
`-created_at`, `-status = 'R'`) in a simple `LOOP`; adding `ls_row-attempt_id = iv_attempt_id.` and a new
`IMPORTING iv_attempt_id` parameter is a small, mechanically correct change exactly as §9 describes, and
its one caller (`persist_pull_result`, which already calls
`persist_missing_objects( iv_repo_key = lv_repo_key it_objects = it_objects ).` unconditionally at its
top) can forward it trivially. **No defect in the change itself.** However, its real-world value is
gated on B-3: `persist_missing_objects` only ever receives a *meaningful* `attempt_id` if its caller
(`persist_pull_result`) itself received one from a caller that actually holds a lock and called
`begin_attempt` — which, per §2, does not happen today for the (more common) fresh-fetch path. Recommend
re-confirming M-5 once B-3 is fixed and unit #2's real location is settled.

## 6. m-1 (status-split discrimination, `OBJ_TYPE IN ('ref_d','ofs_d')`) — carried over

Not part of this pass's explicit re-check list (task items 1-6 do not mention it) and nothing found this
pass contradicts the prior confirmation. Left as resolved/non-blocking per the design's existing §15 D2
file-scope note.

---

## Findings summary

| ID | Severity | Status | Summary |
|----|----------|--------|---------|
| B-1 | was BLOCKING | **RESOLVED** (fix sound; rationale text imprecise, see m-2) | Lock/attempt-id now scoped inside `pull_by_branch`'s own Phase-1b branch — caller-agnostic, covers whichever caller can reach it. |
| B-2 | was BLOCKING | **RESOLVED** | `acquire_repo_lock`/`release_repo_lock` confirmed `PRIVATE` today; planned `PRIVATE`→`PUBLIC` move is sufficient and sole required change. |
| B-3 | **NEW BLOCKING** | OPEN | The design's "unit #2" (inside `zcl_abapgit_ortec_fastpath=>upload_pack_by_branch`) does not contain any persist/certify/commit sequence. The real sequence for a fresh-HTTP-fetch attempt runs one level up, in `zcl_abapgit_ortec_porcelain=>pull_by_branch`'s `INCREMENTAL_UPDATE` branch — a class/method entirely absent from §15's scope — via its own direct call to `persist_pull_result`. Two more pre-existing, unaddressed external callers of `persist_pull_result` exist (`zcl_abapgit_git_porcelain.clas.abap:650`, a unit test). As scoped, the design either fails to compile (mandatory param) or silently defeats its own correlation goal for the common case (optional param with internal fallback), unlocked. |
| M-1 | MAJOR | **RESOLVED** | `decode_and_persist`/`try_filtered_commit_fetch` confirmed untouched and unaffected. |
| M-2 | MAJOR | **RESOLVED for unit #1 / OPEN for unit #2** | Narrowed hold-duration confirmed sound for `pull_by_branch`'s Phase-1b branch; unverifiable for the mislocated unit #2 pending B-3. |
| M-3 | MAJOR | **RESOLVED for unit #1 / OPEN for unit #2** | Exception-conversion/fallback fix confirmed addable at unit #1's call site; unverifiable for unit #2 pending B-3. |
| M-4 | MAJOR | **RESOLVED** | §9 wording now correctly scopes the 4-table-join benefit to the legacy resumable-session path only. |
| M-5 | MAJOR | **RESOLVED mechanically / effectiveness OPEN pending B-3** | `persist_missing_objects`'s own change is correct; its value depends on receiving a real `attempt_id`, which B-3 shows does not reliably happen today. |
| m-1 | minor | carried over, RESOLVED | Not re-examined this pass; no contradicting evidence found. |
| m-2 | **NEW minor** | OPEN (non-blocking) | §11's rationale overstates B-1's coverage — the porcelain-direct call site to `pull_by_branch` can never reach Phase-1b (dead code, guarded by a switch check it can never pass); correct the wording so it doesn't imply this is a second live path for the concern. |

---

## Required revisions before next re-review

1. **B-3 (blocking):** re-locate "unit #2" to `zcl_abapgit_ortec_porcelain=>pull_by_branch`'s
   `INCREMENTAL_UPDATE` branch (or wherever the orchestrator determines the real second attempt boundary
   should be, but it must be justified against the actual call graph traced in §2 of this review, not
   against `upload_pack_by_branch` as currently written). Add that class/method to `SOURCE_SCOPE`/§15.
   Explicitly resolve the two other pre-existing `persist_pull_result` call sites
   (`zcl_abapgit_git_porcelain.clas.abap:650`, the existing unit test) so the new parameter cannot break
   compilation or silently reintroduce an unattempted/unlocked correlation gap.
2. Once B-3 is fixed, re-verify M-2/M-3/M-5 against whichever method now hosts unit #2.
3. Fold in m-2: correct §11's rationale text for B-1 to state plainly that only the nested
   (`upload_pack_by_branch`-internal) call site can reach Phase-1b today; the porcelain-direct call site
   is covered only incidentally/defensively, not because it is a live second path.
