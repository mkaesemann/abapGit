# Design Review — F2-BRANCH-TIP-DEDUP-DESIGN

```text
REVIEWED_ARTIFACT=.memory/logs/f2_branch_tip_dedup_design.md
TASK_ID=F2-BRANCH-TIP-DEDUP-DESIGN
MODE=DESIGN_GATE correctness review (documents-only; no productive code touched)
SOURCE_VERIFIED_AGAINST=src/ortec/git/zcl_abapgit_ortec_porcelain.clas.abap,
  src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap (+ .testclasses.abap),
  src/git/zcl_abapgit_git_transport.clas.abap,
  src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap,
  src/ortec/git/zcl_abapgit_ortec_have_policy.clas.abap,
  src/ortec/git/zcl_abapgit_ortec_repo_state.clas.abap (invalidate_all_history,
  one level beyond SOURCE_SCOPE, needed to verify the §7.2 retry-cascade claim),
  src/git/zcl_abapgit_git_porcelain.clas.abap, src/git/zcl_abapgit_git_commit.clas.abap,
  src/repo/stage/zcl_abapgit_merge.clas.abap, src/repo/zcl_abapgit_repo_online.clas.abap
  (one level beyond SOURCE_SCOPE, needed to verify §5's "no async/RFC boundary" claim)
```

## Verdict
APPROVE_WITH_MINOR_REVISIONS

## Confidence
High — every call-site citation, signature, and invariant claim checked in this
pass was verified directly against current source (10+ spot-checks, exceeding
the 3-check minimum), including two one-level dependency hops needed to settle
the retry-cascade `COMMIT WORK` question and the "same call stack" question.

## Strengths

- **Mechanism is sound and minimal.** Threading a single resolved SHA1 as a
  new `OPTIONAL` parameter, consumed only at site 2, with an `ELSE` branch
  that is a byte-for-byte copy of today's code, is the right shape for this
  problem. `INV-1` (initial ⇒ unchanged behavior) is verified true by direct
  read of `zcl_abapgit_ortec_fastpath.clas.abap:671-678`.
- **Call-site inventory (§1) and all five CHANGE blocks (§4) check out against
  current source almost line-for-line** — porcelain's site-1 anchor
  (`lv_target_commit = zcl_abapgit_git_transport=>branches(...)`), the retry
  block, `zcl_abapgit_git_transport=>upload_pack_by_branch`'s signature/body,
  and `zcl_abapgit_ortec_fastpath`'s `pull_by_branch`/`upload_pack_by_branch`
  signatures and the `ls_pull = pull_by_branch(...)` forward call all matched.
  Only trivial "~line" offsets, consistent with the design's own approximate-
  line convention.
- **Caller inventories are exactly right.** Grepped independently:
  `zcl_abapgit_git_transport=>upload_pack_by_branch` has exactly the four
  callers named in §4.1 (`zcl_abapgit_git_commit`, `zcl_abapgit_git_porcelain`,
  `zcl_abapgit_ortec_porcelain` ×2, `zcl_abapgit_merge`), all use named
  parameters (no positional-call risk). `zcl_abapgit_ortec_fastpath=>
  upload_pack_by_branch` has exactly one caller. `zcl_abapgit_ortec_fastpath=>
  pull_by_branch` has exactly one internal (same-class, unqualified) caller —
  confirmed, though see DR-003 on the grep pattern used to justify this.
- **Hard-constraint compliance confirmed by source, not just by assertion**:
  `classify_operation` (have_policy.clas.abap:134-160) does only two SELECTs,
  no HTTP, no `COMMIT WORK` — matches its own docstring and is untouched by
  this design. No DDIC object is referenced by the change. No fetch-mode
  parameter (`deepen`, thin/self-contained/recovery tier selection) is read
  or written by the new parameter — it only feeds a SHA1 comparison.
  `zcl_abapgit_ortec_filter_walk`/site 4 has zero call edge from any of
  porcelain/fastpath/git_transport (grep-confirmed; the one `filter_walk`
  mention inside fastpath is a doc-comment, not a call) — §7.4's "proven
  inapplicable" claim holds.
- **No async/RFC boundary exists in this call chain.** Traced
  `zcl_abapgit_repo_online=>fetch_remote` → `zcl_abapgit_git_porcelain=>
  pull_by_branch` → `zcl_abapgit_ortec_porcelain=>pull_by_branch` → …:
  a plain synchronous call chain, no `CALL FUNCTION ... STARTING NEW TASK`/
  RFC destination anywhere in it (the `STARTING NEW TASK` occurrences found
  elsewhere in `src/**` belong to unrelated UI-jump and object-serialization
  parallelization paths). §5's "same call stack, same dialog step" premise is
  correct.
- **§12.2's testability-ceiling framing is honest, not evasive.** The cited
  test method names/comments (`lock_not_held_over_http`,
  `porcelain_path_gets_lock`, `resume_new_attempt_when_new`,
  `resume_reuses_attempt`, `one_attempt_one_id`) all exist verbatim in
  `zcl_abapgit_ortec_fastpath.clas.testclasses.abap` with the quoted "no mock
  seam" rationale. Because the new `IF/ELSE` branch (§4.5) is independently
  auditable by direct code review (its inputs/outputs are fully traced in
  this review), citing the pre-existing gap does not appear to be used to
  dodge scrutiny of the new logic itself.
- **Rejected alternatives (§11) are well-reasoned**, in particular correctly
  rejecting a `CLASS-DATA`/TTL cache (the actual cross-request-leakage risk
  this mode's invariants are worried about) and correctly identifying that
  moving resolution into `classify_operation` would violate that method's own
  documented "no HTTP" contract.
- **No bulk/batch/per-object anti-pattern is present.** This is a pure
  control-flow change (which existing call performs one already-existing
  HTTP GET); the mandatory performance-review trigger list in this mode
  (per-object SQL/HTTP, row-only batch limits, payload-for-presence-check,
  persistence-in-recursion, full-repo scans) does not apply here because
  there is no data-volume dimension to this change at all — confirmed by
  source, not just by the design's own §9/§10 assertion.

## Issues

### DR-001
- Type: correctness
- Severity: major (not blocking)
- Evidence: §7.3 states `find_branch_ortec` (site 3) "is always executed
  live" and is "the final arbiter" whose fresh resolution "is the only value
  that reaches the wire." Direct read of `zcl_abapgit_ortec_fastpath.clas.abap`
  shows this is false: `upload_pack_by_branch` (line ~913-917) calls its own
  `pull_by_branch` and `RETURN`s immediately — **never reaching
  `find_branch_ortec`** — whenever that call returns non-initial. `pull_by_branch`
  itself has *two* such early-`RETURN` shortcuts that bypass site 3 entirely:
  Phase 1b (resume-decode match, ~line 730-786, `RETURN. " Success! Avoid
  redundant GET from remote."`) and Phase 3 (remote-unchanged reconstruction,
  ~line 820-865, `RETURN.` after reconstituting from `zaog_obj_store`). Site 3
  is reached only on the Phase 2b fallthrough (no previous fetch, or remote
  genuinely changed).
- Why it matters: §7.2 cites §7.3's "final arbiter" claim as the reason the
  *reverse* failure direction ("incorrectly treat unchanged as moved") is
  safe ("only costs an extra fetch, never correctness, per §7.3's 'final
  arbiter' argument"). That specific supporting citation is not accurate as
  written — site 3 does not backstop every path. This review independently
  re-derived why the design's actual conclusion still holds regardless (see
  "why it doesn't change the verdict" below), but the design's own written
  proof should not rest on a claim that is contradicted by the very source
  file it cites throughout §4.
- Why it doesn't change the verdict: Phase 1b and Phase 3 **already never
  re-verify via site 3 today**, independent of this design — this is a
  pre-existing property of `zcl_abapgit_ortec_fastpath=>pull_by_branch`'s
  shortcut logic, not something F-2 introduces. Whether site 2 gets its tip
  value from its own fresh GET (today) or from the reused `iv_known_branch_tip`
  (after this design), the shortcuts' inherent "trust the check, don't
  re-verify at the wire" exposure window is the same order of magnitude in
  both cases (both are separated from the relevant remote reality check by a
  handful of synchronous ABAP statements, never a yield point). So the
  substitution does not *widen* an existing race window — it just changes
  which of two adjacent GETs supplies the value.
- Fix: Revise §7.3 to state precisely that site 3 is the final arbiter
  **only on the non-shortcut path** (Phase 2b), and rewrite §7.2's "reverse
  direction is safe" argument on the correct grounds: Phase 1b/Phase 3 do not
  re-verify via site 3 *today either*, so reusing site 1's value does not
  introduce a new staleness exposure beyond what already exists in those two
  shortcuts.

### DR-002
- Type: correctness
- Severity: major (not blocking)
- Evidence: §5 point 3 claims "There is no `COMMIT WORK` between the value's
  resolution (site 1) and its last consumption (inside site 2's
  `pull_by_branch`) for the path that matters" and "the only `COMMIT WORK` in
  `pull_by_branch`'s own body occurs *after* the `upload_pack_by_branch` call
  chain has already returned." This is contradicted by the design's **own**
  §4.3 second CHANGE block: the history-repair retry cascade in
  `zcl_abapgit_ortec_porcelain=>pull_by_branch` executes
  `invalidate_all_history(...)` then `COMMIT WORK.` **before** the second
  `upload_pack_by_branch` call, which (per that same §4.3 block) forwards the
  identical `iv_known_branch_tip = lv_target_commit` — i.e., site 2 is
  consumed a second time *after* a `COMMIT WORK`, not only "before" the
  chain "has already returned."
- Why it matters: this is a direct internal inconsistency between §4.3 (which
  the design itself wrote) and §5 (the safety proof) — exactly the kind of
  evidence-precision gap this review is chartered to catch under the "never
  allow missing/stale data to be silently mis-certified" spirit of this
  review's invariants.
- Why it doesn't change the verdict: `COMMIT WORK` commits the current LUW to
  the database; it does not unwind the ABAP call stack, end the dialog
  step/work process, or clear local variables/parameters. The actual
  guarantee §5 needs — "still the same call stack, same work process, same
  dialog step, no cross-request leakage" — holds across a `COMMIT WORK`
  statement just as well as across any other statement in the same method.
  Independently confirmed via `zcl_abapgit_ortec_repo_state=>
  invalidate_all_history` (blanks `zaog_repo_state-fetch_commit` for the
  whole repo key), which additionally guarantees Phase 2's `IF
  ls_state-fetch_commit IS INITIAL. RETURN.` fires on the retry, so Phase 3's
  "remote unchanged" shortcut cannot fire on the retry attempt regardless of
  which tip value is reused — removing any remaining doubt for this specific
  path.
- Fix: correct §5 point 3 to acknowledge the retry-cascade's intervening
  `COMMIT WORK` and state the *actual* invariant relied upon (no call-stack
  unwind / no new dialog step, not "no COMMIT WORK").

### DR-003
- Type: maintainability (evidence accuracy)
- Severity: minor
- Evidence: §12.1 states "the full `ltcl_fastpath` (25 methods)... must be
  re-run." Counting `FOR TESTING` methods in the class definition
  (`zcl_abapgit_ortec_fastpath.clas.testclasses.abap:20-46`) yields 20, not
  25 (`setup`/`teardown`/`cleanup_repo`/`build_commit` are non-test helper
  methods and should not be included in the count).
- Why it matters: purely a documentation-precision nit; does not change the
  validity of the regression-gate argument (an unmodified pass of all 20
  existing tests is still the intended, correct acceptance signal).
- Fix: correct the count to 20 (or say "the full existing test suite"
  without a specific number, to avoid drift if tests are added later).

## Required revisions

1. Correct §7.3's "always executed live / final arbiter" claim (DR-001) and
   re-ground §7.2's reverse-direction argument on the accurate mechanism
   (Phase 1b/Phase 3 already never re-verify via site 3, today or after this
   change).
2. Correct §5 point 3's "no `COMMIT WORK` between resolution and last
   consumption" claim (DR-002) to account for the retry-cascade's
   intervening `COMMIT WORK`, and state the real invariant (call-stack
   continuity, not absence of `COMMIT WORK`).

## Optional improvements

- Fix the `ltcl_fastpath` method count in §12.1 (DR-003).
- §4.5's STOP_IF verification note for `pull_by_branch`'s single-caller claim
  cites grepping for the qualified name `zcl_abapgit_ortec_fastpath=>
  pull_by_branch`, but the one actual call site (line 913) is an unqualified,
  same-class `pull_by_branch(` call that pattern would not match. Correct to
  future implementers so the STOP_IF gate is actually effective (grep for the
  bare method name scoped to the file, as this review did).

## Answers to the review's focus questions

1. **4→2 scoping**: correct in substance. No missed dedup opportunity was
   found (site 3 is structurally tied to the live HTTP connection reused for
   the POST; site 4 has no call edge to thread a parameter through). No real
   risk is hidden by the scoping choice itself — the risks found (DR-001,
   DR-002) are in the *written justification*, not in an actual gap in what
   was chosen to change vs. leave alone.
2. **Spot-checks**: 10+ performed (exceeds the 3 minimum), all call sites,
   signatures, and CHANGE-block anchors matched current source.
3. **§7.2 staleness proof**: core conclusion (reuse cannot newly cause "moved
   treated as unchanged") holds; the "no existing cross-check" premise is
   trivially true (the two values live in unrelated classes today). The
   proof's cited support for the reverse direction (§7.3) is inaccurate — see
   DR-001.
4. **§5 sharing-scope-safety**: accurate as to the actual conclusion
   (verified: plain synchronous call chain, `zcl_abapgit_repo_online=>
   fetch_remote` → ... → `zcl_abapgit_ortec_fastpath=>pull_by_branch`, no
   RFC/`STARTING NEW TASK` anywhere in it), but one supporting factual claim
   about `COMMIT WORK` placement is wrong — see DR-002.
5. **Hard-constraint compliance**: confirmed by source — zero
   `classify_operation` changes, zero persistent cache, zero fetch-mode
   change, zero DDIC change.
6. **§12.2 honesty**: assessed as honest; cited test names/comments are real,
   and the new branch's correctness was independently auditable by direct
   source reading in this review regardless of the automated-test gap.
7. **No productive code changed**: confirmed — this review only reads source
   and writes the single permitted output artifact.
