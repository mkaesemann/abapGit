# Performance design review — variant-b-partial-clone / Slice 2 (explicit ORTEC fetch modes and one request serializer)

Mode: `DESIGN_GATE`
Date: 2026-07-21
Reviewed: `.memory/logs/variant_b_slice2_design.md` in full, including "Review
resolution (correctness review, 2026-07-21)" and "Review resolution
(protocol/persistence review, 2026-07-21)", against §7 "Mandatory performance
model", DR-004, `.github/skills/abap-performance-patterns/SKILL.md`,
`.github/skills/git-partial-clone/SKILL.md`, and current productive source.

## Verdict: `APPROVE_WITH_MINOR_REVISIONS`

## Evidence verified against current source (not just the design doc's claims)

- **Item 1 — `build_request`/`parse_capabilities` zero SQL/HTTP (§2.2
  signatures):** confirmed structurally. Neither signature takes a repo key,
  a `lo_client`/`zcl_abapgit_http_client` reference, nor any DB-bearing type.
  Cross-checked against the two call sites this design replaces —
  [zcl_abapgit_ortec_fastpath.clas.abap](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L462-L500)
  (`try_filtered_commit_fetch`) and
  [zcl_abapgit_ortec_fastpath.clas.abap](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L323-L390)
  (`fetch_tip_commits`) — both currently duplicate an *identical* inline
  capability-parse block (`FIND FIRST OCCURRENCE OF <NUL>` →
  offset → `FIND FIRST OCCURRENCE OF newline` → substring → `CS 'filter'`)
  operating purely on a `string` (`lv_ref_data`) already obtained via
  `lo_client->get_cdata()` *before* the design's proposed `parse_capabilities`
  would run. The HTTP call that produces that string is a pre-existing,
  already-necessary connection step, not something `parse_capabilities`
  itself performs — confirmed genuinely pure. AC1 is realistic, not
  aspirational.
- **Item 2 — `is_commit_complete` swap is a genuine, re-confirmed
  improvement:** re-verified against the *current* (not-yet-swapped) body of
  [zcl_abapgit_ortec_fetch_neg.clas.abap](src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap#L159-L204):
  it still calls `zcl_abapgit_ortec_obj_store=>get_reachable_sha1s` (a real
  reachable-object graph walk) plus `has_dangling_delta_base` for every
  have-candidate. Cross-checked the swap target,
  [zcl_abapgit_ortec_mat_state.clas.abap](src/ortec/git/zcl_abapgit_ortec_mat_state.clas.abap#L410-L422)
  `is_graph_have_eligible`: exactly one `SELECT SINGLE hist_level FROM
  zaog_commit_hist WHERE repo_key = iv_repo_key AND commit_sha1 =
  iv_commit` — the full primary key of `ZAOG_COMMIT_HIST`
  (`MANDT+REPO_KEY+COMMIT_SHA1`, confirmed unchanged by Slice 1's own
  already-audited DDIC evidence in
  [performance_design_variant-b-partial-clone_slice1.md](.memory/reviews/performance_design_variant-b-partial-clone_slice1.md)).
  This is the same finding Slice 1's audit already established (re-confirmed
  here against current source per the task instruction, not re-derived from
  a tree-walk cost model from scratch) — genuine O(reachable-graph-size) →
  O(1) per commit.
- **Item 3 — batch caps are hard, not advisory:** the pre-existing have-cap
  is confirmed hard in
  [zcl_abapgit_ortec_fetch_neg.clas.abap](src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap#L98-L132)
  (`IF lines( rt_haves ) > 200. DELETE rt_haves FROM 201.` — unconditional,
  no caller override). The new `c_materialize_batch_max = 100` does not
  exist yet (Slice 2 unimplemented); the design's AC3 commits to enforcing
  it as a hard `RAISE` inside `build_request` itself rather than a caller
  convention — the correct shape per the mandatory batching rule, and
  consistent with how the existing 200-cap is already enforced centrally
  rather than at each caller. Acceptable at `DESIGN_GATE` (code does not
  exist yet); binding for the `IMPLEMENTATION_AUDIT` to re-verify as an
  actual `RAISE`, not a silent truncation.
- **Item 4 — `reset_completion_budget()` cost:** confirmed against
  [zcl_abapgit_ortec_pack_stream.clas.abap](src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.abap#L314-L316):
  `METHOD reset_completion_budget. gv_completion_attempts = 0. ENDMETHOD.` —
  a single static-variable assignment, zero DB/HTTP/table operations. The
  two existing call sites
  ([zcl_abapgit_ortec_fastpath.clas.abap](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L771-L772),
  [L943](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L943)) each fire
  once per whole `upload_pack_by_branch`/`upload_pack_by_commit` call, before
  tier 1 — the DR-004 addition (one more call immediately before the
  `RECOVERY_BRANCH_FULL` tier, replacing today's `DO
  c_progressive_max_steps TIMES` loop body which currently does **not**
  call it per iteration) adds at most one extra trivial call per logical
  fetch. No performance concern, confirmed against source rather than
  trusted from the design's prose.
- **Item 5 / N-independence claim — partially confirmed, one real gap
  found:** every method Slice 2 actually adds or changes
  (`build_request`, `parse_capabilities`, `raise_unsupported_capability`,
  and `is_commit_complete`'s new one-line delegating body) contains **zero**
  references to `ZAOG_OBJ_STORE` — confirmed by direct inspection, not
  accepted from the design's assertion. However, the design's §7 prose
  overstates this by implying the whole upstream have-resolution *pipeline*
  is now N-independent. `get_have_commits` (unchanged, still the sole
  supplier of `it_certified_haves` for every mode Slice 2's caller will use)
  calls `collect_ancestor_haves`
  ([zcl_abapgit_ortec_fetch_neg.clas.abap](src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap#L224-L230)),
  which runs:
  ```abap
  SELECT obj_sha1, obj_data FROM zaog_obj_store
    INTO TABLE @lt_all_commits
    WHERE repo_key = @iv_repo_key AND obj_type = 'commit' AND status = 'R'.
  ```
  `OBJ_TYPE` is not part of `ZAOG_OBJ_STORE`'s primary key
  (`CLIENT+REPO_KEY+OBJ_SHA1`, confirmed from the DDIC XML) nor of the one
  secondary index found (`REPO_KEY+PACK_ID+STATUS+OBJ_SHA1`) — this reads
  **every ready commit object's full `obj_data`** for the repository,
  unbounded by the 100/200 caps (those are only applied *after* this full
  read, when trimming the resulting BFS). This is a genuinely
  repository-commit-count-dependent cost, on the same hot path Slice 2's
  `build_request` depends on for `it_certified_haves`, executed on every
  `upload_pack` call that resolves haves (i.e. both `INCREMENTAL_THIN` and
  `INCREMENTAL_SELF_CONTAINED` tiers, up to 2× per logical fetch). It is
  **pre-existing and unchanged by Slice 2** (confirmed: Slice 2's design
  explicitly leaves `get_have_commits` untouched, §4), so it is not a new
  cost this slice introduces, and it does not block Slice 2's own new code.
  But §7's claim that "the upstream have-certification step is likewise now
  N-independent" is not fully accurate as written — only the per-candidate
  *certification* step (`is_commit_complete`) became N-independent; the
  *candidate-collection* step (`collect_ancestor_haves`) was never
  N-independent and remains so after Slice 2.
- **Item 6 — no other missing mandatory-model dimension found.** Cardinality,
  SQL/HTTP estimates, row/byte batch limits, cache scope (explicitly "none",
  justified the same way as Slice 1's mat_state), transaction ownership (N/A,
  zero writes, grep-verifiable), and 1/1,000/40,000/1,000,000-object expected
  behavior are all present in §7. Internal-table lookup complexity is
  implicitly trivial and correctly unaddressed: `build_request` only ever
  performs a single linear `LOOP AT` over `it_want_hashes`/
  `it_certified_haves` to emit lines, no nested lookup structure is needed.
  One process gap (not a performance-value gap): unlike §5's DR-001/DR-002
  Slice-3 "binding consequence" paragraphs, the `RECOVERY_BRANCH_FULL`
  memory-risk gate (owner spec: "the current HTTP layer may materialize the
  full response XSTRING") is left as a soft mention ("a Slice 5
  orchestration decision") rather than a named, numbered binding consequence
  for whichever slice wires branch pull/switch decisions. Recommend the
  design adopt the same explicit-binding-consequence pattern it already uses
  elsewhere so this does not silently fall through the slice boundaries.

## Scale check (1 / 1,000 / 40,000 / 1,000,000 stored objects)

- `build_request`/`parse_capabilities`: cost is a pure function of
  `min(want-count, 100)` and `min(have-count, 200)` at every scale — constant
  and N-independent, confirmed by structural absence of any DB/HTTP
  reference in these methods.
- `is_commit_complete` (post-swap): O(1) at every scale, re-confirmed against
  current `is_graph_have_eligible` source (single PK-keyed `SELECT SINGLE`).
- `get_have_commits`/`collect_ancestor_haves` (pre-existing, unchanged):
  **not** N-independent — cost scales with the count of `status = 'R'`
  commit objects stored for the repository (a repo with tens of thousands of
  commits pays a proportionally larger `SELECT` and BFS on every fetch
  attempt that resolves haves). This is an existing condition carried
  forward unchanged, not a regression introduced by Slice 2, and is outside
  this slice's touched-file list (§8) — noted for tracking, not blocking.
- `reset_completion_budget`: O(1) at every scale (single static assignment).
- Net repository-object-count independence of Slice 2's own diff: confirmed
  true. Net repository-object-count independence of the *entire* fetch-mode
  pipeline the design's prose implies: not fully true, per the finding above.

## Minor observations (non-blocking)

- The design's own "follow-up optimization opportunity" (§7: a bulk
  `is_graph_have_eligible` variant to replace up to 200 `SELECT SINGLE`
  calls with one `FOR ALL ENTRIES`) is correctly scoped as optional given
  each call is already O(1) and bounded at 200 — matches the mandatory
  model's "batch limits, not correctness" framing. Not a Slice 2 blocker.
- `MATERIALIZE_BLOBS`'s want-count validation belongs in `build_request`
  itself per AC3 (already the design's stated intent) — flagged here only
  to confirm this reviewer's independent agreement, not as a new
  requirement.
- The `fetch_tip_commits` capability-parse duplicate (§3, deliberately left
  as a 6th, hand-built shape) and `try_filtered_commit_fetch`'s duplicate
  differ in one respect not previously called out: `fetch_tip_commits`
  wraps its parse in `TRY...CATCH cx_sy_range_out_of_bounds` (defensive),
  `try_filtered_commit_fetch` does not. This is pre-existing, already
  flagged by the design (§6) as a preserve-not-fix edge case, and has no
  performance implication either way — mentioned only for completeness.

## Blocking findings

None. All architectural elements of Slice 2 itself (mode enum, pure
serializer, hard `RECOVERY_BRANCH_FULL` single-attempt replacement of the
progressive-deepen loop, hard `MATERIALIZE_BLOBS` cap, trivial
`reset_completion_budget` addition, O(1) `is_commit_complete` swap) satisfy
the mandatory performance model and introduce no new SQL/HTTP-per-object,
no new repository-wide read, and no missing batch limit.

## Required revision (non-blocking, must land before implementation sign-off)

Correct §7's prose so it does not claim the entire upstream have-resolution
pipeline is now N-independent. Replace with an accurate statement: Slice 2
makes per-candidate *certification* O(1) (was O(tree size)); candidate
*collection* (`collect_ancestor_haves`'s unbounded-by-cap `SELECT ... FROM
zaog_obj_store WHERE repo_key = ... AND obj_type = 'commit' AND status =
'R'`) remains a pre-existing, commit-count-dependent read, unaffected by
Slice 2, and should be named as a tracked follow-up (recommend attaching it
as a Slice 3 candidate, since Slice 3 already touches the same
have-negotiation/graph-acquisition area, or an explicitly named later slice
if Slice 3's own scope cannot absorb it). Also recommend promoting the
`RECOVERY_BRANCH_FULL` memory-risk gate from a soft mention to a named,
numbered binding consequence for the slice that implements it (consistent
with §5's and DR-002's existing binding-consequence pattern).

## Report path

This file: `.memory/reviews/performance_design_variant-b-partial-clone_slice2.md`

## Next handoff

Implementation may proceed once the design document's §7 prose is corrected
per the revision above (documentation-only change, no architecture change
required) — the underlying architecture already satisfies the mandatory
performance model. `IMPLEMENTATION_AUDIT` for Slice 2 must re-verify AC1–AC6
against actual committed source once implemented, and must re-confirm the
`c_materialize_batch_max` cap is a genuine `RAISE`. The `collect_ancestor_haves`
N-dependent read remains open and should be picked up explicitly in Slice 3's
own `DESIGN_GATE`, not silently carried forward again.

## Gate Closure

- **Original verdict:** `APPROVE_WITH_MINOR_REVISIONS`.
- **Required revisions:**
  1. Correct §7's prose so it does not claim the entire upstream
     have-resolution pipeline is N-independent; state accurately that only
     per-candidate certification became O(1), and name
     `collect_ancestor_haves`'s unbounded `zaog_obj_store` commit read as a
     pre-existing, unaffected-by-Slice-2 cost with a tracked follow-up.
  2. Promote the `RECOVERY_BRANCH_FULL` memory-risk gate from a soft mention
     ("a Slice 5 orchestration decision") to a named, numbered binding
     consequence for the slice that wires branch pull/switch decisions.
- **Resolution evidence:**
  1. `.memory/logs/variant_b_slice2_design.md` §7, "Expected behavior at
     1 / 1,000 / 40,000 / 1,000,000 stored objects" — the overstated
     N-independence sentence was replaced, and a new "Correction
     (performance `DESIGN_GATE` finding, ...)" paragraph now states the
     `collect_ancestor_haves` cost explicitly and tracks it as a Slice 3
     candidate.
  2. `.memory/logs/variant_b_slice2_design.md` §7, "Peak-memory model"
     paragraph — the `RECOVERY_BRANCH_FULL` memory gate now ends with an
     explicit "**Binding consequence:** whichever slice wires branch
     pull/switch decisions (Slice 5 per the top-level spec) MUST implement
     this memory gate as a named, reviewable design element..." sentence,
     replacing the prior soft mention.
- **Remaining non-blocking preconditions:**
  - Bounding/eliminating `collect_ancestor_haves`'s unbounded
    `zaog_obj_store` read is deferred to Slice 3's own `DESIGN_GATE` — not
    required for Slice 2's own file list (§8), which never touches
    `get_have_commits`/`collect_ancestor_haves`.
  - The `RECOVERY_BRANCH_FULL` memory gate's exact threshold/mechanism is
    deferred to Slice 5 — not required for Slice 2's own file list, which
    only marks `ty_request-mode` for the caller and never invokes an HTTP
    client itself.
  - Both preconditions are documentation-tracked, not silently dropped, and
    neither requires any change to Slice 2's approved architecture or
    touched-file list.
- **Final gate status: CLOSED**
- **Implementation authorization:**
  - `AUTHORIZED_FOR_2A_2B`