# Design Review

## Verdict
- APPROVE_WITH_MINOR_REVISIONS

## Confidence
High

## Strengths
- Root-cause diagnosis is exact and evidence-backed.
- The read-only sparse lookup is separated cleanly from the mutating persistent-cache opt-in.
- The explicit object/path states preserve the invariant that not-buffered data is never treated as deletion.
- The phased plan is safe for large repositories and keeps the first implementation slice minimal.

## Issues

### DR-001
- Type: correctness
- Severity: minor
- Evidence: `.memory/logs/target_design.md`, `.memory/decisions/h4_design_decisions_d1_d7.md`
- Why it matters: `walk_tree` was still scheduled too late for a correctness bug.
- Fix: pull the explicit `repo_key` fix into Phase 1 with the read-path gate removal.

### DR-002
- Type: maintainability
- Severity: minor
- Evidence: `.memory/logs/target_design.md`
- Why it matters: the split between read-path and write-path gating needed to be stated explicitly.
- Fix: document the `persist/protocol` versus read-path split in the target design.

### DR-003
- Type: maintainability
- Severity: minor
- Evidence: `.memory/logs/target_design.md`
- Why it matters: Phase 7 had no crisp exit criteria.
- Fix: add concrete validation success criteria for the regression/performance gate.

## Required revisions
- Move the `walk_tree` `repo_key` correction into Phase 1.
- State the read-path/write-path gate split explicitly.
- Add Phase 7 success criteria.

## Optional improvements
- Resolve D1, D4, and D5 with Michael before implementation begins.

## Follow-up Re-Review (2026-07-10)
- Verdict: APPROVE_WITH_MINOR_REVISIONS
- Confidence: High
- Scope checked: D5 admin report feasibility/safety, D6 validation switch, D7 porcelain-change boundary
- Outcome: No architecture blockers. Minor implementation clarifications requested and applied in `.memory/logs/target_design.md`:
	- reachability-set strategy for admin compact/cleanup,
	- TTL cache scope + invalidation triggers,
	- per-repo atomicity for admin maintenance,
	- quantified D7 change budget,
	- measurable Phase 7 thresholds.
- Remaining owner input: choose D6 TTL benchmark default method (`fixed 2s` vs measured RTT-based formula in Phase 6).

## Delta Resolver Review (2026-07-15)

## Verdict
- Narrow hotfix: APPROVE
- Broader redesign: APPROVE_WITH_MINOR_REVISIONS

## Confidence
High

## Strengths
- The bug #4 diagnosis is correct. In `zcl_abapgit_ortec_delta=>resolve_one`, the REF_DELTA branch captures `sy-tabix` inside `LOOP AT ct_objects ... USING KEY sha`, where `sha` is a non-unique sorted secondary key of `ty_objects_tt`. For a sorted secondary-key loop, `sy-tabix` is the row position in the secondary index, not the primary table index. The current code then passes that value to `resolve_one( iv_tabix = lv_base_tabix )` and reads `ct_objects INDEX lv_base_tabix`, both of which interpret it as a primary index.
- The type definition confirms the exact precondition: `ty_objects_tt` is `STANDARD TABLE OF ty_object WITH DEFAULT KEY WITH NON-UNIQUE SORTED KEY sha COMPONENTS sha1 WITH NON-UNIQUE SORTED KEY type COMPONENTS type sha1` in `src/zif_abapgit_definitions.intf.abap`. There is no unique primary-index correlation in the `sha` key.
- The failure shape is plausibly and sufficiently explained by this defect. On a large pack, SHA secondary order will normally differ from pack/primary order. A wrong primary row can itself be an unresolved delta, giving the observed `resolve_one -> resolve_one -> apply` stack and a misleading `Delta copy instruction exceeds base length` when delta bytes are applied against the wrong base.
- The immediate hotfix is minimal and low-risk if it derives a primary index explicitly: capture the stable `index` field from the found base row during the secondary-key loop, then perform a separate non-secondary lookup by `index` to obtain the primary `sy-tabix` before recursion/read/promotion.
- The proposed hybrid A+C redesign is the right architectural direction: an explicit known-identity side-index removes non-unique secondary-key lookup, mutable-key lookup, secondary `sy-tabix`, and recursion/depth-guessing from the critical path.
- OFS_DELTA remains conceptually correct under the redesign because its identity path is still offset-driven: `base_offset -> obj_index -> primary tabix`; the new driver only changes when a dependent waits for the base row to become non-delta.

## Issues

### DR-001
- Type: correctness
- Severity: major
- Evidence: `.memory/logs/delta_resolver_redesign.md`, proposed finalize step: "run one more fixpoint round" after fetching thin bases.
- Why it matters: If "one more round" means one pass, the redesign can still terminate too early. Example: object 1 depends on object 2, object 2 depends on a thin base fetched after the first no-progress pass. One post-fetch pass may resolve object 2 but skip object 1 if object 1 was visited earlier in that pass. Object 1 is then resolvable but left unresolved.
- Fix: Implement the fixpoint as a reusable bounded loop and rerun the full progress loop after any batch of thin bases is appended. The post-fetch phase should continue until a full pass makes no progress, bounded by the current number of delta rows/objects; only then raise for any remaining deltas.

### DR-002
- Type: performance
- Severity: minor
- Evidence: `.memory/logs/delta_resolver_redesign.md`, thin fallback says `get_object` once per still-missing declared base.
- Why it matters: The project rules reject per-object fetch loops in hot paths. Even if this is expected to be rare because `zcl_abapgit_ortec_pack_dec=>resumable_decode` already performs targeted bulk prefetch and raises on missing bases, the fallback should not preserve the old per-base pattern in the replacement design.
- Fix: Collect unique unresolved REF base SHA1s, exclude bases already in `lt_known`, bulk-read via `zcl_abapgit_ortec_obj_store=>get_objects`, append fetched real bases once, register them in `lt_known`, and raise with the remaining missing set.

### DR-003
- Type: correctness
- Severity: minor
- Evidence: Current thin branch in `zcl_abapgit_ortec_delta=>resolve_one` appends `ls_base_object` and then performs `READ TABLE ct_objects ... WITH KEY sha COMPONENTS sha1 = <ls_object>-sha1` against a non-unique key.
- Why it matters: This is not the live bug #4 trigger, but it is the same identity-binding family. A sibling unresolved REF_DELTA can share the declared base SHA1 and be selected instead of the appended real base.
- Fix: If the narrow hotfix is applied before the redesign, bind the freshly appended thin base by its primary append index (`lines( ct_objects )`) or by a primary `index` value assigned for the appended object, not by the non-unique `sha` key.

## Required revisions
- For the immediate hotfix: replace secondary-key `sy-tabix` reuse with an explicit primary-tabix derivation from the matched row's stable `index` field, and bind appended thin bases by primary index rather than by `sha` first-match.
- For the redesign: specify and implement the post-thin-fetch phase as a full bounded fixpoint rerun, not a single pass.
- For the redesign: use a deduplicated bulk object-store read for thin-base fallback.

## Optional improvements
- Add a fixture guard to the new REF chain test that proves the constructed table actually has `sha` secondary order different from primary order, and that the old secondary `sy-tabix` value would point at the wrong primary row. This makes the bug #4 regression test resistant to accidentally becoming a non-reproducing happy path.
- Keep the proposed multi-byte `apply` vectors, and include at least one vector for the `length == 0 -> 65536` copy rule if memory/runtime allows. Current coverage only exercises the single-byte offset/length path.
- Consider extracting the fixpoint pass into a small private helper so both the initial pass and the post-fetch pass share identical termination behavior.

## Recommendation
- Apply the narrow hotfix immediately as a production stopgap. It directly addresses the independently confirmed live defect, is small, and does not require waiting for a larger rewrite.
- Track and implement the full hybrid A+C redesign as the follow-up phase after the stopgap. This is the fourth failure from the same mechanism, so keeping recursion plus mutable/non-unique secondary-key lookup is no longer a defensible long-term design under the correctness-first priority.

## Streaming Decoder + H4 Walk-Delegation Review (2026-07-17)

## Verdict
- REVISE_AND_REVIEW_ONCE

## Confidence
High

## Strengths
- The two designs compose in the intended direction: the streaming decoder deliberately returns a sparse object table, and H4 is promoted to mandatory specifically to prevent that sparse table from degenerating into one store lookup per tree/blob during plain pull.
- The phasing choice is directionally sound. H4 can provide value before the streaming decoder by fixing the already-known partly-buffered branch-switch/walk case, and the streaming decoder depends on H4 for plain-pull performance once sparse `it_objects` becomes normal.
- The design keeps Ortec concerns isolated through new `zcl_abapgit_ortec_*` classes, with `zcl_abapgit_ortec_walk_prep`, `zcl_abapgit_ortec_porcelain`, `ZCL_ABAPGIT_ORTEC_PACK_STREAM`, and `ZCL_ABAPGIT_ORTEC_BASE_CACHE` all within the ABAP 30-character object-name limit.
- The central delta premise is now empirically supported by Spike A: applying a delta against a DB-fetched base has parity with applying against an in-memory base.
- The permanent platform constraints are correctly accepted: the compressed HTTP response must fit in memory, and there is no planned streaming SHA1 micro-fix.

## Issues

### DR-001
- Type: correctness
- Severity: blocking
- Evidence: `.memory/state.md`, Streaming decoder decisions (6) and (7): for Ortec-active repos, streaming becomes the default path; the old `resumable_decode` path is an explicit fallback; retaining `zaog_fetch_sess` is preferred but may be dropped if it conflicts.
- Why it matters: The fallback cascade is required by Michael, but the design does not yet define what happens to rows already persisted by the streaming decoder before it fails. If partial rows are visible as normal loaded objects, the old fallback can read inconsistent bases, mix old/new pack state, or leave `CORRUPT_OR_INCOMPLETE` data queryable as if it were good cache. That is a correctness hazard, not just cleanup debt.
- Fix: Define the failure boundary before implementation. Streaming writes need session-scoped or status-scoped isolation so partially decoded objects are not exposed as loaded. On failure, either atomically roll back/delete the fetch session's partial rows before invoking `resumable_decode`, or mark them `CORRUPT_OR_INCOMPLETE` and require every object-store read path used by fallback/H4/delta resolution to exclude that state. The fallback cascade must include this cleanup/isolation step explicitly.

### DR-002
- Type: correctness | performance
- Severity: major
- Evidence: `.memory/state.md`, streaming decoder architecture: per-pass bulk base prefetch reuses the existing `FOR ALL ENTRIES` pattern so DB round-trips are per-pass, not per-object; the new LRU has a hardcoded 256MB byte budget.
- Why it matters: Bulk round-trips alone do not guarantee bounded memory. A per-pass `get_objects`/FAE that materializes all distinct base bytes for a large pass can recreate the same memory ceiling the design is meant to remove, just in a different internal table. The LRU budget only helps if base-byte loading itself is byte-bounded.
- Fix: State that delta-base prefetch has two layers: set-based discovery of needed base SHA1s/offsets, followed by byte-budgeted base loading/application batches. No implementation path may load all base bytes for a pass into one internal table unless the total byte estimate is within the active budget. Preserve bulk SQL by fetching batches, not by falling back to per-object SELECTs.

### DR-003
- Type: correctness
- Severity: major
- Evidence: `.memory/state.md`, H4 decisions (1), (2), and streaming decision (3): `fetch_blobs_bulk` is byte-budgeted; blobs are never merged wholesale into `it_objects`; the base-cache budget is hardcoded at 256MB.
- Why it matters: Byte budgets need an explicit progress rule for a single object larger than the budget. Without it, H4 blob batching or the base-cache admission path can either loop forever, reject a valid object, or try to cache/load more than intended. A single very large blob/base can still require one full object in memory because ABAP delta apply and `rt_files` are not streaming; that limit should be explicit rather than accidental.
- Fix: Add an oversize-object rule. A batch must always make progress by allowing one oversize object to be processed alone. Oversize objects should bypass LRU admission after use instead of forcing the cache above its hard budget. If even one-object processing exceeds available memory, fail cleanly through the streaming-fallback/self-heal path and leave persistent state isolated as described in DR-001.

### DR-004
- Type: performance | correctness
- Severity: major
- Evidence: `.memory/state.md`, H4 two-pass mechanism and decisions (2), (4), (5): trees are merged into `it_objects`, blobs are read in bounded batches and never merged, `rt_files` materialization is accepted, and the batched hook is routed through new `zcl_abapgit_ortec_porcelain`.
- Why it matters: The design says blobs are read in bounded batches, but the mirror `walk` contract is not yet precise enough to prove it will cover all reachable blobs without reverting to the old one-at-a-time store fallback. Repositories with more distinct blobs than fit in one byte-budgeted batch are normal, not an edge case.
- Fix: Specify the mirror walk invariant: after pass 1 has enumerated reachable blob SHAs, pass 2 must iterate all blob SHAs in byte-budgeted batches, serve each blob from the warm batch while building `rt_files`, clear the batch before the next one, and treat any residual per-object fallback as an exceptional correctness backstop, not the normal path. The implementation acceptance test should fail if plain-pull over a sparse table performs a SELECT-SINGLE-per-blob pattern.

### DR-005
- Type: maintainability | correctness
- Severity: major
- Evidence: `.memory/state.md`, H4 decision (5): Michael explicitly chose a new mirror class `zcl_abapgit_ortec_porcelain`; the exact routing mechanism is left as an implementation-phase detail. H4 is also supposed to land first as a separate commit.
- Why it matters: H4-first is only shippable if the mirror can be routed without changing behavior for non-Ortec repos and without forking standard porcelain semantics in a way that drifts immediately. Leaving routing to implementation risks either an oversized standard hook, ping-pong calls between standard and Ortec porcelain, or an H4 commit that cannot be enabled independently.
- Fix: Before coding, record the mirror dispatch contract. At minimum: non-Ortec repos call standard porcelain exactly as today; Ortec-active `pull_by_branch`/`pull_by_commit` route to the mirror with `iv_url`; push remains unaffected unless explicitly routed later; the mirror's full-table case must be a no-op relative to current behavior except for the intended warm-tree/blob-batch paths. Add H4-alone validation with complete `it_objects` and with deliberately sparse `it_objects`.

## Required revisions
- Define the streaming failure/fallback boundary, including cleanup or isolation of partially persisted streaming rows before old `resumable_decode` is attempted.
- Make delta-base prefetch byte-bounded at the byte-loading layer, not only bulk at the SQL round-trip layer.
- Add explicit oversize-object handling for both the 256MB LRU and byte-budgeted H4 blob batches.
- Specify the H4 mirror walk batching invariant for repositories whose blob set exceeds one batch.
- Specify the `zcl_abapgit_ortec_porcelain` routing contract enough to make the H4-first commit independently shippable and reversible.

## Optional improvements
- Add a small design note that the sparse `rt_objects` contract for plain-pull is at least `commit` plus any trees H4 chooses to merge, while blob bytes are served through bounded batches and `rt_files` remains the only full file-content materialization.
- Add acceptance metrics to the implementation plan: peak memory for the IT8 crash path, number of object-store reads during sparse plain-pull, and proof that no per-object SELECT loop appears in the hot walk path.
- Keep the old fastpath fallback behind telemetry/logging while streaming is battle-tested, so a fallback event is visible and not silently normalized.

## Streaming Decoder + H4 Walk-Delegation One-Time Re-Review (2026-07-17)

## Verdict
- APPROVE_WITH_MINOR_REVISIONS

## Confidence
High

## Strengths
- DR-001 is now concretely addressed. `.memory/state.md` defines session/status-scoped streaming writes, a dedicated `CORRUPT_OR_INCOMPLETE` in-progress state, end-of-run set-based promotion to `'P'`, mandatory read-path filtering of incomplete rows, and the ordered cascade: streaming persist -> keyed atomic delete of that session's incomplete rows -> old `resumable_decode` on clean state -> existing walk/self-heal backstop.
- DR-002 is now concretely addressed. Delta-base prefetch is explicitly two-layered: set-based identifier discovery first, then byte-budgeted base loading/application batches using the 256MB active budget, preserving bulk SQL without materializing all base bytes for a large pass.
- DR-003 is now concretely addressed for both places it mattered. H4 blob batches and the streaming LRU both have a solo-oversize rule, bypass LRU admission after use, and fail cleanly through the DR-001 fallback/isolation path if even solo processing exceeds available memory.
- DR-004 is now concretely addressed. The H4 mirror contract requires pass 2 to iterate the entire pass-1 blob-SHA set in multiple byte-budgeted warm batches, clearing between batches; per-object fallback is explicitly exceptional and the acceptance criterion rejects a SELECT-SINGLE-per-blob hot path.
- DR-005 is now concretely addressed. The `zcl_abapgit_ortec_porcelain` dispatch contract is specific enough to implement and reverse: non-Ortec repos stay on standard porcelain, Ortec-active `pull_by_branch`/`pull_by_commit` dispatch once at the top to the mirror, push remains untouched, and complete `it_objects` must be byte-identical/no-op versus standard behavior.
- The prior optional improvements were reasonably incorporated: sparse `rt_objects` contract, implementation metrics for memory/read counts/no SELECT loop, and visible telemetry for streaming-to-fastpath fallback are all now recorded.

## Issues

### DR-RR-001
- Type: maintainability
- Severity: minor
- Evidence: `.memory/state.md`, Topic 2 still contains older architecture-report wording that says routing is size-based at store-backed entry points only and plain-pull is out of v1 scope, before later decision bullets and the correction section supersede that with Ortec opt-in default streaming plus mandatory H4-enabled plain-pull.
- Why it matters: The later text is clear enough for approval, but implementers reading top-down could temporarily follow the stale paragraph and miss that decisions (1), (2), (6), the correction section, and Topic 1 supersede it.
- Fix: Before implementation starts, mark that older architecture paragraph as superseded or rewrite it in place so the section has a single current routing story: Ortec-active repos try streaming by default; no size-threshold switch; plain-pull is in v1 scope only with H4; the old fastpath is fallback.

## Required revisions
- DR-001 through DR-005 are closed. No further automatic review iteration is required.
- Apply the minor editorial cleanup in DR-RR-001 before handing the design to implementation, to avoid stale-scope ambiguity.

## Optional improvements
- In the implementation checklist, keep the H4-alone complete-table no-op test and sparse-table no-SELECT-loop test separate from streaming decoder tests. That preserves the intended two-commit sequencing and makes rollback evidence cleaner.
- Add one acceptance note that all object-store reads used by H4, fallback, and delta resolution exclude `CORRUPT_OR_INCOMPLETE` by API contract, not only by individual caller discipline.
