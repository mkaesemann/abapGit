# Package C C2 — productive branch orchestration and certification — checkpoint

Status: `READY_FOR_SAP_VALIDATION` (self/static validation complete; owner-executed
IT8 import/ABAP Unit/ATC still pending, per standard checkpoint discipline).

Baseline: SAP-validated C1 HEAD `39e7ae083f0700d600657c2806ca38fc3610019b`
(SAP/ATC fix `b3701afbc812e5379b886fdc6a1e3db134578012`, marker commit
`39e7ae083f0700d600657c2806ca38fc3610019b`).

## Scope actually implemented

1. `zcl_abapgit_ortec_porcelain=>pull_by_branch` (productive orchestration
   entry point, only method changed in this class):
   - Resolves the ORTEC repo key (unchanged logic) BEFORE deciding anything.
   - When ORTEC is active and a repo key resolves: resolves the advertised
     remote branch tip via one `zcl_abapgit_git_transport=>branches( iv_url
     )->find_by_name( iv_branch_name )-sha1` call (design §2 — disclosed,
     accepted redundant GET), then classifies via
     `zcl_abapgit_ortec_have_policy=>classify_operation`.
   - `COLD_BRANCH` triggers exactly one `try_backfill_target` attempt; on
     success, re-classifies exactly once.
   - `WARM_UNCHANGED`: no upload-pack POST, no Package B network call, no
     re-certification. Seeds `pull()`/`walk()`/`walk_tree()` with exactly one
     bounded `zcl_abapgit_ortec_obj_store=>get_object` read of the target
     commit; existing `PULL`'s own `zcl_abapgit_ortec_walk_prep=>prewarm`/
     `fetch_blobs_bulk` calls already provide bulk, non-repository-wide
     reconstruction for the reachable tree/blob set (unchanged, reused as-is).
   - `COLD_BRANCH` (still cold after the single backfill attempt): calls
     Package B's `zcl_abapgit_ortec_cold_init=>acquire_blobless_graph` then
     `materialize_tip_snapshot` (each already certifies/commits its own work
     — no duplicate certification performed here), then reconstructs locally
     exactly like `WARM_UNCHANGED`.
   - `INCREMENTAL_UPDATE` (and the ORTEC-inactive/never-seen-repo fallback,
     which defaults to this classification): unchanged thin →
     self-contained → at most one recovery cascade via
     `zcl_abapgit_git_transport=>upload_pack_by_branch` (routes internally to
     `zcl_abapgit_ortec_fastpath=>upload_pack_by_branch`, already carrying
     C1's certified-have migration) — this method's own body, including the
     `"Walk,"`-triggered history-invalidation repair retry, is byte-for-byte
     unchanged from pre-C2.
   - `pull_by_commit`: **unmodified**. It never called `persist_pull_result`
     (no raw-insert defect to fix) and its have-selection already inherits
     C1's migration through the shared `upload_pack_by_commit` path — no
     approved C2 requirement was identified that needs a change here.

2. `zcl_abapgit_ortec_fastpath=>persist_pull_result`:
   - Object-persistence loop (bulk existing-SHA1 filter + single `MODIFY
     ... FROM TABLE`) is unchanged.
   - The pre-Package-C raw, non-certifying `INSERT zaog_commit_hist` is
     **removed** and replaced by a call to a new private method,
     `certify_fetched_commit`, which:
     1. `begin_attempt`
     2. `TRY verify_tree_closure CATCH zcx_abapgit_ortec_git` (only this
        exception; expected local incompleteness → return, no publication)
     3. `mark_graph_complete`
     4. `get_tip_blob_sha1s` → `get_missing_sha1s` (bulk, chunked)
     5. only if nothing is missing: `mark_full_complete` +
        `publish_snapshot_complete`
   - `update_after_fetch` bookkeeping call is unchanged and unconditional
     (runs regardless of certification outcome — bookkeeping only, never a
     completeness signal).
   - Exactly one `COMMIT WORK` remains, at the end of `persist_pull_result`,
     covering object persistence + certification + bookkeeping together.
   - This method is reached **only** by the `INCREMENTAL_UPDATE` routing
     branch — `WARM_UNCHANGED`/`COLD_BRANCH` never call it (already certified
     by their own respective paths).

3. New private method `zcl_abapgit_ortec_fastpath=>certify_fetched_commit`
   (`IMPORTING iv_repo_key, iv_commit, iv_branch_name RAISING
   zcx_abapgit_ortec_git`): extracted specifically so the certification
   lifecycle is directly unit-testable — see "Test coverage decision" below
   for why it could not be tested through the public `persist_pull_result`
   entry point.

## Test coverage decision (read before assuming a gap)

The user's 15-item mandatory test list mixes genuinely new, directly
unit-testable logic (items 8–11) with logic whose testability is structurally
blocked in this codebase:

- Items 1, 2, 5, 6 (partly), 7, 12 depend on live HTTP orchestration
  (`branches()`, `upload_pack_by_branch`, `acquire_blobless_graph`,
  `materialize_tip_snapshot`) for which **no HTTP client injection point
  exists anywhere in this codebase** (confirmed precedent:
  `zcl_abapgit_ortec_cold_init.clas.testclasses.abap`'s own comments state
  the same constraint and its tests rely on "all objects already present so
  HTTP is genuinely unreachable" fixtures instead of real routing tests).
  These are verified by direct source inspection in this handoff (see
  "Scope actually implemented" above) rather than by a literal ABAP Unit
  test — consistent with existing precedent for `upload_pack_by_branch`/
  `pull_by_branch`, which have never had literal HTTP-path ABAP Unit
  coverage either.
- Testing `persist_pull_result` itself (not just its extracted lifecycle)
  is additionally blocked by its `is_active_for_repo( iv_url )` guard, which
  is backed by `zcl_abapgit_persistence_ortec`'s shared, singleton,
  XML-serialized user-settings object with its own uncontrolled `COMMIT WORK
  AND WAIT` — unsafe to flip from a unit test (cross-test/cross-session
  pollution, no clean rollback). This is exactly why `certify_fetched_commit`
  was extracted: it is the smallest pure/testable helper that captures 100%
  of the new certification decision logic without that gate.
- Items 3/4 (backfill reclassification) are already covered by C1's existing
  `backfill_completes_locally` / `backfill_skips_never_seen` /
  `backfill_incomplete_no_publish` tests (unchanged, still green) — C2 did
  not change `try_backfill_target` or `classify_operation` at all.
- Items 13/14/15 are regression checks of unmodified test suites, not new
  tests.

New tests added (`zcl_abapgit_ortec_fastpath.clas.testclasses.abap`, all
method names ≤30 chars, `LOCAL FRIENDS` grants access to the private
`certify_fetched_commit`):

- `certify_closure_incomplete` — commit stored, tree/blob missing →
  `verify_tree_closure` raises → hist_level stays below GRAPH_COMPLETE,
  snap_state stays below COMPLETE (item 8).
- `certify_missing_blob` — commit+tree stored, blob missing → graph
  completes, full/snapshot completeness is NOT published (item 9).
- `certify_full_publishes` — commit+tree+blob all stored → FULL_COMPLETE +
  snapshot COMPLETE are published, and the commit is then provably usable as
  a C1 certified have via `get_certified_haves` (item 10, plus an
  end-to-end C1↔C2 integration proof).
- `certify_idempotent_repeat` — calling `certify_fetched_commit` twice for an
  already FULL_COMPLETE commit does not raise or downgrade.
- `persist_stores_new_objects` / `persist_skips_existing_obj` — regression
  guards for the pre-existing, unchanged object-persistence dedup loop that
  now directly precedes the new certification call in the same method.

`zcl_abapgit_ortec_fastpath.clas.xml` gained `<WITH_UNIT_TESTS>X</WITH_UNIT_TESTS>`
(was previously absent — this class had no test include before C2).

## Invariants preserved (self-verified via source re-read, not re-litigated)

- C1's full-certified-have policy: untouched (`have_policy` not modified).
- HIST_LEVEL='F' required for haves: unchanged (`get_certified_haves` not
  modified).
- No raw FETCH_COMMIT trust in classification: `classify_operation` not
  modified; new porcelain code never reads `ZAOG_REPO_STATE.fetch_commit`
  directly for routing.
- No per-object SQL/HTTP: no new loop was introduced anywhere in this
  change; every new call is a single bounded operation per pull attempt.
- No repository-wide object scan: `WARM_UNCHANGED`/`COLD_BRANCH` seed with
  exactly one `get_object` call; `pull()`'s own bulk prewarm/fetch_blobs_bulk
  is pre-existing, unchanged.
- No progressive deepen introduced.
- No new call to a legacy request builder.
- No per-delta-base remote repair.
- Graph and snapshot certificates remain distinct (`mark_graph_complete` /
  `mark_full_complete` / `publish_snapshot_complete` calls unchanged from
  `zcl_abapgit_ortec_mat_state`, only the call site moved).
- Publication follows verification: `mark_full_complete`/
  `publish_snapshot_complete` are only reached after a successful
  `verify_tree_closure` AND an empty `get_missing_sha1s` result.
- Standard abapGit behavior unchanged when ORTEC is disabled: `lv_op_class`
  defaults to `INCREMENTAL_UPDATE` (today's pre-C2 unconditional path)
  whenever `is_active_for_repo` is false or no repo key resolves; the
  ORTEC-inactive code path through `upload_pack_by_branch`/`pull`/
  `persist_pull_result`'s own inactive-guard is byte-for-byte unchanged.
- Shared `zcl_abapgit_ortec_fastpath`'s private `pull_by_branch` (called
  from inside `upload_pack_by_branch`) and `upload_pack_by_branch` itself:
  **not modified** — `REUSE_NO_CHANGE` honored exactly as design requires.
- Package D1/D2/E ownership boundaries respected — no bulk external
  delta-base work, no final-attempt/transaction-isolation work, no legacy
  removal performed.

## Validation performed

- Local `get_errors`: 0 errors across
  `zcl_abapgit_ortec_porcelain.clas.abap`,
  `zcl_abapgit_ortec_fastpath.clas.abap`,
  `zcl_abapgit_ortec_fastpath.clas.testclasses.abap`.
- Method-name length scan (PowerShell regex across all three touched
  files): 0 names over 30 characters.
- Self-performed mandatory performance gate (senior-agent instructions):
  no new per-object SQL/HTTP, no new unbounded loop, no hidden singleton
  call inside a loop, peak in-memory payload for WARM/COLD seeding is a
  single commit object (not the full graph) — reuses Package B/C1's
  already-audited bulk APIs unchanged.
- Agent roster available this turn only listed
  `ortec-abapgit-implementation-junior` and `ortec-abapgit-regression` —
  the dedicated performance-scan/performance-review/design-review agents
  used for C1 were not available; the senior agent (this agent) performed
  the mandatory performance-gate and correctness self-review directly
  instead, per mode instructions' own gate checklist.

## Validation NOT performed (explicit)

- No SAP/IT8 syntax check, ABAP Unit run, or ATC run (arc-1 MCP connection
  is confirmed pointed at an unrelated system per repo memory — not usable
  for this repo's IT8 validation). Owner must run this on import, exactly as
  for every prior checkpoint.
- No dedicated performance-scan/performance-review subagent pass (agent not
  available this turn) — mitigated by the direct self-review above; no
  blocking finding identified.
- No literal ABAP Unit coverage of live HTTP routing branches (see "Test
  coverage decision").

## Files changed

- `src/ortec/git/zcl_abapgit_ortec_porcelain.clas.abap` (pull_by_branch only)
- `src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap` (persist_pull_result
  rewritten; new private certify_fetched_commit; new private-method
  declaration)
- `src/ortec/git/zcl_abapgit_ortec_fastpath.clas.testclasses.abap` (new file)
- `src/ortec/git/zcl_abapgit_ortec_fastpath.clas.xml`
  (`WITH_UNIT_TESTS` added)
- `.memory/state.md`
- `.memory/handoffs/variant-b-package-c-c2-checkpoint.md` (this file)

## Next action

Owner: import into IT8, run ABAP Unit for
`ZCL_ABAPGIT_ORTEC_FASTPATH`/`ZCL_ABAPGIT_ORTEC_PORCELAIN`/
`ZCL_ABAPGIT_ORTEC_HAVE_POLICY` (regression) plus a live pull-by-branch
smoke test against a real repo covering all three classifications if
practical, then run ATC. Report results back before Package D starts.
