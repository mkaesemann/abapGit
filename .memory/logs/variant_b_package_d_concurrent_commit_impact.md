# Variant B Package D — Concurrent Commit Impact (D0 reconciliation)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=D0-CONCURRENT-COMMIT-IMPACT
BASELINE_OLD=29199f629773c676e0eaa2f3a006f5167d304ae8
BASELINE_CANDIDATE=5e5403546554dbe4f0f8e7a1eb2f07ab434dbe95
ALLOWED_CONTEXT=.memory/handoffs/variant-b-package-d-bootstrap.md,.memory/logs/variant_b_package_d_delta_discovery.md,.memory/logs/variant_b_package_d_transaction_discovery.md,.memory/logs/regression_variant_b_package_d_baseline.md,.memory/logs/variant_b_package_d_design.md,.memory/reviews/variant_b_package_d_correctness_review.md,.memory/reviews/variant_b_package_d_correctness_decision.md,.memory/reviews/variant_b_package_d_protocol_review.md,.memory/reviews/variant_b_package_d_protocol_decision.md
SOURCE_SCOPE=files/symbols touched by the 4 named commits (see matrix)
OUTPUT_ARTIFACTS=.memory/logs/variant_b_package_d_concurrent_commit_impact.md
FORBIDDEN_PATHS=.memory/archive/**,.memory/state.md,.memory/diagrams/**,editor-memory/**
STATE_WRITE_ALLOWED=no
DIAGRAM_WRITE_ALLOWED=no
FORBIDDEN_CHANGES=productive ABAP,.github/**,unrelated memory,existing commits
MAX_PARENT_RETURN=12 lines
```

## Ancestry verification (actual, not assumed)

`git merge-base --is-ancestor <sha> HEAD` returned true (exit 0) for all four
commits. `HEAD` at reconciliation time is
`5e5403546554dbe4f0f8e7a1eb2f07ab434dbe95` — i.e. the fourth named commit
*is* the current branch tip; there is no further undocumented commit beyond
it. True chronological order
(`git log --ancestry-path 29199f629773c676e0eaa2f3a006f5167d304ae8..HEAD`),
oldest → newest:

1. `ad51cb49` Repository Cold Init Optimization to Reduce git Roundtrips
2. `df029c5c` Optimize remaining blob set updates
3. `35be4c65` Bulk preload streaming delta data
4. `5e540354` Reuse verified tip blob set during cold init (= HEAD)

This matches the order the owner listed. Two additional commits exist
between the old D0 baseline and the first named commit
(`53796552` "Close-Out Package C", tag `partial-clone-solid-baseline`, and
`1500755e` "Agent hardening & memory cleanup") — these are pre-existing
baseline/tooling commits, not part of this reconciliation's scope (they
predate all four named commits and only touch `.github/**`/`.memory/**`
housekeeping, confirmed via the full-range `git diff --name-status`, which
shows the same 8 productive `src/` files as the four named commits combined
— no extra productive source changed outside their scope).

## Commit-by-commit matrix

### 1. `ad51cb49` — Repository Cold Init Optimization to Reduce git Roundtrips

- Files: `src/http/zcl_abapgit_http_client.clas.abap` (+22, new
  `send_receive_data` method — generic HTTP client reuse-connection helper),
  `src/ortec/git/zcl_abapgit_ortec_cold_init.clas.abap` (+649/-…, cold-init
  rewrite), `..._cold_init.clas.testclasses.abap` (+161), `..._fetch_req
  .clas.abap` (`c_materialize_batch_max` 100→1000, comment formatting
  artifact only), `..._obj_store.clas.abap` (+136: new
  `verify_ready_blobs` method, additive; class-implementation header
  case-fix `ZCL_...`→`zcl_...`, cosmetic).
- Changed methods/symbols: `zcl_abapgit_http_client=>send_receive_data`
  (new), `zcl_abapgit_ortec_cold_init=>*` (Package B cold-init logic, not a
  D1/D2 symbol), `zcl_abapgit_ortec_obj_store=>verify_ready_blobs` (new,
  self-contained `SELECT ... WHERE status = 'R'` chunked in
  `c_select_package_size`, does not call or alter `get_object`/
  `get_objects`/`read_object_rows`).
- SQL/transaction/status shape: `verify_ready_blobs` is additive-only; it
  reuses the existing `status = 'R'` convention read-only, never writes.
  No change to any D1/D2-relied-upon read/write path.
- Classification: Cold-init routing is explicitly out of D1/D2 scope per
  the design's §2 ownership table ("Cold/warm/incremental routing... are
  not reopened; they are inputs D1/D2 must preserve"). `verify_ready_blobs`
  is a new, independent, additive method not called by any D1/D2 code path.
- **Verdict: `NO_D0_IMPACT`.**

### 2. `df029c5c` — Optimize remaining blob set updates

- Files: `src/ortec/git/zcl_abapgit_ortec_walk_prep.clas.abap` (+39/-9,
  replaces a per-loaded-object `DELETE ct_remaining_sha1s WHERE table_line =
  ...` full-table scan with an O(1)-hashed-lookup rebuild),
  `..._walk_prep.clas.testclasses.abap` (+39, new tests).
- Changed methods/symbols: one unnamed-in-diff private method inside
  `zcl_abapgit_ortec_walk_prep` (blob-set candidate loading — Package
  B/materialize territory, not a D1/D2 symbol).
- Classification: `zcl_abapgit_ortec_walk_prep` does not appear in any D1/D2
  discovery, design, or review artifact; it is part of the tree-walk/
  materialize call chain, unrelated to delta resolution (D1) or attempt/
  transaction persistence (D2).
- **Verdict: `NO_D0_IMPACT`.**

### 3. `35be4c65` — Bulk preload streaming delta data

- Files: `src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap` (+93: new
  `get_available_objects` method — lenient bulk read, ignores missing
  SHA1s, warms the existing session-level `mt_cache`),
  `..._obj_store.clas.testclasses.abap` (+61),
  `src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.abap` (+163/-42: new
  `preload_delta_rows` and `preload_external_bases` methods;
  `resolve_streaming` now calls both — `preload_delta_rows` once before
  the Phase-1 fixpoint DO-loop, `preload_external_bases` once after Phase 1
  exhausts in-pack progress and before Phase 2; `resolve_one_meta`'s
  external-base exception handler is simplified — the old
  `complete_missing_base`-retry branch is removed and replaced with a
  direct raise, and `get_base_bytes` is now called before
  `get_object` in the TRY block),
  `..._pack_stream.clas.testclasses.abap` (+76).
- Changed methods/symbols directly overlapping D1/D2 design sections:
  - `preload_delta_rows` (new): calls
    `zcl_abapgit_ortec_obj_store=>get_objects(iv_repo_key, it_sha1s =
    <this pack's unresolved delta temp_keys>, iv_bulk_fetch = abap_false)`
    once, **before** `resolve_one_meta` ever runs, to warm the session
    cache and eliminate the O(K) per-delta `get_object(temp_key)` round
    trips inside `resolve_one_meta` (§1.1's "one real per-object-SQL gap").
    This is the exact class of optimization design §18's PERF-M-1 finding
    said was missing from the cost model.
  - `preload_external_bases` (new): bulk-loads locally available REF_DELTA
    bases via the new lenient `get_available_objects` and warms
    `zcl_abapgit_ortec_base_cache` (process-global) directly — functionally
    parallel to, but **not the same mechanism as**, D1's designed §4
    "Phase 1.5" (which specifies a *strict* `get_objects(iv_bulk_fetch =
    abap_true)` call plus an explicit merge into `ct_meta`/`ct_sha_idx` so
    Phase 2 degrades to a pure in-pack pass). The production code instead
    keeps Phase 2 calling `get_base_bytes`/`get_object` per still-external
    base, now typically cache-warmed hits rather than DB round-trips.
  - `get_objects`'s unconditional strict semantics (raises on any missing
    requested SHA1, confirmed previously against source in this session)
    means `preload_delta_rows` — like the pre-existing `resolve_one_meta`
    — depends on this pack's own delta temp-key rows already being
    `status = 'R'` by the time `resolve_streaming` runs.
- Direct interaction with open Package D findings:
  - **PERF-B-1 (still open, blocking):** the design's proposed §5.3
    `status = 'D'` split for delta temp-key rows would break not only
    `resolve_one_meta`'s existing `get_object(temp_key)` call (as already
    found) but now **also** this new `preload_delta_rows`'s bulk
    `get_objects(temp_keys)` call, for the identical reason (both go
    through `read_object_rows`'s hard-coded `WHERE ... status = 'R'`
    filter). The still-pending PERF-B-1 fix (a dedicated status-`'D'`/`'R'`-
    aware read path) must be scoped to cover **both** call sites, not just
    `resolve_one_meta`.
  - **PERF-M-1 (still open, major):** production code has independently
    added a bulk-preload for the exact O(K) temp-key-read gap PERF-M-1
    flagged. Design §18's cost-table correction (still required) must now
    describe **one bulk `get_objects` call per pack** (via
    `preload_delta_rows`) instead of either "0 calls" (the current wrong
    claim) or "O(K) individual calls" (the pre-commit reality) — the
    production fix already matches the intended target shape for this one
    finding, but the design document text is not yet updated to say so.
  - `complete_missing_base`'s retry branch removal from `resolve_one_meta`
    is **consistent** with §5.2 ("`complete_missing_base` stays a
    documented permanent no-op") — no conflict; the design already treated
    it as dead code.
  - `preload_external_bases`'s cache-warming approach does not corrupt or
    contradict the design's DR-003 auxiliary-index-sync requirement (that
    requirement applies specifically to a Phase-1.5-style *merge into
    ct_meta/ct_sha_idx*, which production does not do — production instead
    leaves Phase 2's existing per-base `get_base_bytes`/`get_object` calls
    in place, now cache-warmed). No correctness regression; §4's Phase 1.5
    design, if implemented as literally written, would still be valid
    *in addition to* the existing preload methods, but the current-source
    narrative in §1.1/§4 needs a factual update so a future implementer
    does not assume no bulk-preload exists today.
- Classification: correctness/protocol/persistence semantics (identity,
  transaction boundaries, locks, commits, attempt_id) are **unchanged** by
  this commit — only D1's delta-resolution call chain and the still-open
  performance fix scope are affected.
- **Verdict: `D0_DESIGN_CHANGE_REQUIRED`** (design-document text only,
  §1.1/§4/§18/§5.3; no correctness or protocol/persistence review reopen
  required — those reviews' conclusions do not depend on this call chain).

### 4. `5e540354` — Reuse verified tip blob set during cold init (HEAD)

- Files: `src/ortec/git/zcl_abapgit_ortec_cold_init.clas.abap` (+28,
  `..._cold_init.clas.testclasses.abap` (+50),
  `src/ortec/git/zcl_abapgit_ortec_porcelain.clas.abap` (+21/-16: adds an
  `et_tip_blob_sha1s`/`it_tip_blob_sha1s` parameter threaded between
  `zcl_abapgit_ortec_cold_init=>acquire_blobless_graph` and
  `=>materialize_tip_snapshot`).
- Location check (critical for D2/§9's Owner-Decision-A relocation of
  Publication Unit #2): the changed `pull_by_branch` hunk is at
  pre-commit lines 173–270, entirely inside the **`COLD_INIT`** branch's
  "cold reconstruction... locally exactly like WARM_UNCHANGED" comment
  block — confirmed by direct diff context, **not** the
  `INCREMENTAL_UPDATE` branch (~line 370) where Unit #2 was just relocated
  this session. The two branches remain textually and behaviorally
  disjoint after this commit.
- Changed methods/symbols: `zcl_abapgit_ortec_cold_init=>
  acquire_blobless_graph`/`materialize_tip_snapshot` signatures gain a new
  blob-SHA1-list parameter (Package B/cold-init territory, not called by
  any D1/D2-designed code path).
- Classification: cold/warm/incremental routing is explicitly out of D1/D2
  scope (design §2 ownership table). No D2 attempt_id-threading target
  method (`begin_attempt`, `persist_pull_result`, `certify_fetched_commit`,
  `decode_and_persist_streaming`, `resumable_decode`) is touched. No
  overlap with the just-applied Owner-Decision-A Unit #2 relocation.
- **Verdict: `NO_D0_IMPACT`.**

## Reconciliation questions answered (only for the impacted commit, `35be4c65`)

1. D1 call-chain accuracy: §1.1/§4's current-source narrative is now
   incomplete — production already added `preload_delta_rows`/
   `preload_external_bases` calls inside `resolve_streaming`. Needs a
   documentation update (Case B/C boundary; treated as C because it also
   widens an open blocking finding's fix scope).
2. D1 target design validity: §4's Phase 1.5 design (strict
   `get_objects(iv_bulk_fetch=abap_true)` + `ct_sha_idx` merge) remains
   valid as a target and is not contradicted or pre-empted by production —
   production solves the same class of problem via cache-warming instead,
   which is compatible, not conflicting.
3. D2 writer/reader/commit-rollback map accuracy: unaffected — no writer/
   reader/commit-boundary code touched by this commit.
4. attempt_id ownership/propagation: unaffected — not touched.
5. Lock ownership/duration: unaffected — not touched.
6. STAGED/READY (`status`) visibility: **affected** — `preload_delta_rows`
   adds a second call site with a hard dependency on temp-key rows already
   being `status = 'R'`, widening PERF-B-1's required fix scope.
7. SQL/HTTP multiplicity and row/byte bounds: improved by production
   (O(K)→O(1) per pack for temp-key reads); design §18 cost table must be
   corrected to reflect actual current-source behavior, not the pre-commit
   O(K) behavior PERF-M-1 flagged.
8. Simultaneous XSTRING/payload-copy assumptions: unaffected.
9. Exact file/symbol scope for the still-open PERF-B-1 fix: widened from
   `resolve_one_meta` alone to `resolve_one_meta` **and**
   `preload_delta_rows`, both in
   `src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.abap`.
10. Test plan: the commit added `preload_delta_rows`/
    `preload_external_bases` unit tests of its own (in
    `..._pack_stream.clas.testclasses.abap`, +76 lines) — these are
    production tests for the *current* (status-'R'-only) behavior; the
    still-pending PERF-B-1 fix's new regression test
    (`resolve_reads_own_d_row`, per the performance review's required-fix
    list) must be written so it does not regress these existing tests.
11. Correctness/protocol owner-decision validity: unaffected — both
    decisions (DR-004 Option A, B-3 Option A/7-point) concern identity/
    publication-unit-location semantics this commit does not touch.
12. Deferred-cross-package-perf classification: none of the four commits
    require deferral; all are already merged and in scope for this
    reconciliation.

## Summary verdicts

| Commit | Verdict |
| --- | --- |
| `ad51cb49` | `NO_D0_IMPACT` |
| `df029c5c` | `NO_D0_IMPACT` |
| `35be4c65` | `D0_DESIGN_CHANGE_REQUIRED` |
| `5e540354` | `NO_D0_IMPACT` |

No commit reaches `D0_REVIEW_REOPEN_REQUIRED` or `BLOCKING_BASELINE_CONFLICT`.
`PACKAGE_C_VALIDATED_BASELINE = 29199f629773c676e0eaa2f3a006f5167d304ae8`
(SAP-validated) remains distinct from
`PACKAGE_D_SOURCE_BASELINE = 5e5403546554dbe4f0f8e7a1eb2f07ab434dbe95`
(verified current HEAD, **not** SAP-validated — only git-ancestry and diff-
content verified in this reconciliation). None of the four commits touch
any Package C productive path exercised by
`regression_variant_b_package_d_baseline.md`'s test list (that baseline's
files are `zcl_abapgit_ortec_mat_state`, `zcl_abapgit_ortec_fastpath`,
`zcl_abapgit_ortec_delta`, `zcl_abapgit_ortec_git_tests` — none touched by
any of the four commits) — Package C regression re-run is not required by
this reconciliation, but remains required after D1/D2 implementation lands
in `zcl_abapgit_ortec_pack_stream`/`zcl_abapgit_ortec_obj_store` per the
existing regression plan.
