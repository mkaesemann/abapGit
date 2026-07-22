# Performance DESIGN_GATE review — Variant B, Package B (Slices 3+4 combined)

- Mode: `DESIGN_GATE`
- Task: `VB-B-B0-PERF`
- Baseline verified: `cbe8bc73f7c1e17e3d436b1371fcc65f806dd989` (HEAD at review time)
- Design under review: `.memory/logs/variant_b_package_b_design.md` (DRAFT)
- Status of productive code: none — design-only review, no ABAP written or
  modified by this review.

## Evidence read

- `.memory/state.md` (topic `variant-b-partial-clone`, Package A / Slice 2C
  `SAP_VALIDATED_COMPLETE`, Package B objective and binding constraints).
- `.github/skills/abap-performance-patterns/SKILL.md`.
- `.github/skills/git-partial-clone/SKILL.md`.
- Current source, `src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap`:
  `get_objects`, `get_object`, `read_object_rows`, `get_present_sha1s`,
  `get_missing_sha1s`, `get_reachable_objects`, `get_reachable_sha1s`,
  `populate_cache`, `has_dangling_delta_base`, plus class-doc comments and
  `c_select_package_size` (=1000) declaration.
- Current source, `src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.abap`:
  `decode_streaming`, `decode_and_persist_streaming`, `resolve_streaming`,
  `c_batch_size` (=500), all `COMMIT WORK`/`ROLLBACK WORK` sites.
- Current source, `src/ortec/git/zcl_abapgit_ortec_fetch_req.clas.abap`:
  `c_materialize_batch_max` (=100) declaration and its defensive-raise use.
- Grep confirmation: no existing `MiB`/byte-ceiling/"memory risk gate"
  constant anywhere under `src/ortec/git/*.abap` (constant proposed in this
  design would be the first of its kind); `RECOVERY_BRANCH_FULL` exists as a
  real fetch mode with no found byte-budget gate either.

## Verification against the design's explicit checklist (task items 1-6)

1. **Complexity framing (K/F/B, never N)** — mostly correct. `T` (unique
   reachable trees) and `F` (frontier/iteration count) framing for
   `verify_tree_closure`/`get_tip_blob_sha1s`, and `K` (candidate/missing
   blob count) framing for `materialize_tip_snapshot`, are the right
   variables and do not scale with `N`. **However, the stated SQL-call
   bound for the walk is factually wrong — see Finding 2.**
2. **No per-object SQL/HTTP** — confirmed. Walks issue one `get_objects`
   call per frontier iteration (not per node); `materialize_tip_snapshot`
   issues one `MATERIALIZE_BLOBS` HTTP request per ≤100-row batch (not per
   blob). No per-object SQL/HTTP call-count violation found.
3. **Row/byte budgets and oversized-object policy** — the HTTP row budget
   (100, reused `c_materialize_batch_max`) and the 25 MiB response ceiling
   with bounded halve-and-retry (≤7 splits) and hard-fail-on-single-SHA1
   are internally consistent (`ceil(log2(100)) = 7` correctly derives the
   worst-case halving depth from 100 → 1). **Two gaps found — see Findings
   1 and 3.**
4. **No new per-object COMMIT WORK** — confirmed. `decode_streaming` issues
   exactly two commits per call today (promote in
   `decode_and_persist_streaming`, final in `resolve_streaming`; the two
   exception-path commits in `decode_and_persist_streaming` are alternates
   of the success-path commit, not additional ones — only one of the three
   fires per call). `zcl_abapgit_ortec_mat_state`'s methods issue none.
   Package B's own claim of "decode_streaming's existing internal commits
   plus one final orchestrator-level commit per method call" matches the
   source exactly. Commit count scales with number of HTTP batches
   (≈`ceil(K/100)`), never per object.
5. **1 / 1,000 / 40,000 / 1,000,000 scenarios plausible** — plausible for
   the SQL layer: `get_objects` and `get_missing_sha1s` are both keyed by
   `repo_key` + an explicit SHA1/frontier list, never an unqualified scan.
   `get_missing_sha1s` (→ `get_present_sha1s`) is confirmed correctly
   chunked at `c_select_package_size = 1000` and never selects `obj_data`.
   The 1,000,000-stored-object / `K`≈100 scenario is sound. (Caveat: this
   plausibility assumes moderate tree-frontier width — see Finding 2.)
6. **No hidden repository-wide `populate_cache` reuse** — confirmed
   correct. `get_reachable_objects` calls `populate_cache` (full-repo
   preload comment: "Pre-load all objects for this repo into the session
   cache in one SELECT"); `get_reachable_sha1s` does **not**. The design
   explicitly models `verify_tree_closure`/`get_tip_blob_sha1s` on
   `get_reachable_sha1s`'s shape, and nothing in §6/§9 calls
   `populate_cache` or the `get_reachable_objects` path. Correct choice,
   correctly justified.

## Findings

### Finding 1 — BLOCKING-severity: unspecified XSTRING peak memory for `acquire_blobless_graph`'s graph fetch

- **Path/method**: `ZCL_ABAPGIT_ORTEC_COLD_INIT=>acquire_blobless_graph`
  (design §4, §11 table row for `acquire_blobless_graph`).
- **Observed design statement**: the mandatory performance table lists "Max
  simultaneous XSTRING" for this path as "one response XSTRING (existing
  `RECOVERY_BRANCH_FULL`-class memory profile, inherent to any single-pack
  HTTP fetch)" — i.e. no numeric ceiling, no split/chunk strategy, and no
  memory-risk precheck of any kind, unlike the materialize path which gets
  an explicit 25 MiB ceiling with bounded halve-and-retry.
- **Why it matters**: `INITIAL_BRANCH_BLOBLESS` is an unbounded, no-`deepen`
  fetch of the *entire* commit+tree closure reachable from the tip. For a
  repository with a long, large history this is a single, entirely
  un-gated XSTRING held in memory (HTTP response, then decode buffers on
  top of it). `.github/skills/git-partial-clone/SKILL.md` explicitly
  requires: "Full branch recovery must be protected by an explicit
  memory-risk gate while HTTP responses are materialized as one XSTRING."
  `acquire_blobless_graph` has the identical architectural profile (single
  XSTRING, no deepen, unbounded history) and — unlike `RECOVERY_BRANCH_FULL`
  (an exceptional, rarely-invoked recovery tier) — is the **normal** cold
  path for every never-before-seen branch, so it will be exercised far more
  routinely once Package C wires it in. Grep confirms no existing byte-
  ceiling/gate constant exists anywhere in `src/ortec/git/*.abap` today, so
  there is no implicit protection being inherited either.
- **Required fix**: before implementation, the design must state an
  explicit numeric or percentage-of-available-memory ceiling (or an
  explicit, reasoned decision that none is needed, e.g. a framework-level
  HTTP response cap already enforced elsewhere) for the `INITIAL_BRANCH_BLOBLESS`
  response in `acquire_blobless_graph`, matching the rigor already applied
  to the materialize path's 25 MiB budget.
- **Regression test/measurement**: none possible pre-implementation; flag
  for `IMPLEMENTATION_AUDIT` as a mandatory large-scale acceptance scenario
  (large/old-history synthetic repo, cold acquisition, memory profile
  captured).

### Finding 2 — MAJOR: performance table's SQL-chunking claim for the tree walk is factually incorrect

- **Path/method**: `zcl_abapgit_ortec_obj_store=>get_objects` as invoked
  from `verify_tree_closure` (§6) and `get_tip_blob_sha1s` (§9), and the
  "Mandatory performance model" table row for both.
- **Observed design claim**: "`O(F)` bulk chunked SELECTs, `F` = frontier
  iterations (tree depth), each ≤ `ceil(T_frontier/1000)` package reads,"
  and §6 step 3's prose: "one bulk `get_objects` call for the entire
  current frontier (`iv_bulk_fetch = abap_true`, one `SELECT ... FOR ALL
  ENTRIES`-shaped **chunked** read inside `obj_store`... not one `SELECT`
  per tree)."
- **Actual source shape** (`get_objects`, lines ~311-406): the
  `c_select_package_size = 1000` chunking (`IF lines( lt_package ) >=
  c_select_package_size`) exists **only** in the `iv_bulk_fetch = abap_false`
  branch. In the `iv_bulk_fetch = abap_true` branch — the mode both new
  methods use — **every** missing SHA1 in the current frontier is appended
  to one `lt_package` with no size check, then passed to
  `read_object_rows` in a **single, unchunked** call
  (`SELECT * FROM zaog_obj_store ... WHERE obj_sha1 IN lr_sha1s`,
  including the `obj_data` payload column). `get_missing_sha1s`'s own
  chunking (via `get_present_sha1s`) is correctly at `c_select_package_size`
  and is not affected by this finding — only the `get_objects` bulk-fetch
  path used by the new tree/commit walk is affected.
- **Production cardinality**: tree-frontier width is normally bounded by
  directory fan-out, but this is an assumption, not an enforced budget —
  a repository with a very wide flat directory (common in some generated
  or vendored trees) could produce a frontier with unbounded row count in
  a single un-batched `SELECT`, each row potentially bearing a nontrivial
  `obj_data` payload (a tree object's serialized entries).
- **Mitigating context**: this exact `get_objects(..., iv_bulk_fetch =
  abap_true)` shape is **inherited unchanged** from the already-productive,
  SAP-validated `get_reachable_objects`/`get_reachable_sha1s` (both call it
  identically for their own tree-frontier walk) — this is not a new risk
  introduced by Package B, and the new methods are explicitly, correctly
  modeled on that precedent. The defect is in the **design document's own
  performance table**, which asserts a chunking bound that does not exist
  in the code it is modeled on.
- **Required fix**: correct the performance table to state the true shape
  (one unchunked, width-unbounded `SELECT` per frontier iteration, `obj_data`
  included), and either (a) add an explicit justification that production
  tree-frontier width remains bounded in practice (consistent with the
  already-accepted `get_reachable_sha1s` risk), or (b) add explicit
  frontier-side row/byte chunking before calling `get_objects` in the new
  methods — the design's own required-input list ("row batch limit", "byte
  batch limit") is populated for the HTTP materialize path only, not for
  this SQL path.
- **Regression test/measurement**: a synthetic wide-frontier fixture
  (single directory with several thousand entries) exercised against
  `verify_tree_closure`/`get_tip_blob_sha1s` during `IMPLEMENTATION_AUDIT`.

### Finding 3 — MAJOR: oversized-batch split budget is call-scoped, not batch-scoped

- **Path/method**: `materialize_tip_snapshot` (design §10, §13).
- **Observed design statement**: "bounded to at most `ceil(log2(100))` = 7
  splits total across the **whole `materialize_tip_snapshot` call** —
  tracked via a call-scoped counter, **not reset per batch**."
- **Why it matters**: `materialize_tip_snapshot` can process many ≤100-row
  batches in one call (`K` missing blobs chunked at 100). If an early batch
  triggers oversized-response splitting and consumes some or all of the
  shared 7-split budget, a **later, independent** batch that is also
  oversized (plausible for repositories with several large binary assets
  spread across different parts of the tree) will hard-fail immediately
  with few or zero splits remaining — even though, in isolation, it would
  resolve within its own fresh 7-split budget. This is a self-inflicted
  availability defect, not a resource-bound issue: it converts a resolvable
  oversized-object condition into a spurious hard failure purely because of
  budget-sharing across unrelated batches.
- **Required fix**: scope the split counter per batch (reset to 0 at the
  start of each batch's send/response cycle), not per call. A per-call cap
  on *total* splitting work (if still desired for defense-in-depth) should
  be a separate, explicitly documented secondary bound, not the only bound.
- **Regression test/measurement**: a synthetic fixture with two
  independently-oversized batches in the same `materialize_tip_snapshot`
  call, asserting both resolve via splitting rather than the second
  hard-failing.

## Non-blocking observations (no action required before APPROVE, track for audit)

- The proposed `c_max_batch_response_bytes` = 25 MiB constant is the first
  byte-ceiling constant in this code area (confirmed via grep — no
  precedent exists). Reasonable as stated; recommend validating against a
  real large-blob batch during `IMPLEMENTATION_AUDIT` rather than treating
  25 MiB as pre-validated.
- `RECOVERY_BRANCH_FULL` (existing, unrelated to Package B) also appears to
  have no explicit memory-risk gate today, despite the git-partial-clone
  skill's requirement. Out of Package B's authoring scope (not modified
  here) — flagged for awareness only, not a Package B blocker.
- `decode_streaming`'s internal resolve pass ("Pass 2... now allowing the
  object-store fetch") is pre-existing, SAP-validated Slice 2C code reused
  unchanged by Package B; its own SQL shape was out of this review's scope
  per the task's explicit boundaries and was not re-audited here.

## Verdict

**REVISE_AND_REVIEW_ONCE**

Rationale: the design is otherwise thorough, correctly avoids per-object
SQL/HTTP, correctly avoids `populate_cache` repository-wide preloading,
correctly reuses existing row-budget/commit-boundary constants, and its
`K`/`F`/`N`-independence argument is sound overall. However, one finding
(Finding 1) is a precise, literal match to a named DESIGN_GATE block
condition ("unspecified XSTRING peak memory") for a path that will become
a routine (not exceptional) cold-start hot path once wired in by Package C,
and Finding 2 shows the design's own mandatory performance table makes an
incorrect claim about existing chunking behavior. Both, plus Finding 3, are
narrow, mechanical fixes (add an explicit bound/gate, correct a table,
rescope a counter) that do not require redesigning the core walk or
materialization algorithm. One revision pass addressing all three findings
should be sufficient for approval.

## Second pass (VB-B-B0-PERF-REV2)

- Mode: `DESIGN_GATE`, second pass. Baseline: `cbe8bc73f7c1e17e3d436b1371fcc65f806dd989`
  (unchanged from first pass). Scope: verify only Findings 1-3 above against
  the revised `.memory/logs/variant_b_package_b_design.md`; no re-review of
  unrelated sections.

### Finding 1 (BLOCKING) — VERIFIED RESOLVED

Design § 10 now defines `zcl_abapgit_ortec_cold_init=>c_max_graph_response_bytes`
(209,715,200 = 200 MiB) and states `acquire_blobless_graph` checks
`xstrlen( response ) > c_max_graph_response_bytes` **immediately after
`send_receive_close` returns and before `decode_streaming` is called**,
raising `zcx_abapgit_ortec_git` on breach (INV-B-12). This is a real,
checkable-before-decode gate — it inspects the already-materialized
response XSTRING's length, not an estimate, and fails fast before any
decode work. The performance table's `acquire_blobless_graph` row correctly
states the gate value and timing ("checked before `decode_streaming` is
invoked"). Resolved.

### Finding 2 (MAJOR) — PARTIALLY RESOLVED, new contradiction introduced (see Finding 2b below)

Design § 6 step 3 and § 9's walk-algorithm paragraph both now correctly
specify `iv_bulk_fetch = abap_false` for `verify_tree_closure`/
`get_tip_blob_sha1s`'s frontier `get_objects` calls (INV-B-13), and the
performance table rows for both methods correctly state chunked reads at
`c_select_package_size`. Re-confirmed against
[zcl_abapgit_ortec_obj_store.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap#L348-L376):
the `iv_bulk_fetch = abap_true` branch (lines 348-357) appends the entire
`lt_missing_sha1s` list to `lt_package` in one loop with no size check, then
issues exactly one `read_object_rows` call — genuinely unchunked. The
`iv_bulk_fetch = abap_false` branch (lines 359-376) checks
`lines( lt_package ) >= c_select_package_size` inside the loop and flushes
in bounded chunks — genuinely chunked. The design's corrected claim
(`abap_false` chunks, `abap_true` does not) is factually accurate.

### Finding 2b (MAJOR, new) — § 9's "Bulk database lookup shapes" bullet list still states the pre-fix claim

Design § 9's "Bulk database lookup shapes" bullet list (the "Tree/commit
frontier reads" bullet) was **not** updated by the same edit and still
reads: *"Tree/commit frontier reads: `get_objects` with `iv_bulk_fetch =
abap_true` — already a chunked, package-size-bounded ... read"* — this
directly contradicts the correction made two paragraphs earlier in the same
section (and in § 6 and the performance table), which correctly states
`abap_true` is the *unchunked* branch and `abap_false` must be used. An
implementer skimming § 9's bullet list in isolation (its own heading claims
"all reused, no new shapes invented", inviting exactly that kind of
skim-and-copy use) could reasonably copy `iv_bulk_fetch = abap_true` for
the frontier read and silently reintroduce the unchunked
`SELECT ... FOR ALL ENTRIES` Finding 2 was raised to eliminate. This is a
genuine new self-contradiction introduced by the partial edit, not a
restatement of Finding 2 — Finding 2's target claims (§ 6, § 9's algorithm
paragraph, the performance table) are all correctly fixed; only this one
leftover bullet is stale.

**Required fix:** in § 9's "Bulk database lookup shapes" list, change the
"Tree/commit frontier reads" bullet to `iv_bulk_fetch = abap_false` and
correct "already a chunked" wording to match § 6 (chunked only via the
`abap_false` branch, `c_select_package_size`-bounded). One-line/one-bullet
fix; does not require touching any other section.

### Finding 3 (MAJOR) — VERIFIED RESOLVED

Design § 10 and § 13 now both state the oversized-batch split counter
(bounded to 7 splits, `ceil(log2(100))`) is "local to EACH top-level
≤100-row batch (reset to 0 when that batch's send/response cycle begins)"
and tracked via "a loop-local variable (not `CLASS-DATA`)" (INV-B-07b),
explicitly distinguished from `pack_stream`'s `gv_completion_attempts`
cross-call pattern. The performance table's `materialize_tip_snapshot` row
correctly reflects "≤7 splits worst case, per-batch budget — INV-B-07b".
Resolved — no residual call-scoped sharing found.

### Additional sanity check — invariant IDs and constant names

Grepped the full revised design for `INV-B-07`/`INV-B-07b`/`INV-B-12`/
`INV-B-13` and for `c_max_graph_response_bytes`/`c_max_batch_response_bytes`:
no duplicate/conflicting invariant numbering, no naming collision between
the two distinct byte-ceiling constants (200 MiB graph vs. 25 MiB batch),
all cross-references resolve to the correct section. No other new
contradictions found outside Finding 2b.

### Second-pass verdict

**APPROVE_WITH_MINOR_REVISIONS**

Findings 1 and 3 are fully resolved with no residual issues. Finding 2's
core claim is fixed everywhere it matters for correctness (algorithm
description and performance table); the newly found Finding 2b is a
one-bullet documentation contradiction left over from that same edit, not
an unresolved architectural or budget defect — it does not require a third
`DESIGN_GATE` pass. Fix the single stale bullet in § 9 before or during
`IMPLEMENTATION_AUDIT`; no other action required.
