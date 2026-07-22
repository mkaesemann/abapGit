# Variant B — Package B design (Slices 3+4 combined)

Status: DRAFT, awaiting protocol/persistence review and performance
`DESIGN_GATE`. No productive ABAP changed by this document.
Baseline: `cbe8bc73f7c1e17e3d436b1371fcc65f806dd989` (Package A / Slice 2C,
SAP-validated). Date: 2026-07-22.
Scope: Slice 3 (cold blobless graph acquisition) + Slice 4 (bounded tip
snapshot materialization) only. No branch-switch decision flow (Package C),
no delta-base bulk repair (Package D1), no final attempt/transaction
architecture (Package D2), no legacy removal (Package E).

Evidence basis: `.memory/state.md`, `.github/prompts/variant-b.prompt.md`,
`.memory/logs/variant_b_slice2_design.md`, current source of
`zcl_abapgit_ortec_fetch_req`, `zcl_abapgit_ortec_fastpath`,
`zcl_abapgit_ortec_pack_stream`, `zcl_abapgit_ortec_obj_store`,
`zcl_abapgit_ortec_mat_state`, `zcl_abapgit_ortec_repo_state`,
`zcl_abapgit_ortec_fetch_neg`, `zcl_abapgit_ortec_filter_walk`,
`zcx_abapgit_ortec_git`.

---

## 1. Entry points and productive caller

**Decision: two new public methods on one new class, no productive caller
migrated in Package B — matches the Slice 2 precedent (`build_request`/
`parse_capabilities` also shipped with zero productive callers, migration
deferred to a later slice).**

New class `ZCL_ABAPGIT_ORTEC_COLD_INIT` (26 chars), `PUBLIC FINAL CREATE
PUBLIC`, owning:

- `acquire_blobless_graph` — B1 (Slice 3) entry point.
- `materialize_tip_snapshot` — B2+B3 (Slice 4) entry point.

Justification for a new class rather than extending `zcl_abapgit_ortec_fastpath`:
`fastpath`'s own class doc scopes it to "Entry point called from standard
abapGit hooks" for the existing incremental/recovery tiers; cold-branch
orchestration is a structurally distinct decision (no prior state to
validate against) that Slice 5 (Package C) will call *instead of*
`pull_by_branch`'s early-return-to-standard-path, not *from inside* it.
Keeping it in its own class means Package C's Slice 5 design can wire it in
as a single call without touching `fastpath`'s existing retry-cascade
methods at all.

Both methods are directly unit-testable today (no live HTTP needed for the
request-shape/persistence/certificate assertions — a fake pack byte string
plays the same role `ltcl_fastpath_protocol` already uses); wiring into
`pull_by_branch`/`zcl_abapgit_ortec_filter_walk=>get_remote_files_for_stage`
is explicitly deferred to Package C per the owner spec's own Slice 5 scope
and the current prompt's "Package B must not implement ... branch-switch
orchestration" exclusion.

## 2. Capability requirements and unsupported-capability behavior

Reused as-is from Slice 2 (`zcl_abapgit_ortec_fetch_req`, already correct
and SAP-validated — verified by direct read of `build_request`, not the
design doc summary):

- `INITIAL_BRANCH_BLOBLESS` hard-requires `filter`; absent →
  `zcx_abapgit_ortec_git` raised by `fetch_req` itself with
  `mv_unsupported_capability = abap_true`, `mv_missing_capability =
  'filter'`. `acquire_blobless_graph` does not catch this — it propagates
  to the caller untouched (no fallback to an unfiltered fetch inside
  Package B; AC8 "Filter absent: controlled fallback, no pretend partial
  success" is satisfied by propagation, not by Package B inventing its own
  fallback policy, which belongs to Package C's decision flow).
- `MATERIALIZE_BLOBS` hard-requires `allow-reachable-sha1-in-want` (or the
  `allow-tip-sha1-in-want` fallback token); absent → same typed raise with
  `mv_missing_capability = 'allow-reachable-sha1-in-want'`. Per the owner
  spec's Slice 4 "if arbitrary reachable SHA wants are not supported ...
  use memory-gated branch recovery or standard fallback with a structured
  reason" — Package B does **not** implement that fallback policy either
  (it belongs to Package C); `materialize_tip_snapshot` propagates the same
  way. **INV-B-01**: no method added in Package B ever falls through to an
  unfiltered/unbounded request on a capability failure.

## 3. Cold-branch classification

Package B does not decide *when* a branch is cold — that is Slice 5's own
decision flow (Package C). `acquire_blobless_graph` is unconditional: given
an already-resolved `iv_repo_key`/`iv_branch_name`/`iv_tip_commit`, it always
attempts a fresh `INITIAL_BRANCH_BLOBLESS` fetch and certifies the result.
Callers (Package C, out of scope here) are responsible for only invoking it
when `zcl_abapgit_ortec_mat_state=>is_graph_have_eligible` is currently
`abap_false` for the tip. This mirrors Slice 2's own boundary: the
serializer doesn't decide policy, callers do.

## 4. Attempt boundary used by the current architecture

**Decision: reuse the existing two-part commit boundary exactly as already
implemented — no new transaction architecture (Package D2 is explicitly
deferred).**

1. `zcl_abapgit_ortec_pack_stream=>decode_streaming` already performs its
   own internal `COMMIT WORK` (once after promoting `'I'`→`'R'` rows in
   `decode_and_persist_streaming`, once after `resolve_streaming`) — this is
   pre-existing, SAP-validated Slice 2C behavior, not something Package B
   introduces or may change. By the time `acquire_blobless_graph`/
   `materialize_tip_snapshot` regain control, every object `decode_streaming`
   returned is already a durably committed, content-addressed, READY
   (`status = 'R'`) row. This does **not** violate "failed attempts publish
   no ready objects" — a bare READY object row is not a certificate; only
   `zaog_commit_hist`/`zaog_repo_state` rows are certificates, and those are
   never touched by `decode_streaming`.
2. `zcl_abapgit_ortec_mat_state` (`begin_attempt`, `mark_graph_complete`,
   `publish_snapshot_complete`) issues **no** `COMMIT WORK` of its own
   (existing class-level invariant, unchanged). Package B's two orchestration
   methods each issue exactly **one** final `COMMIT WORK` themselves, placed
   immediately after the last successful `mark_graph_complete` /
   `publish_snapshot_complete` call and nowhere else — i.e. the certificate
   write is the only thing riding on that final commit; the object rows it
   references are already durably committed beforehand by (1).
3. If verification fails after `decode_streaming` succeeded (e.g. tree
   closure incomplete), `acquire_blobless_graph` does **not** call
   `mark_graph_complete`, does **not** issue `COMMIT WORK`, and re-raises —
   the already-committed object rows remain (harmless, content-addressed,
   reusable by a future retry) but no certificate exists, satisfying
   **INV-B-02**: no graph/snapshot certificate is ever published without a
   preceding successful verification in the same call.
4. `begin_attempt`'s `attempt_id` (32-char) is generated once per
   orchestration call by `begin_attempt` itself
   (`cl_system_uuid=>create_uuid_c32_static`, confirmed by direct read of
   `zcl_abapgit_ortec_mat_state=>begin_attempt` — Package B does not invent
   a new ID scheme, generation, or helper) and threaded through
   `mark_graph_complete`/`publish_snapshot_complete` in that same call, so a
   stale/superseded concurrent attempt is rejected by the existing
   stale-attempt-ID guard. No new session/pack/attempt identity model is
   introduced (Package D2 boundary respected).

## 5. Commit/tree persistence and READY semantics

Unchanged from existing `zaog_obj_store` semantics: `decode_streaming`
persists commit/tree/blob objects under their real SHA1 with `status = 'R'`
once fully resolved. Package B adds no new status value and no new table.
`INITIAL_BRANCH_BLOBLESS`'s response is expected to contain zero blob
objects (server-side `filter blob:none`) and a complete commit+tree closure
for full history reachable from the tip (unbounded — no `deepen`); any blob
objects unexpectedly present in the response are persisted like any other
decoded object (harmless — a already-READY blob only helps future
have/materialization work) but are never required.

## 6. Graph-closure algorithm and termination condition

**Decision: one new method, `zcl_abapgit_ortec_obj_store=>verify_tree_closure`,
distinct from the existing `get_reachable_objects`/`get_reachable_sha1s` —
neither of the latter two is reused as-is because both hard-require blob
presence (`get_reachable_sha1s` raises via `get_present_sha1s` if a blob is
missing), which is wrong for a blobless fetch where historical blobs are
*promised*, not corrupt (owner invariant).**

```abap
CLASS-METHODS verify_tree_closure
  IMPORTING iv_repo_key TYPE ty_repo_key
            iv_commit   TYPE zif_abapgit_git_definitions=>ty_sha1
  RAISING   zcx_abapgit_ortec_git.
```

Algorithm (bulk BFS, same shape as `get_reachable_sha1s` lines 517-622,
reused pattern not reused code — blob branch removed entirely):

1. `get_objects( it_sha1s = [iv_commit], iv_bulk_fetch = abap_false )`; raise
   if not found or not type `commit`; decode via
   `zcl_abapgit_git_pack=>decode_commit`; raise if undecodable or no tree.
2. Iterative frontier: `lt_current_trees` seeded with the commit's tree SHA1;
   `lt_seen_trees` (`HASHED TABLE ... WITH UNIQUE KEY table_line`) is the
   visited-tree dedup set (**INV-B-03**: a tree referenced by multiple
   parents/subtrees is fetched and decoded exactly once).
3. Each iteration: `get_objects` call for the entire current frontier with
   **`iv_bulk_fetch = abap_false`, not `abap_true`** (design-review
   correction, `.memory/reviews/performance_design_variant_b_package_b.md`
   Finding 2: `get_objects`'s `iv_bulk_fetch = abap_true` branch appends the
   *entire* missing-SHA1 list to one unchunked `read_object_rows` call,
   bypassing `c_select_package_size`; only the `abap_false` branch actually
   chunks at 1000 rows per `read_object_rows` call. Tree-frontier width is
   bounded by directory fan-out in practice but is not an enforced budget —
   using `abap_false` guarantees the walk never issues a single
   width-unbounded `SELECT ... FOR ALL ENTRIES`, regardless of how wide a
   real directory turns out to be. **INV-B-13**: every bulk object read in
   `verify_tree_closure`/`get_tip_blob_sha1s` is chunked at
   `c_select_package_size` (1000), never a single unbounded `IN`-list read.);
   raise if any requested tree is absent or not type `tree`; decode each via
   `zcl_abapgit_git_pack=>decode_tree`; raise if undecodable.
4. For each decoded node: `chmod = dir` → enqueue into next frontier if not
   already in `lt_seen_trees`; `chmod = submodule` → skip (matches existing
   `get_reachable_objects` precedent); `chmod` file/executable/symlink →
   **ignored, not collected, not checked for presence** (this method proves
   trees are closed, nothing about blobs); any other `chmod` → raise
   (`INV-B-04`: an undecodable tree node is a hard graph-closure failure,
   never silently skipped).
5. Termination: the frontier is strictly the previous iteration's newly
   discovered, not-yet-visited directory SHA1s; since `lt_seen_trees` only
   grows and the graph is finite and acyclic per tree object identity
   (content-addressed — a cycle would require two different SHA1s to
   decode into structurally identical parent/child pointers, which cannot
   happen for a single real tree), the frontier is empty after at most
   `(unique tree count)` iterations.
6. Success (no raise) = graph closure verified for commit+tree only.

Complexity: `O(T)` bulk-fetched tree objects — chunked at `c_select_package_size`
(1000) per `INV-B-13`, so a single frontier iteration issues
`ceil(frontier_width/1000)` reads, never one unbounded read — across `O(F)`
iterations where `F` is tree-depth (frontier count), never `O(N)` stored
objects. Matches the mandatory performance model's `K`/`F` framing (§ below).

## 7. Tree-frontier representation

Same as existing `get_reachable_objects`/`get_reachable_sha1s`:
`zif_abapgit_git_definitions=>ty_sha1_tt` (`STANDARD TABLE`) for the
"current frontier to bulk-fetch this iteration" and "next frontier"
tables, refreshed every iteration (never accumulated); `HASHED TABLE ...
WITH UNIQUE KEY table_line` (the existing private `ty_sha1_set` type
already declared in `zcl_abapgit_ortec_obj_store`'s private section) for
the visited-tree dedup set. No new types needed.

## 8. Visited and deduplication keys

- Visited trees: tree SHA1 (`ty_sha1_set`, as above) — **INV-B-03**.
- Visited/discovered blobs (Slice 4 only, § 9): blob SHA1
  (`ty_sha1_set`) — **INV-B-05**: a blob referenced by multiple tree
  entries (e.g. an identical file in two directories) is emitted into the
  materialization want-set exactly once.
- No commit-level dedup needed — both new methods operate on exactly one
  commit SHA1 per call (the resolved tip); Package B never walks multiple
  commits/ancestors (that is `zcl_abapgit_ortec_fetch_neg`'s
  `collect_ancestor_haves` concern, untouched by Package B).

## 9. Selected blob set representation and bulk database lookup shapes

**Decision: second new method, `zcl_abapgit_ortec_obj_store=>get_tip_blob_sha1s`,
built on the identical walk as `verify_tree_closure` (§6) plus blob-leaf
collection — no blob presence check, no blob DATA read.**

```abap
CLASS-METHODS get_tip_blob_sha1s
  IMPORTING iv_repo_key       TYPE ty_repo_key
            iv_commit         TYPE zif_abapgit_git_definitions=>ty_sha1
  RETURNING VALUE(rt_sha1s)   TYPE zif_abapgit_git_definitions=>ty_sha1_tt
  RAISING   zcx_abapgit_ortec_git.
```

Same algorithm as §6 steps 1-3 (including the `iv_bulk_fetch = abap_false`
correction, INV-B-13), plus: for `chmod` file/executable/symlink,
if not already in a private `lt_seen_blobs` (`ty_sha1_set`) hashed set,
insert and append to `rt_sha1s` (**INV-B-05**, satisfies B2 AC10 "duplicate
blob SHA1s are emitted once"). `rt_sha1s` is returned deduplicated but
**unfiltered by presence** — callers bulk-subtract READY presence
separately (§10). A tree/commit that fails closure here raises identically
to §6 (B2 AC12 "missing/not-READY trees prevent successful discovery").

**Decision recorded per the checkpoint rule:** `get_tip_blob_sha1s` has no
independent productive caller and no externally observable behavior on its
own — it exists purely to feed `materialize_tip_snapshot` (§11). Per the
explicit checkpoint exception ("B2 may be combined with B3 in one commit
only if B2 has no productive caller or independent externally observable
behavior and splitting it would create a non-activatable checkpoint"), **B2
and B3 are combined into one checkpoint/commit** (`get_tip_blob_sha1s` +
`materialize_tip_snapshot` land together, with B2's own focused tests
against `get_tip_blob_sha1s` directly, matching the existing
`ltcl_*` pattern of testing `obj_store` walk methods standalone). B1
(`verify_tree_closure` + `acquire_blobless_graph`) remains its own
independent, IT8-gated checkpoint.

**Bulk database lookup shapes** (all reused, no new shapes invented):

- Tree/commit frontier reads: `zcl_abapgit_ortec_obj_store=>get_objects`
  with **`iv_bulk_fetch = abap_false`** (INV-B-13 — corrected per
  design-review second pass, `VB-B-B0-PERF-REV2`: only the `abap_false`
  branch actually chunks at `c_select_package_size` = 1000 via
  `read_object_rows`; the `abap_true` branch is unchunked), never one
  `SELECT` per SHA1.
- READY presence bulk-check (§10): `zcl_abapgit_ortec_obj_store=>get_missing_sha1s`
  — already exists, already chunked, already `obj_data`-free (unlike
  `get_objects`, never selects the blob payload column).

## 10. Row and byte batch budgets

- **Want-list row budget (request-side):** reuse
  `zcl_abapgit_ortec_fetch_req=>c_materialize_batch_max` (100) unchanged —
  no new constant. `materialize_tip_snapshot` chunks the deduplicated
  missing-blob list into groups of at most 100 before each `build_request`
  call (**INV-B-06**: `build_request` itself still raises defensively if
  ever handed more than 100, per existing Slice 2 behavior — Package B's
  chunking loop is a second, redundant-but-safe layer, not the only guard).
- **Response byte budget (oversized-object handling):** new constant
  `zcl_abapgit_ortec_cold_init=>c_max_batch_response_bytes` (value: 26,214,400
  = 25 MiB — chosen as a generous multiple of typical abapGit source-file
  sizes while still bounding worst-case single-HTTP-response memory; not a
  protocol-enforced limit, since Git v1 upload-pack has no server-side
  response-size negotiation). After `send_receive_close` returns for a
  batch: if `xstrlen( response ) > c_max_batch_response_bytes` **and** the
  batch requested more than 1 SHA1, halve the batch and retry each half as
  its own `MATERIALIZE_BLOBS` request, bounded to at most `ceil(log2(100))`
  = 7 splits — **rescoped per design-review Finding 3
  (`.memory/reviews/performance_design_variant_b_package_b.md`): the split
  counter is local to EACH top-level ≤100-row batch (reset to 0 when that
  batch's send/response cycle begins), never shared across the other
  batches of the same `materialize_tip_snapshot` call — INV-B-07b: one
  oversized batch exhausting its own split budget must never reduce the
  split budget available to a later, independent batch in the same call.**
  If a **single-SHA1** batch alone exceeds the ceiling, or that batch's own
  split budget is exhausted, raise `zcx_abapgit_ortec_git` for that batch
  (a genuinely oversized blob is a structured failure, never a silent
  unbounded accept — **INV-B-07**; the raise still aborts the whole
  `materialize_tip_snapshot` call per §12/§13 — a per-batch budget only
  prevents *cross-batch* starvation, it does not make a single
  unresolvable batch non-fatal). This is the "oversized-object handling"
  the owner spec's Slice 4 requires; it does not affect `decode_streaming`'s
  own per-object streaming discipline (still never more than one object's
  decompressed bytes resident at once), only the upstream HTTP response
  accumulation Git's wire protocol makes unavoidable per batch.
- **Cold-graph response ceiling (`acquire_blobless_graph`, design-review
  Finding 1 — BLOCKING, now resolved):** `INITIAL_BRANCH_BLOBLESS` has no
  `deepen`/row cap by construction (owner invariant — full history, one
  want) and, unlike the materialize path, cannot be split into smaller
  requests (there is exactly one want: the tip). The `git-partial-clone`
  skill mandates "Full branch recovery must be protected by an explicit
  memory-risk gate while HTTP responses are materialized as one XSTRING" —
  `acquire_blobless_graph` has the identical single-XSTRING-response
  profile and, unlike the exceptional `RECOVERY_BRANCH_FULL` tier, is the
  **normal** cold-start path once Package C wires it in. New constant
  `zcl_abapgit_ortec_cold_init=>c_max_graph_response_bytes` (value:
  209,715,200 = 200 MiB — an order of magnitude above the per-batch blob
  ceiling, sized for a blob-free full commit/tree closure rather than blob
  content). After `send_receive_close` returns for the
  `INITIAL_BRANCH_BLOBLESS` request, if `xstrlen( response ) >
  c_max_graph_response_bytes`, `acquire_blobless_graph` raises
  `zcx_abapgit_ortec_git` immediately, **before** calling `decode_streaming`
  (fail fast — no wasted decode work on a response that will be rejected
  anyway) — **INV-B-12**: an oversized cold-graph response is a structured,
  typed failure, never decoded unconditionally. Package B defines no
  fallback/retry for this condition (matches §13's "propagate, Package C
  decides" pattern; a repository whose blobless history alone exceeds 200
  MiB is exactly the owner spec's own hard-stop scenario "no bounded bulk
  ... alternative is an unsafe unbounded response" and is therefore a
  decision for the owner/Package C, not Package B).

## 11. Blob materialization request construction and response verification

`materialize_tip_snapshot( iv_repo_key, iv_commit, iv_url )`:

1. `get_tip_blob_sha1s( iv_repo_key, iv_commit )` (§9) — raises if the
   commit/tree closure is not itself intact (this re-verifies closure
   independently of any earlier `acquire_blobless_graph` call in the same
   or a prior session — no cross-call in-memory trust, matching Slice 1's
   "no auto-backfill" philosophy and enabling idempotent restart, §14).
2. `get_missing_sha1s( iv_repo_key, rt_sha1s )` (§10) — bulk-subtract READY
   presence; if empty, no HTTP call at all (**INV-B-08**: zero-blob-missing
   is a fast, HTTP-free path).
3. `begin_attempt( iv_repo_key, iv_commit )` once for the whole call
   (single attempt_id reused across every batch — not one attempt per
   batch, matching §4.4's "generated once per orchestration call").
4. Chunk missing SHA1s into ≤100-row batches (§10); for each batch:
   a. resolve an HTTP client via `zcl_abapgit_http=>create_by_url` +
      `parse_capabilities` (identical pattern to
      `upload_pack_by_commit`, §1 of `variant_b_slice2_design.md` — no new
      connection-establishment code invented);
   b. `build_request( iv_mode = materialize_blobs, it_want_hashes =
      <batch>, iv_server_caps = ... )` — raises the typed
      unsupported-capability failure per §2 if the server never advertises
      the required capability (checked once, on the first batch; if it
      fails there it fails identically for every subsequent batch, so
      `materialize_tip_snapshot` does not re-attempt with a different mode
      — **INV-B-01** again);
   c. send, apply the oversized-response split policy (§10);
   d. `zcl_abapgit_ortec_pack_stream=>decode_streaming( iv_data, iv_repo_key,
      iv_url )` — persists and internally commits the batch's objects
      exactly like §4.1;
   e. **response verification (owner spec Slice 4 item 11):** for every
      SHA1 in the batch, confirm it now appears in `rt_objects`-equivalent
      persisted state with `obj_type = 'blob'` and the SHA1 the server
      returned matches the SHA1 requested — `decode_streaming`/
      `decode_and_persist_streaming` already re-derive and verify each
      object's own SHA1 from its decompressed content during decode
      (pre-existing Slice 2C guarantee — an object cannot be persisted
      under a SHA1 that doesn't match its content), so this step is a
      **bulk re-check via `get_missing_sha1s` on the same batch** rather
      than a second independent hash computation: if any requested SHA1 is
      still missing after `decode_streaming` returns without raising, that
      is a distinct, non-corruption failure (server sent a response but did
      not actually include a wanted, capability-advertised object) — raise
      `zcx_abapgit_ortec_git` rather than silently continuing
      (**INV-B-09**).
5. After all batches succeed: final `get_missing_sha1s` re-check across the
   **complete original** `rt_sha1s` set (not just the last batch) — only if
   empty does step 6 run (owner spec Slice 4 item 6 "re-check the complete
   missing set").
6. `publish_snapshot_complete( iv_repo_key, iv_branch_name, iv_commit,
   attempt_id )` — raises per its own existing contract if `hist_level <
   GRAPH_COMPLETE` (i.e. `acquire_blobless_graph` was never successfully
   run for this commit — **INV-B-10**, snapshot cannot precede graph, reused
   unchanged from Slice 1). Exactly one `COMMIT WORK` follows.

`iv_branch_name` is required by `publish_snapshot_complete`'s existing
signature (it also updates `zaog_repo_state`'s branch pointer) —
`materialize_tip_snapshot` therefore takes `iv_branch_name` as an explicit
parameter; Package B does not invent a commit-only publication path.

## 12. Graph and snapshot certificate publication rules

Both orchestration methods follow the identical shape: **verify, then
certify, then commit — never the reverse, never partially.**

- `acquire_blobless_graph`: `begin_attempt` → `decode_streaming` (already
  self-committed) → `verify_tree_closure` (raises = stop, no certificate) →
  `mark_graph_complete` → `COMMIT WORK`.
- `materialize_tip_snapshot`: `begin_attempt` → per-batch materialize+verify
  loop (any raise = stop, no certificate; already-decoded objects from
  earlier batches in the same call remain, harmless, per §4.3) → final
  re-check → `publish_snapshot_complete` → `COMMIT WORK`.

**INV-B-11**: neither method ever calls its respective `mark_*`/`publish_*`
method more than once per invocation, and never after a caught/handled
exception from an earlier step in the same call (no "certify anyway,
downgrade later" pattern).

## 13. Failure behavior and retry limits

- Capability failures (§2): no retry, propagate immediately (INV-B-01).
- Closure verification failures (§6/§9 raise): no retry inside Package B —
  propagate; a future Package C caller decides whether to attempt
  `RECOVERY_BRANCH_FULL` (out of scope here, matches the owner spec's
  Slice 5 ownership of that decision).
- Oversized-batch split (§10): bounded to 7 splits per top-level batch
  (reset for each batch, per Finding 3's fix — never shared across
  batches), tracked via a loop-local variable (not `CLASS-DATA` — no
  cross-call budget needed here, unlike `pack_stream`'s
  `gv_completion_attempts`, because this is a bounded, single-call,
  non-re-entrant loop, not a nested/recursive completion path).
- Cold-graph oversized response (§10, INV-B-12): no retry/split — a single
  want cannot be subdivided; immediate structured failure, propagate.
- Network/decode errors from `decode_streaming` (`zcx_abapgit_ortec_git`):
  no retry inside either method — propagate as-is. Package B does not
  reintroduce `complete_missing_object`-style per-object network repair
  (forbidden by the current stop conditions) and does not call
  `fetch_tip_commits` or any progressive-deepen helper.

## 14. Idempotent restart behavior

- `acquire_blobless_graph`: re-invoking it for the same commit after a
  prior failed attempt is safe — `begin_attempt` never downgrades an
  existing `hist_level`, `mark_graph_complete` is idempotent (no-op if
  already `GRAPH_COMPLETE`/`FULL_COMPLETE`), and any already-persisted
  READY objects from the failed attempt are reused as-is (content-addressed,
  never re-fetched by `INITIAL_BRANCH_BLOBLESS`'s want-only, have-free
  request shape — the server always resends the full closure regardless,
  but `decode_streaming`'s persistence is naturally idempotent via `MODIFY`
  semantics on `zaog_obj_store`).
- `materialize_tip_snapshot`: re-invoking it after a prior partial failure
  is safe and cheap — step 2's `get_missing_sha1s` re-check means already-
  materialized blobs from a prior partial run are never re-requested; only
  the genuinely still-missing subset is fetched again.
- Both methods are therefore safe to call repeatedly without an external
  "have I already tried this" guard — this satisfies the owner spec's
  general no-auto-backfill/re-verification philosophy without Package B
  inventing a new resumability mechanism.

## 15. Standard-abapGit behavior when ORTEC is disabled

Neither new method is reachable from any hook while
`zcl_abapgit_ortec_git_switch=>is_active_for_repo` is false, because
neither has a productive caller yet (§1) — standard abapGit behavior is
therefore provably unchanged by Package B (no code path added to any
existing hook). This will remain true until Package C wires them in behind
the same switch check `pull_by_branch` already performs first.

## 16. Interfaces intentionally deferred to later packages

- Branch-switch decision flow / cold-vs-warm classification / wiring into
  `pull_by_branch` and `zcl_abapgit_ortec_filter_walk` — Package C.
- Generalized certified-have selection/limiting — Package C.
- Bulk external delta-base collect/dedupe/fetch (only affects
  `resolve_streaming`'s existing bounded escalation signal, unchanged and
  still not reachable from any Package B path since `MATERIALIZE_BLOBS`
  responses are self-contained per-batch packs, not deltas against
  external history) — Package D1.
- Final attempt/session/pack identity architecture and staged-vs-published
  visibility model beyond the existing `decode_streaming`
  commit-then-certify boundary reused as-is in §4 — Package D2.
- Physical removal of any now-more-clearly-legacy code — Package E.

---

## Mandatory performance model (owner spec format)

| Path | Cardinality driver | SQL calls | HTTP calls | Max simultaneous XSTRING |
|---|---|---|---|---|
| `verify_tree_closure` | `T` = unique trees reachable from tip | `O(F)` iterations, each `ceil(frontier_width/1000)` chunked reads (`iv_bulk_fetch = abap_false`, INV-B-13 — corrected per design-review Finding 2, `get_objects`'s `abap_true` branch is NOT chunked) | 0 | one tree object's data at a time (loop-local field-symbol) |
| `get_tip_blob_sha1s` | same `T`/`F` as above | identical shape (INV-B-13) | 0 | same as above |
| `acquire_blobless_graph` | 1 commit + `T` trees (server-side, unbounded history, no `deepen`) | `O(F)` walk reads (above) + `decode_streaming`'s existing batched (500-row) writes | 1 (`info/refs`) + 1 (`upload-pack`) | one response XSTRING, explicitly gated at `c_max_graph_response_bytes` = 200 MiB (INV-B-12, resolves design-review Finding 1 — checked before `decode_streaming` is invoked) + one object at a time during decode |
| `get_missing_sha1s` (reused) | `K` = candidate blob count | `O(ceil(K/1000))` chunked, `obj_data`-free | 0 | none (no payload column selected) |
| `materialize_tip_snapshot` | `K` = missing tip-blob count, batched at ≤100 | `O(F)` walk (once) + `O(ceil(K/1000))` presence checks (twice: initial + final) + `decode_streaming` writes | `O(ceil(K/100))` batches × (≤7 splits worst case, per-batch budget — INV-B-07b) | one batch response XSTRING at a time, gated at `c_max_batch_response_bytes` = 25 MiB per batch (never more than one batch's response resident; previous batch's response is out of scope by the time the next begins) |

None of these scale with `N` (total stored objects) — every read is keyed by
the resolved tip's own reachable SHA1 set (`T`) or an explicit candidate
list (`K`), matching **INV-PERF-01** (owner spec: "incremental work scales
with required objects K, not all repository objects N").

Expected behavior at scale (owner spec's four points, `N` = total stored
objects in `zaog_obj_store` for the repository, independent variable from
`T`/`K`/`F` above):
- `N` = 1,000: walk cost bounded by this tip's own `T`/`F`, typically ≪ `N`;
  no method reads or scans the other 999 unrelated rows.
- `N` = 40,000: identical — `get_objects`/`get_missing_sha1s` both filter by
  `repo_key` + an explicit `it_sha1s`/frontier list, never a
  repository-wide `SELECT` without a SHA1 predicate.
- `N` = 1,000,000, `K` ≈ 100 affected tip blobs: `materialize_tip_snapshot`
  issues 1 `get_missing_sha1s` call (≤1 chunk, since `K` ≤ 1000) against a
  keyed `repo_key + obj_sha1 IN (...)` read — never a table scan of the
  million-row table (**INV-PERF-02**, satisfies owner spec acceptance
  scenario 14 verbatim).

## Acceptance scenario / mandatory test coverage map

All 22 tests listed in the current prompt's "Mandatory tests" section map to
this design as follows (evidence for the reviewer, not a restatement of the
test list itself):

- B1 tests 1-3 → `zcl_abapgit_ortec_fetch_req=>build_request` (already
  implemented/SAP-validated in Slice 2C — Package B adds no new assertions
  here beyond confirming `acquire_blobless_graph` calls it with the right
  mode/params) plus a new `ltcl_cold_init` wire-shape test reusing the
  existing `ltcl_fastpath_protocol` buffer-assertion pattern.
- B1 tests 4-6 → `verify_tree_closure` (§6) + `mark_graph_complete`'s
  existing idempotency contract (§14).
- B1 test 7 → no new test needed beyond §15's structural argument (no hook
  changed); a single assertion that `is_active_for_repo = abap_false` short-
  circuits before any Package B code path is reachable is still added for
  regression-proofing, on whichever hook Package C will eventually use —
  deferred concretely to Package C since no hook exists yet in Package B.
- B2 tests 8-13 → `get_tip_blob_sha1s` (§9) directly, using the same fake
  commit/tree/blob object-store fixture pattern already used by
  `get_reachable_objects`/`get_reachable_sha1s`'s own existing tests (see
  `zcl_abapgit_ortec_git_tests.clas.testclasses.abap` lines ~120-330).
- B3 tests 14-21 → `materialize_tip_snapshot` (§11), covering
  `get_missing_sha1s` subtraction, batch chunking (§10), the capability
  raise (§2), and `publish_snapshot_complete`'s existing precondition guard
  (§4.4/§12) for partial-batch non-publication.
- Test 22 (no regression) → existing `ltcl_*` suites for `fetch_req`,
  `pack_stream` (resolve/decode), completeness (`fetch_neg`), and
  `base_cache` are not modified by this design; only new classes/methods
  are added.

All new ABAP test method names will be verified ≤30 characters before any
commit, per the mandatory-tests closing rule.
