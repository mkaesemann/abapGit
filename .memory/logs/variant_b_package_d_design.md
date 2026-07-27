# Variant B Package D — D0 shared design (D1 + D2)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=D-DESIGN
STATUS=IMPLEMENTATION_AUTHORIZED
PACKAGE_C_VALIDATED_BASELINE=29199f629773c676e0eaa2f3a006f5167d304ae8
PACKAGE_D_SOURCE_BASELINE=5e5403546554dbe4f0f8e7a1eb2f07ab434dbe95
INPUTS=variant_b_package_d_delta_discovery.md,variant_b_package_d_transaction_discovery.md,regression_variant_b_package_d_baseline.md,variant_b_package_d_concurrent_commit_impact.md
```

`PACKAGE_D_SOURCE_BASELINE` is the verified current branch tip (git-ancestry
and diff-content verified only; **not** SAP-validated). It supersedes
`PACKAGE_C_VALIDATED_BASELINE` for source-fact purposes after reconciling
four concurrent owner commits — see
`.memory/logs/variant_b_package_d_concurrent_commit_impact.md`.

This design is written directly by the orchestrator from the three focused
discovery artifacts plus targeted re-reads of the cited source (current
productive source outranks discovery prose wherever the two disagree; one
disagreement was found and is called out in §1).

## 1. Verified current-source findings (reconciliation of discovery evidence)

### 1.1 Delta resolution (D1 area) — CONFIRMED_CURRENT

- Non-streaming path: `zcl_abapgit_ortec_pack_dec=>resumable_decode` already
  collects declared REF_DELTA bases not present in the raw-scanned pack
  (`lt_delta_bases` minus `lt_pack_shas`), performs **one** bulk
  `SELECT ... FOR ALL ENTRIES` against `zaog_obj_store` (`status = 'R'`), and
  merges results into `rt_objects` with freshly assigned unique `-index`
  values before calling `zcl_abapgit_ortec_delta=>resolve_all`. This is
  already a correct bulk external-base load — not a per-delta loop.
- `zcl_abapgit_ortec_delta=>resolve_all`/`resolve_one` already implement a
  bounded two-phase fixpoint: repeated in-pack-only ascending sweeps
  (`iv_allow_thin_fetch = abap_false`) until no progress, then one final pass
  that allows the (already bulk-loaded) external bases to be found, or raises
  `Delta base not found`. Chain depth is bounded by
  `c_max_chain_depth = 64`. Identity is resolved via two hashed side-tables
  (`ct_tabix_by_index`, and the non-unique `sha` secondary key filtered by
  self-exclusion and by type), avoiding the two previously-fixed bugs
  (unresolved-sibling false match, index-collision on merged thin bases).
- Streaming path: `zcl_abapgit_ortec_pack_stream=>resolve_streaming`/
  `resolve_one_meta` implement the **same** two-phase fixpoint shape
  (`ty_meta.delta_base`/`base_offset` = declared identity,
  `ty_meta.sha1`/`obj_type`/`is_resolved` = resolved identity,
  `ct_sha_idx`/`ct_tabix_by_offset` = hashed side-tables), but — unlike the
  non-streaming path — it has **no bulk external-base collection step**.
  Its final pass calls `get_base_bytes` → `zcl_abapgit_ortec_obj_store=>
  get_object(...)` **once per still-unresolved external base**, i.e. one SQL
  round-trip per missing base. This is the one real per-object-SQL gap in
  the current implementation and is the primary generalization target for
  D1 (§3).
- `complete_missing_base` (streaming) is a permanent no-op (`RETURN` with a
  comment referencing incident `F-2C-001`); no per-base HTTP repair is
  currently reachable from either resolver.
- Missing-base recovery signal is already unified in shape: both resolvers
  raise, and the streaming resolver's raise carries
  `iv_retry_without_haves = abap_true`, which is the single recovery-tier
  contract consumed one layer up (fastpath's thin → self-contained →
  full-recovery cascade, outside D1's scope).
- `zcl_abapgit_ortec_base_cache` is a **process-global singleton**
  (`CLASS-DATA go_instance`), byte-budgeted at 256 MiB with `by_sha1`
  hashed + `by_seq` sorted secondary keys for O(1) lookup and O(1)
  LRU eviction. It is keyed by real content SHA1, so cross-attempt/
  cross-pack reuse is correctness-safe (content-addressed); it is not,
  and does not need to be, attempt-scoped.

### 1.2 Correction to delta-discovery artifact (documented contradiction)

`variant_b_package_d_delta_discovery.md` §7 states "No dedicated ABAP Unit
test methods for `ofs_d` or mixed `ref_d`/`ofs_d` chains were found" — this
is **contradicted by current source** and is superseded by the regression
baseline artifact's finding. `zcl_abapgit_ortec_git_tests.clas.testclasses.abap`
contains `ref_chain_resolves`, `ofs_chain_resolves`,
`external_thin_base_resolves`, `two_thin_bases_do_not_collide`,
`missing_base_raises`, `missing_base_no_http_retry`, `base_after_dependent`,
`resolve_after_prior_in_pass`, `chain_onto_later_unresolved` (verified by
direct grep against the file, line numbers 262–990). The discrepancy is
explained by the delta-discovery task's narrowed `SOURCE_SCOPE` (it did not
include `zcl_abapgit_ortec_git_tests.clas.testclasses.abap`). Treat the
regression baseline artifact as authoritative for D1 test-coverage claims.

### 1.3 Transaction/persistence (D2 area) — CONFIRMED_CURRENT plus one new finding

- `zcl_abapgit_ortec_mat_state` already implements almost the entire D2
  target state machine: `hist_level` (`U`/`G`/`F`), `snap_state`
  (`N`/`P`/`C`/`I`), per-row `attempt_id` with stale-attempt rejection on
  every mutating call, no internal `COMMIT WORK`/`ROLLBACK WORK` (caller-
  owned), and a set-based `clean_incomplete_attempts` (one bulk `UPDATE`,
  age-gated, `attempt_id`-only clear). `publish_snapshot_complete` already
  enforces `hist_level = FULL_COMPLETE` before writing `snap_state = C`, and
  atomically updates the matching `ZAOG_REPO_STATE` branch pointer in the
  same (uncommitted) LUW.
- `zcl_abapgit_ortec_fastpath=>certify_fetched_commit` is already the
  orchestrator-owned certification sequence (`begin_attempt` →
  `verify_tree_closure` → `mark_graph_complete` → `get_tip_blob_sha1s`/
  `get_missing_sha1s` → `mark_full_complete` → `publish_snapshot_complete`),
  and its only caller, `persist_pull_result`, issues the single `COMMIT
  WORK` after `persist_missing_objects` + `certify_fetched_commit` +
  `update_after_fetch` all succeed. This confirms the orchestrator-owned
  publication boundary pattern already exists for the incremental-update
  path and should be generalized, not replaced.
- **New finding (not previously documented in Package C material):**
  `zcl_abapgit_ortec_pack_stream=>decode_and_persist_streaming` performs a
  blanket `UPDATE zaog_obj_store SET status = 'R' WHERE repo_key = ...
  AND pack_id = ... AND status = c_status_incomplete` immediately after
  trailer verification, **followed by its own `COMMIT WORK`** — before
  `resolve_streaming` ever runs. This promotion is blanket: it applies
  equally to genuinely-resolved non-delta rows (correctly keyed by real
  content SHA1) **and** to delta rows still keyed by a synthetic
  `temp_key` (`pack_id` + zero-padded index), whose `obj_data` is still the
  raw, unresolved delta byte stream, not real object content.
  Consequences, confirmed against `read_object_rows`'s hard-coded
  `WHERE ... status = 'R'` filter:
  - a synthetic temp-key row becomes globally READY/have-eligible-adjacent
    (visible to any `get_object`/`get_objects` call, though the temp key
    itself is not a real 40-hex content SHA1, so it does not collide with
    a real lookup — it is inert garbage, not a false positive for a real
    object);
  - if the process crashes or `resolve_streaming` subsequently raises
    **after** this `COMMIT WORK`, the temp-key rows are already durably
    committed as `status = 'R'`. `decode_streaming`'s `ROLLBACK WORK` (on a
    `resolve_streaming` failure) cannot undo already-committed work, and
    `cleanup_incomplete` only deletes rows still at `status = 'I'` — so a
    failed/interrupted resolve after this point leaks orphaned `status='R'`
    temp-key rows in `ZAOG_OBJ_STORE` permanently (never cleaned by any
    current code path).
  - This is a genuine, previously-undocumented staged-visibility gap
    directly relevant to D2's "staged rows never globally READY" and
    "crash-safe / idempotent retry" requirements. D2 owns the fix (§5).
- Table correlation-ID audit: `ZAOG_FETCH_SESS`/`ZAOG_PACK_META`/
  `ZAOG_PACK_IDX`/`ZAOG_RAW_PACK` are keyed by `session_id`/`pack_id`
  (`pack_id = repo_key + timestamp`, `build_pack_id`), while
  `ZAOG_COMMIT_HIST`'s `attempt_id` is an independently generated UUID from
  `zcl_abapgit_ortec_mat_state=>begin_attempt`. **There is currently no
  shared correlation key across the pack/session layer and the
  materialization-attempt layer** — a `pack_id` cannot currently be joined
  to the `attempt_id` that certified (or failed to certify) the commit it
  belongs to. This is the second concrete D2 gap (§5).
- No cross-repository or same-repository concurrent-attempt serialization
  exists beyond repository-key scoping plus `attempt_id` staleness checks;
  `zcl_abapgit_ortec_pack_raw=>acquire_repo_lock`/`release_repo_lock` and
  `zcl_abapgit_ortec_cache_admin=>acquire_lock` provide repo-scoped
  locking for fetch sessions and cache-clear respectively, but the two
  lock mechanisms are not unified and materialization attempts themselves
  are not lock-protected (only attempt-ID-validated after the fact).

## 2. D1 / D2 ownership boundaries

| Concern | Owner | Notes |
| --- | --- | --- |
| In-pack REF/OFS fixpoint algorithm (`zcl_abapgit_ortec_delta`, `pack_stream` meta resolver) | D1 | Reuse existing shape; no rewrite |
| Bulk external-base collection + load for the streaming resolver | D1 | New: port the non-streaming pattern |
| Unifying non-streaming's manual `FOR ALL ENTRIES` with `obj_store=>get_objects(iv_bulk_fetch = abap_true)` | D1 | Consolidation, not new capability |
| Base-cache scope/limits | D1 (documents only; no code change required) | Already correct |
| One bounded recovery tier contract (`iv_retry_without_haves`) | D1 (owns the delta-layer signal); consuming cascade stays out of scope | Cascade already lives in fastpath |
| `ZAOG_OBJ_STORE` staged (`I`)/ready (`R`) visibility and the streaming blanket-promotion gap | D2 | New fix, see §5 |
| Attempt/session/pack correlation identity | D2 | New: unify `attempt_id` across tables |
| `ZAOG_COMMIT_HIST`/`ZAOG_REPO_STATE` state machine and F/C publication invariant | D2 (verification only; no behavior change required) | Already correct |
| Set-based stale-attempt cleanup | D2 (verification only) | Already correct (`clean_incomplete_attempts`) |
| Commit/rollback ownership map | D2 | Documented in §6; one new commit-boundary fix in §5 |
| Same-repo/cross-repo concurrency | D2 | New: define using existing lock primitives |

Package C's certification sequence, `F/C` publication gate, cold/warm/
incremental routing, and cache administration are **not reopened**; they
are inputs D1/D2 must preserve.

## 3. Current and target identity model (D1)

### Current (per resolver, already sound in shape)

| | Declared base identity | Resolved payload |
| --- | --- | --- |
| Non-streaming REF_DELTA | `ty_object-sha1` (overloaded placeholder) | promoted in place: `type`, `data`, `sha1` become real |
| Non-streaming OFS_DELTA | `ty_ofs_meta-base_offset` (separate side table) | same in-place promotion |
| Streaming | `ty_meta-delta_base` / `ty_meta-base_offset` (dedicated fields) | `ty_meta-sha1` / `obj_type` / `is_resolved` (dedicated fields) |

### Target (D1)

Adopt the **streaming `ty_meta` shape** (separate declared-identity fields,
never overloaded) as the canonical model for any new/shared code, since it
already avoids the overloading present in the non-streaming `ty_object`
representation. Do not force a rewrite of `zcl_abapgit_ortec_delta`'s
existing `ty_object`-based overload — it is proven correct (defensive
identity checks already catch mismatches) and rewriting it is out of
Package D's correctness/perf budget. Instead:

- keep the two resolvers as two call sites of one shared **bulk
  external-base resolution step** (§4), not one merged resolver class;
- both call sites must produce a `ty_ofs_meta`/`ty_offset_entry`-equivalent
  side table using **stable hashed keys** (`by_index`/`by_offset`), exactly
  as today — no new mutable/nonunique secondary key is introduced;
  `sha`/`by_sha1`-style secondary keys remain read-only lookup aids, never
  used as if they were unique object identity.

## 4. D1 iterative fixpoint algorithm (target, unified)

Both resolvers keep their existing Phase 1 (in-pack-only fixpoint) exactly
as-is. A new **Phase 1.5** is inserted between the existing Phase 1 and
Phase 2 in **both** resolvers:

```text
Phase 1   : repeat in-pack-only sweeps until no progress (UNCHANGED)
Phase 1.5 : NEW — collect declared base SHA1s of every still-unresolved
            REF_DELTA row whose base is not resolvable in-pack (i.e. the
            set Phase 1 gave up on); deduplicate; bulk-load via
            zcl_abapgit_ortec_obj_store=>get_objects(
              iv_repo_key = <explicit, nonblank>
              it_sha1s    = <deduplicated set>
              iv_bulk_fetch = abap_true )
            merge results into the in-pack object/meta set with freshly
            assigned unique index/offset-adjacent identity (reusing the
            existing "assign index = lines(...)+1" fix pattern); this is
            ONE bulk call, not a loop.
Phase 2   : one final ascending pass — now every previously-external base
            that exists is already merged in-pack, so Phase 2 degrades to
            the SAME in-pack resolution logic as Phase 1 plus the single
            terminal raise ("Delta base not found") for anything still
            missing. Phase 2 no longer performs its own per-base
            get_object() call.
```

Rationale: this preserves both resolvers' proven fixpoint/identity logic
unchanged and localizes the new work to one bulk collection+load helper
shared by both call sites: a new **PUBLIC** `CLASS-METHODS
bulk_resolve_external_bases` on `zcl_abapgit_ortec_delta`. It must be
public, not `PRIVATE SECTION`/class-internal, because
`zcl_abapgit_ortec_pack_stream`'s streaming resolver calls it cross-class
(there is no friendship or same-class relationship between the two). The
method owns **only** the bounded set-based load and validation
(deduplicate declared external SHA1s, one
`get_objects(iv_bulk_fetch = abap_true)` call, raise on any genuinely
missing/wrong-type entry) and returns the existing, already-public
`ty_object`-shaped `rt_objects` row type (the same shape `resumable_decode`
and `get_objects` already use) — it performs **no** merge into
`ct_objects`/`ct_meta` and **no** auxiliary-index insert itself. Each
caller owns its own resolver-specific merge and auxiliary-index
synchronization: `resumable_decode` merges `rt_objects` into `ct_objects`
and inserts into `ct_tabix_by_index` itself; `resolve_streaming` converts
`rt_objects` to `ty_meta` rows via a thin streaming-side adapter, merges
into `ct_meta`, and inserts into `ct_sha_idx` itself — exactly the
caller-owned responsibility already described by the mandatory
auxiliary-index-synchronization rule below (no change to `ty_meta`'s own
persisted structure).

`c_max_chain_depth = 64` remains the sole recursion bound; Phase 1.5 does
not add recursion, only one bulk read.

**Reconciled after concurrent commit `35be4c65`:** production has already
added a cache-warming bulk preload (`preload_external_bases`) that reduces
Phase 2's per-base calls to cache hits without merging bases into
`ct_meta`/`ct_sha_idx` as this section still specifies. The two approaches
are compatible, not conflicting: Phase 1.5 as designed here remains the
target (it additionally satisfies the DR-003 auxiliary-index-sync
invariant and the `bulk_base_one_call_only`/`no_sql_in_pack_phase` exit
criteria, which cache-warming alone does not guarantee — a cache miss on a
truly cold `base_cache`/`mt_cache` would still fall through to Phase 2's
existing per-base call). Implementation must build Phase 1.5 alongside the
existing `preload_delta_rows`/`preload_external_bases` calls, not assume
they are absent from current source. No design change to the Phase 1.5
algorithm itself is required by this reconciliation.

**Mandatory auxiliary-index synchronization (correctness review DR-003):**
merging a Phase-1.5-loaded row into the working object/meta set is **not
complete** until the same row is also inserted into every hashed lookup
structure Phase 2 actually reads, mirroring exactly what the existing
on-demand-fetch fallback already does for a single fetched base:

- Non-streaming (`zcl_abapgit_ortec_delta`): every row Phase 1.5 merges
  into `ct_objects` must also get an explicit `INSERT ... INTO TABLE
  ct_tabix_by_index` entry (same table `resolve_all`'s startup loop and
  `resolve_one`'s existing on-demand-fetch fallback already populate).
- Streaming (`zcl_abapgit_ortec_pack_stream`): every row Phase 1.5 merges
  into `ct_meta` must also get an explicit `INSERT ... INTO TABLE
  ct_sha_idx` entry (the hashed index `resolve_one_meta`'s REF_DELTA
  lookup reads via `READ TABLE ct_sha_idx ... WITH TABLE KEY sha1`).

Omitting either insert does not corrupt data (both resolvers' existing
`iv_allow_thin_fetch = abap_true` fallback still resolves the row via a
per-base `get_object()` call on a lookup miss), but it silently
reintroduces the exact per-object SQL calls D1 exists to eliminate and
would fail the `bulk_base_one_call_only`/`no_sql_in_pack_phase` exit
criteria (§19). Both new tests must assert zero fallback invocations, not
just a correct final result, to catch a missed index-sync regression.

## 5. Bounded external-base bulk-load and recovery algorithm (D1) + staged-visibility fix (D2)

### 5.1 Bulk-load (D1)

- Exactly one `get_objects(iv_bulk_fetch = abap_true)` call per resolver
  invocation (i.e. per pack), never per delta, never per sweep.
- Row/byte bound: bounded by the number of **distinct declared external
  bases in this one pack** (never repository-wide); `get_objects` already
  chunks internally via `c_select_package_size = 1000` when
  `iv_bulk_fetch = abap_false`, but the `abap_true` branch used by cold-init/
  branch-closure callers is deliberately unchunked per those callers'
  existing INV-B-13 comments — D1 must call it only with the (small, pack-
  bounded) external-base set, never with an unbounded id list, so the
  unchunked branch stays safe for this use.
- Non-streaming `resumable_decode` is refactored to call this same helper
  instead of its own manual `SELECT ... FOR ALL ENTRIES`, removing
  duplicate logic while keeping identical externally-observed behavior
  (verified by the existing `prefetch_bases_do_not_collide` test).

### 5.2 Recovery tier (D1)

Exactly one bounded recovery tier remains: the existing
`zcx_abapgit_ortec_git` raise carrying `iv_retry_without_haves = abap_true`,
consumed by the fastpath's existing thin → self-contained → recovery
cascade (out of D1 scope; D1 must not add a second retry loop inside the
resolver). `complete_missing_base` stays a documented permanent no-op in
D1 (physical removal is Package E's job).

### 5.3 Staged-visibility fix (D2, new)

Target: a delta's synthetic temp-key row must never be readable as
`status = 'R'` before that specific delta is actually resolved.

- `decode_and_persist_streaming`'s blanket promotion is split in two,
  discriminated purely by the already-persisted `OBJ_TYPE` column
  (protocol/persistence review m-1: `WHERE ... AND status = 'I' AND
  obj_type IN ('ref_d','ofs_d')` → `'D'`; the complementary predicate →
  `'R'` — no extra in-memory SHA1-list bookkeeping needed, since delta
  rows are already persisted with the raw pack type marker and non-delta
  rows with their real object type):
  1. non-delta rows (already real-SHA1, already-resolved content) are
     promoted to `status = 'R'` exactly as today;
  2. delta rows (temp-key, `is_resolved = abap_false` in the parallel
     `ty_meta`) are promoted to a **new intermediate status**, `status =
     'D'` (decoded-pending-resolution) instead of `'R'`. `'D'` is excluded
     from every existing READY read (`read_object_rows`'s
     `WHERE ... status = 'R'` and `get_present_sha1s`'s equivalent filter)
     by construction, since those filters already hard-code `'R'` and are
     left unchanged.
- `cleanup_incomplete` (and the streaming failure path in
  `decode_streaming`) is extended to delete rows in **both** `status = 'I'`
  and `status = 'D'` for the failed `pack_id`, closing the orphan leak
  identified in §1.3. This is a bounded, set-based `DELETE ... WHERE
  repo_key = ... AND pack_id = ... AND status IN ('I','D')` — same shape as
  today, one extra value in the `IN` list.
- `resolve_streaming`'s own promotion of a resolved delta to `status = 'R'`
  (via `flush_resolve_batch`'s `store_objects(..., iv_status = 'R')`) is
  unchanged; it already writes the real, final content SHA1.
- This is a **status-value and cleanup-predicate change only**; no DDIC
  structural change to `ZAOG_OBJ_STORE` is required (`status` is already a
  single-character field with room for a new value).
- **Correction (correctness review DR-002):** `zcl_abapgit_ortec_pack_raw=>
  cleanup_partial_session` is **not** part of this fix and must not be
  touched. Direct source confirms its `DELETE FROM zaog_obj_store` filters
  `status = 'P'` — the separate, legacy `resumable_decode`/`pack_dec`
  session convention (`'P'` = pending), not the streaming path's `'I'`/
  `'D'`/`'R'` convention. It has exactly one caller
  (`zcl_abapgit_ortec_fastpath=>pull_by_branch`'s legacy session-mismatch
  cleanup) and is unrelated to `decode_and_persist_streaming`'s leak.
  Widening its predicate would be a no-op for this bug and risks breaking
  the legacy path if `'P'` were mistakenly replaced rather than added to.
  Only `zcl_abapgit_ortec_pack_stream=>cleanup_incomplete` is in scope for
  the `status IN ('I','D')` widening.
- **Widened scope, reconciled after concurrent commit `35be4c65`
  (performance DESIGN_GATE finding PERF-B-1, still open):** the `'D'`
  split above would break not only `resolve_one_meta`'s existing
  `get_object(temp_key)` read but **also** the new `preload_delta_rows`
  method's bulk `get_objects(temp_keys)` call added by that commit — both
  route through `read_object_rows`'s hard-coded `WHERE ... status = 'R'`
  filter. The still-pending PERF-B-1 fix (a dedicated status-`'D'`/`'R'`-
  aware read path, not exposed via the generic `get_object`/`get_objects`
  API surface) must cover both call sites in
  `zcl_abapgit_ortec_pack_stream.clas.abap`, and the
  `resolve_reads_own_d_row` regression test must exercise
  `resolve_streaming` end-to-end (so it also exercises
  `preload_delta_rows`), not call `resolve_one_meta` in isolation. See
  `.memory/logs/variant_b_package_d_concurrent_commit_impact.md`.

### 5.3.1 PERF-B-1 fix (performance DESIGN_GATE, resolved)

- New `zcl_abapgit_ortec_obj_store=>get_staged_delta_objects`: same
  contract, chunking (`c_select_package_size`), and strict
  raise-on-any-missing behavior as the existing `get_objects`, but its
  underlying read filters `WHERE ... status IN ('D', 'R')` instead of
  `status = 'R'`. Documented (ABAP Doc + inline comment) as callable
  **only** by `zcl_abapgit_ortec_pack_stream` to read a pack's own delta
  temp-key rows during that same pack's own resolution — never for a real
  content-SHA1 presence/read, since doing so would violate the §8
  STAGED/READY visibility invariant (a `'D'`-status real object must
  never be returned to a generic caller). This is a new, narrowly-scoped
  method, not a parameter added to `get_object`/`get_objects` — those two
  remain strictly `status = 'R'`-only with no behavior change, preserving
  every existing caller's (and the correctness/protocol reviews')
  verified invariant that a normal read never observes a staged row.
- `resolve_one_meta` (line ~666, this pack's own delta raw-bytes fetch via
  `<ls_row>-temp_key`): switch from `get_object` to
  `get_staged_delta_objects` called with a one-element `it_sha1s` table,
  taking the single returned row. Identical error path (the existing
  `CATCH zcx_abapgit_ortec_git INTO lx_missing` / "Delta temp data
  missing" raise is unchanged, since the new method raises on the exact
  same missing-SHA1 condition `get_object` did).
- `preload_delta_rows` (added by commit `35be4c65`): switch its
  `get_objects(iv_bulk_fetch = abap_false)` call to
  `get_staged_delta_objects` over the same deduplicated `lt_temp_keys`
  set. No other change to that method's logic (dedup, empty-set
  short-circuit, and session-cache warming via the underlying row read
  are all preserved — `get_staged_delta_objects` warms `mt_cache` exactly
  like `get_objects` does, since `resolve_one_meta`'s subsequent
  `get_staged_delta_objects` call for the same temp_key must still be a
  cache hit).
- `read_object_rows` itself is **not modified** (stays `status = 'R'`
  only, used by every other existing caller unchanged); the new method
  implements its own status-parameterized row read, avoiding any change
  to `read_object_rows`'s existing hard-coded-`'R'` contract that every
  other reviewed invariant already depends on.
- **Cache-hit condition (performance DESIGN_GATE iteration 2, PERF-M-2
  fix):** `get_staged_delta_objects`'s session-cache (`mt_cache`) hit-check
  must test `ls_cache_entry-status IN ('D', 'R')`, not `= 'R'` (the
  condition `get_objects`/`get_available_objects` use). Copying the
  `= 'R'`-only check verbatim would make a `'D'`-status entry already
  warmed by `preload_delta_rows` register as a cache miss on
  `resolve_one_meta`'s follow-up call for the same temp key, silently
  reintroducing the exact O(K) per-pack DB read PERF-M-1 exists to
  eliminate. `get_object`/`get_objects`/`get_available_objects` are
  unaffected and keep their existing `= 'R'`-only cache-hit check.

## 6. Base-cache limits, scope, and invalidation (D1, verification)

- Scope: process-global singleton (`CLASS-DATA go_instance`), not attempt-
  scoped. This is intentionally kept: entries are keyed by real content
  SHA1 (`ty_sha1`), so an entry populated by attempt A and later read by
  attempt B (same or different repository) is still correct data — content
  addressing makes cross-attempt reuse safe by construction.
- Limit: `c_budget_bytes = 268435456` (256 MiB) total resident payload
  bytes, enforced in `put()` (oversized single entries are rejected, never
  cached) and by `remove_oldest()` LRU eviction keyed by the `by_seq`
  secondary sorted key (O(1) oldest-entry access).
- Invalidation: none required or added. A failed/rolled-back attempt does
  not need to purge the cache, because (a) cache entries are only ever
  written with already-verified resolved bytes (`resolve_one_meta` puts
  after `apply()` + SHA1 computation succeed, not before), and (b) a stale
  entry can only ever be "more of the same correct content", never
  incorrect content, since the key is the content hash itself.

## 7. D2 state machine (target)

No new states are introduced to `ZAOG_COMMIT_HIST` (`hist_level`,
`snap_state`) — Package C's model is preserved exactly:

```text
hist_level: UNKNOWN -> GRAPH_COMPLETE -> FULL_COMPLETE   (never downgrades)
snap_state: NONE -> PENDING -> COMPLETE                  (or -> INVALID from any state)
invariant : snap_state = COMPLETE requires hist_level = FULL_COMPLETE
```

One new state value is introduced at the **object-row** level only
(§5.3): `ZAOG_OBJ_STORE.status` gains `'D'` (decoded, pending delta
resolution) between existing `'I'` (incomplete/raw) and `'R'` (ready).
`'D'` is write-only-visible to the owning pack's own resolver; it is never
returned by any presence/content read.

## 8. Staged versus READY visibility (D2)

| Table | Staged marker | Visible to normal reads? | Cleanup owner |
| --- | --- | --- | --- |
| `ZAOG_OBJ_STORE` | `status IN ('I','D')` | No (`get_object`/`get_objects`/`get_present_sha1s`/`get_missing_sha1s` all filter `status = 'R'`). **Documented exception:** `get_staged_delta_objects` (§5.3.1) additionally admits `status = 'D'`, but only for a pack's own delta temp-key rows read by `zcl_abapgit_ortec_pack_stream` during that pack's own resolution — never for a real content-SHA1 lookup. | `cleanup_incomplete` (extended, §5.3); the unrelated legacy `cleanup_partial_session` (filters `status = 'P'`) is untouched |
| `ZAOG_COMMIT_HIST` | `attempt_id` populated + `hist_level`/`snap_state` below target | Reads (`is_graph_have_eligible`/`is_full_have_eligible`) key off `hist_level` value directly, not `attempt_id` — an in-progress attempt that has not yet called `mark_graph_complete`/`mark_full_complete` is correctly invisible as have-eligible by construction (no separate staged flag needed) | `clean_incomplete_attempts` (existing, unchanged) |
| `ZAOG_FETCH_SESS` | `status` (session lifecycle) | Session rows are never treated as have-eligibility input; no change needed | `fail_session`/`cleanup_partial_session` (existing, unmodified) |
| `ZAOG_REPO_STATE` | denormalized `snap_state` copy | Only updated by `publish_snapshot_complete`/`update_after_fetch`, both orchestrator-owned | `invalidate_tip_commit`/`invalidate_all_history` (existing) |

## 9. Attempt/session/pack correlation (D2, target)

Current: `attempt_id` (mat_state, UUID) and `pack_id` (`repo_key +
timestamp`) are independent identifiers with no shared column — confirmed
gap (§1.3).

Target: thread the **existing** `attempt_id` (already generated once per
`begin_attempt` call, already the correct top-level correlation identity)
down into the pack/session layer as an additional, non-key correlation
column:

- Each of the two self-contained persist units defined in §11
  (`zcl_abapgit_ortec_fastpath=>pull_by_branch`'s Phase-1b resume-match
  branch; `zcl_abapgit_ortec_porcelain=>pull_by_branch`'s
  `INCREMENTAL_UPDATE` branch — relocated here per Owner Decision A/B-3,
  see the dedicated subsection below; **not**
  `zcl_abapgit_ortec_fastpath=>upload_pack_by_branch`, which owns neither
  unit) calls `begin_attempt` exactly once, immediately before its own
  local decode/resolve/persist sequence begins (not before any preceding
  HTTP round-trip), and passes the resulting `attempt_id` into
  `decode_streaming`/`decode_and_persist_streaming`/`resumable_decode`/
  `persist_pull_result`/`certify_fetched_commit` as a new, optional
  `iv_attempt_id` parameter, and into `persist_missing_objects` (M-5 fix,
  below);
- `decode_and_persist_streaming`/`resumable_decode` store `iv_attempt_id`
  into a new non-key `attempt_id` column on `ZAOG_OBJ_STORE` (staged rows
  only need it for diagnostics/cleanup correlation, not identity — the
  existing `repo_key + obj_sha1` / `repo_key + pack_id + obj_sha1`-shaped
  keys are unchanged);
- `ZAOG_FETCH_SESS`/`ZAOG_PACK_META` gain the same non-key `attempt_id`
  column for the same reason;
- **`persist_missing_objects` (M-5 fix):** this method's direct
  `MODIFY zaog_obj_store FROM TABLE lt_new` (for any tip object not
  already present, reached from `persist_pull_result` before
  `certify_fetched_commit`) gains the same `iv_attempt_id` parameter and
  sets it on every row it builds — closing the one remaining write path
  to `ZAOG_OBJ_STORE` that was outside the threading scope;
- this is additive (one new column per table, backward compatible with
  existing rows where it is blank) and does not change any existing
  primary/secondary key or read predicate.
- **Diagnostic value, corrected scope (M-4 fix):** given an `attempt_id`,
  an operator can join `ZAOG_COMMIT_HIST` with `ZAOG_OBJ_STORE` for
  **every** attempt (both persist units always write both tables). The
  same join additionally includes `ZAOG_FETCH_SESS`/`ZAOG_PACK_META` only
  when the attempt went through the legacy resumable-session path
  (`zcl_abapgit_ortec_pack_raw=>create_session`/
  `update_session_progress`, `zcl_abapgit_ortec_pack_dec=>resumable_decode`)
  — confirmed by source that the default/live streaming decoder
  (`zcl_abapgit_ortec_pack_stream`) deliberately never writes
  `zaog_pack_meta`/`zaog_fetch_sess` at all. This is a correctly bounded,
  non-blocking scope note, not a defect: the 4-table join is a superset
  view available when the legacy path is exercised, and a 2-table
  (`ZAOG_COMMIT_HIST`/`ZAOG_OBJ_STORE`) join is always available
  regardless of which decode path served the attempt.
- **Single attempt_id per real attempt (correctness review DR-004,
  resolved here):** `zcl_abapgit_ortec_mat_state=>begin_attempt` always
  mints a brand-new UUID and unconditionally overwrites
  `ZAOG_COMMIT_HIST.attempt_id` on every call — it does not reuse an
  in-progress attempt_id for the same `repo_key`/`commit_sha1`. Because
  `zcl_abapgit_ortec_fastpath=>certify_fetched_commit` (unmodified by
  Package C) also calls `begin_attempt` internally, calling `begin_attempt`
  a second time early (per this section) would silently produce two
  different UUIDs for one real attempt — one tagging the `ZAOG_OBJ_STORE`/
  `ZAOG_FETCH_SESS`/`ZAOG_PACK_META` rows, a different one persisted to
  `ZAOG_COMMIT_HIST` — defeating the cross-table join this section exists
  for. Fix: `certify_fetched_commit` gains a new `IMPORTING iv_attempt_id`
  parameter and stops calling `begin_attempt` itself; its only caller,
  `persist_pull_result`, passes the SAME `attempt_id` obtained from the
  single early `begin_attempt` call (§11) through to it. `begin_attempt`
  itself is not changed (still always mints a fresh id) — it is simply
  called exactly once per real attempt, at the top of whichever
  self-contained persist unit (§11) is executing that attempt, rather
  than once there and once again inside `certify_fetched_commit`.

**Owner-confirmed binding decision (2026-07-24, supersedes any ambiguity in
the paragraph above):**

1. One real top-level fetch attempt produces exactly one `attempt_id`.
2. `begin_attempt` stays non-idempotent — it still mints a genuinely new
   UUID for every real new attempt (a retry/recovery network attempt is a
   new `begin_attempt` call and gets its own new `attempt_id`; this is
   distinct from calling `begin_attempt` twice *within* the same attempt,
   which is what DR-004 forbids).
3. `begin_attempt` must not be called a second time within the same
   attempt.
4. The early-generated `attempt_id` is explicitly threaded by the
   orchestration through to `persist_pull_result` and
   `certify_fetched_commit`.
5. `certify_fetched_commit` uses the passed-in `attempt_id` and does not
   call `begin_attempt` itself.
6. `ZAOG_OBJ_STORE`, `ZAOG_FETCH_SESS`, `ZAOG_PACK_META`, and
   `ZAOG_COMMIT_HIST` all receive the same `attempt_id` for the same real
   attempt.
7. Stale or missing `attempt_id`s continue to be rejected at publication
   time (`mark_graph_complete`/`mark_full_complete`/
   `publish_snapshot_complete`'s existing exact-match `attempt_id` checks
   are unchanged — no relaxation of stale-attempt rejection).

This is new schema (one nullable column per table); it is additive-only
and does not require migrating existing rows (see §14).

**Owner Decision A — B-3 resolution (2026-07-24): Publication Unit #2
relocated to its real production call site.** The protocol/persistence
review's second iteration (B-3) proved, against live source, that
`zcl_abapgit_ortec_fastpath=>upload_pack_by_branch` never calls
`persist_pull_result`/`certify_fetched_commit` at all — it only runs the
thin/self-contained/recovery HTTP cascade and
`zcl_abapgit_ortec_pack_stream=>decode_streaming` (object persistence
only, matching §10's ownership table). The real, live call for a normal
fresh-fetch ("pull with new commits") is in
`zcl_abapgit_ortec_porcelain.clas.abap` (line ~370), inside
`zcl_abapgit_ortec_porcelain=>pull_by_branch`'s `INCREMENTAL_UPDATE`
branch, whose real sequence (confirmed by direct source read) is: call
`zcl_abapgit_git_transport=>upload_pack_by_branch` (delegates to
`zcl_abapgit_ortec_fastpath=>upload_pack_by_branch` for the HTTP cascade +
streaming decode) → call `pull(...)` (standard tree-walk/file
materialization) → call `persist_pull_result` (already wrapped in its own
`TRY ... CATCH zcx_abapgit_ortec_git` — "persistence failure is
non-critical, continue normally").

Per Michael's binding decision (2026-07-24), Publication Unit #2 is
relocated as follows:

1. **Ownership:** `zcl_abapgit_ortec_porcelain=>pull_by_branch`'s
   `INCREMENTAL_UPDATE` branch owns Publication Unit #2 in full:
   `BEGIN_ATTEMPT` → `persist_missing_objects` (via `persist_pull_result`)
   → `PERSIST_PULL_RESULT` → `CERTIFY_FETCHED_COMMIT` (same `attempt_id`)
   → `update_after_fetch` → the existing single `COMMIT WORK` inside
   `persist_pull_result` (orchestrator-owned, unchanged). This sequence
   already runs entirely inside `persist_pull_result` today (it already
   calls `persist_missing_objects` then `certify_fetched_commit` then
   `COMMIT WORK`) — Unit #2's lock/attempt boundary therefore wraps
   exactly the existing `persist_pull_result` call at
   `zcl_abapgit_ortec_porcelain.clas.abap:370`, mirroring Unit #1's shape
   in `zcl_abapgit_ortec_fastpath=>pull_by_branch`.
2. **`zcl_abapgit_ortec_fastpath=>upload_pack_by_branch` is confirmed to
   own neither `attempt_id` minting nor any lock for this unit** (Michael
   point 5) — no change of any kind is made to that method; it remains
   exactly the HTTP-cascade + `decode_streaming` object-persistence step
   §10 already documents.
3. **Lock scope:** `zcl_abapgit_ortec_porcelain=>pull_by_branch` acquires
   `zcl_abapgit_ortec_pack_dec=>acquire_repo_lock` and calls
   `zcl_abapgit_ortec_mat_state=>begin_attempt` immediately before its
   existing `persist_pull_result` call — i.e. strictly after `pull(...)`
   and the entire HTTP cascade have already completed, never spanning
   unbounded HTTP wait time (Michael point 4). `release_repo_lock` is
   called unconditionally after the existing `TRY ... CATCH
   zcx_abapgit_ortec_git` block around `persist_pull_result` (in both the
   success and caught-exception paths), so a `persist_pull_result`
   failure cannot leak the lock (exception-safe release, no premature
   release before `certify_fetched_commit`/promotion, per Michael point
   4). `acquire_repo_lock`'s own `zcx_abapgit_exception` (lock-acquisition
   timeout) is caught locally at this new call site and treated as a
   non-critical skip-this-round condition (same M-3 fallback pattern as
   Unit #1) — the surrounding `pull(...)`/file-materialization result is
   still returned to the caller either way, since persistence has always
   been best-effort here (existing `CATCH zcx_abapgit_ortec_git` comment:
   "persistence failure is non-critical, continue normally").
4. **`persist_pull_result`/`certify_fetched_commit` signature:**
   `persist_pull_result` gains the same optional `IMPORTING iv_attempt_id`
   parameter already planned for Unit #1 and forwards it to
   `certify_fetched_commit` (which stops calling `begin_attempt` itself —
   DR-004 fix, unchanged). When `iv_attempt_id` is supplied (both
   production callers — `zcl_abapgit_ortec_fastpath=>pull_by_branch`'s
   Phase-1b and `zcl_abapgit_ortec_porcelain=>pull_by_branch`'s
   `INCREMENTAL_UPDATE` branch — always supply it), no internal
   `begin_attempt` call happens inside `persist_pull_result` at all. This
   resolves the two other pre-existing external callers of
   `persist_pull_result` the review flagged
   (`zcl_abapgit_git_porcelain.clas.abap:650`, a switch-inactive branch
   that is unreachable at runtime because `persist_pull_result`'s own
   `IF is_active_for_repo(...) = abap_false. RETURN.` guard fires first
   regardless of `attempt_id`; and
   `zcl_abapgit_ortec_git_tests.clas.testclasses.abap:680`'s
   `persist_creates_state` unit test, a controlled single-threaded test
   context with no concurrent-access risk): both are left calling
   `persist_pull_result` **without** `iv_attempt_id`, in which case
   `persist_pull_result` mints exactly one `begin_attempt` itself, at its
   own top, and threads that single id to `persist_missing_objects`/
   `certify_fetched_commit` internally — preserving the "exactly one
   `begin_attempt` call per real attempt" invariant (Michael point 3)
   regardless of which caller reaches it, and requiring **no signature
   change or behavior change** at either of those two pre-existing call
   sites. Neither compile-breaking nor silently-unlocked regression (the
   two failure modes B-3 originally warned about) is possible under this
   shape.
5. **Resume-match path classification (Michael point 6, resolved against
   current source — no second publication unit invented):**
   `zcl_abapgit_ortec_pack_dec=>resume_decode` (called from Unit #1,
   `zcl_abapgit_ortec_fastpath=>pull_by_branch`'s Phase-1b, **before**
   Unit #1's own lock/`begin_attempt` window opens) was read directly
   (`zcl_abapgit_ortec_pack_dec.clas.abap:482-543`): it acquires/releases
   its own existing, independent lock cycle, calls `resumable_decode`,
   `complete_pack`/`complete_session`, deletes the raw pack, and commits
   — it has **zero interaction with `zcl_abapgit_ortec_mat_state`,
   `attempt_id`, or `ZAOG_COMMIT_HIST` of any kind**. Separately,
   `zcl_abapgit_ortec_mat_state=>begin_attempt`
   (`zcl_abapgit_ortec_mat_state.clas.abap:224-260`) was re-read and
   reconfirmed to have **no read-back/reuse path at all** — it always
   mints a fresh UUID and unconditionally overwrites
   `ZAOG_COMMIT_HIST.attempt_id` on every call, for every caller. Given
   this, none of Michael's three classification branches for a
   "continuation" apply: `resume_decode` does not continue any existing
   *attempt*-scoped work (no attempt_id exists yet at that point — only
   pack/session-scoped state, which is a separate, already-correct
   concept, §9's `pack_id`/`session_id` correlation columns), it performs
   no new *network* fetch itself (it replays an already-locally-stored
   raw pack from an earlier HTTP call), and it performs no certification
   or promotion itself. **Conclusion: the resume-match path requires no
   new/second publication unit and no attempt-id-reuse mechanism — it is,
   and remains, exactly Unit #1 as already designed** (`resume_decode`
   completes and fully releases its own unrelated pack/session lock
   first; Unit #1's lock/`begin_attempt`/`persist_pull_result`/
   `certify_fetched_commit` sequence then runs afterward, always minting
   a genuinely fresh `attempt_id` — there is no scenario in current
   source where an existing `attempt_id` could be reused across a resume,
   because none is ever created before Unit #1's own call). This is
   reflected in §16's test list: `resume_new_attempt_when_new` is
   retained (asserts a fresh `attempt_id` is minted every time Unit #1
   runs after a successful resume, which is unconditionally true today);
   `resume_reuses_attempt` is retained as an explicit `NOT_APPLICABLE`
   placeholder with this evidence recorded, not fabricated as a
   currently-testable behavior — introducing genuine resume-scoped
   attempt persistence would be a distinct, out-of-scope future design
   decision, not part of closing B-3.

## 10. Complete transaction and commit-ownership map (D2)

| Site | Statement | Owns durability of |
| --- | --- | --- |
| `decode_and_persist_streaming` | `COMMIT WORK` (success path) | Non-delta `'R'` rows + delta `'D'` rows (post §5.3 fix) for one pack |
| `decode_and_persist_streaming` (catch blocks) | `COMMIT WORK` after `cleanup_incomplete` | The cleanup delete itself |
| `resolve_streaming` | `COMMIT WORK` (end of method, success only) | Final `flush_resolve_batch` (resolved `'R'` rows + temp-key deletes) |
| `decode_streaming` (catch) | `ROLLBACK WORK` | Uncommitted `resolve_streaming` work only (does not, and cannot, undo the earlier `decode_and_persist_streaming` commit — this is why §5.3's status-split fix is required) |
| `resumable_decode` | commit interval batching (`lv_commit_interval`, default 50) via `MODIFY zaog_obj_store FROM TABLE` | Legacy/resumable decode path; unchanged by D1/D2 |
| `zcl_abapgit_ortec_fastpath=>persist_pull_result` | `COMMIT WORK` (single, end of method) | `persist_missing_objects` + `certify_fetched_commit` (mat_state writes) + `update_after_fetch`, called from Unit #1 (`zcl_abapgit_ortec_fastpath=>pull_by_branch`'s Phase-1b branch) and Unit #2 (`zcl_abapgit_ortec_porcelain=>pull_by_branch`'s `INCREMENTAL_UPDATE` branch — relocated here per Owner Decision A/B-3, §9) |
| `zcl_abapgit_ortec_mat_state=>*` | none | Never commits; always relies on a caller |
| `zcl_abapgit_ortec_cache_admin=>clear_repo` | `COMMIT WORK AND WAIT` / rollback on failure | Full repo-scoped cache clear, under `acquire_lock` |

D2 adds no new commit sites; it only (a) extends the existing cleanup
predicate (§5.3) so cleanup after a `ROLLBACK WORK` also removes rows the
rollback itself cannot reach, and (b) requires that any NEW D1/D2 code
follows the same rule already implicit above: **no `COMMIT WORK`/
`ROLLBACK WORK` inside `zcl_abapgit_ortec_delta`, `zcl_abapgit_ortec_mat_state`,
or any bulk-resolve helper** — only the existing pack-stream/pack-dec/
fastpath orchestration layers may commit.

## 11. Crash, restart, stale-attempt, and concurrency behavior (D2)

- **Crash before publication:** already safe for the `ZAOG_COMMIT_HIST`/
  `ZAOG_REPO_STATE` layer (no commit until `persist_pull_result`'s single
  `COMMIT WORK`). Becomes safe for `ZAOG_OBJ_STORE` too once §5.3 lands
  (a crash between the `decode_and_persist_streaming` commit and
  `resolve_streaming`'s commit leaves only `'D'`-status rows, which
  `cleanup_incomplete`'s extended predicate can now remove on the next
  attempt).
- **Interrupted retry:** `begin_attempt` is already idempotent/resumable
  (creates if absent, upgrades `snap_state` to `PENDING` only from
  `NONE`/`INVALID`, never downgrades `hist_level`). No change required.
- **Stale-attempt rejection:** already enforced by every `mat_state`
  mutator via exact `attempt_id` comparison. No change required.
- **Same-repository concurrency — revised per protocol/persistence review
  (findings B-1, B-2, M-1, M-2, M-3, all resolved here):**

  **Canonical lock primitive (DR-001, unchanged from the correctness-review
  fix):** the `zcl_abapgit_ortec_pack_dec` SAP-enqueue-based
  `acquire_repo_lock`/`release_repo_lock` (`ENQUEUE_EZAOG_REPO_LOCK`,
  `_scope = '2'`, survives an internal `COMMIT WORK`, auto-released on
  process death) remains canonical. The unrelated `zcl_abapgit_ortec_
  pack_raw` DB-row mutex (used only by `zcl_abapgit_ortec_obj_index`)
  is untouched, as before.

  **Visibility fix (B-2):** `acquire_repo_lock`/`release_repo_lock` on
  `zcl_abapgit_ortec_pack_dec` are changed from `PRIVATE SECTION` to
  `PUBLIC SECTION` (mechanical visibility change only, no behavior
  change) so `zcl_abapgit_ortec_fastpath` can call them directly.

  **Lock/attempt scope narrowed to the local persist unit, not the whole
  orchestration (B-1, M-1, M-2 fix):** the original plan to wrap the
  entire `upload_pack_by_branch` method body (including its up-to-3
  sequential HTTP round-trips) in one lock hold is withdrawn — it does
  not actually match Michael's binding decision (§9, "one real top-level
  attempt = one attempt_id"; nothing there requires the lock to span
  network I/O), and it created two real defects: `pull_by_branch` has two
  independent callers (`zcl_abapgit_git_porcelain=>pull_by_branch`
  directly, and `zcl_abapgit_ortec_fastpath=>upload_pack_by_branch`
  internally as its first step) so a single wrap point around
  `upload_pack_by_branch` alone misses the direct-porcelain call path
  entirely (B-1); and extending an existing ~10-15s-timeout-budget lock
  from "brief" to "spans a potentially multi-minute large-pack fetch"
  would make a second, legitimate concurrent lock consumer —
  `zcl_abapgit_ortec_fastpath=>try_filtered_commit_fetch`'s own call to
  `zcl_abapgit_ortec_pack_dec=>decode_and_persist`, a live Stage/Diff
  filtered-fetch path that already, independently, acquires/releases this
  same named lock today — newly likely to hit a hard timeout during
  ordinary concurrent use (M-1/M-2).

  **Revised design:** the lock (and the `begin_attempt` call that produces
  the one `attempt_id` for that same unit of work) is acquired
  immediately before, and released immediately after, exactly the
  **local decode/resolve/persist/certify/commit** sequence for one real
  attempt — never across a preceding HTTP round-trip. This is the same
  hold-duration shape `resume_decode`/`decode_and_persist` already use
  today, merely extended to also cover `certify_fetched_commit` and the
  final `COMMIT WORK` (which today run unlocked). Two self-contained
  units are locked/attempted independently, resolving B-1 without any
  nested-acquisition risk (each acquires and fully releases before the
  next, if any, begins — never nested):
  1. `zcl_abapgit_ortec_fastpath=>pull_by_branch`'s own Phase-1b branch
     ("resumed decode matches remote tip"): acquire the lock and call
     `begin_attempt` immediately before invoking `resume_decode` (with the
     existing `resume_decode` internal acquire/release suppressed via the
     `iv_lock_held = abap_true` parameter described below, since the
     outer `pull_by_branch` call now already holds it), then call
     `persist_pull_result`/`certify_fetched_commit` with the resulting
     `attempt_id`, then release the lock. This fix is caller-agnostic by
     construction: wrapping `pull_by_branch`'s own Phase-1b branch covers
     whichever of its two callers can actually reach that branch. Of the
     two textual call sites of `zcl_abapgit_ortec_fastpath=>pull_by_branch`
     (`zcl_abapgit_git_porcelain.clas.abap:542` and
     `zcl_abapgit_ortec_fastpath.clas.abap:841`, inside
     `upload_pack_by_branch`), only the latter can reach Phase-1b in
     practice today — the porcelain-direct call site is structurally dead
     code for this concern, because `pull_by_branch`'s own first statement
     (`IF is_active_for_repo(...) = abap_false. RETURN. ENDIF.`) is
     guaranteed to fire whenever reached from that call site (m-2,
     protocol review iteration 2 — corrected here; does not change the
     fix itself, only this rationale text so a future reader does not
     conclude the porcelain-direct call site is a live second path for
     this concern).
  2. **Relocated per Owner Decision A / B-3 (2026-07-24; see §9's
     dedicated subsection for the full evidence and rationale) —**
     `zcl_abapgit_ortec_porcelain=>pull_by_branch`'s own
     `INCREMENTAL_UPDATE` branch, immediately before its existing
     `persist_pull_result` call: reached only after the entire
     `zcl_abapgit_git_transport=>upload_pack_by_branch` HTTP cascade
     (which itself, when the switch is active, delegates to
     `zcl_abapgit_ortec_fastpath=>upload_pack_by_branch` for the
     thin/self-contained/recovery tiers and `decode_streaming`'s object
     persistence) and the subsequent `pull(...)` tree-walk have both
     already completed. `zcl_abapgit_ortec_porcelain=>pull_by_branch`
     acquires its own, separate lock + calls `begin_attempt` for this
     second, independent real attempt immediately before its own
     `persist_pull_result` call (which internally runs
     `persist_missing_objects`/`certify_fetched_commit`/
     `update_after_fetch`/`COMMIT WORK`, unchanged), then releases the
     lock unconditionally afterward (exception-safe — see §9). This is
     never nested inside #1's lock (a different call chain entirely;
     `zcl_abapgit_ortec_fastpath=>upload_pack_by_branch` itself acquires
     no lock and mints no `attempt_id` — confirmed not the owner of this
     unit, Michael point 5) and never spans the preceding HTTP
     round-trips.
  `resume_decode` gains the new optional `IMPORTING iv_lock_held TYPE
  abap_bool DEFAULT abap_false` parameter; when `abap_true` (set by
  `pull_by_branch` per #1 above), it skips its own internal
  `acquire_repo_lock`/`release_repo_lock` calls (the caller already holds
  the lock for the full duration of this call). Standalone callers of
  `resume_decode` (none currently exist outside `pull_by_branch`, but the
  default preserves today's behavior) keep the existing internal
  acquire/release. `decode_and_persist`/`try_filtered_commit_fetch` are
  **not modified** — their existing brief, independent lock usage remains
  exactly as today, and is no longer at elevated timeout risk since no
  other caller now holds this lock across a long span (M-1/M-2 resolved
  by narrowing scope rather than by adding a new timeout policy).

  **Exception-handling fix (M-3):** at both of the two new call sites
  above (`zcl_abapgit_ortec_fastpath=>pull_by_branch`'s Phase-1b branch
  and `zcl_abapgit_ortec_porcelain=>pull_by_branch`'s `INCREMENTAL_UPDATE`
  branch), `acquire_repo_lock`'s `zcx_abapgit_exception` (a
  lock-acquisition timeout — a purely infrastructural condition,
  unrelated to pack content) is caught locally and treated exactly like
  the existing "ORTEC fastpath not applicable, fall back to the standard
  fetch" case (the same graceful-degrade pattern
  `zcl_abapgit_git_porcelain=>pull_by_branch`'s own `CATCH
  zcx_abapgit_ortec_git zcx_abapgit_exception` wrapper already applies one
  level up) — never surfaced as a hard failure to the end user.
- **Cross-repository concurrency:** already safe by construction — every
  `mat_state`/`obj_store`/`repo_state` operation is keyed by an explicit,
  non-blank `repo_key` (Package C invariant, preserved), so two attempts
  for different repositories never share a lock or a row.
- **Idempotent retry:** re-running a failed attempt calls `begin_attempt`
  again for the same `repo_key`/`commit_sha1`, which reuses/updates the
  same `ZAOG_COMMIT_HIST` row and issues a fresh `attempt_id`; any
  `'D'`/`'I'`-status leftovers from the failed attempt are cleaned by
  `cleanup_incomplete` (scoped by the failed attempt's own `pack_id`,
  unaffected by the new attempt's different `pack_id`).

## 12. Set-based cleanup (D2)

- `zcl_abapgit_ortec_mat_state=>clean_incomplete_attempts`: unchanged,
  already one bulk `UPDATE ... SET attempt_id = @space WHERE repo_key = ...
  AND attempt_id IS NOT INITIAL AND updated_at < ...`.
- `cleanup_incomplete`: extended per §5.3 to a
  single `DELETE ... WHERE repo_key = ... AND pack_id = ... AND status IN
  ('I','D')` — still one statement, still pack-scoped, never repository-
  wide. `cleanup_partial_session` is intentionally unmodified (§5.3
  correction) — it filters the unrelated legacy `status = 'P'` convention.

## 13. Protocol and fallback behavior

No wire-protocol changes. D1/D2 are entirely persistence/algorithm-side;
they do not alter fetch-mode request serialization (Package C/earlier
slices already own `INITIAL_BRANCH_BLOBLESS`/`INCREMENTAL_THIN`/etc.). The
one existing protocol-adjacent signal D1 preserves unchanged is
`iv_retry_without_haves = abap_true` on the delta-base-not-found exception,
which the fastpath cascade (out of scope) already consumes to decide
between a thin retry, a self-contained retry, and bounded `RECOVERY_BRANCH_FULL`.

## 14. Migration and backward compatibility

- `ZAOG_OBJ_STORE.status = 'D'` is a new permitted value for an existing
  single-character field; no DDIC length/type change. Existing rows
  (all currently `'I'` or `'R'`) are unaffected; no data migration needed.
- New `attempt_id` columns on `ZAOG_OBJ_STORE`/`ZAOG_FETCH_SESS`/
  `ZAOG_PACK_META` are additive, nullable/blank-default, non-key. Existing
  rows keep a blank `attempt_id`; no backfill required (backfilling would
  require guessing a historical correlation that was never recorded — do
  not attempt it, per the standing "no old data interpreted via inference"
  discipline already used for `ZAOG_COMMIT_HIST` certification history).
- Standard abapGit and ORTEC-disabled behavior are untouched — every
  changed method lives in `zcl_abapgit_ortec_*`/`zaog_*`.

## 15. Exact productive file and symbol scope

D1:

- `src/ortec/git/zcl_abapgit_ortec_delta.clas.abap` — add
  `bulk_resolve_external_bases` (new **PUBLIC** `CLASS-METHODS` — legally
  callable by both resolver paths, including cross-class from
  `zcl_abapgit_ortec_pack_stream`; must not be `PRIVATE SECTION` or
  described as class-internal). It owns only the bounded set-based load
  and validation and returns the existing public `ty_object`-shaped
  `rt_objects` row type; it performs no merge or auxiliary-index work
  itself (see §4 rationale). Called from a new "Phase 1.5" step inserted
  into `resolve_all`.
- `src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap` — `resumable_decode`:
  replace the manual `SELECT ... FOR ALL ENTRIES` external-base block with
  a call to the shared helper (behavior-preserving refactor).
- `src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.abap` — `resolve_streaming`:
  insert the new Phase 1.5 bulk-collect-and-merge step before its existing
  final pass; `resolve_one_meta`'s external-base branch no longer calls
  `get_base_bytes`'s DB-fetch path directly (only its cache path remains
  reachable there, since Phase 1.5 has already merged any real external
  base into `ct_meta` beforehand).

D2:

- `src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.abap` —
  `decode_and_persist_streaming`: split the blanket promotion `UPDATE`
  into two predicate-scoped updates (non-delta rows → `'R'`, delta/temp-key
  rows → `'D'`); `cleanup_incomplete`: extend `status = 'I'` to
  `status IN ('I','D')`; `resolve_one_meta` and `preload_delta_rows`
  (production, commit `35be4c65`): switch both temp-key reads from
  `get_object`/`get_objects` to the new `get_staged_delta_objects`
  (PERF-B-1 fix, §5.3.1).
- `src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap` — add
  `get_staged_delta_objects` (new, §5.3.1); no change to `get_object`,
  `get_objects`, `get_available_objects`, or `read_object_rows`.
- `src/ortec/git/zaog_obj_store.tabl.xml`,
  `src/ortec/git/zaog_fetch_sess.tabl.xml`,
  `src/ortec/git/zaog_pack_meta.tabl.xml` — add non-key `ATTEMPT_ID`
  field (`zcl_abapgit_ortec_mat_state=>ty_attempt_id`, `c length 32`).
- `src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap` — `pull_by_branch`:
  in its Phase-1b ("resumed decode matches remote tip") branch, call
  `zcl_abapgit_ortec_pack_dec=>acquire_repo_lock` immediately before, and
  `release_repo_lock` immediately after, that branch's
  `resume_decode`/`persist_pull_result`/`certify_fetched_commit`
  sequence; call `begin_attempt` once inside that same locked window and
  thread the resulting `attempt_id` through `resume_decode` (new
  `iv_attempt_id` parameter), `persist_pull_result`, and
  `certify_fetched_commit` (new `iv_attempt_id` parameter, replacing its
  own internal `begin_attempt` call — DR-004 fix); call `resume_decode`
  with the new `iv_lock_held = abap_true` (§11) so it does not re-acquire
  the lock it is already holding. Catch `acquire_repo_lock`'s
  `zcx_abapgit_exception` locally and fall back exactly like today's
  "ORTEC fastpath not applicable" path (M-3 fix) — never surface it as a
  hard failure. `upload_pack_by_branch` is explicitly **not modified** by
  D2 — confirmed (Owner Decision A / B-3, §9) to own neither
  `attempt_id` minting nor any lock; it remains exactly the HTTP-cascade
  + `decode_streaming` object-persistence step it is today.
  `persist_pull_result` gains a new optional `IMPORTING iv_attempt_id`
  parameter and forwards it to `certify_fetched_commit`; when not
  supplied, `persist_pull_result` calls `begin_attempt` itself exactly
  once and uses that id internally (covers the two pre-existing external
  callers below without requiring any change to them).
  `certify_fetched_commit` gains the same new `IMPORTING iv_attempt_id`
  parameter and stops calling `begin_attempt` itself (DR-004 fix).
  `persist_missing_objects` (called from `persist_pull_result`) gains a
  new `iv_attempt_id` parameter and sets it on every `ZAOG_OBJ_STORE` row
  it builds (M-5 fix).
- `src/ortec/git/zcl_abapgit_ortec_porcelain.clas.abap` — **new file added
  to D2 scope per Owner Decision A / B-3 (§9).** `pull_by_branch`'s
  `INCREMENTAL_UPDATE` branch: immediately before its existing
  `persist_pull_result` call (line ~370, already wrapped in `TRY ... CATCH
  zcx_abapgit_ortec_git`), call `zcl_abapgit_ortec_pack_dec=>
  acquire_repo_lock` and `zcl_abapgit_ortec_mat_state=>begin_attempt` for
  this second, independent real attempt (Publication Unit #2); pass the
  resulting `attempt_id` into `persist_pull_result`; call
  `release_repo_lock` unconditionally after the existing `TRY...CATCH`
  block (exception-safe, covers both the success and caught-exception
  paths). No change to `pull_by_commit` (confirmed, by direct source
  read, to never call `persist_pull_result` at all — a separate,
  pre-existing gap outside Package D's scope).
- `src/git/zcl_abapgit_git_porcelain.clas.abap` — **no change.** Its
  switch-inactive-fallback call to `persist_pull_result` (line ~650) is
  confirmed unreachable at runtime (guarded by `persist_pull_result`'s
  own `is_active_for_repo` check) and continues to call it without
  `iv_attempt_id`, relying on that method's internal fallback mint (see
  above). Must still compile unchanged — verified, since `iv_attempt_id`
  is optional.
- `src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap` — **no
  change to the existing `persist_creates_state` test's call site** (line
  ~680, calls `persist_pull_result` without `iv_attempt_id`, relying on
  the same internal fallback mint); new tests per §16 are added
  alongside it.
- `src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap` —
  `acquire_repo_lock`/`release_repo_lock`: change from `PRIVATE SECTION`
  to `PUBLIC SECTION` (mechanical visibility change only — B-2 fix) so
  `zcl_abapgit_ortec_fastpath` can call them directly; `resume_decode`:
  add `iv_lock_held` and `iv_attempt_id` parameters (§11, propagated from
  `fastpath`'s `pull_by_branch`); when `iv_lock_held = abap_true`, skip
  the method's own internal `acquire_repo_lock`/`release_repo_lock`
  calls. `decode_and_persist` is explicitly **not modified** — its
  existing brief, independent lock usage (reached via
  `try_filtered_commit_fetch`) is unaffected by this design (M-1
  resolved by narrowing the new locks' scope, not by changing this
  method).
- `src/ortec/git/zcl_abapgit_ortec_pack_raw.clas.abap` —
  `create_session`/`update_session_progress` accept and persist
  `iv_attempt_id`. (`cleanup_partial_session` is intentionally NOT
  modified — see §1.3 correction below: it filters `status = 'P'`, the
  unrelated legacy resumable-decode convention, not `'I'`/`'D'`.)

Package E (explicitly excluded, §20) still owns eventual physical removal
of `complete_missing_base`'s dead body and any now-unreachable legacy
force-full deepen constants; D1/D2 do not touch those.

## 16. ABAP Unit plan (method names ≤ 30 characters)

All names below are pre-counted (max observed: 26 characters).

D1 test-placement rule: all new D1 tests are **class-local** — added to
`zcl_abapgit_ortec_delta.clas.testclasses.abap` (new or extended local
test include) and, where a test needs to exercise the streaming adapter
side, to `zcl_abapgit_ortec_pack_stream.clas.testclasses.abap`. D1 tests
must **not** be added to
`zcl_abapgit_ortec_git_tests.clas.testclasses.abap` — that file's existing
legacy tests (§1.2) remain unchanged and must continue to pass unmodified;
it gains no new D1 content. Use the validated LOCAL FRIENDS/testclasses/
XML pattern (private-attribute/method access via `CLASS ltcl_test
DEFINITION FOR TESTING ... . CLASS zcl_abapgit_ortec_delta DEFINITION
LOCAL FRIENDS ltcl_test.`-style declarations in the class's own
`.testclasses.abap` include) where a test needs access to a private
helper or attribute:

- `bulk_base_dedups_sha1s` (22)
- `bulk_base_one_call_only` (23) — asserts exactly one `get_objects` call
  for N external bases in one pack (via a test double/counter on the
  object-store call, not a real DB count)
- `bulk_base_unique_index` (22) — reuses `two_thin_bases_do_not_collide`'s
  pattern but through the new bulk path
- `bulk_base_missing_raises` (24)
- `bulk_base_wrong_type_raises` (27)
- `bulk_base_corrupt_hash_raises` (29)
- `mixed_ref_ofs_chain_ok` (22)
- `shared_base_two_deltas` (22)
- `duplicate_declared_sha_ok` (25)
- `base_later_in_pack_order` (24) — extends `chain_onto_later_unresolved`
  coverage to the new Phase 1.5 path
- `partial_recovery_resumes` (24)
- `exhausted_recovery_raises` (25)
- `no_sql_in_pack_phase` (20) — asserts the in-pack fixpoint phase issues
  zero object-store/DB calls (test double asserts zero invocations)

D2 (`zcl_abapgit_ortec_pack_stream.clas.testclasses.abap` and
`zcl_abapgit_ortec_pack_raw.clas.testclasses.abap`):

- `delta_temp_row_status_d` (24)
- `temp_row_hidden_from_get` (24)
- `cleanup_removes_d_status` (24)
- `resolve_reads_own_d_row` (23) — PERF-B-1 regression: runs
  `resolve_streaming` end-to-end (not `resolve_one_meta` in isolation, so
  `preload_delta_rows` is on the call path) against a pack whose delta
  temp-key rows are `status = 'D'` and asserts resolution succeeds via
  `get_staged_delta_objects`; a companion assertion confirms
  `get_object`/`get_objects` still raise/omit for the same `'D'`-status
  temp keys (§8 invariant unaffected)
- `staged_cache_hit_no_sql` (23) — PERF-M-2 regression: after
  `preload_delta_rows` warms `mt_cache` for a pack's `'D'`-status temp
  keys, asserts `resolve_one_meta`'s follow-up `get_staged_delta_objects`
  call for the same temp key is a cache hit (test double/spy asserts zero
  additional DB reads), catching a regression to an `= 'R'`-only
  cache-hit condition
- `crash_before_resolve_ok` (23) — simulate: run
  `decode_and_persist_streaming`, do NOT call `resolve_streaming`, assert
  `cleanup_incomplete` still removes all rows for that `pack_id`
- `attempt_id_on_obj_store` (23)
- `attempt_id_on_fetch_sess` (24)
- `stale_attempt_rejected` (22) — extends existing `mat_state` coverage
  through the new attempt-id-threaded call path
- `one_attempt_one_id` (18) — one simulated top-level fetch attempt
  (single `begin_attempt` call) asserts the SAME `attempt_id` value ends
  up on `ZAOG_OBJ_STORE`, `ZAOG_FETCH_SESS`, `ZAOG_PACK_META`, and
  `ZAOG_COMMIT_HIST` rows for that attempt (direct DR-004 regression)
- `retry_gets_new_attempt` (22) — a second, independent top-level fetch
  attempt for the same `repo_key`/`commit_sha1` (simulating a real
  retry/recovery network attempt, not a re-entrant call within one
  attempt) asserts `begin_attempt` mints a genuinely different
  `attempt_id` than the first attempt's, and that `ZAOG_COMMIT_HIST`
  reflects the newer one
- `certify_reuses_attempt` (22) — asserts `certify_fetched_commit` never
  calls `begin_attempt` itself and instead uses exactly the `iv_attempt_id`
  value passed in by its caller (test double/spy on `begin_attempt`
  asserting zero invocations from within `certify_fetched_commit`)
- `attempt_id_cross_table` (22) — end-to-end: run the full
  `persist_pull_result` orchestration once and assert a single SQL join
  across `ZAOG_OBJ_STORE`/`ZAOG_FETCH_SESS`/`ZAOG_PACK_META`/
  `ZAOG_COMMIT_HIST` on `attempt_id` returns a consistent, non-empty,
  single-attempt result set
- `same_repo_lock_serializes` (25)
- `cross_repo_no_lock_share` (24)
- `retry_reuses_attempt_row` (24) — distinct from `retry_gets_new_attempt`:
  asserts the retry updates the SAME `ZAOG_COMMIT_HIST` row (keyed by
  `repo_key`/`commit_sha1`), not a new row, even though its `attempt_id`
  differs from the prior attempt's
- `porcelain_path_gets_lock` (24) — protocol review B-1 regression:
  invokes `zcl_abapgit_git_porcelain=>pull_by_branch`'s direct call to
  `zcl_abapgit_ortec_fastpath=>pull_by_branch` (never through
  `upload_pack_by_branch`) and asserts the resulting `attempt_id`/lock
  cycle still runs correctly for this call chain (Unit #1)
- `lock_not_held_over_http` (23) — protocol review M-2 regression:
  asserts (via a timing/call-order spy) that no lock is held across
  either unit's preceding HTTP round-trips — Unit #1's `branches()` call,
  or Unit #2's full `zcl_abapgit_git_transport=>upload_pack_by_branch`
  cascade + `pull(...)` tree-walk — only around each unit's own local
  decode/resolve/persist/certify sequence after a pack/commit has already
  been received
- `lock_timeout_falls_back` (23) — protocol review M-3 regression:
  simulates `acquire_repo_lock` raising `zcx_abapgit_exception` at both
  Unit #1's and Unit #2's call sites and asserts each caller falls back
  gracefully (same path as an unsupported ORTEC capability / a
  non-critical persistence skip), never surfacing a hard error
- `filtered_fetch_lock_ok` (22) — protocol review M-1 regression: asserts
  `zcl_abapgit_ortec_pack_dec=>decode_and_persist` (reached via
  `try_filtered_commit_fetch`) is unmodified and keeps working with its
  own brief internal lock, independent of Unit #1
  (`zcl_abapgit_ortec_fastpath=>pull_by_branch`) and Unit #2
  (`zcl_abapgit_ortec_porcelain=>pull_by_branch`)
- `missing_objects_has_id` (22) — protocol review M-5 regression: asserts
  `persist_missing_objects` sets `attempt_id` on every row it writes to
  `ZAOG_OBJ_STORE`
- `fresh_pull_unit_atomic` (22) — Owner Decision A / B-3 regression:
  invokes `zcl_abapgit_ortec_porcelain=>pull_by_branch`'s
  `INCREMENTAL_UPDATE` branch end-to-end (Unit #2) and asserts a single
  `attempt_id` (from one `begin_attempt` call) is consistently present on
  `ZAOG_OBJ_STORE`/`ZAOG_COMMIT_HIST` rows produced by that one call,
  mirroring `one_attempt_one_id` but for Unit #2's real call site
- `fresh_pull_fail_no_publish` (26) — Owner Decision A / B-3 regression:
  simulates `zcl_abapgit_ortec_obj_store=>verify_tree_closure` raising
  inside `certify_fetched_commit` during Unit #2's run and asserts no
  `mark_graph_complete`/`mark_full_complete` certificate and no branch
  pointer update occur for that `attempt_id` (invariant: no certificate
  from a failed attempt)
- `lock_release_on_failure` (23) — Owner Decision A / B-3 regression:
  simulates `persist_pull_result` raising `zcx_abapgit_ortec_git` inside
  Unit #2 and asserts `release_repo_lock` is still called (via a spy/call
  counter), i.e. the lock is never leaked on the existing
  `TRY ... CATCH` failure path
- `resume_new_attempt_when_new` (27) — Owner Decision A / B-3 resume
  classification (§9): after a successful `resume_decode` (Unit #1's
  precondition), asserts Unit #1's own `begin_attempt` call still mints a
  genuinely fresh `attempt_id` every time — confirmed the only possible
  behavior today, since `resume_decode` never creates or persists any
  `attempt_id` of its own to reuse
- `resume_reuses_attempt` (21) — Owner Decision A / B-3 resume
  classification (§9): **NOT_APPLICABLE against current source**, kept as
  an explicit documented placeholder rather than a real test. Evidence:
  `zcl_abapgit_ortec_pack_dec=>resume_decode` has zero interaction with
  `zcl_abapgit_ortec_mat_state`/`attempt_id`/`ZAOG_COMMIT_HIST`, so no
  existing `attempt_id` is ever available for Unit #1 to reuse across a
  resume. Would become applicable only if a future, separate design
  decision introduces resume-scoped `attempt_id` persistence — out of
  scope for closing B-3.

## 17. Regression plan

Before implementation:

- run the full existing ORTEC ABAP Unit suite listed in
  `regression_variant_b_package_d_baseline.md` at baseline HEAD
  `29199f629773c676e0eaa2f3a006f5167d304ae8` as the D1/D2 pre-change
  snapshot (no code changes in D0 — this is a checkpoint recorded for the
  D1/D2 implementation phase to diff against, not executed in D0 itself).

During D1/D2 implementation (owned by those phases, not D0):

- every existing REF/OFS/mixed/external-base/missing-base test in
  `zcl_abapgit_ortec_git_tests`, `zcl_abapgit_ortec_pack_dec`, and
  `zcl_abapgit_ortec_pack_stream` must still pass unchanged (behavior-
  preserving refactor requirement for §4/§5.1);
- the two explicit gaps recorded in the regression baseline (ORTEC-disabled
  fallback; branch-switch re-entry as a dedicated case) should gain a
  dedicated test if the D1/D2 implementation touches code on those paths —
  otherwise they remain deferred, non-blocking gaps (already the case
  today, not introduced by Package D);
- Package C's cold/warm/incremental/F-C-publication suite
  (`zcl_abapgit_ortec_cold_init`, `zcl_abapgit_ortec_have_policy`,
  `zcl_abapgit_ortec_mat_state`, `zcl_abapgit_ortec_fastpath`) must remain
  green unmodified — none of their public contracts change.

## 18. Performance cost model

All figures are per single fetch/decode attempt for one pack, not
repository-wide, consistent with the "K objects, not N" invariant.

| Hot path | Cardinality | SQL calls | HTTP calls | Row/byte bound | Lookup complexity | Simultaneous payload copies | Cache scope | Txn count |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Phase 1 in-pack fixpoint (both resolvers) | O(objects in pack) per sweep, ≤ `c_max_chain_depth` sweeps | 0 | 0 | n/a (in-memory) | O(1) per lookup (hashed side-tables) | 1 resolved payload at a time (streaming) / in-place per object (non-streaming) | n/a | 0 |
| Temp-key raw-bytes read for this pack's own delta rows (streaming; PERF-M-1 correction) | O(unresolved delta rows in this pack) | 1 bulk call per pack (`preload_delta_rows`, production, commit `35be4c65`) once PERF-B-1's status-aware read path lands — was O(K) individual `get_object(temp_key)` calls pre-commit | 0 | bounded by this pack's own delta-row count | O(1) hashed cache lookup after preload | 1 resolved payload at a time | obj_store session cache (`mt_cache`) | 0 (read-only) |
| Phase 1.5 bulk external-base load (NEW) | O(distinct external bases in this pack, typically ≪ pack size) | exactly 1 (`get_objects` bulk call) | 0 | bounded by this pack's own external-base count, never repo-wide | O(1) hashed merge per base | ≤ 1 full external-base set held in memory transiently | base-cache (256 MiB budget, LRU) | 0 (read-only) |
| Phase 2 final pass | O(objects in pack) | 0 (bases already merged by Phase 1.5) / cache hits via production `preload_external_bases` (commit `35be4c65`) until Phase 1.5 lands | 0 | n/a | O(1) | same as Phase 1 | same | 0 |
| `decode_and_persist_streaming` status split (§5.3) | O(objects in pack) | 2 set-based `UPDATE`s (was 1) instead of a loop | 0 | pack-scoped | n/a | 0 extra (same batches) | n/a | 1 commit (unchanged) |
| `cleanup_incomplete` (extended) | O(rows for this pack_id) | 1 `DELETE` (was 1; predicate widened, not looped) | 0 | pack-scoped | n/a | 0 | n/a | 1 (unchanged) |
| Attempt-ID threading (§9) | O(1) extra column write per existing row write | 0 extra calls (piggybacks existing INSERT/MODIFY) | 0 | n/a | n/a | 0 | n/a | 0 extra |

Scaling table (external-base bulk load, the only new hot path):

| Stored objects (N) | Affected objects (K, one pack) | SQL calls | Notes |
| --- | --- | --- | --- |
| 1 | 1 | 0 or 1 | 0 if the pack is fully self-contained |
| 1,000 | up to a few hundred deltas, typically < 20 distinct external bases | 1 | unchanged shape regardless of N |
| 40,000 | same as above — bounded by pack contents, not store size | 1 | confirms K-not-N: identical call count as the 1,000 case |
| 1,000,000 | ~100 affected objects, incremental fetch | 1 | the bulk call's `WHERE ... obj_sha1 IN (...)` is bounded by the ~100-ish declared external bases in the incoming pack, never by the 1,000,000 stored rows — no repository-wide scan is introduced |

The `attempt_id` column additions add O(1) write cost per existing row
write (one extra field in an already-happening INSERT/MODIFY) and zero
extra SQL statements; they do not change any existing loop's statement
count.

## 19. D1 / D2 checkpoint plans

### D1 checkpoint (delta resolution)

Scope: §3, §4, §5.1, §5.2, §15 (D1 file list), §16 (D1 tests), relevant
rows of §17/§18.

Exit criteria:

- `bulk_resolve_external_bases` implemented once, called from both
  `resumable_decode` and `resolve_streaming`;
- zero per-base `get_object`/SQL calls remain in either resolver's final
  pass (verified by the `bulk_base_one_call_only`/`no_sql_in_pack_phase`
  tests);
- all D1 ABAP Unit methods in §16 green;
- all pre-existing REF/OFS/mixed/external-base tests (§1.1/§1.2) still
  green, unmodified in intent;
- performance scan confirms no new per-object SQL/HTTP and no unbounded
  copy was introduced.

### D2 checkpoint (attempt/transaction isolation)

Scope: §5.3, §7–§14, §15 (D2 file list), §16 (D2 tests), relevant rows of
§17/§18.

Exit criteria:

- `ZAOG_OBJ_STORE.status = 'D'` staging implemented; no `'D'`-status row is
  ever returned by `get_object`/`get_objects`/`get_present_sha1s`/
  `get_missing_sha1s` (verified by `temp_row_hidden_from_get`);
- `resolve_one_meta` and `preload_delta_rows` both read a pack's own
  `status = 'D'` delta temp-key rows successfully via the new
  `get_staged_delta_objects` (PERF-B-1 fix, §5.3.1; verified by
  `resolve_reads_own_d_row`);
- `cleanup_incomplete` removes `'D'`-status rows
  after a simulated crash-before-resolve (verified by
  `crash_before_resolve_ok`);
- `attempt_id` threaded end-to-end from `begin_attempt` through
  `ZAOG_OBJ_STORE`/`ZAOG_FETCH_SESS`/`ZAOG_PACK_META` for at least the
  streaming decode path, and through both self-contained publication
  units (`zcl_abapgit_ortec_fastpath=>pull_by_branch`'s Phase-1b branch —
  Unit #1, reached via both of `pull_by_branch`'s callers,
  `porcelain_path_gets_lock`; and `zcl_abapgit_ortec_porcelain=>
  pull_by_branch`'s `INCREMENTAL_UPDATE` branch — Unit #2, relocated per
  Owner Decision A/B-3, `fresh_pull_unit_atomic`), with
  `zcl_abapgit_ortec_fastpath=>upload_pack_by_branch` confirmed to own
  neither unit;
- no certificate and no branch pointer publish from a failed Unit #2
  attempt (`fresh_pull_fail_no_publish`), and no lock leaked on a Unit #2
  failure path (`lock_release_on_failure`);
- resume-match path classification resolved against source with no
  second/invented publication unit (`resume_new_attempt_when_new`;
  `resume_reuses_attempt` recorded `NOT_APPLICABLE`, §9/§16);
- lock hold is scoped to exactly the local decode/resolve/persist/
  certify/commit sequence per attempt, never across a preceding HTTP
  round-trip (`same_repo_lock_serializes`, `lock_not_held_over_http`),
  with graceful fallback on a lock-acquisition timeout
  (`lock_timeout_falls_back`) and no regression to the independent
  `decode_and_persist`/`try_filtered_commit_fetch` lock usage
  (`filtered_fetch_lock_ok`);
- Package C's `F/C` publication invariant and cold/warm/incremental
  routing remain green, unmodified in contract.

D1 and D2 are independently implementable and independently regression-
testable (D2 does not depend on D1's bulk-load change; D1 does not depend
on D2's status split), so they may be implemented and checkpointed in
either order or in parallel by separate senior-implementation slices.

## 20. Explicit Package E exclusions

Package D (D1/D2) does **not**:

- physically delete `complete_missing_base`'s disabled body, the unused
  `c_max_completion_attempts`/`gv_completion_attempts` bulk-resolution
  scaffolding, or any progressive-deepen/force-full constant — Package E
  owns removal once D1/D2 (and any other still-open Package D consumers)
  are validated;
- remove the legacy `resumable_decode`/`zcl_abapgit_ortec_pack_dec`
  session/commit-interval path in favor of the streaming path — both
  remain live until Package E's validated legacy-code removal;
- touch `zcl_abapgit_ortec_fetch_req`, `zcl_abapgit_ortec_have_policy`, or
  any wire-protocol serialization — those are already correct per Package
  C and are out of D1/D2's algorithm/persistence scope;
- perform the deferred final SAT/ST05 profiling, cache-window tuning, or
  working-set reduction listed in Package C's closeout — those remain
  deferred to the final cross-package performance pass unless a D1/D2
  measurement directly requires a correction (none identified in this
  design).
