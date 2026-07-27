# Performance DESIGN_GATE Review — Variant B Package D (D0: D1 + D2)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=D-REVIEW-PERF-DESIGN-GATE
MODE=DESIGN_GATE
BASELINE=29199f629773c676e0eaa2f3a006f5167d304ae8
REVIEWER_ROLE=ortec-abapgit-performance-review (independent, design-only, no code changes)
DESIGN_UNDER_REVIEW=.memory/logs/variant_b_package_d_design.md (D0, including Owner Decision A / B-3 relocation)
```

All claims below were re-verified directly against current productive source
(`src/ortec/git/*.clas.abap`), not against the design document's own prose or
the discovery/correctness/protocol review artifacts' paraphrase. Where a
design claim could not be reconciled with source, the source wins.

## Verdict

**REJECT** (BLOCK_PERFORMANCE_ARCHITECTURE-equivalent)

One new BLOCKING finding (PERF-B-1) makes the D2 §5.3 staged-visibility fix,
as scoped, break delta resolution for every pack containing any delta
object — not a rare edge case, the normal case for real Git packs. This is
not a tuning/estimate gap; it is a guaranteed runtime failure the design's
own §16/§19 test list would have caught immediately had the design traced
the read side of its own status-split change. One MAJOR finding
(PERF-M-1) shows §18's Phase 1/Phase 2 SQL-call cost table is inaccurate
(claims 0, actual is O(K) per-object calls) for a hot path D1 already
touches. The rest of the design's quantitative claims (Phase 1.5 bulk-load
shape, base-cache sizing, attempt_id piggyback cost, cleanup/status-split
statement counts, lock-scope narrowing) are confirmed correct against
source.

## BLOCKING

### PERF-B-1 — D2 §5.3's `status = 'D'` split makes `resolve_one_meta` unable to read its own pack's delta raw bytes; every delta-bearing pack would fail

- Severity: BLOCKING (correctness, surfaced while verifying the
  performance/SQL shape of the status-split per task item 2/3)
- Path and method: `zcl_abapgit_ortec_pack_stream=>resolve_one_meta`
  (`src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.abap`, line ~601-606):
  `ls_delta_obj = zcl_abapgit_ortec_obj_store=>get_object( iv_repo_key = iv_repo_key iv_sha1 = <ls_row>-temp_key ).`
  — this is the read that fetches the delta's own pre-application raw bytes
  before calling `apply()`. It is not the external-base lookup (that's a
  separate call earlier in the same method); it runs for **every** delta
  object, external base or not.
- Observed call shape: `get_object` → `get_objects` → (cache miss) →
  `read_object_rows`
  (`src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap`), whose `SELECT`
  hard-codes `AND status = 'R'`. Confirmed by direct source read, and
  independently confirmed by `zcl_abapgit_ortec_pack_stream`'s own
  class-level doc comment on `c_status_incomplete`: *"every existing
  object-store read path already hardcodes status = 'R', so rows written
  under this status are already invisible to every existing consumer with
  no further filtering needed."* Today this works because
  `decode_and_persist_streaming` promotes **every** row of the pack
  (including delta temp-key rows) to `status = 'R'` via its blanket
  `UPDATE` before `resolve_streaming`/`resolve_one_meta` ever runs.
- What D2 §5.3 changes: the blanket `UPDATE` is split so that delta/temp-key
  rows are promoted to a **new** `status = 'D'` instead of `'R'`,
  specifically so they are excluded from `read_object_rows`'s
  `status = 'R'` filter (this is stated as the fix's explicit goal:
  *"'D' is excluded from every existing READY read... by construction,
  since those filters already hard-code 'R' and are left unchanged."*).
- Why it matters: `resolve_one_meta`'s own read of `<ls_row>-temp_key`
  goes through **the exact same** `read_object_rows` `status = 'R'` filter
  that the fix deliberately hides `'D'` rows from. Once implemented as
  scoped, the very first delta object in the very first pack processed
  after this change lands will fail with `"Delta temp data missing:
  Object <temp_key> not found in store"` (the method's own catch-and-raise
  at line 606) — because its temp-key row is now `'D'`, not `'R'`, and no
  code path in §15's D2 file-scope list adds an alternate read for
  `status = 'D'` rows. This is 100% reproducible for any pack containing a
  REF_DELTA or OFS_DELTA object — i.e. essentially every real incremental
  Git fetch pack. It is not a rare/edge scenario; it would fail D2's own
  planned tests (`delta_temp_row_status_d`, `crash_before_resolve_ok` would
  likely still pass since they test cleanup, not resolution, but any
  existing REF/OFS/mixed-chain regression test —
  `ref_chain_resolves`/`ofs_chain_resolves`/`external_thin_base_resolves`/
  etc. — would break immediately).
  Non-delta rows are unaffected (promoted to `'R'` exactly as today, and
  looked up by their real content SHA1, not a temp key) — the defect is
  isolated to the delta/temp-key path, but that path is the majority case
  for any pack that isn't 100% non-delta objects.
- Estimated/measured SQL calls: N/A — the operation does not reach a
  measurable steady state; it raises on the first delta object.
- Estimated/measured HTTP calls: none affected.
- Estimated/measured memory impact: none — this is a pure read-visibility
  defect, not a scaling defect.
- Required fix: before D2's status-split can be implemented, add an
  explicit, narrowly-scoped read path for a pack's **own** staged delta
  temp-key rows that is not filtered to `status = 'R'` — e.g. a new
  private helper (or a parameterized read on `read_object_rows`) scoped by
  `repo_key + pack_id + obj_sha1 IN (...)` accepting `status IN ('D','R')`
  (or `status = 'D'` specifically for temp-key reads), used only by
  `resolve_one_meta`'s temp-key fetch — never exposed through the generic
  `get_object`/`get_objects`/`get_present_sha1s`/`get_missing_sha1s` APIs
  those methods must keep hard-coded to `status = 'R'` for every other
  caller. This is a new symbol not currently in §15's D2 file-scope list
  and must be added, along with a dedicated regression test asserting a
  delta object's own raw bytes remain readable by the resolver after the
  status-split lands (not just that they are hidden from `get_object`).
- Regression test to add: `resolve_reads_own_d_row` (or similar,
  ≤30 chars) — decode a pack containing at least one REF_DELTA object,
  assert `decode_and_persist_streaming` leaves its temp-key row at
  `status = 'D'`, then assert `resolve_streaming` still successfully
  resolves it (not just that `cleanup_incomplete`/`get_object` correctly
  hide it). None of the existing planned D2 tests (§16) exercise this
  combination — `temp_row_hidden_from_get` proves the opposite property
  (hidden from `get_object`) without checking whether the one caller that
  legitimately needs to read it (`resolve_one_meta`) still can.

## MAJOR

### PERF-M-1 — §18's Phase 1 / Phase 2 cost table claims "SQL calls: 0"; actual is one `get_object` call per resolved delta object (not just per external base)

- Severity: MAJOR (not blocking — bounded by pack size K, not repository
  size N, so it does not break the cross-repository-size scaling
  invariant, but it is a genuine per-object DB round-trip inside the hot
  delta-resolution loop and directly contradicts the design's own stated
  cost model for a method D1 already modifies)
- Path and method: `zcl_abapgit_ortec_pack_stream=>resolve_one_meta`,
  line ~601-606 (same call site as PERF-B-1, independent of that finding);
  the non-streaming analogue does not have this issue (`ty_object`-based
  `ct_objects` already keeps the delta's raw bytes resident in the row
  itself, no re-read needed).
- Observed call shape: for **every** delta object resolved — during both
  Phase 1's repeated in-pack sweeps and Phase 2's final pass — regardless
  of whether its base was in-pack or external,
  `zcl_abapgit_ortec_obj_store=>get_object( iv_sha1 = <ls_row>-temp_key )`
  issues one single-row-equivalent DB read
  (`read_object_rows`/`SELECT ... obj_sha1 IN lr_sha1s` with exactly one
  value in the range) to fetch that object's own pre-application raw
  bytes. This is a distinct call from the base-cache/base-lookup path
  Phase 1.5 targets; Phase 1.5 does not eliminate it because Phase 1.5 only
  bulk-loads **external bases**, never the resolving object's own raw
  bytes.
- Expected production cardinality: K = number of delta objects in one
  pack (bounded by pack size, not repository size N) — but K itself can be
  large: a sizeable rebase/squash/initial-clone pack can easily contain
  thousands of delta objects, each triggering its own round-trip.
- Why it matters: §18 states "Phase 1 in-pack fixpoint (both resolvers) |
  ... | SQL calls: 0" and "Phase 2 final pass | ... | SQL calls: 0 (bases
  already merged)". Both are inaccurate for the streaming resolver: the
  true count is O(K), one call per resolved delta object, not zero. This
  matches the skill's explicitly forbidden "SELECT per object in a loop"
  shape (`.github/skills/abap-performance-patterns/SKILL.md` §2/§3/§7) and
  is architecturally identical in kind (though smaller in observed scale
  today) to the previously-documented production incident where per-row
  DB round-trips dominated total runtime (82%+ of a 549s trace was pure
  per-row DB open/close overhead, per prior session notes) — the same
  failure mode is latent here for any unusually large single pack.
- Required fix: extend Phase 1.5 (or add a parallel, equally-bounded bulk
  step) to also bulk-preload **this pack's own** temp-key raw bytes for
  every delta row into an in-memory hashed lookup (`temp_key` → bytes)
  before Phase 1 begins, using the same `get_objects(iv_bulk_fetch =
  abap_true)` bulk API Phase 1.5 already introduces for external bases —
  bounded by this pack's own delta-object count, never repository-wide.
  `resolve_one_meta` would then read from that in-memory table instead of
  calling `get_object` per delta. At minimum (if the fix above is deferred
  to a follow-up), §18's cost table must be corrected to state the true
  SQL call count for Phase 1/Phase 2 (O(K) per pack, not 0), so the design
  does not understate the streaming path's actual DB cost.
- Regression test to add: extend `no_sql_in_pack_phase` (§16, currently
  scoped to asserting zero *object-store/DB* calls during the in-pack
  fixpoint) to explicitly cover the streaming resolver's per-delta
  temp-key read, not only the base-lookup path — as currently worded it
  is easy to satisfy by only checking the base-fetch call count and miss
  this one entirely.

## Verified sound (no finding)

- **D1 Phase 1.5 external-base bulk load** — confirmed exactly one
  `get_objects(iv_bulk_fetch = abap_true)` call per resolver invocation:
  `get_objects` (`zcl_abapgit_ortec_obj_store.clas.abap`) branches on
  `iv_bulk_fetch` and, when true, builds the **entire** missing-SHA1 set
  into one package and calls `read_object_rows` exactly once (no
  `c_select_package_size` chunking in that branch) — matches §5.1's
  documented intentional-unchunked-branch reliance, and is safe because
  D1 only ever calls it with this one pack's own distinct external-base
  set, never an unbounded/repo-wide list. `read_object_rows` itself is a
  genuine set-based `SELECT ... obj_sha1 IN lr_sha1s ... status = 'R'`,
  not a loop. Scaling table (1 / 1,000 / 40,000 / 1,000,000 stored
  objects, K bounded by this pack's own external-base count) is accurate
  for this specific mechanism.
- **Base-cache (256 MiB, LRU)** — `zcl_abapgit_ortec_base_cache` confirmed
  O(1) hashed lookup (`by_sha1`) and O(1) LRU eviction (`by_seq` sorted
  key, `remove_oldest` reads/deletes `INDEX 1 USING KEY by_seq`), correct
  byte-budget enforcement in `put()` (oversized entries rejected before
  insertion, running total decremented/incremented exactly around
  eviction/replace), and correctly content-addressed (process-global
  singleton is safe because the key is the payload's own SHA1). Sizing is
  independent of repository object count N — sound for repositories up to
  and beyond 1,000,000 stored objects, since footprint is bounded strictly
  by resident bytes, not row count.
- **`decode_and_persist_streaming`'s status-split statement count** —
  confirmed the split (§5.3) changes exactly one blanket `UPDATE` into two
  predicate-scoped `UPDATE`s (`obj_type IN ('ref_d','ofs_d')` → `'D'`; the
  complement → `'R'`), still O(1) statements per pack, not a loop,
  independent of pack size or repository size.
- **`cleanup_incomplete` extension** — confirmed today's body is a single
  `DELETE FROM zaog_obj_store WHERE repo_key = ... AND pack_id = ... AND
  status = c_status_incomplete`; widening to `status IN ('I','D')` remains
  one set-based `DELETE`, pack-scoped, not repository-wide, regardless of
  how many objects the failed pack contained or how large the repository
  is.
- **`attempt_id` column additions** — confirmed via `persist_missing_objects`
  (`zcl_abapgit_ortec_fastpath.clas.abap`) and `persist_pull_result`
  bodies: the new column is set as one extra field assignment inside an
  **already-existing** per-row-build loop / already-existing single
  `INSERT`/`MODIFY ... FROM TABLE` statement — zero new SQL statements,
  zero new loops, O(1) added cost per existing row write. Matches §18's
  claim for this specific mechanism.
- **Lock/attempt scope narrowing (Unit #1 in `zcl_abapgit_ortec_fastpath
  =>pull_by_branch`'s Phase-1b branch; Unit #2 relocated to
  `zcl_abapgit_ortec_porcelain=>pull_by_branch`'s `INCREMENTAL_UPDATE`
  branch, immediately before its existing `persist_pull_result` call)** —
  confirmed both units wrap only a small, already-in-memory-plus-one-commit
  sequence (`persist_pull_result`'s real body: one loop-based row build,
  one certification call, one repo-state update, one `COMMIT WORK`), never
  a preceding HTTP round-trip. This does not introduce new
  lock-contention/serialization risk beyond what the existing repo-scoped
  SAP-enqueue lock (`ENQUEUE_EZAOG_REPO_LOCK`) already carries for its
  existing, unrelated caller (`try_filtered_commit_fetch`'s
  `decode_and_persist`) — many concurrent users pulling *different*
  repositories never contend (repo-key-scoped), and repeated pulls of the
  *same* large repository each hold the lock only for the bounded local
  persist/certify/commit window, not across network wait time. No new
  hot-path SQL/HTTP was found in the relocated Unit #2 call site beyond
  what §10's existing ownership table already documents.

## Overall K-not-N assessment

The one new intentional hot path (Phase 1.5 external-base bulk load)
correctly stays O(1) SQL calls per pack regardless of repository size, and
the `attempt_id`/status-split/cleanup changes are all O(1)-statement,
piggybacked, or single-set-based-statement changes with no new loop. The
two findings above are about a **different**, already-existing part of the
same hot path (the delta's own raw-byte retrieval) that the design's cost
model incorrectly describes as free, and about a **staged-visibility
side effect that breaks that same retrieval entirely** once the `'D'`
status is introduced. No new O(N)-in-repository-size hot path, no new
unbounded in-memory copy (all external-base and base-cache payloads
remain pack-bounded), and no new per-object HTTP call were found anywhere
in D1/D2 as scoped.

## Required fixes before re-review

1. PERF-B-1 (blocking): add an explicit, narrowly-scoped read path for a
   pack's own `status = 'D'` delta temp-key rows, used only by
   `resolve_one_meta`'s raw-bytes fetch; add it to §15's D2 file scope;
   add the `resolve_reads_own_d_row` regression test (or equivalent).
2. PERF-M-1 (major): either extend Phase 1.5 to also bulk-preload this
   pack's own delta temp-key raw bytes (recommended, reuses the same bulk
   API and closes both findings with one mechanism), or, at minimum,
   correct §18's Phase 1/Phase 2 SQL-call figures and explicitly document
   the O(K) per-pack temp-key read cost as an accepted, bounded,
   pre-existing condition with a tracked follow-up.

## Not re-litigated

Package C's F/C publication invariant, cold/warm/incremental routing, wire
protocol serialization, and the correctness/protocol review tracks'
already-resolved findings (DR-001..DR-004, B-1/B-2/B-3, M-1..M-5, m-1/m-2)
are not reopened here — nothing in this pass contradicts their resolution,
and this review's findings are additive, discovered by tracing the actual
read side of the `status = 'D'` write introduced in §5.3, which none of
the prior review tracks traced end-to-end.

## Concurrent baseline reconciliation (owner commits, not a new review pass)

Four owner commits landed on the branch between this review's iteration 1
and the fix cycle (`ad51cb49`, `df029c5c`, `35be4c65`, `5e540354`; new
verified HEAD `5e5403546554dbe4f0f8e7a1eb2f07ab434dbe95`). Full
commit-by-commit analysis:
`.memory/logs/variant_b_package_d_concurrent_commit_impact.md`. Only one
commit is relevant to this review track:

- `35be4c65` ("Bulk preload streaming delta data") added
  `zcl_abapgit_ortec_pack_stream=>preload_delta_rows`, a bulk
  `get_objects` call over this pack's own unresolved delta `temp_key`s,
  called once before the Phase 1 fixpoint loop inside `resolve_streaming`.
  This is **evidence supporting, not superseding,** required fix #2 above
  (PERF-M-1): production has independently converged on the same "bulk
  preload the pack's own temp-key rows" mechanism this review recommended
  as the preferred fix. It does not close PERF-M-1 on its own, because
  `preload_delta_rows` — exactly like `resolve_one_meta`'s existing call —
  depends on the temp-key rows already being `status = 'R'`, so it is
  **also broken** by the still-unimplemented §5.3 `status = 'D'` split.
- **PERF-B-1's required fix is widened, not changed in kind:** the
  dedicated status-`'D'`/`'R'`-aware read path required by fix #1 above
  must be used by **both** `resolve_one_meta`'s raw-bytes fetch **and**
  `preload_delta_rows`'s bulk fetch. The `resolve_reads_own_d_row`
  regression test must exercise `resolve_streaming` end-to-end (so
  `preload_delta_rows` is on the call path), not `resolve_one_meta` in
  isolation.
- PERF-M-1's recommended fix (option 1, "extend Phase 1.5 to also
  bulk-preload this pack's own delta temp-key raw bytes") is now best
  read as "make `preload_delta_rows` status-`'D'`-aware", since that
  method already exists and already does exactly this bulk preload for
  the `status = 'R'` case.
- No other finding in this review, and no correctness or
  protocol/persistence review conclusion, is affected by any of the four
  commits (the other three touch only Package B cold-init/materialize
  code, confirmed out of D1/D2 scope). No re-review of this discipline is
  required beyond the one already-owed fix-cycle re-review; this section
  only widens the scope of the fix already required before that
  re-review.

## Iteration 2 (post-fix, post-concurrent-commit-reconciliation)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=D-REVIEW-PERF-DESIGN-GATE-ITER2
MODE=DESIGN_GATE (targeted fix verification, not full re-review)
BASELINE_OLD=29199f629773c676e0eaa2f3a006f5167d304ae8 (SAP-validated)
BASELINE_CURRENT=5e5403546554dbe4f0f8e7a1eb2f07ab434dbe95 (verified HEAD, git-ancestry/diff only)
```

Re-verified directly against current workspace source (not the connected
SAP system, which is out of sync with this branch's HEAD — confirmed via
`git rev-parse HEAD` matching `5e540354` while the connected system's
`zcl_abapgit_ortec_pack_stream`/`zcl_abapgit_ortec_obj_store` method lists
are missing `preload_delta_rows`/`preload_external_bases`/
`get_available_objects` entirely; local `src/ortec/git/*.clas.abap` files
were used as the authoritative source per this task's explicit
SOURCE_SCOPE).

### Verdict

**APPROVE_WITH_MINOR_REVISIONS**

### PERF-B-1 — RESOLVED (both call sites confirmed against source)

- `resolve_one_meta` (line 667:
  `ls_delta_obj = zcl_abapgit_ortec_obj_store=>get_object(iv_repo_key,
  <ls_row>-temp_key)`) and `preload_delta_rows` (line ~452:
  `zcl_abapgit_ortec_obj_store=>get_objects(iv_repo_key, lt_temp_keys,
  iv_bulk_fetch = abap_false)`) are both confirmed, today, to depend on
  `read_object_rows`'s hard-coded `status = 'R'` filter (line 1109) — the
  exact dual-call-site dependency the concurrent-commit reconciliation
  widened PERF-B-1 to cover.
- Switching both to the proposed `get_staged_delta_objects`
  (`status IN ('D','R')`) genuinely closes both, since neither call site
  needs any other change (identical error path, identical parameter
  shape, `resolve_streaming` already calls `preload_delta_rows` before
  the Phase 1 DO-loop and `resolve_one_meta` from within it — confirmed
  in `resolve_streaming`, lines ~723-820).
- `get_object`/`get_objects`/`get_available_objects`/`read_object_rows`
  confirmed genuinely unchanged by this fix (no other caller touches
  `get_staged_delta_objects`); §8's STAGED/READY invariant is preserved,
  not weakened — (c) satisfied.

### PERF-M-1 — RESOLVED at the cost-model/documentation level

§18 now has a dedicated "Temp-key raw-bytes read" row citing the
production `preload_delta_rows` bulk call (1 call/pack, was O(K)); §19's
D2 exit criteria require `resolve_reads_own_d_row` to run
`resolve_streaming` end-to-end (so `preload_delta_rows` is genuinely on
the call path, not just `resolve_one_meta` in isolation) — (b) satisfied
for the documented cost model.

### PERF-M-2 (NEW, MAJOR) — `get_staged_delta_objects`'s cache-hit check is unspecified and risks silently reintroducing O(K) per-pack calls

- Severity: MAJOR (same class as the original PERF-M-1: bounded by K, not
  N, but a real per-object DB round-trip risk in the exact hot path this
  fix cycle targeted — and, unlike PERF-M-1, no planned test would catch
  it if it regresses).
- Evidence: `get_objects`'s and `get_available_objects`'s cache-hit checks
  both hard-code `IF sy-subrc = 0 AND ls_cache_entry-status = 'R'`
  (obj_store lines 508, 420). §5.3.1 specifies `get_staged_delta_objects`
  as having "the same contract... as the existing `get_objects`" and
  states its second call (`resolve_one_meta`, after `preload_delta_rows`
  warms the cache) "must still be a cache hit" — but does not say the
  cache-hit condition itself must also accept `status = 'D'`. An
  implementation that copies `get_objects`' body and only changes the
  `read_object_rows`-equivalent DB filter (the literal reading of "same
  contract... different filter") would leave the cache-hit check at
  `status = 'R'` only, causing every `'D'`-status row `preload_delta_rows`
  just cached to register as a cache **miss** on `resolve_one_meta`'s
  follow-up call — reintroducing one individual DB read per delta object,
  silently defeating the bulk-preload fix and PERF-M-1's cost-model
  correction, with zero functional/correctness symptom.
- No existing or planned test (§16) asserts a call count for this path;
  `resolve_reads_own_d_row` only asserts resolution succeeds, not that it
  does so via O(1) DB calls per pack.
- Required fix (minor/mechanical, no architecture change): §5.3.1 must
  state explicitly that `get_staged_delta_objects`'s cache-hit condition
  is `status IN ('D','R')`, not `status = 'R'`; add a companion assertion
  (extend `resolve_reads_own_d_row` or add a small new test, e.g. a
  call-count spy on the object-store bulk/staged read) proving the
  post-`preload_delta_rows` per-delta read is a cache hit, not a new DB
  call.

### Other checks requested (task item d)

- **Leak risk** (MINOR, documentation-only): `get_staged_delta_objects`
  is a `PUBLIC CLASS-METHODS` with no compile-time restriction to
  `zcl_abapgit_ortec_pack_stream` (ABAP has no package-private method
  visibility; a `PRIVATE` + `FRIENDS` declaration could enforce it but is
  not proposed). Practical risk is low: exploiting it requires another
  caller to already possess a `temp_key` pseudo-SHA1, which is never
  exposed outside `pack_stream`'s own `ty_meta`. Matches this codebase's
  existing doc-only-encapsulation convention (e.g. `set_active_repo_key`).
  Non-blocking; recommend noting the exception explicitly in §8's table.
- **`preload_external_bases` interaction**: confirmed NO interaction.
  It resolves `delta_base` values, which are always real content SHA1s
  (never a pack's own temp_key) and are only ever consumed once truly
  `status = 'R'`; `get_available_objects`/`read_object_rows` remain
  `status = 'R'`-only and untouched by this fix. Verified sound.

### Verdict rationale

No blocking finding remains — PERF-B-1 is genuinely fixed for both call
sites. PERF-M-1's cost-model gap is genuinely closed in the documented
design. One new MAJOR finding (PERF-M-2) is a narrowly-scoped
specification gap (one clarifying sentence + one test assertion), not an
architectural defect, and does not require a further full DESIGN_GATE
round-trip — hence `APPROVE_WITH_MINOR_REVISIONS` rather than
`REVISE_AND_REVIEW_ONCE`. Two MINOR items (leak-risk documentation, §8
table completeness) are recorded for the implementation phase.

