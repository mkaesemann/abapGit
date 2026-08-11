# OBJ_PERF_FINAL — Partial Index & Object Store History Archaeology
Read-only git archaeology (task OBJ-PERF-ARCH-1). No design or code proposed here.
Scope: `--all` history of the files listed in the launch prompt's SOURCE_SCOPE, on
branch `ortec/abapgit_1_133-opt-rework`. At the time of this archaeology,
`BASELINE_COMMIT` (`4193733d`) **is** the current branch HEAD — there is no
baseline/current divergence to diff for these files; all mechanisms below are
already merged into current source (spot-checked via `grep_search`, see per-finding
notes). This log is therefore a catalog of the mechanisms/failure history behind
the code that OBJ_PERF_FINAL will build on top of, not a regression diff.
## Summary
- **SAME_FAILURE_CLASS** (mechanisms whose root-cause pattern is a direct risk for
  the new demand-driven partial `ZAOG_OBJ_INDEX` / `ZAOG_OBJ_STORE` read-path work
  if reintroduced): `cd0b277d`, `c8fbdf23`, `58a90001`, `17513ba7` (index side);
  `733bb307`, `2111b288`, `9c3d297a`, `a51e743b` (object-store side).
- **REJECTED_PATTERN** (concrete anti-patterns confirmed bad and replaced —
  do not reintroduce): implicit `deepen=0` + empty haves == "send full history"
  (`58a90001`, `a51e743b`); unbounded `populate_cache()` whole-repo preload keyed
  only by `status = 'R'` (`2111b288`); unchunked `IN <range>` SHA1 list for bulk
  reads (`733bb307`, `29199f62`); `get(...) IS NOT INITIAL` as an existence/cache-hit
  check (`9c3d297a`); silent fallback from a new decoder to the old memory-unsafe
  decoder on the *same* bytes after the new one already failed (`8e03a191`).
Both sections below use the commit's short SHA as the finding ID.
---
## Partial Index history
### `25134aef` — Add Filtered Tree Walk and Object Index to speed up "Stage By Transport"
- Files/methods: introduces `zaog_obj_index` (TABL), `zcl_abapgit_ortec_filter_walk`,
  `zcl_abapgit_ortec_obj_index` (485 new lines), extends `zcl_abapgit_ortec_obj_store`,
  `zcl_abapgit_stage_logic`, `zcl_abapgit_repo_online`.
- Mechanism: origin of the whole partial/filtered index concept — a persisted,
  path-filtered index of tree/blob rows per repo+commit so Stage-by-Transport can
  avoid a full remote-tree walk.
- Observed failure: none at introduction (foundational commit).
- Root cause: n/a.
- Partial clone existed at that time: yes (Variant B blobless-clone work predates
  this in the same branch history).
- Current graph/snapshot certificates change the risk: current `is_index_ready`
  STRICT-mode completion marker (see `c8fbdf23`) did not exist yet at this commit.
- Reuse verdict: **SUPERSEDED** by later completeness/batching fixes below — the
  original shape is still the current class skeleton, but its readiness/batching
  logic has since been hardened twice.
- Regression test to prevent recurrence: n/a (foundational).
- Classification: **SUPERSEDED**.
### `aa8229bd` — Ortec opt-rework Phase 4: bulk missing-object collection for filtered Stage/Diff read path
- Files/methods: `zcl_abapgit_ortec_obj_store=>get_missing_sha1s` (new, chunked,
  no per-object reads); new `zcl_abapgit_ortec_missing_objects` (renamed to
  `..._missing_obj` in `cd2b602f` for the 30-char ABAP name limit) with
  `ensure_available`; `zcl_abapgit_ortec_obj_index=>get_files_for_filter` /
  `build_files_from_rows` gain optional `iv_url`/`iv_commit` best-effort top-up.
- Mechanism: demand-driven top-up — if a filtered index row references a blob
  missing locally, fetch just that gap via one negotiated incremental request
  instead of falling back to the full `get_files_remote()` slow path. Explicit
  safety gate: no network call unless the repo has opted into ORTEC
  write/protocol behavior.
- Observed failure: none yet at this commit — the commit message explicitly
  flags this as syntax-checked only, no live functional validation performed.
- Root cause: n/a (new capability, not a fix).
- Partial clone existed: yes.
- Current certificates change risk: the *shape* of this "bounded top-up on demand"
  design is exactly the shape OBJ_PERF_FINAL's demand-driven partial index needs —
  but its first fetch-sizing (unbounded `upload_pack_by_commit`, no deepen) was
  later found unsafe (see `58a90001`, `17513ba7`).
- Reuse verdict: **REUSABLE_WITH_GUARDS** — the safety-gate + best-effort +
  fall-through-unchanged pattern is sound; the fetch call itself must use the
  later-hardened `materialize_missing_batches` path, not a raw
  `upload_pack_by_commit`.
- Regression test required: a test asserting the top-up call is bounded to the
  caller's own missing SHA1 set size, not the full commit graph (already exists
  as of `17513ba7`; must not regress).
- Classification: **REUSABLE_WITH_GUARDS**.
### `c8fbdf23` — Ortec opt-rework Phase 4b: six-state object model + D4 completeness switch
- Files/methods: `zcl_abapgit_ortec_obj_index=>is_index_ready` /
  `rebuild_index`; `zcl_abapgit_ortec_obj_store` six-state vocabulary (see Object
  Store section); `zcl_abapgit_ortec_git_switch` D4 STRICT/RELAXED switch.
- Mechanism: a `$IDX/__READY__` completion marker row that must be written only
  when `rebuild_index` finishes a **fully successful** walk.
- Observed failure: `is_index_ready` previously treated "any row exists for this
  repo+commit" as "index fully built" — true even for an index left behind by a
  rebuild interrupted partway through (corrupt tree, missing object, decode
  failure). A partially-built index was indistinguishable from a complete one.
- Root cause: missing distinction between "some rows exist" and "the walk that
  produced them finished successfully" — exactly the hazard a *demand-driven
  partial* index must solve correctly, since partial-by-design rows are now the
  norm, not an error state.
- Partial clone existed: yes.
- Current certificates change risk: **this is the direct textbook precedent** for
  OBJ_INDEX_SLICE_1 — any new demand-driven partial index MUST carry an explicit,
  narrower-than-"any row" readiness/completeness signal per scope (not just per
  repo+commit), or it will reproduce this exact bug at the new granularity.
- Reuse verdict: **REUSABLE_WITH_GUARDS** — reuse the "explicit completion marker,
  self-heal on missing/partial marker" pattern; the STRICT/RELAXED switch
  (RELAXED = benchmark-only, never default) is itself a reusable safety-net shape.
- Regression test required: `ltcl_obj_index=>marker_required_for_ready` already
  covers the repo+commit-level marker; the demand-driven partial index needs the
  equivalent test at whatever new (sub-commit / path-scoped) granularity it adds.
- Classification: **SAME_FAILURE_CLASS** (index readiness).
### `58a90001` — Bound missing-object top-up fetch to a single commit, not full history
- Files/methods: `zcl_abapgit_ortec_missing_obj=>ensure_available`.
- Mechanism/failure: `upload_pack_by_commit` called without an explicit deepen
  level (default 0). `deepen=0` + empty have-set is standard git wire-protocol
  shorthand for "send the complete history from the beginning of the repo", not
  "just this commit". Live SYSTEM_NO_ROLL: `CL_ABAP_GZIP=>DECOMPRESS_BINARY`
  requested 1.5+ GB opening the filtered Stage page.
- Root cause: implicit protocol default misread as "narrow request".
- Partial clone existed: yes — this bug undermines partial clone's entire promise
  by fetching full history in a supposedly-scoped top-up.
- Fix: explicit `deepen = 1`.
- Reuse verdict: **REJECTED_PATTERN** — never call a fetch primitive with an
  empty/absent deepen value and rely on the default; any new partial-index top-up
  path must set deepen explicitly (or use `materialize_missing_batches`, which
  already encodes this).
- Regression test required: assert every `upload_pack_by_commit`/`_by_branch`
  call site used by index/store top-up paths passes an explicit bounded deepen or
  verified-complete haves — never both empty.
- Classification: **SAME_FAILURE_CLASS**, **REJECTED_PATTERN** (the deepen=0
  default-abuse pattern itself).
### `17513ba7` — fix D2 TIME_OUT: bound `ensure_available`'s remote top-up to the caller's missing SHA1 set
- Files/methods: `zcl_abapgit_ortec_missing_obj=>ensure_available`,
  `zcl_abapgit_ortec_obj_index=>build_files_from_rows`,
  `zcl_abapgit_ortec_walk_prep=>topup_missing_blobs`, new
  `zcl_abapgit_ortec_cold_init=>materialize_missing_batches`.
- Mechanism/failure: even with `58a90001`'s deepen=1 fix, `ensure_available`
  still fetched a target commit's **entire** reachable graph
  (`upload_pack_by_commit(deepen=1)`) to resolve a caller-supplied set of
  *specific* missing blob SHA1s. Live IT8: 162,919 objects fetched, 132,963
  unresolved deltas, TIME_OUT.
- Root cause: "bound the fetch to one commit" (58a90001) is not the same as
  "bound the fetch to the caller's actual K missing objects" — scaling with N
  (whole commit tree) instead of K (actual gap) was the residual bug.
- Fix: extracted the already-validated adaptive row/byte-bounded
  `MATERIALIZE_BLOBS` batching loop into `materialize_missing_batches`, called
  with exactly the caller's missing SHA1 set. Confirmed still current source
  (`zcl_abapgit_ortec_missing_obj.clas.abap` line ~107 calls
  `materialize_missing_batches`).
- Reuse verdict: **REUSABLE_WITH_GUARDS** — this K-not-N top-up shape is exactly
  the mechanism a demand-driven partial index rebuild/top-up should reuse
  directly (it already exists and is validated); do not reintroduce a
  whole-commit-graph fetch for a narrower gap.
- Regression test required: `materialize_missing_empty`, `capability_intersection`
  (already exist per commit message) — must be preserved/extended for any new
  caller.
- Classification: **SAME_FAILURE_CLASS** (K-vs-N fetch sizing), **REUSABLE**.
### `2111b288` — fix SYSTEM_NO_ROLL: remove unbounded `populate_cache` preload from `get_reachable_objects`
- Cross-listed with Object Store section below (same commit, same failure class
  directly relevant to any index rebuild that calls `get_reachable_objects`).
- Relevance to index workstream: `zcl_abapgit_ortec_obj_index`'s rebuild path
  consumes reachable-object queries from `zcl_abapgit_ortec_obj_store`; any new
  partial-index rebuild logic that widens scope back to "everything ever buffered
  for this repo" instead of "exactly what's reachable from the target commit"
  reproduces this incident.
- Classification: **SAME_FAILURE_CLASS**.
### `29199f62` — Fix 'Large SQL Statement' Crash (`zcl_abapgit_ortec_walk_prep=>fetch_blobs_bulk`)
- Mechanism/failure: built one unchunked `IN @lr_sha1s` range table across an
  unbounded blob-SHA1 set, risking the ~32,767 DBSL bind-marker ceiling (same
  failure family as `733bb307` in Object Store).
- Fix: bounded/deduplicated key window (`lc_key_chunk_size = 2000`) built via a
  hashed dedup set, then a **database-side JOIN** against an internal table
  (`INNER JOIN @lt_requested`) instead of an `IN` range — avoids one DBSL marker
  per SHA1 entirely — plus an explicit byte budget (`lc_byte_budget`) for the
  payload window. Also: any genuinely-missing blob is still drained from
  `ct_remaining_sha1s` before raising, so the caller's batching loop cannot spin
  forever on an unsatisfiable SHA1 (documented invariant in the diff comments).
- Reuse verdict: **REUSABLE_WITH_GUARDS** — the internal-table-JOIN pattern (not
  chunked `IN`) is a stronger, reusable alternative to plain chunking for
  bulk-by-key reads in the new payload-access rework; the "drain from
  remaining-set even on failure" invariant must be preserved by any replacement.
- Classification: **REUSABLE_WITH_GUARDS**, related failure class to `733bb307`.
### `df029c5c` — Optimize remaining blob set updates (`zcl_abapgit_ortec_walk_prep`)
- Small follow-up to `29199f62`'s batching loop; no separate failure — refines
  the same remaining-SHA1-set bookkeeping. **REUSABLE** as part of the same
  batching mechanism above.
### `77b66464` / `8b382a5e` — ORTEC: Optimize / Increase OBJ_INDEX rebuild batching
- Files/methods: `zcl_abapgit_ortec_obj_index` constant `c_index_write_chunk_size`
  for `rebuild_index`'s bulk `MODIFY zaog_obj_index FROM TABLE` persistence.
- Mechanism: raised from an unnamed literal `1000` → named constant `5000`
  (E1-PERF-A design §2) → `30000` (owner-approved after measured DB
  round-trip/array-DML setup cost review at 1000 rows). Confirmed **still current**
  (`c_index_write_chunk_size TYPE i VALUE 30000` present in source today).
- Observed failure: none — pure throughput tuning, not a bug fix; part of
  Package E checkpoint 1 (SAP_VALIDATED_COMPLETE per `.memory/state.md`).
- Reuse verdict: **REUSABLE_WITH_GUARDS** — 30000 is the current, validated
  value; state.md's parked `E1-TREE-REUSE` item explicitly requires a *new* IT8
  SAT trace proving material residual cost *after* this value before any further
  index-rebuild optimization (e.g. tree-decode/mapping reuse) is attempted. Do
  not re-derive or second-guess the chunk size without that trace.
- Classification: **REUSABLE_WITH_GUARDS** (do not reopen without new evidence).
### `cd0b277d` — Fix "tree not found" on Commit/Push for ORTEC-active repos
- Files/methods: `zcl_abapgit_git_porcelain=>push` (1 routing IF, only standard
  change), new `zcl_abapgit_ortec_porcelain=>full_tree` / `push`.
- Mechanism/failure: `zcl_abapgit_ortec_porcelain`'s WARM_UNCHANGED/COLD_BRANCH
  pull classifications seed **only the commit object** into the returned object
  set, relying on `ZAOG_OBJ_STORE` for the tree/blob graph — i.e. an
  intentionally sparse/partial object set. That sparse set was reused as
  `it_old_objects` for Push, but the standard
  `zcl_abapgit_git_porcelain=>push/full_tree/walk_tree` has **no buffer
  fallback**, so it failed immediately with "tree not found" for any commit
  attempt on a branch that was already up to date (or just cold-fetched) — the
  common case.
- Root cause: a sparse/partial in-memory object set silently crossed a boundary
  into a consumer (`full_tree`/`walk_tree`) that assumes a complete set, with no
  contract enforcing completeness at that boundary.
- Partial clone existed: yes.
- Current graph/snapshot certificates change the risk: **this is the single
  most directly relevant precedent in the whole archaeology** for a
  demand-driven *partial* index — every new consumer of a partial
  `ZAOG_OBJ_INDEX` row set must either (a) go through a buffer-aware
  reconstruction path (as `full_tree` now does, pulling from `ZAOG_OBJ_STORE`),
  or (b) be proven to never receive a sparse set. A "just pass it_objects
  through" shortcut anywhere in the new read path reproduces this exact class
  of bug.
- Fix: buffer-aware `full_tree`/`push` clones in `zcl_abapgit_ortec_porcelain`
  that reconstruct the base tree from `ZAOG_OBJ_STORE` when `it_objects` is
  sparse. Confirmed current (`zcl_abapgit_git_porcelain.clas.abap` routes to
  `zcl_abapgit_ortec_porcelain=>push` when ORTEC-active, per `grep_search`).
- Regression test required: `full_tree_sparse_seed`, `full_tree_uses_it_objects`
  (already exist) — any new partial-index consumer needs an equivalent
  sparse-seed regression test.
- Classification: **SAME_FAILURE_CLASS** (sparse/partial object set crossing an
  unguarded completeness boundary) — the single highest-priority historical
  precedent for OBJ_INDEX_SLICE_1.
### `1b0dccc5` — Add delta-base completeness gate for future thin-pack negotiation
- Mechanism: additive, currently-unused gate combining index-readiness +
  full-object-graph presence + missing-delta-base check into one pass/fail
  result, to decide whether a repo's history is safe to offer as a thin-pack
  base.
- Relevance: a reusable **pattern**, not a fix — "compose readiness + presence +
  no-dangling-reference checks into one completeness gate" is exactly the shape
  needed to decide when a demand-driven partial index scope is safe to trust as
  a stand-in for a full one.
- Reuse verdict: **REUSABLE_WITH_GUARDS** as a design pattern only (gate itself
  is for pack negotiation, not index reads — would need adaptation, not reuse
  as-is).
- Classification: **REUSABLE_WITH_GUARDS**.
### `51c1c52e` — fix ES6 walk failure: reliable delta-base repo_key + repo-wide self-heal
- Files/methods: `zcl_abapgit_git_delta`, `zcl_abapgit_ortec_obj_store=>
  set_active_repo_key`, `zcl_abapgit_ortec_porcelain=>pull_by_branch`'s self-heal,
  new `invalidate_all_history`.
- Mechanism/failure #1: delta-base fallback resolved against `mv_cache_repo_key`
  set only as an accidental side effect of an unrelated earlier call — no
  guarantee it held the correct (or any) repo at decode time, risking silently
  leaking a stale/unrelated repo's cached data across repos (cross-repository
  keying hazard).
- Mechanism/failure #2: the "Walk," self-heal retry cleared only
  `ZAOG_REPO_STATE.FETCH_COMMIT` for the failing branch, but
  `get_complete_commits` checks `ZAOG_COMMIT_HIST` first and returns early if
  *any* rows exist for the repo (from any branch) — so a retry could still
  advertise stale haves and reproduce the identical failure. A **partial**
  invalidation (single branch) left a global consumer relying on the
  repo-wide table's leftover rows.
- Fix: explicit `set_active_repo_key()` hook before every fallback decode call;
  `invalidate_all_history` clears history repo-wide (not per-branch) for the
  self-heal path.
- Reuse verdict: **REUSABLE_WITH_GUARDS** — explicit repo-key scoping before any
  cache/store fallback, and "invalidate at the same granularity the reader
  checks at" are both directly reusable guard patterns for the new work
  (especially since OBJ_PERF_FINAL explicitly deals in cross-repository keying
  per the mission's search-term list).
- Classification: **REUSABLE_WITH_GUARDS**, also a **SAME_FAILURE_CLASS**
  warning for any new partial/scoped invalidation that doesn't match its own
  readiness-check granularity.
### `a94f08da` (Package B B1) / `99e20f8d` (Package B B2+B3) — cold blobless graph acquisition; bounded selected-tip blob discovery and materialization
- Mechanism: `acquire_blobless_graph` (one `blob:none` filtered fetch, streaming
  decode, tree-closure verification, `GRAPH_COMPLETE` certificate publication,
  200 MiB memory-risk gate); `get_tip_blob_sha1s` (iterative bounded
  commit→tree walk, no payload reads); `materialize_tip_snapshot` (bulk presence
  subtraction, deduplicated bounded batches, per-batch verification,
  snapshot-complete publication only **after full re-verification**).
- Relevance: this is the **existing, SAP-validated precedent for "demand-driven,
  verified-complete partial materialization"** in this codebase — closest
  functional analog to what OBJ_INDEX_SLICE_1 is being asked to build. Its
  "certify complete only after re-verification, never optimistically" discipline
  is the core lesson to reuse.
- Reuse verdict: **REUSABLE_WITH_GUARDS** — reuse the acquire → discover →
  bounded-batch-materialize → re-verify → certify pipeline shape; do not skip
  the final re-verification step for the sake of demand-driven latency.
- Classification: **REUSABLE_WITH_GUARDS**.
---
## Object Store history
### `bcc91801` — Phase 2: streaming pack decoder (decode-and-free, metadata-only)
- Files/methods: new `zcl_abapgit_ortec_pack_stream`; `zaog_obj_store` status
  `'I'` (incomplete) → `'R'` (ready) promotion.
- Mechanism: decodes one object at a time, persists decompressed bytes
  immediately under status `'I'`, frees the local buffer before the next object
  — never holds more than one object's bytes in memory. On full success
  (including trailer SHA1 verification), all of the run's `'I'` rows are
  promoted to `'R'` in **one set-based UPDATE**; on any failure, all of the run's
  `'I'` rows are removed in **one set-based DELETE** before re-raising — a failed
  or partial run never leaves incomplete rows behind.
- Observed failure being solved: the prior non-streaming decoder's
  full-object-accumulation pattern hit an `rt_objects` memory ceiling on very
  large packs.
- Reuse verdict: **REUSABLE_WITH_GUARDS** — this is the canonical "metadata-only,
  incomplete/ready state machine with atomic promote-or-purge" pattern the
  mission's "payload projection" and "cleanup/read races" search terms describe.
  Note the commit's own callout: a first subagent-delegated attempt reproduced
  the OLD full-accumulation pattern instead of this streaming design and was
  discarded — a concrete warning that "looks similar" implementations of this
  area can silently regress back to the memory-unsafe shape.
- Regression test required: `delta_free_pack_decodes`, `ref_delta_stays_unresolved`,
  `corrupt_trailer_no_rows` (already exist) — any new payload-access path with
  its own incomplete/ready lifecycle needs the equivalent "failure leaves zero
  rows" test.
- Classification: **REUSABLE_WITH_GUARDS**.
### `c8fbdf23` — six-state object model (cross-listed, see Partial Index section for the index-readiness half)
- Files/methods: `zcl_abapgit_ortec_obj_store` `ty_object_state` / `cs_object_state`:
  `LOADED`, `INDEXED_NEEDS_LOAD`, `NOT_BUFFERED`, `UNKNOWN_NEEDS_FETCH`,
  `CONFIRMED_ABSENT`, `CORRUPT_OR_INCOMPLETE`. Confirmed **still current**
  (all six constants present in source today).
- Mechanism: documents the non-negotiable rule that **only `CONFIRMED_ABSENT`**
  may ever be classified as "remote-deleted"; a D4 STRICT/RELAXED compile-time
  switch in `zcl_abapgit_ortec_git_switch` (STRICT ships as default).
- Relevance: directly the "missing-object-vs-remote-absence handling" and
  "commit/tree/blob/tag type identity" search terms from the mission — this
  vocabulary is the canonical place any new payload-access/projection logic
  must classify its results into, rather than inventing new ad hoc states.
- Reuse verdict: **REUSABLE_WITH_GUARDS** — reuse the vocabulary as-is; any new
  "presence"/"metadata-only" read added for OBJ_STORE_SLICE_1 must map its
  results onto these six states, especially never conflating
  `NOT_BUFFERED`/`UNKNOWN_NEEDS_FETCH` with `CONFIRMED_ABSENT`.
- Classification: **REUSABLE_WITH_GUARDS**.
### `733bb307` — fix D2 DBSQL_STMNT_TOO_LARGE: chunk `get_objects` bulk-fetch reads
- Files/methods: `zcl_abapgit_ortec_obj_store=>get_objects` (`iv_bulk_fetch =
  abap_true` branch).
- Mechanism/failure: the bulk-fetch branch built the caller's **entire**
  cache-miss SHA1 set into one unchunked `read_object_rows` call, unlike its own
  `abap_false` branch and every other `read_object_rows` caller in the class.
  Live IT8: a cold branch's blob-level frontier reached 40,891 distinct blob
  SHA1s → one `SELECT ... obj_sha1 IN <range>` with 40,893 bind markers,
  exceeding HANA/DBSL's 32,767-marker ceiling.
- Fix: unify both branches to chunk at `c_select_package_size` (1000) before
  every `read_object_rows` call — matching the pattern already used by
  `get_available_objects`/`has_dangling_delta_base`/`get_present_sha1s`/
  `get_staged_delta_objects`. Confirmed still current (`c_select_package_size =
  1000`, applied uniformly per `grep_search`).
- Reuse verdict: **REJECTED_PATTERN** to avoid reintroducing (unchunked `IN`
  range for a caller-scale SHA1 set); **REUSABLE_WITH_GUARDS** for the fix
  itself — any new "bulk read by SHA1 set" method added for payload-access
  optimization must chunk at (or below) `c_select_package_size`, with no
  exception for "this branch is supposed to be fast".
- Classification: **SAME_FAILURE_CLASS**, **REJECTED_PATTERN**.
### `2111b288` — fix SYSTEM_NO_ROLL: remove unbounded `populate_cache` preload from `get_reachable_objects`
- Files/methods: `zcl_abapgit_ortec_obj_store=>get_reachable_objects`.
- Mechanism/failure: unconditional `populate_cache()` call issuing one unbounded
  `SELECT * FROM zaog_obj_store WHERE repo_key = X AND status = 'R'` with no
  row/byte limit — loading **every** READY object ever buffered for the
  repository (including full blob/tree/commit payloads), not just objects
  reachable from the target commit. Live IT8: `LT_ROWS[54226x280]` resident,
  ~3.96 GB, SYSTEM_NO_ROLL on a 502 KB allocation that was merely the tipping
  point.
- Root cause: the method's own per-level `get_objects(iv_bulk_fetch=abap_true)`
  calls already correctly and sufficiently resolve exactly the reachable
  (K, not N) data — the whole-repo preload was pure dead weight matching the
  sibling method `get_reachable_sha1s`, which never called `populate_cache`.
- Fix: remove the one call; add `reachable_ignores_extra_ready` regression test
  asserting the method returns exactly the reachable set even with many
  unrelated READY rows for the same repo.
- Note from the commit: does **not** fix the paired TIME_OUT half of the same
  incident (`zcl_abapgit_ortec_missing_obj=>ensure_available` fetching a whole
  commit's reachable graph instead of a filtered top-up) — that was fixed
  separately in `17513ba7`.
- Reuse verdict: **REJECTED_PATTERN** (whole-repo, status-only-filtered preload
  as a "just warm the cache" shortcut); any new payload-access optimization that
  wants a warm-cache/prefetch step must scope it to the actual reachable/target
  set, never "everything with status R for this repo".
- Classification: **SAME_FAILURE_CLASS**, **REJECTED_PATTERN**.
### `9c3d297a` — Fix ITAB_DUPLICATE_KEY dump in `base_cache` PUT (real cause: broken empty-blob cache-hit check)
- Files/methods: `zcl_abapgit_ortec_base_cache=>get_base_bytes`/`put`/`touch`,
  called from `zcl_abapgit_ortec_pack_stream=>get_base_bytes` /
  `resolve_one_meta`.
- Mechanism/failure: "is this SHA1 already cached?" was decided via
  `get( ) IS NOT INITIAL`. A genuinely cached **0-byte** object (a real, valid
  empty blob/tree — common, frequently reused across a repo) also returns an
  initial xstring from `get()`, so every request for that SHA1 was wrongly
  treated as a cache miss, triggering a redundant DB fetch + redundant `put()`
  every time. Under scale, the repeated-`put()` path exercised the table's
  UNIQUE secondary key and dumped with the uncatchable `ITAB_DUPLICATE_KEY`.
- Root cause: an emptiness check used as a presence/existence check, where the
  domain's legitimate "empty" value is indistinguishable from "absent". (Matches
  a previously-recorded general lesson in this agent's own memory notes.)
- Fix: added `has(iv_sha1)` — a pure existence check (`find_entry() > 0`)
  independent of the cached value's length; `get_base_bytes` now uses `has()`.
  Defense in depth: `put()`/`touch()` switched from `APPEND` to
  `INSERT ... INTO TABLE` so a UNIQUE-secondary-key violation degrades to
  `sy-subrc <> 0` instead of an uncatchable dump.
- Reuse verdict: **SAME_FAILURE_CLASS** risk for any new "metadata-only/presence
  read" or "duplicate SHA elimination" logic added in the payload-access rework
  — any presence/dedup check must use an explicit existence primitive (`has()`
  or equivalent), never infer presence from payload content/length. The
  `INSERT INTO TABLE` (not `APPEND`) hardening for any table with a UNIQUE
  secondary key is a generally reusable defensive pattern.
- Regression test required: `zero_byte_blob_is_a_hit`, `re_put_same_sha1_no_dump`
  (already exist) — any new cache/dedup structure needs the equivalent
  zero-length-value test.
- Classification: **SAME_FAILURE_CLASS**, **REUSABLE_WITH_GUARDS** (the fix
  pattern).
### `eafdc61a` — large-repo secondary indexes, bulk-prefetch crash-resume lookup, off-hot-path cache admin report
- Mechanism: secondary DB indexes for repo+status (object store) and
  repo+SHA1 (pack index), closing a full-scan gap the delta-base completeness
  check and other status/type-filtered queries depended on without index
  support; replaced a per-object round-trip in crash-resume decode with one
  bulk lookup read into an in-memory hashed table; added a report/class for
  cache visibility and a safeguarded (auth-checked, confirmed, repo-locked)
  manual cache clear, explicitly not wired into any interactive/hot-path
  transaction; compaction/reference cleanup explicitly deferred.
- Reuse verdict: **REUSABLE_WITH_GUARDS** — secondary indexes matching the
  actual query predicates, and "bulk-fetch once into a hashed table, not N
  round-trips" are both directly applicable to any new payload-projection
  query added for OBJ_STORE_SLICE_1. The explicit non-wiring of destructive
  cache-clear into any hot path is a safety precedent worth preserving for any
  new admin/cleanup tooling.
- Classification: **REUSABLE_WITH_GUARDS**.
### `afee6a17` — Fix REF_DELTA chain resolution and remove unsafe standard-decode fallback
- Files/methods: pack decoder `resolve_all`/`resolve_one`.
- Mechanism/failure #1: single ascending-pass resolution could not resolve a
  REF_DELTA chained onto another REF_DELTA positioned *later* in the pack
  (REF_DELTA carries no ordering guarantee) — surfaced as either a generic
  delta-apply failure or a false-positive "Delta base not found" from an
  unsound pre-check.
- Fix #1: multi-pass fixpoint resolution (repeated in-pack-only sweeps, no
  store fetch/raise, until nothing new resolves; only the final pass allows a
  thin store fetch + precise "not found" error).
- Mechanism/failure #2: on a failed Ortec pack decode, the fastpath fell back
  to re-decoding the same bytes with the **standard** decoder — but a thin/deepen
  fetch can legitimately contain `OBJ_OFS_DELTA` entries the standard decoder
  cannot parse, desynchronizing its position tracking and crashing with
  SYSTEM_NO_ROLL (unbounded allocation on corrupt/misaligned input) instead of
  failing cleanly.
- Fix #2: remove that fallback entirely; re-raise so the existing
  thin → non-thin → standard-**via-transport-catch** cascade re-negotiates a
  fresh pack instead of reusing bytes that just failed.
- Reuse verdict: **REUSABLE_WITH_GUARDS** — "never feed bytes that already
  failed one decoder into a structurally different decoder" and "resolve
  same-pack dependency chains via fixpoint iteration, not assumed ordering" are
  both directly relevant to "tree/blob reuse across graph/snapshot/walk/index/
  file-building phases" and "payload corruption/hash verification" in the
  mission scope.
- Classification: **RELATED_BUT_NOW_MITIGATED**.
### `8e03a191` — TEMPORARY DIAGNOSTIC: surface streaming decode failures instead of masking them via the crash-prone fallback
- Mechanism/failure: after Phase 4 (streaming decoder) deployed, a live
  SYSTEM_NO_ROLL crash's ST22 stack proved the streaming decoder ran and failed
  cleanly, but then fell back to the old `decode_and_persist` on the same huge
  pack — which crashed exactly as it always did (the old, memory-unsafe decoder
  this whole effort exists to replace) — with the real streaming failure reason
  never surfaced anywhere.
- Fix (explicitly temporary): re-raise the streaming failure directly instead of
  falling back, in the one call site implicated in the crash stack.
- Reuse verdict: **REJECTED_PATTERN** — "silently fall back from the new/safe
  path to the old/unsafe path on the exact same bad input" masks the real root
  cause and can crash identically anyway; any new payload-access path must
  re-raise with diagnosable detail rather than cascade to a legacy full-buffer
  path on the same bytes. Whether this specific temporary diagnostic commit is
  still the final word (vs. later superseded by a permanent fix) was not traced
  further — flagged as **SUPERSEDED_CANDIDATE**, not confirmed, since its own
  TODO says to revisit once root cause is known.
- Classification: **REJECTED_PATTERN** (the masking-fallback anti-pattern),
  **UNRELATED_OLD_PARTIAL_CLONE_GAP** is not applicable — this is current-era.
### `63e80030` — Preserve raise call site and specific cache-fallback failure reasons
- Mechanism: `zcx_abapgit_ortec_git` captures the real caller's call stack at
  `raise()` construction time, since `RAISE EXCEPTION` always executes inside
  the class's own static `raise()` method and the original call stack is
  otherwise unrecoverable once caught. `serve_cached_when_nothing_new`'s inner
  catch now preserves and appends the real failure reason instead of a generic
  "Cached objects not available" message.
- Reuse verdict: **REUSABLE_WITH_GUARDS** — diagnosability pattern worth
  preserving for any new payload/cache-fallback exception path in the rework.
- Classification: **REUSABLE_WITH_GUARDS**.
### `a51e743b` — Add haves-free retry when the server's "nothing new" claim is wrong
- Mechanism/failure: confirmed live (both `pull_by_branch` and
  `ensure_available`) that a server can respond "nothing new" (empty or
  zero-object pack) for a want whose object graph is **not actually fully
  present locally**. The existing verification correctly detected and raised
  rather than serving broken data, but there was no recovery path once both the
  thin and non-thin attempts hit the identical "nothing new" response (same
  haves offered both times).
- Fix: `zcx_abapgit_ortec_git` gains `mv_retry_without_haves`, set on every
  `serve_cached_when_nothing_new` failure path; one additional attempt forces
  `iv_force_full` (skip have advertisement) before giving up. Also closed a
  related gap: empty have-set + zero deepen previously sent *neither* a deepen
  nor have line (same "send everything" wire-protocol shorthand hazard as
  `58a90001`) — now always sends `deepen 1` minimum whenever no haves are
  offered, for every caller of that method.
- Reuse verdict: **SAME_FAILURE_CLASS** as `58a90001`'s deepen=0 hazard, now
  closed at a broader call-site scope; **REUSABLE_WITH_GUARDS** — "the local
  cache/store cannot safely stand in for a claimed-empty remote response; verify,
  and retry with a stronger request rather than trusting the claim" is directly
  relevant to any new metadata-only/presence-read short-circuit added for
  payload-access optimization (a cache that says "nothing new" must be
  independently verifiable, not trusted blindly).
- Classification: **SAME_FAILURE_CLASS**, **REUSABLE_WITH_GUARDS**.
### `8245dee2` — Handle a well-formed zero-object pack the same as an empty response
- Mechanism: `zcl_abapgit_ortec_pack_dec=>peek_object_count` — a cheap
  header-only peek (no decompression) that validates the PACK magic before
  trusting the byte layout, used to recognize a legitimately-empty pack the
  same way as a response with no pack section at all.
- Relevance: directly the "metadata-only/presence reads" mechanism named in the
  mission scope — a header-only peek without full decode is a reusable
  primitive for any new presence/metadata-only projection over stored objects.
- Reuse verdict: **REUSABLE_WITH_GUARDS**.
- Classification: **REUSABLE_WITH_GUARDS**.
### `435681ca` — Fix `zcl_abapgit_ortec_porcelain` Performance Regression
- Files/methods: `pull` (private), `walk_tree`/`walk` call sites.
- Mechanism/failure: the `it_objects` importing parameter was declared
  `VALUE(it_objects)` (pass-by-value) and mutated in place via
  `APPEND LINES OF lt_objects TO it_objects`; `walk_tree`'s manifest lists
  *every* reachable blob unconditionally, so without filtering, even a
  **complete (non-sparse)** `it_objects` would still redundantly re-walk/re-fetch
  blobs already resident in it.
- Fix: build a new `lt_objects_complete = VALUE #( ( LINES OF it_objects )
  ( LINES OF lt_objects ) )` once, and filter the remaining-fetch list to blobs
  **not already present** in that combined set via `line_exists(...)` before
  batch-fetching.
- Reuse verdict: **REUSABLE_WITH_GUARDS** — this is the concrete "duplicate SHA
  elimination" mechanism named in the mission scope: always check
  already-resident objects before adding them to a remote/DB batch-fetch list,
  and prefer building a new combined set over repeated in-place `APPEND`
  mutation of a large parameter.
- Classification: **REUSABLE_WITH_GUARDS**.
### `36839c7f` — ORTEC: Isolate porcelain extension routing
- Mechanism: restores `zcl_abapgit_git_porcelain` to the standard abapGit
  implementation with only explicit repository-level routing hooks for
  ORTEC-enabled repos (pull-by-branch/pull-by-commit delegate to
  `zcl_abapgit_ortec_porcelain` only when the persistent ORTEC cache is
  enabled); removes duplicated/unreachable ORTEC internals (object-store
  traversal, fastpath fallback, persistence, history invalidation, retry
  handling, "Walk," recovery coupling) from the standard class.
- Relevance: establishes the current single-integration-boundary architecture
  (confirmed current via `grep_search` on `zcl_abapgit_git_porcelain.clas.abap`
  — only `is_active_for_repo` routing IFs remain). Directly relevant to the
  binding invariant in `.memory/state.md`: "Any standard-abapGit-class change
  for an ORTEC hook must stay the smallest possible delegation."
- Reuse verdict: **REUSABLE_WITH_GUARDS** as an architectural constraint — any
  new OBJ_PERF_FINAL hook into standard classes (`zcl_abapgit_git_porcelain`,
  `zcl_abapgit_objects`, `zcl_abapgit_filename_logic`) must follow this same
  "single routing IF, all real logic in the ORTEC-owned class" shape.
- Classification: **REUSABLE_WITH_GUARDS**.
### `99e20f8d` / `a94f08da` — Package B bounded selected-tip blob discovery/materialization (cross-listed)
- See Partial Index section above — identical relevance to the Object Store
  side: `get_tip_blob_sha1s` / `materialize_tip_snapshot` are the store-side
  half of the same demand-driven, verified-complete materialization precedent.
- Classification: **REUSABLE_WITH_GUARDS**.
---
## Notes on scope not fully traced
- `zcl_abapgit_ortec_cache_admin`, `zcl_abapgit_ortec_have_policy`,
  `zcl_abapgit_ortec_pack_dec`/`pack_raw`/`pack_stream` history was consulted
  only where a keyword match surfaced them (they were not in the launch
  prompt's explicit SOURCE_SCOPE); a handful of terse-message commits
  (`3caa75ba`, `c6330e6d`, `3a85b286`, `35be4c65`, `ad51cb49`, `5e540354`)
  were identified via `git log` but not deep-diffed given the mission's
  focus on the two named workstreams — flag for a follow-up pass if the
  design phase needs their detail.
- `cd2b602f` ("rename class over the 30-char ABAP object name limit") was
  seen only as a log title, not diffed — recorded here only as the reason
  `zcl_abapgit_ortec_missing_obj` is not named `..._missing_objects` as in
  the original `aa8229bd` commit message.