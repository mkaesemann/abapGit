# Performance IMPLEMENTATION_AUDIT — Variant B Package D2

```text
PACKET=COMPACT_HANDOFF_V1
TASK=D2-PERFORMANCE-IMPLEMENTATION-AUDIT
MODE=IMPLEMENTATION_AUDIT
BASELINE=6a42ada594760ef822eac8fac260559c51586689 (D2a+D2b1+D2b2 applied, uncommitted)
```

Evidence type: current-source call-chain re-read (not the D2 handoffs' own prose)
+ `git diff` against baseline for every SOURCE_SCOPE file + direct read of the
existing regression tests exercising each changed method. No SAT/ST05 trace,
no synthetic large-fixture run was performed for this pass (none available in
this session) — see "Unexecuted scenarios" below.

## Verdict

**PASS_WITH_MINOR_FINDINGS**

All D2b1 attempt-ID plumbing and D2's staged-visibility (`get_staged_delta_objects`)
mechanisms are confirmed O(1)/O(K-chunked) per pack, correctly reuse the
existing `c_select_package_size = 1000` chunking convention, and introduce no
new per-object SQL, no new per-object HTTP, no unbounded memory growth, and no
WHERE-clause use of the new `attempt_id` column (diagnostics-only, as
designed — confirmed via source-wide grep, zero matches). One MAJOR,
non-blocking finding on the new same-repo lock-contention interaction is
recorded below; it was accepted in principle at DESIGN_GATE but its
production-scale behavior remains unmeasured (only structurally reviewed).

## Findings

### AUDIT-M-1 (MAJOR, not blocking) — Unit #1's repo lock now has a second, genuine same-repo contender (Unit #2), and the shared lock-hold duration is proportional to the size of a resumed decode, with no telemetry and no scale test for this interaction

```text
ID: AUDIT-M-1
Severity: MAJOR (not blocking)
Path and method: zcl_abapgit_ortec_fastpath=>pull_by_branch (Phase-1b,
  src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap ~line 697-802) and
  zcl_abapgit_ortec_pack_dec=>resume_decode/resumable_decode/persist_objects
  (src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap ~line 522-600, 630-720)
Observed call shape: pull_by_branch acquires ENQUEUE_EZAOG_REPO_LOCK, then
  calls resume_decode(iv_lock_held=abap_true), which runs resumable_decode's
  full remaining-object decode/persist loop (periodic COMMIT WORK every 50
  objects, `_scope='2'` explicitly documented to "survive COMMIT WORK inside
  the decode loop") to completion in ONE call — no early return/time-budget
  check exists in resumable_decode/persist_objects. The lock is released only
  after this entire loop plus an in-memory full_tree walk plus
  persist_pull_result complete. Confirmed by direct read: this exact
  acquire/release span around resumable_decode's full loop already existed
  in resume_decode BEFORE D2b2 (pack_dec self-acquired/released the same
  lock around the identical span); D2b2 changes WHO holds it (caller-owned
  via iv_lock_held) and widens it slightly to also cover persist_pull_result
  — but does not fundamentally shorten or bound the pre-existing
  proportional-to-pack-size hold duration.
  What IS new: zcl_abapgit_ortec_porcelain=>pull_by_branch's
  INCREMENTAL_UPDATE branch now ALSO acquires this exact same canonical lock
  before its own persist_pull_result call (previously it never touched this
  lock at all). This is the first real cross-unit contention path for the
  same repo_key.
Expected production cardinality: tens of thousands of objects per pack per
  repository (per task brief); resumable_decode's loop count = objects
  remaining from the last checkpoint to the end of the pack, unbounded by
  this slice.
Estimated SQL/HTTP calls: no change — the loop's own per-batch commits
  (interval 50) are pre-existing behavior, not a new per-object SQL/HTTP
  interaction introduced by D2. HTTP: confirmed the lock is acquired AFTER
  the `branches(iv_url)` call and never re-enters HTTP before release —
  no lock-over-network-wait risk (verified by direct line-order read).
Estimated/measured memory impact: none (no new buffering).
Why it matters: at target cardinality, a single resume_decode invocation
  could plausibly run for a long time (proportional to remaining pack size).
  While it runs, `acquire_repo_lock`'s bounded retry (max 7 attempts,
  exponential backoff capped at 2s/attempt, ≈5.15s worst case total) is the
  only protection for a concurrent same-repo Unit #2 (or a second Unit #1)
  attempt — after that it raises, is caught, and the caller gracefully skips
  its own ORTEC-specific persistence (falls back to the standard/non-ORTEC
  path). This is NOT a correctness risk (both call sites' CATCH blocks are
  confirmed exhaustive and non-fatal) and NOT an unbounded block (bounded to
  ≈5.15s per contending attempt), but it is a genuine, newly-introduced
  latency/availability interaction between two previously-independent units
  that was reviewed only at the design/structural level
  (`performance_design_variant_b_package_d.md`'s "Verified sound" section
  explicitly accepted same-repo serialization as "intentional"), never
  measured, and has zero observability (no counter/log entry fires when a
  lock-timeout fallback is actually taken in production).
Required fix (non-blocking, recommended before wide rollout): (1) emit a
  lightweight diagnostic/counter (per §15 of the performance skill) each
  time `acquire_repo_lock` exhausts its retries in this specific call path,
  so a real-world spike in graceful-fallback frequency is visible; (2) add
  one synthetic-scale regression test that holds the lock artificially (e.g.
  a helper that acquires and sleeps, or a large synthetic pack) while a
  second thread/session attempts the same repo_key, asserting the bounded
  ≈5.15s timeout and graceful fallback actually occur end-to-end — today
  this is entirely unexercised (see below).
Regression test to add: `same_repo_lock_contends_e2e` (or similar) —
  currently `same_repo_lock_serializes`/`lock_timeout_falls_back`/
  `lock_not_held_over_http` are honestly documented (in their own doc
  comments) as primitive-level proxy tests, not true concurrent-contention
  drives, due to a genuine pre-existing fixture limitation (no HTTP mock
  seam, no multi-session test harness). This gap is inherited, not
  introduced by D2b1/D2b2, but it means the exact interaction this finding
  describes has never been exercised even synthetically.
```

## Verified sound (no finding)

- **`decode_and_persist_streaming`'s status-split + attempt_id UPDATEs**
  (`zcl_abapgit_ortec_pack_stream.clas.abap`): confirmed exactly 2 (status
  split) + at most 1 (attempt_id, skipped when blank) set-based `UPDATE`
  statements, each scoped by `repo_key + pack_id` (+ `obj_type` for the
  delta-promotion one), all before the single `COMMIT WORK` — O(1)
  statements per pack regardless of how many thousands of rows the pack
  contains. Matches the D2/D2b1 design exactly.
- **`get_staged_delta_objects`** (`zcl_abapgit_ortec_obj_store.clas.abap`):
  confirmed it reuses the existing `c_select_package_size = 1000` chunking
  constant (same constant/value as every other bulk-read method in this
  class), builds one `IN` range table per 1000-SHA1 chunk (bounded, no
  pathological unbounded `IN` list), uses a `HASHED TABLE ty_sha1_set` for
  dedup and a `HASHED TABLE ... table_line` for the found-set membership
  check (O(1) lookups, not linear scans), and its own cache-hit branch
  correctly admits `status IN ('D','R')` (PERF-M-2 from the design-gate
  review is confirmed fixed) — a `preload_delta_rows`-warmed row is a true
  cache hit on `resolve_one_meta`'s single-element follow-up call, not a
  second DB read. Chunking behavior is unchanged from pre-D2
  `preload_delta_rows` (which already called `get_objects(iv_bulk_fetch =
  abap_false)`, i.e. the same chunked-at-1000 path) — no SQL-call-count
  regression versus the pre-D2 baseline.
- **`attempt_id` column usage** (all three DDIC tables + all read/write
  sites): confirmed via source-wide grep that `attempt_id`/`ATTEMPT_ID` is
  never used in a `WHERE` predicate anywhere in `SOURCE_SCOPE` — it is
  write-only (piggybacked onto pre-existing per-row-build loops feeding a
  single `MODIFY`/`INSERT`/`UPDATE`) and diagnostics-only, exactly as
  designed. No new index was added and none is needed. Confirmed
  `zaog_obj_store`'s existing `RPK` index (`repo_key, pack_id, status,
  obj_sha1`) is unchanged and already covers every new query shape.
- **`persist_missing_objects`/`persist_objects` attempt_id threading**:
  confirmed (by direct line read, not just the D2b1/D2b2 closeout's claim)
  that `iv_attempt_id` is assigned inside the pre-existing per-row loop
  immediately before the single existing `MODIFY ... FROM TABLE`/periodic
  batch `MODIFY` — zero new loops, zero new SQL statements added by this
  change.
- **Lock never spans HTTP**: confirmed by direct line-order read of
  `zcl_abapgit_ortec_fastpath=>pull_by_branch` that `acquire_repo_lock` is
  called strictly after the `branches(iv_url)` HTTP lookup completes, and
  `zcl_abapgit_ortec_porcelain=>pull_by_branch`'s lock acquisition starts
  strictly after the preceding `upload_pack_by_branch`/`pull(...)` calls —
  matches AC-1/AC-2 from the D2b2 closeout.
- **No SQL/HTTP inside per-delta resolution loop**: `resolve_one_meta`'s
  single-element `get_staged_delta_objects` call is a cache hit for every
  delta already warmed by `preload_delta_rows`'s one prior bulk/chunked
  call — confirmed no new per-object round trip was introduced (this closes
  the original design-gate PERF-B-1/PERF-M-1/PERF-M-2 chain; nothing in the
  D2b1/D2b2 diff reopens it).

## Mandatory scale scenarios

- small (1–20 objects): covered by existing/new unit tests
  (`resolve_reads_own_d_row`, `staged_cache_hit_no_sql`,
  `attempt_id_on_obj_store`, etc.) — exercised.
- medium (5,000+ mixed objects, multiple batches): NOT executed this pass;
  no synthetic fixture was run. Static call-chain shape supports it
  (chunked at 1000, periodic commit at 50) but this is an estimate, not a
  measurement.
- large (40,000+ objects/paths, cold/warm cache): NOT executed. Same caveat.
- shared branches (95–98% shared): out of D2 scope (Package B territory);
  not re-verified here.
- incremental store (~100 affected objects / ~1,000,000 stored keys): NOT
  executed; static review confirms no repository-wide read was added
  (`get_staged_delta_objects` and the status-split/attempt_id UPDATEs are
  all `repo_key + pack_id`-scoped, never repository-wide).
- interrupted attempt and retry: partially covered
  (`crash_before_resolve_ok`, `resume_new_attempt_when_new`,
  `resume_reuses_attempt` [documented NOT_APPLICABLE placeholder]); the
  specific "Unit #1 mid-resume while Unit #2 contends for the same lock"
  interaction from AUDIT-M-1 is explicitly UNEXECUTED (no fixture exists to
  simulate real concurrent contention in this test infrastructure).

## Not re-litigated

Correctness/protocol findings (DR-001..DR-004, B-1/B-2/B-3, M-1..M-5,
PERF-B-1, PERF-M-1, PERF-M-2) are confirmed resolved by direct source
re-read in this pass and are not reopened. Package B/variant-b-partial-clone
cold-init/materialize mechanics are out of D2 scope and untouched by this
diff (confirmed via `git diff --stat` scope).

## SAP validation closeout

```text
STATUS=SAP_VALIDATED_COMPLETE
PACKAGE_D_D2_VALIDATED_HEAD=733bb30799886ef8659be7e293b82c3ccfcebbdd
```

AUDIT-M-1 (same-repo lock-contention latency) remains a documented,
non-blocking, structurally-reviewed-only finding - no live contention was
observed in the three subsequent IT8 incident retests (SYSTEM_NO_ROLL,
TIME_OUT, DBSQL_STMNT_TOO_LARGE, all SAP_VALIDATED_RESOLVED) or in the
follow-up SAT trace
(`.memory/incidents/variant_b_d2_sat_warm_to_cold_o4h8794.md`), which found
zero cost contribution from any D2-owned attempt/lock/transaction
mechanism.
