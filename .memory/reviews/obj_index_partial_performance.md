# OBJ_PERF_FINAL — Pre-implementation performance design gate (OBJ-PERF-PERFDESIGN-1)
Mode: DESIGN_GATE
Baseline: `4193733de3c8ad75bae61f6ceb95b1c3e0ccb3e4`
Scope reviewed: `.memory/logs/obj_index_partial_design.md` (cycle-3 FINAL) +
`.memory/logs/obj_store_performance_design.md` (cycle-3 FINAL), cross-checked
against `.memory/reviews/obj_index_partial_adversarial.md` (cycle-3 verdict
APPROVE) and direct re-reads of the cited sections. No source modified; no
implementation performed.
This gate reviews **performance shape only**. Correctness/identity findings
(AR-1-xx/AR-2-xx) were already closed by the adversarial cycle and are not
re-litigated here except where they interact with performance quantification.
## 1. Honesty of the complexity claim (K vs. F)
**Claim under test**: does FILTERED-mode achieve O(K) *traversal*, or is it
O(K) *persistence* on top of an unavoidable O(F) *walk*?
Verified against §2 (candidate evaluation) and §11 step 4 of
`obj_index_partial_design.md`:
- Candidates A (filename-candidate lookup, no walk) and B (devclass
  path-prefix pruning) are both **rejected with source evidence** (no
  centralized suffix/folder knowledge for A; no historical/remote devclass
  for B) — not rejected for being slow, but for being **unsafe** (cannot
  produce a trustworthy negative).
- Candidate C (selected) explicitly states it "keep[s] the mandatory full
  commit→tree BFS walk exactly as `rebuild_index` performs it today" and
  that the improvement is "write volume, not walk volume — see §7 for the
  honest scope of the improvement" (§2, verbatim).
- §11 step 4 confirms `walk_filtered` "performs the **identical** commit→tree
  BFS as `rebuild_index`" and only differs in (a) filtering which leaves are
  buffered for write (`line_exists(lt_filter[...])`, bounds writes to F_k)
  and (b) which table/context key the write lands under. No early-exit,
  no pruning, no partial-subtree skip exists anywhere in the walk itself.
- §7's own table is precise about *which* dimension shrinks: SQL call count
  for the tree walk is **O(L)** (one bulk call per BFS level, batched — this
  is unchanged from today's COMPLETE-mode walk, not newly reduced), rows
  **written** are **O(K)/O(F_k)**, and the text explicitly states
  "confirms O(K)/O(F_k), not O(total store size)" for the *persisted-row*
  dimension only, never for walk cost. §9's Slice 3 bullet says the slice
  "removes the **O(F) write amplification** for K=1" — write amplification,
  not walk cost.
**Verdict on this check: the design does NOT overclaim.** Every place a
complexity bound is stated, it is scoped correctly to what actually shrunk
(persisted rows, cache hit path) versus what did not (SQL call count is
O(L) tree levels — unchanged from today's full walk — not reduced by K;
CPU/decode work touches all F tree leaves during any cold walk, exactly as
`rebuild_index` does today). This also satisfies the mandatory graph pattern
in `abap-performance-patterns.md` §6 ("database calls = O(number of batches
or graph levels) not O(number of nodes)") — the SQL-call bound is honestly
O(L), and the *node-visit* cost (unavoidably O(F) per cold walk) is stated,
not hidden. See PD-01 below for the one observational consequence of this
that should be called out explicitly to operators.
## 2. SQL/HTTP call-count bounds (`c_filter_chunk_size = 5000`)
Confirmed in §3.2, §3.0b, §8 of `obj_index_partial_design.md`:
- `get_coverage` (read `ZAOG_OBJ_COVER`), `write_coverage` (MODIFY
  `ZAOG_OBJ_COVER`), and `select_partial_rows_for_filter` (read
  `ZAOG_OBJ_PIDX`) all chunk `it_filter`/write buffers at the single shared
  constant `c_filter_chunk_size = 5000`. No unchunked `FOR ALL ENTRIES`
  or `MODIFY ... FROM TABLE` keyed by an arbitrary-size caller filter exists
  anywhere in this design (§8, §12 explicit prevention list).
- `select_rows_for_filter` (COMPLETE-mode `ZAOG_OBJ_INDEX` read) is
  chunked identically (AR-1-04, carried from cycle 1).
- HTTP: the only network path touched is the existing, unchanged
  `ensure_available`/`materialize_missing_batches` chain via
  `build_files_from_rows`, bounded to F_k missing blobs, never per-object
  (OS-INV-15, `obj_store_performance_design.md`).
- No per-object or per-filter-row SQL/HTTP call exists in the new code
  (`get_coverage`/`write_coverage`/`select_partial_rows_for_filter`/
  `walk_filtered` are all set-based).
**Check passes.**
## 3. Object Store dispositions (OS-A..OS-J) and the static call-graph gate
`obj_store_performance_design.md` §9's disposition table is internally
consistent with source evidence already gathered by the archaeology
(Q12-Q16): OS-A (dedup), OS-C (metadata-only presence), OS-D (bounded
chunking, **explicitly re-scoped** by AR-1-06 to "the integrated small-K
call graph reachable from `get_files_for_filter`"), OS-F (streaming decode)
are all "preserve, no change" with named evidence commits
(`733bb307`/`29199f62`/`bcc91801`). OS-E/OS-H are deferred with named,
still-unmet `state.md` entry conditions (`E1-TREE-REUSE`,
`E2_CONSUMER_COHERENCE`) rather than silently reintroduced. OS-G is
rejected with source proof (`get_known_commits`'s predicate shape does not
match the flagged column set, and it has no caller on this path).
The "AR-1-06 static call-graph proof requirement" section names the exact
method set (`ANCHOR`), the exact forbidden targets
(`get_all_objects`/`populate_cache`), and a concrete, executable
verification action (`SAPNavigate(action="references", name="GET_ALL_OBJECTS"
/"POPULATE_CACHE", type="CLAS")` reviewed for zero matches from the ANCHOR
list) with an explicit `STOP_IF`. This is checkable, not merely asserted —
it is a precise regression gate a reviewer or CI step can actually execute.
**Check passes**, with one observability suggestion (PD-02, non-blocking).
## 4. DDIC index review
- `ZAOG_OBJ_INDEX`: no secondary index added. The only read in this design's
  scope (`select_rows_for_filter`, COMPLETE-mode only after cycle 3) is a
  chunked `FOR ALL ENTRIES` on the full primary-key prefix (`repo_key,
  commit_sha1, obj_type, obj_name`) with `context_hash` as a residual
  non-key equality filter on an already primary-key-narrowed row set —
  correctly justified as not needing an index (§8).
- `ZAOG_OBJ_PIDX` (new): key order is `(client, repo_key, commit_sha1,
  obj_type, obj_name, context_hash, path_hash)` — `context_hash` is
  deliberately placed **before** the trailing open key component
  (`path_hash`) so every read stays a contiguous primary-key-prefix match
  (5 of 7 key parts fixed, `path_hash` the only intentionally open
  component to allow multiple file rows per object). No secondary index
  needed — verified against the actual query shape in
  `select_partial_rows_for_filter` (§3.0b), which supplies exactly that
  prefix. This is justified from the real predicate, not speculative.
- `ZAOG_OBJ_COVER` (new): key `(client, repo_key, commit_sha1, obj_type,
  obj_name, context_hash)` — `get_coverage`'s `FOR ALL ENTRIES` supplies
  all 5 non-client key components as exact equality per row. No secondary
  index needed, correctly justified.
- `ZAOG_OBJ_STORE`'s one flagged secondary-index candidate
  (`get_known_commits`'s `(repo_key, obj_type, status)`) is correctly
  scoped **out** of this program (OS-G, `REJECT_WITH_SOURCE_PROOF` — no
  caller on the integrated path, real write-amplification cost at
  1,000,000-object scale, no proven hot-path benefit).
**Check passes** — every new/changed SQL statement's predicate shape was
checked directly against its table's proposed key, and all three tables'
"no secondary index" conclusions are justified from the literal predicate
in the paired method spec, not asserted generically.
## 5. Memory bounds
- FILTERED-mode peak write buffer: `MIN(F_k, 5000)` rows for
  `ZAOG_OBJ_PIDX`/`ZAOG_OBJ_COVER` — strictly ≤ today's COMPLETE-mode
  30000-row peak (§7 "Peak-memory model"). Bounded and quantified.
- Oversized single-object behavior: explicitly delegated, unchanged,
  to the existing object-store/blob-fetch byte-budget machinery
  (`c_max_batch_response_bytes` etc. in `zcl_abapgit_ortec_cold_init`) —
  not reinvented or left unspecified (§7).
- Tree-level (widest BFS level) in-memory working set is stated as
  "unchanged" from today's `rebuild_index` — i.e. not quantified with a new
  number, but also not newly regressed; it inherits the existing,
  previously-accepted cost class. See PD-03 (non-blocking) for a note that
  this residual, already-known-and-parked scaling question
  (`E1-TREE-REUSE`) is correctly left out of this program's scope rather
  than silently re-opened or silently declared solved.
- Diagnostics: bounded, aggregate-only (`gv_write_coverage_failures` counter
  + last-error string, §3.2/AR-1-08) — not one log line per object.
**Check passes.**
## 6. Cardinality walk-through (1 / 1,000 / 40,000 / 1,000,000)
| N (objects) | What scales with N | Evidence |
|---|---|---|
| K=1, cold | 1 commit fetch + O(L) tree-level fetches (L = BFS depth, not F) + 1 `get_coverage` (0 rows) + ≤1 `ZAOG_OBJ_PIDX` row + 1 `ZAOG_OBJ_COVER` row — write volume is O(1), not O(F=42000) | §7 table row 1 |
| K≈1,000, cold | Same shape; 1,000 ≪ `c_filter_chunk_size`=5000, so still single-chunk reads/writes | §7 table row "K=100" extrapolates linearly and safely below the 5000 boundary |
| K≈40,000 (≈F), cold | Now above `c_filter_chunk_size` — `get_coverage`/`write_coverage`/`ZAOG_OBJ_PIDX` writes become `ceil(40000/5000)=8` bounded chunks each, never unchunked; walk cost is the same O(F) tree walk COMPLETE mode already pays today | §3.2/§8 chunking spec; no behavior change vs. today's full walk at this K |
| N=1,000,000 stored `ZAOG_OBJ_STORE` objects | Every predicate in this program is `repo_key (+ commit_sha1 + obj_type + obj_name (+ context_hash))` — primary-key-exact; confirmed no predicate scans by total repository object count | §7 table row "1,000,000 stored objects"; §8 DDIC review |
| 95–98%-shared second branch, K=1 cold on branch B | Unchanged from an unrelated cold K=1 walk — tree SHA1s differ per branch even with shared blob content (no cross-commit tree memo reuse; candidate F explicitly avoided, blocked by `state.md`'s parked `E1-TREE-REUSE`); blob-level sharing only reduces new `ZAOG_OBJ_STORE` writes via existing `get_present_sha1s`/`mt_cache`, not tree-walk cost | §2 candidate F disposition; §7 last table row |
No scenario shows an operation scaling with total repository size (N)
where it should scale with K/F/L instead, other than the already-disclosed,
intentionally-out-of-scope tree-walk cost for a cold request (§1 above).
## Findings
### PD-01 (MINOR — documentation/operator-expectation gap, non-blocking)
**Observation**: because FILTERED-mode reuses the identical full commit→tree
BFS walk, a *cold* K=1 request and a *cold* K=40,000 request pay
approximately the **same wall-clock walk cost** (both proportional to F/L) —
only the **persisted row volume** shrinks with K, not request latency for a
cold walk. The design states this honestly in prose (§2, §7, §9) but does
not add an explicit operator-facing statement that "smaller K does not mean
a faster cold request." A caller/operator reading only the cardinality
table's SQL-statement counts (which look K-independent for reads/writes)
could reasonably — and wrongly — infer that a K=1 cold request is
proportionally cheap end-to-end.
**Required action**: none before implementation (this is a documentation
clarity note, not an algorithmic gap — A/B pruning was correctly rejected
on correctness, not performance, grounds). Recommend the eventual
`get_files_for_filter` doc comment or the follow-on IMPLEMENTATION_AUDIT
explicitly record cold-walk wall-clock time as "O(F/L), independent of K"
alongside the already-tracked O(K) write metric, so a future SAT trace is
interpreted against the right expectation.
### PD-02 (MINOR — verification-process gap, non-blocking)
**Observation**: the OS-D/OS-INV-05/OS-INV-12 static call-graph proof
(`get_all_objects`/`populate_cache` unreachable from the ANCHOR method set)
is specified as a manual, human-reviewed `SAPNavigate(action="references")`
check, re-run "whenever any method in the ANCHOR list is modified in a
later slice" — there is no automated regression test enforcing this for
slices beyond the current program.
**Required action**: none before implementation. Recommend a follow-on task
(outside this program's `SOURCE_SCOPE`) convert this into an ATC custom
check or an automated where-used assertion, so a future, unrelated change
to `zcl_abapgit_ortec_obj_index`/`obj_cover` cannot silently reintroduce a
call to `get_all_objects`/`populate_cache` without a human remembering to
re-run the manual gate.
### PD-03 (MINOR — informational, non-blocking)
**Observation**: the widest-BFS-level in-memory working set during a cold
walk is stated only as "unchanged from today," not given a new numeric
bound in this design. This is consistent with `state.md`'s
`E1-TREE-REUSE: PARKED_MEASUREMENT_PENDING` (out of scope until a live SAT
trace + explicit owner GO), and this design correctly does not attempt to
solve or silently reopen it.
**Required action**: none — correctly deferred. Noted for completeness only.
No BLOCKER or MAJOR performance findings.
## Verdict
**APPROVE_WITH_MINOR_REVISIONS**
All required performance quantification is present, internally consistent
with the cited source evidence, correctly bounded (SQL/HTTP/rows/bytes/
memory), and honest about which dimension actually improves (persisted-row
volume, O(K)/O(F_k)) versus which does not (cold-walk SQL-call count is
O(L) tree levels — unchanged from today — and cold-walk node-visit/CPU cost
remains O(F), by deliberate, source-justified design choice, not oversight
or overclaim). The DDIC index review is grounded in the literal predicate
shape of each new/changed method, not asserted generically. The three
findings above (PD-01..PD-03) are documentation/process observations, not
algorithmic or bounding defects, and do not block Slice 1 implementation.