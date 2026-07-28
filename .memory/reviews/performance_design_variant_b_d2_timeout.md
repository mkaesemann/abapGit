# Performance DESIGN_GATE review — Variant B D2 TIMEOUT fix (Candidate A)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=D2-TIMEOUT-PERFORMANCE-DESIGN-GATE
MODE=DESIGN_GATE
BASELINE=2111b2887cc4fbf2ee481f753fd4af2c3e5085c4
REVIEWED_DESIGN=.memory/logs/variant_b_d2_timeout_fix_design.md (Candidate A,
  already corrected per .memory/reviews/variant_b_d2_timeout_protocol_review.md)
STATUS=REVIEW_COMPLETE
```

Independent, read-only review. No productive source was changed. This review
does not re-litigate protocol/persistence correctness (already
`APPROVE_WITH_MINOR_REVISIONS` in the prior pass, condition already applied to
the design doc's §13 test-plan). Focus is exclusively performance/scale.

## 1. Source verified directly (not the design doc's prose alone)

- `src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.abap` — full file read.
  Current `ensure_available`: Step 1/4 `get_missing_sha1s` (bulk), Step 2
  `zcl_abapgit_git_transport=>upload_pack_by_commit(iv_deepen_level=1)`
  (the defect — commit-wide graph, not `it_sha1s`-scoped), Step 3
  `store_objects`.
- `src/ortec/git/zcl_abapgit_ortec_cold_init.clas.abap` lines 1-120,
  380-1000 — `materialize_tip_snapshot`'s exact WHILE loop (lines ~437-520):
  `deduplicate_sha1s` → `WHILE` (`take_next_batch` → `materialize_batch` →
  `calculate_next_batch_size`), wrapped in one `TRY`/`CATCH
  zcx_abapgit_ortec_git` that closes `lo_client` on both paths.
  `begin_attempt` (before the loop), `verify_ready_blobs`,
  `may_publish_snapshot`, `finalize_snapshot`, `COMMIT WORK` (all after the
  loop) are textually and lexically OUTSIDE the block the design proposes
  to extract. `materialize_batch`'s recursive oversize-split logic (halves
  the batch, recurses with `iv_splits_used + 1`, raises via
  `decide_oversize_action` = `RAISE` once `batch_size <= 1` or
  `splits_used >= c_max_oversize_splits`) confirmed exactly as the design
  describes — never silently accepts an unbounded response, never
  degenerates to one-request-per-object.
- `src/ortec/git/zcl_abapgit_ortec_fetch_req.clas.abap` lines 180-245 —
  `build_request`'s `WHEN cs_fetch_mode-materialize_blobs` branch: want-count
  validated against `c_materialize_batch_max` (1000) before any buffer is
  built; capability check (`c_cap_reachable_want` OR `c_cap_tip_want`)
  executes before `build_want_lines`; `have_count = 0` hardcoded; no
  `deepen`/`shallow` token anywhere in the branch.
- `src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap` lines 505-560 —
  `build_files_from_rows` calls `ensure_available` exactly ONCE per
  Stage-By-Filter action, for the whole deduplicated `blob_sha1` set of the
  filter's own already-narrow row set (no per-row loop wrapping the call).
- `src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap` lines 1255-1335 —
  `get_present_sha1s`/`get_missing_sha1s`: presence-only (`SELECT obj_sha1
  ... WHERE obj_sha1 IN lr_sha1s AND status='R'`, never `SELECT *`/loads
  `obj_data`), chunked via `lr_sha1s`/`c_select_package_size`-bounded range
  tables — confirmed `O(K/chunk)` bulk SQL, not `O(N)`, and unaffected by
  total repository size `N`.

## 2. Evaluation against the six required questions

### 2.1 Row/byte bound correctness across K and N

| Scenario | HTTP requests | SQL calls | Peak simultaneous payload |
| --- | --- | --- | --- |
| K=1 (any N) | 1 info/refs GET + 1 materialize POST (batch of 1) | O(1) (`get_missing_sha1s`/`get_present_sha1s` single-chunk, ×2 for Step 1/4) | one response ≤ `c_max_batch_response_bytes` (25 MiB) unless the single blob is itself pathologically large, in which case `decide_oversize_action` = `RAISE` (batch_size=1) — structured failure, never unbounded accept |
| K=1,000 (typical Stage-By-Filter upper bound) | 1 info/refs + 1–3 materialize POSTs (`c_batch_rows_initial`=500 → adaptive growth ×2 can cover the remainder in one more batch) | O(K/chunk) bulk, ×2 | each batch ≤ 25 MiB hard ceiling; adaptive controller *targets* 16 MiB |
| K=40,000 (mandated scale scenario) | 1 info/refs + `ceil(40000 / batch_rows)` POSTs — with `c_batch_rows_max`=1000 as the floor once the controller ramps up, **≥40** sequential POSTs; controller starts at 500 and can at most double per successful batch, so early batches are smaller (worst-case round-trip count is higher than 40 during ramp-up, converging to ~40 once batch size saturates at 1000) | O(K/chunk) bulk, ×2 — unaffected by N | every individual batch still ≤ 25 MiB; **aggregate** bytes across all ~40+ batches is bounded by K's real payload size, not by N |
| K=100 against N=1,000,000 stored objects | 1 info/refs + 1 materialize POST (fits in the 500-row initial batch) | O(1) chunk, ×2 — `get_present_sha1s`/`get_missing_sha1s` are keyed `obj_sha1 IN (...)` lookups scoped to `repo_key`, never scan or load the other 999,900 rows | one batch ≤ 25 MiB |

All four scenarios scale with `K` (or `ceil(K/batch)`), never with `N`. This
is the exact fix the incident calls for: the measured incident case (one
`ensure_available` call fetching 162,919 objects for a caller that needed a
small K-sized subset) becomes structurally impossible after this change,
since the wire request now literally contains only the batch's own SHA1
`want` lines — there is no `iv_commit`/tree-walk on this path at all.

**One quantitative gap in the design's own §14 table (see Minor Finding
M-1):** §14 states "1,000,000 stored → K≈100 → 1 HTTP request" without also
stating the K=40,000 case requires **tens of sequential HTTP round-trips
per single Stage-By-Filter action** (the table's own 40,000-row is
`N`=40,000 with an assumed *typical* `K`≈200, not the `K`=40,000 case this
review's brief explicitly asks to model). Both are true, and neither
represents a defect — but the design doc does not currently disclose the
"K itself is 40,000" worst-case round-trip count anywhere. See M-1.

### 2.2 Extraction fidelity — behavioral equivalence of the loop

Confirmed byte-for-byte: the design's §6.1 extraction boundary
(`deduplicate_sha1s` → WHILE → `take_next_batch`/`materialize_batch`/
`calculate_next_batch_size`, TRY/CATCH client close) is drawn at exactly the
same lexical boundary already independently verified by the protocol review
(§1) and re-confirmed here by direct line-range inspection. No additional
copy, no additional SQL, no additional HTTP round-trip is introduced by
moving this code into a separate method — it is the same statements, same
call order, same client lifetime (one `init_materialize_client` per
top-level invocation, closed exactly once on the success or failure path).
`materialize_tip_snapshot` calling the extracted method instead of running
the loop inline is a pure control-flow indirection (one additional CALL
METHOD dispatch per invocation, not per batch) — performance-irrelevant.

### 2.3 New call-site aggregate cost profile (`ensure_available`, once per Stage-By-Filter action)

`ensure_available`'s Step 1 short-circuit (`IF lt_missing IS INITIAL. RETURN.`)
is **unchanged** by this design (confirmed in §6.2 of the design doc and in
the current source). This means:
- A Stage-By-Filter action against an already-fully-buffered branch/filter
  set pays **zero** HTTP cost (no client, no info/refs GET, no POST) —
  identical to today's behavior.
- Only when Step 1 finds a genuinely missing subset does
  `materialize_missing_batches` create ONE client (one info/refs GET) and
  issue `ceil(K/batch)` POSTs on that same client before closing it. This
  is the same "one client per invocation" shape `ensure_available`'s
  current (pre-fix) `upload_pack_by_commit` call already has — no new
  per-action client setup/teardown overhead is introduced relative to
  baseline; the only change is that a large-K invocation may now issue
  multiple POSTs on that one client instead of one POST, which is strictly
  the intended, bounded trade-off replacing an unbounded single fetch.

No new aggregate cost class is introduced. Acceptable as designed.

### 2.4 K-not-N invariant — no per-object SQL/HTTP anywhere in the new path

Confirmed no per-object SQL: `get_missing_sha1s`/`get_present_sha1s` (Steps
1/4, unchanged) and `store_objects`/`decode_streaming`'s persistence
(inside `materialize_batch`, unchanged) are all bulk/chunked, never
looping a single-row SQL statement per SHA1. Confirmed no per-object HTTP:
`take_next_batch` groups up to `c_batch_rows_max` (1000) SHA1s into one
`want`-list POST; the only per-object work is CPU-bound (building want
lines via string concatenation, decoding the batch's own pack) — no
network or DB round-trip scales with individual object count beyond the
batch/chunk boundary. Recursive `materialize_batch` splits only occur on
oversized batches (rare, bounded to `c_max_oversize_splits`=10 halvings),
never as a normal per-object degeneration path.

### 2.5 Hot-path frequency concern (`build_files_from_rows` vs. `materialize_tip_snapshot`)

`build_files_from_rows` calls `ensure_available` once per Stage-By-Filter
action (confirmed §1 above, not per row). Combined with §2.3's short-circuit
analysis, the "far more frequent" call-site concern reduces to: does
repeated invocation (once per filter action, potentially many times per
user session) create cumulative overhead? Each invocation that actually
needs a fetch pays exactly one info/refs handshake — the same per-call cost
`materialize_tip_snapshot` already pays once per cold-branch-init, and the
same cost `ensure_available`'s own current (pre-fix) implementation already
pays via `upload_pack_by_commit`'s internal client setup. **No new
mitigation is required to approve this design.** A client-reuse
optimization (authenticate once per session/URL, reuse across multiple
Stage-By-Filter actions) is a legitimate **non-blocking future
improvement**, not a design defect — noted as M-2, consistent with the
task brief's framing ("note as a non-blocking future item").

### 2.6 §14 performance model plausibility

Verified against the actual constants and code:
- N=1, K=1 → 1 batch: correct, trivially fits `c_batch_rows_initial`=500.
- N=1,000, K≈20 → 1 batch: correct, fits initial batch.
- N=40,000, K≈200 → 1 batch: correct, fits initial batch (200 < 500).
- N=1,000,000, K≈100 → 1 batch: correct, fits initial batch; SQL scoped to
  `repo_key` + `obj_sha1 IN (K set)`, confirmed unaffected by the other
  999,900 rows (§1 evidence, `get_present_sha1s`).
- "Rows/bytes fetched (before fix)" column's citation of the measured
  incident (162,919 objects for one commit fetch) is accurate — directly
  matches `.memory/incidents/variant_b_d2_it8_system_no_roll_timeout.md`
  §7's measured `ZAOG_OBJ_STORE` breakdown for the retry's pack
  (162,919 total: 132,963 `status=D` + 24,111 blob + 3,555 commit + 2,290
  tree, `status=R`).

The model is quantitatively plausible and consistent with the source and
incident evidence. Its one omission (K itself at 40,000, not N at 40,000)
is documentation completeness, not a numerical error — M-1.

## 3. SQL/HTTP/memory summary

- SQL: O(K/chunk) bulk, chunked via existing `c_select_package_size`-bounded
  range tables; zero new SQL statements introduced by this design; no
  statement scans or loads based on `N`.
- HTTP: O(1) common case (K fits one batch, the overwhelming majority of
  real Stage-By-Filter selections per the design's own "tens to low
  hundreds" characterization); O(ceil(K/adaptive_batch_rows)) worst case,
  never O(K) individual per-object requests, never O(N).
- Memory: per-batch response bounded to `c_max_batch_response_bytes` (25
  MiB hard ceiling, oversize-split down to a single-object `RAISE` rather
  than an unbounded accept); no structure in this call path holds more than
  one batch's decoded pack at a time (matches `materialize_tip_snapshot`'s
  already-reviewed memory shape — this design adds no new resident
  structure).

## 4. Findings

| ID | Severity | Finding |
| --- | --- | --- |
| M-1 | MINOR | §14's performance table does not disclose the K=40,000 worst case (as opposed to N=40,000 with a typical small K) — that scenario requires dozens of sequential HTTP round-trips within one dialog step. Not a defect (still O(K), still bounded, still correct), but should be stated explicitly so a future reader does not assume "1 HTTP request" is universal for every K. Recommend adding one sentence to §14 disclosing expected round-trip count for large-K Stage-By-Filter selections. Non-blocking. |
| M-2 | MINOR | No client-reuse optimization across repeated `ensure_available` invocations within one session (each fetch-needed call re-authenticates via one info/refs handshake). Matches current baseline cost exactly (not a regression) — recommended only as a **future**, non-blocking improvement if Stage-By-Filter is found in practice to be invoked very frequently against the same URL in one session. |

No blocking or major findings. Both minor findings are documentation/future-
optimization items and do not gate implementation start.

## 5. Verdict

**APPROVE_WITH_MINOR_REVISIONS**

Conditions (non-blocking, may be applied during or shortly after
implementation, not required before starting): add the K=40,000 round-trip
disclosure to design §14 (M-1); optionally track the client-reuse idea as a
future backlog item (M-2). The core fix — extracting
`zcl_abapgit_ortec_cold_init`'s already-validated adaptive row/byte-bounded
batching loop into a new public `materialize_missing_batches` entry point
and routing `ensure_available` through it instead of the unbounded
`upload_pack_by_commit(deepen=1)` commit-graph fetch — is performance-sound,
genuinely K-not-N bounded across all four evaluated scales, introduces no
per-object SQL/HTTP, and introduces no new aggregate cost profile at its new
call site relative to baseline.
