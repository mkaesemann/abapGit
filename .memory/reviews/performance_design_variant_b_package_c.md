# Performance Design Gate — Variant B Package C (C0)

Mode: DESIGN_GATE
Task: VB-C-C0-PERFORMANCE-GATE
Baseline: `cf3d5cb87ca4eec4e68b7975df6fc127a8c5abc2` (Package B SAP_VALIDATED_COMPLETE)
Artifact reviewed: `.memory/logs/variant_b_package_c_design.md`
Reviewer evidence basis: local workspace source (`src/ortec/git/*.clas.abap`,
`src/ortec/git/zaog_commit_hist.tabl.xml`) — this is the authoritative "current
productive source" for this workspace's git-based validation model.

## Environment note (non-blocking, disclosed for transparency)

The connected arc-1 SAP system does **not** currently host
`ZCL_ABAPGIT_ORTEC_MAT_STATE` and its `ZAOG_COMMIT_HIST` DDIC table there only
has `MANDT/REPO_KEY/COMMIT_SHA1/BRANCH_NAME/FETCHED_AT` — no `HIST_LEVEL`/
`SNAP_STATE`/`ATTEMPT_ID`/`VERIFIED_AT`/`UPDATED_AT`. That system is evidently
not the environment where Package B was SAP-validated; it is not this
project's source of truth. The local workspace's
`src/ortec/git/zaog_commit_hist.tabl.xml` **does** carry the extended fields
and matches `zcl_abapgit_ortec_mat_state`'s structure usage exactly. This
review therefore treats local workspace source as ground truth, consistent
with how `.memory/state.md` and this design artifact track validation via git
commits, not the arc-1 live connection. Not a design defect; recorded so a
future reviewer does not waste time chasing a false live-system contradiction.

## Verification method

For each design section, the exact current-source method it claims to reuse
or replace was read directly (not re-derived from memory/state claims):

- `zcl_abapgit_ortec_mat_state=>get_state` / `begin_attempt` /
  `mark_graph_complete` / `mark_full_complete` / `publish_snapshot_complete` —
  read in full (lines 1-345). Confirmed: `get_state` is one
  `SELECT SINGLE * ... WHERE repo_key = ... AND commit_sha1 = ...` against
  `ZAOG_COMMIT_HIST` (both leading primary-key columns) — O(1) keyed read, no
  traversal, no loop.
- `zcl_abapgit_ortec_obj_store=>verify_tree_closure` (lines 689-763) — a
  level-by-level, blob-blind, bulk-frontier BFS over trees only
  (`get_objects` called once per frontier level with a hashed hallmark
  `lt_seen_trees` set; no per-node SQL; blob chmod entries are `CONTINUE`d,
  never read). Matches skill §6 pattern exactly.
- `zcl_abapgit_ortec_obj_store=>get_tip_blob_sha1s` (lines 771-849) — same
  frontier-BFS shape, blob SHA1s collected via hashed `lt_seen_blobs`
  dedup, no payload reads for the blobs themselves (only tree payloads are
  decoded to find blob chmod entries — required to discover the blob set).
- `zcl_abapgit_ortec_obj_store=>get_missing_sha1s` (lines 1063-1090) — calls
  `get_present_sha1s` (existence-only, confirmed via its adjacent `exists`/
  `get_present_sha1s` implementation, no `obj_data` column selected) then a
  single in-memory hashed-table diff. No SQL per candidate.
- `zcl_abapgit_ortec_porcelain=>pull` / `walk` / `walk_tree` (lines 61-176) —
  confirmed the batching path only bulk-fetches blobs *not already resident*
  in `it_objects` via `zcl_abapgit_ortec_walk_prep=>fetch_blobs_bulk`
  (bounded, batched); no repository-wide scan.
- `zcl_abapgit_ortec_fastpath=>persist_pull_result` (lines 1509-1568) —
  confirmed the raw `INSERT zaog_commit_hist FROM ls_hist` (no `HIST_LEVEL`/
  `SNAP_STATE` set) exactly as Section 0 claims — the "always-empty certified
  haves in production" defect is real and current.
- `zcl_abapgit_ortec_fetch_neg=>get_have_commits` /
  `get_verified_have_commits` / `collect_ancestor_haves` /
  `is_commit_complete` (lines 87-230+) — confirmed the legacy path does **one
  bulk `SELECT obj_sha1, obj_data FROM zaog_obj_store WHERE repo_key = ... AND
  obj_type = 'commit' AND status = 'R'`, loading every ready commit's full
  payload for the repo**, then an in-memory BFS (`collect_ancestor_haves`,
  capped at 50 levels / 200 commits). This is not a per-object SQL loop, but
  it is materially heavier (full commit-payload load) than the new
  `get_certified_haves` design (small state-table read, no payloads) it
  replaces — confirms the Section 5 improvement claim.
- `zcl_abapgit_ortec_fastpath=>upload_pack` (lines 1077-1200+) — confirmed
  `get_verified_have_commits` is called once per attempt (not per candidate)
  before request assembly, and the single-XSTRING HTTP response pattern
  (`send_receive_close`) is pre-existing/already Package-B-era-audited, not
  newly introduced by Package C.

## Findings by section

### 1. Section 5 — certified-have policy: CONFIRMED, no blocking issue

`get_certified_haves` is one bulk `SELECT` against `ZAOG_COMMIT_HIST` scoped
by `repo_key` — the leading non-MANDT primary-key column, so this is a
primary-index range access, not a full table scan across repositories.
Result cardinality is H (this repo's own certified-commit row count: SHA1 +
two 1-char flags + three timestamps, never object payloads), independent of
N (total repository objects across all repos, or even all objects of this
one repo). Exclude/dedupe/sort/cap-at-50 all happen in ABAP against the
already-bounded H-row result. No per-candidate SQL, no per-candidate object
read. **Confirmed as designed.**

Minor (M2, non-blocking): the classification "any candidate" check (Section
3, step 2) invokes this same method with `iv_max_haves = 1`, but the SQL
itself has no `UP TO 1 ROWS`/row limit — it still fetches all H certified
rows for the repo before capping in ABAP. This stays O(H), not O(N), so it
does not violate the stated complexity bound, but an `EXISTS`-style or
`UP TO 1 ROWS` short-circuit would avoid transferring H rows just to answer a
yes/no question. Optional efficiency improvement, not required for approval.

### 2. Section 3 — classification: CONFIRMED, no blocking issue

Exactly one `SELECT SINGLE` (`mat_state=>get_state`, O(1) keyed) plus, only
when `snap_state <> COMPLETE`, one reused bulk call to
`get_certified_haves`. No additional or new query shape is introduced by
classification itself. **Confirmed as designed.**

### 3. Section 6 — incremental certification lifecycle: CONFIRMED, no blocking issue

`verify_tree_closure`, `get_tip_blob_sha1s`, and `get_missing_sha1s` are all
pre-existing, already-approved (Package B) bulk/frontier APIs — independently
verified above, no new query shape, no per-blob SQL. The design places
`begin_attempt` → `verify_tree_closure` → `mark_graph_complete` →
`get_tip_blob_sha1s`/`get_missing_sha1s` → `mark_full_complete` →
`publish_snapshot_complete` → `update_after_fetch`, then a single `COMMIT
WORK` — identical shape to `persist_pull_result`'s existing single-commit
position (confirmed at line ~1568). No per-object commit is introduced.
**Confirmed as designed.**

### 4. Section 4 WARM_UNCHANGED path: CONFIRMED with one completeness gap (M1)

Confirmed zero upload-pack POST — WARM_UNCHANGED only needs the prior
lightweight `info/refs` GET (branch-tip resolution, Section 2), already an
existing, separate, cheap call. Confirmed `pull`/`walk`/`walk_tree`'s
blob-materialization path is bounded to the batching logic
(`fetch_blobs_bulk`, bulk + deduped), scoped to the reachable tree of the one
target commit (K), not a repository-wide scan.

**M1 (minor, non-blocking):** the design does not specify how the *initial
commit object itself* is supplied to `pull()`'s `it_objects` parameter for
WARM_UNCHANGED. Today, `pull()` unconditionally expects the commit object to
already be present in `it_objects` (`READ TABLE it_objects WITH KEY
type=commit sha1=iv_commit`, raising `'Commit/Branch not found.'` if absent) —
in every existing caller this table originates from the just-fetched HTTP
pack. For WARM_UNCHANGED there is no HTTP fetch, so Package C must seed
`it_objects` with at least that one commit object via a bounded single-key
bulk read (e.g. `obj_store=>get_objects` with a one-element SHA1 list — the
same primitive `verify_tree_closure`/`get_tip_blob_sha1s` already use).
Recommend the design explicitly state this one-object bulk-seed step so an
implementer does not substitute a repository-wide read (e.g. `get_all_objects`)
to "make `pull()` happy" — that would silently turn WARM_UNCHANGED into an
O(N) path, defeating its entire purpose. This is a design-completeness gap,
not a currently-specified performance violation.

### 5. Memory-risk gate: CONFIRMED, no new unbounded path

WARM_UNCHANGED issues no HTTP POST at all. INCREMENTAL_UPDATE's single-XSTRING
`send_receive_close` response handling is pre-existing (confirmed at
`upload_pack`, unchanged by Package C except the haves source). COLD_BRANCH
reuses `acquire_blobless_graph`/`materialize_tip_snapshot` verbatim (Package
B contract, already audited/approved — not re-derived here). No new
full-response-as-one-XSTRING path is introduced by Package C. **Confirmed.**

### 6. Retry bound: CONFIRMED, no reintroduced unbounded retry

The existing Slice 2C wire cascade (thin → self-contained → ≤1 recovery) is
unchanged; Package C only swaps the have-source feeding it. No `deepen`/
`shallow` lines are added (unchanged call sites). No fourth tier, no
progressive-deepen reintroduction (confirmed `first_progressive_deepen`/
`next_progressive_deepen` remain unreferenced by any migrated symbol per the
migration map, consistent with prior Slice 2C findings).

**M3 (minor, documentation-only, non-blocking):** `zcl_abapgit_ortec_porcelain
=>pull_by_branch` (the live orchestration owner, lines 177-276) has its own
pre-existing, separate walk-failure repair retry (`invalidate_all_history` +
one full re-fetch via `upload_pack_by_branch`, gated on the pull error text
containing `'Walk,'`). This is unrelated to and unchanged by Package C, and
is already bounded to one extra attempt — but Section 7's retry accounting
does not mention it. Recommend a one-line addition to Section 7 noting this
pre-existing, orthogonal, already-bounded retry tier exists so a future
reader does not need to rediscover it via source archaeology.

## Complexity summary

- H = this repo's own certified-commit row count (`ZAOG_COMMIT_HIST`,
  `hist_level IN ('G','F')`) — small metadata rows, no payloads. All
  Section 5/3 have-policy SQL is O(H), one round trip.
- K = Package B B2/B3-approved graph/blob scope (commit's reachable tree +
  tip blob set) — unchanged, reused verbatim by `verify_tree_closure`/
  `get_tip_blob_sha1s`/`get_missing_sha1s` and by WARM_UNCHANGED's local
  `pull`/`walk`/`walk_tree` reconstruction.
- R = retry tiers: unchanged existing bound (≤2 wire tiers + 1 recovery for
  INCREMENTAL_UPDATE; 0 extra for WARM_UNCHANGED/COLD_BRANCH), no new tier
  added by Package C.
- B = bytes: no new full-response XSTRING materialization path; existing
  Package B/Slice 2C memory-risk-gated paths reused unchanged.

No path in this design is O(N total repository objects) or O(N total
repositories). No per-candidate/per-object SQL or HTTP was found anywhere in
the reused or newly-specified call chain.

## Verdict

**APPROVE_WITH_MINOR_REVISIONS**

Blocking findings: none.
Major findings: none.
Minor findings: M1 (WARM_UNCHANGED single-commit-object seed step should be
made explicit to prevent an accidental repo-wide read substitute), M2
(optional EXISTS/`UP TO 1 ROWS` short-circuit for the classification
"any-candidate" call), M3 (Section 7 should mention the pre-existing,
unrelated, already-bounded walk-failure retry tier for completeness).

None of M1–M3 block C1/C2 implementation start; M1 should be resolved before
or during C2 implementation (it affects an actual code path), M2/M3 are
documentation/optimization notes only.
