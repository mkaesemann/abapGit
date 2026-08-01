# abapGit — Git Handling and Object Serialization Performance Discovery

```text
PACKET=COMPACT_HANDOFF_V1
TASK=ABAPGIT_GIT_SERIALIZATION_PERFORMANCE_DISCOVERY
PHASE=DISCOVERY (workstreams A-D)
SCOPE=READ_ONLY, no productive/DDIC/test/.github/diagram/.memory/state.md change
```

Evidence priority used (per task brief): current source > current IT8 traces
(none available this session) > SAT/ST05/dump evidence (existing artifacts
only, no new capture) > focused Package C/D/E handoffs/incidents > design
prose > hypotheses. Every material statement below is tagged
`MEASURED | SOURCE_CONFIRMED | OWNER_OBSERVED | HYPOTHESIS | UNKNOWN |
SUPERSEDED`.

## 0. Baseline verification

```text
CURRENT_HEAD=36839c7faa55b4568c1724cf6232468957f6aac8 (branch
  ortec/abapgit_1_133-opt-rework) — SOURCE_CONFIRMED (git rev-parse HEAD)
SAP_VALIDATED_PRODUCTIVE_HEAD=3c77d898b62f3b0464e48cf59844e2e30c7b6e89
  (.memory/state.md PACKAGE_E_CHECKPOINT_1_VALIDATED_HEAD, IT8-validated
  2026-07-29: activation/syntax/ABAP-Unit/ATC all PASS) — OWNER_OBSERVED
MEMORY_ONLY_HEADS_AFTER_BASELINE=12 commits between 3c77d898 and
  36839c7f (1437a875, 8b382a5e, 77b66464, 9972e240, 9a223b13, 7b0115b8,
  0c0f568b, e04ad8c1, 2f27e700, 29b52d16, d1427c98, 341d4cc8, 5b32a775) —
  SOURCE_CONFIRMED (git log 3c77d898..HEAD). These are NOT individually
  IT8-validated per .memory/state.md; state.md records E3_CACHE_ADMIN_F4 as
  "COMPLETED 2026-07-31 (owner-confirmed, manually fixed/verified)" but does
  not pin an exact validated commit SHA1 for that fix.
WORKING_TREE=dirty, unrelated to this task: modified
  .github/agents/00_ortec_abapgit_orchestrator.agent.md,
  .github/agents/03_design_agent.agent.md; untracked
  .github/agents/03c_adversarial_design_review.agent.md and 7
  .memory/{handoffs,logs,reviews}/variant_b_package_e_e1_tree_reuse_*.md
  files plus 2 root-level .txt files — SOURCE_CONFIRMED (git status
  --porcelain). Not touched by this discovery.
OBJ_INDEX_WRITE_BATCH_SIZE=30000 (c_index_write_chunk_size,
  src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap:75) —
  SOURCE_CONFIRMED, current/live value. This does NOT match the value
  state.md's E1_OBJINDEX_PERFORMANCE narrative documents as the "fixed
  contract" (5000); state.md itself already records this exact
  discrepancy as an unresolved, owner-postponed E1 working hypothesis
  (POSTPONED 2026-07-31). Not re-litigated here — see §7.
ALREADY_IMPLEMENTED_PERF_WORK (confirmed in current source, must not be
  re-proposed as new):
  - Variant B Package B/C: blobless cold-branch graph acquisition (no
    deepen/shallow), adaptive byte/row-bounded MATERIALIZE_BLOBS batching,
    certified-have policy replacing tree-walk-based have negotiation
    (SOURCE_CONFIRMED, §2).
  - Bulk, level-by-level (not per-object) commit/tree/blob object-store
    reads in zcl_abapgit_ortec_obj_store=>get_reachable_objects/
    get_reachable_sha1s/verify_tree_closure/get_tip_blob_sha1s
    (SOURCE_CONFIRMED, §2). The prior full-repository `populate_cache`
    preload that caused the D2 SYSTEM_NO_ROLL incident is already removed
    (comment left in place as a regression guardrail,
    zcl_abapgit_ortec_obj_store.clas.abap ~line 718-745).
  - zcl_abapgit_ortec_cts_buffer: bulk TLOCK/E070/E071 transport
    resolution, replacing "16K+ DB accesses" with "~2 DB accesses total"
    per its own class doc (SOURCE_CONFIRMED, §3).
  - zcl_abapgit_ortec_bulk_exists: bulk existence checks for
    TABL/DTEL/CLAS/INTF/DOMA/DSYS/FUGR/MSAG/PROG/SHLP/SMIM/TOBJ/TRAN/TTYP
    TADIR rows, replacing per-object `zcl_abapgit_objects=>exists` calls
    (SOURCE_CONFIRMED, §3).
  - zcl_abapgit_ortec_ser_pref / _ext / _oo: per-run prefetch buffers
    (MSAG/T100/T100T, DOKIL, DTEL/FUGR/PROG/SMIM/TOBJ/TRAN/ENHS text and
    metadata, CLAS/INTF OO descriptions) consumed by both sequential and
    parallel serialization, with per-object buffer extraction for RFC
    worker injection (SOURCE_CONFIRMED, §3).
  - E-HARDEN OF-2 (shared 'Walk,' error-prefix constant) and E1-TEST/
    E3-TEST/E4-VERIFY: SAP_VALIDATED_COMPLETE per state.md, not
    re-analyzed here.
```

## 1. Workstream A — entry-point / phase inventory

Only operations with direct source evidence gathered this pass are filled in
with confidence; the remainder are marked `NOT_INVESTIGATED` rather than
guessed, per the task's evidence discipline. Deeper per-operation phase
tables for the NOT_INVESTIGATED rows are `MEASURE_FIRST`/follow-up scope
(see backlog §E).

### Single-object Diff / Stage-by-Transport / Stage with object filter
`SOURCE_CONFIRMED` — these three share one facade:

```text
ENTRY_POINT        zcl_abapgit_ortec_git_facade=>resolve_filtered_remote
                    -> zcl_abapgit_ortec_filter_walk=>get_remote_files_for_stage
                    (wired from zcl_abapgit_stage_logic=>zif_abapgit_stage_logic~get
                    via a dynamic CALL METHOD ('ZCL_ABAPGIT_ORTEC_GIT_FACADE'))
REMOTE_DISCOVERY    zcl_abapgit_git_transport=>branches( url ) — always an
                    info/refs HTTP GET, no caching at transport level
                    (zcl_abapgit_git_transport.clas.abap:131-138,
                    zcl_abapgit_ortec_filter_walk.clas.abap:132) — only
                    reached on the "no usable warm index for this
                    branch/commit" path, not on every call
GIT_FETCH_OR_NO_FETCH  Conditional: zcl_abapgit_ortec_fastpath=>
                    try_filtered_commit_fetch (blob:none-style) only when
                    the branch tip moved/was never seen; otherwise NO fetch
PACK_DECODE         Only on the above conditional fetch path
OBJECT_STORE        zcl_abapgit_ortec_obj_store=>get_objects(iv_bulk_fetch=
                    abap_true), bulk/chunked, never per-object
                    (zcl_abapgit_ortec_obj_index.clas.abap build_files_from_rows)
TREE_OR_INDEX_BUILD zcl_abapgit_ortec_obj_index=>ensure_index/rebuild_index —
                    ONE full per-commit tree walk + DDIC classification
                    (file_to_object) the FIRST time a given commit SHA1 is
                    used by ANY filtered caller; O(1) FOR ALL ENTRIES SELECT
                    on every subsequent call for the SAME commit (§2.4)
LOCAL_OBJECT_DISCOVERY  N/A (remote-side only for this facade)
LOCAL_SERIALIZATION N/A (remote-side only; local side handled by
                    zcl_abapgit_repo~get_files_local_filtered, see §3)
HASHING             zcl_abapgit_hash=>sha1_string per file path, once per
                    row during rebuild_index only (not on the fast
                    FOR ALL ENTRIES read path)
STATUS_CALCULATION  zcl_abapgit_repo_status=>calculate( it_remote = ... ) —
                    reuses the already-resolved filtered remote set,
                    explicitly documented to avoid a second remote fetch
                    (zcl_abapgit_repo_status.clas.abap:58-64)
UI_LIST_BUILD       NOT_INVESTIGATED this pass
PERSISTENCE         ZAOG_OBJ_INDEX (per-commit), ZAOG_OBJ_STORE (read-only
                    here)
HTTP_CALLS          0 when the caller already has a pinned commit
                    (get_selected_commit() non-blank — the whole tip-
                    resolution block is skipped). Exactly 1 info/refs GET
                    to ESTABLISH freshness when resolved via branch name,
                    even if the index turns out to already be warm/fresh;
                    1 GET + conditional fetch when cold/moved (distinction
                    per performance-review Finding 2)
DB_ROUNDTROUNDTRIPS 1 FOR ALL ENTRIES SELECT on ZAOG_OBJ_INDEX + 1 bulk
                    SELECT on ZAOG_OBJ_STORE for the filtered blob set,
                    once index exists — SOURCE_CONFIRMED
MEMORY_DOMINANT     Filtered blob payload set only (K-sized), not the full
                    commit tree — SOURCE_CONFIRMED
REPEATED_WORK       See §2.2 (redundant branches() calls) and §2.4
                    (per-commit index rebuild not reused across sibling
                    commits/branches, tracked as E1-TREE-REUSE)
```

### Branch pull (warm unchanged / incremental update / cold branch)
`SOURCE_CONFIRMED` from `zcl_abapgit_ortec_porcelain=>pull_by_branch`
(src/ortec/git/zcl_abapgit_ortec_porcelain.clas.abap:150-330):

```text
ENTRY_POINT         zcl_abapgit_ortec_porcelain=>pull_by_branch
REMOTE_DISCOVERY    zcl_abapgit_git_transport=>branches(url)->find_by_name
                    to resolve the advertised tip BEFORE classification —
                    the class's OWN inline comment calls this
                    "disclosed-but-accepted redundant with the cascade's
                    own GET below" (line ~196-199) — OWNER_OBSERVED
                    (documented in source by the implementer), not a new
                    finding
GIT_FETCH_OR_NO_FETCH  WARM_UNCHANGED: none. COLD_BRANCH: one
                    acquire_blobless_graph (no deepen/shallow) +
                    materialize_tip_snapshot (adaptive batches).
                    INCREMENTAL_UPDATE: existing thin -> non-thin ->
                    recovery cascade via zcl_abapgit_ortec_fastpath,
                    now additionally offering ZCL_ABAPGIT_ORTEC_HAVE_POLICY
                    certified haves
PACK_DECODE         zcl_abapgit_ortec_pack_dec (streaming, chunked
                    COMMIT WORK every ~50 objects, resumable)
OBJECT_STORE        Bulk chunked reads/writes throughout (§2.1-§2.3)
TREE_OR_INDEX_BUILD Deferred to the first filtered caller (Diff/Stage),
                    not built during pull itself
LOCAL_OBJECT_DISCOVERY N/A here (remote pull only)
LOCAL_SERIALIZATION N/A here
HASHING             Handled inside pack decode/persist (existing,
                    NOT_RE-AUDITED this pass)
STATUS_CALCULATION  N/A here
PERSISTENCE         ZAOG_OBJ_STORE, ZAOG_COMMIT_HIST, ZAOG_REPO_STATE via
                    zcl_abapgit_ortec_fastpath=>persist_pull_result, guarded
                    by an ORTEC repo lock + attempt id
                    (zcl_abapgit_ortec_porcelain.clas.abap:300-330)
HTTP_CALLS          WARM_UNCHANGED: 1 (branches info/refs) + 0 fetch.
                    COLD_BRANCH: 1 (branches) + 1 (blobless graph) + N
                    adaptive MATERIALIZE_BLOBS batches (bounded, see
                    zcl_abapgit_ortec_cold_init constants). INCREMENTAL:
                    1 (branches) + upload-pack cascade (1-3 POSTs)
DB_ROUNDTRIPS       See AUDIT-M-1 (existing MAJOR, not blocking, same-repo
                    lock contention — OWNER_OBSERVED, already recorded in
                    .memory/logs/performance_audit_variant_b_package_d2.md,
                    not re-derived here)
MEMORY_DOMINANT     Single materialized HTTP response XSTRING per batch,
                    bounded by c_max_graph_response_bytes (200MB) /
                    c_max_batch_response_bytes (25MB) — SOURCE_CONFIRMED
                    ceilings exist; not independently load-tested this pass
REPEATED_WORK       The branches() call described above; see §2.2
```

### Full Stage, repository overview/refresh, Commit/push, post-commit return
`NOT_INVESTIGATED` to full call-graph depth this pass (time-boxed; the
facade-level entry points above account for the highest-value, most
frequently exercised paths: single-object Diff and Stage-by-Transport).
Structural evidence gathered and directly relevant to these operations:

- `zif_abapgit_repo~refresh` unconditionally sets
  `mv_request_local_refresh = abap_true` and calls `reset_remote()`
  (clears `mt_remote`) (src/repo/zcl_abapgit_repo.clas.abap:796-810) —
  `SOURCE_CONFIRMED`. The next `get_files_local()` call after any
  `refresh()` therefore re-runs the FULL, non-filtered
  `zcl_abapgit_serialize=>files_local` for the whole package tree,
  regardless of how many objects actually changed — `SOURCE_CONFIRMED`
  structural fact (see §3.3). Whether `refresh()` is invoked as part of
  the specific "return to repository overview after a successful
  commit/push" navigation was **not confirmed** in this pass — the
  `refresh()` call sites found (`zcl_abapgit_gui_page_pull`,
  `zip_export_transport`, error-recovery branches in
  `zcl_abapgit_gui_page_stage`/`zcl_abapgit_gui_router`) are Pull/zip/
  error paths, not a located post-push success handler. `UNKNOWN` —
  see backlog measurement M-4.
- `SYSTEM_NO_ROLL-OS4-STAGE-AFTER-OVERVIEW` (already tracked in
  `.memory/state.md` Deferred topics, OWNER_OBSERVED, not reproduced or
  re-derived here): a dump seen when Full Stage runs immediately after a
  full Overview serialize of a 17,321-object repo. Cross-referenced, not
  re-investigated (explicit non-goal: do not reopen Package E incidents).

## 2. Workstream B — Git handling findings

### 2.1 Object-store bulk reads (tree walk / reachability)
`SOURCE_CONFIRMED`. `zcl_abapgit_ortec_obj_store=>get_reachable_objects`
and `get_reachable_sha1s` walk commit -> tree -> blob strictly level by
level, issuing exactly one `get_objects(iv_bulk_fetch=abap_true)` bulk read
per tree level (frontier), never per node
(zcl_abapgit_ortec_obj_store.clas.abap:718-956). `get_reachable_sha1s`
additionally never loads blob payload data, only existence
(`get_present_sha1s`), by design — this is the class's own documented
reason for the method's existence. A prior full-repository
`populate_cache` preload that caused a `SYSTEM_NO_ROLL` dump at ~54k
objects / ~3.96GB (incident `variant_b_d2_it8_system_no_roll_timeout`) has
already been removed from this call path; the removal comment is left
in-place as a regression guardrail. **No action proposed — already fixed.**

### 2.2 Redundant remote-tip resolution (`branches()` / info-refs)
`SOURCE_CONFIRMED`, real and currently unaddressed:

- `zcl_abapgit_git_transport=>branches(iv_url)` always performs a fresh
  `info/refs?service=git-upload-pack` HTTP GET
  (zcl_abapgit_git_transport.clas.abap:131-166) — there is no
  session/request-scoped cache at this layer for ANY caller, standard or
  ORTEC.
- `zcl_abapgit_ortec_porcelain=>pull_by_branch` calls it once to classify
  the operation, with the implementer's own comment (line 191, corrected
  from an earlier ~196-199 citation) acknowledging it is
  "disclosed-but-accepted redundant with the cascade's own GET below."
  **Correction (per performance-review Finding 1):** that "cascade's own
  GET" does NOT run inside `zcl_abapgit_git_transport=>upload_pack_by_
  branch`'s standard `find_branch` fallthrough — that fallthrough is
  unreachable for any ORTEC-active repo, since the method's own `TRY`
  block around `zcl_abapgit_ortec_fastpath=>upload_pack_by_branch` either
  `RETURN`s on success or `RAISE`s on failure with no fallthrough
  (`zcl_abapgit_git_transport.clas.abap:412-446`, `SOURCE_CONFIRMED`). The
  real redundant call lives inside `zcl_abapgit_ortec_fastpath` itself:
  its `upload_pack_by_branch` (line 886) first calls its OWN, separate
  `pull_by_branch` method (line 637), which performs its own independent
  `zcl_abapgit_git_transport=>branches(iv_url)` call (line 673); if that
  does not short-circuit, `upload_pack_by_branch` then calls
  `zcl_abapgit_git_transport=>find_branch_ortec` (another info/refs-style
  connection) for the thin-pack attempt, and again inside its own
  self-contained-retry `CATCH` block on a thin-pack failure —
  `SOURCE_CONFIRMED` via direct read of
  `zcl_abapgit_ortec_fastpath.clas.abap:637-1000`. Neither
  `zcl_abapgit_ortec_fastpath=>pull_by_branch` nor this internal cascade
  was listed in the Workstream A branch-pull entry-point table above; that
  table only documents the outer `zcl_abapgit_ortec_porcelain=>
  pull_by_branch` entry point.
- `zcl_abapgit_ortec_filter_walk=>get_remote_files_for_stage` performs its
  OWN independent `branches(lv_url)` call (line 132) to check whether the
  cached branch/commit state is stale, on the "no usable warm index for
  this branch/commit" path.
- Net effect: a single user action (e.g. pull then immediately open Diff)
  can trigger at least 3, plausibly 4, independent info/refs-class HTTP
  round trips for the SAME repository/branch across
  `zcl_abapgit_ortec_porcelain`, `zcl_abapgit_ortec_fastpath` (twice), and
  `zcl_abapgit_ortec_filter_walk` — none shared with each other or with
  any standard abapGit branch-list cache. This is a stronger version of
  the original finding, not a weaker one; only the attributed call site
  was wrong.
- This overlaps, but is analytically distinct from, the already-tracked
  `E2_CONSUMER_COHERENCE` / OS4 `PC-1` correctness hypothesis in
  `.memory/state.md` (stale `mt_remote` vs. ORTEC facade revalidation).
  That investigation's own D1 log
  (`.memory/logs/variant_b_package_e_false_modified_os4_d1.md`) explicitly
  **rejects** PC-1 as the OS4 root cause ("Stage-by-Transport doesn't even
  use `mt_remote`/`get_files_remote`... yet still reproduces the bug").
  The finding here is a **pure network-cost** observation (extra HTTP
  round trips per user action), independent of that correctness question,
  and is NOT proposing to reopen or re-decide E2. `E2_CONSUMER_COHERENCE`
  remains POSTPONED per owner decision 2026-07-31; no code change is
  proposed here for it.

### 2.3 External delta-base loading and pack decode
`SOURCE_CONFIRMED`, already bulk. `zcl_abapgit_ortec_pack_dec` loads
external (out-of-pack) delta bases via one `SELECT ... FOR ALL ENTRIES`
prefetch before `resolve_all`'s own resolution loop
(zcl_abapgit_ortec_pack_dec.clas.abap:1273-1290), not a per-delta singleton
read. Streaming decode persists/commits in bounded chunks (~50 objects,
`_scope='2'` session-survival documented). **No action proposed —
already implemented correctly per the class's own design comments; not
independently re-measured at 100k+-object scale this pass** (see
measurement M-2).

### 2.4 Per-commit filtered index (`ZAOG_OBJ_INDEX`) rebuild cost
`SOURCE_CONFIRMED`. `zcl_abapgit_ortec_obj_index=>rebuild_index` walks the
ENTIRE commit tree (every directory level, every file), classifies every
file via `zcl_abapgit_filename_logic=>file_to_object`, and computes a SHA1
path hash per file — for the FULL commit, not the caller's filter — the
first time ANY filtered caller (Stage-by-Transport OR single-object Diff)
touches a given commit SHA1 (`is_index_ready` gates on an explicit
completion marker row). Subsequent filtered calls against the SAME commit
reuse the index via one `FOR ALL ENTRIES` `SELECT` — `SOURCE_CONFIRMED`,
already efficient for repeat use. This means:
- Cost scales with **N** (total files in the commit tree) on the first
  filtered access to a NEW commit, even when the caller only needs **K**
  (e.g. 5 objects in one transport).
- Cost is amortized (O(K) via the index) for every subsequent filtered
  call against that SAME commit SHA1.
- This is the exact mechanism already tracked as the deferred
  `E1-TREE-REUSE` topic in `.memory/state.md` ("tree-SHA1-keyed or
  incremental-diff `zaog_obj_index` row reuse across commits... PROVEN
  UNSAFE as a bare tree-SHA1 key"). **Not re-opened here** — cited as
  existing evidence for why a cross-commit reuse candidate is
  `DESIGN_REQUIRED`, not `IMPLEMENT_NEXT` (see backlog G-2).

### 2.5 Not investigated this pass
Branch-list caching UI-side reuse, receive-pack/push wire construction,
Stage-by-Transport's exact HTTP count end-to-end, and REF-vs-OFS delta
type distribution were not traced to source this pass — `UNKNOWN`, not
claimed either way.

## 3. Workstream C — local serialization findings

### 3.1 TADIR discovery reads the full package tree before filtering
`SOURCE_CONFIRMED`. `zif_abapgit_tadir~read` (via `build`/`select_objects`,
src/objects/core/zcl_abapgit_tadir.clas.abap:271-330) always issues
`SELECT * FROM tadir ... WHERE devclass IN (package + subpackages)` for
the WHOLE package tree — `it_filter` is applied strictly AFTER this SELECT,
as an in-memory `LOOP ... READ TABLE ... BINARY SEARCH ... DELETE`
(lines 499-513), and the standard `check_exists` (bulk-exists path) runs
on the ALREADY-filtered, narrowed list (`SOURCE_CONFIRMED`, correct
ordering). Net effect: for Stage-by-Transport with a small transport, the
TADIR SQL cost is O(N) (whole package tree), while the (already-batched)
existence-check and (already-narrowed, see §3.2) serializer-creation cost
is O(K). TADIR is a well-indexed dictionary table; this is a genuine but
likely low-severity cost at today's typical package sizes — flagged as
`MEASURE_FIRST`, not asserted as a proven bottleneck (no trace evidence
this pass).

### 3.2 Serializer creation IS already filtered before execution
`SOURCE_CONFIRMED`, answers one of the mandatory challenge questions
directly. `zcl_abapgit_serialize=>add_objects` passes the ALREADY-narrowed
`lt_tadir` (post `it_filter` + `lo_filter->apply`) into `serialize(
it_tadir = lt_tadir )` (zcl_abapgit_serialize.clas.abap:228-254) — the
actual expensive per-object DDIC/source generation loop
(`run_sequential`/`run_parallel`) only ever iterates the filtered K-sized
list for `get_files_local_filtered` callers (Stage-by-Transport,
`zip_export_transport`, single-object patch/diff). **No action proposed —
already correct.**

### 3.3 `refresh()` forces full, non-incremental local re-serialization
`SOURCE_CONFIRMED`. `zif_abapgit_repo~refresh` unconditionally sets
`mv_request_local_refresh = abap_true` (src/repo/zcl_abapgit_repo.clas.abap:
796-802). `zif_abapgit_repo~get_files_local`'s cache short-circuit
(`IF lines( mt_local ) > 0 AND mv_request_local_refresh = abap_false`)
is bypassed whenever this flag is set, regardless of `iv_drop_cache` and
regardless of `mt_local` still holding valid content — the next
non-filtered `get_files_local()` call re-runs `files_local()` for
**every** object in the package tree (N), not just objects that actually
changed. There is no K-sized/incremental local re-serialization path for
"a few objects changed, re-serialize only those." Whether/how often this
fires on the highest-value trigger (a normal commit/push success
returning to the repository overview) was **not confirmed** this pass —
see measurement M-4. The existing per-type prefetch buffers (§0, ser_pref
family) reduce the PER-OBJECT DB cost of that full re-serialization, but
do not eliminate the O(N) object count.

### 3.4 Already-implemented bulk local-serialization support
`SOURCE_CONFIRMED`, cross-referenced from §0/mandatory-challenge answers:
- `zcl_abapgit_ortec_cts_buffer=>determine_transports_bulk` still calls
  `CALL FUNCTION 'TR_CHECK_TYPE'` once per repository item inside a LOOP
  (not a DB call, a lockable-type classification FM) — `SOURCE_CONFIRMED`,
  low severity, flagged `MEASURE_FIRST` only at very large N (40k+), no
  DB/HTTP cost.
- `zcl_abapgit_ortec_ser_pref=>prepare`/`_ext`/`_oo=>prepare` run bulk
  `FOR ALL ENTRIES` reads scoped to the CURRENT (already-filtered, per
  §3.2) `it_tadir` list, not the whole repository — `SOURCE_CONFIRMED`,
  correctly K-scoped for filtered flows and N-scoped (by design, once)
  for full-repository serialization.
- Repo checksums (`zcl_abapgit_repo_checksums`) already implement a
  request-scoped cache with explicit invalidation on update/rebuild
  (`mv_cache_valid`/`mt_checksums_cached`,
  src/repo/zcl_abapgit_repo_checksums.clas.abap:30-31, 214-230,
  261-286) — `SOURCE_CONFIRMED`, already correct.

### 3.5 Parallel serialization — correctness cross-reference (not a new
performance finding, explicitly out of scope for action here)
`OWNER_OBSERVED`, already fully investigated and (per that log) fixed in
current source: `.memory/logs/variant_b_package_e_false_modified_os4_d1.md`
documents that the three ORTEC prefetch classes' `inject_from_buffer`
methods previously allowed pooled/reused RFC parallel-worker sessions to
silently retain STALE cached rows across unrelated dispatches (a
correctness bug, not a raw-throughput one), and records this as already
fixed by clearing the relevant cache table at the top of each
`inject_from_buffer`. This is cited only because Workstream C explicitly
asks about parallel-serialization correctness; it is `E2_CONSUMER_
COHERENCE`-adjacent, POSTPONED per owner decision, and **not re-analyzed,
re-tested, or proposed as new work here**.

### 3.6 Not investigated this pass
Exact FUGR/DDLS/large-object payload byte distribution, P95 payload sizes,
generated-include handling cost, and whether Full Stage's non-filtered
path re-serializes objects already unchanged since the last overview
render within the same session are `UNKNOWN` — no trace evidence
available this pass (see measurement plan M-1, M-3).

## 4. Workstream D — memory and scalability modeling

```text
STRUCTURE                          SCALES_WITH  CONTENT                 EVIDENCE
ZAOG_OBJ_INDEX write buffer         K (rows/    STRING/fixed-CHAR row,  SOURCE_CONFIRMED
  (lt_rows, rebuild_index)          commit,     no XSTRING payload,     zcl_abapgit_ortec_obj_index
                                    chunked)    ~730 bytes/row per      .clas.abap:75 comment +
                                                design comment; chunk   rebuild_index body
                                                cap 30000 rows (~21.9MB
                                                bounded per chunk)
Object-store bulk read buffers      K (frontier  XSTRING payload for    SOURCE_CONFIRMED
  (get_objects/get_reachable_*)     per level)  commit/tree data;       zcl_abapgit_ortec_obj_store
                                                get_reachable_sha1s     .clas.abap:849-956
                                                variant never loads
                                                blob XSTRING payload
HTTP response XSTRING               1 response  Single materialized    SOURCE_CONFIRMED, bounded
  (cold_init/materialize_batch)     per batch,  XSTRING per request,   by explicit constants
                                    B-bounded   ceiling 200MB (graph)/  (c_max_graph_response_bytes,
                                                25MB (batch)            c_max_batch_response_bytes)
Serializer prefetch buffers         K (current  Value tables (T100/    SOURCE_CONFIRMED
  (ser_pref/_ext/_oo CLASS-DATA)    it_tadir)   T100T/DOKIL/etc.),      zcl_abapgit_ortec_ser_pref*
                                                cleared per run via     .clas.abap
                                                clear()/prepare()
Local TADIR working set              N (whole   Metadata rows only     SOURCE_CONFIRMED
  (select_objects et_tadir)         package     (no payload)           zcl_abapgit_tadir.clas.abap
                                    tree)                               :271-330
Local serialized file set            N or K,    STRING/XSTRING file    SOURCE_CONFIRMED shape;
  (rt_files/mt_files, serialize)    depending   payloads, held         NOT measured for peak bytes
                                    on caller   entirely in memory      at 10k/40k-object scale
                                    (§3.2/3.3)  until returned to       this pass — UNKNOWN
                                                caller (stage/status)
```

```text
1,000 objects       : local serialization payload set is small (low tens
                       of MB order-of-magnitude, UNKNOWN exact), object
                       index write is 1 chunk. Not measured.
10,000 objects       : object index write likely 1 chunk (<30000 threshold);
                       TADIR full-tree SELECT still single statement.
                       Not measured.
40,000 objects       : object index write straddles ~2 chunks; full
                       local-serialization payload set size UNKNOWN
                       (flagged for measurement, see M-1/M-3). This is the
                       scale at which §3.3's O(N) refresh()-triggered
                       full re-serialization would be most costly if it
                       fires on a routine navigation.
100,000 generated
  files               : Not modeled this pass — UNKNOWN. FUGR/DDLS/
                       generated-include multi-file objects were not
                       traced to a per-object file count this pass.
1,000,000 persisted
  Git objects         : Bulk object-store reads remain frontier/level-
                       scoped per §2.1 (K-bounded per operation), not
                       N-scoped — SOURCE_CONFIRMED for the tree-walk
                       methods audited; not re-verified for every object-
                       store consumer this pass.
```

No structure identified this pass requires all local payloads or all
remote objects in memory for a K-sized (filtered/incremental) operation,
**except** §3.3's `refresh()` effect, which forces the local side back to
an N-sized (full package) re-serialization regardless of how small the
actual change was.

## 5. Cross-references (not restated, see linked artifacts)

- AUDIT-M-1 (same-repo lock contention during resumed decode, MAJOR, not
  blocking): `.memory/logs/performance_audit_variant_b_package_d2.md`
- E1-TREE-REUSE (per-commit index reuse across commits, PROVEN UNSAFE as a
  bare tree-SHA1 key): `.memory/state.md` Deferred topics
- E2_CONSUMER_COHERENCE / OS4 PC-1 rejection and parallel-worker
  stale-cache fix: `.memory/logs/variant_b_package_e_false_modified_os4_d1.md`
- SYSTEM_NO_ROLL-OS4-STAGE-AFTER-OVERVIEW (unscheduled):
  `.memory/state.md` Deferred topics
- E1_OBJINDEX_PERFORMANCE 5000-vs-30000 chunk-size discrepancy (postponed):
  `.memory/state.md`

## 6. Answers to mandatory challenge questions (discovery-level; ranking
in backlog §G)

```text
Q: Stage by Transport avoid full local serialization before/after?
A: AFTER serializer creation, YES (§3.2, SOURCE_CONFIRMED). BEFORE, the
   TADIR discovery SELECT itself is still whole-package-tree scoped
   (§3.1, SOURCE_CONFIRMED, low measured severity, MEASURE_FIRST).

Q: Does a post-commit navigation trigger a redundant full refresh?
A: UNKNOWN which UI path calls refresh() on commit success (not located
   this pass); IF it does, §3.3 proves the effect is a full O(N)
   re-serialization, not incremental. MEASURE_FIRST (M-4).

Q: Are unchanged objects serialized repeatedly in the same session/action?
A: Only via the refresh() mechanism in §3.3 (N-scoped); the filtered
   (K-scoped) path does not re-serialize unfiltered objects.

Q: Can transport/package/type/name filtering occur before serializer
   creation?
A: YES for the actual serializer loop (§3.2). NO for the initial TADIR
   discovery SELECT (§3.1).

Q: Are LIMU entries normalized/deduplicated before expensive work?
A: NOT_INVESTIGATED this pass — UNKNOWN.

Q: Does parallel serialization reduce elapsed time or only increase
   memory/DB contention?
A: NOT_INVESTIGATED for throughput this pass. A correctness defect in
   the mechanism (stale pooled-worker prefetch cache) was found and
   (per existing log) already fixed — cross-referenced only, §3.5.

Q: Are all generated files held in memory simultaneously?
A: For a given serialize() call, yes (mt_files/rt_files, §4) — this is
   existing standard abapGit shape, not ORTEC-introduced, and scales with
   whatever it_tadir was passed (K or N depending on caller, §3.2/3.3).

Q: Are serializer payloads copied during XML/string/XSTRING/hash
   conversion?
A: NOT_INVESTIGATED this pass — UNKNOWN.

Q: Are Git branch/ref requests repeated?
A: YES — §2.2, SOURCE_CONFIRMED, up to 3 independent info/refs GETs
   possible in one user action (pull_by_branch classification + upload-
   pack's own branch resolution + filter_walk's own staleness check).

Q: Are tree walks or decodes repeated for identical SHA/context?
A: The per-commit filtered index (§2.4) avoids re-walking a commit
   already indexed; a NEW commit still pays a full walk even if a
   sibling/parent commit's tree is 95%+ identical (E1-TREE-REUSE, cited
   not re-opened).

Q: Are remote file providers inconsistent or duplicated?
A: The OS4 D1 log already found and rejected one such hypothesis (PC-1)
   for correctness; not re-analyzed for performance here.

Q: Does any incremental path become O(N) in repository size?
A: YES — §3.3, refresh()-triggered local re-serialization. Git-side
   incremental paths audited in §2.1/§2.3 remain K/frontier-scoped.

Q: Would a proposed cache hide real SAP changes?
A: N/A — no new cache proposed in this discovery.

Q: Is a new persistent local snapshot actually needed, or can existing
   work be reduced first?
A: Existing work has real, evidenced reduction opportunities (§2.2
   redundant branches() calls, §3.1 TADIR full-tree read, §3.3 refresh()
   O(N) re-serialization) that are independent of and cheaper than a new
   persistent snapshot layer — see backlog ranking §G.
```
