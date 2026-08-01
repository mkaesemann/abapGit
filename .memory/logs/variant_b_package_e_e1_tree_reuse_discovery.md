# Package E — E1-TREE-REUSE — Phase 1 baseline and evidence

Scope: this document is discovery/evidence only. No productive code is
touched. Source of authority for this run: the owner's explicit
"E1-TREE-REUSE Convergent Design and Implementation Specification" prompt
(2026-07-31).

## 0. Postponement reconciliation (must-read before anything else)

`.memory/state.md` records a same-day owner decision
(`PACKAGE_E_STATUS=... E1_OBJINDEX_PERFORMANCE ... POSTPONED 2026-07-31 ...
No further E1 work (contract reconciliation or E1-B/D/E) is authorized
right now`). The current explicit owner prompt asks for exactly one thing
that state.md's own "Deferred topics" entry lists as the stated re-entry
condition for `E1-TREE-REUSE`:

> Entry condition: owner approves an additive secondary-index DDIC change
> and a dedicated design + performance DESIGN_GATE for it.

Classification:

- `E1_OBJINDEX_PERFORMANCE POSTPONED` — `CONFIRMED_CURRENT` general
  posture (no E1-A contract reconciliation, no E1-B implementation, no
  live-system change of any kind happens in this run).
- The current prompt is treated as `OWNER_DECISION` satisfying the
  documented `E1-TREE-REUSE` entry condition specifically: **design-only**,
  explicit performance `DESIGN_GATE`, zero productive changes, zero
  `state.md` changes, zero commits. This is fully reversible (memory
  artifacts only) and matches the letter of the pre-written entry
  condition, so it proceeds without a blocking question.
- `PRODUCTIVE_CHANGES_ALLOWED=NO` remains in force. `IMPLEMENTATION_
  AUTHORIZED` at the end of this run must be `no` regardless of gate
  outcomes, per the user prompt's own "do not implement productive code in
  this run" instruction — Phase 5 output is a specification for a FUTURE
  authorized run, not something to execute now.
- `state.md` is intentionally NOT updated by this run (explicit
  instruction). The next action recorded in the final response must tell
  the owner this design is ready for a separate, explicit resume decision.

## 1. Baseline commit / source provenance

```text
HEAD=36839c7faa55b4568c1724cf6232468957f6aac8 ("ORTEC: Isolate porcelain
  extension routing")
WORKING_TREE=dirty, but only in .github/agents/* (agent definition files)
  and two untracked root-level dumps (git-client-agents.txt,
  git-client-package_e.txt — plain concatenation exports for offline
  reading, not part of any memory or productive scope; not read further)
PACKAGE_E_CHECKPOINT_1_VALIDATED_HEAD=3c77d898b62f3b0464e48cf59844e2e30c7b6e89
  (SAP-validated in IT8, 2026-07-29) — HEAD above is later in history than
  this; `src/ortec/**` content read below is taken directly from the
  current workspace files (ABAP-FS/workspace source), not re-fetched from
  SAP, consistent with "ACTIVE_SOURCE=ABAP-FS MCP or verified current Git
  source" in the run's Environment block.
SAP_VALIDATED vs MEMORY_ONLY: the specific fact needed for this design
  (the live `c_index_write_chunk_size` value) is corroborated by TWO
  independent sources — (a) direct read of
  `src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap` this pass shows
  `VALUE 30000`, and (b) `.memory/logs/variant_b_package_e_false_modified_
  os4_d1.md` records this as an owner-observed live-system discrepancy
  vs. the originally-authorized contract value of 5000. Both agree: the
  ACTIVE value is 30000. This reconciles the run's `CURRENT_INDEX_WRITE_
  BATCH=30000` environment fact — CONFIRMED_CURRENT, not a HYPOTHESIS.
```

## 2. Evidence table

```text
FACT       | c_index_write_chunk_size = 30000 (VALUE 30000, private
             CLASS-METHODS constant, zcl_abapgit_ortec_obj_index)
           | source: direct read, this pass, matches false_modified_os4_d1.md
MEASURED   | Chunk=1000 baseline (pre-E1-A, 42,000-row scale, O4H-8794 SAT
             trace): Phase I (MODIFY zaog_obj_index + DB:Exec) =
             9,390,412 µs (~9.39s), 41 MODIFY / 82 DB:Exec. Outer traversal
             loop (LOOP AT lt_nodes, includes the write calls) = 13,608,745
             µs (~13.61s) total; decode_tree itself only 974,748 µs
             (~0.97s, 340 distinct trees). Source:
             .memory/incidents/variant_b_d2_sat_warm_to_cold_o4h8794.md §4-6.
MEASURED   | Row-count-based file-delta proxy across the 4 commits captured
             in that same trace: total index-row counts span 41,988-42,000
             (spread of 12 rows out of ~42,000, ≤0.03%). Source: same
             incident, its own §3/§9. This means: for THESE 4 real commits,
             the number of RESOLVED rows barely varies, but it does NOT by
             itself prove the ROOT tree_sha1 or even individual subtree
             SHA1s are unchanged between them — row-count similarity is
             weaker evidence than tree-identity, see §4 below.
UNKNOWN    | No SAT/timing remeasurement exists at chunk=30000 (searched
             `.memory/logs/performance_scan_variant_b_package_e_checkpoint_1.md`,
             `.memory/logs/regression_variant_b_package_e_checkpoint_1.md`,
             `.memory/logs/variant_b_package_e_false_modified_os4_d1.md` —
             none contain a post-30000 timing trace). The 9.39s figure is
             for chunk=1000 and is now STALE as a "current cost" number;
             it remains valid evidence only for the qualitative claim
             "write-side cost is round-trip-count-driven, not byte-volume-
             driven" (the incident's own §6 conclusion), which chunk size
             does not change.
OWNER_DECISION | OWNER_TESTED_BATCH=20000 "successful" (per this run's
             Environment block) — informal, no timing artifact found in
             memory; treated as "did not error/regress" evidence only, not
             a performance measurement. 50000_TEST=not performed.
HYPOTHESIS | A tree-reuse design keyed ONLY on the commit's ROOT tree_sha1
             (matching literally what the design doc calls "E1-D bare key")
             would have produced ZERO reuse hits for the 4 real commits in
             the cited SAT evidence, because their row counts differ
             (41,988 vs 42,000 etc.) which proves their root trees differ.
             Root-only reuse only helps for byte-identical root trees
             (e.g. a message-only/empty commit, or a revert-to-identical-
             state commit) — a narrow case. SUBTREE-level reuse (reusing
             individual unchanged directories' rows across commits that
             otherwise differ) is the only design shape with a plausible
             chance of matching the realistic workload above, because a
             single changed leaf file still leaves the VAST majority of
             sibling subtrees byte-identical even though the root tree
             SHA1 necessarily changes (Merkle hash propagation). This is
             a load-bearing fact for Phase 2 alternative selection — flag
             to the design agent explicitly, do not let it default to a
             root-tree-only design without addressing this.
SUPERSEDED | Design doc §2's E1-A contract text says "NEW_CONSTANT=...
             VALUE 5000" — superseded by the live 30000 value per the FACT
             row above; already tracked as an open, unrelated
             reconciliation item in state.md (E1-A contract vs. live
             value) and explicitly OUT OF SCOPE for this run (do not touch
             E1-A itself).
```

## 3. Complete current call path (traced from source, this pass)

```text
1. CALLER (only real production caller found, repo-wide grep):
   zcl_abapgit_ortec_filter_walk=>pull_filtered
     -> zcl_abapgit_ortec_obj_index=>get_files_for_filter
   (get_remote_files_for_stage/get_remote_files_for_diff both funnel into
   pull_filtered after resolving repo_key/commit/branch; standard
   zif_abapgit_repo_online->get_files_remote is the non-ORTEC fallback,
   untouched by this design)

2. GET_FILES_FOR_FILTER (zcl_abapgit_ortec_obj_index)
   - ii_obj_filter->get_filter() -> ty_tadir_tt (object/obj_name pairs)
   - RETURN early if filter empty
   - resolve DEVC baseline paths via
     zcl_abapgit_folder_logic=>get_instance()->package_to_path(iv_top=
     iv_devclass, io_dot, iv_package) for each DEVC filter row (best-effort,
     swallows zcx_abapgit_exception)
   - ENSURE_INDEX(repo_key, commit, io_dot, iv_devclass)
   - SELECT_ROWS_FOR_FILTER(repo_key, commit, filter) -> ty_index_rows_tt
   - filter DEVC rows against the resolved devc_paths set
   - if rows empty, RETURN
   - BUILD_FILES_FROM_ROWS(repo_key, rows, url, commit) -> rt_files
     (on zcx_abapgit_exception: DELETE stale rows for this repo+commit,
      re-ENSURE_INDEX, re-SELECT, re-filter, re-BUILD — one retry, no loop)
   - apply_object_filter (standard zcl_abapgit_repo_filter, unchanged)

3. ENSURE_INDEX
   - IF IS_INDEX_READY(repo_key, commit) = true: RETURN (no-op)
   - ELSE: REBUILD_INDEX(repo_key, commit, io_dot, iv_devclass)

4. IS_INDEX_READY (public, STRICT mode default; RELAXED is benchmark-only,
   see zcl_abapgit_ortec_git_switch=>cs_absent_strictness, never ships as
   default)
   - SELECT SINGLE path_hash FROM zaog_obj_index
     WHERE repo_key=? AND commit_sha1=? AND obj_type='$IDX'
       AND obj_name='__READY__' AND idx_status='R'
   - rv_yes = sy-subrc = 0
   - NOTE: doc comment claims this is "exposed publicly so other Ortec
     components (e.g. the delta-base completeness gate) can reuse the
     same completeness signal" — repo-wide grep found NO actual external
     caller today. Treat that doc comment as aspirational/stale, not a
     hidden coupling that constrains this design; PUBLIC signature
     (iv_repo_key, iv_commit -> rv_yes) must not change regardless, since
     it is part of the class's public contract.

5. REBUILD_INDEX (the actual cost center; full source re-read this pass)
   - lv_lock_id = zcl_abapgit_ortec_pack_raw=>acquire_repo_lock(repo_key)
     (mutex row `LOCK_<repo_key>` in zaog_fetch_sess; DO iv_max_attempts
     TIMES with exponential backoff + jitter, WAIT UP TO ... SECONDS;
     raises on timeout). This ALREADY SERIALIZES all rebuild_index calls
     for the SAME repo_key — a load-bearing fact for the C8 concurrency
     context.
   - re-check IS_INDEX_READY inside the lock (double-checked locking);
     RETURN early (release lock) if another process just finished it
   - DELETE FROM zaog_obj_index WHERE repo_key=? AND commit_sha1=?
     (clears any partial rows from a prior interrupted attempt)
   - fetch the commit object (zcl_abapgit_ortec_obj_store=>get_objects,
     iv_bulk_fetch=true, 1 SHA1) -> decode_commit -> ls_commit-tree (root
     tree SHA1)
   - BFS WHILE lt_pending IS NOT INITIAL:
       - bulk get_objects() for ALL pending tree SHA1s in this BFS level
         (iv_bulk_fetch=true — already K-bounded to this level's distinct
         tree count, not per-object SQL)
       - decode_tree() each returned tree blob
       - for each node in each tree:
           DIR   -> if tree_sha1 not in lt_seen_trees (a SINGLE per-call,
                    in-memory HASHED set covering ONLY this one
                    rebuild_index invocation): mark seen, enqueue
                    (child_sha1, child_path) for the NEXT BFS level.
                    ** lt_seen_trees only dedups WITHIN one rebuild_index
                    call — it has no cross-commit/cross-call memory. This
                    is exactly the gap E1-TREE-REUSE targets: an identical
                    subtree appearing in TWO DIFFERENT commits is walked
                    and re-persisted twice today, with zero reuse. **
           FILE  -> zcl_abapgit_filename_logic=>file_to_object(filename,
                    path, iv_devclass, io_dot) -> es_item (obj_type/
                    obj_name); skip (CONTINUE) on exception or empty
                    obj_type/obj_name (generated/unmapped files); skip if
                    path/filename > 255 chars (DDIC field width guard);
                    lv_path_hash = sha1_string(path & filename); build
                    ls_row (repo_key, commit_sha1, obj_type, obj_name,
                    path_hash [KEY FIELDS] + file_path, file_name,
                    blob_sha1, tree_sha1 [immediate parent tree], idx_
                    status='R' [DATA FIELDS]); APPEND to lt_rows; flush
                    (MODIFY zaog_obj_index FROM TABLE lt_rows, then CLEAR)
                    once lines(lt_rows) >= c_index_write_chunk_size
       - lt_pending = lt_next (advance BFS level)
   - final partial-chunk flush if lt_rows not empty
   - UNCONDITIONAL completion-marker MODIFY (obj_type='$IDX',
     obj_name='__READY__', path_hash=40 zeros, idx_status='R') — the SOLE
     positive completeness signal; written strictly LAST
   - release_repo_lock on every exit path (success, zcx_abapgit_exception,
     cx_root) — lock is never leaked

6. SELECT_ROWS_FOR_FILTER
   - SELECT * FROM zaog_obj_index FOR ALL ENTRIES IN it_filter
     WHERE repo_key=? AND commit_sha1=? AND idx_status='R'
       AND obj_type = it_filter-object AND obj_name = it_filter-obj_name
   - DELETE the $IDX/__READY__ marker row from the result set (defensive;
     the marker's obj_type='$IDX' should never match a real TADIR filter
     row, but this is a correctness belt-and-suspenders)

7. BUILD_FILES_FROM_ROWS (row -> zif_abapgit_git_definitions=>ty_file,
   incl. blob payload fetch via zcl_abapgit_ortec_obj_store, negotiated
   fetch on miss when iv_url is supplied — unchanged by this design;
   E1-TREE-REUSE only affects INDEX ROW production, never blob payload
   handling)
```

## 4. Interpretation-input inventory (every input that can change the
   OBJ_TYPE/OBJ_NAME/PATH_HASH/FILE_PATH/FILE_NAME resolution for a given
   raw tree/blob SHA1)

```text
INPUT                    | SOURCE                                | AFFECTS
repo_key                 | caller (resolved from remote URL)     | which zaog_obj_store rows exist (objects are stored PER repo_key,
                          |                                        zaog_obj_store PK = CLIENT+REPO_KEY+OBJ_SHA1 — NOT globally content-
                          |                                        addressed across repos). Confirms C4 (same tree, different repo) MUST
                          |                                        be a negative/ineligible case, not a supported cross-repo reuse target
                          |                                        — a bulk row-copy across repo_key would reference blob_sha1s that may
                          |                                        not exist in the target repo's own object store yet.
root tree_sha1 (or any    | Git commit/tree graph (content-        the exact set of files/subtrees reachable; Git's own Merkle-hash
  subtree's tree_sha1)     addressed, immutable)                   guarantee: identical tree_sha1 = byte-identical {name, mode, child
                          |                                        sha1} set, recursively. This is the ONLY input that is safe to trust
                          |                                        as "content-proven-identical" by construction.
io_dot (.abapgit config)  | caller-supplied zcl_abapgit_dot_abapgit | starting_folder (is_ignored/path scoping), folder_logic (get_folder_
                          |  instance, built from a FILE INSIDE     logic — PREFIX vs FULL, changes package_to_path/path derivation),
                          |  the repo's own tree (versioned,        i18n_languages, master_language, requirements, name/version/
                          |  mutable across commits/branches)       original_system, mapping-relevant fields consumed by object-type
                          |                                         MAP_FILENAME_TO_OBJECT plugins (below). zcl_abapgit_dot_abapgit
                          |                                         ALREADY has a canonical byte-exact content hash: get_signature()
                          |                                         computes sha1_blob(serialize()) — the SAME mechanism .abapgit's own
                          |                                         git blob identity uses elsewhere in abapGit. This is directly
                          |                                         reusable as the canonical ".abapgit content" fingerprint; no new
                          |                                         hashing scheme needs to be invented.
iv_devclass (package)     | caller (repo's assigned package)       zcl_abapgit_filename_logic=>file_to_object passes iv_devclass into
                          |                                         map_filename_to_object -> per-object-type MAP_FILENAME_TO_OBJECT
                          |                                         (dynamic CALL METHOD (lv_class)); package_to_path/DEVC baseline
                          |                                         resolution also devclass-dependent (get_files_for_filter's own DEVC
                          |                                         handling, separate from rebuild_index but same iv_devclass).
object-type mapping        | zcl_abapgit_filename_logic=>          per-object-type plugin classes (ZCL_ABAPGIT_OBJECT_<TYPE>~
  plugin code (STANDARD     map_filename_to_object -> dynamic       MAP_FILENAME_TO_OBJECT) are STANDARD abapGit code, potentially
  abapGit, not ORTEC-owned) CALL METHOD (lv_class)=>(...)            reading SAP system state (TADIR, customizing, namespace tables) that
                          |                                          is NOT purely a function of (filename, path, devclass, dot). This is
                          |                                          the residual risk C7 ("source built by old algorithm/schema
                          |                                          version") must cover: a system/support-package upgrade changing any
                          |                                          object-type's mapping semantics invalidates ALL previously-indexed
                          |                                          rows for that object type, with no automatic signal. Cannot be
                          |                                          fully enumerated (100+ object-type classes); the safe answer is a
                          |                                          single monotonic "index algorithm/schema version" stamp bumped
                          |                                          whenever ANY change to this call chain (ORTEC or standard) could
                          |                                          alter mapping output, checked as an equality gate on every reuse
                          |                                          lookup — never a per-object-type allow-list.
path context (file_path)  | BFS walk position (built by            for the SAME subtree tree_sha1 to be safely reusable at a NEW
                          |  concatenating parent path + node        commit, it must reappear at the SAME resulting file_path — a
                          |  name during the walk)                   directory-rename/move can produce an IDENTICAL subtree tree_sha1 at
                          |                                         a DIFFERENT path (rare but real: `git mv unchanged_dir/ new_name/`).
                          |                                          Any subtree-level reuse design MUST key on (tree_sha1, resulting
                          |                                          path) jointly, or verify the path independently, never tree_sha1
                          |                                          alone even within one repo/context.
```

## 5. Concurrency/lock facts relevant to C8/C9/C10

```text
- zcl_abapgit_ortec_pack_raw=>acquire_repo_lock/release_repo_lock already
  provide a per-repo_key mutex (zaog_fetch_sess row `LOCK_<repo_key>`),
  held for the ENTIRE rebuild_index call today. Any reuse-copy path that
  runs INSIDE rebuild_index (recommended) inherits this same serialization
  for free — two concurrent builds for the SAME repo_key can never race
  (C8 is already closed by the existing lock IF the new path stays inside
  the locked section; flag this constraint to the design explicitly).
- rebuild_index's existing crash-safety shape: DELETE-existing-partial-rows
  FIRST, accumulate/flush data rows, write the completion marker
  UNCONDITIONALLY LAST, release lock on every exit path (success/
  zcx_abapgit_exception/cx_root). Any reuse-copy path must reproduce this
  EXACT shape (delete-first, copy-rows, marker-last, lock-released-on-
  every-exit) to inherit the identical, already-proven C9 (crash mid-copy)
  safety property — a crash after N of M copied/rewalked rows leaves the
  target commit's index rows present but un-marked-READY, so a subsequent
  ENSURE_INDEX call correctly re-triggers a full REBUILD_INDEX (DELETE +
  rebuild), never a silently-incomplete READY index.
- C10 (source rows deleted/changed between eligibility check and copy) is
  a NEW risk this design introduces that does not exist today (today,
  there is no "read another commit's rows" step at all). Any reuse design
  MUST treat the source read and the eligibility check as needing the SAME
  transactional view, or re-validate after the copy (e.g. re-check the
  source commit's own READY marker still holds after the bulk copy,
  before writing the target's own marker) — this is a genuinely new
  invariant this design must define and test, not something inherited for
  free from the existing single-commit-build code path.
```

## 6. Test/fixture conventions already established (for TR-slice test design)

```text
- Test class ltcl_obj_index, RISK LEVEL HARMLESS DURATION SHORT,
  mc_repo = 'ZAOGT_OBJIDX' (12-char CHAR repo key)
- setup/teardown: DELETE FROM zaog_obj_store/zaog_obj_index WHERE
  repo_key = mc_repo, teardown also ROLLBACK WORK
- Manual Git object construction pattern (no mocking framework used):
  build blob/tree/commit bytes via zcl_abapgit_git_pack=>encode_tree/
  encode_commit, hash via zcl_abapgit_hash=>sha1_blob/sha1_tree/
  sha1_commit, persist via zcl_abapgit_ortec_obj_store=>store_object,
  then call get_files_for_filter/is_index_ready and assert
- lo_dot = zcl_abapgit_dot_abapgit=>build_default( ); filter via
  NEW zcl_abapgit_object_filter_obj( it_filter = VALUE #( ( object = ...
  obj_name = ... ) ) )
- Existing E1-T-01/02 tests (index_no_cross_commit_leak,
  ready_rejects_other_commit/ready_accepts_exact_commit) already prove
  TWO DIFFERENT commits under the SAME repo_key produce fully independent,
  non-leaking index rows and independent readiness — this is the exact
  baseline behavior any reuse design must NOT weaken: a target commit's
  rows must remain queryable/correct even if the source commit's rows are
  later deleted (cache-admin cleanup, TTL, etc.) — reuse must COPY, never
  alias/share physical rows across commit_sha1 keys.
- build_bulk_commit / index_bulk_rows_preserved / index_chunk_boundary_ok
  already establish a scalable multi-file fixture-building helper pattern
  (build N trivial PROG files, one commit) reusable for TR-slice fixtures
  needing "commit A and commit B share most of the same file set".
- METHOD names already at the 30-char limit style budget (e.g.
  `index_bulk_rows_preserved` = 25 chars); new TR test method names MUST
  be independently verified <= 30 chars each (repo-memory-documented
  recurring real-compiler-only defect class, not caught by local tooling).
```

## 7. Hard-stop-relevant conclusions from Phase 1 (feed directly into Phase 2/4)

```text
- MEASURED_BASELINE for the CURRENT live config (chunk=30000) is MISSING.
  The only hard timing number available is chunk=1000 @ ~42,000 rows
  (9.39s Phase I / 13.61s full traversal loop). A fresh SAT trace at the
  CURRENT 30000 config is a genuine benchmark prerequisite before any
  numeric "expected speed-up" claim for E1-TREE-REUSE can be asserted as
  fact rather than a bounded estimate. Phase 2/4 must not fabricate a
  specific speed-up number without this prerequisite; a bounded, clearly-
  labeled ESTIMATE derived from the round-trip-count/traversal-count math
  is acceptable, a claimed measured result is not.
- A ROOT-tree-only reuse key (matching literally "bare tree-SHA1", scoped
  up one level to include repo/dot/devclass/version) would have produced
  ZERO reuse hits on the only real multi-commit evidence available (4
  commits, all with different row counts => different root trees). The
  design MUST evaluate subtree/path-level granularity as a real
  alternative, not a stretch goal, or explicitly justify why root-only
  reuse is still worth the DDIC/complexity cost despite this evidence
  (e.g. if the owner's real workload includes many message-only/no-op
  commits not represented in the captured trace — Phase 2 should ask
  rather than assume either way).
- The 30-char method-name limit and the "silent WRONG VALUE" class of
  ABAP pitfalls (string-template PAD, C/X hex-case, offset-in-expression)
  apply to any new implementation slice; Phase 5 slices must call these
  out as explicit test assertions, not rely on local tooling.
```
