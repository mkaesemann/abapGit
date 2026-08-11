# OBJ_PERF_FINAL adversarial review
Task: OBJ-PERF-ADV-1
Cycle: 1 of 3
Baseline: `4193733de3c8ad75bae61f6ceb95b1c3e0ccb3e4`
Verdict: **REVISE**
## Evidence Matrix
| Evidence ID | Source |
|---|---|
| E-STATE | `.memory/state.md`, active topic `OBJ_PERF_FINAL`, binding invariants |
| E-HIST-IDX | `.memory/logs/obj_index_partial_history_archaeology.md`, especially `c8fbdf23`, `cd0b277d`, `58a90001`, `17513ba7`, `2111b288` |
| E-HIST-STORE | `.memory/logs/obj_index_partial_history_archaeology.md`, object-store section, especially `733bb307`, `2111b288`, `9c3d297a`, `a51e743b`, `435681ca` |
| E-IDX-SRC | `.memory/logs/obj_index_partial_current_source.md` Q1-Q11 and direct source read of `src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap` |
| E-STORE-SRC | `.memory/logs/obj_store_performance_current_source.md` Q12-Q16 and direct source read of `src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap` |
| E-DDIC-IDX | Direct source read of `src/ortec/git/zaog_obj_index.tabl.xml`: key is `(CLIENT, REPO_KEY, COMMIT_SHA1, OBJ_TYPE, OBJ_NAME, PATH_HASH)`; no context key |
| E-FILTER-WALK | Direct source read of `src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap`: `get_files_for_filter` receives `ii_repo_online->get_dot_abapgit( )` and `ii_repo_online->get_package( )` |
| E-DOT | Direct source read of `src/repo/zcl_abapgit_dot_abapgit.clas.abap`: `serialize( )` serializes the current `ms_data` XML bytes |
| E-FILTER-API | Direct source read of `src/repo/filter/zif_abapgit_object_filter.intf.abap` and `src/zif_abapgit_definitions.intf.abap`: `get_filter( )` returns an unbounded standard table of TADIR-like rows |
| E-CACHE-ADMIN | Direct source read of `src/ortec/git/zcl_abapgit_ortec_cache_admin.clas.abap`: `clear_repo` deletes `ZAOG_OBJ_INDEX` but no proposed `ZAOG_OBJ_COVER` row |
## Partial Index findings
### ID=AR-1-01
SEVERITY=BLOCKER
CLAIM=The design's `CONTEXT_HASH` prevents `.abapgit`, devclass, namespace/folder logic, and algorithm-version changes from reusing stale partial-index answers.
COUNTEREXAMPLE=Context A maps `src/zcl_foo.clas.abap` to `(CLAS,ZCL_FOO)` and writes a positive `ZAOG_OBJ_INDEX` row. Context B changes folder logic, ignore rules, or the algorithm version so the same file should no longer be returned for that filter. `get_coverage` misses under B and `walk_filtered` computes B coverage, but the old positive row remains in `ZAOG_OBJ_INDEX`. The subsequent `select_rows_for_filter` still predicates only on `(repo_key, commit_sha1, obj_type, obj_name, idx_status)` and returns the stale A row as if it belonged to B.
EVIDENCE=E-IDX-SRC (`select_rows_for_filter` has no context predicate), E-DDIC-IDX (`ZAOG_OBJ_INDEX` key has no `CONTEXT_HASH`/`ALGO_VERSION`), E-DOT, design §3.1 says context hash makes stale answers unreachable, design §11 step 3.4 still re-selects from `ZAOG_OBJ_INDEX`.
IMPACT=correctness / identity canonicalization / false positive files under changed path/config context.
REQUIRED_CHANGE=Either add context identity to every positive mapping read (`ZAOG_OBJ_INDEX` column/key or a new context-scoped positive-row table), or define a locked invalidation/rebuild rule that removes all positive rows produced under incompatible context before any B-context read can select them. The implementation spec must make `select_rows_for_filter` context-aware or prove an equivalent invariant.
RETEST=ABAP Unit: build rows under context A, change only `.abapgit` or `iv_devclass` so `compute_context_hash` changes, resolve the same object under context B, and prove the stale A file is not returned. Include an algorithm-version bump case.
### ID=AR-1-02
SEVERITY=BLOCKER
CLAIM=The existing stale-row retry can be preserved while adding durable coverage rows.
COUNTEREXAMPLE=Object X has `ZAOG_OBJ_COVER` status `FOUND` and positive rows. A later request for object Y hits `build_files_from_rows` `CORRUPT_OR_INCOMPLETE`; today's retry branch deletes all `ZAOG_OBJ_INDEX` rows for `(repo_key, commit)` before retrying. If the new design keeps that delete but does not delete or invalidate `ZAOG_OBJ_COVER`, X's `FOUND` coverage survives while X's positive rows are gone. A future warm request for X sees coverage complete, skips the walk, calls `select_rows_for_filter`, gets zero rows, and returns no remote files.
EVIDENCE=E-IDX-SRC direct source: retry catch deletes `ZAOG_OBJ_INDEX WHERE repo_key = iv_repo_key AND commit_sha1 = iv_commit`; design §6 trigger 3 says the retry behavior is preserved but now calls `ensure_filtered_coverage`; design §5 says `ZAOG_OBJ_COVER` and `$IDX/__READY__` coexist independently and no coverage cleanup is specified.
IMPACT=correctness / false remote deletion / partial publication after stale cleanup.
REQUIRED_CHANGE=Specify one atomic invalidation model for positive rows and coverage rows. If any `ZAOG_OBJ_INDEX` rows for a commit/object/context are deleted, matching `ZAOG_OBJ_COVER` rows must be deleted or downgraded in the same lock/LUW before a warm read can trust them. Prefer object/context-scoped cleanup over the current whole-commit delete; if whole-commit delete remains, delete whole-commit coverage too.
RETEST=ABAP Unit: create coverage+index rows for X and Y, force the Y corrupt-blob retry path, then request X. The test must prove X either re-walks or still returns its file, never zero files from orphaned coverage.
### ID=AR-1-03
SEVERITY=MAJOR
CLAIM=`compute_context_hash` covers the `.abapgit` configuration relevant to the target commit.
COUNTEREXAMPLE=Branch A is local/current with folder logic or ignore list A. Remote branch B at `iv_commit` changes `.abapgit` but has not been pulled locally. `get_remote_files_for_stage` passes `ii_repo_online->get_dot_abapgit( )` into `get_files_for_filter`, so the hash and `file_to_object` mapping are computed from local A configuration while walking B's tree. A zero-match coverage row under A-context can be reused on repeated attempts even though B's own `.abapgit` would map the same tree differently.
EVIDENCE=E-FILTER-WALK (`io_dot = ii_repo_online->get_dot_abapgit( )`), E-DOT (`serialize( )` serializes current `ms_data`), design §3.1 claims serialized `.abapgit` covers folder logic/starting folder/ignore list, design §1 says resolution strength depends on the tree data it was computed against.
IMPACT=context divergence / false negative or false positive under `.abapgit` changes.
REQUIRED_CHANGE=Define whether remote-file filtering is intentionally based on local `.abapgit` or target-commit `.abapgit`. If target-commit semantics are required, derive the context hash from the `.abapgit` blob at `iv_commit` and use that same parsed config for `file_to_object`; if local semantics are required, explicitly weaken the negative states and tests so they never claim remote absence under a remote config change.
RETEST=Integration/ABAP Unit with two commits differing only in `.abapgit` folder logic or ignore rules and the same requested object. The second commit must not reuse or publish a coverage result computed with the first commit's config unless the design explicitly documents local-config semantics.
### ID=AR-1-04
SEVERITY=MAJOR
CLAIM=`get_coverage` can be one `FOR ALL ENTRIES` statement because `it_filter` is caller-bounded K.
COUNTEREXAMPLE=A transport/stage filter can contain tens of thousands of TADIR rows. The proposed `get_coverage` issues one unchunked `FOR ALL ENTRIES` with five equality predicates per filter row. The archaeology already records live DBSQL statement-size failures from unchunked SHA ranges; this design repeats the same unbounded-set assumption on a different key shape.
EVIDENCE=E-FILTER-API (`get_filter( )` returns an unbounded `ty_tadir_tt` standard table), design §3.2 says `get_coverage` is a single statement and `it_filter is caller-bounded K, never chunked`, design §7 lists expected K up to 5000 but does not enforce it, E-HIST-STORE `733bb307` and E-HIST-IDX `29199f62` reject unchunked caller-scale key sets.
IMPACT=performance / SQL parameter or statement-size failure / fallback loops.
REQUIRED_CHANGE=Deduplicate and chunk `it_filter` before `get_coverage`, `select_rows_for_filter`, and coverage writes, using a named constant and tests above the boundary. If the program truly only supports K<=5000, enforce that limit at the API boundary with a safe fallback and document the user-visible behavior.
RETEST=ABAP Unit or SQL seam test with >32767 logical key components worth of filter rows proving the implementation executes multiple bounded statements and returns complete coverage without DBSQL_STMNT_TOO_LARGE.
## Object Store findings
### ID=AR-1-05
SEVERITY=MAJOR
CLAIM=Adding `ZAOG_OBJ_COVER` requires no explicit invalidation sweep because old rows are harmless and small.
COUNTEREXAMPLE=The existing cache-admin `clear_repo` deletes `ZAOG_OBJ_INDEX`, pack, raw, fetch, and related repo cache rows for a repository. After this design, it will leave `ZAOG_OBJ_COVER` rows behind unless amended. A later warm filtered read for that repo/commit/context sees coverage rows, skips the tree walk, and selects zero index rows because the index table was deliberately cleared.
EVIDENCE=E-CACHE-ADMIN (`clear_repo` deletes `ZAOG_OBJ_INDEX`), design §5 says old context rows need no correctness cleanup, design §11 warm step treats coverage alone as enough to avoid a walk.
IMPACT=cleanup/read race / false remote deletion / stale durable fact after manual repair.
REQUIRED_CHANGE=Add `ZAOG_OBJ_COVER` to every repository-cache clear/invalidation path that deletes `ZAOG_OBJ_INDEX` or the underlying object graph. Update result counters/admin UI if applicable. State the delete order under the repo lock.
RETEST=ABAP Unit for cache admin: seed store/index/cover rows, run `clear_repo`, assert cover rows are deleted; then request the object and prove the path re-walks or falls back rather than trusting stale coverage.
### ID=AR-1-06
SEVERITY=MAJOR
CLAIM=The Object Store side satisfies bounded rows/bytes and no full-store scans for this integrated design.
COUNTEREXAMPLE=`zcl_abapgit_ortec_obj_store=>get_all_objects` still calls `populate_cache`, which performs `SELECT * FROM zaog_obj_store WHERE repo_key = ? AND status = 'R' ORDER BY obj_sha1` and loads full `obj_data` for every ready object of the repo. The design marks this out of scope because the small-K path does not call it, but the same document presents OS-INV-05/OS-INV-12 as object-store invariants. A future or existing ORTEC caller that uses `get_all_objects` remains exposed to the exact `SYSTEM_NO_ROLL` failure class the archaeology documents.
EVIDENCE=E-STORE-SRC Q14/Q16 and direct source read of `get_all_objects`/`populate_cache`; design §9 OS-D says the unbounded caller is known and out of scope; design §10 OS-INV-05 and OS-INV-12 state bounded rows/bytes and bounded large-payload memory.
IMPACT=performance / memory amplification / invariant overclaim.
REQUIRED_CHANGE=Either narrow the invariant language to the integrated small-K call graph and add a source-level proof/test that no small-K path reaches `get_all_objects`, or include the `2111b288`-style fix for `get_all_objects` in this program. The review cannot accept a global object-store bounded-memory claim while a live public method violates it.
RETEST=Static call-graph proof plus regression test that Stage/Diff small-K does not call `get_all_objects`; if fixed, scale test or seam test proving `get_all_objects` no longer performs an unbounded payload preload.
## Cross-cutting/integration findings
### ID=AR-1-07
SEVERITY=BLOCKER
CLAIM=Missing commit/tree data safely falls back to `get_files_remote` without creating fallback loops or bypassing missing-object recovery.
COUNTEREXAMPLE=A cold filtered Stage on branch B has enough state to enter `get_files_for_filter`, but one tree object is absent from `ZAOG_OBJ_STORE`. `walk_filtered` raises, then `filter_walk` falls back to `ii_repo_online->get_files_remote( ii_obj_filter )`. The current-source reconciliation states that this standard fallback is a pure read and bypasses `ZAOG_OBJ_STORE`/`ZAOG_OBJ_INDEX` persistence. The next request repeats the same failed partial walk and fallback, so the demand-driven path never learns from a successful fallback and can loop indefinitely.
EVIDENCE=E-IDX-SRC Q8-Q9: missing tree raises and fallback bypasses persistence; E-FILTER-WALK catch returns `get_files_remote`; design §6 triggers 1-2 use existing fallback; design §12 says cache masking missing-tree recovery is avoided but does not provide a recovery or backoff marker.
IMPACT=fallback loop / performance non-convergence / missing tree vs missing blob asymmetry.
REQUIRED_CHANGE=Define a convergence rule after missing commit/tree fallback: either persist the successful fallback's commit/tree/blob facts, perform a bounded graph materialization before retrying, or write a short-lived negative/backoff state that prevents repeated partial attempts for the same `(repo, commit, context)` until the underlying store changes. Missing blobs already have `ensure_available`; missing trees need an equivalent bounded recovery or explicit disablement.
RETEST=Integration test with one missing tree and a successful standard remote fallback. The second identical request must not repeat the same failed partial walk; it must either use newly persisted data, a recorded bypass, or a proven bounded recovery path.
### ID=AR-1-08
SEVERITY=MINOR
CLAIM=`write_coverage` failure can be non-fatal because missing coverage only costs a future repeat walk.
COUNTEREXAMPLE=DB authorization, DDIC activation drift, or table lock causes every `write_coverage` to fail. The current request returns correct files, but every subsequent request repeats the full tree walk with no diagnostic signal. This is not a correctness hole by itself, but it can mask the entire performance feature being disabled in production.
EVIDENCE=Design §3.2 says coverage-write failure is non-fatal and logs/propagates nothing further; design §6 trigger 4 says no fallback or exception surfaces.
IMPACT=operability / silent performance regression.
REQUIRED_CHANGE=Record a diagnosable signal for coverage-write failures: aggregate warning, trace/log entry, or one-shot user-visible message under ORTEC diagnostics. Keep the current request non-fatal, but do not make repeated coverage-write failure invisible.
RETEST=Seam test forcing `write_coverage` failure proves files are returned and a deterministic diagnostic is emitted once per request or per repo/commit.
## Ledger
| ID | Severity | Cycle Introduced | Status | Design Section Affected |
|---|---|---:|---|---|
| AR-1-01 | BLOCKER | 1 | OPEN | Partial §3.1, §5, §11; DDIC `ZAOG_OBJ_INDEX` |
| AR-1-02 | BLOCKER | 1 | OPEN | Partial §5, §6, §11 retry path |
| AR-1-03 | MAJOR | 1 | OPEN | Partial §3.1, §11; `filter_walk` context source |
| AR-1-04 | MAJOR | 1 | OPEN | Partial §3.2, §7; SQL bounds |
| AR-1-05 | MAJOR | 1 | OPEN | Partial §5; Object Store/cache admin cleanup |
| AR-1-06 | MAJOR | 1 | OPEN | Object Store §9 OS-D, §10 OS-INV-05/12 |
| AR-1-07 | BLOCKER | 1 | OPEN | Integrated §6, §11-12; missing tree recovery |
| AR-1-08 | MINOR | 1 | OPEN | Partial §3.2, §6 diagnostics |
## Verdict
**REVISE**.
Cycle 2 must close all BLOCKER and MAJOR IDs before approval can be considered: AR-1-01, AR-1-02, AR-1-03, AR-1-04, AR-1-05, AR-1-06, AR-1-07. The worst risk is AR-1-01: the design context-scopes only the coverage fact, while the positive file rows remain context-blind and are still the source of returned files.
## Cycle 2
Task: OBJ-PERF-ADV-2
Cycle: 2 of 3
Baseline: `4193733de3c8ad75bae61f6ceb95b1c3e0ccb3e4`
Verdict: **REVISE_AND_REVIEW_ONCE**
### Cycle 2 evidence additions
| Evidence ID | Source |
|---|---|
| E-DES-IDX-C2 | `.memory/logs/obj_index_partial_design.md`, revised cycle-2 sections: revision log, §3.0-4.1, §5-13 |
| E-DES-STORE-C2 | `.memory/logs/obj_store_performance_design.md`, revised cycle-2 AR-1-05/AR-1-06 sections |
| E-SRC-IDX-C2 | Current source read of `src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap`: current signatures lack context hash and caller repo object; current retry deletes only `ZAOG_OBJ_INDEX` |
| E-SRC-FILTER-C2 | Current source read of `src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap`: `get_files_for_filter` is called with repo key, commit, local dot, package, URL only; `pull_filtered` has no online repo object/current-remote accessor |
| E-SRC-ONLINE-C2 | Current source read of `src/repo/zif_abapgit_repo_online.intf.abap` and `zcl_abapgit_repo_online.clas.abap`: `get_current_remote( )` exists only on `zif_abapgit_repo_online` and can raise |
| E-SRC-CACHE-C2 | Current source read of `src/ortec/git/zcl_abapgit_ortec_cache_admin.clas.abap`: `clear_repo` uses `ENQUEUE_EZAOG_REPO_LOCK` with `session_id = repo_key` |
| E-SRC-LOCK-C2 | Current source read of `src/ortec/git/zcl_abapgit_ortec_pack_raw.clas.abap`: `acquire_repo_lock` uses a DB mutex row `session_id = |LOCK_{ repo_key }|`, not the cache-admin enqueue key |
| E-SRC-STORE-C2 | Current source/grep of `zcl_abapgit_ortec_obj_store`: `get_all_objects` remains the only productive `populate_cache` caller in ORTEC source; no Stage/Diff small-K caller found |
### AR-1 closure verification
| ID | Cycle-2 status | Counterexample check |
|---|---|---|
| AR-1-01 | **REJECTED_WITH_PROOF** | Cycle 2 makes `CONTEXT_HASH` a non-key column on `ZAOG_OBJ_INDEX`. Two contexts with the same `(repo, commit, obj_type, obj_name, path_hash)` overwrite the same positive row while their distinct `ZAOG_OBJ_COVER` rows survive. See AR-2-01. |
| AR-1-02 | **ACCEPTED_AND_FIXED** | `invalidate_commit_index` is specified as the only commit-scoped stale purge, deleting `ZAOG_OBJ_INDEX` and `ZAOG_OBJ_COVER` in one caller-owned LUW, with both existing obj-index purge call sites replaced. ABAP Open SQL does not expose uncommitted half-deletes to another LUW; the design does not parallelize the two deletes. |
| AR-1-03 | **REJECTED_WITH_PROOF** | The local-config semantics decision is explicit, but the required strong-state gate depends on `ii_repo_online->get_current_remote( )` from inside `walk_filtered`; the specified `get_files_for_filter`/`walk_filtered` signatures do not carry `ii_repo_online` or a precomputed current-remote commit. See AR-2-02. |
| AR-1-04 | **ACCEPTED_AND_FIXED** | `c_filter_chunk_size = 5000` now covers `get_coverage`, `write_coverage`, and `select_rows_for_filter`, with boundary tests required for each. Partial coverage publication between chunks is independently safe because each written row is a valid fact from an already-completed walk and the caller LUW owns commit/rollback. |
| AR-1-05 | **ACCEPTED_WITH_NEW_RELATED_BLOCKER** | The narrow missing `ZAOG_OBJ_COVER` delete in `clear_repo` is specified in the same clear transaction and result counter. However, source verification shows cache admin and filtered walks use different lock mechanisms, creating a new interleaving blocker. See AR-2-03. |
| AR-1-06 | **ACCEPTED_AND_FIXED** | The object-store invariants are now scoped to the integrated small-K call graph, and a mandatory static proof gate names the changed methods and forbidden `get_all_objects`/`populate_cache` calls. Current source grep supports that the unbounded productive caller remains outside ORTEC Stage/Diff small-K source. |
| AR-1-07 | **ACCEPTED_AND_FIXED** | The `'M'` status now has exact write/read semantics, `RESOLVED_AT`, a 300-second TTL, expiry-as-uncovered behavior, and overwrite by the next successful walk. It is no longer a durable false-negative fact; it is a bounded scheduling hint. |
| AR-1-08 | **ACCEPTED_AND_FIXED** | `write_coverage` failures gain a counter and last-error text via `get_diagnostics( )` while preserving non-fatal request behavior. |
### New cycle-2 findings
### ID=AR-2-01
SEVERITY=BLOCKER
CLAIM=Adding `CONTEXT_HASH` as a non-key column to `ZAOG_OBJ_INDEX` makes legacy or incompatible positive rows unreachable without a primary-key migration.
COUNTEREXAMPLE=Context A resolves object X at path P and writes both `ZAOG_OBJ_INDEX(repo, commit, X, path_hash(P), context_hash=A)` and `ZAOG_OBJ_COVER(repo, commit, X, context_hash=A, FOUND)`. Context B later resolves the same object and same path under a different `.abapgit`/algorithm/devclass context. Because `CONTEXT_HASH` is non-key, B's `MODIFY zaog_obj_index` overwrites the same primary-key row and changes its non-key `context_hash` to B. A's coverage row still exists. A future A-context request reads `ZAOG_OBJ_COVER` `FOUND`, skips `walk_filtered`, then `select_rows_for_filter(... context_hash=A)` finds zero positive rows and returns a false empty result.
EVIDENCE=E-DES-IDX-C2 §3.0 says `CONTEXT_HASH` is a new NON-KEY column; E-DES-IDX-C2 §3/§3.2 keys `ZAOG_OBJ_COVER` by `CONTEXT_HASH`; E-DES-IDX-C2 §11 step 3 treats covered `FOUND` objects as sufficient to skip the walk and step 4 re-selects positive rows from `ZAOG_OBJ_INDEX`; E-SRC-IDX-C2 confirms the existing `ZAOG_OBJ_INDEX` primary key does not include context.
IMPACT=correctness / false remote deletion / context identity collision.
REQUIRED_CHANGE=Make positive index mappings context-disjoint. Either add `CONTEXT_HASH` to the `ZAOG_OBJ_INDEX` key (with explicit migration/activation plan), move filtered positive rows into a new context-keyed table, or require a `FOUND` coverage row to be validated against at least one matching context-stamped `ZAOG_OBJ_INDEX` row and re-walk when missing. A non-key context column plus independent context-keyed coverage is not sufficient.
RETEST=ABAP Unit: write A-context `FOUND` coverage and positive index row; write B-context positive row with the same `(repo, commit, obj_type, obj_name, path_hash)`; then resolve under A and prove it re-walks or returns A's file, never an empty result from stale A coverage plus overwritten positive row.
### ID=AR-2-02
SEVERITY=MAJOR
CLAIM=The AR-1-03 local-.abapgit semantics fix prevents `RESOLVED_NOT_PRESENT_REMOTE` for commits whose real target `.abapgit` is not known locally.
COUNTEREXAMPLE=The revised design says `walk_filtered` must require `iv_commit = ii_repo_online->get_current_remote( )` before writing `RESOLVED_NOT_PRESENT_REMOTE`, but the specified `ensure_filtered_coverage(iv_repo_key, iv_commit, io_dot, iv_devclass, lt_filter, iv_context_hash)` and `walk_filtered(iv_repo_key, iv_commit, io_dot, iv_devclass, lt_filter, iv_context_hash)` signatures carry no `ii_repo_online` reference and no `iv_current_remote` value. The existing `pull_filtered` caller also has only `iv_commit`, `io_dot`, `iv_devclass`, and optional repo key. A weak implementer following the spec cannot implement the gate without inventing a new parameter, re-fetching branch state inside `obj_index`, or silently omitting the condition.
EVIDENCE=E-DES-IDX-C2 §11 step 3/4 and §13 W5/W6 signatures omit `ii_repo_online`/`iv_current_remote`; E-DES-IDX-C2 §11.4/§13 W8 requires `ii_repo_online->get_current_remote( )`; E-SRC-FILTER-C2 current caller shape; E-SRC-ONLINE-C2 `get_current_remote( )` belongs to `zif_abapgit_repo_online` and raises.
IMPACT=implementation ambiguity / false strong negative / decision left to implementer.
REQUIRED_CHANGE=Make the current-remote fact an explicit input to the object-index layer. For example, compute it in `get_remote_files_for_stage` while `li_repo_online` is in scope, pass `iv_current_remote` (or `iv_allow_strong_remote_absence`) through `get_files_for_filter`, `ensure_filtered_coverage`, and `walk_filtered`, and specify the `pull_filtered` behavior separately (likely always disallow strong `RESOLVED_NOT_PRESENT_REMOTE` because no online repo object is available).
RETEST=Static signature test/review plus ABAP Unit: the non-online `pull_filtered` path with graph-complete commit must produce only `RESOLVED_NO_FILES`; the online stage path may produce `RESOLVED_NOT_PRESENT_REMOTE` only when `iv_commit = iv_current_remote`.
### ID=AR-2-03
SEVERITY=BLOCKER
CLAIM=Cache-admin `clear_repo` and filtered index/coverage writers are protected by compatible repo locks, so deleting `ZAOG_OBJ_INDEX` and `ZAOG_OBJ_COVER` in one clear transaction cannot leave orphaned coverage relative to a concurrent filtered walk.
COUNTEREXAMPLE=`walk_filtered` writes positive `ZAOG_OBJ_INDEX` rows during the tree walk, then writes `ZAOG_OBJ_COVER` after the walk completes. It holds `zcl_abapgit_ortec_pack_raw=>acquire_repo_lock`, implemented as an inserted `ZAOG_FETCH_SESS` mutex row with `session_id = LOCK_<repo_key>`. `clear_repo` holds a different enqueue lock via `ENQUEUE_EZAOG_REPO_LOCK` with `session_id = repo_key`. These locks do not conflict. Interleaving: `walk_filtered` writes positive rows for X; `clear_repo` concurrently deletes `ZAOG_OBJ_INDEX` and `ZAOG_OBJ_COVER` for the repo and commits; `walk_filtered` then writes `ZAOG_OBJ_COVER FOUND` for X and the caller commits. Final state: coverage says FOUND, backing index rows are gone, and the next warm read can skip the walk and return zero files. The same lock mismatch can also affect COMPLETE rebuild marker ordering.
EVIDENCE=E-DES-IDX-C2 §5 says `walk_filtered` uses the same `acquire_repo_lock` as `rebuild_index`; E-DES-STORE-C2 cache-admin fix says clear uses the existing clear lock/transaction; E-SRC-CACHE-C2 shows `ENQUEUE_EZAOG_REPO_LOCK` with `session_id = repo_key`; E-SRC-LOCK-C2 shows `acquire_repo_lock` uses a DB row `session_id = |LOCK_{ iv_repo_key }|`; E-DES-IDX-C2 §11 writes index rows before coverage.
IMPACT=cleanup/write race / false remote deletion / marker or coverage orphaning.
REQUIRED_CHANGE=Use one canonical repository lock for all writers/deleters of `ZAOG_OBJ_INDEX` and `ZAOG_OBJ_COVER`. The minimal fix is to have cache-admin `clear_repo` acquire `zcl_abapgit_ortec_pack_raw=>acquire_repo_lock( iv_repo_key )` before deleting derived ORTEC tables, or to change both paths to the same enqueue key. Specify lock acquisition/release order if both admin enqueue and pack-raw mutex remain to avoid deadlock.
RETEST=Concurrency/seam test: pause a filtered walk after its first `ZAOG_OBJ_INDEX` write and before coverage write, run `clear_repo`, resume the walk, and prove the final state cannot contain `ZAOG_OBJ_COVER FOUND` without matching context-stamped positive rows.
### Combined ledger after cycle 2
| ID | Severity | Cycle Introduced | Cycle-2 Status | Design Section Affected |
|---|---|---:|---|---|
| AR-1-01 | BLOCKER | 1 | REJECTED_WITH_PROOF | Partial §3.0, §5, §11; DDIC `ZAOG_OBJ_INDEX`; superseded by AR-2-01 |
| AR-1-02 | BLOCKER | 1 | ACCEPTED_AND_FIXED | Partial §5, §6, §13 W4 |
| AR-1-03 | MAJOR | 1 | REJECTED_WITH_PROOF | Partial §11.4, §13 W8; superseded by AR-2-02 |
| AR-1-04 | MAJOR | 1 | ACCEPTED_AND_FIXED | Partial §3.2, §7, §8, §13 W2/W3 |
| AR-1-05 | MAJOR | 1 | ACCEPTED_WITH_RELATED_BLOCKER | Store cache-admin invalidation; related blocker AR-2-03 |
| AR-1-06 | MAJOR | 1 | ACCEPTED_AND_FIXED | Store §9 OS-D, §10 OS-INV-05/12, static proof gate |
| AR-1-07 | BLOCKER | 1 | ACCEPTED_AND_FIXED | Partial §4.1, §6, §11 step 3a/4, §13 W5/W6 |
| AR-1-08 | MINOR | 1 | ACCEPTED_AND_FIXED | Partial §3.2 diagnostics |
| AR-2-01 | BLOCKER | 2 | OPEN | Non-key `ZAOG_OBJ_INDEX-CONTEXT_HASH` plus context-keyed coverage |
| AR-2-02 | MAJOR | 2 | OPEN | Current-remote gate signature / local-config semantics |
| AR-2-03 | BLOCKER | 2 | OPEN | Cache-admin versus filtered-walk lock compatibility |
### Cycle 2 verdict
**REVISE_AND_REVIEW_ONCE**.
Open BLOCKER: AR-1-01, AR-2-01, AR-2-03.
Open MAJOR: AR-1-03, AR-2-02.
Open MINOR: none.
The highest-risk issue is AR-2-01: a non-key context hash can overwrite positive rows across contexts while the old context's durable `FOUND` coverage remains trusted. Cycle 3 must make positive rows and coverage facts share the same identity/lifecycle, and must unify the repo lock used by cache admin and index/coverage writers before approval is possible.
## Cycle 3 (FINAL)
Task: OBJ-PERF-ADV-3
Cycle: 3 of 3 FINAL
Baseline: `4193733de3c8ad75bae61f6ceb95b1c3e0ccb3e4`
Verdict: **APPROVE**
### Cycle 3 evidence additions
| Evidence ID | Source |
|---|---|
| E-DES-IDX-C3 | `.memory/logs/obj_index_partial_design.md`, cycle-3 revision log, §3.0, §3.0b, §5, §7-14 |
| E-DES-STORE-C3 | `.memory/logs/obj_store_performance_design.md`, cycle-3 revision log, OS-INV-01/05/11, cache-admin invalidation section |
| E-SRC-LOCK-C3 | Current source re-read of `zcl_abapgit_ortec_obj_index.clas.abap`, `zcl_abapgit_ortec_cache_admin.clas.abap`, `zcl_abapgit_ortec_pack_raw.clas.abap`: current `rebuild_index` uses only `acquire_repo_lock`; current `clear_repo` uses only `ENQUEUE_EZAOG_REPO_LOCK`; mutex row is `ZAOG_FETCH_SESS-session_id = LOCK_<repo_key>` |
| E-SRC-FILTER-C3 | Current source re-read of `zcl_abapgit_ortec_filter_walk.clas.abap`: stage path holds `ii_repo_online`; `pull_filtered` calls `get_files_for_filter` without any online repo object |
| E-SRC-DISC-C3 | `.memory/logs/obj_index_partial_current_source.md` Q1-Q11 and `.memory/logs/obj_store_performance_current_source.md` Q12-Q16 |
### AR-2 closure verification
| ID | Cycle-3 status | Fresh attack result |
|---|---|---|
| AR-2-01 | **ACCEPTED_AND_FIXED** | `ZAOG_OBJ_INDEX` is now explicitly COMPLETE-mode only, with non-key `CONTEXT_HASH` safe because `rebuild_index` purges-then-rewrites the whole commit through `invalidate_commit_index`. FILTERED positives move to `ZAOG_OBJ_PIDX`, whose key includes `CONTEXT_HASH`; coverage and partial positives are therefore context-disjoint together. Same-request disagreement is structurally avoided because §11 step 3.1 returns from the COMPLETE-ready branch before any `ZAOG_OBJ_COVER`/`ZAOG_OBJ_PIDX` consultation. Promotion is specified: `invalidate_commit_index` deletes `ZAOG_OBJ_INDEX`, `ZAOG_OBJ_COVER`, and `ZAOG_OBJ_PIDX`, and `rebuild_index` calls it before writing the COMPLETE catalog/marker. Stale `PIDX` rows cannot be trusted after READY because READY short-circuits to `ZAOG_OBJ_INDEX`; explicit COMPLETE rebuild also purges them. |
| AR-2-02 | **ACCEPTED_AND_FIXED** | The current-remote fact is now an explicit value, not an implied object dependency: `get_remote_files_for_stage` computes it once from the in-scope `li_repo_online->get_current_remote( )`, blanks it on `zcx_abapgit_exception`, and passes it through `get_files_for_filter` -> `ensure_filtered_coverage` -> `walk_filtered`. The strong negative gate requires `iv_current_remote IS NOT INITIAL AND iv_commit = iv_current_remote`; blank/failure/non-match can only write `RESOLVED_NO_FILES`. A branch update after the value is computed does not let an old row poison a later request because rows are keyed by commit; a later request for a new tip uses a different `COMMIT_SHA1`. `pull_filtered` still has no online repo object in current source and omits the optional parameter, so it structurally cannot produce `RESOLVED_NOT_PRESENT_REMOTE`. |
| AR-2-03 | **ACCEPTED_AND_FIXED** | The design now declares `zcl_abapgit_ortec_pack_raw=>acquire_repo_lock` the canonical mutex for `ZAOG_OBJ_INDEX`/`ZAOG_OBJ_COVER`/`ZAOG_OBJ_PIDX`. `clear_repo` keeps its enqueue lock only as an outer whole-admin-clear lock, then acquires the mutex around exactly the three derived-table deletes and releases it before the existing `ZAOG_FETCH_SESS` delete. Current source verification shows `rebuild_index` takes the mutex and never takes the enqueue lock; `clear_repo` takes the enqueue lock and currently deletes `ZAOG_OBJ_INDEX` before `ZAOG_FETCH_SESS`; `acquire_repo_lock`/`release_repo_lock` operate on `ZAOG_FETCH_SESS` row `LOCK_<repo_key>`. The specified order cannot deadlock with a walk because walks never request the enqueue lock while holding the mutex. |
### New cycle-3 findings
None. No new BLOCKER, MAJOR, or MINOR finding is opened in this final pass.
### Holistic final pass
- Identity is now coherent across all persisted facts: COMPLETE rows are selected only after a context-matched READY marker; FILTERED rows and coverage rows both key by `(repo, commit, obj_type, obj_name, context_hash)`, with `PIDX` adding `path_hash` for multiple files.
- Complete and filtered read paths are mutually exclusive for one request: `is_index_ready(..., iv_context_hash)` returns directly to `select_rows_for_filter`; only a not-ready commit consults coverage and `select_partial_rows_for_filter`.
- Commit-scoped invalidation is consistently three-table and context-blind; admin repo clear is consistently three-table and lock-compatible with writers.
- Strong remote absence is no longer stronger than its context source: graph completeness plus current-remote equality is required; blank or unavailable current-remote downgrades to `RESOLVED_NO_FILES` without fallback ambiguity.
- The known remaining open questions are non-blocking cleanup/polish items: unwritten `UNRESOLVED_AMBIGUOUS_MAPPING` and future orphan cleanup for old contexts/commits. Neither creates a false READY, false FOUND, or false strong negative under the revised flow.
### Final combined ledger
| ID | Severity | Cycle Introduced | Final Status | Closure proof |
|---|---|---:|---|---|
| AR-1-01 | BLOCKER | 1 | CLOSED_BY_AR-2-01 | COMPLETE positives context-predicated; FILTERED positives moved to context-keyed `ZAOG_OBJ_PIDX` |
| AR-1-02 | BLOCKER | 1 | CLOSED | `invalidate_commit_index` deletes INDEX/COVER/PIDX together in one caller-owned LUW |
| AR-1-03 | MAJOR | 1 | CLOSED_BY_AR-2-02 | Local-config semantics retained; strong negative gated by explicit current-remote SHA1 equality |
| AR-1-04 | MAJOR | 1 | CLOSED | Shared `c_filter_chunk_size = 5000` bounds filter-keyed reads/writes, including `select_partial_rows_for_filter` |
| AR-1-05 | MAJOR | 1 | CLOSED_BY_AR-2-03 | Cache admin deletes COVER/PIDX with INDEX under the canonical mutex |
| AR-1-06 | MAJOR | 1 | CLOSED | Object-store boundedness claims scoped to integrated small-K path with static proof gate against `get_all_objects`/`populate_cache` |
| AR-1-07 | BLOCKER | 1 | CLOSED | Missing-tree path writes live `'M'` backoff rows, never resolution facts, and expires/retries |
| AR-1-08 | MINOR | 1 | CLOSED | Coverage-write failures become diagnosable via counter + last-error text |
| AR-2-01 | BLOCKER | 2 | CLOSED | `ZAOG_OBJ_PIDX` key includes `CONTEXT_HASH`; COMPLETE rebuild purges `PIDX`; COMPLETE and FILTERED reads cannot both decide one request |
| AR-2-02 | MAJOR | 2 | CLOSED | `iv_current_remote OPTIONAL` is threaded end-to-end; blank/non-match caps at `RESOLVED_NO_FILES`; `pull_filtered` omits it by construction |
| AR-2-03 | BLOCKER | 2 | CLOSED | `clear_repo` acquires enqueue outer, canonical DB mutex inner around INDEX/COVER/PIDX deletes; writers never take enqueue |
### Final verdict
**APPROVE**.
Open BLOCKER: none.
Open MAJOR: none.
Open MINOR: none.
The cycle-3 revision is decision-free enough for weak-model implementation: the required DDIC tables, key shapes, method signatures, read precedence, invalidation rules, lock order, fallback caps, and tests are specified without leaving the three prior closure points to implementer judgment.