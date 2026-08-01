# E1-TREE-REUSE Adversarial Review

```text
PACKET=COMPACT_HANDOFF_V1
TASK=E1-TREE-REUSE-ADVERSARIAL-V1
CYCLE=1
BASELINE=36839c7faa55b4568c1724cf6232468957f6aac8
REVIEW_MODE=ortec-abapgit-adversarial-design-review
VERDICT=REVISE
```

## Scope Compliance

```text
ALLOWED_CONTEXT_READ=YES
PRODUCTIVE_SOURCE_WRITES=NO
STATE_WRITE=NO
DIAGRAM_WRITE=NO
OUTPUT_ARTIFACT=.memory/reviews/variant_b_package_e_e1_tree_reuse_adversarial.md
```

## Evidence Matrix

```text
E-DESIGN-1=.memory/logs/variant_b_package_e_e1_tree_reuse_design.md §§1-14 read completely.
E-DISC-1=.memory/logs/variant_b_package_e_e1_tree_reuse_discovery.md §§0-7 read completely.
E-PRIOR-1=.memory/logs/variant_b_package_e_design.md §2 and §8 read for prior E1-D/E1-E finding and deferral status.
E-SAT-1=.memory/incidents/variant_b_d2_sat_warm_to_cold_o4h8794.md read for source trace, cardinality, SQL/HTTP, timing facts.
E-SRC-INDEX-1=src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap: current rebuild_index acquires repo lock before delete/fetch/BFS, releases on success and both catch paths, writes READY marker last, and has no COMMIT WORK in this class source.
E-SRC-INDEX-2=src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap: current lt_seen_trees is a HASHED TABLE keyed by tree_sha1 only; root tree is inserted before BFS; DIR children are enqueued only if the child sha1 is not already seen.
E-SRC-INDEX-3=src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap: current FILE branch calls zcl_abapgit_filename_logic=>file_to_object with iv_filename, iv_path, iv_devclass, io_dot; FILE rows are skipped if mapped item is empty or if strlen(path)>255 or strlen(name)>255. DIR names are not length-checked before path concatenation/enqueue.
E-SRC-DOT-1=src/repo/zcl_abapgit_dot_abapgit.clas.abap: get_signature returns sha1_blob( serialize( ) ); serialize calls to_xml( ms_data ) and string_to_xstring_utf8_bom; to_xml serializes the parsed ms_data structure via transformation id and pretty print.
E-SRC-FILENAME-1=src/objects/core/zcl_abapgit_filename_logic.clas.abap: file_to_object parses filename/path, then dynamically calls ZCL_ABAPGIT_OBJECT_<type>~MAP_FILENAME_TO_OBJECT with iv_path, io_dot, iv_package.
E-SRC-LOCK-1=src/ortec/git/zcl_abapgit_ortec_pack_raw.clas.abap: acquire_repo_lock inserts LOCK_<repo_key> into zaog_fetch_sess; release_repo_lock deletes the lock row by session_id/status.
E-DDIC-1=src/ortec/git/zaog_obj_index.tabl.xml: REPO_KEY CHAR12, SHA1 fields CHAR40, FILE_PATH/FILE_NAME CHAR255, OBJ_TYPE CHAR4, OBJ_NAME CHAR40, IDX_STATUS CHAR1; no secondary index present.
E-DDIC-2=src/ortec/git/zaog_obj_store.tabl.xml: primary key CLIENT,REPO_KEY,OBJ_SHA1; object store is repo-scoped, not global.
E-GITDEF-1=src/git/zif_abapgit_git_definitions.intf.abap search result confirms ty_chmod TYPE c LENGTH 6.
E-VERSION-1=src/zif_abapgit_version.intf.abap search result confirms c_abap_version VALUE '1.133.0'.
```

## Cycle 1 Findings

```text
ID=AR-1-1
SEVERITY=BLOCKER
STATUS=OPEN
CLAIM=C7/algo_ver plus dot_sha1/devclass/path is a complete context gate for file_to_object output; old or stale memo rows become ineligible whenever mapping output could differ.
COUNTEREXAMPLE=A standard ZCL_ABAPGIT_OBJECT_<type>~MAP_FILENAME_TO_OBJECT implementation reads mutable SAP state (TADIR/customizing/namespace/package metadata, explicitly admitted by discovery/design). That state changes in the same system without an abapGit version bump and without an ORTEC c_index_algo_version bump. A fresh rebuild would map the same (filename,path,dot,devclass) differently, but the proposed key remains identical and a warm memo HIT replays the old obj_type/obj_name/path rows.
EVIDENCE=E-DISC-1, E-DESIGN-1, E-SRC-INDEX-3, E-SRC-FILENAME-1, E-VERSION-1
IMPACT=correctness; stale/partial/cross-context false positive; false HIT with byte-wrong index rows under a READY marker.
REQUIRED_CHANGE=Either prove every reachable MAP_FILENAME_TO_OBJECT used by rebuild_index is pure with respect to (filename,path,dot,devclass,code version), or amend the design to include a concrete invalidation/fingerprint for mutable SAP mapping state. If that cannot be made decision-free, restrict memo eligibility to object types whose mapping purity is proven and force MISS for all others. The residual-risk text about support-package upgrades is insufficient because it omits ordinary mutable customizing/TADIR/package-state changes.
RETEST=Add a closure proof that changes a mapping-relevant SAP-state input under identical repo_key+dot_sha1+devclass+algo_ver+tree_sha1+path and proves the memo is ineligible or disabled; then show row parity with a fresh full walk.
```

```text
ID=AR-1-2
SEVERITY=MAJOR
STATUS=OPEN
CLAIM=The SQL/memory model is bounded for 1,000,000 rows and never hides a per-row/per-object or oversized read; child reads/writes are chunk-flushed like index rows even for a single flat directory.
COUNTEREXAMPLE=A warm HIT for one flat root tree with 1,000,000 file children has exactly one lt_hit_keys row. The quoted child SELECT is SELECT * FROM zaog_tree_child FOR ALL ENTRIES IN lt_hit_keys ... INTO TABLE @lt_child, with no CHILD_SEQ range, PACKAGE SIZE, cursor loop, or streaming reproduction contract. That single SELECT loads every child row for the flat tree into memory at once before reproduction, contradicting the claimed 30000-row child chunk bound and making the 1,000,000-row acceptance case unbounded in practice.
EVIDENCE=E-DESIGN-1, E-SAT-1, E-DDIC-1
IMPACT=performance/memory; false scalability claim; weak implementer can create SYSTEM_NO_ROLL/TIME_OUT-class behavior on the exact large-repo acceptance case.
REQUIRED_CHANGE=Specify a decision-free child-read streaming plan: partition each hit tree's child rows by CHILD_SEQ ranges or another deterministic package key, reproduce rows chunk-by-chunk, and never materialize more than c_tree_child_chunk_size child rows for one tree in memory. Update SQL shapes, peak-memory model, TR2/TR3 packets, and tests/trace acceptance accordingly.
RETEST=Demonstrate, in design and Stage-4 trace criteria, a single flat directory with >30000 files where child SELECT packages are bounded and no SELECT returns the full child set at once.
```

```text
ID=AR-1-3
SEVERITY=MAJOR
STATUS=OPEN
CLAIM=The proposed zaog_tree_child CHILD_NAME CHAR255 plus MAP_SKIP='X' for oversized-name rows preserves byte-exact rebuild_index behavior.
COUNTEREXAMPLE=Current rebuild_index length-checks only FILE rows before writing zaog_obj_index. DIR names are concatenated into lv_next_path and enqueued without a 255-character guard. A tree memo must therefore reproduce DIR traversal with the exact child name/path, but zaog_tree_child-CHILD_NAME CHAR255 cannot store an over-255 Git directory component. Truncating and marking MAP_SKIP='X' is well-defined for skipped FILE rows, but not for DIR rows whose traversal controls descendant paths and lt_seen_trees first-path semantics.
EVIDENCE=E-DESIGN-1, E-SRC-INDEX-2, E-SRC-INDEX-3, E-DDIC-1
IMPACT=correctness; path-context divergence; byte-exact HIT reproduction not proven for valid Git trees containing oversized directory names or paths.
REQUIRED_CHANGE=Define an explicit DIR-overlength policy. Acceptable closure examples: make any tree containing a DIR child whose exact name cannot be stored in the memo ineligible for memo write/HIT; or use a DDIC representation that stores exact DIR child names and prove activation/storage limits. Do not reuse MAP_SKIP='X' for DIR without proving it preserves current traversal and same-tree-at-two-path behavior.
RETEST=Add a negative test with an oversized DIR component and a descendant file tree; prove memo write is skipped/demoted to MISS or exact reproduction equals a fresh full walk, including lt_seen_trees behavior when the same subtree appears elsewhere.
```

```text
ID=AR-1-4
SEVERITY=MAJOR
STATUS=OPEN
CLAIM=Stage-0's compute >= 30% gate is a settled prerequisite that can drive TR1-TR4 authorization/parking.
COUNTEREXAMPLE=The design calls the threshold a "proposed gate" but the weak-model TR0 packet treats it as an executable STOP_IF decision. No allowed evidence records owner approval of 30%, an SLA, or an engineering rationale that makes 30% non-arbitrary. A result at 29% parks a correctness-complete design; a result at 31% authorizes four implementation packets and two new DDIC tables. Both outcomes depend on an invented number.
EVIDENCE=E-DESIGN-1, E-DISC-1, E-PRIOR-1, E-SAT-1
IMPACT=owner-decision/false READY; implementation may proceed or be parked on an unapproved economic threshold.
REQUIRED_CHANGE=Before TR0 is used as a gate, either record explicit owner approval of the 30% threshold or replace it with an owner-decision checkpoint that reports measured compute/write/storage overhead without making the proceed/park decision. If retaining 30%, document why storage cost, implementation risk, and expected warm-hit rate make that threshold sufficient.
RETEST=Artifact contains an OWNER_DECISION row for the threshold or TR0 STOP_IF is changed to BLOCK_OWNER_DECISION with no autonomous proceed/park action.
```

```text
ID=AR-1-5
SEVERITY=MAJOR
STATUS=OPEN
CLAIM=C10's child-count integrity re-check detects memo rows deleted/changed between eligibility check and copy; a present-and-complete memo can never be silently stale or wrong.
COUNTEREXAMPLE=The specified validation is only loaded child count = header CHILD_COUNT. It detects missing rows, but not same-count changed rows under the same key (for example out-of-band admin repair/corruption, or any future writer bug that overwrites child_seq rows without changing the count). The design's own C10 wording includes "deleted/changed", but the check proves only non-equal cardinality, not rowset identity or per-sequence continuity against the header.
EVIDENCE=E-DESIGN-1, E-DISC-1
IMPACT=correctness; stale/changed memo false HIT; C10 closure proof is overstated.
REQUIRED_CHANGE=Either narrow C10's claim to deletion/cardinality loss only and classify same-count changed rows as out-of-scope corruption requiring admin clear, or strengthen the schema/check with a header rowset hash (over child_seq, chmod, child_name, child_sha1, map fields, map_skip) and verify it after the child SELECT before HIT reproduction. The weak packets must state this explicitly.
RETEST=Add a C10 negative test where child_count matches but one child row differs; expected result is demotion to MISS or documented rejection of the scenario as external DB corruption with an operator recovery step.
```

## Verified Non-Findings / Constraints That Held

```text
NF-1 dot signature path: get_signature is deterministic for the parsed ms_data serialization and byte-exact for serialize( ) output. It is not order-independent in the abstract; ordered tables such as ignore remain order-sensitive. The design's own conservative-miss handling for reordered .abapgit bytes is acceptable, subject to AR-1-1 for mutable plugin state.
NF-2 existing lock placement: current rebuild_index holds the per-repo_key lock across delete, fetch, BFS, final flush, and READY marker write, with release on success and both catch paths. The design is correct only if all new memo reads/writes remain inside this exact locked region.
NF-3 lt_seen_trees composition: preserving tree_sha1-only seen semantics is consistent with current behavior. The design correctly treats tree_path_hash as a cross-build reuse key, not as a change to same-invocation duplicate-tree behavior.
NF-4 additive DDIC: two new tables are additive relative to zaog_obj_index/zaog_obj_store; REPO_KEY CHAR12, SHA1 CHAR40, CHMOD CHAR6, and client-dependent/unbuffered conventions are consistent with current source, except for AR-1-3's memo-specific CHILD_NAME exactness gap.
NF-5 no blob payload in eligibility/copy: the summarized design and packets do not require blob payload reads during memo eligibility or HIT copy. The only hidden scalability violation found is unbounded child metadata loading, AR-1-2.
```

## Ledger

```text
CYCLE=1
OPEN_BLOCKER=AR-1-1
OPEN_MAJOR=AR-1-2,AR-1-3,AR-1-4,AR-1-5
OPEN_MINOR=none
CLOSED=none
CHANGED_SECTIONS_REQUIRED=§2,§3,§5,§8,§9,§10,§11/TR0,§11/TR2,§11/TR3,§12,§13
VERDICT=REVISE
NEXT=Revise design to close AR-1-1 through AR-1-5, then rerun adversarial review cycle 2 against the full revised artifact and closure tests.
```

---

```text
PACKET=COMPACT_HANDOFF_V1
TASK=E1-TREE-REUSE-ADVERSARIAL-V1
CYCLE=2
BASELINE=36839c7faa55b4568c1724cf6232468957f6aac8
REVIEW_MODE=ortec-abapgit-adversarial-design-review
VERDICT=REVISE
```

## Cycle 2 Scope Compliance

```text
ALLOWED_CONTEXT_READ=YES
PRODUCTIVE_SOURCE_WRITES=NO
STATE_WRITE=NO
DIAGRAM_WRITE=NO
OUTPUT_ARTIFACT=.memory/reviews/variant_b_package_e_e1_tree_reuse_adversarial.md
```

## Cycle 2 Evidence Matrix

```text
E2-DESIGN-1=.memory/logs/variant_b_package_e_e1_tree_reuse_design.md CYCLE=2 read completely, including "Cycle 2 revision responses" and §§1-14.
E2-LEDGER-1=.memory/reviews/variant_b_package_e_e1_tree_reuse_adversarial.md cycle-1 ledger AR-1-1..AR-1-5 re-read and verified against the revised design.
E2-DISC-1=.memory/logs/variant_b_package_e_e1_tree_reuse_discovery.md read for original call path, lock facts, test conventions, and deferred-tree-reuse scope.
E2-PRIOR-1=.memory/logs/variant_b_package_e_design.md §§2 and 8 read; confirms E1-D/E1-E were deferred pending richer key/design gate.
E2-SAT-1=.memory/incidents/variant_b_d2_sat_warm_to_cold_o4h8794.md read for measured 42k-row cost, row-count spread, and SQL/HTTP baseline.
E2-SRC-ADMIN-1=src/ortec/git/zcl_abapgit_ortec_cache_admin.clas.abap: clear_repo exists; ty_clear_result and format_clear_result exist; clear_repo deletes zaog_obj_index/pack_idx/raw_pack/pack_meta/fetch_sess/commit_hist/obj_store/repo_state and commits once.
E2-SRC-ADMIN-2=src/ortec/git/zcl_abapgit_ortec_cache_admin.clas.abap: clear_repo acquire_lock uses ENQUEUE_EZAOG_REPO_LOCK with session_id = iv_repo_key, then DELETE FROM zaog_fetch_sess WHERE repo_key = iv_repo_key.
E2-SRC-INDEX-1=src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap: rebuild_index uses zcl_abapgit_ortec_pack_raw=>acquire_repo_lock, delete-first, BFS, c_index_write_chunk_size=30000, marker-last, and releases on success and both catch paths.
E2-SRC-INDEX-2=src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap: current DIR branch concatenates/enqueues without a 255 guard; FILE branch has strlen(path/name)>255 guard before row append.
E2-SRC-RAWLOCK-1=src/ortec/git/zcl_abapgit_ortec_pack_raw.clas.abap: acquire_repo_lock inserts zaog_fetch_sess session_id = |LOCK_{ iv_repo_key }|, repo_key = iv_repo_key; release deletes the same row by session_id/status.
E2-DDIC-LOCK-1=src/ortec/git/ezaog_repo_lock.enqu.xml: enqueue object EZAOG_REPO_LOCK is keyed by ZAOG_FETCH_SESS CLIENT and SESSION_ID.
E2-DDIC-INDEX-1=src/ortec/git/zaog_obj_index.tabl.xml: existing index table has CHAR255 FILE_PATH/FILE_NAME, CHAR40 SHA1 fields, no secondary index.
E2-SRC-FILENAME-1=src/objects/core/zcl_abapgit_filename_logic.clas.abap: file_to_object calls dynamic MAP_FILENAME_TO_OBJECT with iv_path, io_dot, iv_package.
E2-SRC-DOT-1=src/repo/zcl_abapgit_dot_abapgit.clas.abap: get_signature returns sha1_blob( serialize( ) ); serialize is to_xml(ms_data) converted to UTF-8 BOM xstring.
```

## Cycle 1 Closure Verification

```text
ID=AR-1-1
STATUS=FIXED_WITH_NEW_FINDING_AR-2-1
VERIFICATION=The revised design adds a concrete TTL constant c_tree_memo_max_age_secs VALUE 604800, a BUILT_AT TIMESTAMP column on zaog_tree_map, and an eligibility SELECT predicate built_at >= lv_cutoff in §3/§5.1/§9/§11-TR2. The claimed E3 admin surface is real: clear_repo, ty_clear_result, and format_clear_result exist in zcl_abapgit_ortec_cache_admin.clas.abap. The original unbounded mutable-state stale-hit risk is now bounded by TTL plus an operator clear path. However, the admin clear path is not serialized with rebuild_index's real lock; opened separately as AR-2-1.
EVIDENCE=E2-DESIGN-1,E2-SRC-ADMIN-1,E2-SRC-ADMIN-2,E2-SRC-RAWLOCK-1,E2-DDIC-LOCK-1
```

```text
ID=AR-1-2
STATUS=FIXED_WITH_NEW_FINDING_AR-2-2
VERIFICATION=The revised design replaces the cycle-1 unbounded child FAE with a split SMALL/LARGE contract. A single flat directory with >30000 or 1,000,000 children is LARGE and read alone through CHILD_SEQ windows, so the original one-SELECT-full-flat-tree counterexample is closed. The revised text also introduces an internal peak-memory contradiction for SMALL batches (one chunk vs <=~2x chunk); opened separately as AR-2-2.
EVIDENCE=E2-DESIGN-1
```

```text
ID=AR-1-3
STATUS=FIXED
VERIFICATION=The revised design defines an explicit DIR-overlength policy: apply the same 255 guard in the fresh MISS walk, HIT reproduction, and current unmemoized path; skipped DIRs are not enqueued and not memoized. The current source confirms the previous asymmetry: FILE rows were guarded, DIR rows were not. The design explicitly documents the only behavior-changing synthesized case (oversized first path plus normal later path) and classifies it as strictly-more-correct and unreachable for mainstream filesystem-origin repos. This is a deliberate in-scope behavioral amendment rather than an unstated memo truncation gap.
EVIDENCE=E2-DESIGN-1,E2-SRC-INDEX-2,E2-DDIC-INDEX-1
```

```text
ID=AR-1-4
STATUS=FIXED
VERIFICATION=§4, §11/TR0, §12, §13, and §14 consistently state Stage-0 is BLOCK_OWNER_DECISION: TR0 measures/reports and stops; TR1-TR5 require an explicit later owner go decision. The former 30% figure remains only as non-binding engineering reference, with no autonomous proceed/park instruction left.
EVIDENCE=E2-DESIGN-1
```

```text
ID=AR-1-5
STATUS=FIXED
VERIFICATION=C10 is now consistently scoped to child-count/cardinality deletion loss only. Same-count rowset/content corruption is explicitly classified as C12 OUT-OF-SCOPE external DB corruption in §6, §8, §10, and §11; the design no longer claims the count check proves rowset identity.
EVIDENCE=E2-DESIGN-1
```

## Cycle 2 Findings

```text
ID=AR-2-1
SEVERITY=MAJOR
CLAIM=TR5's cache-admin memo clear is a safe operator recovery channel and C8/C10 are serialized by the existing per-repo_key lock.
COUNTEREXAMPLE=rebuild_index's actual lock is zcl_abapgit_ortec_pack_raw=>acquire_repo_lock, which inserts a ZAOG_FETCH_SESS row with SESSION_ID = `LOCK_<repo_key>` and REPO_KEY = repo_key. clear_repo's actual admin lock is a different mechanism/key: ENQUEUE_EZAOG_REPO_LOCK with SESSION_ID = iv_repo_key, while the enqueue object is keyed by CLIENT+SESSION_ID. Therefore clear_repo does not acquire the same lock key as rebuild_index. Worse, clear_repo then executes DELETE FROM zaog_fetch_sess WHERE repo_key = iv_repo_key, which targets the row shape used by rebuild_index's persistent lock. A TR5 clear can therefore overlap an in-flight rebuild/memo read/write rather than being excluded by C8, delete memo/index rows while the rebuild is using them, and potentially remove the lock row the design claims serializes the work.
EVIDENCE=E2-DESIGN-1,E2-SRC-ADMIN-2,E2-SRC-INDEX-1,E2-SRC-RAWLOCK-1,E2-DDIC-LOCK-1
IMPACT=concurrency; lock divergence; TOCTOU; false READY or stale/missing memo/index rows under an operator clear.
REQUIRED_CHANGE=Amend TR5/C8/C10 so cache-admin clear and rebuild_index share one concrete repo mutex. Either clear_repo must acquire the same `LOCK_<repo_key>` lock primitive used by rebuild_index and fail/block while a rebuild is active, or rebuild_index/admin must be moved to one common enqueue key and the zaog_fetch_sess deletion must explicitly preserve/sequence active lock rows. The design must state the exact lock key, lock order, busy behavior, and whether clear waits or rejects; then update tests to cover clear-during-rebuild.
RETEST=With a simulated active rebuild lock for repo R, clear_repo(R) is proven to reject or wait without deleting the rebuild lock row or any memo/index rows. A concurrent rebuild+clear test or live validation proves no interleaving can delete zaog_tree_map/zaog_tree_child/zaog_obj_index between eligibility/reproduction and marker-last publication.
```

```text
ID=AR-2-2
SEVERITY=MAJOR
CLAIM=The revised SQL/memory model is decision-free and consistently bounds peak resident child rows to at most one c_tree_child_chunk_size window.
COUNTEREXAMPLE=The revised §9 SMALL-tree plan closes a batch only after adding keys until `sum(child_count) >= c_tree_child_chunk_size`, and the same section explicitly admits each SMALL FAE can return `<= ~2x chunk` rows or one chunk plus one additional small tree's worth. Later §12/§13 claim peak resident child rows are `at most one c_tree_child_chunk_size window` and `<= one chunk`. A weak implementer following §11/TR2/TR3 can therefore build a SMALL batch with two 29,999-child trees, load 59,998 child rows in one FAE, and still believe the one-chunk acceptance criterion is satisfied because the design contradicts itself.
EVIDENCE=E2-DESIGN-1
IMPACT=performance/memory; false scalability claim; acceptance test can pass the LARGE flat-dir case while violating the general peak-memory property asserted in §12/§13.
REQUIRED_CHANGE=Make the contract single-valued. Either change SMALL batching to close before the row that would exceed c_tree_child_chunk_size (or read the overflow tree in the next batch) so no FAE can exceed one chunk, or explicitly accept a <=2x child-row bound and update §9, §12, §13, the 1,000,000-row table, and reuse_flat_dir_paged/trace acceptance criteria to that larger bound.
RETEST=Add a boundary test with multiple SMALL hit trees whose child_count values straddle the chunk threshold (for example chunk-1 plus chunk-1). The test must prove either no child SELECT returns more than one chunk, or the artifact's updated accepted bound is exactly observed and consistently documented.
```

## Cycle 2 Ledger

```text
CYCLE=2
OPEN_BLOCKER=none
OPEN_MAJOR=AR-2-1,AR-2-2
OPEN_MINOR=none
CLOSED=AR-1-1,AR-1-2,AR-1-3,AR-1-4,AR-1-5
VERDICT=REVISE
NEXT=Revise TR5/C8/C10 locking and make the child-read peak-memory bound internally consistent, then rerun adversarial review cycle 3 against the full artifact.
```

---

```text
PACKET=COMPACT_HANDOFF_V1
TASK=E1-TREE-REUSE-ADVERSARIAL-V1
CYCLE=3
BASELINE=36839c7faa55b4568c1724cf6232468957f6aac8
REVIEW_MODE=ortec-abapgit-adversarial-design-review
VERDICT=BLOCK_OWNER_DECISION
```

## Cycle 3 Scope Compliance

```text
ALLOWED_CONTEXT_READ=YES
PRODUCTIVE_SOURCE_WRITES=NO
STATE_WRITE=NO
DIAGRAM_WRITE=NO
OUTPUT_ARTIFACT=.memory/reviews/variant_b_package_e_e1_tree_reuse_adversarial.md
FINAL_AUTOMATIC_CYCLE=YES
```

## Cycle 3 Evidence Matrix

```text
E3-DESIGN-1=.memory/logs/variant_b_package_e_e1_tree_reuse_design.md CYCLE=3 read completely, including §§1-14 and Cycle 2/3 revision responses.
E3-LEDGER-1=.memory/reviews/variant_b_package_e_e1_tree_reuse_adversarial.md cycles 1-2 re-read; AR-2-1 and AR-2-2 closure claims verified against revised design and current source.
E3-DISC-1=.memory/logs/variant_b_package_e_e1_tree_reuse_discovery.md read for current call path, lock facts, test conventions, and deferred-tree-reuse scope.
E3-SRC-RAWLOCK-1=src/ortec/git/zcl_abapgit_ortec_pack_raw.clas.abap: acquire_repo_lock returns rv_lock_id = |LOCK_{ iv_repo_key }|, inserts zaog_fetch_sess with session_id = rv_lock_id/status='L', retries with bounded backoff, and raises on timeout.
E3-SRC-RAWLOCK-2=src/ortec/git/zcl_abapgit_ortec_pack_raw.clas.abap: release_repo_lock imports iv_lock_id and deletes zaog_fetch_sess WHERE session_id = iv_lock_id AND status='L'.
E3-SRC-ADMIN-1=src/ortec/git/zcl_abapgit_ortec_cache_admin.clas.abap: current clear_repo still uses ENQUEUE_EZAOG_REPO_LOCK keyed by session_id = iv_repo_key and DELETE FROM zaog_fetch_sess WHERE repo_key = iv_repo_key; revised design correctly avoids routing clear_tree_memo through clear_repo.
E3-SRC-INDEX-1=src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap: rebuild_index acquires zcl_abapgit_ortec_pack_raw=>acquire_repo_lock before delete/BFS/marker and releases the returned lv_lock_id on success and both catch paths.
E3-DDIC-LOCK-1=src/ortec/git/ezaog_repo_lock.enqu.xml: EZAOG_REPO_LOCK is keyed by ZAOG_FETCH_SESS CLIENT + SESSION_ID, confirming it is not the LOCK_<repo_key> row mutex used by rebuild_index.
E3-GREP-1=Full-design grep for AR-2-2 stale terms found ~2x/59,998/one-additional-small only in historical correction text or negated "never/no" statements; operative §9/§11/§12/§13 text uses peek-then-decide and <= c_tree_child_chunk_size.
E3-GREP-2=Full-design grep for TBD/as appropriate/to-be-decided found no operative placeholder language; the only "TBD/as appropriate" hit is the compliance assertion that none remains.
```

## Cycle 2 Closure Verification

```text
ID=AR-2-1
STATUS=NOT_CLOSED_OPENED_AR-3-1
VERIFICATION=The revised design correctly identifies the old clear_repo route as unsafe and specifies a new clear_tree_memo method that should use pack_raw's LOCK_<repo_key> mutex, delete only zaog_tree_child/zaog_tree_map, and wait-then-reject while busy. However, the weak-model TR5 exact sequence calls acquire_repo_lock without assigning its returned lock id, then calls release_repo_lock( iv_repo_key ). Current source proves release_repo_lock deletes by session_id = iv_lock_id, while acquire_repo_lock created session_id = |LOCK_{ iv_repo_key }|. The TR5 packet therefore does not actually release the same mutex it acquires. AR-2-1 remains unclosed via AR-3-1.
EVIDENCE=E3-DESIGN-1,E3-SRC-RAWLOCK-1,E3-SRC-RAWLOCK-2,E3-SRC-ADMIN-1,E3-SRC-INDEX-1
```

```text
ID=AR-2-2
STATUS=FIXED
VERIFICATION=The revised design makes SMALL-hit batching single-valued: close the current batch before adding any tree whose child_count would push the running sum over c_tree_child_chunk_size, with key-count as a second cap. §9, TR2, TR3, §12, §13, the 1,000,000-row table, and reuse_small_batch_bound all state the strict <= one-chunk child-row bound. Remaining ~2x/59,998 wording is historical or negated correction text, not an operative contract.
EVIDENCE=E3-DESIGN-1,E3-GREP-1
```

## Cycle 3 Findings

```text
ID=AR-3-1
SEVERITY=BLOCKER
CLAIM=The new self-contained clear_tree_memo action acquires and releases the SAME LOCK_<repo_key> mutex used by rebuild_index, so it cannot deadlock or leave the repository busy after a successful clear.
COUNTEREXAMPLE=TR5's exact sequence invokes zcl_abapgit_ortec_pack_raw=>acquire_repo_lock( iv_repo_key ) but does not store the returned lv_lock_id = |LOCK_<repo_key>|. On the success path it later invokes release_repo_lock( iv_repo_key ). Current pack_raw source proves release_repo_lock deletes WHERE session_id = iv_lock_id AND status='L'. Passing the bare repo key deletes session_id = <repo_key>, not session_id = LOCK_<repo_key>. The successful clear then commits after deleting only memo rows, leaving its own LOCK_<repo_key> mutex row durable. Future rebuild_index and clear_tree_memo calls for that repo wait until timeout/reject, and the required "same acquire/release mutex" proof is false.
EVIDENCE=E3-DESIGN-1,E3-SRC-RAWLOCK-1,E3-SRC-RAWLOCK-2,E3-SRC-INDEX-1
IMPACT=concurrency; lock leak; denial of service; false closure of AR-2-1; decision-free implementation spec is unsafe.
REQUIRED_CHANGE=Amend TR5 to assign DATA(lv_lock_id) = zcl_abapgit_ortec_pack_raw=>acquire_repo_lock( iv_repo_key = iv_repo_key ) and call zcl_abapgit_ortec_pack_raw=>release_repo_lock( lv_lock_id ) on every path after successful acquire. Also add an after-success proof/test that no zaog_fetch_sess row with session_id = |LOCK_<repo_key>| remains after COMMIT WORK AND WAIT, and keep the busy test proving a pre-existing LOCK_<repo_key> row is not deleted.
RETEST=Seed memo rows, call clear_tree_memo, then verify zaog_tree_child/zaog_tree_map rows for the repo are gone, unrelated repos survive, and SELECT SINGLE from zaog_fetch_sess WHERE session_id = |LOCK_<repo_key>| AND status='L' returns no row. Then rerun clear_memo_rejects_if_locked with a pre-inserted LOCK_<repo_key> row and prove it is still present after reject.
```

## Cycle 3 Full-Document Sanity

```text
SANITY_AR_2_2=PASS. Operative batching text is strict peek-then-decide with <= c_tree_child_chunk_size per SMALL FAE and CHILD_SEQ windows for LARGE hits.
SANITY_PLACEHOLDERS=PASS. No operative TBD/as-appropriate/unresolved-alternative wording found in the revised design.
SANITY_NEW_CONTRADICTION=FAIL. TR5 release argument contradicts the current pack_raw lock contract and the surrounding claim that clear_tree_memo releases its own LOCK_<repo_key> row.
```

## Cycle 3 Ledger

```text
CYCLE=3
OPEN_BLOCKER=AR-3-1
OPEN_MAJOR=none
OPEN_MINOR=none
CLOSED=AR-1-1,AR-1-2,AR-1-3,AR-1-4,AR-1-5,AR-2-2
STILL_NOT_CLOSED=AR-2-1 via AR-3-1
VERDICT=BLOCK_OWNER_DECISION
NEXT=Owner decision required: amend TR5 lock-id capture/release and rerun only by explicit owner authorization, since automatic review cycles are exhausted.
```

---

```text
PACKET=COMPACT_HANDOFF_V1
TASK=E1-TREE-REUSE-ADVERSARIAL-V1
CYCLE=4
BASELINE=36839c7faa55b4568c1724cf6232468957f6aac8
REVIEW_MODE=ortec-abapgit-adversarial-design-review
VERDICT=REVISE
```

## Cycle 4 Scope Compliance

```text
ALLOWED_CONTEXT_READ=YES
PRODUCTIVE_SOURCE_WRITES=NO
STATE_WRITE=NO
DIAGRAM_WRITE=NO
OUTPUT_ARTIFACT=.memory/reviews/variant_b_package_e_e1_tree_reuse_adversarial.md
OWNER_ADDITIONAL_CYCLE_BUDGET=cycle 1 of 3 for AR-3-1 convergence
```

## Cycle 4 Evidence Matrix

```text
E4-DESIGN-1=.memory/logs/variant_b_package_e_e1_tree_reuse_design.md CYCLE=4 read completely, including Cycle 4 revision responses, §11/TR5, §12, §13, and §14.
E4-LEDGER-1=.memory/reviews/variant_b_package_e_e1_tree_reuse_adversarial.md cycles 1-3 re-read; AR-3-1 closure claim verified against revised design and source.
E4-DISC-1=.memory/logs/variant_b_package_e_e1_tree_reuse_discovery.md read for current call path, lock facts, and transaction-owner baseline.
E4-SRC-RAWLOCK-1=src/ortec/git/zcl_abapgit_ortec_pack_raw.clas.abap: acquire_repo_lock returns rv_lock_id = |LOCK_{ iv_repo_key }| and INSERTs zaog_fetch_sess with session_id = rv_lock_id, repo_key = iv_repo_key, status='L'. WAIT occurs only after failed INSERT attempts; after a successful INSERT it returns immediately.
E4-SRC-RAWLOCK-2=src/ortec/git/zcl_abapgit_ortec_pack_raw.clas.abap: release_repo_lock guards initial input and DELETEs zaog_fetch_sess WHERE session_id = iv_lock_id AND status = 'L'; it has no COMMIT WORK.
E4-SRC-INDEX-1=src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap: rebuild_index captures lv_lock_id, calls release_repo_lock( lv_lock_id ) on early-ready return, success, zcx_abapgit_exception CATCH, and cx_root CATCH; it issues no COMMIT WORK and no ROLLBACK WORK.
E4-SRC-ADMIN-1=src/ortec/git/zcl_abapgit_ortec_cache_admin.clas.abap: current clear_repo uses the separate ENQUEUE_EZAOG_REPO_LOCK primitive keyed by session_id = iv_repo_key, performs broad repo_key deletes including zaog_fetch_sess, COMMIT WORK AND WAIT on success, and ROLLBACK WORK plus release_lock on error; TR5 correctly does not route through this method.
E4-GREP-1=Full-design grep for acquire_repo_lock/release_repo_lock/ROLLBACK WORK/COMMIT WORK AND WAIT found operative Cycle 4/TR5 text saying the error path is released by ROLLBACK WORK and warning not to call release_repo_lock after rollback; §13 repeats that this matches rebuild_index's exact capture/release pattern.
```

## Cycle 3 Closure Verification

```text
ID=AR-3-1
STATUS=NOT_CLOSED_OPENED_AR-4-1
VERIFICATION=The Cycle 4 design fixes the success-path defect from AR-3-1: TR5 now declares lv_lock_id, assigns the return value of acquire_repo_lock, and calls release_repo_lock( lv_lock_id ) before COMMIT WORK AND WAIT. That closes the original bare-repo-key release bug for successful clears. However, the revised error path deliberately does not call release_repo_lock and instead relies on ROLLBACK WORK to discard the uncommitted acquire INSERT. Current source proves this is a different pattern from rebuild_index, whose CATCH handlers explicitly release the captured lock id and never use rollback semantics. The design does not prove that no commit point can ever be introduced between acquire and catch in the admin action's future implementation context, and its §13 claim that the rollback path matches rebuild_index is false. AR-3-1 therefore remains unclosed through AR-4-1.
EVIDENCE=E4-DESIGN-1,E4-SRC-RAWLOCK-1,E4-SRC-RAWLOCK-2,E4-SRC-INDEX-1,E4-GREP-1
```

## Cycle 4 Findings

```text
ID=AR-4-1
SEVERITY=MAJOR
CLAIM=TR5's error path is safe because ROLLBACK WORK releases the mutex by discarding the uncommitted acquire_repo_lock INSERT, and an explicit release_repo_lock after rollback must not be added.
COUNTEREXAMPLE=The proof is only valid for the current textual mini-sequence if no COMMIT WORK, implicit commit point, update-task boundary, screen transition, or nested helper with its own commit can occur after acquire_repo_lock successfully INSERTs LOCK_<repo_key> and before the CATCH executes ROLLBACK WORK. That is not a stable weak-model contract for a new public admin method that already owns transaction boundaries and explicitly COMMITs on success. If any later helper or UI/admin wrapper introduces a commit after acquire but before an error, the subsequent ROLLBACK WORK no longer removes the durable LOCK_<repo_key> row. Because TR5 forbids an explicit release after rollback, the repo mutex can remain durable and future rebuild_index/clear_tree_memo calls for that repo wait until timeout/reject. Current rebuild_index avoids this proof obligation by explicitly calling release_repo_lock( lv_lock_id ) in both exception handlers; TR5 does not mirror it.
EVIDENCE=E4-DESIGN-1,E4-SRC-RAWLOCK-1,E4-SRC-RAWLOCK-2,E4-SRC-INDEX-1,E4-SRC-ADMIN-1,E4-GREP-1
IMPACT=concurrency; lock leak; denial of service; false closure of AR-3-1; weak-model implementation can depend on a fragile LUW assumption instead of the shared primitive's release contract.
REQUIRED_CHANGE=Amend TR5 so every path after a successful acquire has an explicit captured-id cleanup. A robust error shape is: CATCH cx_root; ROLLBACK WORK to discard memo-table deletes; zcl_abapgit_ortec_pack_raw=>release_repo_lock( lv_lock_id ); COMMIT WORK AND WAIT to make the cleanup durable if the mutex row had become committed; then raise zcx_abapgit_ortec_git. If the owner rejects a catch-local cleanup commit, then the design must instead prove no commit point can occur between acquire and catch in every reachable call context, including future formatter/helper/admin-report integration. Remove the warning that release_repo_lock after rollback is wrong, and remove the §13 statement that the rollback-only path matches rebuild_index's exact pattern.
RETEST=Seed a durable LOCK_<repo_key> status='L' row or simulate a commit after acquire and before a forced error; call clear_tree_memo's error path; prove after the raised zcx_abapgit_ortec_git that SELECT SINGLE zaog_fetch_sess WHERE session_id = |LOCK_<repo_key>| AND status='L' returns no row, memo rows are either rolled back or unchanged per the forced-error point, and a subsequent acquire_repo_lock for the same repo succeeds. Also rerun the normal success and busy-reject tests.
```

## Cycle 4 Full-Document Sanity

```text
SANITY_AR_3_1_SUCCESS=PASS. TR5 now captures lv_lock_id and uses release_repo_lock( lv_lock_id ) on the success path before COMMIT WORK AND WAIT.
SANITY_ERROR_PATH=FAIL. Operative TR5 text and §13 still assert rollback-only release and claim equivalence to rebuild_index, but current rebuild_index explicitly releases in both CATCH handlers and never uses ROLLBACK WORK.
SANITY_COMMIT_SUCCESS=PASS_WITH_NOTE. COMMIT WORK AND WAIT after release on TR5 success does not by itself create a new mutex interaction risk: the own LOCK_<repo_key> row is deleted before the commit, so waiters can acquire after the LUW ends. The design should still state clear_tree_memo is an admin LUW owner, because §12's broad "no new COMMIT WORK anywhere" wording is stale for TR5.
SANITY_CLEAR_REPO_PRIMITIVE=PASS. Current clear_repo remains untouched and still uses the divergent enqueue primitive plus broad repo_key deletion; TR5 correctly stays dedicated rather than routing through it.
```

## Cycle 4 Ledger

```text
CYCLE=4
OPEN_BLOCKER=none
OPEN_MAJOR=AR-4-1
OPEN_MINOR=none
CLOSED=AR-1-1,AR-1-2,AR-1-3,AR-1-4,AR-1-5,AR-2-2
STILL_NOT_CLOSED=AR-2-1 via AR-3-1 via AR-4-1
VERDICT=REVISE
NEXT=Revise TR5 to explicitly release the captured lock id on the error path after rollback with durable cleanup semantics, remove stale rollback-equivalence text, then rerun adversarial cycle 5 within the owner-authorized additional-cycle budget.
```

---

```text
PACKET=COMPACT_HANDOFF_V1
TASK=E1-TREE-REUSE-ADVERSARIAL-V1
CYCLE=5
BASELINE=36839c7faa55b4568c1724cf6232468957f6aac8
REVIEW_MODE=ortec-abapgit-adversarial-design-review
VERDICT=APPROVE
```

## Cycle 5 Scope Compliance

```text
ALLOWED_CONTEXT_READ=YES
PRODUCTIVE_SOURCE_WRITES=NO
STATE_WRITE=NO
DIAGRAM_WRITE=NO
OUTPUT_ARTIFACT=.memory/reviews/variant_b_package_e_e1_tree_reuse_adversarial.md
FINAL_OWNER_EXTENSION_CYCLE=YES
```

## Cycle 5 Evidence Matrix

```text
E5-DESIGN-1=.memory/logs/variant_b_package_e_e1_tree_reuse_design.md CYCLE=5 read completely, especially Cycle 5 responses, §8/C14, §9, §11/TR5, §12, §13, and §14.
E5-LEDGER-1=.memory/reviews/variant_b_package_e_e1_tree_reuse_adversarial.md cycles 1-4 re-read; all prior closure claims re-verified against the Cycle 5 design and current source.
E5-DISC-1=.memory/logs/variant_b_package_e_e1_tree_reuse_discovery.md read for current lock, transaction, call-path, and test-convention baseline.
E5-SRC-RAWLOCK-1=src/ortec/git/zcl_abapgit_ortec_pack_raw.clas.abap: acquire_repo_lock returns rv_lock_id = |LOCK_{ iv_repo_key }| and inserts zaog_fetch_sess with session_id = rv_lock_id/status='L'.
E5-SRC-RAWLOCK-2=src/ortec/git/zcl_abapgit_ortec_pack_raw.clas.abap: release_repo_lock first returns if iv_lock_id IS INITIAL, then DELETEs zaog_fetch_sess WHERE session_id = iv_lock_id AND status = 'L'; it has no COMMIT WORK and a second/missing-row release is a no-op.
E5-SRC-INDEX-1=src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap: rebuild_index captures lv_lock_id, calls release_repo_lock( lv_lock_id ) on ready return, success, zcx catch, and cx_root catch, and contains no COMMIT WORK/ROLLBACK WORK.
E5-SRC-ADMIN-1=src/ortec/git/zcl_abapgit_ortec_cache_admin.clas.abap: current clear_repo remains untouched; it uses ENQUEUE_EZAOG_REPO_LOCK keyed by session_id = iv_repo_key, broad DELETE FROM zaog_fetch_sess WHERE repo_key, COMMIT on success, and ROLLBACK+release_lock on error.
E5-GREP-1=Full-design grep for ROLLBACK WORK/release_repo_lock/matches rebuild_index/exact pattern/no new COMMIT WORK/COMMIT WORK found no operative stale rollback-only or rebuild_index-equivalence claim. Remaining hits are historical/superseded text, explicit removal warnings, or the new captured-id release+commit contract.
```

## Cycle 4 Closure Verification

```text
ID=AR-4-1
STATUS=ACCEPTED_AND_FIXED
VERIFICATION=Cycle 5 TR5 now specifies the robust error shape required by AR-4-1: after a successful acquire, CATCH cx_root executes ROLLBACK WORK, then zcl_abapgit_ortec_pack_raw=>release_repo_lock( lv_lock_id ), then COMMIT WORK AND WAIT, then re-raises zcx_abapgit_ortec_git. The release argument is explicitly the captured lv_lock_id, not iv_repo_key. Source confirms that this is the only argument capable of deleting the actual LOCK_<repo_key> row, and that a repeated or absent release is safe because release_repo_lock guards initial input and DELETEs only status='L' by session_id. The design no longer claims this matches rebuild_index's exact pattern; it justifies the difference on clear_tree_memo's own admin LUW ownership. §10 requires clear_memo_err_releases_lock to prove no LOCK_<repo_key> status='L' row remains and a subsequent acquire succeeds.
EVIDENCE=E5-DESIGN-1,E5-SRC-RAWLOCK-1,E5-SRC-RAWLOCK-2,E5-SRC-INDEX-1,E5-GREP-1
```

```text
ID=AR-3-1
STATUS=FIXED
VERIFICATION=The full AR-3-1 chain is now closed: TR5 declares lv_lock_id, assigns acquire_repo_lock( iv_repo_key = iv_repo_key ), and releases with release_repo_lock( lv_lock_id ) on success and error. The bare iv_repo_key is explicitly forbidden as a release argument. The busy/reject path still has no release or commit because acquire raised before inserting a mutex row. clear_memo_removes_rows and clear_memo_rejects_if_locked remain specified to prove success cleanup and busy-row preservation.
EVIDENCE=E5-DESIGN-1,E5-SRC-RAWLOCK-1,E5-SRC-RAWLOCK-2,E5-SRC-INDEX-1
```

## Prior Closure Re-Verification

```text
AR-1-1=STILL_FIXED. Mandatory 7-day TTL plus dedicated clear_tree_memo remain present; clear_tree_memo is no longer routed through clear_repo.
AR-1-2=STILL_FIXED. LARGE hits use CHILD_SEQ windows; no full flat-tree child SELECT remains.
AR-1-3=STILL_FIXED. DIR 255 guard remains specified for MISS walk, HIT reproduction, and current path.
AR-1-4=STILL_FIXED. TR0 remains BLOCK_OWNER_DECISION; no autonomous 30% proceed/park gate is operative.
AR-1-5=STILL_FIXED. C10 remains narrowed to cardinality/deletion loss; same-count corruption is C12/OOB with TTL+clear recovery.
AR-2-1=STILL_FIXED. TR5 uses the same LOCK_<repo_key> row mutex as rebuild_index, deletes only zaog_tree_child/zaog_tree_map, never issues DELETE FROM zaog_fetch_sess WHERE repo_key, and leaves current clear_repo untouched.
AR-2-2=STILL_FIXED. SMALL-hit batching remains peek-then-decide with a strict <= c_tree_child_chunk_size child-row bound per FAE.
```

## Cycle 5 Full-Document Sanity

```text
SANITY_STALE_ROLLBACK_TEXT=PASS. Rollback-only cleanup survives only as historical defect description or superseded text; operative TR5 uses ROLLBACK WORK -> release_repo_lock( lv_lock_id ) -> COMMIT WORK AND WAIT.
SANITY_REBUILD_EQUIVALENCE=PASS. Operative text says clear_tree_memo deliberately differs from rebuild_index because it owns its admin LUW; no remaining operative claim says the error path matches rebuild_index's exact pattern.
SANITY_COMMIT_SCOPE=PASS. §9 and §12 scope the no-new-COMMIT claim to TR1-TR4/rebuild_index only, with an explicit TR5 carve-out for success and error cleanup commits. This does not invalidate TR1-TR4: current rebuild_index source still has no COMMIT WORK and TR1-TR4 packets continue to state no COMMIT WORK.
SANITY_DOUBLE_RELEASE=PASS. Two commit points in clear_tree_memo do not create a new double-release/double-delete risk: success releases before its commit; error releases after rollback and commits cleanup. If a future exception occurs after an attempted release, release_repo_lock is idempotent for missing/initial rows and deletes only session_id=lv_lock_id AND status='L', so a second call cannot delete active sessions or another repo's mutex.
SANITY_CLEAR_REPO_UNTOUCHED=PASS. Current clear_repo still uses the divergent enqueue primitive and broad repo_key deletion; the design continues to keep clear_tree_memo dedicated and does not reopen clear_repo.
SANITY_NEW_FINDING=PASS. No new BLOCKER/MAJOR issue found in the Cycle 5 edit.
```

## Cycle 5 Ledger

```text
CYCLE=5
OPEN_BLOCKER=none
OPEN_MAJOR=none
OPEN_MINOR=none
CLOSED=AR-1-1,AR-1-2,AR-1-3,AR-1-4,AR-1-5,AR-2-1,AR-2-2,AR-3-1,AR-4-1
VERDICT=APPROVE
NEXT=Proceed to Phase 4 gates starting with TR0 owner-decision measurement; implementation remains gated by that separate owner go/no-go.
```