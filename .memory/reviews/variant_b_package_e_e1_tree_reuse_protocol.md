# E1-TREE-REUSE — Protocol/persistence gate

```text
TASK=E1-TREE-REUSE-PROTOCOL-PERSISTENCE-GATE-V1
BASELINE=36839c7faa55b4568c1724cf6232468957f6aac8
REVIEW_MODE=ortec-abapgit-protocol-persistence
STATUS=RUN
VERDICT=APPROVE
```

## Scope note

E1-TREE-REUSE is a persistence/schema/materialization change only. It adds
zero HTTP, zero have/want negotiation, zero thin-pack/ofs-delta/capability
handling, and zero promised/omitted-blob semantics. The partial-clone /
promisor checklist items in this mode (fetch modes, capability handling,
memory-bounded HTTP recovery) are N/A to this task and are not scored. This
gate is scored purely on: schema additivity, object-identity/repo scoping,
materialization-fact separation, completeness/staleness gating, SQL/HTTP
shape, batching, and transaction/publication boundary — all against the
CYCLE=5 design (adversarially APPROVED, cycle 5, all AR-1-1..AR-4-1
closed) and independently re-verified against current source, not merely
the adversarial ledger's word.

## Evidence matrix (independently re-read this pass)

```text
E-DESIGN   = .memory/logs/variant_b_package_e_e1_tree_reuse_design.md CYCLE=5, §§1-14
             read completely (Cycle 5 responses, §5 DDIC, §6, §7, §8/C1-C14, §9, §11/TR1-TR5,
             §12, §13, §14).
E-ADV      = .memory/reviews/variant_b_package_e_e1_tree_reuse_adversarial.md, all 5 cycles
             read; final CYCLE=5 VERDICT=APPROVE, CLOSED=AR-1-1..AR-1-5,AR-2-1,AR-2-2,AR-3-1,
             AR-4-1, OPEN_BLOCKER=none, OPEN_MAJOR=none.
E-DISC     = .memory/logs/variant_b_package_e_e1_tree_reuse_discovery.md read for call-path/
             lock/test-convention baseline (consistent with E-DESIGN's carried facts).
E-SRC-1    = src/ortec/git/zcl_abapgit_ortec_pack_raw.clas.abap lines 10-16, 160-182, 417-467
             read directly: ty_repo_key TYPE c LENGTH 12; ty_session_id TYPE c LENGTH 32;
             acquire_repo_lock sets rv_lock_id = |LOCK_{ iv_repo_key }|, INSERTs zaog_fetch_sess
             (session_id=rv_lock_id, repo_key=iv_repo_key, status='L'), bounded-backoff retry,
             raises zcx_abapgit_exception on timeout, NO COMMIT WORK; release_repo_lock guards
             `iv_lock_id IS INITIAL` then `DELETE FROM zaog_fetch_sess WHERE session_id =
             iv_lock_id AND status = 'L'`, NO COMMIT WORK.
E-SRC-2    = src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap lines 240-500 read directly:
             is_index_ready (STRICT) SELECTs only zaog_obj_index by repo_key+commit_sha1+
             c_marker_obj_type/c_marker_obj_name+idx_status — no reference to any tree-memo
             table (which does not exist pre-E1). rebuild_index: lv_lock_id captured from
             acquire_repo_lock, released via release_repo_lock(lv_lock_id) on the ready-return,
             success, zcx CATCH, and cx_root CATCH paths; DELETE-first on zaog_obj_index only;
             FILE branch has the `strlen(path)>255 OR strlen(name)>255 -> CONTINUE` guard
             AFTER file_to_object and BEFORE row append; DIR branch (WHEN c_chmod-dir) has
             NO length guard — confirms the exact pre-existing asymmetry AR-1-3 fixes;
             lt_seen_trees is a HASHED TABLE keyed by tree_sha1 alone; c_index_write_chunk_size
             = 30000 (unchanged); marker row written unconditionally LAST, then lock released;
             method contains ZERO COMMIT WORK / ROLLBACK WORK statements anywhere.
E-SRC-3    = src/ortec/git/zcl_abapgit_ortec_cache_admin.clas.abap lines 19-102, 300-360, 416-
             450 read directly: ty_repo_key alias = zcl_abapgit_ortec_repo_state=>ty_repo_key
             (TYPE c LENGTH 12, call-compatible with pack_raw's own ty_repo_key). clear_repo's
             acquire_lock/release_lock use CALL FUNCTION 'ENQUEUE_EZAOG_REPO_LOCK'/
             'DEQUEUE_EZAOG_REPO_LOCK' with session_id = iv_repo_key (bare repo key, a SAP
             lock-table entry — NOT the LOCK_<repo_key> zaog_fetch_sess row-mutex). clear_repo's
             TRY block issues `DELETE FROM zaog_fetch_sess WHERE repo_key = iv_repo_key` — a
             broad delete whose WHERE clause matches ANY row for that repo_key, INCLUDING a
             live `session_id = LOCK_<repo_key>` mutex row (repo_key column = iv_repo_key on
             that row too) — independently confirms the exact hazard AR-2-1 documents and
             confirms clear_tree_memo is correct to avoid this method entirely. clear_repo
             COMMITs on success, ROLLBACK WORK + release_lock (no re-commit) on error — DEQUEUE
             is a lock-table call, not a DB row delete, so it needs no commit to be durable;
             this is a structurally different lock primitive from pack_raw's row-mutex (see
             Transaction/publication boundary below).
E-SRC-4    = src/ortec/git/zaog_obj_index.tabl.xml, src/ortec/git/zaog_obj_store.tabl.xml read
             directly (DD02V/DD03P_TABLE): zaog_obj_index PK = CLIENT,REPO_KEY(CHAR12),
             COMMIT_SHA1(CHAR40),OBJ_TYPE(CHAR4),OBJ_NAME(CHAR40),PATH_HASH(CHAR40); no
             secondary index; delivery class A; buffering off (BUFALLOW=N). zaog_obj_store
             PK = CLIENT,REPO_KEY(CHAR12),OBJ_SHA1(CHAR40) — confirms the object store is
             PER-REPO content-addressed, never global, so cross-repo tree-memo reuse would
             be unsafe (matching blobs may not even exist in another repo's store) and is
             correctly excluded by design (C4).
E-SRC-5    = src/git/zif_abapgit_git_definitions.intf.abap: ty_chmod TYPE c LENGTH 6 (c_chmod-
             file/executable/dir/submodule/symbolic_link) — confirms the design's
             zaog_tree_child-CHMOD CHAR6 field width is exactly right, not a guess.
```

## 1. Schema/index additivity

Confirmed genuinely additive by direct read of both existing table XMLs
(E-SRC-4): `zaog_obj_index` and `zaog_obj_store` show no column, no PK, and
no secondary-index change anywhere in the design's TR1-TR5 packets, and the
two new tables (`zaog_tree_map`, `zaog_tree_child`) are net-new files. Key
field types/lengths are consistent with existing `zaog_*` convention:
`REPO_KEY CHAR12` matches `zaog_obj_index`/`zaog_obj_store` exactly;
`*_SHA1 CHAR40` matches every existing SHA1 field in both tables;
`DEVCLASS` uses the standard SAP data element (CHAR30, package name) rather
than an invented type; `CHMOD CHAR6` is independently confirmed against
`zif_abapgit_git_definitions=>ty_chmod` (E-SRC-5), not merely asserted;
`CLIENT`-first key ordering matches both existing tables. Table names
`ZAOG_TREE_MAP` (13 chars) and `ZAOG_TREE_CHILD` (15 chars) are within the
16-char SAP object-name limit. `CHILD_SEQ INT4` as the trailing key field is
the correct persistence-schema move to make range-window paging
(`child_seq BETWEEN ... ORDER BY PRIMARY KEY`) a native PK-ordered scan
rather than a synthetic sort. No migration/backfill is required (absence of
a memo row is a well-defined MISS), and rollback is a straight drop of the
two new tables with zero productive-row dependency — the correctness gate's
migration/rollback section is architecturally sound from a persistence
standpoint. **Confirmed genuinely additive.**

## 2. Object identity / repository scoping

`zaog_obj_store`'s own PK (CLIENT, REPO_KEY, OBJ_SHA1 — E-SRC-4) proves the
existing raw-object store is repo-scoped, not globally content-addressed;
the new memo tables correctly inherit `repo_key` as the leading key
component and never drop it, so C4 (cross-repo reuse) is structurally
ineligible by key mismatch, independent of any application-level check.
This is the right property for a persistence layer to enforce via the key
shape itself rather than solely via application logic — even a future bug
in the eligibility-lookup WHERE clause could not silently produce a
cross-repo hit, because a cross-repo row simply would not exist under the
querying repo's key. **Confirmed correctly scoped.**

One nuance worth recording precisely for this mode's persistence-schema
lens: `zaog_obj_store`'s raw-object reuse is content-addressed by SHA1
ALONE (a pure function of Merkle-guaranteed bytes, safe across every commit
and branch within one repo). The new memo is a SECOND, higher tier of
content-addressing over DERIVED/INTERPRETED facts (decoded tree children
plus their `MAP_FILENAME_TO_OBJECT` mapping outputs), which are NOT a pure
function of `tree_sha1` alone — interpretation additionally depends on
`dot_sha1`/`devclass`/`algo_ver`/`tree_path_hash` (§3, IN-1..IN-7). The
design correctly folds all of that into the key rather than trying to reuse
the raw-object store's SHA1-only addressing scheme for a fact category that
does not have the same purity guarantee. This is the correct persistence
distinction between "raw bytes are Merkle-pure" and "interpreted output is
context-dependent," and the key design gets it right.

## 3. Materialization separation

Directly confirmed by reading `is_index_ready` and `rebuild_index` in full
(E-SRC-2): `is_index_ready` (both STRICT and RELAXED branches) SELECTs
exclusively from `zaog_obj_index`, gated on the `c_marker_obj_type`/
`c_marker_obj_name` completion-marker row under `idx_status = c_status_
ready`. Neither this method nor `select_rows_for_filter` (unmodified by any
TR packet) reads or references `zaog_tree_map`/`zaog_tree_child` — and
since those two tables do not exist in the current source at all, there is
no way today's readiness/filter logic could be silently touching them. The
memo is correctly a DISTINCT fact category:

```text
repository-wide, cross-commit, content-addressed  : zaog_obj_store (existing,
                                                     unchanged), zaog_tree_map/
                                                     zaog_tree_child (NEW).
commit-specific                                    : zaog_obj_index (existing,
                                                     unchanged PK/columns);
                                                     published via the
                                                     $IDX/__READY__-equivalent
                                                     marker row, written
                                                     unconditionally LAST.
branch/ref-specific                                : none touched by E1 — no
                                                     branch/ref table exists
                                                     in SOURCE_SCOPE and none
                                                     is added.
attempt/mutex-specific (ephemeral control fact)     : LOCK_<repo_key> row in
                                                     zaog_fetch_sess (existing,
                                                     unchanged shape/DELETE
                                                     predicate; TR5 is a new
                                                     CALLER of the existing
                                                     acquire/release pair, not
                                                     a new lock primitive).
```

The memo is correctly cross-commit (keyed by `tree_sha1`+`tree_path_hash`+
context, no `commit_sha1` anywhere in its key) and is never read by the
commit-specific readiness/filter path. §7's publication order (memo child
rows before header, header before nothing-else-depends-on-it; index rows
chunked; marker unconditionally last; whole thing one LUW, no COMMIT WORK)
means a mid-walk crash leaves neither a false-READY commit index nor a
partial ('R'-marked but incomplete) memo header durable — both facts commit
atomically together or not at all, confirmed by the literal absence of any
COMMIT/ROLLBACK statement in `rebuild_index` (E-SRC-2). **Materialization
separation is clean and independently verified, not merely asserted.**

## 4. Completeness / staleness / integrity gating

Design and adversarial ledger content confirmed consistent (no
completeness claim overstated beyond what the mechanism proves):

- **Never inferred from memo presence alone.** Eligibility requires
  `map_status = c_status_ready` AND `built_at >= cutoff` (TTL) in the same
  header-select predicate (§9) — a header without a completed write, or one
  older than `c_tree_memo_max_age_secs` (604800s), is excluded from the
  result set entirely and the tree is treated identically to a cold MISS.
  This is a set-based predicate, not a post-hoc application check, so a
  weak implementer cannot accidentally skip it.
- **Never trusted on a partial child-count.** §8/C10 + §9's integrity
  re-check (loaded child rows summed across CHILD_SEQ pages/FAE batches
  must equal header `CHILD_COUNT`) demotes a short-count tree to MISS
  before any row is reproduced. The design is honest that this proves
  cardinality/deletion loss only (AR-1-5), not full rowset-content
  identity — same-count corruption is explicitly out-of-scope (C12),
  recoverable only via TTL expiry or the admin clear. That is an accurate,
  not overstated, completeness claim for a persistence layer with no
  content checksum over the child rowset.
- **`clear_tree_memo` cannot interfere with `rebuild_index`'s in-flight
  lock — re-verified against current source, not the ledger.** Both
  `acquire_repo_lock`/`release_repo_lock` bodies were read directly this
  pass (E-SRC-1): `acquire_repo_lock` returns `rv_lock_id = |LOCK_{
  iv_repo_key }|` and inserts exactly one `zaog_fetch_sess` row keyed by
  that `session_id` with `status='L'`; `release_repo_lock` deletes `WHERE
  session_id = iv_lock_id AND status = 'L'` and is a safe no-op when
  `iv_lock_id IS INITIAL` or the row is already gone. TR5's packet (§11)
  declares `lv_lock_id` from the SAME `ty_session_id`-typed return value
  and passes that captured id — never the bare `iv_repo_key` — to
  `release_repo_lock` on both the success path and the error path. This
  is exactly call-compatible with the real method bodies, not merely
  textually plausible: passing the bare `iv_repo_key` (the cycle-3 defect
  the ledger records as AR-3-1) would have deleted a nonexistent
  `session_id = <repo_key>` row and left the real `LOCK_<repo_key>` mutex
  row durable forever after a successful clear — confirmed as a real bug
  against the real DELETE predicate, not a hypothetical one. The final
  (cycle-5) design closes it correctly. The error-path
  `ROLLBACK WORK -> release_repo_lock(lv_lock_id) -> COMMIT WORK AND WAIT`
  sequence is sound and, on inspection of `release_repo_lock`'s own guard
  clause, safe to call unconditionally: a repeated/no-op release after an
  already-successful rollback matches zero rows and raises no error. **The
  TR5 lock fix independently holds up against current source, not just the
  adversarial ledger's word.**

## SQL call shape

- Header eligibility: one `SELECT ... FOR ALL ENTRIES IN lt_level_keys ...
  INTO TABLE @lt_hdr` per BFS-level driver chunk (`c_tree_lookup_chunk_size
  = 500`, non-empty-driver-guarded), predicated on the full composite key
  plus `map_status = c_status_ready AND built_at >= lv_cutoff`. This is a
  bulk, chunked, driver-guarded FAE — not a per-object SELECT.
- SMALL-hit children: one FAE per peek-then-decide-closed batch (strictly
  ≤ `c_tree_child_chunk_size` = 30000 rows per FAE, never the ~2x variant
  the adversarial cycle 2/3 caught and cycle 3+ closed — independently
  re-confirmed by reading the operative §9/§11-TR2 text, which is
  peek-then-decide throughout with no surviving unbounded/2x wording).
- LARGE-hit children: one `SELECT ... WHERE ... child_seq BETWEEN lv_from
  AND lv_from + chunk - 1 ORDER BY PRIMARY KEY` window per page, advancing
  until a short page — a native PK-range scan (CHILD_SEQ is the trailing
  key field), not an application-side LIMIT/OFFSET emulation.
- Memo write: two chunked `MODIFY ... FROM TABLE` calls (children first,
  headers last per §6), reusing the existing bulk-DML idiom already used
  for `zaog_obj_index`.
- Zero singleton/per-row SELECT or MODIFY anywhere in the new SQL surface;
  zero new SQL against `zaog_obj_index`/`zaog_obj_store` beyond what
  `rebuild_index` already issues today.

## HTTP request shape

Zero new HTTP. `get_objects(iv_bulk_fetch = abap_true)` is called only for
MISS tree_sha1s per level (unchanged API, unchanged bulk-negotiation
behavior); HIT tree_sha1s never reach `get_objects` at all — this is
precisely where the persistence design converts what would have been a
fetch-then-decode operation into a pure local SQL read, which is the
mechanism by which K-scaling is achieved (see below). No blob-payload HTTP
path (`build_files_from_rows`) is touched by any TR packet.

## Row and byte batching

Index writes: unchanged `c_index_write_chunk_size = 30000` (confirmed
untouched value by direct read, E-SRC-2). Memo writes: new, separate
`c_tree_child_chunk_size = 30000` and `c_tree_lookup_chunk_size = 500`
constants — correctly NOT reusing/aliasing the index constant, so a future
independent retune of one does not silently affect the other. Peak
resident child-row set for any single tree, however large (a 1,000,000-file
flat directory), is bounded to ≤ one `c_tree_child_chunk_size` window at a
time via the LARGE-hit CHILD_SEQ paging contract — never a whole-flat-tree
SELECT. Memo rows are fixed-width CHAR/INT4/TIMESTAMP metadata; no XSTRING/
blob payload is ever part of a memo row.

## External-base bulk strategy

N/A — there is no external/promisor base repository in this design; all
reuse is intra-repo, intra-store (`zaog_obj_store`, already repo-scoped).
No delta-base resolution, thin-pack thickening, or promisor-fetch batching
is introduced or affected.

## Presence versus payload access

The entire reuse mechanism operates on PRESENCE/METADATA only:
`read_tree_headers`/`read_small_children`/`read_large_child_page` return
`CHMOD`, `CHILD_NAME`, `CHILD_SHA1`, `MAP_OBJ_TYPE`, `MAP_OBJ_NAME`,
`MAP_SKIP` — never blob bytes. Even on a MISS, `get_objects` is called only
to fetch and `decode_tree` the TREE object's own bytes (a small
name/mode/sha1 listing), not any child BLOB's payload; blob payload access
remains exclusively in the untouched `build_files_from_rows` path, exactly
as today. Eligibility and HIT reproduction never read `zaog_obj_store.
OBJ_DATA` for any object. **Presence-only, confirmed at the SQL-shape
level.**

## Repository-wide versus branch/commit-specific facts

See §3 above (Materialization separation) for the full decomposition. In
short: the new tables are repository-wide + content-addressed + explicitly
cross-commit facts, kept structurally distinct (different tables, never
joined or unioned into the readiness/filter query path) from
`zaog_obj_index`'s commit-specific facts and from the pre-existing
attempt/mutex-specific `LOCK_<repo_key>` row. No branch/ref fact exists in
this design at all — correctly out of scope, since E1 never reads or writes
branch/ref state.

## Transaction / publication boundary

**TR1-TR4 (library path, unchanged contract):** confirmed by direct read
that `rebuild_index` contains zero `COMMIT WORK`/`ROLLBACK WORK` statements
(E-SRC-2). All new memo reads/writes execute inside the SAME per-repo
`acquire_repo_lock`/`release_repo_lock` critical section as today, inside
the CALLER's existing single LUW. The completion marker, the final index
flush, and every level's memo writes therefore commit atomically together
with the caller — no new false-READY window and no new partially-durable
memo window is introduced.

**TR5 (`clear_tree_memo`, deliberate carve-out — confirmed intentional and
safe, not an inconsistency):** this is a new, standalone, top-level admin
action that owns its own LUW, mirroring the EXISTING `clear_repo` pattern
in the same class (commit on success; rollback + lock-release on error —
E-SRC-3) rather than inventing a new transaction shape. The reason this
carve-out is not merely stylistically consistent but *structurally
required* is a persistence-specific distinction this gate is positioned to
make: `clear_repo`'s lock is a SAP enqueue-table entry
(`ENQUEUE_EZAOG_REPO_LOCK`/`DEQUEUE_EZAOG_REPO_LOCK`), which is released
instantly by the DEQUEUE call independent of the current LUW's commit
state — no COMMIT WORK is needed to make a DEQUEUE durable. `clear_tree_
memo`, by contrast, reuses `acquire_repo_lock`/`release_repo_lock`, whose
"lock" is an ordinary persistent-table row (`zaog_fetch_sess`); a `DELETE`
against that row is exactly as durable as any other DML and REQUIRES a
`COMMIT WORK` to be visible to a concurrent `acquire_repo_lock` INSERT
attempt on another session. This is why TR5 legitimately needs two commit
points (success, and the AR-4-1 error-path release) where `rebuild_index`
correctly needs none — `rebuild_index` never commits its own lock's
release either, and lock visibility for it works the same way (its
DELETE-on-release only becomes visible at the CALLER's eventual commit,
which is acceptable there because `rebuild_index` is a library call inside
a larger caller-owned LUW, not a self-contained admin action). The design
text's own framing ("clear_tree_memo owns its own admin LUW, unlike
rebuild_index") is correct, and this gate's independent structural
reasoning (enqueue-DEQUEUE vs. row-DELETE durability) confirms it is not
an arbitrary choice. **The TR1-TR4 vs. TR5 transaction-boundary split is
intentional, safe, and now doubly justified — by the design's own
reasoning and by this gate's independent persistence-layer analysis of
why the two lock primitives have different durability semantics.**

## Expected incremental scaling with K vs. repository size N

Let **N** = total distinct trees encountered by one commit's BFS walk
(same order as today's full-walk-every-commit baseline), and **K** = the
number of those trees that are cold/MISS relative to any previously
completed, still-within-TTL memo (i.e., genuinely new/changed subtrees).

```text
Header-eligibility SQL row-touch : O(N), same order as today's traversal —
                                    every level's full key set is looked up
                                    regardless of hit/miss, chunked at
                                    c_tree_lookup_chunk_size=500 FAE drivers.
                                    This is intentionally cheap metadata
                                    work, not the expensive part being
                                    optimized.
get_objects fetch + decode_tree +
file_to_object compute            : O(K) — HIT trees bypass get_objects and
                                    decode/map entirely; this is the actual
                                    persistence-driven savings mechanism.
Memo write (MODIFY zaog_tree_child
/zaog_tree_map)                   : O(K) — only newly-decoded (MISS) trees
                                    are memoized; HIT trees write nothing.
zaog_obj_index row output         : O(N) rows, unchanged — every commit
                                    still gets a full, byte-identical,
                                    freshly-written row set (copy-not-alias
                                    by design; §4 rejects B2's aliasing for
                                    exactly this reason), so index WRITE
                                    cost is NOT reduced by K — only the
                                    upstream compute/fetch cost is.
Peak memory                       : bounded independent of both K and N —
                                    one c_index_write_chunk_size index
                                    chunk (~21.9MB) + at most one
                                    c_tree_child_chunk_size child window,
                                    even for a single N=1,000,000-child
                                    flat directory (AR-1-2/AR-2-2 strict
                                    ≤1-chunk bound, independently confirmed
                                    against the operative §9 text).
```

This is the correct scaling shape for a memo/reuse persistence layer: the
cheap presence-check scales with the full traversal size (as it must, to
even discover which subtrees are unchanged), while the expensive
fetch+decode+interpret+persist work — the actual cost this design exists
to avoid — scales with K, the genuinely-changed fraction. No per-object SQL
or per-object HTTP is required at any point to achieve this, satisfying
this mode's hard constraint against approving a design that would need
per-object persistence/retrieval to realize its claimed benefit.

## Findings

None (BLOCKER/MAJOR/MINOR). This review independently re-verified, against
current source rather than the adversarial ledger's summaries, the two
claims most load-bearing for a persistence/schema/locking gate — (a) the
final TR5 lock capture/release sequence is call-compatible with the real
`acquire_repo_lock`/`release_repo_lock` implementations, and (b) `is_index_
ready`/`rebuild_index` genuinely never reference the new memo tables — and
both hold. The additive-DDIC, repo-scoping, and batching claims were
likewise independently re-derived from the table XMLs and class source
rather than accepted from the design doc's own assertions.

## Verdict

```text
VERDICT=APPROVE
STATUS=RUN
GATED_ON=owner go/no-go for TR0 (Stage-0 chunk=30000 SAT measurement,
  AR-1-4/BLOCK_OWNER_DECISION) remains the sole precondition before TR1-TR5
  implementation may start; that gate is orthogonal to and unaffected by
  this persistence/schema review.
```
