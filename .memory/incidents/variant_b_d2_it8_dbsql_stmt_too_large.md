# Incident: Variant B / Package D2 — IT8 DBSQL_STMNT_TOO_LARGE during warm-to-cold branch switch (SAT prep)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=D2-INCIDENT-DBSQL_STMNT_TOO_LARGE
BASELINE=17513ba741322d11078b8c6ff5fe41b1c6978d4c (HEAD, verified)
STATUS=ROOT_CAUSE_CONFIRMED
```

## 1. Verified IT8 connection and tested Git baseline

```text
git rev-parse HEAD                 = 17513ba741322d11078b8c6ff5fe41b1c6978d4c
git status --short                 = only an untracked/no-op line-ending
                                      difference in
                                      zcl_abapgit_ortec_missing_obj.clas.testclasses.abap
                                      (git diff/--stat for that file is EMPTY -
                                      not a real change, left untouched, unrelated
                                      to this incident)
git merge-base --is-ancestor 2111b2887cc4fbf2ee481f753fd4af2c3e5085c4 HEAD  = YES
git merge-base --is-ancestor 17513ba741322d11078b8c6ff5fe41b1c6978d4c HEAD  = YES (is HEAD itself)
git log --oneline -10:
  17513ba7 ORTEC: fix D2 TIME_OUT - bound ensure_available's remote top-up ...
  2111b288 ORTEC: fix SYSTEM_NO_ROLL - remove unbounded populate_cache preload ...
  de0f11ce Unit Test Fixes
  cdc5caed ORTEC Variant B Package D2: staged-visibility, attempt-ID correlation, ...
  6a42ada5 ORTEC: Close Package D1 validation
  73cb519a D1 SAP closeout ...
  8bfca36b Fix Unit Tests for Delta and Pack Decode Handling
  7403a639 Package D1: generalized bounded external delta-base resolution
  5a1171f2 Finalize Package D design
  5e540354 Reuse verified tip blob set during cold init
```

Both prior incident fixes (SYSTEM_NO_ROLL `2111b288`, TIME_OUT `17513ba7`) are
confirmed present on HEAD.

IT8 connection re-verified live (not assumed from a historical name): dump ID
format `...-SAP-HANIT8_IT8_00...MICHAELK...100...` — host `HANIT8`, SID
`IT8`, client `100`, user `MICHAELK`, matching every other dump in this
project's incident history. The dump list additionally shows the confirmed
`ABAP_UNIT=PASS`/`ATC=PASS`/`SYSTEM_NO_ROLL_REPRODUCED=NO`/
`TIME_OUT_REPRODUCED=NO` state implicitly: the most recent
`SYSTEM_NO_ROLL`/`TIME_OUT` dumps for `ZCL_ABAPGIT_ORTEC_OBJ_STORE` are both
from 2026-07-28 11:37/11:50, strictly OLDER than this new dump (15:03) and
older than two unrelated `STRING_LENGTH_NEGATIVE` ADT-tooling dumps at 14:12-
14:13 (owner's own ADT data-preview usage, irrelevant to this incident, not
investigated further).

## 2. Exact dump ID/timestamp

```text
DUMP_ID   = 20260728170321T-SAP-HANIT8_IT8_00...MICHAELK...100...14
TIMESTAMP = 2026-07-28T15:03:21Z
USER      = MICHAELK
ERROR     = DBSQL_STMNT_TOO_LARGE
EXCEPTION = CX_SY_OPEN_SQL_DB
PROGRAM   = ZCL_ABAPGIT_ORTEC_OBJ_STORE===CP
```

No other `DBSQL_STMNT_TOO_LARGE` dump exists on 2026-07-28. One older
`DBSQL_STMNT_TOO_LARGE` dump exists from 2026-07-23 in
`ZCL_ABAPGIT_ORTEC_WALK_PREP===CP` — a different program, four days earlier,
predating this session's baseline commits; not the same incident and not
investigated further (out of scope per the run brief's exact-dump-match
requirement; flagged in §15 as a related-defect-class note only).

## 3. Complete failing call path (from the live dump's own call stack, kap11)

```text
1  EVENT   ZABAPGIT                             START-OF-SELECTION
2  FORM    ZABAPGIT_FORMS                       RUN
3  FORM    ZABAPGIT_FORMS                       OPEN_GUI
...GUI dispatch (routine, omitted)...
18 METHOD  ZCL_ABAPGIT_GUI_PAGE_STAGE=>INIT_FILES
19 METHOD  ZCL_ABAPGIT_STAGE_LOGIC=>ZIF_ABAPGIT_STAGE_LOGIC~GET
20 METHOD  ZCL_ABAPGIT_REPO_ONLINE=>ZIF_ABAPGIT_REPO~GET_FILES_REMOTE
21 METHOD  ZCL_ABAPGIT_REPO_ONLINE=>FETCH_REMOTE
22 METHOD  ZCL_ABAPGIT_GIT_PORCELAIN=>PULL_BY_BRANCH
23 METHOD  ZCL_ABAPGIT_ORTEC_PORCELAIN=>PULL_BY_BRANCH
24 METHOD  ZCL_ABAPGIT_GIT_TRANSPORT=>UPLOAD_PACK_BY_BRANCH
25 METHOD  ZCL_ABAPGIT_ORTEC_FASTPATH=>UPLOAD_PACK_BY_BRANCH
26 METHOD  ZCL_ABAPGIT_ORTEC_FASTPATH=>UPLOAD_PACK
27 METHOD  ZCL_ABAPGIT_ORTEC_FASTPATH=>SERVE_CACHED_WHEN_NOTHING_NEW
28 METHOD  ZCL_ABAPGIT_ORTEC_OBJ_STORE=>GET_REACHABLE_OBJECTS
29 METHOD  ZCL_ABAPGIT_ORTEC_OBJ_STORE=>GET_OBJECTS
30 METHOD  ZCL_ABAPGIT_ORTEC_OBJ_STORE=>READ_OBJECT_ROWS   [CRASH, line 15]
```

**This is the STANDARD (non-filtered) Stage view's remote-fetch path**, NOT
the Stage-By-Filter/`obj_index`/`missing_obj` path that produced the two
prior incidents. It reaches ORTEC's fastpath via
`zcl_abapgit_git_porcelain=>pull_by_branch` →
`zcl_abapgit_ortec_porcelain=>pull_by_branch` →
`zcl_abapgit_git_transport=>upload_pack_by_branch` →
`zcl_abapgit_ortec_fastpath=>upload_pack_by_branch`/`upload_pack`. The git
server reported nothing new for the requested want-commit (the cold branch's
tip, `bugfix/O4H-8794-complete-solution-tour-number-range`), so `upload_pack`
took its **"nothing new, serve from cache" branch**
(`serve_cached_when_nothing_new`), which calls
`zcl_abapgit_ortec_obj_store=>get_reachable_objects` for that ONE want-commit
to reconstruct the complete object set standard abapGit's `full_tree`/pull
logic needs to build every file for the branch. This is the SAME crash
location (`get_reachable_objects`) as the earlier SYSTEM_NO_ROLL incident,
but a **different, previously-latent defect one level deeper**
(`get_objects`/`read_object_rows`), not a recurrence of the fixed
`populate_cache` preload (confirmed absent from `get_reachable_objects`'s
current source — see §9).

`materialize_missing_batches`/`ensure_available` (the TIME_OUT fix, commit
`17513ba7`) do **not** appear anywhere on this stack — S10 is directly
CONTRADICTED by this call stack.

## 4. Exact SQL form and effective parameter/key cardinality

Source extract at the termination point (`read_object_rows`, confirmed
byte-for-byte against the current workspace source at
`src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap:1233`):

```abap
METHOD read_object_rows.
  DATA lr_sha1s TYPE RANGE OF zaog_obj_store-obj_sha1.
  FIELD-SYMBOLS <ls_sha1> LIKE LINE OF it_sha1s.

  IF it_sha1s IS INITIAL.
    RETURN.
  ENDIF.

  LOOP AT it_sha1s ASSIGNING <ls_sha1>.
    APPEND VALUE #( sign   = 'I'
                    option = 'EQ'
                    low    = <ls_sha1>-sha1 ) TO lr_sha1s.
  ENDLOOP.

  SELECT * FROM zaog_obj_store          "<<<< CRASH (kap7 line 15)
    INTO TABLE rt_rows
    WHERE repo_key = iv_repo_key
      AND obj_sha1 IN lr_sha1s
      AND status   = 'R'.
ENDMETHOD.
```

```text
Open SQL form:              IN range (SELECT OPTIONS-style RANGE table),
                             single-value EQ entries only, no FOR ALL ENTRIES
Input key count (IT_SHA1S): 40,891 rows (kap10: "Table IT_1902[40891x80]")
Range table built:          LR_SHA1S, 1:1 with input (kap10:
                             "Table IT_1903[40891x166]", fill = 40891)
Row/byte batching before
  this SQL statement:       NONE - read_object_rows itself has zero
                             chunking, and its caller (get_objects, see §5)
                             performed zero chunking for this call
Empty-set guard:            present (`IF it_sha1s IS INITIAL. RETURN.`) but
                             irrelevant here - the set was not empty, it was
                             40,891 rows, the opposite failure mode
Predicates:                 repo_key = <exact>, obj_sha1 IN <range>,
                             status = 'R' - unchanged, correct, narrow
Selected columns:           SELECT * (full row incl. OBJ_DATA payload) -
                             same shape every other read_object_rows caller
                             already uses; not a new payload-inclusion defect
Expected DB round trips:    1 (the whole point of the defect - one giant
                             statement instead of ~41 chunked ones)
DB interface expansion:     YES - each RANGE entry becomes one bind
                             marker/comparison value; HANA/DBSL enforces a
                             hard cap on the number of markers per prepared
                             statement
```

Exact DBSL/HANA limit from the dump's own `kap3` "Error analysis" text
(preserved verbatim, not summarized):

```text
More information: ZAOG_OBJ_STORE
Number of DBSL marker exceeded
Current = 40893
"Maximum = 32767"
```

`40893` (not exactly `40891`) accounts for the two additional bind values
for `repo_key` and `status` in the same WHERE clause — confirming the range
table itself contributed essentially all 40,891 markers, consistent with
`IT_SHA1S`'s exact row count.

## 5. Where the key set originates and current batching layers

Traced upward from `read_object_rows` through its direct caller,
`get_objects` (`src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap:503`),
whose current source was read in full:

```abap
IF iv_bulk_fetch = abap_true.
  LOOP AT lt_missing_sha1s ASSIGNING <lv_sha1>.
    ls_sha1-sha1 = <lv_sha1>.
    APPEND ls_sha1 TO lt_package.
  ENDLOOP.

  IF lt_package IS NOT INITIAL.
    lt_db_rows = read_object_rows( iv_repo_key = iv_repo_key
                                   it_sha1s    = lt_package ).      " <<<< ONE unchunked call
    APPEND LINES OF lt_db_rows TO lt_rows.
  ENDIF.
ELSE.
  LOOP AT lt_missing_sha1s ASSIGNING <lv_sha1>.
    ls_sha1-sha1 = <lv_sha1>.
    APPEND ls_sha1 TO lt_package.
    IF lines( lt_package ) >= c_select_package_size.               " <<<< chunked here
      lt_db_rows = read_object_rows( iv_repo_key = iv_repo_key
                                     it_sha1s    = lt_package ).
      APPEND LINES OF lt_db_rows TO lt_rows.
      CLEAR lt_package.
    ENDIF.
  ENDLOOP.
  IF lt_package IS NOT INITIAL.
    lt_db_rows = read_object_rows( iv_repo_key = iv_repo_key
                                   it_sha1s    = lt_package ).
    APPEND LINES OF lt_db_rows TO lt_rows.
  ENDIF.
ENDIF.
```

`c_select_package_size = 1000` (class constant, line 310). **The
`iv_bulk_fetch = abap_true` branch has never chunked at any package size —
it builds the entire `lt_missing_sha1s` set (whatever its size) into one
`lt_package` and issues exactly one `read_object_rows` call.** The
`iv_bulk_fetch = abap_false` branch (and every OTHER caller of
`read_object_rows` in this class — `get_available_objects` line ~458,
`has_dangling_delta_base` line ~640, `get_present_sha1s`'s own inline range
build line ~1279, `get_staged_delta_objects`'s own inline range build line
~1357/1394) all correctly chunk at `c_select_package_size` before ever
building a range/executing a SELECT.

This unchunked behavior is not accidental — it is a **known, explicitly
documented design decision (INV-B-13)** from the earlier, already-
SAP-validated Package B design, preserved verbatim in this same class's own
ABAP Doc for two sibling methods:

```text
"! Uses iv_bulk_fetch = abap_false for every frontier read (design
"! decision INV-B-13, .memory/logs/variant_b_package_b_design.md §6):
"! get_objects' iv_bulk_fetch = abap_true branch does not chunk at
"! c_select_package_size, so abap_false is used to guarantee every bulk
"! read stays chunked regardless of how wide a real tree frontier is.
```

(`verify_tree_closure`, line ~161; `get_tip_blob_sha1s`, line ~192 — both
correctly use `iv_bulk_fetch = abap_false` for exactly this reason, and are
therefore **not** affected by this incident.)

`get_reachable_objects` (`line 692`, confirmed pre-dating Package D per the
SYSTEM_NO_ROLL incident's own §12 ownership classification) was **never
updated to follow INV-B-13** — its commit/tree/blob level calls all use
`iv_bulk_fetch = abap_true`:

```abap
" commit level (line ~733), tree level (line ~759 inside WHILE), blob level (line ~808):
lt_blob_objects = get_objects( iv_repo_key   = iv_repo_key
                               it_sha1s      = lt_blob_sha1s
                               iv_bulk_fetch = abap_true ).
```

`lt_blob_sha1s` is the **complete, deduplicated set of every blob SHA1
reachable from the commit's tree** (built by the tree-walk directly above
this call, `zcl_abapgit_git_pack=>decode_tree`-driven, no pre-existing size
cap). For this incident's commit (the O4H-8794 branch tip), that set was
40,891 blobs, all of which were session-cache misses (`mt_cache` had no
warm entries yet for repo `288c81fc1cad` in this session — the session-wide
full-repo preload that used to warm this cache unconditionally,
`populate_cache`, was correctly removed by the SYSTEM_NO_ROLL fix, `2111b288`,
and nothing else in this call path pre-warms the cache for a want-commit
whose objects were persisted via a normal, non-ORTEC-certified pack decode).

**Batching-layer summary (matches the run brief's own S7 pattern
description almost exactly, but for `get_reachable_objects`, not
`materialize_missing_batches`):**

```text
Layer                          Bound?
Tree-walk (commit/tree/blob
  frontier collection)         Unbounded by design (this method's entire
                                purpose is "give me everything reachable
                                from this one commit" for a normal,
                                non-filtered pull) - NOT itself a defect
get_objects' session-cache
  lookup (mt_cache)             O(1) per SHA1, correctly implemented
get_objects' DB-fallback path
  (iv_bulk_fetch = abap_true)   UNBOUNDED - zero chunking (THE DEFECT)
read_object_rows's own SQL      UNBOUNDED - builds one IN-range from
                                whatever it is given, no cap of its own
                                (matches every OTHER caller's already-
                                chunked usage; the defect is entirely in
                                the caller, not this shared low-level method)
```

## 6. Neutral hypothesis matrix

| ID | Candidate | Verdict | Evidence |
| --- | --- | --- | --- |
| S1 | Unbounded `IN @range` expansion | **CONFIRMED** | `read_object_rows` builds `lr_sha1s` 1:1 from whatever `it_sha1s` it receives, no cap; dump's `LR_SHA1S = Table IT_1903[40891x166]`, kap8 source |
| S2 | Oversized `FOR ALL ENTRIES` driving table | **NOT_APPLICABLE** | This statement uses an `IN` range table, not `FOR ALL ENTRIES` (kap8 source confirms) |
| S3 | Missing row-count chunking | **CONFIRMED** | `get_objects`' `iv_bulk_fetch = abap_true` branch has no `IF lines(lt_package) >= c_select_package_size` check at all, unlike its own `abap_false` branch and every other `read_object_rows` caller in the class |
| S4 | Chunking occurs after the failing SQL call | **NOT_APPLICABLE** | No chunking exists anywhere in this call path before OR after the failing call - it is simply absent, not misordered |
| S5 | Deduplication absent, causing key explosion | **CONTRADICTED** | `get_reachable_objects`'s tree-walk already deduplicates via `lt_seen_blobs` (a hashed set) before appending to `lt_blob_sha1s`; `get_objects` itself further deduplicates via `lt_unique_sha1s` before splitting into cache-hit/cache-miss. 40,891 is already a deduplicated count. |
| S6 | Accidental repository-wide key set on a K-sized operation | **CONTRADICTED** | Read-only query: repo `288c81fc1cad` total `status='R'` rows = 73,681 (41,008 blob + 28,858 tree + 3,815 commit). The failing set (40,891 blobs) is this ONE commit's own complete, legitimate blob closure - it is large because this repository currently has few other buffered branches sharing this repo_key, not because the code accidentally widened scope to "all repositories" or ignored a filter. `get_reachable_objects`'s entire contract is "return everything reachable from this one commit" (used for a normal, non-filtered pull) - the size itself is expected behavior, not a scope leak. |
| S7 | `materialize_missing_batches` sends bounded HTTP batches but uses an unbounded DB precheck | **NOT_APPLICABLE** | `materialize_missing_batches`/`ensure_available` do not appear anywhere on this call stack (§3); this incident is entirely inside the ordinary `upload_pack`/`serve_cached_when_nothing_new` path, unrelated to the TIME_OUT fix's code |
| S8 | Current-tip graph traversal returns an unexpectedly huge but legitimate key set | **CONFIRMED (contributing, not itself a defect)** | 40,891 blobs is the real, correct, deduplicated blob closure of one commit for a normal (non-filtered) pull - large but legitimate; the defect is exclusively in how that legitimate set is read from the DB (§5), not that it was collected |
| S9 | HANA/ABAP platform statement-size limit lower than assumed | **CONFIRMED (as the exact mechanism, not the root cause)** | kap3: "Number of DBSL marker exceeded, Current = 40893, Maximum = 32767" - a real, fixed HANA/DBSL bind-marker-per-statement ceiling; the existing `c_select_package_size = 1000` constant already keeps every OTHER call site comfortably under this limit with margin to spare |
| S10 | Recent TIME_OUT fix introduced a new unchunked helper call | **CONTRADICTED** | Call stack (§3) does not touch `materialize_missing_batches`, `ensure_available`, or any file changed by commit `17513ba7`; `git diff --stat` for that commit lists only `zcl_abapgit_ortec_cold_init`/`zcl_abapgit_ortec_missing_obj`/`zcl_abapgit_ortec_fetch_req` testclasses, none of which appear on this stack |
| S11 | Pre-existing shared object-store API defect exposed by the new path | **CONFIRMED** | `get_objects`' `iv_bulk_fetch = abap_true` branch's lack of chunking pre-dates this incident (it is the exact behavior INV-B-13's own doc comment already warns about, written during Package B). It was not previously *triggered* for `get_reachable_objects` at this scale because that method's own `populate_cache` full-repo preload (removed by `2111b288`, the SYSTEM_NO_ROLL fix) would, as an accidental side effect, warm `mt_cache` with every `status='R'` row for the repo BEFORE the per-level walk ran - turning what should have been a 40,891-row cache-miss DB fallback into a 100% cache-hit, zero-SQL path for any repo that had ever been touched before. Removing that unbounded preload (a correct, necessary fix for a different, confirmed defect) exposed this separate, previously-latent chunking gap the first time a genuinely cold (never-before-cached-this-session) commit with a wide blob frontier reached this method. |
| S12 | Unrelated custom SQL or system issue | **CONTRADICTED** | The dump's own termination point, source extract, and call stack are entirely internal to this project's `ZCL_ABAPGIT_ORTEC_OBJ_STORE`; no other custom SQL or third-party code is involved |

## 7. Root cause ranked by confidence

1. **(Highest confidence, directly measured)** `zcl_abapgit_ortec_obj_store=>get_objects`'s
   `iv_bulk_fetch = abap_true` branch performs **zero row-count chunking**
   before calling `read_object_rows`, unlike its own `abap_false` branch and
   every other caller of `read_object_rows` in the same class. This is a
   pre-existing (Package B-era) gap, already implicitly acknowledged by the
   INV-B-13 doc comment on two sibling methods that deliberately avoid this
   branch for exactly this reason.
2. **(Directly measured, confirmed contributing precondition)** `get_reachable_objects`'s
   blob-level frontier for the O4H-8794 cold-branch commit is a legitimate,
   deduplicated 40,891-entry set - large enough, on its own, to exceed
   HANA/DBSL's 32,767 bind-marker-per-statement ceiling once passed
   unchunked into one `SELECT ... obj_sha1 IN lr_sha1s` statement.
3. **(Confirmed, secondary/derivative finding)** This exact combination was
   not previously reachable because `get_reachable_objects`'s own
   `populate_cache` full-repo preload (removed by the unrelated, already-
   committed SYSTEM_NO_ROLL fix, `2111b288`) had been incidentally masking
   this gap by pre-warming the session cache for any previously-touched
   repository. The SYSTEM_NO_ROLL fix is correct and must not be reverted;
   it simply stopped hiding this separate, genuine defect.

No evidence supports a `FOR ALL ENTRIES` misuse, a deduplication gap, an
accidental repository-wide read, a defect in the recent TIME_OUT fix, or an
unrelated system/database issue.

## 8. Ownership classification

```text
OWNERSHIP=CROSS_PACKAGE
```

`get_objects`/`read_object_rows` are shared, pre-Package-D infrastructure
(`zcl_abapgit_ortec_obj_store`, used by Package B/C/D/E code alike) - neither
D1 nor D2's own implementation maps list `get_objects` as an owned symbol,
and the defect itself (missing chunking in one branch of a shared bulk-read
helper) pre-dates Package D entirely, consistent with the SYSTEM_NO_ROLL
incident's own classification of `get_reachable_objects` as cross-package.
It is reachable today via the standard (non-ORTEC-filtered) Stage/pull path,
which is not owned by any single package's slice list. Per this project's
own precedent (the SYSTEM_NO_ROLL incident's root cause #2, "pulled forward
by severity"), a confirmed, live, production-blocking crash is fixed now
rather than deferred to a future package's nominal ownership window.

## 9. Does this block D2 closeout and SAT measurement?

```text
BLOCKS_D2_CLOSEOUT=YES
BLOCKS_SAT_MEASUREMENT=YES
```

The failed branch switch never completed, so the SAT trace captured no valid
warm-to-cold performance baseline (`SAT_RESULT=INVALID_BECAUSE_OPERATION_DUMPED`,
confirmed consistent with the call stack terminating mid-`upload_pack`,
before any file materialization or SAT-relevant work occurred). Package D2
cannot be closed on a workflow that dumps before completing, and the
intended SAT-guided performance analysis remains outstanding regardless of
this fix's own success (per the run brief: "Do not close D2 after merely
removing this dump").

## 10. Exact fix scope

```text
File:   src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap
Method: get_objects
Change: unify the iv_bulk_fetch = abap_true branch to chunk lt_package at
        c_select_package_size before every read_object_rows call, exactly
        matching the abap_false branch's already-existing, already-reviewed
        pattern (and the identical pattern already used by
        get_available_objects, has_dangling_delta_base, get_present_sha1s,
        get_staged_delta_objects in this same class). No change to
        read_object_rows itself (it stays a simple, unchunked, single-SELECT
        primitive - exactly as every other already-correct caller already
        assumes). No change to any WHERE predicate, no change to the
        empty-set guard, no change to the returned row shape/order
        contract (no ORDER BY existed before or after), no change to
        get_objects' external signature/behavior for any existing caller.
Why safe: iv_bulk_fetch's only remaining observable effect after this fix
        is telling get_objects "the caller doesn't care about the (now
        purely internal) chunk-vs-no-chunk distinction" - functionally the
        two branches become behaviorally identical (same predicates, same
        result set, same cache-population side effects), just executed as
        1 vs N SELECT statements depending on set size. A 1-40-object
        request still executes in exactly 1 SELECT either way (no
        regression for the common case); only sets above 1000 (previously
        crash-prone) now correctly chunk. This is the same well-established
        pattern already governing 4 other call sites in this file and
        already covered by this project's Package B/D0/D1 correctness and
        performance reviews for the abap_false branch.
Doc fix: the two stale ABAP Doc comments on verify_tree_closure/
        get_tip_blob_sha1s that state "get_objects' iv_bulk_fetch = abap_true
        branch does not chunk at c_select_package_size" become factually
        incorrect after this fix (their CHOICE to use abap_false remains
        correct and unaffected either way; only the justification text is
        now stale) - corrected in the same commit to avoid a future reader
        trusting an invariant that no longer holds.
Not touched: get_reachable_objects, get_reachable_sha1s, read_object_rows,
        mt_cache/session-cache logic, ZAOG_COMMIT_HIST/ZAOG_REPO_STATE,
        the SYSTEM_NO_ROLL fix (2111b288), the TIME_OUT fix (17513ba7),
        materialize_missing_batches/ensure_available, any wire-protocol
        serialization, any DDIC structure.
```

## 11. Class-local regression tests (added, all names ≤ 30 characters)

New tests in `src/ortec/git/zcl_abapgit_ortec_obj_store.clas.testclasses.abap`
(existing local test include; verified this file already exists and hosts
`reachable_ignores_extra_ready` from the SYSTEM_NO_ROLL fix):

```text
bulk_fetch_dedups_input       - iv_bulk_fetch = abap_true with duplicate
                                 SHA1s in it_sha1s returns exactly one object
                                 per distinct SHA1 (proves dedup unaffected)
bulk_fetch_uses_pkg_size      - a key set larger than c_select_package_size
                                 (1,500 synthetic SHA1s pre-stored as
                                 status='R') is fully returned via
                                 get_objects(iv_bulk_fetch=abap_true) without
                                 requiring millions of rows to reproduce the
                                 statement-size class; uses a test seam
                                 (LOCAL FRIENDS access to a package-count
                                 spy/counter around read_object_rows) to
                                 assert MULTIPLE bounded SQL packages were
                                 issued, none exceeding c_select_package_size
bulk_fetch_empty_no_sql       - empty it_sha1s executes zero read_object_rows
                                 calls (mandatory empty-set guard)
bulk_fetch_preserves_where    - a pre-stored 'D'-status row with a matching
                                 SHA1 is correctly EXCLUDED from the result
                                 (proves the status='R' predicate survives
                                 chunking unchanged)
bulk_fetch_large_n_small_k    - pre-store many unrelated 'R' rows for OTHER
                                 repo_keys (simulating large N) and confirm a
                                 small it_sha1s request for the target
                                 repo_key returns exactly K, unaffected by N
bulk_fetch_no_per_key_sql     - same package-count spy as above: for a
                                 250-entry set (comfortably below
                                 c_select_package_size), assert exactly ONE
                                 read_object_rows call occurs, not 250
                                 (proves this fix does not regress into a
                                 per-key SQL anti-pattern)
```

These satisfy the run brief's required assertions: input deduplicated;
multiple bounded SQL packages used; no package exceeds the configured row
limit; original predicates remain effective; complete result/missing set is
preserved across package boundaries; empty input executes no SQL; no
per-key SQL; large repository N does not affect a small incoming K set. No
test was added to `zcl_abapgit_ortec_git_tests.clas.testclasses.abap` (the
legacy aggregate class), per the run brief's explicit instruction.

## 12. Performance acceptance criteria

```text
- get_objects(iv_bulk_fetch = abap_true) issues ceil(K / 1000) SELECT
  statements for K missing SHA1s, never 1 statement for K > 1000 and never
  K statements (no per-object SQL).
- No statement's IN-range ever exceeds 1000 entries (well under the
  32,767 DBSL marker ceiling with wide margin for the repo_key/status
  literals and any future predicate additions).
- HTTP behavior is completely unaffected (this fix is entirely on the local
  DB-read side; it does not change upload_pack/serve_cached_when_nothing_new
  request shape in any way).
- A large repository N (e.g. 1,000,000 stored objects across many repos/
  branches) does not affect the SQL-call count for a small-K request to a
  DIFFERENT repo_key (bulk_fetch_large_n_small_k).
- The already-certified/committed SYSTEM_NO_ROLL and TIME_OUT fixes remain
  byte-for-byte unmodified (verified by git diff --stat scoped to this
  incident's one file).
```

## 13. IT8 retest and subsequent SAT plan

Reuses the run brief's own required sequence verbatim:

```text
1. Import/activate the corrective commit in IT8.
2. Run affected ABAP Unit tests (zcl_abapgit_ortec_obj_store).
3. Run ATC for affected classes.
4. Warm development/6.0.x.
5. Start a new SAT trace without call aggregation.
6. Switch to bugfix/O4H-8794-complete-solution-tour-number-range while cold.
7. Confirm no DBSQL_STMNT_TOO_LARGE, SYSTEM_NO_ROLL, or TIME_OUT.
8. Stop and export SAT immediately after the branch switch completes.
9. Analyze the new SAT using the separate SAT-guided performance prompt.
```

Do not close Package D2 on this fix alone - the SAT-guided performance
analysis of the warm-to-cold switch itself remains the outstanding,
unrelated deliverable.

## 14. Evidence limitations

```text
- No live SAT/ST05 SQL trace was captured for this exact incident run (the
  branch switch never completed); the dump's own kap10 ("Selected
  Variables") already provides an exact, authoritative 40,891-row/40,893-
  marker measurement, making a supplementary trace unnecessary to confirm
  root cause.
- kap29 ("Database Interface Information") was retrieved but consists
  primarily of a long undecoded numeric buffer dump (DBSL internal marker
  IDs) with no additional decodable facts beyond what kap3's own
  "Number of DBSL marker exceeded" text already states explicitly; not
  reproduced verbatim here as it adds no further evidence.
- The 2026-07-23 DBSQL_STMNT_TOO_LARGE dump in
  ZCL_ABAPGIT_ORTEC_WALK_PREP===CP was NOT investigated as part of this
  incident (different program, four days prior, predates this session's
  tested baseline) - flagged only as a possible related defect class
  (unchunked IN-range) worth a future, separately-scoped check of
  zcl_abapgit_ortec_walk_prep's own SQL call sites, out of this incident's
  bounded scope.
- ZAOG_OBJ_STORE's status breakdown was queried repo-wide and per-repo
  (grouped counts only, no payload/IN-list queries issued during
  diagnosis, per the run brief's explicit prohibition); this confirmed
  73,681 total 'R' rows for repo 288c81fc1cad (41,008 blob) and zero
  'D'/'I' rows repository-wide at the time of this analysis - the
  132,963-row 'D' orphan set reported by the earlier TIME_OUT incident's
  own evidence is no longer present in the live system (not reconciled
  further here; out of this incident's scope, and does not affect this
  incident's root cause or fix).
- ZAOG_COMMIT_HIST shows only two certified (F/C) branches for this repo
  (releases/6.0.1, development/6.0.x) - bugfix/O4H-8794-... is NOT
  certified, meaning its 41,008 blobs most likely reached 'R' status via
  an ordinary (non-ORTEC-certified) pack decode rather than the cold-init/
  materialize certification pipeline. This is additional context, not a
  contributing cause - Package B/E's certification state machine is
  entirely unaffected by and unrelated to this incident's root cause.
```

## 15. SAP validation closeout

```text
STATUS=SAP_VALIDATED_RESOLVED
LIVE_RESULT=not reproduced after SQL-size fix
FOLLOWUP_SAT=completed successfully
PACKAGE_D_D2_VALIDATED_HEAD=733bb30799886ef8659be7e293b82c3ccfcebbdd
```

Live IT8 verification confirmed the fix is active (direct `SAPRead` of
`ZCL_ABAPGIT_ORTEC_OBJ_STORE=>GET_OBJECTS` matched commit `733bb307`
byte-for-byte) and the exact reproduction sequence (§13) completed
successfully with `DBSQL_STMNT_TOO_LARGE_REPRODUCED=NO`, `ABAP_UNIT=PASS`,
`ATC=PASS_WITHOUT_SEVERE_FINDINGS`. The resulting SAT trace was fully
analyzed - see
[.memory/incidents/variant_b_d2_sat_warm_to_cold_o4h8794.md](.memory/incidents/variant_b_d2_sat_warm_to_cold_o4h8794.md) -
and its one measured hotspot was classified `PACKAGE_E_FOLLOWUP`, unrelated
to this fix's own code path (zero `get_objects`/`read_object_rows`
measurable cost in that trace).
