# Package E — E1-TREE-REUSE — Convergent design (CYCLE 5)

```text
TASK=E1-TREE-REUSE-DESIGN-V1
CYCLE=5 (owner-authorized additional cycle, cycle 2 of 3; answers adversarial cycle-4
  finding AR-4-1 — the sole open MAJOR (TR5 error-path mutex cleanup); cycle-3 AR-3-1,
  cycle-2 AR-2-1/AR-2-2 and cycle-1 AR-1-1..AR-1-5 remain closed — see the Cycle 5 /
  Cycle 4 / Cycle 3 / Cycle 2 revision responses below)
BASELINE=36839c7faa55b4568c1724cf6232468957f6aac8
STATE_WRITE=no  DIAGRAM_WRITE=no  PRODUCTIVE_CHANGES=no  IMPLEMENTATION_AUTHORIZED=no
STATUS=DESIGN_CONVERGED_ALL_GATES_APPROVED (design-only; no code, no state.md, no commit)
ADVERSARIAL_REVIEW=APPROVE (5 cycles, 9 findings, all CLOSED — see
  .memory/reviews/variant_b_package_e_e1_tree_reuse_adversarial.md)
CORRECTNESS_GATE=APPROVE, PROTOCOL_PERSISTENCE_GATE=APPROVE,
  PERFORMANCE_DESIGN_GATE=APPROVE (see the three matching files under
  .memory/reviews/variant_b_package_e_e1_tree_reuse_*.md)
REMAINING_PRECONDITION=TR0 Stage-0 SAT measurement + a SEPARATE explicit owner
  go/no-go on TR1-TR5; no implementation is authorized by design approval alone.
SCOPE_AUTHORITY=owner E1-TREE-REUSE prompt (2026-07-31) + cycle-2..5 revision
  prompts; satisfies the pre-written state.md re-entry condition "owner approves an
  additive secondary-index DDIC change and a dedicated design + performance DESIGN_GATE".
```

This design builds strictly on the Phase-1 discovery artifact
(`variant_b_package_e_e1_tree_reuse_discovery.md`) and design-doc §2/§8
(`variant_b_package_e_design.md`). It does not re-run broad discovery; where it
extends the discovery evidence, the addition is marked `[NEW]`.

--------------------------------------------------------------------------------
## Cycle 5 revision responses (AR-4-1)
--------------------------------------------------------------------------------

This is an OWNER-AUTHORIZED additional cycle (cycle 2 of the 3-cycle budget granted for
AR-3-1/AR-4-1 convergence), closing the single remaining MAJOR AR-4-1 from adversarial
cycle 4. Only the TR5 `clear_tree_memo` ERROR-path lock handling and the associated
compliance/section wording are touched; every already-closed finding
(AR-1-1..AR-1-5, AR-2-1, AR-2-2, AR-3-1 success-path) is preserved unchanged.

```text
AR-4-1  MAJOR  ACCEPTED_AND_FIXED.
  Finding accepted: the cycle-4 TR5 error path relied on ROLLBACK WORK ALONE to release
  the LOCK_<repo_key> mutex (by discarding the uncommitted acquire INSERT), and the doc
  forbade calling release_repo_lock after rollback. That proof holds ONLY if no COMMIT
  WORK / implicit commit / update-task boundary / nested helper commit can occur between
  acquire_repo_lock's successful INSERT and the CATCH's ROLLBACK WORK. clear_tree_memo is
  a NEW public admin method that OWNS its transaction boundaries (it already COMMITs on
  success), so that "no intervening commit" assumption is not a stable weak-model
  contract; if any future helper/UI/admin-report wrapper introduces a commit after
  acquire, the ROLLBACK WORK would no longer remove the now-durable mutex row -> lock
  leak / DoS.
  FIX (owner-specified REQUIRED_CHANGE, adopted verbatim): TR5's CATCH cx_root handler now
  performs an EXPLICIT, UNCONDITIONAL captured-id cleanup on every error path:
    CATCH cx_root INTO DATA(lx_err).
      ROLLBACK WORK.                                            " discard memo-table deletes
      zcl_abapgit_ortec_pack_raw=>release_repo_lock( lv_lock_id ). " explicit cleanup, always
      COMMIT WORK AND WAIT.                                       " make the release durable
      RAISE EXCEPTION TYPE zcx_abapgit_ortec_git EXPORTING iv_text =
        |Tree memo clear failed for { iv_repo_key }| ... previous = lx_err.
  RATIONALE (recorded per the owner prompt): clear_tree_memo is its OWN top-level
  admin-action LUW owner — UNLIKE rebuild_index, which is a library method whose CALLER
  owns the LUW and which therefore never commits at all (it releases in both CATCH
  handlers and lets the caller's LUW decide). Because clear_tree_memo already issues
  COMMIT WORK AND WAIT on its success path, it is consistent and safe for it to ALSO
  explicitly release-then-commit its own cleanup on the error path: the ROLLBACK WORK
  discards the uncommitted memo DELETEs, the explicit release_repo_lock( lv_lock_id )
  deletes the mutex row whether or not it had become durable via any hypothetical
  intervening commit, and the trailing COMMIT WORK AND WAIT makes that release durable.
  This is a DELIBERATE, JUSTIFIED difference from rebuild_index's rollback-free /
  release-only pattern — NOT a claim of equivalence to it. clear_tree_memo's cleanup is
  provably durable BECAUSE it owns its own commit boundary, which is a STRONGER guarantee
  than relying on rollback semantics to undo the acquire INSERT. All prior text asserting
  the error path "matches rebuild_index's exact pattern" or warning against calling
  release_repo_lock after rollback is REMOVED as stale.
  RETEST alignment: clear_memo_err_releases_lock now forces a DB error after a successful
  acquire and asserts that after the raised zcx_abapgit_ortec_git, SELECT SINGLE from
  zaog_fetch_sess WHERE session_id = |LOCK_{ iv_repo_key }| AND status='L' returns NO row
  (explicit release + commit cleaned up), and a subsequent acquire_repo_lock for the same
  repo_key succeeds immediately (no leaked mutex).
  §12's "no new COMMIT WORK anywhere" claim is scoped to TR1-TR4 (the rebuild_index/BFS
  library integration, which adds no commit point); TR5 is carved out as a standalone
  admin action that legitimately owns two commit points (success and error cleanup).
  CHANGED_SECTIONS=§11/TR5 (CATCH body, INVARIANTS, ERROR_ROLLBACK_FALLBACK, TESTS, header
  tag), §8/C14 (error-path release note), §10 (clear_memo_err_releases_lock retest), §12
  (COMMIT-WORK scope carve-out), §13 (AR-3-1/AR-4-1 compliance lines rewritten), §14 (cycle
  marker + next action), and the Cycle 4 AR-3-1 response item (d) corrected in place to the
  explicit release+commit error path.
```

--------------------------------------------------------------------------------
## Cycle 4 revision responses (AR-3-1)
--------------------------------------------------------------------------------

This is an OWNER-AUTHORIZED additional cycle (beyond the original 3-cycle automatic cap),
granted specifically to close the single remaining BLOCKER AR-3-1 from adversarial
cycle 3. Only the TR5 `clear_tree_memo` lock-handling is touched; every already-closed
finding (AR-1-1..AR-1-5, AR-2-1 route, AR-2-2 batching) is preserved unchanged.

```text
AR-3-1  BLOCKER  ACCEPTED_AND_FIXED.
  Root cause CONFIRMED by direct source read this pass:
    - zcl_abapgit_ortec_pack_raw=>acquire_repo_lock IMPORTING iv_repo_key TYPE ty_repo_key
      RETURNING VALUE(rv_lock_id) TYPE ty_session_id, and its body sets
      rv_lock_id = |LOCK_{ iv_repo_key }| (verified lines ~169-176 signature, ~417-455 body).
    - release_repo_lock IMPORTING iv_lock_id TYPE ty_session_id, body
      DELETE FROM zaog_fetch_sess WHERE session_id = iv_lock_id AND status = 'L'
      (verified lines ~179-181 signature, ~459-467 body). It guards `IF iv_lock_id IS
      INITIAL. RETURN.` first.
    - rebuild_index's EXACT pattern (verified lines ~298/324/328/492/494/497):
        DATA lv_lock_id TYPE zcl_abapgit_ortec_pack_raw=>ty_session_id.
        lv_lock_id = zcl_abapgit_ortec_pack_raw=>acquire_repo_lock( iv_repo_key = iv_repo_key ).
        ... zcl_abapgit_ortec_pack_raw=>release_repo_lock( lv_lock_id ).   " on the ready
        return, the success path, AND both CATCH paths.
  DEFECT (cycle-3 TR5): the packet called acquire_repo_lock( iv_repo_key ) WITHOUT capturing
  the returned lock id, then called release_repo_lock( iv_repo_key ) — passing the BARE repo
  key. Since release deletes WHERE session_id = iv_lock_id, that would delete session_id =
  <repo_key> (which does not exist) and NEVER delete the actual mutex row session_id =
  |LOCK_{ repo_key }|, so a successful clear would leak its own mutex row and block every
  future rebuild_index/clear_tree_memo for that repo until lock timeout (DoS).
  FIX (mechanical; no architecture change): TR5's body now (a) declares
  `DATA lv_lock_id TYPE zcl_abapgit_ortec_pack_raw=>ty_session_id.` exactly like
  rebuild_index; (b) CAPTURES the id —
  `lv_lock_id = zcl_abapgit_ortec_pack_raw=>acquire_repo_lock( iv_repo_key = iv_repo_key ).`;
  (c) releases with the CAPTURED id on the success path —
  `zcl_abapgit_ortec_pack_raw=>release_repo_lock( lv_lock_id ).`; and (d) on the error path
  [SUPERSEDED by Cycle 5 / AR-4-1: the cycle-4 draft released the mutex by `ROLLBACK WORK`
  alone and forbade a post-rollback release — that relied on no commit point existing
  between acquire and the CATCH. Cycle 5 replaces it with an EXPLICIT, UNCONDITIONAL
  cleanup: `ROLLBACK WORK.` then `release_repo_lock( lv_lock_id ).` then
  `COMMIT WORK AND WAIT.`, provably durable because clear_tree_memo owns its own commit
  boundary — see the Cycle 5 revision responses and §11/TR5]. The busy/reject path never
  acquired the mutex, so it neither releases nor rolls back. clear_memo_removes_rows now
  additionally asserts that
  after a successful clear NO zaog_fetch_sess row with session_id = |LOCK_{ repo_key }| AND
  status='L' remains; clear_memo_rejects_if_locked still proves a pre-existing
  LOCK_<repo_key> status='L' row is untouched after a reject.
  CHANGED_SECTIONS=§11/TR5 (body capture+release, INVARIANTS, TESTS), §10 (clear_memo_*
  assertion notes), §13 (new AR-3-1 compliance line), §14 (next action), and the Cycle 3
  AR-2-1 response item (1) aligned to the named-parameter acquire call for consistency.
```

--------------------------------------------------------------------------------
## Cycle 3 revision responses (AR-2-1, AR-2-2)
--------------------------------------------------------------------------------

This is the FINAL automatic revision cycle. Both remaining cycle-2 MAJOR findings are
answered by ID below with `ACCEPTED_AND_FIXED` (exact changed sections named) or
`REJECTED_WITH_PROOF`. The cycle-1 closures (AR-1-1..AR-1-5, verified FIXED in the
adversarial cycle-2 ledger) are preserved; where a cycle-1 fix's TEXT is now superseded
by a cycle-3 fix (specifically the AR-1-1 admin-clear channel), that text is corrected
in place so no stale, contradicting wording remains.

```text
AR-2-1  MAJOR  ACCEPTED_AND_FIXED.
  Root cause CONFIRMED by direct source read this pass (not merely the reviewer's
  paraphrase):
    - rebuild_index's real mutex is zcl_abapgit_ortec_pack_raw=>acquire_repo_lock,
      which INSERTs a zaog_fetch_sess row with session_id = |LOCK_{repo_key}|,
      status='L', error_text='MUTEX' (verified: acquire_repo_lock lines ~417-455).
      release_repo_lock deletes exactly that row: DELETE FROM zaog_fetch_sess
      WHERE session_id = iv_lock_id AND status = 'L' (verified lines ~459-467).
    - zcl_abapgit_ortec_cache_admin=>clear_repo uses a DIFFERENT primitive:
      its private acquire_lock calls ENQUEUE_EZAOG_REPO_LOCK with session_id =
      iv_repo_key (a SAP enqueue on the EZAOG_REPO_LOCK object, keyed CLIENT +
      SESSION_ID), NOT the LOCK_<repo_key> row-mutex (verified: acquire_lock/
      release_lock + ezaog_repo_lock.enqu.xml). It then executes
      DELETE FROM zaog_fetch_sess WHERE repo_key = iv_repo_key (verified in
      clear_repo's TRY block) — the exact broad delete that would remove an
      in-flight rebuild's LOCK_<repo_key> mutex row plus any active fetch sessions.
  Therefore the cycle-2 plan (route the memo clear THROUGH clear_repo's existing
  per-repo delete block) is UNSAFE: it inherits both the divergent enqueue key AND
  the broad zaog_fetch_sess-by-repo_key delete, so a clear could overlap a live
  rebuild and delete its mutex/memo/index rows (the AR-2-1 TOCTOU).
  FIX (owner-preferred shape, adopted): the memo clear is NO LONGER routed through
  clear_repo. It becomes a NEW, fully self-contained admin action
  zcl_abapgit_ortec_cache_admin=>clear_tree_memo that:
    (1) acquires the SAME mutex rebuild_index uses —
        lv_lock_id = zcl_abapgit_ortec_pack_raw=>acquire_repo_lock( iv_repo_key = iv_repo_key ) —
        so it is serialized against any in-flight rebuild_index by the SAME primitive
        (the DB unique-key on LOCK_<repo_key> is the single point of exclusion);
    (2) under that held lock, deletes ONLY the two memo tables for the repo_key:
        DELETE FROM zaog_tree_child WHERE repo_key = iv_repo_key and
        DELETE FROM zaog_tree_map   WHERE repo_key = iv_repo_key;
    (3) issues NO `DELETE FROM zaog_fetch_sess WHERE repo_key` — EVER. The ONLY
        zaog_fetch_sess row it writes/removes is its OWN LOCK_<repo_key> mutex row,
        created by acquire_repo_lock and removed by release_repo_lock (which keys on
        session_id = LOCK_<repo_key> AND status='L'). Because the mutex is
        single-holder (a concurrent rebuild holding it makes clear_tree_memo's
        acquire INSERT fail/retry/timeout, so clear never reaches release while a
        rebuild holds the row), release_repo_lock can only ever delete clear's OWN
        row, never a rebuild's row and never an active fetch session (status 'A').
  Lock key      = LOCK_<repo_key> row in zaog_fetch_sess (identical to rebuild_index).
  Lock order    = one lock only, no nesting -> no deadlock possible.
  Busy behavior = WAIT then REJECT: acquire_repo_lock retries with bounded
                  exponential backoff (iv_max_attempts=7, capped 2 s/attempt); if a
                  rebuild still holds the mutex after the budget it RAISES
                  zcx_abapgit_exception, which clear_tree_memo converts to
                  zcx_abapgit_ortec_git and re-raises WITHOUT having deleted any memo
                  row. clear_tree_memo never deletes memo/index/lock rows while a
                  rebuild is active.
  General clear_repo (E3, SAP_VALIDATED_COMPLETE) is NOT modified/reopened. It does
  not clear the memo tables; leftover memo rows after a general clear_repo are safe
  (content-addressed by tree_sha1, independent of obj_store, gated by dot_sha1/
  algo_ver, bounded by the TTL, and never a source of blob reads during rebuild) and
  can be removed on demand by clear_tree_memo. Documented in §5.1/§7/§12.
  CHANGED_SECTIONS=§1 (clear_repo fact corrected to the dedicated clear_tree_memo
  channel), §2/IN-7(b), §5.1 (admin-clear line), §5.2 (schema-checklist migration),
  §6 (recovery channel), §7 (sole deletion channel), §8 (C11/C12 recovery + new C14
  clear-during-rebuild row + C8 note), §11/TR5 (rewritten as clear_tree_memo, mutex-
  based, self-contained), §10 (clear_memo_removes_rows + clear_memo_rejects_if_locked
  tests), §12 (migration (c)), §13 (AR-2-1 compliance line), §14. The Cycle 2 AR-1-1
  response (2) bullet is corrected in place to name clear_tree_memo.

AR-2-2  MAJOR  ACCEPTED_AND_FIXED.
  Root cause accepted: the cycle-2 SMALL-tree batching rule closed a batch only AFTER
  the running sum of CHILD_COUNT reached c_tree_child_chunk_size (add-then-check),
  admitting up to ~2x chunk rows in one FAE (e.g. two 29,999-child trees = 59,998
  rows), which contradicted the §12/§13 "at most one chunk resident" claim.
  FIX: make the contract single-valued via PEEK-then-decide. The SMALL batch is CLOSED
  and its FAE issued BEFORE adding any tree whose CHILD_COUNT would push the batch's
  running sum OVER c_tree_child_chunk_size; that tree is deferred to the NEXT batch
  (in addition to the existing lines(batch)=c_tree_lookup_chunk_size cap). Because a
  SMALL tree by definition has CHILD_COUNT <= c_tree_child_chunk_size, an empty batch
  can always accept the next tree, so no tree is ever stuck.
  Exact resulting worst case = a strict `<= c_tree_child_chunk_size` child rows per
  FAE (never ~2x). This single bound is now stated identically in §9 (SMALL text +
  peak-memory model + 1,000,000-row table), §11/TR2 (read_small_children), §11/TR3
  (HIT_SMALL), §12 (batch policy + peak-memory), §13 (AR-1-2/AR-2-2 line), and §10
  (new reuse_small_batch_bound boundary test with chunk-1 + chunk-1 straddle). A grep
  of this revised document for every child-row batching description confirms no
  remaining "~2x" / "one additional small tree's worth" wording.
  CHANGED_SECTIONS=§9, §11/TR2, §11/TR3, §12, §13, §10.
```

--------------------------------------------------------------------------------
## Cycle 2 revision responses (AR-1-1 .. AR-1-5)
--------------------------------------------------------------------------------

Each cycle-1 finding is answered by ID below with `ACCEPTED_AND_FIXED` (exact
changed sections named) or `REJECTED_WITH_PROOF`. No finding is dropped. The
reviewer's Verified Non-Findings NF-1..NF-5 are treated as CONFIRMED and are not
re-litigated (they constrain, not contradict, the fixes below).

```text
AR-1-1  BLOCKER  ACCEPTED_AND_FIXED.
  Root cause accepted: a standard MAP_FILENAME_TO_OBJECT plugin can read mutable
  SAP state (TADIR/customizing/namespace/package metadata) that changes WITHOUT an
  abapGit version bump or an ORTEC algo bump, so a content+path+context key alone
  cannot detect that drift. Closure = a CONCRETE two-part operational safeguard,
  documented as the ACCEPTED residual-risk boundary (same shape as the already-
  accepted E4-OOB-DELETION-RISK precedent), NOT an unbounded silent-wrong risk:
    (1) Mandatory memo max-age (TTL). Every zaog_tree_map header carries a BUILT_AT
        TIMESTAMP written at memo-write time. Eligibility rejects (forces MISS +
        full re-walk + fresh memo) ANY header older than a fixed bound
        c_tree_memo_max_age_secs VALUE 604800 (7 days). This bounds the maximum
        window during which undetected mutable-state drift can be replayed to
        <= 7 days, after which every affected memo self-heals with no operator
        action. Enforced set-based via a cutoff predicate in the header SELECT.
    (2) Explicit, always-available admin clear. A NEW dedicated admin action
        zcl_abapgit_ortec_cache_admin=>clear_tree_memo (added by §11/TR5) deletes
        zaog_tree_child/zaog_tree_map WHERE repo_key = iv_repo_key under the SAME
        repo mutex rebuild_index uses (zcl_abapgit_ortec_pack_raw=>acquire_repo_lock),
        giving an immediate, lock-safe "clear tree memo for repo_key" action after a
        known support-package upgrade. [CORRECTED in Cycle 3 / AR-2-1: the cycle-2
        draft routed this through the existing E3 clear_repo delete block, which used a
        DIVERGENT lock and a broad zaog_fetch_sess-by-repo_key delete; the dedicated
        mutex-based clear_tree_memo replaces that unsafe routing. General clear_repo is
        NOT modified.]
  CHANGED_SECTIONS=§1 (TTL design-constant row), §2/IN-7 (mitigation -> accepted
  boundary), §3 (eligibility now includes BUILT_AT within TTL), §5.1 (BUILT_AT
  column + admin-clear), §8 (C1/C7 + new C11 aged-out), §9 (cutoff compute +
  header predicate), §10 (reuse_miss_memo_aged_out test), §11/TR1 (TTL constant +
  cutoff helper), §11/TR2 (BUILT_AT column + write-time stamp + read cutoff),
  §11/TR5 (NEW cache-admin memo-clear hook), §12 (migration), §13 (compliance).

AR-1-2  MAJOR  ACCEPTED_AND_FIXED.
  Root cause accepted: the cycle-1 single FAE child SELECT over lt_hit_keys loads a
  whole flat tree's child rows at once, unbounded for a 1,000,000-child directory.
  Closure = a decision-free child-read streaming contract that never materializes
  more than c_tree_child_chunk_size child rows for one tree, and reproduces/flushes
  zaog_obj_index rows incrementally per page:
    - Hits are split by header CHILD_COUNT into SMALL (<= c_tree_child_chunk_size)
      and LARGE (> c_tree_child_chunk_size).
    - SMALL trees: FAE over hit keys, driver batch closed by PEEK-then-decide \u2014
      close BEFORE adding any tree whose CHILD_COUNT would push the running sum OVER
      c_tree_child_chunk_size, OR when key count reaches c_tree_lookup_chunk_size
      (whichever first) -> each FAE returns <= c_tree_child_chunk_size rows,
      reproduced+flushed, then discarded before the next batch. Reads stay
      O(level batches), not O(nodes). [CORRECTED in Cycle 3 / AR-2-2: the cycle-2
      draft closed AFTER the sum reached the chunk, admitting up to ~2x chunk rows;
      peek-then-decide makes the bound strictly <= one chunk.]
    - LARGE trees: read ALONE via CHILD_SEQ windows
      (WHERE key AND child_seq BETWEEN f AND f+chunk-1 ORDER BY PRIMARY KEY),
      reproduced+flushed per window, advancing f until a short/exhausted page.
      Never more than c_tree_child_chunk_size child rows resident.
  CHANGED_SECTIONS=§5 (PK CHILD_SEQ enables range paging), §9 (new SQL shapes +
  peak-memory model + 1,000,000-row acceptance), §10 (reuse_flat_dir_paged test),
  §11/TR2 (read_tree_headers / read_small_children / read_large_child_page),
  §11/TR3 (paged HIT reproduction), §12 (memory model / acceptance).

AR-1-3  MAJOR  ACCEPTED_AND_FIXED.
  Root cause accepted: current rebuild_index length-guards only FILE rows; DIR
  names are concatenated + enqueued with no 255 guard, and CHILD_NAME CHAR255
  cannot store an over-255 DIR component. Closure = add the SAME 255-char guard to
  DIR nodes, applied identically in (a) the fresh MISS walk, (b) HIT reproduction,
  and (c) the CURRENT unmemoized path -> a >255 DIR component is skipped (CONTINUE),
  never enqueued, never memoized. Proven strictly conservative/inert for realistic
  repos: a single Git tree-entry name > 255 bytes cannot exist on any mainstream
  filesystem (ext4/NTFS/APFS/HFS+ all cap ONE path component at 255 bytes) and
  abapGit's FILE rows already rely on exactly this 255 bound; and where it could
  fire, EVERY file beneath a >255 component already has path>255 and is already
  skipped by the existing FILE guard, so the subtree already contributes ZERO index
  rows today -> skipping at the DIR is row-output-identical. The memo therefore only
  ever stores <=255 DIR names, making HIT reproduction byte-exact vs the fresh walk
  by construction. This is a tiny, justified, in-scope change to pre-existing
  rebuild_index behavior (required to make the memo byte-exact); the sole non-inert
  difference (the unreachable same-subtree-at-oversized-and-normal-path lt_seen_trees
  case) is documented in §8/§3 as a strictly-more-correct outcome the memo faithfully
  reproduces.
  CHANGED_SECTIONS=§3 (DIR guard in row-identity/traversal), §5.2 (CHILD_NAME now
  provably <=255), §8 (C13 oversized-DIR case), §10 (reuse_dir_overlen_skip test),
  §11/TR3 (DIR guard in shared node processing).

AR-1-4  MAJOR  ACCEPTED_AND_FIXED.
  Root cause accepted: the 30% Stage-0 gate is an invented, unapproved threshold
  that would autonomously proceed/park a correctness-complete design. Closure =
  TR0's STOP_IF is changed to BLOCK_OWNER_DECISION: TR0 MEASURES and REPORTS the
  write:compute split as a fact and STOPS; the artifact explicitly states this
  number requires a separate owner go/no-go before TR1 starts. All language
  implying the design itself authorizes proceeding past Stage-0 is removed. The 30%
  figure is retained ONLY as a non-binding engineering reference the owner may use,
  never as an autonomous gate.
  CHANGED_SECTIONS=§4 (alternative D + DECISION reworded), §11/TR0 (STOP_IF ->
  BLOCK_OWNER_DECISION), §12 (acceptance), §13 (compliance), §14 (next action).

AR-1-5  MAJOR  ACCEPTED_AND_FIXED.
  Root cause accepted: the child-COUNT re-check proves only cardinality/deletion
  loss, not rowset identity, so C10's "deleted/changed" wording over-claimed.
  Closure = C10 is narrowed precisely to deletion/row-count loss (which IT detects
  deterministically). Same-COUNT content corruption (out-of-band admin repair, a
  future writer bug overwriting child_seq rows without changing the count) is
  explicitly classified as OUT-OF-SCOPE external DB corruption, recovered by the
  SAME dedicated admin clear as AR-1-1 (clear_tree_memo; see AR-2-1 correction) and
  additionally bounded to <= TTL by AR-1-1's max-age. No broader guarantee than the
  check delivers is claimed.
  CHANGED_SECTIONS=§6 (completeness claim narrowed), §8 (C10 narrowed + new C12
  same-count-corruption OOB), §10 (reuse_memo_deleted_midway scoped to count loss).
```

--------------------------------------------------------------------------------
## 1. Facts ledger (CONFIRMED / MEASURED / OWNER_DECISION / HYPOTHESIS / UNKNOWN / SUPERSEDED)
--------------------------------------------------------------------------------

Base rows are inherited verbatim from discovery §2 and are not re-listed; only the
load-bearing carries and the new facts derived this pass are recorded.

```text
CONFIRMED  | c_index_write_chunk_size = 30000 (live value; do NOT change — out of scope).
CONFIRMED  | rebuild_index issues NO COMMIT WORK; the entire per-commit build runs in
             the CALLER's single SAP LUW (design-doc §2, re-verified this pass by source
             read of zcl_abapgit_ortec_obj_index.clas.abap). Transaction owner = caller.
CONFIRMED  | rebuild_index is serialized per repo_key by
             zcl_abapgit_ortec_pack_raw=>acquire_repo_lock/release_repo_lock, held for
             the whole call, released on every exit path (success/zcx/cx_root).
CONFIRMED  | Readiness is proven ONLY by the $IDX/__READY__ marker row written
             UNCONDITIONALLY LAST (is_index_ready STRICT). RELAXED mode is benchmark-only,
             never ships as default.
CONFIRMED  | zaog_obj_index primary key = CLIENT,REPO_KEY,COMMIT_SHA1,OBJ_TYPE,OBJ_NAME,
             PATH_HASH. The row set is therefore order-independent; a file row's PATH_HASH
             is sha1_string( file_path && file_name ), TREE_SHA1 is the file's IMMEDIATE
             parent tree. Verified by direct read of zaog_obj_index.tabl.xml (no secondary
             index defined) + rebuild_index source.
CONFIRMED  | zaog_obj_store PK = CLIENT,REPO_KEY,OBJ_SHA1 — objects are stored PER repo_key,
             NOT globally content-addressed across repos. Forces C4 (cross-repo) ineligible.
CONFIRMED  | io_dot->get_signature( )-sha1 = zcl_abapgit_hash=>sha1_blob( serialize( ) ),
             and serialize( ) = string_to_xstring_utf8_bom( to_xml( ms_data ) ). It is a
             deterministic, byte-exact pure function of the ENTIRE parsed .abapgit content
             (starting_folder, folder_logic, ignore/mapping, i18n, requirements, name,
             version, original_system, abap_language_version). [NEW: proven by direct read
             of get_signature/serialize/to_xml this pass — confirms the discovery §4 reuse
             opportunity; ACCEPTED verbatim, see §3.]
CONFIRMED  | file_to_object receives iv_path and dispatches per-object-type
             MAP_FILENAME_TO_OBJECT plugins (standard abapGit, 100+ classes) that may read
             SAP system state. Mapping output is therefore path-dependent and NOT globally
             enumerable — forces path into the reuse key and forces a monotonic algo stamp
             (discovery §4, carried).
CONFIRMED  | [NEW, AR-1-1] MAP_FILENAME_TO_OBJECT plugins can read MUTABLE SAP state
             (TADIR/customizing/namespace/package metadata) that changes WITHOUT any
             abapGit-version or ORTEC-algo bump. A content+path+context key alone cannot
             detect this drift, so a warm HIT could replay stale obj_type/obj_name. This is
             NOT closed by algo_ver; it is closed operationally by a mandatory memo TTL AND
             an admin clear (see the two OWNER_DECISION rows below and §2/IN-7, §8/C11-C12).
CONFIRMED  | [NEW, AR-1-3] Current rebuild_index length-guards ONLY FILE rows
             (strlen(path)>255 OR strlen(name)>255 -> CONTINUE, verified by direct read of
             the WHEN c_chmod-file branch this pass). DIR nodes are CONCATENATEd into
             lv_next_path and enqueued with NO 255 guard. A single Git tree-entry name >255
             bytes cannot exist on any mainstream filesystem (ext4/NTFS/APFS/HFS+ cap one
             path component at 255 bytes) and abapGit's own FILE rows already depend on this
             bound; and every file beneath a >255 component already has path>255 and is
             already skipped today -> such a subtree already yields ZERO index rows. The
             design adds the identical guard to DIR nodes (§3, §11/TR3) so the memo only ever
             stores <=255 DIR names; the change is row-output-identical for realistic repos.
CONFIRMED  | [NEW, AR-1-1] The E3 admin surface zcl_abapgit_ortec_cache_admin=>clear_repo
             (Package E3, SAP_VALIDATED_COMPLETE) already performs a per-repo_key delete
             sweep (zaog_obj_index/pack_idx/raw_pack/pack_meta/fetch_sess/commit_hist/
             obj_store/repo_state) inside a lock + COMMIT WORK AND WAIT, returning a
             ty_clear_result row-count struct. Verified by direct read this pass. [NEW,
             AR-2-1] clear_repo's lock is ENQUEUE_EZAOG_REPO_LOCK (session_id=iv_repo_key),
             a DIFFERENT primitive from rebuild_index's LOCK_<repo_key> zaog_fetch_sess
             row-mutex, and it does DELETE FROM zaog_fetch_sess WHERE repo_key — so it is
             NOT safe to route the memo clear through it. The memo clear is therefore a NEW
             dedicated method clear_tree_memo (§11/TR5) that acquires the SAME
             zcl_abapgit_ortec_pack_raw=>acquire_repo_lock mutex as rebuild_index and
             deletes ONLY the two memo tables; general clear_repo is NOT modified.
OWNER_DECISION | [NEW, AR-1-1] c_tree_memo_max_age_secs shipped default = 604800 (7 days).
             Bounds the maximum undetected-mutable-state-drift replay window to <= 7 days
             (self-healing, no operator action); amortizes to ~one full walk/week/repo (vs
             today's full walk EVERY commit, so still a net win in the warm window). A design
             constant with a concrete decision-free default; owner may retune later.
OWNER_DECISION | [NEW, AR-1-4] The Stage-0 write:compute split (TR0) is a BLOCK_OWNER_
             DECISION: TR0 measures and REPORTS the split, then STOPS. Whether it justifies
             TR1-TR5 is an explicit owner go/no-go, NOT an autonomous design gate. The former
             "compute >= 30%" figure is retained only as a non-binding engineering reference.
CONFIRMED  | lt_seen_trees dedups by tree_sha1 ALONE within one build; a tree_sha1 reachable
             at two paths in one commit is walked once, at the FIRST path only. [NEW: this
             is EXISTING behavior; the design preserves it exactly (§3, §8-C5).]
MEASURED   | chunk=1000 @ ~42,000 rows (SAT O4H-8794): write side (Phase I) 9.39 s / 41
             MODIFY / 82 DB:Exec; decode_tree 0.97 s / 340 trees; path_to_package 0.36 s /
             338; net traversal-compute residual ~4-5 s; read side 11 store reads / 0.07 s.
             Write cost characterised by the incident as round-trip-COUNT-driven, not
             byte-volume-driven.
MEASURED   | Row-count spread across the only 4 real multi-commit indexes = 12 rows of
             ~42,000 (<=0.03%) — strong evidence of a small logical file delta between
             sibling branches, i.e. most subtrees are byte-identical across those commits.
OWNER_DECISION | chunk raised 1000->30000 after review; OWNER_TESTED_BATCH=20000 "no error".
UNKNOWN    | No SAT/timing trace exists at the LIVE chunk=30000. The write:compute split at
             30000 is unmeasured. This is the single gating unknown for the benefit claim
             (§4, §9, §12). At 30000, 42,000 rows = 2 MODIFY chunks; whether the residual
             write cost is now small (round-trip-bound => already fast) or still large
             (byte/array-DML-bound) is NOT established.
HYPOTHESIS | [NEW, refined from discovery §7] Root-tree-only reuse => ZERO hits on the real
             evidence (differing root trees). Per-subtree reuse => high hit rate on the same
             evidence (single leaf change leaves sibling subtrees byte-identical). Subtree
             reuse eliminates fetch+decode+map COMPUTE for unchanged subtrees but does NOT
             reduce the per-commit index WRITE (rows stay commit-scoped, copy-not-alias).
             Net benefit is therefore bounded by the compute fraction, which is UNKNOWN at
             30000 (see UNKNOWN row). Expressed as a bounded ESTIMATE only, never a measured
             claim, until the chunk=30000 trace exists.
SUPERSEDED | design-doc §2 "NEW_CONSTANT ... VALUE 5000" superseded by live 30000; the
             E1-A contract-vs-live reconciliation is a separate, out-of-scope item.
SUPERSEDED | design-doc §2 candidate "E1-D bare (repo_key,tree_sha1) key" — PROVEN UNSAFE;
             this design replaces it with the composite content+path+context key in §3.
```

--------------------------------------------------------------------------------
## 2. Complete interpretation-input inventory (extends discovery §4)
--------------------------------------------------------------------------------

Every input that can change OBJ_TYPE / OBJ_NAME / PATH_HASH / FILE_PATH / FILE_NAME
for a given raw Git tree/blob SHA1. No input from discovery §4 is dropped; two are
sharpened and one is added.

```text
IN-1 repo_key       -> which zaog_obj_store rows exist (per-repo store). MUST be in the
                       reuse key (C4). Verbatim caller value.
IN-2 tree_sha1 (of  -> the exact {name,mode,child-sha1} set, recursively (Git Merkle
     each subtree)     guarantee). The ONLY content-proven-identical input. Reuse anchor.
IN-3 io_dot content -> starting_folder, folder_logic (PREFIX/FULL), ignore/mapping,
                       i18n, master_language, requirements, name/version/original_system,
                       abap_language_version. Canonical byte-exact fingerprint already
                       exists: io_dot->get_signature( )-sha1. Reuse verbatim (§3). (C2)
IN-4 iv_devclass    -> passed into file_to_object -> map_filename_to_object, and into
                       package_to_path/folder resolution. MUST be in the reuse key. (C3)
IN-5 object-type    -> per-type ZCL_ABAPGIT_OBJECT_*~MAP_FILENAME_TO_OBJECT (dynamic CALL
     mapping plugin     METHOD). Standard abapGit; may read TADIR/customizing/namespace
     code + release      tables. Not enumerable. Covered by a monotonic algo fingerprint
                        (IN-7). (C7)
IN-6 path context   -> file_path (= tree's absolute path P) AND, via file_to_object's
     (P = tree path)    iv_path, potentially obj_type/obj_name. Same tree_sha1 can appear
                        at a DIFFERENT path via `git mv unchanged_dir new_name`. MUST be
                        in the reuse key (as sha1(P)); tree_sha1 alone is never sufficient.
IN-7 [NEW] algorithm/schema fingerprint -> a single monotonic stamp that changes whenever
                        ANY link of {ORTEC row-building logic, file_to_object call form,
                        folder-logic contract, tree_map serialization} could alter output.
                        Composed of an ORTEC-owned constant AND the abapGit build version
                        (zif_abapgit_version=>c_abap_version = '1.133.0'), so a tool
                        upgrade auto-invalidates memos. NOT covered by algo_ver alone:
                        SAP support-package upgrades that change a STANDARD object-type
                        plugin WITHOUT bumping the abapGit version, and any other mutable
                        SAP state a plugin reads (TADIR/customizing/namespace/package meta).
                        [AR-1-1 CLOSURE — accepted residual-risk boundary, same shape as the
                        accepted E4-OOB-DELETION-RISK precedent, NOT an unbounded risk]:
                          (a) mandatory memo TTL — a header older than
                              c_tree_memo_max_age_secs (VALUE 604800 = 7 days) is INELIGIBLE
                              (MISS -> re-walk -> fresh memo), bounding any undetected drift
                              replay to <= 7 days with zero operator action (§3, §8/C11, §9);
                          (b) explicit admin clear — a NEW dedicated action
                              zcl_abapgit_ortec_cache_admin=>clear_tree_memo deletes
                              zaog_tree_map/zaog_tree_child for the repo_key UNDER THE SAME
                              repo mutex rebuild_index uses (acquire_repo_lock), giving an
                              immediate, lock-safe invalidation after a known SP upgrade
                              (§11/TR5, AR-2-1). General clear_repo is NOT modified.
```

Note (IN-6 vs IN-2, existing behavior preserved): because lt_seen_trees dedups by
tree_sha1 alone within a build, a tree appearing at two paths in ONE commit is
already indexed only at its first-encountered path today. The design keeps that
exact dedup. tree_path_hash lives in the reuse key purely for CROSS-build path
safety (a tree at path P in commit A is reusable in commit B only if it is again at
path P in commit B).

--------------------------------------------------------------------------------
## 3. Byte-exact canonical reuse identity
--------------------------------------------------------------------------------

Reuse is memoized per TREE (not per commit, not per whole root), content-addressed.

```text
Canonical context  C := ( repo_key, dot_sha1, devclass, algo_ver )
  repo_key  = caller value (zcl_abapgit_ortec_obj_store=>ty_repo_key, CHAR12), verbatim.
  dot_sha1  = io_dot->get_signature( )-sha1                      (CHAR40, verbatim; §1 CONFIRMED)
  devclass  = iv_devclass                                        (DEVCLASS, verbatim)
  algo_ver  = zcl_abapgit_hash=>sha1_string(
                |{ c_index_algo_version }|{ zif_abapgit_version=>c_abap_version }| )  (CHAR40)
              where c_index_algo_version is a new private INT constant (VALUE 1 initially),
              bumped by hand on any output-affecting change to the build logic.

Per-tree reuse key := ( C, tree_sha1, tree_path_hash )
  tree_sha1      = the Git tree object SHA1 encountered as a DIR node / root tree (CHAR40).
  tree_path_hash = zcl_abapgit_hash=>sha1_string( P ), P = the tree's absolute repo-root
                   path as built during BFS (e.g. '/', '/src/', '/src/sub/')      (CHAR40).
```

ALL of C + tree_sha1 + tree_path_hash + map_status='R' + BUILT_AT within TTL +
child-count integrity must match before a memo is trusted. A miss on ANY component
=> full walk of that tree. BUILT_AT within TTL means BUILT_AT >= (now -
c_tree_memo_max_age_secs); it is the AR-1-1 max-age gate that bounds undetected
mutable-plugin-state drift to <= 7 days (§8/C11, §9).

DIR-overlength guard (AR-1-3): a DIR node whose CONCATENATEd path or whose name
exceeds 255 characters is skipped (CONTINUE) — not enqueued, not memoized — in the
fresh MISS walk, in HIT reproduction, and in the current unmemoized path alike. This
is row-output-identical for realistic repos (no >255 single path component can exist
on a mainstream filesystem, and any subtree under a >255 component already yields
zero index rows via the existing FILE guard) and guarantees CHILD_NAME CHAR255 can
always store the exact DIR name, so HIT reproduction is byte-exact by construction.
The only non-inert difference — an identical subtree reachable at BOTH an oversized-
component path and a normal path in ONE commit — is unreachable in practice and, if
synthesized, yields strictly-more-correct output (the normal-path occurrence is now
indexed instead of being suppressed by today's latent lt_seen_trees first-path drop);
the memo records and replays exactly this guarded-walk result.

Reuse GRANULARITY = per-tree (a whole unchanged subtree is reused level-by-level: if
tree T hits, its DIR children go to the next BFS level where they hit too, so the
entire unchanged subtree avoids fetch/decode/map). This is chosen over
whole-root-tree-only precisely because discovery §7 proves root-only would produce
ZERO hits on the only real evidence, whereas per-subtree matches the measured
single-leaf-change workload (§1 MEASURED row-count spread).

Row reproduced on a HIT (byte-identical to a fresh decode+map):
```text
  obj_type   = memo MAP_OBJ_TYPE
  obj_name   = memo MAP_OBJ_NAME
  blob_sha1  = memo CHILD_SHA1
  file_path  = P                      (the CURRENT walk's path for this tree)
  file_name  = memo CHILD_NAME
  path_hash  = zcl_abapgit_hash=>sha1_string( |{ P }{ CHILD_NAME }| )   (SAME formula as
                                        today's rebuild_index)
  tree_sha1  = tree_sha1              (this tree = immediate parent of the file)
  idx_status = 'R'
```
Because inputs (C, tree_sha1, P) are identical to a fresh build and the row set is
key-ordered, the produced zaog_obj_index row set is byte-identical regardless of
reuse — output, commit isolation, and readiness are unchanged.

`io_dot->get_signature( )-sha1` reuse is ACCEPTED (not rejected): it is the exact,
pre-existing, byte-exact identity abapGit itself assigns the .abapgit blob; inventing
a second hash would be a correctness liability with no benefit. Consequence for a
purely non-semantic .abapgit reordering (e.g. ignore-list re-sorted by hand): the
byte-exact serialize output differs => dot_sha1 differs => conservative MISS => full
walk => correct output. That is the safe direction (a miss is always correct; a false
hit never occurs). See §8-C5 for why this fully satisfies the C5 intent.

--------------------------------------------------------------------------------
## 4. Alternatives A-D (+ B2) and the decision
--------------------------------------------------------------------------------

```text
A  Whole-root-tree-only reuse (root tree_sha1 within C).
   REJECTED. Discovery §7 + §1 MEASURED: differing row counts prove differing root
   trees across all real commits => ZERO hits. Merkle propagation makes any single
   changed file change the root SHA1. Helps only message-only/revert-to-identical
   commits — not the evidenced workload.

B  Per-tree content-addressed resolved-child memo  [CHOSEN, staged behind Stage-0].
   Persist, content-addressed by ( C, tree_sha1, tree_path_hash ), the resolved child
   list of each tree the FIRST time it is decoded (dirs + mapped files + skips). On a
   later build, per BFS level, bulk-look-up the level's trees; HITS skip
   fetch+decode_tree+file_to_object entirely and reproduce their rows from the memo;
   MISSES walk as today and write their memo. zaog_obj_index rows are still built
   fresh per commit (COPY-not-alias, commit isolation intact). SAVES the traversal
   COMPUTE for unchanged subtrees; does NOT reduce the per-commit index WRITE.

B2 Cross-commit copy of another commit's zaog_obj_index rows (bare row copy).
   REJECTED in favour of B. A commit's recursive rows are not queryable by a single
   tree_sha1 predicate (rows record only their immediate parent tree), so a whole-
   subtree copy would need a recursive-rowset store (d x storage blow-up) OR aliasing
   (forbidden by discovery §6 COPY-not-alias / C10). Copying another commit's rows
   also creates the exact C10 "source rows deleted/changed between check and copy"
   hazard. B's memo is content-addressed and immutable-by-Merkle, not commit-owned, so
   C10 collapses to a cheap integrity re-check (§8-C10). B is strictly safer.

C  Incremental two-commit tree diff vs one designated indexed parent.
   REJECTED. Requires a "which parent is indexed and closely related" policy (fragile:
   the evidenced pair are SIBLING branches, not necessarily parent/child), plus
   explicit removed-path deletion handling. Its compute saving is a SUBSET of B's
   (B matches unchanged subtrees from ANY prior build, higher hit rate) and it ALSO
   does not reduce the write. No benefit over B; more moving parts.

D  No reuse; keep full-walk-every-commit.
   ADOPTED ONLY as the Stage-0 measurement, NOT as the permanent answer: before any B
   code ships, capture a fresh SAT trace at the live chunk=30000 to measure the
   write:compute split (decode_tree + file_to_object + path work vs the MODIFY write).
   That measurement is a BLOCK_OWNER_DECISION (AR-1-4): TR0 records and REPORTS the
   split, then STOPS. The design does NOT itself authorize proceeding — an explicit
   owner go/no-go on the reported number gates TR1-TR5. No fixed pass/park threshold is
   applied; the earlier "compute >= 30%" is retained only as a non-binding engineering
   reference for that owner conversation, not as an autonomous gate.
```

DECISION: **B, staged behind a mandatory Stage-0 chunk=30000 SAT measurement whose
result is an owner go/no-go.** Fully specified below so implementation can start the
moment the owner approves Stage-0's reported split. The benefit is stated as a bounded
ESTIMATE (elimination of the measured ~4-5 s chunk=1000 traversal-compute residual,
scaled to the unchanged-subtree fraction), explicitly NOT a measured speed-up, until
Stage-0 exists. The design never proceeds past Stage-0 on its own authority.

--------------------------------------------------------------------------------
## 5. Target DDIC / index shape (strictly ADDITIVE; two new tables)
--------------------------------------------------------------------------------

No change to zaog_obj_index (no column change, no PK change, no secondary index).
Two new ORTEC transparent tables (client-dependent, buffering off, delivery class A,
same package as zaog_obj_index). Table names <= 16 chars (SAP limit): verified.

### 5.1 zaog_tree_map  (per-tree memo header)
```text
KEY  CLIENT          CLNT 3
KEY  REPO_KEY        CHAR 12   (same domain as zaog_obj_index-REPO_KEY / ty_repo_key)
KEY  DOT_SHA1        CHAR 40
KEY  DEVCLASS        CHAR 30   (DEVCLASS)
KEY  ALGO_VER        CHAR 40
KEY  TREE_SHA1       CHAR 40
KEY  TREE_PATH_HASH  CHAR 40
     CHILD_COUNT     INT4      (number of tree_child rows written for this tree)
     BUILT_AT        TIMESTAMP (DEC 15; GET TIME STAMP FIELD at memo write; AR-1-1 TTL gate)
     MAP_STATUS      CHAR 1    ('R' = memo complete; written LAST per tree)
```

### 5.2 zaog_tree_child  (per-tree resolved child rows)
```text
KEY  CLIENT          CLNT 3
KEY  REPO_KEY        CHAR 12
KEY  DOT_SHA1        CHAR 40
KEY  DEVCLASS        CHAR 30
KEY  ALGO_VER        CHAR 40
KEY  TREE_SHA1       CHAR 40
KEY  TREE_PATH_HASH  CHAR 40
KEY  CHILD_SEQ       INT4      (ordinal within the tree; preserves decode order; the
                               trailing key field enables CHILD_SEQ-range window paging
                               for large flat trees via ORDER BY PRIMARY KEY — AR-1-2)
     CHMOD           CHAR 6    (must match zcl_abapgit_git_pack=>ty_node-chmod width)
     CHILD_NAME      CHAR 255  (provably sufficient: DIR and FILE nodes >255 chars are
                               skipped upstream by the §3 guard, so no stored name is ever
                               truncated; MAP_SKIP='X' marks reproduce-CONTINUE FILE rows)
     CHILD_SHA1      CHAR 40   (child tree sha1 for DIR; blob sha1 for FILE)
     MAP_OBJ_TYPE    CHAR 4
     MAP_OBJ_NAME    CHAR 40
     MAP_SKIP        CHAR 1    ('X' = reproduce CONTINUE, i.e. no index row)
```

Additive-safety properties (all required by the prompt):
- No existing column/PK touched; existing zaog_obj_index rows remain valid and
  queryable by select_rows_for_filter unchanged.
- No backfill required for correctness: absence of a memo simply yields a MISS =>
  today's full walk. Old rows built before B are never a reuse SOURCE.
- Rollback = drop both tables (or stop reading them); behavior reverts exactly to
  today's full-walk-every-commit. Fully reversible. The NEW dedicated admin action
  zcl_abapgit_ortec_cache_admin=>clear_tree_memo removes both memo tables per repo_key
  on demand, UNDER the same repo mutex as rebuild_index (§11/TR5) — the AR-1-1/AR-1-5
  operator recovery action, serialized against any in-flight rebuild (AR-2-1). General
  clear_repo (E3, SAP_VALIDATED_COMPLETE) is NOT modified and does not clear the memo
  tables; leftover memo after a general clear_repo is safe (content-addressed, gated by
  dot_sha1/algo_ver, bounded by TTL) and removable via clear_tree_memo.

Schema-change checklist (persistence-schema skill):
```text
purpose      : memoize decode_tree + file_to_object per (context, tree, path) to skip
               re-computing unchanged subtrees across commits of the same repo.
change       : two additive transparent tables (above). No change to any zaog_* table.
migration    : none required; memo is lazily populated on first miss. TTL (BUILT_AT +
               c_tree_memo_max_age_secs) ages out stale memos automatically; the NEW
               dedicated clear_tree_memo admin action (mutex-based, AR-2-1) clears them
               immediately after an SP upgrade.
compat risk  : old code ignores the tables (full walk); new code finds no memo for old
               builds (miss => walk + memoize). Safe both directions.
performance  : O(BFS levels) bulk reads + per-large-tree CHILD_SEQ windows (AR-1-2);
               adds memo writes on first-seen trees; no per-row/per-object SQL. See §9.
rollback     : drop tables; no productive-row dependency exists on them.
```

--------------------------------------------------------------------------------
## 6. Source-completeness proof
--------------------------------------------------------------------------------

The reuse SOURCE is the content-addressed memo, NOT another commit's index. Two
independent guarantees make a trusted memo provably complete and correct:

1. Per-tree completeness marker. During a MISS, all of a tree's zaog_tree_child rows
   are written FIRST, then its zaog_tree_map header with CHILD_COUNT = rows-written
   and MAP_STATUS='R' is written LAST. A memo is eligible ONLY when its header
   MAP_STATUS='R'. This mirrors the proven $IDX/__READY__ marker discipline at
   tree granularity. A partial/interrupted memo never has a 'R' header.

2. Content immutability. By the Git Merkle guarantee, a tree_sha1's child set is
   fixed forever, so a completed memo for ( C, tree_sha1, tree_path_hash ) is
   correct for every future occurrence of that tree at that path under that context,
   SUBJECT TO the AR-1-1 boundary: a mapping-relevant mutable-SAP-state change can
   make a within-TTL memo replay stale mapping output. That window is bounded to
   <= c_tree_memo_max_age_secs (BUILT_AT gate, §3/§9) and clearable on demand via the
   E3 admin action. A completed memo can otherwise become invalid only by:
     - explicit DELETE (partial row loss) -> DETECTED by the child-count re-check
       (§8-C10): loaded child count != header CHILD_COUNT demotes the tree to MISS.
       This check proves CARDINALITY/deletion loss ONLY, not full rowset identity.
     - same-COUNT content corruption (out-of-band admin repair, or a hypothetical
       future writer bug overwriting child_seq rows without changing the count) ->
       classified as OUT-OF-SCOPE external DB corruption (§8-C12). It is NOT claimed
       to be detected by the count re-check; recovery is the dedicated mutex-based
       clear_tree_memo admin action (AR-1-1/AR-2-1), and it is additionally bounded to
       <= TTL by the max-age gate.

The design NEVER reads another commit's zaog_obj_index rows as a source, so it can
never trust a partial or RELAXED-mode index. is_index_ready STRICT is unchanged and
remains the sole readiness authority.

--------------------------------------------------------------------------------
## 7. Commit-specific target publication (no false-READY window)
--------------------------------------------------------------------------------

rebuild_index composition, inside the EXISTING repo lock, preserving delete-first /
accumulate / marker-last exactly (discovery §5):

```text
1. acquire_repo_lock(repo_key)                                         [existing]
2. double-check is_index_ready -> release+RETURN if now ready          [existing]
3. DELETE FROM zaog_obj_index WHERE repo_key AND commit_sha1           [existing]
   (clears partial INDEX rows only; tree_map/tree_child are NOT deleted — they are
    content-addressed, cross-commit, always safe to keep.)
4. compute C (dot_sha1, algo_ver) once.                                [new, O(1)]
5. BFS with reuse (per level):                                          [new]
     a. header bulk SELECT over the level's (tree_sha1,tree_path_hash) set, with
        map_status='R' AND built_at >= cutoff (TTL, AR-1-1); TTL-expired => MISS
     b. HITS split by CHILD_COUNT into SMALL (<= chunk) and LARGE (> chunk); children read
        in bounded batches/windows (§9, AR-1-2); verify loaded count = CHILD_COUNT per tree
        (else demote to MISS)
     c. HIT trees: reproduce index rows + enqueue DIR children (respecting lt_seen_trees and
        the §3/AR-1-3 DIR 255-guard)
     d. MISS trees: get_objects(bulk) -> decode_tree -> file_to_object -> build index rows
        + build memo rows (DIR 255-guard applied, AR-1-3); enqueue DIR children
     e. flush zaog_obj_index at c_index_write_chunk_size (existing constant, value untouched)
     f. flush zaog_tree_child at c_tree_child_chunk_size; write zaog_tree_map headers last
        (each header BUILT_AT stamped at write, AR-1-1)
6. final zaog_obj_index flush                                          [existing]
7. write $IDX/__READY__ marker LAST                                    [existing, unchanged]
8. release_repo_lock on EVERY exit path                                [existing]
```

The commit's index becomes visible as READY only at step 7, after ALL rows (reused +
freshly walked) are present — identical to today. Memo tables are invisible to
is_index_ready and to select_rows_for_filter, so no reuse step can create a partial or
false-READY window. Because there is no COMMIT WORK inside rebuild_index (§1), the
marker AND all memo writes share the caller's LUW and commit atomically; a mid-walk
crash leaves nothing durable, and the next ensure_index re-DELETEs and rebuilds
(existing C9 behavior), reusing whatever memo a PRIOR completed build durably left.

The ONLY deliberate deletion channel for the memo tables is the NEW dedicated admin
action `zcl_abapgit_ortec_cache_admin=>clear_tree_memo` (§11/TR5): it acquires the SAME
repo mutex rebuild_index uses (zcl_abapgit_ortec_pack_raw=>acquire_repo_lock) and
deletes ONLY zaog_tree_child/zaog_tree_map for the repo_key, so it is serialized against
any in-flight rebuild and never removes a rebuild's lock row or any active fetch session
(AR-2-1). rebuild_index never deletes the memo tables (they are content-addressed and
cross-commit). General clear_repo (E3) is NOT modified and does NOT clear the memo
tables. That dedicated admin channel, plus the mandatory TTL, is the bounded operator
recovery for AR-1-1 mutable-state drift and the C12 same-count external-corruption case.

--------------------------------------------------------------------------------
## 8. Concurrency / crash matrix
--------------------------------------------------------------------------------

```text
C1  same repo+tree+.abapgit+devclass+version   -> ELIGIBLE. All of C + tree_sha1 +
                                                  tree_path_hash + map_status='R' match
                                                  AND BUILT_AT is within the TTL
                                                  (BUILT_AT >= now - c_tree_memo_max_age_
                                                  secs). An otherwise-matching but expired
                                                  header is INELIGIBLE (see C11).
C2  same tree, different .abapgit               -> INELIGIBLE. dot_sha1 differs -> miss ->
                                                  full walk -> correct output.
C3  same tree, different devclass               -> INELIGIBLE. devclass differs -> miss.
C4  same tree, different repo                    -> INELIGIBLE. repo_key differs -> miss.
                                                  (Also blob_sha1s may not exist in the
                                                  other repo's store; cross-repo reuse is
                                                  never designed.)
C5  same context, irrelevant ordering            -> ELIGIBLE in the realistic same-source
    (filter order / ignore-list order)             case; SAFE always. The FILTER is not an
                                                  index input at all (rebuild_index has no
                                                  filter param; the filter is applied later
                                                  in select_rows_for_filter) -> filter order
                                                  can never affect a memo. A hand-reordered
                                                  ignore-list changes serialize bytes ->
                                                  dot_sha1 -> conservative miss (never a
                                                  wrong hit). Both directions preserve
                                                  correct output; the C5 intent (no wrong
                                                  result from irrelevant ordering) holds.
C6  source has no memo / no 'R' header           -> INELIGIBLE. Header SELECT misses ->
                                                  full walk + memoize. No error surfaced;
                                                  identical to today's behavior.
C7  memo built by older algorithm/schema         -> INELIGIBLE. algo_ver differs (ORTEC
                                                  constant bump and/or abapGit version) ->
                                                  miss -> full walk + fresh memo under the
                                                  new algo_ver. Old memo rows are inert.
                                                  (Mutable-SAP-state drift that does NOT move
                                                  algo_ver is instead bounded by C11's TTL and
                                                  C12's admin clear.)
C8  concurrent builds, same target commit        -> SERIALIZED by the EXISTING per-repo_key
                                                  acquire_repo_lock (held for the whole
                                                  rebuild). The new path runs INSIDE that
                                                  lock, so two builds for one repo_key can
                                                  never race on memo or index. No second
                                                  lock is introduced (proven sufficient:
                                                  all memo reads/writes and all index
                                                  reads/writes occur within the held lock).
                                                  The dedicated memo-clear admin action
                                                  (clear_tree_memo, TR5) acquires the SAME
                                                  LOCK_<repo_key> mutex, so admin clear is
                                                  serialized against rebuild by ONE primitive
                                                  (see C14, AR-2-1).
C9  crash during target-row copy/walk            -> Target never appears READY: the marker
                                                  is step 7, and the whole LUW is atomic
                                                  (no intra-build COMMIT). Retry re-DELETEs
                                                  index rows and rebuilds; durable memo from
                                                  prior COMPLETED builds is reused, making
                                                  the retry faster, never incorrect.
C10 memo rows deleted between eligibility check -> DETECTED (cardinality/deletion loss
    and copy                                        ONLY). After the child read for a hit,
                                                  verify the loaded child count = header
                                                  CHILD_COUNT; on mismatch (partial
                                                  deletion) demote that tree to MISS and
                                                  full-walk it (MODIFY overwrites any stale
                                                  memo rows). Concurrent mid-flight deletion
                                                  by another build is impossible under the
                                                  repo lock (C8); only out-of-band cache
                                                  admin can delete, and the count re-check
                                                  makes even that safe. This check proves
                                                  ROW-COUNT/deletion loss, NOT full rowset
                                                  identity — same-count content corruption is
                                                  C12, not C10 (AR-1-5).
C11 memo header older than the TTL               -> INELIGIBLE (AR-1-1). The header SELECT's
                                                  BUILT_AT >= cutoff predicate excludes it;
                                                  the tree is treated as a MISS -> full walk
                                                  -> fresh memo with a new BUILT_AT. Bounds
                                                  any undetected mutable-plugin-state drift
                                                  to <= c_tree_memo_max_age_secs (7 days) with
                                                  no operator action.
C12 same-COUNT content corruption of memo rows   -> OUT-OF-SCOPE external DB corruption
    (out-of-band admin repair / future writer      (AR-1-5). NOT claimed detected by the
    bug overwriting child_seq rows)                 count re-check. Recovery = the dedicated
                                                  mutex-based clear_tree_memo admin action
                                                  (§11/TR5), same action as C11's SP-upgrade
                                                  case; additionally self-heals within the
                                                  TTL. Accepted residual-risk boundary, same
                                                  shape as the accepted E4-OOB-DELETION-RISK
                                                  precedent — not an unbounded silent risk.
C14 admin memo-clear during an active rebuild    -> SERIALIZED + SAFE (AR-2-1). clear_tree_memo
                                                  acquires the SAME LOCK_<repo_key> mutex via
                                                  zcl_abapgit_ortec_pack_raw=>acquire_repo_lock.
                                                  If a rebuild holds it, clear's acquire INSERT
                                                  fails, retries with bounded backoff, and on
                                                  timeout RAISES (rejects) WITHOUT deleting any
                                                  memo/index/lock row. It only ever deletes
                                                  zaog_tree_child/zaog_tree_map for the repo_key
                                                  under its own held lock; it issues NO
                                                  `DELETE FROM zaog_fetch_sess WHERE repo_key`
                                                  and removes only its OWN LOCK_<repo_key> row
                                                  (session_id + status='L') via release_repo_lock,
                                                  so it can never delete a rebuild's mutex row or
                                                  an active fetch session (status 'A'). Single
                                                  lock, no nesting -> no deadlock. On the ERROR
                                                  path (AR-4-1) clear_tree_memo does an EXPLICIT
                                                  ROLLBACK WORK -> release_repo_lock( lv_lock_id )
                                                  -> COMMIT WORK AND WAIT, so its own mutex row is
                                                  durably removed even if some future intervening
                                                  commit had made the acquire INSERT durable (it
                                                  owns its own admin LUW; §11/TR5).
C13 oversized DIR component (name/path > 255)     -> SKIPPED at the DIR node (CONTINUE) in the
                                                  MISS walk, HIT reproduction, and current
                                                  path alike (AR-1-3). Never enqueued, never
                                                  memoized. Row-output-identical to today for
                                                  realistic repos (§3); memo never stores a
                                                  >255 name, so HIT reproduction stays byte-
                                                  exact.
```

--------------------------------------------------------------------------------
## 9. SQL and memory model (1,000 / 42,000 / 1,000,000 index rows)
--------------------------------------------------------------------------------

Exact statement shapes (no per-row SELECT, no per-object SQL). New bulk-batch
constants are SEPARATE from c_index_write_chunk_size (whose value stays 30000):

```text
c_tree_child_chunk_size   TYPE i VALUE 30000   (rows per zaog_tree_child MODIFY chunk AND
                                                the hard cap on child rows held in memory
                                                for ONE tree during a HIT read — AR-1-2)
c_tree_lookup_chunk_size  TYPE i VALUE 500     (max driver rows per FOR ALL ENTRIES pass)
c_tree_memo_max_age_secs  TYPE i VALUE 604800  (memo TTL = 7 days; AR-1-1 max-age gate)
```

Once per rebuild_index, compute the TTL cutoff (set-based, no per-row work):
```abap
GET TIME STAMP FIELD lv_now.                              " lv_now TYPE timestamp
lv_cutoff = cl_abap_tstmp=>subtractsecs(
              tstmp = lv_now
              secs  = c_tree_memo_max_age_secs ).         " lv_cutoff TYPE timestamp
```

Per BFS level (driver tables chunked to c_tree_lookup_chunk_size; non-empty guard
mandatory before every FAE):

Header eligibility incl. TTL gate (one FAE per driver chunk):
```abap
SELECT tree_sha1 tree_path_hash child_count built_at
  FROM zaog_tree_map
  FOR ALL ENTRIES IN lt_level_keys
  WHERE repo_key       = iv_repo_key
    AND dot_sha1       = lv_dot_sha1
    AND devclass       = iv_devclass
    AND algo_ver       = lv_algo_ver
    AND tree_sha1      = lt_level_keys-tree_sha1
    AND tree_path_hash = lt_level_keys-tree_path_hash
    AND map_status     = c_status_ready
    AND built_at       >= lv_cutoff
  INTO TABLE @lt_hdr.
```
A header excluded by `built_at >= lv_cutoff` (TTL-expired) simply does not appear in
lt_hdr and its tree is therefore a MISS (C11) — full re-walk + fresh memo.

Child read for hits — AR-1-2 bounded streaming (NEVER one unbounded SELECT). Hits are
partitioned by header CHILD_COUNT into SMALL (<= c_tree_child_chunk_size) and LARGE
(> c_tree_child_chunk_size). No path ever materializes more than one
c_tree_child_chunk_size window of child rows before those rows are reproduced into
zaog_obj_index and discarded (AR-2-2: strict single bound, no ~2x case).

SMALL trees — FAE over a batch of hit keys, closed by PEEK-then-decide (AR-2-2): CLOSE
the current batch and issue its FAE BEFORE adding any tree whose CHILD_COUNT would push
the batch's running sum of CHILD_COUNT OVER c_tree_child_chunk_size (that tree is
deferred to the NEXT batch), OR when lines(batch) reaches c_tree_lookup_chunk_size
(whichever first). Since every SMALL tree has CHILD_COUNT <= c_tree_child_chunk_size, an
empty batch always accepts the next tree, so no tree is ever stuck. Each FAE therefore
returns AT MOST c_tree_child_chunk_size child rows (strict; never ~2x), is
reproduced+flushed, then cleared:
```abap
SELECT * FROM zaog_tree_child
  FOR ALL ENTRIES IN lt_small_batch
  WHERE repo_key       = iv_repo_key
    AND dot_sha1       = lv_dot_sha1
    AND devclass       = iv_devclass
    AND algo_ver       = lv_algo_ver
    AND tree_sha1      = lt_small_batch-tree_sha1
    AND tree_path_hash = lt_small_batch-tree_path_hash
  INTO TABLE @lt_child.
```
LARGE trees — read ALONE, one CHILD_SEQ window at a time, ORDER BY PRIMARY KEY (the
window bound uses the trailing key field CHILD_SEQ), reproduced+flushed per window,
advancing lv_from by c_tree_child_chunk_size until a short page (rows < chunk) or the
cumulative reproduced count reaches CHILD_COUNT. At most c_tree_child_chunk_size child
rows are resident at any instant:
```abap
SELECT * FROM zaog_tree_child
  WHERE repo_key       = iv_repo_key
    AND dot_sha1       = lv_dot_sha1
    AND devclass       = iv_devclass
    AND algo_ver       = lv_algo_ver
    AND tree_sha1      = lv_hit_tree_sha1
    AND tree_path_hash = lv_hit_path_hash
    AND child_seq      BETWEEN lv_from AND lv_from + c_tree_child_chunk_size - 1
  ORDER BY PRIMARY KEY
  INTO TABLE @lt_child_page.
```
Integrity re-check (C10, AR-1-5): the SUM of loaded child rows across a hit tree's
pages must equal header CHILD_COUNT; a shortfall (partial deletion) demotes that tree
to MISS and full-walks it. This proves cardinality/deletion loss ONLY; same-count
content corruption is out-of-scope C12.
Miss fetch: existing zcl_abapgit_ortec_obj_store=>get_objects( iv_bulk_fetch=abap_true )
over the level's MISS tree_sha1 set (unchanged bulk API).
Memo write (misses), chunked:
```abap
MODIFY zaog_tree_child FROM TABLE lt_new_child.   " flush at c_tree_child_chunk_size
MODIFY zaog_tree_map   FROM TABLE lt_new_hdr.     " headers, written after their children
```
Index write: existing MODIFY zaog_obj_index FROM TABLE lt_rows (chunk 30000, unchanged).

SQL-call complexity: O(BFS depth) x constant statements for the SMALL-tree path
(1 header FAE + N small-child FAEs where N = ceil(level child rows / chunk), plus
1 get_objects + index/memo MODIFYs per level), NOT O(nodes). The LARGE-tree path adds
O(CHILD_COUNT / c_tree_child_chunk_size) window SELECTs for each oversized flat tree —
bounded by total rows / chunk, never per-node. BFS depth for a real repo ~5-15 levels.
HTTP-call complexity: ZERO new HTTP. get_objects may negotiate-fetch on miss exactly as
today; blob payload fetch stays in build_files_from_rows (untouched). No blob payload is
ever read during eligibility/copy — index/memo metadata rows only.

```text
Scale       zaog_obj_index rows   zaog_tree_map/child (one commit)   peak in-memory
1,000       ~1,000                ~tens hdr / ~1,000 child           << 1 MB
42,000      ~42,000               ~340 hdr / ~42,340 child           lt_rows <=30000x730B
                                    (deduped across commits)          ~21.9MB + one child
                                                                      window (<= chunk rows)
1,000,000   ~1,000,000            ~thousands hdr / ~1,000,000 child  bounded by the 30000-row
            (worst: one huge         (deduped across commits;         index chunk (~21.9MB) +
             commit, or ONE flat     shared subtrees stored once)     <= c_tree_child_chunk_
             dir of 1,000,000                                         size child rows for the
             files)                                                   ONE tree being read
                                                                      (LARGE path: strict
                                                                      CHILD_SEQ windows;
                                                                      SMALL path: <= chunk
                                                                      per FAE batch, peek-
                                                                      then-decide). NO single
                                                                      SELECT returns a whole
                                                                      flat tree's children.
```
Peak XSTRING/payload: none new — memo rows are fixed-width CHAR/INT metadata, never
blob payloads. Cache scope: dot_sha1 and algo_ver are computed ONCE per rebuild_index
call and held in method-local variables; no cross-call in-memory cache is added.
Transaction owner: for the rebuild_index path (this §9 SQL/memory model, i.e. TR1-TR4)
unchanged — the caller's existing LUW; NO new COMMIT WORK. The standalone TR5 admin action
clear_tree_memo owns its own LUW and commits (§12 carve-out); it is not part of this model.

--------------------------------------------------------------------------------
## 10. Class-local ABAP Unit tests (ltcl_obj_index; names verified <= 30 chars)
--------------------------------------------------------------------------------

Reuse the existing fixture conventions (mc_repo, manual encode_tree/encode_commit +
sha1_* + store_object, build_default dot, zcl_abapgit_object_filter_obj, setup/teardown
DELETE+ROLLBACK). Add LOCAL FRIENDS only if a test must seed/inspect a memo directly.

```text
reuse_output_identical      (22) positive: build commit A (memoizes), then build commit B
                                 that SHARES A's /src/ subtree tree_sha1 at the same path
                                 but changes one leaf elsewhere; assert B's resolved files
                                 == a control full-walk build's files, row-for-row.
reuse_hit_same_context      (22) positive: rebuild the SAME commit after wiping only its
                                 zaog_obj_index rows (keep memo); assert identical output
                                 and that no full decode was needed (proxy: memo header
                                 present + rows correct).
memo_written_on_miss        (20) after a first fresh build, assert zaog_tree_map header
                                 (map_status='R', child_count>0) and zaog_tree_child rows
                                 exist for the built trees under the correct C key.
reuse_miss_other_dot        (20) C2: build A with dot1; build B (same tree) with a dot
                                 whose get_signature differs; assert memo NOT reused
                                 (fresh walk) and output correct.
reuse_miss_other_devclass   (25) C3: differing iv_devclass -> miss -> correct output.
reuse_miss_other_repo       (21) C4: same tree under a second repo_key -> miss; assert no
                                 cross-repo blob/row leakage.
reuse_miss_no_memo          (18) C6: build B with an empty memo table -> full walk ->
                                 correct output + marker written.
reuse_miss_old_algo         (19) C7: seed a memo with a different ALGO_VER -> miss ->
                                 fresh walk + a new memo under the current ALGO_VER.
reuse_git_mv_path_miss      (23) IN-6: same tree_sha1 at a DIFFERENT tree_path_hash ->
                                 miss (path in key) -> correct output at the new path.
reuse_crash_partial_safe    (24) C9: seed header map_status='R' child_count=N but only
                                 N-1 child rows -> integrity mismatch -> demote to MISS ->
                                 full walk -> correct output + marker.
reuse_memo_deleted_midway   (25) C10: build A (memo present); delete its zaog_tree_child
                                 rows keeping the header; build B -> loaded child count !=
                                 header CHILD_COUNT (cardinality loss) -> demote to MISS ->
                                 full walk -> correct output. Proves deletion/count loss
                                 ONLY; same-count corruption is C12 (out of scope, below).
reuse_miss_memo_aged_out    (24) C11/AR-1-1: seed a matching header with BUILT_AT older than
                                 c_tree_memo_max_age_secs -> TTL cutoff excludes it -> MISS ->
                                 fresh walk + a new memo whose BUILT_AT is current.
reuse_flat_dir_paged        (20) AR-1-2: build a single flat DIR with > c_tree_child_chunk_
                                 size files (use a small test-only chunk override), warm the
                                 memo, then rebuild; assert output byte-identical to a full
                                 walk AND that reproduction proceeds in bounded pages (proxy:
                                 no single loaded child table exceeds the chunk bound).
reuse_small_batch_bound     (23) AR-2-2: with a small test-only chunk N, warm the memo for two
                                 SMALL sibling trees of (N-1) children each (straddle case);
                                 rebuild and assert peek-then-decide splits them across TWO
                                 FAE batches so NO single read_small_children result exceeds N
                                 rows, and output stays byte-identical to a full walk.
reuse_dir_overlen_skip      (22) AR-1-3/C13: construct a tree with a DIR child name > 255
                                 chars containing a descendant file; assert the subtree is
                                 skipped identically to a control full walk (zero rows from
                                 it) and no memo row with a >255 CHILD_NAME is written.
reuse_marker_still_last     (23) readiness invariant: assert is_index_ready is false until
                                 the $IDX/__READY__ marker exists, regardless of memo state;
                                 memo presence never makes a commit READY on its own.
```

The three TR5 admin tests (clear_memo_removes_rows (23), clear_memo_rejects_if_locked (28),
clear_memo_err_releases_lock (28)) live in the cache_admin class's OWN test class, not
ltcl_obj_index, because clear_tree_memo is a cache_admin method (AR-2-1). They replace the
cycle-2 clear_repo_removes_tree_memo test (clear_repo is no longer modified).
clear_memo_removes_rows additionally asserts (AR-3-1) that after a successful clear NO
zaog_fetch_sess row with session_id = |LOCK_<repo_key>| AND status='L' remains, proving the
captured lock id was released and no mutex row leaked; clear_memo_rejects_if_locked proves a
pre-existing LOCK_<repo_key> status='L' row survives a reject.
clear_memo_err_releases_lock (AR-4-1) forces a DB error after a successful acquire and
asserts that after the raised zcx_abapgit_ortec_git NO zaog_fetch_sess row with
session_id = |LOCK_<repo_key>| AND status='L' remains AND a subsequent acquire_repo_lock for
the same repo_key succeeds immediately, proving the explicit ROLLBACK WORK ->
release_repo_lock( lv_lock_id ) -> COMMIT WORK AND WAIT cleanup released the mutex durably
(see §11/TR5 TESTS for the exact assertions).

Local-coverage limitation (documented, same posture as the existing 30000-chunk note):
the per-level bulk-reuse path is exercised with small fixtures (a few trees); true
42,000-row reuse timing is validated only by the Stage-0/Stage-4 live SAT trace, not by
DURATION SHORT unit tests.

--------------------------------------------------------------------------------
## 11. Implementation packets for weak models (checkpoint slices)
--------------------------------------------------------------------------------

Each slice is independently checkpointable and decision-free. Stage-0 (the chunk=30000
SAT trace) is a PREREQUISITE MEASUREMENT whose result is a BLOCK_OWNER_DECISION: TR0
measures and REPORTS the write:compute split and STOPS. TR1-TR5 are authorized only by a
separate, explicit owner go/no-go on that reported number — the design does NOT
autonomously proceed or park past Stage-0, and no invented threshold gates it.

### TR0 (gate, no code) — Stage-0 measurement (BLOCK_OWNER_DECISION)
```text
FILE_OR_OBJECT=(measurement only; produce a new incident note)
ACTION=measure
CHANGE=Reproduce the O4H-8794 warm-to-cold scenario at the LIVE chunk=30000; capture a
       SAT trace; record write side (MODIFY zaog_obj_index + DB:Exec) vs compute
       (DECODE_TREE + file_to_object/path_to_package + LOOP AT LT_NODES self) split, in
       absolute microseconds AND percentages.
VALIDATION=write:compute split recorded with absolute microseconds and percentages, and
       reported to the owner as a fact.
STOP_IF=ALWAYS stop after reporting — this is a BLOCK_OWNER_DECISION. Do NOT decide
       proceed/park autonomously and do NOT apply any fixed threshold. TR1-TR5 start only
       on an explicit owner go decision referencing the reported split. (The prior draft's
       "compute >= 30%" is a non-binding engineering reference only, not a gate.)
```

### TR1 — algo fingerprint + canonical context helper + TTL cutoff + tests
```text
FILE_OR_OBJECT=src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap
METHOD_OR_DDIC=new private CONSTANTS c_index_algo_version TYPE i VALUE 1,
  c_tree_child_chunk_size TYPE i VALUE 30000, c_tree_lookup_chunk_size TYPE i VALUE 500,
  c_tree_memo_max_age_secs TYPE i VALUE 604800; new private CLASS-METHODS build_context
  IMPORTING io_dot iv_devclass RETURNING VALUE(rs_ctx) TYPE (new local type ty_ctx:
  dot_sha1 CHAR40, algo_ver CHAR40); new private CLASS-METHODS memo_cutoff RETURNING
  VALUE(rv_cutoff) TYPE timestamp.
ANCHOR=the existing private CONSTANTS block (c_status_ready .. c_index_write_chunk_size).
ACTION=insert
CHANGE=
  build_context: rs_ctx-dot_sha1 = io_dot->get_signature( )-sha1.
                 rs_ctx-algo_ver = zcl_abapgit_hash=>sha1_string(
                   |{ c_index_algo_version }|{ zif_abapgit_version=>c_abap_version }| ).
  memo_cutoff:   GET TIME STAMP FIELD DATA(lv_now).   " lv_now TYPE timestamp
                 rv_cutoff = cl_abap_tstmp=>subtractsecs(
                               tstmp = lv_now secs = c_tree_memo_max_age_secs ).
  (repo_key + devclass remain method parameters; do NOT fold them into ty_ctx.)
INVARIANTS=IN-3,IN-4,IN-7; identity per §3; AR-1-1 TTL gate.
SQL_SHAPE=NONE.
ERROR_ROLLBACK_FALLBACK=get_signature raises zcx_abapgit_exception -> propagate; caller's
  existing CATCH in rebuild_index releases the lock and re-raises (no partial state).
TESTS=a helper test asserting build_context is deterministic and that a changed dot
  yields a changed dot_sha1 (feeds reuse_miss_other_dot); a memo_cutoff test asserting the
  returned cutoff is c_tree_memo_max_age_secs seconds before now (feeds reuse_miss_memo_
  aged_out).
VALIDATION=abaplint clean; get_errors clean; syntax dry-run on the real system (confirm
  cl_abap_tstmp=>subtractsecs is available and returns timestamp).
STOP_IF=get_signature is not accessible from this class scope (it is public — must be), or
  cl_abap_tstmp=>subtractsecs is unavailable on the target release.
```

### TR2 — DDIC tables + memo persistence helpers + tests
```text
FILE_OR_OBJECT=src/ortec/git/zaog_tree_map.tabl.xml  and  zaog_tree_child.tabl.xml (new)
METHOD_OR_DDIC=create both transparent tables exactly per §5.1/§5.2 (keys, types, delivery
  class A, buffering off, client-dependent). zaog_tree_map INCLUDES a BUILT_AT TIMESTAMP
  (DEC 15) non-key field (AR-1-1 TTL). CHMOD length MUST equal
  zcl_abapgit_git_pack=>ty_node-chmod (CHAR6 — verify against zif_abapgit_git_definitions).
ANCHOR=(new files; follow the zaog_obj_index.tabl.xml structure verbatim for DD02V/DD09L).
ACTION=insert
CHANGE=+ new private CLASS-METHODS on zcl_abapgit_ortec_obj_index:
  read_tree_headers    IMPORTING iv_repo_key iv_devclass is_ctx iv_cutoff it_level_keys
    (tree_sha1, tree_path_hash) EXPORTING et_hdr (tree_sha1, tree_path_hash, child_count).
    ONE header FAE per driver chunk (c_tree_lookup_chunk_size), non-empty guard, INTO last,
    WITH the `built_at >= iv_cutoff` and `map_status = c_status_ready` predicates (§9). The
    caller partitions et_hdr into SMALL (child_count <= c_tree_child_chunk_size) and LARGE.
  read_small_children  IMPORTING iv_repo_key iv_devclass is_ctx it_small_batch EXPORTING
    et_child. ONE FAE over a batch of small-hit keys; the CALLER closes each batch by
    PEEK-then-decide (§9, AR-2-2): close BEFORE adding a tree whose child_count would push
    the running sum OVER c_tree_child_chunk_size, OR when lines = c_tree_lookup_chunk_size
    (whichever first) -> each FAE returns <= c_tree_child_chunk_size rows (strict).
    Reproduced+flushed then cleared before the next batch.
  read_large_child_page IMPORTING iv_repo_key iv_devclass is_ctx is_hit_key iv_seq_from
    EXPORTING et_child_page. ONE CHILD_SEQ-window SELECT
    (child_seq BETWEEN iv_seq_from AND iv_seq_from + c_tree_child_chunk_size - 1,
    ORDER BY PRIMARY KEY). Caller loops advancing iv_seq_from until a short page; never more
    than c_tree_child_chunk_size rows resident (§9, AR-1-2).
  write_tree_memo      IMPORTING iv_repo_key iv_devclass is_ctx it_child it_hdr. Set each
    header's BUILT_AT via GET TIME STAMP FIELD before write. MODIFY zaog_tree_child FROM
    TABLE chunked at c_tree_child_chunk_size, THEN MODIFY zaog_tree_map FROM TABLE for
    headers (children-before-header per §6).
INVARIANTS=§6 (child rows before header, BUILT_AT stamped at write), §9 SQL shapes,
  additive-only §5; AR-1-2 (no unbounded child read); AR-1-1 (TTL cutoff predicate).
SQL_SHAPE=exactly the header FAE, small-child FAE, large-child window SELECT, and two
  MODIFYs quoted in §9. No per-row/singleton SQL.
ERROR_ROLLBACK_FALLBACK=any DB error -> propagate zcx; rebuild_index CATCH releases lock,
  re-raises; no COMMIT so nothing is durable on failure.
TESTS=memo_written_on_miss (assert BUILT_AT populated, map_status='R', child_count>0);
  a persistence round-trip test (write then read returns the identical child list in
  child_seq order); an integrity test (child_count mismatch is reported to the caller);
  reuse_miss_memo_aged_out (TTL cutoff excludes an old header); reuse_flat_dir_paged (large
  tree read in bounded CHILD_SEQ windows).
VALIDATION=tables activate; abaplint/get_errors clean; FAE INTO-last AND the BETWEEN-window
  + ORDER BY PRIMARY KEY verified by real-system syntax dry-run (local tooling misses the
  clause-order error).
STOP_IF=table name > 16 chars (both are within limit) or CHMOD width mismatch vs ty_node,
  or BUILT_AT TIMESTAMP type not accepted by GET TIME STAMP FIELD / the cutoff predicate.
```

### TR3 — integrate per-tree reuse into rebuild_index BFS + full test matrix
```text
FILE_OR_OBJECT=src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap
METHOD_OR_DDIC=rebuild_index (WHILE lt_pending BFS body).
ANCHOR=the WHILE lt_pending IS NOT INITIAL ... ENDWHILE block, specifically the
  "bulk get_objects for lt_tree_sha1s" fetch and the per-node CASE loop.
ACTION=replace (the per-level body only; keep delete-first, final flush, marker-last,
  lock handling, lt_seen_trees semantics byte-for-byte).
CHANGE=Per level:
  1. compute is_ctx once (TR1 build_context) AND lv_cutoff once (TR1 memo_cutoff) before
     the WHILE (hoist both).
  2. build lt_level_keys = { (tree_sha1, tree_path_hash=sha1_string(path)) } for lt_pending.
  3. read_tree_headers( iv_cutoff = lv_cutoff ... ) -> et_hdr. Partition lt_pending into
     MISS (no matching header) and HIT; split HIT by header child_count into HIT_SMALL
     (<= c_tree_child_chunk_size) and HIT_LARGE (> c_tree_child_chunk_size). The cutoff
     predicate makes TTL-expired headers behave as MISS automatically (C11).
  4. HIT reproduction (bounded, AR-1-2) — for each child in child_seq order reproduce
     today's CASE exactly, tracking a per-tree loaded-count for the C10 integrity check:
       HIT_SMALL: read_small_children over caller-closed batches (§9); reproduce+flush
                  each batch, then CLEAR before the next batch.
       HIT_LARGE: read_large_child_page in CHILD_SEQ windows (§9); reproduce+flush each
                  window, advancing seq_from until a short/exhausted page.
     Per stored child:
       DIR  -> CONCATENATE path name '/'; APPLY THE §3 DIR GUARD (if strlen(path)>255 OR
               strlen(name)>255 CONTINUE — but such a row can never exist in the memo, since
               MISS-walk never stored it); else if child_sha1 not in lt_seen_trees: INSERT +
               APPEND (tree_sha1=child_sha1, path=next) TO lt_next.
       FILE (chmod file/exec/symlink) with MAP_SKIP='' -> build ls_row using memo fields +
               path_hash = sha1_string( |{ path }{ child_name }| ); APPEND; chunk-flush at
               c_index_write_chunk_size.
       MAP_SKIP='X' or other chmod -> CONTINUE.
     After a tree's pages are exhausted, assert loaded-count = header child_count; on
     shortfall demote to MISS and full-walk it (C10).
  5. MISS trees: get_objects(bulk) over MISS tree_sha1s -> decode_tree -> per-node CASE
     building index rows AND accumulating memo child rows. **Add the §3 DIR guard to the
     WHEN c_chmod-dir branch** (currently ONLY the FILE branch is length-guarded) — a tiny,
     justified, in-scope change (AR-1-3): if strlen(path)>255 OR strlen(name)>255 for a DIR,
     CONTINUE (do not enqueue, do not store a memo child row). FILE handling unchanged; memo
     child rows record chmod, name, child_sha1, mapped obj_type/obj_name, and MAP_SKIP='X'
     for the throw/empty/(FILE-)>255 cases so reproduction is byte-exact. child_seq is the
     node ordinal. After a tree's nodes are processed, stage its header (child_count=rows-
     staged, map_status='R'; BUILT_AT stamped in write_tree_memo).
  6. write_tree_memo for the level's MISS trees (TR2) as part of the same LUW.
INVARIANTS=§3 row identity; §7 marker-last; §8 all rows; lt_seen_trees dedup preserved
  (dir dedup by tree_sha1 alone, existing behavior); §3/AR-1-3 DIR 255-guard in BOTH the
  MISS walk and HIT reproduction; AR-1-2 bounded per-tree child residency; no COMMIT WORK.
SQL_SHAPE=§9 (read_tree_headers, read_small_children, read_large_child_page, write_tree_memo,
  existing get_objects + MODIFY zaog_obj_index).
ERROR_ROLLBACK_FALLBACK=on any decode/store/hash error -> existing raise_with_text; existing
  CATCH releases lock + re-raises; marker not written => not READY => retry rebuilds.
TESTS=reuse_output_identical, reuse_hit_same_context, reuse_git_mv_path_miss,
  reuse_crash_partial_safe, reuse_memo_deleted_midway, reuse_marker_still_last,
  reuse_miss_memo_aged_out, reuse_flat_dir_paged, reuse_dir_overlen_skip, plus the
  C2/C3/C4/C6/C7 negatives.
VALIDATION=full ltcl_obj_index green; abaplint/get_errors clean; real-system syntax dry-run;
  confirm existing E1-T tests (index_no_cross_commit_leak, ready_*_commit,
  index_bulk_rows_preserved, index_chunk_boundary_ok) still pass unchanged.
STOP_IF=any existing E1-T test changes behavior, or reuse_output_identical is not byte-exact
  vs the control full walk.
```

### TR5 — dedicated mutex-based memo clear admin action (AR-1-1 / AR-2-1 / AR-3-1 / AR-4-1 / C12 / C14 recovery)
```text
FILE_OR_OBJECT=src/ortec/git/zcl_abapgit_ortec_cache_admin.clas.abap
METHOD_OR_DDIC=NEW public instance/class method clear_tree_memo + NEW result struct
  ty_memo_clear_result + NEW formatter format_memo_clear. The EXISTING clear_repo,
  ty_clear_result and format_clear_result are NOT touched (AR-2-1: clear_repo uses a
  DIVERGENT primitive — ENQUEUE_EZAOG_REPO_LOCK + `DELETE FROM zaog_fetch_sess WHERE
  repo_key` — so the memo clear must NOT be routed through it).
ANCHOR=(new method; place beside clear_repo; reuse its ty_repo_key type
  zcl_abapgit_ortec_repo_state=>ty_repo_key (CHAR12), which is call-compatible with
  zcl_abapgit_ortec_pack_raw=>acquire_repo_lock's IMPORTING ty_repo_key TYPE c LENGTH 12).
ACTION=insert
CHANGE=
  + TYPES ty_memo_clear_result: repo_key TYPE ...ty_repo_key, tree_child_rows TYPE i,
    tree_map_rows TYPE i.
  + METHODS clear_tree_memo IMPORTING iv_repo_key TYPE ...ty_repo_key
      RETURNING VALUE(rs_result) TYPE ty_memo_clear_result
      RAISING zcx_abapgit_ortec_git.
    Body (exact sequence):
      0. DATA lv_lock_id TYPE zcl_abapgit_ortec_pack_raw=>ty_session_id.  "AR-3-1: capture
         the mutex id returned by acquire_repo_lock, EXACTLY as rebuild_index declares it
         (verified src line ~298). acquire_repo_lock RETURNs rv_lock_id = |LOCK_{ repo_key }|
         and release_repo_lock DELETEs WHERE session_id = iv_lock_id AND status='L', so the
         BARE iv_repo_key must NEVER be passed to release_repo_lock — only lv_lock_id."
      1. TRY.
           lv_lock_id = zcl_abapgit_ortec_pack_raw=>acquire_repo_lock(
                          iv_repo_key = iv_repo_key ).  "SAME mutex as rebuild_index;
             retries w/ bounded backoff, raises zcx_abapgit_exception on timeout (busy).
             The returned lv_lock_id (= |LOCK_{ iv_repo_key }|) is captured for release."
         CATCH zcx_abapgit_exception INTO DATA(lx_busy).
           "acquire RAISED -> no mutex row was inserted -> nothing to release/rollback."
           RAISE EXCEPTION TYPE zcx_abapgit_ortec_git EXPORTING iv_text =
             |Tree memo clear rejected: repository { iv_repo_key } is busy|
             ... previous = lx_busy. "WAIT-then-REJECT; no memo/index/lock row deleted."
         ENDTRY.
      2. TRY.  "under the held mutex — delete ONLY the two memo tables, children first"
           DELETE FROM zaog_tree_child WHERE repo_key = iv_repo_key.
           rs_result-tree_child_rows = sy-dbcnt.
           DELETE FROM zaog_tree_map   WHERE repo_key = iv_repo_key.
           rs_result-tree_map_rows   = sy-dbcnt.
           rs_result-repo_key = iv_repo_key.
           zcl_abapgit_ortec_pack_raw=>release_repo_lock( lv_lock_id ). "AR-3-1: pass the
             CAPTURED lock id, NOT iv_repo_key. Deletes ONLY the own LOCK_<repo_key> row
             (session_id = lv_lock_id AND status='L'); NO repo_key-wide delete."
           COMMIT WORK AND WAIT.  "one LUW: both memo deletes + own mutex release."
         CATCH cx_root INTO DATA(lx_err).
           ROLLBACK WORK.  "AR-4-1: discard the uncommitted memo-table DELETEs."
           zcl_abapgit_ortec_pack_raw=>release_repo_lock( lv_lock_id ). "AR-4-1: EXPLICIT,
             UNCONDITIONAL captured-id cleanup — deletes the own LOCK_<repo_key> row whether
             or not any hypothetical intervening commit had made the acquire INSERT durable.
             clear_tree_memo OWNS its LUW, so it does NOT rely on rollback semantics to undo
             the acquire (that would be unsafe if a future helper/wrapper commits between
             acquire and this CATCH). Pass the CAPTURED lv_lock_id, NEVER the bare iv_repo_key."
           COMMIT WORK AND WAIT.  "AR-4-1: make the release durable — the error-path counterpart
             of the success-path commit; clear_tree_memo is its own admin LUW owner."
           RAISE EXCEPTION TYPE zcx_abapgit_ortec_git EXPORTING iv_text =
             |Tree memo clear failed for { iv_repo_key }| ... previous = lx_err.
         ENDTRY.
  + format_memo_clear( is_result ) returning a short human string (repo_key +
    tree_map_rows + tree_child_rows), same style as format_clear_result.
INVARIANTS=(AR-3-1/AR-4-1) release_repo_lock MUST be passed the CAPTURED lv_lock_id
  (= |LOCK_{ repo_key }|), NEVER the bare iv_repo_key. On the success path release runs
  before COMMIT WORK AND WAIT. On the error path the cleanup is EXPLICIT and UNCONDITIONAL:
  ROLLBACK WORK (discard the uncommitted memo DELETEs) -> release_repo_lock( lv_lock_id )
  (delete the own mutex row, durable even if a hypothetical intervening commit had made the
  acquire INSERT durable) -> COMMIT WORK AND WAIT (make the release durable). This is a
  DELIBERATE, JUSTIFIED difference from rebuild_index's rollback-free / release-only pattern:
  clear_tree_memo OWNS its own admin LUW and already commits on success, so it also commits
  its own cleanup on error — a STRONGER guarantee than relying on rollback semantics to undo
  the acquire. Do NOT re-add any text claiming the error path "matches rebuild_index's exact
  pattern" or forbidding a post-rollback release. The busy/reject path never acquired the
  mutex so it neither releases nor commits.
  (AR-2-1) NEVER issue `DELETE FROM zaog_fetch_sess WHERE repo_key = ...`; the ONLY
  zaog_fetch_sess row touched is this call's OWN LOCK_<repo_key> mutex via acquire/release;
  serialized against rebuild_index by the SINGLE shared LOCK_<repo_key> mutex (no second
  primitive, no nesting -> no deadlock, C14); general clear_repo untouched; additive-only;
  memo delete is children-before-header order.
SQL_SHAPE=exactly the two set-based `DELETE ... WHERE repo_key` above + the acquire/release
  row-mutex SQL inside pack_raw; no per-row SQL.
ERROR_ROLLBACK_FALLBACK=busy -> WAIT-then-REJECT (raise, nothing deleted, mutex never
  acquired); any DB error after a successful acquire -> ROLLBACK WORK (discard the
  uncommitted memo DELETEs) -> release_repo_lock( lv_lock_id ) (explicit, durable mutex
  cleanup) -> COMMIT WORK AND WAIT -> re-raise as zcx_abapgit_ortec_git (AR-4-1).
TESTS=clear_memo_removes_rows (seed both memo tables for a repo_key, call clear_tree_memo,
  assert zero rows remain, counters report seeded counts, an unrelated repo_key untouched,
  and — AR-3-1 after-success proof — SELECT SINGLE from zaog_fetch_sess WHERE
  session_id = |LOCK_{ iv_repo_key }| AND status='L' returns NO row, proving the captured
  lock id was correctly released and no mutex row leaked);
  clear_memo_rejects_if_locked (pre-insert a LOCK_<repo_key> mutex row with status='L' to
  simulate an active rebuild, assert clear_tree_memo raises zcx_abapgit_ortec_git AND both
  memo tables' rows survive AND the pre-existing session_id=|LOCK_<repo_key>| status='L'
  mutex row is still present after the reject);
  clear_memo_err_releases_lock (AR-4-1: force a DB error on the memo DELETE AFTER a
  successful acquire — e.g. via a test seam / injected failure — assert clear_tree_memo
  raises zcx_abapgit_ortec_git, that after the raise SELECT SINGLE from zaog_fetch_sess
  WHERE session_id = |LOCK_{ iv_repo_key }| AND status='L' returns NO row, and that a
  subsequent acquire_repo_lock( iv_repo_key ) succeeds immediately — proving the explicit
  ROLLBACK WORK -> release_repo_lock( lv_lock_id ) -> COMMIT WORK AND WAIT cleanup released
  the mutex durably with no leak). All three live in the cache_admin test class.
VALIDATION=abaplint/get_errors clean; real-system syntax dry-run; existing cache-admin tests
  still green; confirm iv_repo_key type is call-compatible with acquire_repo_lock (both
  CHAR12).
STOP_IF=zcl_abapgit_ortec_pack_raw=>acquire_repo_lock/release_repo_lock are not visible from
  cache_admin's scope (both are public class-methods — must be), or the repo_key types are
  incompatible (both CHAR12 — compatible).
```

### TR4 — integration / migration / concurrency / Stage-0-close SAT validation
```text
FILE_OR_OBJECT=(live IT8 validation + a new measurement note)
METHOD_OR_DDIC=n/a
ACTION=validate
CHANGE=Warm-to-cold reproduction at chunk=30000 WITH reuse enabled (memo cold, then warm):
  run 1 (cold memo) = full walk + memoize; run 2 (warm memo, a sibling commit) = high-hit
  reuse. Capture SAT for both; record compute delta vs the TR0 baseline.
INVARIANTS=§9 SQL-call = O(levels); no new HTTP; no COMMIT; ATC + ABAP Unit PASS.
SQL_SHAPE=observed only (assert no per-row/singleton SQL appears in the trace).
ERROR_ROLLBACK_FALLBACK=if reuse shows any output divergence vs full walk in the live run,
  revert by dropping the two tables (behavior returns to today's full walk).
TESTS=live ATC PASS, ABAP Unit PASS; SAT compute reduction recorded as MEASURED.
VALIDATION=run-2 compute (decode+map) measurably below TR0 baseline; index output identical
  (row counts + spot files) to a control full-walk build of the same commit.
STOP_IF=warm-run compute reduction is not material vs TR0 -> record and park B; do not
  claim a speed-up.
```

--------------------------------------------------------------------------------
## 12. Mandatory template sections
--------------------------------------------------------------------------------

Expected production cardinality: per rebuild, ~10^3-10^6 index rows and O(distinct
trees) memo headers; a real repo ~42,000 files / ~340 trees / BFS depth ~5-15.

SQL-call complexity: O(BFS depth) bulk statements per rebuild (2 memo SELECTs +
1 get_objects + index/memo MODIFYs per level), driver-chunked; NOT O(nodes). No
per-row SELECT, no per-object SQL, no repository-wide scan.

HTTP-call complexity: unchanged — ZERO new HTTP; existing get_objects negotiation only;
no blob payload read during eligibility/copy.

Row/byte batch policy: index writes unchanged (c_index_write_chunk_size=30000, ~21.9MB
peak, value untouched); memo writes c_tree_child_chunk_size=30000; FAE drivers
c_tree_lookup_chunk_size=500. A SMALL-hit FAE batch is additionally closed by peek-then-
decide (AR-2-2): BEFORE adding any tree whose child_count would push the batch's running
sum OVER c_tree_child_chunk_size (that tree is deferred to the next batch), so no FAE ever
returns more than one chunk of child rows; a LARGE hit (child_count > chunk) is
read alone in CHILD_SEQ windows of chunk size (AR-1-2). Memo rows are fixed-width
metadata (no XSTRING payload).

Peak-memory model: one 30000-row index chunk (~21.9MB) + at most one c_tree_child_chunk_
size window of child rows for the tree currently being reproduced (NEVER a whole flat
directory, however large — AR-1-2) + method-local dot_sha1/algo_ver/cutoff. A pathological
single flat directory > 30000 files is reproduced page-by-page via read_large_child_page
CHILD_SEQ windows and flushed on the same index chunk pattern. No blob payloads held.

Cache scope: dot_sha1/algo_ver computed once per rebuild, method-local; no cross-call
in-memory cache introduced (persistent memo lives only in the two DB tables).

Transaction owner: for TR1-TR4 (the rebuild_index/BFS library integration) unchanged — the
caller's existing LUW, no new COMMIT WORK, memo and marker commit atomically with the
caller. TR5 (clear_tree_memo) is the ONE carve-out: it is a standalone top-level admin
action that OWNS its own LUW and legitimately issues COMMIT WORK AND WAIT at two points —
on the success path (after the memo DELETEs + its own mutex release) and on the error path
(after ROLLBACK WORK + explicit release_repo_lock cleanup, AR-4-1). It is not a library
method, so this does not affect rebuild_index's transaction-owner contract.

Large-repository acceptance criteria:
```text
- 1,000 rows: reuse adds negligible overhead; full correctness parity with full walk.
- 42,000 rows: on a warm memo (sibling-commit reuse) the per-tree fetch+decode+map compute
  for unchanged subtrees is eliminated; SQL-call count stays O(depth); output byte-identical
  to a full-walk control; readiness/marker semantics unchanged.
- 1,000,000 rows: bounded peak memory (30000-row index chunk + at most one chunk-sized
  child window, even for a single million-entry flat directory — AR-1-2/AR-2-2, strict
  <= one chunk per FAE via peek-then-decide); no per-object
  SQL/HTTP; storage grows O(nodes) but memo is deduped across commits and capped by TTL.
- The numeric speed-up remains an ESTIMATE (elimination of the measured chunk=1000 ~4-5 s
  traversal-compute residual, scaled by the unchanged-subtree fraction) until Stage-0/TR4
  produce a chunk=30000 trace; only then is it a MEASURED claim.
```

Rejected-alternative rationale: A (zero hits), B2 (storage blow-up or forbidden aliasing;
introduces C10 hazard), C (fragile parent policy, subset of B's benefit), D-permanent
(measurement, not a design) — all in §4.

Migration / mixed-version / rollback:
```text
- Old zaog_obj_index rows (built before B, or under an older algo_ver): remain fully valid
  and queryable for their OWN commit via the unchanged select_rows_for_filter; they are
  simply never a reuse SOURCE (no memo, or algo_ver differs) => full walk. No backfill.
- Mixed code versions: pre-B code ignores the memo tables (full walk); post-B code finds no
  memo for pre-B builds (miss => walk + memoize). Safe both directions.
- Rollback: drop zaog_tree_map + zaog_tree_child (or stop reading them); rebuild_index
  reverts exactly to today's full-walk-every-commit. No productive dependency on the memo.
- Residual C7 risk (SAP support-package upgrade changing a standard plugin without an
  abapGit version bump): mitigated three ways — (a) folding c_abap_version into algo_ver so
  any tool upgrade auto-invalidates; (b) the mandatory memo TTL c_tree_memo_max_age_secs
  (604800s / 7 days) that bounds the maximum replay window of undetected drift (AR-1-1);
  (c) the NEW dedicated admin action `clear_tree_memo` (§11/TR5, AR-2-1) to force-clear a
      repo's memo after such upgrades or any suspected corruption (C12), acquiring the SAME
      repo mutex as rebuild_index so it is serialized against any in-flight rebuild.
      Documented, not silently ignored.
- New DDIC field BUILT_AT on zaog_tree_map is additive; pre-B has no such table at all, so
  there is no field-migration concern (both tables are net-new).
```

--------------------------------------------------------------------------------
## 13. Hard-constraint compliance
--------------------------------------------------------------------------------

```text
[x] No repository-wide scan / no per-row SELECT / no per-object SQL/HTTP / no blob payload
    read during eligibility or copy (index + memo metadata rows only; §9).
[x] No deepen/shallow reintroduction (no fetch-protocol change at all).
[x] No cross-repo reuse (C4 ineligible; repo_key in the key; §8).
[x] is_index_ready STRICT unchanged; marker remains the sole readiness authority (§6/§7).
[x] Standard abapGit path unchanged when ORTEC is disabled (only ORTEC classes/tables touched).
[x] c_index_write_chunk_size value NOT changed (new separate constants for memo batches).
[x] Benefit separated MEASURED vs ESTIMATED; Stage-0 chunk=30000 trace is the stated
    prerequisite to convert the estimate to a measured claim (§1/§4/§12).
[x] AR-1-1: mutable-state drift bounded by a mandatory memo TTL (c_tree_memo_max_age_secs
    =604800, cutoff predicate) + the dedicated mutex-based clear_tree_memo admin channel
    (§5.1/§9/§11-TR5, AR-2-1); accepted residual-risk boundary, not an unbounded
    silent-wrong risk.
[x] AR-1-2: no unbounded child SELECT — SMALL hits FAE-batched by key-count AND (AR-2-2)
    peek-then-decide on summed child_count; LARGE hits streamed in CHILD_SEQ windows; peak
    resident child rows strictly <= one chunk (§9/§11-TR3/§12).
[x] AR-1-3: identical 255-char guard applied to DIR nodes in the MISS walk AND HIT
    reproduction (§3/§11-TR3), so CHILD_NAME CHAR255 is provably sufficient.
[x] AR-1-4: Stage-0 is a BLOCK_OWNER_DECISION — TR0 measures/reports the write:compute
    split and STOPS; no invented 30% autonomous gate (§4/§11-TR0/§14).
[x] AR-1-5: C10 narrowed to cardinality/deletion loss only; same-count external corruption
    is out-of-scope OOB (C12), recovered via TTL + the dedicated clear_tree_memo (§8/§10).
[x] AR-2-1: the memo clear is a NEW dedicated method clear_tree_memo that uses the SAME
    LOCK_<repo_key> row-mutex (acquire_repo_lock) as rebuild_index, deletes ONLY the two
    memo tables, issues NO `DELETE FROM zaog_fetch_sess WHERE repo_key`, and touches only
    its OWN mutex row; general clear_repo (divergent ENQUEUE primitive) is NOT modified.
    WAIT-then-REJECT if a rebuild holds the mutex; single lock, no nesting -> no deadlock
    (§6/§7/§8-C14/§11-TR5).
[x] AR-2-2: SMALL-hit FAE batching is strictly bounded to <= one c_tree_child_chunk_size per
    read via peek-then-decide (close BEFORE a tree that would exceed the chunk); no ~2x-chunk
    case remains anywhere (§9/§10-reuse_small_batch_bound/§11-TR2/§12).
[x] AR-3-1: clear_tree_memo CAPTURES the returned mutex id
    (lv_lock_id = zcl_abapgit_ortec_pack_raw=>acquire_repo_lock( iv_repo_key = iv_repo_key ))
    and releases with THAT captured id (release_repo_lock( lv_lock_id )) on the success path,
    never the bare iv_repo_key, so no LOCK_<repo_key> status='L' row survives a successful
    clear (proven by clear_memo_removes_rows) and a busy clear leaves the existing mutex row
    intact (clear_memo_rejects_if_locked) — no lock leak / DoS (§11-TR5/§10).
[x] AR-4-1: the ERROR path does an EXPLICIT, UNCONDITIONAL captured-id cleanup —
    ROLLBACK WORK -> release_repo_lock( lv_lock_id ) -> COMMIT WORK AND WAIT — instead of
    relying on ROLLBACK WORK alone. This is a DELIBERATE, JUSTIFIED difference from
    rebuild_index (a library method that never commits): clear_tree_memo OWNS its own admin
    LUW and already commits on success, so its cleanup is provably durable BECAUSE it owns
    its own commit boundary — a stronger guarantee than rollback semantics. No text claims
    equivalence to rebuild_index's error path. Proven by clear_memo_err_releases_lock (no
    LOCK_<repo_key> status='L' row after a forced error; a subsequent acquire succeeds)
    (§11-TR5/§10/§8-C14/§12).
[x] Every implementation-packet field concrete; no TBD/placeholder/"as appropriate".
```

--------------------------------------------------------------------------------
## 14. Next action for the owner
--------------------------------------------------------------------------------

This is a CYCLE-5 (owner-authorized additional cycle, cycle 2 of 3) revised design-only
artifact closing the sole remaining MAJOR AR-4-1 (TR5 clear_tree_memo ERROR-path explicit
mutex cleanup), on top of the cycle-4 closure of AR-3-1 (success-path capture/release), the
cycle-3 closures of AR-2-1/AR-2-2, and the cycle-2 closures of AR-1-1..AR-1-5. No productive
code, no state.md change, no commit was made. Recommended next step: one NON-automatic
adversarial verification pass re-checking ONLY the corrected TR5 error path for convergence;
then — only if approved — execute TR0, whose result is a BLOCK_OWNER_DECISION (AR-1-4):
TR0 captures and REPORTS the chunk=30000 write:compute split and STOPS. TR1-TR5 are
authorized ONLY by a separate explicit owner go decision on that reported split; the
design never proceeds past Stage-0 on its own authority. Implementation remains
UNAUTHORIZED until that owner resume decision.
```
```
