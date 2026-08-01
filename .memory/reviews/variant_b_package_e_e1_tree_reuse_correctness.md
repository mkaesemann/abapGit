# E1-TREE-REUSE — Correctness Gate

```text
PACKET=COMPACT_HANDOFF_V1
TASK=E1-TREE-REUSE-CORRECTNESS-GATE-V1
MODE=CORRECTNESS_GATE (final independent gate after adversarial APPROVE)
BASELINE=36839c7faa55b4568c1724cf6232468957f6aac8
REVIEW_MODE=ortec-abapgit-design-review (balanced, second reasoning line)
```

## Scope compliance

```text
ALLOWED_CONTEXT_READ=YES — design log (CYCLE=5, all sections §1-14 incl. all
  five cycles of revision responses), adversarial review (all 5 cycles),
  discovery log (§0-4) all read completely.
SOURCE_SCOPE_READ=YES — read directly, this pass, to verify design claims
  against actual current source (not just trusted from the artifacts):
  zcl_abapgit_ortec_obj_index.clas.abap (rebuild_index full body, class
  header/constants), zcl_abapgit_ortec_pack_raw.clas.abap
  (acquire_repo_lock/release_repo_lock full bodies + signatures),
  zcl_abapgit_ortec_cache_admin.clas.abap (clear_repo/acquire_lock/
  release_lock full bodies), ezaog_repo_lock.enqu.xml, zaog_obj_index.tabl.xml
  (field list), zcl_abapgit_filename_logic.clas.abap (dynamic
  MAP_FILENAME_TO_OBJECT dispatch), zcl_abapgit_dot_abapgit.clas.abap
  (get_signature/serialize/to_xml chain), zcl_abapgit_ortec_repo_state.clas.abap
  and zcl_abapgit_ortec_obj_store.clas.abap (ty_repo_key type compatibility).
STATE_WRITE=NO. DIAGRAM_WRITE=NO. PRODUCTIVE_WRITES=NO.
OUTPUT_ARTIFACT=.memory/reviews/variant_b_package_e_e1_tree_reuse_correctness.md
  (this file; the stale NOT_RUN/BLOCKED_UPSTREAM stub is superseded — the
  adversarial gate it was blocked on has since converged to APPROVE across
  5 cycles, verified below).
```

## Verdict
**APPROVE**

## Confidence
High

---

## Strengths

- **Clean ORTEC isolation, independently confirmed.** The entire design touches
  exactly three surfaces: `zcl_abapgit_ortec_obj_index` (TR1/TR3, new private
  constants/methods + the per-level BFS body), two net-new additive tables
  `zaog_tree_map`/`zaog_tree_child` (TR2), and one new public method
  `zcl_abapgit_ortec_cache_admin=>clear_tree_memo` (TR5). Direct read of
  [zcl_abapgit_ortec_obj_index.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap)
  and [zcl_abapgit_ortec_cache_admin.clas.abap](src/ortec/git/zcl_abapgit_ortec_cache_admin.clas.abap)
  confirms `clear_tree_memo` does not exist yet (design-only) and that no
  standard abapGit class, no `zaog_obj_index` column/PK, and no other ORTEC
  table is touched. No hidden standard-abapGit coupling found.
- **The critical invariant holds under direct inspection.** Every ineligibility
  path in this design (C2/C3/C4 context mismatch, C6 no memo, C7 old algo, C10
  count-loss, C11 TTL-expired) resolves to exactly one outcome: **MISS → the
  same full `get_objects`+`decode_tree`+`file_to_object` walk the code already
  performs today**, never a shortcut, never an empty/partial result treated as
  final. A memo miss is never interpreted as "this tree/subtree doesn't exist"
  or "these files were deleted" — it is only ever interpreted as "no cached
  resolution; compute it now," which is the correct `NOT_BUFFERED`/
  `UNKNOWN_NEEDS_FETCH` treatment. Verified directly against the current
  `rebuild_index` body (lines ~297-497) that the fresh-walk code path this
  design falls back to is unchanged.
- **C12 (same-count content corruption) is a distinct, correctly-classified
  risk, not a disguised instance of the forbidden pattern.** It is not
  "missing data read as deletion" — it is "present-but-corrupted data," and
  the design does not claim it is silently safe: it is explicitly named
  out-of-scope, bounded by the mandatory 7-day TTL, and given a concrete
  operator recovery path (`clear_tree_memo`). This is the same accepted
  residual-risk shape as the project's own precedent
  (E4-OOB-DELETION-RISK), correctly cited.
- **AR-1-1 (TTL/admin-clear) spot-check, independently verified against
  CURRENT document text and source.** §3/§9 specify a concrete
  `c_tree_memo_max_age_secs VALUE 604800` cutoff computed once via
  `GET TIME STAMP FIELD` + `cl_abap_tstmp=>subtractsecs`, applied as a
  `built_at >= lv_cutoff` predicate directly in the header eligibility SELECT
  (§9, TR2 `read_tree_headers`) — an expired header simply does not appear in
  the hit set, so TTL enforcement is set-based and decision-free, not an
  after-the-fact patch. The companion `clear_tree_memo` admin channel is
  correctly *not* routed through the existing `clear_repo` — direct read of
  `clear_repo`/`acquire_lock`/`release_lock` in
  [zcl_abapgit_ortec_cache_admin.clas.abap](src/ortec/git/zcl_abapgit_ortec_cache_admin.clas.abap)
  (lines ~315-448) confirms it really does use `ENQUEUE_EZAOG_REPO_LOCK`
  keyed by `session_id = iv_repo_key` (per
  [ezaog_repo_lock.enqu.xml](src/ortec/git/ezaog_repo_lock.enqu.xml), keyed on
  `CLIENT+SESSION_ID`) and really does `DELETE FROM zaog_fetch_sess WHERE
  repo_key = iv_repo_key` — a genuinely different primitive from
  `rebuild_index`'s `LOCK_<repo_key>` row mutex, and one that would delete an
  in-flight rebuild's mutex row. Keeping the two channels separate, as the
  design does, is the correct call, not just a documentation claim.
- **AR-4-1 (TR5 final lock handling) spot-check, independently verified.**
  Direct read of `acquire_repo_lock`/`release_repo_lock`
  (lines ~417-467 of [zcl_abapgit_ortec_pack_raw.clas.abap](src/ortec/git/zcl_abapgit_ortec_pack_raw.clas.abap))
  confirms: (a) `acquire_repo_lock` returns `rv_lock_id = |LOCK_{ iv_repo_key }|`
  and its `INSERT` is **not** committed by the method itself (no `COMMIT WORK`
  anywhere in `pack_raw`); (b) `release_repo_lock` guards `iv_lock_id IS
  INITIAL` and deletes strictly `WHERE session_id = iv_lock_id AND status =
  'L'`, making a repeated or no-op call safe. Given these two facts, the
  Cycle-5 TR5 error-path contract — `ROLLBACK WORK` → `release_repo_lock(
  lv_lock_id )` → `COMMIT WORK AND WAIT` → re-raise — is provably correct in
  both sub-cases: if no intervening commit occurred, `ROLLBACK WORK` alone
  already discards the uncommitted acquire `INSERT` and the explicit release
  is a harmless no-op; if some future intervening commit had made the
  acquire durable, `ROLLBACK WORK` would *not* undo it, but the explicit
  `release_repo_lock( lv_lock_id )` deletes it directly and the final
  `COMMIT WORK AND WAIT` makes that deletion durable. This is a strictly
  stronger guarantee than the cycle-4 rollback-only draft, and the
  documented rationale (TR5 is a standalone admin LUW owner, unlike
  `rebuild_index`, which never commits) is accurate per source. The design
  correctly stopped claiming equivalence to `rebuild_index`'s pattern once
  that claim was shown false in cycle 4.
- **AR-1-3's DIR-length-guard premise is exactly what the current source
  shows.** Direct read confirms the FILE branch alone is length-guarded
  (`IF strlen( <ls_work>-path ) > 255 OR strlen( <ls_node>-name ) > 255.
  CONTINUE.`, only inside the FILE/executable/symlink `WHEN`); the DIR branch
  builds `lv_next_path` and enqueues with no such guard. The design's
  proposed symmetric guard, and its "row-output-identical for realistic
  repos" argument (any subtree under an oversized component already yields
  zero rows via the existing FILE guard), is sound given this confirmed
  asymmetry.
- **Adversarial convergence is real, not rubber-stamped.** All 9 findings
  (AR-1-1..5, AR-2-1/2, AR-3-1, AR-4-1) trace a genuine, source-grounded
  correction chain — most notably AR-2-1→AR-3-1→AR-4-1, where each fix
  exposed a new, real defect (bare-repo-key release, then rollback-only
  error cleanup) rather than papering over the same one. My own reading of
  the current TR5 packet text matches the cycle-5 ledger's `ACCEPTED_
  AND_FIXED` claims verbatim; I did not find any stale/contradictory
  operative text left over from earlier cycles (checked §8/C14, §11/TR5,
  §12, §13 specifically for residual "matches rebuild_index" or
  "rollback-only" language — none found, consistent with the adversarial
  reviewer's own cycle-5 grep sanity checks).
- **Correctness of unaffected surfaces confirmed.** `is_index_ready` reads
  only the `$IDX/__READY__` marker row in `zaog_obj_index`; the memo tables
  are never queried by it or by `select_rows_for_filter`. The marker is
  still written unconditionally last, and delete-first/marker-last/lock-
  scope are preserved byte-for-byte in the TR3 packet's own anchor/action
  framing (replace only the per-level BFS body, not the surrounding
  structure).
- **Git protocol is untouched.** No new HTTP, no `deepen`/shallow handling
  anywhere in this design — it operates purely on objects already resolved
  through the existing `get_objects` bulk API; blob payload fetch
  (`build_files_from_rows`) is explicitly out of scope and unmodified.
- **A complete performance model exists**, satisfying my mode's mandatory
  performance-review gate: no per-object SQL/HTTP (bulk FAE/window SELECTs
  only, verified against the §9 SQL shapes), no payload reads for presence
  checks (eligibility reads only fixed-width metadata columns), peak memory
  explicitly bounded (one index chunk + at most one child-chunk window,
  never a whole flat directory), and a mandatory large-repository gate
  (TR0 pre-req live SAT trace + TR4 live validation) rather than reliance on
  small unit tests alone. Per my mode's performance-review boundary, the
  *quantitative* verdict on this model belongs to the dedicated performance
  reviewer, not to this gate.

---

## Issues

### DR-001
- Type: maintainability
- Severity: minor
- Evidence: TR5's `clear_tree_memo` issues `COMMIT WORK AND WAIT` on both its
  success and error paths (§11/TR5), which is only safe if the method is
  always invoked as a standalone, top-level admin action and never nested
  inside a caller's own open LUW. The existing `clear_repo` makes the
  identical assumption today (confirmed: it also issues `COMMIT WORK AND
  WAIT` / `ROLLBACK WORK`), but neither its own ABAP Doc header nor the new
  `clear_tree_memo` packet in §11/TR5 states this constraint as an explicit
  calling-convention contract for future callers.
- Why it matters: a future caller that invokes `clear_tree_memo` as one step
  inside a larger business transaction (e.g., a batch admin report that
  also updates other tables before its own final commit) would have that
  unrelated pending work silently committed or rolled back by
  `clear_tree_memo`'s internal `COMMIT`/`ROLLBACK WORK` — a correctness
  hazard for the *caller*, not for this design's own logic.
- Fix: add a one-line ABAP Doc note on `clear_tree_memo` (mirroring/
  strengthening `clear_repo`'s existing comment) stating it is a top-level
  admin LUW owner and must not be called from within an open transaction.
  Non-blocking for this gate since it mirrors an already-accepted existing
  pattern in this codebase; recommended as a documentation fix alongside
  TR5's implementation.

---

## Required revisions

None. No blocking or major correctness, architecture, or invariant issue was
found; DR-001 is a minor, non-blocking documentation recommendation.

## Optional improvements

- Consider a future periodic/on-demand janitor for physically purging
  TTL-expired `zaog_tree_map`/`zaog_tree_child` rows. The TTL as designed
  correctly gates *eligibility* (an expired header is never trusted), but it
  does not reclaim storage — rows past `c_tree_memo_max_age_secs` remain on
  disk until an operator explicitly calls `clear_tree_memo` for that
  `repo_key`. This is not a correctness gap (eligibility is fully protected
  regardless of physical row presence) but is worth flagging as a storage-
  growth consideration for the eventual TR4/production rollout note.
- §12's phrase "storage grows O(nodes) but memo is deduped across commits
  and capped by TTL" slightly overstates the TTL's effect — the TTL caps
  *trust*, not physical row count/storage. A future revision could tighten
  this wording so it doesn't read as an automatic storage cap.

---

## Handoff

Per this mode's performance-review boundary: correctness/architecture/
invariant gate is **APPROVE**. Hand off to
`ortec-abapgit-performance-review` in `DESIGN_GATE` mode for the
quantitative verdict on the §9/§12 SQL/memory model (chunk bounds, FAE
batching, TR0/TR4 SAT-trace prerequisite) before TR1-TR5 implementation is
authorized. Implementation remains gated, independent of this gate's
verdict, by the owner go/no-go on TR0's reported write:compute split
(AR-1-4), per the design's own Stage-0 `BLOCK_OWNER_DECISION`.
