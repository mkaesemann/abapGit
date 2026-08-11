# OBJ_PERF_FINAL — IT8 validation handoff

Status: `LOCAL_COMPLETE_AWAITING_IT8`. All source changes are committed locally on
`ortec/abapgit_1_133-opt-rework`, **not pushed**. No live SAP/ADT connectivity was available in
this session (the connected system does not have `ZAOG_OBJ_COVER`/`ZAOG_OBJ_PIDX` at all — confirmed
via `SAPDiagnose`/`SAPRead` during Slice 1c). Everything below is a plan for the owner to execute on
the real IT8 system; nothing in this document claims live validation already happened.

## Checkpoint commits (local only, in order)

```text
f7be8296  ORTEC: Add ZAOG_OBJ_COVER/ZAOG_OBJ_PIDX partial-index coverage model (Slice 1/1b/1c/1d)
3e1e804a  Memory: Record discovery, design, adversarial review, Slice 1/1b/1c/1d implementation
9603813e  ORTEC: Resolve filtered commit paths from persisted coverage (Slice 2)
bdacce79  Memory: Record Slice 2 implementation notes
b239dd2a  ORTEC: Avoid complete index rebuild for covered filters (Slice 3, includes Slice 4's
          current-remote gate, implemented together per this program's own instruction)
80641c3b  Memory: Record Slice 3 implementation notes
d0d7f3eb  Memory: Record Object Store Store-A verification (NO_CHANGE_JUSTIFIED)
09c695dd  ORTEC: Fix walk_filtered O(F x K) membership scan and un-chunked select_rows_for_filter
          (performance audit PS-001/PA-001)
d3f0679d  Memory: Record performance scan/audit findings and fixes
```

## Activation order

1. Import/activate DDIC first, in this order: `ZAOG_OBJ_COVER` (new table), `ZAOG_OBJ_PIDX` (new
   table), `ZAOG_OBJ_INDEX` (existing table, append-only `CONTEXT_HASH CHAR40` non-key column).
2. Activate `ZCL_ABAPGIT_ORTEC_OBJ_COVER` (new class — remember its `.clas.xml` must import too,
   not just the `.clas.abap` source, or abapGit will silently fail to create the class).
3. Activate `ZCL_ABAPGIT_ORTEC_OBJ_INDEX` (heavily changed — new `iv_context_hash`/
   `iv_current_remote` parameters, `invalidate_commit_index`, `walk_filtered`,
   `select_partial_rows_for_filter`, `ensure_filtered_coverage`).
4. Activate `ZCL_ABAPGIT_ORTEC_CACHE_ADMIN` and `ZCL_ABAPGIT_ORTEC_FILTER_WALK`.
5. Run ABAP Unit for all four classes before any functional testing.

## ABAP Unit classes/methods to run

- `ZCL_ABAPGIT_ORTEC_OBJ_INDEX` — full `ltcl_obj_index` suite (26 test methods: 7 pre-existing +
  19 new). See `.memory/logs/obj_index_partial_regression.md` §2 for the full name-to-behavior map.
- `ZCL_ABAPGIT_ORTEC_OBJ_COVER` — full `ltcl_obj_cover` suite (10 test methods).
- `ZCL_ABAPGIT_ORTEC_CACHE_ADMIN` — the 3 new tests (`clear_repo_deletes_derived`,
  `clear_repo_then_filtered_read_rewalks`, `clear_repo_blocks_on_pack_lock`) plus its existing
  suite.
- `ZCL_ABAPGIT_ORTEC_OBJ_STORE` — existing suite only, as a pure regression guard (this program made
  zero changes to this class; a failure here would indicate an environment issue, not a defect
  introduced by this program).

## ATC scope

Run ATC on all newly created/changed objects listed above plus the two new DDIC tables. No
standard abapGit class was touched by this program (only ORTEC-namespace classes and tables).

## Feature OFF / standard behavior

For a repository NOT registered with ORTEC (or with the ORTEC git-switch feature flag off),
`zcl_abapgit_ortec_filter_walk=>get_remote_files_for_stage` resolves an initial `repo_key` and
takes the pre-existing `ii_repo_online->get_files_remote(...)` fallback — it never reaches
`zcl_abapgit_ortec_obj_index` at all. Validate: Stage/Diff on a non-ORTEC repo behaves identically
to before this program (no new SQL, no new DDIC access).

## Feature ON — required functional/output-parity scenarios

1. **Cold Stage by Transport, small K, never-indexed commit.** Pick a repo/commit with a
   COMPLETE index NOT yet built. Request a small transport (K≈5–20 objects). Expect: no
   `ZAOG_OBJ_INDEX` rebuild; `ZAOG_OBJ_PIDX` gains exactly the K objects' rows (not F); output
   files match what the OLD (pre-this-program) cold path would have returned (compare against a
   COMPLETE-index rebuild's result for the same filter, or the standard `get_files_remote` result,
   for output parity).
2. **Repeat the identical request (warm).** Expect: zero tree walk (verify via SQL trace / ST05 —
   only `get_coverage` + `select_partial_rows_for_filter`, no `get_objects` calls for commit/tree
   SHA1s).
3. **Single-object Diff** on the same cold commit for one of the K objects — same warm/cold
   behavior expectations as above, K=1.
4. **Branch A → B → A.** Stage a small filter on branch A, then B (different commit, different
   objects), then back to A. Confirm A's second request is warm (via its own `ZAOG_OBJ_PIDX`/
   `ZAOG_OBJ_COVER` rows, unaffected by B's separate commit-scoped rows).
5. **Full repository Stage/Status (K≈F).** Confirm this still triggers/uses the COMPLETE-mode
   `ZAOG_OBJ_INDEX` path (via `is_index_ready`/`rebuild_index`), not the FILTERED path — output
   must match today's COMPLETE-index behavior exactly (output parity control case).
6. **Missing-tree/object injection**, if feasible on a sandboxed repo: force
   `ZAOG_OBJ_STORE` to be missing a required tree for a cold filtered request. Expect:
   `walk_filtered` raises, one `M` (`unresolved_missing_local_data`) coverage row is written per
   requested object, and the outer fallback (`get_files_remote`) still returns correct data.
   Retry the identical request within ~5 minutes: expect an immediate, cheap raise→fallback (no
   re-walk). Retry again after >5 minutes (`c_missing_data_backoff_seconds = 300`): expect a real
   retry walk.
7. **Repository clear (`clear_repo` / cache-admin reset)** on a repo with existing
   `ZAOG_OBJ_INDEX`/`ZAOG_OBJ_COVER`/`ZAOG_OBJ_PIDX` rows. Confirm all three are emptied and the
   next filtered request re-walks cleanly (no false-empty result from orphaned coverage).
8. **.abapgit / devclass change** between two requests for the same commit — confirm the second
   request computes a different `CONTEXT_HASH` and does not reuse the first request's rows (this is
   what makes `RESOLVED_NOT_PRESENT_REMOTE`/`RESOLVED_NO_FILES` safe under a config change).
9. **`RESOLVED_NOT_PRESENT_REMOTE` requires current-remote match.** Stage-by-Transport (the online
   path, which computes `iv_current_remote`) against the actual current remote tip should be able to
   reach the strong state for a genuinely-deleted-remote object once the commit graph is
   `GRAPH_COMPLETE`/`FULL_COMPLETE`. A Pull-based path (`pull_filtered`, no online repo object) must
   NEVER reach the strong state — confirm via `SELECT resolution_status FROM zaog_obj_cover` after a
   `pull_filtered`-triggered walk that resolves an object with zero matches: it must be
   `RESOLVED_NO_FILES` ('N'), never `RESOLVED_NOT_PRESENT_REMOTE` ('D').

## Required owner measurements (old cold-complete-index path vs. new cold-partial path)

For the SAME repo/commit/filter, capture both:

```text
OLD (force COMPLETE-mode, e.g. via a repo that already has $IDX/__READY__ for a different, wider
     context, or by temporarily comparing against a full-Stage baseline)
NEW (cold, FILTERED-mode, small K)

requested objects (K)
files returned
output hashes/parity (must match exactly)
elapsed total
elapsed tree resolution (SAT: time inside walk_filtered/rebuild_index)
elapsed ZAOG_OBJ_INDEX/ZAOG_OBJ_PIDX/ZAOG_OBJ_COVER reads+writes
rows written (ZAOG_OBJ_INDEX should be 0 for the NEW path; ZAOG_OBJ_PIDX should be ~K)
SQL call count (ST05)
fallback reason, if any (should be NONE for a healthy repo)
```

Target expectation (per the approved design, `.memory/logs/obj_index_partial_design.md` §7):
for K=1 against F≈40,000+, the NEW path should write O(1) rows (not O(F)) while the tree-walk SQL
call count itself remains O(L) (BFS depth), identical to today — the design's own honest complexity
claim is a WRITE-volume improvement, not a walk-volume improvement (no candidate for safe
prefix-pruning was found — see `.memory/logs/obj_index_partial_design.md` §2, candidates A/B
rejected with source proof).

## Known non-blocking residual items (owner may decide, not required before SAP_VALIDATED_COMPLETE)

- `clear_repo`'s exception path releases `zcl_abapgit_ortec_pack_raw`'s mutex in every path this
  program's regression review could identify, but has no dedicated `CLEANUP`/second safety-net
  release specifically for the narrow window between a successful `acquire_repo_lock` and the three
  plain `DELETE` statements. Every `DELETE` in that window is a single-table, primary-key-prefix
  delete against an existing ORTEC table — the same class of statement already relied on elsewhere
  in this class as "cannot itself fail under normal DB operation" (`invalidate_commit_index`'s own
  documented rationale). Accepted as a non-blocking, extremely low-probability residual risk;
  revisit only if a real stuck-lock incident is observed.
- `OBJ_STORE_SLICE_1=NO_CHANGE_JUSTIFIED` — `get_all_objects`/`populate_cache`'s pre-existing
  unbounded `SELECT * WHERE status='R'` full-payload preload (a separate, already-known risk,
  unrelated to and unreachable from this program's new call chain) remains untouched and out of
  scope. A future dedicated slice mirroring the `2111b288` fix pattern would need its own
  design/review cycle.
- Full-scale (≈1,000,000 stored object, 95–98%-shared-branch) measurements were not performed in
  this environment (no live system access) — required as final acceptance evidence before
  `SAP_VALIDATED_COMPLETE`, per the launch spec's own allowance ("do not require a production-scale
  full-repository run before providing the first IT8 handoff").

## Sign-off criteria for `SAP_VALIDATED_COMPLETE`

- ABAP Unit PASS for all four classes above.
- ATC PASS (no new findings) for all new/changed objects.
- All 9 functional/output-parity scenarios above produce correct, parity-matching results.
- At least the K=1/K=100–250 measurements from the "Required owner measurements" table are
  captured and show the expected write-volume reduction (rows written to `ZAOG_OBJ_INDEX` = 0 for a
  cold FILTERED-mode request).
- No regression in `ZCL_ABAPGIT_ORTEC_OBJ_STORE`'s own existing test suite (this program made zero
  changes to that class).
