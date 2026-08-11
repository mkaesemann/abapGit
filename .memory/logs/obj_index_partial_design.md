# OBJ_PERF_FINAL — Partial `ZAOG_OBJ_INDEX` Design (OBJ-PERF-DESIGN-1)
Design only. No source modified by this task. Grounded exclusively in
`.memory/state.md`, `.memory/logs/obj_index_partial_history_archaeology.md`,
`.memory/logs/obj_index_partial_current_source.md`, and direct re-reads of
`zcl_abapgit_ortec_obj_index.clas.abap`, `zaog_obj_index.tabl.xml`,
`zaog_obj_store.tabl.xml`, `zcl_abapgit_ortec_git_switch.clas.abap`,
`zcl_abapgit_ortec_filter_walk.clas.abap`, `zcl_abapgit_ortec_mat_state.clas.abap`,
`zcl_abapgit_ortec_missing_obj.clas.abap`, `zcl_abapgit_ortec_walk_prep.clas.abap`,
`zcl_abapgit_ortec_cold_init.clas.abap`, `zaog_commit_hist.tabl.xml`,
`zaog_repo_state.tabl.xml`, `zcl_abapgit_filename_logic.clas.abap`,
`zcl_abapgit_dot_abapgit.clas.abap`, `zcl_abapgit_hash.clas.abap` (all under
`SOURCE_SCOPE`, BASELINE_COMMIT `4193733d`). Object Store side is
cross-referenced into `obj_store_performance_design.md`; do not duplicate its
OS-A..J/OS-INV content here except in the integration section (§11-12).
## Revision log (cycle 3 — FINAL)
Task `OBJ-PERF-DESIGN-3`, revising after adversarial cycle 2
(`.memory/reviews/obj_index_partial_adversarial.md`, "## Cycle 2"). Closes
the three open cycle-2 findings with a definitive fix — no "implementer
decides"/"equivalent invariant" language remains for any of them.
```text
AR-2-01 (BLOCKER) -> §3.0 (narrowed), §3.0b (new), §5 (three bullets
  rewritten), §7, §8, §9 Slice 1d (new), §11 steps 3.1/3.4/4/5, §12, §3.0b.
  CLOSURE: option (b) selected. FILTERED-mode's positive rows move out of
  `ZAOG_OBJ_INDEX` into a NEW table, `ZAOG_OBJ_PIDX`, whose primary key is
  `(MANDT, REPO_KEY, COMMIT_SHA1, OBJ_TYPE, OBJ_NAME, CONTEXT_HASH,
  PATH_HASH)` — `CONTEXT_HASH` is a real KEY field here, so two contexts'
  rows for the same object/path physically coexist as two distinct rows;
  `walk_filtered`'s `MODIFY` can never again overwrite another context's
  row. `ZAOG_OBJ_INDEX` itself keeps cycle 2's non-key `CONTEXT_HASH`
  column unchanged, because that column was never actually unsafe there —
  `rebuild_index` always purges the whole commit before rewriting under its
  own context (no upsert-without-purge pattern exists on that table).
  `select_rows_for_filter` keeps reading `ZAOG_OBJ_INDEX`
  (COMPLETE-mode/warm-index path only, §11 step 3.1); a new
  `select_partial_rows_for_filter` reads `ZAOG_OBJ_PIDX` (FILTERED-mode
  path, §11 steps 3.4/4/5) and projects results into the identical
  `ty_index_rows_tt` shape, so `build_files_from_rows` needs zero changes.
  Chosen over option (a) (adding `CONTEXT_HASH` to `ZAOG_OBJ_INDEX`'s own
  primary key) because option (b) requires no DDIC key migration/table
  conversion of an already-shipped table and does not touch COMPLETE
  mode's already-correct behavior at all — strictly smaller/safer, the
  same additive-table risk class cycle 1 already accepted for
  `ZAOG_OBJ_COVER`.
AR-2-02 (MAJOR) -> §11.4 (rewritten), §13 W8 (rewritten), §14 W12 (new,
  `filter_walk` changes). CLOSURE: `get_files_for_filter`,
  `ensure_filtered_coverage`, and `walk_filtered` all gain an explicit
  `iv_current_remote TYPE zif_abapgit_git_definitions=>ty_sha1 OPTIONAL`
  parameter. `zcl_abapgit_ortec_filter_walk=>get_remote_files_for_stage`
  computes it once, best-effort (`TRY`/`CATCH zcx_abapgit_exception` ->
  blank on failure), from `li_repo_online->get_current_remote( )` — the
  online repo object it already holds in scope — and passes it down. The
  Slice 4 gate becomes a plain value comparison (`iv_current_remote IS NOT
  INITIAL AND iv_commit = iv_current_remote`), with no object reference
  needed inside `zcl_abapgit_ortec_obj_index` at all. `pull_filtered` never
  supplies this parameter, so it is structurally always initial there,
  which structurally fails the gate's own `IS NOT INITIAL` check —
  `pull_filtered` can therefore never produce `RESOLVED_NOT_PRESENT_REMOTE`,
  without a second branch or an implementer's judgment call.
AR-2-03 (BLOCKER) -> `obj_store_performance_design.md` cache-admin section
  (rewritten), §5 (new bullet), §9 Slice 1c (updated). CLOSURE:
  `zcl_abapgit_ortec_pack_raw=>acquire_repo_lock`/`release_repo_lock` (the
  same DB-mutex-row lock `rebuild_index`/`walk_filtered` already use) is
  declared the ONE canonical lock gating `ZAOG_OBJ_INDEX`/`ZAOG_OBJ_COVER`/
  `ZAOG_OBJ_PIDX`. `clear_repo` keeps its own `ENQUEUE_EZAOG_REPO_LOCK` for
  an unrelated reason (serializing concurrent whole-repo admin clears
  across every ORTEC cache table, not just these three) but now ALSO
  acquires `acquire_repo_lock`, narrowly around exactly its three DELETEs
  against these three tables, after the enqueue lock and before touching
  them. Fixed acquire order (enqueue lock outer/whole-method, mutex
  inner/three-deletes-only, mutex released immediately after those three
  deletes) is deadlock-free by construction: `walk_filtered`/`rebuild_index`
  never acquire the enqueue lock, so no acquisition cycle can form.
```
Blocking: 0 (all three cycle-2 findings AR-2-01, AR-2-02, AR-2-03 closed in
this revision; every cycle-1 finding remains closed per the cycle-2
revision log below, none reopened by this revision's changes).
## Revision log (cycle 2)
Task `OBJ-PERF-DESIGN-2`, revising after adversarial cycle 1
(`.memory/reviews/obj_index_partial_adversarial.md`). Every BLOCKER/MAJOR is
closed with a concrete, decision-free change — none is left to "an
equivalent invariant the implementer proves later".
```text
AR-1-01 (BLOCKER) -> §3.0 (new), §3.2, §5, §8, §9 Slice 1b, §11 steps 1-5,
  §13 W1-W3. CLOSURE: ZAOG_OBJ_INDEX gains a new NON-KEY CONTEXT_HASH
  column, populated by both rebuild_index (COMPLETE mode) and walk_filtered
  (FILTERED mode) using the same compute_context_hash already defined for
  ZAOG_OBJ_COVER. is_index_ready and select_rows_for_filter both gain a
  mandatory iv_context_hash parameter/predicate, so a marker or file row
  written under an incompatible or legacy (blank, pre-migration) context is
  invisible to a caller in a different context. A config change can
  therefore never return stale positive files — it triggers exactly one
  self-healing rebuild under the new context. Superseded-context rows
  become permanently unreachable orphans (never returned, never trusted) —
  a bounded, non-blocking cleanup follow-up, accepted on the same terms §5
  already accepted for ZAOG_OBJ_COVER orphans.
AR-1-02 (BLOCKER) -> §5, §6 trigger 3, §9 Slice 3, §13 W4. CLOSURE: a new
  shared private helper invalidate_commit_index deletes ZAOG_OBJ_INDEX and
  ZAOG_OBJ_COVER rows for (repo_key, commit) together, in one LUW, and
  replaces every existing standalone DELETE FROM zaog_obj_index call site
  (rebuild_index's own pre-walk purge and get_files_for_filter's
  CORRUPT_OR_INCOMPLETE retry purge). No code path can delete one table's
  rows for a commit without the other in the same transaction.
AR-1-03 (MAJOR) -> §4.1 (new), §11.4, §13 W8. CLOSURE: local-.abapgit
  semantics are made explicit and permanent — no new fetch/decode of a
  target commit's own .abapgit is introduced. The strong
  RESOLVED_NOT_PRESENT_REMOTE state additionally requires
  iv_commit = ii_repo_online->get_current_remote( ) (the one commit the
  local .abapgit is actually known to describe); any other iv_commit caps
  the result at the weaker RESOLVED_NO_FILES regardless of graph
  completeness.
AR-1-04 (MAJOR) -> §3.2, §7, §8, §13 W3. CLOSURE: a shared
  c_filter_chunk_size = 5000 constant bounds get_coverage, write_coverage,
  and (now context-aware) select_rows_for_filter — every SQL statement in
  this program keyed by a caller-supplied it_filter loops in fixed
  5000-row chunks; a boundary test above 5000 is mandatory for each.
AR-1-05 (MAJOR) -> obj_store_performance_design.md cache-admin section
  (cross-referenced, not duplicated here). CLOSURE: zcl_abapgit_ortec_cache_admin's
  clear_repo gains one more DELETE FROM zaog_obj_cover in the same
  lock/transaction as its existing zaog_obj_index delete, before the
  existing COMMIT WORK AND WAIT.
AR-1-06 (MAJOR) -> obj_store_performance_design.md §9 OS-D, §10
  OS-INV-05/OS-INV-12 (cross-referenced, not duplicated here). CLOSURE:
  both invariants are re-scoped in words to "the integrated small-K call
  graph reachable from get_files_for_filter" plus a mandatory static
  call-graph proof gate; get_all_objects itself stays out of this
  program's scope, but the claim is no longer overclaimed as general.
AR-1-07 (BLOCKER) -> §4, §4.1 (new), §6 trigger 2, §11 step 3a (new), step
  4, §13 W5-W6. CLOSURE: 'M' (UNRESOLVED_MISSING_LOCAL_DATA) is promoted
  from reserved-but-unwritten to actively written: a missing commit/tree
  exception now writes one short-lived 'M' coverage row per object
  walk_filtered was asked to resolve this round, before re-raising to the
  unchanged outer fallback. A live (< c_missing_data_backoff_seconds old)
  'M' row for every currently-uncovered filter object short-circuits
  straight to the same fallback exception without re-attempting the
  doomed walk; an expired 'M' row is treated as plain "uncovered" and
  retried normally. 'M' is never read as FOUND/RESOLVED_NO_FILES/
  RESOLVED_NOT_PRESENT_REMOTE by any caller — it is a scheduling hint only
  (INV-01/INV-06 preserved).
AR-1-08 (MINOR) -> §3.2, §13 (covered inline in §3.2, no separate W item).
  CLOSURE: zcl_abapgit_ortec_obj_cover gains a CLASS-DATA failure counter +
  last-error text and a public get_diagnostics( ) reader, incremented on
  every write_coverage catch; behavior stays non-fatal, but the condition
  is now observable.
```
## 1. Functional model & truth rules
Six concepts, never interchangeable, never derived from one another implicitly:
| Concept | What proves it | Owner artifact |
|---|---|---|
| COMMIT GRAPH COMPLETENESS | `zcl_abapgit_ortec_mat_state` `hist_level = GRAPH_COMPLETE` (`G`) — commit + full tree closure verified present; blobs may be promised | `ZAOG_COMMIT_HIST` |
| SELECTED TIP SNAPSHOT COMPLETENESS | `hist_level = FULL_COMPLETE` (`F`) — additionally every reachable blob verified present | `ZAOG_COMMIT_HIST` |
| COMPLETE `ZAOG_OBJ_INDEX` COVERAGE | `$IDX/__READY__` marker row present for `(repo_key, commit)`, `idx_status='R'` | `ZAOG_OBJ_INDEX` (unchanged, §5) |
| PARTIAL FILTER RESOLUTION COVERAGE | a `ZAOG_OBJ_COVER` row exists for `(repo_key, commit, obj_type, obj_name, context_hash)` (new, §3) | `ZAOG_OBJ_COVER` (new) |
| EXACT POSITIVE FILE MAPPINGS | actual `ZAOG_OBJ_INDEX` rows for that `(obj_type, obj_name)` at that commit | `ZAOG_OBJ_INDEX` |
| SAFE NEGATIVE RESOLUTION FACTS | a `ZAOG_OBJ_COVER` row with `resolution_status IN (RESOLVED_NO_FILES, RESOLVED_NOT_PRESENT_REMOTE)` produced by a walk that touched the **entire** tree for that commit | `ZAOG_OBJ_COVER` |
Binding truth rules (all source-evidence-backed, §2 of the archaeology, `c8fbdf23`/`cd0b277d`):
- A **missing row is never remote absence**. `ZAOG_OBJ_INDEX` having zero rows
  for `(commit, obj_type, obj_name)` proves nothing by itself — it could mean
  "never resolved", "resolved and genuinely absent", or "resolution aborted
  partway" (see §4). Only an explicit `ZAOG_OBJ_COVER` row with a terminal
  status answers which.
- `GRAPH_COMPLETE`/`SNAPSHOT_COMPLETE` are **not** "the file index is
  complete". They certify the underlying commit/tree/blob *object graph*;
  `ZAOG_OBJ_INDEX`/`ZAOG_OBJ_COVER` completeness is a separate, independent
  signal layered on top (confirmed: today's `rebuild_index` never reads/writes
  `ZAOG_COMMIT_HIST` at all — Q6). This design wires them together for the
  first time (§4, §11) but keeps the two certificates structurally distinct.
- Only a **complete-index certificate** (`$IDX/__READY__`) proves *arbitrary*
  index absence (i.e. "ask me about any object at this commit, I have the
  answer"). A `ZAOG_OBJ_COVER` row only proves the answer for the **specific**
  `(obj_type, obj_name)` it names — it must never be read as evidence about
  any other object.
- A resolution fact is only as strong as the tree data it was computed against.
  A zero-match result reached while the commit's tree closure was **not**
  `GRAPH_COMPLETE` is `RESOLVED_NO_FILES` (weak: "not found in what we could
  see"); the same zero-match result reached while the tree closure **was**
  `GRAPH_COMPLETE` at walk time is the stronger `RESOLVED_NOT_PRESENT_REMOTE`
  (req #4). Neither may ever be surfaced by a caller as "confirmed deleted"
  beyond what `zcl_abapgit_ortec_obj_store=>cs_object_state-confirmed_absent`
  already governs at the object-store layer — this design does not change
  Stage/Diff's own Added/Modified/Deleted verdict logic, which is out of
  `SOURCE_SCOPE`.
## 2. Candidate evaluation (A–F)
Evidence check before evaluating: `zcl_abapgit_filename_logic` maps
`(path, filename) -> object` via `file_to_object`, and separately
`object -> filename` (single default extension only, no folder) via
`object_to_file`/`map_object_to_filename`. Folder placement for a **package**
is available via `zcl_abapgit_folder_logic=>package_to_path`, but no
object-level "candidate folder for this specific object" lookup exists in
`SOURCE_SCOPE`, and per Q3 a filter row (`ty_tadir` from
`ii_obj_filter->get_filter()`) carries only `object`/`obj_name` — no devclass.
TADIR itself cannot supply devclass for an object that exists remotely but was
never pulled locally (the single most valuable use case: discovering
new/changed remote objects, per Q9/filter_walk's own doc).
- **A — object/filename-candidate lookup only (no tree walk)**: **REJECT**.
  A full candidate path requires (a) the object's folder (only computable from
  a *local* devclass, unavailable for net-new remote objects) and (b) the
  *complete* set of extra file suffixes a given object type produces (main +
  XML + generated companions such as `.clas.testclasses.abap`), which is
  handler-specific knowledge not centralized in `file_to_object`/
  `object_to_file` and not present in `SOURCE_SCOPE`. Even where a candidate
  path could be built, a miss at that one path proves nothing (wrong folder
  guess, missing suffix in the static list, or a historically different
  `.abapgit` folder-logic mode) — it cannot produce a SAFE NEGATIVE, only an
  unreliable one. Guessing this would violate the "no invented facts" bar.
- **B — path-prefix-pruned tree walk only**: **REJECT**. The only
  currently-available prefix derivation is `package_to_path(devclass)`
  (already used for the existing `DEVC`-only post-filter in
  `get_files_for_filter`, Q4). Reusing it for arbitrary filter objects would
  require a devclass per object, which does not exist for objects not yet
  pulled locally (same gap as A) and reflects **current local** package
  assignment, not the **historical remote** tree at `iv_commit` — a real
  correctness gap for repos where objects moved packages. Unsafe as the sole
  or primary mechanism for the exact use case this feature targets.
- **C — hybrid**: **SELECTED**. Defined precisely in §3/§11: keep the
  mandatory full commit→tree BFS walk exactly as `rebuild_index` performs it
  today (only a full traversal can safely prove a negative given A/B's
  rejection), but change **what gets persisted and when a walk is required at
  all** — demand-driven, per-object coverage rows short-circuit the walk on
  repeat access, and a filtered walk persists only the caller's own K objects'
  rows instead of the full `F`-row catalog. This is the only candidate that is
  both source-grounded (reuses the exact walk already proven safe) and
  actually reduces cost for the K=1 case (write volume, not walk volume — see
  §7 for the honest scope of the improvement).
- **D — full index build with larger batches only**: **PARTIALLY REUSED, NOT
  SELECTED ALONE**. `c_index_write_chunk_size = 30000` is already the
  owner-approved, IT8-validated batch size (`77b66464`/`8b382a5e`,
  `E1-TREE-REUSE` parked) — this design does not re-tune it and does not
  propose a larger value (no new evidence). D alone (do nothing but batch
  bigger) does not address the K=1-on-a-huge-repo write-amplification problem
  the mission calls out, so it is folded into C rather than selected
  standalone.
- **E — async full warmup**: **AVOIDED per mission instruction**. No
  background/async pre-warm is introduced; every write in this design happens
  synchronously inside the caller's own request, matching the existing
  transaction-owner pattern (§7).
- **F — cross-commit tree memo reuse**: **AVOIDED per mission instruction and
  blocked by `state.md`'s `E1-TREE-REUSE` (PARKED_MEASUREMENT_PENDING)**. No
  tree decode result is cached or reused across different commits. Each
  commit's `ZAOG_OBJ_COVER`/`ZAOG_OBJ_INDEX` rows are keyed by that commit's
  own `commit_sha1` only (§3); a 95–98%-shared sibling branch pays its own
  full walk (accepted limitation, quantified in §7).
## 3. Persistence design — `ZAOG_OBJ_COVER`
**Decision: add a new, small, typed coverage table** (not reuse of
`ZAOG_OBJ_INDEX` rows for negatives, not request-scoped-only). Justification:
`ZAOG_OBJ_INDEX`'s primary key already encodes exact positive file mappings
(`obj_type, obj_name, path_hash`) and has zero rows for objects with no files —
overloading it with negative-fact rows would require a synthetic
`path_hash`/`file_path` (like today's `$IDX/__READY__` marker row) per
negative object, indistinguishable from real file rows in every existing
`SELECT *`/`FOR ALL ENTRIES` reader without adding a new predicate everywhere.
A dedicated table keeps `ZAOG_OBJ_INDEX` semantics ("a row is a file")
unchanged and gives the new resolution-state vocabulary (§4) its own column
instead of encoding it into `idx_status`/marker `obj_name` hacks.
```text
FILE_OR_OBJECT=zaog_obj_cover (new transparent table, DDIC)
METHOD_OR_DDIC=DD02V/DD03P_TABLE
ANCHOR=none (new object); field/key style mirrors zaog_obj_index.tabl.xml
  (6-key-field pattern) and zaog_commit_hist.tabl.xml (MANDT via ROLLNAME=MANDT,
  TZNTSTMPL timestamps)
ACTION=insert
CHANGE=
  DD02V: TABNAME=ZAOG_OBJ_COVER, TABCLASS=TRANSP, CLIDEP=X,
    DDTEXT='ORTEC Git: Partial Filter Resolution Coverage', CONTFLAG=L (buffer
    off, matches ZAOG_OBJ_STORE/ZAOG_COMMIT_HIST — never client-buffered,
    written on every cold filtered access)
  DD09L: BUFALLOW=N
  Fields (key order matches this list):
    1. MANDT      ROLLNAME=MANDT              KEY
    2. REPO_KEY   CHAR12   (= ty_repo_key)    KEY
    3. COMMIT_SHA1 CHAR40  (= ty_sha1)        KEY
    4. OBJ_TYPE   CHAR4    (= TADIR-OBJECT)   KEY
    5. OBJ_NAME   CHAR40   (= TADIR-OBJ_NAME) KEY
    6. CONTEXT_HASH CHAR40 (sha1 hex, §3.1)   KEY
    7. RESOLUTION_STATUS CHAR1 (§4 cs_resolution)
    8. FILE_COUNT INT4 (diagnostic only — count of ZAOG_OBJ_INDEX rows found
       for this object at write time; never read as authoritative, callers
       always re-SELECT ZAOG_OBJ_INDEX for the real file list)
    9. ALGO_VERSION CHAR4 (diagnostic copy of the literal embedded inside
       CONTEXT_HASH's input — never used in a WHERE clause)
    10. WALK_HIST_LEVEL CHAR1 (copy of zcl_abapgit_ortec_mat_state ty_hist_level
        at walk time: 'U'/'G'/'F' — governs the FIND_FILES/NO_FILES vs.
        NOT_PRESENT_REMOTE distinction, §4)
    11. RESOLVED_AT ROLLNAME=TZNTSTMPL
  No DD12V/DD17V secondary index (see §8 — every read is an exact/near-exact
  primary-key match, same conclusion Q10 reached for ZAOG_OBJ_INDEX).
INVARIANTS=OS-INV-01, OS-INV-05, OS-INV-10
SQL_SHAPE=NONE (DDIC only)
ERROR_ROLLBACK_FALLBACK=NONE (additive DDIC object; no data migration)
TESTS=none at this slice (table has no behavior); covered indirectly by
  ltcl_obj_cover in Slice 1 (§9)
VALIDATION=SAPDiagnose(action="syntax", type="TABL", name="ZAOG_OBJ_COVER")
  clean; activation succeeds
STOP_IF=table activation fails, or field lengths do not match the referenced
  zaog_obj_index/zaog_commit_hist columns exactly (byte-for-byte key
  compatibility is required for future joins)
```
### 3.0 `ZAOG_OBJ_INDEX` context identity — COMPLETE mode only (AR-1-01 fix; scope corrected in cycle 3, see §3.0b/AR-2-01)
Cycle 1 proved that scoping only the NEGATIVE fact table (`ZAOG_OBJ_COVER`)
by `CONTEXT_HASH` was not enough: `select_rows_for_filter` and
`is_index_ready` read `ZAOG_OBJ_INDEX` with no context predicate at all, so
a POSITIVE row (or the whole-commit `$IDX/__READY__` marker) written under
one `.abapgit`/devclass/algorithm context stays fully trusted and returned
after that context changes — the exact asymmetry the review demonstrated.
Fixing this only inside `ZAOG_OBJ_COVER` (§3.1-3.2) cannot close it, because
`select_rows_for_filter` never consults `ZAOG_OBJ_COVER` at all (§11 steps
1 and 4) — the positive rows must carry the same identity themselves.
**Decision (unchanged from cycle 2 for this table): add `CONTEXT_HASH` as a
new NON-KEY column to `ZAOG_OBJ_INDEX`** (not a key-field/DDIC-key change).
Cycle 2 scoped this fix to both COMPLETE mode (`rebuild_index`) and
FILTERED mode (`walk_filtered`) sharing this same non-key column; cycle 2's
own adversarial review (AR-2-01) proved the FILTERED-mode half of that was
unsafe — `walk_filtered` never purges before writing (it only ever
`MODIFY`s its own filter's rows, deliberately, so a second overlapping
filtered walk cannot destroy another caller's rows, §11 step 4), so a
non-key `CONTEXT_HASH` there lets context B's `MODIFY` silently overwrite
context A's row at the same primary key while A's `ZAOG_OBJ_COVER` row
survives untouched. **Cycle 3 narrows this section's scope to COMPLETE
mode only** — `rebuild_index` remains the sole writer of `ZAOG_OBJ_INDEX`,
and it is safe with a non-key `CONTEXT_HASH` specifically *because* it
always calls `invalidate_commit_index` (§5, §13 W4) to purge every row for
`(repo_key, commit)` — across every prior context — before writing a
single new row under its own context. There is no upsert-without-purge
code path against `ZAOG_OBJ_INDEX`, so no cross-context overwrite can ever
occur on this table; a non-key column is provably sufficient here.
FILTERED mode's positive rows move to a new, context-keyed table entirely
— see §3.0b (AR-2-01, cycle 3).
A non-key column requires no primary-key migration and no re-keying of
existing production rows — every row written before this change simply has
an initial (blank) `CONTEXT_HASH`, which is guaranteed to compare unequal
to any real `compute_context_hash( )` output (a 40-character lowercase-hex
SHA1 can never be all-initial/blank). Every pre-migration row is therefore
automatically, safely "context-unknown" — never wrongly matched, never
wrongly returned — at the cost of exactly one extra rebuild the first time
each already-`READY` commit is touched after upgrade (self-healing,
bounded, one-time per commit; the same "a missing row is never remote
absence" philosophy from §1, applied to context instead of existence).
```text
FILE_OR_OBJECT=zaog_obj_index (existing table, DDIC)
METHOD_OR_DDIC=DD03P_TABLE (append field, no key change)
ANCHOR=existing field list ending at IDX_STATUS (last non-key field per
  BASELINE_COMMIT read of zaog_obj_index.tabl.xml)
ACTION=insert
CHANGE=append one new NON-KEY field after the existing last field:
  CONTEXT_HASH CHAR40 (ROLLNAME pattern matching ZAOG_OBJ_COVER's own
  CONTEXT_HASH field, §3), NOT part of the primary key, initial value
  blank for all rows that exist before this change ships.
INVARIANTS=OS-INV-01
SQL_SHAPE=NONE (DDIC only; no data migration script — blank rows self-heal
  on first post-upgrade access per below)
ERROR_ROLLBACK_FALLBACK=NONE (additive, non-key column)
TESTS=covered by ltcl_obj_index additions in §13 W1-W3
VALIDATION=SAPDiagnose(action="syntax", type="TABL", name="ZAOG_OBJ_INDEX")
  clean; activation succeeds; existing ltcl_obj_index tests
  (marker_required_for_ready, index_no_cross_commit_leak,
  ready_rejects_other_commit, ready_accepts_exact_commit,
  index_chunk_boundary_ok, index_bulk_rows_preserved, index_empty_no_match)
  still PASS after updating their call sites to pass an explicit
  iv_context_hash (§13 W2-W3)
STOP_IF=table activation fails, or the field cannot be added without a key
  change (re-verify DD02V/TABCLASS at implementation time — this design
  requires a NON-KEY append, never a key-field insert)
```
`rebuild_index` (COMPLETE mode, unchanged trigger conditions) already
receives `io_dot`/`iv_devclass` (Q4) — it computes
`iv_context_hash = zcl_abapgit_ortec_obj_cover=>compute_context_hash(
iv_devclass, io_dot )` once at the top (identical call `walk_filtered`
makes, §11) and stamps it onto every `ZAOG_OBJ_INDEX` row it writes,
**including the `$IDX/__READY__` marker row**. `walk_filtered` **no longer
writes to `ZAOG_OBJ_INDEX` at all** (cycle 3, §3.0b) — its own positive
rows carry `CONTEXT_HASH` as a genuine KEY field on the separate
`ZAOG_OBJ_PIDX` table instead.
`ZAOG_OBJ_INDEX`'s readers become context-aware, scoped to the
COMPLETE-mode/warm-index path only (§11 step 3.1):
- `is_index_ready` gains a mandatory `iv_context_hash` parameter; its
  `SELECT SINGLE` predicate adds `AND context_hash = iv_context_hash`. A
  marker written under a different (or blank/legacy) context is invisible —
  `is_index_ready` correctly reports "not ready" and the caller proceeds
  through the normal coverage/walk path (§11), which re-establishes a
  correctly-stamped `READY` marker the next time a COMPLETE rebuild is
  actually triggered (§9 Slice 1b).
- `select_rows_for_filter` gains the same mandatory `iv_context_hash`
  parameter and predicate, chunked per §3.2/§8's `c_filter_chunk_size`
  (AR-1-04), and cycle 3 restricts it to the `is_index_ready = true`
  branch only (§11 step 3.1) — the FILTERED-mode branch now calls the new
  `select_partial_rows_for_filter` instead (§3.0b, §11 steps 3.4/4/5).
This closes the COMPLETE-mode half of AR-1-01 (the whole-commit marker
bypass, never actually reachable via an upsert-without-purge path); the
FILTERED-mode half (the per-object incremental upsert leak, reopened by
the reviewer as AR-2-01) is closed by §3.0b below.
### 3.0b Context-keyed partial positive rows — `ZAOG_OBJ_PIDX` (AR-2-01 fix, cycle 3)
**Counterexample this closes**: context A resolves object X at path P and
writes both a `ZAOG_OBJ_INDEX` row (non-key `CONTEXT_HASH=A`) and a
`ZAOG_OBJ_COVER(..., context_hash=A, FOUND)` row via `walk_filtered`.
Context B later resolves the same object/path under a different
`.abapgit`/devclass/algorithm context. Because `CONTEXT_HASH` was a
non-key column and `walk_filtered` only ever `MODIFY`s (never purges) its
own filter's rows, B's `MODIFY` overwrites the SAME primary-key row in
place and changes its `CONTEXT_HASH` to B, while A's `ZAOG_OBJ_COVER` row
survives untouched. A later A-context request reads `FOUND` coverage,
skips the walk, selects zero matching positive rows, and returns a false
empty result (AR-2-01).
**Decision: option (b) — move FILTERED-mode's positive rows out of
`ZAOG_OBJ_INDEX` into a new, context-keyed table, `ZAOG_OBJ_PIDX`.**
Rejected alternative: option (a), adding `CONTEXT_HASH` to
`ZAOG_OBJ_INDEX`'s own primary key. Option (a) requires a DDIC key change
(a real table conversion) to an EXISTING, already-shipped production table
whose COMPLETE-mode rows are not part of this bug at all (§3.0 proves
COMPLETE mode was never actually unsafe — it always purges before
writing). Option (b) touches zero existing rows/queries on
`ZAOG_OBJ_INDEX`, is the same additive-table risk class cycle 1 already
accepted for `ZAOG_OBJ_COVER`, and cleanly separates "the full-rebuild
answer" (`ZAOG_OBJ_INDEX`, one row set per commit, always fresh) from "one
context's incremental partial answer" (`ZAOG_OBJ_PIDX`, many row sets per
commit, one per context, individually addressable and individually
disposable) — the smaller, safer DDIC change.
```text
FILE_OR_OBJECT=zaog_obj_pidx (new transparent table, DDIC)
METHOD_OR_DDIC=DD02V/DD03P_TABLE
ANCHOR=none (new object); mirrors zaog_obj_index.tabl.xml's field list
  exactly, with CONTEXT_HASH inserted as a KEY field between OBJ_NAME and
  PATH_HASH so every read remains a primary-key-prefix match (repo_key,
  commit_sha1, obj_type, obj_name, context_hash) with only the trailing
  PATH_HASH left open to return multiple file rows per object — identical
  shape/reasoning to the original table's own key (§8)
ACTION=insert
CHANGE=
  DD02V: TABNAME=ZAOG_OBJ_PIDX, TABCLASS=TRANSP, CLIDEP=X,
    DDTEXT='ORTEC Git: Context-Scoped Partial Index Rows', CONTFLAG=A
    (matches ZAOG_OBJ_INDEX's own CONTFLAG)
  DD09L: BUFALLOW=N
  Fields (key order matches this list, mirrors zaog_obj_index.tabl.xml
    field-for-field with one insertion):
    1. CLIENT       ROLLNAME=MANDT              KEY
    2. REPO_KEY     CHAR12   (= ty_repo_key)     KEY
    3. COMMIT_SHA1  CHAR40   (= ty_sha1)         KEY
    4. OBJ_TYPE     CHAR4    (= TADIR-OBJECT)    KEY
    5. OBJ_NAME     CHAR40   (= TADIR-OBJ_NAME)  KEY
    6. CONTEXT_HASH CHAR40   (sha1 hex, §3.1)    KEY  <- new key position,
       inserted here (NOT after PATH_HASH), so the WHERE clause stays a
       contiguous primary-key prefix ending in the caller-supplied
       CONTEXT_HASH, with only PATH_HASH left open to return multiple rows
    7. PATH_HASH    CHAR40   (= ty_sha1)         KEY
    8. FILE_PATH    CHAR255
    9. FILE_NAME    CHAR255
    10. BLOB_SHA1   CHAR40
    11. TREE_SHA1   CHAR40
    12. IDX_STATUS  CHAR1
  No DD12V/DD17V secondary index — same conclusion as ZAOG_OBJ_INDEX (§8):
  every read is a primary-key-prefix FOR ALL ENTRIES.
INVARIANTS=OS-INV-01, OS-INV-05, OS-INV-10
SQL_SHAPE=NONE (DDIC only)
ERROR_ROLLBACK_FALLBACK=NONE (additive DDIC object; no data migration —
  table starts empty, no legacy-row concern applies here at all, unlike
  ZAOG_OBJ_INDEX's blank-context self-heal)
TESTS=none at this slice (table has no behavior; covered indirectly by the
  new obj_index tests in §9 Slice 1d)
VALIDATION=SAPDiagnose(action="syntax", type="TABL", name="ZAOG_OBJ_PIDX")
  clean; activation succeeds
STOP_IF=table activation fails, or any field length does not byte-match its
  ZAOG_OBJ_INDEX/ZAOG_OBJ_COVER counterpart exactly
```
`walk_filtered` (§11 step 4) writes exclusively to `ZAOG_OBJ_PIDX` now — it
never touches `ZAOG_OBJ_INDEX`. A tree leaf's row is appended to the write
buffer only if `line_exists( lt_filter[ ... ] )` (bounds writes to F_k, not
F), and every appended row's `CONTEXT_HASH` is the walk's own
`iv_context_hash`, as a genuine key component — a second, differently
contexted `walk_filtered` call for the same object/path physically
produces a SEPARATE row, never an overwrite. `MODIFY zaog_obj_pidx FROM
TABLE`, chunked identically to today's `rebuild_index` bulk-write idiom.
A new private reader, `select_partial_rows_for_filter`, mirrors
`select_rows_for_filter`'s exact shape but targets `ZAOG_OBJ_PIDX` and
always supplies `iv_context_hash` as a real key-equality predicate (not a
residual filter on a non-key column):
```text
FILE_OR_OBJECT=zcl_abapgit_ortec_obj_index.clas.abap
METHOD_OR_DDIC=select_partial_rows_for_filter (new private method)
ANCHOR=select_rows_for_filter's own body (§13 W2) as the pattern to mirror
ACTION=insert
CHANGE=
  CLASS-METHODS select_partial_rows_for_filter
    IMPORTING
      iv_repo_key     TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
      iv_commit       TYPE zif_abapgit_git_definitions=>ty_sha1
      iv_context_hash TYPE zif_abapgit_git_definitions=>ty_sha1
      it_filter       TYPE zif_abapgit_definitions=>ty_tadir_tt
    RETURNING
      VALUE(rt_rows) TYPE ty_index_rows_tt.
  " body (decision-free): loop it_filter in chunks of
  " zcl_abapgit_ortec_obj_cover=>c_filter_chunk_size (same idiom as
  " select_rows_for_filter, §13 W2), for each chunk:
  "   SELECT * FROM zaog_obj_pidx INTO TABLE @DATA(lt_chunk_rows)
  "     FOR ALL ENTRIES IN <chunk>
  "     WHERE repo_key      = iv_repo_key
  "       AND commit_sha1   = iv_commit
  "       AND obj_type      = <chunk>-object
  "       AND obj_name      = <chunk>-obj_name
  "       AND context_hash  = iv_context_hash
  "       AND idx_status    = 'R'.
  " then for each lt_chunk_rows line, MOVE-CORRESPONDING into a
  " zaog_obj_index-shaped work area (every field except CONTEXT_HASH has an
  " identical name/type on both tables, so MOVE-CORRESPONDING drops exactly
  " and only that one field) and APPEND to rt_rows. The returned shape is
  " therefore byte-identical to select_rows_for_filter's own output, so
  " build_files_from_rows (§11 step 5) requires ZERO changes.
INVARIANTS=OS-INV-01, OS-INV-04, OS-INV-05
SQL_SHAPE=chunked FOR ALL ENTRIES at c_filter_chunk_size, full
  primary-key-prefix (repo_key, commit_sha1, obj_type, obj_name,
  context_hash) per chunk row, same shape as select_rows_for_filter
ERROR_ROLLBACK_FALLBACK=NONE (pure read)
TESTS=partial_rows_context_disjoint (AR-2-01 direct retest: write a
  ZAOG_OBJ_PIDX row for object X under context A, a DIFFERENT row for the
  same X/path under context B, assert select_partial_rows_for_filter(...,
  context_hash=A) returns exactly A's row and select_partial_rows_for_
  filter(..., context_hash=B) returns exactly B's row — both coexist,
  neither overwrites the other), select_partial_rows_chunk_boundary
  (>5000 filter rows, AR-1-04 carried over to the new method)
VALIDATION=SAPDiagnose(action="unittest", type="CLAS",
  name="ZCL_ABAPGIT_ORTEC_OBJ_INDEX") PASS
STOP_IF=any field of zaog_obj_pidx fails to MOVE-CORRESPONDING cleanly onto
  a zaog_obj_index work area (re-verify field names/types byte-for-byte at
  implementation time)
```
This closes AR-2-01: positive rows and coverage facts now share the same
lifecycle/identity discipline — both `ZAOG_OBJ_COVER` and `ZAOG_OBJ_PIDX`
carry `CONTEXT_HASH` as a real key component, so a context can never
overwrite another context's fact on either table, and the two tables are
always written together (write_coverage/partial-row write both scoped to
the same `walk_filtered` call, same `iv_context_hash`, §11 step 4).
### 3.1 Canonical identity — `CONTEXT_HASH`
`CONTEXT_HASH` bundles every input `file_to_object` is sensitive to
(namespace/folder logic, `.abapgit` config, devclass, algorithm version) into
one deterministic key component, so a config change never silently reuses a
stale answer — it simply produces a **different** key, which naturally reads
as "no coverage row" (never as a wrong answer) without needing separate
stale-detection logic:
```text
FILE_OR_OBJECT=zcl_abapgit_ortec_obj_cover (new class)
METHOD_OR_DDIC=compute_context_hash
ANCHOR=none (new method)
ACTION=insert
CHANGE=
  CLASS-METHODS compute_context_hash
    IMPORTING iv_devclass    TYPE devclass
              io_dot         TYPE REF TO zcl_abapgit_dot_abapgit
    RETURNING VALUE(rv_hash) TYPE zif_abapgit_git_definitions=>ty_sha1
    RAISING   zcx_abapgit_exception.
  " body (decision-free):
  " lv_prefix_string = |{ c_algo_version }|{ iv_devclass }|.
  " lv_prefix_xstr   = zcl_abapgit_convert=>string_to_xstring_utf8( lv_prefix_string ).
  " lv_dot_xstr      = io_dot->serialize( ).   " confirmed existing method,
  "   returns the full canonical .abapgit XML bytes — covers folder logic,
  "   starting folder, ignore list, i18n languages, everything file_to_object
  "   can be sensitive to, without hand-picking individual getters.
  " rv_hash = zcl_abapgit_hash=>sha1_raw( lv_prefix_xstr && lv_dot_xstr ).
  CONSTANTS c_algo_version TYPE c LENGTH 4 VALUE '0001'.
INVARIANTS=(identity exactness — no field of the resolution context may be
  omitted from the hash input)
SQL_SHAPE=NONE
ERROR_ROLLBACK_FALLBACK=io_dot->serialize raises zcx_abapgit_exception on a
  malformed .abapgit — propagate unchanged; caller (get_files_for_filter)
  already has an outer CATCH zcx_abapgit_exception that falls back to
  get_files_remote, so a hash-computation failure degrades to the existing
  safe fallback, never a crash
TESTS=context_hash_stable_for_same_input, context_hash_changes_on_devclass,
  context_hash_changes_on_dot_abapgit_change, context_hash_changes_on_algo_bump
VALIDATION=unit test asserts two calls with identical inputs produce identical
  hashes and any single differing input changes the hash
STOP_IF=io_dot has no serialize() method with this exact signature at
  implementation time (re-verify against current source before coding — this
  design confirmed it exists as of BASELINE_COMMIT via direct source read)
```
### 3.2 Coverage read/write API
```text
FILE_OR_OBJECT=zcl_abapgit_ortec_obj_cover (new class, PUBLIC FINAL CREATE PUBLIC)
METHOD_OR_DDIC=get_coverage, write_coverage, get_diagnostics
ANCHOR=none (new class, mirrors zcl_abapgit_ortec_mat_state's style: no COMMIT
  WORK issued anywhere in this class — caller's LUW owns it)
ACTION=insert
CHANGE=
  TYPES: BEGIN OF ty_coverage,
           obj_type          TYPE zaog_obj_cover-obj_type,
           obj_name          TYPE zaog_obj_cover-obj_name,
           resolution_status TYPE zaog_obj_cover-resolution_status,
           walk_hist_level   TYPE zaog_obj_cover-walk_hist_level,
           resolved_at       TYPE zaog_obj_cover-resolved_at,
         END OF ty_coverage,
         ty_coverage_tt TYPE STANDARD TABLE OF ty_coverage WITH DEFAULT KEY.
  " resolved_at is new in cycle 2 so a caller can evaluate the AR-1-07
  " backoff window (§4.1, §11 step 3a) without a second SELECT.
  CONSTANTS: BEGIN OF cs_resolution,
               found                          TYPE c LENGTH 1 VALUE 'F',
               resolved_no_files              TYPE c LENGTH 1 VALUE 'N',
               resolved_not_present_remote    TYPE c LENGTH 1 VALUE 'D',
               unresolved_missing_local_data  TYPE c LENGTH 1 VALUE 'M',
               unresolved_ambiguous_mapping   TYPE c LENGTH 1 VALUE 'A',
             END OF cs_resolution.
  CONSTANTS c_filter_chunk_size TYPE i VALUE 5000.
  " Cycle 2 (AR-1-04): renamed from c_cover_write_chunk_size and now shared
  " by get_coverage's read, write_coverage's write, AND
  " zcl_abapgit_ortec_obj_index=>select_rows_for_filter's now-context-aware
  " FOR ALL ENTRIES (§3.0, §13 W3) — every new/changed SQL statement keyed
  " by a caller-supplied it_filter in this program chunks at this one named
  " constant. Deliberately NOT 30000 (c_index_write_chunk_size) — that value
  " was tuned for the F-row (whole-repo) MODIFY case this design explicitly
  " avoids reintroducing; 5000 is the original conservative E1-PERF-A value.
  CONSTANTS c_missing_data_backoff_seconds TYPE i VALUE 300.
  " AR-1-07: how long an UNRESOLVED_MISSING_LOCAL_DATA ('M') row suppresses
  " a repeat walk_filtered attempt for the same object before it is treated
  " as plain "uncovered" again (§4.1, §11 step 3a).
  CLASS-DATA gv_write_coverage_failures TYPE i.
  CLASS-DATA gv_last_write_coverage_error TYPE string.
  " AR-1-08: diagnosable, non-fatal write_coverage failure signal.
  CLASS-METHODS get_coverage
    IMPORTING iv_repo_key      TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
              iv_commit        TYPE zif_abapgit_git_definitions=>ty_sha1
              iv_context_hash  TYPE zif_abapgit_git_definitions=>ty_sha1
              it_filter        TYPE zif_abapgit_definitions=>ty_tadir_tt
    RETURNING VALUE(rt_coverage) TYPE ty_coverage_tt.
  " body (decision-free): IF it_filter IS INITIAL, RETURN. Else split
  " it_filter into chunks of c_filter_chunk_size (LOOP ... FROM ... TO, the
  " same idiom read_object_rows already uses for c_select_package_size) and
  " for each chunk:
  "   SELECT obj_type, obj_name, resolution_status, walk_hist_level,
  "     resolved_at
  "     FROM zaog_obj_cover APPENDING TABLE rt_coverage
  "     FOR ALL ENTRIES IN <chunk>
  "     WHERE repo_key      = iv_repo_key
  "       AND commit_sha1   = iv_commit
  "       AND obj_type      = <chunk>-object
  "       AND obj_name      = <chunk>-obj_name
  "       AND context_hash  = iv_context_hash.
  " (cycle 2/AR-1-04: APPENDING TABLE across chunks, never a single
  "  unchunked statement)
  CLASS-METHODS write_coverage
    IMPORTING iv_repo_key        TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
              iv_commit          TYPE zif_abapgit_git_definitions=>ty_sha1
              iv_context_hash    TYPE zif_abapgit_git_definitions=>ty_sha1
              iv_walk_hist_level TYPE zcl_abapgit_ortec_mat_state=>ty_hist_level
              it_results         TYPE ty_coverage_tt
    RAISING   zcx_abapgit_ortec_git.
  " body (decision-free): build zaog_obj_cover rows from it_results (+ the 4
  " fixed inputs + RESOLVED_AT = current timestamp — always the current
  " timestamp, never a caller-supplied it_results-resolved_at, since a write
  " always means "as of now" — + ALGO_VERSION = the literal embedded in
  " compute_context_hash), MODIFY (never INSERT — an overlapping re-walk for
  " a superset filter must upsert idempotently, same reasoning as
  " rebuild_index's own MODIFY usage) in chunks of c_filter_chunk_size. On
  " MODIFY failure (sy-subrc <> 0): ADD 1 TO gv_write_coverage_failures,
  " gv_last_write_coverage_error = the failure text, THEN raise
  " zcx_abapgit_ortec_git unchanged (the exception itself still carries the
  " failure to the immediate caller; the counter is for callers that,
  " per §6 trigger 4, deliberately swallow it).
  CLASS-METHODS get_diagnostics
    RETURNING VALUE(rs_diagnostics) TYPE ty_cover_diagnostics.
  " AR-1-08. body: return { failure_count = gv_write_coverage_failures,
  " last_error = gv_last_write_coverage_error }. Read-only, no side effect,
  " no reset — a future admin surface (e.g.
  " zcl_abapgit_ortec_cache_admin=>get_overview) can report it; this design
  " adds only the observable counter, not that UI wiring (out of
  " SOURCE_SCOPE).
INVARIANTS=OS-INV-01, OS-INV-02 (no obj_data anywhere in this table by
  construction — it has none), OS-INV-04, OS-INV-05, OS-INV-10
SQL_SHAPE=get_coverage: chunked FOR ALL ENTRIES at c_filter_chunk_size,
  5-column exact-key WHERE, no range; write_coverage: MODIFY ... FROM TABLE,
  chunked at c_filter_chunk_size
ERROR_ROLLBACK_FALLBACK=write_coverage failure raises zcx_abapgit_ortec_git;
  caller (§11 ensure_filtered_coverage) treats a coverage-write failure as
  non-fatal to the CALLER'S OWN request (the just-walked ZAOG_OBJ_INDEX rows
  and the file result are still valid and returned) but now increments the
  diagnosable counter above before re-swallowing (AR-1-08) — a missing
  coverage row only costs a future caller a repeat walk, never a
  correctness gap (matches "missing row != absence")
TESTS=coverage_round_trip, coverage_context_mismatch_excluded (a row written
  under context hash A is invisible to a get_coverage call using hash B),
  coverage_upsert_idempotent, coverage_write_chunk_boundary (>5000 rows,
  proves multiple statements execute and results are complete — AR-1-04),
  coverage_read_chunk_boundary (>5000-row it_filter on get_coverage —
  AR-1-04), write_coverage_failure_increments_counter (forces a MODIFY
  failure via a seam and asserts get_diagnostics( )-failure_count increased
  and files are still returned — AR-1-08), missing_data_marker_suppresses_
  retry_within_backoff, missing_data_marker_expires_after_backoff (both
  AR-1-07, see §11 step 3a)
VALIDATION=SAPDiagnose(action="unittest", type="CLAS",
  name="ZCL_ABAPGIT_ORTEC_OBJ_COVER") PASS
STOP_IF=any test requires a secondary index to pass performantly (would
  contradict §8's "no secondary index needed" conclusion — re-open §8 if so)
```
## 4. Safe resolution states
| State | Constant | Meaning | Proof required before writing it |
|---|---|---|---|
| FOUND | `cs_resolution-found` | ≥1 `ZAOG_OBJ_INDEX` row exists for this object at this commit | at least one tree leaf's `file_to_object` result matched during a walk that covered this object |
| RESOLVED_NO_FILES | `cs_resolution-resolved_no_files` | walk covered the object's identity and found zero matches, but the underlying tree data was **not** `GRAPH_COMPLETE` at walk time | `WALK_HIST_LEVEL <> 'G'/'F'` recorded alongside |
| RESOLVED_NOT_PRESENT_REMOTE | `cs_resolution-resolved_not_present_remote` | same zero-match result, but `zcl_abapgit_ortec_mat_state=>is_graph_have_eligible` was **true** for this commit at walk time (tree closure verified complete, per §11.4) | `WALK_HIST_LEVEL IN ('G','F')` recorded alongside — the row's own field is the proof, no re-derivation needed at read time |
| UNRESOLVED_MISSING_LOCAL_DATA | `cs_resolution-unresolved_missing_local_data` | the walk raised (`zcx_abapgit_ortec_git`: missing commit/tree object) before reaching a verdict for this object | **cycle 2/AR-1-07: actively written** — see §4.1/§11 step 4; the row's own `RESOLVED_AT` is the proof of *when* the failed attempt happened, nothing more |
| UNRESOLVED_AMBIGUOUS_MAPPING | `cs_resolution-unresolved_ambiguous_mapping` | `build_files_from_rows`'s existing `CORRUPT_OR_INCOMPLETE` case (a `blob_sha1` resolves to a non-blob type) | reserved, still not written in cycle 2 — see open question 1 |
| STALE_OR_VERSION_MISMATCH | *(not persisted)* | query-time-only label: a `ZAOG_OBJ_COVER` row exists but its `CONTEXT_HASH` differs from the caller's freshly-computed one | never written as a column value — `get_coverage`'s own `WHERE context_hash = iv_context_hash` makes a mismatched row invisible by construction, so a mismatch is indistinguishable from "no row" to the caller, which is the correct, safe behavior (§1 truth rule) |
`UNRESOLVED_AMBIGUOUS_MAPPING` remains reserved-but-unwritten in cycle 2
(unchanged rationale: would require restructuring `build_files_from_rows`'s
raise to carry the specific failing `(obj_type, obj_name)`, out of this
design's minimal-diff scope — open question 1). `UNRESOLVED_MISSING_LOCAL_DATA`
is promoted to actively written in cycle 2 to close AR-1-07 — see §4.1.
### 4.1 Backoff semantics for `UNRESOLVED_MISSING_LOCAL_DATA` (AR-1-07 fix)
The cycle-1 review proved that a missing commit/tree causes `walk_filtered`
to raise and fall back to `ii_repo_online->get_files_remote( )` (§6 trigger
2) — correct, but a bypassed-persistence pure read (Q9), so an **identical
repeat request** re-attempts the same doomed tree walk and pays the same
failure cost every time, forever, with no convergence.
**Fix**: `walk_filtered` now writes one `cs_resolution-
unresolved_missing_local_data` (`'M'`) coverage row, via `write_coverage`,
for every object in the `lt_filter` set it was asked to resolve **this
round**, immediately before re-raising the caught missing-commit/tree
exception (§11 step 4; this `write_coverage` call is itself best-effort —
a failure here is just another non-fatal `write_coverage` failure per §6
trigger 4/AR-1-08, and does not block the re-raise). `RESOLVED_AT` on that
row is the current timestamp; `WALK_HIST_LEVEL` is whatever this design's
Slice 3/4 default is at the time (§11.4) — irrelevant, since `'M'` rows are
never read as a resolution answer.
`ensure_filtered_coverage` (§11 step 3a, new) consults these rows purely as
a **scheduling hint**, never as a resolution fact:
- A filter object whose only `lt_coverage` row is `'M'` **and** whose
  `RESOLVED_AT >= ( current timestamp - c_missing_data_backoff_seconds )`
  is *live-backed-off*.
- A filter object whose only `lt_coverage` row is `'M'` but older than the
  backoff window is treated exactly like "no coverage row at all" —
  eligible for a fresh `walk_filtered` attempt, same as today.
- If **every** object in `lt_uncovered` is live-backed-off, `walk_filtered`
  is not called at all this round; `ensure_filtered_coverage` immediately
  raises the same `zcx_abapgit_exception` `walk_filtered` itself would have
  raised on a real attempt, so the existing, unmodified outer
  `CATCH zcx_abapgit_exception → get_files_remote( )` fallback (§6) fires —
  **no new fallback primitive, no new catch block**, just an earlier, cheap
  exit from a walk that would certainly fail again.
- If `lt_uncovered` is a **mix** of live-backed-off and genuinely-fresh
  objects, this design does **not** special-case the split — `walk_filtered`
  is still invoked for the full `lt_uncovered` set (always safe: identical
  cost to today's behavior in the worst case, never incorrect). The
  convergence guarantee this design provides is specifically the
  **repeated-identical-request** loop the review demonstrated, which only
  requires the all-backed-off case above; a partially-fresh mixed request
  gets no additional short-circuit, by explicit scope choice, not omission.
A later successful `walk_filtered` attempt (once the missing commit/tree
is fetched/repaired by any means) upserts a real `FOUND`/`RESOLVED_NO_FILES`/
`RESOLVED_NOT_PRESENT_REMOTE` row over the stale `'M'` row for that object,
superseding it — no separate cleanup step is required (bounded row count:
at most one coverage row per object per commit per context at any time,
same invariant §3 already establishes for every other resolution state).
## 5. Full-index interaction
- **Coexistence**: `ZAOG_OBJ_COVER` and the existing `$IDX/__READY__` marker
  are two independent signals. `is_index_ready` (now context-aware, §3.0) is
  never consulted by, nor consults, `ZAOG_OBJ_COVER`.
- **Promotion/replacement on full rebuild**: `rebuild_index` (still
  callable, reachable only via a future explicit admin trigger once Slice 3
  lands, §12) remains the only writer of the complete `F`-row catalog +
  marker, now also stamping `CONTEXT_HASH` on every row it writes (§3.0). It
  is **not** required to also write `ZAOG_OBJ_COVER` rows (a complete index
  under the CURRENT context already answers every object via
  `select_rows_for_filter`'s own SQL — `ZAOG_OBJ_COVER` would be redundant
  once `is_index_ready(..., iv_context_hash)` is true, which
  `ensure_filtered_coverage` checks first, §11). **Cycle 3**: `rebuild_index`'s
  existing pre-walk `invalidate_commit_index` call (§13 W4) now also purges
  `ZAOG_OBJ_PIDX` for that `(repo_key, commit)` across every context (§3.0b)
  — a COMPLETE rebuild always supersedes and discards any leftover
  FILTERED-mode partial rows for the commit, regardless of which context(s)
  produced them, so no orphaned partial row can outlive a full rebuild.
- **READY marker ordering**: unchanged — still written only as the provably
  last step of a walk that touched 100% of the tree and wrote 100% of its
  rows (`rebuild_index`'s existing final `MODIFY ls_row` for the marker is
  untouched, now additionally carrying `CONTEXT_HASH`, §3.0).
- **Invalidation on algorithm/config change (AR-1-01/AR-2-01, cycle 2/3)**:
  handled by `CONTEXT_HASH` on **all three** tables (§3.0, §3.0b, §3.1), but
  by two different mechanisms matched to each table's write pattern —
  `ZAOG_OBJ_INDEX` uses a non-key column (safe because `rebuild_index`
  always purges-then-rewrites the whole commit, §3.0); `ZAOG_OBJ_PIDX` and
  `ZAOG_OBJ_COVER` use a real KEY component (required because
  `walk_filtered` only ever upserts its own filter's rows without a purge,
  §3.0b) so a config/algorithm change under a new context can never
  overwrite a prior context's row on either table — it always produces a
  structurally distinct row. Every reader (`is_index_ready`,
  `select_rows_for_filter`, `select_partial_rows_for_filter`, and
  `get_coverage`) predicates on `iv_context_hash`, so a superseded-context
  row on any of the three tables becomes simply unreachable (never wrongly
  trusted), never overwritten and never a source of a false empty result.
  No explicit invalidation *sweep* is required for correctness —
  unreachability is structural, not time-based. A bounded periodic cleanup
  of orphaned old-context rows on any of the three tables is a
  non-blocking follow-up (open question 2), not required for correctness.
- **Atomic cross-table invalidation on stale-row purge (AR-1-02, cycle 2;
  extended cycle 3)**: the pre-cycle-2 design had `rebuild_index`'s own
  pre-walk purge and `get_files_for_filter`'s `CORRUPT_OR_INCOMPLETE` retry
  purge each issue a standalone `DELETE FROM zaog_obj_index WHERE repo_key
  = ... AND commit_sha1 = ...` with no matching `ZAOG_OBJ_COVER` cleanup —
  an unrelated object's `FOUND` coverage row could then survive while its
  backing `ZAOG_OBJ_INDEX` rows were gone, producing a false empty result on
  the next warm read. **Fix**: both call sites now go through one shared
  private helper, `invalidate_commit_index(iv_repo_key, iv_commit)` (§13
  W4), which deletes `ZAOG_OBJ_INDEX`, `ZAOG_OBJ_COVER`, **and (cycle 3)
  `ZAOG_OBJ_PIDX`** rows for that `(repo_key, commit)` — across every
  context, since a commit-scoped invalidation is intentionally
  context-blind — in the same LUW, with no `COMMIT WORK` of its own
  (consistent with this class's existing no-commit discipline — the
  caller's own LUW still owns atomicity/rollback). There is no longer any
  code path that can purge one of these three tables' rows for a commit
  without the other two. The cache-admin `clear_repo` path (AR-1-05/AR-2-03)
  is a **separate, whole-repo** cleanup surface with its own
  lock/transaction and is fixed independently in
  `obj_store_performance_design.md` (same underlying principle, now also
  covering all three tables and, per AR-2-03, the same canonical repo-mutex
  lock as `walk_filtered`/`rebuild_index`).
- **Stale-row handling after interruption**: a `ZAOG_OBJ_COVER` write for a
  **successful** walk only happens after `walk_filtered` (§11) completes
  without exception — an interrupted filtered walk leaves **zero**
  `FOUND`/`RESOLVED_*` coverage rows for the objects it was resolving (same
  all-or-nothing-per-request discipline `rebuild_index` already uses for the
  marker, just scoped to K instead of F). **Cycle 2 exception**: on the
  specific missing-commit/tree failure path, `walk_filtered` now writes
  short-lived `'M'` (`unresolved_missing_local_data`) rows before re-raising
  (§4.1, AR-1-07) — these are a scheduling hint, never a resolution fact,
  and are superseded by a real result on the next successful attempt.
- **How complete readers avoid mistaking partial for complete**: any consumer
  that needs "give me literally everything in this commit" (none exist in
  `SOURCE_SCOPE` today) must use `is_index_ready`/`rebuild_index`, never
  `ZAOG_OBJ_COVER` — `ZAOG_OBJ_COVER` has no API that returns "all objects",
  only "the objects I asked about" (`get_coverage` requires `it_filter`).
- **Concurrent partial/full builds**: `walk_filtered` (§11) acquires the
  **same** `zcl_abapgit_ortec_pack_raw=>acquire_repo_lock` as today's
  `rebuild_index`, for the same reason (tree-walk + bulk write consistency) —
  no new concurrency model, no new lock granularity. A concurrent complete
  rebuild and a filtered walk for the same repo serialize on the existing
  repo-wide lock exactly as two concurrent `rebuild_index` calls do today
  (Q11) — no regression, no new race surface.
- **Cache-admin clear vs. filtered/complete writers (AR-2-03, cycle 3)**:
  cycle 2 left `zcl_abapgit_ortec_cache_admin=>clear_repo` and
  `walk_filtered`/`rebuild_index` on two non-conflicting locks —
  `ENQUEUE_EZAOG_REPO_LOCK` (session_id = `iv_repo_key`) for `clear_repo`
  versus the `ZAOG_FETCH_SESS`-row mutex `acquire_repo_lock`
  (session_id = `LOCK_<repo_key>`) for the writers — so a `clear_repo` could
  physically interleave with a concurrent `walk_filtered`'s index-then-
  cover write sequence and leave `ZAOG_OBJ_COVER FOUND` with no backing
  positive rows. **Fix**: `zcl_abapgit_ortec_pack_raw=>acquire_repo_lock`/
  `release_repo_lock` is declared the ONE canonical lock for
  `ZAOG_OBJ_INDEX`/`ZAOG_OBJ_COVER`/`ZAOG_OBJ_PIDX`. `clear_repo` keeps its
  own enqueue lock (it still serializes concurrent whole-repo admin clears
  across every OTHER ORTEC cache table too, e.g. `ZAOG_OBJ_STORE`,
  `ZAOG_COMMIT_HIST`, `ZAOG_REPO_STATE` — an unrelated concern this fix does
  not touch) but now additionally acquires `acquire_repo_lock` immediately
  before, and releases it immediately after, its three `DELETE`s against
  these three tables specifically (full mechanics and exact call site in
  `obj_store_performance_design.md`'s cache-admin section). Acquire order
  is fixed and one-directional — enqueue lock (whole method) outer,
  `acquire_repo_lock` (three deletes only) inner — and is deadlock-free by
  construction because `walk_filtered`/`rebuild_index` never acquire the
  enqueue lock, so no cycle can form between the two lock types.
- **Rollback/retry**: unchanged LUW discipline — every write in this design
  (`ZAOG_OBJ_INDEX` rows, `ZAOG_OBJ_COVER` rows, `invalidate_commit_index`'s
  deletes) happens in the caller's own LUW with no `COMMIT WORK` issued by
  either class, exactly like today's `rebuild_index`/`store_object`. A
  caller-level rollback discards both tables' writes/deletes together,
  atomically, with no partial-publication window.
## 6. Fallback policy
Exact, bounded trigger list — never "every cold request":
1. `io_dot->serialize()` (inside `compute_context_hash`) raises
   `zcx_abapgit_exception` → propagate to `get_files_for_filter`'s existing
   outer `CATCH zcx_abapgit_exception` → `get_files_remote()` (unchanged
   standard fallback).
2. `walk_filtered` (§11) raises (missing commit/tree object, decode failure —
   same conditions `rebuild_index` raises on today) → same outer fallback as
   (1). **Cycle 2/AR-1-07**: before re-raising, `walk_filtered` writes one
   `'M'` (`unresolved_missing_local_data`) coverage row per object it was
   asked to resolve this round (§4.1) — a best-effort write, itself subject
   to trigger 4 below. `ensure_filtered_coverage` additionally short-circuits
   straight to this same fallback, without calling `walk_filtered` at all,
   whenever every currently-uncovered filter object already has a live
   (unexpired) `'M'` row from a prior attempt (§4.1, §11 step 3a) — this is
   the mechanism that stops a repeated identical request from re-attempting
   the same doomed walk forever.
3. `build_files_from_rows`'s existing `CORRUPT_OR_INCOMPLETE` raise (a
   resolved blob is not actually type `blob`) → **unchanged**: the existing
   `get_files_for_filter` retry-once behavior is preserved, but its retry now
   calls `ensure_filtered_coverage` instead of `ensure_index` (§9 Slice 3),
   so the retry no longer forces a full `F`-row rebuild either. **Cycle
   2/AR-1-02, extended cycle 3**: the retry's stale-row purge now calls the
   shared `invalidate_commit_index` helper (§5, §13 W4) instead of a
   standalone `DELETE FROM zaog_obj_index`, so the matching `ZAOG_OBJ_COVER`
   **and `ZAOG_OBJ_PIDX`** rows for that commit are purged in the same LUW —
   no orphaned coverage or partial-index row can survive this retry.
4. `write_coverage` raising `zcx_abapgit_ortec_git` → **non-fatal** to the
   current request (§3.2) — the already-resolved file result is still
   returned; no fallback triggered, no exception surfaces to
   `get_files_for_filter`'s caller. **Cycle 2/AR-1-08**: this failure now
   also increments `zcl_abapgit_ortec_obj_cover`'s diagnosable counter
   (§3.2) before being swallowed — the non-fatal behavior itself is
   unchanged.
No other trigger exists. In particular: a `ZAOG_OBJ_COVER` cache miss (no row,
a context-hash mismatch, or an expired `'M'` row) is **not** a fallback
trigger — it simply means "go compute it" (§11), staying entirely inside the
ORTEC fast path.
## 7. Performance requirements (quantified)
Notation: **K** = caller's filter size, **F** = 42000 (total files at the
target commit, per mission), **F_k** = files actually matching the K
requested objects.
**Expected production cardinality**: K ∈ {1, 100, 5000}; F = 42000; repo-wide
`ZAOG_OBJ_STORE` up to 1,000,000 objects; a second branch sharing 95–98% of
blob content with the first.
| Scenario | SQL statements | HTTP requests | Rows written | Peak bytes (walk-time) |
|---|---|---|---|---|
| K=1, cold commit, no coverage, no `$IDX` marker | 1 commit `get_objects` (chunked, 1 stmt) + L tree-level `get_objects` (1 per BFS level, unchanged from today) + 1 `get_coverage` (0 rows back) + ≤1 `MODIFY zaog_obj_pidx` (≤1 row, well under 5000/30000 chunk) + ≤1 `MODIFY zaog_obj_cover` (1 row) + 1 blob `get_objects` (≤ a few blobs) | 0–1 (unchanged best-effort top-up) | **≤1** `ZAOG_OBJ_PIDX` row + **1** `ZAOG_OBJ_COVER` row (vs. 42001 `ZAOG_OBJ_INDEX` rows today) | bounded by widest tree level's node count (unchanged) — **no** F-row (30000-chunk) buffer needed |
| K=100, cold | same shape, chunk counts unchanged (100 ≪ 5000) | 0–1 | ≤~a few hundred `ZAOG_OBJ_PIDX` rows (F_k) + 100 `ZAOG_OBJ_COVER` rows (1 stmt) | same tree-level bound as K=1 |
| K=5000, cold | `get_coverage`/`write_coverage`/`ZAOG_OBJ_PIDX` write each exactly 1 chunk boundary (5000 = `c_filter_chunk_size`) | 0–1 | F_k `ZAOG_OBJ_PIDX` rows + 5000 cover rows (1 write chunk) | same tree-level bound; still strictly ≤ today's 30000-row COMPLETE-mode buffer |
| Any K, warm via coverage (`ZAOG_OBJ_COVER`/`ZAOG_OBJ_PIDX` rows exist, context hash matches) | 1 `get_coverage` (K rows) + 1 `select_partial_rows_for_filter` (F_k rows, `ZAOG_OBJ_PIDX`) + 1 blob `get_objects` | 0 | 0 (read-only) | F_k blob payload only — **zero tree decode**, matches today's already-optimal "Index: complete (warm)" cost class, now reachable without ever having paid for a full F-row write |
| Any K, warm via COMPLETE index (`is_index_ready` true) | 1 `select_rows_for_filter` (F_k rows, `ZAOG_OBJ_INDEX`) + 1 blob `get_objects` | 0 | 0 (read-only) | F_k blob payload only — unchanged from today's COMPLETE-mode warm path |
| 1,000,000 stored objects (repo-wide `ZAOG_OBJ_STORE`) | no change — every predicate in this design is `repo_key + commit_sha1 + obj_type + obj_name (+ context_hash)`, primary-key-exact; no predicate scans by total object count | unaffected | unaffected | unaffected — confirms O(K)/O(F_k), not O(total store size) |
| 95–98% blob-shared second branch, K=1 cold on branch B | **unchanged** from a same-K cold walk on an unrelated commit — tree SHA1s differ per branch even when blob content is shared (no cross-commit tree memo reuse, §2 candidate F explicitly avoided); blob-level sharing helps via existing `get_present_sha1s`/`mt_cache` (fewer new `ZAOG_OBJ_STORE` writes) but does **not** reduce tree-walk SQL/CPU cost | 0–1 (only for the 2–5% genuinely new blobs) | same shape as any cold K=1 | same bound |
**SQL-call complexity**: O(1) for warm coverage; O(L) tree-level calls
(unchanged from today) + O(1) coverage read/write for cold, where L is BFS
depth, never O(F).
**HTTP-call complexity**: 0 for warm; 0–1 best-effort top-up for cold
(unchanged existing mechanism, scoped to the missing blob subset only).
**Row/byte batch policy**: `c_select_package_size=1000` (existing, object
store), `c_index_write_chunk_size=30000` (existing, COMPLETE-mode only,
untouched), `c_cover_write_chunk_size=5000` (new, §3.2).
**Oversized-object behavior**: unchanged — governed entirely by the existing
object-store/blob-fetch machinery (`c_max_batch_response_bytes` etc. in
`zcl_abapgit_ortec_cold_init`), not modified by this design.
**Peak-memory model**: FILTERED-mode walk's peak buffer is `MIN(F_k, 5000)`
rows for `ZAOG_OBJ_PIDX`/`ZAOG_OBJ_COVER` writes — provably ≤ today's
COMPLETE-mode peak (bounded by 30000), a strict improvement, never a
regression. Tree-level in-memory working set (widest BFS level) is
**unchanged**.
**Cache scope**: existing session-static `zcl_abapgit_ortec_obj_store::mt_cache`
only — **no new cache is introduced** (see `obj_store_performance_design.md`
§OS-B for why a new cross-phase cache is deliberately not built here).
**Transaction owner**: `zcl_abapgit_ortec_obj_index=>get_files_for_filter`
remains the sole LUW owner, exactly as today — neither
`zcl_abapgit_ortec_obj_cover` nor the modified `obj_index` methods issue
`COMMIT WORK`.
**Large-repository acceptance criteria**: for F=42000 and 1,000,000 total
stored objects, a K=1 request must (a) never write more than O(1)
`ZAOG_OBJ_PIDX` rows + O(1) `ZAOG_OBJ_COVER` row on first cold access, (b)
never issue an unchunked SQL statement, (c) on any subsequent access with an
unchanged `.abapgit`/devclass (i.e. an unchanged `CONTEXT_HASH`), resolve via
exactly 2 SQL statements (`is_index_ready` implicitly folded into the
context-aware `get_coverage`/`select_partial_rows_for_filter` pair) plus the
blob fetch, with **zero** tree decode.
**Cycle 2 cost additions (AR-1-01/AR-1-04/AR-1-07)**: adding the
`context_hash` predicate to `is_index_ready`/`select_rows_for_filter` (§3.0)
adds one equality condition to an already primary-key-prefixed read — no
new statement, no new index, no measurable cost change. Chunking
`get_coverage`/`write_coverage`/`select_rows_for_filter` at
`c_filter_chunk_size` (AR-1-04) does not change the statement count for any
K ≤ 5000 (the entire K range named in this design's cardinality table
above) — it only bounds the K > 5000 case that was previously unbounded and
un-tested. The AR-1-07 backoff check (§4.1) adds at most one extra
in-memory comparison per uncovered filter object (against the `RESOLVED_AT`
already returned by the same `get_coverage` call) — no new SQL statement.
**Cycle 3 cost additions (AR-2-01/AR-2-02/AR-2-03)**: `ZAOG_OBJ_PIDX` is a
new, empty table at ship time — splitting FILTERED-mode's writes/reads onto
it changes the TARGET table of an already-planned statement, not the
statement count or shape (still one chunked `MODIFY`/`FOR ALL ENTRIES` at
`c_filter_chunk_size`, §3.0b). Threading `iv_current_remote` through
`get_files_for_filter`/`ensure_filtered_coverage`/`walk_filtered` (AR-2-02)
adds zero new SQL/HTTP calls — it is a single already-computed SHA1 value
passed by reference; `get_current_remote( )` itself is unchanged and was
already characterized as a zero-new-I/O accessor in cycle 2 (§11.4), only
its call site moves from inside `obj_index` to inside `filter_walk`, where
it sits alongside that class's other existing `li_repo_online` accessor
calls (`get_url`, `get_package`, `get_dot_abapgit`). Wrapping `clear_repo`'s
three deletes in `acquire_repo_lock` (AR-2-03) adds the identical bounded
retry/backoff cost profile (≤7 attempts, exponential 50ms base, ≤2s cap)
`rebuild_index`/`walk_filtered` already pay today for the same lock — no new
cost class, and it only applies to the already-infrequent admin clear
operation, never to the per-request read/write path.
## 8. DDIC index review
- **`ZAOG_OBJ_INDEX`**: **no new index** even after the cycle-2 `CONTEXT_HASH`
  non-key column addition (§3.0) — the COMPLETE-mode-only read in this
  design (`select_rows_for_filter`, used exclusively by §11 step 3.1) still
  uses `FOR ALL ENTRIES` on the full primary-key prefix `repo_key,
  commit_sha1, obj_type, obj_name` (chunked at `c_filter_chunk_size`,
  AR-1-04) with `context_hash` applied as a residual equality filter on an
  already narrow, primary-key-covered row set — a non-key equality
  predicate on top of a primary-key-prefixed read needs no secondary index
  to stay cheap. `rebuild_index`'s writes are `MODIFY`, not predicate-driven
  reads, and `invalidate_commit_index`'s deletes (§5, §13 W4) are
  primary-key-prefix `DELETE`s, also covered. Cycle 3: `walk_filtered` no
  longer reads or writes this table at all (§3.0b) — its access pattern is
  strictly narrower than cycle 2's, not wider.
- **`ZAOG_OBJ_PIDX`** (new table, cycle 3, §3.0b): **no secondary index**.
  `select_partial_rows_for_filter` is a chunked `FOR ALL ENTRIES` on the
  full primary-key prefix `repo_key, commit_sha1, obj_type, obj_name,
  context_hash` (6 of 7 key components — `MANDT` is implicit, `PATH_HASH`
  is the one trailing, intentionally open key component that lets one
  object return multiple file rows) — an exact-match, primary-key-covered
  read by construction, identical shape/reasoning to `ZAOG_OBJ_COVER`
  below. `walk_filtered`'s writes are `MODIFY`, not predicate-driven reads;
  `invalidate_commit_index`'s delete (§5, §13 W4) is a primary-key-prefix
  `DELETE` (repo_key + commit_sha1 only, intentionally context-blind for a
  full commit-scoped purge), also covered.
- **`ZAOG_OBJ_COVER`** (new table, §3): **no secondary index**. `get_coverage`
  is a chunked `FOR ALL ENTRIES` on the full primary-key prefix
  `repo_key, commit_sha1, obj_type, obj_name, context_hash` (5 of 6 key
  components — `MANDT` is implicit) — an exact-match, primary-key-covered
  read by construction, since `it_filter` always supplies concrete
  `obj_type`/`obj_name` values and `iv_context_hash` is always a single fixed
  value per call. Chunking (AR-1-04) splits the row count per statement, not
  the key shape — still no range scan, no partial-prefix scan anywhere in
  this design.
- **`ZAOG_OBJ_STORE`**'s one flagged `ddic_index_candidate`
  (`get_known_commits`'s `(repo_key, obj_type, status)` predicate, Q14) is
  **out of scope for this table** — it belongs to the Object Store design;
  see `obj_store_performance_design.md` §OS-G for its disposition
  (`REJECT_WITH_SOURCE_PROOF`, not on this integrated path).
## 9. Weak-model change list (Slices 1–4) & commit slices
**Recommended commit-slice boundaries** (justification: Slice 1 is a pure,
independently-testable addition with zero behavior change and zero risk to
existing `ltcl_obj_index` tests; Slice 1b closes the COMPLETE-mode half of
AR-1-01 before any caller is wired to the new coverage table, isolating the
signature-breaking (but behavior-preserving-under-a-stable-context)
`ZAOG_OBJ_INDEX` change so it can be reviewed/tested on its own; Slice 1d
(cycle 3) closes AR-2-01 by introducing `ZAOG_OBJ_PIDX` before Slice 3 wires
any writer to it; Slice 2 unlocks the warm-coverage fast path with zero
change to walk/write volume; Slice 3 is the only slice that changes
persisted-row volume and therefore carries the archaeology's
`c8fbdf23`/`cd0b277d` precedent risk — kept separate so its regression tests
can be reviewed on their own; Slice 4 is deferred because it is the
**first-ever** wiring between `mat_state` and `obj_index`/`filter_walk`, per
Q6, and deserves isolated IT8 validation before any caller depends on the
stronger `RESOLVED_NOT_PRESENT_REMOTE` state):
- **Slice 1 (Index-A)** — `ZAOG_OBJ_COVER` DDIC (§3) + new class
  `ZCL_ABAPGIT_ORTEC_OBJ_COVER` (§3.1, §3.2) with its own `ltcl_obj_cover`
  test include. No caller wired. Zero behavior change to any existing class.
- **Slice 1b (Index-A2, new in cycle 2, closes the COMPLETE-mode half of
  AR-1-01)** — `ZAOG_OBJ_INDEX` DDIC append of the non-key `CONTEXT_HASH`
  column (§3.0); `is_index_ready`, `select_rows_for_filter`, and
  `rebuild_index` gain the mandatory `iv_context_hash` parameter and the new
  predicate/stamp (§13 W1-W2, W6). Existing `ltcl_obj_index` tests
  (`marker_required_for_ready`, `index_no_cross_commit_leak`,
  `ready_rejects_other_commit`, `ready_accepts_exact_commit`,
  `index_chunk_boundary_ok`, `index_bulk_rows_preserved`,
  `index_empty_no_match`) are updated to pass an explicit (any fixed)
  context hash on both write and read sides — behavior is unchanged for a
  caller that always supplies the same context, which is exactly what every
  existing test and caller does today. New tests:
  `ready_rejects_different_context`, `select_rows_excludes_other_context`,
  `blank_legacy_context_is_never_ready` (proves a pre-migration blank-context
  marker/row is correctly treated as not-ready/not-selected). Depends on
  Slice 1 only for the shared `compute_context_hash` helper — no dependency
  on Slice 2/3's caller wiring.
- **Slice 1c (Store-B, new in cycle 2, closes AR-1-05, extended cycle 3 for
  AR-2-03, cross-referenced)** — `zcl_abapgit_ortec_cache_admin=>clear_repo`'s
  `ZAOG_OBJ_COVER`/`ZAOG_OBJ_PIDX` cleanup, now additionally acquiring
  `zcl_abapgit_ortec_pack_raw=>acquire_repo_lock` around those two deletes
  plus the existing `ZAOG_OBJ_INDEX` delete; full spec in
  `obj_store_performance_design.md`'s cache-admin section. Depends on
  Slice 1's DDIC (`ZAOG_OBJ_COVER` must exist) and Slice 1d's DDIC
  (`ZAOG_OBJ_PIDX` must exist). Independent of Slice 1b/2/3 otherwise — may
  land in parallel with them once both dependency DDIC objects exist.
- **Slice 1d (Index-A3, new in cycle 3, closes AR-2-01)** — `ZAOG_OBJ_PIDX`
  DDIC (§3.0b) + new private method `select_partial_rows_for_filter` on
  `zcl_abapgit_ortec_obj_index` (§3.0b). No caller wired yet (Slice 3 wires
  `walk_filtered` to write this table and `ensure_filtered_coverage` to read
  it via this method). New test: `partial_rows_context_disjoint` (direct
  AR-2-01 retest, §3.0b), `select_partial_rows_chunk_boundary`. Depends on
  Slice 1 only (shared `compute_context_hash`/`c_filter_chunk_size`); no
  dependency on Slice 1b, 2, or 3.
- **Slice 2 (Index-B)** — wire `zcl_abapgit_ortec_obj_index` to consult
  coverage **before** any walk (§11 steps 1–5, the "warm" fast path only).
  New method `ensure_filtered_coverage` added, now with its full cycle-3
  signature including `iv_current_remote OPTIONAL` (§14 W12 — threaded but
  unread until Slice 4, same pattern §11.4 already uses for
  `iv_walk_hist_level`); `get_files_for_filter` calls it instead of
  `ensure_index` for its primary (non-retry) call, now passing the
  `iv_context_hash` Slice 1b's `is_index_ready`/`select_rows_for_filter`
  require. If coverage is incomplete, `ensure_filtered_coverage` falls
  through to the **existing** `ensure_index`/`rebuild_index` (still COMPLETE
  mode, Slice 1b-updated) — i.e. this slice adds the fast path but does
  **not yet** change cold-walk write volume. `zcl_abapgit_ortec_filter_walk`'s
  `get_remote_files_for_stage` also gains its `iv_current_remote` computation
  and pass-through in this slice (§14 W12) — a signature-only, always-safe
  change, since nothing reads the value until Slice 4.
- **Slice 3 (Index-C)** — introduce `walk_filtered` (§11), now writing
  exclusively to `ZAOG_OBJ_PIDX` (Slice 1d's table, §3.0b) via the new
  `select_partial_rows_for_filter` for its own re-select, and switch
  `ensure_filtered_coverage`'s incomplete-coverage branch to call
  `walk_filtered` instead of `rebuild_index`. This is the slice that
  actually removes the O(F) write amplification for K=1. Also introduces,
  in the same slice (both are small, tightly-coupled fixes to the same
  retry/invalidation path):
  - the shared `invalidate_commit_index` helper (§5, §13 W4, closes AR-1-02,
    extended cycle 3 to a three-table delete), replacing both
    `rebuild_index`'s own pre-walk purge and `get_files_for_filter`'s
    `CORRUPT_OR_INCOMPLETE` retry purge;
  - `c_filter_chunk_size`-bounded chunking of `get_coverage`/`write_coverage`/
    `select_partial_rows_for_filter` (§3.2, §13 W2, closes AR-1-04);
  - the `'M'`/backoff mechanics (§4.1, §11 step 3a/step 4, §13 W5, closes
    AR-1-07).
  Requires: `index_no_cross_commit_leak`, `marker_required_for_ready`,
  `ready_rejects_other_commit` (existing tests) re-verified unaffected (they
  exercise `rebuild_index`/`is_index_ready` directly, untouched by this
  slice beyond Slice 1b's context parameter) **plus** new tests
  `filtered_walk_writes_only_requested_objects`,
  `filtered_walk_never_sets_ready_marker`,
  `filtered_walk_idempotent_on_overlap`,
  `filtered_walk_writes_context_hash_as_key` (AR-2-01, replaces cycle 2's
  `filtered_walk_stamps_context_hash`),
  `filtered_walk_no_cross_context_overwrite` (AR-2-01 direct retest: two
  `walk_filtered` calls under contexts A and B for the same object/path
  both leave a readable, distinct `ZAOG_OBJ_PIDX` row — neither overwrites
  the other, and a later A-context request under `FOUND` coverage still
  returns A's file),
  `retry_purge_removes_all_three_tables` (AR-1-02/AR-2-01, renamed from
  cycle 2's `retry_purge_removes_both_tables`),
  `coverage_write_chunk_boundary`/`coverage_read_chunk_boundary`/
  `select_partial_rows_chunk_boundary` (AR-1-04, all >5000 rows),
  `missing_tree_writes_m_row_then_reraises`,
  `repeat_request_within_backoff_skips_walk`,
  `repeat_request_after_backoff_retries_walk` (AR-1-07).
- **Slice 4 (Index-D)** — wire `zcl_abapgit_ortec_mat_state=>is_graph_have_eligible`
  into `walk_filtered` to enable the `RESOLVED_NOT_PRESENT_REMOTE` upgrade
  (§4, §11.4), **now gated by the AR-2-02 current-remote value comparison**
  (§11.4, §13 W8: `iv_current_remote IS NOT INITIAL AND iv_commit =
  iv_current_remote` required before the strong state may be written — no
  object reference, no `ii_repo_online` dependency inside `obj_index`).
  Isolated because it is a new cross-class dependency with no existing
  precedent (Q6). Until this slice ships, every zero-match result from
  Slice 3 is conservatively recorded as the weaker `RESOLVED_NO_FILES`
  (always safe, per §1's truth rules — never a regression, just a missed
  strengthening opportunity). New test:
  `not_present_remote_requires_current_remote_commit` (AR-2-02, updated to
  assert against the `iv_current_remote` parameter instead of a mocked
  `ii_repo_online`), `pull_filtered_never_writes_not_present_remote`
  (AR-2-02 structural proof: `pull_filtered`'s call chain never supplies
  `iv_current_remote`, so every `walk_filtered` invocation it triggers must
  record `RESOLVED_NO_FILES` for every zero-match object, never the strong
  state, regardless of graph completeness).
`obj_store_performance_design.md` §"Weak-model change list" adds
cross-referenced slices: Store-A (verification-only, unchanged from cycle 1)
and Store-B (cache-admin `ZAOG_OBJ_COVER`/`ZAOG_OBJ_PIDX` cleanup plus the
AR-2-03 lock unification, cycle 2/3).
## Integrated small-K path
### 11. End-to-end flow (K → files)
1. `filter_walk=>get_remote_files_for_stage`/`get_remote_files_for_diff`
   resolve `repo_key`/`commit` exactly as today (unchanged) and call
   `zcl_abapgit_ortec_obj_index=>get_files_for_filter`. **[NEW, cycle
   3/AR-2-02]**: `get_remote_files_for_stage`, which already holds
   `li_repo_online TYPE REF TO zif_abapgit_repo_online` in scope, computes
   `lv_current_remote` via `TRY. lv_current_remote =
   li_repo_online->get_current_remote( ). CATCH zcx_abapgit_exception. CLEAR
   lv_current_remote. ENDTRY.` (best-effort — a failure here degrades to
   "current remote unknown", never blocks the request) and passes it as the
   new `iv_current_remote` parameter to `get_files_for_filter` (§14 W12).
   `pull_filtered` has no online repo object at all and never supplies this
   parameter — it is structurally always initial on that call path (§11.4).
2. **[NEW]** `get_files_for_filter` computes
   `iv_context_hash = zcl_abapgit_ortec_obj_cover=>compute_context_hash(
   iv_devclass, io_dot)` (§3.1) — pure, no I/O beyond `io_dot->serialize()`.
3. `get_files_for_filter` calls the **new**
   `ensure_filtered_coverage(iv_repo_key, iv_commit, io_dot, iv_devclass,
   lt_filter, iv_context_hash, iv_current_remote)` instead of today's
   unconditional `ensure_index`:
   1. `IF is_index_ready(iv_repo_key, iv_commit, iv_context_hash) =
      abap_true` → `RETURN select_rows_for_filter(iv_repo_key, iv_commit,
      lt_filter, iv_context_hash)` — reads `ZAOG_OBJ_INDEX` only (today's
      already-optimal warm-complete path, now context-checked,
      §3.0/AR-1-01).
   2. Else `lt_coverage = zcl_abapgit_ortec_obj_cover=>get_coverage(
      iv_repo_key, iv_commit, iv_context_hash, lt_filter)` (chunked at
      `c_filter_chunk_size`, §3.2/AR-1-04).
   3. Partition `lt_filter` into `lt_uncovered` (no matching `lt_coverage`
      row for that `obj_type`/`obj_name`, OR only an expired `'M'` row, §4.1)
      vs. the covered remainder. A covered-`RESOLVED_NO_FILES`/
      `RESOLVED_NOT_PRESENT_REMOTE` object contributes **zero** rows and
      needs no further action — `SAFE NEGATIVE RESOLUTION FACTS` in action.
   3a. **[NEW, AR-1-07]** `IF lt_uncovered IS NOT INITIAL AND` every entry in
       `lt_uncovered` has a matching `lt_coverage` row with
       `resolution_status = cs_resolution-unresolved_missing_local_data AND
       resolved_at >= ( current timestamp - c_missing_data_backoff_seconds )`
       → do **not** call `walk_filtered`; `RAISE EXCEPTION TYPE
       zcx_abapgit_exception` (the same exception type `walk_filtered` would
       have raised on a real attempt), so the existing, unmodified outer
       `CATCH zcx_abapgit_exception → get_files_remote( )` fallback (§6
       trigger 2) fires without re-attempting the doomed walk (§4.1 full
       spec). A mixed backed-off/fresh `lt_uncovered` does not take this
       branch (§4.1 explicit scope limit) and falls through to step 5
       as normal.
   4. `IF lt_uncovered IS INITIAL` → `RETURN select_partial_rows_for_filter(
      iv_repo_key, iv_commit, iv_context_hash, lt_filter)` — reads
      `ZAOG_OBJ_PIDX` (§3.0b/AR-2-01), **zero tree walk**, exactly the "warm
      via coverage" row of §7's table. `select_partial_rows_for_filter`'s
      own SQL already returns 0 rows for the negative objects, no
      special-casing needed.
   5. Else → `walk_filtered(iv_repo_key, iv_commit, io_dot, iv_devclass,
      lt_filter, iv_context_hash, iv_current_remote)` (§11.4), then `RETURN
      select_partial_rows_for_filter(iv_repo_key, iv_commit, iv_context_hash,
      lt_filter)` (re-select the rows the walk just wrote from
      `ZAOG_OBJ_PIDX` — reuses the Slice 1d SELECT, §3.0b).
4. **[NEW]** `walk_filtered` (private, `zcl_abapgit_ortec_obj_index`):
   acquires the **same** `acquire_repo_lock` as `rebuild_index`; re-checks
   `is_index_ready(..., iv_context_hash)` after acquiring the lock (mirrors
   `rebuild_index`'s own double-check, avoids a redundant walk if a
   concurrent COMPLETE rebuild finished first); performs the **identical**
   commit→tree BFS as `rebuild_index` (shared private walk primitive,
   extracted once — no duplicated traversal logic between the two modes),
   with these differences:
   - a tree leaf's row is appended to the write buffer **only if**
     `line_exists( lt_filter[ object = ls_item-obj_type obj_name =
     ls_item-obj_name ] )` (bounds `ZAOG_OBJ_PIDX` writes to F_k, not F);
   - every appended row carries `CONTEXT_HASH = iv_context_hash` **as a KEY
     field** of `ZAOG_OBJ_PIDX` (§3.0b/AR-2-01 — not a stamp on a non-key
     column, so a differently-contexted concurrent/later call can never
     overwrite this row);
   - a hashed set of every `(obj_type, obj_name)` actually matched during the
     walk is tracked; after the walk completes without exception, for every
     `lt_filter` entry check that set — `FOUND` if present, else consult
     `zcl_abapgit_ortec_mat_state=>is_graph_have_eligible(iv_repo_key,
     iv_commit)` and `iv_current_remote` (§11.4 below) to choose
     `RESOLVED_NO_FILES` vs. `RESOLVED_NOT_PRESENT_REMOTE`, then call
     `write_coverage` once with the full result set;
   - **[NEW, AR-1-07]** if the walk instead raises on a missing commit/tree
     object (the same conditions `rebuild_index` raises on today, Q8), the
     existing `CATCH`/release-lock block additionally calls `write_coverage`
     once with a `cs_resolution-unresolved_missing_local_data` row for every
     object in `lt_filter` (best-effort — its own failure is just another
     non-fatal `write_coverage` failure, §6 trigger 4/AR-1-08) **before**
     releasing the lock and re-raising the original exception unchanged
     (§4.1, §6 trigger 2).
   `walk_filtered` **never** writes to `ZAOG_OBJ_INDEX` at all (cycle 3,
   §3.0b) and **never** issues a standalone `DELETE` against any of the
   three tables — any commit-scoped invalidation goes exclusively through
   the shared `invalidate_commit_index` helper (§5, §13 W4/AR-1-02/AR-2-01),
   which `walk_filtered` itself never calls — it only ever `MODIFY`s
   (upserts) `ZAOG_OBJ_PIDX` rows for its own `lt_filter`'s objects under
   its own `iv_context_hash`, so a second filtered walk with an
   overlapping-but-different filter (same or different context) is
   idempotent and never destroys another caller's already-written rows.
5. Blob resolution is **unchanged**: `build_files_from_rows` (existing,
   untouched) receives exactly the rows `select_rows_for_filter`/
   `select_partial_rows_for_filter` returned (byte-identical
   `ty_index_rows_tt` shape either way, §3.0b) and performs its existing
   best-effort `ensure_available` top-up + `get_objects` blob fetch, bounded
   to F_k blobs, exactly as today.
6. `get_files_for_filter`'s existing `apply_object_filter` re-derivation pass
   and existing `DEVC`-path post-filter are **unchanged**.
### 11.4 mat_state wiring (Slice 4 only, §9)
Before choosing between `RESOLVED_NO_FILES` and `RESOLVED_NOT_PRESENT_REMOTE`,
`walk_filtered` calls `zcl_abapgit_ortec_mat_state=>is_graph_have_eligible(
iv_repo_key, iv_commit)` exactly once per walk (O(1) keyed read against
`ZAOG_COMMIT_HIST`, not a tree walk) and records the resulting boolean's
corresponding `hist_level` value into `WALK_HIST_LEVEL` on every coverage row
written by that walk. Until Slice 4 ships, `walk_filtered` always passes
`iv_walk_hist_level = zcl_abapgit_ortec_mat_state=>cs_hist_level-unknown` and
every zero-match result is conservatively `RESOLVED_NO_FILES` (§9 Slice 3
default, always safe).
**AR-1-03/AR-2-02 local-config strength gate (cycle 2, signature fixed cycle
3)**: `compute_context_hash` (§3.1) is computed from
`ii_repo_online->get_dot_abapgit( )` — the **local** `.abapgit`, not
necessarily `iv_commit`'s own `.abapgit` blob (a target commit being
compared against, e.g. a different/newer remote branch tip in a
Stage-by-Transport scenario, can have a different `.abapgit` that was never
fetched or parsed). This design deliberately keeps local-config semantics
permanently (no new fetch/decode of a target commit's own `.abapgit` is
introduced — the only existing mechanism that could resolve a target
commit's real `.abapgit`, `find_remote_dot_abapgit( )`, itself calls the
unfiltered `get_files_remote( )` and would reintroduce the exact O(F) cost
this program removes). To keep `RESOLVED_NOT_PRESENT_REMOTE`'s claimed
strength honest under this choice, `walk_filtered` gains one additional
required condition before writing the strong state: `iv_current_remote IS
NOT INITIAL AND iv_commit = iv_current_remote` (the one commit the local
`.abapgit` is actually known, by existing convention elsewhere in this
codebase, e.g. `find_remote_dot_abapgit`/`get_files_remote`, to describe).
**Cycle 3/AR-2-02 fix**: cycle 2 specified this condition as
`iv_commit = ii_repo_online->get_current_remote( )`, but neither
`walk_filtered` nor `ensure_filtered_coverage` ever received `ii_repo_online`
or any current-remote value in their own cycle-2 signatures — an
implementation gap the reviewer correctly flagged as ambiguous (AR-2-02).
The fix removes the object dependency entirely: `iv_current_remote TYPE
zif_abapgit_git_definitions=>ty_sha1 OPTIONAL` is now an explicit parameter
on `get_files_for_filter`, `ensure_filtered_coverage`, and `walk_filtered`
(§14 W12), computed exactly once by `zcl_abapgit_ortec_filter_walk=>
get_remote_files_for_stage` — the one caller that actually holds a
`zif_abapgit_repo_online` reference in scope (§11 step 1) — via
`li_repo_online->get_current_remote( )` (an existing, zero-new-I/O accessor,
Q4 evidence class; a raise is caught and degrades to blank, never blocking
the request). `zcl_abapgit_ortec_obj_index` never needs an
`ii_repo_online`/online-repo reference at all; the gate becomes a plain
value comparison. `pull_filtered` (§14 W12) has no online repo object and
never supplies `iv_current_remote` — it is therefore structurally always
initial on that call path, which structurally fails the gate's own `IS NOT
INITIAL` condition. **`pull_filtered` can therefore never produce
`RESOLVED_NOT_PRESENT_REMOTE`, only `RESOLVED_NO_FILES`, by construction —
not by a second branch or an implementer's judgment call.** If `iv_commit`
differs from `iv_current_remote` (or `iv_current_remote` is initial),
`is_graph_have_eligible` being true is **not** sufficient on its own — the
result is capped at the weaker `RESOLVED_NO_FILES` regardless of graph
completeness (§13 W8 for the exact decision-free change).
### 12. Explicit prevention list
- **Payload loading merely to test presence**: `get_coverage`/`write_coverage`
  never touch `ZAOG_OBJ_STORE` at all — pure `ZAOG_OBJ_COVER`/`ZAOG_OBJ_INDEX`/
  `ZAOG_OBJ_PIDX` reads. Blob content is loaded exactly once, only for F_k
  matched blobs, by the unchanged `build_files_from_rows`.
- **Full `ZAOG_OBJ_STORE` scan**: no new code path calls `get_all_objects`/
  `populate_cache` (see `obj_store_performance_design.md` §OS-D/OS-J).
- **Repeated fetch/decode of the same key in one action**: unchanged existing
  dedup (`lt_seen_trees` inside one walk call; `mt_cache` across calls in one
  session) — no regression, no new duplication introduced.
- **Full `ZAOG_OBJ_INDEX` rebuild as silent fallback**: explicitly prevented —
  `ensure_filtered_coverage` is now the **only** entry point
  `get_files_for_filter` calls (both its primary call and its
  `CORRUPT_OR_INCOMPLETE` retry, §6 trigger 3); `rebuild_index` (COMPLETE
  mode) is reachable only by a future, not-yet-existing explicit admin
  trigger — no code path in this design silently falls back to it.
- **Object Store row absence becoming a negative index fact**: a missing
  `ZAOG_OBJ_STORE` row causes `walk_filtered` to **raise** (via the same
  `get_objects` raise `rebuild_index` relies on today), which aborts the walk
  and writes **no** coverage rows (§5, §6 trigger 2) — it is never
  interpreted as "object has no files".
- **Cache masking missing-tree recovery**: no new cache exists to mask
  anything; a genuinely missing tree still raises through the identical path
  as today, reaching the identical outer `get_files_remote` fallback.
- **Partial coverage becoming complete**: structurally enforced —
  `walk_filtered` never writes to `ZAOG_OBJ_INDEX` or its `$IDX/__READY__`
  marker at all (cycle 3, §3.0b); `is_index_ready` is the sole authority
  for "complete", never reads `ZAOG_OBJ_COVER`/`ZAOG_OBJ_PIDX`.
- **Cross-commit tree reuse without path/config identity**: not introduced —
  every `ZAOG_OBJ_COVER`/`ZAOG_OBJ_PIDX` key includes both `COMMIT_SHA1` and
  `CONTEXT_HASH`; no row is ever read across a different commit or a
  different config context (§2 candidate F explicitly avoided).
- **Context-blind positive-row reuse / cross-context overwrite (AR-1-01,
  reopened and closed as AR-2-01)**: not possible on either table now —
  `ZAOG_OBJ_INDEX` reads (`is_index_ready`, `select_rows_for_filter`)
  predicate on its non-key `CONTEXT_HASH` (§3.0), safe because
  `rebuild_index` always purges the whole commit before rewriting, so no
  upsert-without-purge overwrite can occur there; `ZAOG_OBJ_PIDX` (§3.0b)
  carries `CONTEXT_HASH` as a real KEY field, so `walk_filtered`'s
  upsert-without-purge `MODIFY` can never overwrite a different context's
  row — two contexts' rows for the same object/path are always two
  physically distinct rows. A row or marker written under a
  superseded/legacy context on either table is structurally unreachable to
  a reader supplying the current context, never a silent stale answer, and
  never a silent overwrite.
- **Split-table invalidation leaving orphaned coverage or index/partial rows
  (AR-1-02, extended AR-2-01)**: not possible — every commit-scoped
  invalidation goes exclusively through `invalidate_commit_index`, which
  deletes `ZAOG_OBJ_INDEX`, `ZAOG_OBJ_COVER`, **and `ZAOG_OBJ_PIDX`** rows
  for that commit in one LUW (§5, §13 W4); no remaining code path deletes
  one of these three tables alone.
- **Cache-admin clear racing a concurrent filtered/complete writer
  (AR-2-03)**: not possible — `clear_repo` and `walk_filtered`/
  `rebuild_index` now serialize on the same canonical
  `zcl_abapgit_ortec_pack_raw=>acquire_repo_lock` mutex for exactly the
  three tables this design's writers touch (§5, `obj_store_performance_
  design.md` cache-admin section); a clear's three deletes and a walk's
  index-then-cover write sequence can no longer interleave, so
  `ZAOG_OBJ_COVER`/`ZAOG_OBJ_PIDX` `FOUND` can never survive without its
  backing rows.
- **Unbounded `FOR ALL ENTRIES` against a caller-supplied filter (AR-1-04)**:
  not possible — `get_coverage`, `write_coverage`,
  `select_rows_for_filter`, and `select_partial_rows_for_filter` all chunk
  at the shared `c_filter_chunk_size` (§3.2, §3.0b, §8); no statement in
  this program executes an unchunked `FOR ALL ENTRIES`/`MODIFY ... FROM
  TABLE` keyed by an arbitrary-size caller filter.
- **Repeated doomed walk attempts after a missing-tree fallback (AR-1-07)**:
  not possible for the repeated-identical-request case — a live `'M'` row
  for every currently-uncovered object short-circuits straight to the
  existing fallback exception without re-attempting `walk_filtered` (§4.1,
  §11 step 3a); the backoff window bounds how long this suppression lasts,
  guaranteeing eventual retry once the underlying data gap might be fixed.
## Open questions for review
1. **RESOLVED in cycle 2 (AR-1-07)**: `UNRESOLVED_MISSING_LOCAL_DATA` is now
   actively written by Slice 3 (§4.1) to close the repeated-doomed-walk
   blocker — superseding the original "write no row on failure" proposal.
   `UNRESOLVED_AMBIGUOUS_MAPPING` remains unwritten/reserved: still open
   whether Slice 3+ should also populate it by threading the specific
   failing `obj_type`/`obj_name` out of `build_files_from_rows`'s
   `CORRUPT_OR_INCOMPLETE` raise, or whether the existing retry-and-purge
   behavior (§6 trigger 3) is sufficient without it.
2. Is a bounded periodic cleanup of orphaned `ZAOG_OBJ_COVER`, `ZAOG_OBJ_
   INDEX` (cycle 2), **and `ZAOG_OBJ_PIDX` (cycle 3)** rows (stale
   `CONTEXT_HASH`, or commits no longer referenced by any branch) needed in
   this program, or deferred to a future Package-F-style cleanup pass (§5,
   §3.0, §3.0b)?
3. **RESOLVED in cycle 3 (AR-2-01/AR-2-02/AR-2-03)**: see the cycle-3
   revision log at the top of this file for the closure of all three
   cycle-2 findings.
Blocking: 0 (all cycle-1 BLOCKER/MAJOR findings AR-1-01, AR-1-02, AR-1-03,
AR-1-04, AR-1-05, AR-1-06, AR-1-07 closed by cycle 2; AR-1-08 MINOR closed
by cycle 2; all cycle-2 findings AR-2-01, AR-2-02, AR-2-03 closed by this
cycle-3 revision; see both revision logs at the top of this file).
## 13. Weak-model change list — cycle 2 mechanics (AR-1-01, AR-1-02, AR-1-03, AR-1-07)
The `ZAOG_OBJ_INDEX`/`ZAOG_OBJ_COVER` DDIC and class-level API changes are
fully specified in §3.0/§3.2 above (items W1/W3 below point back to them);
this section specifies the remaining method-level signature/body changes to
`zcl_abapgit_ortec_obj_index` that those sections reference but do not
themselves spell out. **Cycle 3 note**: W4/W5/W6/W8 below are amended in
place for AR-2-01/AR-2-02; §3.0b and §14 hold the net-new cycle-3 blocks
(`ZAOG_OBJ_PIDX`, `select_partial_rows_for_filter`, `filter_walk` changes).
```text
FILE_OR_OBJECT=zaog_obj_index.tabl.xml
METHOD_OR_DDIC=DD03P_TABLE
ANCHOR=see §3.0 (W1 is §3.0's own FILE_OR_OBJECT block — referenced here for
  slice-numbering completeness only, not duplicated)
ACTION=insert
CHANGE=see §3.0
INVARIANTS=OS-INV-01
SQL_SHAPE=NONE
ERROR_ROLLBACK_FALLBACK=NONE
TESTS=see §3.0/§9 Slice 1b
VALIDATION=see §3.0
STOP_IF=see §3.0
```
```text
FILE_OR_OBJECT=zcl_abapgit_ortec_obj_index.clas.abap (W2: is_index_ready,
  select_rows_for_filter)
METHOD_OR_DDIC=is_index_ready, select_rows_for_filter
ANCHOR=existing method signatures/bodies as read at BASELINE_COMMIT 4193733d
  (Q1, Q10)
ACTION=replace
CHANGE=
  is_index_ready gains a new mandatory IMPORTING iv_context_hash TYPE
  zif_abapgit_git_definitions=>ty_sha1. Its SELECT SINGLE predicate gains
  AND context_hash = iv_context_hash (both the STRICT marker-row variant and
  the RELAXED any-row variant, Q10).
  select_rows_for_filter gains the same new mandatory IMPORTING
  iv_context_hash. Its body changes from one unchunked FOR ALL ENTRIES to a
  loop over it_filter in chunks of
  zcl_abapgit_ortec_obj_cover=>c_filter_chunk_size (AR-1-04), each chunk's
  SELECT gaining AND context_hash = iv_context_hash, results APPENDED across
  chunks into rt_files (or the existing returning table name). Cycle 3:
  select_rows_for_filter's caller set narrows to exactly one call site —
  §11 step 3.1 (the `is_index_ready = true` branch only); the FILTERED-mode
  branch (§11 steps 3.4/4/5) calls the new select_partial_rows_for_filter
  instead (§3.0b) — select_rows_for_filter's own body/signature here is
  otherwise unchanged from this cycle-2 block.
INVARIANTS=OS-INV-01, OS-INV-04, OS-INV-05
SQL_SHAPE=is_index_ready: SELECT SINGLE, primary-key-prefix + one residual
  equality, unchanged shape otherwise; select_rows_for_filter: chunked FOR
  ALL ENTRIES at c_filter_chunk_size, primary-key-prefix + one residual
  equality per chunk
ERROR_ROLLBACK_FALLBACK=NONE (pure reads; no new exception path)
TESTS=ready_rejects_different_context, select_rows_excludes_other_context,
  blank_legacy_context_is_never_ready, select_rows_chunk_boundary (>5000
  filter rows)
VALIDATION=SAPDiagnose(action="unittest", type="CLAS",
  name="ZCL_ABAPGIT_ORTEC_OBJ_INDEX") PASS, including every pre-existing
  ltcl_obj_index test updated to pass an explicit context hash
STOP_IF=any existing caller of is_index_ready/select_rows_for_filter outside
  this class is found at implementation time (re-verify with a where-used
  search before coding — this design's discovery found none, Q1)
```
```text
FILE_OR_OBJECT=zcl_abapgit_ortec_obj_index.clas.abap (W4: invalidate_commit_index)
METHOD_OR_DDIC=invalidate_commit_index (new private helper), rebuild_index,
  get_files_for_filter
ANCHOR=rebuild_index's existing pre-walk DELETE FROM zaog_obj_index (Q1,
  Q11); get_files_for_filter's CORRUPT_OR_INCOMPLETE retry's identical
  DELETE (Q1, Q8)
ACTION=replace
CHANGE=
  CLASS-METHODS invalidate_commit_index
    IMPORTING iv_repo_key TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
              iv_commit   TYPE zif_abapgit_git_definitions=>ty_sha1.
  " body (decision-free, no RAISING needed — a plain DELETE cannot itself
  " fail under normal DB operation; sy-subrc merely reflects "0 rows
  " found", not an error). Cycle 3 (AR-2-01): a third DELETE is added for
  " the new ZAOG_OBJ_PIDX table (§3.0b) — all three deletes are
  " intentionally context-blind (no context_hash predicate): a
  " commit-scoped invalidation must purge every context's rows, not just
  " the caller's own current context, so a superseded context can never
  " resurface after this purge either:
  "   DELETE FROM zaog_obj_index WHERE repo_key = iv_repo_key AND
  "     commit_sha1 = iv_commit.
  "   DELETE FROM zaog_obj_cover WHERE repo_key = iv_repo_key AND
  "     commit_sha1 = iv_commit.
  "   DELETE FROM zaog_obj_pidx WHERE repo_key = iv_repo_key AND
  "     commit_sha1 = iv_commit.
  " All three deletes execute in the caller's own LUW (no COMMIT WORK here,
  " consistent with this class's existing no-commit discipline).
  Replace rebuild_index's own pre-walk DELETE with a call to
  invalidate_commit_index(iv_repo_key, iv_commit). Replace
  get_files_for_filter's CORRUPT_OR_INCOMPLETE retry DELETE with the same
  call.
INVARIANTS=OS-INV-10, OS-INV-11
SQL_SHAPE=three primary-key-prefix DELETEs, same LUW, no chunking needed
  (single-commit scope, not caller-filter-sized)
ERROR_ROLLBACK_FALLBACK=NONE (a plain DELETE does not raise; any downstream
  failure in the caller's own walk still rolls back the caller's LUW,
  discarding all three deletes together)
TESTS=retry_purge_removes_all_three_tables (seed ZAOG_OBJ_INDEX +
  ZAOG_OBJ_COVER + ZAOG_OBJ_PIDX rows for a commit, force the
  CORRUPT_OR_INCOMPLETE retry, assert all three tables are empty for that
  commit before the retry's rebuild/walk begins)
VALIDATION=SAPDiagnose(action="unittest", type="CLAS",
  name="ZCL_ABAPGIT_ORTEC_OBJ_INDEX") PASS
STOP_IF=any other call site issues its own DELETE FROM zaog_obj_index/
  zaog_obj_cover/zaog_obj_pidx for a commit scope at implementation time
  (re-verify with grep before coding — this design's discovery found
  exactly the two call sites listed here, Q1)
```
```text
FILE_OR_OBJECT=zcl_abapgit_ortec_obj_index.clas.abap (W5/W6: walk_filtered,
  ensure_filtered_coverage)
METHOD_OR_DDIC=walk_filtered (new), ensure_filtered_coverage (new)
ANCHOR=§11 steps 3a and 4 above (narrative); rebuild_index's existing
  CATCH zcx_abapgit_exception/CATCH cx_root release-lock-and-reraise block
  (Q8, Q11) is the pattern walk_filtered's own CATCH extends
ACTION=insert
CHANGE=exactly as narrated in §11 steps 3a and 4 — no additional decision
  beyond what those steps already specify. Exact signatures (cycle 3 adds
  iv_current_remote to both, AR-2-02; cycle 3 also drops any ZAOG_OBJ_INDEX
  reference from walk_filtered's own write path, AR-2-01/§3.0b):
  CLASS-METHODS ensure_filtered_coverage
    IMPORTING iv_repo_key       TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
              iv_commit         TYPE zif_abapgit_git_definitions=>ty_sha1
              io_dot            TYPE REF TO zcl_abapgit_dot_abapgit
              iv_devclass       TYPE devclass
              it_filter         TYPE zif_abapgit_definitions=>ty_tadir_tt
              iv_context_hash   TYPE zif_abapgit_git_definitions=>ty_sha1
              iv_current_remote TYPE zif_abapgit_git_definitions=>ty_sha1 OPTIONAL
    RETURNING VALUE(rt_rows) TYPE ty_index_rows_tt
    RAISING   zcx_abapgit_exception.
  CLASS-METHODS walk_filtered
    IMPORTING iv_repo_key       TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
              iv_commit         TYPE zif_abapgit_git_definitions=>ty_sha1
              io_dot            TYPE REF TO zcl_abapgit_dot_abapgit
              iv_devclass       TYPE devclass
              it_filter         TYPE zif_abapgit_definitions=>ty_tadir_tt
              iv_context_hash   TYPE zif_abapgit_git_definitions=>ty_sha1
              iv_current_remote TYPE zif_abapgit_git_definitions=>ty_sha1 OPTIONAL
    RAISING   zcx_abapgit_exception.
  In particular: the 'M'-row write on the missing-commit/tree CATCH path
  calls write_coverage with cs_resolution-unresolved_missing_local_data for
  every entry of the lt_filter this walk_filtered invocation was given,
  BEFORE release_lock and the re-RAISE; the 3a backoff short-circuit lives
  in ensure_filtered_coverage, not walk_filtered, and raises
  zcx_abapgit_exception directly (no call into walk_filtered at all for the
  all-backed-off case). walk_filtered's own positive-row writes and
  re-select both target ZAOG_OBJ_PIDX exclusively (§3.0b) — it never reads
  or writes ZAOG_OBJ_INDEX.
INVARIANTS=OS-INV-01, OS-INV-06, OS-INV-10, OS-INV-14
SQL_SHAPE=see §3.2/§3.0b (write_coverage's existing chunked MODIFY, and the
  new ZAOG_OBJ_PIDX chunked MODIFY — no new SQL shape introduced by the
  'M'-row path)
ERROR_ROLLBACK_FALLBACK=see §6 triggers 2 and 4
TESTS=missing_tree_writes_m_row_then_reraises,
  repeat_request_within_backoff_skips_walk,
  repeat_request_after_backoff_retries_walk (all §9 Slice 3)
VALIDATION=SAPDiagnose(action="unittest", type="CLAS",
  name="ZCL_ABAPGIT_ORTEC_OBJ_INDEX") PASS
STOP_IF=write_coverage's own failure inside the 'M'-row write path is ever
  allowed to suppress or replace the original missing-commit/tree exception
  — the original exception must always still reach the caller unchanged
```
```text
FILE_OR_OBJECT=zcl_abapgit_ortec_obj_index.clas.abap (W8: AR-2-02 gate, supersedes cycle-2 AR-1-03 wording)
METHOD_OR_DDIC=walk_filtered (mat_state branch, Slice 4)
ANCHOR=§11.4's existing is_graph_have_eligible call (Slice 4, not yet
  implemented pre-cycle-2 either — this is a same-slice addition, not a
  retrofit of shipped code)
ACTION=insert
CHANGE=before writing cs_resolution-resolved_not_present_remote, require
  BOTH zcl_abapgit_ortec_mat_state=>is_graph_have_eligible(iv_repo_key,
  iv_commit) = abap_true AND iv_current_remote IS NOT INITIAL AND iv_commit
  = iv_current_remote. If the second/third condition is false, write
  cs_resolution-resolved_no_files instead, unconditionally, regardless of
  the first condition's result. iv_current_remote is walk_filtered's own
  IMPORTING parameter (W5/W6 above) — no ii_repo_online/object reference is
  read or held anywhere inside zcl_abapgit_ortec_obj_index.
INVARIANTS=(identity exactness — a negative fact's strength may never exceed
  what the config it was computed under can actually support)
SQL_SHAPE=NONE (a plain value comparison; no new SQL)
ERROR_ROLLBACK_FALLBACK=NONE (no new exception path — iv_current_remote is a
  plain value, not a call; any raise from computing it already happened in
  the caller, §14 W12)
TESTS=not_present_remote_requires_current_remote_commit (walk against a
  non-tip iv_commit with is_graph_have_eligible=true and a non-matching
  iv_current_remote still yields RESOLVED_NO_FILES, never
  RESOLVED_NOT_PRESENT_REMOTE), not_present_remote_requires_current_remote_
  supplied (iv_current_remote initial/omitted also yields RESOLVED_NO_FILES
  even when iv_commit happens to be graph-have-eligible)
VALIDATION=SAPDiagnose(action="unittest", type="CLAS",
  name="ZCL_ABAPGIT_ORTEC_OBJ_INDEX") PASS (Slice 4 gate)
STOP_IF=any caller passes a placeholder/synthetic iv_current_remote instead
  of a real zif_abapgit_repo_online=>get_current_remote( ) result — this
  parameter's entire safety argument rests on it always being either a
  faithfully-computed current-remote SHA1 or genuinely absent (§14 W12)
```
## 14. Weak-model change list — cycle 3 additions (AR-2-02)
§3.0b above already fully specifies the cycle-3 `ZAOG_OBJ_PIDX`/
`select_partial_rows_for_filter` change (AR-2-01); it is not duplicated
here. This section specifies the remaining AR-2-02 change: threading
`iv_current_remote` from `zcl_abapgit_ortec_filter_walk` down into
`zcl_abapgit_ortec_obj_index=>get_files_for_filter` (the one signature not
already covered by W5/W6's `ensure_filtered_coverage`/`walk_filtered`
block above).
```text
FILE_OR_OBJECT=zcl_abapgit_ortec_obj_index.clas.abap (get_files_for_filter) (W12: filter_walk + get_files_for_filter threading),
  zcl_abapgit_ortec_filter_walk.clas.abap (get_remote_files_for_stage,
  pull_filtered)
METHOD_OR_DDIC=get_files_for_filter, get_remote_files_for_stage,
  pull_filtered
ANCHOR=get_files_for_filter's existing signature (iv_repo_key, iv_commit,
  ii_obj_filter, io_dot, iv_devclass, iv_url OPTIONAL) as read at
  BASELINE_COMMIT 4193733d; get_remote_files_for_stage's existing body
  (li_repo_online already cast and in scope before its own
  get_files_for_filter call); pull_filtered's existing body (no online
  repo object anywhere)
ACTION=replace
CHANGE=
  get_files_for_filter gains one new OPTIONAL IMPORTING parameter, appended
  after the existing iv_url:
    iv_current_remote TYPE zif_abapgit_git_definitions=>ty_sha1 OPTIONAL
  and passes it straight through to ensure_filtered_coverage (§11 step 3).
  get_remote_files_for_stage adds, immediately before its existing call to
  get_files_for_filter:
    DATA lv_current_remote TYPE zif_abapgit_git_definitions=>ty_sha1.
    TRY.
        lv_current_remote = li_repo_online->get_current_remote( ).
      CATCH zcx_abapgit_exception.
        CLEAR lv_current_remote.
    ENDTRY.
  and adds iv_current_remote = lv_current_remote to that call's parameter
  list.
  pull_filtered is NOT changed — it has no online repo object in scope
  (confirmed at BASELINE_COMMIT) and its existing call to
  get_files_for_filter simply omits iv_current_remote, which is OPTIONAL
  and therefore structurally initial.
INVARIANTS=(identity exactness — see W8's STOP_IF: this parameter must
  always be either a faithfully-computed current-remote SHA1 or genuinely
  absent, never a placeholder)
SQL_SHAPE=NONE (get_current_remote is an existing, already-used accessor;
  no new SQL/HTTP call is introduced by this change)
ERROR_ROLLBACK_FALLBACK=get_current_remote raising zcx_abapgit_exception is
  caught locally in get_remote_files_for_stage and degrades to a blank
  iv_current_remote — never propagates, never blocks the request (matches
  every other best-effort accessor already in this method, e.g. the
  existing io_dot/iv_devclass calls)
TESTS=pull_filtered_never_writes_not_present_remote (§9 Slice 4, structural
  proof: pull_filtered's call chain never supplies iv_current_remote),
  get_remote_files_for_stage_passes_current_remote (asserts the value
  reaching get_files_for_filter matches li_repo_online->get_current_remote(
  )'s result on a happy path), get_remote_files_for_stage_degrades_on_
  current_remote_failure (a raising get_current_remote still returns a
  file result via the unchanged downstream path, with iv_current_remote
  blank)
VALIDATION=SAPDiagnose(action="unittest", type="CLAS",
  name="ZCL_ABAPGIT_ORTEC_FILTER_WALK") PASS; SAPDiagnose(action="unittest",
  type="CLAS", name="ZCL_ABAPGIT_ORTEC_OBJ_INDEX") PASS
STOP_IF=any other existing caller of get_files_for_filter is found at
  implementation time beyond get_remote_files_for_stage/
  get_remote_files_for_diff/pull_filtered (re-verify with a where-used
  search before coding — this design's discovery found exactly these three
  entry points, all inside zcl_abapgit_ortec_filter_walk)
```