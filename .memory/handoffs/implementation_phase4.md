# Handoff — Implementation Phase 4 (2026-07-11)

## Prerequisite confirmed
Michael imported the Phase 1 + Phase 3 changes into the real SAP system IT8 and confirmed
they compiled successfully. This resolves the residual "real system validation" concern
raised in the Phase 1+3 handoff (`implementation_phase1_phase3.md`) more strongly than the
blocked local transpile harness ever would have.

## Scope decision (read this before assuming Phase 4 is "the six-state model")
The original design description of Phase 4 (`.memory/logs/target_design.md` section 5)
described reworking `zcl_abapgit_git_porcelain=>walk`/`walk_tree` into a bulk
collect→fetch→persist→retry algorithm. Investigation this session found:

- `pull_by_branch`'s existing self-heal (`reset_fetch_commit` + full non-thin refetch on
  `Walk,` errors) already works correctly today and needs no changes. Reworking
  `walk`/`walk_tree`'s control flow into a two-pass bulk-collect algorithm would very likely
  exceed the D7 minimal-touch budget (signature/control-flow changes to standard
  `zcl_abapgit_git_porcelain`) for a repair path that isn't actually broken.
- A concrete, real, higher-value gap was found instead in
  `zcl_abapgit_ortec_obj_index=>build_files_from_rows` (used by the **filtered** Stage/Diff
  read path): if even **one** blob referenced by the filtered index rows was missing from
  the local object store, the entire filtered fast path raised and fell back to the **full**
  slow `get_files_remote()` — exactly the large-repo performance regression called out in
  `.memory/state.md`'s "Known issues to fix".

Phase 4 as implemented targets this second, concrete gap. `walk`/`walk_tree` are untouched
beyond Phase 1's `repo_key` fix (zero further standard-code changes — trivially satisfies D7).

## What was implemented
- **`zcl_abapgit_ortec_obj_store=>get_missing_sha1s`** (new): one chunked, set-based SELECT
  (reusing the existing `read_object_rows` helper) that returns which of a candidate SHA1 set
  are not present in the store. No per-object DB reads.
- **`zcl_abapgit_ortec_missing_obj=>ensure_available`** (new class): the bulk
  missing-object collector.
  1. Local bulk check (`get_missing_sha1s`) — return immediately if nothing missing.
  2. **Safety gate**: if `iv_url` is blank or
     `zcl_abapgit_ortec_git_switch=>is_active_for_repo(iv_url)` is false, raise immediately
     **without any network call**. This is the critical design decision: a repo that has not
     opted into the ORTEC write/protocol behavior must never have a read-only filtered Stage/
     Diff operation silently trigger a full, non-negotiated fetch.
  3. If the gate passes: **one** call to `zcl_abapgit_git_transport=>upload_pack_by_commit`,
     which internally routes through the ORTEC fastpath's incremental have/want negotiation
     (`zcl_abapgit_ortec_fetch_neg`) — a negotiated, not full, fetch.
  4. Persist via `zcl_abapgit_ortec_obj_store=>store_objects` (bulk insert).
  5. Retry the local bulk check **once**. If still missing, raise (never silently proceeds,
     never classifies as deleted).
- **`zcl_abapgit_ortec_obj_index`**: `get_files_for_filter` and `build_files_from_rows` gained
  optional `iv_url`/`iv_commit` parameters. `build_files_from_rows` now attempts the new
  top-up (best-effort, failure swallowed) before its existing `get_objects` call, which is
  otherwise completely unchanged — if the top-up isn't possible or doesn't fully resolve the
  gap, execution falls straight through to the pre-existing miss-handling/raise/fallback,
  identical to before this phase.
- **`zcl_abapgit_ortec_filter_walk=>get_remote_files_for_stage`**: passes the already-resolved
  `lv_url` into `get_files_for_filter`. `pull_filtered` was left unchanged (no URL available
  there without extra plumbing not needed for this phase's target scenario).
- **Tests**: `missing_sha1s_none`/`missing_sha1s_some` (obj_store bulk-check correctness), and
  a new `ltcl_missing_objects` class specifically proving the safety gate: `noop_when_nothing_
  missing`, `no_fetch_without_url`, `no_fetch_when_opt_in_off` — the latter two assert an
  exception is raised with **no** network attempt.

## Validation performed
- `get_errors` and `abaplint` clean on all 6 changed/new files (0 new issues beyond one
  deliberate style match: the new test class's setup/teardown use the same unescaped
  `DELETE FROM ... WHERE repo_key = mc_repo` idiom as every other existing test class in the
  same file, for consistency).
- Independent regression subagent review: **PASS_WITH_NOTES**, no hard-stop violations. It
  specifically traced the safety gate end-to-end and confirmed
  `upload_pack_by_commit` is unreachable unless the gate passes. See
  `.memory/logs/regression_phase4.md`.
- Documented (non-blocking) trade-off: `get_files_for_filter`'s stale-index rebuild-and-retry
  path can invoke the new top-up a second time in a rare double-failure scenario. Bounded at 2
  attempts total, never unbounded, never incorrect — not fixed in this phase to avoid added
  complexity for an edge case.

## Deferred by design (not a gap)
- The six explicit object/path states (`LOADED`, `INDEXED_NEEDS_LOAD`, `NOT_BUFFERED`,
  `UNKNOWN_NEEDS_FETCH`, `CONFIRMED_ABSENT`, `CORRUPT_OR_INCOMPLETE`) and the D4
  `cs_absent_strictness` STRICT/RELAXED switch are **not yet implemented**. They have no
  concrete consuming logic yet (no unified status engine exists — that was already deferred
  from Phase 3), so adding the switch/states now would be empty scaffolding. This mirrors the
  same deferral reasoning already applied to `zcl_abapgit_ortec_status_engine` in Phase 3.

## Next recommended step
- Phase 4 is committed (`aa8229bd`, work branch only) and staged for IT8 import. **Michael
  has been explicit: IT8 import for Phase 4 is an ADT syntax check only, not functional
  testing.** A successful syntax check confirms compilation, nothing more - the negotiated
  fetch, the safety gate, and store persistence still need real runtime/functional
  validation before any of this is considered proven correct.
- Phase 5 (delta-base completeness + protocol hardening, D3) or the deferred six-state model
  work are the next design-approved slices. Both are larger lifts — recommend explicit
  go-ahead before starting either, and recommend real functional validation of Phase 4 before
  building further phases on top of it.
