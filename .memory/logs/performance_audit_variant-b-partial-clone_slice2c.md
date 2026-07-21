# Performance audit: Variant B Slice 2C correction pass (dedicated, F-2C-004)

Scope: the focused correctness correction to Slice 2C
(`.memory/logs/variant_b_slice2c_migration_map.md`,
`.memory/handoffs/variant-b-slice2c-checkpoint.md`), addressing findings
F-2C-001 through F-2C-003. This audit is dedicated to 2C's own correction
and is explicitly separate from
`.memory/logs/performance_audit_variant-b-partial-clone_slice2.md` (scoped
to 2A/2B only).

## Verdict: `PASS`

No blocking finding. The correction strictly reduces network/DB cost versus
the pre-correction 2C state; no new SQL or HTTP call shape was introduced.

## Changed call chain inspected

`upload_pack_by_branch`, `upload_pack_by_commit`, `upload_pack` (private),
`complete_missing_object`, `zcl_abapgit_ortec_pack_stream=>complete_missing_base`,
`get_verified_have_commits`, `is_commit_complete` - all read directly from
current source (`src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap`,
`zcl_abapgit_ortec_fetch_neg.clas.abap`, `zcl_abapgit_ortec_pack_stream.clas.abap`),
not from prose.

## SQL calls per logical fetch

- `upload_pack`'s have resolution (`INCREMENTAL_THIN`/
  `INCREMENTAL_SELF_CONTAINED` only): `get_verified_have_commits` ->
  `get_have_commits` issues 2 SELECTs (`zaog_commit_hist` DISTINCT,
  `zaog_repo_state` DISTINCT) + `collect_ancestor_haves`'s own single bulk
  SELECT from `zaog_obj_store` (capped at 200 results/50 BFS depth) - all
  pre-existing, unchanged by this correction pass, and already tracked as a
  Slice 3 candidate (not this pass's scope; re-confirmed untouched).
  `is_commit_complete` -> `is_graph_have_eligible` then runs one O(1) SELECT
  per surviving candidate (bounded by the ≤200 cap above, not by repo size)
  - also pre-existing/unchanged.
- `complete_missing_base`: **0 SQL** (now an unconditional `RETURN`, before
  this correction it also never issued its own SQL beyond delegating to
  `complete_missing_object`, which is now unreachable).
- `get_base_bytes`'s own `get_object` attempt on a delta base: 1 SELECT per
  distinct missing base encountered during a resolve pass - pre-existing,
  unrelated to F-2C-001 (this is a local object-store lookup, not part of
  the removed per-base HTTP repair), unchanged by this correction.
- None of the above scale with the number of *missing* objects/bases beyond
  the pre-existing, already-reviewed `collect_ancestor_haves`/per-candidate
  certification shape.

## HTTP calls per logical fetch

- `upload_pack_by_branch`/`upload_pack_by_commit`: unchanged by this
  correction - up to 3 total upload-pack HTTP round trips per logical fetch
  (thin -> self-contained -> at most one `RECOVERY_BRANCH_FULL`), bounded,
  not scaling with object/base count. Confirmed unchanged: this correction
  touched only the `ev_deepen_used` assignment and the have-resolution
  catch inside `upload_pack`, neither of which adds or removes an HTTP call.
- `complete_missing_base`: **0 HTTP** (was 1 targeted `MATERIALIZE_BLOBS`
  request per call before this correction - now a permanent no-op that
  never calls `zcl_abapgit_ortec_fastpath=>complete_missing_object`).
  Confirmed via grep: `complete_missing_object` has zero live callers
  after this change (only referenced in comments).
- Net effect: the previous per-missing-delta-base HTTP request (F-2C-001,
  unbounded in practice within the pre-existing 20-attempt budget) is
  eliminated entirely from the correctness path. A pack with N missing
  external bases in one resolve pass now issues 0 additional HTTP requests
  for those bases (each simply raises `retry_without_haves = abap_true`
  immediately), instead of up to N additional HTTP requests.

## Does any count scale with missing object/base count?

No. Before this correction, `complete_missing_base` issued exactly one HTTP
request per distinct missing base encountered (bounded only by the
20-attempt `c_max_completion_attempts` budget) - a real
one-request-per-object shape. After this correction, the count is always 0
regardless of how many bases are missing; a genuinely incomplete pack now
escalates via the existing bounded 3-attempt upload-pack cascade instead.

## Behavior when certificate/have lookup fails

`upload_pack`'s `get_verified_have_commits` call is no longer wrapped in an
empty `CATCH zcx_abapgit_ortec_git`. Because `upload_pack` already declares
`RAISING zcx_abapgit_ortec_git`, any technical failure raised by that chain
now propagates unchanged to `upload_pack`'s callers
(`upload_pack_by_branch`/`upload_pack_by_commit`/`complete_missing_object`),
which already `CATCH zcx_abapgit_ortec_git zcx_abapgit_exception` as part
of their existing thin -> self-contained -> recovery cascade (or, for
`complete_missing_object`, propagate it further unchanged). No new
repository-wide fallback read or empty-have substitution is introduced.
Confirmed by source read that no current call in the
`get_have_commits`/`get_verified_have_commits`/`is_commit_complete`/
`is_graph_have_eligible` chain actually raises `zcx_abapgit_ortec_git`
today (`is_graph_have_eligible` has no `RAISING` clause at all) - so this
change is a zero-cost, purely defensive correction against a currently
dormant but latent swallow bug; it does not add any SQL/HTTP call on any
path, live or hypothetical.

## Effective `ev_deepen_used` semantics

Now unconditionally `0` for all three tiers (`INCREMENTAL_THIN`,
`INCREMENTAL_SELF_CONTAINED`, `RECOVERY_BRANCH_FULL`) in both
`upload_pack_by_branch` and `upload_pack_by_commit` - confirmed via grep
(6 occurrences, all `= 0`). Cost-neutral: this is a single scalar
assignment, not a new SQL/HTTP call. Downstream, `persist_pull_result` ->
`update_after_fetch` will now persist `deepen_lvl = 0` for every migrated
fetch (a single existing `MODIFY zaog_repo_state`, unchanged shape) -
purely a value change, no new call.

## Per-object remote repair reachability

Confirmed NOT reachable from any live path:
- `complete_missing_base`'s body is now `RETURN.` unconditionally - grep
  confirms zero remaining calls to `complete_missing_object` from
  non-comment code.
- Both call sites of `complete_missing_base` (`get_base_bytes`,
  `resolve_one_meta`'s REF_DELTA branch) already had a pre-existing `ELSE`/
  failure branch that raises with `iv_retry_without_haves = abap_true` -
  since `rv_attempted` is now always `abap_false`, that branch is the only
  one ever taken; the "successful completion, retry get_object" branch is
  dead code but harmless (matches the codebase's existing
  `LEGACY_BUT_UNREACHABLE_AFTER_2C` phased-removal convention).

## Memory/XSTRING footprint

No change - this correction touches only control flow (an unconditional
early `RETURN`, two scalar re-assignments, and removal of an empty
exception handler). No new buffer, no new batch size, no new resident
object set.

## Conclusion

The correction is strictly performance-positive versus the pre-correction
2C state (eliminates a real one-request-per-object HTTP pattern) and
performance-neutral on every other measured dimension. No blocking finding.
`collect_ancestor_haves`'s pre-existing unbounded-by-repo-size read remains
the sole carried-forward Slice 3 candidate, unchanged and unworsened by
this pass.
