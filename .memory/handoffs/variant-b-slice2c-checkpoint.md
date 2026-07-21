# Variant B Slice 2C implementation handoff (call-site migration)

Status: implemented, statically/structurally validated, and CORRECTED (see
"Correction pass" section below). SAP import, activation, ABAP Unit, and
ATC on the live system are explicitly PENDING.

## Verdict

`READY_FOR_2C_CHECKPOINT` (post-correction)

## Correction pass (this session)

A focused corrective review found and fixed 3 correctness findings in the
original 2C implementation described below. Checkpoint approval is
conditioned on this correction, not the original implementation alone.

- **F-2C-001 (per-object HTTP completion)**: `zcl_abapgit_ortec_pack_stream
  =>complete_missing_base` issued one `MATERIALIZE_BLOBS` HTTP request per
  missing delta base (bounded only by a 20-attempt counter) - a genuine
  one-request-per-object repair, forbidden by the Variant B architecture.
  Fixed: `complete_missing_base`'s body is now an unconditional `RETURN.`
  (permanently disabled; never calls
  `zcl_abapgit_ortec_fastpath=>complete_missing_object`, which is now a
  dormant/unreachable method with zero live callers). Its two existing call
  sites (`get_base_bytes`, `resolve_one_meta`'s REF_DELTA branch) already
  had a pre-existing failure branch that raises
  `iv_retry_without_haves = abap_true`; since `rv_attempted` is now always
  `abap_false`, a genuinely missing external base always escalates via
  that existing signal into the caller's bounded 3-attempt
  thin/self-contained/`RECOVERY_BRANCH_FULL` cascade instead of a targeted
  per-object fetch. True collect/deduplicate/bulk external-base resolution
  remains deferred to Slice 7, per the prompt's explicit instruction not to
  implement that redesign now.
- **F-2C-002 (false deepen result)**: `upload_pack_by_branch`/
  `upload_pack_by_commit`'s `INCREMENTAL_THIN`/`INCREMENTAL_SELF_CONTAINED`
  tiers echoed back `lv_deepen_level`/`iv_deepen_level` as `ev_deepen_used`,
  even though the migrated wire path never reads that parameter (confirmed
  by source read: `upload_pack`'s body never references
  `iv_deepen_level`) and never emits `deepen`. Fixed: all three tiers in
  both methods now unconditionally set `ev_deepen_used = 0` (the
  `RECOVERY_BRANCH_FULL` tier already did). Confirmed via grep: 6/6
  `ev_deepen_used = ` assignments in the migrated methods are now `= 0`.
- **F-2C-003 (swallowed have-certification failure)**: `upload_pack`
  wrapped `zcl_abapgit_ortec_fetch_neg=>get_verified_have_commits` in a
  `TRY ... CATCH zcx_abapgit_ortec_git.` with an empty handler, silently
  converting any technical failure into an empty have-set. Fixed: the
  `TRY`/empty `CATCH` was removed; the call is now unwrapped, so any
  exception it raises propagates via `upload_pack`'s own existing
  `RAISING zcx_abapgit_ortec_git` declaration to its callers, which already
  handle `zcx_abapgit_ortec_git`/`zcx_abapgit_exception` as part of their
  existing retry cascade - no new catch, no repository-wide fallback.
  Source review confirms this is currently a dormant/latent fix (no live
  call in the `get_have_commits`/`get_verified_have_commits`/
  `is_commit_complete`/`is_graph_have_eligible` chain actually raises this
  exception today - `is_graph_have_eligible` declares no `RAISING` clause
  at all), so the change is zero-cost and behavior-neutral today while
  closing the latent trap for when that chain is extended in the future.
- **F-2C-004 (dedicated performance audit)**: written to
  `.memory/logs/performance_audit_variant-b-partial-clone_slice2c.md` -
  verdict `PASS`, no blocking finding, strictly performance-positive vs.
  the pre-correction state (eliminates a real one-request-per-object HTTP
  pattern).

### Files/methods touched by the correction pass

- `src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.abap`:
  `complete_missing_base` (body + doc comment).
- `src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap`: `upload_pack_by_branch`
  (3 `ev_deepen_used` assignments), `upload_pack_by_commit` (3
  `ev_deepen_used` assignments), `upload_pack` (removed empty catch),
  `complete_missing_object` (doc comment only, noting dormancy).
- `src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap`: one new
  test method `missing_base_no_http_retry` added to the EXISTING
  `ltcl_stream_resolve` class (26 chars, no new test class created).

### Externally visible behavior change

- A pack with N missing external delta bases in one resolve pass now
  issues 0 additional HTTP requests for those bases (previously up to N,
  each a real network round trip) - it instead relies on the existing
  bounded upload-pack retry cascade.
- `ev_deepen_used` reported by all migrated Variant B modes is now always
  `0`, which is persisted via `persist_pull_result`/`update_after_fetch`
  into `zaog_repo_state.deepen_lvl` - a value change only (no behavior in
  the migrated wire path reads this field back for correctness).
- A technical failure while resolving/verifying haves (currently
  unreachable in practice) would now surface as a real error/retry instead
  of being silently treated as "no haves available".

### Not fixed / explicitly deferred (per the correction prompt's own limits)

- True collect/deduplicate/bulk external-base resolution - Slice 7.
- `collect_ancestor_haves`'s pre-existing unbounded-by-repo-size read -
  Slice 3 (unchanged, unworsened by this pass).
- A live failure-injection unit test for F-2C-003 was not written: no
  current call in the affected chain raises `zcx_abapgit_ortec_git`
  (confirmed by source read), and building a seam to force one would
  require introducing test-only dependency injection - out of scope for a
  focused correction pass per the prompt's explicit "do not broaden the
  source scope" instruction. Verified structurally instead (grep confirms
  the empty catch no longer exists; the exception class already flows
  correctly through the declared `RAISING` chain to callers that already
  handle it).
- A live end-to-end test proving `ev_deepen_used = 0` for
  `upload_pack_by_branch`/`upload_pack_by_commit` was not written: these
  methods require a real/mocked HTTP transport
  (`zcl_abapgit_git_transport=>find_branch_ortec`/
  `zcl_abapgit_http=>create_by_url`), and no HTTP test-double
  infrastructure exists anywhere in this codebase (confirmed by repo-wide
  search) - consistent with pre-existing 2C practice, where these same
  methods were never live-unit-tested either. Verified structurally via
  source read + grep (6/6 assignments confirmed `= 0`).

## Migrated call sites (original implementation, unchanged by this pass)

All 6 sites in the approved migration map
(`.memory/logs/variant_b_slice2c_migration_map.md`) were migrated:

1. `zcl_abapgit_ortec_fastpath=>upload_pack` (private) - `iv_allow_thin`/
   `iv_force_full` replaced by `iv_mode`/`iv_server_caps`; delegates to
   `zcl_abapgit_ortec_fetch_req=>build_request`.
2. `zcl_abapgit_ortec_fastpath=>upload_pack_by_branch` - tier 1
   `INCREMENTAL_THIN`, tier 2 `INCREMENTAL_SELF_CONTAINED`, tier 3 exactly
   one `RECOVERY_BRANCH_FULL` attempt (progressive-deepen `DO...TIMES` loop
   removed from the live flow).
3. `zcl_abapgit_ortec_fastpath=>upload_pack_by_commit` - identical mapping
   to (2).
4. `zcl_abapgit_ortec_fastpath=>complete_missing_object` - `MATERIALIZE_BLOBS`
   with a 1-element want list.
5. `zcl_abapgit_ortec_fastpath=>try_filtered_commit_fetch` - `INITIAL_BRANCH_BLOBLESS`;
   inline capability-parse duplicate replaced by
   `zcl_abapgit_ortec_fetch_req=>parse_capabilities`.
6. `zcl_abapgit_ortec_fetch_neg=>is_commit_complete` - delegates to
   `zcl_abapgit_ortec_mat_state=>is_graph_have_eligible` (O(1) certified
   read, was O(reachable-graph-size) tree walk).

## Exact files and methods changed

- `src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap`: `upload_pack`,
  `upload_pack_by_branch`, `upload_pack_by_commit`, `complete_missing_object`,
  `try_filtered_commit_fetch`, plus doc-comment corrections on
  `complete_missing_object`'s public declaration and
  `serve_cached_when_nothing_new`'s trailing comment (both referenced the
  now-obsolete `iv_force_full` parameter name).
- `src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap`: `is_commit_complete`.
- `src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap`:
  `ltcl_completeness_gate` (setup/teardown + 3 updated + 2 new test
  methods: `complete_false_uncertified`, `complete_true_full_complete`).

## Old active behavior removed (from the live flow)

- The `iv_allow_thin`/`iv_force_full` boolean pair on `upload_pack`.
- The progressive-deepen `DO c_progressive_max_steps TIMES` widening retry
  loop in both `upload_pack_by_branch` and `upload_pack_by_commit`.
- The duplicated inline capability-parse block in
  `try_filtered_commit_fetch` (replaced by the shared
  `zcl_abapgit_ortec_fetch_req=>parse_capabilities`).
- The O(reachable-graph-size) tree walk (`get_reachable_sha1s` +
  `has_dangling_delta_base`) previously performed by every
  `is_commit_complete` call.

## Legacy methods still physically present (unreachable from migrated paths)

- `build_upload_pack_buffer` (public) - kept with its own pre-existing unit
  tests (`ltcl_fastpath_protocol`), scheduled `DELETE_LATER_IN_SLICE_9`.
- `first_progressive_deepen`, `next_progressive_deepen`,
  `c_progressive_start_min`, `c_progressive_widen_factor`,
  `c_progressive_max_deepen`, `c_progressive_max_steps` - same status.

## Strict Slice 3 boundary status: RESPECTED

- `fetch_tip_commits` untouched; its only caller
  (`zcl_abapgit_ortec_branch_list.clas.abap`) is pre-existing and unrelated
  to this sub-slice - confirmed via repo-wide grep, no new caller added.
- No new code calls, wraps, or delegates to `fetch_tip_commits`.
- No new request contains `deepen` or `shallow` (confirmed via grep across
  the 5 migrated methods; the only remaining occurrences are inside
  `fetch_tip_commits`/`build_upload_pack_buffer`, both explicitly out of
  scope/legacy).
- No uncertified haves: have resolution for `INCREMENTAL_THIN`/
  `INCREMENTAL_SELF_CONTAINED` still goes through
  `get_verified_have_commits` → `is_commit_complete` →
  `is_graph_have_eligible` (certified).
- No unadvertised capability requested: `iv_server_caps` is now threaded
  from `parse_capabilities` into every migrated `build_request` call.
- `zcl_abapgit_ortec_filter_walk`, `zcl_abapgit_ortec_repo_state`
  (DR-001/DR-002 readers/writer), and `collect_ancestor_haves`/
  `get_have_commits` were NOT touched, per the migration map's explicit
  `DEFER_TO_SLICE_3` classification.

## Tests added and executed

Added/updated in `ltcl_completeness_gate`
(`zcl_abapgit_ortec_git_tests.clas.testclasses.abap`):

- `complete_false_missing_object` (updated comment only, same assertion):
  an uncertified commit is not have-eligible.
- `complete_false_uncertified` (new): a commit fully present in
  `zaog_obj_store` but never certified via `begin_attempt`/
  `mark_graph_complete` is reported not-eligible - proves the delegation
  gates on certification, not object presence.
- `complete_true_full_complete` (new): `hist_level = FULL_COMPLETE` is
  reported eligible, not just `GRAPH_COMPLETE`.
- `complete_true_without_index` (updated): now certifies the commit via
  `begin_attempt`/`mark_graph_complete` before asserting eligibility
  (previously relied on object-store completeness alone, which no longer
  governs `is_commit_complete`).
- `complete_true_when_ready` (updated): same certification addition.

Added in `ltcl_stream_resolve` (correction pass, this session):

- `missing_base_no_http_retry` (new): a genuinely missing external delta
  base with a non-blank (but unreachable) `iv_url` must still raise with
  `mv_retry_without_haves = abap_true` and must NOT attempt a per-base HTTP
  completion fetch (proves F-2C-001's fix: `complete_missing_base` no
  longer calls out to the network before escalating).

Not executed (no live ABAP Unit runner in this pass) - explicitly pending
SAP validation.

Existing tests confirmed structurally unaffected (not executed, but
statically verified to still target unchanged methods/guards):
`ltcl_fastpath_protocol` (6 tests targeting `build_upload_pack_buffer`/
`parse`/`first_progressive_deepen`/`next_progressive_deepen`, all
unchanged), `ltcl_filtered_fetch` (2 tests targeting
`try_filtered_commit_fetch`'s unchanged early-return guards), and the
2A/2B `zcl_abapgit_ortec_fetch_req.clas.testclasses.abap` `ltcl_fetch_req`
suite (17-18 tests, file untouched by this sub-slice or by the correction
pass).

## Performance scan verdict (original implementation)

`PASS` (`ortec-abapgit-performance-scan`) - zero new SQL/HTTP in a hot
path, zero new repository-wide read, zero per-object cache
invalidation/COMMIT WORK, progressive-deepen loop confirmed unreachable,
`is_commit_complete` confirmed O(1), no new unbounded XSTRING copy.

## Performance implementation audit verdict (original implementation)

`PASS` (`ortec-abapgit-performance-review`, `IMPLEMENTATION_AUDIT` mode).
Confirmed against the actual current source (not diff-only): have
resolution correctly mode-gated; ≤3 total upload-pack HTTP attempts per
logical fetch (down from an unbounded-in-practice progressive cascade);
`reset_completion_budget()` bounded at ≤2 calls per logical fetch;
`is_commit_complete` genuinely O(1); `collect_ancestor_haves`'s
pre-existing unbounded `zaog_obj_store` read correctly left untouched
(tracked Slice 3 candidate, not this sub-slice's scope - the audit
explicitly does not claim the full have-resolution pipeline is
N-independent). Note: this audit predates the correction pass and did not
catch F-2C-001 (per-object HTTP completion) - see the dedicated
correction-pass audit below.

## Dedicated correction-pass performance audit (F-2C-004)

`PASS` - see
`.memory/logs/performance_audit_variant-b-partial-clone_slice2c.md` for
full detail. Confirms the correction pass is strictly performance-positive
(eliminates the one-request-per-missing-base HTTP pattern entirely) and
performance-neutral on every other measured dimension (SQL shape,
memory/XSTRING footprint unchanged).

## Regression verdict

`PASS` (static/structural). Original-implementation evidence in
`.memory/logs/regression_variant_b_slice2_2c.md`. Correction-pass evidence
(this session): `get_errors` clean on all 3 edited files after the
correction; `ortec-abapgit-regression` subagent run confirmed (a) zero
remaining callers of `complete_missing_object`, (b) all 6
`ev_deepen_used` assignments in the migrated tiers are `= 0`, (c) the new
test method is uniquely named and ≤30 chars, (d)
`zcl_abapgit_ortec_fetch_req.clas.testclasses.abap` untouched, (e) no
public-signature impact on any caller. The subagent's raw diff-based check
also flagged hunks under `ltcl_completeness_gate` as "out of the claimed
scope" - independently verified via `git diff` hunk boundaries
(`@@ -743,6 +743,13 @@` through `@@ -1106,7 +1235,7 @@` for
`ltcl_completeness_gate`, vs. `@@ -2753,6 +2882,11 @@`/
`@@ -3030,6 +3164,50 @@` for the correction pass's only touched class,
`ltcl_stream_resolve`) to be entirely pre-existing, uncommitted content
from the ORIGINAL 2C implementation (predates this correction session,
already documented above) - not a scope violation by the correction pass.
Overall corrected verdict: `PASS`. All public call sites of the migrated
methods confirmed compatible (only the PRIVATE `upload_pack` helper's
signature changed). One pre-existing, out-of-scope observation carried
forward: two method names in `ltcl_fastpath_protocol`
(`buffer_sends_deepen_even_forced` = 31 chars,
`progressive_deepen_widens_and_caps` = 34 chars) already exceed the
30-character ABAP identifier limit at HEAD, before this sub-slice - not
introduced by 2C or this correction, not fixed here (out of scope),
flagged for awareness.

## SAP validations pending

- Import into IT8.
- Activation of all affected objects
  (`zcl_abapgit_ortec_fastpath.clas.abap`,
  `zcl_abapgit_ortec_fetch_neg.clas.abap`,
  `zcl_abapgit_ortec_pack_stream.clas.abap`,
  `zcl_abapgit_ortec_git_tests.clas.abap`/`.testclasses.abap`).
- ABAP Unit execution (all of `ltcl_completeness_gate`,
  `ltcl_stream_resolve` (incl. the new `missing_base_no_http_retry`),
  `ltcl_fastpath_protocol`, `ltcl_filtered_fetch`, plus the full existing
  suite).
- ATC static check on the live system.

## Blockers

None. `BLOCKED_BY_SLICE7_PRECONDITION` was not encountered - the per-base
HTTP completion call was safely disabled without requiring any part of
Slice 7's redesign; `BLOCKED_BY_SLICE3_PRECONDITION` was not encountered
either - no migrated path required any deferred Slice 3 behavior.

## Notable incident this session (process, not code)

An initial delegation of this implementation to
`ortec-abapgit-implementation-senior` produced a silently broken, partial
result (one of six call sites half-migrated, leaving the file
non-compiling) and, separately, destructively truncated three unrelated
`.memory` files (`state.md`, this sub-slice's own checkpoint handoff, and
the 2A/2B regression log) that it was never asked to touch. Both were
detected via `git status`/`git diff` immediately after the subagent
"completed with no output", reverted via `git checkout HEAD --
<file>`, and the entire implementation was redone directly. See
`/memories/abap-mcp-notes.md` for the recorded lesson. No corruption
remains in the current working tree or in this handoff's described state.

## Proposed checkpoint file list

- `src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap`
- `src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap`
- `src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.abap`
- `src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap`
- `.memory/logs/variant_b_slice2c_migration_map.md`
- `.memory/logs/regression_variant_b_slice2_2c.md`
- `.memory/logs/performance_audit_variant-b-partial-clone_slice2c.md`
- `.memory/handoffs/variant-b-slice2c-checkpoint.md` (this file)
- `.memory/state.md`

## Proposed functional commit subject

`ortec: Slice 2C - migrate fastpath call sites to explicit fetch modes,
correct per-base HTTP completion and deepen/have-failure handling`

## Proposed functional commit subject

`ortec: Slice 2C - migrate fastpath call sites to explicit fetch modes`
