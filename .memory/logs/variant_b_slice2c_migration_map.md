# Variant B — Slice 2C call-site migration map

Status: pre-implementation map, written before any productive edit.
Scope: `variant-b-partial-clone`, Sub-slice 2C only. Basis: current source
(read directly, not re-derived from the Slice 2 design's own line-number
citations, which may have drifted) —
`src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap`,
`zcl_abapgit_ortec_fetch_neg.clas.abap`, `zcl_abapgit_ortec_fetch_req.clas.abap`
(existing 2A/2B serializer), `zcx_abapgit_ortec_git.clas.abap`,
`zcl_abapgit_ortec_mat_state.clas.abap`, `zcl_abapgit_ortec_pack_stream.clas.abap`,
`src/git/zcl_abapgit_git_transport.clas.abap` — plus
`.memory/logs/variant_b_slice2_design.md` §3/§4/§8 for the approved shape.

**Confirmed gap vs. the 2A/2B checkpoint:** `zcl_abapgit_ortec_fetch_neg=>is_commit_complete`
still runs the O(tree-size) `get_reachable_sha1s`/`has_dangling_delta_base` walk
today — the §4 swap to `zcl_abapgit_ortec_mat_state=>is_graph_have_eligible`
was **not** part of 2A/2B and is required by 2C's own mandatory behavior #3/#4
("materialization certification for have eligibility", "only certified
commits can become haves"). Included below as call site #6.

**Confirmed gap vs. the design's capability-gating assumption:** none of the
three live tiers in `upload_pack_by_branch`/`upload_pack_by_commit` currently
parse server-advertised capabilities at all — `build_upload_pack_buffer`
decides thin-pack/ofs-delta advertisement purely from "do verified haves
exist", never from what the server actually advertises. 2C requirement #5
("intersect requested capabilities with server-advertised capabilities")
requires adding one `zcl_abapgit_ortec_fetch_req=>parse_capabilities(
lo_client->get_cdata( ) )` call per connection, immediately after the client
is obtained and before `set_headers` (the same point `try_filtered_commit_fetch`/
`fetch_tip_commits` already read `get_cdata()` at — confirmed calling
`get_cdata()` twice on one client, once inside `find_branch_ortec`→`branch_list`
and once again by the caller, already works today, since `try_filtered_commit_fetch`
does exactly this against a client obtained via `find_branch_ortec`).

---

## Call sites

### 1. `zcl_abapgit_ortec_fastpath=>upload_pack` (private) — `MIGRATE_IN_2C`

- Current request-building method: `build_upload_pack_buffer( iv_deepen_level, it_hashes, it_ortec_haves, iv_allow_thin, iv_force_full )`.
- Selected Fetch Mode: caller-supplied — signature changes from
  `iv_allow_thin TYPE abap_bool` / `iv_force_full TYPE abap_bool` to a single
  `iv_mode TYPE zcl_abapgit_ortec_fetch_req=>ty_fetch_mode`. Haves are
  resolved (`get_verified_have_commits`) only for
  `incremental_thin`/`incremental_self_contained`; never for
  `recovery_branch_full`/`materialize_blobs`.
- Current have source: `zcl_abapgit_ortec_fetch_neg=>get_verified_have_commits`
  (unchanged call).
- Future certified have source: same call — now backed by call site #6's
  `is_graph_have_eligible` swap.
- Capability source: new `iv_server_caps TYPE string OPTIONAL` parameter,
  supplied by the caller (see #2/#3/#4) via `parse_capabilities`.
- Current retry/recovery behavior: none inside `upload_pack` itself (caller-owned).
- Planned new retry/recovery: unchanged responsibility boundary — `upload_pack`
  becomes: resolve certified haves (mode-gated) → build capability-gated
  `it_want_hashes` (unchanged: `it_hashes` as passed) → call
  `zcl_abapgit_ortec_fetch_req=>build_request` → send/decode exactly as today.
- Files/methods to change: `zcl_abapgit_ortec_fastpath.clas.abap`, private
  method `upload_pack`.
- Files/methods explicitly unchanged: `serve_cached_when_nothing_new`,
  the streaming/non-streaming decode dispatch below the buffer-build point.
- Tests required: a class-local test proving `upload_pack` delegates to
  `zcl_abapgit_ortec_fetch_req=>build_request` (not `build_upload_pack_buffer`)
  for each of the three modes it can receive.

### 2. `zcl_abapgit_ortec_fastpath=>upload_pack_by_branch` — `MIGRATE_IN_2C`

- Current request-building: 3-tier cascade — tier 1 `iv_allow_thin = abap_true`;
  tier 2 `iv_allow_thin = abap_false`; tier 3 a `DO c_progressive_max_steps TIMES`
  loop with `iv_force_full = abap_true` and a widening `iv_deepen_level`
  (`first_progressive_deepen`/`next_progressive_deepen`).
- Selected Fetch Mode: tier 1 → `INCREMENTAL_THIN`; tier 2 →
  `INCREMENTAL_SELF_CONTAINED`; tier 3 → exactly **one**
  `RECOVERY_BRANCH_FULL` attempt (loop, widening constants, and
  `first_progressive_deepen`/`next_progressive_deepen` calls removed from
  this method's live flow).
- Current have source / future certified have source: same as #1 (delegated).
- Capability source: add `lv_ref_data = lo_client->get_cdata( )` +
  `zcl_abapgit_ortec_fetch_req=>parse_capabilities( lv_ref_data )` immediately
  after each `find_branch_ortec` call (3 call sites today — tier 1, tier 2,
  tier 3 loop body; becomes 3 call sites — tier 1, tier 2, single tier 3
  attempt), before `set_headers`.
- Current retry/recovery: thin → non-thin → progressive-widening loop
  (bounded by `c_progressive_max_steps`/`c_progressive_max_deepen`).
- Planned new retry/recovery: thin → self-contained → **one**
  `RECOVERY_BRANCH_FULL` attempt, no widening, no `deepen`. Add
  `zcl_abapgit_ortec_pack_stream=>reset_completion_budget( )` immediately
  before the `RECOVERY_BRANCH_FULL` attempt (DR-004 resolution — in addition
  to, not instead of, the existing once-per-call reset at method entry).
- Files/methods to change: `zcl_abapgit_ortec_fastpath.clas.abap`, method
  `upload_pack_by_branch`.
- Tests required: mode selection per tier; no `deepen`/`shallow` token at
  tier 3; structural proof the `DO ... TIMES` loop and
  `first_progressive_deepen`/`next_progressive_deepen` are unreachable from
  this method.

### 3. `zcl_abapgit_ortec_fastpath=>upload_pack_by_commit` — `MIGRATE_IN_2C`

- Mirrors #2 exactly, keyed off `iv_deepen_level` instead of branch
  resolution; client obtained via `zcl_abapgit_http=>create_by_url` (not
  `find_branch_ortec`) at each of the 3 call sites today.
- Selected Fetch Mode: identical mapping to #2.
- Capability source: `parse_capabilities( lo_client->get_cdata( ) )`
  immediately after each `create_by_url` call, before `set_headers`.
- Retry/recovery: identical replacement to #2 (one `RECOVERY_BRANCH_FULL`
  attempt, `reset_completion_budget( )` added before it).
- Files/methods to change: `zcl_abapgit_ortec_fastpath.clas.abap`, method
  `upload_pack_by_commit`.
- Tests required: same shape as #2.

### 4. `zcl_abapgit_ortec_fastpath=>complete_missing_object` — `MIGRATE_IN_2C`

- Current: `upload_pack( iv_deepen_level = 1, it_hashes = [iv_sha1],
  iv_allow_thin = abap_false, iv_force_full = abap_true )`.
- Selected Fetch Mode: `MATERIALIZE_BLOBS`, `it_want_hashes = [iv_sha1]`
  (batch of exactly 1, well under `c_materialize_batch_max`).
- Have source: none (mode never negotiates haves — matches today's
  `iv_force_full = abap_true` behavior of skipping have resolution).
- Capability source: `parse_capabilities( lo_client->get_cdata( ) )`
  immediately after `zcl_abapgit_http=>create_by_url`. `MATERIALIZE_BLOBS`
  hard-requires `allow-reachable-sha1-in-want`/`allow-tip-sha1-in-want`; a
  missing capability makes `build_request` raise `zcx_abapgit_ortec_git`
  with `mv_unsupported_capability = abap_true`, which propagates through
  `complete_missing_object` unchanged (its existing `CATCH
  zcx_abapgit_exception` does not intercept `zcx_abapgit_ortec_git`, so no
  new catch is needed — behavior is "raise as ortec_git", identical in
  shape to today's decode-failure path, just a different cause).
- Current/planned retry: none, single attempt, unchanged.
- Files/methods: `zcl_abapgit_ortec_fastpath.clas.abap`, method
  `complete_missing_object`.
- Tests required: assert `MATERIALIZE_BLOBS` with a 1-element want list is
  used; assert a missing-capability server response raises
  `mv_unsupported_capability = abap_true`.

### 5. `zcl_abapgit_ortec_fastpath=>try_filtered_commit_fetch` — `MIGRATE_IN_2C`

- Current: hand-built `want <commit> ... deepen 1 ... filter blob:none`
  buffer with an inline capability-parse block duplicating
  `fetch_tip_commits`'s.
- Selected Fetch Mode: `INITIAL_BRANCH_BLOBLESS`, `it_want_hashes = [iv_commit]`.
- Have source: none (mode never negotiates haves — unchanged).
- Capability source: replace the inline duplicate parse with
  `zcl_abapgit_ortec_fetch_req=>parse_capabilities( lv_ref_data )` (same
  `lv_ref_data` already captured via `lo_client->get_cdata( )` at the
  existing point in this method).
- Current retry/recovery: none; the entire body is wrapped in
  `CATCH zcx_abapgit_exception zcx_abapgit_ortec_git. CLEAR rv_applicable.`
  (silent fallback to `rv_applicable = abap_false`); missing `filter`
  capability today causes an early plain `RETURN` (also `rv_applicable =
  abap_false`).
- Planned new retry/recovery: **unchanged outer behavior**, per the approved
  design's DR-001 resolution — end-to-end surfacing of
  `mv_unsupported_capability` past this method's own catch to its caller
  (`zcl_abapgit_ortec_filter_walk=>get_remote_files_for_stage`) is
  explicitly deferred to Slice 3. `build_request` raising on missing
  `filter` is allowed to fall into the same existing broad catch, producing
  the identical `rv_applicable = abap_false` result as today's early
  `RETURN` — net caller-visible behavior does not change in 2C.
- Files/methods: `zcl_abapgit_ortec_fastpath.clas.abap`, method
  `try_filtered_commit_fetch`.
- Tests required: successful-path buffer shape matches
  `build_request( iv_mode = initial_branch_blobless )`'s output (delegation);
  missing-filter behavior (`rv_applicable = abap_false`) proven unchanged
  (regression, not new).

### 6. `zcl_abapgit_ortec_fetch_neg=>is_commit_complete` — `MIGRATE_IN_2C`

- Current: `zcl_abapgit_ortec_obj_store=>get_reachable_sha1s` +
  `has_dangling_delta_base` (O(reachable-graph-size) walk per candidate).
- New: single delegating line —
  `rv_yes = zcl_abapgit_ortec_mat_state=>is_graph_have_eligible( iv_repo_key =
  iv_repo_key iv_commit = iv_commit ).` (O(1) certified read). Signature and
  all callers (`get_verified_have_commits`) unchanged.
- Files/methods: `zcl_abapgit_ortec_fetch_neg.clas.abap`, method
  `is_commit_complete`.
- Tests required: a commit with no `zaog_commit_hist` row (or
  `hist_level = UNKNOWN`) is reported not-eligible even if fully present in
  `zaog_obj_store`; a commit with `hist_level IN (GRAPH_COMPLETE,
  FULL_COMPLETE)` is reported eligible.

---

## Explicitly NOT migrated in 2C

- `zcl_abapgit_ortec_fastpath=>fetch_tip_commits` — `DEFER_TO_SLICE_3`
  (strict Slice 3 boundary; owner's 5-mode list has no
  commits-plus-shallow-trees-only shape; no new 2C code may call, wrap, or
  reuse this method).
- `zcl_abapgit_ortec_filter_walk=>get_remote_files_for_stage` (default
  `FETCH_COMMIT` lookup) — `DEFER_TO_SLICE_3` (design §5, DR-001).
- `zcl_abapgit_ortec_fastpath=>pull_by_branch` fast-path shortcut —
  `DEFER_TO_SLICE_3` (design §5, DR-001).
- `zcl_abapgit_ortec_repo_state=>update_after_fetch` (writer) —
  `DEFER_TO_SLICE_3` (design §5, DR-002); `get_complete_commits` is not
  itself changed, but its candidates transitively benefit from #6's swap.
- `zcl_abapgit_ortec_fetch_neg=>get_have_commits`/`collect_ancestor_haves` —
  `DEFER_TO_SLICE_3` (explicit strict-boundary item: "redesign of the
  N-dependent `collect_ancestor_haves`"). Unchanged in 2C; still the sole
  supplier of have *candidates* before #6's certification filter runs.
- `build_upload_pack_buffer` (public), `first_progressive_deepen`,
  `next_progressive_deepen`, `c_progressive_start_min`,
  `c_progressive_widen_factor`, `c_progressive_max_deepen`,
  `c_progressive_max_steps` — `LEGACY_BUT_UNREACHABLE_AFTER_2C`: no longer
  called from any live tier once #2/#3 land; declarations, bodies, and their
  existing unit tests remain physically present (phased-removal rule) and
  are scheduled for `DELETE_LATER_IN_SLICE_9`.
- `is_retry_without_haves`/`mv_retry_without_haves` — unchanged; still gates
  whether the (now-single) `RECOVERY_BRANCH_FULL` attempt is attempted at
  all, orthogonal to the wire-shape change (design §3, "Unchanged").

## Ambiguous classification (none blocking 2C)

No discovered request path in scope for 2C was left ambiguous. Every path
above is either `MIGRATE_IN_2C`, `DEFER_TO_SLICE_3`, or
`LEGACY_BUT_UNREACHABLE_AFTER_2C` with a `DELETE_LATER_IN_SLICE_9` follow-up.

## Focused protocol/persistence validation of this map

- No DDIC object is added or changed by any item in this map (confirmed:
  every change is either a private-method signature/body edit or a
  delegating one-liner — no new table, no new field).
- The new "call `get_cdata()` a second time on an already-connected client"
  pattern (call sites #2/#3/#4) is confirmed safe by direct source read:
  `zcl_abapgit_http_client=>get_cdata` (`src/http/zcl_abapgit_http_client.clas.abap#L121`)
  is `rv_value = mi_client->response->get_cdata( ).` — a pure, non-destructive
  getter on the already-received response object. This is the exact pattern
  `try_filtered_commit_fetch`/`fetch_tip_commits` already rely on today
  (`find_branch_ortec` internally calls `get_cdata()` once via `branch_list`
  to build the branch list, then the caller calls it again) — confirmed
  already working, not a new risk introduced by this map.
- No transaction/COMMIT WORK implication: every migrated method remains
  read-only with respect to `ZAOG_*` tables except through the existing,
  unchanged decode/persist pipeline invoked after a successful fetch
  (unchanged by this slice).
- `is_commit_complete`'s swap (#6) reuses the exact `is_graph_have_eligible`
  API and stale-read characterization already reviewed and accepted in the
  Slice 2 design's §4 "Concurrency note" and the protocol/persistence
  review's resolution recorded in `.memory/logs/variant_b_slice2_design.md`
  — no new concurrency surface is introduced by wiring it into 2C's actual
  call site versus the design's already-approved description.
- Conclusion: no new protocol/persistence risk beyond what Slice 2's design
  review already accepted. No additional dedicated protocol/persistence
  subagent pass required for 2C.
