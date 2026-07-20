# Incident: branch-switch "Walk, tree not found" in ES6 production (2026-07-12)

## Report
Michael tested in ES6 (production) after a clean IT8 import (Phase 1-7 + ATC fixes, up to
commit `85d44685`). Switching branches: shows "retrieves data from remote repo", then spends a
long time in a "decode delta" step calling `zcl_abapgit_zlib_huffman` (the slow pure-ABAP
manual decoder), then fails with "Walk, tree not found".

## Answer to "is the target flow (target_architecture.mmd) implemented or incomplete?"
**Incomplete - a specific, known box in the diagram was deliberately deferred and never built.**
The diagram's `H4` hook (`zcl_abapgit_git_porcelain=>walk / walk_tree` -> "missing node ->
delegate to collector" -> `MO` = `zcl_abapgit_ortec_missing_obj`) was explicitly deferred in
Phase 4 ("deliberately left walk/walk_tree ... untouched ... since that repair path already
works correctly" - see the Phase 4 scope note in `.memory/state.md`). That assumption is now
shown to be wrong for the branch-switch/full-pull scenario. Separately, the six-state model /
unified status engine (`SE` in the diagram) is ALSO not wired in (confirmed: `cs_object_state`
has exactly one production reference, an error-message string) - but that gap affects
Stage/Diff *status classification* only, not this walk crash. Two different deferred pieces of
the same diagram, both still incomplete.

## Root cause analysis (source-verified, not yet live-reproduced)

### Why `walk`/`walk_tree` can still fail with "Walk, tree not found"
`zcl_abapgit_git_porcelain=>walk`/`walk_tree` DO have a per-node Ortec fallback: if a tree/blob
is missing from the just-fetched `it_objects`, they try ONE local lookup via
`zcl_abapgit_ortec_obj_store=>get_object( iv_repo_key, iv_sha1 )`. If that ALSO misses, they
raise immediately. There is NO bulk-collect-then-remote-fetch-then-retry step at this level
(unlike the filtered Stage/Diff path, which has exactly that via
`zcl_abapgit_ortec_missing_obj`/`zcl_abapgit_ortec_obj_index=>get_files_for_filter`). For a
genuinely new/never-fetched branch, a thin/incremental pack can easily omit trees/blobs that
are also not yet in the local store, and there is nothing to recover with.

### Why thin-pack (Phase 5b.2) made this worse
Before Phase 5b.2, thin-pack/ofs-delta was never negotiated (dead code) - every fetch was a
full, self-contained pack. Phase 5b.2 made thin-pack live. Thin/incremental packs are, by
design, missing content the server assumes the client already has - which is exactly the
content `walk` cannot recover from. Enabling thin-pack exposed a pre-existing weakness
(walk's no-network-fallback) that was rarely hit before.

### Bug 1 - `zcl_abapgit_git_delta=>delta` (standard decoder's Ortec delta-base fallback)
`src/git/zcl_abapgit_git_delta.clas.abap`, method `delta`, around line 90:
```abap
ls_ortec_base = zcl_abapgit_ortec_obj_store=>get_object(
  iv_repo_key = ''    " repo_key is resolved internally
  iv_sha1     = is_object-sha1 ).
```
This is PRE-EXISTING code, not introduced this session. The comment is wrong/aspirational:
`get_object` resolves a blank `iv_repo_key` via `mv_cache_repo_key`, a CLASS-DATA (session-
global, not request-scoped) variable that is only set as a side effect of a prior call to
`get_objects`/`populate_cache` for some repo. `populate_cache` itself is never called anywhere
in the codebase (dead code). `decode_and_persist`'s own delta-base prefetch bypasses
`get_objects` entirely (raw SQL against `zaog_obj_store`), so it never sets this variable
either. Net effect: when the STANDARD decoder's delta-base fallback runs (i.e., after the
Ortec decode path has already failed and fallen back to `zcl_abapgit_git_pack=>decode`), there
is no reliable guarantee `mv_cache_repo_key` holds the correct repo's key - it could be blank
(raises "Repository key missing") or stale from an unrelated earlier repo accessed in the same
session (silently wrong data). This is very likely also what triggers the slow standard-decoder
path itself: decode_and_persist fails on this exact lookup, falls back to
`zcl_abapgit_git_pack=>decode` (the "ultra slow manual decode" Michael observed), which then
hits the SAME broken fallback for its own delta resolution.

### Bug 2 - the "self-heal" retry does not actually force a full/non-thin pack
`src/git/zcl_abapgit_git_porcelain.clas.abap`, method `pull_by_branch`: on a caught "Walk,"
error, the self-heal calls `zcl_abapgit_ortec_repo_state=>reset_fetch_commit` (clears only
`ZAOG_REPO_STATE.FETCH_COMMIT` for this branch) and retries. But
`zcl_abapgit_ortec_fetch_neg=>get_have_commits` sources haves from
`zcl_abapgit_ortec_repo_state=>get_complete_commits`, which checks `ZAOG_COMMIT_HIST` FIRST and
only falls back to `ZAOG_REPO_STATE.FETCH_COMMIT` if history is empty. `reset_fetch_commit`
never touches `ZAOG_COMMIT_HIST`. So once any commit has ever been recorded in history for this
repo (i.e., after the very first successful fetch), the "self-heal" retry can still advertise
those historical commits as haves, the server can still respond with a thin/delta pack, and the
retry can fail the exact same way. The comment's stated guarantee ("forcing the server to
deliver a complete (non-thin) pack") does not actually hold.

## Recommended fix plan (not yet implemented - pending Michael's direction)
1. **Immediate mitigation (lowest risk, fastest to ship):** disable thin-pack/ofs-delta
   negotiation again (force `iv_allow_thin = abap_false` always, or flip a switch) until the
   walk-delegation gap is properly closed. Restores pre-Phase-5b.2 behavior: always-full packs,
   self-contained, no external delta-base dependency - eliminates the new exposure while keeping
   everything else (Phases 1-7) in place. Performance cost: loses the thin-pack bandwidth win.
2. **Two concrete, surgical bug fixes (small, safe, independent of #1):**
   - Fix `zcl_abapgit_git_delta=>delta`'s fallback to accept/require a real `iv_repo_key`
     instead of relying on the unreliable `mv_cache_repo_key` ambient state (needs a signature
     change and updating `decode_deltas`'s/`decode`'s call chain to thread the repo_key through -
     touches standard `zcl_abapgit_git_pack`/`zcl_abapgit_git_delta`, needs care re: D7 budget).
   - Fix `reset_fetch_commit`'s self-heal to also clear (or bypass) `ZAOG_COMMIT_HIST` for the
     affected commit/branch so the retry is actually guaranteed non-thin, OR switch the self-heal
     to call `invalidate_tip_commit` (which already clears history) instead of `reset_fetch_commit`.
3. **Full architectural fix (larger, matches target_architecture.mmd's H4 box):** give
   `walk`/`walk_tree` a real bulk-collect-then-fetch-then-persist-then-retry capability
   (mirroring `zcl_abapgit_ortec_missing_obj`), so a genuinely-missing node triggers ONE targeted
   remote repair instead of an immediate hard failure. This is the correct, complete fix but is
   a bigger change that deserves its own design/review pass (this is exactly the kind of change
   Phase 4 explicitly deferred to avoid exceeding the D7 minimal-touch budget without a design
   review first).

Status: diagnosis only, no code changed yet. Awaiting direction on scope/urgency.

## Update 2026-07-12: option (b) implemented, option (c) still pending

Michael directed: skip the immediate mitigation (option a, disable thin-pack), implement the
surgical bug fixes now, and separately design the full H4 architecture (option c) as a proper
follow-up. Option (b) is implemented and committed (`51c1c52e` on
`ortec/abapgit_1_133-opt-rework`):

- `zcl_abapgit_ortec_obj_store=>set_active_repo_key` (new): explicitly sets `mv_cache_repo_key`.
  Called immediately before every standard-decoder fallback that could hit
  `zcl_abapgit_git_delta=>delta`'s blank-`iv_repo_key` lookup:
  `zcl_abapgit_ortec_fastpath`'s shared `upload_pack` (using the already-resolved `lv_ortec_rk`),
  and `zcl_abapgit_git_transport`'s `upload_pack_by_branch`/`upload_pack_by_commit` (resolving via
  `zcl_abapgit_ortec_repo_state=>get_repo_key_for_url`). This closes the "accidental, possibly
  stale/wrong repo" fragility without touching any standard method signature.
- `zcl_abapgit_ortec_repo_state=>invalidate_all_history` (new): clears `ZAOG_COMMIT_HIST` and
  blanks `ZAOG_REPO_STATE.FETCH_COMMIT` for the WHOLE repo (all branches), not just one
  commit/branch. `pull_by_branch`'s self-heal now calls this instead of `reset_fetch_commit`,
  so the retry's `get_complete_commits`/`get_have_commits` genuinely return no haves, forcing a
  full/deepen pack as the self-heal's comment always claimed it did.
- Regression tests added: `ltcl_obj_store=>active_repo_key_fallback`,
  `ltcl_repo_state=>invalidate_all_history_repo_wide`.

**What this does NOT fix**: a genuinely first-time-seen commit/tree/blob (never persisted to
`ZAOG_OBJ_STORE` before, e.g. a brand-new branch whose ancestry isn't shared with anything
already cached) will still hit "Walk, tree not found" with no automatic repair, because
`walk`/`walk_tree` still only have a single local-store lookup and no bulk-fetch-and-retry
capability. That is option (c) - the H4 architecture - and remains a separate, larger
design/implementation effort, not started.

Not yet re-verified live by Michael in IT8/ES6.

## Reassessment: is H4 still a correctness gap, or now "only" a performance one?

Worth flagging before investing in a full H4 design pass: with `invalidate_all_history` fixed,
the self-heal's retry sends a `pull_by_branch`/`upload_pack_by_branch` request with **zero have
lines** for the WHOLE repo. Per standard git semantics, a want-request with no haves and no
`deepen` still yields a full, self-contained pack (this is exactly what a fresh clone gets) -
independent of whether `deepen` fires. A self-contained pack cannot contain a ref-delta whose
base is outside the pack (that's specifically what makes a pack "thin"), so:

- `zcl_abapgit_git_delta=>delta`'s blank-`iv_repo_key` fallback (Bug 1) should never even be
  needed on the retry - there's nothing to resolve externally.
- `walk`/`walk_tree` should find every tree/blob directly in the freshly-decoded `it_objects` on
  the retry - the local-store fallback (the thing H4 would replace) never needs to fire either.

If this reasoning holds, the two fixes already landed close the **correctness** gap for the
"genuinely first-time-seen object" case too (via one full re-fetch on self-heal), not just the
"stale/wrong repo_key" and "self-heal is a no-op" cases they were designed for. What H4 would
still add is **efficiency**: avoiding a full repo re-fetch in favor of a small, targeted
bulk-fetch of just the missing objects when only a handful of nodes are actually absent (e.g. a
large repo where only one new branch's few new commits need fetching, not its entire shared
history). Under the project's own priority order (correctness > performance > maintainability),
that reclassifies H4 from "must-fix architecture gap" to "worthwhile future optimization" -
pending Michael confirming this reasoning holds up against a live ES6 retest.

## Decision (2026-07-12)

Michael: retest commit `51c1c52e` in IT8/ES6 (reproduce the branch-switch scenario) before
committing effort to H4. If the retest confirms correctness is restored, H4 is DEFERRED to the
performance backlog (targeted-fetch efficiency only, not a correctness fix) - not scheduled as
next work. If the retest still fails, re-open root-cause analysis (the "zero haves -> always a
full, self-contained pack" assumption would need to be revisited - e.g. check whether the ORTEC
fastpath's OWN internal negotiation ever advertises something outside `ZAOG_COMMIT_HIST`/
`ZAOG_REPO_STATE` that `invalidate_all_history` doesn't clear).
