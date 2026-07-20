# Investigation: "Delta base not found, 35fcfcb0379260fd21602953a666827b64d931f3"

Status: **READ-ONLY investigation, re-run from scratch, no files edited.**
Scope: `zcl_abapgit_ortec_pack_stream`, `zcl_abapgit_ortec_obj_store`,
`zcl_abapgit_ortec_fastpath`, `zcl_abapgit_ortec_delta` (all fully read),
plus live queries against `zaog_obj_store` / `zaog_repo_state` on IT8.

## 1. Findings (evidence-based, file/line refs)

- Confirmed live (arc-1 `SAPQuery`, re-verifying the user's own DB check):
  `SELECT * FROM zaog_obj_store WHERE obj_sha1 = '35fcfcb0...'` → **0 rows**,
  across every `repo_key`/`status` currently in the table. The SHA1 is
  genuinely absent, not a status-filter/repo_key artifact.
- Repo identity confirmed via `zaog_repo_state`: `repo_key = d3bd7be4d030`,
  `REMOTE_URL = https://github.com/mkaesemann/abapGit`, tracked with
  **`IS_SHALLOW = 'X'`, `DEEPEN_LVL = 1`** on both tracked branches
  (`ortec/abapgit_1_133-optimized`, `ortec/abapgit_1_133-opt-rework`). This
  repo is a shallow (depth-1) snapshot, not a full clone — the mechanism
  described in `/memories/session/shallow_negotiation_design.md` (client
  sends `have <shallow-commit>` without ever sending a matching `shallow`
  line, so the server is entitled to assume the client possesses that
  commit's full ancestry) applies directly to this repo.
- [zcl_abapgit_ortec_pack_stream.clas.abap](src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.abap):
  `get_base_bytes` (~line 267) and `resolve_one_meta`'s REF_DELTA-external
  branch both raise `"Delta base not found, {sha1}"` with
  `iv_retry_without_haves = abap_true`. Verified byte-for-byte identical
  REF_DELTA/OFS_DELTA cursor math (`get_type`/`get_length`/`get_offset`,
  the `+lv_compressed_len` then `+4` Adler32 skip) against the proven
  `zcl_abapgit_ortec_pack_dec` decoder — no divergence found.
- `resolve_streaming`'s two-pass algorithm (repeated ascending in-pack
  sweeps, then one thin-fetch pass) is structurally the same design as
  `zcl_abapgit_ortec_delta=>resolve_all`/`resolve_one` (fully read this
  session) — a design already proven in the older, non-streaming Ortec
  decoder. Traced convergence by hand for both in-order and
  backward-declared REF_DELTA chains: it terminates correctly in O(n)
  sweeps in every case considered; no cap on sweep count.
- [zcl_abapgit_ortec_obj_store.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap):
  `get_object`/`get_objects` (~line 284/311) — the session cache
  (`mt_cache`) only ever caches rows already filtered `status = 'R'`
  (both the cache-hit path and `read_object_rows`/`populate_cache`
  enforce this), so the raw `UPDATE ... SET status = 'R'` at the end of
  `decode_and_persist_streaming` not calling `invalidate_cache()` is
  **not** exploitable — an `'I'`-status row can never enter the cache in
  the first place. Repo-key handling (`iv_repo_key OPTIONAL` fallback via
  `set_active_repo_key`/`mv_cache_repo_key`) is irrelevant here: every
  caller in `zcl_abapgit_ortec_pack_stream` passes `iv_repo_key` explicitly
  and non-blank.
- [zcl_abapgit_ortec_fastpath.clas.abap](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap):
  independently re-verified `build_upload_pack_buffer` (~line 1107) — for
  `iv_force_full = abap_true` no `shallow`, no `deepen`, no `have` lines are
  emitted; only `want <sha> <caps>` + flush + `done`. Also verified
  `is_retry_without_haves` (~line 1098) does a correct `IS INSTANCE OF
  zcx_abapgit_ortec_git` + safe down-cast, so it works correctly
  regardless of the static type ABAP infers for the multi-class
  `CATCH zcx_abapgit_ortec_git zcx_abapgit_exception INTO DATA(...)` used
  at every retry tier — the tier-3 gate is not silently short-circuited.
  All three retry tiers are confirmed reachable and each does obtain a
  fresh `lo_client`/`ev_branch` via `find_branch_ortec` before its own
  `upload_pack` call.
- `zcl_abapgit_ortec_base_cache` uses a 256 MB byte-budget LRU
  (`c_budget_bytes`), not a row-count cap; this repo's total stored volume
  (≈1.8k–41k rows depending on repo_key) is nowhere near that budget, so a
  cache-eviction-races-DB-flush theory is possible in principle but very
  unlikely to be the trigger for this specific, small repo.

## 2. Ranked root-cause hypotheses

1. **(Leading, unresolved) Shallow "have"-without-"shallow" negotiation lie
   causing the thin/non-thin tiers to receive a pack whose REF_DELTA bases
   assume ancestor objects we never actually fetched.** Directly
   corroborated by live data: this exact repo is tracked shallow
   (`DEEPEN_LVL=1`) and — per prior design review — never sends a
   `shallow <sha>` line alongside its `have <sha>` lines. A compliant
   server receiving `have <shallow-tip>` with no `shallow` marker is
   entitled to assume full ancestry is present client-side and may
   therefore emit REF_DELTA objects encoded against ancestor content the
   client does not truly hold. This fully explains identical failures on
   tiers 1 and 2 (both still send this same shallow commit as a `have`).
   **Gap**: does not, by itself, explain why tier 3 (force_full — zero
   `have`/`shallow`/`deepen`, a bare `want`-only request) reproduces the
   *identical* SHA1 failure; a compliant server given a haves-free `want`
   should return a fully self-contained pack for that commit regardless of
   local shallow state. This gap must be closed before treating hypothesis
   1 as sufficient on its own.
2. **(Plausible, unverified without a wire capture) Stale/rewritten branch
   tip on the GitHub remote.** Both tracked branches are Ortec WIP
   branches (`ortec/abapgit_1_133-optimized`, `-opt-rework`), the kind
   routinely rebased/force-pushed during active development. If the branch
   was rewritten upstream after the last successful local fetch
   (2026-07-05 / 2026-07-15 per `zaog_repo_state`), `find_branch_ortec`
   would resolve a *new* current tip each retry (ruling out a stale-`want`
   bug in our own code), but if GitHub itself already garbage-collected an
   object now unreachable from any live ref — or if there is any residual
   client-side reliance on the old tip anywhere upstream of
   `upload_pack_by_branch` not covered by this read — a genuinely missing
   upstream object would reproduce identically on every tier, independent
   of thin/non-thin/deepen framing. Not ruled in or out by static review;
   requires a live wire capture or a direct GitHub API check of object
   `35fcfcb0...`/its referencing commit.
3. **(Unlikely but not fully eliminated) A resolver edge case in
   `resolve_streaming`/`resolve_one_meta` not surfaced by manual chain
   tracing.** All hand-traced scenarios (forward order, reverse order,
   OFS+REF mixed chains) converge correctly, and the algorithm mirrors the
   already-proven `zcl_abapgit_ortec_delta=>resolve_all`. No concrete
   defect found, but "no defect found by manual tracing" is weaker
   evidence than a reproducible counter-example.
4. **(Ruled out)** Cache/repo_key contamination in `zcl_abapgit_ortec_obj_store`
   — confirmed the cache can never hold a non-`'R'` row, and every relevant
   caller passes an explicit `iv_repo_key`.
5. **(Ruled out)** Decoder cursor-desync (Adler32/varint length bugs)
   producing a fabricated/garbage SHA1 that coincidentally looks real —
   the exact same shared cursor-math code (`get_type`, `get_length`,
   `get_offset`, compressed-length + Adler32 skip) is used unmodified by
   the long-proven non-streaming decoder; no divergence found anywhere in
   the streaming decoder's use of it.
6. **(Ruled out)** Tier-3 (force_full) silently not executing due to the
   multi-catch static-typing of `lx_thin_branch`/`lx_nonthin_branch` —
   `is_retry_without_haves` uses a runtime `IS INSTANCE OF` check, immune
   to this.

## 3. Fix proposal (for an implementation agent — NOT applied this session)

Two independent gaps should be closed; either could be *the* actual cause
depending on which hypothesis above is correct, and both are worth fixing
regardless:

**A. Close the shallow "have"-without-"shallow" lie** (addresses
hypothesis 1, and is a correctness bug in its own right regardless of this
specific incident):
- In whatever builds the have-set for tiers 1/2 (`zcl_abapgit_ortec_fetch_neg=>get_verified_have_commits`
  / `get_have_commits`), a commit sourced from a shallow-tracked repo
  (`zaog_repo_state-is_shallow = 'X'`) must never be offered as a bare
  `have` line without an accompanying `shallow <sha>` line in the same
  request. Proposed signature addition:
  ```abap
  "! Build the `shallow` pkt-lines that must accompany any `have` line
  "! sourced from a commit whose ancestry this client does not actually
  "! hold (i.e. every commit recorded with is_shallow = 'X' in
  "! zaog_repo_state for this repo_key).
  CLASS-METHODS get_shallow_lines
    IMPORTING iv_repo_key        TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
              it_haves           TYPE zif_abapgit_git_definitions=>ty_sha1_tt
    RETURNING VALUE(rt_shallow)  TYPE zif_abapgit_git_definitions=>ty_sha1_tt
    RAISING   zcx_abapgit_ortec_git.
  ```
  `build_upload_pack_buffer` would then always emit a `shallow` line for
  every returned SHA1 alongside the existing `have` lines (currently it
  only ever sends `shallow` lines derived from `iv_deepen_level`/first
  fetch, not from previously-recorded shallow haves on a *subsequent*
  fetch — verify against the actual current shallow-line construction
  before wiring this in).

**B. Make the "Delta base not found" diagnostics actionable for exactly
this ambiguity**, so the *next* live occurrence doesn't require a full
re-investigation:
  ```abap
  "! Classify a genuinely-missing delta base against local repo state, so
  "! callers/logs can immediately tell "expected consequence of a shallow
  "! have-lie" apart from "should never happen even with force_full".
  CLASS-METHODS classify_missing_base
    IMPORTING iv_repo_key       TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
              iv_missing_sha1   TYPE zif_abapgit_git_definitions=>ty_sha1
              iv_was_force_full TYPE abap_bool
    RETURNING VALUE(rv_class)   TYPE string  " 'SHALLOW_SUSPECT' | 'FORCE_FULL_ANOMALY' | 'UNKNOWN'
    RAISING   zcx_abapgit_ortec_git.
  ```
  Call this from the tier-3 CATCH in `upload_pack_by_branch` and fold the
  classification into the raised message text. A `'FORCE_FULL_ANOMALY'`
  result (missing base even with zero haves/shallow/deepen) is the signal
  that hypothesis 2/3 (remote GC or a real resolver bug), not hypothesis 1,
  is in play — which is exactly the ambiguity this investigation could not
  close from static review alone.

**Do not** re-attempt fixes 1–3's territory (padding, retry-flag plumbing,
deepen-skip) — all three are independently confirmed correctly implemented
and present in the current source.

## 4. Suggested regression tests

- `zcl_abapgit_ortec_fetch_neg`: a shallow-repo have-set test asserting
  every `have` line sourced from a `is_shallow = 'X'` commit is
  accompanied by a matching `shallow` line in the built request buffer.
- `zcl_abapgit_ortec_pack_stream`: a `resolve_streaming` test with a
  REF_DELTA chain where the base object is declared **after** the
  dependent delta in pack order (already covered conceptually by the
  hand-trace above, but not confirmed to exist as an actual unit test —
  add one with 3+ levels of chain to catch any regression in the
  multi-sweep convergence).
- `zcl_abapgit_ortec_obj_store`: a `get_objects` test with a mixed
  cache-hit/cache-miss batch immediately after a raw (non-`invalidate_cache`)
  status-promotion `UPDATE`, to lock in that stale `'I'`-status rows can
  never leak into `mt_cache`.
- A `build_upload_pack_buffer` test explicitly asserting that
  `iv_force_full = abap_true` produces a wire buffer containing **only**
  `want`, flush, `done` — no `have`/`shallow`/`deepen` lines whatsoever
  (locks in fix attempt #3, currently unit-test-verified only by inference
  from source reading in this investigation, not confirmed to have its own
  test).

## 5. Verdict on attempts 1–3

All three are **correct, and each independently fixes a real, distinct
defect** (ITAB_DUPLICATE_KEY in `base_cache`, the tier-3 retry-flag
escalation, and the deepen-skip on `force_full`). None of them are flawed,
regressed, or partially applied — all were independently re-verified
present and correct in the current source this session. They are
**correct-but-insufficient**: none of them address the actual remaining
root cause, which (per the ranked hypotheses above) most likely lives in
the shallow-negotiation "have without shallow" gap for tiers 1–2, with an
unresolved and equally important open question of why tier 3's
haves-free/deepen-free/shallow-free bare `want` request reproduces the
identical failure — that gap cannot be closed by source review alone and
needs either a live wire/pack capture of the tier-3 request-response pair,
or independent confirmation (e.g. via GitHub's API) of whether object
`35fcfcb0379260fd21602953a666827b64d931f3` or its referencing commit is
still reachable from any ref in `mkaesemann/abapGit`.

## Risks requiring Michael's review

- Hypothesis 2 (rewritten/GC'd upstream history) cannot be confirmed or
  ruled out without either GitHub-side access/API calls or a raw HTTP
  trace of the tier-3 request — recommend Michael capture one before
  further code changes are attempted, since fixing hypothesis 1 alone will
  not resolve the bug if hypothesis 2 or 3 is the actual cause on the
  force_full tier.
- Fix proposal A (shallow-line emission) changes wire behavior for every
  existing shallow-tracked repo, not just this one — needs Michael's
  sign-off before implementation given the blast radius across all Ortec
  fastpath consumers.

---

# Protocol/persistence review: Variant B Slice 1 (durable materialization model)

Status: **REVIEW, no files edited.** Topic `variant-b-partial-clone`, Slice 1
only. Reviewed `.memory/logs/variant_b_design.md` against the owner spec
(`.github/prompts/variant-b.prompt.md`, Slice 1 section), current DDIC
(`zaog_commit_hist.tabl.xml`, `zaog_repo_state.tabl.xml`) and current
readers/writers (`zcl_abapgit_ortec_repo_state`, `zcl_abapgit_ortec_fetch_neg`).

## 1. Extend `ZAOG_COMMIT_HIST` vs. new table

**Correct call.** Current PK is exactly `(MANDT, REPO_KEY, COMMIT_SHA1)` —
the identical grain the new certificate needs; no key change, no secondary
index defined today, `BUFALLOW = N` (never buffered), so there is no buffer
sync/invalidation concern from the append. Adding 3×CHAR + 2×`TZNTSTMPL`
fields is a pure column append (no reorder, no key touch) — on HANA this is
effectively a metadata-only `ALTER TABLE ADD COLUMN`; on any DB requiring a
table conversion, the row count this table is expected to hold (design's own
estimate: low hundreds–thousands per repo) makes the activation-time
conversion low-risk. **Minor operational note, not a blocker**: schedule the
transport activation for a low-traffic window as routine practice for any
table conversion touching a table that may already hold production rows —
no design change needed.

## 2. `ATTEMPT_ID` (CHAR32, no separate table) vs. Slice 8 needs

**Not a foreclosure, but under-specified — the most important finding of
this review.** Slice 8 asks for *four* distinct identifiers (correlation,
attempt, session, pack) "per network attempt" plus staged-row visibility
scoped to an attempt. Slice 1's single `ATTEMPT_ID` column models "at most
one in-flight certification attempt per (repo, commit)" — a *commit-lifecycle*
identifier. A second, concurrent `begin_attempt` call for the same commit
silently overwrites it (last-writer-wins), which is safe (the first attempt's
later `mark_graph_complete`/`publish_snapshot_complete` correctly raises on
stale `attempt_id` per the design — no corruption), but it means this single
column cannot also carry per-network-call `session_id`/`pack_id`/
`correlation_id` for an attempt that spans multiple pack fetches (e.g. a thin
attempt that falls back to self-contained). That is fine **only if** Slice 8
introduces its own staging construct (most likely an attempt-scoped column
set on `ZAOG_OBJ_STORE` or a small attempt-log table, keyed by this same
`ATTEMPT_ID` as a logical parent) rather than trying to widen this one field.
**Recommendation:** add one sentence to the Slice 1 design doc stating this
split explicitly (commit-level `ATTEMPT_ID` here = parent/lifecycle key;
session/pack/correlation IDs are network-call-level and belong to Slice 8's
own staging schema) — costs nothing now, prevents a real ambiguity/rework
risk once Slice 8 is designed. This does **not** require touching Slice 1's
schema or expanding its scope.

## 3. `SNAP_STATE` denormalization onto `ZAOG_REPO_STATE`

**Safe.** `publish_snapshot_complete` is specified as one LUW, zero
`COMMIT WORK` (AC2, grep-verifiable), so the commit-row UPDATE and the
branch-row UPDATE/INSERT are atomic from any other session's point of view —
uncommitted writes are invisible to concurrent readers regardless of write
order, so there is no dual-write inconsistency window as long as AC2 holds
at implementation time and no caller commits mid-sequence. One real but
non-blocking gap for Slice 3/5 wiring: if `publish_snapshot_complete` is ever
the *first* writer of a `ZAOG_REPO_STATE` row for a brand-new branch (fires
before any `update_after_fetch`), the resulting INSERT would leave
`REMOTE_URL`/`URL_HASH` blank on that row — harmless for `get_repo_key_for_url`
as long as at least one other row for the repo has the URL populated, but
worth a one-line note for whoever wires Slice 3/5 orchestration so it isn't
rediscovered as a bug later.

## 4. Secondary index for have-eligibility checks

**None needed; design's claim is correct.** `is_graph_have_eligible`/
`is_full_have_eligible` are exact-PK reads (`repo_key` + `commit_sha1`), and
`get_complete_commits`'s `WHERE repo_key = ...` scan already uses the leading
PK column — both are natively index-served by the existing primary key with
no new DDIC index required. Matches actual current call sites (single-commit
lookups in `is_commit_complete`, not bulk scans).

## 5. DDIC/release compatibility

**Clean.** `VERIFIED_AT`/`UPDATED_AT` reuse the same `TZNTSTMPL` ROLLNAME
already used by `FETCHED_AT`/`FETCH_TS`/`CHANGED_AT` in both tables; the new
CHAR1 status fields follow the same inline-CHAR1 convention as
`ZAOG_OBJ_STORE-STATUS`; `ATTEMPT_ID` CHAR32 matches the existing
`cl_system_uuid=>create_uuid_c32_static()`/`ty_session_id` convention. No new
domain, no deep/string type, no buffering interaction. No compatibility
concern found.

## Verdict: `APPROVE_WITH_MINOR_REVISIONS`

Minor revisions (documentation only, no schema/scope change): (1) add the
one-sentence Slice-8 ID-split clarification in §2 above to
`variant_b_design.md`; (2) add a one-line wiring note for Slice 3/5 about
`publish_snapshot_complete`'s branch-row INSERT path leaving `REMOTE_URL`
blank for a brand-new branch. Neither blocks proceeding to performance
`DESIGN_GATE` for Slice 1.

## Risks requiring Michael's review

- None requiring owner escalation — both findings above are documentation
  clarifications the design author can add directly, not disagreements with
  the owner spec.
