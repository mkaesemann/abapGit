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

# Protocol/persistence review: Variant B Slice 2 (explicit fetch modes and request serializer)

Status: **REVIEW, read-only, no files edited.** Topic `variant-b-partial-clone`,
Slice 2 only. Reviewed `.memory/logs/variant_b_slice2_design.md` (including
its "Review resolution" section) against
`.memory/logs/variant_b_slice2_reconciliation.md`, the already-resolved
correctness review (`.memory/reviews/variant_b_slice2_design_review.md`,
verdict `APPROVE_WITH_MINOR_REVISIONS`), and live-read source:
`zcl_abapgit_ortec_mat_state.clas.abap` (full), `zcx_abapgit_ortec_git.clas.abap`
(header/constructor), `zcl_abapgit_ortec_pack_stream.clas.abap`
(`reset_completion_budget` + its doc), and a repo-wide grep for `ENQUEUE`
usage under `src/ortec/git`.

## 1. §8 file-list / no-new-DDIC claim

**Confirmed true.** The two new exception attributes
(`mv_unsupported_capability TYPE abap_bool`, `mv_missing_capability TYPE
string`) use only built-in ABAP types — no new data element, domain, or
structure is required to add them to the existing global class
`zcx_abapgit_ortec_git`, exactly mirroring how the pre-existing
`mv_retry_without_haves TYPE abap_bool`/`mv_text TYPE string` attributes on
the same class needed none. The new `zcl_abapgit_ortec_fetch_req` class's own
types (`ty_fetch_mode TYPE c LENGTH 1`, `ty_request` structure) are both
class-local `TYPES` declarations, not DDIC objects, matching the precedent
`zcl_abapgit_ortec_mat_state=>ty_hist_level`/`ty_snap_state` already set in
Slice 1. §8's file list is exactly: one new class+xml, one existing-class
attribute/method addition, two existing-class body-only changes. No
`zaog_*` table, structure, or table type is touched or needed by Slice 2.

## 2. Git wire-protocol correctness (§2.3)

**`INCREMENTAL_THIN`'s haves-gated thin/ofs-delta suppression is correct,
not just defensible.** A real git client never requests `thin-pack`
capability when it has no `have`s to offer: thin-pack only has meaning as
"the server may omit objects reachable from a base the client already has
and delta against it instead" — with zero certified haves there is no base
to delta against, so advertising the capability would be inert at best. The
design's soft-gate (§2.3: "advertised only... and only if
`it_certified_haves` non-empty") reproduces this real client behavior
exactly, and correctly keeps it a *soft* requirement (a missing
`thin-pack`/`ofs-delta` server capability degrades the request, it does not
fail it) — this matches the protocol's own soft/hard capability split (thin
is a response-shape optimization; filter and sha1-in-want are eligibility
gates without which the mode's whole purpose cannot be honored at all).
There is no scenario in the table where a "haves-free thin request" is ever
actually sent — the have-line loop and the capability-token decision share
the same `it_certified_haves` emptiness check, so they cannot disagree.

**`MATERIALIZE_BLOBS`'s "want N blob SHAs directly" pattern is protocol-legal
exactly as scoped, but has a real interop caveat worth flagging even with
correct capability gating.** `allow-reachable-sha1-in-want` is specified
(Documentation/technical/protocol-capabilities.txt) to permit a `want` for
any object reachable from an advertised ref, which by definition includes
blob objects, not only commits/tags — so the design's gating (raise
`mv_unsupported_capability` when the server hasn't advertised this token) is
the structurally correct and sufficient check *for protocol legality*. What
source review cannot settle is *server-implementation* behavior once the
capability is advertised: several widely used git server implementations
have historically scoped their practical support for arbitrary-SHA1 wants
more narrowly than the spec allows (e.g. treating it as "commit reachable
from a ref" rather than "any object reachable from a ref", for reachability-
proof cost and information-disclosure reasons), and reachability
verification for a single deep blob can be considerably more expensive
server-side than for a commit. This cannot be resolved by reading ORTEC's
own source — it depends on which git hosting product(s) the repositories
`zcl_abapgit_ortec_fastpath` talks to actually run, and their current
`upload-pack` behavior. Recorded under Risks below.

**`shallow`/`deepen` removal (DR-005, already resolved in the design) is
independently reconfirmed protocol-correct**: once haves are sourced only
from `is_graph_have_eligible`-certified commits (§4), telling the server
"my history is boundary-truncated at commit X" via `shallow` no longer
describes anything true about the client's local state in the sense the
protocol intends (shallow boundaries model an intentionally truncated clone,
not an as-yet-uncertified one) — collapsing both `shallow` and `deepen` out
of every mode's decision branches is the correct simplification once
certified-have negotiation is the sole trust mechanism.

## 3. Exception-type design: `mv_unsupported_capability` alongside `mv_retry_without_haves`

**Safe as designed, but the class already tolerates an un-enforced
ambiguous state, and Slice 2 adds a third flag to that same pattern rather
than introducing a new risk category.** `zcx_abapgit_ortec_git` already
carries two independently-settable boolean signals today
(`mv_is_corruption`, set only via a direct `constructor` call with
`iv_is_corruption = abap_true` — never via the `raise()` factory;
`mv_retry_without_haves`, set only via `raise()`) with nothing in the class
structurally preventing a future caller from constructing an instance with
both true. In current source, this never happens because `raise_corruption`
routes to a *different* exception class (`zcx_abapgit_exception`) entirely,
and `raise()` never receives `iv_is_corruption`. Adding
`mv_unsupported_capability` via a third, equally dedicated static factory
(`raise_unsupported_capability`, per the design's proposed signature —
taking `iv_mode`/`iv_capability`, not `iv_retry_without_haves`) preserves
the same one-flag-per-factory discipline: as long as every raise path goes
through exactly one of `raise` / `raise_corruption` / the new
`raise_unsupported_capability`, no live call site can produce an instance
with two decision flags simultaneously true, matching the finding
`variant_b_slice2_design_review.md`'s own optional-improvement note already
made about `is_retry_without_haves`-style symmetry. This is not a
regression Slice 2 introduces — the class's own `constructor` signature
already permits the ambiguous combination for `mv_is_corruption`/
`mv_retry_without_haves` and always has. If both flags were ever
simultaneously true on one instance, the only place that would matter is a
catch site that checks one flag without checking whether the other implies
a stronger, correctness-relevant condition; today's fastpath catch sites
(`is_retry_without_haves`) only inspect the one flag they care about and
otherwise fall back to a generic swallow, so no live code path would
mis-prioritize between the two even in the hypothetical case. **Minor,
optional, matches the existing design review's own optional-tier item**:
adding a doc-comment note on `zcx_abapgit_ortec_git` stating that its
decision flags are mutually exclusive by convention (one per dedicated
`raise*` factory), not by structural enforcement, would make this
convention visible rather than merely true-by-accident-of-discipline — not
a blocker for Slice 2.

## 4. `is_commit_complete` behavior swap: concurrent-reader safety against `zcl_abapgit_ortec_mat_state`

**Confirmed against the actual method bodies: `zcl_abapgit_ortec_mat_state`
holds no ENQUEUE lock anywhere** (repo-wide grep for `ENQUEUE` under
`src/ortec/git` finds it only in `zcl_abapgit_ortec_cache_admin` and
`zcl_abapgit_ortec_pack_dec`'s `acquire_repo_lock`/`release_repo_lock` —
`EZAOG_REPO_LOCK`, scoped to the pack-decode/persist phase, not to any
`zcl_abapgit_ortec_mat_state` method). `get_state`/`is_graph_have_eligible`
are plain `SELECT SINGLE`; `begin_attempt`/`mark_graph_complete`/
`invalidate_commit`/`publish_snapshot_complete` are plain read-then-`MODIFY`
sequences with no explicit lock object and no optimistic-concurrency token
beyond the `attempt_id` string comparison. This means:

- **Not a real race in the corruption sense.** Every writer method already
  fails safe under concurrent writers without needing a lock: two
  overlapping `begin_attempt` calls for the same `(repo_key, commit)` simply
  last-writer-wins the `attempt_id` column (both today's Slice 1 review, §2,
  and this review reach the same conclusion) — the losing attempt's later
  `mark_graph_complete`/`publish_snapshot_complete` correctly raises "stale
  attempt ID" rather than silently corrupting `hist_level`/`snap_state`.
  `hist_level` is also structurally never downgraded by any writer.
  Standard DB read-committed isolation additionally guarantees
  `is_graph_have_eligible`'s single-column, single-row `SELECT SINGLE` can
  only ever observe a *fully pre-* or *fully post-*commit value for a
  concurrent `invalidate_commit`/`mark_graph_complete` — never a torn read
  — since each writer is exactly one `MODIFY` statement touching that row.
- **It is, however, a genuine stale-read (TOCTOU) window with a real,
  bounded, self-correcting side effect that is worth naming explicitly,
  which the design does not currently do.** If `is_graph_have_eligible`
  reads a commit's `hist_level = GRAPH_COMPLETE` a moment *before* a
  concurrent `invalidate_commit` call (e.g. corruption just detected
  elsewhere) commits its `UNKNOWN`/`INVALID` write, the have-negotiation
  path (`get_verified_have_commits`, unchanged this slice) will offer that
  commit as a `have` in the in-flight fetch's request. The consequence is
  **not** that the client trusts bad data as complete going forward — the
  next `is_graph_have_eligible` read (e.g. the next fetch attempt, or a
  concurrent one starting slightly later) will correctly see the
  invalidated state once the writer's `MODIFY` is visible. The consequence
  *is* that **this one in-flight fetch** may not receive replacement
  objects for the commit whose corruption was just discovered, because the
  server accepted the `have` as a valid common base and skipped sending its
  reachable objects — deferring healing to a subsequent attempt rather than
  this one. This is a stale-but-safe read with respect to the persistence
  layer (no corrupted state is ever written or read as if certified when it
  never was) but not a stale-but-*inconsequential* read with respect to
  in-flight negotiation outcomes. Recommend one sentence be added to either
  `is_commit_complete`'s doc comment or §4 of the design noting this
  trade-off explicitly — analogous in spirit to how Slice 1's own review
  (§3 above) flagged the `publish_snapshot_complete` new-branch-row gap as a
  documentation-only, non-blocking note. Not a required revision; this
  characteristic is inherent to any O(1) certificate read replacing a
  fresh-per-call tree walk, and Slice 1's own class-doc already frames the
  certificate as a durable record, not a live-recomputed guarantee.

## 5. DR-004 decode-local cache reset: `reset_completion_budget()` idempotency

**Confirmed safe by reading the actual method body.**
`zcl_abapgit_ortec_pack_stream=>reset_completion_budget` is exactly:

```abap
METHOD reset_completion_budget.
  gv_completion_attempts = 0.
ENDMETHOD.
```

— a single `CLASS-DATA` integer reset with no other side effect anywhere in
the method. The counter it resets (`gv_completion_attempts`, bounded by
`c_max_completion_attempts = 20`) exists solely to cap nested
completion-fetch calls *within one top-level decode attempt*
(`complete_missing_base`'s own doc: "shared across an entire top-level fetch
attempt, including any nested completion fetches"). Because
`upload_pack_by_branch`/`by_commit`'s three tiers execute **sequentially**,
not concurrently — tier 2 begins only after tier 1 has fully failed/returned,
and tier 3 (`RECOVERY_BRANCH_FULL`) only after tier 2 has — there is no
"mid-flight" thin or self-contained attempt still consuming this counter at
the moment `RECOVERY_BRANCH_FULL` starts; whatever attempts it made are
already complete by construction before the next tier's code path runs.
Calling `reset_completion_budget()` an extra time immediately before the
recovery tier's own attempt therefore cannot discard state a still-running
prior tier depends on — there is no still-running prior tier at that point.
The design's characterization (giving the recovery tier its own full 20-call
budget rather than sharing whatever the failed thin/self-contained tiers
already spent) is correct and matches the owner's "decode-local cache" reset
requirement for that mode.

## 6. Other findings

- No other protocol- or persistence-layer correctness concern found beyond
  §§2-5 above. The serializer's "zero SQL, zero HTTP" claim (§7 of the
  design) is consistent with everything read this session — no method
  signature proposed for `zcl_abapgit_ortec_fetch_req` takes a repo key,
  opens a cursor, or accepts an `lo_client` reference, so there is no
  plausible path for it to acquire either dependency later without a
  visible signature change.
- The have-list/want-list cardinality bounds stated in §7 (200 haves,
  100 `MATERIALIZE_BLOBS` wants) are unchanged/new caps enforced at
  call-time, not DDIC-level constraints — consistent with §1's finding that
  no schema change is needed or proposed to support them.

## Verdict: `APPROVE_WITH_MINOR_REVISIONS`

Minor, documentation-only revisions (neither blocks proceeding past this
review; neither requires a schema, scope, or architecture change):

1. Add one sentence to `is_commit_complete`'s doc comment or design §4
   making explicit that a stale-but-committed `is_graph_have_eligible` read
   can, in a narrow concurrent-invalidation window, cause one in-flight
   fetch attempt to skip healing a just-invalidated commit (self-corrects on
   the next attempt/read) — finding §4 above.
2. (Optional, matches the correctness review's own optional tier) Document
   on `zcx_abapgit_ortec_git` that its boolean decision flags
   (`mv_is_corruption`, `mv_retry_without_haves`, and the new
   `mv_unsupported_capability`) are mutually exclusive by the convention of
   one flag per dedicated static `raise*` factory, not by any structural
   enforcement on the shared `constructor` — finding §3 above.

No change is required to the approved architecture: the mode enum, the pure
serializer, the `is_commit_complete` swap, and the DR-004 decode-local cache
reset placement all stand as designed.

## Risks requiring Michael's review

- **`MATERIALIZE_BLOBS` blob-SHA1-want interop risk (finding §2).** The
  design's capability-gating on `allow-reachable-sha1-in-want`/
  `allow-tip-sha1-in-want` is structurally correct and sufficient per the
  git protocol specification, but whether the specific git server
  product(s) ORTEC's fetch requests actually target honor arbitrary *blob*
  SHA1 wants in practice (as opposed to commit/tag SHA1 wants) once that
  capability is advertised cannot be determined from source review alone —
  it depends on live server behavior. Recommend a live-system smoke test of
  a single-blob `MATERIALIZE_BLOBS` request against the actual target
  server(s) before Slice 4 (the first slice expected to exercise this mode
  for real) is treated as production-ready, independent of Slice 2's own
  correctness (Slice 2 only builds and gates the request; it does not send
  one in any currently-wired call site).
