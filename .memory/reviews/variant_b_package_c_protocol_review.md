# Variant B Package C — Protocol & Persistence Review (C0)

Reviewer scope: Git wire-protocol + persistence/transaction correctness only.
Baseline: HEAD `cf3d5cb87ca4eec4e68b7975df6fc127a8c5abc2` (Package B SAP_VALIDATED_COMPLETE).
Artifact reviewed: `.memory/logs/variant_b_package_c_design.md` (full read).

## Item 1 — Certified-have query shape vs. ZAOG_COMMIT_HIST primary key

Read `src/ortec/git/zaog_commit_hist.tabl.xml` (local, authoritative — the connected
arc-1 SAP system is a known-unrelated environment per
`/memories/repo/git-state-notes.md`, confirmed again this session: its live
`ZAOG_COMMIT_HIST` lacks HIST_LEVEL/SNAP_STATE/ATTEMPT_ID/VERIFIED_AT/UPDATED_AT
entirely — ignored as not representative).

Primary key (DD03P, KEYFLAG=X, in order): `MANDT`, `REPO_KEY` (CHAR12),
`COMMIT_SHA1` (CHAR40). `REPO_KEY` is the leading non-client key field, so
`WHERE repo_key = @iv_repo_key` (design §5) is a genuine primary-key-prefix range
scan — bounded by this one repository's own certified-row count, not a table
scan across all repositories. Confirmed.

Design §5's SQL:
```
SELECT commit_sha1, updated_at FROM zaog_commit_hist INTO TABLE @DATA(lt_candidates)
  WHERE repo_key = @iv_repo_key AND hist_level IN ( 'G', 'F' ).
```
- One bulk SELECT. No per-candidate SQL anywhere in §5's ABAP-side steps
  (exclude/dedupe/sort/cap all operate on the already-fetched internal table).
- No `ZAOG_OBJ_STORE` (object payload) read — `ZAOG_COMMIT_HIST` is a small
  certification table (SHA1 + 2 one-char flags + timestamps), never object
  bytes.
- Result is capped ABAP-side at `c_max_certified_haves = 50` before being
  returned; not unbounded on the wire.

**Verdict: PASS.** Matches the mandatory "SQL call shape / row batching /
presence-vs-payload" constraints.

## Item 2 — No migrated mode emits deepen/shallow

Read `src/ortec/git/zcl_abapgit_ortec_fetch_req.clas.abap` `build_request` in
full (all 5 `CASE iv_mode` branches: `initial_branch_blobless`,
`incremental_thin`, `incremental_self_contained`, `materialize_blobs`,
`recovery_branch_full`). None of the five branches construct a `deepen` or
`shallow` pkt-line; only `want`/`have`/`filter blob:none`/`0000`/`done` lines
are ever emitted. The class's own header doc independently asserts this
("none of the five modes' decision branches ever emit a `deepen` or `shallow`
token").

**Verdict: PASS**, confirmed by direct source read, not just doc trust.

## Item 3 — RECOVERY_BRANCH_FULL never sends haves

Read `zcl_abapgit_ortec_fastpath.clas.abap` `upload_pack` (lines ~1077–1135)
and the two recovery call sites in `upload_pack_by_branch` (~line 951) and
`upload_pack_by_commit` (~line 1057).

`upload_pack`'s `lt_ortec_haves` is populated **only** inside:
```
IF iv_mode = cs_fetch_mode-incremental_thin OR iv_mode = cs_fetch_mode-incremental_self_contained.
  lt_ortec_haves = zcl_abapgit_ortec_fetch_neg=>get_verified_have_commits(...).
ENDIF.
```
Both recovery tier calls pass `iv_mode = cs_fetch_mode-recovery_branch_full`,
which does not match that IF, so `lt_ortec_haves` stays initial (empty) and is
forwarded as-is into `build_request`'s `it_certified_haves`, which for
`recovery_branch_full` is ignored entirely by `build_request` anyway (that
branch never references `it_certified_haves`).

**Verdict: PASS.**

## Item 4 — mat_state lifecycle preconditions vs. design §6 call order

Read `zcl_abapgit_ortec_mat_state.clas.abap` in full.

- `begin_attempt`: creates row if absent (`hist_level=U`, `snap_state=N`);
  **never downgrades** `hist_level`; sets `snap_state=PENDING` only when
  currently `NONE`/`INVALID` (leaves `COMPLETE`/`PENDING` untouched). No
  `COMMIT WORK`.
- `mark_graph_complete`: raises if no row exists ("no attempt in progress");
  **idempotent no-op returns BEFORE the attempt_id check** if `hist_level`
  already `G`/`F` (see Finding F3 below); otherwise raises on `attempt_id`
  mismatch, then sets `hist_level=G`. No `COMMIT WORK`.
- `mark_full_complete`: idempotent no-op if already `F`; **raises** unless
  current `hist_level = G` (enforces graph-before-full ordering, checked
  before the attempt_id check here, unlike `mark_graph_complete`); raises on
  `attempt_id` mismatch; sets `hist_level=F`. No `COMMIT WORK`.
- `publish_snapshot_complete`: raises unless `hist_level IN (G,F)` ("snapshot
  cannot precede graph"); raises on `attempt_id` mismatch; sets
  `snap_state=COMPLETE` on `ZAOG_COMMIT_HIST` **and** upserts
  `ZAOG_REPO_STATE` (`fetch_commit`, `snap_state=COMPLETE`) in the same call,
  same LUW. No `COMMIT WORK` (doc comment explicitly states this and states
  the caller owns the single commit).
- Grepped the whole file for `COMMIT WORK`: only appears in doc comments
  explaining that the class never issues one. Confirmed no method body
  contains an actual `COMMIT WORK` statement.

Design §6's call order (`begin_attempt` → `verify_tree_closure` →
`mark_graph_complete` → `get_tip_blob_sha1s`/`get_missing_sha1s` →
[`mark_full_complete` → `publish_snapshot_complete`] → `update_after_fetch` →
one `COMMIT WORK`) is consistent with every documented precondition above:
graph-before-full-before-publish is honored, and the single terminal commit is
correctly the only commit in the whole sequence (mat_state contributes none).

**Verdict: PASS, with two non-blocking precision findings (F2, F3 below).**

## Item 5 — Package B's own commits in zcl_abapgit_ortec_cold_init

Grepped `COMMIT WORK` in `zcl_abapgit_ortec_cold_init.clas.abap`: 2 hits,
lines 332 and 409. Read the surrounding code:
- Line 332: last statement of `acquire_blobless_graph` (METHOD starts line
  232), immediately after its own `mark_graph_complete` call.
- Line 409: last statement of `materialize_tip_snapshot` (METHOD starts line
  337), immediately after its own `publish_snapshot_complete` call.

Both are genuinely **inside** their respective methods, each the true final
statement before `ENDMETHOD`. Design §4 COLD_BRANCH correctly does not wrap
these two calls in an additional orchestrator-level commit — doing so would
be a no-op at best (nothing left uncommitted) since both methods are already
self-contained, single-purpose transactions.

**Verdict: PASS.** (This 2-phase/2-commit structure is also the direct cause
of Finding F1 below — flagged there, not here.)

## Item 6 — Dangling-delta-base / thin-pack-safety risk from get_state-only classification

**Finding F1 (real, evidenced, but PRE-EXISTING — not a Package C regression).**

`zcl_abapgit_ortec_cold_init=>acquire_blobless_graph` and `=>materialize_tip_snapshot`
are two **separate transactions with two separate `COMMIT WORK`s** (confirmed
Item 5). Tracing the actual state written:

1. `acquire_blobless_graph` calls `begin_attempt` (→ `snap_state=PENDING` if
   previously `NONE`/`INVALID`) then `mark_graph_complete` (→ `hist_level=G`)
   then commits. **This row (`hist_level=G`, `snap_state=PENDING`) is now
   durable**, independent of whether `materialize_tip_snapshot` ever runs.
2. If `materialize_tip_snapshot` is never called again, fails mid-way (HTTP
   error), or finds blobs still missing after all batches (raises via
   `may_publish_snapshot`), the row is **permanently stuck** at `hist_level=G`,
   `snap_state=PENDING` — `clean_incomplete_attempts` only clears
   `attempt_id`, it never touches `hist_level`/`snap_state`
   (confirmed: its own doc says "does not alter hist_level/snap_state").
3. Design §5's `get_certified_haves` filters **only** on
   `hist_level IN ('G','F')` — it does not check `snap_state`. A stalled row
   from step 2 therefore qualifies as a "certified have" forever, and would
   be offered on the wire as a `have` line in a future
   `INCREMENTAL_THIN`/`INCREMENTAL_SELF_CONTAINED` negotiation for this repo,
   even though its blob set (per the very definition of `GRAPH_COMPLETE`:
   "historical blobs may be promised") was never confirmed present. This is
   the textbook dangling-delta-base hazard: telling the server we hold a
   commit's full object closure when we may not.
4. **This is not new.** Traced the CURRENT productive equivalent:
   `zcl_abapgit_ortec_repo_state=>get_complete_commits` (candidate seed for
   today's `get_have_commits`/`get_verified_have_commits`) does
   `SELECT DISTINCT commit_sha1 FROM zaog_commit_hist WHERE repo_key = ...`
   with **no `hist_level` filter at all**, unions in every
   `zaog_repo_state.fetch_commit` too, and the **only** completeness gate
   anywhere downstream is `is_commit_complete` →
   `zcl_abapgit_ortec_mat_state=>is_graph_have_eligible`
   (`hist_level IN ('G','F')`, identical semantics, identical blind spot to
   `snap_state`). So today's live code has the exact same exposure; Package
   C's `get_certified_haves` reproduces it via a cheaper bulk query rather
   than introducing it.

**This is still worth blocking on**, because: (a) the design's own stated
goal is fixing exactly this class of certification-integrity defect (§0), (b)
the design text never acknowledges or tests this residual gap anywhere in
§5/§10/§14 ("pending/failed/legacy-only ineligible" in §14 most plausibly
covers `hist_level=U`/blank rows, not `hist_level=G`+`snap_state=PENDING` —
the latter is never explicitly listed as a test case), and (c) fixing it is
cheap: add `AND snap_state = 'C'` (require the full lifecycle to have
published) or `AND ( hist_level = 'F' OR snap_state = 'C' )` to §5's SELECT,
with a corresponding `clean_incomplete_attempts`-style follow-up to actually
downgrade/expire stuck `PENDING` rows rather than leaving them silently
have-eligible forever.

**Recommendation:** REQUIRED before C1 sign-off — either (a) tighten §5's
filter to also require `snap_state = 'C'`, explicitly documenting the
resulting behavior change (fewer, but always blob-verified, haves), or (b) if
the team decides today's `is_graph_have_eligible` semantics (accept
graph-only completeness as have-eligible) are intentionally being preserved
as-is, add an explicit written risk-acceptance in §10 plus a §14 test case
asserting `hist_level=G, snap_state=PENDING` is (deliberately) still returned
as a candidate, so this isn't silently unreviewed. Either answer is
acceptable; silence is not.

## Additional findings (minor, non-blocking)

**F2 — §6 step 2 exception-catch specificity not stated.**
`zcl_abapgit_ortec_obj_store=>verify_tree_closure` is declared
`RAISING zcx_abapgit_ortec_git` only (confirmed via its signature and every
raise site inside the method — commit/tree not found, wrong type, undecodable,
unrecognized chmod all raise this one type). Design §6 step 2 ("On failure:
do not call mark_graph_complete...") must explicitly `TRY ... CATCH
zcx_abapgit_ortec_git` around this one call — catching anything broader (or
an un-scoped blanket catch) would risk silently absorbing a genuine
technical/SQL failure inside the tree-walk as "just not graph complete yet,"
directly contradicting §5's own stated principle that technical certificate-
read failures must propagate uncaught. The design text doesn't specify the
catch's exception type; should be made explicit in the implementation spec.

**F3 — `mark_graph_complete`'s idempotency check precedes its attempt_id check.**
Unlike `mark_full_complete` (which checks idempotency, then hist_level
ordering, then attempt_id), `mark_graph_complete` returns as an idempotent
no-op as soon as `hist_level` is already `G`/`F`, **before** ever comparing
`attempt_id`. A call with a stale/foreign `attempt_id` against an
already-graph-complete row therefore succeeds silently instead of raising.
Benign for design §6's own call order (its `attempt_id` is always freshly
minted by `begin_attempt` immediately prior in the same call chain), but
worth a one-line note in the design if `mark_graph_complete` is ever called
from a second, independent caller in a later package.

**F4 — `get_certified_haves` narrows the candidate source vs. legacy `get_complete_commits`.**
`get_complete_commits` (today) unions `zaog_commit_hist` SHA1s **and**
`zaog_repo_state.fetch_commit` pointers per branch. Design §5's
`get_certified_haves` reads only `zaog_commit_hist`. Any branch whose
`fetch_commit` was set without a corresponding `zaog_commit_hist` row (e.g.
very old data, or a branch touched only by non-Ortec/standard abapGit code)
would silently stop being offered as a have candidate under the new method.
This is a fail-safe-direction change (fewer haves offered, never an unsafe
extra have) and not a correctness bug, but the design should state this
trade-off explicitly rather than silently narrowing the source.

## Summary

| # | Topic | Result |
|---|---|---|
| 1 | Certified-have query shape / PK-prefix access | PASS |
| 2 | No migrated mode emits deepen/shallow | PASS |
| 3 | RECOVERY_BRANCH_FULL never sends haves | PASS |
| 4 | mat_state lifecycle preconditions vs. §6 call order | PASS (F2, F3 minor) |
| 5 | Package B self-commits, no redundant orchestrator commit | PASS |
| 6 | Dangling-delta-base risk from snap_state-blind have certification | **REQUIRED FIX** (F1) — pre-existing, unaddressed, must be an explicit design decision |

STATUS: **APPROVE_WITH_MINOR_REVISIONS** — F1 must be resolved (either tighten
the filter or explicitly document/accept + test the residual risk) before C1
implementation begins; F2–F4 are documentation/precision fixes only.
