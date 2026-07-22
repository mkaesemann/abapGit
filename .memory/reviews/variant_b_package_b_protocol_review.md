# VB-B-B0-PROTOCOL — Protocol/persistence review of Package B (Slices 3+4)

Reviewer: protocol/persistence agent (design-only review, no ABAP written).
Baseline verified: `cbe8bc73f7c1e17e3d436b1371fcc65f806dd989` (matches design
doc header and current `git rev-parse HEAD`).
Design reviewed: `.memory/logs/variant_b_package_b_design.md` (16 sections +
performance model + acceptance-scenario map).

Method: every quoted/summarized behavior claim in the design was checked
against the real, current source of the five cited symbols (not the design
doc's own paraphrase). No productive ABAP exists for Package B yet — this is
a pure design-vs-reality consistency check plus an independent protocol/
persistence soundness review.

---

## 1. `deepen`/`shallow` — VERIFIED CLEAN

Read `zcl_abapgit_ortec_fetch_req=>build_request` in full
([zcl_abapgit_ortec_fetch_req.clas.abap](../../src/ortec/git/zcl_abapgit_ortec_fetch_req.clas.abap#L128-L237)).
All five `CASE iv_mode` branches (`incremental_thin`,
`incremental_self_contained`, `initial_branch_blobless`,
`materialize_blobs`, `recovery_branch_full`) were read line-by-line. Neither
the string literal `deepen` nor `shallow` appears anywhere in the method.
Design claim (class doc line 11, §1) is **accurate**.

## 2. `INITIAL_BRANCH_BLOBLESS` request shape — VERIFIED CLEAN

Lines 136–154: `validate_single_want` (exactly one want, else raises) →
`IF NOT ( iv_server_caps CS c_cap_filter )` → typed raise via
`zcx_abapgit_ortec_git=>raise_unsupported_capability( iv_mode, c_cap_filter )`
→ **only then** does the method build `rs_request-buffer`. The buffer is
want-lines + `filter blob:none` pkt-line + `0000` flush + `done` — **no have
lines are ever appended for this mode** (`rs_request-have_count = 0` is the
only have-related field touched). This is structurally impossible to
violate later since the have-building helper (`build_have_lines`) is never
called in this branch.

The capability check happens entirely in-memory, before any buffer bytes
are assembled, and `build_request`/`parse_capabilities` are documented and
confirmed (class-doc line 7) to perform **zero SQL and zero HTTP** — so
"gates before the first HTTP call" is trivially true for this method in
isolation. The actual HTTP call ordering promised by design §11 (capability
gate before `send_receive_close`) is a **forward design decision for the
not-yet-written orchestrator**, not something verifiable against existing
code — flagged as **INFO**, not a defect (see §8 below, no code exists to
falsify it, and the design's own step ordering in §11.4 places `build_request`
strictly before the send in every batch).

Design claims in §2/§6 are **accurate**.

## 3. `MATERIALIZE_BLOBS` request shape — VERIFIED CLEAN

Lines 192–221: want-count validated first (`IF it_want_hashes IS INITIAL`
→ raise; `IF lines(...) > c_materialize_batch_max` → raise), **then**
capability check (`allow-reachable-sha1-in-want` else
`allow-tip-sha1-in-want` else typed raise), **then** buffer assembly. No
`build_have_lines` call in this branch (`have_count = 0` unconditionally),
no `thin`/`ofs-delta`/`filter` capability requested or emitted. Design §2/§11
claims are **accurate**. `c_materialize_batch_max = 100` constant confirmed
at line 35, matching design §10's reused-constant claim (design does not
redefine it).

## 4. Two-part commit boundary — VERIFIED, with one clarification

- `zcl_abapgit_ortec_mat_state` — confirmed **zero** `COMMIT WORK`/
  `ROLLBACK WORK` statements anywhere in the class (class doc line 5–8
  states this explicitly, and a full read of `begin_attempt`,
  `mark_graph_complete`, `publish_snapshot_complete`, `mark_full_complete`,
  `invalidate_commit`, `clean_incomplete_attempts` confirms it — every
  method ends on `MODIFY`/`UPDATE` + a raise on `sy-subrc <> 0`, never a
  commit).
- `zcl_abapgit_ortec_pack_stream=>decode_and_persist_streaming` issues its
  own `COMMIT WORK` at
  [line 976](../../src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.abap#L976)
  (success path, immediately after the set-based
  `UPDATE zaog_obj_store SET status = 'R' ... WHERE status = c_status_incomplete`
  two lines above) **and** at lines 980/985 (failure paths, after
  `cleanup_incomplete` deletes the run's own `'I'`-status rows — i.e. commit
  of a rollback-by-delete, not commit of partial work).
- `resolve_streaming` issues its own `COMMIT WORK` at
  [line 758](../../src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.abap#L758)
  on success; on failure it does **not** commit or rollback itself (per its
  own doc, lines 88–91) — `decode_streaming` (the caller) does
  `ROLLBACK WORK` at [line 798](../../src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.abap#L798)
  in its `CATCH` block.
- Net effect confirmed exactly as design §4.1 states: by the time
  `decode_streaming` returns successfully to any caller, **two** separate
  `COMMIT WORK`s have already executed inside it (one per internal phase),
  and every object row it returned is durably `'R'`.

**Clarification worth recording (not a defect):** promotion to `status='R'`
at line 976 is a single blanket `UPDATE ... WHERE status = c_status_incomplete`
for the **whole pack_id**, which also promotes not-yet-resolved delta rows
still keyed under their synthetic `temp_key` (not a real content SHA1) to
`'R'`. This is pre-existing, unchanged Slice 2C behavior that Package B does
not touch or rely on beyond "decode_streaming returns real-SHA1 rows
already committed" — harmless because nothing outside `pack_stream` itself
ever queries by a `temp_key` value, but worth the implementer knowing this
is *not* a two-phase-commit in the ACID sense: it is two independent,
sequential, non-atomic LUW boundaries the design deliberately treats as
"already-durable, content-addressed, harmless-if-orphaned" rather than
requiring atomicity with the eventual certificate commit. This matches
INV-B-02's actual guarantee (no certificate without prior verification),
not a stronger "graph objects and certificate commit atomically" guarantee,
and the design never claims the stronger one — consistent.

`publish_snapshot_complete`'s precondition
(`ls_row-hist_level <> cs_hist_level-graph_complete AND <> cs_hist_level-full_complete`
→ raise, [lines 285–290](../../src/ortec/git/zcl_abapgit_ortec_mat_state.clas.abap#L285-L290))
and both methods' stale-attempt-ID guards
(`IF ls_row-attempt_id <> iv_attempt_id` → raise, present in
`mark_graph_complete`, `publish_snapshot_complete`, `mark_full_complete`)
are **confirmed exactly as design §4/§12 describes**.

## 5. `verify_tree_closure`/`get_tip_blob_sha1s` vs. `get_reachable_sha1s` — VERIFIED, blob-presence distinction is real and necessary

`get_reachable_sha1s`
([lines 516–622](../../src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap#L516-L622))
walks commit→tree→blob identically to `get_reachable_objects`, but for the
final blob set calls `get_present_sha1s` and **raises**
(`zcx_abapgit_ortec_git=>raise( |Object { <lv_blob> } not found in store| )`,
line ~617) for **any** blob not present with `status='R'`. This is a hard,
unconditional raise with no distinction between "genuinely corrupt/missing"
and "intentionally filtered by `blob:none`" — confirmed this method is
unconditionally unsuitable for a blobless-fetch closure check, exactly as
design §6 argues. `verify_tree_closure`/`get_tip_blob_sha1s` correctly avoid
this by never checking blob presence at all (only trees/commit) and, for
`get_tip_blob_sha1s`, returning blob SHA1s unfiltered by presence for the
caller to bulk-subtract separately (§10 of the design, via
`get_missing_sha1s`, itself confirmed correct — see §6 below). This
distinction is real, necessary, and correctly reasoned.

## 6. Bulk database lookup shapes — ONE MAJOR DISCREPANCY FOUND

`get_missing_sha1s` → `get_present_sha1s`
([lines 779–826](../../src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap#L779-L826)):
confirmed genuinely chunked — builds a `RANGE` table incrementally and
flushes a `SELECT ... WHERE obj_sha1 IN lr_sha1s AND status = 'R'` every
`c_select_package_size` (1000) entries, plus one final flush. Never selects
`obj_data`. Design's claim ("already chunked, already `obj_data`-free") is
**accurate**.

**However**, `get_objects( iv_bulk_fetch = abap_true )` — the primitive the
design proposes reusing for the tree/commit frontier reads in both new
methods (§6 step 3, §9) and the primitive `get_reachable_objects`/
`get_reachable_sha1s` already use for the same purpose — does **not**
chunk at all when `iv_bulk_fetch = abap_true`:

```
IF iv_bulk_fetch = abap_true.
  LOOP AT lt_missing_sha1s ASSIGNING <lv_sha1>.
    ...
    APPEND ls_sha1 TO lt_package.
  ENDLOOP.
  IF lt_package IS NOT INITIAL.
    lt_db_rows = read_object_rows( iv_repo_key = iv_repo_key it_sha1s = lt_package ).
```
([lines 348–357](../../src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap#L348-L357))
— the `c_select_package_size` chunking loop only exists in the `ELSE`
(non-bulk) branch immediately below it. `read_object_rows`
([lines 757–776](../../src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap#L757-L776))
itself builds **one** `RANGE` table from the **entire** input list and
issues **one** `SELECT * FROM zaog_obj_store ... WHERE obj_sha1 IN lr_sha1s`
— not `FOR ALL ENTRIES`, and with no size cap.

This means: for any single frontier iteration (§6 step 3 / §9), the
proposed methods would issue exactly **one unbounded `SELECT ... IN`**
covering the entire current tree frontier, regardless of its size — not
"`O(F)` bulk **chunked** SELECTs, each ≤ `ceil(T_frontier/1000)` package
reads" as the design's own Mandatory Performance Model table states for
`verify_tree_closure`/`get_tip_blob_sha1s`. For a normal, deeply-nested
repository this is harmless (frontier width per level is small). For a
pathologically flat/wide tree (e.g. thousands of top-level
package/module directories in one commit's root tree, which does occur in
real monorepos) this becomes a single `SELECT` with a multi-thousand-entry
`IN` range in one statement — a real, uncapped statement-size/optimizer-
load risk this design's own performance section claims does not exist.

This is **pre-existing behavior** (`get_objects`, `read_object_rows`,
`get_reachable_objects`, and `get_reachable_sha1s` are all unchanged,
already-shipped Slice 1/2 code) — Package B does not introduce the gap,
it inherits it via reuse and then **mischaracterizes it as already chunked**
in a document whose explicit job is to state accurate SQL call shapes.

**Finding (MAJOR, not blocking):** correct the Mandatory Performance Model
table's row for `verify_tree_closure`/`get_tip_blob_sha1s` to state the
true shape (`O(F)` calls, each **one unbounded `IN`-range SELECT over the
full current frontier**, not chunked), and decide explicitly — before or
during implementation, not silently — whether that is acceptable as-is
(mirrors already-validated `get_reachable_objects`/`get_reachable_sha1s`
risk profile, so arguably no *new* risk) or whether the two new methods
should chunk the frontier into `c_select_package_size` groups themselves
before each `get_objects( iv_bulk_fetch = abap_true )` call (a purely
additive, non-invasive change confined to the two new methods, touching no
existing code). Either answer is acceptable for approval; silently keeping
the current mischaracterization in the design's performance table is not.

## 7. `MATERIALIZE_BLOBS` batch construction and capability gate ordering — VERIFIED

`build_request`'s `materialize_blobs` branch never touches `have`, `thin`,
`deepen`, `shallow`, or `filter` tokens (confirmed by full read, §3 above).
The capability check (lines 210–217) runs unconditionally before buffer
assembly and is pure in-memory logic (class doc: zero SQL/HTTP for
`build_request` itself). Design §11's orchestration ordering (build request
→ send → split-on-oversize → decode) places the capability-raising call
before the first HTTP send in every batch; since `materialize_tip_snapshot`
does not exist yet, this is a forward commitment rather than a verifiable
fact, but nothing in the cited primitives contradicts it, and the design
explicitly notes the capability failure "fails identically for every
subsequent batch" so it is checked once — an acceptable, non-wasteful
design choice given `build_request` has no state and no side effects.

## 8. Idempotent restart — VERIFIED against real guard logic

- `begin_attempt`
  ([lines 253–275](../../src/ortec/git/zcl_abapgit_ortec_mat_state.clas.abap#L253-L275)):
  confirmed it never modifies `hist_level` on an existing row (only sets it
  when the row is freshly created), and only advances `snap_state` from
  `NONE`/`INVALID` to `PENDING` (leaves `COMPLETE`/`PENDING` alone).
- `mark_graph_complete`
  ([lines 277–295](../../src/ortec/git/zcl_abapgit_ortec_mat_state.clas.abap#L277-L295)):
  confirmed idempotent no-op (`RETURN`) if `hist_level` is already
  `GRAPH_COMPLETE`/`FULL_COMPLETE`, confirmed stale-attempt-ID raise.
- `publish_snapshot_complete`
  ([lines 297–330](../../src/ortec/git/zcl_abapgit_ortec_mat_state.clas.abap#L297-L330)):
  confirmed `hist_level >= GRAPH_COMPLETE` precondition raise, confirmed
  stale-attempt-ID raise.
- `get_missing_sha1s` re-check (design §11 steps 2 and 5): confirmed
  correct, chunked, `obj_data`-free (§6 above) — a prior partial
  `materialize_tip_snapshot` run's already-persisted blobs are never
  re-requested on retry.

No downgrade path exists in any of the three methods. Design §14's
idempotent-restart claims **hold** against real code.

One caveat correctly disclosed by the design itself and confirmed here:
`INITIAL_BRANCH_BLOBLESS`'s want-only, have-free request shape (§2/§3 above)
means a retried `acquire_blobless_graph` re-downloads the **entire**
commit+tree closure from the server every time, even if 99% of it is
already `'R'` from a prior failed attempt — this is a real, disclosed
network-cost inefficiency, not a correctness defect, and is explicitly
acknowledged in design §14 ("the server always resends the full closure
regardless"). No further action needed; flagged here only so it is not
mistaken for an oversight.

## 9. Oversized-batch split policy (§10) — no invariant violation found

- Split happens strictly **after** `send_receive_close` returns and
  **before** `decode_streaming` is invoked (design §11 step ordering:
  4c "apply split policy" precedes 4d "decode_streaming") — so an
  oversized response is never partially decoded before being discarded and
  re-split; nothing is persisted from a response that gets split.
- The single `attempt_id` from `begin_attempt` (§11.3) is generated once
  per `materialize_tip_snapshot` call and is not re-derived per batch or
  per split — splitting a batch into two smaller `MATERIALIZE_BLOBS`
  requests does not call `begin_attempt` again, so no double-counting or
  attempt-ID churn occurs.
- The 7-split budget is a call-scoped local counter (not `CLASS-DATA`), so
  it cannot leak state across separate `materialize_tip_snapshot`
  invocations or interfere with `pack_stream`'s own unrelated
  `gv_completion_attempts` budget.
- A single-SHA1 batch that still exceeds the byte ceiling, or a
  split-budget exhaustion, raises rather than silently accepting an
  unbounded response — confirmed as a design decision (no code to
  contradict, since `c_max_batch_response_bytes` doesn't exist yet); this
  is consistent with every other hard-fail-not-silent-fallback pattern
  already verified in the real capability-gate code (§2/§3).

No violation of any existing invariant found.

## 10. Package D2 dependency check — NOT required by any Package B decision

Re-read design §4 and §16 specifically for any hidden dependency on a
"final attempt/session/pack identity architecture." Confirmed: Package B's
two new orchestration methods each generate exactly one `attempt_id` via
the **existing, unchanged** `begin_attempt`, use it only within their own
single call, and never assume any cross-call/cross-session identity
tracking, staged-vs-published visibility model, or session/pack-scoped
budget beyond the already-existing per-call `gv_completion_attempts` in
`pack_stream` (untouched, and per design §16 explicitly out of reach since
`MATERIALIZE_BLOBS` responses are non-thin/self-contained). **No BLOCKED_BY
condition applies** — Package B's design is genuinely independent of
Package D2's still-undesigned final transaction architecture, matching its
own §16 claim.

## 11. Secondary accuracy check — `begin_attempt`'s UUID call (MINOR)

Design §4.4 states the attempt ID is generated via
"`cl_system_uuid=>create_uuid_x16_static` fed through the same helper
`zcl_abapgit_ortec_mat_state` already uses internally." The real code
(`begin_attempt`,
[line 258](../../src/ortec/git/zcl_abapgit_ortec_mat_state.clas.abap#L258))
calls `cl_system_uuid=>create_uuid_c32_static( )` directly — there is no
separate "x16-then-hex" helper, and the method name differs
(`create_uuid_c32_static`, not `create_uuid_x16_static`). This does not
change the actual design decision (Package B calls `begin_attempt()` and
never generates an attempt ID itself, per §11.3 — the operative, correct
commitment), so it is **cosmetic**, but should be corrected so the design
doc does not misdescribe the reused primitive for a future reader who
diffs it against source.

## 12. Schema/materialization-separation check (protocol/persistence agent's own mandate)

- `zaog_commit_hist` is keyed `repo_key + commit_sha1` (confirmed via every
  `SELECT SINGLE ... WHERE repo_key = ... AND commit_sha1 = ...` in
  `mat_state`) — a **repository-wide, commit-scoped** fact, correctly
  independent of branch, matching the required separation between physical/
  commit-graph facts and branch/ref pointers.
- `zaog_repo_state` is keyed `repo_key + branch_name` — a **branch-specific**
  fact (`publish_snapshot_complete` writes it separately from
  `zaog_commit_hist`, confirmed lines ~316–329).
- `zaog_obj_store` (via `get_objects`/`get_missing_sha1s`/`read_object_rows`)
  is keyed `repo_key + obj_sha1`, no branch column anywhere in any cited
  read — matches "do not include branch in the physical object-store key."
- No method reviewed infers graph/snapshot completeness from object
  presence alone (`is_graph_have_eligible`/`is_full_have_eligible` read the
  certificate table directly, not `zaog_obj_store`) — matches "do not infer
  completeness solely from object presence."
- No certificate write (`mark_graph_complete`/`publish_snapshot_complete`)
  is reachable without a preceding successful verification step in the same
  call, per the design's own §12 ordering and the precondition raises
  confirmed in §4/§8 above — matches "do not publish pending attempt data
  as READY" (a `zaog_obj_store` row reaching `'R'` is never itself treated
  as a certificate anywhere in the reviewed code or the design).

No schema or materialization-boundary violation found. Package B proposes
**zero** new tables/columns — this review's "schema/index proposal"
deliverable is therefore: **none needed**; the existing `zaog_commit_hist` /
`zaog_repo_state` / `zaog_obj_store` schema and its repo-wide vs.
branch-specific key split (documented above) is sufficient and correctly
respected by this design as written.

---

## Method signatures reviewed (no changes proposed beyond the design's own)

Both new signatures (`verify_tree_closure`, `get_tip_blob_sha1s`) reuse
existing types (`ty_repo_key`, `ty_sha1`, `ty_sha1_tt`) and the existing
`zcx_abapgit_ortec_git` exception — no new type needed, confirmed against
`zif_abapgit_git_definitions` usage patterns already present in
`get_reachable_sha1s`. The only concrete recommendation from this review is
in §6: if the implementer chooses to add explicit frontier chunking, the
natural place is a private helper inside `zcl_abapgit_ortec_obj_store`
(e.g. chunking `it_sha1s` into `c_select_package_size` groups before each
`get_objects( iv_bulk_fetch = abap_true )` call within the new methods'
own loop) — no signature change to `get_objects` itself is required or
recommended, since `get_reachable_objects`/`get_reachable_sha1s` also call
it and changing its contract is out of Package B's scope.

---

## Summary of findings

| # | Severity | Item | Disposition |
|---|---|---|---|
| 1 | MAJOR | §6 Mandatory Performance Model row for `verify_tree_closure`/`get_tip_blob_sha1s` mischaracterizes `get_objects(iv_bulk_fetch=abap_true)` as chunked `FOR ALL ENTRIES` reads; real code is one unbounded `SELECT...IN` per frontier iteration via `read_object_rows`. | Correct the table and make an explicit chunking decision (reuse-as-is vs. add local chunking) before/during implementation. |
| 2 | MINOR | §4.4 misnames the UUID primitive (`create_uuid_x16_static` vs actual `create_uuid_c32_static`) and implies a separate helper that doesn't exist. | Cosmetic; fix wording, no design change needed. |
| — | INFO | Retry-without-haves inefficiency of `INITIAL_BRANCH_BLOBLESS` on restart (full re-download every retry) | Already disclosed by design §14; no action needed. |

No BLOCKING findings. No violation of INV-B-01 through INV-B-11 found
against real source. No dependency on Package D2 found. No schema change
proposed or needed.

**Verdict: APPROVE_WITH_MINOR_REVISIONS.**
